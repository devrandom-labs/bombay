//! One local typed mailbox behind the existing Engine environment port.
//!
//! This is deliberately crate-private. It proves the mailbox/address layer
//! without prescribing construction, task, handle, or System APIs.

use core::future::{Future, pending};
use core::hash::Hash;
use core::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Instant;

use crate::address::MailAddr;
use crate::interpret::ActionSettlementOf;
use crate::observation::FactQueue;
use crate::observe::{Observation, Publisher, affine_pair};
use crate::time::LocalTimers;
use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, BehaviorSettlements, ClassifySettlement,
    EstablishedRecipient, InjectEvent, Interpretation, Never, Protocol, SourceCustody, User,
    UserEvent,
};
use behavior_actors::{Exit, ShutdownRejection, ShutdownRequested};
use bombay_address::{AddressSpace, ClaimError, Lease};
use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};
use communication::{
    Config, Consumer, ControlClosed, ControlSender, Drained, MailboxOwner, MailboxRef, Received,
    UserClosed, mailbox_channel,
};
use tokio::sync::oneshot;
use tokio::task::JoinSet;

pub(crate) type Termination<A> = Result<Exit<A>, behavior_actors::Crash>;

/// Actor-owned external activation work with exact closed-lane recovery.
pub(crate) struct ActivationTasks<E> {
    tasks: JoinSet<Result<(), E>>,
}

impl<E> ActivationTasks<E> {
    pub(crate) fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }

    pub(crate) fn spawn(&mut self, task: impl Future<Output = Result<(), E>> + Send + 'static)
    where
        E: Send + 'static,
    {
        self.tasks.spawn(task);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(crate) async fn settle(mut self) -> Vec<E>
    where
        E: 'static,
    {
        let mut control = Vec::new();
        while let Some(completed) = self.tasks.join_next().await {
            match completed {
                Ok(Ok(())) => {}
                Ok(Err(event)) => control.push(event),
                Err(failure) if failure.is_panic() => {
                    std::panic::resume_unwind(failure.into_panic())
                }
                Err(failure) => panic!("activation task was cancelled unexpectedly: {failure}"),
            }
        }
        control
    }
}

/// Exact actor-local capability output transferred into environment retirement.
pub(crate) struct CapabilityRetirement<E, Descendants> {
    pub(crate) activation_tasks: ActivationTasks<E>,
    pub(crate) descendants: Descendants,
}

/// Affine fact that the actor's task owner requested forced retirement.
pub(crate) struct OwnerCancellation;

#[cfg(test)]
impl<E, Descendants> CapabilityRetirement<E, Descendants> {
    pub(crate) fn without_activations(descendants: Descendants) -> Self {
        Self {
            activation_tasks: ActivationTasks::new(),
            descendants,
        }
    }
}

/// Commits one complete action value for the concrete local actor.
///
/// Product traversal and concrete runtime services remain behind this private
/// seam. It is not an application extension API.
pub(crate) trait CommitActions<B: BehaviorSettlements<Ph = Never>> {
    type Retired;

    fn commit(
        &mut self,
        actions: ActionsOf<B>,
    ) -> impl Future<Output = Interpretation<ActionSettlementOf<B>>> + Send;

    fn offer_next(
        &mut self,
        settlement: ActionSettlementOf<B>,
    ) -> impl Future<Output = SourceCustody<ActionSettlementOf<B>>> + Send;

    fn retire(self) -> impl Future<Output = CapabilityRetirement<B::Event, Self::Retired>> + Send
    where
        Self: Sized + Send;
}

/// Shared access to one affine Communication admission owner.
///
/// The active environment holds the only strong reference. Actor references
/// hold a weak reference so they cannot keep admission open. Closing takes the
/// owner exactly once; dropping the final strong reference closes it through
/// Communication's `MailboxOwner` drop law.
pub(crate) struct Admission<U> {
    owner: Mutex<Option<MailboxOwner<U>>>,
}

#[allow(
    dead_code,
    reason = "the Entity ingress variant remains private until typed application topology can materialize its requirements"
)]
pub(crate) enum LocalIngress<A, M> {
    Message(User<A, M>),
    Fence(Publisher<Result<(), crate::entity::FenceFailure>>),
}

pub(crate) struct StandardIngress;

#[allow(
    dead_code,
    reason = "the Entity ingress mode remains private until typed application topology can materialize its requirements"
)]
pub(crate) struct EntityIngress;

pub(crate) enum EndpointMailbox<A, M> {
    Standard {
        mailbox: MailboxRef<User<A, M>>,
        admission: Weak<Admission<User<A, M>>>,
    },
    Entity {
        mailbox: MailboxRef<LocalIngress<A, M>>,
        admission: Weak<Admission<LocalIngress<A, M>>>,
    },
}

impl<A, M> Clone for EndpointMailbox<A, M> {
    fn clone(&self) -> Self {
        match self {
            Self::Standard { mailbox, admission } => Self::Standard {
                mailbox: mailbox.clone(),
                admission: admission.clone(),
            },
            Self::Entity { mailbox, admission } => Self::Entity {
                mailbox: mailbox.clone(),
                admission: admission.clone(),
            },
        }
    }
}

impl<A, M> EndpointMailbox<A, M> {
    fn close_admission(&self) -> Option<()> {
        match self {
            Self::Standard { admission, .. } => {
                admission.upgrade().and_then(|admission| admission.close())
            }
            Self::Entity { admission, .. } => {
                admission.upgrade().and_then(|admission| admission.close())
            }
        }
    }
}

pub(crate) trait IngressMode<B: Behavior> {
    type Item: Send;
    type Retired;

    fn endpoint(
        mailbox: MailboxRef<Self::Item>,
        admission: Weak<Admission<Self::Item>>,
    ) -> EndpointMailbox<BehaviorAddr<B>, BehaviorMessage<B>>;

    fn into_event(item: Self::Item) -> Option<B::Event>;

    fn retain_at_retirement(item: Self::Item) -> Option<Self::Retired>;
}

impl<B> IngressMode<B> for StandardIngress
where
    B: Behavior,
    B::Event: UserEvent<Addr = BehaviorAddr<B>, Message = BehaviorMessage<B>>,
    User<BehaviorAddr<B>, BehaviorMessage<B>>: Send,
{
    type Item = User<BehaviorAddr<B>, BehaviorMessage<B>>;
    type Retired = Self::Item;

    fn endpoint(
        mailbox: MailboxRef<Self::Item>,
        admission: Weak<Admission<Self::Item>>,
    ) -> EndpointMailbox<BehaviorAddr<B>, BehaviorMessage<B>> {
        EndpointMailbox::Standard { mailbox, admission }
    }

    fn into_event(user: Self::Item) -> Option<B::Event> {
        Some(B::Event::user(user.from, user.message))
    }

    fn retain_at_retirement(item: Self::Item) -> Option<Self::Retired> {
        Some(item)
    }
}

impl<B> IngressMode<B> for EntityIngress
where
    B: Behavior,
    B::Event: UserEvent<Addr = BehaviorAddr<B>, Message = BehaviorMessage<B>>,
    LocalIngress<BehaviorAddr<B>, BehaviorMessage<B>>: Send,
{
    type Item = LocalIngress<BehaviorAddr<B>, BehaviorMessage<B>>;
    type Retired = User<BehaviorAddr<B>, BehaviorMessage<B>>;

    fn endpoint(
        mailbox: MailboxRef<Self::Item>,
        admission: Weak<Admission<Self::Item>>,
    ) -> EndpointMailbox<BehaviorAddr<B>, BehaviorMessage<B>> {
        EndpointMailbox::Entity { mailbox, admission }
    }

    fn into_event(item: Self::Item) -> Option<B::Event> {
        match item {
            LocalIngress::Message(user) => Some(B::Event::user(user.from, user.message)),
            LocalIngress::Fence(publisher) => {
                publisher.complete(Ok(()));
                None
            }
        }
    }

    fn retain_at_retirement(item: Self::Item) -> Option<Self::Retired> {
        match item {
            LocalIngress::Message(user) => Some(user),
            LocalIngress::Fence(publisher) => {
                publisher.complete(Err(crate::entity::FenceFailure::Acknowledgement));
                None
            }
        }
    }
}

fn collect_retired_ingress<B, M>(
    consumer: Consumer<B::Event, M::Item>,
) -> Drained<B::Event, M::Retired>
where
    B: Behavior,
    M: IngressMode<B>,
{
    let Drained { control, user } = consumer.drain();
    let user = user
        .into_iter()
        .filter_map(M::retain_at_retirement)
        .collect();
    Drained { control, user }
}

struct LocalInbox<B, M>
where
    B: Behavior,
    M: IngressMode<B>,
{
    consumer: Option<Consumer<B::Event, M::Item>>,
    mode: PhantomData<fn() -> M>,
}

impl<B, M> LocalInbox<B, M>
where
    B: Behavior,
    M: IngressMode<B>,
{
    fn new(consumer: Consumer<B::Event, M::Item>) -> Self {
        Self {
            consumer: Some(consumer),
            mode: PhantomData,
        }
    }

    async fn recv(&mut self) -> Option<Received<B::Event, M::Item>> {
        self.consumer
            .as_mut()
            .expect("active local inbox retains its consumer")
            .recv()
            .await
    }

    async fn recv_source(&mut self) -> Option<B::Event> {
        self.consumer
            .as_mut()
            .expect("active local inbox retains its consumer")
            .recv_control()
            .await
    }

    fn drain(&mut self) -> Option<Drained<B::Event, M::Retired>> {
        self.consumer.take().map(collect_retired_ingress::<B, M>)
    }
}

impl<B, M> Drop for LocalInbox<B, M>
where
    B: Behavior,
    M: IngressMode<B>,
{
    fn drop(&mut self) {
        drop(self.drain());
    }
}

#[allow(
    dead_code,
    reason = "queued Entity fences are drained only by the private native Entity ingress mode"
)]
impl<A, M> LocalIngress<A, M> {
    fn fail_fence(self) {
        if let Self::Fence(publisher) = self {
            publisher.complete(Err(crate::entity::FenceFailure::Acknowledgement));
        }
    }
}

trait ShutdownControl: Send + Sync {
    fn request(&self) -> Result<(), ShutdownSignalRejection>;
}

enum ShutdownSignalRejection {
    ControlClosed,
}

struct TypedShutdownControl<E> {
    control: ControlSender<E>,
}

impl<E> ShutdownControl for TypedShutdownControl<E>
where
    E: InjectEvent<ShutdownRequested, behavior::Here> + Send,
{
    fn request(&self) -> Result<(), ShutdownSignalRejection> {
        self.control
            .send(E::inject_at(ShutdownRequested))
            .map_err(|ControlClosed(_rejected_event)| ShutdownSignalRejection::ControlClosed)
    }
}

impl<U> Admission<U> {
    pub(crate) fn new(owner: MailboxOwner<U>) -> Self {
        Self {
            owner: Mutex::new(Some(owner)),
        }
    }

    pub(crate) fn close(&self) -> Option<()> {
        let owner = self
            .owner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()?;
        owner.close_admission();
        Some(())
    }
}

/// A non-owning, protocol-indexed user-lane capability.
///
/// The Communication anchor cannot keep or resurrect an actor. Indexing by
/// `P` preserves destination protocol identity independently of the concrete
/// behavior or transparent wrappers currently implementing it.
pub struct ActorRef<P: Protocol> {
    address: P::Addr,
    endpoint: EndpointMailbox<P::Addr, P::Msg>,
    shutdown: Option<Weak<dyn ShutdownControl>>,
    termination: Observation<Termination<P::Addr>>,
    protocol: PhantomData<fn() -> P>,
}

impl<P: Protocol> Clone for ActorRef<P> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            endpoint: self.endpoint.clone(),
            shutdown: self.shutdown.clone(),
            termination: self.termination.clone(),
            protocol: PhantomData,
        }
    }
}

impl<P: Protocol> ActorRef<P> {
    fn new(
        address: P::Addr,
        endpoint: EndpointMailbox<P::Addr, P::Msg>,
        shutdown: Weak<dyn ShutdownControl>,
        termination: Observation<Termination<P::Addr>>,
    ) -> Self {
        Self {
            address,
            endpoint,
            shutdown: Some(shutdown),
            termination,
            protocol: PhantomData,
        }
    }

    pub(crate) fn external(
        address: P::Addr,
        mailbox: MailboxRef<User<P::Addr, P::Msg>>,
        admission: Weak<Admission<User<P::Addr, P::Msg>>>,
        termination: Observation<Termination<P::Addr>>,
    ) -> Self {
        Self {
            address,
            endpoint: EndpointMailbox::Standard { mailbox, admission },
            shutdown: None,
            termination,
            protocol: PhantomData,
        }
    }

    /// Observe the retained terminal result of this exact incarnation.
    ///
    /// Every clone refers to the same one-publication fact. Resolving a later
    /// actor at the same address produces a different observation.
    ///
    /// # Errors
    ///
    /// Resolves to the exact crash when this incarnation terminates
    /// abnormally; normal exits retain their typed exit reason.
    pub fn termination(&self) -> impl Future<Output = Termination<P::Addr>> + use<P> {
        let observation = self.termination.clone();
        async move { observation.await }
    }

    pub(crate) fn termination_observation(&self) -> Observation<Termination<P::Addr>> {
        self.termination.clone()
    }

    pub(crate) fn request_shutdown(&self) -> Result<(), ShutdownRejection> {
        if self.termination.try_get().is_some() {
            return Err(ShutdownRejection::AlreadyStopped);
        }
        let Some(shutdown) = self.shutdown.as_ref().and_then(Weak::upgrade) else {
            return Err(self.shutdown_rejection());
        };
        if self.endpoint.close_admission().is_none() {
            return Err(self.shutdown_rejection());
        }
        match shutdown.request() {
            Ok(()) => Ok(()),
            Err(ShutdownSignalRejection::ControlClosed) => Err(self.shutdown_rejection()),
        }
    }

    fn shutdown_rejection(&self) -> ShutdownRejection {
        if self.termination.try_get().is_some() {
            ShutdownRejection::AlreadyStopped
        } else {
            ShutdownRejection::AlreadyStopping
        }
    }

    /// The typed address of this exact actor reference.
    #[must_use]
    pub const fn address(&self) -> P::Addr {
        self.address
    }

    /// Admit one user message from an explicitly supplied typed origin through
    /// the behavior's complete event sum.
    ///
    /// This reference resolves only the target. It does not claim, host, or
    /// validate `from`; the external boundary that owns that identity must
    /// supply it truthfully.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] with the original message when this incarnation's
    /// user lane is no longer live.
    pub async fn send_from(&self, from: P::Addr, message: P::Msg) -> Result<(), SendError<P::Msg>> {
        match &self.endpoint {
            EndpointMailbox::Standard { mailbox, .. } => mailbox
                .send(User::new(from, message))
                .await
                .map_err(SendError::from_standard_rejection),
            EndpointMailbox::Entity { mailbox, .. } => mailbox
                .send(LocalIngress::Message(User::new(from, message)))
                .await
                .map_err(SendError::from_entity_rejection),
        }
    }

    #[allow(
        dead_code,
        reason = "the Entity fence remains private until typed application topology can materialize its requirements"
    )]
    pub(crate) async fn fence(&self) -> Result<(), crate::entity::FenceFailure> {
        let (publisher, observation) = affine_pair();
        let EndpointMailbox::Entity { mailbox, .. } = &self.endpoint else {
            return Err(crate::entity::FenceFailure::Enqueue);
        };
        if mailbox.send(LocalIngress::Fence(publisher)).await.is_err() {
            return Err(crate::entity::FenceFailure::Enqueue);
        }
        observation.await
    }
}

impl<P> ActorRef<P>
where
    P: Protocol<Addr = MailAddr>,
{
    /// Issue the exact recipient for this installed incarnation.
    ///
    /// The capability retains this endpoint directly. It performs no address
    /// lookup and never retargets to a replacement at the same logical
    /// address.
    #[must_use]
    pub fn established_recipient(&self) -> EstablishedRecipient<P> {
        EstablishedRecipient::issued(self.clone())
    }
}

/// Exact user payload rejected because the incarnation is no longer live.
#[derive(thiserror::Error)]
#[error("actor reference is closed")]
pub struct SendError<M>(M);

impl<M> core::fmt::Debug for SendError<M> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<M> SendError<M> {
    /// Recover the exact message rejected by the closed actor reference.
    #[must_use]
    pub fn into_message(self) -> M {
        self.0
    }

    fn from_standard_rejection<A>(rejected: UserClosed<User<A, M>>) -> Self {
        let UserClosed(user) = rejected;
        Self(user.message)
    }

    fn from_entity_rejection<A>(rejected: UserClosed<LocalIngress<A, M>>) -> Self {
        let UserClosed(LocalIngress::Message(user)) = rejected else {
            unreachable!("ordinary ActorRef delivery creates only message ingress");
        };
        Self(user.message)
    }
}

pub(crate) enum LocalResidual<A, S, E, U, Descendants = ()> {
    Uncommitted {
        initialization: A,
        ingress: Drained<E, U>,
        activation_tasks: ActivationTasks<E>,
        descendants: Descendants,
    },
    Retired {
        settlements: Vec<S>,
        ingress: Drained<E, U>,
        activation_tasks: ActivationTasks<E>,
        descendants: Descendants,
        owner_cancellation: Option<OwnerCancellation>,
    },
}

impl<A, S, E, U, Descendants> LocalResidual<A, S, E, U, Descendants>
where
    E: Send + 'static,
{
    pub(crate) async fn settle_activation_tasks(self) -> Self {
        match self {
            Self::Uncommitted {
                initialization,
                mut ingress,
                activation_tasks,
                descendants,
            } => {
                let mut completed = activation_tasks.settle().await;
                ingress.control.append(&mut completed);
                Self::Uncommitted {
                    initialization,
                    ingress,
                    activation_tasks: ActivationTasks::new(),
                    descendants,
                }
            }
            Self::Retired {
                settlements,
                mut ingress,
                activation_tasks,
                descendants,
                owner_cancellation,
            } => {
                let mut completed = activation_tasks.settle().await;
                ingress.control.append(&mut completed);
                Self::Retired {
                    settlements,
                    ingress,
                    activation_tasks: ActivationTasks::new(),
                    descendants,
                    owner_cancellation,
                }
            }
        }
    }
}

/// The prepared local half of one mailbox-backed behavior generation.
pub(crate) struct LocalEnvironment<
    B: Behavior,
    I,
    M = StandardIngress,
    P = fn(ActorRef<<B as Behavior>::Protocol>),
> where
    M: IngressMode<B>,
{
    address: BehaviorAddr<B>,
    addresses: AddressSpace<BehaviorAddr<B>, ActorRef<B::Protocol>>,
    endpoint: ActorRef<B::Protocol>,
    consumer: Consumer<B::Event, M::Item>,
    admission: Arc<Admission<M::Item>>,
    control_liveness: Arc<ControlSender<B::Event>>,
    shutdown_liveness: Arc<dyn ShutdownControl>,
    timers: LocalTimers<B::Event>,
    facts: FactQueue<BehaviorAddr<B>, B::Event>,
    interpreter: I,
    owner_cancellation: oneshot::Receiver<OwnerCancellation>,
    publish: P,
}

impl<B, I, M> LocalEnvironment<B, I, M, fn(ActorRef<B::Protocol>)>
where
    B: Behavior,
    M: IngressMode<B>,
    BehaviorAddr<B>: Hash,
    B::Event: InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
{
    /// Prepare one concrete ingress mode and construct its interpreter with a
    /// clone of the control-lane capability for this exact incarnation.
    pub(crate) fn prepare(
        address: BehaviorAddr<B>,
        addresses: AddressSpace<BehaviorAddr<B>, ActorRef<B::Protocol>>,
        config: Config,
        termination: Observation<Termination<BehaviorAddr<B>>>,
        owner_cancellation: oneshot::Receiver<OwnerCancellation>,
        make_interpreter: impl FnOnce(
            ControlSender<B::Event>,
            LocalTimers<B::Event>,
            FactQueue<BehaviorAddr<B>, B::Event>,
        ) -> I,
    ) -> Self {
        let (control_liveness, owner, mailbox, consumer) =
            mailbox_channel::<B::Event, M::Item>(config);
        let control_liveness = Arc::new(control_liveness);
        let admission = Arc::new(Admission::new(owner));
        let shutdown_liveness: Arc<dyn ShutdownControl> = Arc::new(TypedShutdownControl {
            control: (*control_liveness).clone(),
        });
        let endpoint = ActorRef::new(
            address,
            M::endpoint(mailbox, Arc::downgrade(&admission)),
            Arc::downgrade(&shutdown_liveness),
            termination,
        );
        let timers = LocalTimers::new();
        let facts = FactQueue::new();
        let interpreter =
            make_interpreter((*control_liveness).clone(), timers.clone(), facts.clone());
        Self {
            address,
            addresses,
            endpoint,
            consumer,
            admission,
            control_liveness,
            shutdown_liveness,
            timers,
            facts,
            interpreter,
            owner_cancellation,
            publish: ignore_publication::<B>,
        }
    }
}

fn ignore_publication<B: Behavior>(_: ActorRef<B::Protocol>) {}

impl<B, I, M, P> LocalEnvironment<B, I, M, P>
where
    B: Behavior,
    M: IngressMode<B>,
{
    pub(crate) fn control(&self) -> ControlSender<B::Event> {
        (*self.control_liveness).clone()
    }

    pub(crate) fn publish_with<N>(self, publish: N) -> LocalEnvironment<B, I, M, N> {
        LocalEnvironment {
            address: self.address,
            addresses: self.addresses,
            endpoint: self.endpoint,
            consumer: self.consumer,
            admission: self.admission,
            control_liveness: self.control_liveness,
            shutdown_liveness: self.shutdown_liveness,
            timers: self.timers,
            facts: self.facts,
            interpreter: self.interpreter,
            owner_cancellation: self.owner_cancellation,
            publish,
        }
    }
}

enum Publication<P, Endpoint> {
    Pending { publish: P, endpoint: Endpoint },
    Published,
}

/// The only local value with mailbox ingress and address ownership.
pub(crate) struct ActiveLocalEnvironment<
    B: Behavior,
    I,
    M = StandardIngress,
    P = fn(ActorRef<<B as Behavior>::Protocol>),
> where
    BehaviorAddr<B>: Hash,
    BehaviorMessage<B>: Send,
    M: IngressMode<B>,
{
    inbox: LocalInbox<B, M>,
    admission: Arc<Admission<M::Item>>,
    control_liveness: Option<Arc<ControlSender<B::Event>>>,
    shutdown_liveness: Arc<dyn ShutdownControl>,
    timers: LocalTimers<B::Event>,
    facts: FactQueue<BehaviorAddr<B>, B::Event>,
    interpreter: I,
    owner_cancellation: Option<oneshot::Receiver<OwnerCancellation>>,
    cancellation: Option<OwnerCancellation>,
    publication: Publication<P, ActorRef<B::Protocol>>,
    _lease: Lease<BehaviorAddr<B>, ActorRef<B::Protocol>>,
}

#[allow(
    refining_impl_trait,
    reason = "Bombay's concrete Tokio environment refines executor-neutral Engine futures"
)]
impl<B, I, M, P> Environment<B> for LocalEnvironment<B, I, M, P>
where
    B: BehaviorSettlements<Ph = Never> + Send,
    M: IngressMode<B>,
    M::Retired: Send,
    BehaviorAddr<B>: Hash + Clone + Send + Sync,
    B::Event: Send,
    B::Event: InjectEvent<ShutdownRequested, behavior::Here> + 'static,
    B::Sends: Send,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <BehaviorAddr<B> as behavior::Address>::Nonce: Send,
    BehaviorMessage<B>: Send,
    I: CommitActions<B> + Send,
    I::Retired: Send,
    P: FnOnce(ActorRef<B::Protocol>) + Send,
    ActionSettlementOf<B>: ClassifySettlement + Send,
{
    type Active = ActiveLocalEnvironment<B, I, M, P>;
    type Settlement = ActionSettlementOf<B>;
    type Error = ClaimError<BehaviorAddr<B>>;
    type Residual =
        LocalResidual<ActionsOf<B>, ActionSettlementOf<B>, B::Event, M::Retired, I::Retired>;

    async fn activate(
        mut self,
        actions: ActionsOf<B>,
    ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
    {
        let published = self.endpoint.clone();
        let lease = match self.addresses.try_claim(self.address, self.endpoint) {
            Ok(lease) => lease,
            Err(error) => {
                let CapabilityRetirement {
                    activation_tasks,
                    descendants,
                } = self.interpreter.retire().await;
                let ingress = collect_retired_ingress::<B, M>(self.consumer);
                return Err((
                    error,
                    LocalResidual::Uncommitted {
                        initialization: actions,
                        ingress,
                        activation_tasks,
                        descendants,
                    },
                ));
            }
        };
        let interpretation = self.interpreter.commit(actions).await;
        let control_liveness = Some(self.control_liveness);
        let active = ActiveLocalEnvironment {
            inbox: LocalInbox::new(self.consumer),
            admission: self.admission,
            control_liveness,
            shutdown_liveness: self.shutdown_liveness,
            timers: self.timers,
            facts: self.facts,
            interpreter: self.interpreter,
            owner_cancellation: Some(self.owner_cancellation),
            cancellation: None,
            publication: Publication::Pending {
                publish: self.publish,
                endpoint: published,
            },
            _lease: lease,
        };
        Ok((active, interpretation))
    }

    async fn retire(self) -> Self::Residual {
        match self.admission.close() {
            Some(()) | None => {}
        }
        let CapabilityRetirement {
            activation_tasks,
            descendants,
        } = self.interpreter.retire().await;
        let ingress = collect_retired_ingress::<B, M>(self.consumer);
        drop((
            self.admission,
            self.control_liveness,
            self.shutdown_liveness,
            self.timers,
            self.facts,
            self.owner_cancellation,
        ));
        LocalResidual::Retired {
            settlements: Vec::new(),
            ingress,
            activation_tasks,
            descendants,
            owner_cancellation: None,
        }
    }
}

#[allow(
    refining_impl_trait,
    reason = "Bombay's concrete Tokio environment refines executor-neutral Engine futures"
)]
impl<B, I, M, P> ActiveEnvironment<B> for ActiveLocalEnvironment<B, I, M, P>
where
    B: BehaviorSettlements<Ph = Never> + Send,
    M: IngressMode<B>,
    M::Retired: Send,
    BehaviorAddr<B>: Hash + Send + Sync,
    B::Event: Send,
    B::Sends: Send,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <BehaviorAddr<B> as behavior::Address>::Nonce: Send,
    BehaviorMessage<B>: Send,
    I: CommitActions<B> + Send,
    I::Retired: Send,
    P: FnOnce(ActorRef<B::Protocol>) + Send,
    ActionSettlementOf<B>: ClassifySettlement + Send,
{
    type Settlement = ActionSettlementOf<B>;
    type Residual =
        LocalResidual<ActionsOf<B>, ActionSettlementOf<B>, B::Event, M::Retired, I::Retired>;

    async fn next(&mut self) -> Option<B::Event> {
        enum Acquired<E, U> {
            Mailbox(Option<Received<E, U>>),
            Fact(E),
            Deadline,
            OwnerCancellation(Option<OwnerCancellation>),
        }

        loop {
            let cancellation = async {
                match self.owner_cancellation.as_mut() {
                    Some(receiver) => receiver.await.ok(),
                    None => pending().await,
                }
            };
            let acquired = if let Some(deadline) = self.timers.next_deadline() {
                tokio::select! {
                    biased;
                    cancellation = cancellation => Acquired::OwnerCancellation(cancellation),
                    received = self.inbox.recv() => Acquired::Mailbox(received),
                    event = self.facts.next() => Acquired::Fact(event),
                    () = tokio::time::sleep_until(deadline.into()) => Acquired::Deadline,
                }
            } else {
                tokio::select! {
                    biased;
                    cancellation = cancellation => Acquired::OwnerCancellation(cancellation),
                    received = self.inbox.recv() => Acquired::Mailbox(received),
                    event = self.facts.next() => Acquired::Fact(event),
                }
            };
            let received = match acquired {
                Acquired::OwnerCancellation(Some(cancellation)) => {
                    self.owner_cancellation = None;
                    self.cancellation = Some(cancellation);
                    return None;
                }
                Acquired::OwnerCancellation(None) => {
                    self.owner_cancellation = None;
                    continue;
                }
                Acquired::Fact(event) => return Some(event),
                Acquired::Deadline => {
                    if let Some(event) = self.timers.pop_due(Instant::now()) {
                        return Some(event);
                    }
                    continue;
                }
                Acquired::Mailbox(received) => received?,
            };
            match received {
                Received::Control(event) => return Some(event),
                Received::User(item) => {
                    if let Some(event) = M::into_event(item) {
                        return Some(event);
                    }
                }
                Received::UserLaneClosed => {
                    // This closes only the bounded user lane. Control events
                    // remain admissible until the complete consumer closes.
                }
            }
        }
    }

    async fn next_source(&mut self) -> Option<B::Event> {
        self.inbox.recv_source().await
    }

    async fn apply(&mut self, actions: ActionsOf<B>) -> Interpretation<Self::Settlement> {
        self.interpreter.commit(actions).await
    }

    async fn offer_next(
        &mut self,
        settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement> {
        self.interpreter.offer_next(settlement).await
    }

    fn publish(&mut self) {
        let publication = core::mem::replace(&mut self.publication, Publication::Published);
        match publication {
            Publication::Pending { publish, endpoint } => publish(endpoint),
            Publication::Published => {
                unreachable!("the Driver publishes one installed incarnation exactly once")
            }
        }
    }

    async fn retire(self, settlements: Vec<Self::Settlement>) -> Self::Residual {
        let Self {
            mut inbox,
            admission,
            control_liveness,
            shutdown_liveness,
            timers,
            facts,
            interpreter,
            owner_cancellation,
            cancellation,
            publication,
            _lease: lease,
        } = self;
        drop(owner_cancellation);
        match admission.close() {
            Some(()) | None => {}
        }
        let CapabilityRetirement {
            activation_tasks,
            descendants,
        } = interpreter.retire().await;
        let ingress = inbox
            .drain()
            .expect("active local inbox is retired exactly once");
        drop((
            inbox,
            admission,
            control_liveness,
            shutdown_liveness,
            timers,
            facts,
            publication,
            lease,
        ));
        LocalResidual::Retired {
            settlements,
            ingress,
            activation_tasks,
            descendants,
            owner_cancellation: cancellation,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::mem::size_of;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use behavior::{
        Actions, BehaviorActed, InitializationTurn, MessageProtocol, Never, NoBirths, User,
    };
    use behavior_actors::{Crash, Exit, StopOnShutdown};
    use communication::{Config, mailbox_channel};

    use crate::MailAddr;

    use super::*;

    struct TestShutdownControl(AtomicUsize);

    impl ShutdownControl for TestShutdownControl {
        fn request(&self) -> Result<(), ShutdownSignalRejection> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn rejected_user_delivery_recovers_the_exact_owned_payload() {
        let payload = String::from("application-owned-payload");
        let rejected = UserClosed(LocalIngress::Message(User::new(MailAddr(7), payload)));

        let error = SendError::from_entity_rejection(rejected);

        assert_eq!(error.into_message(), "application-owned-payload");
    }

    struct FenceProbe;

    impl Behavior for FenceProbe {
        type Protocol = behavior::MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    struct PanickingFenceProbe;

    impl Behavior for PanickingFenceProbe {
        type Protocol = behavior::MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            panic!("deliberate failure before the queued fence")
        }
    }

    #[test]
    fn ordinary_ingress_retains_the_exact_user_item_layout() {
        type Hosted = StopOnShutdown<FenceProbe>;
        type StandardItem = <StandardIngress as IngressMode<Hosted>>::Item;

        assert_eq!(size_of::<StandardItem>(), size_of::<User<MailAddr, ()>>());
        assert!(size_of::<LocalIngress<MailAddr, ()>>() > size_of::<StandardItem>());
    }

    #[tokio::test]
    async fn retirement_returns_every_queued_ingress_item_in_fifo_order() {
        let (control, admission_owner, user_mailbox, consumer) =
            mailbox_channel::<User<MailAddr, ()>, User<MailAddr, ()>>(Config::new(2));
        control.send(User::new(MailAddr(71), ())).unwrap();
        control.send(User::new(MailAddr(73), ())).unwrap();
        user_mailbox
            .send(User::new(MailAddr(79), ()))
            .await
            .unwrap();
        let mut inbox = LocalInbox::<FenceProbe, StandardIngress>::new(consumer);

        let retired = inbox
            .drain()
            .expect("the local inbox retains its consumer before retirement");
        let origins = retired
            .control
            .into_iter()
            .map(|event| event.from)
            .collect::<Vec<_>>();
        let user_origins = retired
            .user
            .into_iter()
            .map(|event| event.from)
            .collect::<Vec<_>>();

        assert_eq!(origins, vec![MailAddr(71), MailAddr(73)]);
        assert_eq!(user_origins, vec![MailAddr(79)]);
        drop((admission_owner, user_mailbox));
    }

    #[test]
    fn exact_shutdown_classifies_repeated_and_stopped_requests_without_resending() {
        type ProbeProtocol = MessageProtocol<MailAddr, ()>;

        let (control, owner, mailbox, consumer) =
            mailbox_channel::<(), User<MailAddr, ()>>(Config::new(2));
        let admission = Arc::new(Admission::new(owner));
        let shutdown = Arc::new(TestShutdownControl(AtomicUsize::new(0)));
        let (termination_publisher, termination) = crate::observe::pair();
        let actor = ActorRef::<ProbeProtocol>::new(
            MailAddr(36),
            EndpointMailbox::Standard {
                mailbox,
                admission: Arc::downgrade(&admission),
            },
            Arc::downgrade(&(Arc::clone(&shutdown) as Arc<_>)),
            termination,
        );

        assert_eq!(actor.request_shutdown(), Ok(()));
        assert_eq!(
            actor.request_shutdown(),
            Err(ShutdownRejection::AlreadyStopping)
        );
        assert_eq!(shutdown.0.load(Ordering::SeqCst), 1);

        termination_publisher.complete(Ok(Exit::Normal));
        assert_eq!(
            actor.request_shutdown(),
            Err(ShutdownRejection::AlreadyStopped)
        );
        assert_eq!(shutdown.0.load(Ordering::SeqCst), 1);
        drop((actor, admission, consumer, control, shutdown));
    }

    #[tokio::test]
    async fn ordinary_actor_does_not_admit_entity_fences() {
        let actor = crate::launch::launch_inert(
            crate::launch::LocalAddresses::new(),
            Config::new(2),
            MailAddr(37),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await
        .unwrap();

        assert_eq!(
            actor.fence().await,
            Err(crate::entity::FenceFailure::Enqueue)
        );
        actor.send_from(actor.address(), ()).await.unwrap();
        assert_eq!(actor.request_shutdown(), Ok(()));
        assert_eq!(actor.termination().await, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn entity_launch_preserves_the_exact_conflicting_address() {
        let actors = crate::launch::LocalAddresses::new();
        let first = crate::launch::launch_inert_entity(
            actors.clone(),
            Config::new(2),
            MailAddr(40),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await
        .unwrap();
        let conflict = crate::launch::launch_inert_entity(
            actors,
            Config::new(2),
            MailAddr(40),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await;

        match conflict {
            Err(crate::launch::SpawnError::HostRejected {
                error: ClaimError::AddressInUse(address),
                ..
            }) => {
                assert_eq!(address, MailAddr(40));
            }
            Err(error) => panic!("address conflict was misclassified: {error:?}"),
            Ok(_) => panic!("duplicate address was accepted"),
        }
        assert_eq!(first.request_shutdown(), Ok(()));
        assert_eq!(first.termination().await, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn user_lane_fence_follows_the_preceding_applied_action() {
        let committed = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&committed);
        let actor = crate::launch::launch_inert_entity(
            crate::launch::LocalAddresses::new(),
            Config::new(4),
            MailAddr(41),
            StopOnShutdown::new(FenceProbe),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();

        actor.send_from(actor.address(), ()).await.unwrap();
        actor.fence().await.unwrap();

        assert_eq!(committed.load(Ordering::SeqCst), 2);
        assert_eq!(actor.request_shutdown(), Ok(()));
        assert_eq!(actor.termination().await, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn fence_rejected_before_enqueue_reports_the_exact_stage() {
        let actor = crate::launch::launch_inert_entity(
            crate::launch::LocalAddresses::new(),
            Config::new(2),
            MailAddr(43),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(actor.request_shutdown(), Ok(()));
        assert_eq!(actor.termination().await, Ok(Exit::Normal));

        assert_eq!(
            actor.fence().await,
            Err(crate::entity::FenceFailure::Enqueue)
        );
    }

    #[tokio::test]
    async fn accepted_fence_collected_during_failure_reports_acknowledgement() {
        let actor = crate::launch::launch_inert_entity(
            crate::launch::LocalAddresses::new(),
            Config::new(2),
            MailAddr(47),
            StopOnShutdown::new(PanickingFenceProbe),
            |_| {},
        )
        .await
        .unwrap();

        actor.send_from(actor.address(), ()).await.unwrap();
        assert_eq!(
            actor.fence().await,
            Err(crate::entity::FenceFailure::Acknowledgement)
        );
        assert_eq!(actor.termination().await, Err(Crash::Panicked));
    }

    async fn direct_ingress_time(iterations: u64) -> Duration {
        let (control, owner, mailbox, mut consumer) =
            mailbox_channel::<(), User<MailAddr, u64>>(Config::new(1_024));
        let receiver = tokio::spawn(async move {
            for expected in 0..iterations {
                let Some(Received::User(message)) = consumer.recv().await else {
                    panic!("direct ingress ended before message {expected}")
                };
                assert_eq!(message.message, expected);
            }
        });
        let start = Instant::now();
        for message in 0..iterations {
            mailbox.send(User::new(MailAddr(1), message)).await.unwrap();
        }
        receiver.await.unwrap();
        let elapsed = start.elapsed();
        drop((control, owner));
        elapsed
    }

    struct DirectActorRef {
        mailbox: MailboxRef<User<MailAddr, u64>>,
    }

    impl DirectActorRef {
        async fn send(&self, from: MailAddr, message: u64) -> Result<(), SendError<u64>> {
            self.mailbox
                .send(User::new(from, message))
                .await
                .map_err(SendError::from_standard_rejection)
        }
    }

    async fn direct_actor_ref_time(iterations: u64) -> Duration {
        let (control, owner, mailbox, mut consumer) =
            mailbox_channel::<(), User<MailAddr, u64>>(Config::new(1_024));
        let actor = DirectActorRef { mailbox };
        let receiver = tokio::spawn(async move {
            for expected in 0..iterations {
                let Some(Received::User(message)) = consumer.recv().await else {
                    panic!("direct ActorRef ingress ended before message {expected}")
                };
                assert_eq!(message.message, expected);
            }
        });
        let start = Instant::now();
        for message in 0..iterations {
            actor.send(MailAddr(1), message).await.unwrap();
        }
        receiver.await.unwrap();
        let elapsed = start.elapsed();
        drop((actor, control, owner));
        elapsed
    }

    async fn tagged_ingress_time(iterations: u64) -> Duration {
        let (control, owner, mailbox, mut consumer) =
            mailbox_channel::<(), LocalIngress<MailAddr, u64>>(Config::new(1_024));
        let receiver = tokio::spawn(async move {
            for expected in 0..iterations {
                let Some(Received::User(LocalIngress::Message(message))) = consumer.recv().await
                else {
                    panic!("tagged ingress ended before message {expected}")
                };
                assert_eq!(message.message, expected);
            }
        });
        let start = Instant::now();
        for message in 0..iterations {
            assert!(
                mailbox
                    .send(LocalIngress::Message(User::new(MailAddr(1), message)))
                    .await
                    .is_ok()
            );
        }
        receiver.await.unwrap();
        let elapsed = start.elapsed();
        drop((control, owner));
        elapsed
    }

    async fn standard_actor_ref_time(iterations: u64) -> Duration {
        type ProbeProtocol = MessageProtocol<MailAddr, u64>;

        let (control, owner, mailbox, mut consumer) =
            mailbox_channel::<(), User<MailAddr, u64>>(Config::new(1_024));
        let admission = Arc::new(Admission::new(owner));
        let shutdown = Arc::new(TestShutdownControl(AtomicUsize::new(0)));
        let (termination_publisher, termination) = crate::observe::pair();
        let actor = ActorRef::<ProbeProtocol>::new(
            MailAddr(2),
            EndpointMailbox::Standard {
                mailbox,
                admission: Arc::downgrade(&admission),
            },
            Arc::downgrade(&(Arc::clone(&shutdown) as Arc<_>)),
            termination,
        );
        let receiver = tokio::spawn(async move {
            for expected in 0..iterations {
                let Some(Received::User(message)) = consumer.recv().await else {
                    panic!("standard ActorRef ingress ended before message {expected}")
                };
                assert_eq!(message.message, expected);
            }
        });
        let start = Instant::now();
        for message in 0..iterations {
            actor.send_from(MailAddr(1), message).await.unwrap();
        }
        receiver.await.unwrap();
        let elapsed = start.elapsed();
        drop((actor, admission, control, shutdown, termination_publisher));
        elapsed
    }

    #[tokio::test]
    #[ignore = "manual ordinary-ingress performance comparison"]
    async fn compare_direct_and_entity_capable_ingress() {
        const ITERATIONS: u64 = 1_000_000;
        const RUNS: usize = 7;

        direct_ingress_time(10_000).await;
        direct_actor_ref_time(10_000).await;
        standard_actor_ref_time(10_000).await;
        tagged_ingress_time(10_000).await;

        let mut direct = Vec::with_capacity(RUNS);
        let mut direct_actor_ref = Vec::with_capacity(RUNS);
        let mut standard = Vec::with_capacity(RUNS);
        let mut tagged = Vec::with_capacity(RUNS);
        for round in 0..RUNS {
            if round % 2 == 0 {
                direct.push(direct_ingress_time(ITERATIONS).await);
                direct_actor_ref.push(direct_actor_ref_time(ITERATIONS).await);
                standard.push(standard_actor_ref_time(ITERATIONS).await);
                tagged.push(tagged_ingress_time(ITERATIONS).await);
            } else {
                tagged.push(tagged_ingress_time(ITERATIONS).await);
                standard.push(standard_actor_ref_time(ITERATIONS).await);
                direct_actor_ref.push(direct_actor_ref_time(ITERATIONS).await);
                direct.push(direct_ingress_time(ITERATIONS).await);
            }
        }
        direct.sort_unstable();
        direct_actor_ref.sort_unstable();
        standard.sort_unstable();
        tagged.sort_unstable();
        let direct = direct[RUNS / 2];
        let direct_actor_ref = direct_actor_ref[RUNS / 2];
        let standard = standard[RUNS / 2];
        let tagged = tagged[RUNS / 2];
        println!(
            "direct={direct:?} direct_actor_ref={direct_actor_ref:?} standard_actor_ref={standard:?} tagged={tagged:?} direct_size={} standard_size={} tagged_size={}",
            size_of::<User<MailAddr, u64>>(),
            size_of::<User<MailAddr, u64>>(),
            size_of::<LocalIngress<MailAddr, u64>>()
        );
    }
}
