//! One local typed mailbox behind the existing Engine environment port.
//!
//! This is deliberately crate-private. It proves the mailbox/address layer
//! without prescribing construction, task, handle, or System APIs.

use core::fmt;
use core::future::{Future, pending};
use core::hash::Hash;
use core::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Instant;

use crate::address::MailAddr;
use crate::interpret::ActionSettlementOf;
use crate::observation::TerminationObservations;
use crate::observe::{Observation, Publisher, affine_pair};
use crate::time::LocalTimers;
use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, BehaviorSettlements, ClassifySettlement,
    EstablishedRecipient, Ingress, InjectEvent, Interpretation, Never, Protocol, SourceCustody,
    User, UserEvent,
};
use behavior_actors::{Exit, ShutdownRejection, ShutdownRequested};
use bombay_address::{AddressSpace, ClaimError, Lease, Reservation};
use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};
use communication::{
    Config, Consumer, ControlSender, Drained, MailboxOwner, MailboxRef, Received, UserClosed,
    mailbox_channel,
};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinSet};

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

    pub(crate) async fn next_event(&mut self) -> E
    where
        E: 'static,
    {
        loop {
            match self.tasks.join_next().await {
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(event))) => return event,
                Some(Err(failure)) if failure.is_panic() => {
                    std::panic::resume_unwind(failure.into_panic())
                }
                Some(Err(failure)) => panic!("actor task was cancelled unexpectedly: {failure}"),
                None => pending().await,
            }
        }
    }

    pub(crate) async fn settle(mut self) -> Result<Vec<E>, JoinError>
    where
        E: 'static,
    {
        let mut control = Vec::new();
        while let Some(completed) = self.tasks.join_next().await {
            match completed {
                Ok(Ok(())) => {}
                Ok(Err(event)) => control.push(event),
                Err(failure) => return Err(failure),
            }
        }
        Ok(control)
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

    fn next_local_event(&mut self) -> impl Future<Output = B::Event> + Send;

    fn next_deadline(&mut self) -> Option<Instant> {
        None
    }

    fn pop_due(&mut self, _now: Instant) -> Option<B::Event> {
        None
    }

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

#[must_use = "admission closure reports whether this call won the transition"]
pub(crate) enum AdmissionClosure {
    Closed,
    AlreadyClosed,
}
pub(crate) enum LocalIngress<A, M> {
    Message(User<A, M>),
    Fence(Publisher<Result<(), crate::entity::FenceFailure>>),
}

pub(crate) struct StandardIngress;

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
    fn close_admission(&self) -> AdmissionClosure {
        match self {
            Self::Standard { admission, .. } => Admission::close_from_endpoint(admission),
            Self::Entity { admission, .. } => Admission::close_from_endpoint(admission),
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

impl<U> Admission<U> {
    pub(crate) fn new(owner: MailboxOwner<U>) -> Self {
        Self {
            owner: Mutex::new(Some(owner)),
        }
    }

    fn close_from_endpoint(admission: &Weak<Self>) -> AdmissionClosure {
        match admission.upgrade() {
            Some(admission) => admission.close(),
            None => AdmissionClosure::AlreadyClosed,
        }
    }

    pub(crate) fn close(&self) -> AdmissionClosure {
        self.owner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .map_or(AdmissionClosure::AlreadyClosed, |owner| {
                owner.close_admission();
                AdmissionClosure::Closed
            })
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
    termination: Observation<Termination<P::Addr>>,
    protocol: PhantomData<fn() -> P>,
}

impl<P> fmt::Debug for ActorRef<P>
where
    P: Protocol,
    P::Addr: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorRef")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// One installed incarnation's message endpoint and exact behavior control.
///
/// The runtime issues this value only after committing the child's binding.
pub struct InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    recipient: ActorRef<B::Protocol>,
    control: ControlSender<B::Event>,
    behavior: PhantomData<fn() -> B>,
}

impl<B> fmt::Debug for InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledActor")
            .field("recipient", &self.recipient)
            .finish_non_exhaustive()
    }
}

impl<B> Clone for InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn clone(&self) -> Self {
        Self {
            recipient: self.recipient.clone(),
            control: self.control.clone(),
            behavior: PhantomData,
        }
    }
}

impl<B> InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    pub(crate) fn new(recipient: ActorRef<B::Protocol>, control: ControlSender<B::Event>) -> Self {
        Self {
            recipient,
            control,
            behavior: PhantomData,
        }
    }

    pub(crate) fn recipient(&self) -> ActorRef<B::Protocol> {
        self.recipient.clone()
    }

    pub(crate) fn request_shutdown<TargetPath>(
        &self,
        ingress: Ingress<ShutdownRequested, TargetPath>,
    ) -> Result<(), ShutdownRejection>
    where
        B::Event: InjectEvent<ShutdownRequested, TargetPath>,
    {
        request_actor_shutdown(&self.recipient, &self.control, ingress)
    }
}

pub(crate) fn request_actor_shutdown<P: Protocol, E, TargetPath>(
    recipient: &ActorRef<P>,
    control: &ControlSender<E>,
    ingress: Ingress<ShutdownRequested, TargetPath>,
) -> Result<(), ShutdownRejection>
where
    E: InjectEvent<ShutdownRequested, TargetPath>,
{
    if recipient.termination.try_get().is_some() {
        return Err(ShutdownRejection::AlreadyStopped);
    }
    match recipient.endpoint.close_admission() {
        AdmissionClosure::Closed => {}
        AdmissionClosure::AlreadyClosed => return Err(recipient.shutdown_rejection()),
    }
    control
        .send(ingress.event(ShutdownRequested))
        .map_err(|_| recipient.shutdown_rejection())
}

impl<P: Protocol> Clone for ActorRef<P> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            endpoint: self.endpoint.clone(),
            termination: self.termination.clone(),
            protocol: PhantomData,
        }
    }
}

impl<P: Protocol> ActorRef<P> {
    fn new(
        address: P::Addr,
        endpoint: EndpointMailbox<P::Addr, P::Msg>,
        termination: Observation<Termination<P::Addr>>,
    ) -> Self {
        Self {
            address,
            endpoint,
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

    pub(crate) fn shutdown_rejection(&self) -> ShutdownRejection {
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

impl<M> fmt::Debug for SendError<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    Prepared {
        ingress: Drained<E, U>,
        activation_tasks: ActivationTasks<E>,
        descendants: Descendants,
    },
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
    },
    OwnerCancelled {
        settlements: Vec<S>,
        ingress: Drained<E, U>,
        activation_tasks: ActivationTasks<E>,
        descendants: Descendants,
        cancellation: OwnerCancellation,
    },
}

#[derive(Debug)]
pub(crate) enum LocalActivationRejection<A> {
    Address(ClaimError<A>),
    BindingAbandoned,
}

impl<A, S, E, U, Descendants> LocalResidual<A, S, E, U, Descendants>
where
    E: Send + 'static,
{
    pub(crate) async fn settle_activation_tasks(self) -> Result<Self, JoinError> {
        Ok(match self {
            Self::Prepared {
                mut ingress,
                activation_tasks,
                descendants,
            } => {
                let mut completed = activation_tasks.settle().await?;
                ingress.control.append(&mut completed);
                Self::Prepared {
                    ingress,
                    activation_tasks: ActivationTasks::new(),
                    descendants,
                }
            }
            Self::Uncommitted {
                initialization,
                mut ingress,
                activation_tasks,
                descendants,
            } => {
                let mut completed = activation_tasks.settle().await?;
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
            } => {
                let mut completed = activation_tasks.settle().await?;
                ingress.control.append(&mut completed);
                Self::Retired {
                    settlements,
                    ingress,
                    activation_tasks: ActivationTasks::new(),
                    descendants,
                }
            }
            Self::OwnerCancelled {
                settlements,
                mut ingress,
                activation_tasks,
                descendants,
                cancellation,
            } => {
                let mut completed = activation_tasks.settle().await?;
                ingress.control.append(&mut completed);
                Self::OwnerCancelled {
                    settlements,
                    ingress,
                    activation_tasks: ActivationTasks::new(),
                    descendants,
                    cancellation,
                }
            }
        })
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
    interpreter: I,
    owner_cancellation: oneshot::Receiver<OwnerCancellation>,
    #[allow(
        clippy::type_complexity,
        reason = "the private binding transports one exact actor reference and its affine acknowledgement"
    )]
    commitment: Option<oneshot::Sender<(ActorRef<B::Protocol>, oneshot::Sender<()>)>>,
    publication_notice: PublicationNotice<P>,
}

enum PublicationNotice<P> {
    Unobserved,
    Notify(P),
}

impl<B, I, M> LocalEnvironment<B, I, M, fn(ActorRef<B::Protocol>)>
where
    B: Behavior,
    M: IngressMode<B>,
    BehaviorAddr<B>: Hash,
    B::Event: Send + 'static,
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
            TerminationObservations<BehaviorAddr<B>, B::Event>,
        ) -> I,
    ) -> Self {
        let (control_liveness, owner, mailbox, consumer) =
            mailbox_channel::<B::Event, M::Item>(config);
        let control_liveness = Arc::new(control_liveness);
        let admission = Arc::new(Admission::new(owner));
        let endpoint = ActorRef::new(
            address,
            M::endpoint(mailbox, Arc::downgrade(&admission)),
            termination,
        );
        let timers = LocalTimers::new();
        let observations = TerminationObservations::new();
        let interpreter = make_interpreter((*control_liveness).clone(), timers, observations);
        Self {
            address,
            addresses,
            endpoint,
            consumer,
            admission,
            control_liveness,
            interpreter,
            owner_cancellation,
            commitment: None,
            publication_notice: PublicationNotice::Unobserved,
        }
    }
}

impl<B, I, M, P> LocalEnvironment<B, I, M, P>
where
    B: Behavior,
    M: IngressMode<B>,
{
    pub(crate) fn control(&self) -> ControlSender<B::Event> {
        (*self.control_liveness).clone()
    }

    pub(crate) fn shutdown_control(&self) -> Weak<ControlSender<B::Event>> {
        Arc::downgrade(&self.control_liveness)
    }

    pub(crate) fn with_private_commitment(
        mut self,
        commitment: oneshot::Sender<(ActorRef<B::Protocol>, oneshot::Sender<()>)>,
    ) -> Self {
        self.commitment = Some(commitment);
        self
    }

    pub(crate) fn publish_with<N>(self, notice: N) -> LocalEnvironment<B, I, M, N> {
        LocalEnvironment {
            address: self.address,
            addresses: self.addresses,
            endpoint: self.endpoint,
            consumer: self.consumer,
            admission: self.admission,
            control_liveness: self.control_liveness,
            interpreter: self.interpreter,
            owner_cancellation: self.owner_cancellation,
            commitment: self.commitment,
            publication_notice: PublicationNotice::Notify(notice),
        }
    }
}

enum Publication<P, A: Eq + Hash, Endpoint> {
    Pending {
        reservation: Reservation<A, Endpoint>,
        publication_notice: PublicationNotice<P>,
        endpoint: Endpoint,
    },
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
    interpreter: I,
    owner_cancellation: Option<oneshot::Receiver<OwnerCancellation>>,
    cancellation: Option<OwnerCancellation>,
    publication: Publication<P, BehaviorAddr<B>, ActorRef<B::Protocol>>,
    lease: Option<Lease<BehaviorAddr<B>, ActorRef<B::Protocol>>>,
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
    type Error = LocalActivationRejection<BehaviorAddr<B>>;
    type Residual =
        LocalResidual<ActionsOf<B>, ActionSettlementOf<B>, B::Event, M::Retired, I::Retired>;

    async fn activate(
        mut self,
        actions: ActionsOf<B>,
    ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
    {
        let published = self.endpoint.clone();
        let reservation = match self.addresses.try_reserve(self.address) {
            Ok(reservation) => reservation,
            Err(error) => {
                let CapabilityRetirement {
                    activation_tasks,
                    descendants,
                } = self.interpreter.retire().await;
                let ingress = collect_retired_ingress::<B, M>(self.consumer);
                return Err((
                    LocalActivationRejection::Address(error),
                    LocalResidual::Uncommitted {
                        initialization: actions,
                        ingress,
                        activation_tasks,
                        descendants,
                    },
                ));
            }
        };
        if let Some(commitment) = self.commitment.take() {
            let (acknowledge, acknowledged) = oneshot::channel();
            let acknowledgement = match commitment.send((self.endpoint.clone(), acknowledge)) {
                Ok(()) => acknowledged.await.ok(),
                Err(_) => None,
            };
            if acknowledgement.is_none() {
                drop(reservation);
                let CapabilityRetirement {
                    activation_tasks,
                    descendants,
                } = self.interpreter.retire().await;
                let ingress = collect_retired_ingress::<B, M>(self.consumer);
                return Err((
                    LocalActivationRejection::BindingAbandoned,
                    LocalResidual::Uncommitted {
                        initialization: actions,
                        ingress,
                        activation_tasks,
                        descendants,
                    },
                ));
            }
        }
        let interpretation = self.interpreter.commit(actions).await;
        let control_liveness = Some(self.control_liveness);
        let active = ActiveLocalEnvironment {
            inbox: LocalInbox::new(self.consumer),
            admission: self.admission,
            control_liveness,
            interpreter: self.interpreter,
            owner_cancellation: Some(self.owner_cancellation),
            cancellation: None,
            publication: Publication::Pending {
                reservation,
                publication_notice: self.publication_notice,
                endpoint: published,
            },
            lease: None,
        };
        Ok((active, interpretation))
    }

    async fn retire(self) -> Self::Residual {
        match self.admission.close() {
            AdmissionClosure::Closed | AdmissionClosure::AlreadyClosed => {}
        }
        let CapabilityRetirement {
            activation_tasks,
            descendants,
        } = self.interpreter.retire().await;
        let ingress = collect_retired_ingress::<B, M>(self.consumer);
        drop((
            self.admission,
            self.control_liveness,
            self.owner_cancellation,
            self.commitment,
        ));
        LocalResidual::Prepared {
            ingress,
            activation_tasks,
            descendants,
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
            Local(E),
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
            let acquired = if let Some(deadline) = self.interpreter.next_deadline() {
                tokio::select! {
                    biased;
                    cancellation = cancellation => Acquired::OwnerCancellation(cancellation),
                    received = self.inbox.recv() => Acquired::Mailbox(received),
                    event = self.interpreter.next_local_event() => Acquired::Local(event),
                    () = tokio::time::sleep_until(deadline.into()) => Acquired::Deadline,
                }
            } else {
                tokio::select! {
                    biased;
                    cancellation = cancellation => Acquired::OwnerCancellation(cancellation),
                    received = self.inbox.recv() => Acquired::Mailbox(received),
                    event = self.interpreter.next_local_event() => Acquired::Local(event),
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
                Acquired::Local(event) => return Some(event),
                Acquired::Deadline => {
                    if let Some(event) = self.interpreter.pop_due(Instant::now()) {
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
        tokio::select! {
            event = self.inbox.recv_source() => event,
            event = self.interpreter.next_local_event() => Some(event),
        }
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
            Publication::Pending {
                reservation,
                publication_notice,
                endpoint,
            } => {
                self.lease = Some(reservation.publish(endpoint.clone()));
                if let PublicationNotice::Notify(notice) = publication_notice {
                    notice(endpoint);
                }
            }
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
            interpreter,
            owner_cancellation,
            cancellation,
            publication,
            lease,
        } = self;
        drop(owner_cancellation);
        match admission.close() {
            AdmissionClosure::Closed | AdmissionClosure::AlreadyClosed => {}
        }
        let CapabilityRetirement {
            activation_tasks,
            descendants,
        } = interpreter.retire().await;
        let ingress = inbox
            .drain()
            .expect("active local inbox is retired exactly once");
        drop((inbox, admission, control_liveness, publication, lease));
        match cancellation {
            Some(cancellation) => LocalResidual::OwnerCancelled {
                settlements,
                ingress,
                activation_tasks,
                descendants,
                cancellation,
            },
            None => LocalResidual::Retired {
                settlements,
                ingress,
                activation_tasks,
                descendants,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::pending;
    use std::mem::size_of;
    use std::panic::panic_any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use behavior::{
        ActionItem, Actions, BehaviorActed, EventLayer, Here, InitializationTurn, InterpretItem,
        InterpreterFault, InterpreterRequest, InterpreterRequests, ItemSettlement, MessageProtocol,
        Never, NoBirthProtocols, NoBirths, NoReturnToEmitter, Own, SettlementStatus, User,
    };
    use behavior_actors::{Crash, Exit, StopOnShutdown};
    use communication::{Config, mailbox_channel};

    use crate::MailAddr;
    use crate::observe::pair;

    use super::*;

    #[tokio::test]
    async fn activation_task_custody_reports_pending_work_until_it_settles() {
        let mut tasks = ActivationTasks::<u64>::new();
        assert!(tasks.is_empty());
        tasks.spawn(async { Ok(()) });
        assert!(!tasks.is_empty());
        let completed = tasks.settle().await.expect("the task completes normally");
        assert_eq!(completed.len(), 0);
    }

    struct ActivationTaskPanic;

    #[tokio::test]
    async fn activation_task_event_preserves_task_panic_and_rejects_cancellation() {
        let mut panicking = ActivationTasks::<u64>::new();
        panicking.spawn(async { panic_any(ActivationTaskPanic) });
        let panicked = tokio::spawn(async move { panicking.next_event().await })
            .await
            .expect_err("the task panic crosses the activation owner");
        assert!(panicked.into_panic().is::<ActivationTaskPanic>());

        let mut cancelled = ActivationTasks::<u64>::new();
        cancelled.spawn(async { pending::<Result<(), u64>>().await });
        cancelled.tasks.abort_all();
        let rejected = tokio::spawn(async move { cancelled.next_event().await })
            .await
            .expect_err("cancelled activation work is rejected");
        let description = rejected
            .into_panic()
            .downcast::<String>()
            .expect("the cancellation diagnostic owns its text");
        assert!(description.contains("actor task was cancelled unexpectedly"));
    }

    #[test]
    fn rejected_user_delivery_recovers_the_exact_owned_payload() {
        let payload = String::from("application-owned-payload");
        let rejected = UserClosed(LocalIngress::Message(User::new(MailAddr(7), payload)));

        let error = SendError::from_entity_rejection(rejected);
        assert_eq!(format!("{error:?}"), "SendError { .. }");

        let rejected_message = error.into_message();
        assert_eq!(rejected_message, "application-owned-payload");
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

    #[test]
    fn standard_and_entity_ingress_preserve_exact_user_and_retired_payload() {
        let address = MailAddr(7);
        let standard =
            <StandardIngress as IngressMode<FenceProbe>>::into_event(User::new(address, ()));
        assert_eq!(standard, Some(User::new(address, ())));

        let entity = <EntityIngress as IngressMode<FenceProbe>>::into_event(LocalIngress::Message(
            User::new(address, ()),
        ));
        assert_eq!(entity, Some(User::new(address, ())));

        let retired = <EntityIngress as IngressMode<FenceProbe>>::retain_at_retirement(
            LocalIngress::Message(User::new(address, ())),
        );
        assert_eq!(retired, Some(User::new(address, ())));
    }

    #[test]
    fn exact_actor_and_installed_authority_debug_identify_their_address() {
        let (control, mailbox_owner, mailbox, receiver) =
            mailbox_channel::<User<MailAddr, ()>, User<MailAddr, ()>>(Config::new(1));
        let admission = Arc::new(Admission::new(mailbox_owner));
        let (publisher, observation) = pair();
        let actor = ActorRef::<MessageProtocol<MailAddr, ()>>::external(
            MailAddr(73),
            mailbox,
            Arc::downgrade(&admission),
            observation,
        );
        let installed = InstalledActor::<FenceProbe>::new(actor.clone(), control);

        let actor_description = format!("{actor:?}");
        assert!(actor_description.contains("ActorRef"));
        assert!(actor_description.contains("MailAddr(73)"));
        let installed_description = format!("{installed:?}");
        assert!(installed_description.contains("InstalledActor"));
        assert!(installed_description.contains("MailAddr(73)"));
        drop((publisher, admission, receiver));
    }

    type ActivationEvent = EventLayer<ShutdownRequested, User<MailAddr, ()>>;

    #[derive(Debug, PartialEq, Eq)]
    struct InitializationRequest(u8);

    #[derive(Debug, PartialEq, Eq)]
    struct InitializationReceipt(u8);

    #[derive(Debug, PartialEq, Eq)]
    enum InitializationRejection {
        Refused,
    }

    impl ActionItem for InitializationRequest {
        type Accepted = InitializationReceipt;
        type Rejection = InitializationRejection;
        type Prerequisite = Never;
    }

    impl InterpreterRequest for InitializationRequest {
        type ReturnToEmitter = NoReturnToEmitter;
        type LogicalProtocols = NoBirthProtocols;
    }

    struct ActivationProbe;

    impl Behavior for ActivationProbe {
        type Protocol = MessageProtocol<MailAddr, ()>;
        type Event = ActivationEvent;
        type Sends = InterpreterRequests<InitializationRequest>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::cont().with_send::<InitializationRequest, Own>(InitializationRequest(23)))
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    #[derive(Clone, Copy)]
    enum InitializationDisposition {
        Accepted,
        Rejected,
        Corrupt,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RetirementCustody(u8);

    struct GatedInitializationInterpreter {
        disposition: InitializationDisposition,
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
        retirement_task: Option<RetirementTask>,
    }

    struct RetirementTask {
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
        completion: RetirementTaskCompletion,
    }

    enum RetirementTaskCompletion {
        Signal(oneshot::Sender<()>),
        Panic,
    }

    impl InterpretItem<InitializationRequest, ActivationEvent, Here>
        for GatedInitializationInterpreter
    {
        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn interpret_item(
            &mut self,
            item: InitializationRequest,
        ) -> ItemSettlement<
            InitializationRequest,
            InitializationReceipt,
            InitializationRejection,
            Never,
        > {
            match self.disposition {
                InitializationDisposition::Accepted => {
                    ItemSettlement::Accepted(InitializationReceipt(item.0))
                }
                InitializationDisposition::Rejected => ItemSettlement::Rejected {
                    item,
                    reason: InitializationRejection::Refused,
                },
                InitializationDisposition::Corrupt => ItemSettlement::Corrupt {
                    item,
                    fault: InterpreterFault::CorruptTraversal,
                },
            }
        }
    }

    impl CommitActions<ActivationProbe> for GatedInitializationInterpreter {
        type Retired = RetirementCustody;

        async fn next_local_event(&mut self) -> ActivationEvent {
            core::future::pending().await
        }

        async fn commit(
            &mut self,
            actions: ActionsOf<ActivationProbe>,
        ) -> Interpretation<ActionSettlementOf<ActivationProbe>> {
            let entered = self
                .entered
                .take()
                .expect("initialization enters commitment exactly once");
            let release = self
                .release
                .take()
                .expect("initialization owns one commit gate");
            let entered_result = entered.send(());
            assert!(entered_result.is_ok(), "the commit observer remains live");
            let release_result = release.await;
            assert!(release_result.is_ok(), "the commit gate is released once");
            actions.interpret::<Self, ActivationEvent, Here>(self).await
        }

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn offer_next(
            &mut self,
            settlement: ActionSettlementOf<ActivationProbe>,
        ) -> SourceCustody<ActionSettlementOf<ActivationProbe>> {
            SourceCustody::Exhausted(settlement)
        }

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn retire(self) -> CapabilityRetirement<ActivationEvent, Self::Retired> {
            let mut activation_tasks = ActivationTasks::new();
            if let Some(task) = self.retirement_task {
                activation_tasks.spawn(async move {
                    let entered = task.entered.send(());
                    assert!(entered.is_ok(), "the task-start observer remains live");
                    let release = task.release.await;
                    assert!(release.is_ok(), "the activation task receives its release");
                    match task.completion {
                        RetirementTaskCompletion::Signal(completed) => {
                            let completed = completed.send(());
                            assert!(completed.is_ok(), "the completion observer remains live");
                        }
                        RetirementTaskCompletion::Panic => {
                            panic!("the actor-owned activation task panicked")
                        }
                    }
                    Ok(())
                });
            }
            CapabilityRetirement {
                activation_tasks,
                descendants: RetirementCustody(23),
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum AddressVisibility {
        Absent,
        Present,
    }

    fn address_visibility(
        addresses: &AddressSpace<MailAddr, ActorRef<MessageProtocol<MailAddr, ()>>>,
        address: MailAddr,
    ) -> AddressVisibility {
        match addresses.resolve(&address) {
            Some(_) => AddressVisibility::Present,
            None => AddressVisibility::Absent,
        }
    }

    type ActivationEnvironment =
        LocalEnvironment<ActivationProbe, GatedInitializationInterpreter, StandardIngress>;

    struct GatedActivation {
        environment: ActivationEnvironment,
        initialization: ActionsOf<ActivationProbe>,
        addresses: AddressSpace<MailAddr, ActorRef<MessageProtocol<MailAddr, ()>>>,
        commit_entered: oneshot::Receiver<()>,
        release_commit: oneshot::Sender<()>,
    }

    fn prepare_gated_activation(disposition: InitializationDisposition) -> GatedActivation {
        let address = MailAddr(23);
        let addresses = AddressSpace::new();
        let observer = addresses.clone();
        let (_termination_publisher, termination) = crate::observe::pair();
        let (_owner_cancellation, owner_cancellation) = oneshot::channel();
        let (entered, commit_entered) = oneshot::channel();
        let (release_commit, release) = oneshot::channel();
        let environment = ActivationEnvironment::prepare(
            address,
            addresses,
            Config::new(2),
            termination,
            owner_cancellation,
            move |_, _, _| GatedInitializationInterpreter {
                disposition,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        );
        let mut behavior = ActivationProbe;
        let initialization = behavior::initialize(&mut behavior)
            .expect("the activation probe initializes successfully");

        GatedActivation {
            environment,
            initialization,
            addresses: observer,
            commit_entered,
            release_commit,
        }
    }

    #[tokio::test]
    async fn dropped_root_activation_waiter_releases_its_reservation() {
        let address = MailAddr(24);
        let addresses = AddressSpace::new();
        let observer = addresses.clone();
        let (entered, mut commitment_entered) = oneshot::channel();
        let (release_commitment, release) = oneshot::channel();
        let mut launch = Box::pin(crate::launch::spawn_root_with(
            addresses,
            Config::new(2),
            address,
            ActivationProbe,
            move |_, _, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        ));

        tokio::select! {
            _ = &mut launch => panic!("activation settled before the test gate"),
            entry = &mut commitment_entered => {
                entry.expect("the actor entered interpretation after spawn");
            }
        }
        assert!(
            observer.resolve(&address).is_none(),
            "the reservation must remain invisible during interpretation"
        );
        drop(launch);
        let release_result = release_commitment.send(());

        let reservation_released = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match observer.try_reserve(address) {
                    Ok(reservation) => {
                        drop(reservation);
                        break;
                    }
                    Err(ClaimError::AddressInUse(_)) => tokio::task::yield_now().await,
                    Err(error) => panic!("the reservation probe failed: {error:?}"),
                }
            }
        })
        .await;
        assert!(
            reservation_released.is_ok(),
            "dropping the startup waiter left a reserved actor without a cleanup owner; gate release: {release_result:?}"
        );
    }

    #[tokio::test]
    async fn dropped_finish_waiter_retains_actor_cleanup() {
        let (entered, mut commitment_entered) = oneshot::channel();
        let (release_commitment, release) = oneshot::channel();
        let mut launch = Box::pin(crate::launch::spawn_root_with(
            AddressSpace::new(),
            Config::new(2),
            MailAddr(25),
            ActivationProbe,
            move |_, _, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        ));
        tokio::select! {
            _ = &mut launch => panic!("activation settled before the test gate"),
            entry = &mut commitment_entered => {
                entry.expect("the root entered initialization interpretation");
            }
        }
        release_commitment
            .send(())
            .expect("the root still owns its initialization");
        let root = launch.await.expect("the root becomes active");
        let actor = root.actor.clone();
        let mut finishing = Box::pin(root.task.finish());
        tokio::select! {
            biased;
            _ = &mut finishing => panic!("the waiting root finished without an input"),
            () = tokio::task::yield_now() => {}
        }
        drop(finishing);

        let termination = tokio::time::timeout(Duration::from_secs(1), actor.termination()).await;
        assert!(
            termination.is_ok(),
            "dropping the finish waiter left the active actor without a cleanup owner"
        );
    }

    #[tokio::test]
    async fn dropped_finish_waiter_settles_actor_owned_activation_task() {
        let (commit_entered, mut entered_commitment) = oneshot::channel();
        let (release_commitment, commit_release) = oneshot::channel();
        let (task_entered, entered_task) = oneshot::channel();
        let (release_task, task_release) = oneshot::channel();
        let (task_completed, completed_task) = oneshot::channel();
        let mut launch = Box::pin(crate::launch::spawn_root_with(
            AddressSpace::new(),
            Config::new(2),
            MailAddr(26),
            ActivationProbe,
            move |_, _, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(commit_entered),
                release: Some(commit_release),
                retirement_task: Some(RetirementTask {
                    entered: task_entered,
                    release: task_release,
                    completion: RetirementTaskCompletion::Signal(task_completed),
                }),
            },
        ));
        tokio::select! {
            _ = &mut launch => panic!("the root activated before its initialization gate"),
            entered = &mut entered_commitment => {
                entered.expect("the root entered interpretation");
            }
        }
        release_commitment
            .send(())
            .expect("the root still owns its initialization");
        let root = launch.await.expect("the root becomes active");
        let actor = root.actor.clone();
        let mut finishing = Box::pin(root.task.finish());
        tokio::select! {
            biased;
            _ = &mut finishing => panic!("the root finished before owner cancellation"),
            () = tokio::task::yield_now() => {}
        }
        drop(finishing);

        let termination = tokio::time::timeout(Duration::from_secs(1), actor.termination())
            .await
            .expect("owner cancellation publishes the root termination");
        assert_eq!(termination, Err(Crash::Cancelled));
        let entered = tokio::time::timeout(Duration::from_secs(1), entered_task)
            .await
            .expect("the actor-owned activation task starts");
        assert!(entered.is_ok(), "retirement did not abort task startup");
        let released = release_task.send(());
        assert!(
            released.is_ok(),
            "the task retains its receiver after termination"
        );
        let completed = tokio::time::timeout(Duration::from_secs(1), completed_task)
            .await
            .expect("the actor-owned activation task finishes");
        assert!(
            completed.is_ok(),
            "the actor-owned task completed exactly once"
        );
    }

    #[tokio::test]
    async fn activation_task_panic_still_unwinds_the_joining_owner() {
        let (commit_entered, mut entered_commitment) = oneshot::channel();
        let (release_commitment, commit_release) = oneshot::channel();
        let (task_entered, entered_task) = oneshot::channel();
        let (release_task, task_release) = oneshot::channel();
        let mut launch = Box::pin(crate::launch::spawn_root_with(
            AddressSpace::new(),
            Config::new(2),
            MailAddr(27),
            ActivationProbe,
            move |_, _, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(commit_entered),
                release: Some(commit_release),
                retirement_task: Some(RetirementTask {
                    entered: task_entered,
                    release: task_release,
                    completion: RetirementTaskCompletion::Panic,
                }),
            },
        ));
        tokio::select! {
            _ = &mut launch => panic!("the root activated before its initialization gate"),
            entered = &mut entered_commitment => {
                entered.expect("the root entered interpretation");
            }
        }
        release_commitment
            .send(())
            .expect("the root still owns its initialization");
        let root = launch.await.expect("the root becomes active");
        let retirement = tokio::spawn(root.task.retire());
        entered_task
            .await
            .expect("the activation task starts during retirement");
        release_task
            .send(())
            .expect("the activation task retains its release receiver");
        let Err(failure) = retirement.await else {
            panic!("the actor-owned task panic reaches its joining owner")
        };
        assert!(failure.is_panic(), "the panic is not an actor cancellation");
    }

    fn assert_retired_initialization(
        residual: LocalResidual<
            ActionsOf<ActivationProbe>,
            ActionSettlementOf<ActivationProbe>,
            ActivationEvent,
            User<MailAddr, ()>,
            RetirementCustody,
        >,
        expected_status: SettlementStatus,
    ) {
        let LocalResidual::Retired {
            settlements,
            ingress,
            activation_tasks,
            descendants,
        } = residual
        else {
            panic!("interpreted initialization was returned as uncommitted")
        };
        let statuses = settlements
            .iter()
            .map(ClassifySettlement::settlement_status)
            .collect::<Vec<_>>();

        assert_eq!(statuses, [expected_status]);
        assert_eq!(settlements.len(), 1);
        assert_eq!(settlements[0].creations.len(), 0);
        assert!(matches!(settlements[0].become_, behavior::Step::Continue));
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, RetirementCustody(23));
    }

    #[tokio::test]
    async fn address_is_absent_until_accepted_initialization_commit_completes() {
        let address = MailAddr(23);
        let GatedActivation {
            environment,
            initialization,
            addresses,
            commit_entered,
            release_commit,
        } = prepare_gated_activation(InitializationDisposition::Accepted);
        let activation = tokio::spawn(async move { environment.activate(initialization).await });
        let entered_result = commit_entered.await;
        assert!(entered_result.is_ok(), "initialization commit must start");
        let pending_visibility = address_visibility(&addresses, address);
        let release_result = release_commit.send(());
        assert!(release_result.is_ok(), "initialization commit must resume");
        let activation_result = activation
            .await
            .expect("the activation task must retain ownership");
        let Ok((mut active, interpretation)) = activation_result else {
            panic!("accepted initialization was rejected")
        };
        let committed_visibility = address_visibility(&addresses, address);
        let status = interpretation.settlement_status();
        let settlement = interpretation.into_settlement();
        active.publish();
        let published_visibility = address_visibility(&addresses, address);
        let residual = active.retire(vec![settlement]).await;
        let retired_visibility = address_visibility(&addresses, address);

        assert_eq!(pending_visibility, AddressVisibility::Absent);
        assert_eq!(committed_visibility, AddressVisibility::Absent);
        assert_eq!(published_visibility, AddressVisibility::Present);
        assert_eq!(status, SettlementStatus::Accepted);
        assert_eq!(retired_visibility, AddressVisibility::Absent);
        assert_retired_initialization(residual, SettlementStatus::Accepted);
    }

    async fn assert_failed_initialization_never_claims(
        disposition: InitializationDisposition,
        expected_status: SettlementStatus,
    ) {
        let address = MailAddr(23);
        let GatedActivation {
            environment,
            initialization,
            addresses,
            commit_entered,
            release_commit,
        } = prepare_gated_activation(disposition);
        let activation = tokio::spawn(async move { environment.activate(initialization).await });
        let entered_result = commit_entered.await;
        assert!(entered_result.is_ok(), "initialization commit must start");
        let pending_visibility = address_visibility(&addresses, address);
        let release_result = release_commit.send(());
        assert!(release_result.is_ok(), "initialization commit must resume");
        let activation_result = activation
            .await
            .expect("the activation task must retain ownership");
        let committed_visibility = address_visibility(&addresses, address);
        let Ok((active, interpretation)) = activation_result else {
            panic!("settlement failure lost its unpublished retirement owner")
        };
        let status = interpretation.settlement_status();
        let settlement = interpretation.into_settlement();
        let residual = active.retire(vec![settlement]).await;
        let retired_visibility = address_visibility(&addresses, address);

        assert_eq!(pending_visibility, AddressVisibility::Absent);
        assert_eq!(committed_visibility, AddressVisibility::Absent);
        assert_eq!(status, expected_status);
        assert_eq!(retired_visibility, AddressVisibility::Absent);
        assert_retired_initialization(residual, expected_status);
    }

    #[tokio::test]
    async fn rejected_initialization_never_becomes_addressable() {
        assert_failed_initialization_never_claims(
            InitializationDisposition::Rejected,
            SettlementStatus::Rejected,
        )
        .await;
    }

    #[tokio::test]
    async fn corrupt_initialization_never_becomes_addressable() {
        assert_failed_initialization_never_claims(
            InitializationDisposition::Corrupt,
            SettlementStatus::Corrupt,
        )
        .await;
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
    fn admission_closure_names_first_and_repeated_dispositions() {
        let (_control, owner, _mailbox, _consumer) = mailbox_channel::<(), ()>(Config::new(2));
        let admission = Admission::new(owner);

        let first = admission.close();
        let repeated = admission.close();

        assert!(matches!(first, AdmissionClosure::Closed));
        assert!(matches!(repeated, AdmissionClosure::AlreadyClosed));
    }

    #[tokio::test]
    async fn exact_shutdown_classifies_repeated_and_stopped_requests_without_resending() {
        type ProbeProtocol = MessageProtocol<MailAddr, ()>;
        type ShutdownEvent = <StopOnShutdown<FenceProbe> as Behavior>::Event;

        let (control, owner, mailbox, mut consumer) =
            mailbox_channel::<ShutdownEvent, User<MailAddr, ()>>(Config::new(2));
        let admission = Arc::new(Admission::new(owner));
        let (termination_publisher, termination) = crate::observe::pair();
        let actor = ActorRef::<ProbeProtocol>::new(
            MailAddr(36),
            EndpointMailbox::Standard {
                mailbox,
                admission: Arc::downgrade(&admission),
            },
            termination,
        );

        let accepted = request_actor_shutdown(&actor, &control, Ingress::new());
        let repeated = request_actor_shutdown(&actor, &control, Ingress::new());

        assert_eq!(accepted, Ok(()));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        let accepted_event = consumer.recv_control().await;
        assert!(accepted_event.is_some());

        termination_publisher.complete(Ok(Exit::Normal));
        let stopped = request_actor_shutdown(&actor, &control, Ingress::new());

        assert_eq!(stopped, Err(ShutdownRejection::AlreadyStopped));
        drop(control);
        let closed_control = consumer.recv_control().await;
        assert!(closed_control.is_none());
        drop((actor, admission, consumer));
    }

    #[tokio::test]
    async fn ordinary_actor_does_not_admit_entity_fences() {
        let actor = crate::launch::launch_inert(
            crate::ActorSpace::new(),
            Config::new(2),
            MailAddr(37),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await
        .unwrap();

        let fence = actor.actor.fence().await;
        assert_eq!(fence, Err(crate::entity::FenceFailure::Enqueue));
        actor
            .actor
            .send_from(actor.actor.address(), ())
            .await
            .unwrap();
        let control = actor.shutdown_control.upgrade().unwrap();
        let shutdown = request_actor_shutdown(&actor.actor, &control, Ingress::new());
        assert_eq!(shutdown, Ok(()));
        let termination = actor.actor.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn entity_launch_preserves_the_exact_conflicting_address() {
        let actors = crate::ActorSpace::new();
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
        let shutdown = request_actor_shutdown(&first.actor, &first.control, Ingress::new());
        assert_eq!(shutdown, Ok(()));
        let termination = first.actor.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn user_lane_fence_follows_the_preceding_applied_action() {
        let committed = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&committed);
        let actor = crate::launch::launch_inert_entity(
            crate::ActorSpace::new(),
            Config::new(4),
            MailAddr(41),
            StopOnShutdown::new(FenceProbe),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();

        actor
            .actor
            .send_from(actor.actor.address(), ())
            .await
            .unwrap();
        actor.actor.fence().await.unwrap();

        assert_eq!(committed.load(Ordering::SeqCst), 2);
        let shutdown = request_actor_shutdown(&actor.actor, &actor.control, Ingress::new());
        assert_eq!(shutdown, Ok(()));
        let termination = actor.actor.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn fence_rejected_before_enqueue_reports_the_exact_stage() {
        let actor = crate::launch::launch_inert_entity(
            crate::ActorSpace::new(),
            Config::new(2),
            MailAddr(43),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await
        .unwrap();
        let shutdown = request_actor_shutdown(&actor.actor, &actor.control, Ingress::new());
        assert_eq!(shutdown, Ok(()));
        let termination = actor.actor.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));

        let fence = actor.actor.fence().await;
        assert_eq!(fence, Err(crate::entity::FenceFailure::Enqueue));
    }

    #[tokio::test]
    async fn accepted_fence_collected_during_failure_reports_acknowledgement() {
        let actor = crate::launch::launch_inert_entity(
            crate::ActorSpace::new(),
            Config::new(2),
            MailAddr(47),
            StopOnShutdown::new(PanickingFenceProbe),
            |_| {},
        )
        .await
        .unwrap();

        actor
            .actor
            .send_from(actor.actor.address(), ())
            .await
            .unwrap();
        let fence = actor.actor.fence().await;
        assert_eq!(fence, Err(crate::entity::FenceFailure::Acknowledgement));
        let termination = actor.actor.termination().await;
        assert_eq!(termination, Err(Crash::Panicked));
    }
}

#[cfg(test)]
mod shutdown_admission_contract {
    use super::{ActorRef, Admission, InstalledActor, LocalResidual};
    use crate::ActorExecutionOutcome;
    use crate::ActorSpace;
    use crate::actor;
    use crate::address::MailAddr;
    use crate::launch::launch_inert_entity;
    use crate::observe;
    use behavior::{
        Actions, Become, Behavior, BehaviorActed, BehaviorBase, EventLayer, Here, Ingress, Inside,
        Never, NoSends, Step, Stopped, User,
    };
    use behavior_actors::{Exit, ShutdownRejection, ShutdownRequested, StopOnShutdown};
    use bombay_engine::Completion;
    use communication::{Config, Received, mailbox_channel};
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct LedgerSubmission {
        entries: Vec<u64>,
    }

    #[derive(Default)]
    struct DeliveryLedger {
        received: Vec<User<MailAddr, LedgerSubmission>>,
    }
    #[actor]
    impl DeliveryLedger {
        fn receive(&mut self, from: MailAddr, values: LedgerSubmission) -> BehaviorActed<Self> {
            self.received.push(User::new(from, values));
            Ok(Actions::cont())
        }
    }
    #[tokio::test]
    async fn installed_shutdown_preserves_preclose_permit_and_rejects_later_original() {
        type Target = StopOnShutdown<DeliveryLedger>;
        let (control, owner, mailbox, mut receiver) = mailbox_channel::<
            <Target as Behavior>::Event,
            User<MailAddr, LedgerSubmission>,
        >(Config::new(1));
        let admission = Arc::new(Admission::new(owner));
        let (publication, observation) = observe::pair();
        let endpoint = ActorRef::<DeliveryLedger>::external(
            MailAddr(571),
            mailbox,
            Arc::downgrade(&admission),
            observation,
        );
        let installed = InstalledActor::<Target>::new(endpoint.clone(), control);
        let prefix = LedgerSubmission {
            entries: vec![67, 71],
        };
        let prefix_allocation = prefix.entries.as_ptr();
        let sent = endpoint.send_from(MailAddr(73), prefix).await;
        assert!(sent.is_ok());
        let second = LedgerSubmission {
            entries: vec![79, 83],
        };
        let second_allocation = second.entries.as_ptr();
        let sent = endpoint.send_from(MailAddr(89), second).await;
        assert!(sent.is_ok());
        let values = LedgerSubmission {
            entries: vec![97, 101],
        };
        let allocation = values.entries.as_ptr();
        let mut acquired = pin!(endpoint.send_from(MailAddr(103), values));
        let mut context = Context::from_waker(Waker::noop());
        let pending = acquired.as_mut().poll(&mut context);
        assert!(matches!(pending, Poll::Pending));
        let shutdown = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(shutdown, Ok(()));
        let postclose = LedgerSubmission {
            entries: vec![107, 109],
        };
        let postclose_allocation = postclose.entries.as_ptr();
        let mut later = pin!(endpoint.send_from(MailAddr(113), postclose));
        let attempted = later.as_mut().poll(&mut context);
        let Poll::Ready(rejected) = attempted else {
            panic!("postclose admission must reject immediately")
        };
        let original = rejected
            .expect_err("first-polled postclose delivery cannot acquire admission")
            .into_message();
        assert_eq!(original.entries, [107, 109]);
        assert_eq!(original.entries.as_ptr(), postclose_allocation);
        let replay = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(replay, Err(ShutdownRejection::AlreadyStopping));
        let requested = receiver
            .recv_control()
            .await
            .expect("one original shutdown input");
        assert!(matches!(requested, EventLayer::Owned(ShutdownRequested)));
        let first = receiver.recv().await;
        let Some(Received::User(first)) = first else {
            panic!("first admitted original")
        };
        assert_eq!(first.from, MailAddr(73));
        assert_eq!(first.message.entries, [67, 71]);
        assert_eq!(first.message.entries.as_ptr(), prefix_allocation);
        let second = receiver.recv().await;
        let Some(Received::User(second)) = second else {
            panic!("second admitted original")
        };
        assert_eq!(second.from, MailAddr(89));
        assert_eq!(second.message.entries, [79, 83]);
        assert_eq!(second.message.entries.as_ptr(), second_allocation);
        let admitted = acquired.await;
        assert!(admitted.is_ok());
        let third = receiver.recv().await;
        let Some(Received::User(third)) = third else {
            panic!("preclose permit must finish before terminal marker")
        };
        assert_eq!(third.from, MailAddr(103));
        assert_eq!(third.message.entries, [97, 101]);
        assert_eq!(third.message.entries.as_ptr(), allocation);
        let terminal = receiver.recv().await;
        assert!(matches!(terminal, Some(Received::UserLaneClosed)));
        publication.complete(Ok(Exit::Normal));
        let stopped = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        let repeated = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(stopped, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopped));
        drop(installed);
        let exhausted = receiver.recv().await;
        assert!(exhausted.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep both owned lease incarnations, full results, and replay trace together."
    )]
    async fn installed_shutdown_cannot_retarget_actual_distinct_event_replacement() {
        type Direct = StopOnShutdown<DeliveryLedger>;
        type Nested = StopOnShutdown<Direct>;
        let actors = ActorSpace::<DeliveryLedger>::new();
        let address = MailAddr(593);
        let old_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&old_actions);
        let old = launch_inert_entity(
            actors.clone(),
            Config::new(2),
            address,
            StopOnShutdown::new(DeliveryLedger::default()),
            move |actions| {
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.sends.owned, NoSends));
                assert!(matches!(actions.sends.inner, NoSends));
                recording
                    .lock()
                    .expect("complete action observation")
                    .push(actions.become_);
            },
        )
        .await
        .unwrap_or_else(|_| panic!("old installed incarnation"));
        let installed = InstalledActor::<Direct>::new(old.actor.clone(), old.control.clone());
        let requested = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        let replay = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(requested, Ok(()));
        assert_eq!(replay, Err(ShutdownRejection::AlreadyStopping));
        let terminal = old.actor.termination().await;
        assert_eq!(terminal, Ok(Exit::Normal));
        let retired = old.task.finish().await;
        let ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    settlements,
                    ingress,
                    descendants,
                    activation_tasks,
                },
            completion,
        } = retired
        else {
            panic!("whole old owned result")
        };
        assert!(activation_tasks.is_empty());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(behavior.base().received.len(), 0);
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert_eq!(descendants, ());
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].creations.is_empty());
        assert!(matches!(settlements[0].sends.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner, NoSends));
        assert!(matches!(settlements[0].become_, Step::Stop(_)));
        assert_eq!(
            *old_actions.lock().expect("old full actions"),
            [Step::Continue, Step::Stop(Stopped)]
        );
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
        let fresh_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&fresh_actions);
        let fresh = launch_inert_entity(
            actors.clone(),
            Config::new(2),
            address,
            StopOnShutdown::new(StopOnShutdown::new(DeliveryLedger::default())),
            move |actions| {
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.sends.owned, NoSends));
                assert!(matches!(actions.sends.inner.owned, NoSends));
                assert!(matches!(actions.sends.inner.inner, NoSends));
                recording
                    .lock()
                    .expect("complete action observation")
                    .push(actions.become_);
            },
        )
        .await
        .unwrap_or_else(|_| panic!("actual same-address nested replacement"));
        let replacement = InstalledActor::<Nested>::new(fresh.actor.clone(), fresh.control.clone());
        let stale = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        let stale_replay = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(stale, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(stale_replay, Err(ShutdownRejection::AlreadyStopped));
        let values = LedgerSubmission {
            entries: vec![157, 163],
        };
        let allocation = values.entries.as_ptr();
        let sent = fresh.actor.send_from(MailAddr(167), values).await;
        assert!(sent.is_ok());
        let committed = fresh.actor.fence().await;
        assert!(committed.is_ok());
        let mut publication = pin!(fresh.actor.termination());
        let mut context = Context::from_waker(Waker::noop());
        let pending = publication.as_mut().poll(&mut context);
        assert!(matches!(pending, Poll::Pending));
        let requested =
            replacement.request_shutdown(Ingress::<ShutdownRequested, Inside<Here>>::new());
        let repeated =
            replacement.request_shutdown(Ingress::<ShutdownRequested, Inside<Here>>::new());
        assert_eq!(requested, Ok(()));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        let stopped = publication.await;
        assert_eq!(stopped, Ok(Exit::Normal));
        let retired = fresh.task.finish().await;
        let ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    settlements,
                    ingress,
                    descendants,
                    activation_tasks,
                },
            completion,
        } = retired
        else {
            panic!("whole replacement owned result")
        };
        assert!(activation_tasks.is_empty());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert_eq!(descendants, ());
        assert_eq!(
            *fresh_actions.lock().expect("fresh full actions"),
            [Step::Continue, Step::Continue, Step::Stop(Stopped)]
        );
        assert_eq!(behavior.base().received.len(), 1);
        let original = &behavior.base().received[0];
        assert_eq!(original.from, MailAddr(167));
        assert_eq!(original.message.entries, [157, 163]);
        assert_eq!(original.message.entries.as_ptr(), allocation);
        let final_turn = settlements.len() - 1;
        for (turn, settlement) in settlements.into_iter().enumerate() {
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends.owned, NoSends));
            assert!(matches!(settlement.sends.inner.owned, NoSends));
            assert!(matches!(settlement.sends.inner.inner, NoSends));
            match settlement.become_ {
                Step::Continue => assert!(turn < final_turn),
                Step::Stop(_) => assert_eq!(turn, final_turn),
            }
        }
        let stale_replay = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        let final_replay =
            replacement.request_shutdown(Ingress::<ShutdownRequested, Inside<Here>>::new());
        assert_eq!(stale_replay, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(final_replay, Err(ShutdownRejection::AlreadyStopped));
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
    }
}
