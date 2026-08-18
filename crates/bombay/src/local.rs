//! One local typed mailbox behind the existing Engine environment port.
//!
//! This is deliberately crate-private. It proves the mailbox/address layer
//! without prescribing construction, task, handle, or System APIs.

use core::future::Future;
use core::hash::Hash;
use core::marker::PhantomData;

use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, InjectEvent, Never, Protocol, ShutdownRequested, User,
    UserEvent,
};
use bombay_address::{AddressSpace, ClaimError, Lease};
use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};
use communication::{
    Config, Consumer, ControlSender, MailboxOwner, MailboxRef, Received, UserClosed,
    mailbox_channel,
};
use observe::Observation;

pub(crate) type Termination<A> = Result<behavior::Exit<A>, behavior::Crash>;

/// Commits one complete action value for the concrete local actor.
///
/// Product traversal and concrete runtime services remain behind this private
/// seam. It is not an application extension API.
pub trait CommitActions<B: Behavior<Ph = Never>> {
    type Error;

    fn commit(
        &mut self,
        actions: ActionsOf<B>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn retire(self) -> impl Future<Output = ()> + Send
    where
        Self: Sized + Send,
    {
        async {}
    }
}

/// Shared access to one affine Communication admission owner.
///
/// The active environment holds the only strong reference. Actor references
/// hold a weak reference so they cannot keep admission open. Closing takes the
/// owner exactly once; dropping the final strong reference closes it through
/// Communication's `MailboxOwner` drop law.
struct Admission<U> {
    owner: std::sync::Mutex<Option<MailboxOwner<U>>>,
}

trait ShutdownControl: Send + Sync {
    fn request(&self) -> bool;
}

struct TypedShutdownControl<E> {
    control: ControlSender<E>,
}

impl<E> ShutdownControl for TypedShutdownControl<E>
where
    E: InjectEvent<ShutdownRequested, behavior::Here> + Send,
{
    fn request(&self) -> bool {
        self.control.send(E::inject_at(ShutdownRequested)).is_ok()
    }
}

impl<U> Admission<U> {
    fn new(owner: MailboxOwner<U>) -> Self {
        Self {
            owner: std::sync::Mutex::new(Some(owner)),
        }
    }

    fn close(&self) {
        if let Some(owner) = self
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            owner.close_admission();
        }
    }
}

/// A non-owning, protocol-indexed user-lane capability.
///
/// The Communication anchor cannot keep or resurrect an actor. Indexing by
/// `P` preserves destination protocol identity independently of the concrete
/// behavior or transparent wrappers currently implementing it.
pub struct ActorRef<P: Protocol> {
    address: P::Addr,
    mailbox: MailboxRef<User<P::Addr, P::Msg>>,
    admission: std::sync::Weak<Admission<User<P::Addr, P::Msg>>>,
    shutdown: std::sync::Weak<dyn ShutdownControl>,
    termination: Observation<Termination<P::Addr>>,
    protocol: PhantomData<fn() -> P>,
}

impl<P: Protocol> Clone for ActorRef<P> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            mailbox: self.mailbox.clone(),
            admission: self.admission.clone(),
            shutdown: self.shutdown.clone(),
            termination: self.termination.clone(),
            protocol: PhantomData,
        }
    }
}

impl<P: Protocol> ActorRef<P> {
    fn new(
        address: P::Addr,
        mailbox: MailboxRef<User<P::Addr, P::Msg>>,
        admission: std::sync::Weak<Admission<User<P::Addr, P::Msg>>>,
        shutdown: std::sync::Weak<dyn ShutdownControl>,
        termination: Observation<Termination<P::Addr>>,
    ) -> Self {
        Self {
            address,
            mailbox,
            admission,
            shutdown,
            termination,
            protocol: PhantomData,
        }
    }

    /// Observe the retained terminal result of this exact incarnation.
    ///
    /// Every clone refers to the same one-publication fact. Resolving a later
    /// actor at the same address produces a different observation.
    #[must_use]
    pub fn termination(&self) -> Observation<Termination<P::Addr>> {
        self.termination.clone()
    }

    pub(crate) fn request_shutdown(&self) -> bool {
        let Some(admission) = self.admission.upgrade() else {
            return false;
        };
        let Some(shutdown) = self.shutdown.upgrade() else {
            return false;
        };
        admission.close();
        shutdown.request()
    }

    /// The typed address of this exact actor reference.
    #[must_use]
    pub const fn address(&self) -> P::Addr {
        self.address
    }

    /// Admit one user message through the behavior's complete event sum.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] with the original message when this incarnation's
    /// user lane is no longer live.
    pub async fn send(&self, from: P::Addr, message: P::Msg) -> Result<(), SendError<P::Msg>> {
        self.mailbox
            .send(User::new(from, message))
            .await
            .map_err(SendError::from_rejected)
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

    fn from_rejected<A>(rejected: UserClosed<User<A, M>>) -> Self {
        let UserClosed(user) = rejected;
        Self(user.message)
    }
}

/// Failure while committing initialization or claiming the exact address.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum LocalActivationError<C, A> {
    #[error("initial behavior actions could not be committed")]
    Commit(#[source] C),
    #[error("actor address generation could not be claimed")]
    Address(#[source] A),
}

/// The prepared local half of one mailbox-backed behavior generation.
pub struct LocalEnvironment<B: Behavior, I, P = fn(ActorRef<<B as Behavior>::Protocol>)> {
    address: BehaviorAddr<B>,
    addresses: AddressSpace<BehaviorAddr<B>, ActorRef<B::Protocol>>,
    endpoint: ActorRef<B::Protocol>,
    consumer: Consumer<B::Event, User<BehaviorAddr<B>, BehaviorMessage<B>>>,
    admission: std::sync::Arc<Admission<User<BehaviorAddr<B>, BehaviorMessage<B>>>>,
    control_liveness: std::sync::Arc<ControlSender<B::Event>>,
    shutdown_liveness: std::sync::Arc<dyn ShutdownControl>,
    timers: crate::time::LocalTimers<B::Event>,
    facts: crate::observation::FactQueue<B::Event>,
    interpreter: I,
    publish: P,
}

impl<B, I> LocalEnvironment<B, I, fn(ActorRef<B::Protocol>)>
where
    B: Behavior,
    BehaviorAddr<B>: Hash,
    B::Event: InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
{
    /// Prepare one mailbox and construct its interpreter with a clone of the
    /// control-lane capability for this exact incarnation.
    pub(crate) fn with_interpreter(
        address: BehaviorAddr<B>,
        addresses: AddressSpace<BehaviorAddr<B>, ActorRef<B::Protocol>>,
        config: Config,
        termination: Observation<Termination<BehaviorAddr<B>>>,
        make_interpreter: impl FnOnce(
            ControlSender<B::Event>,
            std::sync::Arc<crate::generation::TerminalOverride<BehaviorAddr<B>>>,
            crate::time::LocalTimers<B::Event>,
            crate::observation::FactQueue<B::Event>,
        ) -> I,
        terminal_override: std::sync::Arc<crate::generation::TerminalOverride<BehaviorAddr<B>>>,
    ) -> Self {
        let (control_liveness, owner, mailbox, consumer) = mailbox_channel(config);
        let control_liveness = std::sync::Arc::new(control_liveness);
        let admission = std::sync::Arc::new(Admission::new(owner));
        let shutdown_liveness: std::sync::Arc<dyn ShutdownControl> =
            std::sync::Arc::new(TypedShutdownControl {
                control: (*control_liveness).clone(),
            });
        let endpoint = ActorRef::new(
            address,
            mailbox,
            std::sync::Arc::downgrade(&admission),
            std::sync::Arc::downgrade(&shutdown_liveness),
            termination,
        );
        let timers = crate::time::LocalTimers::new();
        let facts = crate::observation::FactQueue::new();
        let interpreter = make_interpreter(
            (*control_liveness).clone(),
            terminal_override,
            timers.clone(),
            facts.clone(),
        );
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
            publish: ignore_publication::<B>,
        }
    }
}

fn ignore_publication<B: Behavior>(_: ActorRef<B::Protocol>) {}

impl<B: Behavior, I, P> LocalEnvironment<B, I, P> {
    pub(crate) fn publish_with<N>(self, publish: N) -> LocalEnvironment<B, I, N> {
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
            publish,
        }
    }
}

/// The only local value with mailbox ingress and address ownership.
pub struct ActiveLocalEnvironment<B: Behavior, I>
where
    BehaviorAddr<B>: Hash,
{
    consumer: Consumer<B::Event, User<BehaviorAddr<B>, BehaviorMessage<B>>>,
    admission: std::sync::Arc<Admission<User<BehaviorAddr<B>, BehaviorMessage<B>>>>,
    control_liveness: Option<std::sync::Arc<ControlSender<B::Event>>>,
    shutdown_liveness: std::sync::Arc<dyn ShutdownControl>,
    timers: crate::time::LocalTimers<B::Event>,
    facts: crate::observation::FactQueue<B::Event>,
    interpreter: I,
    _lease: Lease<BehaviorAddr<B>, ActorRef<B::Protocol>>,
}

#[allow(
    refining_impl_trait,
    reason = "Bombay's concrete Tokio environment refines executor-neutral Engine futures"
)]
impl<B, I, P> Environment<B> for LocalEnvironment<B, I, P>
where
    B: Behavior<Ph = Never> + Send,
    BehaviorAddr<B>: Hash + Clone + Send + Sync,
    B::Event: Send,
    B::Event: InjectEvent<ShutdownRequested, behavior::Here> + 'static,
    B::Sends: Send,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <BehaviorAddr<B> as behavior::Address>::Nonce: Send,
    I: CommitActions<B> + Send,
    P: FnOnce(ActorRef<B::Protocol>) + Send,
{
    type Active = ActiveLocalEnvironment<B, I>;
    type Error = LocalActivationError<I::Error, ClaimError<BehaviorAddr<B>>>;

    async fn activate(mut self, actions: ActionsOf<B>) -> Result<Self::Active, Self::Error> {
        self.interpreter
            .commit(actions)
            .await
            .map_err(LocalActivationError::Commit)?;
        let published = self.endpoint.clone();
        let lease = self
            .addresses
            .try_claim(self.address, self.endpoint)
            .map_err(LocalActivationError::Address)?;
        (self.publish)(published);
        let control_liveness = Some(self.control_liveness);
        Ok(ActiveLocalEnvironment {
            consumer: self.consumer,
            admission: self.admission,
            control_liveness,
            shutdown_liveness: self.shutdown_liveness,
            timers: self.timers,
            facts: self.facts,
            interpreter: self.interpreter,
            _lease: lease,
        })
    }
}

#[allow(
    refining_impl_trait,
    reason = "Bombay's concrete Tokio environment refines executor-neutral Engine futures"
)]
impl<B, I> ActiveEnvironment<B> for ActiveLocalEnvironment<B, I>
where
    B: Behavior<Ph = Never> + Send,
    BehaviorAddr<B>: Hash + Send + Sync,
    B::Event: Send,
    B::Sends: Send,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <BehaviorAddr<B> as behavior::Address>::Nonce: Send,
    I: CommitActions<B> + Send,
{
    type Error = I::Error;

    async fn next(&mut self) -> Option<B::Event> {
        enum Acquired<E, U> {
            Mailbox(Option<Received<E, U>>),
            Fact(E),
            Deadline,
        }

        loop {
            let acquired = if let Some(deadline) = self.timers.next_deadline() {
                tokio::select! {
                    biased;
                    received = self.consumer.recv() => Acquired::Mailbox(received),
                    event = self.facts.next() => Acquired::Fact(event),
                    () = tokio::time::sleep_until(deadline.into()) => Acquired::Deadline,
                }
            } else {
                tokio::select! {
                    biased;
                    received = self.consumer.recv() => Acquired::Mailbox(received),
                    event = self.facts.next() => Acquired::Fact(event),
                }
            };
            let received = match acquired {
                Acquired::Fact(event) => return Some(event),
                Acquired::Deadline => {
                    if let Some(event) = self.timers.pop_due(std::time::Instant::now()) {
                        return Some(event);
                    }
                    continue;
                }
                Acquired::Mailbox(received) => received?,
            };
            match received {
                Received::Control(event) => return Some(event),
                Received::User(user) => return Some(B::Event::user(user.from, user.message)),
                Received::UserLaneClosed => {
                    // This closes only the bounded user lane. Control events
                    // remain admissible until the complete consumer closes.
                }
            }
        }
    }

    fn apply(
        &mut self,
        actions: ActionsOf<B>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.interpreter.commit(actions)
    }

    async fn retire(self) {
        let Self {
            consumer,
            admission,
            control_liveness,
            shutdown_liveness,
            timers,
            facts,
            interpreter,
            _lease: lease,
        } = self;
        admission.close();
        interpreter.retire().await;
        drop((
            consumer,
            admission,
            control_liveness,
            shutdown_liveness,
            timers,
            facts,
            lease,
        ));
    }
}
