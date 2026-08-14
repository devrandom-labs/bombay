//! Actor execution ownership.

use behavior::{
    Address, Behavior, BirthMode, EventInput, Never, RouteInput, ShutdownRequested, TimerElapsed,
};
use bombay_engine::{Driver, Environment, RunError, RunExit};
use observe::ObservationSpace;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::lifecycle::{IncarnationReporter, LifecycleFactory};
use super::{
    CreationFailure, LaunchMode, Lifecycle, NoLifecycle, PreparedIncarnation,
    ProvisionalIncarnation,
};
use super::{IncarnationEffects, NoParent, ParentReporter};
use crate::{
    ActorEnvironment, ActorRef, ChildLease, ChildRuntime, EndpointRegistry, EventSender, Handle,
    IncarnationEndpoint, MailboxAnchor, MailboxConfig, MailboxDeliveryClosed, MailboxReceiver,
    MailboxSender, ObservesCreations, RejectedDelivery, RouteSends, RuntimeBirthMode,
    ShutdownRequestError, SystemChildren, TaskOutcome,
};

/// The result produced by one actor task.
#[doc(hidden)]
pub(crate) type ActorResult<B, E> = Result<
    RunExit<behavior::Exit<<B as Behavior>::Addr>>,
    RunError<<B as Behavior>::Error, <E as Environment>::Error>,
>;

/// Tokio-backed actor execution boundary with shared typed routing.
pub struct System<R, L = NoLifecycle> {
    mailbox: MailboxConfig,
    router: R,
    lifecycle: L,
}

/// One inert actor definition, before Bombay allocates runtime resources.
///
/// The behavior owns application state and statically selects its complete
/// protocol; the address value selects this actor's identity. Constructing an
/// actor creates no mailbox, task, registration, lifecycle state, or handle.
///
/// An address from another behavior namespace cannot form the actor:
///
/// ```compile_fail
/// use bombay::{Actor, behavior::{Actions, MailAddr, Never, NoBirths}};
///
/// struct State;
/// #[bombay::behavior::behavior(
///     addr = MailAddr,
///     message = (),
///     sends = Vec<Never>,
///     births = NoBirths,
///     error = Never,
/// )]
/// impl State {
///     fn receive(
///         &mut self,
///         _: MailAddr,
///         _: (),
///     ) -> bombay::behavior::BehaviorActed<Self> {
///         Ok(Actions::cont())
///     }
/// }
///
/// let _ = Actor::new(7_u8, State);
/// ```
pub struct Actor<B: Behavior> {
    address: B::Addr,
    definition: behavior::Compose<B>,
}

impl<B: Behavior> Actor<B> {
    /// Pair one behavior with an address from its declared namespace.
    #[must_use]
    pub const fn new(address: B::Addr, behavior: B) -> Self {
        Self::from_definition(address, behavior::Compose::new(behavior))
    }

    /// Pair one composed behavior definition with its address.
    #[must_use]
    pub const fn from_definition(address: B::Addr, definition: behavior::Compose<B>) -> Self {
        Self {
            address,
            definition,
        }
    }

    /// Borrow this actor's address.
    #[must_use]
    pub const fn address(&self) -> &B::Addr {
        &self.address
    }

    /// Borrow this actor's behavior and application state.
    #[must_use]
    pub fn behavior(&self) -> &B {
        self.definition.definition()
    }

    /// Recover the inert definition without allocating runtime resources.
    #[must_use]
    pub fn into_parts(self) -> (B::Addr, behavior::Compose<B>) {
        (self.address, self.definition)
    }
}

impl<R: Clone, L: Clone> Clone for System<R, L> {
    fn clone(&self) -> Self {
        Self {
            mailbox: self.mailbox,
            router: self.router.clone(),
            lifecycle: self.lifecycle.clone(),
        }
    }
}

impl<R> System<R, NoLifecycle> {
    /// Construct a Tokio-backed system from its communication configuration and router.
    pub const fn new(mailbox: MailboxConfig, router: R) -> Self {
        Self {
            mailbox,
            router,
            lifecycle: NoLifecycle,
        }
    }

    /// Construct a Tokio-backed system with a statically dispatched lifecycle sink.
    pub const fn with_lifecycle<S>(
        mailbox: MailboxConfig,
        router: R,
        lifecycle: S,
    ) -> System<R, Lifecycle<S>> {
        System {
            mailbox,
            router,
            lifecycle: Lifecycle(lifecycle),
        }
    }
}

/// Address projected from a behavior.
type AddrOf<B> = <B as Behavior>::Addr;

/// Event projected from a behavior.
type EventOf<B> = <B as Behavior>::Event;

/// Nonce projected from a behavior's address.
type NonceOf<B> = <AddrOf<B> as Address>::Nonce;

/// Non-owning mailbox endpoint for a behavior's event protocol.
type AnchorOf<B> = MailboxAnchor<EventOf<B>>;

/// Child behavior projected from a behavior's birth mode.
type ChildOf<B> = <<B as Behavior>::Birth as BirthMode>::Child;

/// Child runtime constructed by a behavior's birth mode under `Y`.
type RuntimeOf<B, Y> =
    <<B as Behavior>::Birth as RuntimeBirthMode<AddrOf<B>, Y, AnchorOf<B>>>::Runtime;

/// Registered endpoint published for one behavior incarnation.
type AnchorEndpoint<B> = IncarnationEndpoint<AddrOf<B>, ActorRef<AddrOf<B>, AnchorOf<B>>>;

/// Generation-local effects interpreting one behavior's send algebra.
type EffectsOf<R, B, Y, P> = IncarnationEffects<
    R,
    NonceOf<B>,
    <RuntimeOf<B, Y> as ChildRuntime<AddrOf<B>, ChildOf<B>, AnchorOf<B>>>::Lease,
    AnchorOf<B>,
    P,
    AddrOf<B>,
>;

/// Direct reference produced for a behavior by Bombay Communication.
#[doc(hidden)]
pub(crate) type BehaviorRef<B, L = NoLifecycle> = ActorRef<AddrOf<B>, MailboxSender<EventOf<B>>, L>;

/// Child runtime derived from a behavior's birth mode.
#[doc(hidden)]
pub(crate) type BehaviorChildren<R, B, L = NoLifecycle> = RuntimeOf<B, System<R, L>>;

/// Environment constructed for a behavior by a configured system.
#[doc(hidden)]
pub(crate) type BehaviorEnvironment<R, B, P = NoParent, L = NoLifecycle> =
    ActorEnvironment<B, MailboxReceiver<EventOf<B>>, R, BehaviorChildren<R, B, L>, AnchorOf<B>, P>;

/// Registration ownership token claimed for one behavior incarnation.
#[doc(hidden)]
type BehaviorRegistration<R, B> = <R as EndpointRegistry<B, AnchorEndpoint<B>>>::Registration;

/// Lifecycle reporter derived for one behavior incarnation.
type BehaviorReporter<R, B, L> =
    <L as LifecycleFactory<AddrOf<B>, BehaviorRegistration<R, B>>>::Reporter;

/// Exact terminal value produced by one transactionally activated root.
#[doc(hidden)]
pub type RootOutcome<R, B, L = NoLifecycle> =
    ActorResult<B, BehaviorEnvironment<R, B, NoParent, L>>;

/// Cloneable, delivery-only capability for one activated root incarnation.
pub struct RootEndpoint<B: Behavior<Ph = Never>> {
    inner: ActorRef<AddrOf<B>, AnchorOf<B>>,
}

impl<B> Clone for RootEndpoint<B>
where
    B: Behavior<Ph = Never>,
    AddrOf<B>: Clone,
    AnchorOf<B>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B> crate::DeliveryEndpoint<AddrOf<B>, B::Msg> for RootEndpoint<B>
where
    B: Behavior<Ph = Never>,
    AddrOf<B>: Send + Sync,
    B::Event: behavior::UserEvent<Addr = AddrOf<B>> + Send,
    B::Msg: Send,
{
    type Error = MailboxDeliveryClosed;

    async fn deliver(
        &self,
        from: AddrOf<B>,
        message: B::Msg,
    ) -> Result<(), RejectedDelivery<B::Msg, Self::Error>> {
        crate::DeliveryEndpoint::deliver(&self.inner, from, message).await
    }
}

/// Affine retirement authority for one activated root.
pub struct RootRetirement<R, T> {
    handle: Handle<R, T>,
}

impl<R, T> RootRetirement<R, T> {
    /// Request hard cancellation of this exact incarnation.
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// Await the exact classified terminal outcome.
    pub async fn outcome(self) -> TaskOutcome<T> {
        self.handle.outcome().await
    }
}

impl<A, E, L, T> RootRetirement<ActorRef<A, MailboxSender<E>, L>, T>
where
    E: EventInput<ShutdownRequested>,
    L: IncarnationReporter,
{
    /// Publish one typed graceful-shutdown request.
    ///
    /// # Errors
    ///
    /// Returns the typed protocol-construction or closed-mailbox failure.
    pub fn request_shutdown(&self) -> Result<(), ShutdownRequestError> {
        self.handle.actor_ref().request_shutdown()
    }
}

/// Nameable retirement type produced for one behavior and system.
#[doc(hidden)]
pub type BehaviorRetirement<R, B, L = NoLifecycle> =
    RootRetirement<BehaviorRef<B, BehaviorReporter<R, B, L>>, RootOutcome<R, B, L>>;

/// Separate delivery and retirement seats returned by transactional activation.
pub struct RootActivation<B: Behavior<Ph = Never>, R> {
    /// Cloneable delivery-only capability.
    pub endpoint: RootEndpoint<B>,
    /// Affine graceful/forced retirement authority.
    pub retirement: R,
}

/// Nameable transactional activation result for one behavior and system.
#[doc(hidden)]
pub type BehaviorActivation<R, B, L = NoLifecycle> = RootActivation<B, BehaviorRetirement<R, B, L>>;

/// Result of constructing and spawning one behavior actor.
#[doc(hidden)]
pub type BehaviorSpawnResult<R, B, L = NoLifecycle> = Result<
    Handle<
        BehaviorRef<B, BehaviorReporter<R, B, L>>,
        ActorResult<B, BehaviorEnvironment<R, B, NoParent, L>>,
    >,
    <R as EndpointRegistry<B, AnchorEndpoint<B>>>::Error,
>;

/// Prepared-but-unlaunched ownership state for one behavior actor.
#[doc(hidden)]
type BehaviorPreparation<R, B, P, L> = PreparedIncarnation<
    B,
    BehaviorEnvironment<R, B, P, L>,
    BehaviorRegistration<R, B>,
    BehaviorRef<B, BehaviorReporter<R, B, L>>,
    ActorResult<B, BehaviorEnvironment<R, B, P, L>>,
    AddrOf<B>,
    BehaviorReporter<R, B, L>,
>;

/// Provisional, unregistered ownership state for one behavior actor.
#[doc(hidden)]
type BehaviorProvisional<R, B, P, L> = ProvisionalIncarnation<
    B,
    BehaviorEnvironment<R, B, P, L>,
    ActorResult<B, BehaviorEnvironment<R, B, P, L>>,
    AddrOf<B>,
    AnchorOf<B>,
    MailboxSender<EventOf<B>>,
>;

/// Failure of one transactional child birth.
///
/// The exact stage is preserved: registration collisions, initialization
/// fold failures, and initialization effect failures classify differently
/// for same-action rejection delivery.
#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
pub enum SystemBirthError<R, I, F> {
    /// The address generation could not be claimed at commit.
    #[error("child address registration failed")]
    Registration(#[source] R),
    /// The synchronous initialization fold failed.
    #[error("child initialization failed")]
    Initialization(#[source] I),
    /// An initialization effect failed during interpretation.
    #[error("child initialization effect failed")]
    Effects(#[source] F),
}

impl<R, I, F> From<RunError<I, F>> for SystemBirthError<R, I, F> {
    fn from(error: RunError<I, F>) -> Self {
        match error {
            RunError::Behavior(initialization) => Self::Initialization(initialization),
            RunError::Environment(effects) => Self::Effects(effects),
            RunError::Poisoned => unreachable!("init cannot poison the executor"),
        }
    }
}

impl<R, I, F> CreationFailure for SystemBirthError<R, I, F> {
    fn rejection(&self) -> behavior::CreationRejection {
        match self {
            Self::Registration(_) | Self::Effects(_) => {
                behavior::CreationRejection::EnvironmentFailed
            }
            Self::Initialization(_) => behavior::CreationRejection::InitializationFailed,
        }
    }
}

impl<R: Clone, L: Clone> System<R, L> {
    /// Initialize, register, and launch one exact root actor transactionally.
    ///
    /// # Errors
    ///
    /// Preserves initialization-fold, initialization-effect, and registration
    /// failures as separate variants. No endpoint is registered when either
    /// initialization stage fails.
    pub async fn activate<B>(
        &self,
        actor: Actor<B>,
    ) -> Result<
        BehaviorActivation<R, B, L>,
        SystemBirthError<
            <R as EndpointRegistry<B, AnchorEndpoint<B>>>::Error,
            B::Error,
            <BehaviorEnvironment<R, B, NoParent, L> as Environment>::Error,
        >,
    >
    where
        B: Behavior<Ph = Never> + Send + 'static,
        AddrOf<B>: Send + Sync + 'static,
        NonceOf<B>: Send + 'static,
        B::Sends: RouteSends<AddrOf<B>, EffectsOf<R, B, Self, NoParent>>
            + ObservesCreations<NonceOf<B>>
            + Send
            + 'static,
        <B::Sends as RouteSends<AddrOf<B>, EffectsOf<R, B, Self, NoParent>>>::Error:
            Send + Sync + 'static,
        B::Birth: RuntimeBirthMode<AddrOf<B>, Self, AnchorOf<B>> + 'static,
        ChildOf<B>: Send + 'static,
        RuntimeOf<B, Self>: Send + Sync + 'static,
        <RuntimeOf<B, Self> as ChildRuntime<AddrOf<B>, ChildOf<B>, AnchorOf<B>>>::Error:
            CreationFailure + Send + Sync + 'static,
        B::Event: RouteInput<TimerElapsed> + Send + 'static,
        B::Msg: Send + 'static,
        B::Error: Send + Sync + 'static,
        R: Clone + EndpointRegistry<B, AnchorEndpoint<B>> + Send + Sync + 'static,
        BehaviorRegistration<R, B>: Send + 'static,
        L: LifecycleFactory<AddrOf<B>, BehaviorRegistration<R, B>>,
    {
        let address = *actor.address();
        let mut provisional = self.prepare_provisional(actor, NoParent);
        let pending_exit = match provisional.driver().run_init().await {
            Ok(exit) => exit,
            Err(error) => {
                provisional.driver().retire().await;
                return Err(SystemBirthError::from(error));
            }
        };
        let prepared = provisional
            .commit(&self.router, &self.lifecycle)
            .map_err(SystemBirthError::Registration)?;
        let actor_ref = prepared.actor_ref().clone();
        let endpoint = RootEndpoint {
            inner: ActorRef::new(address, actor_ref.sender_anchor()),
        };
        let handle = prepared.launch(LaunchMode::Initialized(pending_exit), false);
        Ok(RootActivation {
            endpoint,
            retirement: RootRetirement { handle },
        })
    }

    /// Consume one inert actor definition and spawn its runtime incarnation.
    ///
    /// # Errors
    ///
    /// Returns endpoint-registration failure, including address collision.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime, or if a freshly allocated
    /// private Bombay Observe namespace cannot resolve the subject registered
    /// immediately beforehand.
    pub fn spawn<B>(&self, actor: Actor<B>) -> BehaviorSpawnResult<R, B, L>
    where
        B: Behavior<Ph = Never> + Send + 'static,
        AddrOf<B>: Send + Sync + 'static,
        NonceOf<B>: Send + 'static,
        B::Sends: RouteSends<AddrOf<B>, EffectsOf<R, B, Self, NoParent>>
            + ObservesCreations<NonceOf<B>>
            + Send
            + 'static,
        <B::Sends as RouteSends<AddrOf<B>, EffectsOf<R, B, Self, NoParent>>>::Error:
            Send + Sync + 'static,
        B::Birth: RuntimeBirthMode<AddrOf<B>, Self, AnchorOf<B>> + 'static,
        ChildOf<B>: Send + 'static,
        RuntimeOf<B, Self>: Send + Sync + 'static,
        <RuntimeOf<B, Self> as ChildRuntime<AddrOf<B>, ChildOf<B>, AnchorOf<B>>>::Error:
            CreationFailure + Send + Sync + 'static,
        B::Event: RouteInput<TimerElapsed> + Send + 'static,
        B::Msg: Send + 'static,
        B::Error: Send + Sync + 'static,
        R: Clone + EndpointRegistry<B, AnchorEndpoint<B>> + Send + Sync + 'static,
        BehaviorRegistration<R, B>: Send + 'static,
        L: LifecycleFactory<AddrOf<B>, BehaviorRegistration<R, B>>,
    {
        let prepared = self.prepare(actor, NoParent)?;
        Ok(prepared.launch(LaunchMode::Uninitialized, false))
    }

    #[allow(
        clippy::type_complexity,
        reason = "the private result retains the exact typed registration error"
    )]
    fn prepare<B, Parent>(
        &self,
        actor: Actor<B>,
        parent: Parent,
    ) -> Result<
        BehaviorPreparation<R, B, Parent, L>,
        <R as EndpointRegistry<B, AnchorEndpoint<B>>>::Error,
    >
    where
        B: Behavior<Ph = Never> + Send + 'static,
        AddrOf<B>: Send + Sync + 'static,
        NonceOf<B>: Send + 'static,
        B::Sends: RouteSends<AddrOf<B>, EffectsOf<R, B, Self, Parent>>
            + ObservesCreations<NonceOf<B>>
            + Send
            + 'static,
        <B::Sends as RouteSends<AddrOf<B>, EffectsOf<R, B, Self, Parent>>>::Error:
            Send + Sync + 'static,
        B::Birth: RuntimeBirthMode<AddrOf<B>, Self, AnchorOf<B>> + 'static,
        ChildOf<B>: Send + 'static,
        RuntimeOf<B, Self>: Send + Sync + 'static,
        <RuntimeOf<B, Self> as ChildRuntime<AddrOf<B>, ChildOf<B>, AnchorOf<B>>>::Error:
            CreationFailure + Send + Sync + 'static,
        B::Event: RouteInput<TimerElapsed> + Send + 'static,
        B::Msg: Send + 'static,
        B::Error: Send + Sync + 'static,
        Parent: Send + 'static,
        R: Clone + EndpointRegistry<B, AnchorEndpoint<B>> + Send + Sync + 'static,
        BehaviorRegistration<R, B>: Send + 'static,
        L: LifecycleFactory<AddrOf<B>, BehaviorRegistration<R, B>>,
    {
        self.prepare_provisional(actor, parent)
            .commit(&self.router, &self.lifecycle)
    }

    /// Prepare every actor resource without claiming the address generation.
    fn prepare_provisional<B, Parent>(
        &self,
        actor: Actor<B>,
        parent: Parent,
    ) -> BehaviorProvisional<R, B, Parent, L>
    where
        B: Behavior<Ph = Never> + Send + 'static,
        AddrOf<B>: Send + Sync + 'static,
        NonceOf<B>: Send + 'static,
        B::Sends: RouteSends<AddrOf<B>, EffectsOf<R, B, Self, Parent>>
            + ObservesCreations<NonceOf<B>>
            + Send
            + 'static,
        <B::Sends as RouteSends<AddrOf<B>, EffectsOf<R, B, Self, Parent>>>::Error:
            Send + Sync + 'static,
        B::Birth: RuntimeBirthMode<AddrOf<B>, Self, AnchorOf<B>> + 'static,
        ChildOf<B>: Send + 'static,
        RuntimeOf<B, Self>: Send + Sync + 'static,
        <RuntimeOf<B, Self> as ChildRuntime<AddrOf<B>, ChildOf<B>, AnchorOf<B>>>::Error:
            CreationFailure + Send + Sync + 'static,
        B::Event: RouteInput<TimerElapsed> + Send + 'static,
        B::Msg: Send + 'static,
        B::Error: Send + Sync + 'static,
        Parent: Send + 'static,
        R: Clone + EndpointRegistry<B, AnchorEndpoint<B>> + Send + Sync + 'static,
        BehaviorRegistration<R, B>: Send + 'static,
        L: LifecycleFactory<AddrOf<B>, BehaviorRegistration<R, B>>,
    {
        let (address, definition) = actor.into_parts();
        let observation_space = ObservationSpace::new();
        let subject = observation_space
            .subject(())
            .expect("a fresh completion namespace must be vacant");
        let observation = observation_space
            .observe(&())
            .expect("a newly registered completion subject must resolve");
        let peer_space = ObservationSpace::new();
        let peer_subject = peer_space
            .subject(())
            .expect("a fresh peer-completion namespace must be vacant");
        let (sender, source) = self.mailbox.create::<B::Event>();
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let response = sender.anchor();
        let endpoint =
            IncarnationEndpoint::new(ActorRef::new(address, response.clone()), peer_space);
        let children =
            <B::Birth as RuntimeBirthMode<AddrOf<B>, Self, AnchorOf<B>>>::runtime(self.clone());
        let environment = ActorEnvironment::new(
            address,
            source,
            self.router.clone(),
            children,
            response,
            parent,
        );
        ProvisionalIncarnation::new(
            Driver::from_definition(definition, environment),
            address,
            endpoint,
            sender,
            subject,
            observation,
            peer_subject,
            cancellation_requested,
        )
    }
}

impl<R, L, B, ParentSink> ChildRuntime<AddrOf<B>, B, ParentSink> for SystemChildren<System<R, L>>
where
    B: Behavior<Ph = Never> + Send + 'static,
    AddrOf<B>: Send + Sync + 'static,
    NonceOf<B>: Send + 'static,
    B::Sends: RouteSends<AddrOf<B>, EffectsOf<R, B, System<R, L>, ParentReporter<AddrOf<B>, ParentSink>>>
        + ObservesCreations<NonceOf<B>>
        + Send
        + 'static,
    <B::Sends as RouteSends<
        AddrOf<B>,
        EffectsOf<R, B, System<R, L>, ParentReporter<AddrOf<B>, ParentSink>>,
    >>::Error: Send + Sync + 'static,
    B::Birth: RuntimeBirthMode<AddrOf<B>, System<R, L>, AnchorOf<B>> + 'static,
    ChildOf<B>: Send + 'static,
    RuntimeOf<B, System<R, L>>: Send + Sync + 'static,
    <RuntimeOf<B, System<R, L>> as ChildRuntime<AddrOf<B>, ChildOf<B>, AnchorOf<B>>>::Error:
        CreationFailure + Send + Sync + 'static,
    B::Event: RouteInput<ShutdownRequested> + RouteInput<TimerElapsed> + Send + 'static,
    B::Msg: Send + 'static,
    B::Error: Send + Sync + 'static,
    R: Clone + EndpointRegistry<B, AnchorEndpoint<B>> + Send + Sync + 'static,
    BehaviorRegistration<R, B>: Send + 'static,
    L: Clone + LifecycleFactory<AddrOf<B>, BehaviorRegistration<R, B>> + Send + Sync + 'static,
    ParentSink: EventSender + Clone + Send + Sync + 'static,
    ParentSink::Event: Send + 'static,
{
    type Lease = ChildLease<
        BehaviorRef<B, BehaviorReporter<R, B, L>>,
        ActorResult<B, BehaviorEnvironment<R, B, ParentReporter<AddrOf<B>, ParentSink>, L>>,
    >;
    type Error = SystemBirthError<
        <R as EndpointRegistry<B, AnchorEndpoint<B>>>::Error,
        B::Error,
        <BehaviorEnvironment<R, B, ParentReporter<AddrOf<B>, ParentSink>, L> as Environment>::Error,
    >;

    async fn birth(
        &self,
        parent: AddrOf<B>,
        child: behavior::Create<AddrOf<B>, B>,
        response: ParentSink,
    ) -> Result<Self::Lease, Self::Error> {
        let address = parent.birth(child.nonce);
        let kind = child.kind;
        let reporter = ParentReporter::new(child.nonce, response);
        let mut provisional = self
            .system
            .prepare_provisional(Actor::new(address, child.child), reporter);
        let pending_exit = match provisional.driver().run_init().await {
            Ok(exit) => exit,
            Err(error) => {
                provisional.driver().retire().await;
                return Err(SystemBirthError::from(error));
            }
        };
        let prepared = provisional
            .commit(&self.system.router, &self.system.lifecycle)
            .map_err(SystemBirthError::Registration)?;
        let restarted = matches!(kind, behavior::CreationKind::ReplacementIncarnation { .. });
        let handle = prepared.launch(LaunchMode::Initialized(pending_exit), restarted);
        Ok(handle.into_child_lease())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use crate::{
        Actor, ActorRef, AddressRouter, EndpointRegistry, IncarnationEndpoint, MailboxConfig,
        RunExit, System, TaskOutcome,
    };
    use behavior::{
        Actions, Address, Behavior, Births, Create, Delivery, MailAddr, Never, NoBirths, Recipient,
        ServiceSends, UnwatchPeer, User, WatchEvent,
    };

    struct StopWithMessage;

    struct CancelUnknownPeer;

    impl Behavior for CancelUnknownPeer {
        type Addr = MailAddr;
        type Msg = u8;
        type Event = WatchEvent<User<MailAddr, u8>>;
        type Sends = ServiceSends<UnwatchPeer<MailAddr>>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn init(&mut self, _: behavior::InitializationTurn) -> behavior::BehaviorActed<Self> {
            Ok(Actions::new(
                ServiceSends::one(UnwatchPeer::new(MailAddr(8))),
                Vec::new(),
                behavior::Step::Stop(behavior::Exit::Normal),
            ))
        }

        fn transition(
            &mut self,
            _: behavior::ActiveTurn,
            _event: Self::Event,
        ) -> behavior::BehaviorActed<Self> {
            unreachable!("the initialization fold stops")
        }
    }

    struct ForwardTo(MailAddr);

    #[behavior::behavior(addr = MailAddr, message = u8, sends = Vec<Never>, births = NoBirths, error = Never)]
    impl StopWithMessage {
        fn receive(
            &mut self,
            _from: MailAddr,
            _message: u8,
        ) -> behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
            Ok(Actions::stop(behavior::Exit::Normal))
        }
    }

    #[test]
    fn actor_definition_round_trips_only_address_and_behavior() {
        let actor = Actor::new(MailAddr(9), StopWithMessage);

        assert_eq!(*actor.address(), MailAddr(9));
        let _ = actor.behavior();
        let (address, behavior) = actor.into_parts();
        assert_eq!(address, MailAddr(9));
        let _ = behavior;
    }

    #[behavior::behavior(addr = MailAddr, message = u8, sends = Vec<Delivery<StopAfterReceiving>>, births = NoBirths, error = Never)]
    impl ForwardTo {
        fn receive(
            &mut self,
            _from: MailAddr,
            message: u8,
        ) -> behavior::Acted<MailAddr, Never, Vec<Delivery<StopAfterReceiving>>, NoBirths, Never>
        {
            Ok(Actions {
                sends: vec![Delivery::new(Recipient::global(self.0), message + 1)],
                creates: Vec::new(),
                become_: behavior::Step::Stop(behavior::Exit::Normal),
            })
        }
    }

    struct StopAfterReceiving;

    struct BirthAndSend {
        receiver: MailAddr,
    }

    struct ForwardChild {
        receiver: MailAddr,
    }

    type ChildBehavior = ForwardChild;

    #[behavior::behavior(addr = MailAddr, message = u8, sends = Vec<Delivery<ChildBehavior>>, births = Births<ChildBehavior>, error = Never)]
    impl BirthAndSend {
        fn receive(
            &mut self,
            _from: MailAddr,
            message: u8,
        ) -> behavior::Acted<
            MailAddr,
            Never,
            Vec<Delivery<ChildBehavior>>,
            Births<ChildBehavior>,
            Never,
        > {
            Ok(Actions {
                sends: vec![Delivery::new(
                    Recipient::global(MailAddr(1).birth(7)),
                    message,
                )],
                creates: vec![Create::birth(
                    7,
                    ForwardChild {
                        receiver: self.receiver,
                    },
                )],
                become_: behavior::Step::Stop(behavior::Exit::Normal),
            })
        }
    }

    #[behavior::behavior(addr = MailAddr, message = u8, sends = Vec<Delivery<StopAfterReceiving>>, births = NoBirths, error = Never)]
    impl ForwardChild {
        fn receive(
            &mut self,
            _from: MailAddr,
            message: u8,
        ) -> behavior::Acted<MailAddr, Never, Vec<Delivery<StopAfterReceiving>>, NoBirths, Never>
        {
            Ok(Actions {
                sends: vec![Delivery::new(Recipient::global(self.receiver), message + 1)],
                creates: Vec::new(),
                become_: behavior::Step::Stop(behavior::Exit::Normal),
            })
        }
    }

    #[behavior::behavior(addr = MailAddr, message = u8, sends = Vec<Never>, births = NoBirths, error = Never)]
    impl StopAfterReceiving {
        fn receive(
            &mut self,
            _from: MailAddr,
            _message: u8,
        ) -> behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
            Ok(Actions::stop(behavior::Exit::Normal))
        }
    }

    #[derive(Clone, Copy)]
    struct NoRouter;

    impl<B, S> EndpointRegistry<B, IncarnationEndpoint<MailAddr, ActorRef<MailAddr, S>>> for NoRouter
    where
        B: Behavior<Addr = MailAddr, Msg = u8>,
    {
        type Error = Infallible;
        type Registration = ();

        fn register(
            &self,
            _address: MailAddr,
            _endpoint: IncarnationEndpoint<MailAddr, ActorRef<MailAddr, S>>,
        ) -> Result<Self::Registration, Self::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn configured_system_constructs_mailbox_and_returns_actor_reference() {
        let system = System::new(MailboxConfig::bounded(4), NoRouter);

        let spawned = system
            .spawn(Actor::new(MailAddr(9), StopWithMessage))
            .unwrap();
        spawned.actor_ref().send(MailAddr(7), 1).await.unwrap();

        assert_eq!(
            spawned.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(behavior::Exit::Normal)))
        );
    }

    #[tokio::test]
    async fn unwatch_service_composes_through_system_without_a_creation_lane() {
        let system = System::new(MailboxConfig::bounded(1), NoRouter);
        let spawned = system
            .spawn(Actor::new(MailAddr(9), CancelUnknownPeer))
            .unwrap();

        assert_eq!(
            spawned.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(behavior::Exit::Normal)))
        );
    }

    #[tokio::test]
    async fn address_registration_lives_exactly_as_long_as_actor_task() {
        let router = AddressRouter::default();
        let system = System::new(MailboxConfig::bounded(4), router);

        let first = system
            .spawn(Actor::new(MailAddr(9), ForwardTo(MailAddr(88))))
            .unwrap();
        assert!(
            system
                .spawn(Actor::new(MailAddr(9), ForwardTo(MailAddr(88))))
                .is_err(),
            "a running actor owns its address generation"
        );

        first.actor_ref().send(MailAddr(7), 1).await.unwrap();
        assert!(matches!(
            first.outcome().await,
            TaskOutcome::Returned(Err(_))
        ));

        let replacement = system
            .spawn(Actor::new(MailAddr(9), ForwardTo(MailAddr(88))))
            .expect("task exit releases the old address generation");
        replacement.actor_ref().send(MailAddr(7), 1).await.unwrap();
        assert!(matches!(
            replacement.outcome().await,
            TaskOutcome::Returned(Err(_))
        ));
    }

    #[tokio::test]
    async fn two_actors_exchange_a_typed_message_through_shared_routing() {
        let router = AddressRouter::default();
        let system = System::new(MailboxConfig::bounded(4), router);
        let receiver = system
            .spawn(Actor::new(MailAddr(2), StopAfterReceiving))
            .unwrap();
        let sender = system
            .spawn(Actor::new(MailAddr(1), ForwardTo(MailAddr(2))))
            .unwrap();

        sender.actor_ref().send(MailAddr(0), 40).await.unwrap();

        assert!(matches!(
            sender.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(behavior::Exit::Normal)))
        ));
        assert!(matches!(
            receiver.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(behavior::Exit::Normal)))
        ));
    }

    #[tokio::test]
    async fn mailbox_anchor_routes_without_pinning_mailbox_open() {
        let router = AddressRouter::default();
        let system = System::new(MailboxConfig::bounded(4), router);
        let receiver = system
            .spawn(Actor::new(MailAddr(2), StopAfterReceiving))
            .unwrap();
        let sender = system
            .spawn(Actor::new(MailAddr(1), ForwardTo(MailAddr(2))))
            .unwrap();

        sender.actor_ref().send(MailAddr(0), 40).await.unwrap();
        assert!(matches!(
            sender.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(behavior::Exit::Normal)))
        ));
        assert!(matches!(
            receiver.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(behavior::Exit::Normal)))
        ));

        let parked = system
            .spawn(Actor::new(MailAddr(3), StopAfterReceiving))
            .unwrap();
        assert!(matches!(
            parked.close().await,
            TaskOutcome::Returned(Ok(RunExit::EnvironmentClosed))
        ));
        assert!(
            system
                .spawn(Actor::new(MailAddr(3), StopAfterReceiving))
                .is_ok(),
            "environment closure releases the address generation"
        );
    }

    #[tokio::test]
    async fn typed_creator_spawns_child_before_routing_same_transition_send() {
        let router = AddressRouter::default();
        let system = System::new(MailboxConfig::bounded(4), router);
        let receiver = system
            .spawn(Actor::new(MailAddr(9), StopAfterReceiving))
            .unwrap();
        let parent = system
            .spawn(Actor::new(
                MailAddr(1),
                BirthAndSend {
                    receiver: MailAddr(9),
                },
            ))
            .unwrap();

        parent.actor_ref().send(MailAddr(0), 40).await.unwrap();

        assert!(matches!(
            parent.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(behavior::Exit::Normal)))
        ));
        assert!(matches!(
            receiver.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(behavior::Exit::Normal)))
        ));
    }
}
