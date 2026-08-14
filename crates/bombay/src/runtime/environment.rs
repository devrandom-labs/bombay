//! Behavior effect interpretation for one actor.

use behavior::{Address, Behavior, BirthMode, TimeEvent};
use bombay_engine::{Environment, RuntimeEffects};

use crate::{ChildRuntime, EventSource, ObservesCreations, RouteSends, RuntimeEffectError};

use super::{CreationFailure, IncarnationEffects};

/// Effect capabilities owned by one generation of behavior `B`.
type EffectsFor<B, R, Lease, Sink, Parent> = IncarnationEffects<
    R,
    <<B as Behavior>::Addr as Address>::Nonce,
    Lease,
    Sink,
    Parent,
    <B as Behavior>::Addr,
>;

/// Connects an actor-local event source to shared delivery routing.
///
/// The behavior is the single authority for the address, send-algebra, and
/// birth-mode facts this environment interprets; none of them is an
/// independent choice.
#[doc(hidden)]
pub struct ActorEnvironment<B, Source, R, Children, Sink, Parent>
where
    B: Behavior,
    B::Addr: Address,
    Source: EventSource,
    Children: ChildRuntime<B::Addr, <B::Birth as BirthMode>::Child, Sink>,
{
    address: B::Addr,
    source: Source,
    effects: EffectsFor<B, R, Children::Lease, Sink, Parent>,
    child_runtime: Children,
}

impl<B, Source, R, Children, Sink, Parent> ActorEnvironment<B, Source, R, Children, Sink, Parent>
where
    B: Behavior,
    B::Addr: Address,
    Source: EventSource,
    Children: ChildRuntime<B::Addr, <B::Birth as BirthMode>::Child, Sink>,
{
    /// Construct an actor environment with a handle to shared routing.
    pub(crate) fn new(
        address: B::Addr,
        source: Source,
        router: R,
        child_runtime: Children,
        response: Sink,
        parent: Parent,
    ) -> Self {
        Self {
            address,
            source,
            effects: IncarnationEffects::new(router, response, parent),
            child_runtime,
        }
    }
}

impl<B, Source, R, Children, Sink, Parent> Environment
    for ActorEnvironment<B, Source, R, Children, Sink, Parent>
where
    B: Behavior,
    B::Addr: Address + Send,
    <B::Addr as Address>::Nonce: Send,
    B::Sends: RouteSends<B::Addr, EffectsFor<B, R, Children::Lease, Sink, Parent>>
        + ObservesCreations<<B::Addr as Address>::Nonce>
        + Send,
    <B::Birth as BirthMode>::Child: Send,
    B::Event: TimeEvent + Send,
    Source: EventSource<Event = B::Event> + Send,
    R: Send + Sync,
    Children: ChildRuntime<B::Addr, <B::Birth as BirthMode>::Child, Sink> + Send + Sync,
    Children::Error: CreationFailure,
    Sink: Clone + Send,
    Parent: Send,
{
    type Event = B::Event;
    type Effect = RuntimeEffects<B::Addr, B::Sends, B::Birth>;
    type Error = RuntimeEffectError<
        Children::Error,
        <B::Sends as RouteSends<B::Addr, EffectsFor<B, R, Children::Lease, Sink, Parent>>>::Error,
    >;

    async fn next(&mut self) -> Option<Self::Event> {
        loop {
            tokio::select! {
                event = self.source.next() => return event,
                reached = self.effects.next_timer() => {
                    if let Some(event) = B::Event::time_reached(reached) {
                        return Some(event);
                    }
                }
            }
        }
    }

    async fn interpret(&mut self, effect: Self::Effect) -> Result<(), Self::Error> {
        self.effects.begin_creation_resolution();
        for creation in effect.creates {
            let nonce = creation.nonce;
            let kind = creation.kind;
            let observed = effect.sends.observes_creation(nonce);
            if !self.effects.child_nonce_is_fresh(nonce) {
                if observed {
                    self.effects
                        .record_creation(behavior::CreationResolved::rejected(
                            nonce,
                            kind,
                            behavior::CreationRejection::NonceAlreadyBound,
                        ));
                    continue;
                }
                return Err(RuntimeEffectError::DuplicateChild);
            }
            self.effects.reserve_child(nonce);
            match self
                .child_runtime
                .birth(self.address, creation, self.effects.response())
                .await
            {
                Ok(capability) => {
                    self.effects.install_child(nonce, capability);
                    self.effects
                        .record_creation(behavior::CreationResolved::installed(nonce, kind));
                }
                Err(error) => {
                    self.effects.cancel_child_reservation(nonce);
                    if observed {
                        self.effects
                            .record_creation(behavior::CreationResolved::rejected(
                                nonce,
                                kind,
                                error.rejection(),
                            ));
                        continue;
                    }
                    return Err(RuntimeEffectError::Birth(error));
                }
            }
        }
        effect
            .sends
            .route(self.address, &mut self.effects)
            .await
            .map_err(RuntimeEffectError::Delivery)
    }

    async fn retire(&mut self) {
        self.effects.retire_children().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use behavior::{
        Actions, Behavior, Create, Delivery, Handler, MailAddr, NoBirths, Pure, Recipient, User,
    };
    use bombay_engine::{Environment, RunExit, RuntimeEffects};

    use super::ActorEnvironment;
    use crate::runtime::NoChildren;
    use crate::{ChildRuntime, DeliveryRouter, EventSender, EventSource, RuntimeEffectError};

    struct Echo;
    struct Quiet;

    impl Handler for Quiet {
        type Addr = MailAddr;
        type Msg = u8;

        fn receive(
            &mut self,
            _from: MailAddr,
            _message: u8,
        ) -> behavior::Acted<
            MailAddr,
            behavior::Never,
            Vec<behavior::Never>,
            NoBirths,
            behavior::Never,
        > {
            Ok(Actions::cont())
        }
    }

    type Sink = Pure<Quiet>;
    type EchoSends = Vec<Delivery<Sink>>;
    type EchoBehavior = Pure<Echo, EchoSends>;

    /// Behavior with a string child birth mode for interpretation tests.
    struct StrBirths;

    impl Behavior for StrBirths {
        type Addr = MailAddr;
        type Msg = u8;
        type Event = User<MailAddr, u8>;
        type Sends = EchoSends;
        type Ph = behavior::Never;
        type Error = Infallible;
        type Birth = behavior::Births<&'static str>;

        fn init(&mut self) -> behavior::BehaviorActed<Self> {
            unreachable!("interpret tests never run the behavior")
        }

        fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
            unreachable!("interpret tests never run the behavior")
        }
    }

    struct QueueSender<E>(Arc<Mutex<VecDeque<E>>>);
    struct QueueSource<E>(Arc<Mutex<VecDeque<E>>>);

    fn queue<E>() -> (QueueSender<E>, QueueSource<E>) {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        (QueueSender(queue.clone()), QueueSource(queue))
    }

    impl<E: Send> EventSender for QueueSender<E> {
        type Event = E;
        type Error = Infallible;

        async fn send(&self, event: E) -> Result<(), Self::Error> {
            self.0.lock().expect("queue lock").push_back(event);
            Ok(())
        }
    }

    impl<E: Send> EventSource for QueueSource<E> {
        type Event = E;

        async fn next(&mut self) -> Option<Self::Event> {
            self.0.lock().expect("queue lock").pop_front()
        }
    }

    impl Handler<EchoSends> for Echo {
        type Addr = MailAddr;
        type Msg = u8;

        fn receive(
            &mut self,
            from: MailAddr,
            message: u8,
        ) -> behavior::Acted<MailAddr, behavior::Never, EchoSends, NoBirths, behavior::Never>
        {
            Ok(Actions {
                sends: vec![Delivery::new(Recipient::global(from), message + 1)],
                creates: Vec::new(),
                become_: behavior::Step::Continue,
            })
        }
    }

    type RecordedDeliveries = Arc<Mutex<Vec<(MailAddr, Delivery<Sink>)>>>;

    #[derive(Clone, Default)]
    struct SharedRouter(RecordedDeliveries);

    #[derive(Clone)]
    struct RecordingChildren(Arc<Mutex<Vec<&'static str>>>);

    struct FailingChildren;
    struct TestLease;

    impl crate::runtime::CreationFailure for Infallible {
        fn rejection(&self) -> behavior::CreationRejection {
            match *self {}
        }
    }

    impl crate::runtime::CreationFailure for &'static str {
        fn rejection(&self) -> behavior::CreationRejection {
            behavior::CreationRejection::EnvironmentFailed
        }
    }

    #[derive(Clone)]
    struct FailingRouter;

    impl crate::runtime::SealedChildRuntime for RecordingChildren {}
    impl crate::runtime::SealedChildRuntime for FailingChildren {}

    impl crate::runtime::CoordinatedChild for TestLease {
        fn request_shutdown(&self) {}

        async fn retire(self) {}
    }

    impl<S: Send> ChildRuntime<MailAddr, &'static str, S> for RecordingChildren {
        type Lease = TestLease;
        type Error = Infallible;

        async fn birth(
            &self,
            _parent: MailAddr,
            child: Create<MailAddr, &'static str>,
            _response: S,
        ) -> Result<Self::Lease, Self::Error> {
            self.0.lock().expect("order lock").push(child.child);
            Ok(TestLease)
        }
    }

    impl<S: Send> ChildRuntime<MailAddr, &'static str, S> for FailingChildren {
        type Lease = TestLease;
        type Error = &'static str;

        async fn birth(
            &self,
            _parent: MailAddr,
            _child: Create<MailAddr, &'static str>,
            _response: S,
        ) -> Result<Self::Lease, Self::Error> {
            Err("birth failed")
        }
    }

    #[derive(Clone)]
    struct OrderingRouter(Arc<Mutex<Vec<&'static str>>>);

    impl DeliveryRouter<Sink> for OrderingRouter {
        type Error = Infallible;

        async fn deliver(
            &self,
            _from: MailAddr,
            _delivery: Delivery<Sink>,
        ) -> Result<(), Self::Error> {
            self.0.lock().expect("order lock").push("send");
            Ok(())
        }
    }

    impl DeliveryRouter<Sink> for SharedRouter {
        type Error = Infallible;

        async fn deliver(
            &self,
            from: MailAddr,
            delivery: Delivery<Sink>,
        ) -> Result<(), Self::Error> {
            self.0.lock().expect("delivery lock").push((from, delivery));
            Ok(())
        }
    }

    impl DeliveryRouter<Sink> for FailingRouter {
        type Error = &'static str;

        async fn deliver(
            &self,
            _from: MailAddr,
            _delivery: Delivery<Sink>,
        ) -> Result<(), Self::Error> {
            Err("delivery failed")
        }
    }

    #[tokio::test]
    async fn actor_retains_a_handle_to_shared_delivery_routing() {
        let (sender, source) = queue();
        let router = SharedRouter::default();
        let behavior = Pure::new(Echo);
        let environment = ActorEnvironment::<EchoBehavior, _, _, _, _, _>::new(
            MailAddr(7),
            source,
            router.clone(),
            NoChildren::new(),
            (),
            (),
        );
        sender
            .send(User {
                from: MailAddr(3),
                message: 4,
            })
            .await
            .unwrap();

        let mut driver = bombay_engine::Driver::new(behavior, environment);
        assert_eq!(driver.run().await, Ok(RunExit::EnvironmentClosed));
        let deliveries = router.0.lock().expect("delivery lock");
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].0, MailAddr(7));
        assert_eq!(deliveries[0].1.to.resolve(MailAddr(7)), MailAddr(3));
        assert_eq!(deliveries[0].1.message, 5);
    }

    #[tokio::test]
    async fn interprets_all_creates_before_any_send() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let source = QueueSource(Arc::new(Mutex::new(VecDeque::<User<MailAddr, u8>>::new())));
        let mut environment = ActorEnvironment::<StrBirths, _, _, _, _, _>::new(
            MailAddr(7),
            source,
            OrderingRouter(order.clone()),
            RecordingChildren(order.clone()),
            (),
            (),
        );
        let effect: RuntimeEffects<_, _, behavior::Births<&'static str>> = RuntimeEffects {
            sends: vec![Delivery::new(Recipient::global(MailAddr(9)), 1)],
            creates: vec![Create::birth(1, "first"), Create::birth(2, "second")],
        };

        Environment::interpret(&mut environment, effect)
            .await
            .unwrap();

        assert_eq!(
            *order.lock().expect("order lock"),
            ["first", "second", "send"]
        );
    }

    #[tokio::test]
    async fn failed_birth_is_reported_before_any_delivery() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let source = QueueSource(Arc::new(Mutex::new(VecDeque::<User<MailAddr, u8>>::new())));
        let mut environment = ActorEnvironment::<StrBirths, _, _, _, _, _>::new(
            MailAddr(7),
            source,
            OrderingRouter(order.clone()),
            FailingChildren,
            (),
            (),
        );
        let effect: RuntimeEffects<_, _, behavior::Births<&'static str>> = RuntimeEffects {
            sends: vec![Delivery::new(Recipient::global(MailAddr(9)), 1)],
            creates: vec![Create::birth(1, "child")],
        };

        assert_eq!(
            Environment::interpret(&mut environment, effect).await,
            Err(RuntimeEffectError::Birth("birth failed"))
        );
        assert!(order.lock().expect("order lock").is_empty());
    }

    #[tokio::test]
    async fn duplicate_child_nonce_is_rejected_before_a_second_birth() {
        let births = Arc::new(Mutex::new(Vec::new()));
        let source = QueueSource(Arc::new(Mutex::new(VecDeque::<User<MailAddr, u8>>::new())));
        let mut environment = ActorEnvironment::<StrBirths, _, _, _, _, _>::new(
            MailAddr(7),
            source,
            OrderingRouter(Arc::new(Mutex::new(Vec::new()))),
            RecordingChildren(births.clone()),
            (),
            (),
        );
        let effect: RuntimeEffects<_, EchoSends, behavior::Births<&'static str>> = RuntimeEffects {
            sends: Vec::new(),
            creates: vec![Create::birth(1, "first"), Create::birth(1, "duplicate")],
        };

        assert_eq!(
            Environment::interpret(&mut environment, effect).await,
            Err(RuntimeEffectError::DuplicateChild)
        );
        assert_eq!(*births.lock().expect("birth record lock"), ["first"]);
    }

    #[tokio::test]
    async fn delivery_failure_is_distinct_from_child_birth_failure() {
        let source = QueueSource(Arc::new(Mutex::new(VecDeque::<User<MailAddr, u8>>::new())));
        let mut environment = ActorEnvironment::<EchoBehavior, _, _, _, _, _>::new(
            MailAddr(7),
            source,
            FailingRouter,
            NoChildren::new(),
            (),
            (),
        );
        let effect: RuntimeEffects<_, _, NoBirths> = RuntimeEffects {
            sends: vec![Delivery::new(Recipient::global(MailAddr(9)), 1)],
            creates: Vec::new(),
        };

        assert_eq!(
            Environment::interpret(&mut environment, effect).await,
            Err(RuntimeEffectError::Delivery("delivery failed"))
        );
    }
}
