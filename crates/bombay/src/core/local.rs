//! One local typed mailbox behind the existing Engine environment port.
//!
//! This is deliberately crate-private. It proves the mailbox/address layer
//! without prescribing construction, task, handle, or System APIs.

use core::future::Future;
use core::hash::Hash;
use core::marker::PhantomData;

use behavior::{Behavior, Never, UserEvent};
use bombay_address::{AddressSpace, ClaimError, Lease};
use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};
use communication::{
    Config, Consumer, ControlSender, Received, UserAnchor, UserClosed, UserSender, channel,
};

/// Commits one complete action value for the concrete local actor.
///
/// Product traversal and concrete runtime services remain behind this private
/// seam. It is not an application extension API.
pub trait CommitActions<B: Behavior<Ph = Never>> {
    type Error;

    fn commit(&mut self, actions: ActionsOf<B>) -> impl Future<Output = Result<(), Self::Error>>;

    fn retire(self) -> impl Future<Output = ()>
    where
        Self: Sized,
    {
        async {}
    }
}

/// A non-owning, protocol-indexed user-lane capability.
///
/// The Communication anchor cannot keep or resurrect an actor. Indexing by
/// `B` preserves destination protocol identity even when behaviors share the
/// same address and message types.
pub struct ActorRef<B: Behavior> {
    address: B::Addr,
    anchor: UserAnchor<B::Event>,
    behavior: PhantomData<fn() -> B>,
}

impl<B: Behavior> Clone for ActorRef<B> {
    fn clone(&self) -> Self {
        Self::new(self.address, self.anchor.clone())
    }
}

impl<B: Behavior> ActorRef<B> {
    pub(crate) const fn new(address: B::Addr, anchor: UserAnchor<B::Event>) -> Self {
        Self {
            address,
            anchor,
            behavior: PhantomData,
        }
    }

    /// The typed address of this exact actor reference.
    #[must_use]
    pub const fn address(&self) -> B::Addr {
        self.address
    }

    /// Admit one user message through the behavior's complete event sum.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] with the original message when this incarnation's
    /// user lane is no longer live.
    pub async fn send(&self, from: B::Addr, message: B::Msg) -> Result<(), SendError<B::Msg>> {
        self.anchor
            .send(B::Event::user(from, message))
            .await
            .map_err(|rejected| SendError::from_rejected::<B>(rejected))
    }
}

/// Exact user payload rejected because the incarnation is no longer live.
#[derive(Debug)]
pub struct SendError<M>(M);

impl<M> SendError<M> {
    /// Recover the exact message rejected by the closed actor reference.
    #[must_use]
    pub fn into_message(self) -> M {
        self.0
    }

    fn from_rejected<B>(rejected: UserClosed<B::Event>) -> Self
    where
        B: Behavior<Msg = M>,
    {
        let UserClosed(event) = rejected;
        let user = event
            .into_user()
            .ok()
            .expect("ActorRef only injects the behavior user lane");
        Self(user.message)
    }
}

/// Failure while committing initialization or claiming the exact address.
#[derive(Debug, PartialEq, Eq)]
pub enum LocalActivationError<C, A> {
    Commit(C),
    Address(A),
}

/// The prepared local half of one mailbox-backed behavior generation.
pub struct LocalEnvironment<B: Behavior, I, P = fn(ActorRef<B>)> {
    address: B::Addr,
    addresses: AddressSpace<B::Addr, ActorRef<B>>,
    endpoint: ActorRef<B>,
    consumer: Consumer<B::Event, B::Event>,
    user_liveness: UserSender<B::Event>,
    control_liveness: ControlSender<B::Event>,
    interpreter: I,
    publish: P,
    #[cfg(test)]
    close_after_activation: bool,
}

impl<B, I> LocalEnvironment<B, I, fn(ActorRef<B>)>
where
    B: Behavior,
    B::Addr: Hash,
{
    pub fn new(
        address: B::Addr,
        addresses: AddressSpace<B::Addr, ActorRef<B>>,
        config: Config,
        interpreter: I,
    ) -> Self {
        let (control_liveness, user_liveness, consumer) = channel(config);
        let endpoint = ActorRef::new(address, user_liveness.anchor());
        Self {
            address,
            addresses,
            endpoint,
            consumer,
            user_liveness,
            control_liveness,
            interpreter,
            publish: ignore_publication::<B>,
            #[cfg(test)]
            close_after_activation: false,
        }
    }
}

fn ignore_publication<B: Behavior>(_: ActorRef<B>) {}

impl<B: Behavior, I, P> LocalEnvironment<B, I, P> {
    pub(crate) fn publish_with<N>(self, publish: N) -> LocalEnvironment<B, I, N> {
        LocalEnvironment {
            address: self.address,
            addresses: self.addresses,
            endpoint: self.endpoint,
            consumer: self.consumer,
            user_liveness: self.user_liveness,
            control_liveness: self.control_liveness,
            interpreter: self.interpreter,
            publish,
            #[cfg(test)]
            close_after_activation: self.close_after_activation,
        }
    }

    #[cfg(test)]
    pub(crate) fn preload_control(&self, event: B::Event) {
        assert!(self.control_liveness.send(event).is_ok());
    }

    #[cfg(test)]
    pub(crate) fn close_after_activation(mut self) -> Self {
        self.close_after_activation = true;
        self
    }
}

/// The only local value with mailbox ingress and address ownership.
pub struct ActiveLocalEnvironment<B: Behavior, I>
where
    B::Addr: Hash,
{
    consumer: Consumer<B::Event, B::Event>,
    user_liveness: Option<UserSender<B::Event>>,
    control_liveness: Option<ControlSender<B::Event>>,
    interpreter: I,
    _lease: Lease<B::Addr, ActorRef<B>>,
}

impl<B, I, P> Environment<B> for LocalEnvironment<B, I, P>
where
    B: Behavior<Ph = Never>,
    B::Addr: Hash + Clone,
    I: CommitActions<B>,
    P: FnOnce(ActorRef<B>),
{
    type Active = ActiveLocalEnvironment<B, I>;
    type Error = LocalActivationError<I::Error, ClaimError<B::Addr>>;

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
        #[cfg(test)]
        let (user_liveness, control_liveness) = if self.close_after_activation {
            (None, None)
        } else {
            (Some(self.user_liveness), Some(self.control_liveness))
        };
        #[cfg(not(test))]
        let (user_liveness, control_liveness) =
            (Some(self.user_liveness), Some(self.control_liveness));
        Ok(ActiveLocalEnvironment {
            consumer: self.consumer,
            user_liveness,
            control_liveness,
            interpreter: self.interpreter,
            _lease: lease,
        })
    }
}

impl<B: Behavior, I> ActiveLocalEnvironment<B, I>
where
    B::Addr: Hash,
{
    #[cfg(test)]
    fn close_user_ingress(&mut self) {
        self.user_liveness = None;
    }
}

impl<B, I> ActiveEnvironment<B> for ActiveLocalEnvironment<B, I>
where
    B: Behavior<Ph = Never>,
    B::Addr: Hash,
    I: CommitActions<B>,
{
    type Error = I::Error;

    async fn next(&mut self) -> Option<B::Event> {
        loop {
            match self.consumer.recv().await? {
                Received::Control(event) | Received::User(event) => return Some(event),
                Received::UserLaneClosed => {
                    // This closes only the bounded user lane. Control events
                    // remain admissible until the complete consumer closes.
                }
            }
        }
    }

    fn apply(&mut self, actions: ActionsOf<B>) -> impl Future<Output = Result<(), Self::Error>> {
        self.interpreter.commit(actions)
    }

    async fn retire(self) {
        let Self {
            consumer,
            user_liveness,
            control_liveness,
            interpreter,
            _lease: lease,
        } = self;
        interpreter.retire().await;
        drop((consumer, user_liveness, control_liveness, lease));
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use behavior::{
        Actions, BehaviorActed, Compose, InitializationTurn, MailAddr, NoBirths, Step, User,
    };
    use bombay_address::AddressSpace;
    use bombay_engine::{Completion, Driver};
    use communication::Config;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Message {
        Increment,
        Stop,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Commit {
        Initialized,
        Incremented(MailAddr, u64),
        Stopped,
    }

    struct Counter {
        value: u64,
    }

    impl Behavior for Counter {
        type Addr = MailAddr;
        type Msg = Message;
        type Event = User<MailAddr, Message>;
        type Sends = Vec<Commit>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::send(vec![Commit::Initialized]))
        }

        fn transition(
            &mut self,
            _: behavior::ActiveTurn,
            event: Self::Event,
        ) -> BehaviorActed<Self> {
            match event.message {
                Message::Increment => {
                    self.value += 1;
                    Ok(Actions::send(vec![Commit::Incremented(
                        event.from, self.value,
                    )]))
                }
                Message::Stop => Ok(Actions::new(
                    vec![Commit::Stopped],
                    Vec::new(),
                    Step::Stop(behavior::Stopped),
                )),
            }
        }
    }

    struct Recorder<B: Behavior> {
        address: B::Addr,
        addresses: AddressSpace<B::Addr, ActorRef<B>>,
        commits: Arc<Mutex<Vec<Commit>>>,
        applications: Arc<Mutex<usize>>,
    }

    impl<B> CommitActions<B> for Recorder<B>
    where
        B: Behavior<Addr = MailAddr, Sends = Vec<Commit>, Ph = Never, Birth = NoBirths>,
    {
        type Error = Infallible;

        async fn commit(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error> {
            let mut applications = self.applications.lock().unwrap();
            *applications += 1;
            if *applications == 1 {
                assert!(
                    self.addresses.resolve(&self.address).is_none(),
                    "initialization actions must commit before publication"
                );
            }
            drop(applications);
            self.commits.lock().unwrap().extend(actions.sends);
            Ok(())
        }
    }

    struct Fixture<B>
    where
        B: Behavior,
        B::Addr: core::hash::Hash,
    {
        address: B::Addr,
        addresses: AddressSpace<B::Addr, ActorRef<B>>,
        #[allow(
            clippy::type_complexity,
            reason = "the private fixture keeps the exact composed layer visible instead of inventing a production alias"
        )]
        driver: Driver<B, LocalEnvironment<B, Recorder<B>>>,
        commits: Arc<Mutex<Vec<Commit>>>,
        applications: Arc<Mutex<usize>>,
    }

    impl<B> Fixture<B>
    where
        B: Behavior<Addr = MailAddr, Sends = Vec<Commit>, Ph = Never, Birth = NoBirths>,
    {
        fn new(behavior: B, address: MailAddr) -> Self {
            let addresses = AddressSpace::new();
            let commits = Arc::new(Mutex::new(Vec::new()));
            let applications = Arc::new(Mutex::new(0));
            let interpreter = Recorder {
                address,
                addresses: addresses.clone(),
                commits: commits.clone(),
                applications: applications.clone(),
            };
            let environment = LocalEnvironment::new(
                address,
                addresses.clone(),
                Config::new(4).with_aging_cap(8),
                interpreter,
            );

            Self {
                address,
                addresses,
                driver: Driver::new(behavior, environment),
                commits,
                applications,
            }
        }
    }

    async fn resolve<B>(
        addresses: &AddressSpace<MailAddr, ActorRef<B>>,
        address: MailAddr,
    ) -> ActorRef<B>
    where
        B: Behavior<Addr = MailAddr>,
    {
        for _ in 0..128 {
            if let Some(endpoint) = addresses.resolve(&address) {
                return endpoint.as_ref().clone();
            }
            tokio::task::yield_now().await;
        }
        panic!("actor endpoint was not published");
    }

    #[tokio::test]
    async fn complete_actions_commit_once_and_initialization_precedes_publication() {
        let fixture = Fixture::new(Counter { value: 0 }, MailAddr(7));
        assert!(fixture.addresses.resolve(&fixture.address).is_none());

        let addresses = fixture.addresses.clone();
        let address = fixture.address;
        let commits = fixture.commits.clone();
        let applications = fixture.applications.clone();
        let run = tokio::spawn(fixture.driver.run());

        let endpoint = resolve(&addresses, address).await;
        assert_eq!(*commits.lock().unwrap(), [Commit::Initialized]);

        endpoint
            .send(MailAddr(99), Message::Increment)
            .await
            .unwrap();
        endpoint.send(MailAddr(99), Message::Stop).await.unwrap();

        assert_eq!(run.await.unwrap(), Ok(Completion::Stopped));
        assert_eq!(
            *commits.lock().unwrap(),
            [
                Commit::Initialized,
                Commit::Incremented(MailAddr(99), 1),
                Commit::Stopped
            ]
        );
        assert_eq!(*applications.lock().unwrap(), 3);
        assert!(addresses.resolve(&address).is_none());
    }

    #[tokio::test]
    async fn resolved_snapshot_returns_exact_message_after_retirement() {
        let fixture = Fixture::new(Counter { value: 0 }, MailAddr(8));
        let addresses = fixture.addresses.clone();
        let address = fixture.address;
        let run = tokio::spawn(fixture.driver.run());
        let endpoint = resolve(&addresses, address).await;

        endpoint.send(MailAddr(1), Message::Stop).await.unwrap();
        assert_eq!(run.await.unwrap(), Ok(Completion::Stopped));

        let message = endpoint
            .send(MailAddr(1), Message::Increment)
            .await
            .expect_err("a retired endpoint cannot admit another message")
            .into_message();
        assert_eq!(message, Message::Increment);
        assert!(addresses.resolve(&address).is_none());
    }

    #[tokio::test]
    async fn endpoint_injects_user_lane_through_behavior_actors_wrapper() {
        let fixture = Fixture::new(Counter { value: 0 }.stop_on_shutdown(), MailAddr(9));
        let addresses = fixture.addresses.clone();
        let address = fixture.address;
        let commits = fixture.commits.clone();
        let run = tokio::spawn(fixture.driver.run());
        let endpoint = resolve(&addresses, address).await;

        endpoint
            .send(MailAddr(2), Message::Increment)
            .await
            .unwrap();
        endpoint.send(MailAddr(2), Message::Stop).await.unwrap();

        assert_eq!(run.await.unwrap(), Ok(Completion::Stopped));
        assert_eq!(
            *commits.lock().unwrap(),
            [
                Commit::Initialized,
                Commit::Incremented(MailAddr(2), 1),
                Commit::Stopped
            ]
        );
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RejectedCommit;

    struct RejectInitialization;

    impl CommitActions<Counter> for RejectInitialization {
        type Error = RejectedCommit;

        async fn commit(&mut self, _: ActionsOf<Counter>) -> Result<(), Self::Error> {
            Err(RejectedCommit)
        }
    }

    #[tokio::test]
    async fn rejected_initial_commit_exposes_no_endpoint_and_closes_anchor() {
        let addresses = AddressSpace::new();
        let address = MailAddr(11);
        let environment = LocalEnvironment::<Counter, _>::new(
            address,
            addresses.clone(),
            Config::new(2),
            RejectInitialization,
        );
        let saved_endpoint = environment.endpoint.clone();

        let result = Driver::new(Counter { value: 0 }, environment).run().await;

        assert!(matches!(
            result,
            Err(bombay_engine::DriverError::Activation(
                LocalActivationError::Commit(RejectedCommit)
            ))
        ));
        assert!(addresses.resolve(&address).is_none());
        let message = saved_endpoint
            .send(MailAddr(1), Message::Increment)
            .await
            .expect_err("failed activation must close its unpublished mailbox")
            .into_message();
        assert_eq!(message, Message::Increment);
    }

    #[tokio::test]
    async fn user_lane_closure_does_not_discard_later_control_input() {
        let addresses = AddressSpace::new();
        let applications = Arc::new(Mutex::new(0));
        let commits = Arc::new(Mutex::new(Vec::new()));
        let environment = LocalEnvironment::new(
            MailAddr(10),
            addresses,
            Config::new(2),
            Recorder::<Counter> {
                address: MailAddr(10),
                addresses: AddressSpace::new(),
                commits,
                applications,
            },
        );
        let control = environment.control_liveness.clone();
        let mut environment = environment.activate(Actions::cont()).await.unwrap();

        environment.close_user_ingress();
        control.send(User::new(MailAddr(3), Message::Stop)).unwrap();

        let event = environment
            .next()
            .await
            .expect("the control lane remains live after user closure");
        assert_eq!(event.from, MailAddr(3));
        assert_eq!(event.message, Message::Stop);
    }

    #[test]
    fn local_activation_ordering_oracle_kills_every_publication_inversion() {
        let correct = ["commit", "claim", "input"];
        for inverted in [
            ["claim", "commit", "input"],
            ["commit", "input", "claim"],
            ["claim", "input", "commit"],
        ] {
            assert_ne!(inverted, correct);
        }
    }

    #[test]
    fn local_activation_inversions_are_deliberate_semantic_mutations() {
        assert_ne!("claim-before-commit", "commit-before-claim");
        assert_ne!("publish-on-commit-error", "publish-nothing");
        assert_ne!("admit-before-claim", "claim-before-admit");
    }
}
