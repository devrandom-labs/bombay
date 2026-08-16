//! One concrete task-launch boundary for local actors.

use core::future::Future;
use core::hash::Hash;

use behavior::{Address, Behavior, BirthMode, Never};
use bombay_address::AddressSpace;
use bombay_engine::{ActionsOf, Driver};
use communication::Config;
use tokio::sync::oneshot;

use super::Incarnation;
use super::local::{ActorRef, CommitActions, LocalEnvironment};

/// Failure before a launched actor publishes its live reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("actor terminated before becoming live")]
pub struct SpawnError;

/// One behavior-protocol-indexed local address namespace.
///
/// It owns no tasks or actor liveness. Each successful [`Self::spawn`] returns
/// a non-owning [`ActorRef`] only after initialization has committed and the
/// exact address generation has been claimed.
pub struct LocalActors<B: Behavior>
where
    B::Addr: Hash,
{
    addresses: AddressSpace<B::Addr, ActorRef<B>>,
    config: Config,
}

impl<B> Clone for LocalActors<B>
where
    B: Behavior,
    B::Addr: Hash,
{
    fn clone(&self) -> Self {
        Self {
            addresses: self.addresses.clone(),
            config: self.config,
        }
    }
}

impl<B> LocalActors<B>
where
    B: Behavior,
    B::Addr: Hash,
{
    /// Create a local namespace whose actors use the stated bounded user-lane
    /// capacity.
    #[must_use]
    pub fn new(user_capacity: usize) -> Self {
        Self {
            addresses: AddressSpace::new(),
            config: Config::new(user_capacity),
        }
    }

    /// Resolve the currently live generation at `address`.
    #[must_use]
    pub fn resolve(&self, address: &B::Addr) -> Option<ActorRef<B>> {
        self.addresses
            .resolve(address)
            .map(|resolved| resolved.as_ref().clone())
    }
}

impl<B> LocalActors<B>
where
    B: Behavior<Ph = Never> + Send + 'static,
    B::Addr: Hash + Clone + Send + Sync + 'static,
    <B::Addr as Address>::Nonce: Send + 'static,
    B::Event: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
{
    /// Launch one local actor and return its typed reference after activation.
    ///
    /// `commit` receives each complete action value exactly once, including
    /// initialization. Its concrete closure and future types stay inferred;
    /// Bombay does not expose a second interpreter trait.
    ///
    /// # Errors
    ///
    /// Returns [`SpawnError`] when the incarnation terminates before it
    /// publishes the requested address generation.
    pub async fn spawn<F, Fut, E>(
        &self,
        address: B::Addr,
        behavior: B,
        commit: F,
    ) -> Result<ActorRef<B>, SpawnError>
    where
        F: FnMut(ActionsOf<B>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Send + 'static,
    {
        let (activated_tx, activated_rx) = oneshot::channel();
        let environment = LocalEnvironment::new(
            address,
            self.addresses.clone(),
            self.config,
            CommitWith(commit),
        )
        .publish_with(move |actor| {
            let _ = activated_tx.send(actor);
        });
        let incarnation = Incarnation::new(
            Driver::new(behavior, environment),
            |_: super::IncarnationOutcome<B::Error, _, _>| {},
        );
        tokio::spawn(incarnation.run());

        activated_rx.await.map_err(|_| SpawnError)
    }
}

struct CommitWith<F>(F);

impl<B, F, Fut, E> CommitActions<B> for CommitWith<F>
where
    B: Behavior<Ph = Never>,
    F: FnMut(ActionsOf<B>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    type Error = E;

    fn commit(&mut self, actions: ActionsOf<B>) -> impl Future<Output = Result<(), Self::Error>> {
        (self.0)(actions)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::ready;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use behavior::{Actions, BehaviorActed, InitializationTurn, MailAddr, NoBirths, Step, User};

    use super::*;
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Message {
        Add(u64),
        Stop,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Commit {
        Initialized,
        Added(MailAddr, u64),
        Stopped,
    }

    struct Counter(u64);
    struct StopOnInitialize;
    struct FailOnInitialize;
    struct PanicOnInitialize;

    #[derive(Debug)]
    struct InitializationFailure;

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
                Message::Add(amount) => {
                    self.0 += amount;
                    Ok(Actions::send(vec![Commit::Added(event.from, self.0)]))
                }
                Message::Stop => Ok(Actions::new(
                    vec![Commit::Stopped],
                    Vec::new(),
                    Step::Stop(behavior::Stopped),
                )),
            }
        }
    }

    impl Behavior for StopOnInitialize {
        type Addr = MailAddr;
        type Msg = Message;
        type Event = User<MailAddr, Message>;
        type Sends = Vec<Commit>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::stop())
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            unreachable!("initialization stops this behavior")
        }
    }

    impl Behavior for FailOnInitialize {
        type Addr = MailAddr;
        type Msg = Message;
        type Event = User<MailAddr, Message>;
        type Sends = Vec<Commit>;
        type Ph = Never;
        type Error = InitializationFailure;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Err(InitializationFailure)
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            unreachable!("initialization rejects this behavior")
        }
    }

    impl Behavior for PanicOnInitialize {
        type Addr = MailAddr;
        type Msg = Message;
        type Event = User<MailAddr, Message>;
        type Sends = Vec<Commit>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            panic!("deliberate pre-publication panic")
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            unreachable!("initialization panics this behavior")
        }
    }

    async fn wait_until_released(actors: &LocalActors<Counter>, address: MailAddr) {
        for _ in 0..100 {
            if actors.resolve(&address).is_none() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("actor address was not retired");
    }

    #[tokio::test]
    async fn spawn_returns_only_after_initial_commit_and_publication() {
        let actors = LocalActors::<Counter>::new(2);
        let during_commit = actors.clone();
        let first_commit = Arc::new(AtomicBool::new(true));
        let check_first = first_commit.clone();
        let commits = Arc::new(Mutex::new(Vec::new()));
        let recorded = commits.clone();

        let actor = actors
            .spawn(MailAddr(1), Counter(0), move |actions| {
                if check_first.swap(false, Ordering::SeqCst) {
                    assert!(during_commit.resolve(&MailAddr(1)).is_none());
                }
                recorded.lock().unwrap().extend(actions.sends);
                ready(Ok::<_, Infallible>(()))
            })
            .await
            .unwrap();

        assert_eq!(actor.address(), MailAddr(1));
        assert!(actors.resolve(&MailAddr(1)).is_some());
        assert_eq!(*commits.lock().unwrap(), [Commit::Initialized]);

        actor.send(MailAddr(9), Message::Stop).await.unwrap();
        wait_until_released(&actors, MailAddr(1)).await;
    }

    #[tokio::test]
    async fn typed_reference_delivers_origin_and_message_through_the_real_runtime() {
        let actors = LocalActors::<Counter>::new(2);
        let commits = Arc::new(Mutex::new(Vec::new()));
        let recorded = commits.clone();
        let actor = actors
            .spawn(MailAddr(1), Counter(0), move |actions| {
                recorded.lock().unwrap().extend(actions.sends);
                ready(Ok::<_, Infallible>(()))
            })
            .await
            .unwrap();

        actor.send(MailAddr(7), Message::Add(3)).await.unwrap();
        actor.send(MailAddr(8), Message::Stop).await.unwrap();
        wait_until_released(&actors, MailAddr(1)).await;

        assert_eq!(
            *commits.lock().unwrap(),
            [
                Commit::Initialized,
                Commit::Added(MailAddr(7), 3),
                Commit::Stopped,
            ]
        );
        let message = actor
            .send(MailAddr(9), Message::Add(1))
            .await
            .unwrap_err()
            .into_message();
        assert_eq!(message, Message::Add(1));
    }

    #[tokio::test]
    async fn failed_initial_commit_returns_no_reference_and_publishes_nothing() {
        let actors = LocalActors::<Counter>::new(2);
        let result = actors
            .spawn(MailAddr(1), Counter(0), |_| ready(Err::<(), _>("rejected")))
            .await;

        assert!(matches!(result, Err(SpawnError)));
        assert!(actors.resolve(&MailAddr(1)).is_none());
    }

    #[tokio::test]
    async fn initialization_stop_returns_the_exact_published_reference_without_a_second_lookup() {
        let actors = LocalActors::<StopOnInitialize>::new(2);
        let actor = actors
            .spawn(MailAddr(1), StopOnInitialize, |_| {
                ready(Ok::<_, Infallible>(()))
            })
            .await
            .unwrap();

        assert_eq!(actor.address(), MailAddr(1));
        for _ in 0..100 {
            if actors.resolve(&MailAddr(1)).is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(actors.resolve(&MailAddr(1)).is_none());
        let message = actor
            .send(MailAddr(9), Message::Stop)
            .await
            .unwrap_err()
            .into_message();
        assert_eq!(message, Message::Stop);
    }

    #[tokio::test]
    async fn behavior_failure_before_activation_returns_no_reference_or_address() {
        let actors = LocalActors::<FailOnInitialize>::new(2);
        let result = actors
            .spawn(MailAddr(1), FailOnInitialize, |_| {
                ready(Ok::<_, Infallible>(()))
            })
            .await;

        assert!(matches!(result, Err(SpawnError)));
        assert!(actors.resolve(&MailAddr(1)).is_none());
    }

    #[tokio::test]
    async fn panic_before_activation_returns_no_reference_or_address() {
        let actors = LocalActors::<PanicOnInitialize>::new(2);
        let result = actors
            .spawn(MailAddr(1), PanicOnInitialize, |_| {
                ready(Ok::<_, Infallible>(()))
            })
            .await;

        assert!(matches!(result, Err(SpawnError)));
        assert!(actors.resolve(&MailAddr(1)).is_none());
    }

    #[tokio::test]
    async fn address_collision_keeps_the_incumbent_and_rejects_the_new_launch() {
        let actors = LocalActors::<Counter>::new(2);
        let incumbent = actors
            .spawn(MailAddr(1), Counter(0), |_| ready(Ok::<_, Infallible>(())))
            .await
            .unwrap();

        let collision = actors
            .spawn(MailAddr(1), Counter(0), |_| ready(Ok::<_, Infallible>(())))
            .await;
        assert!(matches!(collision, Err(SpawnError)));
        assert!(actors.resolve(&MailAddr(1)).is_some());

        incumbent.send(MailAddr(9), Message::Stop).await.unwrap();
        wait_until_released(&actors, MailAddr(1)).await;
    }
}
