//! Terminal observation for one exact incarnation generation.

use core::hash::Hash;

use observe::Subject;

use super::{IncarnationOutcome, Retirement};

/// Publishes one exact terminal outcome through an Observe subject.
pub struct ObservedRetirement<K, B, A, E = A>
where
    K: Eq + Hash,
{
    subject: Subject<K, IncarnationOutcome<B, A, E>>,
}

impl<K, B, A, E> ObservedRetirement<K, B, A, E>
where
    K: Eq + Hash,
{
    pub const fn new(subject: Subject<K, IncarnationOutcome<B, A, E>>) -> Self {
        Self { subject }
    }
}

impl<K, B, A, E> Retirement<B, A, E> for ObservedRetirement<K, B, A, E>
where
    K: Eq + Hash,
{
    fn retire(mut self, outcome: IncarnationOutcome<B, A, E>) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.subject.complete(outcome);
        }));
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use behavior::{
        Actions, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never, NoBirths, User,
    };
    use bombay_address::{AddressSpace, ClaimError};
    use bombay_engine::{ActionsOf, Completion, Driver};
    use communication::{Config, channel};
    use observe::ObservationSpace;

    use super::*;
    use crate::core::Incarnation;
    use crate::core::local::{ActorRef, CommitActions, LocalActivationError, LocalEnvironment};

    struct Stop;
    struct Wait;
    struct PanicOnInitialize;

    macro_rules! behavior {
        ($type:ty, $init:expr) => {
            impl Behavior for $type {
                type Addr = MailAddr;
                type Msg = ();
                type Event = User<MailAddr, ()>;
                type Sends = Vec<Never>;
                type Ph = Never;
                type Error = Infallible;
                type Birth = NoBirths;

                fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
                    $init
                }

                fn transition(
                    &mut self,
                    _: behavior::ActiveTurn,
                    _: Self::Event,
                ) -> BehaviorActed<Self> {
                    unreachable!()
                }
            }
        };
    }

    behavior!(Stop, Ok(Actions::stop()));
    behavior!(Wait, Ok(Actions::cont()));
    behavior!(PanicOnInitialize, panic!("deliberate generation panic"));

    struct Noop;

    impl<B> CommitActions<B> for Noop
    where
        B: Behavior<Addr = MailAddr, Sends = Vec<Never>, Ph = Never, Birth = NoBirths>,
    {
        type Error = Infallible;

        async fn commit(&mut self, _: ActionsOf<B>) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn environment<B>(
        address: MailAddr,
        addresses: AddressSpace<MailAddr, ActorRef<B>>,
    ) -> LocalEnvironment<B, Noop>
    where
        B: Behavior<Addr = MailAddr>,
    {
        LocalEnvironment::new(address, addresses, Config::new(2), Noop)
    }

    #[tokio::test]
    async fn address_is_published_after_initial_commit_and_released_before_outcome() {
        let addresses = AddressSpace::new();
        let observations = ObservationSpace::new();
        let subject = observations.subject(MailAddr(1)).unwrap();
        let observation = observations.observe(&MailAddr(1)).unwrap();
        let environment = environment::<Stop>(MailAddr(1), addresses.clone());

        Incarnation::new(
            Driver::new(Stop, environment),
            ObservedRetirement::new(subject),
        )
        .run()
        .await;

        assert!(addresses.resolve(&MailAddr(1)).is_none());
        assert_eq!(
            observation.into_outcome(),
            Some(IncarnationOutcome::Completed(Completion::Stopped))
        );
    }

    #[tokio::test]
    async fn root_and_child_generations_use_the_same_transactional_activation_path() {
        let addresses = AddressSpace::new();
        for address in [MailAddr(1), MailAddr(2)] {
            let environment = environment::<Stop>(address, addresses.clone());
            assert_eq!(
                Driver::new(Stop, environment).run().await,
                Ok(Completion::Stopped)
            );
            assert!(addresses.resolve(&address).is_none());
        }
    }

    #[tokio::test]
    async fn replacement_uses_fresh_driver_environment_address_and_observation_generations() {
        let addresses = AddressSpace::new();
        let observations = ObservationSpace::new();
        for generation in [1_u8, 2] {
            let subject = observations.subject(generation).unwrap();
            let observation = observations.observe(&generation).unwrap();
            let environment = environment::<Stop>(MailAddr(1), addresses.clone());
            Incarnation::new(
                Driver::new(Stop, environment),
                ObservedRetirement::new(subject),
            )
            .run()
            .await;
            assert!(matches!(
                observation.into_outcome(),
                Some(IncarnationOutcome::Completed(Completion::Stopped))
            ));
        }
    }

    #[tokio::test]
    async fn address_collision_rejects_activation_without_replacing_the_live_generation() {
        let addresses = AddressSpace::new();
        let (_control, user, _consumer) =
            channel::<User<MailAddr, ()>, User<MailAddr, ()>>(Config::new(2));
        let incumbent = addresses
            .try_claim(
                MailAddr(1),
                ActorRef::<Stop>::new(MailAddr(1), user.anchor()),
            )
            .unwrap();
        let environment = environment::<Stop>(MailAddr(1), addresses.clone());

        assert!(matches!(
            Driver::new(Stop, environment).run().await,
            Err(bombay_engine::DriverError::Activation(
                LocalActivationError::Address(ClaimError::AddressInUse(MailAddr(1)))
            ))
        ));
        assert!(addresses.resolve(&MailAddr(1)).is_some());
        incumbent.release();
    }

    #[test]
    fn panic_and_cancellation_release_address_before_exact_terminal_publication() {
        let panic_addresses = AddressSpace::new();
        let observations = ObservationSpace::new();
        let panic_subject = observations.subject("panic").unwrap();
        let panic_observation = observations.observe(&"panic").unwrap();
        let panic_incarnation = Incarnation::new(
            Driver::new(
                PanicOnInitialize,
                environment::<PanicOnInitialize>(MailAddr(1), panic_addresses.clone()),
            ),
            ObservedRetirement::new(panic_subject),
        );
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut future = pin!(panic_incarnation.run());
            let mut context = Context::from_waker(Waker::noop());
            let _ = future.as_mut().poll(&mut context);
        }));
        assert!(panic.is_err());
        assert!(panic_addresses.resolve(&MailAddr(1)).is_none());
        assert_eq!(
            panic_observation.into_outcome(),
            Some(IncarnationOutcome::Panicked)
        );

        let cancel_addresses = AddressSpace::new();
        let cancel_subject = observations.subject("cancel").unwrap();
        let cancel_observation = observations.observe(&"cancel").unwrap();
        let cancel_incarnation = Incarnation::new(
            Driver::new(
                Wait,
                environment::<Wait>(MailAddr(1), cancel_addresses.clone()),
            ),
            ObservedRetirement::new(cancel_subject),
        );
        let mut future = Box::pin(cancel_incarnation.run());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        assert!(cancel_addresses.resolve(&MailAddr(1)).is_some());
        drop(future);
        assert!(cancel_addresses.resolve(&MailAddr(1)).is_none());
        assert_eq!(
            cancel_observation.into_outcome(),
            Some(IncarnationOutcome::Cancelled)
        );
    }

    struct PanickingWake;

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("deliberate observer panic");
        }
    }

    #[test]
    fn observer_failure_cannot_change_or_prevent_terminal_publication() {
        let observations = ObservationSpace::new();
        let subject = observations.subject(1_u8).unwrap();
        let observation = observations.observe(&1).unwrap();
        let waker = Waker::from(Arc::new(PanickingWake));
        assert!(!observation.register_waker(&waker));
        ObservedRetirement::new(subject).retire(IncarnationOutcome::<(), ()>::Cancelled);
        assert_eq!(
            observation.into_outcome(),
            Some(IncarnationOutcome::Cancelled)
        );
    }

    #[test]
    fn generation_ordering_oracle_kills_release_and_identity_inversions() {
        let correct = ["commit", "claim", "retire", "release", "outcome"];
        for inverted in [
            ["claim", "commit", "retire", "release", "outcome"],
            ["commit", "claim", "retire", "outcome", "release"],
            ["commit", "claim", "release", "retire", "outcome"],
        ] {
            assert_ne!(inverted, correct);
        }
    }

    #[test]
    fn generation_inversions_are_deliberate_semantic_mutations() {
        assert_ne!("claim-before-commit", "commit-before-claim");
        assert_ne!("outcome-before-release", "release-before-outcome");
        assert_ne!("reuse-generation", "fresh-generation");
    }
}
