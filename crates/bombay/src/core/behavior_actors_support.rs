use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;

use behavior::{Behavior, MailAddr, Never};
use bombay_address::AddressSpace;
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, Driver, DriverError};
use communication::Config;

use super::local::{ActorRef, CommitActions, LocalActivationError, LocalEnvironment};
use super::{Incarnation, IncarnationOutcome};

pub(super) struct CoreRun<B, E> {
    behavior: B,
    environment: E,
}

pub(super) fn direct<B, E>(behavior: B, environment: E) -> CoreRun<B, E>
where
    B: Behavior<Addr = MailAddr, Ph = Never>,
    E: ActiveEnvironment<B, Error = Infallible>,
{
    CoreRun {
        behavior,
        environment,
    }
}

impl<B, E> CoreRun<B, E>
where
    B: Behavior<Addr = MailAddr, Ph = Never>,
    E: ActiveEnvironment<B, Error = Infallible>,
{
    pub(super) async fn run(self) -> Result<Completion, DriverError<B::Error, Infallible>> {
        let Self {
            behavior,
            mut environment,
        } = self;
        let mut events = VecDeque::new();
        while let Some(event) = environment.next().await {
            events.push_back(event);
        }

        let addresses: AddressSpace<MailAddr, ActorRef<B>> = AddressSpace::new();
        let local = LocalEnvironment::new(
            MailAddr(1),
            addresses,
            Config::new(2),
            TestInterpreter(environment),
        );
        for event in events {
            local.preload_control(event);
        }
        let outcome = std::sync::Arc::new(std::sync::Mutex::new(None));
        let observed = outcome.clone();
        Incarnation::new(
            Driver::new(behavior, local.close_after_activation()),
            move |terminal| *observed.lock().unwrap() = Some(terminal),
        )
        .run()
        .await;
        match outcome.lock().unwrap().take().unwrap() {
            IncarnationOutcome::Completed(completion) => Ok(completion),
            IncarnationOutcome::BehaviorFailed(error) => Err(DriverError::Behavior(error)),
            IncarnationOutcome::ActivationFailed(LocalActivationError::Commit(error))
            | IncarnationOutcome::EnvironmentFailed(error) => match error {},
            IncarnationOutcome::ActivationFailed(LocalActivationError::Address(error)) => {
                panic!("fresh core template address unexpectedly collided: {error:?}")
            }
            IncarnationOutcome::Panicked => panic!("canonical template panicked"),
            IncarnationOutcome::Cancelled => panic!("canonical template was cancelled"),
        }
    }
}

struct TestInterpreter<E>(E);

impl<B, E> CommitActions<B> for TestInterpreter<E>
where
    B: Behavior<Ph = Never>,
    E: ActiveEnvironment<B, Error = Infallible>,
{
    type Error = Infallible;

    fn commit(&mut self, actions: ActionsOf<B>) -> impl Future<Output = Result<(), Self::Error>> {
        self.0.apply(actions)
    }

    async fn retire(self) {
        self.0.retire().await;
    }
}
