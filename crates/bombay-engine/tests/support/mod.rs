use behavior::{Behavior, Never};
use bombay_engine::{ActionsOf, ActiveEnvironment, Driver, Environment};

pub struct TestEnvironment<E>(E);

impl<B, E> Environment<B> for TestEnvironment<E>
where
    B: Behavior<Ph = Never>,
    E: ActiveEnvironment<B>,
{
    type Active = E;
    type Error = E::Error;

    async fn activate(mut self, actions: ActionsOf<B>) -> Result<Self::Active, Self::Error> {
        match self.0.apply(actions).await {
            Ok(()) => Ok(self.0),
            Err(error) => {
                self.0.retire().await;
                Err(error)
            }
        }
    }
}

pub fn direct<B: Behavior, E>(behavior: B, environment: E) -> Driver<B, TestEnvironment<E>> {
    Driver::new(behavior, TestEnvironment(environment))
}
