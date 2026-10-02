use core::future::Future;

use behavior::{
    Behavior, ClassifySettlement, Interpretation, Never, SettlementStatus, SourceCustody,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Driver, Environment};

pub struct TestEnvironment<E>(E);

pub trait TestActions<B: Behavior> {
    type Error;
    type Residual;

    fn next(&mut self) -> impl Future<Output = Option<B::Event>>;
    fn apply(&mut self, actions: ActionsOf<B>) -> impl Future<Output = Result<(), Self::Error>>;
    fn retire(self) -> impl Future<Output = Self::Residual>;
}

pub enum TestSettlement<E> {
    Applied,
    Failed(E),
}

impl<E> ClassifySettlement for TestSettlement<E> {
    fn settlement_status(&self) -> SettlementStatus {
        match self {
            Self::Applied => SettlementStatus::Accepted,
            Self::Failed(_) => SettlementStatus::Corrupt,
        }
    }
}

fn interpret<E>(result: Result<(), E>) -> Interpretation<TestSettlement<E>> {
    match result {
        Ok(()) => Interpretation::Complete(TestSettlement::Applied),
        Err(error) => Interpretation::Corrupt(TestSettlement::Failed(error)),
    }
}

impl<B, E> Environment<B> for TestEnvironment<E>
where
    B: Behavior<Ph = Never>,
    E: TestActions<B>,
{
    type Active = Self;
    type Settlement = TestSettlement<E::Error>;
    type Error = core::convert::Infallible;
    type Residual = E::Residual;

    async fn activate(
        mut self,
        actions: ActionsOf<B>,
    ) -> Result<
        (Self::Active, behavior::Interpretation<Self::Settlement>),
        (Self::Error, Self::Residual),
    > {
        let interpretation = interpret(self.0.apply(actions).await);
        Ok((self, interpretation))
    }

    fn retire(self) -> impl core::future::Future<Output = Self::Residual> {
        self.0.retire()
    }
}

impl<B, E> ActiveEnvironment<B> for TestEnvironment<E>
where
    B: Behavior<Ph = Never>,
    E: TestActions<B>,
{
    type Settlement = TestSettlement<E::Error>;
    type Residual = E::Residual;

    fn next(&mut self) -> impl Future<Output = Option<B::Event>> {
        self.0.next()
    }

    async fn next_source(&mut self) -> Option<B::Event> {
        unreachable!("the test adapter never admits a source result")
    }

    async fn apply(&mut self, actions: ActionsOf<B>) -> Interpretation<Self::Settlement> {
        interpret(self.0.apply(actions).await)
    }

    async fn offer_next(
        &mut self,
        settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement> {
        SourceCustody::Exhausted(settlement)
    }

    fn publish(&mut self) {}

    async fn retire(self, settlements: Vec<Self::Settlement>) -> Self::Residual {
        drop(settlements);
        self.0.retire().await
    }
}

pub fn direct<B, E>(behavior: B, environment: E) -> Driver<B, TestEnvironment<E>>
where
    B: Behavior<Ph = Never>,
    E: TestActions<B>,
{
    Driver::new(behavior, TestEnvironment(environment))
}
