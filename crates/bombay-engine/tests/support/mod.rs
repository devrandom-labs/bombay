use core::future::Future;
use core::ops::ControlFlow;

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
    fn retire(&mut self) -> impl Future<Output = Self::Residual>;
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
    type RetirementRequest = Never;

    async fn activate(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<
            Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>,
        >,
    ) {
        if received.is_some() {
            return;
        }
        let Some(original) = environment.as_mut() else {
            return;
        };
        let Some(actions) = actions.take() else {
            return;
        };
        let interpretation = interpret(original.0.apply(actions).await);
        let active = environment
            .take()
            .expect("the original test environment remains available");
        *received = Some(Ok((active, interpretation)));
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() || actions.is_some() {
            return;
        }
        let Some(original) = environment.as_mut() else {
            return;
        };
        *received = Some(original.0.retire().await);
        drop(environment.take());
    }
}

impl<B, E> ActiveEnvironment<B> for TestEnvironment<E>
where
    B: Behavior<Ph = Never>,
    E: TestActions<B>,
{
    type Settlement = TestSettlement<E::Error>;
    type Residual = E::Residual;
    type RetirementRequest = Never;

    async fn next(&mut self) -> ControlFlow<Never, Option<B::Event>> {
        ControlFlow::Continue(self.0.next().await)
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next_source(&mut self) -> ControlFlow<Never, Option<B::Event>> {
        unreachable!("the test adapter never admits a source result")
    }

    async fn apply(
        &mut self,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<Interpretation<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        let Some(actions) = actions.take() else {
            return;
        };
        *received = Some(interpret(self.0.apply(actions).await));
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer exact source ownership transfer until the future is polled."
    )]
    async fn offer_next(
        &mut self,
        settlement: &mut Option<Self::Settlement>,
        received: &mut Option<SourceCustody<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        if let Some(settlement) = settlement.take() {
            *received = Some(SourceCustody::Exhausted(settlement));
        }
    }

    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()> {
        ControlFlow::Continue(())
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        interpretation: &mut Option<Interpretation<Self::Settlement>>,
        source: &mut Option<SourceCustody<Self::Settlement>>,
        source_index: &mut Option<usize>,
        ingress: &mut Option<ControlFlow<Self::RetirementRequest, Option<B::Event>>>,
        settlements: &mut Option<Vec<Self::Settlement>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some()
            || actions.is_some()
            || interpretation.is_some()
            || source.is_some()
            || source_index.is_some()
            || ingress.is_some()
        {
            return;
        }
        let Some(original) = environment.as_mut() else {
            return;
        };
        let Some(settlements) = settlements.take() else {
            return;
        };
        drop(settlements);
        *received = Some(original.0.retire().await);
        drop(environment.take());
    }
}

pub fn direct<B, E>(behavior: B, environment: E) -> Driver<B, TestEnvironment<E>>
where
    B: Behavior<Ph = Never>,
    E: TestActions<B>,
{
    Driver::new(behavior, TestEnvironment(environment))
}
