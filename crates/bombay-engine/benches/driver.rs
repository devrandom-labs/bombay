use core::ops::ControlFlow;
use std::convert::Infallible;
use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use behavior::{
    Actions, Behavior, BehaviorActed, Interpretation, MailAddr, Never, NoBirths, SourceCustody,
    User,
};
use bombay_engine::{
    ActionsOf, ActiveEnvironment, Completion, Driver, DriverRetirement, Environment,
};
use criterion::{Criterion, criterion_group, criterion_main};

#[derive(Debug, PartialEq, Eq)]
struct OneTurn;

impl Behavior for OneTurn {
    type Protocol = behavior::MessageProtocol<MailAddr, u8>;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }
}

enum Ingress {
    Pending,
    Exhausted,
}

struct Immediate(Ingress);

impl ActiveEnvironment<OneTurn> for Immediate {
    type Settlement = Vec<Never>;
    type Residual = Vec<Self::Settlement>;
    type RetirementRequest = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> ControlFlow<Never, Option<<OneTurn as Behavior>::Event>> {
        ControlFlow::Continue({
            match std::mem::replace(&mut self.0, Ingress::Exhausted) {
                Ingress::Pending => Some(User::new(MailAddr(1), 1)),
                Ingress::Exhausted => None,
            }
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next_source(&mut self) -> ControlFlow<Never, Option<<OneTurn as Behavior>::Event>> {
        unreachable!("the benchmark has no source-returning actions")
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, _: ActionsOf<OneTurn>) -> Interpretation<Self::Settlement> {
        Interpretation::Complete(Vec::new())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn offer_next(
        &mut self,
        settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement> {
        SourceCustody::Exhausted(settlement)
    }

    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()> {
        ControlFlow::Continue(())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn retire(self, settlements: Vec<Self::Settlement>) -> Self::Residual {
        settlements
    }
}

impl Environment<OneTurn> for Immediate {
    type Active = Self;
    type Settlement = Vec<Never>;
    type Error = Infallible;
    type Residual = Vec<Self::Settlement>;
    type RetirementRequest = Never;

    async fn activate(
        mut self,
        actions: ActionsOf<OneTurn>,
    ) -> Result<(Self, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)> {
        let interpretation = self.apply(actions).await;
        Ok((self, interpretation))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn retire(self) -> Self::Residual {
        Vec::new()
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("immediate benchmark environment pended"),
    }
}

fn driver_benchmark(criterion: &mut Criterion) {
    let retirement = block_on(Driver::new(OneTurn, Immediate(Ingress::Pending)).run());
    assert_eq!(
        retirement,
        DriverRetirement {
            behavior: OneTurn,
            residual: vec![Vec::new()],
            disposition: Ok(Completion::Stopped),
        }
    );

    criterion.bench_function("driver/init_commit_turn_commit_stop_retire", |bencher| {
        bencher.iter(|| {
            black_box(block_on(
                Driver::new(OneTurn, Immediate(Ingress::Pending)).run(),
            ))
        });
    });
}

criterion_group!(benches, driver_benchmark);
criterion_main!(benches);
