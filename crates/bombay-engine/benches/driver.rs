use std::convert::Infallible;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use behavior::{
    Actions, Behavior, BehaviorActed, Interpretation, MailAddr, Never, NoBirths, SourceCustody,
    User,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, Driver, Environment};
use criterion::{Criterion, criterion_group, criterion_main};

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

struct Immediate(bool);

impl ActiveEnvironment<OneTurn> for Immediate {
    type Settlement = Vec<Never>;
    type Residual = ();

    async fn next(&mut self) -> Option<<OneTurn as Behavior>::Event> {
        (!std::mem::replace(&mut self.0, true)).then(|| User::new(MailAddr(1), 1))
    }

    async fn next_source(&mut self) -> Option<<OneTurn as Behavior>::Event> {
        unreachable!("the benchmark has no source-returning actions")
    }

    async fn apply(&mut self, _: ActionsOf<OneTurn>) -> Interpretation<Self::Settlement> {
        Interpretation::Complete(Vec::new())
    }

    async fn offer_next(
        &mut self,
        settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement> {
        SourceCustody::Exhausted(settlement)
    }

    fn publish(&mut self) {}

    async fn retire(self, _: Vec<Self::Settlement>) {}
}

impl Environment<OneTurn> for Immediate {
    type Active = Self;
    type Settlement = Vec<Never>;
    type Error = Infallible;
    type Residual = ();

    async fn activate(
        mut self,
        actions: ActionsOf<OneTurn>,
    ) -> Result<(Self, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)> {
        let interpretation = self.apply(actions).await;
        Ok((self, interpretation))
    }

    async fn retire(self) {}
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
    criterion.bench_function("driver/init_commit_turn_commit_stop_retire", |bencher| {
        bencher.iter(|| {
            let result = block_on(Driver::new(OneTurn, Immediate(false)).run());
            assert_eq!(result.disposition, Ok(Completion::Stopped));
        });
    });
}

criterion_group!(benches, driver_benchmark);
criterion_main!(benches);
