use std::convert::Infallible;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, User};
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
    type Error = Infallible;

    async fn next(&mut self) -> Option<<OneTurn as Behavior>::Event> {
        (!std::mem::replace(&mut self.0, true)).then(|| User::new(MailAddr(1), 1))
    }

    async fn apply(&mut self, _: ActionsOf<OneTurn>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(self) {}
}

impl Environment<OneTurn> for Immediate {
    type Active = Self;
    type Error = Infallible;

    async fn activate(mut self, actions: ActionsOf<OneTurn>) -> Result<Self, Self::Error> {
        self.apply(actions).await?;
        Ok(self)
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
    criterion.bench_function("driver/init_commit_turn_commit_stop_retire", |bencher| {
        bencher.iter(|| {
            let result = block_on(Driver::new(OneTurn, Immediate(false)).run());
            assert_eq!(result, Ok(Completion::Stopped));
        });
    });
}

criterion_group!(benches, driver_benchmark);
criterion_main!(benches);
