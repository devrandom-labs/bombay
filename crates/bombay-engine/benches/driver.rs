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
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, Driver, Environment};
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
    async fn apply(
        &mut self,
        actions: &mut Option<ActionsOf<OneTurn>>,
        received: &mut Option<Interpretation<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        if let Some(actions) = actions.take() {
            assert_eq!(actions.sends, [] as [Never; 0]);
            assert!(actions.creates.is_empty());
            drop(actions);
            *received = Some(Interpretation::Complete(Vec::new()));
        }
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
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

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<OneTurn>>,
        interpretation: &mut Option<Interpretation<Self::Settlement>>,
        source: &mut Option<SourceCustody<Self::Settlement>>,
        source_index: &mut Option<usize>,
        ingress: &mut Option<ControlFlow<Never, Option<<OneTurn as Behavior>::Event>>>,
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
        if environment.is_none() {
            return;
        }
        if let Some(settlements) = settlements.take() {
            *received = Some(settlements);
            *environment = None;
        }
    }
}

impl Environment<OneTurn> for Immediate {
    type Active = Self;
    type Settlement = Vec<Never>;
    type Error = Infallible;
    type Residual = Vec<Self::Settlement>;
    type RetirementRequest = Never;

    async fn activate(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<OneTurn>>,
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
        let mut interpretation = None;
        ActiveEnvironment::apply(original, actions, &mut interpretation).await;
        if let Some(interpretation) = interpretation {
            let active = environment
                .take()
                .expect("the benchmark environment remains available");
            *received = Some(Ok((active, interpretation)));
        }
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<OneTurn>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() || actions.is_some() || environment.is_none() {
            return;
        }
        *received = Some(Vec::new());
        *environment = None;
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

#[expect(
    clippy::result_large_err,
    reason = "the measured result preserves the original affine Driver without another allocation"
)]
fn driver_benchmark(criterion: &mut Criterion) {
    let retirement = block_on(Driver::new(OneTurn, Immediate(Ingress::Pending)).run())
        .unwrap_or_else(|driver| {
            drop(driver);
            panic!("the immediate benchmark host completes retirement")
        });
    assert_eq!(retirement.behavior, OneTurn);
    assert_eq!(retirement.residual, vec![Vec::new()]);
    assert!(matches!(retirement.disposition, Ok(Completion::Stopped)));

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
