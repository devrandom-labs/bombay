use core::ops::ControlFlow;
use std::collections::VecDeque;
use std::convert::Infallible;

use behavior::{
    Actions, Behavior, BehaviorActed, Creations, EventLayer, MailAddr, Never, NoBirths, Step, User,
};
use behavior::{ClassifySettlement, Interpretation, SettlementStatus, SourceCustody};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, Driver, Environment};

type SettlementEvent = EventLayer<u8, User<MailAddr, ()>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecutionEvent {
    Committed(u8),
    Continued,
    Stopped,
    RequestedSource,
    RequestedOrdinary,
    DeliveredSource(u8),
    DeliveredOrdinary,
    Retired,
}

struct SettlementActor {
    delivered_inputs: Vec<SettlementInput>,
}

#[derive(Debug, PartialEq, Eq)]
enum SettlementInput {
    Source(u8),
    Ordinary,
}

impl Behavior for SettlementActor {
    type Protocol = behavior::MessageProtocol<MailAddr, ()>;
    type Event = SettlementEvent;
    type Sends = Vec<u8>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::send(vec![1]))
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            EventLayer::Owned(1) => {
                self.delivered_inputs.push(SettlementInput::Source(1));
                Ok(Actions::send(vec![2]))
            }
            EventLayer::Owned(2) => {
                self.delivered_inputs.push(SettlementInput::Source(2));
                Ok(Actions::cont())
            }
            EventLayer::Owned(other) => panic!("unexpected settlement {other}"),
            EventLayer::Inner(_) => {
                self.delivered_inputs.push(SettlementInput::Ordinary);
                Ok(Actions::stop())
            }
        }
    }
}

struct SettlementEnvironment {
    source: VecDeque<SettlementEvent>,
    ordinary: Option<SettlementEvent>,
    execution_trace: Vec<ExecutionEvent>,
}

impl SettlementEnvironment {
    fn take_source(&mut self) -> Option<SettlementEvent> {
        let event = self.source.pop_front();
        if let Some(EventLayer::Owned(value)) = &event {
            self.execution_trace
                .push(ExecutionEvent::DeliveredSource(*value));
        }
        event
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Settlement(VecDeque<u8>);

impl ClassifySettlement for Settlement {
    fn settlement_status(&self) -> SettlementStatus {
        SettlementStatus::Accepted
    }
}

impl ActiveEnvironment<SettlementActor> for SettlementEnvironment {
    type Settlement = Settlement;
    type Residual = Vec<ExecutionEvent>;
    type RetirementRequest = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> ControlFlow<Never, Option<SettlementEvent>> {
        ControlFlow::Continue({
            if let Some(event) = self.ordinary.take() {
                self.execution_trace.push(ExecutionEvent::RequestedOrdinary);
                self.execution_trace.push(ExecutionEvent::DeliveredOrdinary);
                return ControlFlow::Continue(Some(event));
            }
            self.execution_trace.push(ExecutionEvent::RequestedSource);
            self.take_source()
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next_source(&mut self) -> ControlFlow<Never, Option<SettlementEvent>> {
        ControlFlow::Continue({
            self.execution_trace.push(ExecutionEvent::RequestedSource);
            self.take_source()
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(
        &mut self,
        actions: ActionsOf<SettlementActor>,
    ) -> Interpretation<Self::Settlement> {
        let mut settlements = VecDeque::new();
        for value in actions.sends {
            self.execution_trace.push(ExecutionEvent::Committed(value));
            settlements.push_back(value);
        }
        assert_eq!(actions.creates, Creations::empty());
        match actions.become_ {
            Step::Continue => self.execution_trace.push(ExecutionEvent::Continued),
            Step::Stop(behavior::Stopped) => self.execution_trace.push(ExecutionEvent::Stopped),
            Step::Goto(never) => match never {},
        }
        Interpretation::Complete(Settlement(settlements))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn offer_next(
        &mut self,
        mut settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement> {
        let Some(value) = settlement.0.pop_front() else {
            return SourceCustody::Exhausted(settlement);
        };
        self.source.push_back(EventLayer::Owned(value));
        SourceCustody::Admitted(settlement)
    }

    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()> {
        ControlFlow::Continue(())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer owned retirement trace transfer until the future is polled."
    )]
    async fn retire(mut self, settlements: Vec<Self::Settlement>) -> Self::Residual {
        assert_eq!(settlements, [Settlement(VecDeque::new())]);
        self.execution_trace.push(ExecutionEvent::Retired);
        self.execution_trace
    }
}

impl Environment<SettlementActor> for SettlementEnvironment {
    type Active = Self;
    type Settlement = Settlement;
    type Error = Infallible;
    type Residual = Vec<ExecutionEvent>;
    type RetirementRequest = Never;

    async fn activate(
        mut self,
        actions: ActionsOf<SettlementActor>,
    ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
    {
        let interpretation = ActiveEnvironment::apply(&mut self, actions).await;
        Ok((self, interpretation))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer owned retirement trace transfer until the future is polled."
    )]
    async fn retire(self) -> Self::Residual {
        self.execution_trace
    }
}

#[tokio::test]
async fn transitive_source_results_settle_before_ordinary_input() {
    let retirement = Driver::new(
        SettlementActor {
            delivered_inputs: Vec::new(),
        },
        SettlementEnvironment {
            source: VecDeque::new(),
            ordinary: Some(EventLayer::Inner(User::new(MailAddr(9), ()))),
            execution_trace: Vec::new(),
        },
    )
    .run()
    .await;

    assert_eq!(retirement.disposition, Ok(Completion::Stopped));
    assert_eq!(
        retirement.behavior.delivered_inputs,
        [
            SettlementInput::Source(1),
            SettlementInput::Source(2),
            SettlementInput::Ordinary
        ]
    );
    assert_eq!(
        retirement.residual,
        [
            ExecutionEvent::Committed(1),
            ExecutionEvent::Continued,
            ExecutionEvent::RequestedSource,
            ExecutionEvent::DeliveredSource(1),
            ExecutionEvent::Committed(2),
            ExecutionEvent::Continued,
            ExecutionEvent::RequestedSource,
            ExecutionEvent::DeliveredSource(2),
            ExecutionEvent::Continued,
            ExecutionEvent::RequestedOrdinary,
            ExecutionEvent::DeliveredOrdinary,
            ExecutionEvent::Stopped,
            ExecutionEvent::Retired,
        ]
    );
}
