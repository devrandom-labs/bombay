use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use behavior::{Actions, Behavior, BehaviorActed, EventLayer, MailAddr, Never, NoBirths, User};
use behavior::{ClassifySettlement, Interpretation, SettlementStatus, SourceCustody};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, Driver, Environment};

type SettlementEvent = EventLayer<u8, User<MailAddr, ()>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fact {
    Committed(u8),
    RequestedSource,
    RequestedOrdinary,
    FoldedReturned(u8),
    FoldedOrdinary,
    Retired,
}

struct SettlementBehavior {
    facts: Arc<Mutex<Vec<Fact>>>,
}

impl Behavior for SettlementBehavior {
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
                self.facts.lock().unwrap().push(Fact::FoldedReturned(1));
                Ok(Actions::send(vec![2]))
            }
            EventLayer::Owned(2) => {
                self.facts.lock().unwrap().push(Fact::FoldedReturned(2));
                Ok(Actions::cont())
            }
            EventLayer::Owned(other) => panic!("unexpected settlement {other}"),
            EventLayer::Inner(_) => {
                self.facts.lock().unwrap().push(Fact::FoldedOrdinary);
                Ok(Actions::stop())
            }
        }
    }
}

struct SettlementEnvironment {
    source: VecDeque<SettlementEvent>,
    ordinary: Option<SettlementEvent>,
    facts: Arc<Mutex<Vec<Fact>>>,
}

#[derive(Debug, PartialEq, Eq)]
struct Settlement(VecDeque<u8>);

impl ClassifySettlement for Settlement {
    fn settlement_status(&self) -> SettlementStatus {
        SettlementStatus::Accepted
    }
}

impl ActiveEnvironment<SettlementBehavior> for SettlementEnvironment {
    type Settlement = Settlement;
    type Residual = ();

    async fn next(&mut self) -> Option<SettlementEvent> {
        if let Some(event) = self.ordinary.take() {
            self.facts.lock().unwrap().push(Fact::RequestedOrdinary);
            return Some(event);
        }
        self.facts.lock().unwrap().push(Fact::RequestedSource);
        self.source.pop_front()
    }

    async fn next_source(&mut self) -> Option<SettlementEvent> {
        self.facts.lock().unwrap().push(Fact::RequestedSource);
        self.source.pop_front()
    }

    async fn apply(
        &mut self,
        actions: ActionsOf<SettlementBehavior>,
    ) -> Interpretation<Self::Settlement> {
        let mut settlements = VecDeque::new();
        for value in actions.sends {
            self.facts.lock().unwrap().push(Fact::Committed(value));
            settlements.push_back(value);
        }
        Interpretation::Complete(Settlement(settlements))
    }

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

    fn publish(&mut self) {}

    async fn retire(self, settlements: Vec<Self::Settlement>) {
        assert_eq!(settlements, [Settlement(VecDeque::new())]);
        self.facts.lock().unwrap().push(Fact::Retired);
    }
}

impl Environment<SettlementBehavior> for SettlementEnvironment {
    type Active = Self;
    type Settlement = Settlement;
    type Error = Infallible;
    type Residual = ();

    async fn activate(
        mut self,
        actions: ActionsOf<SettlementBehavior>,
    ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
    {
        let interpretation = ActiveEnvironment::apply(&mut self, actions).await;
        Ok((self, interpretation))
    }

    async fn retire(self) {}
}

#[tokio::test]
async fn transitive_source_results_settle_before_ordinary_input() {
    let facts = Arc::new(Mutex::new(Vec::new()));
    let retirement = Driver::new(
        SettlementBehavior {
            facts: facts.clone(),
        },
        SettlementEnvironment {
            source: VecDeque::new(),
            ordinary: Some(EventLayer::Inner(User::new(MailAddr(9), ()))),
            facts: facts.clone(),
        },
    )
    .run()
    .await;

    assert_eq!(retirement.disposition, Ok(Completion::Stopped));
    assert_eq!(
        *facts.lock().unwrap(),
        [
            Fact::Committed(1),
            Fact::RequestedSource,
            Fact::FoldedReturned(1),
            Fact::Committed(2),
            Fact::RequestedSource,
            Fact::FoldedReturned(2),
            Fact::RequestedOrdinary,
            Fact::FoldedOrdinary,
            Fact::Retired,
        ]
    );
}
