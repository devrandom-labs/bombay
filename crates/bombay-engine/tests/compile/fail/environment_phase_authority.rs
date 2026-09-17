use std::convert::Infallible;

use behavior::{
    Actions, Behavior, BehaviorActed, ClassifySettlement, Interpretation, MailAddr, Never,
    NoBirths, SettlementStatus, SourceCustody, User,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};

struct Definition;

impl Behavior for Definition {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct Active;
struct Prepared;
struct Settlement;

impl ClassifySettlement for Settlement {
    fn settlement_status(&self) -> SettlementStatus {
        SettlementStatus::Accepted
    }
}

impl Environment<Definition> for Prepared {
    type Active = Active;
    type Settlement = Settlement;
    type Error = Infallible;
    type Residual = ();

    async fn activate(
        self,
        _: ActionsOf<Definition>,
    ) -> Result<
        (Self::Active, Interpretation<Self::Settlement>),
        (Self::Error, Self::Residual),
    > {
        Ok((Active, Interpretation::Complete(Settlement)))
    }

    async fn retire(self) {}
}

impl ActiveEnvironment<Definition> for Active {
    type Settlement = Settlement;
    type Residual = ();

    async fn next(&mut self) -> Option<<Definition as Behavior>::Event> {
        None
    }

    async fn next_source(&mut self) -> Option<<Definition as Behavior>::Event> {
        None
    }

    async fn apply(&mut self, _: ActionsOf<Definition>) -> Interpretation<Self::Settlement> {
        Interpretation::Complete(Settlement)
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

fn main() {
    let mut prepared = Prepared;
    let _ = prepared.next();

    let active = Active;
    let _ = active.activate(Actions::stop());
}
