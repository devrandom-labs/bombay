use core::ops::ControlFlow;
use std::cell::Cell;
use std::convert::Infallible;
use std::rc::Rc;

use behavior::{
    Actions, Behavior, BehaviorActed, ClassifySettlement, Interpretation, MailAddr, Never,
    NoBirths, SettlementStatus, SourceCustody, User,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Driver, Environment};

struct SendNotSync(Cell<u8>);

impl Behavior for SendNotSync {
    type Protocol = behavior::MessageProtocol<MailAddr, Box<u8>>;
    type Event = User<MailAddr, Box<u8>>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.0.set(*event.message);
        Ok(Actions::stop())
    }
}

struct Env {
    event: Option<Box<u8>>,
    local: Rc<Cell<u8>>,
}

struct Settlement;

impl ClassifySettlement for Settlement {
    fn settlement_status(&self) -> SettlementStatus {
        SettlementStatus::Accepted
    }
}

impl ActiveEnvironment<SendNotSync> for Env {
    type Settlement = Settlement;
    type Residual = ();
    type RetirementRequest = Never;

    async fn next(&mut self) -> ControlFlow<Never, Option<<SendNotSync as Behavior>::Event>> {
        ControlFlow::Continue({
            self.local.set(1);
            self.event.take().map(|value| User::new(MailAddr(1), value))
        })
    }

    async fn next_source(
        &mut self,
    ) -> ControlFlow<Never, Option<<SendNotSync as Behavior>::Event>> {
        ControlFlow::Continue(None)
    }

    async fn apply(&mut self, _: ActionsOf<SendNotSync>) -> Interpretation<Self::Settlement> {
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

impl Environment<SendNotSync> for Env {
    type Active = Self;
    type Settlement = Settlement;
    type Error = Infallible;
    type Residual = ();
    type RetirementRequest = Never;

    async fn activate(
        self,
        _: ActionsOf<SendNotSync>,
    ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
    {
        Ok((self, Interpretation::Complete(Settlement)))
    }

    async fn retire(self) {}
}

fn main() {
    let _driver = Driver::new(
        SendNotSync(Cell::new(0)),
        Env {
            event: Some(Box::new(1)),
            local: Rc::new(Cell::new(0)),
        },
    );
}
