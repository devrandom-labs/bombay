use std::cell::Cell;
use std::convert::Infallible;
use std::rc::Rc;

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, User};
use bombay_engine::{ActionsOf, ActiveEnvironment, Driver, Environment};

struct SendNotSync(Cell<u8>);

impl Behavior for SendNotSync {
    type Addr = MailAddr;
    type Msg = Box<u8>;
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

impl ActiveEnvironment<SendNotSync> for Env {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<SendNotSync as Behavior>::Event> {
        self.local.set(1);
        self.event
            .take()
            .map(|value| User::new(MailAddr(1), value))
    }

    async fn apply(&mut self, _: ActionsOf<SendNotSync>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(self) {}
}

impl Environment<SendNotSync> for Env {
    type Active = Self;
    type Error = Infallible;

    async fn activate(mut self, actions: ActionsOf<SendNotSync>) -> Result<Self, Self::Error> {
        self.apply(actions).await?;
        Ok(self)
    }
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
