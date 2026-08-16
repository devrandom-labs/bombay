use std::convert::Infallible;

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, User};
use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};

struct Definition;

impl Behavior for Definition {
    type Addr = MailAddr;
    type Msg = Never;
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

impl Environment<Definition> for Prepared {
    type Active = Active;
    type Error = Infallible;

    async fn activate(self, _: ActionsOf<Definition>) -> Result<Active, Self::Error> {
        Ok(Active)
    }
}

impl ActiveEnvironment<Definition> for Active {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<Definition as Behavior>::Event> {
        None
    }

    async fn apply(&mut self, _: ActionsOf<Definition>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(self) {}
}

fn main() {
    let mut prepared = Prepared;
    let _ = prepared.next();

    let active = Active;
    let _ = active.activate(Actions::stop());
}
