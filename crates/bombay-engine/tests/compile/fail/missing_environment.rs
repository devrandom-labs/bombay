use std::convert::Infallible;

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, User};
use bombay_engine::Driver;

struct Example;

impl Behavior for Example {
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

fn main() {
    let _ = Driver::new(Example, ()).run();
}
