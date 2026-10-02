#![deny(unused_must_use)]

use bombay::behavior::{ActiveTurn, Behavior, BehaviorActed, Never, NoBirths, Protocol, User};
use bombay::prelude::{ActorRetirement, ActorExt, MailAddr};

struct Root;

impl Protocol for Root {
    type Addr = MailAddr;
    type Msg = Never;
}

impl Behavior for Root {
    type Protocol = Self;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

fn discard_terminal(terminal: ActorRetirement<Root, ()>) {
    terminal;
}

fn main() {
    Root.stop_on_shutdown();
    let _ = discard_terminal;
}
