use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, Never, NoBirths, Protocol, StashRoute, User,
};
use bombay::prelude::*;

struct Phased;

impl Protocol for Phased {
    type Addr = MailAddr;
    type Msg = ();
}

impl Behavior for Phased {
    type Protocol = Self;
    type Event = User<MailAddr, ()>;
    type Sends = Vec<Never>;
    type Ph = u8;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

const fn deliver(_: &()) -> StashRoute {
    StashRoute::Deliver
}

fn main() {
    let _ = Phased.with_stash(deliver);
}
