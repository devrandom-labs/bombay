use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, Never, NoBirths, Protocol, User,
};
use bombay::{composition::StashRoute, prelude::*};

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

const fn stash(_: &Phased, _: &()) -> StashRoute {
    StashRoute::Stash
}

fn main() {
    let refused = bombay::behavior::Recipient::<
        bombay::behavior::MessageProtocol<MailAddr, User<MailAddr, ()>>,
    >::global(MailAddr::APPLICATION_ROOT);
    let _ = Phased.with_stash(2, refused, stash);
}
