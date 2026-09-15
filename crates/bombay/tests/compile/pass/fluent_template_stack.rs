use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorActed, Never, NoBirths, Protocol, StashRoute, User,
};
use bombay::prelude::*;

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

const fn route(message: &Never) -> StashRoute {
    match *message {}
}

fn main() {
    let root = Root.with_stash(route).stop_on_shutdown();
    let _application = Application::new(root);
}
