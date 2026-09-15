use core::time::Duration;

use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, Never, NoBirths, OneShot, Protocol,
    StopOnShutdown, User,
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

fn expects_timer_then_shutdown(_: StopOnShutdown<OneShot<Root>>) {}

fn main() {
    let shutdown_then_timer = Root
        .stop_on_shutdown()
        .with_one_shot(TimerId(1), Duration::from_secs(1), |_| Actions::stop());

    expects_timer_then_shutdown(shutdown_then_timer);
}
