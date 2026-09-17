//! DX37 closure: the ordinary application runtime must refuse to host a
//! behavior whose sends product contains a logical `Delivery` lane.
//!
//! Established-recipient delivery is the only internal delivery path; logical
//! recipients remain a namespace-boundary concern for explicitly selected
//! transport or discovery interpreters. This fixture self-hosts the root's own
//! protocol, so the retired transitional resolver (`ActorSpace`/`Hosts`/
//! `ResolveLogical`) used to satisfy the logical lane and let this program
//! compile; the denial must now be static.

use core::convert::Infallible;

use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, BehaviorSettlements, InitializationTurn, Never,
    NoBirths, Protocol, User,
};
use bombay::prelude::*;

struct Ping;

impl Protocol for Ping {
    type Addr = MailAddr;
    type Msg = u8;
}

struct LogicalPinger;

impl Behavior for LogicalPinger {
    type Protocol = Ping;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Delivery<Ping>>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::send(vec![Delivery::new(
            Recipient::global(MailAddr::APPLICATION_ROOT),
            7,
        )]))
    }

    fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

impl BehaviorBase for LogicalPinger {
    type Base = Self;

    fn base(&self) -> &Self::Base {
        self
    }
}

#[derive(TerminalProjection)]
enum RunTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: ActorOrigin<R>,
        terminal: ActorRetirement<R, Self>,
    },
}

fn main() {
    let _terminal: RunTerminal<_> = Application::new(LogicalPinger.stop_on_shutdown())
        .run()
        .expect("the run boundary is reached");
}
