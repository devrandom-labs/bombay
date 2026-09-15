use bombay::prelude::*;
use bombay::{ActorSpace, ActorSpaces, Hosts};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

struct Payments;

impl Protocol for Payments {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(ActorSpaces)]
struct Actors {
    orders: ActorSpace<Orders>,
}

fn require_hosts<P: Protocol<Addr = MailAddr>, A: Hosts<P>>(_: &A) {}

fn main() {
    let actors = Actors {
        orders: ActorSpace::default(),
    };
    require_hosts::<Payments, _>(&actors);
}
