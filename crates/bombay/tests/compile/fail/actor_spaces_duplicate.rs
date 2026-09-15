use bombay::prelude::*;
use bombay::{ActorSpace, ActorSpaces};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(ActorSpaces)]
struct Actors {
    primary: ActorSpace<Orders>,
    duplicate: ActorSpace<Orders>,
}

fn main() {}
