use bombay::prelude::*;
use bombay::{ActorSpace, ActorSpaces};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(ActorSpaces)]
struct Actors(ActorSpace<Orders>);

fn main() {}
