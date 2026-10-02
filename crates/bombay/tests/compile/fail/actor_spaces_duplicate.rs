use bombay::prelude::*;
use bombay::{ActorSpace, ActorSpaces};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(ActorSpaces)]
struct Actors {
    #[actor_space(Orders)]
    primary: ActorSpace<Orders>,
    #[actor_space(Orders)]
    duplicate: ActorSpace<Orders>,
}

fn main() {}
