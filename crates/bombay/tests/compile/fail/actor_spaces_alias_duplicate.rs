use bombay::prelude::*;
use bombay::{ActorSpace, ActorSpaces};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

type OrderSpace = ActorSpace<Orders>;

#[derive(ActorSpaces)]
struct Actors {
    #[actor_space(Orders)]
    primary: OrderSpace,
    #[actor_space(Orders)]
    duplicate: ActorSpace<Orders>,
}

fn main() {}
