use bombay::prelude::*;
use bombay::ActorSpaces;

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(ActorSpaces)]
struct Actors {
    #[actor_space(Orders)]
    orders: u16,
}

fn main() {}
