use bombay::prelude::*;

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

fn main() {
    let _: Option<ActorSpace<Orders>> = None;
}
