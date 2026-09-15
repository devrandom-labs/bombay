use bombay::prelude::{MailAddr, Protocol};
use bombay::{ActorSpace, ActorSpaces, App, Hosts};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(ActorSpaces)]
struct LocalActors {
    orders: ActorSpace<Orders>,
}

fn require_host<T: Hosts<Orders>>(_: &T) {}

fn main() {
    let actors = LocalActors {
        orders: ActorSpace::new(),
    };
    require_host(&actors);
    let _ = App::new((), actors);
}
