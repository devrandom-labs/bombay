use bombay::prelude::{MailAddr, Protocol};
use bombay::{ActorSpace, ActorSpaces, App, Hosts};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(ActorSpaces)]
struct OrderSpaces {
    #[actor_space(Orders)]
    orders: ActorSpace<Orders>,
}

fn require_host<T: Hosts<Orders>>(_: &T) {}

fn main() {
    let spaces = OrderSpaces {
        orders: ActorSpace::new(),
    };
    require_host(&spaces);
    let _ = App::new((), spaces);
}
