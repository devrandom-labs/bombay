use bombay::prelude::{MailAddr, Protocol};
use bombay::{LocalAddresses, HostedAddresses, App, HostedAddresses};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(HostedAddresses)]
struct LocalActors {
    orders: LocalAddresses<Orders>,
}

fn require_host<T: HostedAddresses<Orders>>(_: &T) {}

fn main() {
    let actors = LocalActors {
        orders: LocalAddresses::new(),
    };
    require_host(&actors);
    let _ = App::new((), actors);
}
