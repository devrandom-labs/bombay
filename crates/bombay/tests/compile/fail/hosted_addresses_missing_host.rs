use bombay::prelude::*;
use bombay::{LocalAddresses, HostedAddresses, HostedAddresses};

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

#[derive(HostedAddresses)]
struct Actors {
    orders: LocalAddresses<Orders>,
}

fn require_hosted<P: Protocol<Addr = MailAddr>, A: HostedAddresses<P>>(_: &A) {}

fn main() {
    let actors = Actors {
        orders: LocalAddresses::default(),
    };
    require_hosted::<Payments, _>(&actors);
}
