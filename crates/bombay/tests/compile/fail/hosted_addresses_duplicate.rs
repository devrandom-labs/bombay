use bombay::prelude::*;
use bombay::{LocalAddresses, HostedAddresses};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

#[derive(HostedAddresses)]
struct Actors {
    primary: LocalAddresses<Orders>,
    duplicate: LocalAddresses<Orders>,
}

fn main() {}
