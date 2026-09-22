use bombay::prelude::*;

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

fn require_host<T: HostedAddresses<Orders>>() {}

fn main() {}
