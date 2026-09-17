use bombay::prelude::*;
use bombay::{LocalAddresses, HostedAddresses, HostedAddresses};

struct First;

impl Protocol for First {
    type Addr = MailAddr;
    type Msg = ();
}

struct Second;

impl Protocol for Second {
    type Addr = MailAddr;
    type Msg = u8;
}

#[derive(HostedAddresses)]
struct Actors {
    renamed_first_field: LocalAddresses<First>,
    ignored_configuration: u16,
    second: bombay::LocalAddresses<Second>,
}

fn require_hosted<P: Protocol<Addr = MailAddr>, A: HostedAddresses<P>>(_: &A) {}

fn main() {
    let actors = Actors {
        renamed_first_field: LocalAddresses::default(),
        ignored_configuration: 7,
        second: LocalAddresses::default(),
    };
    require_hosted::<First, _>(&actors);
    require_hosted::<Second, _>(&actors);
    assert_eq!(actors.ignored_configuration, 7);
}
