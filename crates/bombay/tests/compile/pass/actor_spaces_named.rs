use bombay::prelude::*;
use bombay::{ActorSpace, ActorSpaces, Hosts};

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

#[derive(ActorSpaces)]
struct Actors {
    renamed_first_field: ActorSpace<First>,
    ignored_configuration: u16,
    second: bombay::ActorSpace<Second>,
}

fn require_hosts<P: Protocol<Addr = MailAddr>, A: Hosts<P>>(_: &A) {}

fn main() {
    let actors = Actors {
        renamed_first_field: ActorSpace::default(),
        ignored_configuration: 7,
        second: ActorSpace::default(),
    };
    require_hosts::<First, _>(&actors);
    require_hosts::<Second, _>(&actors);
    assert_eq!(actors.ignored_configuration, 7);
}
