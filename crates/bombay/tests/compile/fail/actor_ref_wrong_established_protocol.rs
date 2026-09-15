use bombay::behavior::EstablishedRecipient;
use bombay::{ActorRef, MailAddr};

struct Primary;

impl bombay::behavior::Protocol for Primary {
    type Addr = MailAddr;
    type Msg = ();
}

struct Secondary;

impl bombay::behavior::Protocol for Secondary {
    type Addr = MailAddr;
    type Msg = ();
}

fn wrong_protocol(reference: &ActorRef<Primary>) -> EstablishedRecipient<Secondary> {
    reference.established_recipient()
}

fn main() {}
