use bombay::{ExternalActor, MailAddr};
use bombay::behavior::MessageProtocol;

type Replies = MessageProtocol<MailAddr, u64>;

fn duplicate(actor: ExternalActor<Replies>) {
    let _duplicate = actor.clone();
}

fn main() {}
