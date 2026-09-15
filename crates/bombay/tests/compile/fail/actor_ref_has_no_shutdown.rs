use bombay::prelude::*;

struct RootProtocol;

impl Protocol for RootProtocol {
    type Addr = MailAddr;
    type Msg = Never;
}

fn request_application_shutdown(root: ActorRef<RootProtocol>) {
    let _accepted = root.request_shutdown();
}

fn main() {}
