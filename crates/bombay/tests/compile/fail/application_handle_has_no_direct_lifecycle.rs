use bombay::prelude::*;

struct RootProtocol;

impl Protocol for RootProtocol {
    type Addr = MailAddr;
    type Msg = Never;
}

fn request_application_shutdown(application: ApplicationHandle<RootProtocol>) {
    let _accepted = application.request_shutdown();
}

fn main() {}
