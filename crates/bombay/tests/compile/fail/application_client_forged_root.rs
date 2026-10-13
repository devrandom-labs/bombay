fn forge(
    root: bombay::behavior::EstablishedRecipient<
        bombay::behavior::MessageProtocol<bombay::MailAddr, u8>,
    >,
    caller: bombay::ExternalActor<
        bombay::behavior::MessageProtocol<bombay::MailAddr, u64>,
    >,
) {
    let _forged = bombay::ApplicationClient { target: root, caller };
}

fn main() {}
