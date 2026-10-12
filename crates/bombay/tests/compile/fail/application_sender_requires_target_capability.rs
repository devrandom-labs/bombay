fn select(
    application: bombay::ApplicationHandle<
        bombay::behavior::MessageProtocol<bombay::MailAddr, u8>,
        bombay::behavior::User<bombay::MailAddr, u8>,
    >,
) {
    let _sender = application.sender_to(bombay::MailAddr::APPLICATION_ROOT);
}

fn main() {}
