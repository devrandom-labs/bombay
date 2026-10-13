fn request(application: bombay::ApplicationHandle<
    bombay::behavior::MessageProtocol<bombay::MailAddr, u8>,
    bombay::behavior::User<bombay::MailAddr, u8>,
>) {
    let _request = application.request_shutdown();
}

fn main() {}
