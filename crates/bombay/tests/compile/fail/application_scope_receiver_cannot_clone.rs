fn main() {
    let scope = bombay::ApplicationScope::new();
    let receiver = scope.external::<bombay::behavior::MessageProtocol<bombay::MailAddr, u64>>().expect("early endpoint");
    let _duplicate = receiver.clone();
}
