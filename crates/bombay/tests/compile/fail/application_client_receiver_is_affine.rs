fn duplicate(client: bombay::ApplicationClient<
    bombay::behavior::EstablishedRecipient<
        bombay::behavior::MessageProtocol<bombay::MailAddr, u8>,
    >,
    bombay::behavior::MessageProtocol<bombay::MailAddr, u64>,
>) {
    let _duplicate = client.clone();
}

fn main() {}
