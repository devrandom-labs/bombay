fn shutdown(client: bombay::ApplicationClient<
    bombay::behavior::EstablishedRecipient<
        bombay::behavior::MessageProtocol<bombay::MailAddr, u8>,
    >,
    bombay::behavior::MessageProtocol<bombay::MailAddr, u64>,
>) {
    let _shutdown = client.shutdown();
}

fn main() {}
