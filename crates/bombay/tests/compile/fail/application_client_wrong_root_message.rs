async fn send(client: bombay::ApplicationClient<
    bombay::behavior::EstablishedRecipient<
        bombay::behavior::MessageProtocol<bombay::MailAddr, u8>,
    >,
    bombay::behavior::MessageProtocol<bombay::MailAddr, u64>,
>) {
    let _admission = client.send(String::from("another protocol")).await;
}

fn main() {}
