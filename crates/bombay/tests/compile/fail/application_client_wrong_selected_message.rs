async fn send(
    client: bombay::ApplicationClient<
        bombay::behavior::EstablishedRecipient<
            bombay::behavior::MessageProtocol<bombay::MailAddr, u16>,
        >,
        bombay::behavior::MessageProtocol<bombay::MailAddr, u64>,
    >,
) {
    let _admission = client.send(String::from("another selected protocol")).await;
}

fn main() {}
