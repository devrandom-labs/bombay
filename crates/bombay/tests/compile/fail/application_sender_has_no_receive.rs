async fn receive(mut sender: bombay::ApplicationSender<
    bombay::behavior::EstablishedRecipient<
        bombay::behavior::MessageProtocol<bombay::MailAddr, u8>,
    >,
>) {
    let _reply = sender.receive().await;
}

fn main() {}
