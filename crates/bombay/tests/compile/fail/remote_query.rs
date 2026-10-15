use bombay::MailAddr;
use bombay::behavior::MessageProtocol;
use bombay::remote::Export;

async fn inspect(
    export: &mut Export<MessageProtocol<MailAddr, u8>, MessageProtocol<MailAddr, u64>, ()>,
) {
    let mut received = None;
    export.receive_query(&mut received).await;
    if let Some(Some((query, _permit))) = received {
        let native_key = query.key_expr();
        let _text = native_key.as_str();
    }
}

fn main() {}
