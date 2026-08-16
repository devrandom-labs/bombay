use std::convert::Infallible;

use bombay::behavior::MailAddr;
use bombay::{Effect, LocalActors};

struct Printer {
    messages: u64,
}

#[bombay::actor]
impl Printer {
    fn receive(&mut self, from: MailAddr, message: String) -> Effect<String> {
        self.messages += 1;
        let message = message.into_boxed_str();
        Effect::send(format!("{} says {message} ({})", from.0, self.messages)).stop()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let actors = LocalActors::<Printer>::new(32);
    let printer = actors
        .spawn(MailAddr(1), Printer { messages: 0 }, |actions| async move {
            for line in actions.sends {
                println!("{line}");
            }
            Ok::<_, Infallible>(())
        })
        .await
        .expect("printer should become live");

    printer
        .send(MailAddr(2), "hello from Bombay".to_owned())
        .await
        .expect("printer should accept the message");

    while actors.resolve(&MailAddr(1)).is_some() {
        tokio::task::yield_now().await;
    }
}
