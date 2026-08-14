use bombay::behavior::{Actions, Delivery, Exit, Handler, MailAddr, Never, NoBirths, Pure};
use bombay::{Actor, AddressRouter, MailboxConfig, RunExit, System, TaskOutcome};

struct StopOnMessage;

impl Handler<String> for StopOnMessage {
    type Addr = MailAddr;
    type Msg = String;

    fn receive(
        &mut self,
        from: MailAddr,
        message: String,
    ) -> bombay::behavior::Acted<
        Self::Addr,
        Never,
        Vec<Delivery<Self::Addr, String>>,
        NoBirths,
        Never,
    > {
        println!("{} says {message}", from.0);
        Ok(Actions::stop(Exit::Normal))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(32), router);

    // `spawn` returns the affine `Handle`; `actor_ref` is the clonable,
    // typed `ActorRef` used for delivery.
    let handle = system
        .spawn(Actor::new(MailAddr(1), Pure::new(StopOnMessage)))
        .expect("the address is vacant");
    let actor_ref = handle.actor_ref().clone();

    actor_ref
        .send(MailAddr(2), "hello from bombay".to_owned())
        .await
        .expect("the actor is accepting messages");

    assert!(matches!(
        handle.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}
