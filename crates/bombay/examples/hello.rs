use bombay::behavior::{Actions, Exit, MailAddr, Never, NoBirths};
use bombay::{Actor, AddressRouter, MailboxConfig, RunExit, System, TaskOutcome};

struct StopOnMessage;

#[bombay::behavior::behavior(addr = MailAddr, message = String, sends = Vec<Never>, births = NoBirths, error = Never)]
impl StopOnMessage {
    fn receive(
        &mut self,
        from: MailAddr,
        message: String,
    ) -> bombay::behavior::BehaviorActed<Self> {
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
        .spawn(Actor::new(MailAddr(1), StopOnMessage))
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
