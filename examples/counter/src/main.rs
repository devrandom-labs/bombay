//! For learning direct Level-1 actor authoring through one stateful counter:
//! messages, state, typed reply, domain error, and transition remain together.

use bombay::prelude::*;

mod counter;
use crate::counter::{Counter, CounterMessage};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let mut caller = application
                .client()
                .expect("the counter customer is established");
            caller
                .send(CounterMessage::Increment)
                .await
                .expect("increment admitted");
            caller
                .send(CounterMessage::Read(caller.recipient()))
                .await
                .expect("read admitted");
            let reply = caller.receive().await.expect("counter reply");
            application.shutdown().expect("shutdown admitted");
            reply.message
        })
        .await
        .unwrap_or_else(|_| panic!("the counter application starts"));
    let (value, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the counter work completes"));
    println!("counter: {value}");
    // Execution already performed cleanup; these reports are optional inspection.
    println!(
        "cleanup: {:?}; notifications: {:?}",
        retirement.cleanup(),
        retirement.notifications()
    );
    let (origin, root) = retirement
        .retirement()
        .as_ref()
        .expect("the original counter retirement remains available");
    println!("actor {:?}: {:?}", origin.address(), root.completion());
}
