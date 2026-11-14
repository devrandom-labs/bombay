use bombay::Application;
use bombay::actors::ActorExt;
use bombay::behavior::Never;
use bombay::remote::Network;
use serde::Serialize;

#[path = "../../../../../examples/counter/src/counter.rs"]
#[expect(dead_code, reason = "reuse the actual local Counter protocol")]
mod counter;

async fn configured<Command: Serialize>(network: Network, command: Command) {
    let node = Application::new(counter::Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("the supplied network opens"));
    // Serialization alone does not declare an operation, provider or typed
    // remote interpreter for this actual local Counter.
    let mut node = node.exports(command, async |application| {
        Ok::<_, Never>(application.root().established_recipient())
    });
    let outcome = node.run().await;
    drop((outcome, node));
}

fn main() {
    let _construction = configured::<serde_json::Value>;
}
