use bombay::Application;
use bombay::actors::ActorExt;
use bombay::remote::Network;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

use counter::Counter;

async fn close_running_node(network: Network) {
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("the node opens"));
    let (execution, receiving) = node
        .execute_with(|application| async move { application.shutdown() })
        .unwrap_or_else(|_| panic!("the declared actor work is issued"));
    let _closed = node.close().await;
    let _received = tokio::join!(execution, receiving);
}

fn main() {}
