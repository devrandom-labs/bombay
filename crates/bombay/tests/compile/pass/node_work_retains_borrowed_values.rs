use bombay::Application;
use bombay::actors::ActorExt;
use bombay::remote::Network;
use std::cell::Cell;
use std::rc::Rc;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

use counter::Counter;

async fn joined_work(network: Network, borrowed: &Cell<usize>, original: Rc<Vec<u8>>) {
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("the node opens"));
    let outcome = node
        .run_with(|application| async move {
            let shutdown = application.request_shutdown();
            (borrowed, original, shutdown)
        })
        .await
        .unwrap_or_else(|_| panic!("actor work is admitted"));
    let _retained = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original caller-local values and retirement return"));
    let _closed = node.close().await;
}

async fn paired_work(network: Network, borrowed: &Cell<usize>, original: Rc<Vec<u8>>) {
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("the node opens"));
    let (execution, receiving) = node
        .execute_with(|application| async move {
            let shutdown = application.request_shutdown();
            (borrowed, original, shutdown)
        })
        .unwrap_or_else(|_| panic!("actor work is issued"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let _retained = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original caller-local values and retirement return"));
    let _closed = node.close().await;
}

fn main() {}
