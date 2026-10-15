use bombay::Application;
use bombay::actors::ActorExt;
use zenoh_config::Config;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

use counter::Counter;

async fn borrowed_session(config: Config) {
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(config)
        .await
        .unwrap_or_else(|_| panic!("the node opens"));
    let (execution, receiving) = node
        .execute_with(|application, session| async move {
            let shutdown = application.request_shutdown();
            (session, shutdown)
        })
        .unwrap_or_else(|_| panic!("the operation issues"));
    execution.await;
    let outcome = receiving.await;
    let ((session, _shutdown), _retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the work completes"));
    let _closed = node.close().await;
    assert!(!session.is_closed());
}

fn main() {}
