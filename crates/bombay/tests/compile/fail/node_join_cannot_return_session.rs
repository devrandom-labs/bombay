use bombay::Application;
use bombay::actors::ActorExt;
use zenoh::Session;
use zenoh_config::Config;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

use counter::Counter;

async fn borrowed_session(config: Config) {
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(config)
        .await
        .unwrap_or_else(|_| panic!("the node opens"));
    let _outcome = node
        .run_with(async |application, session: &Session| {
            let shutdown = application.request_shutdown();
            (session, shutdown)
        })
        .await;
}

fn main() {}
