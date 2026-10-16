use bombay::actors::ActorExt;
use bombay::behavior::Behavior;
use bombay::prelude::{ShutdownRejection, StopOnShutdown};
use bombay::remote::Network;
use bombay::{Application, ApplicationHandle};
use zenoh::Session;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

use counter::Counter;

async fn receive_session(
    application: ApplicationHandle<
        <StopOnShutdown<Counter> as Behavior>::Protocol,
        <StopOnShutdown<Counter> as Behavior>::Event,
    >,
    session: &Session,
) -> Result<(), ShutdownRejection> {
    assert!(!session.is_closed());
    application.request_shutdown()
}

async fn actor_work(network: Network) {
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("the node opens"));
    let _execution = node.execute_with(receive_session);
}

fn main() {}
