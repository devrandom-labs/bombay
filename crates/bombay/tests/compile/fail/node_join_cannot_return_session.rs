use bombay::{Application, ApplicationHandle};
use bombay::actors::ActorExt;
use bombay::behavior::Behavior;
use bombay::prelude::{ShutdownRejection, StopOnShutdown};
use zenoh::Session;
use zenoh_config::Config;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

use counter::Counter;

async fn return_session<'loan>(
    application: ApplicationHandle<
        <StopOnShutdown<Counter> as Behavior>::Protocol,
        <StopOnShutdown<Counter> as Behavior>::Event,
    >,
    session: &'loan Session,
) -> (&'loan Session, Result<(), ShutdownRejection>) {
    let shutdown = application.request_shutdown();
    (session, shutdown)
}

async fn borrowed_session(config: Config) {
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(config)
        .await
        .unwrap_or_else(|_| panic!("the node opens"));
    let _outcome = node.run_with(return_session).await;
}

fn main() {}
