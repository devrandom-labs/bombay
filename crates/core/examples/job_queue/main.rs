//! Runnable job-queue demo — bombay's M1 compositional example (card #218).
//!
//! Twenty jobs through a supervised worker pool: two poison (panic), one
//! failing (typed error), the rest complete. Watch the restarts in the
//! tracing output, then read the drain report.
//!
//! Run: `cargo run -p bombay --example job_queue`

mod app;

use std::{sync::Arc, time::Duration};

use app::{
    DISPATCHER_NAME, Dispatcher, DispatcherConfig, DispatcherMsg, Job, JobKind, OverseerMsg,
};
use bombay::{
    registry::Registry,
    restart::{RestartConfig, RestartPolicy},
};

#[expect(
    clippy::print_stdout,
    clippy::expect_used,
    reason = "a demo binary narrates its results and fails loudly"
)]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init(); // bombay only emits; the app subscribes

    let registry = Arc::new(Registry::new());
    let app = app::start(DispatcherConfig {
        workers: 3,
        queue_cap: 32,
        retry_cap: 2,
        retry_backoff: Duration::from_millis(50),
        restart: RestartConfig::new(RestartPolicy::Permanent)
            .with_min_backoff(Duration::from_millis(10))
            .with_max_restarts(20),
        registry: Arc::clone(&registry),
    })
    .await;

    // clients never hold the spawn handle — they resolve by name
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    for id in 0..20u64 {
        let kind = match id {
            7 | 11 => JobKind::Poison,
            13 => JobKind::Fail,
            _ => JobKind::Ok(Duration::from_millis(20)),
        };
        dispatcher
            .ask(|reply| DispatcherMsg::Submit {
                job: Job { id, kind },
                reply,
            })
            .await
            .expect("submit accepted");
    }

    let report = dispatcher
        .ask(|reply| DispatcherMsg::Drain { reply })
        .no_timeout()
        .await
        .expect("drain reply");
    println!("drained: {report:?}");

    // give the death notice a moment to reach the overseer
    tokio::time::sleep(Duration::from_millis(100)).await;
    let observed = app
        .overseer
        .ask(|reply| OverseerMsg::Observed { reply })
        .await
        .expect("overseer reply");
    println!("overseer saw dispatcher exit: {observed:?}");
}
