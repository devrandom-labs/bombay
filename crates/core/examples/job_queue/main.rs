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
    DISPATCHER_NAME, Dispatcher, DispatcherConfig, DispatcherMsg, Intake, IntakeMsg, Job, JobKind,
    OverseerMsg,
};
use bombay::{
    actor::Spawn,
    mailbox::Capacity,
    registry::Registry,
    restart::{RestartConfig, RestartPolicy},
    stash::Stashed,
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
        worker_stopped_tx: None,
        worker_grace: Duration::from_secs(5),
        audit: None,
    })
    .await;

    // clients never hold the spawn handle — they resolve by name
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    // Card #224: submissions go through a `Stashed<Intake>` front door — a
    // bounded-stash gate that can defer submits during maintenance. This
    // demo never pauses it; the deferral path is the app-level test.
    let intake = Stashed::<Intake>::spawn((
        dispatcher.clone(),
        Capacity::try_from(8usize).expect("valid intake stash capacity"),
    ));

    for id in 0..20u64 {
        let kind = match id {
            7 | 11 => JobKind::Poison,
            13 => JobKind::Fail,
            _ => JobKind::Ok(Duration::from_millis(20)),
        };
        intake
            .ask(|reply| IntakeMsg::Submit {
                job: Job { id, kind },
                reply,
            })
            .await
            .expect("submit accepted");
    }

    // Card #225 control-lane beat: a burst of submits is in flight through
    // the intake when a new worker is `supervise`d — the op rides the
    // unbounded control lane (ADR-0021), so it lands promptly instead of
    // queueing behind the burst.
    let mut burst = Vec::new();
    for id in 100..108u64 {
        let intake = intake.clone();
        burst.push(tokio::spawn(async move {
            intake
                .ask(|reply| IntakeMsg::Submit {
                    job: Job {
                        id,
                        kind: JobKind::Ok(Duration::from_millis(20)),
                    },
                    reply,
                })
                .await
        }));
    }
    let extra_worker_id = dispatcher
        .supervise(
            RestartConfig::new(RestartPolicy::Permanent)
                .with_min_backoff(Duration::from_millis(10))
                .with_max_restarts(20),
            {
                let disp = dispatcher.downgrade();
                let done_port = dispatcher.recipient::<app::Done>();
                move || {
                    let worker = app::Worker::spawn(app::WorkerArgs {
                        slot: 3,
                        dispatcher: done_port.clone(),
                        stopped_tx: None,
                    });
                    if let Some(d) = disp.upgrade() {
                        // same WorkerReplaced seam as on_start's factories
                        let _ = d
                            .tell(DispatcherMsg::WorkerReplaced {
                                slot: 3,
                                id: worker.id(),
                                worker: worker.recipient::<Job>(),
                            })
                            .try_send();
                    }
                    worker
                }
            },
        )
        .await
        .expect("supervise lands on the control lane, never behind the burst");
    println!("supervised extra worker {extra_worker_id:?} mid-burst (control lane)");
    for submit in burst {
        submit
            .await
            .expect("burst task")
            .expect("burst submit accepted");
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
