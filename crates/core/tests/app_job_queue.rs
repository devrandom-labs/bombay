//! Card #218 — the M1 exit gate: the four cross-cutting test categories
//! applied at APP level over the job-queue mini-app.
//!
//! The app under test is the real example (`examples/job_queue/app.rs`),
//! included by path so the demo and the gate compile the same code.

#![expect(clippy::expect_used, reason = "test assertions fail loudly by design")]

#[path = "../examples/job_queue/app.rs"]
mod app;

use std::{future::IntoFuture, sync::Arc, time::Duration};

use app::{
    DISPATCHER_NAME, Dispatcher, DispatcherConfig, DispatcherMsg, DrainReport, Job, JobKind,
    SubmitError,
};
use bombay::{
    ActorId,
    error::AskError,
    registry::Registry,
    restart::{RestartConfig, RestartPolicy},
    test_support::terminate_bound,
};
use tokio::time::timeout;

const WORK: Duration = Duration::from_millis(5);

fn config(
    registry: &Arc<Registry>,
    worker_stopped_tx: Option<flume::Sender<ActorId>>,
) -> DispatcherConfig {
    DispatcherConfig {
        workers: 2,
        queue_cap: 8,
        retry_cap: 2,
        retry_backoff: Duration::from_millis(5),
        restart: RestartConfig::new(RestartPolicy::Permanent)
            .with_min_backoff(Duration::from_millis(1))
            .with_max_backoff(Duration::from_millis(5))
            .with_max_restarts(50)
            .with_max_total(200),
        registry: Arc::clone(registry),
        worker_stopped_tx,
    }
}

fn config_no_seam(registry: &Arc<Registry>) -> DispatcherConfig {
    config(registry, None)
}

/// Every terminal await is bounded — a hung app flow must fail the test, not
/// stall the mutants/MIRI lanes.
async fn bounded<F: IntoFuture>(fut: F) -> F::Output {
    timeout(terminate_bound(), fut)
        .await
        .expect("app flow must resolve within the terminate bound")
}

/// Poll the overseer until it records the dispatcher's death or the global
/// terminate bound expires. Drain can take >1s (three 500ms jobs on one worker),
/// so a fixed small iteration count is not enough.
async fn poll_observed_death(app: &app::App) -> Option<(ActorId, bool)> {
    let deadline = tokio::time::Instant::now() + terminate_bound();
    while tokio::time::Instant::now() < deadline {
        let seen = bounded(
            app.overseer
                .ask(|reply| app::OverseerMsg::Observed { reply }),
        )
        .await
        .expect("overseer reply");
        if seen.is_some() {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

const fn ok_job(id: u64) -> Job {
    Job {
        id,
        kind: JobKind::Ok(WORK),
    }
}

#[tokio::test]
async fn sequence_submit_stats_drain_reports_exact_counts() {
    let registry = Arc::new(Registry::new());
    let (worker_stopped_tx, worker_stopped_rx) = flume::unbounded::<ActorId>();
    let app = app::start(config(&registry, Some(worker_stopped_tx))).await;
    // clients resolve the dispatcher by NAME — the registry seam is load-bearing
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    for id in 0..8u64 {
        bounded(dispatcher.ask(|reply| DispatcherMsg::Submit {
            job: ok_job(id),
            reply,
        }))
        .await
        .expect("submit accepted under cap");
    }

    let stats = bounded(dispatcher.ask(|reply| DispatcherMsg::Stats { reply }))
        .await
        .expect("stats reply");
    assert_eq!(stats.submitted, 8);

    let report = bounded(
        dispatcher
            .ask(|reply| DispatcherMsg::Drain { reply })
            .no_timeout(),
    )
    .await
    .expect("drain reply");
    assert_eq!(
        report,
        DrainReport {
            submitted: 8,
            completed: 8,
            failed: 0,
            retried: 0,
            rebuilds: 0
        },
        "every submitted job completed exactly once on the happy path",
    );

    // The supervisor's own exit tears down its children (ADR-0019); wait for
    // the dispatcher death to be observed, then assert every worker reported
    // its stop through the example-level seam.
    let observed = poll_observed_death(&app).await;
    assert!(
        observed.is_some(),
        "the overseer must observe the dispatcher's death"
    );
    let mut seen = Vec::new();
    for _ in 0..2 {
        let id = bounded(worker_stopped_rx.recv_async())
            .await
            .expect("worker must report its stop");
        assert!(!seen.contains(&id), "duplicate worker stop signal: {id:?}");
        seen.push(id);
    }
    assert_eq!(seen.len(), 2, "both workers stopped");
}

#[allow(
    clippy::too_many_lines,
    reason = "lifecycle test is intentionally one sequential scenario"
)]
#[tokio::test]
async fn lifecycle_crash_rebuild_requeue_no_job_lost() {
    let registry = Arc::new(Registry::new());
    let app = app::start(config_no_seam(&registry)).await;
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    // the initial WorkerReplaced announcements race the first Stats ask —
    // poll until both slots are in the roster
    let mut before_stats = None;
    for _ in 0..200 {
        let stats = bounded(dispatcher.ask(|reply| DispatcherMsg::Stats { reply }))
            .await
            .expect("stats reply");
        if stats.worker_ids.len() == 2 {
            before_stats = Some(stats);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let before =
        before_stats.expect("both slots must announce via WorkerReplaced within the bound");

    // 6 completable jobs + 2 always-crashing jobs (one Err, one panic)
    for id in 0..6u64 {
        bounded(dispatcher.ask(|reply| DispatcherMsg::Submit {
            job: ok_job(id),
            reply,
        }))
        .await
        .expect("submit accepted");
    }
    for job in [
        Job {
            id: 100,
            kind: JobKind::Fail,
        },
        Job {
            id: 101,
            kind: JobKind::Poison,
        },
    ] {
        bounded(dispatcher.ask(|reply| DispatcherMsg::Submit { job, reply }))
            .await
            .expect("submit accepted");
    }

    // wait until the crashes have produced at least one rebuild, then assert
    // the roster carries a FRESH ActorId (a rebuilt child is a new actor)
    let mut after_stats = None;
    for _ in 0..200 {
        let stats = bounded(dispatcher.ask(|reply| DispatcherMsg::Stats { reply }))
            .await
            .expect("stats reply");
        if stats.rebuilds > 0 {
            after_stats = Some(stats);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let after = after_stats.expect("a crash must produce a rebuild within the bound");
    assert!(
        after
            .worker_ids
            .iter()
            .any(|id| !before.worker_ids.contains(id)),
        "a rebuilt worker must carry a fresh ActorId: before {:?} after {:?}",
        before.worker_ids,
        after.worker_ids,
    );

    let report = bounded(
        dispatcher
            .ask(|reply| DispatcherMsg::Drain { reply })
            .no_timeout(),
    )
    .await
    .expect("drain reply");

    // retry_cap = 2: each crashing job runs 3 times (initial + 2 retries)
    // then is recorded failed. Every crash rebuilds the worker (Permanent).
    assert_eq!(
        report,
        DrainReport {
            submitted: 8,
            completed: 6,
            failed: 2,
            retried: 4,
            rebuilds: 6
        },
        "at-least-once: no job lost, crashing jobs retried to cap then recorded",
    );

    // the dispatcher stopped normally after drain; the overseer must see it
    let dispatcher_id = dispatcher.id();
    let observed = poll_observed_death(&app).await;
    assert_eq!(
        observed,
        Some((dispatcher_id, true)),
        "death-watch: overseer observes the dispatcher's normal death",
    );
}

#[tokio::test]
async fn boundary_queue_full_draining_and_timeout_classified() {
    let registry = Arc::new(Registry::new());
    // one slow worker + tiny queue so rejection layers are reachable
    let mut cfg = config_no_seam(&registry);
    cfg.workers = 1;
    cfg.queue_cap = 2;
    let app = app::start(cfg).await;
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");
    let dispatcher_id = dispatcher.id();

    let slow = Duration::from_millis(500);
    // job 0 is dispatched immediately (outstanding); 1 and 2 fill pending
    for id in 0..3u64 {
        bounded(dispatcher.ask(|reply| DispatcherMsg::Submit {
            job: Job {
                id,
                kind: JobKind::Ok(slow),
            },
            reply,
        }))
        .await
        .expect("accepted up to cap");
    }

    // pending == queue_cap → typed app-level rejection, not a mailbox error
    let queue_full_err = bounded(dispatcher.ask(|reply| DispatcherMsg::Submit {
        job: ok_job(9),
        reply,
    }))
    .await
    .expect_err("queue is full");
    assert!(
        matches!(queue_full_err, AskError::Handler(SubmitError::QueueFull)),
        "expected typed QueueFull, got {queue_full_err:?}",
    );

    // a 1ms Drain ask cannot finish while 500ms jobs are in flight:
    // delivery succeeds, the reply is held until drain completes
    let timeout_err = bounded(
        dispatcher
            .ask(|reply| DispatcherMsg::Drain { reply })
            .timeout(Duration::from_millis(1)),
    )
    .await
    .expect_err("drain of 500ms jobs cannot finish in 1ms");
    assert!(
        matches!(timeout_err, AskError::Timeout) && !timeout_err.is_retryable(),
        "#113: ask timeouts are not retryable, got {timeout_err:?}",
    );

    // the Drain delivery set draining=true, so new submits are rejected
    let draining_err = bounded(dispatcher.ask(|reply| DispatcherMsg::Submit {
        job: ok_job(10),
        reply,
    }))
    .await
    .expect_err("draining rejects new work");
    assert!(
        matches!(draining_err, AskError::Handler(SubmitError::Draining)),
        "expected typed Draining, got {draining_err:?}",
    );

    // the dispatcher stops normally after drain; the overseer must see it
    let observed = poll_observed_death(&app).await;
    assert_eq!(
        observed,
        Some((dispatcher_id, true)),
        "death-watch: overseer observes the dispatcher's normal death",
    );
}

const PRODUCERS: usize = 8;
const PER_PRODUCER: u64 = 25;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linear_concurrent_producers_no_loss_no_phantom() {
    let registry = Arc::new(Registry::new());
    let mut cfg = config_no_seam(&registry);
    cfg.workers = 3;
    cfg.queue_cap = 1024; // accept everything; loss-accounting is the subject
    let _app = app::start(cfg).await;
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    let barrier = Arc::new(tokio::sync::Barrier::new(PRODUCERS));
    let mut producers = Vec::new();
    for p in 0..PRODUCERS {
        let dispatcher_for_task = dispatcher.clone();
        let barrier_clone = Arc::clone(&barrier);
        producers.push(tokio::spawn(async move {
            barrier_clone.wait().await; // all producers submit concurrently
            let mut accepted = 0u64;
            for i in 0..PER_PRODUCER {
                let id = u64::try_from(p).expect("small") * 1000 + i;
                let kind = if i % 10 == 3 {
                    JobKind::Fail // ~10% of jobs crash their worker
                } else {
                    JobKind::Ok(Duration::from_millis(1))
                };
                dispatcher_for_task
                    .ask(|reply| DispatcherMsg::Submit {
                        job: Job { id, kind },
                        reply,
                    })
                    .no_timeout()
                    .await
                    .expect("submit accepted under a 1024 cap");
                accepted += 1;
            }
            accepted
        }));
    }

    let mut submitted = 0u64;
    for producer in producers {
        submitted += bounded(producer).await.expect("producer task");
    }

    let report = bounded(
        dispatcher
            .ask(|reply| DispatcherMsg::Drain { reply })
            .no_timeout(),
    )
    .await
    .expect("drain reply");
    assert_eq!(
        report.submitted, submitted,
        "dispatcher accounted every accepted submit"
    );
    assert_eq!(
        report.completed + report.failed,
        submitted,
        "at-least-once accounting: no lost job, no phantom job",
    );
}
