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

// ---------------------------------------------------------------------------
// Card #225 — the control lane at APP level (walking skeleton): a dispatcher
// whose mailbox is FULL still adopts a supervised child promptly.
// ---------------------------------------------------------------------------

/// The dispatcher's user lane is filled to capacity (two pending `Submit`
/// asks, mailbox cap 2) BEFORE the loop starts. A `supervise` op enqueued
/// against that full mailbox rides the unbounded control lane (ADR-0021), so
/// it is applied before the backlog drains — proven by the child being
/// adopted: its death drives a rebuild, which only a table-present, watched
/// child can produce. Pre-#225 the `supervise` call parked on the full
/// mailbox and this test's bounded wrapper fired.
///
/// NOT fixed here (and asserted as such): the #218 wart-3 `WorkerReplaced`
/// roster update is a USER message — it still rides the bounded lane and is
/// dropped on the full mailbox below, so the roster stays empty even though
/// the child is supervised. #244 tracks it.
#[tokio::test]
async fn supervise_lands_while_dispatcher_backlog_is_full() {
    use std::sync::Mutex;

    use bombay::{
        actor::{ActorRef, PreparedActor, RunResult, Spawn, WeakActorRef},
        error::ActorStopReason,
        mailbox::Capacity,
        test_support::set_supervisor_rng_seed,
    };
    use futures::FutureExt;

    set_supervisor_rng_seed(Some(3));
    let registry = Arc::new(Registry::new());
    let cfg = DispatcherConfig {
        workers: 0, // the test drives `supervise` itself
        ..config_no_seam(&registry)
    };
    let (prepared, link_rx) =
        PreparedActor::<Dispatcher>::new_linked(Capacity::try_from(2usize).expect("cap"));
    let dispatcher_ref = prepared.actor_ref().clone();

    // Fill the user lane: two `Submit` asks, each polled exactly once so the
    // message enqueues but the reply stays pending — mailbox now FULL.
    let mut ask1 = Box::pin(
        dispatcher_ref
            .ask(|reply| DispatcherMsg::Submit {
                job: ok_job(900),
                reply,
            })
            .into_future(),
    );
    let mut ask2 = Box::pin(
        dispatcher_ref
            .ask(|reply| DispatcherMsg::Submit {
                job: ok_job(901),
                reply,
            })
            .into_future(),
    );
    assert!(ask1.as_mut().now_or_never().is_none(), "ask 1 enqueued");
    assert!(
        ask2.as_mut().now_or_never().is_none(),
        "ask 2 enqueued — lane full"
    );

    // The supervise factory mirrors the app's own (weak dispatcher + done
    // port); its `WorkerReplaced` roster tell rides the USER lane and is
    // dropped on the full mailbox — wart 3, unchanged by this card (#244).
    let (birth_tx, birth_rx) = flume::unbounded::<u32>();
    let (stopped_tx, stopped_rx) = flume::unbounded::<ActorId>();
    let stash: Arc<Mutex<Option<ActorRef<app::Worker>>>> = Arc::new(Mutex::new(None));
    let next = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let child_id = {
        let done_port = dispatcher_ref.recipient::<app::Done>();
        let disp_weak: WeakActorRef<Dispatcher> = dispatcher_ref.downgrade();
        let stash = Arc::clone(&stash);
        let next = Arc::clone(&next);
        let restart = cfg.restart.clone();
        bounded(dispatcher_ref.supervise(restart, move || {
            let seq = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            birth_tx.try_send(seq).expect("unbounded birth tape");
            let worker = app::Worker::spawn(app::WorkerArgs {
                slot: 9,
                dispatcher: done_port.clone(),
                stopped_tx: Some(stopped_tx.clone()),
            });
            if let Some(disp) = disp_weak.upgrade() {
                let _ = disp
                    .tell(DispatcherMsg::WorkerReplaced {
                        slot: 9,
                        id: worker.id(),
                        worker: worker.recipient::<app::Job>(),
                    })
                    .try_send(); // wart 3: dropped while the mailbox is full
            }
            *stash.lock().expect("stash lock") = Some(worker.clone());
            worker
        }))
        .await
        .expect("supervise must not hang on the full backlog")
    };
    // The first incarnation spawned inline at the call — with the mailbox full.
    assert_eq!(
        birth_rx.try_recv().ok(),
        Some(0),
        "incarnation 0 was born at the supervise call",
    );

    let run = prepared.spawn_supervised_task(cfg, link_rx);

    // Kill incarnation 0: only an APPLIED Add (table insert + installed watch
    // edge, or the installer's self-healing Closed path) turns that death into
    // a rebuild. No rebuild ⇒ the op was lost to the backlog.
    stash
        .lock()
        .expect("stash lock")
        .as_ref()
        .expect("incarnation 0 stashed")
        .kill();
    let reborn = bounded(birth_rx.recv_async())
        .await
        .expect("a rebuild must arrive — the supervise op was applied");
    assert_eq!(
        reborn, 1,
        "the backlogged dispatcher adopted and rebuilt the child"
    );

    // The user backlog drained AFTER the control op: both asks resolve.
    bounded(&mut ask1).await.expect("submit 1 reply");
    bounded(&mut ask2).await.expect("submit 2 reply");

    // Wart 3, asserted unchanged: the roster update was dropped on the full
    // mailbox, so the roster stays EMPTY even though the child is supervised.
    let stats = bounded(dispatcher_ref.ask(|reply| DispatcherMsg::Stats { reply }))
        .await
        .expect("stats reply");
    assert!(
        stats.worker_ids.is_empty(),
        "WorkerReplaced rides the user lane and was dropped — #244, not this card",
    );

    drop(dispatcher_ref); // collection: backlog drained, supervisor stops
    let outcome = bounded(run).await.expect("run joins");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "ref-drop collection, got {outcome:?}",
    );
    // The adopted child was swept at supervisor exit (#245): incarnation 1's
    // graceful stop is the one stop the seam reports (incarnation 0 was
    // killed — `on_stop` skipped).
    let swept = bounded(stopped_rx.recv_async())
        .await
        .expect("the surviving incarnation is stopped with the supervisor");
    assert_eq!(
        swept,
        stash
            .lock()
            .expect("stash lock")
            .as_ref()
            .expect("incarnation 1 stashed")
            .id(),
        "the swept incarnation is the rebuild",
    );
    assert!(
        stopped_rx.try_recv().is_err(),
        "exactly one graceful child stop (the killed incarnation skips on_stop)",
    );
    let _ = child_id;
}
