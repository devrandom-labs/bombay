//! Card #218 — the M1 exit gate: the four cross-cutting test categories
//! applied at APP level over the job-queue mini-app.
//!
//! The app under test is the real example (`examples/job_queue/app.rs`),
//! included by path so the demo and the gate compile the same code.

#![expect(clippy::expect_used, reason = "test assertions fail loudly by design")]

#[path = "../examples/job_queue/app.rs"]
mod app;

use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
    time::Duration,
};

use app::{
    DISPATCHER_NAME, Dispatcher, DispatcherConfig, DispatcherMsg, DrainReport, Intake, IntakeMsg,
    Job, JobKind, SubmitError,
};
use bombay::{
    ActorId,
    actor::Spawn as _,
    error::AskError,
    mailbox::Capacity,
    registry::Registry,
    restart::{RestartConfig, RestartPolicy},
    stash::Stashed,
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
        worker_grace: Duration::from_secs(5),
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

/// Card #257 bullet 4: the app wires `DispatcherConfig.worker_grace` into each
/// worker's `SpawnConfig.on_stop_grace`. A DISTINCT non-default grace (30 s,
/// not the 5 s default) must not change the drain contract: the job completes,
/// the graceful supervisor teardown still stops the worker, and the worker's
/// `on_stop` reports through the app-level seam inside the harness bound.
#[tokio::test]
async fn worker_drains_under_custom_on_stop_grace() {
    let registry = Arc::new(Registry::new());
    let (worker_stopped_tx, worker_stopped_rx) = flume::unbounded::<ActorId>();
    let cfg = DispatcherConfig {
        workers: 1,
        worker_grace: Duration::from_secs(30),
        ..config(&registry, Some(worker_stopped_tx))
    };
    let app = app::start(cfg).await;
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    bounded(dispatcher.ask(|reply| DispatcherMsg::Submit {
        job: ok_job(1),
        reply,
    }))
    .await
    .expect("submit accepted under cap");

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
            submitted: 1,
            completed: 1,
            failed: 0,
            retried: 0,
            rebuilds: 0
        },
        "the single job drains cleanly under the custom grace",
    );

    // Drain stops the dispatcher gracefully (the file's shutdown idiom); the
    // supervisor teardown then stops the worker, whose `on_stop` must report
    // within the harness bound — the grace flows app → `SpawnConfig` →
    // `teardown_children`.
    let observed = poll_observed_death(&app).await;
    assert!(
        observed.is_some(),
        "the overseer must observe the dispatcher's death"
    );
    bounded(worker_stopped_rx.recv_async())
        .await
        .expect("worker must report its stop within the bound");
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
/// roster update is a USER message — it still rides the bounded lane, so
/// incarnation 0's roster tell (fired at the inline spawn, mailbox full) is
/// dropped. Incarnation 1's, fired during the in-loop rebuild after the
/// backlog drained, lands — the roster ends with exactly the rebuild. #244
/// tracks the underlying loss.
#[tokio::test]
async fn supervise_lands_while_dispatcher_backlog_is_full() {
    use std::sync::Mutex;

    use bombay::{
        actor::{ActorRef, PreparedActor, RunResult, Spawn, SpawnConfig, WeakActorRef},
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
    let (prepared, link_rx) = PreparedActor::<Dispatcher>::new_linked(SpawnConfig {
        capacity: Capacity::try_from(2usize).expect("cap"),
        ..Default::default()
    });
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

    // The supervise factory mirrors the app's own — but captures NO strong
    // dispatcher handle: the closure lives in the dispatcher's own child
    // table, so a captured `Recipient` would be a self-cycle and ref-count
    // stop (ADR-0003) could never fire. The done port is derived INSIDE the
    // closure from the weak ref instead. Its `WorkerReplaced` roster tell
    // rides the USER lane and is dropped on the full mailbox — wart 3,
    // unchanged by this card (#244).
    let (birth_tx, birth_rx) = flume::unbounded::<u32>();
    let (stopped_tx, stopped_rx) = flume::unbounded::<ActorId>();
    let stash: Arc<Mutex<Option<ActorRef<app::Worker>>>> = Arc::new(Mutex::new(None));
    let next = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let child_id = {
        let disp_weak: WeakActorRef<Dispatcher> = dispatcher_ref.downgrade();
        let stash = Arc::clone(&stash);
        let next = Arc::clone(&next);
        let restart = cfg.restart.clone();
        bounded(dispatcher_ref.supervise(restart, move || {
            let seq = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            birth_tx.try_send(seq).expect("unbounded birth tape");
            let disp = disp_weak
                .upgrade()
                .expect("the dispatcher is alive whenever this factory runs");
            let worker = app::Worker::spawn(app::WorkerArgs {
                slot: 9,
                dispatcher: disp.recipient::<app::Done>(),
                stopped_tx: Some(stopped_tx.clone()),
            });
            let _ = disp
                .tell(DispatcherMsg::WorkerReplaced {
                    slot: 9,
                    id: worker.id(),
                    worker: worker.recipient::<app::Job>(),
                })
                .try_send(); // wart 3: dropped while the mailbox is full
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

    // Wart 3, true semantics: ONLY incarnation 0's `WorkerReplaced` was
    // dropped — the mailbox was full at the inline first spawn. Incarnation
    // 1's factory ran during the in-loop rebuild AFTER the backlog drained, so
    // its roster tell landed. The roster holds exactly the rebuild.
    let stats = bounded(dispatcher_ref.ask(|reply| DispatcherMsg::Stats { reply }))
        .await
        .expect("stats reply");
    let occupant_id = stash
        .lock()
        .expect("stash lock")
        .as_ref()
        .expect("incarnation 1 stashed")
        .id();
    assert_eq!(
        stats.worker_ids,
        vec![occupant_id],
        "only incarnation 0's roster update was dropped (wart 3 — #244, not \
         this card); the rebuild's landed once the backlog had drained",
    );

    // The adopted child is stopped via the supervisor's own `stop_child`
    // (control lane): its graceful stop drops the worker's `Recipient<Done>`
    // — a STRONG dispatcher sender (recipient.rs erases a strong `ActorRef`),
    // so a live worker pins the dispatcher exactly like the app's own workers
    // do, and collection below could never fire while it lives. Releasing it
    // first is what lets the ref-count stop happen at all; the app's real
    // dispatcher never relies on collection (it stops in-band on Drain).
    bounded(dispatcher_ref.stop_child(occupant_id))
        .await
        .expect("the dispatcher is alive");
    // Incarnation 1's graceful stop is the one stop the seam reports
    // (incarnation 0 was killed — `on_stop` skipped).
    let swept = bounded(stopped_rx.recv_async())
        .await
        .expect("the surviving incarnation is stopped via stop_child");
    assert_eq!(swept, occupant_id, "the stopped incarnation is the rebuild");
    assert!(
        stopped_rx.try_recv().is_err(),
        "exactly one graceful child stop (the killed incarnation skips on_stop)",
    );

    drop(dispatcher_ref); // every strong sender gone: collection fires
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
    // Lineage semantics (ADR-0015): `supervise` returned the FIRST
    // incarnation's id; the roster/sweep name the REBUILD, a fresh id.
    assert_ne!(
        child_id, occupant_id,
        "the returned id names the first incarnation; the rebuild minted a fresh one",
    );
}

/// #224 walking skeleton: pause intake, submit while paused (deferred, asker
/// still waiting), resume — the deferred submissions complete in order,
/// ahead of a post-resume submission. Real time, NOT `start_paused`: the
/// deferred ask rides the 5 s default ask deadline.
#[tokio::test]
async fn intake_defers_submissions_during_maintenance() {
    let registry = Arc::new(Registry::new());
    let app = app::start(config_no_seam(&registry)).await;
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");
    let intake = Stashed::<Intake>::spawn((
        dispatcher,
        Capacity::try_from(8usize).expect("valid intake stash capacity"),
    ));

    // Maintenance: submissions stash instead of forwarding. The tell's await
    // is delivery, so `Pause` is enqueued before anything below it.
    bounded(intake.tell(IntakeMsg::Pause))
        .await
        .expect("pause delivered");

    // Deferred ask A: the reply future pends on the stashed Submit. The
    // ActorRef is cloned into the task (the ask builder borrows it);
    // completion order is taped by job id.
    let order = Arc::new(Mutex::new(Vec::new()));
    let intake_a = intake.clone();
    let order_a = Arc::clone(&order);
    let a = tokio::spawn(async move {
        let reply = intake_a
            .ask(|reply| IntakeMsg::Submit {
                job: ok_job(100),
                reply,
            })
            .await;
        order_a.lock().expect("order").push(100u64);
        reply
    });
    // Deterministic enqueue order Pause, A, Resume: on the current-thread
    // runtime one yield runs A's task to its first pending point (the ask's
    // reply await), so its Submit is enqueued before Resume is sent.
    tokio::task::yield_now().await;
    assert!(
        order.lock().expect("order").is_empty(),
        "A is deferred: no reply while paused"
    );

    bounded(intake.tell(IntakeMsg::Resume))
        .await
        .expect("resume delivered");
    let intake_b = intake.clone();
    let order_b = Arc::clone(&order);
    let b = tokio::spawn(async move {
        let reply = intake_b
            .ask(|reply| IntakeMsg::Submit {
                job: ok_job(101),
                reply,
            })
            .await;
        order_b.lock().expect("order").push(101u64);
        reply
    });

    let a_reply = bounded(a).await.expect("A task joins");
    let b_reply = bounded(b).await.expect("B task joins");
    assert!(a_reply.is_ok(), "deferred A completes Ok: {a_reply:?}");
    assert!(b_reply.is_ok(), "post-resume B completes Ok: {b_reply:?}");
    assert_eq!(
        order.lock().expect("order").as_slice(),
        [100, 101],
        "the deferred submission completes ahead of the post-resume one"
    );

    // Both submissions landed exactly once on the dispatcher.
    let stats = bounded(app.dispatcher.ask(|reply| DispatcherMsg::Stats { reply }))
        .await
        .expect("stats reply");
    assert_eq!(stats.submitted, 2);
}
