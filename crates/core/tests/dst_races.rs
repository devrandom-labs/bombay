//! Deterministic interleaving suite for the actor run-loop (card #116).
//!
//! The stop / cancel / kill / startup races are covered here by *forcing* one
//! specific ordering per test with `oneshot` barriers and (where a stop must win
//! or lose a race with a kill) the single-threaded runtime's no-preemption
//! guarantee — so each interleaving is exercised deterministically rather than
//! left to timing luck. Every "must terminate" await is wrapped in a 5 s
//! `tokio::time::timeout`, so a regression that hangs the loop FAILS FAST here
//! instead of stalling the suite. Each test asserts a *specific* outcome (which
//! hooks ran, via `Arc<AtomicU32>` spies, and the exact `RunResult` variant), not
//! merely "didn't hang".
//!
//! These are the GAP scenarios: the happy-path "finish-in-flight-on-cancel" and
//! "kill-mid-handler" races already live in `spawn.rs` unit tests and are not
//! duplicated here.
//!
//! # loom: justified N/A (not applied)
//!
//! loom explores permutations of **std synchronization primitives** — the
//! interleavings of `atomic` / `Mutex` / `UnsafeCell` operations admitted by the
//! C11 memory model. It does **not** model an async executor's task-scheduling
//! choices; that is outside its scope. #116's run-state is a single tokio task
//! that owns `&mut self` and drives the actor sequentially — there is no shared
//! mutable state read concurrently from two threads for loom to permute. The one
//! atomic in the whole spine is `NEXT_ACTOR_ID` (a `Relaxed` monotonic counter in
//! `spawn.rs`), whose correctness is "each `fetch_add` returns a distinct value"
//! — a property of atomic increment alone, needing no happens-before. A loom
//! model of it here would require either (a) an invasive `#[cfg(loom)]` swap of
//! the production `static` plus a production loom dependency, or (b)
//! reimplementing the counter inside the test — which would then assert on the
//! reimplementation, not the SUT (test-quality rule #8). Neither is worth doing
//! for a lone Relaxed counter, so loom is deliberately not applied. The async
//! orderings that DO matter for #116 are covered deterministically below with
//! barriers and the single-threaded runtime.
//!
//! #150 re-examined loom (and shuttle) for the *ref-model* and reached the
//! same verdict for a stronger reason: the ref-count liveness lives in
//! flume's `sender_count` (ADR-0003), and flume ships no loom/shuttle
//! instrumentation, so neither tool can observe the interleavings that
//! matter. MIRI — which interprets flume's real atomics — covers them
//! instead, in the scheduled `miri.yml` lane. Evidence: ADR-0005.

use core::convert::Infallible;
use std::{
    collections::HashMap,
    future::IntoFuture,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout},
};

use bombay::{
    ActorId, SendContext,
    actor::{
        Actor, ActorRef, Flow, Normal, PreparedActor, RunResult, SpawnConfig, SupervisedReact,
        WeakActorRef,
    },
    capability::{self, Never, Step},
    error::{ActorStopReason, AskError, TellError},
    mailbox::{Capacity, ControlSignal, Mailboxed, Signal},
    message::Msg,
    reply::ReplySender,
    restart::{RestartConfig, RestartPolicy},
    test_support::{set_supervisor_rng_seed, terminate_bound},
};

/// The suite-wide fail-fast bound: any terminal await that exceeds this is a hung
/// loop, and the test fails here rather than stalling the whole run. Scaled under
/// MIRI — see `terminate_bound`.
const TERMINATE: Duration = terminate_bound();

/// The removed `Spawn` verbs, suite-local over the public floor.
fn spawn_plain<A: Actor>(args: A::Args) -> ActorRef<A> {
    spawn_plain_with_config(SpawnConfig::default(), args)
}

fn spawn_plain_with_config<A: Actor>(config: SpawnConfig, args: A::Args) -> ActorRef<A> {
    let prepared = PreparedActor::<A>::new(config);
    let actor_ref = prepared.actor_ref().clone();
    let _join = prepared.spawn(args);
    actor_ref
}

fn cap(n: usize) -> Capacity {
    Capacity::try_from(n).expect("valid test capacity")
}

/// Bounds a pre-run send under the fail-fast bound (card #179): a mutant that
/// stalls the mailbox (e.g. `Capacity::get -> 0` turning the queue into a
/// rendezvous with no receiver yet) must FAIL here, not hang the whole test
/// binary past the mutants sweep timeout.
async fn bounded<F: std::future::IntoFuture>(fut: F) -> F::Output {
    timeout(TERMINATE, fut)
        .await
        .expect("send must not hang: the mailbox stalled")
}

/// A reusable spy actor: counts handled messages and how many times `on_stop`
/// ran, via shared atomics the test inspects. The SUT is the real loop.
struct Spy {
    handled: Arc<AtomicU32>,
    stopped: Arc<AtomicU32>,
}
#[derive(Debug)]
struct Ping;
impl Msg for Ping {}
impl Mailboxed for Spy {
    type Msg = Ping;
}
impl Actor for Spy {
    type Args = (Arc<AtomicU32>, Arc<AtomicU32>);
    type Error = Infallible;
    async fn on_start(
        (handled, stopped): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self { handled, stopped })
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Scenario 1 — kill during `on_start`, before any state is built.
// ---------------------------------------------------------------------------

/// `kill()` while `on_start` is parked (state not yet built) aborts the whole
/// lifecycle: the outcome is `Killed`, `on_stop` never runs, and message handling
/// never begins. A message is pre-queued precisely to prove it is never handled,
/// since `on_start` never completes to reach the loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_during_on_start_yields_killed_no_on_stop_no_handling() {
    struct StartGate {
        handled: Arc<AtomicU32>,
        stopped: Arc<AtomicU32>,
    }
    impl Mailboxed for StartGate {
        type Msg = Ping;
    }
    impl Actor for StartGate {
        // (entered, release, handled, stopped)
        type Args = (
            oneshot::Sender<()>,
            oneshot::Receiver<()>,
            Arc<AtomicU32>,
            Arc<AtomicU32>,
        );
        type Error = Infallible;
        async fn on_start(
            (entered, release, handled, stopped): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            let _ = entered.send(()); // "on_start reached the gate"
            let _ = release.await; // park here forever (test never releases)
            Ok(Self { handled, stopped })
        }
        async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(Flow::Continue)
        }
        async fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            _: ActorStopReason,
        ) -> Result<(), Self::Error> {
            self.stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let (entered_tx, entered_rx) = oneshot::channel();
    let (_release_tx, release_rx) = oneshot::channel(); // never fired
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<StartGate>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    // Pre-queue a message: it must never be handled, because on_start never ends.
    bounded(actor_ref.tell(Ping)).await.expect("pre-queue");
    let run = prepared.spawn((
        entered_tx,
        release_rx,
        Arc::clone(&handled),
        Arc::clone(&stopped),
    ));

    timeout(TERMINATE, entered_rx)
        .await
        .expect("on_start must reach the gate, not hang")
        .expect("on_start reached the gate");
    actor_ref.kill(); // abort while on_start is parked

    let outcome = timeout(TERMINATE, run)
        .await
        .expect("kill() must abort the parked on_start")
        .expect("join");
    assert!(
        matches!(outcome, RunResult::Killed),
        "kill mid-on_start → Killed, got {outcome:?}",
    );
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        0,
        "on_stop never ran (no state was built)"
    );
    assert_eq!(
        handled.load(Ordering::SeqCst),
        0,
        "message handling never began"
    );
}

// ---------------------------------------------------------------------------
// Scenario 2 — kill during `on_stop`, while the cleanup hook is parked.
// ---------------------------------------------------------------------------

/// A graceful stop drives `on_stop`; `kill()` while `on_stop` is parked aborts the
/// lifecycle → `Killed`, and the hook's post-park side effect never fires. This
/// pins that a hard kill wins even against the shutdown hook already in progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_during_on_stop_yields_killed_and_skips_post_park_effect() {
    struct StopGate {
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
        post_park: Arc<AtomicU32>,
    }
    impl Mailboxed for StopGate {
        type Msg = Ping;
    }
    impl Actor for StopGate {
        // (entered, release, post_park)
        type Args = (oneshot::Sender<()>, oneshot::Receiver<()>, Arc<AtomicU32>);
        type Error = Infallible;
        async fn on_start(
            (entered, release, post_park): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self {
                entered: Some(entered),
                release: Some(release),
                post_park,
            })
        }
        async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            Ok(Flow::Continue)
        }
        async fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            _: ActorStopReason,
        ) -> Result<(), Self::Error> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(()); // "on_stop reached the gate"
            }
            if let Some(release) = self.release.take() {
                let _ = release.await; // park here forever (test never releases)
            }
            self.post_park.fetch_add(1, Ordering::SeqCst); // must NOT run if killed here
            Ok(())
        }
    }

    let (entered_tx, entered_rx) = oneshot::channel();
    let (_release_tx, release_rx) = oneshot::channel(); // never fired
    let post_park = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<StopGate>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn((entered_tx, release_rx, Arc::clone(&post_park)));

    actor_ref.stop(); // graceful → loop returns Normal → on_stop runs
    timeout(TERMINATE, entered_rx)
        .await
        .expect("on_stop must reach the gate, not hang")
        .expect("on_stop reached the gate");
    actor_ref.kill(); // abort while on_stop is parked

    let outcome = timeout(TERMINATE, run)
        .await
        .expect("kill() must abort the parked on_stop")
        .expect("join");
    assert!(
        matches!(outcome, RunResult::Killed),
        "kill mid-on_stop → Killed, got {outcome:?}",
    );
    assert_eq!(
        post_park.load(Ordering::SeqCst),
        0,
        "on_stop's post-park side effect never fired",
    );
}

// ---------------------------------------------------------------------------
// Scenario 3a — `stop()` then `kill()` before the loop observes the stop.
// ---------------------------------------------------------------------------

/// An actor that signals when `on_start` has completed (so the test knows the loop
/// is parked on `recv`), and counts `on_stop`.
struct StartSignaled {
    stopped: Arc<AtomicU32>,
}
impl Mailboxed for StartSignaled {
    type Msg = Ping;
}
impl Actor for StartSignaled {
    type Args = (oneshot::Sender<()>, Arc<AtomicU32>);
    type Error = Infallible;
    async fn on_start(
        (started, stopped): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let _ = started.send(()); // on_start done; the loop is about to park on recv
        Ok(Self { stopped })
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// `stop()` immediately followed by `kill()` — with no await between them on the
/// single-threaded runtime, so the loop task is not polled in the gap — means the
/// abort flag is already set when the loop is next polled. `Abortable` checks
/// `is_aborted()` before polling the inner future, so the kill WINS: the outcome
/// is `Killed` and `on_stop` never runs, even though a graceful stop was requested
/// first. (current_thread is load-bearing: on a multi-thread runtime the loop
/// could observe the cancel on another worker before the kill lands.)
#[tokio::test] // current_thread — no preemption between stop() and kill()
async fn stop_then_kill_before_observe_is_killed_and_skips_on_stop() {
    let (started_tx, started_rx) = oneshot::channel();
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<StartSignaled>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    let run = tokio::spawn(prepared.run((started_tx, Arc::clone(&stopped))));

    started_rx
        .await
        .expect("on_start done, loop parked on recv");
    actor_ref.stop(); // graceful cancel requested…
    actor_ref.kill(); // …but killed before the loop task is polled to observe it

    let outcome = timeout(TERMINATE, run)
        .await
        .expect("must terminate")
        .expect("join");
    assert!(
        matches!(outcome, RunResult::Killed),
        "kill wins the race → Killed, got {outcome:?}",
    );
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        0,
        "on_stop never ran — kill won"
    );
}

// ---------------------------------------------------------------------------
// Scenario 3b — a graceful stop that FULLY completes, THEN `kill()` (no-op).
// ---------------------------------------------------------------------------

/// A queued `Signal::Stop` stops the actor normally (running `on_stop` once); a
/// `kill()` issued AFTER the run has fully returned is a harmless no-op on an
/// already-stopped actor — no panic, and the recorded outcome is unchanged.
#[tokio::test]
async fn graceful_stop_completes_then_kill_is_a_noop() {
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<Spy>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("enqueue Stop");

    let outcome = timeout(
        TERMINATE,
        prepared.run((Arc::clone(&handled), Arc::clone(&stopped))),
    )
    .await
    .expect("Signal::Stop must terminate the actor");

    // The actor is fully stopped; killing it now must not panic or change anything.
    actor_ref.kill();

    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "graceful stop → Normal, got {outcome:?}",
    );
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        1,
        "on_stop ran exactly once"
    );
    assert_eq!(
        handled.load(Ordering::SeqCst),
        0,
        "no domain message was handled"
    );
}

// ---------------------------------------------------------------------------
// Scenario 4 — idempotent `stop()` from multiple ref clones.
// ---------------------------------------------------------------------------

/// Calling `stop()` several times — twice on one ref and once on a clone — stops
/// the actor exactly once: `on_stop` runs once and the outcome is `Normal`. The
/// cancellation is sticky, so pre-run `stop()`s collapse into a single stop.
#[tokio::test]
async fn idempotent_stop_stops_once_and_runs_on_stop_once() {
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<Spy>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    let clone = actor_ref.clone();

    actor_ref.stop();
    actor_ref.stop(); // repeated on the same ref
    clone.stop(); // and from a distinct clone

    let outcome = timeout(
        TERMINATE,
        prepared.run((Arc::clone(&handled), Arc::clone(&stopped))),
    )
    .await
    .expect("stop() must terminate the actor");

    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "idempotent stop → Normal, got {outcome:?}",
    );
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        1,
        "on_stop ran exactly once despite 3 stop() calls"
    );
    assert_eq!(handled.load(Ordering::SeqCst), 0, "no message handled");
}

// ---------------------------------------------------------------------------
// Scenario 5 — `stop()` racing a `Signal::Stop` already queued.
// ---------------------------------------------------------------------------

/// A `Signal::Stop` is enqueued AND `stop()` (the cancel token) is fired: whichever
/// the loop observes first, the result is a single `Normal` stop with `on_stop`
/// run exactly once — no hang, no double `on_stop`, and no message handled.
#[tokio::test]
async fn stop_racing_a_queued_stop_signal_stops_normally_once() {
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<Spy>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("enqueue Stop");
    actor_ref.stop(); // cancel token races the queued Stop

    let outcome = timeout(
        TERMINATE,
        prepared.run((Arc::clone(&handled), Arc::clone(&stopped))),
    )
    .await
    .expect("the queued Stop / cancel race must terminate the actor");

    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "either path → Normal, got {outcome:?}",
    );
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        1,
        "on_stop ran exactly once — not twice"
    );
    assert_eq!(handled.load(Ordering::SeqCst), 0, "no message handled");
}

// ---------------------------------------------------------------------------
// Scenario 6 — `send` racing termination: send after a graceful stop fails.
// ---------------------------------------------------------------------------

/// After a graceful stop completes the run-loop drops its mailbox receiver, so a
/// subsequent `send` on a still-held sender fails (the actor is gone) — the
/// message is handed back rather than lost into the void.
#[tokio::test]
async fn send_after_graceful_stop_fails() {
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<Spy>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("enqueue Stop");

    let outcome = timeout(
        TERMINATE,
        prepared.run((Arc::clone(&handled), Arc::clone(&stopped))),
    )
    .await
    .expect("Signal::Stop must terminate the actor");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "graceful stop → Normal, got {outcome:?}",
    );
    assert_eq!(stopped.load(Ordering::SeqCst), 1, "on_stop ran once");

    // The receiver is gone; the send must fail and return the undelivered message.
    let resend = bounded(actor_ref.mailbox_sender().send(Signal::Message {
        msg: Ping,
        self_sender: actor_ref.mailbox_sender().clone(),
        ctx: SendContext::capture(),
    }))
    .await;
    assert!(
        matches!(
            resend,
            Err(bombay::mailbox::SendError(Signal::Message {
                msg: Ping,
                ..
            }))
        ),
        "send after the actor stopped must fail with the message handed back",
    );
    assert_eq!(
        handled.load(Ordering::SeqCst),
        0,
        "the post-stop message was never handled"
    );
}

// ---------------------------------------------------------------------------
// Scenario 7 — `kill()` after a normal completion (via the handler returning
// `Flow::Stop(Normal)`).
// ---------------------------------------------------------------------------

/// An actor that finishes itself by returning `Flow::Stop(Normal)` from its handler,
/// then counts `on_stop`.
struct SelfStop {
    handled: Arc<AtomicU32>,
    stopped: Arc<AtomicU32>,
}
impl Mailboxed for SelfStop {
    type Msg = Ping;
}
impl Actor for SelfStop {
    type Args = (Arc<AtomicU32>, Arc<AtomicU32>);
    type Error = Infallible;
    async fn on_start(
        (handled, stopped): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self { handled, stopped })
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(Flow::Stop(Normal)) // stop cleanly after this handler returns
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The actor stops normally via its handler returning `Flow::Stop(Normal)`; a `kill()`
/// issued AFTER
/// the run has returned is a no-op — no panic, and the outcome stays `Normal` with
/// `on_stop` having run once.
#[tokio::test]
async fn kill_after_normal_completion_is_a_noop() {
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<SelfStop>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.tell(Ping))
        .await
        .expect("enqueue one message");

    let outcome = timeout(
        TERMINATE,
        prepared.run((Arc::clone(&handled), Arc::clone(&stopped))),
    )
    .await
    .expect("Flow::Stop(Normal) must terminate the actor");

    // Actor already finished normally; killing the corpse must not panic.
    actor_ref.kill();

    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "self-stop → Normal, got {outcome:?}",
    );
    assert_eq!(
        handled.load(Ordering::SeqCst),
        1,
        "the single message was handled"
    );
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        1,
        "on_stop ran exactly once; kill added nothing"
    );
}

// ---------------------------------------------------------------------------
// #118 / #122-#4 — DST: a saturated cyclic topology under the handler
// discipline (fire-and-forget + self-continuation, never ask().await) stays
// live: every request resolves or times out; nothing hangs forever.
// ---------------------------------------------------------------------------

/// A ring node: forwards work to the next node **without ever blocking its
/// own loop** — `try_send`, shedding on backpressure — and answers external
/// probes through the typed port. This is the #118 decision's discipline
/// encoded as code; the deadlock (each handler parked on the next full
/// mailbox, all four Coffman conditions) is impossible without a blocking
/// wait inside `handle`.
struct Node {
    next: Option<ActorRef<Node>>,
    processed: u64,
}
#[derive(Debug)]
enum NodeMsg {
    SetNext(ActorRef<Node>),
    Work { hops: u32 },
    Probe { reply: ReplySender<u64> },
}
impl Msg for NodeMsg {}
impl Mailboxed for Node {
    type Msg = NodeMsg;
}
impl Actor for Node {
    type Args = ();
    type Error = Infallible;
    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            next: None,
            processed: 0,
        })
    }
    async fn handle(&mut self, msg: NodeMsg, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        match msg {
            NodeMsg::SetNext(next) => self.next = Some(next),
            NodeMsg::Work { hops } => {
                self.processed += 1;
                if hops > 0 {
                    if let Some(next) = &self.next {
                        // Fire-and-forget: a full peer sheds the hop instead
                        // of parking this loop (the discipline under test).
                        let _ = next.tell(NodeMsg::Work { hops: hops - 1 }).try_send();
                    }
                }
            }
            NodeMsg::Probe { reply } => {
                let _ = reply.send(self.processed);
            }
        }
        Ok(Flow::Continue)
    }
}

/// `cyclic_topology_never_deadlocks` (card #118, seeded): a 3-node ring of
/// capacity-1 mailboxes is stormed with seeded work injections while an
/// external client asks every node under a deadline. Invariants, per seed:
/// every concurrent ask RESOLVES within the fail-fast bound (reply, or a
/// timeout variant — never a hang), and after the storm drains the ring is
/// still live (a quiescent ask succeeds and every node did real work).
#[tokio::test(start_paused = true)]
async fn cyclic_topology_never_deadlocks() {
    for seed in [0xDEAD_BEEF_u64, 42, 7, 0xBAD_C0FFE] {
        let nodes: Vec<ActorRef<Node>> = (0..3)
            .map(|_| {
                spawn_plain_with_config::<Node>(
                    SpawnConfig {
                        capacity: cap(1),
                        ..Default::default()
                    },
                    (),
                )
            })
            .collect();
        for (i, node) in nodes.iter().enumerate() {
            let next = nodes[(i + 1) % nodes.len()].clone();
            timeout(TERMINATE, node.tell(NodeMsg::SetNext(next)))
                .await
                .expect("ring wiring must deliver within the bound")
                .expect("delivered");
        }

        // Guarantee every node sees at least one unit of circulating work…
        for node in &nodes {
            timeout(TERMINATE, node.tell(NodeMsg::Work { hops: 6 }))
                .await
                .expect("work seeding must deliver within the bound")
                .expect("delivered");
        }
        // …then storm the saturated ring in a seed-determined pattern (an LCG;
        // sheds on Full are expected and part of the discipline).
        let mut lcg = seed;
        for _ in 0..64 {
            lcg = lcg.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let target = &nodes[(lcg >> 33) as usize % nodes.len()];
            let hops = ((lcg >> 8) % 8) as u32;
            let _ = target.tell(NodeMsg::Work { hops }).try_send();
        }

        // Concurrent external asks under a deadline, against the live storm.
        let asks = nodes.iter().map(|node| {
            node.ask(|reply| NodeMsg::Probe { reply })
                .timeout(Duration::from_millis(100))
                .into_future()
        });
        let outcomes = timeout(TERMINATE, futures::future::join_all(asks))
            .await
            .expect("every ask must RESOLVE within the fail-fast bound — a hang here is the #122-#4 deadlock");
        for outcome in outcomes {
            assert!(
                matches!(
                    outcome,
                    Ok(_)
                        | Err(AskError::Timeout)
                        | Err(AskError::Deliver(TellError::SendTimeout(_)))
                ),
                "an ask resolves with a reply or a timeout, never another failure: {outcome:?}",
            );
        }

        // Liveness after the storm: the ring drains and still answers.
        for node in &nodes {
            let processed = timeout(TERMINATE, node.ask(|reply| NodeMsg::Probe { reply }))
                .await
                .expect("a quiescent ask must resolve within the bound")
                .expect("a drained ring answers");
            assert!(
                processed >= 1,
                "every node did real work during the storm (seed {seed:#x})",
            );
        }
    }
}

// ===========================================================================
// Card #199 — restart-set-strategy storm invariants (deterministic simulation).
//
// The three `dst_*` tests below storm the OneForAll/RestForOne set-cycle engine
// (ADR-0014) under a SEEDED jitter RNG (`set_supervisor_rng_seed`, the
// integration seam) and tokio's paused virtual clock, so every restart wave is
// replayable to the nanosecond. Each independent run owns its OWN current-thread
// paused runtime, and every trace is keyed on a child's STABLE LOGICAL TAG plus
// a virtual timestamp — never a raw `ActorId`, which is process-global and mints
// higher values on the second run in the same test binary.
// ===========================================================================

/// A crash-on-command worker for the storms. Stateless — each rebuild is a fresh
/// incarnation with a new id, exactly what the trace records by tag.
struct Worker;
#[derive(Debug)]
struct Crash;
impl Msg for Crash {}
impl Mailboxed for Worker {
    type Msg = Crash;
}
impl Actor for Worker {
    type Args = ();
    type Error = Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
    async fn handle(&mut self, _: Crash, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        panic!("crash on command")
    }
}

/// The supervisors under test. Idle mailbox — the storm is driven entirely
/// through child crashes and `supervise`/`unsupervise`, never supervisor
/// messages.
#[derive(Debug)]
struct SupIdle;
impl Msg for SupIdle {}

macro_rules! storm_supervisor {
    ($t:ident, $caps:ident, $strategy:ident) => {
        struct $t;

        #[derive(bombay_macros::Provide)]
        struct $caps {
            watching: capability::Watching<capability::OtpPropagation>,
            supervising: capability::Supervising<capability::$strategy>,
        }
        impl capability::CapSet<$t> for $caps {
            fn build((): &()) -> Self {
                Self {
                    watching: capability::Watching::new(),
                    supervising: capability::Supervising::new(),
                }
            }
        }
        impl capability::Actor for $t {
            type Msg = SupIdle;
            type Args = ();
            type Error = Infallible;
            type Caps = $caps;
            async fn init((): (), _: capability::Ctx<'_, Self>) -> Result<Self, Self::Error> {
                Ok(Self)
            }
            async fn handle(
                &mut self,
                _: SupIdle,
                _: capability::Ctx<'_, Self>,
            ) -> Result<Flow, Self::Error> {
                Ok(Flow::Continue)
            }
        }
    };
}
storm_supervisor!(AllSup, AllSupCaps, OneForAll);
storm_supervisor!(RestSup, RestSupCaps, RestForOne);

/// `tag -> current incarnation's strong ref`: kept so a child is not
/// ref-count-stopped before the storm drives it (in production an external ref
/// plays this role — the supervisor never pins a child). Overwritten on every
/// rebuild, so it always holds the live incarnation for that tag.
type LiveChildren = Arc<Mutex<HashMap<&'static str, ActorRef<Worker>>>>;
/// A birth/rebuild report: virtual ms since the run's origin, and the stable tag.
type ReportRx = mpsc::UnboundedReceiver<(u128, &'static str)>;
type ReportTx = mpsc::UnboundedSender<(u128, &'static str)>;

/// The user factory `supervise` wraps: spawns a fresh tagged `Worker`, timestamps
/// and reports the birth on the tape, and stashes a strong ref under its tag.
fn tagged_factory(
    tag: &'static str,
    origin: Instant,
    report: ReportTx,
    live: LiveChildren,
) -> impl FnMut() -> ActorRef<Worker> + Send + 'static {
    move || {
        let child = spawn_plain::<Worker>(());
        let _ = report.send((origin.elapsed().as_millis(), tag));
        live.lock()
            .expect("live-map lock")
            .insert(tag, child.clone());
        child
    }
}

/// A rebuild await bound that tolerates a FULL capped backoff (the 30 s
/// `max_backoff`) plus its 20% jitter. The suite-wide `TERMINATE` (15 s) is
/// SHORTER than a late-attempt virtual backoff, so a paused-clock rebuild that
/// legitimately waits ~30 s would trip it. Still a virtual bound — a genuine hang
/// (no rebuild ever) fires it instantly in real time.
fn rebuild_bound() -> Duration {
    if cfg!(miri) {
        Duration::from_secs(3600)
    } else {
        Duration::from_secs(120)
    }
}

/// A fresh single-threaded runtime with a PAUSED virtual clock — the isolation
/// unit for one deterministic run (two runs must not share a clock).
fn paused_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("current-thread paused runtime")
}

/// Crashes the CURRENT incarnation of `tag` (a no-op if it is already dead —
/// mid-teardown or mid-backoff — which is itself deterministic).
fn crash_tag(live: &LiveChildren, tag: &'static str) {
    if let Some(child) = live.lock().expect("live-map lock").get(tag) {
        let _ = child.tell(Crash).try_send();
    }
}

/// Collects every rebuild report until the storm goes quiet. A gap longer than
/// the 30 s `max_backoff` cap with no report means no rebuild is armed: under the
/// paused clock any pending backoff timer fires (virtually) before this elapses,
/// so a timeout here is true quiescence, not impatience.
async fn drain_rebuilds(rx: &mut ReportRx) -> Vec<(u128, &'static str)> {
    let quiet = Duration::from_secs(120);
    let mut trace = Vec::new();
    while let Ok(Some(report)) = timeout(quiet, rx.recv()).await {
        trace.push(report);
    }
    trace
}

/// Spawns an `OneForAll` supervisor over four `Permanent` tagged children, drives
/// the scripted crash `schedule` at seeded virtual times, and returns the
/// `(virtual_ms, tag)` trace of every rebuild until quiescent. Its own runtime,
/// its own clock, its own seed — fully replayable.
fn storm_trace(seed: u64, schedule: &[(u64, &'static str)]) -> Vec<(u128, &'static str)> {
    const TAGS: [&str; 4] = ["a", "b", "c", "d"];
    paused_runtime().block_on(async move {
        set_supervisor_rng_seed(Some(seed));
        let origin = Instant::now();
        let (tx, mut rx) = mpsc::unbounded_channel::<(u128, &'static str)>();
        let live: LiveChildren = Arc::new(Mutex::new(HashMap::new()));
        let sup = capability::spawn::<AllSup>(());

        for tag in TAGS {
            timeout(
                TERMINATE,
                sup.supervise(
                    RestartPolicy::Permanent,
                    tagged_factory(tag, origin, tx.clone(), Arc::clone(&live)),
                ),
            )
            .await
            .expect("supervise must not hang")
            .expect("the supervisor is alive");
            // The first incarnation spawns inline in `supervise`, so its birth is
            // already on the tape; discard it — the trace is the STORM's rebuilds.
            let (_, born) = timeout(TERMINATE, rx.recv())
                .await
                .expect("a birth arrives within the bound")
                .expect("the tape is open");
            assert_eq!(born, tag, "births arrive in supervise order");
        }

        for &(at_ms, tag) in schedule {
            timeout(
                TERMINATE,
                tokio::time::sleep_until(origin + Duration::from_millis(at_ms)),
            )
            .await
            .expect("virtual sleep resolves");
            crash_tag(&live, tag);
        }

        let trace = drain_rebuilds(&mut rx).await;
        drop(sup);
        trace
    })
}

/// Invariant: a set-restart storm is a pure function of (jitter seed, crash
/// schedule). Same seed ⇒ byte-identical rebuild interleaving (wave times AND
/// order); a different seed moves the jittered backoff deadlines and so the whole
/// schedule. The #100-class storm, made replayable.
#[test]
fn dst_restart_storm_deterministic() {
    let sched: &[(u64, &'static str)] = &[(0, "b"), (50, "c"), (120, "b")];
    let a = storm_trace(42, sched);
    let b = storm_trace(42, sched);
    assert_eq!(
        a, b,
        "same seed + schedule ⇒ identical rebuild interleaving"
    );
    let c = storm_trace(43, sched);
    assert_ne!(a, c, "a different jitter seed varies the schedule");
    assert!(!a.is_empty(), "the storm actually stormed");
}

/// Drives a seeded storm of interleaved `crash` / `unsupervise` ops against a set
/// supervisor, then asserts the two race invariants. Generic over the strategy so
/// both `OneForAll` and `RestForOne` run the same body. Give-up budgets are raised
/// out of the way: this test probes the unwatch/removal race, not the trip
/// counters, so a surviving supervisor means "no wedge", never "not yet escalated".
async fn link_unlink_storm<S>(seed: u64)
where
    S: capability::Actor<Args = ()>,
    capability::Shell<S>: SupervisedReact,
    <S::Caps as capability::SelectRunner<S>>::Runner: capability::RunKind<S>,
{
    const TAGS: [&str; 3] = ["a", "b", "c"];
    set_supervisor_rng_seed(Some(seed));
    let origin = Instant::now();
    let (tx, mut rx) = mpsc::unbounded_channel::<(u128, &'static str)>();
    let live: LiveChildren = Arc::new(Mutex::new(HashMap::new()));
    let sup = capability::spawn::<S>(());
    let cfg = RestartConfig::new(RestartPolicy::Permanent)
        .with_max_restarts(u32::MAX)
        .with_max_total(u32::MAX);

    for tag in TAGS {
        timeout(
            TERMINATE,
            sup.supervise(
                cfg,
                tagged_factory(tag, origin, tx.clone(), Arc::clone(&live)),
            ),
        )
        .await
        .expect("supervise must not hang")
        .expect("the supervisor is alive");
        let _ = timeout(TERMINATE, rx.recv())
            .await
            .expect("a birth within the bound")
            .expect("the tape is open");
    }

    // `tag -> virtual ms at which it was first unsupervised`; once detached a tag
    // is never re-supervised, so no later rebuild for it may ever appear.
    let mut removed: HashMap<&'static str, u128> = HashMap::new();
    let mut lcg = seed ^ 0x9E37_79B9_7F4A_7C15;
    for _ in 0..12_u32 {
        lcg = lcg
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let gap = Duration::from_millis((lcg >> 40) % 90);
        let tag = TAGS[((lcg >> 8) % TAGS.len() as u64) as usize];
        timeout(TERMINATE, tokio::time::sleep(gap))
            .await
            .expect("virtual sleep resolves");
        if (lcg >> 20) & 1 == 0 {
            crash_tag(&live, tag);
        } else if let Some(id) = live
            .lock()
            .expect("live-map lock")
            .get(tag)
            .map(ActorRef::id)
        {
            timeout(TERMINATE, sup.unsupervise(id))
                .await
                .expect("unsupervise must not hang")
                .expect("the supervisor is alive");
            removed
                .entry(tag)
                .or_insert_with(|| origin.elapsed().as_millis());
        }
    }

    let trace = drain_rebuilds(&mut rx).await;
    for (ms, tag) in trace {
        if let Some(&removed_at) = removed.get(tag) {
            assert!(
                ms <= removed_at,
                "seed {seed}: tag {tag} rebuilt at {ms}ms, AFTER it was unsupervised at \
                 {removed_at}ms — a detached entry was resurrected (the #195 unwatch race)",
            );
        }
    }
    assert!(
        sup.is_alive(),
        "seed {seed}: the supervisor wedged or died under the link/unlink storm",
    );
    drop(sup);
}

/// Invariant: seeded interleavings of `crash` / `unsupervise` under set strategies
/// never resurrect a detached child and never wedge the supervisor. The #195
/// unwatch race, replayed across strategies and seeds with the widened window a
/// set cycle opens.
#[test]
fn dst_concurrent_link_unlink_die() {
    for seed in 0_u64..8 {
        paused_runtime().block_on(async move {
            if seed % 2 == 0 {
                link_unlink_storm::<AllSup>(seed).await;
            } else {
                link_unlink_storm::<RestSup>(seed).await;
            }
        });
    }
}

/// Crashes a single `Permanent` child `crashes` times in a row under a seeded
/// jitter RNG and returns the `(attempt, measured_delay_ms)` for each rebuild.
/// The child is crashed the instant it is reborn, so its consecutive-failure
/// counter climbs monotonically (no healthy-uptime reset), and the virtual clock
/// makes each death→rebuild delay exact. Give-up budgets are raised so the full
/// backoff curve is observable; `min_backoff`/`max_backoff`/`jitter` stay at the
/// #196 defaults — they are the curve under measurement.
fn backoff_delays(seed: u64, crashes: u32) -> Vec<(u32, u128)> {
    paused_runtime().block_on(async move {
        set_supervisor_rng_seed(Some(seed));
        let origin = Instant::now();
        let (tx, mut rx) = mpsc::unbounded_channel::<(u128, &'static str)>();
        let live: LiveChildren = Arc::new(Mutex::new(HashMap::new()));
        let sup = capability::spawn::<AllSup>(());
        let cfg = RestartConfig::new(RestartPolicy::Permanent)
            .with_max_restarts(u32::MAX)
            .with_max_total(u32::MAX);
        timeout(
            TERMINATE,
            sup.supervise(
                cfg,
                tagged_factory("x", origin, tx.clone(), Arc::clone(&live)),
            ),
        )
        .await
        .expect("supervise must not hang")
        .expect("the supervisor is alive");
        let _ = timeout(TERMINATE, rx.recv())
            .await
            .expect("the birth within the bound")
            .expect("the tape is open");

        let mut delays = Vec::with_capacity(crashes as usize);
        for attempt in 1..=crashes {
            let before = Instant::now();
            crash_tag(&live, "x");
            let _ = timeout(rebuild_bound(), rx.recv())
                .await
                .expect("a rebuild must arrive — the child did not come back")
                .expect("the tape is open");
            delays.push((attempt, before.elapsed().as_millis()));
        }
        drop(sup);
        delays
    })
}

/// Invariant: the measured restart backoff obeys `delay ∈ [base(n), base(n)·1.2]`
/// for its consecutive-attempt number `n` — exponential doubling off `min_backoff`,
/// capped at `max_backoff`, plus at most the 20% jitter — and the jitter is LIVE
/// (the collected delays differ across seeds). These are the numbers that resolve
/// #196's "expected to move" tuning note; the distribution is printed for the card.
#[test]
fn dst_backoff_distribution_measured() {
    const K: u32 = 12;
    let cfg = RestartConfig::new(RestartPolicy::Permanent);
    let runs: Vec<(u64, Vec<(u32, u128)>)> = [1_u64, 2, 3]
        .map(|seed| (seed, backoff_delays(seed, K)))
        .into();

    for (seed, delays) in &runs {
        eprintln!("dst_backoff seed {seed}:");
        for (attempt, delay_ms) in delays {
            let base_ms = bombay::restart::base_backoff(&cfg, *attempt).as_millis();
            let ceiling = base_ms + base_ms / 5;
            eprintln!(
                "  attempt {attempt:>2}: base {base_ms:>6}ms  delay {delay_ms:>6}ms  \
                 (envelope {base_ms}..={ceiling})",
            );
            assert!(
                *delay_ms >= base_ms && *delay_ms <= ceiling,
                "seed {seed} attempt {attempt}: delay {delay_ms}ms outside \
                 [{base_ms}, {ceiling}]",
            );
        }
    }

    let d1 = &runs[0].1;
    let d2 = &runs[1].1;
    let d3 = &runs[2].1;
    assert!(
        !(d1 == d2 && d2 == d3),
        "jitter must vary the measured delays across seeds, got identical runs: {d1:?}",
    );
}

// ---------------------------------------------------------------------------
// Scenario 8 (card #225, ADR-0021) — control-signal interleavings: the public
// `watch` API against a target whose handler is PARKED and whose user lane is
// FULL. Pre-#225 the registration awaited mailbox capacity and this test's
// bounded wrapper fired; on the control lane it completes immediately, and the
// installed edge delivers the death notice once the backlog has drained.
// ---------------------------------------------------------------------------

/// A linked watcher that tapes every death notice id its policy receives.
struct TapingWatcher {
    notices: flume::Sender<bombay::ActorId>,
}

/// The taping reaction, as a named `WatchPolicy` (stage-3 shape).
struct TapingPolicy;
impl capability::WatchPolicy<TapingWatcher> for TapingPolicy {
    async fn on_link_died(
        actor: &mut TapingWatcher,
        id: bombay::ActorId,
        _: ActorStopReason,
        _: bool,
    ) -> Result<Step<Never, ActorStopReason>, Infallible> {
        actor
            .notices
            .try_send(id)
            .expect("the notice tape is unbounded");
        Ok(Step::Continue)
    }
}

#[derive(bombay_macros::Provide)]
struct TapingCaps {
    watching: capability::Watching<TapingPolicy>,
}
impl capability::CapSet<TapingWatcher> for TapingCaps {
    fn build(_: &<TapingWatcher as capability::Actor>::Args) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for TapingWatcher {
    type Msg = Ping;
    type Args = flume::Sender<bombay::ActorId>;
    type Error = Infallible;
    type Caps = TapingCaps;
    async fn init(notices: Self::Args, _: capability::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Self { notices })
    }
    async fn handle(&mut self, _: Ping, _: capability::Ctx<'_, Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

/// A spy whose FIRST handler call parks until the test releases it — the gate
/// that keeps the user backlog undrained while the control ops land.
struct GatedSpy {
    release: Option<oneshot::Receiver<()>>,
    handled: Arc<AtomicU32>,
}
impl Mailboxed for GatedSpy {
    type Msg = Ping;
}
impl Actor for GatedSpy {
    // (entered, release, handled)
    type Args = (oneshot::Sender<()>, oneshot::Receiver<()>, Arc<AtomicU32>);
    type Error = Infallible;
    async fn on_start(
        (entered, release, handled): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let _ = entered.send(()); // "loop parked on the first handler"
        Ok(Self {
            release: Some(release),
            handled,
        })
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        if let Some(release) = self.release.take() {
            let _ = release.await; // park with the backlog queued behind us
        }
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The full-mailbox interleaving, deterministically: the target parks in its
/// first handler with three more messages queued (lane FULL); a linked watcher's
/// `watch()` must still complete (control lane, not capacity), the backlog must
/// drain in order afterwards, and the installed edge must deliver the notice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_completes_while_target_backlog_is_full_and_parked() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let handled = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<GatedSpy>::new(SpawnConfig {
        capacity: cap(3),
        ..Default::default()
    });
    let target_ref = prepared.actor_ref().clone();
    let target_id = target_ref.id();
    let run = prepared.spawn((entered_tx, release_rx, Arc::clone(&handled)));

    // The loop dequeues message 1 and parks in its handler...
    bounded(target_ref.tell(Ping)).await.expect("message 1");
    timeout(TERMINATE, entered_rx)
        .await
        .expect("the handler must reach the gate, not hang")
        .expect("the handler is parked");
    // ...and the user lane is now filled to capacity behind it.
    for _ in 0..3 {
        bounded(target_ref.tell(Ping)).await.expect("backlog fill");
    }

    // The watcher registers from a real linked actor: pre-#225 this `.await`ed
    // the full bounded mailbox and deadlocked against the parked handler.
    let (notice_tx, notice_rx) = flume::unbounded();
    let (watcher_prepared, watcher_link_rx) =
        PreparedActor::<capability::Shell<TapingWatcher>>::new_linked(SpawnConfig {
            capacity: cap(1),
            ..Default::default()
        });
    let watcher_ref = watcher_prepared.actor_ref().clone();
    let watcher_run = watcher_prepared.spawn_linked_task(notice_tx, watcher_link_rx);
    bounded(watcher_ref.watch(&target_ref))
        .await
        .expect("watch must complete on the control lane, not await the full backlog");

    // Release the gate: the control op applies, then the backlog drains FIFO.
    release_tx.send(()).expect("release the parked handler");
    drop(target_ref); // collection after the drain
    let outcome = timeout(TERMINATE, run)
        .await
        .expect("the target must stop, not hang")
        .expect("join");
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
    assert_eq!(
        handled.load(Ordering::SeqCst),
        4,
        "the full backlog drained, undisturbed by the control op",
    );

    // The edge installed through the full backlog delivers the death notice.
    let noticed = timeout(TERMINATE, notice_rx.recv_async())
        .await
        .expect("the installed edge must deliver, not hang")
        .expect("the notice tape is open");
    assert_eq!(noticed, target_id, "the watcher learned the target's death");

    drop(watcher_ref);
    let watcher_outcome = timeout(TERMINATE, watcher_run)
        .await
        .expect("the watcher must stop, not hang")
        .expect("join");
    assert!(
        matches!(watcher_outcome, RunResult::Stopped { .. }),
        "the watcher stops cleanly, got {watcher_outcome:?}",
    );
}

// ---------------------------------------------------------------------------
// Scenario 9 (card #225, ADR-0021) — SEEDED control/user interleavings. Per
// seed, a fastrand script interleaves user-lane tells with control-lane
// watch/unwatch ops over two watcher ids, all enqueued while the target's
// handler is parked and its mailbox FULL. After release: every user message
// is handled (user FIFO intact under control interleavings), and each watcher
// id's edge state is its script's LAST op (control intra-lane FIFO), proven
// by notice/no-notice at teardown. The overtake half is structural here: the
// sync `send_control` lands while the handler is parked and the lane is full.
// ---------------------------------------------------------------------------

/// The control op kinds a seeded script can hold: each `Watch` mints a fresh
/// registration (`Watchers::apply` keeps duplicates, Erlang-style), each
/// `Unwatch` removes every edge of its id.
#[derive(Clone, Copy, Debug)]
enum ScriptCtl {
    WatchA,
    UnwatchA,
    WatchB,
    UnwatchB,
}

impl ScriptCtl {
    fn watcher(self) -> bombay::ActorId {
        match self {
            Self::WatchA | Self::UnwatchA => bombay::ActorId::from_raw_for_test(101),
            Self::WatchB | Self::UnwatchB => bombay::ActorId::from_raw_for_test(102),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_control_user_interleavings_preserve_per_lane_fifo() {
    use bombay::test_support::watch_signal;

    for seed in [3_u64, 17, 42] {
        let mut rng = fastrand::Rng::with_seed(seed);

        // A seeded script: exactly 4 user tells (fills cap(4) while parked)
        // plus 6 control ops over the two ids, shuffled together.
        let mut script: Vec<Option<ScriptCtl>> = vec![None; 4];
        for _ in 0..6 {
            let op = match rng.usize(..4) {
                0 => ScriptCtl::WatchA,
                1 => ScriptCtl::UnwatchA,
                2 => ScriptCtl::WatchB,
                _ => ScriptCtl::UnwatchB,
            };
            script.push(Some(op));
        }
        rng.shuffle(&mut script);

        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let handled = Arc::new(AtomicU32::new(0));
        let prepared = PreparedActor::<GatedSpy>::new(SpawnConfig {
            capacity: cap(4),
            ..Default::default()
        });
        let target_ref = prepared.actor_ref().clone();
        let target_id = target_ref.id();
        let run = prepared.spawn((entered_tx, release_rx, Arc::clone(&handled)));

        // Park the handler, then play the script against the full mailbox.
        bounded(target_ref.tell(Ping))
            .await
            .expect("park the handler");
        timeout(TERMINATE, entered_rx)
            .await
            .expect("the handler must reach the gate, not hang")
            .expect("the handler is parked");

        // The link receivers of every Watch minted per id, split by survival:
        // an edge survives iff no Unwatch for its id follows it in the script
        // (`Watchers::remove` clears ALL edges of the id). Surviving DUPLICATE
        // edges each deliver a notice at teardown.
        let mut alive_a = Vec::new();
        let mut dead_a = Vec::new();
        let mut alive_b = Vec::new();
        let mut dead_b = Vec::new();
        for step in &script {
            match step {
                None => bounded(target_ref.tell(Ping)).await.expect("user tell"),
                Some(op) => match op {
                    ScriptCtl::WatchA | ScriptCtl::WatchB => {
                        let (signal, link_rx) = watch_signal(op.watcher(), false);
                        // Overtake witness: lands synchronously while the handler
                        // is parked and the user lane full.
                        target_ref
                            .mailbox_sender()
                            .send_control(signal)
                            .expect("control op lands on the full mailbox");
                        if matches!(op, ScriptCtl::WatchA) {
                            alive_a.push(link_rx);
                        } else {
                            alive_b.push(link_rx);
                        }
                    }
                    ScriptCtl::UnwatchA | ScriptCtl::UnwatchB => {
                        target_ref
                            .mailbox_sender()
                            .send_control(ControlSignal::Unwatch(op.watcher()))
                            .expect("unwatch lands on the full mailbox");
                        // Every edge minted so far for this id is now removed.
                        if matches!(op, ScriptCtl::UnwatchA) {
                            dead_a.append(&mut alive_a);
                        } else {
                            dead_b.append(&mut alive_b);
                        }
                    }
                },
            }
        }

        release_tx.send(()).expect("release the parked handler");
        drop(target_ref); // collection after the drain
        let outcome = timeout(TERMINATE, run)
            .await
            .expect("the target must stop, not hang")
            .expect("join");
        assert!(
            matches!(
                outcome,
                RunResult::Stopped {
                    reason: ActorStopReason::Collected,
                    ..
                }
            ),
            "seed {seed}: ref-drop collection, got {outcome:?}",
        );
        assert_eq!(
            handled.load(Ordering::SeqCst),
            5,
            "seed {seed}: the parked message plus all 4 scripted tells were handled",
        );

        // Control intra-lane FIFO: every edge that survived to teardown (no
        // later Unwatch for its id) delivers a notice; every removed edge is
        // silent. Duplicates survive together (Erlang-style monitors).
        let expect = [(alive_a, dead_a, "A"), (alive_b, dead_b, "B")];
        for (alive, dead, name) in expect {
            for rx in alive {
                let notice = timeout(TERMINATE, rx.recv_async())
                    .await
                    .expect("seed {seed}: every surviving edge must deliver")
                    .expect("the link channel is open");
                assert_eq!(
                    notice.id, target_id,
                    "seed {seed}: watcher {name}'s surviving edge names the target",
                );
                assert!(
                    matches!(notice.reason, ActorStopReason::Collected),
                    "seed {seed}: the true reason rides watcher {name}'s notice",
                );
            }
            assert!(
                dead.iter().all(|rx| rx.try_recv().is_err()),
                "seed {seed}: watcher {name}'s removed edges deliver no notice",
            );
        }
    }
}

// ===========================================================================
// Card #266 — drain/steady watch equivalence under seeded death/close races.
//
// The strict trace oracle lives in `tests/drain_equivalence.rs`; this leg
// fuzzes the CHOREOGRAPHY around it: a seeded LCG (same pattern as
// `cyclic_topology_never_deadlocks`) derives the knob-set — how many
// watch-messages are queued (1..=3), whether the target is dropped or killed,
// whether death is injected while the watcher is parked in its handler
// (notice queued before the loop resumes) or only after the loop's break
// decision (designed-lost, card #266 decision 1), plus extra yield points —
// and the SAME knob-set runs in steady mode (external watcher ref held,
// message enqueued after spawn) and drain mode (no external ref, enqueued
// before). The oracle is the canonicalized outcome: the SORTED notice
// multiset by (reason-kind, linked) — order relative to other events may
// race — plus the ordered watch-result list and the final reason-kind.
// Exact counts are asserted per knob-set: N queued watch-messages install N
// duplicate edges, so a pre-break death delivers exactly N notices and a
// post-break death exactly 0.
//
// `WatchReasonKind` duplicates `drain_equivalence.rs`'s `ReasonKind` —
// integration tests share no code across files; keep this copy minimal.
// ===========================================================================

/// Canonical `ActorStopReason` mirror (no `PartialEq` upstream), with `Ord`
/// so the notice multiset can be sorted. Exhaustive match: a new upstream
/// variant breaks compilation here.
#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
enum WatchReasonKind {
    Normal,
    SupervisorRestart,
    Collected,
    Killed,
    AlreadyDead,
    Panicked,
    LinkDied(Box<Self>),
    RestartLimitExceeded,
    ChildLifecycleFailed,
}

impl WatchReasonKind {
    fn of(reason: &ActorStopReason) -> Self {
        match reason {
            ActorStopReason::Normal => Self::Normal,
            ActorStopReason::SupervisorRestart => Self::SupervisorRestart,
            ActorStopReason::Collected => Self::Collected,
            ActorStopReason::Killed => Self::Killed,
            ActorStopReason::AlreadyDead => Self::AlreadyDead,
            ActorStopReason::Panicked(_) => Self::Panicked,
            ActorStopReason::LinkDied { reason: inner, .. } => {
                Self::LinkDied(Box::new(Self::of(inner)))
            }
            ActorStopReason::RestartLimitExceeded { .. } => Self::RestartLimitExceeded,
            ActorStopReason::ChildLifecycleFailed { .. } => Self::ChildLifecycleFailed,
        }
    }
}

/// The seeded-race watcher: every queued `RaceGo` registers another watch on
/// the SAME target from that message's own handler ref; the LAST one drops
/// the watcher's target pin, signals `entered`, and parks on `release`.
/// `on_stop` signals the loop's break decision (`stopping`) and parks again,
/// so the test can inject the target's death either side of the break.
struct RaceWatcher {
    target: Option<ActorRef<Spy>>,
    watches_left: usize,
    watch_results: Arc<Mutex<Vec<bool>>>,
    notices: Arc<Mutex<Vec<(WatchReasonKind, bool)>>>,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
    stopping: Option<oneshot::Sender<()>>,
    stop_release: Option<oneshot::Receiver<()>>,
}

struct RaceWatcherArgs {
    target: ActorRef<Spy>,
    watches: usize,
    watch_results: Arc<Mutex<Vec<bool>>>,
    notices: Arc<Mutex<Vec<(WatchReasonKind, bool)>>>,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    stopping: oneshot::Sender<()>,
    stop_release: oneshot::Receiver<()>,
}

#[derive(Debug)]
struct RaceGo;
impl Msg for RaceGo {}
/// The recording reaction, as a named `WatchPolicy` (stage-3 shape).
struct RacePolicy;
impl capability::WatchPolicy<RaceWatcher> for RacePolicy {
    async fn on_link_died(
        actor: &mut RaceWatcher,
        _id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> Result<Step<Never, ActorStopReason>, Infallible> {
        actor
            .notices
            .lock()
            .expect("lock")
            .push((WatchReasonKind::of(&reason), linked));
        Ok(Step::Continue)
    }
}

#[derive(bombay_macros::Provide)]
struct RaceCaps {
    watching: capability::Watching<RacePolicy>,
}
impl capability::CapSet<RaceWatcher> for RaceCaps {
    fn build(_: &RaceWatcherArgs) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for RaceWatcher {
    type Msg = RaceGo;
    type Args = RaceWatcherArgs;
    type Error = Infallible;
    type Caps = RaceCaps;
    async fn init(args: Self::Args, _: capability::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        let RaceWatcherArgs {
            target,
            watches,
            watch_results,
            notices,
            entered,
            release,
            stopping,
            stop_release,
        } = args;
        Ok(Self {
            target: Some(target),
            watches_left: watches,
            watch_results,
            notices,
            entered: Some(entered),
            release: Some(release),
            stopping: Some(stopping),
            stop_release: Some(stop_release),
        })
    }
    async fn handle(
        &mut self,
        _: RaceGo,
        cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        let borrowed = self.target.as_ref().expect("the target is present");
        let outcome = bounded(cx.self_ref().watch(borrowed)).await.is_ok();
        self.watch_results.lock().expect("lock").push(outcome);
        self.watches_left = self
            .watches_left
            .checked_sub(1)
            .expect("exactly `watches` messages were enqueued");
        if self.watches_left > 0 {
            return Ok(Flow::Continue);
        }
        let owned = self.target.take().expect("the target is present");
        drop(owned); // release the pin BEFORE the test drops its own ref
        self.entered
            .take()
            .expect("entered once")
            .send(())
            .expect("the test is listening");
        bounded(self.release.take().expect("release once"))
            .await
            .expect("the release channel is open");
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<capability::Shell<Self>>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.stopping
            .take()
            .expect("on_stop runs once")
            .send(())
            .expect("the test is listening");
        // Parked past the loop's break decision; the configured 1-minute
        // grace (SpawnConfig::on_stop_grace) never trips in this choreography.
        bounded(self.stop_release.take().expect("stop_release once"))
            .await
            .expect("the stop_release channel is open");
        Ok(())
    }
}

/// One seeded knob-set, shared verbatim by the steady and drain runs.
#[derive(Debug, Clone, Copy)]
struct RaceKnobs {
    watches: usize,
    kill: bool,
    after_release: bool,
    yields: u64,
}

/// The canonicalized outcome: (sorted notice multiset, watch results, final
/// reason-kind). Sorting makes the oracle robust to notice/other-event
/// interleavings while keeping exact counts.
type RaceOutcome = (Vec<(WatchReasonKind, bool)>, Vec<bool>, WatchReasonKind);

/// Kills or drops the target and asserts its exact terminal outcome.
async fn race_death_and_join(
    kill: bool,
    target_ref: ActorRef<Spy>,
    target_join: tokio::task::JoinHandle<RunResult<Spy>>,
) {
    if kill {
        target_ref.kill();
    } else {
        drop(target_ref);
    }
    let outcome = bounded(target_join).await.expect("join target");
    if kill {
        assert!(
            matches!(outcome, RunResult::Killed),
            "kill -> Killed, got {outcome:?}"
        );
    } else {
        assert!(
            matches!(
                outcome,
                RunResult::Stopped {
                    reason: ActorStopReason::Collected,
                    ..
                }
            ),
            "drop -> Collected, got {outcome:?}"
        );
    }
}

/// Runs one seeded scenario. `steady` decides ONLY the two allowed things:
/// the held external watcher ref and enqueue-after-run vs enqueue-before-run.
/// The choreography is oneshot-gated: `stopping` proves the loop's break
/// decision, so a post-break (`after_release`) death deterministically
/// delivers ZERO notices (designed-lost), a pre-break death exactly N.
async fn race_run(steady: bool, knobs: RaceKnobs) -> RaceOutcome {
    let t_prepared = PreparedActor::<Spy>::new(SpawnConfig {
        capacity: cap(4),
        ..Default::default()
    });
    let target_ref = t_prepared.actor_ref().clone();
    let target_join = t_prepared.spawn((Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0))));

    let watch_results = Arc::new(Mutex::new(Vec::new()));
    let notices = Arc::new(Mutex::new(Vec::new()));
    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (stopping_tx, stopping) = oneshot::channel();
    let (stop_release, stop_release_rx) = oneshot::channel();
    let (w_prepared, w_link_rx) =
        PreparedActor::<capability::Shell<RaceWatcher>>::new_linked(SpawnConfig {
            capacity: cap(8),
            on_stop_grace: Duration::from_mins(1),
        });
    let external = if steady {
        Some(w_prepared.actor_ref().clone())
    } else {
        None
    };
    if !steady {
        for _ in 0..knobs.watches {
            bounded(w_prepared.actor_ref().tell(RaceGo))
                .await
                .expect("enqueue before run");
        }
    }
    let args = RaceWatcherArgs {
        target: target_ref.clone(),
        watches: knobs.watches,
        watch_results: Arc::clone(&watch_results),
        notices: Arc::clone(&notices),
        entered: entered_tx,
        release: release_rx,
        stopping: stopping_tx,
        stop_release: stop_release_rx,
    };
    let watcher_join = w_prepared.spawn_linked_task(args, w_link_rx);
    if steady {
        let held = external.as_ref().expect("steady holds a ref");
        for _ in 0..knobs.watches {
            bounded(held.tell(RaceGo)).await.expect("enqueue after run");
        }
    }

    bounded(entered).await.expect("all watches registered");
    for _ in 0..knobs.yields {
        tokio::task::yield_now().await;
    }
    // The death injection point is a KNOB (shared), never a mode branch:
    // `deferred` moves the target's fate to whichever side of the break
    // decision the seed chose.
    let mut deferred = Some((target_ref, target_join));
    if !knobs.after_release {
        let (t_ref, t_join) = deferred.take().expect("injected once");
        race_death_and_join(knobs.kill, t_ref, t_join).await;
    }
    release.send(()).expect("the watcher is parked");
    drop(external);
    bounded(stopping)
        .await
        .expect("the break decision was taken");
    if let Some((t_ref, t_join)) = deferred.take() {
        race_death_and_join(knobs.kill, t_ref, t_join).await;
    }
    stop_release.send(()).expect("on_stop is parked");
    let watcher_outcome = bounded(watcher_join).await.expect("join watcher");
    assert!(
        matches!(watcher_outcome, RunResult::Stopped { .. }),
        "the watcher stops gracefully, got {watcher_outcome:?}"
    );
    let RunResult::Stopped { reason, .. } = watcher_outcome else {
        panic!("asserted Stopped above")
    };

    let mut sorted = notices.lock().expect("lock").clone();
    sorted.sort();
    let results = watch_results.lock().expect("lock").clone();
    (sorted, results, WatchReasonKind::of(&reason))
}

/// Card #266 (bullet 7): seeded watch-vs-death-vs-close races must be
/// steady/drain equivalent. Per seed, the steady run IS the oracle: full
/// equality on the canonicalized outcome, plus explicit per-knob exact
/// assertions so the pair cannot pass on two equally-wrong runs.
#[tokio::test]
async fn drain_window_watch_races_target_death_and_close_equivalence() {
    for seed in [0xDEAD_BEEF_u64, 42, 7, 0xBAD_C0FFE] {
        let mut lcg = seed;
        let mut next = || {
            lcg = lcg.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            lcg >> 33
        };
        let knobs = RaceKnobs {
            watches: 1 + usize::try_from(next() % 3).expect("a u64 mod 3 fits usize"),
            kill: next() % 2 == 0,
            after_release: next() % 2 == 0,
            yields: next() % 4,
        };
        let steady = race_run(true, knobs).await;
        let drain = race_run(false, knobs).await;
        assert_eq!(
            steady, drain,
            "seed {seed:#x}: steady and drain runs diverge ({knobs:?})"
        );

        let expected_reason = if knobs.kill {
            WatchReasonKind::Killed
        } else {
            WatchReasonKind::Collected
        };
        let expected_count = if knobs.after_release {
            0
        } else {
            knobs.watches
        };
        let expected: Vec<(WatchReasonKind, bool)> = (0..expected_count)
            .map(|_| (expected_reason.clone(), false))
            .collect();
        assert_eq!(
            steady.0, expected,
            "seed {seed:#x}: the exact notice multiset ({knobs:?})"
        );
        assert_eq!(
            steady.1,
            vec![true; knobs.watches],
            "seed {seed:#x}: every watch succeeded ({knobs:?})"
        );
        assert_eq!(
            steady.2,
            WatchReasonKind::Collected,
            "seed {seed:#x}: the watcher collects ({knobs:?})"
        );
    }
}
