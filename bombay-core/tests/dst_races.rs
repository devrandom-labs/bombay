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

use core::convert::Infallible;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::{sync::oneshot, time::timeout};

use bombay_core::{
    actor::{Actor, ActorRef, PreparedActor, RunResult, WeakActorRef},
    error::ActorStopReason,
    mailbox::{Capacity, Mailboxed, Signal},
    message::Msg,
};

/// The suite-wide fail-fast bound: any terminal await that exceeds this is a hung
/// loop, and the test fails here rather than stalling the whole run.
const TERMINATE: Duration = Duration::from_secs(5);

fn cap(n: usize) -> Capacity {
    Capacity::try_from(n).expect("valid test capacity")
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
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(())
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
        async fn handle(
            &mut self,
            _: Ping,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(())
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

    let prepared = PreparedActor::<StartGate>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    // Pre-queue a message: it must never be handled, because on_start never ends.
    actor_ref.tell(Ping).await.expect("pre-queue");
    let run = prepared.spawn((
        entered_tx,
        release_rx,
        Arc::clone(&handled),
        Arc::clone(&stopped),
    ));

    entered_rx.await.expect("on_start reached the gate");
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
        async fn handle(
            &mut self,
            _: Ping,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
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

    let prepared = PreparedActor::<StopGate>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn((entered_tx, release_rx, Arc::clone(&post_park)));

    actor_ref.stop(); // graceful → loop returns Normal → on_stop runs
    entered_rx.await.expect("on_stop reached the gate");
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
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
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

    let prepared = PreparedActor::<StartSignaled>::new(cap(4));
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

    let prepared = PreparedActor::<Spy>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref
        .mailbox_sender()
        .send(Signal::Stop)
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

    let prepared = PreparedActor::<Spy>::new(cap(4));
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

    let prepared = PreparedActor::<Spy>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref
        .mailbox_sender()
        .send(Signal::Stop)
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

    let prepared = PreparedActor::<Spy>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref
        .mailbox_sender()
        .send(Signal::Stop)
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
    let resend = actor_ref
        .mailbox_sender()
        .send(Signal::Message {
            msg: Ping,
            self_sender: actor_ref.mailbox_sender().clone(),
        })
        .await;
    assert!(
        matches!(
            resend,
            Err(bombay_core::mailbox::SendError(Signal::Message {
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
// Scenario 7 — `kill()` after a normal completion (via the handler stop flag).
// ---------------------------------------------------------------------------

/// An actor that finishes itself by setting the stop flag in its handler, then
/// counts `on_stop`.
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
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        stop: &mut bool,
    ) -> Result<(), Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        *stop = true; // stop cleanly after this handler returns Ok
        Ok(())
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

/// The actor stops normally via its handler's stop flag; a `kill()` issued AFTER
/// the run has returned is a no-op — no panic, and the outcome stays `Normal` with
/// `on_stop` having run once.
#[tokio::test]
async fn kill_after_normal_completion_is_a_noop() {
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<SelfStop>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref.tell(Ping).await.expect("enqueue one message");

    let outcome = timeout(
        TERMINATE,
        prepared.run((Arc::clone(&handled), Arc::clone(&stopped))),
    )
    .await
    .expect("the stop flag must terminate the actor");

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
