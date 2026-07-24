//! Authoritative, DIRECT verification of the actor-system invariants (card #116).
//!
//! Each test asserts the invariant *itself* — the property an actor model must
//! carry — using the public `bombay_core` API only, with `oneshot` barriers to
//! force orderings and a 5 s `tokio::time::timeout` around every "must terminate"
//! await so a regression that hangs the loop FAILS FAST rather than stalling the
//! suite. Handler panics are caught by the loop and observed via `RunResult` +
//! spies (never `#[should_panic]`). Every assertion is a *specific* value (exact
//! counts, exact `RunResult` variant + `PanicReason`, exact ordered vectors).
//!
//! # Invariant map (I1–I23)
//!
//! Proven directly in THIS file:
//!   I1  single-writer mutual exclusion (max concurrent handlers == 1)
//!         -> i1_single_writer_mutual_exclusion
//!   I3  macro-step atomicity (read-modify-write across an await is not torn)
//!         -> i3_macro_step_atomicity
//!   I4  message FIFO ordering                 -> covered by i5_fifo_exactly_once
//!   I5  exactly-once-while-alive              -> i5_fifo_exactly_once
//!   I7  no reentrancy / self-send is enqueued, not re-entered
//!         -> i7_no_reentrancy_self_send_is_enqueued
//!   I8  lifecycle ordering (start < handle* < stop)
//!         -> i8_i10_i11_lifecycle_order_normal / _panic
//!   I9c on_stop is NOT run on startup failure -> i9c_on_stop_not_run_on_startup_failure
//!   I10 exactly-once lifecycle hooks          -> i8_i10_i11_lifecycle_order_normal
//!   I11 nothing runs after on_stop            -> i8_i10_i11_lifecycle_order_normal
//!   I12 alive-window (pre-run buffered handled; post-stop send rejected)
//!         -> i12_alive_window
//!   I13 no loss / no duplication              -> covered by i5_fifo_exactly_once
//!   I14 stop-reason fidelity under on_stop failure (+ panic containment)
//!         -> i14a_normal_stop_on_stop_panic_preserves_normal
//!            i14b_normal_stop_on_stop_err_preserves_normal
//!            i14c_handler_panic_then_on_stop_panic_preserves_original_cause
//!   I15 fault isolation across actors         -> i15_fault_isolation
//!   I17 distinct actor ids                    -> i17_distinct_ids
//!   I19 ref-send liveness / send-to-dead fails -> i19_send_and_weak_upgrade_after_termination
//!   I20 backpressure: a full mailbox rejects (message handed back)
//!         -> i20_i21_backpressure_and_capacity_freed_by_draining
//!   I21 capacity is freed by the loop draining -> (same test)
//!
//! Proven elsewhere (listed here, not re-implemented):
//!   I6  startup buffering (msgs during on_start handled after, in FIFO)
//!         -> spawn.rs::messages_during_on_start_are_handled_after_in_order
//!   I8  finish-current-then-stop, no drain
//!         -> spawn.rs::cancel_finishes_in_flight_then_stops; dst_races.rs
//!   I9  kill skips on_stop
//!         -> spawn.rs::kill_skips_on_stop_and_drops_in_flight; dst_races.rs
//!   I16 poison: on_stop observes the torn field (counter == 99)
//!         -> spawn.rs::on_stop_after_panic_observes_torn_state
//!   I18 weak-upgrade while open, None after the last strong sender drops
//!         -> actor_ref.rs::weak_upgrades_while_open_then_none_after_drop
//!   I23 no starvation (implied by FIFO + exactly-once, I4 + I5)
//!         -> i5_fifo_exactly_once
//!
//! I2 and I22 are not individually enumerated by card #116's invariant set; the
//! one-message-at-a-time property they would name is exactly what I1 asserts here.

use core::convert::Infallible;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::{sync::oneshot, task::yield_now, time::timeout};

use bombay_core::{
    actor::{Actor, ActorRef, PreparedActor, RunResult, WeakActorRef},
    error::{ActorStopReason, PanicError, PanicReason, TellError},
    mailbox::{Capacity, Mailboxed, Signal, TrySendError},
    message::Msg,
    test_support::terminate_bound,
};

/// The suite-wide fail-fast bound: any terminal await exceeding this is a hung
/// loop — a real bug — and the test fails here rather than stalling the run.
/// Scaled under MIRI — see `terminate_bound`.
const TERMINATE: Duration = terminate_bound();

fn cap(n: usize) -> Capacity {
    Capacity::try_from(n).expect("valid test capacity")
}

/// Bounds a pre-run/test-side send under the fail-fast bound (card #179): a
/// mutant that stalls the mailbox (e.g. `Capacity::get -> 0` turning the queue
/// into a rendezvous with no receiver yet) must FAIL here, not hang the whole
/// test binary past the mutants sweep timeout.
async fn bounded<F: std::future::IntoFuture>(fut: F) -> F::Output {
    timeout(TERMINATE, fut)
        .await
        .expect("send must not hang: the mailbox stalled")
}

/// A stand-in domain error (any `Debug + Send + Sync + 'static` is a `ReplyError`).
#[derive(Debug)]
struct Boom;

/// A trivial spy actor: counts handled messages. Reused by the alive-window,
/// distinct-id and send-to-dead invariants where no bespoke behaviour is needed.
struct Bank {
    handled: Arc<AtomicU32>,
}
#[derive(Debug)]
struct Poke;
impl Msg for Poke {}
impl Mailboxed for Bank {
    type Msg = Poke;
}
impl Actor for Bank {
    type Args = Arc<AtomicU32>;
    type Error = Infallible;
    async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { handled })
    }
    async fn handle(
        &mut self,
        _: Poke,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// I1 — single-writer mutual exclusion: at most ONE handler runs at any instant.
// ---------------------------------------------------------------------------

/// Eight senders race `PER_SENDER` messages each at one actor from the same
/// instant. The handler bumps a live `concurrent` counter, records the running
/// `max`, yields three times to open a real interleaving window, then drops the
/// counter. If the loop EVER ran two handlers at once, `max` would be >= 2.
/// ASSERT `max == 1` (mutual exclusion) AND every message handled exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn i1_single_writer_mutual_exclusion() {
    use tokio::sync::Barrier;

    const SENDERS: u32 = 8;
    const PER_SENDER: u32 = 25;
    const TOTAL: u32 = SENDERS * PER_SENDER;

    struct Excl {
        concurrent: Arc<AtomicU32>,
        max: Arc<AtomicU32>,
        handled: Arc<AtomicU32>,
        done_at: u32,
    }
    #[derive(Debug)]
    struct Bump;
    impl Msg for Bump {}
    impl Mailboxed for Excl {
        type Msg = Bump;
    }
    impl Actor for Excl {
        type Args = (Arc<AtomicU32>, Arc<AtomicU32>, Arc<AtomicU32>, u32);
        type Error = Infallible;
        async fn on_start(
            (concurrent, max, handled, done_at): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self {
                concurrent,
                max,
                handled,
                done_at,
            })
        }
        async fn handle(
            &mut self,
            _: Bump,
            _: ActorRef<Self>,
            stop: &mut bool,
        ) -> Result<(), Self::Error> {
            let n = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
            self.max.fetch_max(n, Ordering::SeqCst);
            yield_now().await;
            yield_now().await;
            yield_now().await;
            self.concurrent.fetch_sub(1, Ordering::SeqCst);
            let handled = self.handled.fetch_add(1, Ordering::SeqCst) + 1;
            if handled == self.done_at {
                *stop = true;
            }
            Ok(())
        }
    }

    let concurrent = Arc::new(AtomicU32::new(0));
    let max = Arc::new(AtomicU32::new(0));
    let handled = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<Excl>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn((
        Arc::clone(&concurrent),
        Arc::clone(&max),
        Arc::clone(&handled),
        TOTAL,
    ));

    let start = Arc::new(Barrier::new(SENDERS as usize));
    let mut tasks = Vec::new();
    for _ in 0..SENDERS {
        let sender = actor_ref.mailbox_sender().clone();
        let start = Arc::clone(&start);
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            for _ in 0..PER_SENDER {
                timeout(TERMINATE, sender.send_message(Bump))
                    .await
                    .expect("send must not hang: the mailbox stalled")
                    .expect("send");
            }
        }));
    }
    for task in tasks {
        task.await.expect("sender task");
    }
    let outcome = timeout(TERMINATE, run)
        .await
        .expect("the actor must stop after the last message")
        .expect("join");

    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "clean normal stop, got {outcome:?}",
    );
    assert_eq!(
        max.load(Ordering::SeqCst),
        1,
        "at most one handler ran at any instant — the single-writer invariant",
    );
    assert_eq!(
        handled.load(Ordering::SeqCst),
        TOTAL,
        "every message handled exactly once (none lost, none double-counted)",
    );
}

// ---------------------------------------------------------------------------
// I3 — macro-step atomicity: a read-modify-write straddling an await is not torn.
// ---------------------------------------------------------------------------

/// A single sender sends `Add(1..=10)`. Each handler reads `counter`, yields, then
/// writes `old + n` — a read-modify-write with an await in the middle. Because
/// handlers never overlap, no update is lost and no torn intermediate is observed
/// across the await. ASSERT the recovered `counter` equals the exact sum (55).
#[tokio::test]
async fn i3_macro_step_atomicity() {
    struct Acc {
        counter: u64,
    }
    #[derive(Debug)]
    struct Add(u64);
    impl Msg for Add {}
    impl Mailboxed for Acc {
        type Msg = Add;
    }
    impl Actor for Acc {
        type Args = ();
        type Error = Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { counter: 0 })
        }
        async fn handle(
            &mut self,
            Add(n): Add,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            let old = self.counter;
            yield_now().await;
            self.counter = old + n;
            Ok(())
        }
    }

    let prepared = PreparedActor::<Acc>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn(());
    for n in 1..=10u64 {
        bounded(actor_ref.tell(Add(n))).await.expect("send add");
    }
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("send stop");

    let outcome = timeout(TERMINATE, run)
        .await
        .expect("the actor must stop")
        .expect("join");
    let RunResult::Stopped { actor, reason } = outcome else {
        panic!("expected Stopped, got {outcome:?}");
    };
    assert_eq!(
        actor.counter, 55,
        "read-modify-write across the await accumulated exactly — no lost update",
    );
    assert!(matches!(reason, ActorStopReason::Normal), "clean stop");
}

// ---------------------------------------------------------------------------
// I5 — message FIFO + exactly-once while alive (covers I4 FIFO + I13 no loss/dup).
// ---------------------------------------------------------------------------

/// One sender sends `0..N`; the actor records each received value. ASSERT the
/// recorded vector equals `(0..N)` exactly — every message handled exactly once,
/// in order, none lost, none duplicated.
#[tokio::test]
async fn i5_fifo_exactly_once() {
    const N: u64 = 100;

    struct Rec {
        seen: Arc<Mutex<Vec<u64>>>,
    }
    #[derive(Debug)]
    struct V(u64);
    impl Msg for V {}
    impl Mailboxed for Rec {
        type Msg = V;
    }
    impl Actor for Rec {
        type Args = Arc<Mutex<Vec<u64>>>;
        type Error = Infallible;
        async fn on_start(seen: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { seen })
        }
        async fn handle(
            &mut self,
            V(v): V,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            self.seen.lock().expect("lock").push(v);
            Ok(())
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let prepared = PreparedActor::<Rec>::new(cap(8));
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn(Arc::clone(&seen));
    for v in 0..N {
        bounded(actor_ref.tell(V(v))).await.expect("send");
    }
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop");

    let outcome = timeout(TERMINATE, run)
        .await
        .expect("the actor must stop")
        .expect("join");
    assert!(matches!(
        outcome,
        RunResult::Stopped {
            reason: ActorStopReason::Normal,
            ..
        }
    ));
    assert_eq!(
        *seen.lock().expect("lock"),
        (0..N).collect::<Vec<u64>>(),
        "every message handled exactly once, in FIFO order — no loss, no duplication",
    );
}

// ---------------------------------------------------------------------------
// I7 — no reentrancy: a self-send is enqueued, never re-entered inside a handler.
// ---------------------------------------------------------------------------

/// The `First` handler marks itself active, self-sends `Second`, then yields
/// (opening a reentrancy window) before clearing the flag. If the loop dispatched
/// `Second` reentrantly inside `First`, the `Second` handler would observe the
/// flag still set. ASSERT `Second` sees the flag clear (no reentrancy) and the
/// handled order is exactly `[First, Second]`.
#[tokio::test]
async fn i7_no_reentrancy_self_send_is_enqueued() {
    #[derive(Debug)]
    enum M {
        First,
        Second,
    }
    impl Msg for M {}
    struct Reentry {
        in_handle: Arc<AtomicBool>,
        handled: Arc<Mutex<Vec<&'static str>>>,
    }
    impl Mailboxed for Reentry {
        type Msg = M;
    }
    impl Actor for Reentry {
        type Args = (Arc<AtomicBool>, Arc<Mutex<Vec<&'static str>>>);
        type Error = Infallible;
        async fn on_start(
            (in_handle, handled): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self { in_handle, handled })
        }
        async fn handle(
            &mut self,
            msg: M,
            actor_ref: ActorRef<Self>,
            stop: &mut bool,
        ) -> Result<(), Self::Error> {
            match msg {
                M::First => {
                    assert!(
                        !self.in_handle.load(Ordering::SeqCst),
                        "no handler was active when First began",
                    );
                    self.in_handle.store(true, Ordering::SeqCst);
                    self.handled.lock().expect("lock").push("First");
                    actor_ref.tell(M::Second).await.expect("self-send Second");
                    yield_now().await; // a reentrant loop would run Second here
                    self.in_handle.store(false, Ordering::SeqCst);
                }
                M::Second => {
                    assert!(
                        !self.in_handle.load(Ordering::SeqCst),
                        "Second did NOT run reentrantly inside First",
                    );
                    self.handled.lock().expect("lock").push("Second");
                    *stop = true;
                }
            }
            Ok(())
        }
    }

    let in_handle = Arc::new(AtomicBool::new(false));
    let handled = Arc::new(Mutex::new(Vec::new()));
    let prepared = PreparedActor::<Reentry>::new(cap(8));
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn((Arc::clone(&in_handle), Arc::clone(&handled)));
    bounded(actor_ref.tell(M::First)).await.expect("send First");

    let outcome = timeout(TERMINATE, run)
        .await
        .expect("the actor must stop")
        .expect("join");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "no in-handler assert tripped (which would surface as Panicked), got {outcome:?}",
    );
    assert_eq!(
        *handled.lock().expect("lock"),
        vec!["First", "Second"],
        "self-sent Second was enqueued and handled after First returned",
    );
}

// ---------------------------------------------------------------------------
// I8 / I10 / I11 — lifecycle ordering, exactly-once hooks, nothing after on_stop.
// ---------------------------------------------------------------------------

/// One actor whose every lifecycle step appends to a shared log: `on_start`
/// pushes `"start"`, each `handle` pushes `"handle"`, `on_panic` pushes `"panic"`,
/// `on_stop` pushes `"stop"`. `panic_at` makes the handler panic on the Nth
/// message (torn-write-free: it panics before pushing).
struct Life {
    count: u32,
    panic_at: Option<u32>,
    log: Arc<Mutex<Vec<&'static str>>>,
}
#[derive(Debug)]
struct Tick;
impl Msg for Tick {}
impl Mailboxed for Life {
    type Msg = Tick;
}
impl Actor for Life {
    type Args = (Option<u32>, Arc<Mutex<Vec<&'static str>>>);
    type Error = Infallible;
    async fn on_start((panic_at, log): Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        log.lock().expect("lock").push("start");
        Ok(Self {
            count: 0,
            panic_at,
            log,
        })
    }
    async fn handle(
        &mut self,
        _: Tick,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        self.count += 1;
        if self.panic_at == Some(self.count) {
            panic!("handler boom");
        }
        self.log.lock().expect("lock").push("handle");
        Ok(())
    }
    async fn on_panic(&mut self, _: WeakActorRef<Self>, err: PanicError) -> ActorStopReason {
        self.log.lock().expect("lock").push("panic");
        ActorStopReason::Panicked(err)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.log.lock().expect("lock").push("stop");
        Ok(())
    }
}

/// Normal path: three messages then a graceful stop. ASSERT the log is exactly
/// `["start", "handle", "handle", "handle", "stop"]` — `on_start` first and once,
/// three handles in the middle, `on_stop` last and once (nothing after it).
#[tokio::test]
async fn i8_i10_i11_lifecycle_order_normal() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let prepared = PreparedActor::<Life>::new(cap(8));
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn((None, Arc::clone(&log)));
    for _ in 0..3 {
        bounded(actor_ref.tell(Tick)).await.expect("send");
    }
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop");

    let outcome = timeout(TERMINATE, run)
        .await
        .expect("the actor must stop")
        .expect("join");
    assert!(matches!(
        outcome,
        RunResult::Stopped {
            reason: ActorStopReason::Normal,
            ..
        }
    ));
    assert_eq!(
        *log.lock().expect("lock"),
        vec!["start", "handle", "handle", "handle", "stop"],
        "start once & first; three handles; stop once & LAST",
    );
}

/// Panic path: the handler panics on the 2nd message. ASSERT the log is exactly
/// `["start", "handle", "panic", "stop"]` — `on_panic` runs on the panic path,
/// once, before `on_stop`, and `on_stop` still runs and is last.
#[tokio::test]
async fn i8_i10_i11_lifecycle_order_panic() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let prepared = PreparedActor::<Life>::new(cap(8));
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn((Some(2), Arc::clone(&log)));
    bounded(actor_ref.tell(Tick)).await.expect("send 1");
    bounded(actor_ref.tell(Tick))
        .await
        .expect("send 2 (panics)");

    let outcome = timeout(TERMINATE, run)
        .await
        .expect("the actor must stop")
        .expect("join");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Panicked(_),
                ..
            }
        ),
        "handler panic → Stopped/Panicked, got {outcome:?}",
    );
    assert_eq!(
        *log.lock().expect("lock"),
        vec!["start", "handle", "panic", "stop"],
        "on_panic before on_stop, both once; on_stop last",
    );
}

// ---------------------------------------------------------------------------
// I9c — on_stop is NOT run on startup failure (and IS run on a normal stop).
// ---------------------------------------------------------------------------

/// `on_start` returning `Err` yields `StartupFailed` and never runs `on_stop`
/// (no state was built to clean up); a normal stop runs `on_stop` exactly once.
#[tokio::test]
async fn i9c_on_stop_not_run_on_startup_failure() {
    struct MaybeStart {
        spy: Arc<AtomicU32>,
    }
    #[derive(Debug)]
    struct Go;
    impl Msg for Go {}
    impl Mailboxed for MaybeStart {
        type Msg = Go;
    }
    impl Actor for MaybeStart {
        type Args = (bool, Arc<AtomicU32>);
        type Error = Boom;
        async fn on_start((fail, spy): Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            if fail { Err(Boom) } else { Ok(Self { spy }) }
        }
        async fn handle(
            &mut self,
            _: Go,
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
            self.spy.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // Startup failure → StartupFailed, on_stop never runs.
    let spy = Arc::new(AtomicU32::new(0));
    let outcome = timeout(
        TERMINATE,
        PreparedActor::<MaybeStart>::new(cap(4)).run((true, Arc::clone(&spy))),
    )
    .await
    .expect("startup failure must terminate the run");
    assert!(
        matches!(outcome, RunResult::StartupFailed(_)),
        "on_start Err → StartupFailed, got {outcome:?}",
    );
    assert_eq!(
        spy.load(Ordering::SeqCst),
        0,
        "on_stop NOT run when on_start failed",
    );

    // Normal stop → on_stop runs exactly once.
    let spy2 = Arc::new(AtomicU32::new(0));
    let prepared = PreparedActor::<MaybeStart>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop");
    let outcome2 = timeout(TERMINATE, prepared.run((false, Arc::clone(&spy2))))
        .await
        .expect("normal stop must terminate the run");
    assert!(matches!(
        outcome2,
        RunResult::Stopped {
            reason: ActorStopReason::Normal,
            ..
        }
    ));
    assert_eq!(
        spy2.load(Ordering::SeqCst),
        1,
        "on_stop runs exactly once on a normal stop",
    );
}

// ---------------------------------------------------------------------------
// I12 — alive-window: pre-run buffered message IS handled; post-stop send fails.
// ---------------------------------------------------------------------------

/// (a) A message enqueued BEFORE `run` starts (into the `PreparedActor`'s mailbox)
/// IS handled after start. (b) After a graceful stop completes, a `send` on a
/// retained sender is rejected with `SendError` (the message handed back), not
/// silently accepted.
#[tokio::test]
async fn i12_alive_window() {
    let handled = Arc::new(AtomicU32::new(0));
    let prepared = PreparedActor::<Bank>::new(cap(8));
    let actor_ref = prepared.actor_ref().clone();

    // (a) enqueue before the loop starts, plus a Stop to end it.
    bounded(actor_ref.tell(Poke)).await.expect("pre-run send");
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop");

    let outcome = timeout(TERMINATE, prepared.run(Arc::clone(&handled)))
        .await
        .expect("the actor must stop");
    assert!(matches!(
        outcome,
        RunResult::Stopped {
            reason: ActorStopReason::Normal,
            ..
        }
    ));
    assert_eq!(
        handled.load(Ordering::SeqCst),
        1,
        "the message buffered before run was handled after start",
    );

    // (b) send-after-stop is rejected with the message handed back.
    let resend = actor_ref.tell(Poke).await;
    assert!(
        matches!(resend, Err(TellError::ActorNotAlive(Poke))),
        "send after a completed stop is rejected, not silently accepted",
    );
}

// ---------------------------------------------------------------------------
// I14 — stop-reason fidelity under on_stop failure (+ panic containment, I5/I6).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum StopMode {
    Panic,
    Err,
}

/// Handler panics (or not) and `on_stop` panics / errors / succeeds, per config.
struct Cleanup {
    handler_panics: bool,
    on_stop_mode: StopMode,
}
#[derive(Debug)]
struct Do;
impl Msg for Do {}
impl Mailboxed for Cleanup {
    type Msg = Do;
}
impl Actor for Cleanup {
    type Args = (bool, StopMode);
    type Error = Boom;
    async fn on_start(
        (handler_panics, on_stop_mode): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            handler_panics,
            on_stop_mode,
        })
    }
    async fn handle(&mut self, _: Do, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
        if self.handler_panics {
            panic!("handler boom");
        }
        Ok(())
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        match self.on_stop_mode {
            StopMode::Panic => panic!("cleanup boom"),
            StopMode::Err => Err(Boom),
        }
    }
}

/// (a) A normal stop whose `on_stop` PANICS is `Stopped { reason: Normal }` — the
/// panic is contained (the test completing proves it did not abort the process)
/// AND the reason is preserved, not rewritten to Panicked/OnStop.
#[tokio::test]
async fn i14a_normal_stop_on_stop_panic_preserves_normal() {
    let prepared = PreparedActor::<Cleanup>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop");
    let outcome = timeout(TERMINATE, prepared.run((false, StopMode::Panic)))
        .await
        .expect("must terminate");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "on_stop panic contained; Normal preserved, got {outcome:?}",
    );
}

/// (b) A normal stop whose `on_stop` returns `Err` is still `Stopped { Normal }` —
/// the error is logged, never unwrapped, and the reason is preserved.
#[tokio::test]
async fn i14b_normal_stop_on_stop_err_preserves_normal() {
    let prepared = PreparedActor::<Cleanup>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop");
    let outcome = timeout(TERMINATE, prepared.run((false, StopMode::Err)))
        .await
        .expect("must terminate");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "on_stop Err surfaced but reason preserved as Normal, got {outcome:?}",
    );
}

/// (c) A handler panic (→ Panicked/HandlerPanic) followed by an `on_stop` that
/// ALSO panics is `Stopped { reason: Panicked(pe) }` with `pe.reason() ==
/// HandlerPanic` — the ORIGINAL cause survives; the on_stop panic is contained and
/// does not overwrite it with OnStop.
#[tokio::test]
async fn i14c_handler_panic_then_on_stop_panic_preserves_original_cause() {
    let prepared = PreparedActor::<Cleanup>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.tell(Do)).await.expect("send");
    let outcome = timeout(TERMINATE, prepared.run((true, StopMode::Panic)))
        .await
        .expect("must terminate");
    let RunResult::Stopped {
        reason: ActorStopReason::Panicked(pe),
        ..
    } = outcome
    else {
        panic!("expected Stopped/Panicked, got {outcome:?}");
    };
    assert_eq!(
        pe.reason(),
        PanicReason::HandlerPanic,
        "the original handler-panic cause is preserved, not overwritten by OnStop",
    );
}

// ---------------------------------------------------------------------------
// I15 — fault isolation: one actor's crash does not affect another or the runtime.
// ---------------------------------------------------------------------------

/// Two actors run on the same runtime. A's handler panics; B then handles a
/// message normally and stops. ASSERT A's outcome is Panicked(HandlerPanic), B
/// handled its message and stopped Normal — the crash is contained to A.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i15_fault_isolation() {
    struct Faulty;
    #[derive(Debug)]
    struct Crash;
    impl Msg for Crash {}
    impl Mailboxed for Faulty {
        type Msg = Crash;
    }
    impl Actor for Faulty {
        type Args = ();
        type Error = Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Faulty)
        }
        async fn handle(
            &mut self,
            _: Crash,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            panic!("actor A handler boom");
        }
    }

    struct Healthy {
        handled: Arc<AtomicU32>,
    }
    #[derive(Debug)]
    struct Work;
    impl Msg for Work {}
    impl Mailboxed for Healthy {
        type Msg = Work;
    }
    impl Actor for Healthy {
        type Args = Arc<AtomicU32>;
        type Error = Infallible;
        async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { handled })
        }
        async fn handle(
            &mut self,
            _: Work,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let a_prepared = PreparedActor::<Faulty>::new(cap(4));
    let a_ref = a_prepared.actor_ref().clone();
    let a_run = a_prepared.spawn(());

    let b_spy = Arc::new(AtomicU32::new(0));
    let b_prepared = PreparedActor::<Healthy>::new(cap(4));
    let b_ref = b_prepared.actor_ref().clone();
    let b_run = b_prepared.spawn(Arc::clone(&b_spy));

    // A crashes.
    bounded(a_ref.tell(Crash)).await.expect("send crash to A");
    // B keeps working after A's crash.
    bounded(b_ref.tell(Work)).await.expect("send work to B");
    bounded(b_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop B");

    let a_out = timeout(TERMINATE, a_run)
        .await
        .expect("A must terminate")
        .expect("A join");
    let b_out = timeout(TERMINATE, b_run)
        .await
        .expect("B must terminate")
        .expect("B join");

    let RunResult::Stopped {
        reason: ActorStopReason::Panicked(pe),
        ..
    } = a_out
    else {
        panic!("A should have panicked, got {a_out:?}");
    };
    assert_eq!(
        pe.reason(),
        PanicReason::HandlerPanic,
        "A crashed via a handler panic",
    );
    assert!(
        matches!(
            b_out,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "B is unaffected by A's crash and stops Normal, got {b_out:?}",
    );
    assert_eq!(
        b_spy.load(Ordering::SeqCst),
        1,
        "B still handled its message despite A crashing",
    );
}

// ---------------------------------------------------------------------------
// I17 — every actor gets a distinct id.
// ---------------------------------------------------------------------------

/// Build 100 `PreparedActor`s and collect their ids. ASSERT all 100 are pairwise
/// distinct. (`ActorId` is `Eq` but not `Hash`, so distinctness is asserted by
/// pairwise `assert_ne!` rather than a `HashSet`.)
#[test]
fn i17_distinct_ids() {
    let ids: Vec<_> = (0..100)
        .map(|_| PreparedActor::<Bank>::new(cap(1)).actor_ref().id())
        .collect();
    assert_eq!(ids.len(), 100, "built 100 actors");
    for i in 0..ids.len() {
        for j in (i + 1)..ids.len() {
            assert_ne!(ids[i], ids[j], "actor ids at {i} and {j} must be distinct");
        }
    }
}

// ---------------------------------------------------------------------------
// I19 — ref-send liveness + weak-ref liveness after termination.
// ---------------------------------------------------------------------------

/// After a normal stop: a `send` on a retained strong sender fails (the receiver
/// is gone, message handed back), and — once the last strong sender is dropped —
/// a retained `WeakActorRef::upgrade()` returns `None`. (The weak handle tracks
/// strong senders, so the strong ref is dropped between the two checks.)
#[tokio::test]
async fn i19_send_and_weak_upgrade_after_termination() {
    let handled = Arc::new(AtomicU32::new(0));
    let prepared = PreparedActor::<Bank>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    let weak = actor_ref.downgrade();
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop");

    let outcome = timeout(TERMINATE, prepared.run(Arc::clone(&handled)))
        .await
        .expect("the actor must stop");
    assert!(matches!(
        outcome,
        RunResult::Stopped {
            reason: ActorStopReason::Normal,
            ..
        }
    ));

    // Ref-send liveness: the receiver is gone, so `tell` fails with the message
    // back (and, unlike raw `send`, drops the envelope — so it retains no strong
    // sender that would keep the weak upgrade below alive; ADR-0003).
    let resend = actor_ref.tell(Poke).await;
    assert!(
        matches!(resend, Err(TellError::ActorNotAlive(Poke))),
        "send to a terminated actor fails with the message handed back",
    );

    // Weak-ref liveness: drop the last strong sender, then upgrade must be None.
    drop(actor_ref);
    assert!(
        weak.upgrade().is_none(),
        "no strong sender remains → weak upgrade yields None",
    );
}

// ---------------------------------------------------------------------------
// I20 / I21 — backpressure rejects (message returned) + capacity freed by drain.
// ---------------------------------------------------------------------------

/// A capacity-1 mailbox with a handler that blocks on the first message. With the
/// slot then filled, a further `try_send` is rejected with `Full` and the message
/// handed back (no loss). Releasing the handler lets the loop drain the queued
/// message, freeing the slot; the SAME handed-back message then `try_send`s
/// successfully — backpressure is transient, capacity is freed by draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i20_i21_backpressure_and_capacity_freed_by_draining() {
    #[derive(Debug)]
    enum Cmd {
        Block,
        Drain,
    }
    impl Msg for Cmd {}
    struct Gate {
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
        drained: Option<oneshot::Sender<()>>,
    }
    impl Mailboxed for Gate {
        type Msg = Cmd;
    }
    impl Actor for Gate {
        type Args = (
            oneshot::Sender<()>,
            oneshot::Receiver<()>,
            oneshot::Sender<()>,
        );
        type Error = Infallible;
        async fn on_start(
            (entered, release, drained): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self {
                entered: Some(entered),
                release: Some(release),
                drained: Some(drained),
            })
        }
        async fn handle(
            &mut self,
            msg: Cmd,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            match msg {
                Cmd::Block => {
                    if let Some(entered) = self.entered.take() {
                        let _ = entered.send(());
                    }
                    if let Some(release) = self.release.take() {
                        let _ = release.await;
                    }
                }
                Cmd::Drain => {
                    // Fires as the queued message is dequeued for handling — i.e.
                    // once the slot it occupied has been freed.
                    if let Some(drained) = self.drained.take() {
                        let _ = drained.send(());
                    }
                }
            }
            Ok(())
        }
    }

    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (drained_tx, drained_rx) = oneshot::channel();

    let prepared = PreparedActor::<Gate>::new(cap(1));
    let actor_ref = prepared.actor_ref().clone();
    let run = prepared.spawn((entered_tx, release_rx, drained_tx));

    // (1) The Block message enters the handler and parks; its slot is now free.
    bounded(actor_ref.tell(Cmd::Block))
        .await
        .expect("send Block");
    timeout(TERMINATE, entered_rx)
        .await
        .expect("Block must reach the handler, not hang")
        .expect("handler entered and parked");

    // (2) Fill the single slot.
    actor_ref
        .mailbox_sender()
        .try_send(Signal::Message {
            msg: Cmd::Drain,
            self_sender: actor_ref.mailbox_sender().clone(),
        })
        .expect("the one free slot accepts Drain");

    // (3) The mailbox is full: try_send is rejected and hands the message back.
    let rejected = actor_ref.mailbox_sender().try_send(Signal::Message {
        msg: Cmd::Drain,
        self_sender: actor_ref.mailbox_sender().clone(),
    });
    let Err(TrySendError::Full(returned)) = rejected else {
        panic!("expected Full rejection, got {rejected:?}");
    };

    // (4) Release the handler; the loop drains the queued Drain, freeing the slot.
    release_tx.send(()).expect("release the parked handler");
    timeout(TERMINATE, drained_rx)
        .await
        .expect("the queued Drain must be dequeued, not hang")
        .expect("queued Drain dequeued (slot freed)");

    // (5) The SAME handed-back message now fits — capacity was freed by draining.
    actor_ref
        .mailbox_sender()
        .try_send(returned)
        .expect("capacity freed by the loop draining; the returned message fits");

    // Clean up: stop the actor and confirm a normal termination.
    actor_ref.stop();
    let outcome = timeout(TERMINATE, run)
        .await
        .expect("the actor must stop")
        .expect("join");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "clean normal stop, got {outcome:?}",
    );
}

// ---------------------------------------------------------------------------
// #113 deferral, landed on #118: ActorNotAlive unifies every terminal state.
// ---------------------------------------------------------------------------

/// `actor_not_alive_unifies_terminal` — every way an actor can be *not
/// running* surfaces to a sender as the SAME terminal
/// `TellError::ActorNotAlive`, message handed back, so a caller needs no
/// state-specific handling:
///
/// - **never run** — the `PreparedActor` was dropped before `run`; the actor
///   never existed (this test);
/// - **startup failed** — `on_start` returned `Err`, no actor was produced
///   (this test);
/// - **stopped** — asserted where the stop flows already live
///   (`i12_alive_window`, `i19_send_and_weak_upgrade_after_termination`).
///
/// The #113 list also names **passivated**: passivation does not exist in
/// bombay-core (no lifecycle knob yet), so that leg is unassertable today —
/// recorded on card #118, to land with whichever card introduces passivation.
#[tokio::test]
async fn actor_not_alive_unifies_terminal() {
    // Leg 1: never run. Dropping the PreparedActor drops the only receiver.
    let prepared = PreparedActor::<Bank>::new(cap(4));
    let never_run = prepared.actor_ref().clone();
    drop(prepared);

    let err = never_run
        .tell(Poke)
        .try_send()
        .expect_err("a never-run actor cannot receive");
    assert!(err.is_terminal(), "never-run is terminal, never retryable");
    assert!(
        matches!(err, TellError::ActorNotAlive(Poke)),
        "never-run unifies to ActorNotAlive with the message back: {err:?}",
    );
    assert!(
        matches!(
            never_run.tell(Poke).await,
            Err(TellError::ActorNotAlive(Poke))
        ),
        "the blocking form agrees with try_send",
    );

    // Leg 2: startup failed. `on_start` errors, so no actor was ever produced.
    struct FailStart;
    #[derive(Debug)]
    struct Go;
    impl Msg for Go {}
    impl Mailboxed for FailStart {
        type Msg = Go;
    }
    impl Actor for FailStart {
        type Args = ();
        type Error = Boom;
        async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Err(Boom)
        }
        async fn handle(
            &mut self,
            _: Go,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let prepared = PreparedActor::<FailStart>::new(cap(4));
    let failed = prepared.actor_ref().clone();
    let outcome = timeout(TERMINATE, prepared.run(()))
        .await
        .expect("startup failure must terminate the run");
    assert!(
        matches!(outcome, RunResult::StartupFailed(_)),
        "precondition: on_start Err → StartupFailed, got {outcome:?}",
    );

    let err = failed
        .tell(Go)
        .try_send()
        .expect_err("a failed-start actor cannot receive");
    assert!(
        err.is_terminal(),
        "failed-start is terminal, never retryable"
    );
    assert!(
        matches!(err, TellError::ActorNotAlive(Go)),
        "startup-failure unifies to ActorNotAlive with the message back: {err:?}",
    );
}
