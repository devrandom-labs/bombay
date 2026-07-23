//! Model-based fuzz of the actor MESSAGE LOOP (`spawn.rs::run_lifecycle` /
//! `kind.rs::run_message_loop`) and its stop modes — the surface #149 left
//! uncovered (its target only reached `bombay_core::mailbox`, i.e. flume).
//!
//! Asserts bombay's OWN invariants, not flume's: drain-or-abandon per stop
//! mode, a message enqueued before the last ref drops is still handled, and the
//! loop holds no strong self-ref (dropping the last external `ActorRef` stops
//! the actor). The oracle is a three-branch stop-mode predicate, deliberately
//! not a re-encoding of the loop.
//!
//! bolero's corpus is filesystem-backed, so this target is fuzz-lane only; the
//! same loop is MIRI-covered by `bombay-core`'s `#[tokio::test]` cases (the
//! miri lane drives `PreparedActor::run` green — ADR-0005).

use std::future::IntoFuture;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bolero::{TypeGenerator, check};
use bombay_core::actor::{Actor, ActorRef, PreparedActor, RunResult, WeakActorRef};
use bombay_core::error::ActorStopReason;
use bombay_core::mailbox::{ActorId, Capacity, Mailboxed, Signal};
use bombay_core::message::Msg;
use bombay_core::test_support::{terminate_bound, watch_signal};

/// One driver step. `Send` enqueues a message; `StopInBand` enqueues a FIFO
/// `Signal::Stop`; `CancelStop` fires the out-of-band cancel token; `Kill`
/// aborts (hard kill); `Watch` enqueues a FIFO `Signal::Watch` registration and
/// `Unwatch` enqueues a FIFO `Signal::Unwatch`, both control signals (card
/// #195). The whole script is applied before `run`, so execution is
/// deterministic on the single-threaded runtime — a sound fuzz oracle.
///
/// `Signal::Watch` carries a boxed `WatchReg`, which lives in bombay-core's
/// private `watch` module and is not part of the public API. The fuzz crate
/// mints one through the `test-support`-gated `test_support::watch_signal` seam,
/// which builds the watcher's unbounded link channel internally and returns the
/// enqueue-able signal plus its `LinkReceiver` — so this target drives the
/// death-watch REGISTRATION arm of the loop (`watchers.apply`) under random
/// op interleavings, the #100-class race surface. `Unwatch` takes a plain
/// public `ActorId` and drives the deregistration arm.
#[derive(Debug, Clone, TypeGenerator)]
enum Op {
    Send,
    StopInBand,
    CancelStop,
    Kill,
    Watch { linked: bool },
    Unwatch,
}

/// Deterministic oracle: how many messages the loop handles for an op script,
/// per the three stop modes (`kind.rs` "finish-current-then-stop, no drain").
/// This is a small stop-mode predicate, NOT a re-encoding of the loop.
fn expected_handled(ops: &[Op]) -> u32 {
    // A pre-run cancel (checked before every `recv`) or a pre-run abort both
    // end the run before any message is drained, so no message is handled.
    if ops.iter().any(|op| matches!(op, Op::CancelStop | Op::Kill)) {
        return 0;
    }
    // Otherwise messages are handled FIFO until the first in-band `Stop`; if
    // there is none, the backlog drains and the mailbox closes (refs dropped).
    let mut handled: u32 = 0;
    for op in ops {
        match op {
            Op::Send => handled = handled.saturating_add(1),
            Op::StopInBand => return handled,
            // A queued `Watch`/`Unwatch` is a control signal the loop processes
            // without handling a message or stopping — it never changes the count.
            Op::CancelStop | Op::Kill | Op::Watch { .. } | Op::Unwatch => {}
        }
    }
    handled
}

/// Fuzz-local actor: counts handled messages into a shared atomic the target
/// inspects. The SUT is the real loop, never a reimplementation.
struct Counter {
    handled: Arc<AtomicU32>,
}

#[derive(Debug)]
struct Tick;
impl Msg for Tick {}
impl Mailboxed for Counter {
    type Msg = Tick;
}

impl Actor for Counter {
    type Args = Arc<AtomicU32>;
    type Error = core::convert::Infallible;

    async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { handled })
    }

    async fn handle(
        &mut self,
        _: Tick,
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
        Ok(())
    }
}

/// `on_start` panics — exercises the startup-failure path. Same `Tick`/`Args`
/// shape as `Counter` so the generic `drive` harness covers it unchanged.
struct StartPanics {
    handled: Arc<AtomicU32>,
}
impl Mailboxed for StartPanics {
    type Msg = Tick;
}
impl Actor for StartPanics {
    type Args = Arc<AtomicU32>;
    type Error = core::convert::Infallible;

    async fn on_start(_: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        panic!("on_start deliberately fails");
    }

    async fn handle(
        &mut self,
        _: Tick,
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
        Ok(())
    }
}

/// `on_stop` panics — the loop must catch it, log, and PRESERVE the stop reason
/// (`spawn.rs::log_on_stop_outcome`), still returning `Stopped`, not corrupting
/// the outcome. Messages are handled exactly as for `Counter`.
struct StopPanics {
    handled: Arc<AtomicU32>,
}
impl Mailboxed for StopPanics {
    type Msg = Tick;
}
impl Actor for StopPanics {
    type Args = Arc<AtomicU32>;
    type Error = core::convert::Infallible;

    async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { handled })
    }

    async fn handle(
        &mut self,
        _: Tick,
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
        panic!("on_stop deliberately fails");
    }
}

/// Bounds every lifecycle await so a regression that stalls the loop FAILS via
/// the MIRI-aware timeout instead of hanging (a hang reports as a cargo-mutants
/// TIMEOUT rather than a caught mutant — #148/#179).
async fn bounded<F: IntoFuture>(fut: F) -> F::Output {
    tokio::time::timeout(terminate_bound(), fut)
        .await
        .expect("actor lifecycle op must terminate, not hang")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current_thread runtime")
}

/// Drives the real `PreparedActor::run` over an op script and returns the
/// outcome. The caller owns the `handled` counter (passed as args) and reads it
/// after. Generic over the actor so the startup-failure and failing-`on_stop`
/// variants reuse the exact same driving logic — the SUT is always the real
/// loop, never a reimplementation. Mailbox is sized to hold every enqueuing op
/// so a pre-run `tell` never blocks on backpressure (the mailbox target's job).
fn drive<A>(ops: &[Op], args: A::Args) -> RunResult<A>
where
    A: Actor + Mailboxed<Msg = Tick>,
{
    let enqueued = ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                Op::Send | Op::StopInBand | Op::Watch { .. } | Op::Unwatch
            )
        })
        .count();
    let cap = Capacity::try_from(enqueued.max(1)).expect("valid capacity");
    runtime().block_on(async {
        let prepared = PreparedActor::<A>::new(cap);
        let actor_ref = prepared.actor_ref().clone();
        // Keep each `Watch` registration's `LinkReceiver` alive for the whole
        // run: dropping it would close the link channel, making the guard's
        // death notify a stale-edge no-op instead of a real delivery.
        let mut link_rxs = Vec::new();
        for op in ops {
            match op {
                Op::Send => bounded(actor_ref.tell(Tick)).await.expect("enqueue"),
                Op::StopInBand => bounded(actor_ref.mailbox_sender().send(Signal::Stop))
                    .await
                    .expect("enqueue stop"),
                Op::CancelStop => actor_ref.stop(),
                Op::Kill => actor_ref.kill(),
                Op::Watch { linked } => {
                    let (signal, link_rx) = watch_signal::<A>(ActorId::new(1), *linked);
                    bounded(actor_ref.mailbox_sender().send(signal))
                        .await
                        .expect("enqueue watch");
                    link_rxs.push(link_rx);
                }
                Op::Unwatch => bounded(
                    actor_ref
                        .mailbox_sender()
                        .send(Signal::Unwatch(ActorId::new(0))),
                )
                .await
                .expect("enqueue unwatch"),
            }
        }
        drop(actor_ref);
        let outcome = bounded(prepared.run(args)).await;
        // The `LinkReceiver`s are held here (not asserted on) deliberately: they
        // keep each watch edge's link channel OPEN across the run so the
        // registration path is realistic (a dropped receiver would make the
        // guard's notify a stale-edge no-op). Exact death-DELIVERY is not
        // asserted because it is not deterministic across this generic harness:
        // a `Kill` aborts (guard fires `Killed`, and a registration queued after
        // the abort point may never be applied), an `on_start` panic
        // short-circuits before any registration is drained, and a `Watch`
        // queued behind an in-band `Stop` is never reached — pinning exact
        // per-watcher delivery would re-encode the loop's FIFO ordering, which
        // this oracle refuses to do. The invariant here is the registration arm
        // itself: enqueuing `Signal::Watch` under random interleavings must not
        // panic and the loop must terminate (bounded above). Deterministic
        // death-delivery stays covered by bombay-core's `#[tokio::test]` cases.
        drop(link_rxs);
        outcome
    })
}

/// Core invariants and RunResult-matches-path across all stop modes:
/// * **drain-or-abandon per mode** — `handled` matches the oracle;
/// * **enqueued-before-drop still handled** — the drain-then-close branch;
/// * **no strong self-ref held** — the no-`Stop`/no-`Cancel`/no-`Kill` case ends
///   ONLY because dropping the last external ref closes the mailbox; a leaked
///   strong self-ref would hang here (`bounded` fires). Falsification anchor:
///   `drop(actor_ref)` at spawn.rs:165.
/// * **RunResult matches the path** — a `Kill` aborts to `Killed`; every other
///   mode is a normal `Stopped`.
#[test]
fn actor_loop_state_machine() {
    check!().with_type::<Vec<Op>>().for_each(|ops| {
        let handled = Arc::new(AtomicU32::new(0));
        let outcome = drive::<Counter>(ops, Arc::clone(&handled));
        if ops.iter().any(|op| matches!(op, Op::Kill)) {
            assert!(
                matches!(outcome, RunResult::Killed),
                "a hard kill must abort to Killed, got {outcome:?}",
            );
        } else {
            assert!(
                matches!(
                    outcome,
                    RunResult::Stopped {
                        reason: ActorStopReason::Normal,
                        ..
                    }
                ),
                "every non-kill stop mode is a normal stop, got {outcome:?}",
            );
        }
        assert_eq!(
            handled.load(Ordering::SeqCst),
            expected_handled(ops),
            "handled count must match the drain-or-abandon oracle for {ops:?}",
        );
    });
}

/// RunResult matches the path: a panicking `on_start` short-circuits to
/// `StartupFailed` before the loop runs, whatever is queued — no message handled.
#[test]
fn on_start_panic_yields_startup_failed() {
    check!().with_type::<Vec<Op>>().for_each(|ops| {
        // Exclude Kill: a pre-run abort would short-circuit to Killed before
        // on_start ever runs; this test isolates the startup-failure path.
        let ops: Vec<Op> = ops
            .iter()
            .filter(|op| !matches!(op, Op::Kill))
            .cloned()
            .collect();
        let handled = Arc::new(AtomicU32::new(0));
        let outcome = drive::<StartPanics>(&ops, Arc::clone(&handled));
        assert!(
            matches!(outcome, RunResult::StartupFailed(_)),
            "on_start panic must yield StartupFailed, got {outcome:?}",
        );
        assert_eq!(handled.load(Ordering::SeqCst), 0, "the loop never ran");
    });
}

/// The stop reason survives a failing `on_stop`: the panic is caught and logged
/// (`log_on_stop_outcome`), the reason preserved, the outcome still `Stopped`,
/// and the handled count unchanged.
#[test]
fn stop_reason_preserved_through_panicking_on_stop() {
    check!().with_type::<Vec<Op>>().for_each(|ops| {
        // Exclude Kill: an abort skips on_stop entirely (RunResult::Killed);
        // this test isolates the failing-on_stop-preserves-reason path.
        let ops: Vec<Op> = ops
            .iter()
            .filter(|op| !matches!(op, Op::Kill))
            .cloned()
            .collect();
        let handled = Arc::new(AtomicU32::new(0));
        let outcome = drive::<StopPanics>(&ops, Arc::clone(&handled));
        assert!(
            matches!(
                outcome,
                RunResult::Stopped {
                    reason: ActorStopReason::Normal,
                    ..
                }
            ),
            "a failing on_stop must not corrupt the outcome, got {outcome:?}",
        );
        assert_eq!(
            handled.load(Ordering::SeqCst),
            expected_handled(&ops),
            "handled count unchanged by a failing on_stop for {ops:?}",
        );
    });
}
