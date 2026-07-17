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

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bolero::{TypeGenerator, check};
use bombay_core::actor::{Actor, ActorRef, PreparedActor, RunResult, WeakActorRef};
use bombay_core::error::ActorStopReason;
use bombay_core::mailbox::{Capacity, Mailboxed, Signal};
use bombay_core::message::Msg;
use bombay_core::test_support::terminate_bound;

/// One driver step. `Send` enqueues a message; `StopInBand` enqueues a FIFO
/// `Signal::Stop`; `CancelStop` fires the out-of-band cancel token. The whole
/// script is applied before `run`, so execution is deterministic on the
/// single-threaded runtime — a sound fuzz oracle.
#[derive(Debug, TypeGenerator)]
enum Op {
    Send,
    StopInBand,
    CancelStop,
}

/// Deterministic oracle: how many messages the loop handles for an op script,
/// per the three stop modes (`kind.rs` "finish-current-then-stop, no drain").
/// This is a small stop-mode predicate, NOT a re-encoding of the loop.
fn expected_handled(ops: &[Op]) -> u32 {
    // Cancel is checked before every `recv`, and here it fires before `run`
    // even starts, so it abandons the entire backlog.
    if ops.iter().any(|op| matches!(op, Op::CancelStop)) {
        return 0;
    }
    // Otherwise messages are handled FIFO until the first in-band `Stop`; if
    // there is none, the backlog drains and the mailbox closes (refs dropped).
    let mut handled: u32 = 0;
    for op in ops {
        match op {
            Op::Send => handled = handled.saturating_add(1),
            Op::StopInBand => return handled,
            Op::CancelStop => {}
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

/// Bounds every lifecycle await so a regression that stalls the loop FAILS via
/// the MIRI-aware timeout instead of hanging (a hang reports as a cargo-mutants
/// TIMEOUT rather than a caught mutant — #148/#179).
async fn bounded<F: Future>(fut: F) -> F::Output {
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

/// Drives the real loop over a generated op script and asserts bombay's
/// invariants across all three stop modes:
///
/// * **drain-or-abandon per mode** — `handled` matches the oracle (FIFO before
///   an in-band `Stop`; whole backlog on cancel; drain-then-close otherwise);
/// * **enqueued-before-drop still handled** — the drain-then-close branch;
/// * **no strong self-ref held** — with no `Stop`/`CancelStop`, the loop only
///   ends because dropping the last external ref closes the mailbox; if it held
///   a strong self-ref it would hang here and `bounded` would fire. This is the
///   `drop(actor_ref)` (spawn.rs:165) falsification anchor.
///
/// Every outcome is a normal stop: `Counter` never errors/panics, so the crash
/// and startup-failure paths (covered separately) cannot arise here.
#[test]
fn actor_loop_state_machine() {
    check!().with_type::<Vec<Op>>().for_each(|ops| {
        // Size the mailbox to hold every enqueuing op so a pre-run `tell` never
        // blocks on backpressure (that surface is the mailbox target's job).
        let enqueued = ops.iter().filter(|op| !matches!(op, Op::CancelStop)).count();
        let cap = Capacity::try_from(enqueued.max(1)).expect("valid capacity");

        let handled = Arc::new(AtomicU32::new(0));
        let outcome = runtime().block_on(async {
            let prepared = PreparedActor::<Counter>::new(cap);
            let actor_ref = prepared.actor_ref().clone();
            for op in ops {
                match op {
                    Op::Send => bounded(actor_ref.tell(Tick)).await.expect("enqueue"),
                    Op::StopInBand => bounded(actor_ref.mailbox_sender().send(Signal::Stop))
                        .await
                        .expect("enqueue stop"),
                    Op::CancelStop => actor_ref.stop(),
                }
            }
            drop(actor_ref);
            bounded(prepared.run(Arc::clone(&handled))).await
        });

        assert!(
            matches!(outcome, RunResult::Stopped { reason: ActorStopReason::Normal, .. }),
            "every stop mode here is a normal stop, got {outcome:?}",
        );
        assert_eq!(
            handled.load(Ordering::SeqCst),
            expected_handled(ops),
            "handled count must match the drain-or-abandon oracle for {ops:?}",
        );
    });
}
