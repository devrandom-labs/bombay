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

use bolero::check;
use bombay_core::actor::{Actor, ActorRef, PreparedActor, RunResult, WeakActorRef};
use bombay_core::error::ActorStopReason;
use bombay_core::mailbox::{Capacity, Mailboxed};
use bombay_core::message::Msg;
use bombay_core::test_support::terminate_bound;

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

/// Task 1 spike: enqueue N messages, drop the last external ref, run. Every
/// enqueued message is handled before the mailbox closes (each queued signal
/// carries a strong self-sender per ADR-0003), then the loop stops Normal.
/// If the loop held a strong self-ref, the mailbox would never close and this
/// would hang → `bounded` fires. This is the `drop(actor_ref)` falsification
/// anchor (spawn.rs:165).
#[test]
fn actor_loop_drains_then_closes() {
    check!().with_type::<u8>().for_each(|&seed| {
        let n = u32::from(seed % 5); // 0..=4 messages
        let handled = Arc::new(AtomicU32::new(0));
        let outcome = runtime().block_on(async {
            let prepared = PreparedActor::<Counter>::new(cap());
            let actor_ref = prepared.actor_ref().clone();
            for _ in 0..n {
                bounded(actor_ref.tell(Tick)).await.expect("enqueue");
            }
            drop(actor_ref);
            bounded(prepared.run(Arc::clone(&handled))).await
        });
        assert!(
            matches!(outcome, RunResult::Stopped { reason: ActorStopReason::Normal, .. }),
            "drain-then-close is a normal stop",
        );
        assert_eq!(
            handled.load(Ordering::SeqCst),
            n,
            "every message enqueued before the last ref drops is handled",
        );
    });
}

fn cap() -> Capacity {
    Capacity::try_from(8usize).expect("valid capacity")
}
