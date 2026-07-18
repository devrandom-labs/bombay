//! Exact allocation counts of the #118 request hot paths.
//!
//! ONE test, in its OWN binary, on purpose — same rationale as
//! `alloc_exact.rs`: a `#[global_allocator]` counts every allocation in its
//! process, and only a single-test binary is process-isolated under BOTH
//! harnesses. Don't add a second test here.
//!
//! The card's un-templated claims, measured (card #118 / #122-#11):
//! - `tell` moves the message by value into the queue — **0 allocations**;
//! - `ask` adds exactly the reply channel — **1 allocation** (the oneshot),
//!   across the whole round trip (deliver → handler reply → caller resolve).

use std::{alloc::System, future::IntoFuture};

use bombay_core::{
    actor::{Actor, ActorRef},
    error::Infallible,
    mailbox::{Capacity, Mailbox, Mailboxed, Signal},
    message::Msg,
    reply::ReplySender,
    test_support::{CountingAlloc, unstarted_actor},
};
use tokio::runtime::Builder;

#[global_allocator]
static COUNTER: CountingAlloc = CountingAlloc::new(System);

struct Probe;

#[derive(Debug)]
enum ProbeMsg {
    Note(u64),
    Get { reply: ReplySender<u64> },
}
impl Msg for ProbeMsg {}
impl Mailboxed for Probe {
    type Msg = ProbeMsg;
}
impl Actor for Probe {
    type Args = ();
    type Error = Infallible;
    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
    async fn handle(
        &mut self,
        _: ProbeMsg,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// One full round of both hot paths — used as warm-up and as the measured run.
/// Returns (gross allocations during tell, gross during the ask round trip).
/// The channel is shared across rounds: its queue storage grows once, on the
/// warm-up round, so the measured round sees only the per-message cost.
fn round(
    rt: &tokio::runtime::Runtime,
    actor_ref: &ActorRef<Probe>,
    rx: &mut bombay_core::mailbox::MailboxReceiver<Probe>,
) -> (isize, isize) {
    // Tell: enqueue by value, then drain (the drained signal drops here).
    let tell_before = COUNTER.gross_allocs();
    rt.block_on(actor_ref.tell(ProbeMsg::Note(7)).into_future())
        .expect("open mailbox accepts the message");
    let tell_gross = COUNTER.gross_allocs() - tell_before;
    drop(rt.block_on(rx.recv()).expect("the note is queued"));

    // Ask: full round trip, driving the handler side by hand off the receiver.
    let ask_before = COUNTER.gross_allocs();
    let answer = rt.block_on(async {
        let pending = actor_ref.ask(|reply| ProbeMsg::Get { reply });
        let ask = std::pin::pin!(pending.into_future());
        let serve = async {
            let signal = rx.recv().await.expect("the ask is queued");
            let Signal::Message {
                msg: ProbeMsg::Get { reply },
                ..
            } = signal
            else {
                unreachable!("only the ask is queued")
            };
            reply.send(42).expect("asker is waiting");
        };
        let (outcome, ()) = futures::join!(ask, serve);
        outcome
    });
    let ask_gross = COUNTER.gross_allocs() - ask_before;
    assert_eq!(answer.ok(), Some(42), "the round trip really completed");

    (tell_gross, ask_gross)
}

#[test]
fn tell_is_zero_alloc_and_ask_is_one_alloc() {
    let rt = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    let cap = Capacity::try_from(4_usize).expect("valid capacity");
    let (actor_ref, mut rx) = unstarted_actor::<Probe>(Mailbox::<Probe>::bounded(cap));

    // Warm-up: identical round BEFORE measuring, so one-time lazy init
    // (harness, flume queue growth, timer wheel) never pollutes the
    // measurement.
    round(&rt, &actor_ref, &mut rx);

    let live_baseline = COUNTER.snapshot();
    let (tell_gross, ask_gross) = round(&rt, &actor_ref, &mut rx);

    assert_eq!(
        tell_gross, 0,
        "tell moves the message by value — zero heap allocations",
    );
    assert_eq!(
        ask_gross, 1,
        "ask allocates exactly the oneshot reply channel",
    );
    assert_eq!(
        COUNTER.snapshot(),
        live_baseline,
        "both hot paths reclaim exactly — nothing leaks",
    );
}
