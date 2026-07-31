//! Zero-box guard for the `Recipient<M>` tell path and the `From<M>`
//! conversion boundary (card #207).
//!
//! ONE test, in its OWN binary, on purpose — same rationale as
//! `alloc_exact.rs`/`alloc_request.rs`: a `#[global_allocator]` counts every
//! allocation in its process, and only a single-test binary is process-isolated
//! under BOTH harnesses (nextest per-process; plain `cargo test` shares a
//! process per binary and runs tests on parallel threads). Don't add a second
//! test here.
//!
//! `Recipient<M>` exists **for** zero-box message transit: the message travels
//! inline by value in the queue slot; only the handle is `Arc<dyn …>`
//! (ADR-0004, which quantifies the boxed alternative as a 256× queue-memory
//! swing). The inline `#[cfg(test)]` tests in `recipient.rs` prove erasure +
//! dispatch *route the right variant*; none proves the message was **not**
//! heap-boxed. This is that missing guard — the #145 design spec's
//! "counted by #151's counting allocator later" promise (#168 land-on-target).
//!
//! The message payload is non-ZST on purpose: `Box::new(zst)` does not
//! allocate, so a boxed ZST message would be invisible. A `u64` payload makes a
//! per-message box a real, countable heap allocation — red if the message is
//! boxed.

use std::{alloc::System, hint::black_box};

use bombay::{
    actor::{Actor, ActorRef, Flow, Recipient},
    error::Infallible,
    mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed},
    message::Msg,
    test_support::{CountingAlloc, terminate_bound, unstarted_actor},
};
use tokio::{runtime::Builder, time::timeout};

#[global_allocator]
static COUNTER: CountingAlloc = CountingAlloc::new(System);

/// The erased source message. Non-ZST (a boxed ZST is a no-op alloc, invisible
/// to the counter) and `Clone` because `Recipient<M>` requires it (the
/// typed-handback consequence, ADR-0004).
#[derive(Clone, Debug)]
struct Ping(u64);

/// The target's closed menu — a `u64` variant `From<Ping>`.
#[derive(Debug)]
enum ProbeMsg {
    #[expect(dead_code, reason = "payload exists for its size, not its value")]
    Beat(u64),
}
impl Msg for ProbeMsg {}
impl From<Ping> for ProbeMsg {
    fn from(ping: Ping) -> Self {
        Self::Beat(ping.0)
    }
}

struct Probe;
impl Mailboxed for Probe {
    type Msg = ProbeMsg;
}
impl Actor for Probe {
    type Args = ();
    type Error = Infallible;
    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
    async fn handle(&mut self, _: ProbeMsg, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

/// Pulls one queued signal off the receiver by hand (no run-loop). Bounded by
/// `terminate_bound` (card #148/#179): under a `Capacity::get -> 0` mutant the
/// queue is a rendezvous and this hand-driven receiver never overlaps the send,
/// so the drain must FAIL fast, not hang the binary. `block_on`/`timeout` here
/// is OUTSIDE every measured window.
fn drain(rt: &tokio::runtime::Runtime, rx: &mut MailboxReceiver<Probe>) {
    drop(
        rt.block_on(async { timeout(terminate_bound(), rx.recv()).await })
            .expect("the message must be received within the bound")
            .expect("the message is queued"),
    );
}

/// One round of all three measured operations — used as warm-up and as the
/// measured run. Returns (direct typed send, erased `try_tell`, `From`
/// boundary) gross allocation counts. The channel is shared across rounds: its
/// queue storage grows once, on the warm-up round, so the measured round sees
/// only the per-message cost.
fn round(
    rt: &tokio::runtime::Runtime,
    actor_ref: &ActorRef<Probe>,
    recipient: &Recipient<Ping>,
    rx: &mut MailboxReceiver<Probe>,
) -> (isize, isize, isize) {
    // The primitive the erased path mirrors: a direct typed send moves the
    // message by value into the queue slot — proven 0 by `alloc_request.rs`.
    let before = COUNTER.gross_allocs();
    actor_ref
        .tell(ProbeMsg::Beat(7))
        .try_send()
        .expect("open mailbox accepts the message");
    let direct = COUNTER.gross_allocs() - before;
    drain(rt, rx);

    // The erased send: clone `Ping` (Copy-ish u64, no heap), convert by value,
    // enqueue inline. The `Arc<dyn>` handle was built once at construction, not
    // here — so this must match `direct` exactly.
    let before = COUNTER.gross_allocs();
    recipient
        .try_tell(Ping(7))
        .expect("open mailbox accepts the message");
    let erased = COUNTER.gross_allocs() - before;
    drain(rt, rx);

    // The conversion boundary in isolation: `From<Ping>` for the representative
    // message must not itself allocate. `black_box` both ends so the compiler
    // cannot elide the conversion.
    let before = COUNTER.gross_allocs();
    let converted = black_box(ProbeMsg::from(black_box(Ping(7))));
    let boundary = COUNTER.gross_allocs() - before;
    drop(black_box(converted));

    (direct, erased, boundary)
}

#[test]
fn recipient_try_tell_is_zero_box_like_a_direct_send() {
    let rt = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    let cap = Capacity::try_from(4_usize).expect("valid capacity");
    let (actor_ref, mut rx) = unstarted_actor::<Probe>(Mailbox::<Probe>::bounded(
        cap,
        ActorId::from_raw_for_test(0),
    ));
    // The `Arc<dyn ErasedRecipient>` is built HERE, once — never per message.
    let recipient: Recipient<Ping> = actor_ref.recipient();

    // Warm-up: identical round BEFORE measuring, so one-time lazy init
    // (harness, flume queue growth, timer wheel) never pollutes the measurement.
    round(&rt, &actor_ref, &recipient, &mut rx);

    let live_baseline = COUNTER.snapshot();
    let (direct, erased, boundary) = round(&rt, &actor_ref, &recipient, &mut rx);

    assert_eq!(
        direct, 0,
        "a direct typed send moves the message by value — zero heap allocations",
    );
    assert_eq!(
        erased, direct,
        "erasure changes nothing about the message's allocation profile — \
         the erased try_tell performs the EXACT allocation count of a direct send",
    );
    assert_eq!(
        erased, 0,
        "the erased try_tell adds no per-message box: the message rides inline \
         in the queue slot, only the handle is Arc<dyn> (ADR-0004)",
    );
    assert_eq!(
        boundary, 0,
        "the From<Ping> conversion boundary does not itself allocate for the \
         representative message",
    );
    assert_eq!(
        COUNTER.snapshot(),
        live_baseline,
        "the whole round reclaims exactly — nothing leaks",
    );
}
