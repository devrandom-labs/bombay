//! Zero-box guard for the `ReplyRecipient<M, R, E>` ask path (card #207).
//!
//! ONE test, in its OWN binary, on purpose — same rationale as
//! `alloc_exact.rs`/`alloc_request.rs`: a `#[global_allocator]` counts every
//! allocation in its process, and only a single-test binary is process-isolated
//! under BOTH harnesses. Don't add a second test here.
//!
//! The ask-side sibling of `alloc_recipient.rs`. `ReplyRecipient` erases the
//! actor the same way `Recipient` does (ADR-0004): the request message rides
//! **inline** inside the `Ask<M, R, E>` carrier, converted `-> A::Msg` by value
//! and enqueued; only the handle is `Arc<dyn …>`. The erased path *does* box its
//! futures — the unavoidable cost of `dyn` async dispatch — but **never the
//! message** (ADR-0004, design-spec `2026-07-13-145-recipient-design.md`).
//!
//! The guard: an erased ask allocates exactly the reply port a **direct** ask
//! allocates (`alloc_request.rs` pins that at 1) plus the two `dyn`-dispatch
//! future boxes — and **nothing more**. A refactor that boxes the message adds
//! a third heap allocation over the direct ask and turns this red. The `Query`
//! payload is non-ZST for the same reason as `alloc_recipient.rs`: a boxed ZST
//! is a no-op alloc and would be invisible.

use std::{alloc::System, future::IntoFuture, pin::pin};

use bombay::{
    actor::{Actor, ActorRef, Flow, ReplyRecipient},
    error::Infallible,
    mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed, Recv, Signal},
    message::Msg,
    reply::ReplySender,
    request::Ask,
    test_support::{CountingAlloc, terminate_bound, unstarted_actor},
};
use tokio::{runtime::Builder, time::timeout};

#[global_allocator]
static COUNTER: CountingAlloc = CountingAlloc::new(System);

/// The erased request payload. Non-ZST (a boxed ZST is a no-op alloc, invisible
/// to the counter) and `Clone` because `ReplyRecipient<M, …>` requires it (the
/// typed-handback consequence, ADR-0004).
#[derive(Clone, Debug)]
struct Query(#[expect(dead_code, reason = "payload exists for its size, not its value")] u64);

/// The target's closed menu: a hand-built ask variant for the direct baseline,
/// and the `Ask` carrier variant the erased path converts into.
#[derive(Debug)]
enum ProbeMsg {
    Get { reply: ReplySender<u64> },
    Erased(Ask<Query, u64, Infallible>),
}
impl Msg for ProbeMsg {}
impl From<Ask<Query, u64, Infallible>> for ProbeMsg {
    fn from(ask: Ask<Query, u64, Infallible>) -> Self {
        Self::Erased(ask)
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

/// The two `dyn`-dispatch future boxes an erased ask adds over a direct ask:
/// `RecipientAskRequest::into_future`'s outer `Box::pin`, and the
/// `ErasedReplyRecipient::deliver` `Box::pin`. This is the whole erasure tax —
/// the message is NOT among these boxes. Bump this only alongside a deliberate
/// change to the erased future structure; a surprise +1 is a boxed message.
const DYN_ASK_FUTURE_BOXES: isize = 2;

/// One full round of both ask paths, driving the handler side by hand off the
/// receiver — used as warm-up and as the measured run. Returns (direct ask,
/// erased ask) gross allocation counts.
fn round(
    rt: &tokio::runtime::Runtime,
    actor_ref: &ActorRef<Probe>,
    reply_recipient: &ReplyRecipient<Query, u64, Infallible>,
    rx: &mut MailboxReceiver<Probe>,
) -> (isize, isize) {
    // Direct ask: builds the reply port, delivers the hand-written `Get`
    // variant, awaits the reply. `alloc_request.rs` pins this at exactly 1.
    let before = COUNTER.gross_allocs();
    let direct_answer = rt.block_on(async {
        let ask = pin!(actor_ref.ask(|reply| ProbeMsg::Get { reply }).into_future());
        let serve = async {
            let signal = timeout(terminate_bound(), rx.recv())
                .await
                .expect("the direct ask must be received within the bound")
                .expect("the direct ask is queued");
            let Recv::Signal(Signal::Message {
                msg: ProbeMsg::Get { reply },
                ..
            }) = signal
            else {
                unreachable!("only the direct ask is queued")
            };
            reply.send(42).expect("asker is waiting");
        };
        let (outcome, ()) = futures::join!(ask, serve);
        outcome
    });
    let direct = COUNTER.gross_allocs() - before;
    assert_eq!(
        direct_answer.ok(),
        Some(42),
        "the direct round trip completed"
    );

    // Erased ask: the message rides inline in the `Ask` carrier, converted by
    // value; the path boxes its two futures (the `dyn` cost) but not the
    // message. The `Arc<dyn>` handle was built once, not here.
    let before = COUNTER.gross_allocs();
    let erased_answer = rt.block_on(async {
        let ask = pin!(reply_recipient.ask(Query(7)).into_future());
        let serve = async {
            let signal = timeout(terminate_bound(), rx.recv())
                .await
                .expect("the erased ask must be received within the bound")
                .expect("the erased ask is queued");
            let Recv::Signal(Signal::Message {
                msg: ProbeMsg::Erased(Ask { reply, .. }),
                ..
            }) = signal
            else {
                unreachable!("only the erased ask is queued")
            };
            reply.send(42).expect("asker is waiting");
        };
        let (outcome, ()) = futures::join!(ask, serve);
        outcome
    });
    let erased = COUNTER.gross_allocs() - before;
    assert_eq!(
        erased_answer.ok(),
        Some(42),
        "the erased round trip completed"
    );

    (direct, erased)
}

#[test]
fn reply_recipient_ask_boxes_only_futures_never_the_message() {
    let rt = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    let cap = Capacity::try_from(4_usize).expect("valid capacity");
    let (actor_ref, mut rx) = unstarted_actor::<Probe>(Mailbox::<Probe>::bounded(
        cap,
        ActorId::from_raw_for_test(0),
    ));
    // The `Arc<dyn ErasedReplyRecipient>` is built HERE, once — never per ask.
    let reply_recipient: ReplyRecipient<Query, u64, Infallible> = actor_ref.reply_recipient();

    // Warm-up: identical round BEFORE measuring, so one-time lazy init
    // (harness, flume queue growth, tokio timer wheel) never pollutes the
    // measurement.
    round(&rt, &actor_ref, &reply_recipient, &mut rx);

    let live_baseline = COUNTER.snapshot();
    let (direct, erased) = round(&rt, &actor_ref, &reply_recipient, &mut rx);

    assert_eq!(
        direct, 1,
        "a direct ask allocates exactly the oneshot reply port",
    );
    assert_eq!(
        erased,
        direct + DYN_ASK_FUTURE_BOXES,
        "the erased ask adds ONLY the two dyn-dispatch future boxes over a \
         direct ask — the message rides inline in the Ask carrier, never boxed \
         (ADR-0004). A surprise extra allocation is a boxed message.",
    );
    assert_eq!(
        COUNTER.snapshot(),
        live_baseline,
        "both round trips reclaim exactly — nothing leaks",
    );
}
