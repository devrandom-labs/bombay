//! Shared `Mailbox` World + step definitions for the core `mailbox` scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_mailbox_bdd.rs`        — the example feature (mailbox.feature)
//!   * `core_mailbox_props_bdd.rs`  — the property/model laws
//!     (mailbox.properties.feature)
//!
//! The SUT is `src/mailbox.rs`: the bounded/unbounded `Mailbox` mpsc signal
//! channel created by `mailbox::bounded` / `mailbox::unbounded`, its
//! `MailboxSender` / `MailboxReceiver` / `WeakMailboxSender` handles, the
//! `Signal<A>` enum, and the `front: VecDeque<Signal<A>>` restart push-back
//! buffer. These scenarios pin the *channel mechanics directly* (not the actor
//! runtime above them), so the World holds raw mailbox halves built over a
//! minimal `Probe` actor type — the channel is generic over `A: Actor` and never
//! spawns a task.
//!
//! Distinguishable signals: a production `Signal::Message` boxes a real message
//! plus an `ActorRef` and reply channel and is unconstructable out-of-crate, so
//! the tests use the `mailbox::testing::tagged_signal(tag)` shim — a real
//! channel-shaped `Signal::LinkDied` whose tag rides in its `ActorId` and is read
//! back with `mailbox::testing::signal_tag`. Ordering/identity assertions compare
//! these tags.
//!
//! ## Gotchas honoured
//!
//! * **No blocking await into a permanently-full channel.** Full-channel
//!   @boundary scenarios fill via the non-blocking `try_send` to capacity, assert
//!   the `Full`/`MailboxFull` boundary, then drain via the receiver half — they
//!   never `send().await` into a channel that can never free a slot on the test
//!   thread. The one scenario that DOES park a blocking send (`blocking_send
//!   parks until capacity frees`) runs the send on a `spawn_blocking` thread and
//!   frees the slot from the test thread, then joins.
//! * **`blocking_*` → `tokio::task::spawn_blocking`** (they panic on the async
//!   worker otherwise).
//! * **@timing → `tokio::time` pause/advance** inside a dedicated paused
//!   current-thread runtime on a blocking thread (the `send_timeout` boundary),
//!   never a real sleep.
//! * **Bounded `settle()` polling** for the linearizability drains; never an
//!   unbounded await on an observable that may never arrive.

use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use bombay::{
    error::{Infallible, SendError},
    mailbox::{
        self, MailboxReceiver, MailboxSender, Signal, SignalMailbox, WeakMailboxSender,
        testing::{push_front, signal_tag, tagged_signal},
    },
    prelude::*,
};
use cucumber::{World, given, then, when};
use proptest::prelude::*;
use tokio::{
    sync::{Barrier, mpsc, oneshot},
    task::JoinHandle,
};

// ===========================================================================
// Probe actor — the channel's `A: Actor` parameter only. Never spawned.
// ===========================================================================

#[derive(Clone)]
struct Probe;

impl Actor for Probe {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

type Sig = Signal<Probe>;

/// A no-op `Waker` so `poll_recv` can be driven from a synchronous step without
/// a runtime registering a real waker. The scenarios only assert the immediate
/// `Ready`/`Pending` of an already-buffered front, so a no-op wake is sufficient.
struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn noop_waker() -> Waker {
    Waker::from(Arc::new(NoopWake))
}

// ===========================================================================
// World
// ===========================================================================

#[derive(Default, World)]
#[world(init = Self::new)]
pub struct MailboxWorld {
    sender: Option<MailboxSender<Probe>>,
    /// A second strong sender (clone) used by the strong-count scenarios.
    sender2: Option<MailboxSender<Probe>>,
    /// A sender from a *different* channel, for `same_channel` falsity.
    other_sender: Option<MailboxSender<Probe>>,
    /// An unbounded sender, for the cross-kind `same_channel` scenario.
    unbounded_sender: Option<MailboxSender<Probe>>,
    receiver: Option<MailboxReceiver<Probe>>,
    weak: Option<WeakMailboxSender<Probe>>,
    /// Tags of signals the receiver has yielded, in order.
    received: Vec<u64>,
    /// Outcome of the most recent `try_send`.
    try_send_result: Option<Result<(), tokio::sync::mpsc::error::TrySendError<Sig>>>,
    /// Outcome of the most recent blocking `send`/`send_timeout` family call.
    send_result: Option<Result<(), String>>,
    /// Outcome of the most recent `signal_*` SignalMailbox call.
    signal_result: Option<Result<(), SendError>>,
    /// Most recent `recv_many` / `poll_recv_many` count.
    last_count: Option<usize>,
    /// Captured panic flag for the capacity-0 constructor scenario.
    panicked: Option<bool>,
    /// Captured booleans for `same_channel` / `is_*` assertions.
    bool_a: Option<bool>,
    /// Join handle for a parked `blocking_send` on a std thread.
    blocking_handle: Option<thread::JoinHandle<Result<(), mpsc::error::SendError<Sig>>>>,
    /// Join handle for a parked async `send`.
    send_task: Option<JoinHandle<Result<(), mpsc::error::SendError<Sig>>>>,
    /// Join handles for the concurrent-sender linearizability scenarios.
    send_handles: Vec<JoinHandle<()>>,
    /// Expected total signal count for the exactly-once linearizability scenario.
    expected_total: Option<usize>,
}

// `Signal<A>` (carried inside several result/handle fields) is not `Debug`, so a
// derive is impossible; cucumber's `World` only needs *a* `Debug`. This summary
// impl reports the scalar observables the scenarios actually assert on.
impl fmt::Debug for MailboxWorld {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MailboxWorld")
            .field("received", &self.received)
            .field("last_count", &self.last_count)
            .field("panicked", &self.panicked)
            .field("bool_a", &self.bool_a)
            .field("expected_total", &self.expected_total)
            .field("signal_result", &self.signal_result)
            .field("send_result", &self.send_result)
            .finish_non_exhaustive()
    }
}

impl MailboxWorld {
    fn new() -> Self {
        MailboxWorld::default()
    }

    fn sender(&self) -> &MailboxSender<Probe> {
        self.sender.as_ref().expect("sender set")
    }

    fn receiver(&mut self) -> &mut MailboxReceiver<Probe> {
        self.receiver.as_mut().expect("receiver set")
    }
}

// Unique tag source so distinct named signals (S1.., C1.., F1..) never collide
// within a scenario; reset is unnecessary since each tag is compared by the same
// allocation within one scenario.
static TAG_SEQ: AtomicU64 = AtomicU64::new(1);

fn fresh_tag() -> u64 {
    TAG_SEQ.fetch_add(1, Ordering::Relaxed)
}

// ===========================================================================
// Construction Givens
// ===========================================================================

#[given(regex = r"^a bounded mailbox with capacity (\d+)$")]
async fn given_bounded(world: &mut MailboxWorld, cap: usize) {
    let (tx, rx) = mailbox::bounded::<Probe>(cap);
    world.sender = Some(tx);
    world.receiver = Some(rx);
}

#[given(regex = r"^a bounded mailbox with capacity (\d+) whose channel is empty$")]
async fn given_bounded_empty(world: &mut MailboxWorld, cap: usize) {
    given_bounded(world, cap).await;
}

#[given(regex = r"^a bounded mailbox with capacity (\d+) that is currently full$")]
async fn given_bounded_full(world: &mut MailboxWorld, cap: usize) {
    given_bounded(world, cap).await;
    for _ in 0..cap {
        world
            .sender()
            .try_send(tagged_signal(fresh_tag()))
            .expect("fill to capacity must succeed");
    }
}

#[given(regex = r"^an unbounded mailbox$")]
async fn given_unbounded(world: &mut MailboxWorld) {
    let (tx, rx) = mailbox::unbounded::<Probe>();
    world.sender = Some(tx);
    world.receiver = Some(rx);
}

// ===========================================================================
// @sequence — FIFO + push_front protocol
// ===========================================================================

#[when(regex = r"^signals S1, S2, S3 are sent in that order$")]
async fn when_send_s1_s2_s3(world: &mut MailboxWorld) {
    for tag in [101u64, 102, 103] {
        world
            .sender()
            .try_send(tagged_signal(tag))
            .expect("send within capacity");
    }
}

#[when(regex = r"^the receiver calls recv three times$")]
async fn when_recv_three(world: &mut MailboxWorld) {
    for _ in 0..3 {
        let sig = world.receiver().recv().await.expect("a signal");
        world.received.push(signal_tag(&sig).expect("tagged"));
    }
}

#[then(regex = r"^the receiver yields S1, then S2, then S3 in that exact order$")]
async fn then_yields_s1_s2_s3(world: &mut MailboxWorld) {
    assert_eq!(world.received, vec![101, 102, 103]);
}

#[given(regex = r"^signals C1, C2 are already queued in the channel$")]
async fn given_c1_c2_queued(world: &mut MailboxWorld) {
    for tag in [201u64, 202] {
        world
            .sender()
            .try_send(tagged_signal(tag))
            .expect("queue within capacity");
    }
}

#[when(regex = r"^the receiver push_fronts the ordered signals F1, F2$")]
async fn when_push_front_f1_f2(world: &mut MailboxWorld) {
    let batch: VecDeque<Sig> = [301u64, 302].into_iter().map(tagged_signal).collect();
    push_front(world.receiver(), batch);
}

#[when(regex = r"^the receiver calls recv four times$")]
async fn when_recv_four(world: &mut MailboxWorld) {
    for _ in 0..4 {
        let sig = world.receiver().recv().await.expect("a signal");
        world.received.push(signal_tag(&sig).expect("tagged"));
    }
}

#[then(regex = r"^the receiver yields F1, F2, C1, C2 in that exact order$")]
async fn then_yields_f1_f2_c1_c2(world: &mut MailboxWorld) {
    assert_eq!(world.received, vec![301, 302, 201, 202]);
}

#[when(regex = r"^the receiver calls blocking_recv four times on a blocking thread$")]
async fn when_blocking_recv_four(world: &mut MailboxWorld) {
    let mut rx = world.receiver.take().expect("receiver set");
    let (rx, got) = tokio::task::spawn_blocking(move || {
        let mut got = Vec::new();
        for _ in 0..4 {
            let sig = rx.blocking_recv().expect("a signal");
            got.push(signal_tag(&sig).expect("tagged"));
        }
        (rx, got)
    })
    .await
    .expect("blocking recv thread");
    world.receiver = Some(rx);
    world.received = got;
}

#[when(regex = r"^the receiver is polled via poll_recv four times$")]
async fn when_poll_recv_four(world: &mut MailboxWorld) {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut got = Vec::new();
    let rx = world.receiver();
    for _ in 0..4 {
        match rx.poll_recv(&mut cx) {
            Poll::Ready(Some(sig)) => got.push(signal_tag(&sig).expect("tagged")),
            _ => panic!("expected Ready(Some)"),
        }
    }
    world.received = got;
}

#[then(
    regex = r"^poll_recv yields Ready\(F1\), Ready\(F2\), Ready\(C1\), Ready\(C2\) in that exact order$"
)]
async fn then_poll_yields_f1_f2_c1_c2(world: &mut MailboxWorld) {
    assert_eq!(world.received, vec![301, 302, 201, 202]);
}

#[when(regex = r"^the receiver push_fronts F1, F2 and calls blocking_recv_many with limit (\d+)$")]
async fn when_push_front_then_blocking_recv_many(world: &mut MailboxWorld, limit: usize) {
    let batch: VecDeque<Sig> = [301u64, 302].into_iter().map(tagged_signal).collect();
    push_front(world.receiver(), batch);
    let mut rx = world.receiver.take().expect("receiver set");
    let (rx, count, tags) = tokio::task::spawn_blocking(move || {
        let mut buf: Vec<Sig> = Vec::new();
        let count = rx.blocking_recv_many(&mut buf, limit);
        let tags: Vec<u64> = buf.iter().map(|s| signal_tag(s).expect("tagged")).collect();
        (rx, count, tags)
    })
    .await
    .expect("blocking recv_many thread");
    world.receiver = Some(rx);
    world.last_count = Some(count);
    world.received = tags;
}

#[then(
    regex = r"^the call returns exactly F1, F2 \(count 2\) and leaves C1, C2 for the next call$"
)]
async fn then_recv_many_front_only(world: &mut MailboxWorld) {
    assert_eq!(world.last_count, Some(2));
    assert_eq!(world.received, vec![301, 302]);
    // C1, C2 remain in the channel for the next call.
    assert_eq!(world.receiver().len(), 2);
}

#[then(regex = r"^poll_recv_many behaves identically when the front buffer is non-empty$")]
async fn then_poll_recv_many_front_only(_world: &mut MailboxWorld) {
    // Fresh channel exercising the poll_recv_many front short-circuit directly.
    let (tx, mut rx) = mailbox::bounded::<Probe>(8);
    tx.try_send(tagged_signal(201)).unwrap();
    tx.try_send(tagged_signal(202)).unwrap();
    let batch: VecDeque<Sig> = [301u64, 302].into_iter().map(tagged_signal).collect();
    push_front(&mut rx, batch);

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut buf: Vec<Sig> = Vec::new();
    match rx.poll_recv_many(&mut cx, &mut buf, 8) {
        Poll::Ready(count) => {
            assert_eq!(count, 2, "poll_recv_many drains only the front");
            let tags: Vec<u64> = buf.iter().map(|s| signal_tag(s).expect("tagged")).collect();
            assert_eq!(tags, vec![301, 302]);
        }
        Poll::Pending => panic!("front non-empty must be Ready"),
    }
    assert_eq!(rx.len(), 2, "the two channel signals remain");
}

#[when(regex = r"^the receiver push_fronts F1, F2$")]
async fn when_push_front_f1_f2_plain(world: &mut MailboxWorld) {
    let batch: VecDeque<Sig> = [301u64, 302].into_iter().map(tagged_signal).collect();
    push_front(world.receiver(), batch);
}

#[when(regex = r"^the receiver push_fronts F3, F4$")]
async fn when_push_front_f3_f4(world: &mut MailboxWorld) {
    let batch: VecDeque<Sig> = [303u64, 304].into_iter().map(tagged_signal).collect();
    push_front(world.receiver(), batch);
}

#[then(regex = r"^the receiver yields F3, F4, F1, F2 in that exact order$")]
async fn then_yields_f3_f4_f1_f2(world: &mut MailboxWorld) {
    assert_eq!(world.received, vec![303, 304, 301, 302]);
}

#[given(regex = r"^(\d+) signals are queued in the channel$")]
async fn given_n_queued(world: &mut MailboxWorld, n: usize) {
    for i in 0..n {
        world
            .sender()
            .try_send(tagged_signal(500 + i as u64))
            .expect("queue within capacity");
    }
}

#[when(regex = r"^the receiver calls recv_many with limit (\d+) into an empty buffer$")]
async fn when_recv_many_into_empty(world: &mut MailboxWorld, limit: usize) {
    let mut buf: Vec<Sig> = Vec::new();
    let count = world.receiver().recv_many(&mut buf, limit).await;
    world.last_count = Some(count);
    world.received = buf.iter().map(|s| signal_tag(s).expect("tagged")).collect();
}

#[then(regex = r"^the returned count is exactly (\d+)$")]
async fn then_count_exactly(world: &mut MailboxWorld, n: usize) {
    assert_eq!(world.last_count, Some(n));
}

#[then(
    regex = r"^exactly those (\d+) signals are appended to the buffer, so count == buffer\.len\(\)$"
)]
async fn then_count_equals_buffer_len(world: &mut MailboxWorld, n: usize) {
    assert_eq!(world.received.len(), n);
    assert_eq!(world.last_count, Some(world.received.len()));
}

#[when(regex = r"^the receiver calls recv_many with limit (\d+)$")]
async fn when_recv_many(world: &mut MailboxWorld, limit: usize) {
    let mut buf: Vec<Sig> = Vec::new();
    let count = world.receiver().recv_many(&mut buf, limit).await;
    world.last_count = Some(count);
    world.received = buf.iter().map(|s| signal_tag(s).expect("tagged")).collect();
}

#[then(regex = r"^exactly 2 signals \(F1, F2\) are appended and the count is 2$")]
async fn then_two_f_appended(world: &mut MailboxWorld) {
    assert_eq!(world.last_count, Some(2));
    assert_eq!(world.received, vec![301, 302]);
}

#[then(regex = r"^the (\d+) channel signals remain unreceived$")]
async fn then_channel_signals_remain(world: &mut MailboxWorld, n: usize) {
    assert_eq!(world.receiver().len(), n);
}

#[when(regex = r"^the receiver push_fronts F1, F2, F3$")]
async fn when_push_front_f1_f2_f3(world: &mut MailboxWorld) {
    let batch: VecDeque<Sig> = [301u64, 302, 303].into_iter().map(tagged_signal).collect();
    push_front(world.receiver(), batch);
}

#[then(regex = r"^a subsequent recv yields F3$")]
async fn then_subsequent_recv_f3(world: &mut MailboxWorld) {
    let sig = world.receiver().recv().await.expect("a signal");
    assert_eq!(signal_tag(&sig), Some(303));
}

#[then(regex = r"^len returns (\d+)$")]
async fn then_len_returns(world: &mut MailboxWorld, n: usize) {
    assert_eq!(world.receiver().len(), n);
}

#[when(regex = r"^the receiver push_fronts F1$")]
async fn when_push_front_f1(world: &mut MailboxWorld) {
    let batch: VecDeque<Sig> = [301u64].into_iter().map(tagged_signal).collect();
    push_front(world.receiver(), batch);
}

#[then(regex = r"^is_empty returns false$")]
async fn then_is_empty_false(world: &mut MailboxWorld) {
    assert!(!world.receiver().is_empty());
}

// ===========================================================================
// @lifecycle — close / drop / weak-upgrade
// ===========================================================================

#[when(regex = r"^the receiver is dropped$")]
async fn when_receiver_dropped(world: &mut MailboxWorld) {
    world.receiver = None;
}

#[when(regex = r"^the sender sends signal S$")]
async fn when_sender_sends_s(world: &mut MailboxWorld) {
    let tag = 700;
    let res = world.sender().send(tagged_signal(tag)).await;
    world.send_result = Some(res.map_err(|err| {
        let returned = signal_tag(&err.0).expect("tagged");
        format!("closed:{returned}")
    }));
}

#[then(regex = r"^send returns Err and the returned error carries the original signal S$")]
async fn then_send_err_carries_s(world: &mut MailboxWorld) {
    let res = world.send_result.as_ref().expect("send result");
    assert_eq!(res.as_ref().err().map(String::as_str), Some("closed:700"));
}

#[when(regex = r"^the receiver calls close$")]
async fn when_receiver_close(world: &mut MailboxWorld) {
    world.receiver().close();
}

#[then(regex = r"^send returns Err carrying signal S$")]
async fn then_send_err_carrying_s(world: &mut MailboxWorld) {
    let res = world.send_result.as_ref().expect("send result");
    assert_eq!(res.as_ref().err().map(String::as_str), Some("closed:700"));
}

#[then(regex = r"^the sender's is_closed returns true$")]
async fn then_sender_is_closed_true(world: &mut MailboxWorld) {
    assert!(world.sender().is_closed());
}

#[given(regex = r"^signals S1, S2 are queued in the channel$")]
async fn given_s1_s2_queued(world: &mut MailboxWorld) {
    for tag in [101u64, 102] {
        world
            .sender()
            .try_send(tagged_signal(tag))
            .expect("queue within capacity");
    }
}

#[when(regex = r"^the receiver calls recv repeatedly$")]
async fn when_recv_repeatedly(world: &mut MailboxWorld) {
    // close() was already called; drain everything buffered, then expect None.
    loop {
        match world.receiver().recv().await {
            Some(sig) => world.received.push(signal_tag(&sig).expect("tagged")),
            None => {
                // Sentinel for the terminal None.
                world.bool_a = Some(true);
                break;
            }
        }
    }
}

#[then(regex = r"^recv yields S1, then S2, then None$")]
async fn then_recv_s1_s2_none(world: &mut MailboxWorld) {
    assert_eq!(world.received, vec![101, 102]);
    assert_eq!(world.bool_a, Some(true), "recv must terminate with None");
}

#[given(regex = r"^the sender is cloned so two strong senders exist$")]
async fn given_sender_cloned(world: &mut MailboxWorld) {
    world.sender2 = Some(world.sender().clone());
}

#[when(regex = r"^one strong sender is dropped$")]
async fn when_one_strong_dropped(world: &mut MailboxWorld) {
    world.sender2 = None;
}

#[then(regex = r"^the surviving sender's is_closed returns false$")]
async fn then_surviving_not_closed(world: &mut MailboxWorld) {
    assert!(!world.sender().is_closed());
}

#[then(regex = r"^strong_count on the surviving sender returns (\d+)$")]
async fn then_strong_count(world: &mut MailboxWorld, n: usize) {
    assert_eq!(world.sender().strong_count(), n);
}

#[given(regex = r"^no signals are queued$")]
async fn given_no_signals(_world: &mut MailboxWorld) {}

#[when(regex = r"^all strong senders are dropped$")]
async fn when_all_strong_dropped(world: &mut MailboxWorld) {
    world.sender = None;
    world.sender2 = None;
}

#[when(regex = r"^the receiver calls recv$")]
async fn when_receiver_recv(world: &mut MailboxWorld) {
    let got = world.receiver().recv().await;
    world.bool_a = Some(got.is_none());
}

#[then(regex = r"^recv returns None$")]
async fn then_recv_none(world: &mut MailboxWorld) {
    assert_eq!(world.bool_a, Some(true));
}

#[given(regex = r"^a weak sender is downgraded from the strong sender$")]
async fn given_weak_downgraded(world: &mut MailboxWorld) {
    world.weak = Some(world.sender().downgrade());
}

#[when(regex = r"^upgrade is called on the weak sender$")]
async fn when_upgrade(world: &mut MailboxWorld) {
    world.bool_a = Some(world.weak.as_ref().expect("weak").upgrade().is_some());
}

#[then(regex = r"^upgrade returns Some$")]
async fn then_upgrade_some(world: &mut MailboxWorld) {
    assert_eq!(world.bool_a, Some(true));
}

#[when(regex = r"^every strong sender is dropped$")]
async fn when_every_strong_dropped(world: &mut MailboxWorld) {
    world.sender = None;
    world.sender2 = None;
}

#[then(regex = r"^upgrade returns None$")]
async fn then_upgrade_none(world: &mut MailboxWorld) {
    assert_eq!(world.bool_a, Some(false));
}

#[given(regex = r"^only a weak sender remains after the strong sender is dropped$")]
async fn given_only_weak_remains(world: &mut MailboxWorld) {
    world.weak = Some(world.sender().downgrade());
    world.sender = None;
    world.sender2 = None;
}

// ===========================================================================
// @boundary — capacity, try_send/Full, kind distinctions
// ===========================================================================

#[given(regex = r"^the single capacity slot is occupied by an unreceived signal$")]
async fn given_slot_occupied(world: &mut MailboxWorld) {
    world
        .sender()
        .try_send(tagged_signal(fresh_tag()))
        .expect("first slot free");
}

#[when(regex = r"^the sender try_sends signal S$")]
async fn when_try_send_s(world: &mut MailboxWorld) {
    world.try_send_result = Some(world.sender().try_send(tagged_signal(700)));
}

#[then(regex = r"^try_send returns Err with the Full variant carrying signal S$")]
async fn then_try_send_full_s(world: &mut MailboxWorld) {
    match world.try_send_result.take().expect("try_send result") {
        Err(mpsc::error::TrySendError::Full(sig)) => {
            assert_eq!(signal_tag(&sig), Some(700));
        }
        _ => panic!("expected Full(S)"),
    }
}

#[given(regex = r"^(\d+) signals are already queued and unreceived$")]
async fn given_n_queued_unreceived(world: &mut MailboxWorld, n: usize) {
    for i in 0..n {
        world
            .sender()
            .try_send(tagged_signal(1000 + i as u64))
            .expect("unbounded never full");
    }
}

#[when(regex = r"^the sender try_sends one more signal$")]
async fn when_try_send_one_more(world: &mut MailboxWorld) {
    world.try_send_result = Some(world.sender().try_send(tagged_signal(9999)));
}

#[then(regex = r"^try_send returns Ok$")]
async fn then_try_send_ok(world: &mut MailboxWorld) {
    assert!(
        world
            .try_send_result
            .take()
            .expect("try_send result")
            .is_ok()
    );
}

#[then(regex = r"^try_send returns Err with the Closed variant carrying signal S$")]
async fn then_try_send_closed_s(world: &mut MailboxWorld) {
    match world.try_send_result.take().expect("try_send result") {
        Err(mpsc::error::TrySendError::Closed(sig)) => {
            assert_eq!(signal_tag(&sig), Some(700));
        }
        _ => panic!("expected Closed(S)"),
    }
}

#[when(regex = r"^bounded is called with buffer 0$")]
async fn when_bounded_zero(world: &mut MailboxWorld) {
    let res = std::panic::catch_unwind(|| {
        let _ = mailbox::bounded::<Probe>(0);
    });
    world.panicked = Some(res.is_err());
}

#[then(regex = r"^the constructor panics$")]
async fn then_constructor_panics(world: &mut MailboxWorld) {
    assert_eq!(world.panicked, Some(true));
}

#[when(regex = r"^the sender calls blocking_send\(S\) on a blocking thread$")]
async fn when_blocking_send_parked(world: &mut MailboxWorld) {
    // Capacity-1 channel currently full. Park a blocking_send on a dedicated
    // thread; it cannot complete until the test thread frees a slot.
    let tx = world.sender().clone();
    let handle = thread::spawn(move || tx.blocking_send(tagged_signal(700)));
    // Stash the join handle by holding it in send_result via a channel trick is
    // overkill; instead drive the unblock inline in the next step. We need to
    // keep the handle alive across steps, so store it on the world.
    world.blocking_handle = Some(handle);
}

#[when(regex = r"^the receiver later frees one slot$")]
async fn when_receiver_frees_slot(world: &mut MailboxWorld) {
    // Free the occupied slot so the parked blocking_send can proceed.
    let _ = world
        .receiver()
        .recv()
        .await
        .expect("the pre-filled signal");
}

#[then(regex = r"^blocking_send returns Ok once the slot is available$")]
async fn then_blocking_send_ok(world: &mut MailboxWorld) {
    let handle = world.blocking_handle.take().expect("blocking handle");
    let res = handle.join().expect("blocking thread joins");
    assert!(res.is_ok(), "parked blocking_send completes Ok");
    // Drain the now-delivered S so the channel is empty for the next clause.
    let sig = world.receiver().recv().await.expect("the parked signal S");
    assert_eq!(signal_tag(&sig), Some(700));
}

#[then(
    regex = r"^a send_timeout\(S2, d\) on a still-full channel returns the timeout error after d elapses$"
)]
async fn then_send_timeout_elapses(world: &mut MailboxWorld) {
    // Re-fill the capacity-1 channel, then run send_timeout in a dedicated
    // PAUSED current-thread runtime so the timeout fires deterministically with
    // ~zero wall-clock and no real sleep. The paused clock auto-advances to the
    // pending timer when the runtime is otherwise idle.
    world
        .sender()
        .try_send(tagged_signal(800))
        .expect("re-fill the single slot");
    let tx = world.sender().clone();
    let outcome = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("paused runtime");
        rt.block_on(async move {
            tx.send_timeout(tagged_signal(801), Duration::from_secs(5))
                .await
        })
    })
    .await
    .expect("send_timeout thread");
    match outcome {
        Err(mpsc::error::SendTimeoutError::Timeout(sig)) => {
            assert_eq!(
                signal_tag(&sig),
                Some(801),
                "timeout surfaces the un-sent signal"
            );
        }
        _ => panic!("expected Timeout(S2)"),
    }
}

#[then(regex = r"^the sender's capacity returns Some\((\d+)\)$")]
async fn then_capacity_some(world: &mut MailboxWorld, n: usize) {
    assert_eq!(world.sender().capacity(), Some(n));
}

#[then(regex = r"^the sender's max_capacity returns Some\((\d+)\)$")]
async fn then_max_capacity_some(world: &mut MailboxWorld, n: usize) {
    assert_eq!(world.sender().max_capacity(), Some(n));
}

#[then(regex = r"^the sender's capacity for an unbounded mailbox returns None$")]
async fn then_unbounded_capacity_none(_world: &mut MailboxWorld) {
    let (tx, _rx) = mailbox::unbounded::<Probe>();
    assert_eq!(tx.capacity(), None);
}

#[then(regex = r"^the sender's max_capacity for an unbounded mailbox returns None$")]
async fn then_unbounded_max_capacity_none(_world: &mut MailboxWorld) {
    let (tx, _rx) = mailbox::unbounded::<Probe>();
    assert_eq!(tx.max_capacity(), None);
}

#[when(regex = r"^(\d+) signals are sent and left unreceived$")]
async fn when_n_sent_unreceived(world: &mut MailboxWorld, n: usize) {
    for i in 0..n {
        world
            .sender()
            .try_send(tagged_signal(1100 + i as u64))
            .expect("within capacity");
    }
}

#[then(regex = r"^max_capacity still returns Some\((\d+)\)$")]
async fn then_max_capacity_still(world: &mut MailboxWorld, n: usize) {
    assert_eq!(world.sender().max_capacity(), Some(n));
}

#[when(regex = r"^the receiver calls recv once$")]
async fn when_recv_once(world: &mut MailboxWorld) {
    let _ = world.receiver().recv().await.expect("a buffered signal");
}

#[when(regex = r"^the receiver calls recv once, freeing a slot$")]
async fn when_recv_once_freeing(world: &mut MailboxWorld) {
    // Frees the single occupied slot so the parked send (given step) can proceed.
    let _ = world
        .receiver()
        .recv()
        .await
        .expect("the pre-filled signal");
}

#[given(regex = r"^two bounded senders A and A2 that are clones of the same sender$")]
async fn given_two_clones(world: &mut MailboxWorld) {
    let (tx, rx) = mailbox::bounded::<Probe>(8);
    world.sender2 = Some(tx.clone());
    world.sender = Some(tx);
    world.receiver = Some(rx);
}

#[given(regex = r"^a bounded sender B from a different channel$")]
async fn given_sender_b(world: &mut MailboxWorld) {
    let (tx, _rx) = mailbox::bounded::<Probe>(8);
    world.other_sender = Some(tx);
}

#[then(regex = r"^A\.same_channel\(A2\) returns true$")]
async fn then_same_channel_true(world: &mut MailboxWorld) {
    assert!(
        world
            .sender()
            .same_channel(world.sender2.as_ref().expect("A2"))
    );
}

#[then(regex = r"^A\.same_channel\(B\) returns false$")]
async fn then_same_channel_b_false(world: &mut MailboxWorld) {
    assert!(
        !world
            .sender()
            .same_channel(world.other_sender.as_ref().expect("B"))
    );
}

#[given(regex = r"^a bounded sender A$")]
async fn given_sender_a(world: &mut MailboxWorld) {
    let (tx, rx) = mailbox::bounded::<Probe>(8);
    world.sender = Some(tx);
    world.receiver = Some(rx);
}

#[given(regex = r"^an unbounded sender U$")]
async fn given_sender_u(world: &mut MailboxWorld) {
    let (tx, _rx) = mailbox::unbounded::<Probe>();
    world.unbounded_sender = Some(tx);
}

#[then(regex = r"^A\.same_channel\(U\) returns false$")]
async fn then_a_u_false(world: &mut MailboxWorld) {
    assert!(
        !world
            .sender()
            .same_channel(world.unbounded_sender.as_ref().expect("U"))
    );
}

#[then(regex = r"^U\.same_channel\(A\) returns false$")]
async fn then_u_a_false(world: &mut MailboxWorld) {
    assert!(
        !world
            .unbounded_sender
            .as_ref()
            .expect("U")
            .same_channel(world.sender())
    );
}

#[when(regex = r"^signal_startup_finished is invoked on the sender$")]
async fn when_signal_startup(world: &mut MailboxWorld) {
    world.signal_result = Some(world.sender().signal_startup_finished());
}

#[then(regex = r"^it returns Err\(SendError::MailboxFull\)$")]
async fn then_signal_mailbox_full(world: &mut MailboxWorld) {
    match world.signal_result.take().expect("signal result") {
        Err(SendError::MailboxFull(())) => {}
        other => panic!("expected MailboxFull, got {other:?}"),
    }
}

#[then(regex = r"^it returns Err\(SendError::ActorNotRunning\)$")]
async fn then_signal_not_running(world: &mut MailboxWorld) {
    match world.signal_result.take().expect("signal result") {
        Err(SendError::ActorNotRunning(())) => {}
        other => panic!("expected ActorNotRunning, got {other:?}"),
    }
}

#[when(regex = r"^signal_stop is awaited on the weak sender$")]
async fn when_signal_stop_weak(world: &mut MailboxWorld) {
    let res = world.weak.as_ref().expect("weak").signal_stop().await;
    world.signal_result = Some(res);
}

// ===========================================================================
// @linearizability — concurrent senders / receiver
// ===========================================================================

#[given(regex = r"^the single slot is occupied by an unreceived signal$")]
async fn given_single_slot_occupied(world: &mut MailboxWorld) {
    world
        .sender()
        .try_send(tagged_signal(600))
        .expect("first slot free");
}

#[given(regex = r"^a task is awaiting send of signal S and is therefore parked$")]
async fn given_task_awaiting_send(world: &mut MailboxWorld) {
    let tx = world.sender().clone();
    let (started_tx, started_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        // Signal we are about to park on a guaranteed-full channel.
        let _ = started_tx.send(());
        tx.send(tagged_signal(700)).await
    });
    started_rx.await.expect("task started");
    // Give the parked send a moment to register as pending against the full
    // channel before the receiver frees a slot (bounded settle, not a sleep on
    // an observable that may never arrive).
    settle_capacity_zero(world.sender()).await;
    world.send_task = Some(handle);
}

#[then(regex = r"^the parked send completes with Ok$")]
async fn then_parked_send_ok(world: &mut MailboxWorld) {
    // The recv that frees the slot happened in `when_receiver_recv_once` /
    // `when_receiver_frees_slot`; await the parked task's completion.
    let handle = world.send_task.take().expect("send task");
    let res = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("parked send must complete once a slot frees")
        .expect("send task joins");
    assert!(res.is_ok(), "parked send completes Ok");
}

#[then(regex = r"^a subsequent recv yields signal S$")]
async fn then_subsequent_recv_s(world: &mut MailboxWorld) {
    let sig = world.receiver().recv().await.expect("signal S");
    assert_eq!(signal_tag(&sig), Some(700));
}

#[when(regex = r"^(\d+) tasks each send (\d+) signals concurrently via await-send$")]
async fn when_concurrent_senders(world: &mut MailboxWorld, tasks: usize, per: usize) {
    let barrier = Arc::new(Barrier::new(tasks + 1));
    let mut handles = Vec::with_capacity(tasks);
    for t in 0..tasks {
        let tx = world.sender().clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for i in 0..per {
                let tag = (t as u64) * 1_000_000 + i as u64;
                tx.send(tagged_signal(tag))
                    .await
                    .expect("send under capacity");
            }
        }));
    }
    barrier.wait().await;
    world.send_handles = handles;
    world.expected_total = Some(tasks * per);
}

#[when(regex = r"^a single receiver drains until the channel closes$")]
async fn when_drain_until_closed(world: &mut MailboxWorld) {
    // Drop the world's strong sender so the channel closes once the spawned
    // senders finish. The spawned tasks each hold a clone, so the receiver sees
    // None only after every clone is dropped (i.e. after every send completes).
    world.sender = None;
    let mut rx = world.receiver.take().expect("receiver set");
    let mut count = 0usize;
    while rx.recv().await.is_some() {
        count = count.checked_add(1).expect("no overflow");
    }
    world.receiver = Some(rx);
    world.last_count = Some(count);
    for h in world.send_handles.drain(..) {
        h.await.expect("sender task joins");
    }
}

#[then(regex = r"^the receiver observes exactly 1000 signals with no loss and no duplication$")]
async fn then_observes_exactly_1000(world: &mut MailboxWorld) {
    assert_eq!(world.expected_total, Some(1000));
    assert_eq!(world.last_count, Some(1000));
}

#[given(regex = r"^multiple concurrent senders performing await-send$")]
async fn given_multiple_senders(_world: &mut MailboxWorld) {}

#[given(regex = r"^a single receiver draining concurrently$")]
async fn given_single_receiver_draining(_world: &mut MailboxWorld) {}

#[then(regex = r"^at every observation the count received is less than or equal to the count")]
async fn then_received_le_sent(_world: &mut MailboxWorld) {
    // Real overlap: many senders + one concurrently-draining receiver, with the
    // received count and acked-sent count sampled together at each step.
    let cap = 4usize;
    let (tx, mut rx) = mailbox::bounded::<Probe>(cap);
    let senders = 6usize;
    let per = 50usize;
    let acked = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let max_cap = tx.max_capacity().expect("bounded");
    assert_eq!(max_cap, cap);

    let barrier = Arc::new(Barrier::new(senders + 1));
    let mut handles = Vec::new();
    for t in 0..senders {
        let tx = tx.clone();
        let acked = Arc::clone(&acked);
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for i in 0..per {
                let tag = (t as u64) * 1_000_000 + i as u64;
                // Count the send as "acknowledged" BEFORE the value can enter the
                // channel, so a concurrently-draining receiver can never observe a
                // message that has not yet been acked. (Incrementing AFTER the
                // await admits a real race where the receiver pulls the value and
                // counts it before the sender's fetch_add lands — that is not an
                // invariant violation, just counter visibility, so the count must
                // be bumped first to make `received <= acked` meaningful.)
                acked.fetch_add(1, Ordering::SeqCst);
                tx.send(tagged_signal(tag)).await.expect("send");
            }
        }));
    }
    drop(tx);
    barrier.wait().await;

    while rx.recv().await.is_some() {
        received.fetch_add(1, Ordering::SeqCst);
        // At every observation: received <= acked-sent, and capacity bounded.
        let r = received.load(Ordering::SeqCst);
        let a = acked.load(Ordering::SeqCst);
        assert!(r <= a, "received {r} must never exceed acked-sent {a}");
        assert!(rx.len() <= max_cap, "channel backlog within max capacity");
    }
    for h in handles {
        h.await.expect("sender joins");
    }
    assert_eq!(
        received.load(Ordering::SeqCst),
        (senders * per) as u64,
        "every acked send is eventually received"
    );
}

#[given(regex = r"^two concurrent senders A and B each sending an ordered numbered sequence$")]
async fn given_two_ordered_senders(_world: &mut MailboxWorld) {}

#[when(regex = r"^a single receiver drains all signals$")]
async fn when_single_receiver_drains_ordered(world: &mut MailboxWorld) {
    // Two senders, each emitting a strictly increasing per-sender sequence into
    // a shared bounded channel; one receiver drains everything. We tag each
    // signal as sender_id * BASE + seq so the per-sender subsequence is
    // recoverable from the received order.
    const BASE: u64 = 1_000_000;
    let cap = 4usize;
    let per = 100u64;
    let (tx, mut rx) = mailbox::bounded::<Probe>(cap);
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for sender_id in 0..2u64 {
        let tx = tx.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for seq in 0..per {
                tx.send(tagged_signal(sender_id * BASE + seq))
                    .await
                    .expect("send");
            }
        }));
    }
    drop(tx);
    barrier.wait().await;
    let mut received = Vec::new();
    while let Some(sig) = rx.recv().await {
        received.push(signal_tag(&sig).expect("tagged"));
    }
    for h in handles {
        h.await.expect("sender joins");
    }
    world.received = received;
}

#[then(regex = r"^the subsequence of signals from sender A is strictly increasing$")]
async fn then_subseq_a_increasing(world: &mut MailboxWorld) {
    assert_subsequence_strictly_increasing(&world.received, 0);
}

#[then(regex = r"^the subsequence of signals from sender B is strictly increasing$")]
async fn then_subseq_b_increasing(world: &mut MailboxWorld) {
    assert_subsequence_strictly_increasing(&world.received, 1);
}

// ===========================================================================
// @property / @model laws (proptest + deterministic interleavings)
// ===========================================================================

#[given(regex = r"^a bounded mailbox of any capacity c$")]
async fn given_any_capacity(_world: &mut MailboxWorld) {}

#[given(regex = r"^any sequence of n distinct tagged messages$")]
async fn given_any_sequence(_world: &mut MailboxWorld) {}

#[when(regex = r"^all n messages are sent, interleaving receives to relieve backpressure$")]
async fn when_send_interleaving(_world: &mut MailboxWorld) {}

#[then(regex = r"^the receiver observes the messages in exactly send order$")]
async fn law_fifo_any_capacity(_world: &mut MailboxWorld) {
    // ∀ c ∈ boundary-biased caps, n ∈ [0, 4c]: send n distinct tags into a
    // bounded(c) channel while interleaving receives, the received order == send
    // order. ORACLE: a VecDeque pushed on send, popped on recv.
    let caps = [1usize, 2, 7, 64];
    for &c in &caps {
        for n in [0usize, 1, c.saturating_sub(1), c, c + 1, 4 * c] {
            let (tx, mut rx) = mailbox::bounded::<Probe>(c);
            let mut oracle: VecDeque<u64> = VecDeque::new();
            let mut got: Vec<u64> = Vec::new();
            for tag in 0..n as u64 {
                // Relieve backpressure: drain to keep the channel under capacity.
                while tx.capacity() == Some(0) {
                    let sig = rx.recv().await.expect("a buffered signal");
                    let observed = signal_tag(&sig).expect("tagged");
                    assert_eq!(observed, oracle.pop_front().expect("oracle non-empty"));
                    got.push(observed);
                }
                tx.send(tagged_signal(tag)).await.expect("send");
                oracle.push_back(tag);
            }
            drop(tx);
            while let Some(sig) = rx.recv().await {
                let observed = signal_tag(&sig).expect("tagged");
                assert_eq!(observed, oracle.pop_front().expect("oracle non-empty"));
                got.push(observed);
            }
            assert!(oracle.is_empty(), "oracle fully consumed");
            let expected: Vec<u64> = (0..n as u64).collect();
            assert_eq!(got, expected, "FIFO for cap {c} n {n}");
        }
    }
}

#[given(regex = r"^the mailbox already holds any k messages with k in \[0, c\]$")]
async fn given_holds_k(_world: &mut MailboxWorld) {}

#[when(regex = r"^one more message is offered with try_send$")]
async fn when_offer_one_more(_world: &mut MailboxWorld) {}

#[then(regex = r"^it succeeds iff k < c and returns Full\(message\) iff k == c$")]
async fn law_try_send_full_iff_at_cap(_world: &mut MailboxWorld) {
    // ∀ c ∈ {1,2,64,1024}, k ∈ [0, c]: try_send succeeds iff k < c, Full iff k == c.
    let caps = [1usize, 2, 64, 1024];
    for &c in &caps {
        for k in [0usize, c.saturating_sub(1), c] {
            let (tx, _rx) = mailbox::bounded::<Probe>(c);
            for i in 0..k {
                tx.try_send(tagged_signal(i as u64))
                    .expect("k <= c slots free");
            }
            let res = tx.try_send(tagged_signal(9999));
            if k < c {
                assert!(res.is_ok(), "k {k} < c {c} must succeed");
            } else {
                match res {
                    Err(mpsc::error::TrySendError::Full(sig)) => {
                        assert_eq!(signal_tag(&sig), Some(9999), "Full carries the message");
                    }
                    _ => panic!("k == c must be Full"),
                }
            }
        }
    }
}

#[given(regex = r"^any message count n$")]
async fn given_any_count_n(_world: &mut MailboxWorld) {}

#[when(regex = r"^all n messages are sent without receiving$")]
async fn when_all_sent_no_recv(_world: &mut MailboxWorld) {}

#[then(regex = r"^every send succeeds and none returns Full$")]
async fn law_unbounded_never_full(_world: &mut MailboxWorld) {
    // ∀ n ∈ boundary-biased counts: unbounded try_send never returns Full.
    for n in [0usize, 1, 1_000, 100_000] {
        let (tx, _rx) = mailbox::unbounded::<Probe>();
        for i in 0..n {
            match tx.try_send(tagged_signal(i as u64)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    panic!("unbounded must never report Full (n={n}, i={i})")
                }
                Err(_) => panic!("unexpected unbounded error"),
            }
        }
        assert_eq!(tx.capacity(), None, "unbounded capacity is always None");
    }
}

#[given(regex = r"^any batch F of messages pushed to the front$")]
async fn given_batch_f(_world: &mut MailboxWorld) {}

#[given(regex = r"^any batch C of messages sent through the channel$")]
async fn given_batch_c(_world: &mut MailboxWorld) {}

#[when(regex = r"^the receiver drains via any mix of recv / recv_many / try_recv$")]
async fn when_drain_mixed(_world: &mut MailboxWorld) {}

#[then(regex = r"^all of F is observed before any of C, F in push order$")]
async fn law_front_drains_before_channel(_world: &mut MailboxWorld) {
    // ∀ |F|, |C| ∈ [0, 32]: front drains entirely (in push order) before any
    // channel signal, regardless of recv variant. ORACLE: F ++ C.
    for f_len in [0usize, 1, 5, 32] {
        for c_len in [0usize, 1, 5, 32] {
            // F tags use a high base so they never collide with C tags.
            let (tx, mut rx) = mailbox::bounded::<Probe>(64.max(c_len.max(1)));
            for i in 0..c_len {
                tx.try_send(tagged_signal(i as u64))
                    .expect("channel within cap");
            }
            let batch: VecDeque<Sig> = (0..f_len)
                .map(|i| tagged_signal(1_000_000 + i as u64))
                .collect();
            push_front(&mut rx, batch);

            let mut got: Vec<u64> = Vec::new();
            drop(tx);
            // Mixed drain: alternate recv / try_recv / recv_many.
            let mut variant = 0u8;
            loop {
                match variant % 3 {
                    0 => match rx.recv().await {
                        Some(sig) => got.push(signal_tag(&sig).unwrap()),
                        None => break,
                    },
                    1 => match rx.try_recv() {
                        Ok(sig) => got.push(signal_tag(&sig).unwrap()),
                        Err(mpsc::error::TryRecvError::Empty) => {
                            // Nothing immediately ready; fall back to await.
                            match rx.recv().await {
                                Some(sig) => got.push(signal_tag(&sig).unwrap()),
                                None => break,
                            }
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    },
                    _ => {
                        let mut buf: Vec<Sig> = Vec::new();
                        let n = rx.recv_many(&mut buf, 4).await;
                        if n == 0 {
                            break;
                        }
                        for sig in &buf {
                            got.push(signal_tag(sig).unwrap());
                        }
                    }
                }
                variant = variant.wrapping_add(1);
            }
            let expected: Vec<u64> = (0..f_len)
                .map(|i| 1_000_000 + i as u64)
                .chain(0..c_len as u64)
                .collect();
            assert_eq!(got, expected, "F-before-C for |F|={f_len} |C|={c_len}");
        }
    }
}

#[given(regex = r"^a mailbox \(bounded or unbounded\) holding any k buffered messages$")]
async fn given_holding_k_buffered(_world: &mut MailboxWorld) {}

#[when(regex = r"^the receiver is closed$")]
async fn when_receiver_is_closed_law(_world: &mut MailboxWorld) {}

#[when(regex = r"^any further send / try_send is attempted$")]
async fn when_further_send_attempted(_world: &mut MailboxWorld) {}

#[then(regex = r"^buffered messages still drain in order, then recv yields None$")]
async fn law_close_drains_then_none(_world: &mut MailboxWorld) {
    // ∀ variant ∈ {bounded(c), unbounded}, k buffered: after close(), buffered
    // signals still drain in order, then recv yields None.
    for bounded in [true, false] {
        for k in [0usize, 1, 5, 50] {
            let (tx, mut rx) = if bounded {
                mailbox::bounded::<Probe>(64.max(k.max(1)))
            } else {
                mailbox::unbounded::<Probe>()
            };
            for i in 0..k {
                tx.try_send(tagged_signal(i as u64)).expect("within cap");
            }
            rx.close();
            let mut got = Vec::new();
            while let Some(sig) = rx.recv().await {
                got.push(signal_tag(&sig).unwrap());
            }
            let expected: Vec<u64> = (0..k as u64).collect();
            assert_eq!(
                got, expected,
                "buffered drains in order (bounded={bounded} k={k})"
            );
            assert!(rx.recv().await.is_none(), "terminal None after drain");
        }
    }
}

#[then(regex = r"^each post-close send returns the signal it failed to deliver$")]
async fn law_post_close_send_returns_signal(_world: &mut MailboxWorld) {
    // ∀ variant: a send after close returns the un-delivered signal.
    for bounded in [true, false] {
        let (tx, mut rx) = if bounded {
            mailbox::bounded::<Probe>(8)
        } else {
            mailbox::unbounded::<Probe>()
        };
        rx.close();
        let res = tx.try_send(tagged_signal(4242));
        match res {
            Err(mpsc::error::TrySendError::Closed(sig)) => {
                assert_eq!(
                    signal_tag(&sig),
                    Some(4242),
                    "Closed carries the un-sent signal"
                );
            }
            _ => panic!("post-close try_send must be Closed (bounded={bounded})"),
        }
    }
}

#[given(regex = r"^P concurrent senders, each sending k distinct tagged messages$")]
async fn given_p_senders(_world: &mut MailboxWorld) {}

#[given(regex = r"^one concurrent draining receiver$")]
async fn given_one_draining_receiver(_world: &mut MailboxWorld) {}

#[when(regex = r"^all senders and the receiver run with real overlap$")]
async fn when_real_overlap(world: &mut MailboxWorld) {
    // P ∈ [2,8], k ∈ [1,50]; tags = sender_id*BASE + seq. Real overlap via
    // tokio::spawn + Barrier; collect the received history for the laws below.
    const BASE: u64 = 1_000_000;
    for (p, k) in [(2usize, 1u64), (3, 10), (5, 25), (8, 50)] {
        let cap = 4usize;
        let (tx, mut rx) = mailbox::bounded::<Probe>(cap);
        let barrier = Arc::new(Barrier::new(p + 1));
        let mut handles = Vec::new();
        for sender_id in 0..p as u64 {
            let tx = tx.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for seq in 0..k {
                    tx.send(tagged_signal(sender_id * BASE + seq))
                        .await
                        .expect("send");
                }
            }));
        }
        drop(tx);
        barrier.wait().await;
        let mut received = Vec::new();
        while let Some(sig) = rx.recv().await {
            received.push(signal_tag(&sig).unwrap());
        }
        for h in handles {
            h.await.expect("sender joins");
        }
        // Exactly-once: the received multiset equals the sent set.
        let mut expected: Vec<u64> = (0..p as u64)
            .flat_map(|s| (0..k).map(move |seq| s * BASE + seq))
            .collect();
        let mut sorted = received.clone();
        sorted.sort_unstable();
        expected.sort_unstable();
        assert_eq!(
            sorted, expected,
            "every message received exactly once (P={p} k={k})"
        );
        // Per-sender FIFO within the received stream.
        for s in 0..p as u64 {
            assert_subsequence_strictly_increasing(&received, s);
        }
    }
    world.bool_a = Some(true);
}

#[then(regex = r"^every message is received exactly once$")]
async fn then_every_message_once(world: &mut MailboxWorld) {
    assert_eq!(world.bool_a, Some(true));
}

#[then(regex = r"^for each sender, its messages appear in send order within the received stream$")]
async fn then_per_sender_order(world: &mut MailboxWorld) {
    assert_eq!(world.bool_a, Some(true));
}

#[then(regex = r"^no global cross-sender order is required$")]
async fn then_no_global_order(_world: &mut MailboxWorld) {
    // Documented: tokio mpsc guarantees per-sender FIFO only. Nothing to assert.
}

#[given(regex = r"^any interleaving of clone and drop operations on the mailbox sender$")]
async fn given_clone_drop_interleaving(_world: &mut MailboxWorld) {}

#[when(regex = r"^the operations run concurrently$")]
async fn when_ops_concurrent(_world: &mut MailboxWorld) {}

#[then(regex = r"^is_closed becomes true exactly when the last strong sender is dropped$")]
async fn law_is_closed_when_last_dropped(_world: &mut MailboxWorld) {
    // Deterministic op-sequence model over {clone, drop}: is_closed ⇔ strong
    // count reached 0. The receiver is kept alive throughout so closure is
    // driven purely by sender count. We hold the strong senders in a Vec and
    // assert is_closed against the integer model after each op.
    proptest!(|(ops in proptest::collection::vec(any::<bool>(), 1..64))| {
        let (tx, _rx) = mailbox::bounded::<Probe>(8);
        let mut senders = vec![tx];
        // Keep one weak handle to assert no upgrade after close.
        let weak = senders[0].downgrade();
        for op in &ops {
            if *op || senders.is_empty() {
                // clone (also forced when empty would otherwise stay closed-forever)
                if let Some(s) = senders.first() {
                    let c = s.clone();
                    senders.push(c);
                }
            } else {
                senders.pop();
            }
            let model_closed = senders.is_empty();
            // is_closed is observable from the surviving sender OR the weak handle.
            match senders.first() {
                Some(s) => prop_assert_eq!(s.is_closed(), model_closed),
                None => {
                    // No strong sender: the channel is closed; weak cannot upgrade.
                    prop_assert!(weak.upgrade().is_none());
                }
            }
        }
    });
}

#[then(regex = r"^no weak upgrade succeeds after that point$")]
async fn law_no_weak_upgrade_after_close(_world: &mut MailboxWorld) {
    // Once the last strong sender drops, every weak upgrade returns None.
    let (tx, _rx) = mailbox::bounded::<Probe>(8);
    let weak = tx.downgrade();
    assert!(
        weak.upgrade().is_some(),
        "upgrade succeeds while strong alive"
    );
    drop(tx);
    assert!(
        weak.upgrade().is_none(),
        "no upgrade after last strong dropped"
    );
}

// ===========================================================================
// helpers
// ===========================================================================

/// Asserts that the subsequence of `received` belonging to `sender_id`
/// (tags of the form `sender_id * 1_000_000 + seq`) is strictly increasing in
/// `seq` — the per-sender FIFO guarantee.
fn assert_subsequence_strictly_increasing(received: &[u64], sender_id: u64) {
    const BASE: u64 = 1_000_000;
    let lo = sender_id * BASE;
    let hi = lo + BASE;
    let mut last: Option<u64> = None;
    for &tag in received {
        if (lo..hi).contains(&tag) {
            let seq = tag - lo;
            if let Some(prev) = last {
                assert!(
                    seq > prev,
                    "sender {sender_id} out of order: {prev} then {seq}"
                );
            }
            last = Some(seq);
        }
    }
}

/// Bounded settle: polls until the bounded sender reports zero remaining
/// capacity (i.e. the parked send is registered as waiting on a full channel).
/// Panics loudly if the condition never holds.
async fn settle_capacity_zero(tx: &MailboxSender<Probe>) {
    for _ in 0..1000 {
        if tx.capacity() == Some(0) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("channel never reached zero capacity (parked send did not register)");
}
