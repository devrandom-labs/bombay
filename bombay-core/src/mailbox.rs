//! The actor's in-memory mailbox: a bounded MPSC queue of [`Signal`]s.
//!
//! The local tier of the two-tier message model (#66): typed, in-memory,
//! **zero-serialize**. `tell` moves an `A::Msg` into a queue slot — no
//! per-message heap box. Built on `tokio::sync::mpsc`, bounded for backpressure.

use std::{fmt, num::NonZeroUsize};

use tokio::sync::mpsc;

/// A validated mailbox capacity: at least `1`, at most [`Capacity::MAX`].
///
/// Makes both illegal capacities unrepresentable, so [`bounded`] cannot fail:
/// zero is excluded by `NonZeroUsize`, and the upper bound is checked here
/// rather than trusting `tokio::sync::mpsc::channel` (which panics outside
/// `1..=MAX`). Validating at our own boundary is deliberate — we do not rely on
/// an upstream crate's panic as our error path (rule 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity(NonZeroUsize);

impl Capacity {
    /// The largest capacity `tokio::sync::mpsc::channel` accepts — the tokio
    /// `Semaphore` permit ceiling (`usize::MAX >> 3`). Mirrored here because
    /// tokio does not expose it; the `capacity_at_the_upper_boundary_is_usable`
    /// test guards this constant against tokio lowering the limit.
    pub const MAX: usize = usize::MAX >> 3;

    /// Builds a `Capacity`, returning `None` if `value` exceeds [`Capacity::MAX`].
    #[must_use]
    pub const fn new(value: NonZeroUsize) -> Option<Self> {
        if value.get() > Self::MAX {
            None
        } else {
            Some(Self(value))
        }
    }

    /// The capacity as a `usize`, always in `1..=MAX`.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// The seam between a mailbox and its actor.
///
/// A mailbox is monomorphized per actor `A`, carrying that actor's single closed
/// message type `A::Msg` by value — no `Box<dyn>`. This scaffold trait is what
/// the rebuilt `Actor` trait (#114/#116) will later subsume; keeping it separate
/// lets the mailbox be built and hard-tested on its own (#112).
///
/// `Msg` is `Send + 'static` for now; the cfg-gated `MaybeSend` relaxation for
/// single-threaded client builds arrives with #9.
pub trait Mailboxed {
    /// The actor's single closed message type, stored in the queue by value.
    type Msg: Send + 'static;
}

/// Scaffold actor identity. #121 replaces this with the identity-first AID /
/// key-expr `ActorId`; it exists here only so the mailbox's [`LinkDied`] arm has
/// a concrete shape to carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorId(u64);

impl ActorId {
    /// Wraps a raw identifier.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

/// Scaffold reason a linked actor stopped. #113/#120 own the real
/// error/supervision model; the `String` in `Panicked` is deliberately the fat
/// field that makes [`LinkDied`] worth boxing.
#[derive(Debug, Clone)]
#[expect(
    clippy::exhaustive_enums,
    reason = "scaffold placeholder for the #113/#120 stop-reason model"
)]
pub enum StopReason {
    /// The actor returned from its run-loop normally.
    Normal,
    /// The actor's handler panicked, with the panic message.
    Panicked(String),
}

/// The payload of a [`Signal::LinkDied`]: a linked actor has terminated.
///
/// Boxed inside [`Signal`] because it is a **cold** control path — boxing it
/// keeps the hot [`Signal::Message`] slot small (see the large-variant
/// discipline).
#[derive(Debug, Clone)]
#[expect(
    clippy::exhaustive_structs,
    reason = "scaffold placeholder for the #120 links/supervision payload"
)]
pub struct LinkDied {
    /// The dead actor's identity.
    pub id: ActorId,
    /// Why it stopped.
    pub reason: StopReason,
}

/// A signal in an actor's mailbox: a domain message or a system control signal.
///
/// A **concrete, closed** envelope — no `Box<dyn>` at either layer. `tell` moves
/// an `A::Msg` into a [`Signal::Message`] slot, so a send is zero-allocation.
#[expect(
    clippy::exhaustive_enums,
    reason = "the signal set is deliberately closed so the run-loop is a total match; \
              new arms (Stop, LinkDied, …) are added under their driving cards"
)]
pub enum Signal<A: Mailboxed> {
    /// A domain message for the actor to handle.
    Message(A::Msg),
    /// Asks the actor to stop after draining messages queued before it.
    Stop,
    /// A linked actor has terminated. Boxed: this is a cold path, and inlining
    /// its fields would inflate every message slot (large-variant discipline).
    LinkDied(Box<LinkDied>),
}

/// Sends [`Signal`]s to an actor's mailbox. Cloneable; the channel stays open
/// while any sender is alive.
pub struct MailboxSender<A: Mailboxed> {
    tx: mpsc::Sender<Signal<A>>,
}

impl<A: Mailboxed> Clone for MailboxSender<A> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

/// A non-pinning handle to a mailbox: holding one does **not** keep it alive.
///
/// [`upgrade`](Self::upgrade) yields a strong sender only while a real
/// [`MailboxSender`] still exists — the primitive death-watch is built on.
pub struct WeakMailboxSender<A: Mailboxed> {
    weak: mpsc::WeakSender<Signal<A>>,
}

impl<A: Mailboxed> WeakMailboxSender<A> {
    /// Upgrades to a strong [`MailboxSender`], or `None` if every strong sender
    /// has been dropped (the actor is gone).
    #[must_use]
    pub fn upgrade(&self) -> Option<MailboxSender<A>> {
        self.weak.upgrade().map(|tx| MailboxSender { tx })
    }
}

impl<A: Mailboxed> Clone for WeakMailboxSender<A> {
    fn clone(&self) -> Self {
        Self {
            weak: self.weak.clone(),
        }
    }
}

/// The single consumer of an actor's mailbox. The run-loop pulls from it.
pub struct MailboxReceiver<A: Mailboxed> {
    rx: mpsc::Receiver<Signal<A>>,
}

/// The receiver was dropped, so the signal could not be delivered.
///
/// Carries the undelivered [`Signal`] back to the caller (rule 3: never silently
/// drop the payload).
pub struct SendError<A: Mailboxed>(pub Signal<A>);

impl<A: Mailboxed> fmt::Debug for SendError<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(receiver dropped)")
    }
}

/// Why a non-blocking [`MailboxSender::try_send`] could not enqueue a signal.
///
/// Both variants carry the undelivered [`Signal`] back to the caller. `Full` is
/// retryable (drain, then retry); `Closed` is terminal (the actor is gone).
#[expect(
    clippy::exhaustive_enums,
    reason = "closed set — a try_send fails for exactly these two reasons"
)]
pub enum TrySendError<A: Mailboxed> {
    /// The mailbox is at capacity; back off and retry.
    Full(Signal<A>),
    /// The receiver has been dropped; the actor is no longer running.
    Closed(Signal<A>),
}

impl<A: Mailboxed> fmt::Debug for TrySendError<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("TrySendError::Full(mailbox at capacity)"),
            Self::Closed(_) => f.write_str("TrySendError::Closed(receiver dropped)"),
        }
    }
}

/// Creates a bounded mailbox with room for `capacity` queued signals.
///
/// Bounded is the only mode: a full mailbox exerts backpressure rather than
/// growing without limit (an unbounded queue is a memory footgun). Infallible
/// by construction — [`Capacity`] has already excluded the values that would
/// make the underlying channel panic.
#[must_use]
pub fn bounded<A: Mailboxed>(capacity: Capacity) -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::channel(capacity.get());
    (MailboxSender { tx }, MailboxReceiver { rx })
}

impl<A: Mailboxed> MailboxSender<A> {
    /// Sends `signal`, waiting for capacity if the mailbox is full.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] (carrying `signal` back) if the receiver has been
    /// dropped, i.e. the actor is no longer running.
    pub async fn send(&self, signal: Signal<A>) -> Result<(), SendError<A>> {
        self.tx.send(signal).await.map_err(|err| SendError(err.0))
    }

    /// Tries to enqueue `signal` without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if the mailbox is at capacity, or
    /// [`TrySendError::Closed`] if the receiver has been dropped. Both carry
    /// `signal` back to the caller.
    pub fn try_send(&self, signal: Signal<A>) -> Result<(), TrySendError<A>> {
        self.tx.try_send(signal).map_err(|err| match err {
            mpsc::error::TrySendError::Full(undelivered) => TrySendError::Full(undelivered),
            mpsc::error::TrySendError::Closed(undelivered) => TrySendError::Closed(undelivered),
        })
    }

    /// Downgrades to a [`WeakMailboxSender`] that does not keep the mailbox open.
    #[must_use]
    pub fn downgrade(&self) -> WeakMailboxSender<A> {
        WeakMailboxSender {
            weak: self.tx.downgrade(),
        }
    }
}

impl<A: Mailboxed> MailboxReceiver<A> {
    /// Receives the next signal, waiting until one is available.
    ///
    /// Returns `None` once the mailbox is closed and drained.
    pub async fn recv(&mut self) -> Option<Signal<A>> {
        self.rx.recv().await
    }

    /// Closes the mailbox: senders can no longer enqueue, but signals already
    /// queued still drain through [`recv`](Self::recv) before it yields `None`.
    ///
    /// Used for a graceful stop — the run-loop finishes in-flight work rather
    /// than dropping it.
    pub fn close(&mut self) {
        self.rx.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{num::NonZeroUsize, sync::Arc};

    use proptest::prelude::*;
    use tokio::{runtime::Builder, sync::Barrier};

    /// Scaffold actor for the mailbox tests. `Mailboxed` is the seam the
    /// not-yet-rebuilt `Actor` trait (#114/#116) will later subsume.
    struct Probe;
    impl Mailboxed for Probe {
        type Msg = u64;
    }

    /// A message tagged with `(sender_id, seq)`; also proves `Msg` is any
    /// concrete type, not just a primitive.
    struct Tagged;
    impl Mailboxed for Tagged {
        type Msg = (u32, u32);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n_senders_one_receiver_preserve_per_sender_order() {
        const SENDERS: u32 = 8;
        const PER_SENDER: u32 = 64;

        // Small capacity so senders genuinely contend and backpressure.
        let (tx, mut rx) = bounded::<Tagged>(cap(4));
        let start = Arc::new(Barrier::new(SENDERS as usize));

        let mut handles = Vec::with_capacity(SENDERS as usize);
        for sender_id in 0..SENDERS {
            let tx = tx.clone();
            let start = Arc::clone(&start);
            handles.push(tokio::spawn(async move {
                start.wait().await; // all senders race from the same instant
                for seq in 0..PER_SENDER {
                    tx.send(Signal::Message((sender_id, seq)))
                        .await
                        .expect("send");
                }
            }));
        }
        drop(tx); // recv ends only once every sender has dropped its clone

        let mut next_expected = vec![0u32; SENDERS as usize];
        let mut total = 0u32;
        while let Some(signal) = rx.recv().await {
            let Signal::Message((sender_id, seq)) = signal else {
                panic!("unexpected non-message signal");
            };
            let slot = &mut next_expected[sender_id as usize];
            assert_eq!(seq, *slot, "FIFO-per-sender violated for sender {sender_id}");
            *slot += 1;
            total += 1;
        }

        assert_eq!(total, SENDERS * PER_SENDER, "lost or duplicated messages");
        for (sender_id, &count) in next_expected.iter().enumerate() {
            assert_eq!(count, PER_SENDER, "sender {sender_id} did not fully arrive");
        }
        for handle in handles {
            handle.await.expect("sender task panicked");
        }
    }

    /// Builds a valid [`Capacity`] for tests; panics on out-of-range input,
    /// which in a test is a programmer error in the test itself.
    fn cap(n: usize) -> Capacity {
        Capacity::new(NonZeroUsize::new(n).expect("test capacity must be nonzero"))
            .expect("test capacity must be within Capacity::MAX")
    }

    #[tokio::test]
    async fn sent_message_is_received() {
        let (tx, mut rx) = bounded::<Probe>(cap(4));

        tx.send(Signal::Message(42)).await.expect("send should succeed");

        assert!(matches!(rx.recv().await, Some(Signal::Message(42))));
    }

    #[tokio::test]
    async fn capacity_at_the_upper_boundary_is_usable() {
        // A mailbox built at tokio's true maximum must not panic and must work.
        // Guards Capacity::MAX against tokio ever lowering its semaphore limit.
        let (tx, mut rx) = bounded::<Probe>(cap(Capacity::MAX));

        tx.try_send(Signal::Message(7)).expect("send into max-capacity mailbox");

        assert!(matches!(rx.recv().await, Some(Signal::Message(7))));
    }

    #[test]
    fn capacity_rejects_values_above_tokio_max() {
        assert!(Capacity::new(NonZeroUsize::MIN).is_some());
        assert!(Capacity::new(NonZeroUsize::new(Capacity::MAX).expect("nonzero")).is_some());

        let too_big = NonZeroUsize::new(Capacity::MAX.checked_add(1).expect("no overflow")).expect("nonzero");
        assert!(Capacity::new(too_big).is_none());
        // Capacity zero is unrepresentable: NonZeroUsize cannot hold it.
    }

    #[test]
    fn link_died_variant_is_boxed_so_message_slots_stay_small() {
        use std::mem::size_of;

        // The cold LinkDied variant is boxed, so a small-message actor's queue
        // slot is bounded by the hot Message(u64) path — not inflated by
        // LinkDied's fields (id + a String-bearing reason). Guards the
        // "every slot = largest variant" trap; clippy::large_enum_variant is the
        // compile-time backstop via the workspace bar.
        assert!(
            size_of::<Signal<Probe>>() <= 2 * size_of::<u64>(),
            "Signal<Probe> slot is {} bytes; LinkDied is not boxed",
            size_of::<Signal<Probe>>()
        );
    }

    #[tokio::test]
    async fn link_died_signal_round_trips() {
        let (tx, mut rx) = bounded::<Probe>(cap(2));

        tx.send(Signal::LinkDied(Box::new(LinkDied {
            id: ActorId::new(7),
            reason: StopReason::Normal,
        })))
        .await
        .expect("send link-died");

        let Some(Signal::LinkDied(link_died)) = rx.recv().await else {
            panic!("expected a LinkDied signal");
        };
        assert_eq!(link_died.id, ActorId::new(7));
        assert!(matches!(link_died.reason, StopReason::Normal));
    }

    #[tokio::test]
    async fn weak_sender_tracks_the_last_strong_sender() {
        let (tx, _rx) = bounded::<Probe>(cap(2));
        let tx2 = tx.clone();
        let weak = tx.downgrade();

        drop(tx);
        assert!(
            weak.upgrade().is_some(),
            "one strong sender remains -> still alive"
        );

        drop(tx2);
        assert!(
            weak.upgrade().is_none(),
            "all strong senders gone -> non-pinning weak handle reports dead"
        );
    }

    #[tokio::test]
    async fn upgraded_weak_sender_can_send() {
        let (tx, mut rx) = bounded::<Probe>(cap(2));
        let weak = tx.downgrade();

        let strong = weak.upgrade().expect("channel still alive");
        strong.send(Signal::Message(5)).await.expect("send via upgraded");

        assert!(matches!(rx.recv().await, Some(Signal::Message(5))));
    }

    #[tokio::test]
    async fn stop_signal_is_delivered_in_order_after_a_message() {
        let (tx, mut rx) = bounded::<Probe>(cap(4));

        tx.send(Signal::Message(1)).await.expect("message");
        tx.send(Signal::Stop).await.expect("stop");

        // FIFO: the domain message precedes the control signal that followed it.
        assert!(matches!(rx.recv().await, Some(Signal::Message(1))));
        assert!(matches!(rx.recv().await, Some(Signal::Stop)));
    }

    #[tokio::test]
    async fn closing_the_receiver_stops_new_sends_but_drains_queued() {
        let (tx, mut rx) = bounded::<Probe>(cap(4));
        tx.send(Signal::Message(1)).await.expect("queued before close");

        rx.close();

        // New sends are rejected (the message comes back)...
        assert!(matches!(
            tx.send(Signal::Message(2)).await,
            Err(SendError(Signal::Message(2)))
        ));
        // ...but messages queued before the close still drain, then None.
        assert!(matches!(rx.recv().await, Some(Signal::Message(1))));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn send_after_receiver_dropped_returns_the_message() {
        let (tx, rx) = bounded::<Probe>(cap(4));
        drop(rx);

        assert!(matches!(
            tx.send(Signal::Message(9)).await,
            Err(SendError(Signal::Message(9)))
        ));
        assert!(matches!(
            tx.try_send(Signal::Message(9)),
            Err(TrySendError::Closed(Signal::Message(9)))
        ));
    }

    #[tokio::test]
    async fn recv_returns_none_after_all_senders_dropped_and_drained() {
        let (tx, mut rx) = bounded::<Probe>(cap(4));
        tx.send(Signal::Message(1)).await.expect("queued");
        drop(tx);

        // Queued message drains first, then the closed-and-empty channel ends.
        assert!(matches!(rx.recv().await, Some(Signal::Message(1))));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn full_mailbox_rejects_try_send_and_returns_the_message() {
        let (tx, mut rx) = bounded::<Probe>(cap(1));

        tx.try_send(Signal::Message(1)).expect("first signal fits");

        // Mailbox is now full: try_send must reject and hand the message back.
        let rejected = tx.try_send(Signal::Message(2));
        assert!(matches!(rejected, Err(TrySendError::Full(Signal::Message(2)))));

        // Draining one slot frees capacity for the next try_send.
        assert!(matches!(rx.recv().await, Some(Signal::Message(1))));
        tx.try_send(Signal::Message(3)).expect("fits after drain");
    }

    proptest! {
        /// `Capacity::new` accepts a value iff it is within `MAX`, and preserves
        /// it. The strategy pins the interesting boundaries: `1`, `MAX-1`, `MAX`,
        /// `MAX+1`, `usize::MAX`.
        #[test]
        fn prop_capacity_accepts_iff_within_max(
            n in prop_oneof![
                1usize..=4096,
                Just(Capacity::MAX - 1),
                Just(Capacity::MAX),
                Just(Capacity::MAX + 1),
                Just(usize::MAX),
            ],
        ) {
            let value = NonZeroUsize::new(n).expect("strategy yields n >= 1");
            let capacity = Capacity::new(value);

            prop_assert_eq!(capacity.is_some(), n <= Capacity::MAX);
            if let Some(capacity) = capacity {
                prop_assert_eq!(capacity.get(), n);
            }
        }

        /// A single sender's messages come out in the exact order they went in,
        /// for any message sequence and any capacity — the queue neither drops,
        /// duplicates, nor reorders (FIFO).
        #[test]
        fn prop_fifo_roundtrip_single_sender(
            messages in prop::collection::vec(any::<u64>(), 0..200),
            capacity in 1usize..=64,
        ) {
            let sent = messages.clone();
            let received = Builder::new_current_thread()
                .build()
                .expect("current-thread runtime")
                .block_on(async move {
                    let (tx, mut rx) = bounded::<Probe>(cap(capacity));
                    let expected = messages.len();
                    let producer = tokio::spawn(async move {
                        for message in messages {
                            tx.send(Signal::Message(message)).await.expect("send");
                        }
                    });

                    let mut got = Vec::with_capacity(expected);
                    while got.len() < expected {
                        let Some(Signal::Message(message)) = rx.recv().await else {
                            break;
                        };
                        got.push(message);
                    }
                    producer.await.expect("producer task");
                    got
                });

            prop_assert_eq!(received, sent);
        }
    }
}
