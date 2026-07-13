//! The actor's in-memory mailbox: a bounded MPSC queue of [`Signal`]s.
//!
//! The local tier of the two-tier message model (#66): typed, in-memory,
//! **zero-serialize**. `tell` moves an `A::Msg` into a queue slot — no
//! per-message heap box.
//!
//! Construction hangs off the [`Mailbox`] namespace: `Mailbox::<A>::bounded(cap)`.
//! Bounded is the only mode — a full mailbox exerts backpressure rather than
//! growing without limit (an unbounded queue is a memory footgun).
//!
//! **The channel seam.** The queue is backed by `flume` (chosen on measured
//! evidence — see `docs/adr/0001`), but that is an implementation detail: `flume`
//! appears *only* inside [`MailboxSender`] / [`WeakMailboxSender`] /
//! [`MailboxReceiver`], never in the public API. Swapping the primitive (a
//! `no_std`/Embassy channel for M6, or a deterministic channel for the DST) means
//! reimplementing those three wrappers and nothing else. The seam is trait-ified
//! at the *second* impl, not pre-abstracted for one.
//!
//! **Shutdown** is not a channel concern: the mailbox is pure transport. A
//! graceful stop is the run-loop's job (#116) — finish the in-flight handler on
//! a [`Signal::Stop`], then drop the receiver (which disconnects every sender);
//! queued messages are abandoned, not drained. [`MailboxReceiver::drain`] exists
//! to release the strong `self_sender` each queued [`Signal::Message`] carries
//! (ADR-0003) when the receiver is dropped — see [`MailboxReceiver::drop`].

use std::{fmt, marker::PhantomData, num::NonZeroUsize};

/// A validated mailbox capacity: at least `1`, at most [`Capacity::MAX`].
///
/// Makes both illegal capacities unrepresentable, so [`Mailbox::bounded`] cannot
/// fail: zero is excluded by `NonZeroUsize`, and the upper bound is checked here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity(NonZeroUsize);

impl Capacity {
    /// The largest capacity the backing channel accepts. Kept comfortably within
    /// any candidate's limit; a mailbox this deep is already a design smell.
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

/// Why a `usize` could not be a [`Capacity`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    clippy::exhaustive_enums,
    reason = "a capacity is invalid for exactly these two reasons"
)]
pub enum CapacityError {
    /// The value was `0`; a mailbox needs room for at least one signal.
    Zero,
    /// The value exceeded [`Capacity::MAX`].
    TooLarge,
}

impl fmt::Display for CapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => f.write_str("mailbox capacity must be at least 1"),
            Self::TooLarge => f.write_str("mailbox capacity exceeds the maximum"),
        }
    }
}

impl std::error::Error for CapacityError {}

impl TryFrom<NonZeroUsize> for Capacity {
    type Error = CapacityError;

    fn try_from(value: NonZeroUsize) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(CapacityError::TooLarge)
    }
}

impl TryFrom<usize> for Capacity {
    type Error = CapacityError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let nonzero = NonZeroUsize::new(value).ok_or(CapacityError::Zero)?;
        Self::try_from(nonzero)
    }
}

/// The seam between a mailbox and its actor.
///
/// A mailbox is monomorphized per actor `A`, carrying that actor's single closed
/// message type `A::Msg` by value — no `Box<dyn>`. This scaffold trait is what
/// the rebuilt `Actor` trait (#114/#116) will later subsume.
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
/// keeps the hot [`Signal::Message`] slot small (large-variant discipline).
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
              new arms are added under their driving cards"
)]
pub enum Signal<A: Mailboxed> {
    /// A domain message for the actor to handle, carrying a **strong** clone of
    /// the sender that enqueued it (`self_sender`). That clone keeps the mailbox
    /// open while the message waits, so a queued message **pins the actor alive**
    /// until it is handled (ref-count-driven stop drains the backlog), and the
    /// run-loop lifts a strong self-[`ActorRef`](crate::actor::ActorRef) out of
    /// it without holding one itself (ADR-0003). Only the sender is embedded —
    /// it is the sole handle that gates liveness.
    Message {
        /// The domain message.
        msg: A::Msg,
        /// A strong clone of the enqueuing sender (the actor's own mailbox).
        self_sender: MailboxSender<A>,
    },
    /// Asks the actor to stop after draining messages queued before it.
    Stop,
    /// A linked actor has terminated. Boxed: this is a cold path, and inlining
    /// its fields would inflate every message slot (large-variant discipline).
    LinkDied(Box<LinkDied>),
}

/// The construction namespace for an actor's mailbox.
///
/// Never instantiated — it exists so construction reads as
/// `Mailbox::<A>::bounded(cap)`, keeping the sender/receiver/weak types cohesive
/// under one entry point instead of a free-floating function.
pub struct Mailbox<A: Mailboxed>(PhantomData<fn() -> A>);

impl<A: Mailboxed> Mailbox<A> {
    /// Creates a bounded mailbox with room for `capacity` queued signals.
    ///
    /// Infallible by construction — [`Capacity`] has already excluded the values
    /// the backing channel would reject.
    #[must_use]
    pub fn bounded(capacity: Capacity) -> (MailboxSender<A>, MailboxReceiver<A>) {
        let (tx, rx) = flume::bounded(capacity.get());
        (MailboxSender { tx }, MailboxReceiver { rx })
    }
}

/// Sends [`Signal`]s to an actor's mailbox. Cloneable; the channel stays open
/// while any sender is alive.
pub struct MailboxSender<A: Mailboxed> {
    tx: flume::Sender<Signal<A>>,
}

impl<A: Mailboxed> Clone for MailboxSender<A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<A: Mailboxed> MailboxSender<A> {
    /// Sends `signal`, waiting for capacity if the mailbox is full.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] (carrying `signal` back) if the receiver has been
    /// dropped, i.e. the actor is no longer running.
    pub async fn send(&self, signal: Signal<A>) -> Result<(), SendError<A>> {
        self.tx
            .send_async(signal)
            .await
            .map_err(|err| SendError(err.into_inner()))
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
            flume::TrySendError::Full(undelivered) => TrySendError::Full(undelivered),
            flume::TrySendError::Disconnected(undelivered) => TrySendError::Closed(undelivered),
        })
    }

    /// Enqueues a domain `msg`, embedding a **strong** clone of this sender as
    /// the message's `self_sender` (ADR-0003) so the queued message pins the
    /// actor alive until handled. Waits for capacity if the mailbox is full.
    ///
    /// # Errors
    ///
    /// Returns `msg` back if the mailbox has closed (the actor has stopped).
    pub async fn send_message(&self, msg: A::Msg) -> Result<(), A::Msg> {
        let signal = Signal::Message {
            msg,
            self_sender: self.clone(),
        };
        match self.tx.send_async(signal).await {
            Ok(()) => Ok(()),
            // flume hands back the exact value we sent, which is `Message`.
            Err(err) => match err.into_inner() {
                Signal::Message {
                    msg: undelivered, ..
                } => Err(undelivered),
                Signal::Stop | Signal::LinkDied(_) => {
                    unreachable!("send_message enqueues only Signal::Message")
                }
            },
        }
    }

    /// Whether the mailbox has closed — the receiver (the actor's run-loop) has
    /// been dropped, so no further signal can be delivered. A send-and-observe
    /// backup to push death-detection; **not** a pre-send liveness gate (that
    /// would be TOCTOU-wrong — a send races the close either way).
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_disconnected()
    }

    /// Downgrades to a [`WeakMailboxSender`] that does not keep the mailbox open.
    #[must_use]
    pub fn downgrade(&self) -> WeakMailboxSender<A> {
        WeakMailboxSender {
            weak: self.tx.downgrade(),
        }
    }
}

/// A non-pinning handle to a mailbox: holding one does **not** keep it alive.
///
/// [`upgrade`](Self::upgrade) yields a strong sender only while a real
/// [`MailboxSender`] still exists — the primitive death-watch is built on.
pub struct WeakMailboxSender<A: Mailboxed> {
    weak: flume::WeakSender<Signal<A>>,
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
    rx: flume::Receiver<Signal<A>>,
}

impl<A: Mailboxed> MailboxReceiver<A> {
    /// Receives the next signal, waiting until one is available.
    ///
    /// Returns `None` once every sender has dropped and the queue is drained.
    pub async fn recv(&mut self) -> Option<Signal<A>> {
        self.rx.recv_async().await.ok()
    }

    /// Drains every currently-queued signal without waiting, in FIFO order.
    ///
    /// A queued [`Signal::Message`] holds a strong `self_sender` (ADR-0003), so
    /// draining is what releases those senders and breaks the self-referential
    /// cycle between the channel and its backlog — see [`Drop`](Self::drop).
    pub fn drain(&mut self) -> impl Iterator<Item = Signal<A>> + '_ {
        self.rx.drain()
    }
}

impl<A: Mailboxed> Drop for MailboxReceiver<A> {
    /// Drops the receiver **and** its backlog. Each queued [`Signal::Message`]
    /// holds a strong `self_sender` clone of this very mailbox (ADR-0003), so a
    /// non-empty queue forms a cycle: `Shared → queue → Signal → Sender →
    /// Arc<Shared>`. Unlike tokio's mpsc, flume's `Receiver::drop` does **not**
    /// purge its queue, so on a hard kill (the run-loop future is dropped mid-
    /// backlog) that cycle would leak. Draining here releases the embedded
    /// senders and lets the channel free.
    fn drop(&mut self) {
        self.rx.drain().for_each(drop);
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

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

    /// A message that owns an `Arc` canary, so a test can observe — via a
    /// `Weak` that upgrades iff the payload is still alive — whether a queued
    /// message is actually freed when the receiver is dropped mid-backlog.
    struct Canary;
    impl Mailboxed for Canary {
        type Msg = Arc<()>;
    }

    /// Builds a valid [`Capacity`] for tests; panics on out-of-range input,
    /// which in a test is a programmer error in the test itself.
    fn cap(n: usize) -> Capacity {
        Capacity::try_from(n).expect("test capacity must be valid")
    }

    #[tokio::test]
    async fn sent_message_is_received() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4));

        tx.send(Signal::Message {
            msg: 42,
            self_sender: tx.clone(),
        })
        .await
        .expect("send should succeed");

        assert!(matches!(
            rx.recv().await,
            Some(Signal::Message { msg: 42, .. })
        ));
    }

    #[tokio::test]
    async fn capacity_at_the_upper_boundary_is_usable() {
        // A mailbox built at the capacity ceiling must not panic and must work.
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(Capacity::MAX));

        tx.try_send(Signal::Message {
            msg: 7,
            self_sender: tx.clone(),
        })
        .expect("send into max-capacity mailbox");

        assert!(matches!(
            rx.recv().await,
            Some(Signal::Message { msg: 7, .. })
        ));
    }

    #[test]
    fn capacity_rejects_zero_and_values_above_max() {
        assert_eq!(Capacity::try_from(0usize), Err(CapacityError::Zero));
        assert!(Capacity::try_from(1usize).is_ok());
        assert!(Capacity::try_from(Capacity::MAX).is_ok());
        assert_eq!(
            Capacity::try_from(Capacity::MAX.checked_add(1).expect("no overflow")),
            Err(CapacityError::TooLarge)
        );
    }

    #[test]
    fn capacity_max_is_the_documented_ceiling() {
        // A behavioural boundary test can't catch a wrong MAX here: flume grows
        // lazily and won't panic on a huge bound (unlike tokio's mpsc), so pin
        // the ceiling constant directly.
        assert_eq!(Capacity::MAX, usize::MAX >> 3);
    }

    #[test]
    fn capacity_error_display_is_stable() {
        assert_eq!(
            CapacityError::Zero.to_string(),
            "mailbox capacity must be at least 1"
        );
        assert_eq!(
            CapacityError::TooLarge.to_string(),
            "mailbox capacity exceeds the maximum"
        );
    }

    #[test]
    fn link_died_variant_is_boxed_so_message_slots_stay_small() {
        use std::mem::size_of;

        // The cold LinkDied variant is boxed, so a small-message actor's queue
        // slot is bounded by the hot Message path: `msg` + the embedded
        // `self_sender` (one Arc pointer, ADR-0003) + a discriminant word. If
        // LinkDied were inlined, its fat StopReason (a `String`) would blow this
        // bound. Guards the "every slot = largest variant" trap.
        let hot_bound = size_of::<u64>() + size_of::<MailboxSender<Probe>>() + size_of::<usize>();
        assert!(
            size_of::<Signal<Probe>>() <= hot_bound,
            "Signal<Probe> slot is {} bytes (hot bound {hot_bound}); LinkDied is not boxed",
            size_of::<Signal<Probe>>()
        );
    }

    /// Demonstration (measured, not derived) of the **worst case** of the
    /// monomorphic, by-value `Signal<A>`: every queue slot costs `size_of` of the
    /// actor's *largest* `Msg` variant. One fat command variant therefore taxes
    /// every slot — even tiny messages — unless the user boxes it (the same
    /// discipline `LinkDied` uses).
    ///
    /// Measured (aarch64): `small = 16 B`, `fat inline = 4104 B`, `boxed = 16 B`
    /// → for 1_000 queued messages, **4.10 MB vs 16 KB (256×)**. See #122.
    #[test]
    #[expect(
        dead_code,
        reason = "the Msg variants exist only to measure enum layout via size_of"
    )]
    fn monomorphic_slot_cost_is_the_largest_msg_variant() {
        use std::mem::size_of;

        enum SmallMsg {
            Ping,
            Pong(u64),
        }
        struct Small;
        impl Mailboxed for Small {
            type Msg = SmallMsg;
        }

        // One fat command variant, stored inline (the footgun).
        enum FatMsg {
            Ping,
            Bulk([u8; 4096]),
        }
        struct Fat;
        impl Mailboxed for Fat {
            type Msg = FatMsg;
        }

        // The mitigation: box the fat variant, as `Signal` boxes `LinkDied`.
        enum BoxedFatMsg {
            Ping,
            Bulk(Box<[u8; 4096]>),
        }
        struct BoxedFat;
        impl Mailboxed for BoxedFat {
            type Msg = BoxedFatMsg;
        }

        let small = size_of::<Signal<Small>>();
        let fat = size_of::<Signal<Fat>>();
        let boxed = size_of::<Signal<BoxedFat>>();

        assert!(small <= 24, "small slot = {small}");
        assert!(fat >= 4096, "fat inline slot = {fat}");
        assert!(boxed <= 24, "boxed slot = {boxed}");

        let queued = 1_000;
        let (fat_total, boxed_total) = (fat * queued, boxed * queued);
        assert!(
            fat_total > 100 * boxed_total,
            "expected >100x blowup, got fat={fat_total} boxed={boxed_total}"
        );
    }

    #[test]
    fn error_debug_formats_are_stable() {
        // The Debug impls are hand-written (so the error types don't inherit an
        // `A::Msg: Debug` bound); pin their output so a regression is caught.
        let send_err: SendError<Probe> = SendError(Signal::Stop);
        assert_eq!(format!("{send_err:?}"), "SendError(receiver dropped)");

        let full: TrySendError<Probe> = TrySendError::Full(Signal::Stop);
        assert_eq!(
            format!("{full:?}"),
            "TrySendError::Full(mailbox at capacity)"
        );

        let closed: TrySendError<Probe> = TrySendError::Closed(Signal::Stop);
        assert_eq!(
            format!("{closed:?}"),
            "TrySendError::Closed(receiver dropped)"
        );
    }

    #[tokio::test]
    async fn weak_sender_tracks_the_last_strong_sender() {
        let (tx, _rx) = Mailbox::<Probe>::bounded(cap(2));
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
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(2));
        let weak = tx.downgrade();

        let strong = weak.upgrade().expect("channel still alive");
        strong
            .send(Signal::Message {
                msg: 5,
                self_sender: tx.clone(),
            })
            .await
            .expect("send via upgraded");

        assert!(matches!(
            rx.recv().await,
            Some(Signal::Message { msg: 5, .. })
        ));
    }

    #[tokio::test]
    async fn stop_signal_is_delivered_in_order_after_a_message() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4));

        tx.send(Signal::Message {
            msg: 1,
            self_sender: tx.clone(),
        })
        .await
        .expect("message");
        tx.send(Signal::Stop).await.expect("stop");

        // FIFO: the domain message precedes the control signal that followed it.
        assert!(matches!(
            rx.recv().await,
            Some(Signal::Message { msg: 1, .. })
        ));
        assert!(matches!(rx.recv().await, Some(Signal::Stop)));
    }

    #[tokio::test]
    async fn drain_flushes_queued_signals_in_order() {
        // Graceful-stop primitive: after a Stop, the run-loop flushes the rest
        // with `drain` before dropping the receiver.
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(8));
        for i in 0..3 {
            tx.send(Signal::Message {
                msg: i,
                self_sender: tx.clone(),
            })
            .await
            .expect("queued");
        }

        assert!(matches!(
            rx.recv().await,
            Some(Signal::Message { msg: 0, .. })
        ));

        let flushed: Vec<u64> = rx
            .drain()
            .map(|signal| match signal {
                Signal::Message { msg: m, .. } => m,
                _ => panic!("unexpected signal"),
            })
            .collect();
        assert_eq!(flushed, vec![1, 2]);
    }

    #[tokio::test]
    async fn send_after_receiver_dropped_returns_the_message() {
        let (tx, rx) = Mailbox::<Probe>::bounded(cap(4));
        drop(rx);

        assert!(matches!(
            tx.send(Signal::Message {
                msg: 9,
                self_sender: tx.clone()
            })
            .await,
            Err(SendError(Signal::Message { msg: 9, .. }))
        ));
        assert!(matches!(
            tx.try_send(Signal::Message {
                msg: 9,
                self_sender: tx.clone()
            }),
            Err(TrySendError::Closed(Signal::Message { msg: 9, .. }))
        ));
    }

    #[tokio::test]
    async fn recv_returns_none_after_all_senders_dropped_and_drained() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4));
        tx.send(Signal::Message {
            msg: 1,
            self_sender: tx.clone(),
        })
        .await
        .expect("queued");
        drop(tx);

        // Queued message drains first, then the disconnected channel ends.
        assert!(matches!(
            rx.recv().await,
            Some(Signal::Message { msg: 1, .. })
        ));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn dropping_receiver_mid_backlog_frees_the_queued_message() {
        // Each queued `Signal::Message` embeds a strong `self_sender` (ADR-0003),
        // forming a `Shared -> queue -> Signal -> Sender -> Arc<Shared>` cycle.
        // flume's `Receiver::drop` does NOT purge its queue, so without
        // `MailboxReceiver::drop` draining it, a hard kill (receiver dropped mid-
        // backlog) leaks the queued message and everything it owns.
        let (tx, rx) = Mailbox::<Canary>::bounded(cap(4));

        let canary = Arc::new(());
        let observer = Arc::downgrade(&canary);

        // Move the sole strong payload ref into the queued signal.
        tx.try_send(Signal::Message {
            msg: canary,
            self_sender: tx.clone(),
        })
        .expect("enqueue into an open mailbox");

        // Hard kill: both handles gone while the message is still queued, never
        // received. Drop the receiver last so its `drop` sees the backlog.
        drop(tx);
        drop(rx);

        // The drain-on-drop released the queued signal, so its payload is freed.
        // Delete `impl Drop for MailboxReceiver` and this upgrades to `Some`.
        assert!(
            observer.upgrade().is_none(),
            "queued message leaked: MailboxReceiver::drop did not drain the backlog",
        );
    }

    #[tokio::test]
    async fn full_mailbox_rejects_try_send_and_returns_the_message() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(1));

        tx.try_send(Signal::Message {
            msg: 1,
            self_sender: tx.clone(),
        })
        .expect("first signal fits");

        // Mailbox is now full: try_send must reject and hand the message back.
        let rejected = tx.try_send(Signal::Message {
            msg: 2,
            self_sender: tx.clone(),
        });
        assert!(matches!(
            rejected,
            Err(TrySendError::Full(Signal::Message { msg: 2, .. }))
        ));

        // Draining one slot frees capacity for the next try_send.
        assert!(matches!(
            rx.recv().await,
            Some(Signal::Message { msg: 1, .. })
        ));
        tx.try_send(Signal::Message {
            msg: 3,
            self_sender: tx.clone(),
        })
        .expect("fits after drain");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n_senders_one_receiver_preserve_per_sender_order() {
        const SENDERS: u32 = 8;
        const PER_SENDER: u32 = 64;

        // Small capacity so senders genuinely contend and backpressure.
        let (tx, mut rx) = Mailbox::<Tagged>::bounded(cap(4));
        let start = Arc::new(Barrier::new(SENDERS as usize));

        let mut handles = Vec::with_capacity(SENDERS as usize);
        for sender_id in 0..SENDERS {
            let tx = tx.clone();
            let start = Arc::clone(&start);
            handles.push(tokio::spawn(async move {
                start.wait().await; // all senders race from the same instant
                for seq in 0..PER_SENDER {
                    tx.send(Signal::Message {
                        msg: (sender_id, seq),
                        self_sender: tx.clone(),
                    })
                    .await
                    .expect("send");
                }
            }));
        }
        drop(tx); // recv ends only once every sender has dropped its clone

        let mut next_expected = vec![0u32; SENDERS as usize];
        let mut total = 0u32;
        while let Some(signal) = rx.recv().await {
            let Signal::Message {
                msg: (sender_id, seq),
                ..
            } = signal
            else {
                panic!("unexpected non-message signal");
            };
            let slot = &mut next_expected[sender_id as usize];
            assert_eq!(
                seq, *slot,
                "FIFO-per-sender violated for sender {sender_id}"
            );
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

    proptest! {
        /// `Capacity::try_from` accepts a value iff it is in `1..=MAX`, and
        /// preserves it. The strategy pins the boundaries: `0`, `1`, `MAX-1`,
        /// `MAX`, `MAX+1`, `usize::MAX`.
        #[test]
        fn prop_capacity_accepts_iff_in_range(
            n in prop_oneof![
                Just(0usize),
                1usize..=4096,
                Just(Capacity::MAX - 1),
                Just(Capacity::MAX),
                Just(Capacity::MAX + 1),
                Just(usize::MAX),
            ],
        ) {
            let capacity = Capacity::try_from(n);
            prop_assert_eq!(capacity.is_ok(), (1..=Capacity::MAX).contains(&n));
            if let Ok(capacity) = capacity {
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
                    let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(capacity));
                    let expected = messages.len();
                    let producer = tokio::spawn(async move {
                        for message in messages {
                            tx.send(Signal::Message { msg: message, self_sender: tx.clone() }).await.expect("send");
                        }
                    });

                    let mut got = Vec::with_capacity(expected);
                    while got.len() < expected {
                        let Some(Signal::Message { msg: message, .. }) = rx.recv().await else {
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
