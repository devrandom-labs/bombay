//! The actor's in-memory mailbox: a bounded MPSC queue of [`Signal`]s.
//!
//! The local tier of the two-tier message model (#66): typed, in-memory,
//! **zero-serialize**. `tell` moves an `A::Msg` into a queue slot — no
//! per-message heap box.
//!
//! Construction hangs off the [`Mailbox`] namespace: `Mailbox::<A>::bounded(cap, id)`.
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

use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use flume::r#async::SendFut;

use crate::{
    error::ActorStopReason,
    watch::{LinkDied, WatchReg},
};

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
/// key-expr `ActorId`; it exists here so the [`crate::watch`] death-notice types
/// have a concrete shape to carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorId(u64);

impl ActorId {
    /// Wraps a raw identifier.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
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
    /// A watch registration: enqueue a watcher onto this actor's watcher set so
    /// it is notified when this actor stops. Boxed — a cold control path; inlining
    /// `WatchReg` (which holds a `flume::Sender`) would inflate every message slot.
    Watch(Box<WatchReg>),
    /// Deregister a watcher by id (the `unwatch` path).
    Unwatch(ActorId),
}

/// The construction namespace for an actor's mailbox.
///
/// Never instantiated — it exists so construction reads as
/// `Mailbox::<A>::bounded(cap, id)`, keeping the sender/receiver/weak types cohesive
/// under one entry point instead of a free-floating function.
pub struct Mailbox<A: Mailboxed>(PhantomData<fn() -> A>);

impl<A: Mailboxed> Mailbox<A> {
    /// Creates a bounded mailbox with room for `capacity` queued signals, owned
    /// by the actor identified as `id`.
    ///
    /// The receiver carries `id` because its `Drop` is the actor's true death
    /// edge: a [`Signal::Watch`] still queued when the receiver goes away must
    /// be answered with a death notice naming this actor (see
    /// [`MailboxReceiver`]'s `Drop`), and only the receiver ever sees that
    /// backlog on a hard kill.
    ///
    /// Infallible by construction — [`Capacity`] has already excluded the values
    /// the backing channel would reject.
    #[must_use]
    pub fn bounded(capacity: Capacity, id: ActorId) -> (MailboxSender<A>, MailboxReceiver<A>) {
        let (tx, rx) = flume::bounded(capacity.get());
        (MailboxSender { tx }, MailboxReceiver { rx, me: id })
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
    /// Returns the *named* [`SendMessageFut`] rather than an opaque future so
    /// the #118 request builders can embed it without boxing (`IntoFuture`
    /// needs a nameable associated type on stable).
    ///
    /// # Errors
    ///
    /// The future resolves to `Err(msg)` — the exact undelivered message back —
    /// if the mailbox has closed (the actor has stopped).
    pub fn send_message(&self, msg: A::Msg) -> SendMessageFut<'_, A> {
        SendMessageFut {
            inner: self.tx.send_async(Signal::Message {
                msg,
                self_sender: self.clone(),
            }),
        }
    }

    /// Non-blocking sibling of [`send_message`](Self::send_message): enqueues a
    /// domain `msg` (embedding a strong `self_sender`, ADR-0003) without waiting.
    ///
    /// # Errors
    ///
    /// [`TrySendError::Full`] if the mailbox is at capacity (retryable
    /// backpressure) or [`TrySendError::Closed`] if the receiver has been dropped
    /// (terminal). Both carry the undelivered [`Signal`] back.
    pub fn try_send_message(&self, msg: A::Msg) -> Result<(), TrySendError<A>> {
        self.try_send(Signal::Message {
            msg,
            self_sender: self.clone(),
        })
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
    /// The owning actor's identity — stamped onto the death notice sent for any
    /// still-queued [`Signal::Watch`] when the backlog is rejected
    /// ([`reject_queued_watchers`](MailboxReceiver::reject_queued_watchers),
    /// which this receiver's `Drop` also routes through).
    me: ActorId,
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

    /// Drains the backlog, answering every still-queued [`Signal::Watch`] with a
    /// death notice carrying `reason`, and releasing the queued messages'
    /// `self_sender` cycle — the two duties documented on [`Drop`](Self::drop),
    /// which routes through here with the synthetic
    /// [`AlreadyDead`](ActorStopReason::AlreadyDead).
    ///
    /// Callers that *know* the true stop reason pre-empt that fallback by calling
    /// this first: the startup-failure path (card #196) answers with
    /// `Panicked(OnStart)`, because a supervisor treats `AlreadyDead` as
    /// restart-worthy and would crash-loop a child that can never start.
    ///
    /// `cleanup_failed` rides along for the same reason and must be as true as
    /// `reason` is: the graceful teardown passes the outcome its `Watchers` guard
    /// just reported, so a backlog answered here says exactly what the guard's own
    /// notices said. The two callers that cannot know it pass `false`, paired with
    /// a reason (`AlreadyDead`, `Panicked(OnStart)`) that already means no cleanup
    /// ran.
    // `&self` despite emptying the queue: flume's `Receiver::drain` is itself
    // `&self` (its state lives behind the shared `Chan` lock), and taking `&mut`
    // here would be a lie the borrow checker cannot cash — `Drop::drop` reborrows
    // it anyway, and exclusivity is already guaranteed structurally (the receiver
    // is the mailbox's single consumer, and `drain(&mut self)` next door hands out
    // a borrowing iterator that cannot overlap with this call).
    pub(crate) fn reject_queued_watchers(&self, reason: &ActorStopReason, cleanup_failed: bool) {
        for signal in self.rx.drain() {
            if let Signal::Watch(reg) = signal {
                let _ = reg.link_tx.try_send(LinkDied {
                    id: self.me,
                    reason: reason.clone(),
                    linked: reg.linked,
                    cleanup_failed,
                });
            }
        }
    }
}

impl<A: Mailboxed> Drop for MailboxReceiver<A> {
    /// Drops the receiver **and** its backlog — and answers every still-queued
    /// [`Signal::Watch`] with a synthetic death notice first.
    ///
    /// Two duties, one drain (both discharged by
    /// [`reject_queued_watchers`](Self::reject_queued_watchers)):
    ///
    /// 1. **Leak fix.** Each queued [`Signal::Message`] holds a strong
    ///    `self_sender` clone of this very mailbox (ADR-0003), so a non-empty
    ///    queue forms a cycle: `Shared → queue → Signal → Sender → Arc<Shared>`.
    ///    Unlike tokio's mpsc, flume's `Receiver::drop` does **not** purge its
    ///    queue, so on a hard kill (the run-loop future is dropped mid-backlog)
    ///    that cycle would leak. Draining releases the embedded senders.
    /// 2. **No missed death (card #195).** A queued `Signal::Watch` was accepted
    ///    by a successful `send` — the watcher believes it is watching. This
    ///    drop is the last code that ever sees the registration, so it must
    ///    deliver the notice: reason
    ///    [`AlreadyDead`](ActorStopReason::AlreadyDead), because the true stop
    ///    reason is unknowable *here* (Erlang's `noproc`), paired with
    ///    `cleanup_failed: false` — no cleanup outcome is observable from here
    ///    either, and "unknown" is what both fields then mean together. Every
    ///    path that DOES know pre-empts this one (card #196): startup failure
    ///    drains with `Panicked(OnStart)`, and the graceful teardown drains three
    ///    times — before `on_stop`, after it, and once more after the guard has
    ///    fired, that last one carrying the guard's true reason and outcome. What
    ///    reaches this drop is therefore a hard kill, or a registration accepted
    ///    in the final instants before the mailbox itself went away. The send is
    ///    non-blocking into the watcher's UNBOUNDED link channel and only fails
    ///    if the watcher itself is gone — a stale edge, correctly skipped.
    ///
    /// A queued `Signal::Unwatch` is unenforceable here (the watcher set is gone
    /// with the loop); as in Erlang, a `demonitor` racing the death may still be
    /// followed by a delivered notice.
    fn drop(&mut self) {
        self.reject_queued_watchers(&ActorStopReason::AlreadyDead, false);
    }
}

/// The in-flight future of a [`MailboxSender::send_message`]: waits for mailbox
/// capacity, then enqueues the message.
///
/// A named wrapper over the channel primitive's send future (the seam rule: the
/// primitive appears only inside this module's wrappers), so callers — the #118
/// request builders above all — can hold it in a struct field without a box.
/// Resolves to `Err(msg)` with the exact undelivered message if the mailbox
/// closed.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct SendMessageFut<'a, A: Mailboxed> {
    inner: SendFut<'a, Signal<A>>,
}

impl<A: Mailboxed> Future for SendMessageFut<'_, A> {
    type Output = Result<(), A::Msg>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `SendFut` is explicitly `Unpin` (flume `async.rs`), so this struct is
        // too and plain re-pinning is sound without projection.
        Pin::new(&mut self.get_mut().inner).poll(cx).map(|result| {
            result.map_err(|err| match err.into_inner() {
                // flume hands back the exact value we sent, which is `Message`.
                Signal::Message {
                    msg: undelivered, ..
                } => undelivered,
                Signal::Stop | Signal::Watch(_) | Signal::Unwatch(_) => {
                    unreachable!("send_message enqueues only Signal::Message")
                }
            })
        })
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

    use crate::test_support::terminate_bound;

    /// Scaffold actor for the mailbox tests. `Mailboxed` is the seam the
    /// not-yet-rebuilt `Actor` trait (#114/#116) will later subsume.
    struct Probe;
    impl Mailboxed for Probe {
        type Msg = u64;
    }

    #[test]
    fn signal_watch_and_unwatch_are_carried() {
        let (tx, _rx) = flume::unbounded::<LinkDied>();
        let reg = WatchReg {
            watcher: ActorId::new(9),
            link_tx: tx,
            linked: true,
        };
        // Compiles only if Signal carries Watch/Unwatch (this is the whole assertion).
        let _watch: Signal<Probe> = Signal::Watch(Box::new(reg));
        let _unwatch: Signal<Probe> = Signal::Unwatch(ActorId::new(9));
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

    /// Awaits a `recv` under the fail-fast bound (card #179).
    ///
    /// A regression that makes a queued message vanish leaves the receiver
    /// waiting forever, so an unbounded `rx.recv().await` HANGS instead of
    /// failing. `spawn.rs` has held this discipline since #148; `mailbox.rs` did
    /// not, which is why `send -> Ok(())`, `try_send -> Ok(())` and
    /// `recv -> None` reported as **TIMEOUT** rather than caught — cargo-mutants
    /// exits 3 on a timeout, so those alone kept the whole sweep red, and a
    /// timeout burns the full budget while reporting as neither caught nor
    /// missed.
    async fn recv_bounded<A: Mailboxed>(rx: &mut MailboxReceiver<A>) -> Option<Signal<A>> {
        tokio::time::timeout(terminate_bound(), rx.recv())
            .await
            .expect("recv must not hang: a queued message went missing")
    }

    /// Awaits a `send` under the same bound (card #179).
    ///
    /// Separate from [`recv_bounded`] because the two hang for different
    /// reasons: `Capacity::get -> 0` turns the queue into a **rendezvous**
    /// channel (and `-> 1` into a depth-1 one), where a send with no waiting
    /// receiver blocks forever — the send side, not the recv side.
    async fn send_bounded<A: Mailboxed>(
        tx: &MailboxSender<A>,
        signal: Signal<A>,
    ) -> Result<(), SendError<A>> {
        tokio::time::timeout(terminate_bound(), tx.send(signal))
            .await
            .expect("send must not hang: the queue never drained")
    }

    #[tokio::test]
    async fn sent_message_is_received() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::new(0));

        send_bounded(
            &tx,
            Signal::Message {
                msg: 42,
                self_sender: tx.clone(),
            },
        )
        .await
        .expect("send should succeed");

        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Signal::Message { msg: 42, .. })
        ));
    }

    #[tokio::test]
    async fn capacity_at_the_upper_boundary_is_usable() {
        // A mailbox built at the capacity ceiling must not panic and must work.
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(Capacity::MAX), ActorId::new(0));

        tx.try_send(Signal::Message {
            msg: 7,
            self_sender: tx.clone(),
        })
        .expect("send into max-capacity mailbox");

        assert!(matches!(
            recv_bounded(&mut rx).await,
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
        let (tx, _rx) = Mailbox::<Probe>::bounded(cap(2), ActorId::new(0));
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
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(2), ActorId::new(0));
        let weak = tx.downgrade();

        let strong = weak.upgrade().expect("channel still alive");
        send_bounded(
            &strong,
            Signal::Message {
                msg: 5,
                self_sender: tx.clone(),
            },
        )
        .await
        .expect("send via upgraded");

        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Signal::Message { msg: 5, .. })
        ));
    }

    #[tokio::test]
    async fn stop_signal_is_delivered_in_order_after_a_message() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::new(0));

        send_bounded(
            &tx,
            Signal::Message {
                msg: 1,
                self_sender: tx.clone(),
            },
        )
        .await
        .expect("message");
        send_bounded(&tx, Signal::Stop).await.expect("stop");

        // FIFO: the domain message precedes the control signal that followed it.
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Signal::Message { msg: 1, .. })
        ));
        assert!(matches!(recv_bounded(&mut rx).await, Some(Signal::Stop)));
    }

    #[tokio::test]
    async fn drain_flushes_queued_signals_in_order() {
        // Graceful-stop primitive: after a Stop, the run-loop flushes the rest
        // with `drain` before dropping the receiver.
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(8), ActorId::new(0));
        for i in 0..3 {
            send_bounded(
                &tx,
                Signal::Message {
                    msg: i,
                    self_sender: tx.clone(),
                },
            )
            .await
            .expect("queued");
        }

        assert!(matches!(
            recv_bounded(&mut rx).await,
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
        let (tx, rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::new(0));
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
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::new(0));
        send_bounded(
            &tx,
            Signal::Message {
                msg: 1,
                self_sender: tx.clone(),
            },
        )
        .await
        .expect("queued");
        drop(tx);

        // Queued message drains first, then the disconnected channel ends.
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Signal::Message { msg: 1, .. })
        ));
        assert!(recv_bounded(&mut rx).await.is_none());
    }

    /// `@bug` (card #195 review): a `Signal::Watch` still QUEUED when the
    /// receiver drops (hard kill mid-backlog, or the graceful window between the
    /// teardown drain and the receiver drop) was accepted by a successful `send`
    /// — silently discarding it is a missed death, the worst bug in the
    /// death-watch subsystem. The receiver's drop must instead deliver a
    /// synthetic [`LinkDied`](LinkDied) with the actor's own id,
    /// reason [`AlreadyDead`](ActorStopReason::AlreadyDead)
    /// (the true reason is unknowable here — Erlang's `noproc`), and the edge's
    /// `linked` flag preserved. FAILS while the drop drain `for_each(drop)`s the
    /// registration.
    #[tokio::test]
    async fn dropping_receiver_notifies_queued_watch_regs_already_dead() {
        let (tx, rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::new(77));

        let (link_tx, link_rx) = flume::unbounded::<LinkDied>();
        tx.try_send(Signal::Watch(Box::new(WatchReg {
            watcher: ActorId::new(1),
            link_tx,
            linked: true,
        })))
        .expect("reg enqueued into the open mailbox");

        drop(rx); // receiver gone with the reg still queued

        let notice = link_rx
            .try_recv()
            .expect("a queued watch reg must be notified, never silently dropped");
        assert_eq!(
            notice.id,
            ActorId::new(77),
            "the notice names the dead actor"
        );
        assert!(
            matches!(notice.reason, ActorStopReason::AlreadyDead),
            "true reason unknowable => AlreadyDead, got {:?}",
            notice.reason,
        );
        assert!(notice.linked, "the edge's linked flag rides the notice");
    }

    #[tokio::test]
    async fn dropping_receiver_mid_backlog_frees_the_queued_message() {
        // Each queued `Signal::Message` embeds a strong `self_sender` (ADR-0003),
        // forming a `Shared -> queue -> Signal -> Sender -> Arc<Shared>` cycle.
        // flume's `Receiver::drop` does NOT purge its queue, so without
        // `MailboxReceiver::drop` draining it, a hard kill (receiver dropped mid-
        // backlog) leaks the queued message and everything it owns.
        let (tx, rx) = Mailbox::<Canary>::bounded(cap(4), ActorId::new(0));

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
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(1), ActorId::new(0));

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
            recv_bounded(&mut rx).await,
            Some(Signal::Message { msg: 1, .. })
        ));
        tx.try_send(Signal::Message {
            msg: 3,
            self_sender: tx.clone(),
        })
        .expect("fits after drain");
    }

    #[tokio::test]
    async fn try_send_message_delivers_then_reports_full_then_closed() {
        // Delivers into an open mailbox, embedding a self_sender (ADR-0003).
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(1), ActorId::new(0));
        tx.try_send_message(1).expect("first fits");
        // Capacity 1 and full: the next try is backpressure, not delivery.
        assert!(matches!(
            tx.try_send_message(2),
            Err(TrySendError::Full(Signal::Message { msg: 2, .. }))
        ));
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Signal::Message { msg: 1, .. })
        ));

        // Receiver dropped: a try now reports the terminal Closed, not Full.
        let (closed_tx, closed_rx) = Mailbox::<Probe>::bounded(cap(1), ActorId::new(0));
        drop(closed_rx);
        assert!(matches!(
            closed_tx.try_send_message(9),
            Err(TrySendError::Closed(Signal::Message { msg: 9, .. }))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n_senders_one_receiver_preserve_per_sender_order() {
        const SENDERS: u32 = 8;
        const PER_SENDER: u32 = 64;

        // Small capacity so senders genuinely contend and backpressure.
        let (tx, mut rx) = Mailbox::<Tagged>::bounded(cap(4), ActorId::new(0));
        let start = Arc::new(Barrier::new(SENDERS as usize));

        let mut handles = Vec::with_capacity(SENDERS as usize);
        for sender_id in 0..SENDERS {
            let tx = tx.clone();
            let start = Arc::clone(&start);
            handles.push(tokio::spawn(async move {
                start.wait().await; // all senders race from the same instant
                for seq in 0..PER_SENDER {
                    send_bounded(
                        &tx,
                        Signal::Message {
                            msg: (sender_id, seq),
                            self_sender: tx.clone(),
                        },
                    )
                    .await
                    .expect("send");
                }
            }));
        }
        drop(tx); // recv ends only once every sender has dropped its clone

        let mut next_expected = vec![0u32; SENDERS as usize];
        let mut total = 0u32;
        while let Some(signal) = recv_bounded(&mut rx).await {
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
                .enable_time()
                .build()
                .expect("current-thread runtime")
                .block_on(async move {
                    let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(capacity), ActorId::new(0));
                    let expected = messages.len();
                    let producer = tokio::spawn(async move {
                        for message in messages {
                            send_bounded(&tx, Signal::Message { msg: message, self_sender: tx.clone() })
                                .await
                                .expect("send");
                        }
                    });

                    let mut got = Vec::with_capacity(expected);
                    while got.len() < expected {
                        let Some(Signal::Message { msg: message, .. }) = recv_bounded(&mut rx).await else {
                            break;
                        };
                        got.push(message);
                    }
                    // The consumer has taken all it will take. Stop the producer
                    // so a mutation that makes `recv` drop messages fails on the
                    // `received != sent` assertion below, instead of deadlocking
                    // the producer on a full, undrained queue (card #179). In the
                    // unmutated run every message was delivered and consumed, so
                    // the producer has already finished and this abort is a no-op.
                    producer.abort();
                    let _ = producer.await;
                    got
                });

            prop_assert_eq!(received, sent);
        }
    }
}
