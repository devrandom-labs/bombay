//! The actor's in-memory mailbox: a bounded MPSC queue of [`Signal`]s plus,
//! since #225, a second UNBOUNDED lane of [`ControlSignal`]s (ADR-0021).
//!
//! The local tier of the two-tier message model (#66): typed, in-memory,
//! **zero-serialize**. `tell` moves an `A::Msg` into a queue slot — no
//! per-message heap box.
//!
//! Construction hangs off the [`Mailbox`] namespace: `Mailbox::<A>::bounded(cap, id)`.
//! The USER lane is bounded — a full mailbox exerts backpressure rather than
//! growing without limit (an unbounded queue is a memory footgun). The CONTROL
//! lane (watch registration/removal, supervision ops) is deliberately unbounded
//! and merged ahead of the user backlog inside [`MailboxReceiver::recv`], so
//! runtime supervision never queues behind user messages (Erlang/OTP 28's
//! EEP-76; #218 wart 3). Its bound is the caller's own call rate, the same
//! trust class as the unbounded link channel.
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
    actor::SupervisionOp,
    error::ActorStopReason,
    trace::{self, SendContext},
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

/// Re-export: `ActorId` lives in [`crate::id`] (#206); this path stays for the
/// mailbox-composition surface ([`Mailbox::bounded`] takes it).
pub use crate::id::ActorId;

/// A signal on an actor mailbox's **user lane**: a domain message, or the
/// in-band graceful-stop marker.
///
/// A **concrete, closed** envelope — no `Box<dyn>` at either layer. `tell` moves
/// an `A::Msg` into a [`Signal::Message`] slot, so a send is zero-allocation.
/// Watch/supervision control signals do NOT live here: since #225 they ride the
/// separate unbounded control lane ([`ControlSignal`]), merged ahead of this
/// backlog inside [`MailboxReceiver::recv`] (ADR-0021).
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
        /// Caller-side trace context captured at enqueue (card #209): the
        /// sender's current span, so the handler's span parents to it. A ZST
        /// when the `tracing` feature is off.
        ctx: SendContext,
    },
    /// Asks the actor to stop after draining messages queued before it. Stays
    /// on the USER lane deliberately (ADR-0021): a control signal may overtake
    /// the backlog, but a `Stop` must not — "handle everything I already sent,
    /// then stop" is the whole contract of an in-band stop.
    Stop,
}

/// A runtime control signal on the mailbox's **control lane** (card #225,
/// ADR-0021): watch registration, watch removal, and supervision ops.
///
/// Control signals ride their own UNBOUNDED channel and are merged ahead of the
/// user backlog inside [`MailboxReceiver::recv`], so a watch or a supervision op
/// reaches a full-mailbox actor without queueing behind user messages. The lane
/// is FIFO *within itself* — a watch-then-unwatch pair applies in order — while
/// a control signal enqueued after a user message may overtake it.
///
/// Non-generic by construction: no payload mentions the actor's `Msg`, so every
/// actor's control lane carries this one concrete type.
#[expect(
    clippy::exhaustive_enums,
    reason = "the control-signal set is deliberately closed so the run-loop is a total match; \
              new arms are added under their driving cards"
)]
pub enum ControlSignal {
    /// A watch registration: enqueue a watcher onto this actor's watcher set so
    /// it is notified when this actor stops. Boxed — a cold control path; inlining
    /// `WatchReg` (which holds a `flume::Sender`) would inflate every control
    /// slot.
    Watch(Box<WatchReg>),
    /// Deregister a watcher by id (the `unwatch` path).
    Unwatch(ActorId),
    /// A supervision-table operation for the supervisor's loop (card #196).
    /// Boxed for the same reason [`Watch`](Self::Watch) is: the op embeds a
    /// restart config plus an erased rebuild factory, and every control slot
    /// costs the largest variant.
    Supervision(Box<SupervisionOp>),
}

/// One delivery from [`MailboxReceiver::recv`]: which lane the item came in on.
///
/// The control lane is merged ahead of the user lane (control-first biased,
/// ADR-0021); consumers match the two kinds without learning the policy.
#[expect(
    clippy::exhaustive_enums,
    reason = "a recv yields exactly one of the two lanes; the set is closed"
)]
pub enum Recv<A: Mailboxed> {
    /// A control-lane signal (watch/supervision), served ahead of the user
    /// backlog.
    Control(ControlSignal),
    /// A user-lane signal (a domain message or the in-band stop marker).
    Signal(Signal<A>),
}

/// The construction namespace for an actor's mailbox.
///
/// Never instantiated — it exists so construction reads as
/// `Mailbox::<A>::bounded(cap, id)`, keeping the sender/receiver/weak types cohesive
/// under one entry point instead of a free-floating function.
pub struct Mailbox<A: Mailboxed>(PhantomData<fn() -> A>);

impl<A: Mailboxed> Mailbox<A> {
    /// Creates a bounded mailbox with room for `capacity` queued USER-lane
    /// signals, owned by the actor identified as `id`, plus the paired UNBOUNDED
    /// control lane (card #225, ADR-0021) that watch/supervision ops ride.
    ///
    /// The receiver carries `id` because its `Drop` is the actor's true death
    /// edge: a [`ControlSignal::Watch`] still queued when the receiver goes away
    /// must be answered with a death notice naming this actor (see
    /// [`MailboxReceiver`]'s `Drop`), and only the receiver ever sees that
    /// backlog on a hard kill.
    ///
    /// Infallible by construction — [`Capacity`] has already excluded the values
    /// the backing channel would reject.
    #[must_use]
    pub fn bounded(capacity: Capacity, id: ActorId) -> (MailboxSender<A>, MailboxReceiver<A>) {
        let (tx, rx) = flume::bounded(capacity.get());
        let (ctl_tx, ctl_rx) = flume::unbounded();
        (
            MailboxSender { tx, ctl_tx },
            MailboxReceiver {
                rx,
                ctl_rx,
                user_open: true,
                ctl_open: true,
                me: id,
            },
        )
    }
}

/// Sends [`Signal`]s to an actor's mailbox. Cloneable; the channel stays open
/// while any sender is alive.
pub struct MailboxSender<A: Mailboxed> {
    tx: flume::Sender<Signal<A>>,
    /// The UNBOUNDED control lane (card #225, ADR-0021). Always paired with `tx`
    /// — every strong handle carries both halves, so the two channels share a
    /// sender count and disconnect together; [`is_closed`](Self::is_closed)
    /// needs to check only one.
    ctl_tx: flume::Sender<ControlSignal>,
}

impl<A: Mailboxed> Clone for MailboxSender<A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            ctl_tx: self.ctl_tx.clone(),
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
                ctx: SendContext::capture(),
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
            ctx: SendContext::capture(),
        })
    }

    /// Enqueues a control signal on the UNBOUNDED control lane, to be served
    /// ahead of the user backlog (card #225, ADR-0021).
    ///
    /// Synchronous: the lane has no capacity to await, so a watch registration
    /// or supervision op never queues behind user messages — the whole point of
    /// the lane. The bound on lane growth is the caller's own call rate, the
    /// same trust class as the unbounded link channel. A control send embeds no
    /// `self_sender`: only [`Signal::Message`] pins the actor (ADR-0003).
    ///
    /// # Errors
    ///
    /// Returns [`ControlClosed`] (carrying `signal` back) if the receiver has
    /// been dropped, i.e. the actor is no longer running.
    pub fn send_control(&self, signal: ControlSignal) -> Result<(), ControlClosed> {
        self.ctl_tx
            .send(signal)
            .map_err(|err| ControlClosed(err.into_inner()))
    }

    /// Whether the mailbox has closed — the receiver (the actor's run-loop) has
    /// been dropped, so no further signal can be delivered. A send-and-observe
    /// backup to push death-detection; **not** a pre-send liveness gate (that
    /// would be TOCTOU-wrong — a send races the close either way).
    ///
    /// Checks only the user-lane half: the receiver drops both lanes together
    /// and every [`MailboxSender`] carries both halves, so the two disconnect
    /// edges are coupled by construction and one check speaks for both lanes.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_disconnected()
    }

    /// Downgrades to a [`WeakMailboxSender`] that does not keep the mailbox open.
    #[must_use]
    pub fn downgrade(&self) -> WeakMailboxSender<A> {
        WeakMailboxSender {
            weak: self.tx.downgrade(),
            ctl_weak: self.ctl_tx.downgrade(),
        }
    }
}

/// A non-pinning handle to a mailbox: holding one does **not** keep it alive.
///
/// [`upgrade`](Self::upgrade) yields a strong sender only while a real
/// [`MailboxSender`] still exists — the primitive death-watch is built on.
pub struct WeakMailboxSender<A: Mailboxed> {
    weak: flume::WeakSender<Signal<A>>,
    ctl_weak: flume::WeakSender<ControlSignal>,
}

impl<A: Mailboxed> WeakMailboxSender<A> {
    /// Upgrades to a strong [`MailboxSender`], or `None` if every strong sender
    /// has been dropped (the actor is gone).
    ///
    /// Both lanes upgrade together or not at all: the strong counts cannot
    /// diverge (every [`MailboxSender`] carries both halves), and a half-paired
    /// sender would break the coupled-disconnect invariant
    /// [`is_closed`](MailboxSender::is_closed) relies on — so a half-upgrade
    /// (a sender pair dropped between the two upgrades) reports dead.
    #[must_use]
    pub fn upgrade(&self) -> Option<MailboxSender<A>> {
        match (self.weak.upgrade(), self.ctl_weak.upgrade()) {
            (Some(tx), Some(ctl_tx)) => Some(MailboxSender { tx, ctl_tx }),
            // A half-pair lost the race with a dropping sender pair; the claimed
            // half is released with the tuple and the handle reports dead.
            _ => None,
        }
    }
}

impl<A: Mailboxed> Clone for WeakMailboxSender<A> {
    fn clone(&self) -> Self {
        Self {
            weak: self.weak.clone(),
            ctl_weak: self.ctl_weak.clone(),
        }
    }
}

/// The single consumer of an actor's mailbox. The run-loop pulls from it.
pub struct MailboxReceiver<A: Mailboxed> {
    rx: flume::Receiver<Signal<A>>,
    ctl_rx: flume::Receiver<ControlSignal>,
    /// Per-lane close latches for the [`recv`](Self::recv) merge (the link-arm
    /// guard pattern from `actor::kind`): once a lane reports
    /// disconnected-and-empty it can never deliver again, so its select arm is
    /// disabled — a ready `Err` under `biased` would otherwise spin the merge
    /// and starve the surviving lane.
    user_open: bool,
    ctl_open: bool,
    /// The owning actor's identity — stamped onto the death notice sent for any
    /// still-queued [`ControlSignal::Watch`] when the backlog is rejected
    /// ([`reject_queued_watchers`](MailboxReceiver::reject_queued_watchers),
    /// which this receiver's `Drop` also routes through).
    me: ActorId,
}

impl<A: Mailboxed> MailboxReceiver<A> {
    /// Receives the next item, control lane first, waiting until one is available.
    ///
    /// The two-lane merge (card #225, ADR-0021): `try_recv` the control lane,
    /// then the user lane, else a `biased` select over both `recv_async`
    /// futures with the control arm first — a queued control signal is always
    /// served ahead of the user backlog, and one that arrives while the loop
    /// was parked overtakes the backlog from the very next poll. Cancel-safe:
    /// `recv` sits under `run_until_cancelled` and the loops' biased selects,
    /// and flume buffers items in shared state, so dropping the losing arm's
    /// future loses nothing.
    ///
    /// Returns `None` once BOTH lanes are closed and drained. The lanes
    /// disconnect together (every sender carries both halves), so `None` keeps
    /// its pre-split meaning: every strong sender is gone and the backlog is
    /// empty.
    pub async fn recv(&mut self) -> Option<Recv<A>> {
        // Fast path: serve a waiting item without yielding to the scheduler.
        // Control first — that is the lane's raison d'être (a full user
        // backlog must not delay it).
        if self.ctl_open {
            match self.ctl_rx.try_recv() {
                Ok(signal) => return Some(Recv::Control(signal)),
                Err(flume::TryRecvError::Disconnected) => self.ctl_open = false,
                Err(flume::TryRecvError::Empty) => {}
            }
        }
        if self.user_open {
            match self.rx.try_recv() {
                Ok(signal) => return Some(Recv::Signal(signal)),
                Err(flume::TryRecvError::Disconnected) => self.user_open = false,
                Err(flume::TryRecvError::Empty) => {}
            }
        }
        loop {
            tokio::select! {
                biased;
                ctl = self.ctl_rx.recv_async(), if self.ctl_open => {
                    match ctl {
                        Ok(signal) => return Some(Recv::Control(signal)),
                        // Disconnected AND drained: latch the arm off so a ready
                        // `Err` cannot spin the biased select (the link-arm
                        // guard pattern).
                        Err(_) => self.ctl_open = false,
                    }
                }
                user = self.rx.recv_async(), if self.user_open => {
                    match user {
                        Ok(signal) => return Some(Recv::Signal(signal)),
                        Err(_) => self.user_open = false,
                    }
                }
                // Both lanes latched off: closed and drained.
                else => return None,
            }
        }
    }

    /// Drains every currently-queued USER-lane signal without waiting, in FIFO
    /// order.
    ///
    /// A queued [`Signal::Message`] holds a strong `self_sender` (ADR-0003), so
    /// draining is what releases those senders and breaks the self-referential
    /// cycle between the channel and its backlog — see [`Drop`](Self::drop).
    pub fn drain(&mut self) -> impl Iterator<Item = Signal<A>> + '_ {
        self.rx.drain()
    }

    /// Drains every currently-queued CONTROL-lane signal without waiting, in
    /// FIFO order (card #225, ADR-0021): the teardown paths' view of the
    /// watch/supervision backlog. Graceful teardown APPLIES the drained ops
    /// (`drain_queued_supervision`, `apply_raced_registrations`); a hard kill
    /// ANSWERS the drained watches ([`reject_queued_watchers`](Self::reject_queued_watchers)).
    pub fn drain_control(&mut self) -> impl Iterator<Item = ControlSignal> + '_ {
        self.ctl_rx.drain()
    }

    /// Drains BOTH lanes' backlogs, answering every still-queued
    /// [`ControlSignal::Watch`] with a death notice carrying `reason`, and
    /// releasing the queued messages' `self_sender` cycle — the two duties
    /// documented on [`Drop`](Self::drop), which routes through here with the
    /// synthetic [`AlreadyDead`](ActorStopReason::AlreadyDead).
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
        // User lane: no watches live here since #225 — draining simply drops the
        // backlog, which releases each queued message's strong `self_sender`
        // (ADR-0003) and so discharges the leak-fix half of this drain.
        for signal in self.rx.drain() {
            drop(signal);
        }
        // Control lane: every still-queued watch registration is answered. A
        // queued `Unwatch` is unenforceable here (the watcher set is gone with
        // the loop); as in Erlang, a `demonitor` racing the death may still be
        // followed by a delivered notice. A queued `Supervision` op is dropped
        // — the armed registration's own `Drop` stops the never-tabled child
        // (#248), same as on the pre-split path.
        for signal in self.ctl_rx.drain() {
            if let ControlSignal::Watch(reg) = signal {
                trace::death_notice(reg.watcher, reason, cleanup_failed);
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
    /// Drops the receiver **and** both lanes' backlogs — and answers every
    /// still-queued [`ControlSignal::Watch`] with a synthetic death notice
    /// first.
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
    /// 2. **No missed death (card #195; moved lanes intact by #225).** A queued
    ///    `ControlSignal::Watch` was accepted by a successful send — the watcher
    ///    believes it is watching. This drop is the last code that ever sees the
    ///    registration, so it must deliver the notice: reason
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
    /// A queued `ControlSignal::Unwatch` is unenforceable here (the watcher set
    /// is gone with the loop); as in Erlang, a `demonitor` racing the death may
    /// still be followed by a delivered notice.
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
                Signal::Stop => unreachable!("send_message enqueues only Signal::Message"),
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

/// The control lane is closed — the receiver was dropped, so the control
/// signal could not be delivered.
///
/// Carries the undelivered [`ControlSignal`] back to the caller (rule 3: never
/// silently drop the payload). The control lane is unbounded, so closure is the
/// ONLY failure mode of [`MailboxSender::send_control`] — there is no `Full`.
pub struct ControlClosed(pub ControlSignal);

impl fmt::Debug for ControlClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ControlClosed(receiver dropped)")
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
    fn control_watch_and_unwatch_are_carried() {
        let (tx, _rx) = flume::unbounded::<LinkDied>();
        let reg = WatchReg {
            watcher: ActorId::from_raw_for_test(9),
            link_tx: tx,
            linked: true,
        };
        // Compiles only if ControlSignal carries Watch/Unwatch (this is the whole assertion).
        let _watch: ControlSignal = ControlSignal::Watch(Box::new(reg));
        let _unwatch: ControlSignal = ControlSignal::Unwatch(ActorId::from_raw_for_test(9));
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
    async fn recv_bounded<A: Mailboxed>(rx: &mut MailboxReceiver<A>) -> Option<Recv<A>> {
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
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));

        send_bounded(
            &tx,
            Signal::Message {
                msg: 42,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            },
        )
        .await
        .expect("send should succeed");

        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 42, .. }))
        ));
    }

    #[tokio::test]
    async fn capacity_at_the_upper_boundary_is_usable() {
        // A mailbox built at the capacity ceiling must not panic and must work.
        let (tx, mut rx) =
            Mailbox::<Probe>::bounded(cap(Capacity::MAX), ActorId::from_raw_for_test(0));

        tx.try_send(Signal::Message {
            msg: 7,
            self_sender: tx.clone(),
            ctx: SendContext::capture(),
        })
        .expect("send into max-capacity mailbox");

        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 7, .. }))
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
    fn cold_variants_are_boxed_so_message_slots_stay_small() {
        use std::mem::size_of;

        // Every cold CONTROL payload is boxed — `ControlSignal::Watch(Box<WatchReg>)`
        // and `ControlSignal::Supervision(Box<SupervisionOp>)` — so a small-message
        // actor's USER-lane slot is bounded by the hot Message path: `msg` + the
        // embedded `self_sender` + the caller-span trace context (ONE word — a
        // boxed-and-niched `Span`, card #209; a ZST in an off build, so the term
        // adds nothing there) + a discriminant word. Since #225 the `self_sender`
        // is TWO `flume::Sender`s (user + control halves, ADR-0021) — one Arc
        // word each, measured 8 B — so the Message slot grew by exactly one word;
        // the allocation PROFILE (zero per-message heap boxes) is untouched.
        // Inlined, `WatchReg` (a `flume::Sender` + id + flag) or `SupervisionOp`
        // (a `RestartConfig` + an erased factory + a tracker) would blow the
        // control lane's own slot bound for EVERY op. Guards the "every slot =
        // largest variant" trap on both lanes.
        assert!(
            size_of::<SendContext>() <= size_of::<usize>(),
            "trace context must stay one word — box, never inline, a Span in the envelope"
        );
        let hot_bound = size_of::<u64>()
            + size_of::<MailboxSender<Probe>>()
            + size_of::<SendContext>()
            + size_of::<usize>();
        assert!(
            size_of::<Signal<Probe>>() <= hot_bound,
            "Signal<Probe> slot is {} bytes (hot bound {hot_bound} = msg + self_sender + \
             trace ctx + discriminant); a user-lane variant is not bounded",
            size_of::<Signal<Probe>>()
        );
        // The control lane's own tripwire: `WatchReg`/`SupervisionOp` boxed,
        // `Unwatch(ActorId)` one word — the whole enum stays two words
        // (measured aarch64: 16 B = one payload word + one discriminant word).
        assert!(
            size_of::<ControlSignal>() <= 2 * size_of::<usize>(),
            "ControlSignal slot is {} bytes (> 2 words); a cold control payload is not boxed",
            size_of::<ControlSignal>()
        );
    }

    /// Demonstration (measured, not derived) of the **worst case** of the
    /// monomorphic, by-value `Signal<A>`: every queue slot costs `size_of` of the
    /// actor's *largest* `Msg` variant. One fat command variant therefore taxes
    /// every slot — even tiny messages — unless the user boxes it (the same
    /// discipline `LinkDied` uses).
    ///
    /// Measured (aarch64): `small = 40 B`, `fat inline = 4104 B+`, `boxed = 40 B`
    /// → for 1_000 queued messages, **4.10 MB vs 40 KB**. See #122.
    /// Since #209 every slot also carries the ONE-word caller-span trace
    /// context, so each bound below gains that word (zero in an off build);
    /// since #225 the embedded `self_sender` is two `flume::Sender`s (user +
    /// control halves, ADR-0021), another +8 B on every slot.
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

        let small_bound = 32 + size_of::<SendContext>();
        assert!(
            small <= small_bound,
            "small slot = {small} (bound 32 + one #209 trace-context word: 16 msg + \
             16 two-lane self_sender)"
        );
        assert!(fat >= 4096, "fat inline slot = {fat}");
        assert!(
            boxed <= small_bound,
            "boxed slot = {boxed} (bound 32 + one #209 trace-context word)"
        );

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

        let ctl_closed = ControlClosed(ControlSignal::Unwatch(ActorId::from_raw_for_test(1)));
        assert_eq!(format!("{ctl_closed:?}"), "ControlClosed(receiver dropped)");
    }

    #[tokio::test]
    async fn weak_sender_tracks_the_last_strong_sender() {
        let (tx, _rx) = Mailbox::<Probe>::bounded(cap(2), ActorId::from_raw_for_test(0));
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
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(2), ActorId::from_raw_for_test(0));
        let weak = tx.downgrade();

        let strong = weak.upgrade().expect("channel still alive");
        send_bounded(
            &strong,
            Signal::Message {
                msg: 5,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            },
        )
        .await
        .expect("send via upgraded");

        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 5, .. }))
        ));
    }

    #[tokio::test]
    async fn stop_signal_is_delivered_in_order_after_a_message() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));

        send_bounded(
            &tx,
            Signal::Message {
                msg: 1,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            },
        )
        .await
        .expect("message");
        send_bounded(&tx, Signal::Stop).await.expect("stop");

        // FIFO: the domain message precedes the control signal that followed it.
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 1, .. }))
        ));
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Stop))
        ));
    }

    #[tokio::test]
    async fn drain_flushes_queued_signals_in_order() {
        // Graceful-stop primitive: after a Stop, the run-loop flushes the rest
        // with `drain` before dropping the receiver.
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(8), ActorId::from_raw_for_test(0));
        for i in 0..3 {
            send_bounded(
                &tx,
                Signal::Message {
                    msg: i,
                    self_sender: tx.clone(),
                    ctx: SendContext::capture(),
                },
            )
            .await
            .expect("queued");
        }

        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 0, .. }))
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
        let (tx, rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));
        drop(rx);

        assert!(matches!(
            tx.send(Signal::Message {
                msg: 9,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            })
            .await,
            Err(SendError(Signal::Message { msg: 9, .. }))
        ));
        assert!(matches!(
            tx.try_send(Signal::Message {
                msg: 9,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            }),
            Err(TrySendError::Closed(Signal::Message { msg: 9, .. }))
        ));
    }

    #[tokio::test]
    async fn recv_returns_none_after_all_senders_dropped_and_drained() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));
        send_bounded(
            &tx,
            Signal::Message {
                msg: 1,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            },
        )
        .await
        .expect("queued");
        drop(tx);

        // Queued message drains first, then the disconnected channel ends.
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 1, .. }))
        ));
        assert!(recv_bounded(&mut rx).await.is_none());
    }

    /// `@bug` (card #195 review; moved to the control lane by #225): a
    /// `ControlSignal::Watch` still QUEUED when the receiver drops (hard kill
    /// mid-backlog, or the graceful window between the teardown drain and the
    /// receiver drop) was accepted by a successful send — silently discarding
    /// it is a missed death, the worst bug in the death-watch subsystem. The
    /// receiver's drop must instead deliver a synthetic [`LinkDied`](LinkDied)
    /// with the actor's own id, reason [`AlreadyDead`](ActorStopReason::AlreadyDead)
    /// (the true reason is unknowable here — Erlang's `noproc`), and the edge's
    /// `linked` flag preserved. FAILS while the drop drain `for_each(drop)`s the
    /// registration.
    #[tokio::test]
    async fn dropping_receiver_notifies_queued_watch_regs_already_dead() {
        let (tx, rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(77));

        let (link_tx, link_rx) = flume::unbounded::<LinkDied>();
        tx.send_control(ControlSignal::Watch(Box::new(WatchReg {
            watcher: ActorId::from_raw_for_test(1),
            link_tx,
            linked: true,
        })))
        .expect("reg enqueued into the open control lane");

        drop(rx); // receiver gone with the reg still queued

        let notice = link_rx
            .try_recv()
            .expect("a queued watch reg must be notified, never silently dropped");
        assert_eq!(
            notice.id,
            ActorId::from_raw_for_test(77),
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
        let (tx, rx) = Mailbox::<Canary>::bounded(cap(4), ActorId::from_raw_for_test(0));

        let canary = Arc::new(());
        let observer = Arc::downgrade(&canary);

        // Move the sole strong payload ref into the queued signal.
        tx.try_send(Signal::Message {
            msg: canary,
            self_sender: tx.clone(),
            ctx: SendContext::capture(),
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
    async fn send_control_reports_closed_after_receiver_drop_and_hands_back_the_signal() {
        let (tx, rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));
        drop(rx);

        let returned = tx.send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(7)));
        let Err(ControlClosed(ControlSignal::Unwatch(id))) = returned else {
            panic!("a closed control lane hands the signal back, got {returned:?}");
        };
        assert_eq!(
            id,
            ActorId::from_raw_for_test(7),
            "the exact undelivered signal came back"
        );
    }

    #[tokio::test]
    async fn upgraded_weak_sender_sends_on_both_lanes() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(2), ActorId::from_raw_for_test(0));
        let weak = tx.downgrade();

        let strong = weak.upgrade().expect("channel still alive");
        strong
            .send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(3)))
            .expect("control via the upgraded pair");
        send_bounded(
            &strong,
            Signal::Message {
                msg: 5,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            },
        )
        .await
        .expect("message via the upgraded pair");

        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Control(ControlSignal::Unwatch(_)))
        ));
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 5, .. }))
        ));
    }

    #[tokio::test]
    async fn control_overtakes_an_earlier_user_message() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));
        send_bounded(
            &tx,
            Signal::Message {
                msg: 1,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            },
        )
        .await
        .expect("message queued first");
        tx.send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(2)))
            .expect("control queued second");

        // The merge serves the control lane first even though the user signal
        // was enqueued earlier — the deliberate ordering relaxation (ADR-0021,
        // invariant 4).
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Control(ControlSignal::Unwatch(_)))
        ));
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 1, .. }))
        ));
    }

    #[tokio::test]
    async fn control_lane_preserves_its_own_fifo() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));
        for tag in 0..8u64 {
            tx.send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(tag)))
                .expect("control enqueued");
        }
        for tag in 0..8u64 {
            let Some(Recv::Control(ControlSignal::Unwatch(id))) = recv_bounded(&mut rx).await
            else {
                panic!("expected the control signal");
            };
            assert_eq!(id, ActorId::from_raw_for_test(tag), "intra-lane FIFO");
        }
    }

    #[tokio::test]
    async fn recv_returns_none_only_when_both_lanes_are_closed_and_empty() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));
        send_bounded(
            &tx,
            Signal::Message {
                msg: 1,
                self_sender: tx.clone(),
                ctx: SendContext::capture(),
            },
        )
        .await
        .expect("queued");
        tx.send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(1)))
            .expect("control queued");
        drop(tx);

        // Both lanes drain first (control ahead), then — with the queued
        // message's embedded `self_sender` released by the drain and `tx` gone
        // — the disconnected pair reports None (the lanes close together, ADR-0021).
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Control(ControlSignal::Unwatch(id)))
                if id == ActorId::from_raw_for_test(1)
        ));
        assert!(matches!(recv_bounded(&mut rx).await, Some(Recv::Signal(_))));
        assert!(recv_bounded(&mut rx).await.is_none());
    }

    #[tokio::test]
    async fn full_mailbox_rejects_try_send_and_returns_the_message() {
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(1), ActorId::from_raw_for_test(0));

        tx.try_send(Signal::Message {
            msg: 1,
            self_sender: tx.clone(),
            ctx: SendContext::capture(),
        })
        .expect("first signal fits");

        // Mailbox is now full: try_send must reject and hand the message back.
        let rejected = tx.try_send(Signal::Message {
            msg: 2,
            self_sender: tx.clone(),
            ctx: SendContext::capture(),
        });
        assert!(matches!(
            rejected,
            Err(TrySendError::Full(Signal::Message { msg: 2, .. }))
        ));

        // Draining one slot frees capacity for the next try_send.
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 1, .. }))
        ));
        tx.try_send(Signal::Message {
            msg: 3,
            self_sender: tx.clone(),
            ctx: SendContext::capture(),
        })
        .expect("fits after drain");
    }

    #[tokio::test]
    async fn try_send_message_delivers_then_reports_full_then_closed() {
        // Delivers into an open mailbox, embedding a self_sender (ADR-0003).
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(1), ActorId::from_raw_for_test(0));
        tx.try_send_message(1).expect("first fits");
        // Capacity 1 and full: the next try is backpressure, not delivery.
        assert!(matches!(
            tx.try_send_message(2),
            Err(TrySendError::Full(Signal::Message { msg: 2, .. }))
        ));
        assert!(matches!(
            recv_bounded(&mut rx).await,
            Some(Recv::Signal(Signal::Message { msg: 1, .. }))
        ));

        // Receiver dropped: a try now reports the terminal Closed, not Full.
        let (closed_tx, closed_rx) =
            Mailbox::<Probe>::bounded(cap(1), ActorId::from_raw_for_test(0));
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
        let (tx, mut rx) = Mailbox::<Tagged>::bounded(cap(4), ActorId::from_raw_for_test(0));
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
                            ctx: SendContext::capture(),
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
            let Recv::Signal(Signal::Message {
                msg: (sender_id, seq),
                ..
            }) = signal
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

    /// One producer step in the control-lane property: a user-lane message or a
    /// control-lane signal. The control arm is an `Unwatch` whose `ActorId`
    /// carries the FIFO tag — the one control variant constructible without a
    /// link channel.
    #[derive(Clone, Copy, Debug)]
    enum LaneOp {
        Msg(u64),
        Control(u64),
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
        /// for any message sequence and any capacity, EVEN WITH control signals
        /// interleaved (card #225, ADR-0021): the queue neither drops,
        /// duplicates, nor reorders — user-lane FIFO survives the control-lane
        /// merge, and the control signals keep their own intra-lane FIFO
        /// (invariants 2 and 3). MIRI-skipped by prefix (the repo's `prop_`
        /// naming contract).
        #[test]
        fn prop_fifo_roundtrip_single_sender(
            ops in prop::collection::vec(
                prop_oneof![
                    any::<u64>().prop_map(LaneOp::Msg),
                    any::<u64>().prop_map(LaneOp::Control),
                ],
                0..200,
            ),
            capacity in 1usize..=64,
        ) {
            let sent_msgs: Vec<u64> = ops
                .iter()
                .filter_map(|op| match op {
                    LaneOp::Msg(m) => Some(*m),
                    LaneOp::Control(_) => None,
                })
                .collect();
            let sent_ctls: Vec<ActorId> = ops
                .iter()
                .filter_map(|op| match op {
                    LaneOp::Control(tag) => Some(ActorId::from_raw_for_test(*tag)),
                    LaneOp::Msg(_) => None,
                })
                .collect();
            let msg_count = sent_msgs.len();
            let ctl_count = sent_ctls.len();
            let (got_msgs, got_ctls) = Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("current-thread runtime")
                .block_on(async move {
                    let (tx, mut rx) =
                        Mailbox::<Probe>::bounded(cap(capacity), ActorId::from_raw_for_test(0));
                    let expected = ops.len();
                    let producer = tokio::spawn(async move {
                        for op in ops {
                            match op {
                                LaneOp::Msg(message) => {
                                    send_bounded(&tx, Signal::Message { msg: message, self_sender: tx.clone(), ctx: SendContext::capture() })
                                        .await
                                        .expect("send");
                                }
                                LaneOp::Control(tag) => {
                                    tx.send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(tag)))
                                        .expect("control send on the unbounded lane");
                                }
                            }
                        }
                    });

                    let mut got_msgs = Vec::with_capacity(msg_count);
                    let mut got_ctls = Vec::with_capacity(ctl_count);
                    while got_msgs.len() + got_ctls.len() < expected {
                        match recv_bounded(&mut rx).await {
                            Some(Recv::Signal(Signal::Message { msg: message, .. })) => {
                                got_msgs.push(message);
                            }
                            Some(Recv::Control(ControlSignal::Unwatch(id))) => {
                                got_ctls.push(id);
                            }
                            Some(Recv::Signal(Signal::Stop)) | None => {
                                panic!("only Message/Control were enqueued, got Stop or closed")
                            }
                            Some(Recv::Control(_)) => {
                                panic!("only Unwatch controls were enqueued")
                            }
                        }
                    }
                    // The consumer has taken all it will take. Stop the producer
                    // so a mutation that makes `recv` drop messages fails on the
                    // subsequence assertions below, instead of deadlocking the
                    // producer on a full, undrained queue (card #179). In the
                    // unmutated run every item was delivered and consumed, so the
                    // producer has already finished and this abort is a no-op.
                    producer.abort();
                    let _ = producer.await;
                    (got_msgs, got_ctls)
                });

            prop_assert_eq!(got_msgs, sent_msgs, "user-lane FIFO broken under control interleavings");
            prop_assert_eq!(got_ctls, sent_ctls, "control-lane intra-FIFO broken");
        }
    }
}
