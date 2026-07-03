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
}

/// Sends [`Signal`]s to an actor's mailbox. Cloneable; the channel stays open
/// while any sender is alive.
pub struct MailboxSender<A: Mailboxed> {
    tx: mpsc::Sender<Signal<A>>,
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
}

impl<A: Mailboxed> MailboxReceiver<A> {
    /// Receives the next signal, waiting until one is available.
    ///
    /// Returns `None` once the mailbox is closed and drained.
    pub async fn recv(&mut self) -> Option<Signal<A>> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::num::NonZeroUsize;

    /// Scaffold actor for the mailbox tests. `Mailboxed` is the seam the
    /// not-yet-rebuilt `Actor` trait (#114/#116) will later subsume.
    struct Probe;
    impl Mailboxed for Probe {
        type Msg = u64;
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
}
