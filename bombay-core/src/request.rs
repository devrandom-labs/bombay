//! The ask/tell request builders (card #118).
//!
//! A send is a value first, an effect second: [`ActorRef::tell`] returns a
//! [`TellRequest`] that does nothing until consumed — `.await` runs the
//! backpressure-blocking send, [`try_send`](TellRequest::try_send) the
//! non-blocking one. Hand-rolled `IntoFuture` structs, not a builder crate
//! (ADR-0007): the surface awaits the request *directly*, which a
//! finishing-call builder cannot express, and the zero-alloc tell path (#114)
//! rules out boxing the future.
//!
//! No pre-send liveness check anywhere — send-and-observe (TOCTOU): the
//! [`TellError`] a consumed request resolves to *is* the death signal.
//!
//! [`ActorRef::tell`]: crate::actor::ActorRef::tell

use std::{
    cmp,
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::time::{Instant, Sleep, sleep_until};

use crate::{
    error::TellError,
    mailbox::{MailboxSender, Mailboxed, SendMessageFut, Signal, TrySendError},
};

/// First retry delay of a timed tell's bounded backoff; doubles per retry up
/// to [`MAX_RETRY_DELAY`]. See [`TellWithTimeout`] for why a timed tell
/// retries instead of parking on the channel's own send future (ADR-0008).
const INITIAL_RETRY_DELAY: Duration = Duration::from_micros(100);

/// Ceiling for the timed tell's retry backoff.
const MAX_RETRY_DELAY: Duration = Duration::from_millis(10);

/// A prepared `tell`: the message and its target, effect deferred until the
/// request is consumed.
///
/// - `.await` — the backpressure-blocking send: waits for mailbox capacity.
/// - [`try_send`](Self::try_send) — non-blocking: a full mailbox is
///   [`TellError::MailboxFull`], handed straight back.
#[must_use = "a tell does nothing until awaited or sent with `try_send`"]
pub struct TellRequest<'a, A: Mailboxed> {
    mailbox: &'a MailboxSender<A>,
    msg: A::Msg,
}

impl<'a, A: Mailboxed> TellRequest<'a, A> {
    pub(crate) const fn new(mailbox: &'a MailboxSender<A>, msg: A::Msg) -> Self {
        Self { mailbox, msg }
    }

    /// Sends without waiting: enqueues if the mailbox has a free slot, fails
    /// immediately otherwise. Capacity pressure is a `Result`, never a panic.
    ///
    /// # Errors
    ///
    /// [`TellError::MailboxFull`] if the mailbox is at capacity (retryable
    /// backpressure), [`TellError::ActorNotAlive`] if the actor has stopped
    /// (terminal). Both hand the exact undelivered message back.
    pub fn try_send(self) -> Result<(), TellError<A::Msg>> {
        self.mailbox
            .try_send_message(self.msg)
            .map_err(|err| match err {
                TrySendError::Full(signal) => TellError::MailboxFull(undelivered_msg(signal)),
                TrySendError::Closed(signal) => TellError::ActorNotAlive(undelivered_msg(signal)),
            })
    }

    /// Bounds the blocking send: wait for mailbox capacity at most `deadline`,
    /// then resolve to [`TellError::SendTimeout`] with the message back.
    ///
    /// **Guaranteed handback** (ADR-0008): the timed send owns the message for
    /// the entire wait, so `SendTimeout` means *definitely never delivered* —
    /// re-sending cannot duplicate. A zero `deadline` still makes exactly one
    /// delivery attempt. The typestate consumes `try_send` deliberately:
    /// a timeout on an instantaneous send is meaningless, so it does not
    /// compile.
    pub fn timeout(self, deadline: Duration) -> TellWithTimeout<'a, A> {
        TellWithTimeout {
            mailbox: self.mailbox,
            msg: self.msg,
            deadline,
        }
    }
}

/// Recovers the domain message from a bounced [`Signal`]. The request layer
/// only ever enqueues `Signal::Message`, so the other arms cannot bounce here.
fn undelivered_msg<A: Mailboxed>(signal: Signal<A>) -> A::Msg {
    match signal {
        Signal::Message { msg, .. } => msg,
        Signal::Stop | Signal::LinkDied(_) => {
            unreachable!("the request layer enqueues only Signal::Message")
        }
    }
}

impl<'a, A: Mailboxed> IntoFuture for TellRequest<'a, A> {
    type Output = Result<(), TellError<A::Msg>>;
    type IntoFuture = TellFut<'a, A>;

    /// Begins the backpressure-blocking send (what `.await` on the request
    /// runs): waits for mailbox capacity, resolving to
    /// [`TellError::ActorNotAlive`] with the message back if the actor stopped.
    fn into_future(self) -> Self::IntoFuture {
        TellFut {
            inner: self.mailbox.send_message(self.msg),
        }
    }
}

/// A prepared, deadline-bounded `tell` — consume with `.await`.
///
/// The wait is a deadline-bounded `try_send` retry loop (exponential backoff,
/// [`INITIAL_RETRY_DELAY`]→[`MAX_RETRY_DELAY`]) rather than a cancelled park
/// on the channel's send future: cancelling the channel primitive's send is
/// *indeterminate* (the receiver may claim the queued item in the same
/// instant), which would make a returned message unsafe to retry. Retrying
/// `try_send` keeps ownership of the message the whole wait, so
/// [`TellError::SendTimeout`] is a hard "never delivered" (ADR-0008). The
/// price: no queue position — under sustained saturation, parked untimed
/// senders win slots first and the timed tell times out, which is the signal a
/// deadline-bearing caller asked for.
#[must_use = "a tell does nothing until awaited"]
pub struct TellWithTimeout<'a, A: Mailboxed> {
    mailbox: &'a MailboxSender<A>,
    msg: A::Msg,
    deadline: Duration,
}

impl<'a, A: Mailboxed> IntoFuture for TellWithTimeout<'a, A> {
    type Output = Result<(), TellError<A::Msg>>;
    type IntoFuture = TellTimeoutFut<'a, A>;

    fn into_future(self) -> Self::IntoFuture {
        let now = Instant::now();
        TellTimeoutFut {
            mailbox: self.mailbox,
            msg: Some(self.msg),
            // `None` = the caller's deadline overflows the clock: effectively
            // unbounded, so the loop simply never times out (no panic on
            // absurd input — rule: capacity/limit hits are `Result`s).
            deadline: now.checked_add(self.deadline),
            retry_delay: INITIAL_RETRY_DELAY,
            // Re-armed before every park; the initial target is irrelevant.
            sleep: Box::pin(sleep_until(now)),
        }
    }
}

/// The in-flight future of an awaited [`TellWithTimeout`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TellTimeoutFut<'a, A: Mailboxed> {
    mailbox: &'a MailboxSender<A>,
    msg: Option<A::Msg>,
    deadline: Option<Instant>,
    retry_delay: Duration,
    // The timer needs a stable pinned home; one allocation per *timed* tell —
    // the same price tokio's own `Interval` pays (it holds a boxed `Sleep`).
    // The un-timed `TellFut` path stays allocation-free.
    sleep: Pin<Box<Sleep>>,
}

// Sound: no field is structurally pinned — `msg` is plain owned data the poll
// moves in and out of, and the timer's address stability comes from its own
// `Pin<Box<Sleep>>`, not from this struct's location. Same move flume makes
// for `SendFut` (`impl<T> Unpin for SendFut<'_, T> {}`).
impl<A: Mailboxed> Unpin for TellTimeoutFut<'_, A> {}

impl<A: Mailboxed> TellTimeoutFut<'_, A> {
    /// One delivery attempt. `Some` = final result; `None` = mailbox full,
    /// message restored to `self.msg` for the next retry.
    fn attempt(&mut self) -> Option<Result<(), TellError<A::Msg>>> {
        let Some(msg) = self.msg.take() else {
            unreachable!("TellTimeoutFut polled after completion")
        };
        match self.mailbox.try_send_message(msg) {
            Ok(()) => Some(Ok(())),
            Err(TrySendError::Closed(signal)) => {
                Some(Err(TellError::ActorNotAlive(undelivered_msg(signal))))
            }
            Err(TrySendError::Full(signal)) => {
                self.msg = Some(undelivered_msg(signal));
                None
            }
        }
    }
}

impl<A: Mailboxed> Future for TellTimeoutFut<'_, A> {
    type Output = Result<(), TellError<A::Msg>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            if let Some(result) = this.attempt() {
                return Poll::Ready(result);
            }

            let now = Instant::now();
            if this.deadline.is_some_and(|deadline| now >= deadline) {
                let Some(msg) = this.msg.take() else {
                    unreachable!("the failed attempt restored the message")
                };
                return Poll::Ready(Err(TellError::SendTimeout(msg)));
            }

            // Park until the next retry, never past the deadline itself.
            let mut next = now.checked_add(this.retry_delay).unwrap_or(now);
            if let Some(deadline) = this.deadline {
                next = cmp::min(next, deadline);
            }
            this.sleep.as_mut().reset(next);
            this.retry_delay = cmp::min(this.retry_delay.saturating_mul(2), MAX_RETRY_DELAY);
            match this.sleep.as_mut().poll(cx) {
                Poll::Ready(()) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// The in-flight future of an awaited [`TellRequest`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TellFut<'a, A: Mailboxed> {
    inner: SendMessageFut<'a, A>,
}

impl<A: Mailboxed> Future for TellFut<'_, A> {
    type Output = Result<(), TellError<A::Msg>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `SendMessageFut` is `Unpin` (it wraps flume's explicitly-`Unpin`
        // send future), so plain re-pinning is sound without projection.
        Pin::new(&mut self.get_mut().inner)
            .poll(cx)
            .map(|result| result.map_err(TellError::ActorNotAlive))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use crate::{
        actor::{Actor, ActorRef},
        error::TellError,
        mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed, Signal},
        message::Msg,
        test_support::terminate_bound,
    };

    // Minimal actor to key the mailbox/ref — the loop never runs in these
    // tests. `ProbeMsg` carries a `u64` so handback assertions can prove the
    // *exact* undelivered message returns, not just the variant.
    struct Probe;
    #[derive(Debug)]
    struct ProbeMsg(u64);
    impl Msg for ProbeMsg {}
    impl Mailboxed for Probe {
        type Msg = ProbeMsg;
    }
    impl Actor for Probe {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Probe)
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

    fn build_ref(cap: usize) -> (ActorRef<Probe>, MailboxReceiver<Probe>) {
        let cap = Capacity::try_from(cap).expect("valid capacity");
        let (tx, rx) = Mailbox::<Probe>::bounded(cap);
        let (abort, _reg) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(ActorId::new(1), tx, CancellationToken::new(), abort);
        (actor_ref, rx)
    }

    /// Card #118 (defensive boundary): a non-blocking send to a *full* bounded
    /// mailbox must surface `TellError::MailboxFull` — a `Result`, never a
    /// panic — carrying the exact undelivered message back, and classify as
    /// retryable backpressure.
    #[tokio::test]
    async fn try_send_full_returns_mailbox_full_with_msg() {
        let (actor_ref, _rx) = build_ref(1);
        actor_ref
            .tell(ProbeMsg(1))
            .try_send()
            .expect("first message fits the capacity-1 mailbox");

        let err = actor_ref
            .tell(ProbeMsg(9))
            .try_send()
            .expect_err("second message hits a full mailbox");

        assert!(
            err.is_retryable(),
            "backpressure is retryable, not terminal"
        );
        let TellError::MailboxFull(ProbeMsg(recovered)) = err else {
            panic!("expected MailboxFull carrying the message, got {err:?}");
        };
        assert_eq!(recovered, 9, "the exact undelivered message is handed back");
    }

    /// Card #118 (defensive/liveness, deferred from #113): a timed blocking
    /// send against a mailbox that stays full for the whole deadline resolves —
    /// never hangs — to `SendTimeout` carrying the exact undelivered message
    /// (guaranteed handback, ADR-0008), classified retryable.
    #[tokio::test(start_paused = true)]
    async fn tell_timeout_on_saturated_mailbox_returns_send_timeout_with_msg() {
        let (actor_ref, _rx) = build_ref(1);
        actor_ref
            .tell(ProbeMsg(1))
            .try_send()
            .expect("first message fills the capacity-1 mailbox");

        let err = tokio::time::timeout(
            terminate_bound(),
            actor_ref
                .tell(ProbeMsg(7))
                .timeout(Duration::from_millis(50)),
        )
        .await
        .expect("a timed tell must resolve within its deadline, not hang")
        .expect_err("the mailbox stays full for the whole deadline");

        assert!(err.is_retryable(), "never delivered, so retry is safe");
        let TellError::SendTimeout(ProbeMsg(recovered)) = err else {
            panic!("expected SendTimeout carrying the message, got {err:?}");
        };
        assert_eq!(recovered, 7, "the exact undelivered message is handed back");
    }

    /// Sequence: a timed tell parked on a full mailbox delivers as soon as the
    /// receiver frees a slot before the deadline — the timeout is a bound, not
    /// a delay, and the exact message lands in the mailbox.
    #[tokio::test(start_paused = true)]
    async fn tell_timeout_delivers_when_capacity_frees_before_deadline() {
        let (actor_ref, mut rx) = build_ref(1);
        actor_ref
            .tell(ProbeMsg(1))
            .try_send()
            .expect("first message fills the capacity-1 mailbox");

        let sender = tokio::spawn(async move {
            actor_ref
                .tell(ProbeMsg(7))
                .timeout(Duration::from_secs(5))
                .await
        });
        // Let the timed sender attempt once and park on the full mailbox.
        tokio::task::yield_now().await;

        let first = rx.recv().await.expect("the fill message is queued");
        assert!(
            matches!(
                first,
                Signal::Message {
                    msg: ProbeMsg(1),
                    ..
                }
            ),
            "the fill message drains first (FIFO)",
        );

        sender
            .await
            .expect("sender task")
            .expect("a freed slot before the deadline means delivery, not timeout");
        let delivered = rx.recv().await.expect("the timed message is queued");
        assert!(
            matches!(
                delivered,
                Signal::Message {
                    msg: ProbeMsg(7),
                    ..
                }
            ),
            "the exact timed message was enqueued",
        );
    }

    /// Boundary (`Duration::ZERO`): a zero deadline still makes exactly one
    /// delivery attempt before checking the clock — an empty mailbox accepts
    /// the message rather than reporting a vacuous timeout.
    #[tokio::test(start_paused = true)]
    async fn tell_timeout_zero_still_attempts_once() {
        let (actor_ref, mut rx) = build_ref(1);

        actor_ref
            .tell(ProbeMsg(3))
            .timeout(Duration::ZERO)
            .await
            .expect("an empty mailbox accepts the message at deadline zero");

        let delivered = rx.recv().await.expect("the message is queued");
        assert!(
            matches!(
                delivered,
                Signal::Message {
                    msg: ProbeMsg(3),
                    ..
                }
            ),
            "the exact message was enqueued",
        );
    }
}
