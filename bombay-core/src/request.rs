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

use pin_project_lite::pin_project;
use tokio::time::{Instant, Sleep, sleep_until};

use crate::{
    error::{AskError, Infallible, TellError},
    mailbox::{MailboxSender, Mailboxed, SendMessageFut, Signal, TrySendError},
    reply::{ReplyReceiver, ReplySender, reply_channel},
};

/// First retry delay of a timed tell's bounded backoff; doubles per retry up
/// to [`MAX_RETRY_DELAY`]. See [`TellWithTimeout`] for why a timed tell
/// retries instead of parking on the channel's own send future (ADR-0008).
const INITIAL_RETRY_DELAY: Duration = Duration::from_micros(100);

/// Ceiling for the timed tell's retry backoff.
const MAX_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Every ask carries this deadline unless the builder overrides it.
///
/// The Erlang `gen_server:call` 5000 ms precedent (OTP), chosen so an
/// accidental blocking cycle resolves as [`AskError::Timeout`] instead of
/// hanging (card #118 decision, #122-#4). Opt out explicitly with
/// [`AskRequest::no_timeout`].
pub const DEFAULT_ASK_TIMEOUT: Duration = Duration::from_secs(5);

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
        Signal::Stop | Signal::Watch(_) | Signal::Unwatch(_) => {
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
        // `None` = the caller's deadline overflows the clock: effectively
        // unbounded, so the loop simply never times out (no panic on
        // absurd input — rule: capacity/limit hits are `Result`s).
        let deadline = now.checked_add(self.deadline);
        TellTimeoutFut {
            mailbox: self.mailbox,
            msg: Some(self.msg),
            deadline,
            retry_delay: INITIAL_RETRY_DELAY,
            sleep: sleep_until(initial_park_target(deadline, now)),
        }
    }
}

/// The retry timer's target before `arm_retry` first runs: the deadline (or a
/// distant fallback when unbounded) — never an already-elapsed instant, so an
/// un-armed timer *parks* rather than reporting Ready in a loop.
fn initial_park_target(deadline: Option<Instant>, now: Instant) -> Instant {
    deadline
        .or_else(|| now.checked_add(Duration::from_hours(24)))
        .unwrap_or(now)
}

pin_project! {
    /// The in-flight future of an awaited [`TellWithTimeout`].
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TellTimeoutFut<'a, A: Mailboxed> {
        mailbox: &'a MailboxSender<A>,
        msg: Option<A::Msg>,
        deadline: Option<Instant>,
        retry_delay: Duration,
        // The `!Unpin` timer lives inline behind structural pinning — no
        // per-request allocation on the timed path (allocate-last).
        #[pin]
        sleep: Sleep,
    }
}

/// One non-blocking delivery attempt out of `slot`. `Some` = final outcome;
/// `None` = mailbox full, message restored to `slot` for the next retry.
fn attempt_send<A: Mailboxed>(
    mailbox: &MailboxSender<A>,
    slot: &mut Option<A::Msg>,
) -> Option<Result<(), TellError<A::Msg>>> {
    let Some(msg) = slot.take() else {
        unreachable!("send future polled after completion")
    };
    match mailbox.try_send_message(msg) {
        Ok(()) => Some(Ok(())),
        Err(TrySendError::Closed(signal)) => {
            Some(Err(TellError::ActorNotAlive(undelivered_msg(signal))))
        }
        Err(TrySendError::Full(signal)) => {
            *slot = Some(undelivered_msg(signal));
            None
        }
    }
}

/// Arms `sleep` for the next retry — never past `deadline` — and doubles
/// `retry_delay` up to [`MAX_RETRY_DELAY`]. The caller polls the armed sleep
/// itself: keeping the poll out of this helper means a whole-body mutation
/// cannot turn the park into an unpreemptible in-poll spin (a lesson from the
/// #118 mutation sweep — the spin ran 60 s inside one `poll` call, beyond any
/// test timeout's reach).
fn arm_retry(
    mut sleep: Pin<&mut Sleep>,
    retry_delay: &mut Duration,
    deadline: Option<Instant>,
    now: Instant,
) {
    let unclamped = now.checked_add(*retry_delay).unwrap_or(now);
    let next = deadline.map_or(unclamped, |hard_stop| cmp::min(unclamped, hard_stop));
    sleep.as_mut().reset(next);
    *retry_delay = cmp::min(retry_delay.saturating_mul(2), MAX_RETRY_DELAY);
}

impl<A: Mailboxed> Future for TellTimeoutFut<'_, A> {
    type Output = Result<(), TellError<A::Msg>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            if let Some(result) = attempt_send(this.mailbox, this.msg) {
                return Poll::Ready(result);
            }

            let now = Instant::now();
            if this.deadline.is_some_and(|deadline| now >= deadline) {
                let Some(msg) = this.msg.take() else {
                    unreachable!("the failed attempt restored the message")
                };
                return Poll::Ready(Err(TellError::SendTimeout(msg)));
            }

            arm_retry(this.sleep.as_mut(), this.retry_delay, *this.deadline, now);
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

/// The carrier an erased ask converts through: the request payload `M` plus
/// the typed reply port, absorbed into a target's closed menu via
/// `A::Msg: From<Ask<M, R, E>>`.
///
/// This is the ask-capable half of the #145 conversion boundary (ADR-0004):
/// `Recipient<M>` rides `From<M>`, [`ReplyRecipient`] rides `From<Ask<..>>` —
/// the menu stays closed and by-value, and the port travels inside the
/// constructed variant like any hand-written ask variant.
///
/// [`ReplyRecipient`]: crate::actor::ReplyRecipient
#[derive(Debug)]
pub struct Ask<M, R, E = Infallible> {
    /// The erased request payload.
    pub msg: M,
    /// The typed reply port to embed in the constructed variant.
    pub reply: ReplySender<R, E>,
}

/// A prepared `ask`: the request message (already carrying its typed reply
/// port) and its target, effect deferred until `.await`.
///
/// One deadline budgets the *whole* request — delivery and reply. Expiry
/// during delivery is `Deliver(SendTimeout(M))` (retryable, message back,
/// ADR-0008); expiry while awaiting the reply is [`AskError::Timeout`] (not
/// retryable — the message is already in the actor). The default deadline is
/// [`DEFAULT_ASK_TIMEOUT`]; opt out with [`no_timeout`](Self::no_timeout).
///
/// **Discipline (#122-#4):** a *handler* must never `ask(..).await` another
/// actor — that is the bounded-mailbox cycle deadlock. Handlers `tell` (or
/// emit an event) and take the reply as a new message; blocking asks belong
/// outside handlers, where the deadline backstops any accidental cycle.
#[must_use = "an ask does nothing until awaited"]
pub struct AskRequest<'a, A: Mailboxed, R, E = Infallible> {
    mailbox: &'a MailboxSender<A>,
    msg: A::Msg,
    rx: ReplyReceiver<R, E>,
    deadline: Option<Duration>,
}

impl<'a, A: Mailboxed, R, E> AskRequest<'a, A, R, E> {
    pub(crate) fn new(
        mailbox: &'a MailboxSender<A>,
        make_msg: impl FnOnce(ReplySender<R, E>) -> A::Msg,
    ) -> Self {
        let (reply_sender, rx) = reply_channel();
        Self {
            mailbox,
            msg: make_msg(reply_sender),
            rx,
            deadline: Some(DEFAULT_ASK_TIMEOUT),
        }
    }

    /// Replaces the [default](DEFAULT_ASK_TIMEOUT) deadline with `deadline`.
    pub const fn timeout(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Removes the deadline entirely — the ask waits forever. An explicit,
    /// discouraged opt-in: an accidental blocking cycle then hangs instead of
    /// resolving as [`AskError::Timeout`] (#122-#4).
    pub const fn no_timeout(mut self) -> Self {
        self.deadline = None;
        self
    }
}

impl<'a, A: Mailboxed, R, E> IntoFuture for AskRequest<'a, A, R, E> {
    type Output = Result<R, AskError<A::Msg, E>>;
    type IntoFuture = AskFut<'a, A, R, E>;

    fn into_future(self) -> Self::IntoFuture {
        let now = Instant::now();
        // Unrepresentable deadline (clock overflow) degrades to unbounded,
        // same as the timed tell.
        let deadline = self.deadline.and_then(|deadline| now.checked_add(deadline));
        AskFut {
            mailbox: self.mailbox,
            msg: Some(self.msg),
            rx: self.rx,
            deadline,
            retry_delay: INITIAL_RETRY_DELAY,
            delivered: false,
            sleep: sleep_until(initial_park_target(deadline, now)),
        }
    }
}

pin_project! {
    /// The in-flight future of an awaited [`AskRequest`]: delivers the request
    /// (bounded-retry, ADR-0008), then awaits the typed reply — one deadline
    /// across both phases, one timer reused for both.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct AskFut<'a, A: Mailboxed, R, E> {
        mailbox: &'a MailboxSender<A>,
        msg: Option<A::Msg>,
        rx: ReplyReceiver<R, E>,
        deadline: Option<Instant>,
        retry_delay: Duration,
        delivered: bool,
        #[pin]
        sleep: Sleep,
    }
}

impl<A: Mailboxed, R, E> Future for AskFut<'_, A, R, E> {
    type Output = Result<R, AskError<A::Msg, E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        while !*this.delivered {
            match attempt_send(this.mailbox, this.msg) {
                Some(Ok(())) => {
                    *this.delivered = true;
                    // Re-aim the retry timer at the deadline itself: whatever
                    // budget delivery left over now bounds the reply wait.
                    if let Some(deadline) = *this.deadline {
                        this.sleep.as_mut().reset(deadline);
                    }
                }
                Some(Err(tell_err)) => return Poll::Ready(Err(AskError::Deliver(tell_err))),
                None => {
                    let now = Instant::now();
                    if this.deadline.is_some_and(|deadline| now >= deadline) {
                        let Some(msg) = this.msg.take() else {
                            unreachable!("the failed attempt restored the message")
                        };
                        return Poll::Ready(Err(AskError::Deliver(TellError::SendTimeout(msg))));
                    }
                    arm_retry(this.sleep.as_mut(), this.retry_delay, *this.deadline, now);
                    match this.sleep.as_mut().poll(cx) {
                        Poll::Ready(()) => {}
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }

        // Reply phase: the port wins over a simultaneous deadline.
        if let Poll::Ready(outcome) = this.rx.poll_recv(cx) {
            return Poll::Ready(outcome);
        }
        if this.deadline.is_some() && this.sleep.poll(cx).is_ready() {
            return Poll::Ready(Err(AskError::Timeout));
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use super::Ask;
    use crate::{
        actor::{Actor, ActorRef, ReplyRecipient, Spawn},
        error::{AskError, TellError},
        mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed, Signal},
        message::Msg,
        reply::ReplySender,
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

    fn build_unstarted<A: Actor>(cap: usize) -> (ActorRef<A>, MailboxReceiver<A>) {
        let cap = Capacity::try_from(cap).expect("valid capacity");
        let (tx, rx) = Mailbox::<A>::bounded(cap);
        let (abort, _reg) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(ActorId::new(1), tx, CancellationToken::new(), abort);
        (actor_ref, rx)
    }

    fn build_ref(cap: usize) -> (ActorRef<Probe>, MailboxReceiver<Probe>) {
        build_unstarted::<Probe>(cap)
    }

    /// A stand-in domain error — the shape a nexus aggregate's `thiserror`
    /// enum takes (optimistic-concurrency `Conflict`, …).
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Conflict;

    /// A real, spawnable actor for the end-to-end ask tests. Each variant
    /// pins one arm of the ask outcome map.
    struct Counter {
        count: u64,
        parked: Vec<ReplySender<u64, Conflict>>,
    }
    #[derive(Debug)]
    enum CounterMsg {
        Add(u64),
        Get {
            reply: ReplySender<u64, Conflict>,
        },
        GetPlus {
            extra: u64,
            reply: ReplySender<u64, Conflict>,
        },
        FailingGet {
            reply: ReplySender<u64, Conflict>,
        },
        ReplyParked,
        Park {
            reply: ReplySender<u64, Conflict>,
        },
        PanicGet {
            reply: ReplySender<u64, Conflict>,
        },
    }
    impl Msg for CounterMsg {}
    impl Mailboxed for Counter {
        type Msg = CounterMsg;
    }
    impl Actor for Counter {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self {
                count: 0,
                parked: Vec::new(),
            })
        }
        async fn handle(
            &mut self,
            msg: CounterMsg,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            match msg {
                CounterMsg::Add(n) => self.count += n,
                CounterMsg::Get { reply } => drop(reply.send(self.count)),
                CounterMsg::GetPlus { extra, reply } => drop(reply.send(self.count + extra)),
                CounterMsg::FailingGet { reply } => drop(reply.send_err(Conflict)),
                CounterMsg::Park { reply } => self.parked.push(reply),
                CounterMsg::ReplyParked => {
                    let count = self.count;
                    self.parked.drain(..).for_each(|reply| {
                        let _ = reply.send(count);
                    });
                }
                CounterMsg::PanicGet {
                    reply: _dropped_unsent,
                } => {
                    panic!("dies mid-handle: the reply port drops unsent")
                }
            }
            Ok(())
        }
    }

    // The conversion boundary a `ReplyRecipient<u64, u64, Conflict>` rides on:
    // the closed menu absorbs an erased `Ask` carrier into its own variant.
    impl From<Ask<u64, u64, Conflict>> for CounterMsg {
        fn from(ask: Ask<u64, u64, Conflict>) -> Self {
            Self::GetPlus {
                extra: ask.msg,
                reply: ask.reply,
            }
        }
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

        let start = tokio::time::Instant::now();
        let sender = tokio::spawn(async move {
            actor_ref
                .tell(ProbeMsg(7))
                .timeout(Duration::from_secs(5))
                .await
        });
        // Let the timed sender attempt once and park on the full mailbox.
        tokio::task::yield_now().await;

        let first = tokio::time::timeout(terminate_bound(), rx.recv())
            .await
            .expect("the fill message must be received within the bound")
            .expect("the fill message is queued");
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

        tokio::time::timeout(terminate_bound(), sender)
            .await
            .expect("the sender must resolve within the bound")
            .expect("sender task")
            .expect("a freed slot before the deadline means delivery, not timeout");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "delivery follows the freed slot within the retry backoff — \
             waiting out the whole deadline instead means the retry timer was \
             never armed (elapsed {:?})",
            start.elapsed(),
        );
        let delivered = tokio::time::timeout(terminate_bound(), rx.recv())
            .await
            .expect("the timed message must be received within the bound")
            .expect("the timed message is queued");
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

        let delivered = tokio::time::timeout(terminate_bound(), rx.recv())
            .await
            .expect("the message must be received within the bound")
            .expect("the message is queued");
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

    /// Sequence: end-to-end ask over a *running* actor — the handler's typed
    /// `Ok` reply reaches the caller intact (card #118 FINALIZED surface).
    #[tokio::test]
    async fn ask_reply_reaches_caller_end_to_end() {
        let actor_ref = Counter::spawn(());
        tokio::time::timeout(terminate_bound(), actor_ref.tell(CounterMsg::Add(2)))
            .await
            .expect("tell resolves within the bound")
            .expect("delivered");

        let count = tokio::time::timeout(
            terminate_bound(),
            actor_ref.ask(|reply| CounterMsg::Get { reply }),
        )
        .await
        .expect("ask resolves within the bound")
        .expect("the handler replies");

        assert_eq!(count, 2, "the typed reply arrives intact");
    }

    /// A handler that answers with its own domain error surfaces as
    /// `AskError::Handler(E)` — typed, un-erased, never retryable.
    #[tokio::test]
    async fn ask_handler_error_reaches_caller_typed() {
        let actor_ref = Counter::spawn(());
        let err = tokio::time::timeout(
            terminate_bound(),
            actor_ref.ask(|reply| CounterMsg::FailingGet { reply }),
        )
        .await
        .expect("ask resolves within the bound")
        .expect_err("the handler answers with its domain error");

        assert!(
            !err.is_retryable(),
            "a domain error must never be re-driven"
        );
        assert_eq!(
            err.err(),
            Some(Conflict),
            "the domain error survives un-erased"
        );
    }

    /// `@bug` (card #118, ref #122-#3, sequence/lifecycle): the actor accepts
    /// the ask, then dies mid-handle (panic; the reply port drops unsent). The
    /// caller MUST see `AskError::Interrupted` — NOT `ActorNotAlive` (the
    /// actor was alive at accept) and NOT `Timeout` (no deadline elapsed).
    #[tokio::test]
    async fn ask_actor_dies_mid_handle_maps_interrupted() {
        let actor_ref = Counter::spawn(());
        let err = tokio::time::timeout(
            terminate_bound(),
            actor_ref.ask(|reply| CounterMsg::PanicGet { reply }),
        )
        .await
        .expect("ask resolves within the bound, not a hang")
        .expect_err("the actor died before replying");

        assert!(
            matches!(err, AskError::Interrupted),
            "died-after-accept is Interrupted, not ActorNotAlive/Timeout: {err:?}",
        );
    }

    /// Card #118 (defensive/liveness, ref #122-#4 detection):
    /// `ask_times_out_when_target_saturated` — the target's bounded mailbox
    /// stays full for the whole deadline, so the ask resolves (never hangs)
    /// with the *delivery* half timing out: `Deliver(SendTimeout)`, retryable,
    /// message handed back (it never entered the actor).
    #[tokio::test(start_paused = true)]
    async fn ask_times_out_when_target_saturated() {
        let (actor_ref, _rx) = build_unstarted::<Counter>(1);
        actor_ref
            .tell(CounterMsg::Add(1))
            .try_send()
            .expect("first message fills the capacity-1 mailbox");

        let err = tokio::time::timeout(
            terminate_bound(),
            actor_ref
                .ask(|reply| CounterMsg::Get { reply })
                .timeout(Duration::from_millis(50)),
        )
        .await
        .expect("a timed ask must resolve within its deadline, not hang")
        .expect_err("the mailbox stays full for the whole deadline");

        assert!(
            err.is_retryable(),
            "undelivered ask is retryable backpressure"
        );
        assert!(
            matches!(
                err,
                AskError::Deliver(TellError::SendTimeout(CounterMsg::Get { .. }))
            ),
            "the exact request comes back undelivered: {err:?}",
        );
    }

    /// The reply-side timeout: the message *was* delivered but the handler
    /// never replies (it parks the port), so the deadline resolves to
    /// `AskError::Timeout` — NOT retryable (the message is already in the
    /// actor; a re-send would duplicate it) and carrying nothing back.
    #[tokio::test(start_paused = true)]
    async fn ask_times_out_when_handler_never_replies() {
        let actor_ref = Counter::spawn(());
        let err = tokio::time::timeout(
            terminate_bound(),
            actor_ref
                .ask(|reply| CounterMsg::Park { reply })
                .timeout(Duration::from_millis(50)),
        )
        .await
        .expect("a timed ask must resolve within its deadline, not hang")
        .expect_err("the parked handler never replies");

        assert!(
            matches!(err, AskError::Timeout),
            "delivered-but-unanswered is Timeout: {err:?}"
        );
        assert!(
            !err.is_retryable(),
            "the message is already in the actor — a retry would duplicate it",
        );
    }

    /// Every ask carries a default deadline (the Erlang `gen_server:call`
    /// 5000 ms precedent) so an accidental blocking cycle resolves as
    /// `Timeout` instead of hanging forever (card #118 decision, #122-#4).
    /// Paused time pins the exact default.
    #[tokio::test(start_paused = true)]
    async fn ask_default_timeout_is_five_seconds() {
        let actor_ref = Counter::spawn(());
        let start = tokio::time::Instant::now();

        let err = actor_ref
            .ask(|reply| CounterMsg::Park { reply })
            .await
            .expect_err("the parked handler never replies");

        assert!(matches!(err, AskError::Timeout), "got {err:?}");
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_secs(5) && waited < Duration::from_secs(6),
            "the default deadline is 5s, waited {waited:?}",
        );
    }

    /// The infinite ask is an explicit, discouraged opt-in: with
    /// `no_timeout()` the ask outlives any deadline — still pending after an
    /// hour of (paused) clock rather than resolving to `Timeout`.
    #[tokio::test(start_paused = true)]
    async fn ask_no_timeout_outlives_the_default_deadline() {
        let actor_ref = Counter::spawn(());
        let still_pending = tokio::time::timeout(
            Duration::from_secs(3600),
            actor_ref
                .ask(|reply| CounterMsg::Park { reply })
                .no_timeout(),
        )
        .await;

        assert!(
            still_pending.is_err(),
            "no_timeout must wait indefinitely, not resolve to Timeout",
        );
    }

    /// #145 deferral landed here: the ask-capable erased handle. A
    /// `ReplyRecipient<M, R, E>` targets any actor whose menu absorbs the
    /// `Ask<M, R, E>` carrier (`A::Msg: From<Ask<..>>`); the typed reply comes
    /// back through the erasure intact.
    #[tokio::test]
    async fn reply_recipient_ask_round_trips_typed_reply() {
        let actor_ref = Counter::spawn(());
        tokio::time::timeout(terminate_bound(), actor_ref.tell(CounterMsg::Add(2)))
            .await
            .expect("tell resolves within the bound")
            .expect("delivered");

        let recipient: ReplyRecipient<u64, u64, Conflict> = actor_ref.reply_recipient();
        let sum = tokio::time::timeout(terminate_bound(), recipient.ask(40))
            .await
            .expect("ask resolves within the bound")
            .expect("the handler replies");

        assert_eq!(sum, 42, "count 2 + extra 40 arrives typed through erasure");
    }

    /// Erased delivery failure keeps the typed handback: a reaped target
    /// surfaces `Deliver(ActorNotAlive(M))` with the exact `M` back (the
    /// ADR-0004 clone-before-convert price buys this).
    #[tokio::test]
    async fn reply_recipient_ask_to_reaped_actor_hands_msg_back() {
        let (actor_ref, rx) = build_unstarted::<Counter>(1);
        let recipient: ReplyRecipient<u64, u64, Conflict> = actor_ref.reply_recipient();

        // Identity + liveness survive the erasure (mutation-sweep survivors:
        // both `is_alive` directions and the Debug impl were unasserted).
        assert_eq!(
            recipient.id(),
            actor_ref.id(),
            "id preserved through erasure"
        );
        assert!(recipient.is_alive(), "open mailbox reads alive");
        let shown = format!("{recipient:?}");
        assert!(
            shown.contains("ReplyRecipient") && shown.contains("u64"),
            "debug names the struct and the erased M: {shown}",
        );

        drop(rx); // reap: the run-loop's receiver is gone
        assert!(!recipient.is_alive(), "the recipient observes the reap");

        let err = tokio::time::timeout(terminate_bound(), recipient.ask(7))
            .await
            .expect("ask resolves within the bound")
            .expect_err("the target is reaped");

        assert!(err.is_terminal(), "a reaped target is terminal");
        assert!(
            matches!(err, AskError::Deliver(TellError::ActorNotAlive(7))),
            "the exact M comes back through the erasure: {err:?}",
        );
    }

    /// Erased ask against a saturated target: the deadline resolves the
    /// delivery half as `Deliver(SendTimeout(M))` — retryable, exact `M` back —
    /// mirroring the typed ask's contract through the erasure.
    #[tokio::test(start_paused = true)]
    async fn reply_recipient_ask_times_out_saturated_with_msg_back() {
        let (actor_ref, _rx) = build_unstarted::<Counter>(1);
        actor_ref
            .tell(CounterMsg::Add(1))
            .try_send()
            .expect("first message fills the capacity-1 mailbox");
        let recipient: ReplyRecipient<u64, u64, Conflict> = actor_ref.reply_recipient();

        let err = tokio::time::timeout(
            terminate_bound(),
            recipient.ask(9).timeout(Duration::from_millis(50)),
        )
        .await
        .expect("a timed ask must resolve within its deadline, not hang")
        .expect_err("the mailbox stays full for the whole deadline");

        assert!(err.is_retryable(), "undelivered is retryable backpressure");
        assert!(
            matches!(err, AskError::Deliver(TellError::SendTimeout(9))),
            "the exact M comes back undelivered: {err:?}",
        );
    }

    /// **Delegated reply is structural** (#115 deferral resolved on #118): the
    /// port is a plain value in the message, so a handler delegates by simply
    /// *keeping* it and replying from a later handle call — no `DelegatedReply`
    /// marker type exists or is needed (kameo needed one to suppress its
    /// implicit auto-reply; bombay has no auto-reply to suppress).
    #[tokio::test]
    async fn delegated_reply_arrives_from_a_later_handle_call() {
        let actor_ref = Counter::spawn(());

        let asker = actor_ref.clone();
        let pending =
            tokio::spawn(async move { asker.ask(|reply| CounterMsg::Park { reply }).await });
        tokio::task::yield_now().await;

        // Two later messages: one mutates state, one releases the parked port.
        tokio::time::timeout(terminate_bound(), actor_ref.tell(CounterMsg::Add(5)))
            .await
            .expect("bound")
            .expect("delivered");
        tokio::time::timeout(terminate_bound(), actor_ref.tell(CounterMsg::ReplyParked))
            .await
            .expect("bound")
            .expect("delivered");

        let replied = tokio::time::timeout(terminate_bound(), pending)
            .await
            .expect("the delegated ask resolves within the bound")
            .expect("asker task");
        assert_eq!(
            replied.ok(),
            Some(5),
            "the delegated reply reflects state changed AFTER the original ask",
        );
    }

    /// **Forwarded reply is structural**: the port is `Send`, so a front actor
    /// forwards an ask by moving the port into a message to a second actor,
    /// which replies directly to the original asker — no `ForwardedReply` type.
    #[tokio::test]
    async fn forwarded_reply_comes_from_the_second_actor() {
        struct Front {
            back: ActorRef<Counter>,
        }
        #[derive(Debug)]
        enum FrontMsg {
            ForwardGet { reply: ReplySender<u64, Conflict> },
        }
        impl Msg for FrontMsg {}
        impl Mailboxed for Front {
            type Msg = FrontMsg;
        }
        impl Actor for Front {
            type Args = ActorRef<Counter>;
            type Error = core::convert::Infallible;
            async fn on_start(back: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Ok(Self { back })
            }
            async fn handle(
                &mut self,
                FrontMsg::ForwardGet { reply }: FrontMsg,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                // Forwarding = moving the port into another actor's message.
                // Fire-and-forget (#122-#4 discipline: no ask().await here).
                drop(self.back.tell(CounterMsg::Get { reply }).try_send());
                Ok(())
            }
        }

        let back = Counter::spawn(());
        tokio::time::timeout(terminate_bound(), back.tell(CounterMsg::Add(11)))
            .await
            .expect("bound")
            .expect("delivered");
        let front = Front::spawn(back);

        let count = tokio::time::timeout(
            terminate_bound(),
            front.ask(|reply| FrontMsg::ForwardGet { reply }),
        )
        .await
        .expect("the forwarded ask resolves within the bound")
        .expect("the BACK actor replies through the forwarded port");

        assert_eq!(count, 11, "the reply came from the second actor's state");
    }
}
