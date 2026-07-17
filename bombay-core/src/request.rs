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
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    error::TellError,
    mailbox::{MailboxSender, Mailboxed, SendMessageFut, Signal, TrySendError},
};

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
    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use crate::{
        actor::{Actor, ActorRef},
        error::TellError,
        mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed},
        message::Msg,
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
}
