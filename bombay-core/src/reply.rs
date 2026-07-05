//! The actor's typed, single-shot reply channel (card #115).
//!
//! Local tier of the two-tier message model (#66): an `ask` awaits exactly one
//! `Result<R, E>` back from a handler — **in-process, zero-serialize**, no
//! `Box<dyn Any>`. `R` is the reply value; `E` is the handler's own domain error
//! (a nexus `Conflict`, …), kept typed end to end. `E` defaults to [`Infallible`]
//! so an infallible reply is just `ReplySender<R>`.
//!
//! Backed by `tokio::sync::oneshot` (ADR-0002), kept an implementation detail
//! behind [`ReplySender`] / [`ReplyReceiver`] — the mailbox channel-seam
//! philosophy (ADR-0001): swap the primitive for M6 / `no_std` at the second impl.
//!
//! Out of scope (deferred to their machinery): `DelegatedReply` / `ForwardedReply`
//! are produced only by `Context::reply_sender`/`forward` (#116/#118).

use tokio::sync::oneshot;

use crate::error::{AskError, Infallible};

/// Sends the single reply to a waiting `ask`. Held by the handler; consuming
/// `self` on send makes a second reply a compile error.
#[must_use = "the asker is waiting for this reply"]
pub struct ReplySender<R, E = Infallible> {
    tx: oneshot::Sender<Result<R, E>>,
}

impl<R, E> ReplySender<R, E> {
    /// Sends the successful reply `R`. Consumes `self`. `Err(AskerGone)` if the
    /// asker already dropped its receiver (the ask was abandoned).
    pub fn send(self, reply: R) -> Result<(), AskerGone> {
        self.tx.send(Ok(reply)).map_err(|_| AskerGone)
    }

    /// Sends the handler's typed domain error `E` as the reply (surfaces as
    /// [`AskError::Handler`]). Consumes `self`. `Err(AskerGone)` if the asker is
    /// gone.
    pub fn send_err(self, error: E) -> Result<(), AskerGone> {
        self.tx.send(Err(error)).map_err(|_| AskerGone)
    }
}

/// The receive half held by the `ask`. Yields the single reply, mapped into the
/// typed [`AskError`].
pub struct ReplyReceiver<R, E = Infallible> {
    rx: oneshot::Receiver<Result<R, E>>,
}

impl<R, E> ReplyReceiver<R, E> {
    /// Awaits the one reply and maps the outcome into [`AskError`]:
    /// `Ok(Ok r) → Ok(r)`, `Ok(Err e) → Handler(e)`, sender-dropped →
    /// `Interrupted`.
    ///
    /// `M` is free: this layer never produces `Deliver`/`Timeout` (the ask
    /// builder's, #118), so it returns an `AskError<M, E>` ready for any `M`.
    pub async fn recv<M>(self) -> Result<R, AskError<M, E>> {
        match self.rx.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(handler_err)) => Err(AskError::Handler(handler_err)),
            Err(_recv_error) => Err(AskError::Interrupted),
        }
    }
}

/// The asker had already dropped its receiver, so the reply went nowhere. A unit
/// signal, not the payload: a reply to a vanished asker is un-actionable (nothing
/// to retry, unlike the mailbox's returned `Signal`).
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("asker gone; reply discarded")]
pub struct AskerGone;

/// Builds a fresh reply channel: the sender for the handler, the receiver for the
/// waiting `ask`.
#[must_use]
pub fn reply_channel<R, E>() -> (ReplySender<R, E>, ReplyReceiver<R, E>) {
    let (tx, rx) = oneshot::channel();
    (ReplySender { tx }, ReplyReceiver { rx })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in domain error — the shape a nexus aggregate's own `thiserror`
    /// enum takes (optimistic-concurrency `Conflict`, …).
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Conflict;

    /// Sequence: a handler's `Ok` reply reaches the caller, typed and intact.
    #[tokio::test]
    async fn ask_ok_reply_reaches_caller() {
        let (tx, rx) = reply_channel::<u32, Infallible>();
        tx.send(7).expect("asker still waiting");
        let got = rx.recv::<()>().await;
        assert_eq!(got.ok(), Some(7), "the Ok reply arrives typed and intact");
    }

    /// `@bug` — a handler that answers with its own domain error `E` must reach
    /// the caller as `AskError::Handler(E)`, **typed, not erased**. Fails if the
    /// port were `oneshot<R>` instead of `oneshot<Result<R, E>>`. (Ref #122-#2.)
    #[tokio::test]
    async fn ask_handler_error_reaches_caller_typed() {
        let (tx, rx) = reply_channel::<u32, Conflict>();
        tx.send_err(Conflict).expect("asker still waiting");
        let recovered = rx.recv::<()>().await.err().and_then(AskError::err);
        assert_eq!(recovered, Some(Conflict), "the domain error survives un-erased");
    }
}
