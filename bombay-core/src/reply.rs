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
    /// Sends the successful reply `R`.
    ///
    /// Consumes `self`, so a second reply does not compile — `send` moves `self`:
    ///
    /// ```compile_fail
    /// # use bombay_core::reply::reply_channel;
    /// # use bombay_core::error::Infallible;
    /// let (tx, _rx) = reply_channel::<u32, Infallible>();
    /// let _ = tx.send(1);
    /// let _ = tx.send(2); // ← tx already moved: E0382
    /// ```
    ///
    /// # Errors
    ///
    /// [`AskerGone`] if the asker already dropped its receiver (the ask was
    /// abandoned) — the reply is discarded, and the caller may ignore it.
    pub fn send(self, reply: R) -> Result<(), AskerGone> {
        self.tx.send(Ok(reply)).map_err(|_| AskerGone)
    }

    /// Sends the handler's typed domain error `E` as the reply.
    ///
    /// Surfaces to the asker as [`AskError::Handler`]. Consumes `self`.
    ///
    /// # Errors
    ///
    /// [`AskerGone`] if the asker already dropped its receiver.
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
    /// Awaits the one reply, mapped into the typed [`AskError`].
    ///
    /// The outcome map: `Ok(Ok r) → Ok(r)`, `Ok(Err e) → Handler(e)`, and a
    /// dropped sender → `Interrupted`.
    ///
    /// `M` is free: this layer never produces `Deliver`/`Timeout` (the ask
    /// builder's, #118), so it returns an `AskError<M, E>` ready for any `M`.
    ///
    /// # Errors
    ///
    /// [`AskError::Handler`] if the handler replied with its domain error `E`, or
    /// [`AskError::Interrupted`] if the sender was dropped before replying.
    pub async fn recv<M>(self) -> Result<R, AskError<M, E>> {
        match self.rx.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(handler_err)) => Err(AskError::Handler(handler_err)),
            Err(_recv_error) => Err(AskError::Interrupted),
        }
    }
}

/// The asker had already dropped its receiver, so the reply went nowhere.
///
/// A unit signal, not the payload: a reply to a vanished asker is un-actionable
/// (nothing to retry, unlike the mailbox's returned `Signal`).
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("asker gone; reply discarded")]
pub struct AskerGone;

/// Builds a fresh reply channel: the sender for the handler, the receiver for the
/// waiting `ask`.
pub fn reply_channel<R, E>() -> (ReplySender<R, E>, ReplyReceiver<R, E>) {
    let (tx, rx) = oneshot::channel();
    (ReplySender { tx }, ReplyReceiver { rx })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use proptest::prelude::*;
    use tokio::{runtime::Builder, sync::Barrier};

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
        assert_eq!(
            recovered,
            Some(Conflict),
            "the domain error survives un-erased"
        );
    }

    /// Lifecycle: dropping the `ReplySender` without replying must surface
    /// `AskError::Interrupted` to the asker — and **return**, never hang. This is
    /// the card's central "drop → error, not a deadlock" guarantee.
    #[tokio::test]
    async fn dropping_sender_interrupts_the_ask() {
        let (tx, rx) = reply_channel::<u32, Conflict>();
        drop(tx);
        assert!(matches!(rx.recv::<()>().await, Err(AskError::Interrupted)));
    }

    /// Defensive: if the asker dropped its receiver (ask abandoned), the
    /// handler's `send`/`send_err` report `AskerGone` rather than deadlocking or
    /// panicking. The reply is discarded — un-actionable, so no payload returns.
    #[tokio::test]
    async fn send_to_gone_asker_reports_asker_gone() {
        let (send_tx, send_rx) = reply_channel::<u32, Conflict>();
        drop(send_rx);
        assert_eq!(send_tx.send(9), Err(AskerGone));

        let (err_tx, err_rx) = reply_channel::<u32, Conflict>();
        drop(err_rx);
        assert_eq!(err_tx.send_err(Conflict), Err(AskerGone));
    }

    /// A `tell` carries no reply port and cannot fail with a domain error, so its
    /// reply type is `E = Infallible`: `send_err` is uncallable (Infallible is
    /// uninhabited — there is no value to pass), and only the `Ok` path exists.
    /// This pins that the Infallible-defaulted channel roundtrips a plain value.
    #[tokio::test]
    async fn infallible_reply_has_no_error_path() {
        let (tx, rx) = reply_channel::<u32, Infallible>();
        tx.send(42).expect("asker still waiting");
        assert_eq!(rx.recv::<()>().await.ok(), Some(42));
    }

    /// Linearizability: a sender and a receiver race from the same instant on a
    /// multi-thread runtime; the exact sent value must arrive exactly once,
    /// whichever side wins the start. Real overlap (spawn + `Barrier`), not
    /// sequential-then-check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_send_and_recv_deliver_the_exact_value() {
        let (tx, rx) = reply_channel::<u64, Infallible>();
        let start = Arc::new(Barrier::new(2));

        let sender_start = Arc::clone(&start);
        let sender = tokio::spawn(async move {
            sender_start.wait().await;
            tx.send(0xABCD_1234).expect("receiver present");
        });
        let receiver = tokio::spawn(async move {
            start.wait().await;
            rx.recv::<()>().await
        });

        sender.await.expect("sender task");
        let got = receiver.await.expect("receiver task");
        assert_eq!(got.ok(), Some(0xABCD_1234), "the exact value arrives once");
    }

    /// Sequence (the *reverse* ordering): the receiver `recv`s and **parks** on an
    /// empty channel *before* any reply exists, then a later `send` must wake it
    /// with the value. Every other test sends before recv (value already buffered);
    /// this deterministically exercises the oneshot waker path instead. On a
    /// current-thread runtime, `yield_now` after the spawn guarantees the receiver
    /// has polled once and registered its waker before `send` runs.
    #[tokio::test(flavor = "current_thread")]
    async fn recv_parks_then_a_later_send_wakes_it() {
        let (tx, rx) = reply_channel::<u32, Infallible>();
        let receiver = tokio::spawn(async move { rx.recv::<()>().await });

        // Let the receiver task run to its await point and park on the empty channel.
        tokio::task::yield_now().await;

        // The receiver is parked (not gone): send must succeed and wake it.
        assert_eq!(
            tx.send(99),
            Ok(()),
            "receiver is parked and waiting, not gone"
        );
        assert_eq!(
            receiver.await.expect("recv task").ok(),
            Some(99),
            "the parked recv wakes with the value"
        );
    }

    /// The reply-outcome mapping holds for every handler action, driven under a
    /// single-thread runtime for deterministic, replayable interleaving. Each
    /// action pins exactly one arm of `recv`'s match; proptest sweeps all three.
    #[derive(Debug, Clone)]
    enum Action {
        Reply(u32),
        Fail,
        Drop,
    }

    proptest! {
        #[test]
        fn prop_reply_outcome_matches_action(
            action in prop_oneof![
                any::<u32>().prop_map(Action::Reply),
                Just(Action::Fail),
                Just(Action::Drop),
            ],
        ) {
            let rt = Builder::new_current_thread().build().expect("current-thread rt");
            rt.block_on(async {
                let (tx, rx) = reply_channel::<u32, Conflict>();
                match action.clone() {
                    Action::Reply(v) => { let _ = tx.send(v); }
                    Action::Fail => { let _ = tx.send_err(Conflict); }
                    Action::Drop => drop(tx),
                }
                let got = rx.recv::<()>().await;
                match action {
                    Action::Reply(v) => prop_assert_eq!(got.ok(), Some(v)),
                    Action::Fail => {
                        prop_assert_eq!(got.err().and_then(AskError::err), Some(Conflict));
                    }
                    Action::Drop => prop_assert!(matches!(got, Err(AskError::Interrupted))),
                }
                Ok(())
            })?;
        }
    }
}
