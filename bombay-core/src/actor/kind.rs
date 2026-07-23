//! The actor run-loop (card #116): drive `on_start` → message loop → `on_stop`,
//! with a `catch_unwind` around each hook so a panic becomes an inspectable
//! `PanicError` instead of tearing down the task.

use std::{ops::ControlFlow, panic::AssertUnwindSafe};

use futures::{FutureExt, stream::AbortHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{MailboxReceiver, Signal},
};

/// Runs the message loop until a stop condition, returning the stop reason.
///
/// `state` is the live actor; `self_ref` is its **weak** self-handle — the loop
/// deliberately holds no strong self-ref so that dropping the last external
/// [`ActorRef`] closes the mailbox and stops the actor (ref-count-driven stop,
/// #117). `cancel`/`abort` are the loop's own copies of the cold lifecycle
/// handles, kept for minting drain-window handler refs (ADR-0010). The loop
/// finishes any in-flight handler before observing a graceful stop
/// ("finish-current-then-stop, no drain").
pub(super) async fn run_message_loop<A: Actor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    cancel: &CancellationToken,
    abort: &AbortHandle,
    mailbox_rx: &mut MailboxReceiver<A>,
) -> ActorStopReason {
    loop {
        match cancel.run_until_cancelled(mailbox_rx.recv()).await {
            // Either the cancel token fired (out-of-band graceful stop) or every
            // strong sender dropped (all-senders-gone — now reachable, since the
            // loop holds only a weak self-ref). Both are a clean, normal stop.
            None | Some(None) => return ActorStopReason::Normal,
            Some(Some(signal)) => match signal {
                Signal::Message { msg, self_sender } => {
                    // Steady state: share the external allocation — one CAS, no
                    // alloc. Drain window (external refs gone; the dequeued
                    // self_sender is what kept the message deliverable,
                    // ADR-0003): mint a fresh shared alloc from that sender
                    // plus the loop's own cold copies (ADR-0010). Either way
                    // the handler's ref pins the actor while it is held.
                    let actor_ref = self_ref.upgrade().unwrap_or_else(|| {
                        ActorRef::new(self_ref.id(), self_sender, cancel.clone(), abort.clone())
                    });
                    if let ControlFlow::Break(reason) =
                        handle_message(state, actor_ref, self_ref, msg).await
                    {
                        return reason;
                    }
                }
                // In-band graceful stop (FIFO): everything queued ahead was
                // already handled above.
                Signal::Stop => return ActorStopReason::Normal,
                // Watch/Unwatch registration handled by the loop's control path
                // (Task 6 wires the Watchers guard); until then, ignore to keep
                // the match total.
                Signal::Watch(_) | Signal::Unwatch(_) => {}
            },
        }
    }
}

/// Handles one message under `catch_unwind`. `Continue` keeps looping; `Break`
/// carries the terminal stop reason.
async fn handle_message<A: Actor>(
    state: &mut A,
    actor_ref: ActorRef<A>,
    self_ref: &WeakActorRef<A>,
    msg: A::Msg,
) -> ControlFlow<ActorStopReason> {
    let mut stop = false;
    let result = AssertUnwindSafe(state.handle(msg, actor_ref, &mut stop))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(())) if stop => ControlFlow::Break(ActorStopReason::Normal),
        Ok(Ok(())) => ControlFlow::Continue(()),
        // A returned Err is a controlled crash: observe via on_panic, then stop.
        Ok(Err(err)) => {
            let panic = PanicError::new(Box::new(err), PanicReason::HandlerPanic);
            ControlFlow::Break(run_on_panic(state, self_ref, panic).await)
        }
        // The handler unwound: catch, observe via on_panic, then stop.
        Err(payload) => {
            let panic = PanicError::from_panic_any(payload, PanicReason::HandlerPanic);
            ControlFlow::Break(run_on_panic(state, self_ref, panic).await)
        }
    }
}

/// Runs `on_panic` (infallible, stop-only) under `catch_unwind`; if the hook
/// itself panics, that becomes the terminal reason instead.
async fn run_on_panic<A: Actor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    err: PanicError,
) -> ActorStopReason {
    match AssertUnwindSafe(state.on_panic(self_ref.clone(), err))
        .catch_unwind()
        .await
    {
        Ok(reason) => reason,
        Err(payload) => {
            ActorStopReason::Panicked(PanicError::from_panic_any(payload, PanicReason::OnPanic))
        }
    }
}
