//! The actor run-loop (card #116): drive `on_start` → message loop → `on_stop`,
//! with a `catch_unwind` around each hook so a panic becomes an inspectable
//! `PanicError` instead of tearing down the task.

use std::{ops::ControlFlow, panic::AssertUnwindSafe};

use futures::FutureExt;

use crate::{
    actor::{Actor, ActorRef},
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{MailboxReceiver, Signal},
};

/// Runs the message loop until a stop condition, returning the stop reason.
///
/// `state` is the live actor; `actor_ref` is its strong self-handle (kept strong
/// in #116 — ref-count-driven stop is #117). The loop finishes any in-flight
/// handler before observing a graceful stop ("finish-current-then-stop, no
/// drain").
pub(crate) async fn run_message_loop<A: Actor>(
    state: &mut A,
    actor_ref: &ActorRef<A>,
    mailbox_rx: &mut MailboxReceiver<A>,
) -> ActorStopReason {
    let cancel = actor_ref.cancel_token();
    loop {
        match cancel.run_until_cancelled(mailbox_rx.recv()).await {
            // Token cancelled (out-of-band graceful stop).
            None => return ActorStopReason::Normal,
            // All senders dropped (unreachable in #116 — the loop holds one).
            Some(None) => return ActorStopReason::Normal,
            Some(Some(signal)) => match signal {
                Signal::Message(msg) => {
                    if let ControlFlow::Break(reason) = handle_message(state, actor_ref, msg).await
                    {
                        return reason;
                    }
                }
                // In-band graceful stop (FIFO): everything queued ahead was
                // already handled above.
                Signal::Stop => return ActorStopReason::Normal,
                // Nothing produces LinkDied pre-#120; ignore and keep running.
                Signal::LinkDied(_) => {}
            },
        }
    }
}

/// Handles one message under `catch_unwind`. `Continue` keeps looping; `Break`
/// carries the terminal stop reason.
async fn handle_message<A: Actor>(
    state: &mut A,
    actor_ref: &ActorRef<A>,
    msg: A::Msg,
) -> ControlFlow<ActorStopReason> {
    let mut stop = false;
    let result = AssertUnwindSafe(state.handle(msg, actor_ref.clone(), &mut stop))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(())) if stop => ControlFlow::Break(ActorStopReason::Normal),
        Ok(Ok(())) => ControlFlow::Continue(()),
        // A returned Err is a controlled crash: observe via on_panic, then stop.
        Ok(Err(err)) => {
            let panic = PanicError::new(Box::new(err), PanicReason::HandlerPanic);
            ControlFlow::Break(run_on_panic(state, actor_ref, panic).await)
        }
        // The handler unwound: catch, observe via on_panic, then stop.
        Err(payload) => {
            let panic = PanicError::from_panic_any(payload, PanicReason::HandlerPanic);
            ControlFlow::Break(run_on_panic(state, actor_ref, panic).await)
        }
    }
}

/// Runs `on_panic` (infallible, stop-only) under `catch_unwind`; if the hook
/// itself panics, that becomes the terminal reason instead.
async fn run_on_panic<A: Actor>(
    state: &mut A,
    actor_ref: &ActorRef<A>,
    err: PanicError,
) -> ActorStopReason {
    let weak = actor_ref.downgrade();
    match AssertUnwindSafe(state.on_panic(weak, err))
        .catch_unwind()
        .await
    {
        Ok(reason) => reason,
        Err(payload) => {
            ActorStopReason::Panicked(PanicError::from_panic_any(payload, PanicReason::OnPanic))
        }
    }
}
