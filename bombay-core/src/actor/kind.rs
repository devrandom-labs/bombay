//! The actor run-loop (card #116): drive `on_start` → message loop → `on_stop`,
//! with a `catch_unwind` around each hook so a panic becomes an inspectable
//! `PanicError` instead of tearing down the task.

use std::{ops::ControlFlow, panic::AssertUnwindSafe};

use futures::{FutureExt, stream::AbortHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, ActorRef, Watch, WeakActorRef},
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{MailboxReceiver, Mailboxed, Signal},
};

/// The loop's own copies of the cold lifecycle handles (ADR-0010): grouped so the
/// message loop stays within the argument budget and its linked sibling can reuse
/// them. `cancel` ends the loop out-of-band; both are cloned into a fresh
/// [`ActorRef`] to mint a drain-window handler ref when no external strong ref
/// survives.
pub(super) struct LoopHandles {
    pub(super) cancel: CancellationToken,
    pub(super) abort: AbortHandle,
}

/// The two channels the linked loop selects over, grouped so the loop stays
/// within the argument budget (the `LoopHandles` pattern, #195): the bounded
/// message mailbox and the actor's own UNBOUNDED link channel.
pub(super) struct LinkedChannels<'a, A: Mailboxed> {
    pub(super) mailbox_rx: &'a mut MailboxReceiver<A>,
    pub(super) link_rx: &'a crate::watch::LinkReceiver,
}

/// Runs the message loop until a stop condition, returning the stop reason.
///
/// `state` is the live actor; `self_ref` is its **weak** self-handle — the loop
/// deliberately holds no strong self-ref so that dropping the last external
/// [`ActorRef`] closes the mailbox and stops the actor (ref-count-driven stop,
/// #117). `handles` are the loop's own copies of the cold lifecycle handles, kept
/// for minting drain-window handler refs (ADR-0010). `watchers` is the task-owned
/// set of death-watchers this actor must notify on stop (card #195). The loop
/// finishes any in-flight handler before observing a graceful stop
/// ("finish-current-then-stop, no drain").
pub(super) async fn run_message_loop<A: Actor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    handles: &LoopHandles,
    mailbox_rx: &mut MailboxReceiver<A>,
    watchers: &mut crate::watch::Watchers,
) -> ActorStopReason {
    loop {
        let signal = handles
            .cancel
            .run_until_cancelled(mailbox_rx.recv())
            .await
            .flatten();
        if let ControlFlow::Break(reason) =
            handle_mailbox_step(state, self_ref, handles, watchers, signal).await
        {
            return reason;
        }
    }
}

/// Applies one mailbox poll result. `Break(reason)` is a terminal stop; `Continue`
/// keeps the loop going. Shared verbatim by the plain and linked loops so the two
/// treat every signal identically — the linked loop only *adds* a death arm, it
/// never diverges on the message side.
///
/// `signal` is the flattened result of
/// `cancel.run_until_cancelled(mailbox_rx.recv())`: `None` collapses both stop
/// cases — the cancel token firing (out-of-band graceful stop) and all strong
/// senders gone (all-senders-gone stop, reachable because the loop holds only a
/// weak self-ref) — into one clean normal stop, which is exactly how the loop
/// treats them.
async fn handle_mailbox_step<A: Actor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    handles: &LoopHandles,
    watchers: &mut crate::watch::Watchers,
    signal: Option<Signal<A>>,
) -> ControlFlow<ActorStopReason> {
    let Some(next) = signal else {
        return ControlFlow::Break(ActorStopReason::Normal);
    };
    match next {
        Signal::Message { msg, self_sender } => {
            // Steady state: share the external allocation — one CAS, no alloc.
            // Drain window (external refs gone; the dequeued self_sender is what
            // kept the message deliverable, ADR-0003): mint a fresh shared alloc
            // from that sender plus the loop's own cold copies (ADR-0010), with no
            // link channel — a rebuilt handler ref needs none. Either way the
            // handler's ref pins the actor while it is held.
            // TODO(#195 Q5): this drain-window ref carries `link_tx: None`, so if
            // handler-context self-watch is ever added, a self-`watch`/`link` here
            // would wrongly get `ActorNotLinked` — thread the actor's own link_tx
            // through `LoopHandles` if that capability lands.
            let actor_ref = self_ref.upgrade().unwrap_or_else(|| {
                ActorRef::new(
                    self_ref.id(),
                    self_sender,
                    handles.cancel.clone(),
                    handles.abort.clone(),
                    None,
                )
            });
            handle_message(state, actor_ref, self_ref, msg).await
        }
        // In-band graceful stop (FIFO): everything queued ahead was already handled.
        Signal::Stop => ControlFlow::Break(ActorStopReason::Normal),
        // Register/deregister a watcher on the task-owned guard. The guard's `Drop`
        // (in `run_lifecycle`) fires the death notices, so being watched is
        // universal and passive — every actor honors it.
        Signal::Watch(reg) => {
            watchers.apply(*reg);
            ControlFlow::Continue(())
        }
        Signal::Unwatch(id) => {
            watchers.remove(id);
            ControlFlow::Continue(())
        }
    }
}

/// The `Watch`-actor run-loop (#195): the plain message loop PLUS a second,
/// `biased`-first select arm draining the actor's UNBOUNDED link channel and
/// dispatching [`Watch::on_link_died`]. A `Break` from the hook (default: a linked
/// abnormal death) stops the actor with the propagated reason; an `Err`/panic from
/// the hook is a controlled crash tagged [`PanicReason::OnLinkDied`].
///
/// Death is handled before messages (`biased;`) so a failure is reacted to
/// promptly. The link arm is disabled once `recv_async` reports the channel closed:
/// with `biased` a ready `Err` would otherwise spin the select and starve the
/// mailbox arm. The channel closes only when the actor's own `link_tx` (in
/// `RefShared`) and every watcher-held clone are gone — which means all strong
/// `ActorRef`s have dropped, so the mailbox is closing too and the mailbox arm then
/// drives the imminent normal stop. Disabling loses nothing: no further death can
/// ever arrive on a closed channel.
pub(super) async fn run_linked_message_loop<A: Watch>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    handles: &LoopHandles,
    watchers: &mut crate::watch::Watchers,
    channels: LinkedChannels<'_, A>,
) -> ActorStopReason {
    let LinkedChannels {
        mailbox_rx,
        link_rx,
    } = channels;
    let mut link_open = true;
    loop {
        tokio::select! {
            biased;
            death = link_rx.recv_async(), if link_open => {
                match death {
                    Ok(notice) => {
                        if let ControlFlow::Break(reason) = handle_link_died(state, notice).await {
                            return reason;
                        }
                    }
                    // All link senders are gone: stop polling this arm so a ready
                    // `Err` cannot spin the biased select (see fn docs).
                    Err(_) => link_open = false,
                }
            }
            maybe = handles.cancel.run_until_cancelled(mailbox_rx.recv()) => {
                if let ControlFlow::Break(reason) =
                    handle_mailbox_step(state, self_ref, handles, watchers, maybe.flatten()).await
                {
                    return reason;
                }
            }
        }
    }
}

/// Runs [`Watch::on_link_died`] under `catch_unwind` and maps the outcome: the
/// hook's own `ControlFlow` on success, a terminal `Panicked(OnLinkDied)` on either
/// a returned `Err` (controlled crash) or a caught unwind.
async fn handle_link_died<A: Watch>(
    state: &mut A,
    notice: crate::watch::LinkDied,
) -> ControlFlow<ActorStopReason> {
    let crate::watch::LinkDied { id, reason, linked } = notice;
    let result = AssertUnwindSafe(state.on_link_died(id, reason, linked))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(flow)) => flow,
        Ok(Err(err)) => ControlFlow::Break(ActorStopReason::Panicked(PanicError::new(
            Box::new(err),
            PanicReason::OnLinkDied,
        ))),
        Err(payload) => ControlFlow::Break(ActorStopReason::Panicked(PanicError::from_panic_any(
            payload,
            PanicReason::OnLinkDied,
        ))),
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
