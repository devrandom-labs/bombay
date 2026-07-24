//! The actor run-loop (card #116): drive `on_start` → message loop → `on_stop`,
//! with a `catch_unwind` around each hook so a panic becomes an inspectable
//! `PanicError` instead of tearing down the task.

use std::{ops::ControlFlow, panic::AssertUnwindSafe};

use fastrand::Rng;
use futures::{FutureExt, StreamExt, stream::AbortHandle};
use tokio::time::Instant;
use tokio_util::{sync::CancellationToken, time::DelayQueue};

use crate::{
    actor::{
        Actor, ActorRef, Supervisor, Watch, WeakActorRef,
        supervision::{
            Child, ChildHandle, Children, Spawned, SuperviseReg, SupervisionOp, WatchInstaller,
            WatchOutcome,
        },
    },
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{ActorId, MailboxReceiver, Mailboxed, Signal},
    restart::{GiveUp, RestartVerdict, jittered_backoff, should_restart},
    watch::{LinkDied, LinkReceiver, LinkSender, WatchReg, Watchers},
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
    pub(super) link_rx: &'a LinkReceiver,
}

/// The supervised loop's working set beyond the actor-side refs
/// (`state`/`self_ref`/`handles`/`watchers`), grouped so the loop stays within
/// the argument budget (the `LinkedChannels`/`LoopHandles` pattern, #196): the
/// two selectable channels plus the loop-owned supervision state — the child
/// table, the restart-backoff queue, and the jitter RNG. All three are
/// task-owned (never in the user's `&mut self`), so a handler panic cannot tear
/// the supervision bookkeeping (crash-only recovery, #195's `Watchers` argument).
pub(super) struct SupervisedState<'a, A: Mailboxed> {
    pub(super) channels: LinkedChannels<'a, A>,
    pub(super) children: &'a mut Children,
    pub(super) retries: &'a mut DelayQueue<ActorId>,
    pub(super) rng: &'a mut Rng,
    /// The supervisor's own [`ActorId`] — names it as the watcher on every child
    /// edge the loop installs.
    pub(super) sup_id: ActorId,
    /// A clone of the supervisor's own link sender: both the watch registrations
    /// the loop enqueues on children and any synthetic self-notice ride it. It
    /// gates only the separate unbounded link channel, never the mailbox, so
    /// holding it in the loop does not defeat ref-count-driven stop (ADR-0003).
    pub(super) sup_link_tx: LinkSender,
}

/// The supervisor's identity as the loop uses it to watch a child: the id that
/// names it as the watcher, and the link sender the registration — and any
/// synthetic link-to-dead notice — travels on. Assembled once at the loop head
/// from [`SupervisedState`] so the watch-install helpers take one argument, not
/// two.
struct SupervisorRef {
    id: ActorId,
    link_tx: LinkSender,
}

impl SupervisorRef {
    /// The `link` registration to enqueue on a child: propagating (`linked =
    /// true`), because a supervisor MUST react to a child's death.
    fn watch_reg(&self) -> WatchReg {
        WatchReg {
            watcher: self.id,
            link_tx: self.link_tx.clone(),
            linked: true,
        }
    }

    /// Delivers the synthetic [`AlreadyDead`](ActorStopReason::AlreadyDead)
    /// notice `register_on` uses onto the supervisor's OWN link channel, so the
    /// next poll runs [`handle_child_death`] for a table-present `child` and
    /// rebuilds it. The same failure domain (Erlang's `noproc`): the child's true
    /// reason is unknowable once its mailbox is gone.
    fn synthesize_child_death(&self, child: ActorId) {
        // Unbounded link channel: the send fails only if the supervisor's own
        // receiver is gone — i.e. the supervisor is already stopping — in which
        // case the lost notice is moot.
        let _ = self.link_tx.try_send(LinkDied {
            id: child,
            reason: ActorStopReason::AlreadyDead,
            linked: true,
            cleanup_failed: false,
        });
    }
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
    watchers: &mut Watchers,
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
    watchers: &mut Watchers,
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
        // An unsupervised loop owns no child table, so there is nothing to apply
        // the op to. Reserved-arm shape, exactly as `LinkDied` was before #195
        // made it real: the supervised loop (the next slice of #196) is what
        // gives this signal an effect.
        Signal::Supervision(_) => ControlFlow::Continue(()),
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
    watchers: &mut Watchers,
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
    notice: LinkDied,
) -> ControlFlow<ActorStopReason> {
    // Exhaustive rather than `..`: `on_link_died`'s signature does not take
    // `cleanup_failed`, and binding every field means a future notice field
    // cannot be dropped here without a compile error.
    let LinkDied {
        id,
        reason,
        linked,
        cleanup_failed: _,
    } = notice;
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

/// The `Supervisor` run-loop (#196): the linked loop PLUS a restart-backoff arm.
/// Three `biased` arms, in priority order:
///
/// 1. **the link channel** — a death notice. A supervised child's drives the
///    restart policy ([`handle_child_death`]); any other peer's drives the
///    user's [`Watch::on_link_died`] hook (the #195 path, unchanged). Unlike the
///    linked loop, this arm needs no `link_open` disable flag: the loop holds a
///    clone of the supervisor's own link sender (to install child watch edges),
///    so the channel never reaches all-senders-gone and `recv_async` never spins
///    on a ready `Err`.
/// 2. **the restart-backoff queue** — a child's backoff deadline fired, so the
///    incarnation is rebuilt ([`rebuild_child`]). Disabled while
///    `retries.is_empty()`: `DelayQueue`'s stream yields `Ready(None)` on an
///    empty queue, which under `biased` would spin the select and starve the
///    mailbox — the identical hazard the `link_open` flag guards.
/// 3. **the message mailbox** — a [`Signal::Supervision`] mutates the child
///    table ([`apply_supervision_op`]); every other signal is the shared
///    [`handle_mailbox_step`], exactly as the plain and linked loops treat it.
///
/// Because a *waiting* child's deadline leaves arm 2 `Pending`, the supervisor
/// keeps serving its mailbox throughout a child's backoff — the whole reason the
/// delay is a select arm rather than an inline `sleep`.
pub(super) async fn run_supervised_message_loop<A: Supervisor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    handles: &LoopHandles,
    watchers: &mut Watchers,
    sup: SupervisedState<'_, A>,
) -> ActorStopReason {
    let SupervisedState {
        channels: LinkedChannels {
            mailbox_rx,
            link_rx,
        },
        children,
        retries,
        rng,
        sup_id,
        sup_link_tx,
    } = sup;
    let supervisor = SupervisorRef {
        id: sup_id,
        link_tx: sup_link_tx,
    };
    loop {
        tokio::select! {
            biased;
            death = link_rx.recv_async() => {
                // `SupervisorRef` holds a clone of the supervisor's OWN link
                // sender for the loop's whole life, so this channel always has a
                // sender: `recv_async` only ever yields a notice or stays pending,
                // and the all-senders-gone `Err` the linked loop must disable its
                // arm against is unreachable here — no `link_open` flag needed. On
                // the impossible `Err` the arm does nothing and the select waits
                // again (it cannot spin: `Err` requires zero senders).
                if let Ok(notice) = death {
                    // A supervised child's death drives restart policy silently;
                    // any other id is a peer this supervisor merely watches, whose
                    // death still reaches the user hook (the #195 path). The
                    // restart handler's own table lookup is the membership test —
                    // `None` means "not a child, route to the peer hook".
                    let flow = match handle_child_death(children, retries, rng, &notice) {
                        Some(flow) => flow,
                        None => handle_link_died(state, notice).await,
                    };
                    if let ControlFlow::Break(reason) = flow {
                        return reason;
                    }
                }
            }
            next_retry = retries.next(), if !retries.is_empty() => {
                if let Some(expired) = next_retry {
                    rebuild_child(children, &supervisor, expired.into_inner());
                }
            }
            maybe = handles.cancel.run_until_cancelled(mailbox_rx.recv()) => {
                match maybe.flatten() {
                    // The supervised loop gives `Supervision` an effect (the plain
                    // and linked loops ignore it): apply the table mutation here,
                    // so it never reaches `handle_mailbox_step`'s reserved arm.
                    Some(Signal::Supervision(op)) => apply_supervision_op(children, &supervisor, *op),
                    other => {
                        if let ControlFlow::Break(reason) =
                            handle_mailbox_step(state, self_ref, handles, watchers, other).await
                        {
                            return reason;
                        }
                    }
                }
            }
        }
    }
}

/// Applies one link death to the restart policy **iff** it names a supervised
/// child — the single table lookup doubles as the membership test.
///
/// `None`: `notice.id` is not a supervised child (a peer this supervisor merely
/// watches), so the caller routes it to the [`Watch::on_link_died`] hook (the
/// #195 path). `Some(flow)` is the restart decision for a real child —
/// `Continue` keeps the supervisor running (a rebuild was scheduled, or the child
/// is left dead), `Break(reason)` escalates: a budget tripped
/// ([`RestartLimitExceeded`](ActorStopReason::RestartLimitExceeded)), or the child
/// died in a lifecycle hook, a knowable crash loop that escalates *without*
/// scheduling a retry ([`ChildLifecycleFailed`](ActorStopReason::ChildLifecycleFailed)).
///
/// **Pure and synchronous**: it decides and *arms* (schedules a backoff deadline,
/// or breaks to escalate) and never awaits. Deciding and polling stay separate so
/// no restart decision hides inside a future poll where mutation testing cannot
/// reach it (the discipline from earlier cards). The lookup ends before the
/// function returns, so no borrow is held across the caller's peer-path await.
fn handle_child_death(
    children: &mut Children,
    retries: &mut DelayQueue<ActorId>,
    rng: &mut Rng,
    notice: &LinkDied,
) -> Option<ControlFlow<ActorStopReason>> {
    let child = children.get_mut(notice.id)?;
    // The live incarnation is gone; the entry survives (factory + accounting
    // persist across incarnations) but now holds no handle.
    child.handle = None;
    Some(match should_restart(child.config.policy, &notice.reason) {
        RestartVerdict::LeaveDead => ControlFlow::Continue(()),
        // A lifecycle-hook failure re-panics on the next incarnation: escalate at
        // once, bypassing both backoff and the counters.
        RestartVerdict::Escalate => {
            ControlFlow::Break(ActorStopReason::ChildLifecycleFailed { child: notice.id })
        }
        RestartVerdict::Restart => restart_or_give_up(child, retries, rng, notice.id),
    })
}

/// The restart-or-escalate half of [`handle_child_death`], split out so each
/// function stays under the cognitive-complexity bar: records the failure and
/// either arms a jittered backoff (`Continue`) or escalates on a tripped budget
/// (`Break(RestartLimitExceeded)`).
fn restart_or_give_up(
    child: &mut Child,
    retries: &mut DelayQueue<ActorId>,
    rng: &mut Rng,
    id: ActorId,
) -> ControlFlow<ActorStopReason> {
    match child.tracker.record_failure(&child.config, Instant::now()) {
        GiveUp::Yes { rebuilds } => ControlFlow::Break(ActorStopReason::RestartLimitExceeded {
            child: id,
            rebuilds,
        }),
        GiveUp::No { attempt } => {
            let delay = jittered_backoff(&child.config, attempt, rng);
            retries.insert(id, delay);
            ControlFlow::Continue(())
        }
    }
}

/// Rebuilds one child after its backoff deadline fires: runs the erased,
/// spawn-only factory for a **fresh** incarnation (a new [`ActorId`]), re-keys the
/// table entry to it, installs the supervisor's watch edge on the new
/// incarnation, and re-arms the healthy-uptime clock. A rebuilt child is a new
/// actor, never the resumed corpse (crash-only recovery); its death arrives under
/// the new id, which is why the table is re-keyed.
///
/// **Watch-after-rekey** (the #196 registration-hazard fix, applied to the
/// rebuild path too): the edge is installed only once `new_id` is in the table,
/// so a death cannot be observed for it before the table holds it. Synchronous —
/// the factory no longer awaits (watch-install left it) — so no borrow of
/// `children` is held across an await.
///
/// A miss — `old_id` no longer in the table — is a reported no-op: an
/// `unsupervise`/`stop_child` can remove the entry while the deadline is pending,
/// and that race must not resurrect it (the #195 `Unwatch`-race carry-forward).
fn rebuild_child(children: &mut Children, sup: &SupervisorRef, old_id: ActorId) {
    // Call the factory under a borrow that ends where the `map` returns: it hands
    // back an owned `Spawned`, so no borrow of `children` outlives this line and
    // the re-key below is free to reborrow the table.
    let Some(Spawned {
        handle,
        install_watch,
    }) = children.get_mut(old_id).map(|child| (child.factory)())
    else {
        return;
    };
    let new_id = handle.id();
    // Re-key BEFORE watching or storing the handle. A raced removal
    // (`unsupervise`/`stop_child` between the deadline and here) makes `rekey` a
    // no-op, and the fresh incarnation is left unsupervised rather than
    // re-inserted under a stale key.
    if children.rekey(old_id, new_id) {
        install_child_watch(sup, &handle, install_watch);
        if let Some(child) = children.get_mut(new_id) {
            child.handle = Some(handle);
            child.tracker.record_started(Instant::now());
        }
    }
}

/// Installs the supervisor's watch edge on a freshly-spawned child — the caller
/// guarantees the child is ALREADY in the table, which is the whole of the #196
/// registration-hazard fix: a death cannot be observed for an id the table holds,
/// then routed to the peer-watch hook that would kill the supervisor.
///
/// The install is a single non-blocking `try_send` (inside `install_watch`),
/// never an await, so a slow child can never stall the loop. Its three outcomes:
///
/// - [`Installed`](WatchOutcome::Installed): the edge is live; done.
/// - [`Closed`](WatchOutcome::Closed): the child died in its unwatched window, so
///   its own notice never reached us — synthesize the `AlreadyDead` one, which the
///   next poll turns into a restart (the child is table-present).
/// - [`Full`](WatchOutcome::Full): the child was flooded before we could watch it.
///   A bounded wait here would stall ALL supervision, so the child is killed and
///   synthesized as an immediate failed incarnation; the restart policy then
///   rebuilds it (or, under `Never`, leaves it dead — the caller's intent).
fn install_child_watch(sup: &SupervisorRef, handle: &ChildHandle, install_watch: WatchInstaller) {
    match install_watch(sup.watch_reg()) {
        WatchOutcome::Installed => {}
        WatchOutcome::Full => {
            handle.cancel.cancel();
            handle.abort.abort();
            sup.synthesize_child_death(handle.id);
        }
        WatchOutcome::Closed => sup.synthesize_child_death(handle.id),
    }
}

/// Applies a child-table [`SupervisionOp`] that arrived on the supervisor's own
/// mailbox. The table is task-owned, so this is its ONLY writer — no lock, and
/// no ordering rule beyond the mailbox's FIFO.
fn apply_supervision_op(children: &mut Children, sup: &SupervisorRef, op: SupervisionOp) {
    match op {
        // Insert FIRST, then install the watch edge on the first incarnation:
        // once the table holds `id`, a death for it routes to the restart policy,
        // never to the peer-watch hook (the #196 registration-hazard fix). The
        // handle is cloned out before the move so the watch-install can name the
        // child after `child` is consumed by `insert`.
        SupervisionOp::Add(reg) => {
            let SuperviseReg {
                child,
                id,
                install_watch,
            } = reg;
            let first_handle = child.handle.clone();
            children.insert(id, child);
            if let Some(handle) = first_handle {
                install_child_watch(sup, &handle, install_watch);
            }
        }
        // Drop the supervision edge; the child keeps running, now unwatched.
        SupervisionOp::Remove(id) => {
            children.remove(id);
        }
        // Drop the edge AND stop the child. Provisional crash-only stop here
        // (`cancel` then `abort`); the graceful `stop_grace` window *between* them
        // — and the escalation sweep that reuses it — is the next slice of #196.
        SupervisionOp::Stop(id) => {
            if let Some(child) = children.remove(id)
                && let Some(handle) = child.handle
            {
                handle.cancel.cancel();
                handle.abort.abort();
            }
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

#[cfg(test)]
mod supervised_tests {
    use core::time::Duration;

    use futures::stream::AbortHandle;
    use tokio::time::Instant;
    use tokio_util::{sync::CancellationToken, time::DelayQueue};

    use super::{
        SupervisorRef, apply_supervision_op, handle_child_death, install_child_watch, rebuild_child,
    };
    use crate::{
        actor::supervision::{
            Child, ChildHandle, Children, RebuildFactory, Spawned, SuperviseReg, SupervisionOp,
            WatchInstaller, WatchOutcome, watch_installer,
        },
        error::{ActorStopReason, PanicError, PanicReason},
        mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed, Signal},
        message::Msg,
        restart::{RestartConfig, RestartPolicy, RestartTracker},
        watch::{LinkDied, LinkReceiver},
    };
    use core::ops::ControlFlow;

    /// A minimal actor purely to key a real mailbox in the watch-install tests —
    /// its `Msg` is never handled here, only enqueued and drained.
    struct Probe;
    #[derive(Debug)]
    struct ProbeMsg;
    impl Msg for ProbeMsg {}
    impl Mailboxed for Probe {
        type Msg = ProbeMsg;
    }

    fn cap(n: usize) -> Capacity {
        Capacity::try_from(n).expect("valid test capacity")
    }

    /// A throwaway [`ChildHandle`] — the decision tests never actually stop
    /// anything, so the stop edges are inert.
    fn handle(id: ActorId) -> ChildHandle {
        let (abort, _reg) = AbortHandle::new_pair();
        ChildHandle {
            id,
            cancel: CancellationToken::new(),
            abort,
        }
    }

    /// A no-op watch installer that claims success without touching a mailbox —
    /// the table/decision tests never install a real edge.
    fn noop_installer() -> WatchInstaller {
        Box::new(|_reg| WatchOutcome::Installed)
    }

    /// A [`SupervisorRef`] plus the receiver a synthesized notice lands on, so a
    /// test can both drive `install_child_watch` and observe what it delivered.
    fn supervisor(id: ActorId) -> (SupervisorRef, LinkReceiver) {
        let (link_tx, link_rx) = flume::unbounded();
        (SupervisorRef { id, link_tx }, link_rx)
    }

    /// A live child entry under `config`, its current incarnation freshly
    /// started at `started`.
    fn child(config: RestartConfig, started: Instant) -> Child {
        Child {
            factory: Box::new(move || Spawned {
                handle: handle(ActorId::new(999)),
                install_watch: noop_installer(),
            }),
            handle: Some(handle(ActorId::new(1))),
            config,
            tracker: RestartTracker::new(started),
        }
    }

    fn panicked(reason: PanicReason) -> ActorStopReason {
        ActorStopReason::Panicked(PanicError::new(Box::new("boom"), reason))
    }

    fn notice(id: ActorId, reason: ActorStopReason) -> LinkDied {
        LinkDied {
            id,
            reason,
            linked: true,
            cleanup_failed: false,
        }
    }

    fn one_child(config: RestartConfig) -> (Children, ActorId) {
        let id = ActorId::new(1);
        let mut children = Children::new();
        children.insert(id, child(config, Instant::now()));
        (children, id)
    }

    /// A `Never` child's abnormal death is left dead: the loop keeps running, the
    /// entry is retained with no live handle, and no rebuild is scheduled.
    #[tokio::test]
    async fn leave_dead_retains_entry_and_schedules_nothing() {
        let (mut children, id) = one_child(RestartConfig::new(RestartPolicy::Never));
        let mut retries = DelayQueue::new();
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut retries,
            &mut rng,
            &notice(id, ActorStopReason::Killed),
        );

        assert!(
            matches!(flow, Some(ControlFlow::Continue(()))),
            "Never leaves the child dead and keeps the supervisor running",
        );
        assert!(retries.is_empty(), "no rebuild was scheduled");
        assert!(
            children
                .get_mut(id)
                .expect("entry retained")
                .handle
                .is_none(),
            "the dead incarnation's handle is cleared",
        );
    }

    /// A death notice for an id the table never held is `None` — the single
    /// lookup IS the membership test — so the caller routes it to the peer-watch
    /// hook (the #195 path) instead of the restart machinery.
    #[tokio::test]
    async fn a_non_child_death_is_none_and_arms_nothing() {
        let (mut children, _id) = one_child(RestartConfig::new(RestartPolicy::Permanent));
        let mut retries = DelayQueue::new();
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut retries,
            &mut rng,
            &notice(ActorId::new(999), ActorStopReason::Killed),
        );

        assert!(
            flow.is_none(),
            "a peer this supervisor merely watches is not handled by restart policy",
        );
        assert!(
            retries.is_empty(),
            "no rebuild is scheduled for a non-child"
        );
    }

    /// A lifecycle-hook panic escalates immediately with
    /// [`ActorStopReason::ChildLifecycleFailed`] — a knowable crash loop — and
    /// bypasses backoff: no retry is scheduled. Distinct from a budget trip.
    #[tokio::test]
    async fn lifecycle_hook_death_escalates_without_scheduling_a_retry() {
        let (mut children, id) = one_child(RestartConfig::new(RestartPolicy::Permanent));
        let mut retries = DelayQueue::new();
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut retries,
            &mut rng,
            &notice(id, panicked(PanicReason::OnStart)),
        );

        assert!(
            matches!(
                flow,
                Some(ControlFlow::Break(ActorStopReason::ChildLifecycleFailed { child })) if child == id
            ),
            "an on_start panic escalates as ChildLifecycleFailed, got {flow:?}",
        );
        assert!(
            retries.is_empty(),
            "a lifecycle-hook escalation bypasses backoff — no retry armed",
        );
    }

    /// A restartable death under budget schedules a backoff (arm the retry queue)
    /// and keeps the supervisor running.
    #[tokio::test]
    async fn restartable_death_arms_a_backoff_retry() {
        let (mut children, id) = one_child(RestartConfig::new(RestartPolicy::Permanent));
        let mut retries = DelayQueue::new();
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut retries,
            &mut rng,
            &notice(id, panicked(PanicReason::HandlerPanic)),
        );

        assert!(
            matches!(flow, Some(ControlFlow::Continue(()))),
            "a handler panic under budget keeps the supervisor running",
        );
        assert_eq!(retries.len(), 1, "exactly one rebuild was scheduled");
    }

    /// A trip of the restart budget escalates with
    /// [`ActorStopReason::RestartLimitExceeded`], carrying the lifetime rebuild
    /// count, and schedules no further retry. `max_restarts = 0` makes the very
    /// first failure the one-too-many.
    #[tokio::test]
    async fn budget_trip_escalates_restart_limit_exceeded() {
        let config = RestartConfig::new(RestartPolicy::Permanent).with_max_restarts(0);
        let (mut children, id) = one_child(config);
        let mut retries = DelayQueue::new();
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut retries,
            &mut rng,
            &notice(id, ActorStopReason::Killed),
        );

        assert!(
            matches!(
                flow,
                Some(ControlFlow::Break(ActorStopReason::RestartLimitExceeded { child, rebuilds }))
                    if child == id && rebuilds == 1
            ),
            "the first failure trips a zero budget as RestartLimitExceeded, got {flow:?}",
        );
        assert!(retries.is_empty(), "an escalation arms no retry");
    }

    /// `Add` installs a child under its id; `Remove` drops the edge but leaves
    /// the child running (the entry is gone, no stop signal fired); `Stop` drops
    /// the edge and cancels + aborts the child's stop edges.
    #[tokio::test]
    async fn supervision_ops_mutate_the_table() {
        let (sup, _link_rx) = supervisor(ActorId::new(100));
        let mut children = Children::new();
        let id = ActorId::new(1);
        apply_supervision_op(
            &mut children,
            &sup,
            SupervisionOp::Add(SuperviseReg {
                child: child(RestartConfig::new(RestartPolicy::Permanent), Instant::now()),
                id,
                install_watch: noop_installer(),
            }),
        );
        assert!(children.get_mut(id).is_some(), "Add installs the child");

        // Stop: capture the child's stop edges before applying, then assert they fired.
        let stop_edges = {
            let entry = children.get_mut(id).expect("present");
            entry.handle.clone().expect("live incarnation")
        };
        apply_supervision_op(&mut children, &sup, SupervisionOp::Stop(id));
        assert!(children.get_mut(id).is_none(), "Stop drops the edge");
        assert!(
            stop_edges.cancel.is_cancelled(),
            "Stop cancels the child's graceful token",
        );
        assert!(
            stop_edges.abort.is_aborted(),
            "Stop aborts the child's task",
        );

        // Remove: the edge is dropped, but no stop edge is driven.
        let other = ActorId::new(2);
        apply_supervision_op(
            &mut children,
            &sup,
            SupervisionOp::Add(SuperviseReg {
                child: child(RestartConfig::new(RestartPolicy::Permanent), Instant::now()),
                id: other,
                install_watch: noop_installer(),
            }),
        );
        let survivor = {
            let entry = children.get_mut(other).expect("present");
            entry.handle.clone().expect("live incarnation")
        };
        apply_supervision_op(&mut children, &sup, SupervisionOp::Remove(other));
        assert!(children.get_mut(other).is_none(), "Remove drops the edge");
        assert!(
            !survivor.cancel.is_cancelled() && !survivor.abort.is_aborted(),
            "Remove leaves the child running — no stop edge is driven",
        );
    }

    /// The registration-hazard fix, happy path: installing the watch on an OPEN
    /// child enqueues the supervisor's propagating `link` edge onto the child's
    /// mailbox and synthesizes no death. The watcher on the enqueued reg is the
    /// supervisor — this is the edge that later carries the child's death back.
    #[tokio::test]
    async fn install_child_watch_enqueues_the_edge_on_an_open_child() {
        let (sup, link_rx) = supervisor(ActorId::new(100));
        let child_id = ActorId::new(9);
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), child_id);
        install_child_watch(&sup, &handle(child_id), watch_installer(tx));

        assert!(
            link_rx.try_recv().is_err(),
            "an installed edge self-heals nothing",
        );
        let signal = rx.drain().next().expect("the watch reg reached the child");
        let Signal::Watch(reg) = signal else {
            panic!("expected a queued Watch reg");
        };
        assert_eq!(
            reg.watcher,
            ActorId::new(100),
            "the supervisor is the watcher"
        );
        assert!(reg.linked, "a supervisor watches with a propagating link");
    }

    /// @bug (card #196 Task-10 review) The self-healing heart of the hazard fix: a
    /// child that died in its UNWATCHED window (spawn → loop insert) closed its
    /// mailbox, so `try_send` fails and the supervisor never received a real
    /// notice. `install_child_watch` must synthesize the `AlreadyDead` notice on
    /// the supervisor's own channel — a restart-worthy death — rather than drop
    /// it. FAILS if the closed-mailbox branch is a silent no-op: the child would
    /// then never restart, a permanently missed death.
    #[tokio::test]
    async fn install_child_watch_synthesizes_alreadydead_when_child_died_unwatched() {
        let (sup, link_rx) = supervisor(ActorId::new(100));
        let child_id = ActorId::new(7);
        let (tx, rx) = Mailbox::<Probe>::bounded(cap(4), child_id);
        drop(rx); // the child died before the loop could watch it -> mailbox closed
        install_child_watch(&sup, &handle(child_id), watch_installer(tx));

        let notice = link_rx
            .try_recv()
            .expect("a lost unwatched death must be synthesized, never dropped");
        assert_eq!(
            notice.id, child_id,
            "the synthetic notice names the dead child"
        );
        assert!(
            matches!(notice.reason, ActorStopReason::AlreadyDead),
            "self-healed as AlreadyDead (Erlang noproc), got {:?}",
            notice.reason,
        );
        assert!(notice.linked, "a supervisor edge is a propagating link");
    }

    /// A child flooded before the loop could watch it (`try_send` reports `Full`)
    /// is not waited on — a bounded wait would stall ALL supervision. It is killed
    /// (cancel + abort) and synthesized as an immediate `AlreadyDead` failure, so
    /// the restart policy rebuilds it. Proves the loop never blocks on a full
    /// child mailbox.
    #[tokio::test]
    async fn install_child_watch_kills_and_synthesizes_when_child_mailbox_full() {
        let (sup, link_rx) = supervisor(ActorId::new(100));
        let child_id = ActorId::new(8);
        let (tx, _rx) = Mailbox::<Probe>::bounded(cap(1), child_id);
        tx.try_send(Signal::Stop).expect("the one slot fills");
        let h = handle(child_id);
        install_child_watch(&sup, &h, watch_installer(tx));

        assert!(h.cancel.is_cancelled(), "a flooded child is cancelled...");
        assert!(h.abort.is_aborted(), "...and hard-aborted, never waited on");
        let notice = link_rx
            .try_recv()
            .expect("the killed incarnation is synthesized as a death");
        assert_eq!(notice.id, child_id);
        assert!(matches!(notice.reason, ActorStopReason::AlreadyDead));
    }

    /// @bug (card #196 Task-10 review) The rebuild-path half of the hazard fix:
    /// `rebuild_child` re-keys the table to the fresh incarnation and THEN installs
    /// the watch edge on it — so a death can never precede the table entry. Proven
    /// by draining the rebuilt child's mailbox and finding the supervisor's watch
    /// reg. FAILS if the rebuild ever installs the edge before (or instead of) the
    /// re-key, or skips it.
    #[tokio::test]
    async fn rebuild_installs_the_watch_edge_on_the_rebuilt_incarnation() {
        use std::sync::{Arc, Mutex};

        let (sup, sup_link_rx) = supervisor(ActorId::new(100));
        let new_id = ActorId::new(50);
        // The factory stashes the fresh child's receiver so the test can prove the
        // rebuilt incarnation actually received the supervisor's watch reg.
        let stashed: Arc<Mutex<Option<MailboxReceiver<Probe>>>> = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&stashed);
        let factory: RebuildFactory = Box::new(move || {
            let (tx, rx) = Mailbox::<Probe>::bounded(cap(4), new_id);
            *slot.lock().expect("lock") = Some(rx);
            Spawned {
                handle: handle(new_id),
                install_watch: watch_installer(tx),
            }
        });
        let old_id = ActorId::new(1);
        let mut children = Children::new();
        children.insert(
            old_id,
            Child {
                factory,
                handle: None, // in the backoff window: no live incarnation
                config: RestartConfig::new(RestartPolicy::Permanent),
                tracker: RestartTracker::new(Instant::now()),
            },
        );

        rebuild_child(&mut children, &sup, old_id);

        assert!(
            children.get_mut(new_id).is_some(),
            "the table is re-keyed to the rebuilt id",
        );
        assert!(children.get_mut(old_id).is_none(), "the old key is gone");
        let mut guard = stashed.lock().expect("lock");
        let mut rx = guard
            .take()
            .expect("the factory spawned a fresh incarnation");
        let signal = rx
            .drain()
            .next()
            .expect("the watch reg reached the rebuilt child");
        let Signal::Watch(reg) = signal else {
            panic!("expected a queued Watch reg on the rebuilt child");
        };
        assert_eq!(
            reg.watcher,
            ActorId::new(100),
            "the supervisor watches the rebuilt incarnation",
        );
        assert!(
            sup_link_rx.try_recv().is_err(),
            "an open rebuild synthesizes no death",
        );
    }

    /// A backoff delay is bounded by the child's config: a restartable failure
    /// arms a retry whose deadline is within `min_backoff ..= max_backoff + jitter`.
    /// Guards against a rebuild that never fires (deadline never set) — a
    /// `start_paused` clock lets the assertion read the deadline exactly.
    #[tokio::test(start_paused = true)]
    async fn armed_backoff_deadline_is_within_the_configured_bounds() {
        let config = RestartConfig::new(RestartPolicy::Permanent)
            .with_min_backoff(Duration::from_millis(100))
            .with_max_backoff(Duration::from_secs(30));
        let (mut children, id) = one_child(config);
        let mut retries = DelayQueue::new();
        let mut rng = fastrand::Rng::with_seed(7);

        let before = Instant::now();
        let flow = handle_child_death(
            &mut children,
            &mut retries,
            &mut rng,
            &notice(id, ActorStopReason::Killed),
        );
        assert!(matches!(flow, Some(ControlFlow::Continue(()))));

        let expired = futures::StreamExt::next(&mut retries)
            .await
            .expect("the armed retry must fire");
        assert_eq!(expired.into_inner(), id, "the retry names the failed child");
        let waited = Instant::now().duration_since(before);
        assert!(
            waited >= Duration::from_millis(100),
            "first-attempt backoff is at least min_backoff, waited {waited:?}",
        );
        assert!(
            waited <= Duration::from_millis(120),
            "first-attempt backoff stays within min_backoff + 20% jitter, waited {waited:?}",
        );
    }
}
