//! The actor run-loop (card #116): drive `on_start` → message loop → `on_stop`,
//! with a `catch_unwind` around each hook so a panic becomes an inspectable
//! `PanicError` instead of tearing down the task.

use core::time::Duration;
use std::{ops::ControlFlow, panic::AssertUnwindSafe};

use fastrand::Rng;
use futures::{
    FutureExt, StreamExt,
    stream::{AbortHandle, FuturesUnordered},
};
use smallvec::SmallVec;
use tokio::time::{Instant, sleep, sleep_until};
use tokio_util::{sync::CancellationToken, time::DelayQueue};

use crate::{
    actor::{
        Actor, ActorRef, Flow, LinkReact, SupervisedReact, WeakActorRef,
        supervision::{
            ArmedReg, ChildHandle, Children, CycleState, PendingAbort, Spawned, SuperviseReg,
            SupervisionOp, WatchInstaller, WatchOutcome,
        },
    },
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{ActorId, ControlSignal, MailboxReceiver, Mailboxed, Recv, Signal},
    restart::{GiveUp, RestartVerdict, SupervisionStrategy, jittered_backoff, should_restart},
    trace,
    watch::{LinkDied, LinkReceiver, LinkSender, WatchReg, Watchers},
};

/// The loop's own copies of the cold lifecycle handles (ADR-0010): grouped so the
/// message loop stays within the argument budget and its linked sibling can reuse
/// them. `cancel` ends the loop out-of-band; all three are cloned into a fresh
/// [`ActorRef`] to mint a drain-window handler ref when no external strong ref
/// survives.
pub(super) struct LoopHandles {
    pub(super) cancel: CancellationToken,
    pub(super) abort: AbortHandle,
    /// The loop's own cold copy of the actor's link-channel sender, cloned into
    /// the drain-window handler-ref mint so a handler-context `watch`/`link`
    /// there behaves exactly as in steady state (#260). `None` on the
    /// plain-spawn path (a plain actor has no link channel). Not a mailbox
    /// sender — it does not pin the ref-count stop (I-C).
    pub(super) link_tx: Option<LinkSender>,
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
    /// Deferred hard-kills for children stopped via `stop_child` (#196): each
    /// entry is a cancelled child's sender-less [`ChildHandle`], keyed on its
    /// `stop_grace` deadline. When the deadline fires the loop aborts the child —
    /// the crash-only backstop for a child that ignored the graceful `cancel`.
    /// Deferring the abort through this queue (rather than an inline
    /// `sleep(grace).await`) is what keeps the single-threaded loop serving every
    /// other child throughout one child's grace window. The queue owns
    /// [`PendingAbort`] handles; each one's [`Drop`] aborts the child if the
    /// supervisor exits before the deadline fires, truncating the remaining grace.
    pub(super) pending_aborts: &'a mut DelayQueue<PendingAbort>,
    /// The set-cycle coordinator (card #199, ADR-0014) — loop-owned like the
    /// child table it coordinates, so a handler panic cannot tear a cycle
    /// mid-teardown (crash-only recovery, the `Children`/`Watchers` argument).
    pub(super) cycle: &'a mut CycleState,
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
    /// The watch registration to enqueue on a child: **monitoring**, not linking
    /// (`linked = false`). A supervisor reacts to a child's death through its
    /// restart table ([`handle_child_death`], keyed purely on
    /// `should_restart(policy, reason)` — `notice.linked` is never read on that
    /// path), so a propagating `link` buys nothing while the child is supervised.
    /// It would only bite AFTER [`unsupervise`](crate::actor::ActorRef::unsupervise):
    /// the entry is gone, the death falls through to
    /// [`on_link_died`](crate::actor::LinkReact::on_link_died), and a propagating edge
    /// (`linked = true`) would make the default hook stop the supervisor on the
    /// detached child's abnormal death — a death the supervisor can never un-watch
    /// (its [`ChildHandle`](crate::actor::supervision::ChildHandle) is sender-less).
    /// Monitoring is Erlang's `monitor` (notify + react-via-table); linking is
    /// `link` (propagate-via-hook), which supervision is not.
    fn watch_reg(&self) -> WatchReg {
        WatchReg {
            watcher: self.id,
            link_tx: self.link_tx.clone(),
            linked: false,
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
            // Monitoring, not linking (matches `watch_reg`): inert today because
            // this notice always targets a table-present child that
            // `handle_child_death` decides without reading `linked`, but keeping it
            // `false` closes the latent hole where it could ever race a removal and
            // fall through to the propagating peer-hook path.
            linked: false,
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
    let mut last_fired: Option<Instant> = None;
    loop {
        let (armed, due) = arm_deadline(state, last_fired);
        // The plain loop's FIRST select (ADR-0025): the deadline arm sits
        // ABOVE the always-ready mailbox arm — below it, a due deadline
        // starves until the backlog fully drains (model P1b; the placement
        // is structural, not stylistic). Cancellation is observed inside
        // `poll_mailbox`, so a due deadline delays cancel observation by at
        // most one hook turn — bounded, pinned by test. v1 recreates
        // `sleep_until` per iteration (O(1) wheel ops — hierarchical timing
        // wheel, Varghese & Lauck, IEEE/ACM ToN 5(6) 1997); a pinned
        // `Sleep::reset` is the named optimization if the bench shows pain.
        tokio::select! {
            biased;
            () = deadline_sleep(due), if armed => {
                last_fired = Some(due);
                if let ControlFlow::Break(reason) = handle_deadline(state, self_ref).await {
                    return reason;
                }
            }
            poll = poll_mailbox(&handles.cancel, mailbox_rx) => {
                if let ControlFlow::Break(reason) =
                    handle_mailbox_step(state, self_ref, handles, watchers, poll).await
                {
                    return reason;
                }
            }
        }
    }
}

/// The deadline arm's per-iteration arming decision (ADR-0025 P-D1/P-D4):
/// re-reads the declarative slot each iteration — state changes only in
/// steps the loop itself runs, so this is exact — and disables the arm
/// when no deadline is declared or the current value already fired
/// (fires-once-per-value: the spin-hazard class the `DelayQueue` arms
/// guard with `is_empty()`). The fallback instant is never polled: a
/// disarmed arm is excluded from the select, and an unpolled `Sleep`
/// registers nothing with the timer wheel.
fn arm_deadline<A: Actor>(state: &A, last_fired: Option<Instant>) -> (bool, Instant) {
    let deadline = state.next_deadline();
    let armed = deadline.is_some() && deadline != last_fired;
    (armed, deadline.unwrap_or_else(Instant::now))
}

/// The deadline arm's future, one async-fn indirection on purpose:
/// `select!` CONSTRUCTS every arm's future even when its `if` guard is
/// false, and a `Sleep` built directly would grab the timer-driver handle
/// there — panicking a timer-less runtime's loop on every iteration (the
/// pinned timerless behavior is that only the `on_stop` bound needs the
/// driver). Calling an async fn only builds the state machine; the `Sleep`
/// inside is created — and registers with the wheel — on first poll, which
/// a disarmed arm never gets.
async fn deadline_sleep(due: Instant) {
    sleep_until(due).await;
}

/// Runs [`Actor::on_deadline`] under `catch_unwind` and maps the outcome
/// exactly as a handler step's: `Flow::Continue` keeps looping,
/// `Flow::Stop` is a `Normal` stop, and a returned `Err` (controlled
/// crash) or caught unwind is a terminal `Panicked` tagged
/// [`PanicReason::OnDeadline`] — handler-like, restart-eligible.
async fn handle_deadline<A: Actor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
) -> ControlFlow<ActorStopReason> {
    let result = AssertUnwindSafe(state.on_deadline(self_ref.clone()))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(Flow::Continue)) => ControlFlow::Continue(()),
        Ok(Ok(Flow::Stop)) => ControlFlow::Break(ActorStopReason::Normal),
        Ok(Err(err)) => ControlFlow::Break(ActorStopReason::Panicked(PanicError::new(
            Box::new(err),
            PanicReason::OnDeadline,
        ))),
        Err(payload) => ControlFlow::Break(ActorStopReason::Panicked(PanicError::from_panic_any(
            payload,
            PanicReason::OnDeadline,
        ))),
    }
}

/// One poll of the mailbox arm with the two stop causes kept distinct — the
/// split #253 needs: cancellation is a graceful stop (`Normal`), a closed
/// mailbox is ref-count collection (`Collected`, ADR-0020). Flattening them
/// into one `None` is exactly the bug that made collection restart-worthy.
pub(super) enum MailboxPoll<A: Mailboxed> {
    /// The cancel token fired: an out-of-band graceful stop.
    Cancelled,
    /// The mailbox closed: every strong sender is gone and the queue is
    /// drained (ADR-0003 drain-then-stop already happened).
    Closed,
    /// A control-lane signal (watch/supervision), merged ahead of the user
    /// backlog by `MailboxReceiver::recv` (card #225, ADR-0021).
    Control(ControlSignal),
    /// An ordinary user-lane signal.
    Signal(Signal<A>),
}

/// Awaits the next mailbox event under the cancel token and names which of
/// the outcomes happened. The one place the
/// `run_until_cancelled(recv())` nesting is interpreted.
async fn poll_mailbox<A: Mailboxed>(
    cancel: &CancellationToken,
    mailbox_rx: &mut MailboxReceiver<A>,
) -> MailboxPoll<A> {
    match cancel.run_until_cancelled(mailbox_rx.recv()).await {
        None => MailboxPoll::Cancelled,
        Some(None) => MailboxPoll::Closed,
        Some(Some(Recv::Control(signal))) => MailboxPoll::Control(signal),
        Some(Some(Recv::Signal(signal))) => MailboxPoll::Signal(signal),
    }
}

/// Applies one mailbox poll result. `Break(reason)` is a terminal stop; `Continue`
/// keeps the loop going. Shared verbatim by the plain and linked loops so the two
/// treat every signal identically — the linked loop only *adds* a death arm, it
/// never diverges on the message side.
///
/// `poll` distinguishes the two stop causes that `cancel.run_until_cancelled(recv())`
/// collapses into `None`: a cancel-token graceful stop (`Normal`) and a closed
/// mailbox from ref-count collection (`Collected`). `Signal::Stop` and a
/// handler's `Flow::Stop` remain `Normal`.
async fn handle_mailbox_step<A: Actor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    handles: &LoopHandles,
    watchers: &mut Watchers,
    poll: MailboxPoll<A>,
) -> ControlFlow<ActorStopReason> {
    let next = match poll {
        MailboxPoll::Cancelled => return ControlFlow::Break(ActorStopReason::Normal),
        MailboxPoll::Closed => return ControlFlow::Break(ActorStopReason::Collected),
        // Register/deregister a watcher on the task-owned guard. The guard's `Drop`
        // (in `run_lifecycle`) fires the death notices, so being watched is
        // universal and passive — every actor honors it. Control signals are
        // served ahead of the user backlog (ADR-0021), so a watch reaches a
        // full-mailbox actor without waiting on its queue.
        MailboxPoll::Control(ControlSignal::Watch(reg)) => {
            watchers.apply(*reg);
            return ControlFlow::Continue(());
        }
        MailboxPoll::Control(ControlSignal::Unwatch(id)) => {
            watchers.remove(id);
            return ControlFlow::Continue(());
        }
        // An unsupervised loop owns no child table, so there is nothing to apply
        // the op to. Reserved-arm shape, exactly as `LinkDied` was before #195
        // made it real: the supervised loop (the next slice of #196) is what
        // gives this signal an effect.
        MailboxPoll::Control(ControlSignal::Supervision(_)) => {
            return ControlFlow::Continue(());
        }
        MailboxPoll::Signal(next) => next,
    };
    match next {
        Signal::Message {
            msg,
            self_sender,
            ctx,
        } => {
            // Steady state: share the external allocation — one CAS, no alloc.
            // Drain window (external refs gone; the dequeued self_sender is what
            // kept the message deliverable, ADR-0003): mint a fresh shared alloc
            // from that sender plus the loop's own cold copies (ADR-0010). The
            // minted ref carries the loop's own cold copy of `link_tx`, so a
            // handler-context `watch`/`link` in the drain window behaves exactly
            // as in steady state (#260); on the plain-spawn path that copy is
            // `None` and `watch` still errs `ActorNotLinked`. Either way the
            // handler's ref pins the actor while it is held.
            let actor_ref = self_ref.upgrade().unwrap_or_else(|| {
                ActorRef::new(
                    self_ref.id(),
                    self_sender,
                    handles.cancel.clone(),
                    handles.abort.clone(),
                    handles.link_tx.clone(),
                )
            });
            let span: trace::Span = ctx.handle_span::<A>();
            trace::instrument(handle_message(state, actor_ref, self_ref, msg), span).await
        }
        // In-band graceful stop (FIFO on the USER lane, ADR-0021): everything
        // queued ahead was already handled. Control signals may overtake a
        // `Stop`, but a `Stop` never overtakes the messages queued before it.
        Signal::Stop => ControlFlow::Break(ActorStopReason::Normal),
    }
}

/// The linked run-loop (#195): the plain message loop PLUS a second,
/// `biased`-first select arm draining the actor's UNBOUNDED link channel and
/// dispatching [`LinkReact::on_link_died`]. A `Break` from the hook (default: a linked
/// abnormal death) stops the actor with the propagated reason; an `Err`/panic from
/// the hook is a controlled crash tagged [`PanicReason::OnLinkDied`].
///
/// Death is handled before messages (`biased;`) so a failure is reacted to
/// promptly. The link arm is disabled once `recv_async` reports the channel closed:
/// with `biased` a ready `Err` would otherwise spin the select and starve the
/// mailbox arm. On the production path that `Err` is unreachable: the loop's
/// [`LoopHandles`] holds a clone of the actor's own link sender for the loop's
/// whole life (#260), and this loop's one callsite (`run_lifecycle_linked`)
/// passes the actor's own receiver, so the channel never reaches
/// all-senders-gone while the loop lives. The `link_open` flag stays as a cheap
/// defensive guard against a mismatched, externally-constructed [`LinkReceiver`]
/// (a public alias) whose senders have all dropped — a shape only an in-module
/// test can construct. Disabling loses nothing there: no further death can ever
/// arrive on a closed channel.
pub(super) async fn run_linked_message_loop<A: LinkReact>(
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
    let mut last_fired: Option<Instant> = None;
    loop {
        let (armed, due) = arm_deadline(state, last_fired);
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
            // The deadline arm (ADR-0025): below the link arm — a ready
            // death notice beats a due deadline — and above the mailbox
            // (model P1). No existing inter-arm relation changes.
            () = deadline_sleep(due), if armed => {
                last_fired = Some(due);
                if let ControlFlow::Break(reason) = handle_deadline(state, self_ref).await {
                    return reason;
                }
            }
            maybe = poll_mailbox(&handles.cancel, mailbox_rx) => {
                if let ControlFlow::Break(reason) =
                    handle_mailbox_step(state, self_ref, handles, watchers, maybe).await
                {
                    return reason;
                }
            }
        }
    }
}

/// Runs [`LinkReact::on_link_died`] under `catch_unwind` and maps the outcome: the
/// hook's own `ControlFlow` on success, a terminal `Panicked(OnLinkDied)` on either
/// a returned `Err` (controlled crash) or a caught unwind.
async fn handle_link_died<A: LinkReact>(
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

/// The supervised run-loop (#196): the linked loop PLUS a restart-backoff arm.
/// Three `biased` arms, in priority order:
///
/// 1. **the link channel** — a death notice. A supervised child's drives the
///    restart policy ([`handle_child_death`]); any other peer's drives the
///    user's [`LinkReact::on_link_died`] hook (the #195 path, unchanged). Unlike the
///    linked loop, this arm needs no `link_open` disable flag: the loop holds a
///    clone of the supervisor's own link sender (to install child watch edges),
///    so the channel never reaches all-senders-gone and `recv_async` never spins
///    on a ready `Err`.
/// 2. **the restart-backoff queue** — a child's backoff deadline fired, so the
///    incarnation is rebuilt ([`rebuild_child`]). Disabled while
///    `retries.is_empty()`: `DelayQueue`'s stream yields `Ready(None)` on an
///    empty queue, which under `biased` would spin the select and starve the
///    mailbox — the identical hazard the `link_open` flag guards.
/// 3. **the message mailbox** — a [`ControlSignal::Supervision`] mutates the
///    child table ([`apply_supervision_op`]); every other signal is the shared
///    [`handle_mailbox_step`], exactly as the plain and linked loops treat it.
///    Control signals are merged ahead of the user backlog inside `recv`
///    (ADR-0021), so a `supervise` op never queues behind the supervisor's own
///    message traffic.
///
/// Because a *waiting* child's deadline leaves arm 2 `Pending`, the supervisor
/// keeps serving its mailbox throughout a child's backoff — the whole reason the
/// delay is a select arm rather than an inline `sleep`.
pub(super) async fn run_supervised_message_loop<A: SupervisedReact>(
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
        pending_aborts,
        cycle,
        rng,
        sup_id,
        sup_link_tx,
    } = sup;
    let supervisor = SupervisorRef {
        id: sup_id,
        link_tx: sup_link_tx,
    };
    let strategy = A::strategy();
    let mut last_fired: Option<Instant> = None;
    loop {
        let (armed, due) = arm_deadline(state, last_fired);
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
                if let Ok(notice) = death
                    && let ControlFlow::Break(reason) = dispatch_death(
                        state,
                        children,
                        &mut SetCycleCtx::new(retries, pending_aborts, cycle),
                        strategy,
                        rng,
                        notice,
                    )
                    .await
                {
                    return reason;
                }
            }
            next_retry = retries.next(), if !retries.is_empty() => {
                if let Some(expired) = next_retry {
                    // A cycle's rebuild deadline is matched by KEY (its carried id
                    // is incidental); everything else is a solo `OneForOne` backoff
                    // for the id it carries. `cycling_rebuild_ids` clears the flags
                    // first, so the cycle's own rebuilds pass `rebuild_child`'s
                    // cycling guard while stale solo strays do not.
                    if matches!(cycle, CycleState::Waiting { key } if *key == expired.key()) {
                        *cycle = CycleState::Idle;
                        for id in children.cycling_rebuild_ids() {
                            rebuild_child(children, &supervisor, id);
                        }
                    } else {
                        rebuild_child(children, &supervisor, expired.into_inner());
                    }
                }
            }
            // The deferred-abort backstop for `stop_child`: a child's grace
            // deadline fired, so the cancelled-but-still-running incarnation is now
            // hard-aborted. Disabled while empty for the same `Ready(None)`-spin
            // reason as the retries arm. Lowest housekeeping priority, so a ready
            // message or death is still served first.
            expired_abort = pending_aborts.next(), if !pending_aborts.is_empty() => {
                // Dropping the expired PendingAbort aborts the child via its
                // Drop impl; explicit abort is not required.
                drop(expired_abort);
            }
            // The deadline arm (ADR-0025): below every housekeeping arm — a
            // ready death notice, due rebuild, or due abort beats a due
            // deadline — and above the mailbox (model P1). No existing
            // inter-arm relation changes.
            () = deadline_sleep(due), if armed => {
                last_fired = Some(due);
                if let ControlFlow::Break(reason) = handle_deadline(state, self_ref).await {
                    return reason;
                }
            }
            maybe = poll_mailbox(&handles.cancel, mailbox_rx) => {
                match maybe {
                    MailboxPoll::Control(ControlSignal::Supervision(op)) => apply_supervision_op(
                        children,
                        &supervisor,
                        &mut SetCycleCtx::new(retries, pending_aborts, cycle),
                        *op,
                    ),
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

/// Routes one link death notice to the restart policy iff it names a supervised
/// child; any other id is a peer and reaches the user's `on_link_died` hook.
///
/// A supervised child's death drives restart policy silently; any other id is a
/// peer this supervisor merely watches, whose death still reaches the user hook
/// (the #195 path). [`handle_child_death`]'s own table lookup is the membership
/// test — `None` means "not a child, route to the peer hook".
///
/// All exit paths, including the #195 peer-death path, now rely on the lifecycle
/// epilogue sweep in `run_lifecycle_supervised` to tear down surviving children
/// (ADR-0019). The `dispatch_death` helper no longer contains a sweep site; it
/// only decides whether this notice keeps the loop running or breaks it.
#[expect(
    clippy::too_many_arguments,
    reason = "`state` and `children` are actor-side handles, not set-cycle \
              coordinator state, so they stay separate from the SetCycleCtx bundle \
              (which folds retries/pending_aborts/cycle into one borrow); even so \
              the death path is 6 args"
)]
async fn dispatch_death<A: SupervisedReact>(
    state: &mut A,
    children: &mut Children,
    ctx: &mut SetCycleCtx<'_>,
    strategy: SupervisionStrategy,
    rng: &mut Rng,
    notice: LinkDied,
) -> ControlFlow<ActorStopReason> {
    match handle_child_death(children, ctx, strategy, rng, &notice) {
        Some(ControlFlow::Break(reason)) => ControlFlow::Break(reason),
        Some(ControlFlow::Continue(())) => ControlFlow::Continue(()),
        None => handle_link_died(state, notice).await,
    }
}

/// The single non-skippable supervisor-exit sweep (ADR-0019): cancel every live
/// child, then join their death notices on the supervisor's own `link_rx` — the
/// watch edges installed at spawn already carry those notices. Per-child bound is
/// its own `stop_grace`, graces run concurrently (the sweep is bounded by the
/// largest grace, not their sum). A child whose grace fires before its notice is
/// hard-aborted; post-abort confirmation is bounded by the supervisor's
/// `on_stop_grace`, after which a missing notice is traced and the
/// supervisor exits anyway — a non-yielding child must not wedge the supervisor.
/// A child in a backoff window has no live handle and is skipped by
/// [`drain_live_handles`](Children::drain_live_handles).
pub(super) async fn teardown_children(
    children: &mut Children,
    link_rx: &LinkReceiver,
    on_stop_grace: Duration,
) {
    let handles = children.drain_live_handles();
    if handles.is_empty() {
        return;
    }

    // Cancel every live child up front, then wait for their death notices.
    let mut pending: SmallVec<[(ActorId, ChildHandle); 4]> = SmallVec::new();
    let mut grace_futs = FuturesUnordered::new();
    for (handle, grace) in handles {
        handle.cancel.cancel();
        let id = handle.id;
        pending.push((id, handle));
        grace_futs.push(async move {
            sleep(grace).await;
            id
        });
    }

    let mut aborted: SmallVec<[(ActorId, ChildHandle); 4]> = SmallVec::new();

    // Join phase: drain the link channel until every child either confirms its
    // death or exceeds its grace. Non-child notices (peers, duplicates) are
    // discarded — the loop is exiting and no longer routes them.
    while !pending.is_empty() {
        tokio::select! {
            biased;
            recv = link_rx.recv_async() => {
                if let Ok(death_notice) = recv {
                    if let Some(idx) = pending.iter().position(|(id, _)| *id == death_notice.id) {
                        pending.swap_remove(idx);
                    }
                } else {
                    // `sup_link_tx` lives inside the abortable region and is
                    // dropped by sweep time; once the channel is drained, no
                    // more notices will arrive. Abort every remaining child
                    // and proceed to post-abort confirmation.
                    for (id, handle) in pending.drain(..) {
                        handle.abort.abort();
                        aborted.push((id, handle));
                    }
                }
            }
            Some(expired_id) = grace_futs.next() => {
                if let Some(idx) = pending.iter().position(|(id, _)| *id == expired_id) {
                    let (id, handle) = pending.swap_remove(idx);
                    handle.abort.abort();
                    aborted.push((id, handle));
                }
            }
        }
    }

    // Post-abort confirmation: aborted children must still announce their death
    // so the supervisor's own watchers can observe a clean teardown. Bounded by
    // `on_stop_grace`; a non-yielding child may never produce a notice, so
    // trace the missing ones and proceed rather than wedging the exit.
    if !aborted.is_empty() {
        let mut bound = std::pin::pin!(sleep(on_stop_grace));
        loop {
            tokio::select! {
                biased;
                () = &mut bound => {
                    for (id, _) in &aborted {
                        trace::child_teardown_abandoned(*id, on_stop_grace);
                    }
                    break;
                }
                recv = link_rx.recv_async() => {
                    if let Ok(death_notice) = recv {
                        if let Some(idx) = aborted.iter().position(|(id, _)| *id == death_notice.id) {
                            aborted.swap_remove(idx);
                            if aborted.is_empty() {
                                break;
                            }
                        }
                    } else {
                        for (id, _) in &aborted {
                            trace::child_teardown_abandoned(*id, on_stop_grace);
                        }
                        break;
                    }
                }
            }
        }
    }
}

/// The set-cycle coordinator's mutable working set (card #199, ADR-0014),
/// threaded as one borrow to the decision helpers so they stay under the argument
/// budget without hiding the loop's disjoint field borrows. It keeps three
/// separate fields rather than collapsing to one queue because the supervised
/// loop's `select!` arms still poll `retries` and `pending_aborts` independently
/// and mutate `cycle` directly — the ctx is constructed transiently only inside
/// the death and supervision arms, never held across a poll.
struct SetCycleCtx<'a> {
    /// The restart-backoff queue: solo `OneForOne` deadlines and the single
    /// cycle-rebuild deadline both ride it, discriminated by [`CycleState`].
    retries: &'a mut DelayQueue<ActorId>,
    /// Deferred hard-kills for cancelled cycle members (and `stop_child`ed
    /// children) — the same queue the loop's abort arm drains. The queue owns
    /// [`PendingAbort`] handles; each one's [`Drop`] aborts the child if the
    /// supervisor exits before its deadline fires, truncating the remaining grace.
    pending_aborts: &'a mut DelayQueue<PendingAbort>,
    /// The at-most-one active set-cycle.
    cycle: &'a mut CycleState,
}

impl<'a> SetCycleCtx<'a> {
    /// Bundles the loop's three cycle-coordinator borrows for one decision-helper
    /// call. Constructed transiently inside a `select!` arm — the loop's other
    /// arms keep polling the same queues through their own disjoint field borrows,
    /// because passing the loop's `&mut` locals here reborrows rather than moves.
    const fn new(
        retries: &'a mut DelayQueue<ActorId>,
        pending_aborts: &'a mut DelayQueue<PendingAbort>,
        cycle: &'a mut CycleState,
    ) -> Self {
        Self {
            retries,
            pending_aborts,
            cycle,
        }
    }
}

/// Applies one link death to the restart policy **iff** it names a supervised
/// child — the single table lookup doubles as the membership test.
///
/// `None`: `notice.id` is not a supervised child (a peer this supervisor merely
/// watches), so the caller routes it to the [`LinkReact::on_link_died`] hook (the
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
///
/// **Absorb-first** (card #199, ADR-0014): a death that names a *cycling* member
/// is expected — the set-cycle is tearing it down — so it is counted against the
/// teardown and absorbed BEFORE any membership lookup or restart verdict, whatever
/// its reason.
fn handle_child_death(
    children: &mut Children,
    ctx: &mut SetCycleCtx<'_>,
    strategy: SupervisionStrategy,
    rng: &mut Rng,
    notice: &LinkDied,
) -> Option<ControlFlow<ActorStopReason>> {
    // Echo absorption FIRST (ADR-0014): a cycling member's death is expected —
    // count the teardown down, whatever the reason says. A crash (even an
    // on_stop-hook panic) DURING deliberate teardown is not crash-loop evidence;
    // the fresh incarnation runs a fresh on_start.
    if let Some(was_awaited) = children.absorb_cycling_death(notice.id) {
        if was_awaited {
            cycle_count_down(ctx, notice.id);
        }
        return Some(ControlFlow::Continue(()));
    }
    let child = children.get_mut(notice.id)?;
    // The live incarnation is gone; the entry survives (factory + accounting
    // persist across incarnations) but now holds no handle.
    child.handle = None;
    Some(match should_restart(child.config.policy, &notice.reason) {
        RestartVerdict::LeaveDead => {
            // #253/ADR-0020: a collected child is left dead SILENTLY by policy —
            // the trace event is the only witness (the #244 observability concern).
            if matches!(notice.reason, ActorStopReason::Collected) {
                trace::child_collected(notice.id);
            }
            ControlFlow::Continue(())
        }
        // A lifecycle-hook failure re-panics on the next incarnation: escalate at
        // once, bypassing both backoff and the counters.
        RestartVerdict::Escalate => {
            trace::child_escalated(notice.id);
            ControlFlow::Break(ActorStopReason::ChildLifecycleFailed { child: notice.id })
        }
        RestartVerdict::Restart => restart_or_give_up(children, ctx, strategy, rng, notice.id),
    })
}

/// Counts one awaited teardown death (or removal) down; on the LAST one, arms the
/// single cycle-rebuild deadline and moves to `Waiting`. A no-op outside
/// `Tearing`. The `DelayQueue` value is `id` only because the queue carries
/// [`ActorId`]s — the cycle-rebuild path matches on the KEY, never the value.
fn cycle_count_down(ctx: &mut SetCycleCtx<'_>, id: ActorId) {
    // A no-op outside `Tearing` is legitimate (a late death after the cycle armed
    // its rebuild, or on an idle coordinator).
    let CycleState::Tearing { awaiting, backoff } = &mut *ctx.cycle else {
        return;
    };
    // `Tearing` always holds `awaiting >= 1` by construction — the zero transition
    // moves it to `Waiting` — so this never underflows; a future regression that
    // broke the invariant must panic loudly here, not silently wedge the cycle.
    #[expect(
        clippy::expect_used,
        reason = "Tearing's awaiting >= 1 invariant is a programmer guarantee; an \
                  underflow would be an unreachable coordinator bug, surfaced as a \
                  panic rather than a silent wedge"
    )]
    let left = awaiting
        .checked_sub(1)
        .expect("Tearing always holds awaiting >= 1 (the 0 transition moves to Waiting)");
    if left == 0 {
        let key = ctx.retries.insert(id, *backoff);
        *ctx.cycle = CycleState::Waiting { key };
    } else {
        *awaiting = left;
    }
}

/// Starts a set-cycle over the suffix `[from..]`, or WIDENS the active one — the
/// same operation, because [`flag_cycle`](Children::flag_cycle) is idempotent and
/// every restart subset is a suffix (nested; ADR-0014). Cancels newly flagged live
/// members in reverse birth order with deferred hard-kills, then re-derives the
/// cycle state. Any armed rebuild deadline is REMOVED first: left in place it would
/// fire mid-teardown of the widened set and rebuild a half-alive set.
fn start_or_widen_cycle(
    children: &mut Children,
    ctx: &mut SetCycleCtx<'_>,
    from: usize,
    delay: Duration,
    trigger: ActorId,
) {
    if let CycleState::Waiting { key } = &mut *ctx.cycle {
        ctx.retries.remove(key);
    }
    let (stops, added) = children.flag_cycle(from);
    for (handle, grace) in stops {
        handle.cancel.cancel();
        ctx.pending_aborts.insert(PendingAbort::new(handle), grace);
    }
    let pending = match &*ctx.cycle {
        CycleState::Tearing { awaiting, .. } => *awaiting,
        CycleState::Idle | CycleState::Waiting { .. } => 0,
    };
    // `awaiting` is bounded by live child fan-out (≤ the table length), so the sum
    // cannot overflow u32: a silent `saturating`/`unwrap_or(MAX)` here would wedge
    // the cycle forever (waiting on MAX deaths that never land), so an overflow is
    // surfaced as a panic rather than absorbed.
    #[expect(
        clippy::expect_used,
        reason = "the widened awaiting count is bounded by the child table length \
                  and cannot overflow u32; an overflow would be an unreachable bug, \
                  surfaced as a panic rather than a silent cycle wedge"
    )]
    let awaiting = pending
        .checked_add(added)
        .expect("awaiting is bounded by live child fan-out (≤ table length); cannot overflow u32");
    if awaiting == 0 {
        let key = ctx.retries.insert(trigger, delay);
        *ctx.cycle = CycleState::Waiting { key };
    } else {
        *ctx.cycle = CycleState::Tearing {
            awaiting,
            backoff: delay,
        };
    }
}

/// The restart-or-escalate half of [`handle_child_death`], split out so each
/// function stays under the cognitive-complexity bar: records the failure and
/// either arms a solo backoff (`OneForOne`) or a set-cycle
/// (`RestForOne`/`OneForAll`) — or escalates on a tripped budget. One
/// `record_failure`, on the TRIGGER only; a set cycle is one recovery action, so
/// siblings' counters are untouched (OTP parity, ADR-0014).
fn restart_or_give_up(
    children: &mut Children,
    ctx: &mut SetCycleCtx<'_>,
    strategy: SupervisionStrategy,
    rng: &mut Rng,
    id: ActorId,
) -> ControlFlow<ActorStopReason> {
    #[expect(
        clippy::expect_used,
        reason = "the caller (handle_child_death) verified `id` is in the table in \
                  the same synchronous scope before dispatching here; a miss is an \
                  unreachable programmer bug, surfaced as a panic"
    )]
    let child = children
        .get_mut(id)
        .expect("caller verified membership in the same synchronous scope");
    let delay = match child.tracker.record_failure(&child.config, Instant::now()) {
        GiveUp::Yes { rebuilds } => {
            trace::restart_gave_up(id, rebuilds);
            return ControlFlow::Break(ActorStopReason::RestartLimitExceeded {
                child: id,
                rebuilds,
            });
        }
        GiveUp::No { attempt } => {
            let backoff = jittered_backoff(&child.config, attempt, rng);
            trace::restart_scheduled(id, attempt, backoff);
            backoff
        }
    };
    // `child` borrow ends above; the set path reborrows `children`.
    match strategy {
        SupervisionStrategy::OneForOne => {
            ctx.retries.insert(id, delay);
        }
        SupervisionStrategy::RestForOne => {
            #[expect(
                clippy::expect_used,
                reason = "`position` cannot fail once `get_mut` succeeded on the same \
                          key in this synchronous scope; a miss is an unreachable bug"
            )]
            let from = children
                .position(id)
                .expect("membership verified above in the same scope");
            start_or_widen_cycle(children, ctx, from, delay, id);
        }
        // The whole set is the suffix from birth index 0.
        SupervisionStrategy::OneForAll => {
            start_or_widen_cycle(children, ctx, 0, delay, id);
        }
    }
    ControlFlow::Continue(())
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
    }) = children
        .get_mut(old_id)
        // Drop-at-fire supersession: if the entry is cycling, a set-cycle owns it
        // now; a stale SOLO backoff deadline (whose Key 2a discards) must not
        // rebuild one member mid-teardown. The cycle's own rebuild sweep clears
        // the flag first, so cycle rebuilds pass this guard.
        .filter(|child| !child.cycling)
        .map(|child| (child.factory)())
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
/// The install is a single synchronous send on the child's UNBOUNDED control
/// lane (inside `install_watch`), never an await, so a slow child can never
/// stall the loop — and since #225 never a kill-on-full either: the lane has no
/// capacity to exhaust, so a flooded child is watched (late) instead of being
/// synthesized as a failed incarnation (ADR-0021). Its two outcomes:
///
/// - [`Installed`](WatchOutcome::Installed): the edge is live; done.
/// - [`Closed`](WatchOutcome::Closed): the child died in its unwatched window, so
///   its own notice never reached us — synthesize the `AlreadyDead` one, which the
///   next poll turns into a restart (the child is table-present).
fn install_child_watch(sup: &SupervisorRef, handle: &ChildHandle, install_watch: WatchInstaller) {
    match install_watch(sup.watch_reg()) {
        WatchOutcome::Installed => {}
        WatchOutcome::Closed => sup.synthesize_child_death(handle.id),
    }
}

/// Disarms the armed registration, inserts the child into the table, and installs
/// the supervisor's watch edge on the first incarnation. Insertion must precede
/// the watch install so that a death racing registration routes to the restart
/// policy, never to the peer-watch hook (the #196 registration-hazard fix). The
/// handle is cloned out before the move so the watch-install can name the child
/// after `child` is consumed by `insert`.
fn install_registration(children: &mut Children, sup: &SupervisorRef, armed: ArmedReg) {
    let SuperviseReg {
        child,
        id,
        install_watch,
    } = armed.disarm();
    let first_handle = child.handle.clone();
    children.insert(id, child);
    if let Some(handle) = first_handle {
        install_child_watch(sup, &handle, install_watch);
    }
}

/// Applies a child-table [`SupervisionOp`] that arrived on the supervisor's own
/// mailbox. The table is task-owned, so this is its ONLY writer — no lock, and
/// no ordering rule beyond the mailbox's FIFO.
fn apply_supervision_op(
    children: &mut Children,
    sup: &SupervisorRef,
    ctx: &mut SetCycleCtx<'_>,
    op: SupervisionOp,
) {
    match op {
        SupervisionOp::Add(armed) => install_registration(children, sup, armed),
        // Drop the supervision edge; the child keeps running, now unwatched. If it
        // was an AWAITED cycle member, count the teardown down — else the cycle
        // waits forever for a death that will land as a table-miss.
        SupervisionOp::Remove(id) => {
            if let Some(child) = children.remove(id)
                && child.cycling
                && child.handle.is_some()
            {
                cycle_count_down(ctx, id);
            }
        }
        // Drop the edge AND stop the child crash-only, OTP `terminate_child/2`:
        // `cancel` asks it to stop gracefully NOW, and the hard `abort` is deferred
        // by `stop_grace` onto `pending_aborts` (a select arm), so a cooperating
        // child stops within the grace and a non-cooperating one is aborted when
        // the deadline fires — WITHOUT the loop blocking on the grace. `abort` on a
        // task that already stopped gracefully is a harmless no-op
        // (`futures::stream::AbortHandle`), so no liveness is tracked between the
        // two edges. The edge is removed first: a death already in flight for `id`
        // then routes to the ignored peer path, never a rebuild (the #195
        // unwatch-races-death rule).
        SupervisionOp::Stop(id) => {
            if let Some(child) = children.remove(id) {
                // Compute BEFORE the handle is moved out below.
                let was_awaited = child.cycling && child.handle.is_some();
                if let Some(handle) = child.handle {
                    handle.cancel.cancel();
                    ctx.pending_aborts
                        .insert(PendingAbort::new(handle), child.config.stop_grace);
                }
                if was_awaited {
                    cycle_count_down(ctx, id);
                }
            }
        }
    }
}

/// Drains queued supervision ops from a supervisor's CONTROL lane after the
/// message loop has exited gracefully. The loop is gone, so cycle bookkeeping is
/// unnecessary and discarded; the goal is to land every queued child in the
/// table so the following teardown sweep treats it like any other supervised
/// child. This is mechanism 1 of #248 / the ADR-0019 amendment, moved lanes
/// intact by #225: the graceful path APPLIES the ops through the same logic the
/// loop uses, never the reject path.
///
/// `Add` is disarmed, inserted, and watched (insert-before-watch, #196). `Remove`
/// detaches the child without stopping it. `Stop` cancels and schedules a deferred
/// abort (the following `pending_aborts.clear()` truncates the grace). `Watch`/
/// `Unwatch` are applied to `watchers`. The user-lane backlog is discarded — but
/// still drained, releasing each queued message's strong `self_sender` (ADR-0003).
#[expect(
    clippy::too_many_arguments,
    reason = "the epilogue needs one borrow per task-owned structure plus the \
              supervisor identity and link sender; grouping would not reduce \
              complexity and would widen the SupervisorRef API"
)]
pub(super) fn drain_queued_supervision<A: Actor>(
    mailbox_rx: &mut MailboxReceiver<A>,
    children: &mut Children,
    watchers: &mut Watchers,
    pending_aborts: &mut DelayQueue<PendingAbort>,
    sup_id: ActorId,
    sup_link_tx: LinkSender,
) {
    let sup = SupervisorRef {
        id: sup_id,
        link_tx: sup_link_tx,
    };
    for signal in mailbox_rx.drain_control() {
        match signal {
            ControlSignal::Supervision(op) => match *op {
                SupervisionOp::Add(armed) => install_registration(children, &sup, armed),
                SupervisionOp::Remove(id) => {
                    children.remove(id);
                }
                SupervisionOp::Stop(id) => {
                    if let Some(child) = children.remove(id)
                        && let Some(handle) = child.handle
                    {
                        handle.cancel.cancel();
                        pending_aborts.insert(PendingAbort::new(handle), child.config.stop_grace);
                    }
                }
            },
            ControlSignal::Watch(reg) => watchers.apply(*reg),
            ControlSignal::Unwatch(id) => watchers.remove(id),
        }
    }
    // The user backlog is abandoned on this path, never applied — but draining
    // it releases each queued message's strong `self_sender` (ADR-0003).
    for signal in mailbox_rx.drain() {
        drop(signal);
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
    let result = AssertUnwindSafe(state.handle(msg, actor_ref))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(Flow::Continue)) => ControlFlow::Continue(()),
        Ok(Ok(Flow::Stop)) => ControlFlow::Break(ActorStopReason::Normal),
        // A returned Err is a controlled crash: observe via on_panic, then stop.
        Ok(Err(err)) => {
            let panic = PanicError::new(Box::new(err), PanicReason::HandlerPanic);
            trace::handler_crashed(&panic);
            ControlFlow::Break(run_on_panic(state, self_ref, panic).await)
        }
        // The handler unwound: catch, observe via on_panic, then stop.
        Err(payload) => {
            let panic = PanicError::from_panic_any(payload, PanicReason::HandlerPanic);
            trace::handler_crashed(&panic);
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

    use futures::{future::Abortable, stream::AbortHandle};
    use tokio::time::Instant;
    use tokio_util::{sync::CancellationToken, time::DelayQueue};

    use fastrand::Rng;

    use super::{
        SetCycleCtx, SupervisorRef, apply_supervision_op, drain_queued_supervision,
        handle_child_death, install_child_watch, rebuild_child, teardown_children,
    };
    use crate::{
        actor::spawn::DEFAULT_ON_STOP_NOTICE_GRACE,
        actor::supervision::{
            ArmedReg, Child, ChildHandle, Children, CycleState, RebuildFactory, Spawned,
            SuperviseReg, SupervisionOp, WatchInstaller, WatchOutcome, watch_installer,
        },
        error::{ActorStopReason, PanicError, PanicReason},
        mailbox::{ActorId, Capacity, ControlSignal, Mailbox, MailboxReceiver, Mailboxed, Signal},
        message::Msg,
        restart::{RestartConfig, RestartPolicy, RestartTracker, SupervisionStrategy},
        watch::{LinkDied, LinkReceiver, LinkSender, WatchReg, Watchers},
    };
    use core::ops::ControlFlow;

    /// A minimal actor purely to key a real mailbox in the watch-install tests —
    /// its `Msg` is never handled here, only enqueued and drained. The `Actor`
    /// impl exists so `drain_queued_supervision` (generic over `A: Actor`) can
    /// take its mailbox.
    struct Probe;
    #[derive(Debug)]
    struct ProbeMsg;
    impl Msg for ProbeMsg {}
    impl Mailboxed for Probe {
        type Msg = ProbeMsg;
    }
    impl crate::actor::Actor for Probe {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start((): (), _: crate::actor::ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
        async fn handle(
            &mut self,
            _: ProbeMsg,
            _: crate::actor::ActorRef<Self>,
        ) -> Result<crate::actor::Flow, Self::Error> {
            Ok(crate::actor::Flow::Continue)
        }
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

    /// A [`ChildHandle`] plus a background task that sends a synthetic death
    /// notice on the supervisor's link channel when the child is cancelled or
    /// aborted. Used to make `teardown_children`'s join observable with mock
    /// handles.
    fn mock_child(id: ActorId, link_tx: LinkSender) -> ChildHandle {
        let (abort, reg) = AbortHandle::new_pair();
        tokio::spawn(async move {
            let _ = Abortable::new(core::future::pending::<()>(), reg).await;
            let _ = link_tx.send(LinkDied {
                id,
                reason: ActorStopReason::Normal,
                linked: false,
                cleanup_failed: false,
            });
        });
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
                handle: handle(ActorId::from_raw_for_test(999)),
                install_watch: noop_installer(),
            }),
            handle: Some(handle(ActorId::from_raw_for_test(1))),
            config,
            tracker: RestartTracker::new(started),
            cycling: false,
        }
    }

    fn panicked(reason: PanicReason) -> ActorStopReason {
        ActorStopReason::Panicked(PanicError::new(Box::new("boom"), reason))
    }

    fn notice(id: ActorId, reason: ActorStopReason) -> LinkDied {
        LinkDied {
            id,
            reason,
            // A supervisor MONITORS its children (`watch_reg` uses `linked: false`):
            // it reacts via the restart table, never by propagating the child's
            // death through its own hook.
            linked: false,
            cleanup_failed: false,
        }
    }

    fn one_child(config: RestartConfig) -> (Children, ActorId) {
        let id = ActorId::from_raw_for_test(1);
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
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForOne,
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
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForOne,
            &mut rng,
            &notice(ActorId::from_raw_for_test(999), ActorStopReason::Killed),
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
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForOne,
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
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForOne,
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
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = fastrand::Rng::with_seed(0);

        let flow = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForOne,
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

    /// #196 invariant 6 (`already_dead_counts_as_restartable`): an `AlreadyDead`
    /// notice is abnormal under `Transient`, so the child is REBUILT — and each one
    /// COUNTS toward the give-up budget like any other failure. `max_restarts = 1`:
    /// the first `AlreadyDead` arms a rebuild (attempt 1); the second trips
    /// (attempt 2 > 1), which can only happen if the counter advanced rather than
    /// resetting to "attempt 1" on every `AlreadyDead`. FAILS if `AlreadyDead` were
    /// classified `LeaveDead` (no retry armed on the first) or did not count (the
    /// second never trips).
    #[tokio::test]
    async fn already_dead_is_restartable_under_transient_and_counts() {
        let (mut children, id) =
            one_child(RestartConfig::new(RestartPolicy::Transient).with_max_restarts(1));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = fastrand::Rng::with_seed(0);

        let first = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForOne,
            &mut rng,
            &notice(id, ActorStopReason::AlreadyDead),
        );
        assert!(
            matches!(first, Some(ControlFlow::Continue(()))),
            "AlreadyDead is abnormal under Transient — the first one rebuilds, got {first:?}",
        );
        assert_eq!(
            retries.len(),
            1,
            "a rebuild was armed for the AlreadyDead child"
        );

        let second = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForOne,
            &mut rng,
            &notice(id, ActorStopReason::AlreadyDead),
        );
        assert!(
            matches!(
                second,
                Some(ControlFlow::Break(ActorStopReason::RestartLimitExceeded { child, rebuilds }))
                    if child == id && rebuilds == 2
            ),
            "the second AlreadyDead advanced the counter and tripped the budget, got {second:?}",
        );
    }

    /// `Add` installs a child under its id; `Remove` drops the edge but leaves
    /// the child running (the entry is gone, no stop signal fired); `Stop` drops
    /// the edge, cancels the child at once, and SCHEDULES (does not yet fire) the
    /// hard abort on the pending-abort queue.
    #[tokio::test(start_paused = true)]
    async fn supervision_ops_mutate_the_table() {
        let (sup, _link_rx) = supervisor(ActorId::from_raw_for_test(100));
        let mut children = Children::new();
        let mut pending_aborts = DelayQueue::new();
        let mut retries = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let id = ActorId::from_raw_for_test(1);
        apply_supervision_op(
            &mut children,
            &sup,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionOp::Add(ArmedReg::new(SuperviseReg {
                child: child(RestartConfig::new(RestartPolicy::Permanent), Instant::now()),
                id,
                install_watch: noop_installer(),
            })),
        );
        assert!(children.get_mut(id).is_some(), "Add installs the child");

        // Stop: capture the child's stop edges before applying, then assert the
        // cancel fired immediately and the abort was only SCHEDULED, not yet fired.
        let stop_edges = {
            let entry = children.get_mut(id).expect("present");
            entry.handle.clone().expect("live incarnation")
        };
        apply_supervision_op(
            &mut children,
            &sup,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionOp::Stop(id),
        );
        assert!(children.get_mut(id).is_none(), "Stop drops the edge");
        assert!(
            stop_edges.cancel.is_cancelled(),
            "Stop cancels the child's graceful token at once",
        );
        assert!(
            !stop_edges.abort.is_aborted(),
            "Stop defers the abort — it must NOT fire before the grace elapses",
        );
        assert_eq!(
            pending_aborts.len(),
            1,
            "Stop scheduled exactly one deferred abort",
        );

        // Remove: the edge is dropped, but no stop edge is driven and nothing is
        // scheduled.
        let other = ActorId::from_raw_for_test(2);
        apply_supervision_op(
            &mut children,
            &sup,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionOp::Add(ArmedReg::new(SuperviseReg {
                child: child(RestartConfig::new(RestartPolicy::Permanent), Instant::now()),
                id: other,
                install_watch: noop_installer(),
            })),
        );
        let survivor = {
            let entry = children.get_mut(other).expect("present");
            entry.handle.clone().expect("live incarnation")
        };
        apply_supervision_op(
            &mut children,
            &sup,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionOp::Remove(other),
        );
        assert!(children.get_mut(other).is_none(), "Remove drops the edge");
        assert!(
            !survivor.cancel.is_cancelled() && !survivor.abort.is_aborted(),
            "Remove leaves the child running — no stop edge is driven",
        );
        assert_eq!(
            pending_aborts.len(),
            1,
            "Remove schedules no abort — the queue is unchanged",
        );
    }

    /// The deferred-abort backstop: a `Stop`ped child that never observes the
    /// graceful `cancel` is hard-aborted once its `stop_grace` deadline fires off
    /// the pending-abort queue. Under `start_paused` the abort must NOT fire a
    /// nanosecond before the grace and MUST fire at it — the crash-only bound
    /// `stop_child` promises even for a non-cooperating child.
    #[tokio::test(start_paused = true)]
    async fn stop_defers_the_abort_until_the_grace_deadline() {
        let (sup, _link_rx) = supervisor(ActorId::from_raw_for_test(100));
        let mut children = Children::new();
        let mut pending_aborts = DelayQueue::new();
        let mut retries = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let id = ActorId::from_raw_for_test(1);
        let grace = Duration::from_secs(5);
        let mut entry = child(RestartConfig::new(RestartPolicy::Permanent), Instant::now());
        entry.config = entry.config.with_stop_grace(grace);
        let edges = entry.handle.clone().expect("live incarnation");
        children.insert(id, entry);

        apply_supervision_op(
            &mut children,
            &sup,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionOp::Stop(id),
        );

        // One nanosecond short of the grace: the entry is not yet expired, so a
        // non-blocking poll of the queue reports nothing ready.
        tokio::time::advance(grace - Duration::from_nanos(1)).await;
        let short =
            core::future::poll_fn(|cx| core::task::Poll::Ready(pending_aborts.poll_expired(cx)))
                .await;
        assert!(
            short.is_pending(),
            "the abort entry must not be ready before the grace elapses",
        );
        assert!(!edges.abort.is_aborted(), "still within grace: not aborted");

        // Cross the deadline: the entry expires and the loop would abort it.
        tokio::time::advance(Duration::from_nanos(1)).await;
        let expired = futures::StreamExt::next(&mut pending_aborts)
            .await
            .expect("the deferred abort fires at the deadline");
        drop(expired.into_inner());
        assert!(
            edges.abort.is_aborted(),
            "at the grace deadline the child is hard-aborted",
        );
    }

    /// The lifecycle-epilogue sweep stops every SURVIVING child: a live child
    /// is cancelled, joined via its death notice, and (if it ignores the notice
    /// deadline) aborted; a backoff-window child (no live handle) is skipped;
    /// the table is emptied. Proves the single non-skippable sweep site that
    /// closes the orphaned-children gap (ADR-0019).
    #[tokio::test(start_paused = true)]
    async fn teardown_children_cancels_and_aborts_live_ones() {
        let mut children = Children::new();
        let (link_tx, link_rx) = flume::unbounded();

        // A live survivor with a zero grace: cancel fires, then the grace timer
        // fires immediately, so the sweep aborts it. The mock task sends the
        // death notice once it is aborted.
        let alive = ActorId::from_raw_for_test(1);
        let alive_handle = mock_child(alive, link_tx.clone());
        let alive_edges = alive_handle.clone();
        let alive_entry = {
            let mut entry = child(
                RestartConfig::new(RestartPolicy::Permanent).with_stop_grace(Duration::ZERO),
                Instant::now(),
            );
            entry.handle = Some(alive_handle);
            entry
        };
        children.insert(alive, alive_entry);

        // A backoff-window child: no live incarnation to stop.
        let dead = ActorId::from_raw_for_test(2);
        let mut dead_entry = child(RestartConfig::new(RestartPolicy::Permanent), Instant::now());
        dead_entry.handle = None;
        children.insert(dead, dead_entry);

        teardown_children(&mut children, &link_rx, DEFAULT_ON_STOP_NOTICE_GRACE).await;

        assert!(
            alive_edges.cancel.is_cancelled(),
            "the live survivor is cancelled",
        );
        assert!(
            alive_edges.abort.is_aborted(),
            "and, past its (zero) grace, hard-aborted",
        );
        assert_eq!(
            children.ids().count(),
            0,
            "the sweep empties the child table",
        );
    }

    /// Minimal tracing capture for the unit tests in this module — mirrors
    /// `tests/tracing_capture.rs::capture`, which integration tests cannot share
    /// with in-crate unit tests.
    #[cfg(feature = "tracing")]
    mod trace_capture {
        use std::fmt::Write as _;
        use std::sync::{Arc, Mutex};

        use tracing::{
            Event, Subscriber,
            field::{Field, Visit},
        };
        use tracing_subscriber::{
            Layer, layer::Context, layer::SubscriberExt as _, registry::LookupSpan,
        };

        /// One captured event: its level and recorded fields (the `message`
        /// field carries the format message).
        #[derive(Debug, Clone)]
        pub struct EventRec {
            pub level: String,
            pub fields: Vec<(String, String)>,
        }

        pub fn field(fields: &[(String, String)], name: &str) -> Option<String> {
            fields
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }

        struct FieldVisitor<'a>(&'a mut Vec<(String, String)>);

        impl Visit for FieldVisitor<'_> {
            fn record_debug(&mut self, f: &Field, value: &dyn core::fmt::Debug) {
                let mut s = String::new();
                let _ = write!(s, "{value:?}");
                self.0.push((f.name().to_owned(), s));
            }
            fn record_str(&mut self, f: &Field, value: &str) {
                self.0.push((f.name().to_owned(), value.to_owned()));
            }
        }

        #[derive(Clone, Default)]
        pub struct CaptureLayer {
            pub store: Arc<Mutex<Vec<EventRec>>>,
        }

        impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for CaptureLayer {
            fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
                let mut fields = Vec::new();
                event.record(&mut FieldVisitor(&mut fields));
                self.store
                    .lock()
                    .expect("capture store poisoned")
                    .push(EventRec {
                        level: event.metadata().level().to_string(),
                        fields,
                    });
            }
        }

        /// Installs a fresh capture subscriber as the THREAD default. Tests must
        /// run on a current-thread runtime (`#[tokio::test]` default) so the
        /// sweep's events emit on this thread.
        pub fn install() -> (Arc<Mutex<Vec<EventRec>>>, tracing::subscriber::DefaultGuard) {
            let layer = CaptureLayer::default();
            let store = Arc::clone(&layer.store);
            let subscriber = tracing_subscriber::registry().with(layer);
            (store, tracing::subscriber::set_default(subscriber))
        }
    }

    /// A child that is aborted at supervisor exit but never confirms its death
    /// (a non-yielding child produces no notice) must not wedge the sweep: after
    /// `on_stop_grace` the supervisor traces the missing notice and proceeds.
    /// Kills the `child_teardown_abandoned` -> () mutant (trace.rs:172).
    #[cfg(feature = "tracing")]
    #[tokio::test(start_paused = true)]
    async fn teardown_traces_abandonment_when_a_child_never_confirms_death() {
        let (link_tx, link_rx) = flume::unbounded();
        let mut children = Children::new();
        let child_id = ActorId::from_raw_for_test(1);
        let mut entry = child(
            RestartConfig::new(RestartPolicy::Permanent).with_stop_grace(Duration::ZERO),
            Instant::now(),
        );
        // A SILENT handle: no backing task, so no `LinkDied` ever arrives — the
        // aborted child never confirms its death. `link_tx` stays bound for the
        // whole sweep so the channel never closes and the sweep parks on
        // `recv_async` until the (virtual) `on_stop_grace` deadline fires.
        entry.handle = Some(handle(child_id));
        children.insert(child_id, entry);

        let (store, _guard) = trace_capture::install();
        let on_stop_grace = Duration::from_secs(1);
        teardown_children(&mut children, &link_rx, on_stop_grace).await;
        drop(link_tx);

        let expected_id = format!("{child_id:?}");
        let captured = store.lock().expect("capture store poisoned");
        let events: Vec<_> = captured
            .iter()
            .filter(|e| {
                trace_capture::field(&e.fields, "message").as_deref()
                    == Some("child teardown notice missing after abort; abandoning")
            })
            .collect();
        assert_eq!(events.len(), 1, "exactly one abandonment trace event fires");
        let event = events[0];
        assert_eq!(event.level, "ERROR", "the abandonment is an error event");
        assert_eq!(
            trace_capture::field(&event.fields, "child.id").as_deref(),
            Some(expected_id.as_str()),
            "the event names the abandoned child",
        );
        assert!(
            trace_capture::field(&event.fields, "grace").is_some_and(|grace| !grace.is_empty()),
            "the event carries the grace that elapsed",
        );
        drop(captured);
        assert_eq!(
            children.ids().count(),
            0,
            "the sweep still empties the child table",
        );
    }

    /// The registration-hazard fix, happy path: installing the watch on an OPEN
    /// child enqueues the supervisor's MONITORING edge onto the child's CONTROL
    /// lane and synthesizes no death. The watcher on the enqueued reg is the
    /// supervisor — this is the edge that later carries the child's death back.
    #[tokio::test]
    async fn install_child_watch_enqueues_the_edge_on_an_open_child() {
        let (sup, link_rx) = supervisor(ActorId::from_raw_for_test(100));
        let child_id = ActorId::from_raw_for_test(9);
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), child_id);
        install_child_watch(&sup, &handle(child_id), watch_installer(tx));

        assert!(
            link_rx.try_recv().is_err(),
            "an installed edge self-heals nothing",
        );
        let signal = rx
            .drain_control()
            .next()
            .expect("the watch reg reached the child's control lane");
        let ControlSignal::Watch(reg) = signal else {
            panic!("expected a queued Watch reg");
        };
        assert_eq!(
            reg.watcher,
            ActorId::from_raw_for_test(100),
            "the supervisor is the watcher"
        );
        assert!(
            !reg.linked,
            "a supervisor MONITORS its children (linked == false): it reacts via \
             the restart table, never by propagating a detached child's death into \
             itself through the on_link_died hook",
        );
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
        let (sup, link_rx) = supervisor(ActorId::from_raw_for_test(100));
        let child_id = ActorId::from_raw_for_test(7);
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
        assert!(
            !notice.linked,
            "a supervisor edge is a MONITOR (linked == false), not a propagating link",
        );
    }

    /// #225 / ADR-0021 — the deliberate semantic CHANGE: a child flooded before
    /// the loop could watch it is no longer a failed incarnation. The install
    /// rides the child's UNBOUNDED control lane, which has no capacity to
    /// exhaust, so the edge lands on the full child and NOTHING is killed or
    /// synthesized. Regression-guards the removed `WatchOutcome::Full`: the
    /// kill-and-synthesize behaviour was the bounded-lane compromise; on the
    /// control lane the registration is never lost to user backlog (the exact
    /// point of the card).
    #[tokio::test]
    async fn install_child_watch_lands_on_a_flooded_child_via_the_control_lane() {
        let (sup, link_rx) = supervisor(ActorId::from_raw_for_test(100));
        let child_id = ActorId::from_raw_for_test(8);
        let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(1), child_id);
        tx.try_send(Signal::Stop)
            .expect("the one user-lane slot fills");
        let h = handle(child_id);
        install_child_watch(&sup, &h, watch_installer(tx));

        assert!(
            !h.cancel.is_cancelled(),
            "a flooded child is watched, never killed",
        );
        assert!(
            !h.abort.is_aborted(),
            "and never aborted as a failed incarnation",
        );
        assert!(
            link_rx.try_recv().is_err(),
            "no synthetic death — the edge landed on the live child",
        );
        // The watch reg rode the control lane AROUND the full user lane...
        let signal = rx
            .drain_control()
            .next()
            .expect("the watch reg reached the flooded child's control lane");
        let ControlSignal::Watch(reg) = signal else {
            panic!("expected a queued Watch reg on the control lane");
        };
        assert_eq!(reg.watcher, ActorId::from_raw_for_test(100));
        // ...and the earlier user-lane signal is untouched behind it.
        assert!(
            matches!(rx.drain().next(), Some(Signal::Stop)),
            "the flooded user backlog is undisturbed",
        );
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

        let (sup, sup_link_rx) = supervisor(ActorId::from_raw_for_test(100));
        let new_id = ActorId::from_raw_for_test(50);
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
        let old_id = ActorId::from_raw_for_test(1);
        let mut children = Children::new();
        children.insert(
            old_id,
            Child {
                factory,
                handle: None, // in the backoff window: no live incarnation
                config: RestartConfig::new(RestartPolicy::Permanent),
                tracker: RestartTracker::new(Instant::now()),
                cycling: false,
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
            .drain_control()
            .next()
            .expect("the watch reg reached the rebuilt child's control lane");
        let ControlSignal::Watch(reg) = signal else {
            panic!("expected a queued Watch reg on the rebuilt child");
        };
        assert_eq!(
            reg.watcher,
            ActorId::from_raw_for_test(100),
            "the supervisor watches the rebuilt incarnation",
        );
        assert!(
            sup_link_rx.try_recv().is_err(),
            "an open rebuild synthesizes no death",
        );
    }

    /// The #248 epilogue path on the #225 lane, pinned at the unit level: the
    /// epilogue is not deterministically reachable from outside a live loop
    /// (control-first recv always applies a queued op in-loop first), so the
    /// drain is driven directly here with a preloaded control lane —
    /// `Add` + `Watch` + `Stop`. The `Add` lands in the child table through the
    /// SAME apply logic the loop uses (insert-before-watch, #196), the `Watch`
    /// in the watcher set, and the `Stop` (on an already-tabled child) cancels
    /// it and defers the abort onto `pending_aborts`.
    #[tokio::test]
    async fn drain_queued_supervision_applies_a_preloaded_control_lane() {
        let sup_id = ActorId::from_raw_for_test(100);
        let (sup, _sup_link_rx) = supervisor(sup_id);
        let SupervisorRef {
            link_tx: sup_link_tx,
            ..
        } = sup;
        let (tx, mut mailbox_rx) = Mailbox::<Probe>::bounded(cap(8), sup_id);

        // Add: a fresh child whose watch install lands on its open control lane.
        let add_id = ActorId::from_raw_for_test(1);
        let (child_tx, _child_rx) = Mailbox::<Probe>::bounded(cap(4), add_id);
        let add_reg = SuperviseReg {
            child: child(RestartConfig::new(RestartPolicy::Permanent), Instant::now()),
            id: add_id,
            install_watch: watch_installer(child_tx),
        };
        tx.send_control(ControlSignal::Supervision(Box::new(SupervisionOp::Add(
            ArmedReg::new(add_reg),
        ))))
        .expect("add enqueued");

        // Watch: a watcher edge on the supervisor itself.
        let (watch_link_tx, watch_link_rx) = flume::unbounded::<LinkDied>();
        tx.send_control(ControlSignal::Watch(Box::new(WatchReg {
            watcher: ActorId::from_raw_for_test(9),
            link_tx: watch_link_tx,
            linked: false,
        })))
        .expect("watch enqueued");

        // Stop: an already-tabled child — cancel now, abort deferred.
        let stop_id = ActorId::from_raw_for_test(2);
        let stop_handle = handle(stop_id);
        let stop_edges = stop_handle.clone();
        let mut children = Children::new();
        children.insert(stop_id, {
            let mut entry = child(RestartConfig::new(RestartPolicy::Permanent), Instant::now());
            entry.handle = Some(stop_handle);
            entry
        });
        tx.send_control(ControlSignal::Supervision(Box::new(SupervisionOp::Stop(
            stop_id,
        ))))
        .expect("stop enqueued");

        let mut watchers = Watchers::new(sup_id);
        let mut pending_aborts = DelayQueue::new();
        drain_queued_supervision(
            &mut mailbox_rx,
            &mut children,
            &mut watchers,
            &mut pending_aborts,
            sup_id,
            sup_link_tx,
        );

        assert!(
            children.get_mut(add_id).is_some(),
            "the drained Add landed in the child table",
        );
        assert!(
            children.get_mut(stop_id).is_none(),
            "the drained Stop dropped the table entry",
        );
        assert!(
            stop_edges.cancel.is_cancelled(),
            "the drained Stop cancelled the child",
        );
        assert!(
            !stop_edges.abort.is_aborted(),
            "the abort is DEFERRED, not fired inline",
        );
        assert_eq!(
            pending_aborts.len(),
            1,
            "the drained Stop's abort waits on pending_aborts",
        );

        // The drained Watch was applied to the watcher set: dropping the guard
        // fires its notice.
        watchers.set_reason(ActorStopReason::Normal);
        drop(watchers);
        let notice = watch_link_rx
            .try_recv()
            .expect("the drained Watch must be applied to the watcher set");
        assert_eq!(notice.id, sup_id, "the notice names the supervisor");
        assert!(
            matches!(notice.reason, ActorStopReason::Normal),
            "the guard's reason rides the notice",
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
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = fastrand::Rng::with_seed(7);

        let before = Instant::now();
        let flow = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForOne,
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

    /// A birth-ordered table of `n` live `Permanent` children keyed `1..=n`, the
    /// set-strategy tests' fixture.
    fn table_of(n: u32) -> Children {
        let mut children = Children::new();
        for i in 1..=n {
            children.insert(
                ActorId::from_raw_for_test(u64::from(i)),
                child(RestartConfig::new(RestartPolicy::Permanent), Instant::now()),
            );
        }
        children
    }

    /// OneForAll trigger: the whole table is flagged, live siblings' cancels fire,
    /// the trigger's counters advance ONCE, siblings' never.
    #[tokio::test(start_paused = true)]
    async fn set_trigger_flags_set_and_counts_trigger_once() {
        let mut children = table_of(3);
        children
            .get_mut(ActorId::from_raw_for_test(2))
            .unwrap()
            .handle = None; // the trigger died
        let (sup, _link_rx) = supervisor(ActorId::from_raw_for_test(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);

        let flow = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForAll,
            &mut rng,
            &notice(ActorId::from_raw_for_test(2), ActorStopReason::Killed),
        );

        assert!(matches!(flow, Some(ControlFlow::Continue(()))));
        assert!(
            matches!(cycle, CycleState::Tearing { awaiting: 2, .. }),
            "{cycle:?}"
        );
        for i in [1_u64, 3] {
            let sibling = children.get_mut(ActorId::from_raw_for_test(i)).unwrap();
            assert!(sibling.cycling);
            assert!(
                sibling.handle.as_ref().unwrap().cancel.is_cancelled(),
                "sibling {i} cancelled",
            );
        }
        assert_eq!(pending_aborts.len(), 2, "deferred hard-kills armed");
        assert_eq!(retries.len(), 0, "no rebuild while teardown pending");
        let _ = sup;
    }

    /// Absorb: a cycling member's death decrements awaiting; the LAST one arms the
    /// single cycle-rebuild deadline (Waiting) instead of a policy verdict — even
    /// when the death reason is a lifecycle-hook panic (an `on_stop` panic during
    /// deliberate teardown is not crash-loop evidence; the reason is diagnostic
    /// only on this path).
    #[tokio::test(start_paused = true)]
    async fn absorbed_deaths_count_down_and_arm_rebuild() {
        let mut children = table_of(3);
        children
            .get_mut(ActorId::from_raw_for_test(2))
            .unwrap()
            .handle = None;
        let (_sup, _link_rx) = supervisor(ActorId::from_raw_for_test(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForAll,
            &mut rng,
            &notice(ActorId::from_raw_for_test(2), ActorStopReason::Killed),
        );

        let hook_panic = ActorStopReason::Panicked(PanicError::new(
            Box::new("on_stop blew up during teardown"),
            PanicReason::OnStop,
        ));
        let first = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForAll,
            &mut rng,
            &notice(ActorId::from_raw_for_test(3), hook_panic),
        );
        assert!(
            matches!(first, Some(ControlFlow::Continue(()))),
            "absorbed, not escalated"
        );
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 1, .. }));

        let last = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForAll,
            &mut rng,
            &notice(ActorId::from_raw_for_test(1), ActorStopReason::Killed),
        );
        assert!(matches!(last, Some(ControlFlow::Continue(()))));
        assert!(
            matches!(cycle, CycleState::Waiting { .. }),
            "teardown complete: rebuild armed"
        );
        assert_eq!(retries.len(), 1, "exactly one cycle deadline");
        let trigger = children.get_mut(ActorId::from_raw_for_test(2)).unwrap();
        assert_eq!(
            trigger.tracker,
            {
                let mut t = RestartTracker::new(Instant::now());
                t.record_failure(&trigger.config, Instant::now());
                t
            },
            "trigger charged once; absorbs charged nothing"
        );
    }

    /// Widen: an elder Supervised death mid-Tearing folds into the active cycle
    /// (RestForOne: its suffix is a superset), recomputing awaiting and NOT
    /// double-cancelling already-cycling members.
    #[tokio::test(start_paused = true)]
    async fn elder_death_mid_tearing_widens_the_cycle() {
        let mut children = table_of(3);
        children
            .get_mut(ActorId::from_raw_for_test(2))
            .unwrap()
            .handle = None;
        let (_sup, _link_rx) = supervisor(ActorId::from_raw_for_test(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        // Trigger: child 2 → cycle {2,3} (RestForOne suffix), awaiting {3}.
        handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::RestForOne,
            &mut rng,
            &notice(ActorId::from_raw_for_test(2), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 1, .. }));

        // Elder child 1 dies spontaneously mid-cycle → widen to {1,2,3}.
        let flow = handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::RestForOne,
            &mut rng,
            &notice(ActorId::from_raw_for_test(1), ActorStopReason::Killed),
        );
        assert!(matches!(flow, Some(ControlFlow::Continue(()))));
        // 1 was live? No — it just DIED (its death is the trigger); nothing new to
        // await: still awaiting only {3}.
        assert!(
            matches!(cycle, CycleState::Tearing { awaiting: 1, .. }),
            "{cycle:?}"
        );
        assert!(
            children
                .get_mut(ActorId::from_raw_for_test(1))
                .unwrap()
                .cycling,
            "elder folded in"
        );
        assert_eq!(pending_aborts.len(), 1, "no double-cancel of member 3");
    }

    /// Widen during Waiting: the armed rebuild deadline is REMOVED and re-armed
    /// (the stale-deadline/half-alive hazard, ADR-0014's counterexample table).
    #[tokio::test(start_paused = true)]
    async fn widen_during_waiting_replaces_the_armed_deadline() {
        let mut children = table_of(2);
        children
            .get_mut(ActorId::from_raw_for_test(2))
            .unwrap()
            .handle = None;
        let (_sup, _link_rx) = supervisor(ActorId::from_raw_for_test(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        // Child 2 is the LAST child: RestForOne suffix = {2} alone, all dead →
        // straight to Waiting.
        handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::RestForOne,
            &mut rng,
            &notice(ActorId::from_raw_for_test(2), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Waiting { .. }));
        assert_eq!(retries.len(), 1);

        // Elder child 1 dies in the Waiting window → widen to {1,2}: the stale
        // deadline is removed, one fresh deadline armed.
        children
            .get_mut(ActorId::from_raw_for_test(1))
            .unwrap()
            .handle = None; // it died
        handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::RestForOne,
            &mut rng,
            &notice(ActorId::from_raw_for_test(1), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Waiting { .. }));
        assert_eq!(
            retries.len(),
            1,
            "stale deadline removed, exactly one armed"
        );
        assert!(
            children
                .get_mut(ActorId::from_raw_for_test(1))
                .unwrap()
                .cycling
        );
    }

    /// `rebuild_child` on a cycling entry is a no-op: a pre-cycle solo backoff
    /// deadline firing mid-cycle must not rebuild one member of a set mid-teardown
    /// (drop-at-fire supersession — 2a discards solo `Key`s).
    #[tokio::test(start_paused = true)]
    async fn rebuild_child_is_superseded_for_cycling_entries() {
        let mut children = table_of(1);
        children
            .get_mut(ActorId::from_raw_for_test(1))
            .unwrap()
            .handle = None;
        children
            .get_mut(ActorId::from_raw_for_test(1))
            .unwrap()
            .cycling = true;
        let (sup, _link_rx) = supervisor(ActorId::from_raw_for_test(9));

        rebuild_child(&mut children, &sup, ActorId::from_raw_for_test(1));

        let entry = children
            .get_mut(ActorId::from_raw_for_test(1))
            .expect("entry retained");
        assert!(entry.handle.is_none(), "no rebuild while cycling");
        assert!(entry.cycling, "flag untouched — the cycle still owns it");
    }

    /// Removal mid-cycle (`unsupervise`/`stop_child` of an awaited member) must
    /// count the teardown down — else the cycle waits forever for a death that
    /// will land as a table-miss (the wedge counterexample).
    #[tokio::test(start_paused = true)]
    async fn removing_an_awaited_member_counts_the_teardown_down() {
        let mut children = table_of(2);
        children
            .get_mut(ActorId::from_raw_for_test(1))
            .unwrap()
            .handle = None;
        let (sup, _link_rx) = supervisor(ActorId::from_raw_for_test(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::OneForAll,
            &mut rng,
            &notice(ActorId::from_raw_for_test(1), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 1, .. }));

        apply_supervision_op(
            &mut children,
            &sup,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionOp::Remove(ActorId::from_raw_for_test(2)),
        );

        assert!(
            matches!(cycle, CycleState::Waiting { .. }),
            "last awaited member removed ⇒ rebuild armed"
        );
        assert!(
            children.get_mut(ActorId::from_raw_for_test(2)).is_none(),
            "entry gone, never rebuilt"
        );
    }

    /// Stopping a member that is NOT an awaited cycle member must NOT count the
    /// teardown down: `was_awaited` needs BOTH `cycling` AND a live handle.
    /// Under `RestForOne` an elder stays non-cycling during a junior's cycle;
    /// `stop_child`ing it mid-cycle leaves `awaiting` untouched. Kills the
    /// `&& → ||` mutant in the `Stop` arm — under `||` a non-cycling *live*
    /// child would wrongly decrement the teardown count and prematurely arm the
    /// rebuild.
    #[tokio::test(start_paused = true)]
    async fn stopping_a_non_cycling_member_mid_cycle_does_not_count_down() {
        let mut children = table_of(3);
        children
            .get_mut(ActorId::from_raw_for_test(2))
            .unwrap()
            .handle = None; // the trigger died
        let (sup, _link_rx) = supervisor(ActorId::from_raw_for_test(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        // RestForOne on child 2 → cycle {2,3}; child 3 live+awaited (awaiting 1);
        // child 1 (the elder) stays non-cycling and live.
        handle_child_death(
            &mut children,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionStrategy::RestForOne,
            &mut rng,
            &notice(ActorId::from_raw_for_test(2), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 1, .. }));

        // Stop the non-cycling elder: it has a live handle but is NOT cycling,
        // so it was never awaited — the teardown count must be unchanged.
        apply_supervision_op(
            &mut children,
            &sup,
            &mut SetCycleCtx {
                retries: &mut retries,
                pending_aborts: &mut pending_aborts,
                cycle: &mut cycle,
            },
            SupervisionOp::Stop(ActorId::from_raw_for_test(1)),
        );

        assert!(
            matches!(cycle, CycleState::Tearing { awaiting: 1, .. }),
            "stopping a non-cycling member must NOT count the teardown down",
        );
        assert!(
            children.get_mut(ActorId::from_raw_for_test(1)).is_none(),
            "the elder was stopped and its entry removed",
        );
    }
}
