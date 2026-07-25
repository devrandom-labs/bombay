//! The handle to a running actor: inline `id` + ONE shared allocation
//! (ADR-0010), so a clone is a single Arc RMW instead of three contended
//! cacheline hits (the measured #119/#186 bottleneck).
//!
//! Liveness stays flume's `sender_count` (ADR-0003): every strong `ActorRef`
//! shares the one [`MailboxSender`] inside [`RefShared`], so external handles
//! together contribute exactly 1 — dropping the last strong ref drops the
//! sender and the `1 → 0` transition wakes the loop's `recv` with `None`.

use core::fmt;
use std::sync::{Arc, Weak};

use futures::stream::AbortHandle;
use tokio_util::sync::CancellationToken;

use tokio::time::Instant;

use crate::{
    actor::{
        Actor, Spawn, Supervisor, Watch,
        supervision::{
            Child, ChildHandle, RebuildFactory, Spawned, SuperviseReg, SupervisionOp,
            watch_installer,
        },
    },
    error::{ActorNotLinked, ActorStopReason, TellError},
    mailbox::{ActorId, MailboxSender, Signal},
    reply::ReplySender,
    request::{AskRequest, TellRequest},
    restart::{RestartConfig, RestartTracker},
    watch::{LinkDied, WatchReg},
};

/// The one heap allocation every strong handle to an actor shares (ADR-0010):
/// the external mailbox sender plus the cold lifecycle handles.
struct RefShared<A: Actor> {
    sender: MailboxSender<A>,
    cancel: CancellationToken,
    abort: AbortHandle,
    /// The actor's own link-channel sender — `Some` only for actors spawned via
    /// `spawn_linked` (they can watch); `None` for plain actors. Behind the
    /// shared `Arc`, so it does NOT change clone cost (still one Arc RMW) nor the
    /// two-word size of [`ActorRef`].
    link_tx: Option<crate::watch::LinkSender>,
}

/// A cloneable handle to a running actor: enqueue signals, stop it gracefully,
/// or kill it.
///
/// Two words — inline `id` + one shared pointer — so a clone is one Arc RMW
/// (ADR-0010). Dropping the last strong `ActorRef` stops the actor after its
/// queued backlog drains (ref-count-driven stop, ADR-0003).
pub struct ActorRef<A: Actor> {
    id: ActorId,
    shared: Arc<RefShared<A>>,
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<A: Actor> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("id", &self.id)
            .field("actor", &A::name())
            .finish_non_exhaustive()
    }
}

impl<A: Actor> ActorRef<A> {
    /// Assembles a strong handle around its parts, minting a fresh shared
    /// allocation. Called once per actor at spawn — and once per drained
    /// message in the post-external-refs drain window, where the run-loop
    /// rebuilds a handler's ref from the queued `self_sender` plus its own
    /// cold copies (ADR-0010).
    pub(crate) fn new(
        id: ActorId,
        sender: MailboxSender<A>,
        cancel: CancellationToken,
        abort: AbortHandle,
        link_tx: Option<crate::watch::LinkSender>,
    ) -> Self {
        Self {
            id,
            shared: Arc::new(RefShared {
                sender,
                cancel,
                abort,
                link_tx,
            }),
        }
    }

    /// This actor's own link-channel sender, if it was spawned linked (`None`
    /// for a plain-`spawn`ed actor, which cannot watch).
    pub(crate) fn link_tx(&self) -> Option<&crate::watch::LinkSender> {
        self.shared.link_tx.as_ref()
    }

    /// The actor's scaffold identity (replaced by the AID in #121).
    #[must_use]
    pub const fn id(&self) -> ActorId {
        self.id
    }

    /// Whether the actor is still running (its mailbox is still open). A
    /// send-and-observe backup — never a pre-send gate (a send races the stop
    /// regardless), so prefer acting on the [`TellError`] a [`tell`](Self::tell)
    /// returns.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        !self.shared.sender.is_closed()
    }

    /// Prepares a fire-and-forget send of `msg` (card #118). The returned
    /// [`TellRequest`] does nothing until consumed:
    ///
    /// - `.await` — waits for mailbox capacity (backpressure), resolving to
    ///   [`TellError::ActorNotAlive`](crate::error::TellError::ActorNotAlive)
    ///   with `msg` handed back if the actor has stopped.
    /// - [`.try_send()`](TellRequest::try_send) — non-blocking; a full mailbox
    ///   is [`TellError::MailboxFull`](crate::error::TellError::MailboxFull).
    pub fn tell(&self, msg: A::Msg) -> TellRequest<'_, A> {
        TellRequest::new(&self.shared.sender, msg)
    }

    /// Prepares a request/reply `ask` (card #118): builds the message around a
    /// fresh typed reply port and returns an [`AskRequest`] that delivers it
    /// and awaits the reply on `.await`.
    ///
    /// ```ignore
    /// let count = actor_ref.ask(|reply| CounterMsg::Get { reply }).await?;
    /// ```
    ///
    /// One deadline (default [`DEFAULT_ASK_TIMEOUT`](crate::request::DEFAULT_ASK_TIMEOUT),
    /// override via [`timeout`](AskRequest::timeout), opt out via
    /// [`no_timeout`](AskRequest::no_timeout)) budgets delivery *and* reply.
    /// Handlers must never `ask(..).await` another actor (#122-#4) — that is
    /// the bounded-mailbox cycle deadlock; the deadline is the backstop, not
    /// the license.
    pub fn ask<R, E>(
        &self,
        make_msg: impl FnOnce(ReplySender<R, E>) -> A::Msg,
    ) -> AskRequest<'_, A, R, E> {
        AskRequest::new(&self.shared.sender, make_msg)
    }

    /// The sender half of the actor's mailbox — used to enqueue `Signal`s. The
    /// ergonomic `tell`/`ask` builders wrap this in #118.
    #[must_use]
    pub fn mailbox_sender(&self) -> &MailboxSender<A> {
        &self.shared.sender
    }

    /// The loop's graceful-cancellation token (loop-internal).
    pub(crate) fn cancel_token(&self) -> &CancellationToken {
        &self.shared.cancel
    }

    /// The loop's hard-kill handle (loop-internal): the run-loop copies it out
    /// before dropping its strong self-ref, so drain-window handler refs can
    /// be minted with the REAL abort handle (ADR-0010).
    pub(crate) fn abort_handle(&self) -> &AbortHandle {
        &self.shared.abort
    }

    /// Requests a graceful, out-of-band stop: the in-flight message finishes,
    /// then the actor stops and `on_stop` runs. Queued messages are abandoned.
    pub fn stop(&self) {
        self.shared.cancel.cancel();
    }

    /// Hard-kills the actor: the task is aborted at its next await point,
    /// `on_stop` does **not** run, and any in-flight message is dropped.
    pub fn kill(&self) {
        self.shared.abort.abort();
    }

    /// Downgrades to a non-pinning [`WeakActorRef`].
    #[must_use]
    pub fn downgrade(&self) -> WeakActorRef<A> {
        WeakActorRef {
            id: self.id,
            shared: Arc::downgrade(&self.shared),
        }
    }
}

/// The death-watch verbs (card #195). Only a [`Watch`] actor can watch, and only
/// if it was spawned via `spawn_linked` (so it owns a link channel to receive
/// death notices on); a plain-spawned `Watch` actor returns [`ActorNotLinked`].
impl<A: Watch> ActorRef<A> {
    /// Watches `target`: this actor's [`on_link_died`](Watch::on_link_died) fires
    /// when `target` stops. One-directional and notify-only (`linked = false`), so
    /// the default hook merely observes — a `target` death never propagates here.
    /// `target` may be any [`Actor`] (being watched is universal); it need not
    /// itself be a [`Watch`] actor.
    ///
    /// The registration rides `target`'s bounded message mailbox, so this `.await`s
    /// for mailbox capacity — ordinary backpressure, not a failure. It resolves only
    /// once the registration is enqueued (or `target` is found already dead).
    ///
    /// # Errors
    ///
    /// [`ActorNotLinked`] if this actor was spawned via the plain `spawn` path and
    /// so has no link channel to receive notices on. Spawn watchers with
    /// `spawn_linked`.
    pub async fn watch<B: Actor>(&self, target: &ActorRef<B>) -> Result<(), ActorNotLinked> {
        self.register_on(target, false).await
    }

    /// Links with `peer`: bidirectional. Each side's
    /// [`on_link_died`](Watch::on_link_died) fires on the other's death; the
    /// default hook propagates an abnormal death (`Break`). Requires both actors to
    /// be [`Watch`] (both must react). If `peer` is already dead, its side yields an
    /// immediate synthetic notice on this actor's channel (Erlang's link-to-dead
    /// rule).
    ///
    /// Both link channels are checked present **before** either registration, so a
    /// missing channel is an atomic `Err` with no half-installed one-directional
    /// edge. Like [`watch`](Self::watch), each registration `.await`s for the peer's
    /// mailbox capacity (backpressure).
    ///
    /// Repeated `link` calls install duplicate edges (a recorded divergence from
    /// Erlang's at-most-one link per pair; the duplicate notice's first `Break`
    /// wins). Self-linking (`a.link(&a)`) is likewise not special-cased — where
    /// Erlang's is a no-op, it installs a self-edge whose death notice lands on
    /// the actor's own, by-then-undrained channel: harmless.
    ///
    /// # Errors
    ///
    /// [`ActorNotLinked`] if **either** actor lacks a link channel (was not spawned
    /// via `spawn_linked`) — checked up front, so neither side is mutated on `Err`.
    pub async fn link<B: Watch>(&self, peer: &ActorRef<B>) -> Result<(), ActorNotLinked> {
        // Both sides must be linked-spawned before either edge is installed, so a
        // plain-spawned peer yields a clean `Err` and never a half-link.
        if self.link_tx().is_none() || peer.link_tx().is_none() {
            return Err(ActorNotLinked);
        }
        self.register_on(peer, true).await?;
        peer.register_on(self, true).await
    }

    /// Stops watching `target`: removes **every** edge this actor holds on
    /// `target` — watch and link edges alike, coarser than Erlang's per-monitor
    /// `demonitor`. Best-effort — the send `.await`s for capacity and, if
    /// `target` has already stopped, simply fails with nothing left to remove.
    /// As with Erlang's `demonitor`, an `unwatch` racing the target's death may
    /// still be followed by a delivered notice.
    pub async fn unwatch<B: Actor>(&self, target: &ActorRef<B>) {
        let _ = target
            .mailbox_sender()
            .send(Signal::Unwatch(self.id()))
            .await;
    }

    /// Registers this actor as a watcher on `target` with the given `linked` flag:
    /// reads this actor's own `link_tx` (the receive end the notice will arrive on)
    /// and enqueues a [`WatchReg`] onto `target`'s mailbox, `.await`ing for capacity.
    ///
    /// The `.await` on [`send`](MailboxSender::send) is true backpressure: a
    /// momentarily-full but alive mailbox makes it WAIT, and it errors **only** when
    /// the mailbox is closed (`target` dead). On that closed error this actor is
    /// given an immediate synthetic [`LinkDied`] on its own channel (Erlang's
    /// link-to-dead rule) — the reason is
    /// [`AlreadyDead`](ActorStopReason::AlreadyDead), its own failure domain
    /// (Erlang's `noproc`): the target's true reason is unknowable once its mailbox
    /// is gone, and conflating that with a real [`Killed`](ActorStopReason::Killed)
    /// would misinform a supervisor. A full-mailbox (`Full`) case must never take
    /// this branch, or ordinary backpressure would self-terminate a linked watcher.
    async fn register_on<B: Actor>(
        &self,
        target: &ActorRef<B>,
        linked: bool,
    ) -> Result<(), ActorNotLinked> {
        let Some(link_tx) = self.link_tx() else {
            return Err(ActorNotLinked);
        };
        let reg = WatchReg {
            watcher: self.id(),
            link_tx: link_tx.clone(),
            linked,
        };
        if target
            .mailbox_sender()
            .send(Signal::Watch(Box::new(reg)))
            .await
            .is_err()
        {
            // `send().await` errors ONLY on a closed mailbox (target dead), never on
            // a full-but-alive one — so this is the genuine link-to-dead path.
            let _ = link_tx.try_send(LinkDied {
                id: target.id(),
                reason: ActorStopReason::AlreadyDead,
                linked,
                // Synthetic notice: the target's teardown is unobservable from
                // here, so claiming a cleanup failure would be a fabrication.
                cleanup_failed: false,
            });
        }
        Ok(())
    }
}

/// The restart-supervision verb (card #196). Only a [`Supervisor`] can supervise,
/// and — like [`watch`](Self::watch)/[`link`](Self::link) — only because it was
/// spawned via `spawn_supervised` and so owns the link channel a child's death
/// arrives on.
impl<S: Supervisor> ActorRef<S> {
    /// Registers a supervised child under an explicit restart policy. The first
    /// incarnation is spawned HERE, in the caller's task — which is what lets this
    /// be a `tell` returning the child's [`ActorId`]. The closure re-runs per
    /// rebuild inside the supervisor's loop; it **spawns only** (the loop installs
    /// the watch edge, so a child is never observably dead before it is in the
    /// table).
    ///
    /// The wrapper drops the strong [`ActorRef<A>`] the closure returns — the
    /// supervisor never pins a child (ADR-0003), so keeping a supervised child
    /// *reachable* is the caller's job (a name in the registry, a captured handle,
    /// or work the child itself drives). The closure must capture only the child's
    /// spawn inputs — **never** a strong `ActorRef<S>` of the supervisor (kameo
    /// #171: a strong self-ref in the loop-owned table makes ref-count-driven stop
    /// unreachable).
    ///
    /// **An unanchored child is actively fatal, not merely idle.** The instant the
    /// loop installs the watch edge and drops the installer's transient sender, a
    /// child no one else holds a strong ref to has zero senders and ref-count-stops
    /// (ADR-0003). For a [`Permanent`](crate::restart::RestartPolicy::Permanent)
    /// child — or a [`Transient`](crate::restart::RestartPolicy::Transient) one
    /// that dies abnormally — the supervisor rebuilds it, the rebuild also stops
    /// at once, and every incarnation dies with an uptime of ≈0. That never earns
    /// the healthy-uptime reset (default `reset_after` = 1 min), so `consecutive`
    /// only climbs: within a few backoffs the supervisor trips `max_restarts`
    /// (default 5) and **escalates to its OWN death via
    /// [`RestartLimitExceeded`](crate::error::ActorStopReason::RestartLimitExceeded).**
    /// Where an ordinary unreferenced actor just stops quietly, supervision
    /// converts "the child quietly stopped" into rebuild churn that kills the
    /// supervisor — so a supervised child MUST have a liveness anchor.
    ///
    /// This `.await`s for the supervisor's own mailbox capacity — ordinary
    /// backpressure, not failure.
    ///
    /// # Errors
    ///
    /// [`TellError::ActorNotAlive`] if the supervisor's mailbox is closed (it has
    /// stopped). The first incarnation was already spawned; the dropped
    /// [`SendError`](crate::mailbox::SendError) takes the registration — and with
    /// it the installer's transient sender — so an *unanchored* first incarnation
    /// then ref-count-stops rather than continuing, while an anchored one keeps
    /// running, now unsupervised.
    pub async fn supervise<A, F>(
        &self,
        config: impl Into<RestartConfig>,
        mut factory: F,
    ) -> Result<ActorId, TellError<()>>
    where
        A: Actor,
        F: FnMut() -> ActorRef<A> + Send + 'static,
    {
        // The erased, spawn-only rebuild edge: each call runs the user's spawn,
        // lifts the sender-less handle + a one-shot watch installer out of the
        // fresh child, and drops the strong ref (never pin the child).
        let mut rebuild: RebuildFactory = Box::new(move || spawn_child(&mut factory));
        // Spawn the first incarnation inline, in the caller's task, so its id can
        // be returned; the loop installs its watch edge after the table insert.
        let Spawned {
            handle,
            install_watch,
        } = rebuild();
        let id = handle.id();
        let reg = SuperviseReg {
            child: Child {
                factory: rebuild,
                handle: Some(handle),
                config: config.into(),
                tracker: RestartTracker::new(Instant::now()),
            },
            id,
            install_watch,
        };
        match self
            .mailbox_sender()
            .send(Signal::Supervision(Box::new(SupervisionOp::Add(reg))))
            .await
        {
            Ok(()) => Ok(id),
            // The supervisor's mailbox is closed (it stopped). `send().await` errors
            // only on a closed mailbox, so this is the genuine dead-supervisor path.
            Err(_) => Err(TellError::ActorNotAlive(())),
        }
    }

    /// Stops supervising `id`: drops the supervision edge and **detaches** the
    /// child. The child KEEPS RUNNING (use [`stop_child`](Self::stop_child) to also
    /// stop it), is never rebuilt again, and its later death — normal or abnormal —
    /// no longer affects the supervisor. The supervisor *monitors* its children
    /// (it reacts to a child's death through its restart table, not by propagating
    /// it), so once the table entry is dropped a subsequent death for `id` is a
    /// non-child notice the supervisor simply ignores; even a notice already in
    /// flight is harmless.
    ///
    /// Best-effort against a **concurrently-dying** child (Erlang's `demonitor`
    /// racing an exit): if the child is already failing and its restart is armed
    /// when the `Remove` is applied, the rebuild may re-key the entry under the new
    /// incarnation's id first, leaving the `Remove` to no-op on the stale key — the
    /// child then stays supervised under a fresh id. Detachment is guaranteed only
    /// for a child that is not mid-restart.
    ///
    /// The op rides the supervisor's own mailbox (the child table is loop-owned;
    /// all mutation goes through the loop), so this `.await`s for mailbox capacity
    /// — ordinary backpressure, not failure.
    ///
    /// # Errors
    ///
    /// [`TellError::ActorNotAlive`] if the supervisor's mailbox is closed (it has
    /// stopped); the edge it would have dropped is already gone with it.
    pub async fn unsupervise(&self, id: ActorId) -> Result<(), TellError<()>> {
        self.send_supervision(SupervisionOp::Remove(id)).await
    }

    /// Stops supervising `id` AND stops the child: `cancel` → `stop_grace` →
    /// `abort` (OTP's `terminate_child/2`). Use this, never [`kill`](Self::kill),
    /// to permanently stop a supervised child — `kill` is an abnormal exit a
    /// [`Permanent`](crate::restart::RestartPolicy::Permanent)/[`Transient`](crate::restart::RestartPolicy::Transient)
    /// policy would rebuild, whereas `stop_child` drops the edge first so the death
    /// can never route to a rebuild.
    ///
    /// The stop is crash-only and bounded: the child is asked to stop gracefully,
    /// then hard-aborted if it has not stopped within its `stop_grace` — it never
    /// depends on the child cooperating. The op rides the supervisor's own mailbox,
    /// so this `.await`s for mailbox capacity (backpressure).
    ///
    /// Best-effort against a **concurrently-dying** child, exactly as
    /// [`unsupervise`](Self::unsupervise): a child whose restart is already armed
    /// may be re-keyed under a new incarnation before the `Stop` lands, which then
    /// no-ops on the stale key.
    ///
    /// # Errors
    ///
    /// [`TellError::ActorNotAlive`] if the supervisor's mailbox is closed (it has
    /// stopped); a stopped supervisor has already dropped every child handle.
    pub async fn stop_child(&self, id: ActorId) -> Result<(), TellError<()>> {
        self.send_supervision(SupervisionOp::Stop(id)).await
    }

    /// Ships a child-table [`SupervisionOp`] to the supervisor's own mailbox, the
    /// table's single writer. Errors only on a closed mailbox (`send().await`
    /// never errors on a full-but-alive one), which is the genuine dead-supervisor
    /// path — mapped to the terminal [`TellError::ActorNotAlive`].
    async fn send_supervision(&self, op: SupervisionOp) -> Result<(), TellError<()>> {
        match self
            .mailbox_sender()
            .send(Signal::Supervision(Box::new(op)))
            .await
        {
            Ok(()) => Ok(()),
            Err(_) => Err(TellError::ActorNotAlive(())),
        }
    }

    /// [`supervise`](Self::supervise) shorthand for a child whose `Args` are
    /// `Clone`: the rebuild closure re-spawns `A` from a fresh clone of `args`
    /// each incarnation.
    ///
    /// # Errors
    ///
    /// [`TellError::ActorNotAlive`] if the supervisor's mailbox is closed, exactly
    /// as [`supervise`](Self::supervise).
    pub async fn supervise_cloned<A: Actor>(
        &self,
        config: impl Into<RestartConfig>,
        args: A::Args,
    ) -> Result<ActorId, TellError<()>>
    where
        A::Args: Clone,
    {
        self.supervise(config, move || A::spawn(args.clone())).await
    }
}

/// Runs one spawn from the user's factory and lifts the fresh child into a
/// [`Spawned`]: its sender-less [`ChildHandle`] plus the one-shot that installs
/// the supervisor's watch edge (over a transient clone of the child's sender).
///
/// The strong [`ActorRef`] is dropped as this returns — the child table never
/// holds one (ADR-0003) — and the installer's captured sender is itself dropped
/// the instant the loop calls it, so nothing here pins the child past
/// registration.
fn spawn_child<A: Actor>(factory: &mut impl FnMut() -> ActorRef<A>) -> Spawned {
    let child = factory();
    let handle = ChildHandle {
        id: child.id(),
        cancel: child.cancel_token().clone(),
        abort: child.abort_handle().clone(),
    };
    let install_watch = watch_installer(child.mailbox_sender().clone());
    Spawned {
        handle,
        install_watch,
    }
}

/// A non-pinning handle to an actor: inline `id` (a tombstone that outlives
/// the actor) + one weak pointer.
///
/// [`upgrade`](WeakActorRef::upgrade) yields a strong [`ActorRef`] only while
/// an **external strong ref** still exists — in the drain window (external
/// refs gone, queued messages still pinning the channel) it answers `None`,
/// because an actor no external handle can reach is dying and must not be
/// resurrectable (ADR-0010).
pub struct WeakActorRef<A: Actor> {
    id: ActorId,
    shared: Weak<RefShared<A>>,
}

impl<A: Actor> Clone for WeakActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            shared: Weak::clone(&self.shared),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakActorRef")
            .field("id", &self.id)
            .field("actor", &A::name())
            .finish_non_exhaustive()
    }
}

impl<A: Actor> WeakActorRef<A> {
    /// The actor's scaffold identity.
    #[must_use]
    pub const fn id(&self) -> ActorId {
        self.id
    }

    /// Upgrades to a strong [`ActorRef`], or `None` once every external strong
    /// ref has dropped — one CAS on the shared allocation's refcount
    /// (`std::sync::Weak::upgrade`, ADR-0010). `None` does not always mean the
    /// backlog is done: queued messages may still be draining (they self-pin
    /// via their `self_sender`, ADR-0003), but no new external handle to the
    /// dying actor can be minted from here.
    #[must_use]
    pub fn upgrade(&self) -> Option<ActorRef<A>> {
        self.shared.upgrade().map(|shared| ActorRef {
            id: self.id,
            shared,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use crate::{
        error::TellError,
        mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed, Signal},
        message::Msg,
    };

    // A minimal Actor purely to key the mailbox/ref. `on_start`/`handle` are
    // never called in this task's tests (no loop yet) — they exist so the type
    // satisfies `Actor`. `ProbeMsg` carries a `u64` so a delivery-failure test
    // can prove the *exact* undelivered message is handed back, not just the
    // variant (a ZST would make the handback unfalsifiable).
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

    // Keeps the `MailboxReceiver` alive so a test can *reap* the actor on its own
    // terms (dropping the receiver is exactly what the run-loop does on stop —
    // see the `mailbox` module doc). Dropping the receiver early would leave the
    // channel already disconnected before the test even begins.
    fn build_ref_with_rx() -> (ActorRef<Probe>, WeakActorRef<Probe>, MailboxReceiver<Probe>) {
        let cap = Capacity::try_from(4usize).expect("valid capacity");
        let (tx, rx) = Mailbox::<Probe>::bounded(cap, ActorId::new(7));
        let (abort, _reg) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(ActorId::new(7), tx, CancellationToken::new(), abort, None);
        let weak = actor_ref.downgrade();
        (actor_ref, weak, rx)
    }

    fn build_ref() -> (ActorRef<Probe>, WeakActorRef<Probe>) {
        let (actor_ref, weak, _rx) = build_ref_with_rx();
        (actor_ref, weak)
    }

    /// #186 / ADR-0010: each handle is `id` + ONE shared pointer — two words.
    /// Fails while the ref carries independently-shared fields (the ADR-0003
    /// shape was four words), guarding the single-allocation layout the 1-RMW
    /// clone claim rests on.
    #[test]
    fn handles_are_two_words() {
        assert_eq!(
            size_of::<ActorRef<Probe>>(),
            2 * size_of::<usize>(),
            "ActorRef = inline id + one Arc pointer",
        );
        assert_eq!(
            size_of::<WeakActorRef<Probe>>(),
            2 * size_of::<usize>(),
            "WeakActorRef = inline id (the tombstone) + one Weak pointer",
        );
    }

    /// #186 / ADR-0010, the one semantic change: in the DRAIN WINDOW (every
    /// external strong ref dropped, a queued message still pinning the channel
    /// via its `self_sender`) a weak upgrade answers `None` — an actor no
    /// external handle can reach is dying and must not be resurrectable — while
    /// the queued message itself is still delivered (ADR-0003's self-pin is
    /// untouched). Fails under the ADR-0003 shape, where `upgrade` reads
    /// flume's `sender_count` and the queued self_sender keeps it non-zero.
    #[tokio::test]
    async fn weak_upgrade_is_none_in_the_drain_window() {
        let (actor_ref, weak, mut rx) = build_ref_with_rx();
        actor_ref
            .tell(ProbeMsg(9))
            .try_send()
            .expect("open mailbox accepts the message");

        drop(actor_ref); // drain window: only the queued self_sender remains

        assert!(
            weak.upgrade().is_none(),
            "no external strong ref exists, so upgrade must not resurrect",
        );
        let queued = rx.recv().await;
        assert!(
            matches!(
                queued,
                Some(Signal::Message {
                    msg: ProbeMsg(9),
                    ..
                })
            ),
            "the queued message still self-pins and is delivered",
        );
    }

    /// The `ActorRef` debug view names the struct and surfaces its id and actor
    /// name — guards the hand-written `Debug` impl against being stubbed to an
    /// empty formatter (`Ok(Default::default())`).
    #[test]
    fn actor_ref_debug_names_struct_id_and_actor() {
        let (actor_ref, _weak) = build_ref();
        let shown = format!("{actor_ref:?}");
        assert!(
            shown.contains("ActorRef"),
            "debug names the struct: {shown}"
        );
        assert!(shown.contains('7'), "debug surfaces the id: {shown}");
        assert!(
            shown.contains("Probe"),
            "debug surfaces the actor name: {shown}"
        );
    }

    /// Same guard for the weak handle's `Debug` impl.
    #[test]
    fn weak_actor_ref_debug_names_struct_and_id() {
        let (_actor_ref, weak) = build_ref();
        let shown = format!("{weak:?}");
        assert!(
            shown.contains("WeakActorRef"),
            "debug names the struct: {shown}"
        );
        assert!(shown.contains('7'), "debug surfaces the id: {shown}");
        assert!(
            shown.contains("Probe"),
            "debug surfaces the actor name: {shown}"
        );
    }

    /// `Actor::name` defaults to the concrete type name — guards the trait
    /// default against being stubbed to a constant/empty string.
    #[test]
    fn actor_name_defaults_to_type_name() {
        assert!(
            Probe::name().contains("Probe"),
            "name() returns the type name, got {:?}",
            Probe::name(),
        );
    }

    /// Lifecycle: a weak ref upgrades while the mailbox is open, and returns
    /// `None` once every strong sender (incl. the one inside `ActorRef`) drops.
    #[tokio::test]
    async fn weak_upgrades_while_open_then_none_after_drop() {
        let (actor_ref, weak) = build_ref();
        assert_eq!(weak.id(), ActorId::new(7));
        assert!(weak.upgrade().is_some(), "mailbox open -> upgradable");

        drop(actor_ref);
        assert!(
            weak.upgrade().is_none(),
            "all strong senders dropped -> not upgradable",
        );
    }

    /// A `WeakActorRef` — even several clones of one — carries no pinning power:
    /// once the sole strong `ActorRef` drops, the mailbox channel is gone. Proven
    /// from both ends: the weak handle cannot re-`upgrade` to a strong sender, and
    /// the receiver observes the channel as disconnected (`recv` yields `None`).
    #[tokio::test]
    async fn weak_actor_ref_does_not_pin_channel() {
        let (actor_ref, weak, mut rx) = build_ref_with_rx();
        let weak_clone = weak.clone();

        drop(actor_ref); // only weak handles remain

        assert!(
            weak.upgrade().is_none(),
            "a WeakActorRef must not resurrect a strong sender",
        );
        assert!(
            weak_clone.upgrade().is_none(),
            "cloning the weak handle adds no pinning power",
        );
        assert!(
            rx.recv().await.is_none(),
            "a weak handle must not keep the mailbox channel open",
        );
    }

    /// `@bug` — a `WeakActorRef` captured while the actor was alive must never be
    /// a back door to resurrect it after the actor is reaped (every strong sender
    /// dropped *and* the run-loop's receiver gone). `upgrade` stays `None`, and
    /// re-cloning the stale handle is not a resurrection path either. The `id`
    /// survives as a tombstone (useful for logging a dead link) but must not
    /// imply liveness. FAILS if `upgrade` ever hands back a sender for a
    /// disconnected channel.
    #[tokio::test]
    async fn stale_ref_cannot_resurrect_reaped_actor() {
        let (actor_ref, _weak, rx) = build_ref_with_rx();
        let stale = actor_ref.downgrade();

        drop(actor_ref);
        drop(rx); // full reap: no senders, no receiver

        assert!(
            stale.upgrade().is_none(),
            "a reaped actor cannot be upgraded from a stale weak ref",
        );
        assert!(
            stale.clone().upgrade().is_none(),
            "cloning a stale weak ref does not resurrect the actor",
        );
        assert_eq!(
            stale.id(),
            ActorId::new(7),
            "the id survives as a tombstone, but that is not liveness",
        );
    }

    /// A `tell` to a reaped actor fails terminally with
    /// [`TellError::ActorNotAlive`], handing the *bare, undelivered* message
    /// straight back — nothing is lost into the void, and a retry loop can see
    /// (via `is_terminal`) that re-sending would only spin.
    #[tokio::test]
    async fn send_to_reaped_actor_returns_actor_not_alive() {
        let (actor_ref, _weak, rx) = build_ref_with_rx();

        drop(rx); // reap: the run-loop's receiver is gone

        let err = actor_ref
            .tell(ProbeMsg(42))
            .await
            .expect_err("tell to a reaped actor must fail");

        assert!(
            err.is_terminal(),
            "a reaped actor is terminal, never retryable",
        );
        let TellError::ActorNotAlive(ProbeMsg(returned)) = err else {
            panic!("expected ActorNotAlive carrying the message, got {err:?}");
        };
        assert_eq!(returned, 42, "the exact undelivered message is handed back");
    }

    /// `upgrade`'s happy path could be stubbed to `None` (a viable mutant) or
    /// to a handle around FRESH parts. This is the compensating control: an
    /// upgraded ref shares the ORIGINAL's identity and lifecycle handles,
    /// proven by observing shared state through each field.
    ///
    /// - `id`: a plain `Copy` value — `assert_eq!` is a direct, exact check.
    /// - `cancel`: `CancellationToken::clone` "will get cancelled whenever
    ///   the current token gets cancelled, and vice versa" (tokio-util
    ///   `cancellation_token.rs` doc on `impl Clone`) — so cancelling
    ///   through the upgraded ref must flip the ORIGINAL's token too, which a
    ///   fresh `CancellationToken::new()` could never do.
    /// - `abort`: `AbortHandle` clones share one `Arc<AbortInner>` (futures
    ///   `abortable.rs`) — `is_aborted` reads that shared `AtomicBool`. Both
    ///   fields are private but observable here: `tests` is a descendant
    ///   module of the struct's defining module, so plain field access
    ///   (`.shared.cancel`, `.shared.abort`) is in scope without needing a
    ///   public liveness-identity accessor that would otherwise leak them.
    #[tokio::test]
    async fn upgrade_preserves_id_cancel_and_abort() {
        let (actor_ref, weak, _rx) = build_ref_with_rx();

        let upgraded = weak.upgrade().expect("strong ref alive -> upgradable");

        assert_eq!(
            upgraded.id(),
            ActorId::new(7),
            "id must be copied verbatim from the weak ref",
        );

        assert!(
            !actor_ref.shared.cancel.is_cancelled(),
            "precondition: nothing cancelled yet",
        );
        upgraded.stop();
        assert!(
            actor_ref.shared.cancel.is_cancelled(),
            "upgrade must share the SAME cancellation token as the \
             original — a fresh token would leave the original uncancelled",
        );

        assert!(
            !actor_ref.shared.abort.is_aborted(),
            "precondition: nothing aborted yet",
        );
        upgraded.kill();
        assert!(
            actor_ref.shared.abort.is_aborted(),
            "upgrade must share the SAME abort handle as the original — \
             a fresh handle would leave the original's abort unset",
        );
    }

    /// Liveness is a property of the shared channel, not of any one handle:
    /// `is_alive`/`is_closed` read identically across cloned senders. A surviving
    /// clone keeps the actor alive after the original drops; reaping the actor
    /// (receiver gone) flips *every* clone to closed at once.
    #[tokio::test]
    async fn cloned_sender_liveness_via_is_closed() {
        let (actor_ref, _weak, rx) = build_ref_with_rx();
        let clone = actor_ref.clone();

        assert!(actor_ref.is_alive(), "original sees a live actor");
        assert!(clone.is_alive(), "clone sees the same live actor");
        assert!(
            !clone.mailbox_sender().is_closed(),
            "an open channel is not closed",
        );

        // Dropping the original strong handle does not close the channel: the
        // clone is still a strong sender and the receiver is still up.
        drop(actor_ref);
        assert!(clone.is_alive(), "a surviving clone keeps liveness true",);

        // Reaping the actor flips liveness for the clone too — is_closed reflects
        // the shared channel, not the individual handle.
        drop(rx);
        assert!(!clone.is_alive(), "the clone observes the reap");
        assert!(
            clone.mailbox_sender().is_closed(),
            "and reports the channel as closed",
        );
    }
}
