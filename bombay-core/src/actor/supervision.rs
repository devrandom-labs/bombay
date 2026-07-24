//! Supervision runtime types (card #196): the loop-owned child table and the
//! erased rebuild edge.
//!
//! The factory closure is the feature's ONLY `dyn`. It is the one place the
//! child's concrete type is in scope — spawn and the erased watch-installer it
//! hands back both live inside it — so erasing there is what lets a supervisor
//! hold children of several types in one homogeneous table without the
//! supervisor's own type growing a parameter per child. The factory itself is
//! **spawn-only**: it never installs the watch edge (the loop does, after the
//! table insert), which is what closes the #196 registration hazard.
//!
//! A factory must never capture a strong [`ActorRef`](crate::actor::ActorRef) of
//! the supervisor OR of a child: a strong ref pins liveness, which makes
//! ref-count-driven stop unreachable (ADR-0003; kameo issue #171). It captures
//! the pieces it needs to *build* a ref instead.
//!
//! The whole table is **task-owned**: it lives beside [`Watchers`] in the
//! supervisor's loop, not inside the `&mut self` a panicking handler can tear
//! (crash-only recovery applied to our own runtime — bookkeeping that survives a
//! fault is what makes the fault recoverable). Every mutation therefore arrives
//! as a [`SupervisionOp`] on the supervisor's own mailbox, which is why nothing
//! here takes a lock or documents an ordering rule.
//!
//! [`Watchers`]: crate::watch::Watchers

use core::time::Duration;

use futures::stream::AbortHandle;
use smallvec::SmallVec;
use tokio_util::sync::CancellationToken;

use crate::{
    mailbox::{ActorId, MailboxSender, Mailboxed, Signal, TrySendError},
    restart::{RestartConfig, RestartTracker},
    watch::WatchReg,
};

/// A non-generic handle to one child incarnation: its identity and its two stop
/// edges, and deliberately **no mailbox sender**.
///
/// A sender would be a strong handle to the child, pinning its mailbox open for
/// as long as the supervisor holds the entry — ref-count-driven stop (ADR-0003)
/// would then never fire for any supervised child. Supervision needs to *stop* a
/// child, not to message it: `cancel` asks for a graceful stop, `abort` is the
/// hard kill after the grace.
#[derive(Debug, Clone)]
pub struct ChildHandle {
    pub(crate) id: ActorId,
    pub(crate) cancel: CancellationToken,
    pub(crate) abort: AbortHandle,
}

impl ChildHandle {
    /// The identity of this incarnation.
    ///
    /// A rebuilt child is a **new** actor with a new [`ActorId`], so this changes
    /// across restarts and the supervisor's child table is re-keyed to match.
    #[must_use]
    pub const fn id(&self) -> ActorId {
        self.id
    }
}

/// What installing the supervisor's watch edge on a freshly-spawned child did.
///
/// The install is a single non-blocking [`try_send`](crate::mailbox::MailboxSender::try_send)
/// of a [`Signal::Watch`](crate::mailbox::Signal::Watch) onto the child's bounded
/// mailbox — never an `await`, so a slow or flooded child can never stall the
/// supervisor's loop. The three outcomes are exactly `try_send`'s three results.
pub enum WatchOutcome {
    /// The registration was accepted: the watch edge is live and the child is
    /// supervised. The normal case — a freshly-spawned child's mailbox is empty.
    Installed,
    /// The child's mailbox was **full** — it was flooded in the window between
    /// its spawn and the loop's watch-install. The child is alive but
    /// unwatchable without waiting; the loop treats this as an immediate failed
    /// incarnation rather than blocking on a bounded send.
    Full,
    /// The child's mailbox was **closed** — it died in the unwatched window
    /// between its spawn (in the caller's task) and the loop's table insert. Its
    /// own death notice never reached the supervisor (it was not yet a watcher),
    /// so the loop synthesizes the [`AlreadyDead`](crate::error::ActorStopReason::AlreadyDead)
    /// notice `register_on` uses, self-healing the lost death into a restart.
    Closed,
}

/// The one-shot that installs the supervisor's watch edge on a freshly-spawned
/// child, produced alongside the child's [`ChildHandle`] by the factory.
///
/// It is the ONE place a child's concrete type outlives `spawn`: it captures the
/// child's typed [`MailboxSender`](crate::mailbox::MailboxSender) — the sender
/// [`ChildHandle`] deliberately withholds — to enqueue the watch registration.
/// The loop calls it exactly once, immediately after the table insert, and drops
/// it; the captured strong sender therefore **never outlives registration**, so
/// the sender-less [`ChildHandle`] the table keeps cannot pin the child (ADR-0003).
///
/// `FnOnce`: one incarnation is watched once. A *rebuild* mints a fresh installer
/// from the next factory call.
pub type WatchInstaller = Box<dyn FnOnce(WatchReg) -> WatchOutcome + Send>;

/// Builds the one-shot [`WatchInstaller`] over a child incarnation's typed
/// mailbox `sender`. On call it enqueues the supervisor's watch registration
/// with a single non-blocking [`try_send`](MailboxSender::try_send) and reports
/// the outcome; then the closure — and the strong `sender` it captured — is
/// dropped, so the sender never outlives registration and the table's
/// sender-less [`ChildHandle`] cannot pin the child (ADR-0003).
pub fn watch_installer<A: Mailboxed + 'static>(sender: MailboxSender<A>) -> WatchInstaller {
    Box::new(
        move |reg| match sender.try_send(Signal::Watch(Box::new(reg))) {
            Ok(()) => WatchOutcome::Installed,
            Err(TrySendError::Full(_)) => WatchOutcome::Full,
            Err(TrySendError::Closed(_)) => WatchOutcome::Closed,
        },
    )
}

/// A freshly-spawned child incarnation as the factory hands it back: its
/// sender-less [`ChildHandle`] plus the one-shot that installs the supervisor's
/// watch edge on it.
///
/// The two are split so the watch edge can be installed **by the loop, after the
/// table insert** — the ordering that closes the registration hazard (#196): a
/// death can never be observed for an id the table does not yet hold, so it can
/// never route to the peer-watch hook and kill the supervisor.
pub struct Spawned {
    /// The child's identity and stop edges.
    pub(crate) handle: ChildHandle,
    /// Installs the supervisor's watch edge; consumed once by the loop.
    pub(crate) install_watch: WatchInstaller,
}

/// The erased rebuild edge: spawn a fresh incarnation and hand back its
/// [`Spawned`] (handle + watch installer). **Spawn-only** — it never installs the
/// watch edge itself; the loop does, after the table insert.
///
/// `FnMut`, not `FnOnce` — a child is rebuilt as many times as its budget allows.
/// **Synchronous**: spawning is non-blocking (`spawn` returns immediately) and
/// the watch-install left the factory, so there is nothing left to `await` — one
/// fewer boxed future per rebuild than the awaiting-install design would cost.
///
/// A child that spawns but then fails `on_start` is not a factory failure: it is
/// a live actor that dies, and it reports itself through the ordinary death
/// notice (or, if it died before the loop watched it, through the synthetic
/// [`WatchOutcome::Closed`] path).
pub type RebuildFactory = Box<dyn FnMut() -> Spawned + Send>;

/// One supervised child in the loop-owned table: how to rebuild it, its current
/// incarnation, and its restart tuning plus accounting.
pub struct Child {
    /// The erased rebuild edge.
    pub(crate) factory: RebuildFactory,
    /// The live incarnation, or `None` while a rebuild waits out its backoff —
    /// the window in which the child genuinely does not exist.
    pub(crate) handle: Option<ChildHandle>,
    /// This child's own tuning. Per child, not per supervisor: siblings may
    /// disagree on policy, budgets and grace.
    pub(crate) config: RestartConfig,
    /// This child's give-up accounting, carried ACROSS incarnations — a fresh
    /// tracker per rebuild would make every budget unreachable.
    pub(crate) tracker: RestartTracker,
}

/// A supervise registration in transit on the supervisor's own mailbox.
///
/// The first incarnation is already spawned — in the *caller's* task, before the
/// registration is enqueued — which is what lets `supervise` be a `tell` that
/// still returns the child's [`ActorId`] to the caller.
pub struct SuperviseReg {
    /// The table entry to install, first incarnation included.
    pub(crate) child: Child,
    /// The key to install it under: the first incarnation's id.
    pub(crate) id: ActorId,
    /// Installs the supervisor's watch edge on the first incarnation, run by the
    /// loop **after** [`child`](Self::child) is in the table — never before, or a
    /// death racing the insert would route to the peer-watch hook (the #196
    /// registration hazard).
    pub(crate) install_watch: WatchInstaller,
}

/// A child-table operation shipped over the supervisor's own mailbox.
///
/// The table is task-owned, so *all* mutation goes through the loop and arrives
/// in mailbox FIFO order — there is no lock to take and no ordering rule for a
/// caller to get wrong.
///
/// Exhaustive on purpose: the supervision verb set is deliberately closed so the
/// supervised run-loop is a total match; new arms are added under their driving
/// cards. (No `clippy::exhaustive_enums` `#[expect]` — that lint fires only on
/// *exported* enums, and this one is crate-private, riding
/// `Signal::Supervision(Box<SupervisionOp>)` without being re-exported.)
pub enum SupervisionOp {
    /// Start supervising a child that is already running.
    Add(SuperviseReg),
    /// Drop the supervision edge. The child keeps running, now unwatched — the
    /// caller is taking ownership of its lifetime.
    Remove(ActorId),
    /// Drop the edge **and** stop the child (cancel → `stop_grace` → abort) —
    /// OTP's `terminate_child/2`. Without it a caller's `kill()` races the
    /// policy, which would see the death and dutifully rebuild what the caller
    /// just asked to be gone.
    Stop(ActorId),
}

/// The supervisor's children, keyed by the **current** incarnation's
/// [`ActorId`].
///
/// Insertion order IS birth order and every operation preserves it, including
/// [`rekey`](Self::rekey) — a later card's `RestForOne` strategy restarts each
/// child born after the one that failed, so the sequence is load-bearing.
///
/// A `SmallVec` with 4 inline slots rather than a map: supervisor fan-out is
/// small, a linear scan over a handful of contiguous ids beats hashing, and a
/// map would have to be an *ordered* one to keep the property above.
pub struct Children {
    entries: SmallVec<[(ActorId, Child); 4]>,
}

impl Children {
    /// An empty table.
    pub(crate) fn new() -> Self {
        Self {
            entries: SmallVec::new(),
        }
    }

    /// Adds `child` under `id`, at the end of the birth order.
    ///
    /// The key is passed explicitly rather than read out of
    /// [`Child::handle`]: during a backoff window there is no handle to read it
    /// from, and the caller — the loop — always has the id in hand anyway.
    pub(crate) fn insert(&mut self, id: ActorId, child: Child) {
        self.entries.push((id, child));
    }

    /// The entry currently keyed by `id`, or `None`. A miss is ordinary: a death
    /// notice can name an actor this supervisor merely watches.
    pub(crate) fn get_mut(&mut self, id: ActorId) -> Option<&mut Child> {
        self.entries
            .iter_mut()
            .find(|(key, _)| *key == id)
            .map(|(_, child)| child)
    }

    /// Removes and returns the entry keyed by `id`, closing the gap so the
    /// surviving siblings keep their relative birth order. The entry is handed
    /// back because the caller usually still needs its handle — to stop the child
    /// it just stopped supervising.
    pub(crate) fn remove(&mut self, id: ActorId) -> Option<Child> {
        let index = self.entries.iter().position(|(key, _)| *key == id)?;
        Some(self.entries.remove(index).1)
    }

    /// Re-keys an entry from `old` to `new` **in place**, reporting whether
    /// `old` was found.
    ///
    /// A rebuilt child is a new actor with a new [`ActorId`], so its key has to
    /// move while its birth position must not — which is exactly what a `remove`
    /// followed by an `insert` would get wrong (the entry would reappear at the
    /// end). A miss is a reported no-op rather than a panic: a rebuild can race
    /// an `unsupervise` that already dropped the entry, and that race must not
    /// resurrect it.
    pub(crate) fn rekey(&mut self, old: ActorId, new: ActorId) -> bool {
        self.entries
            .iter_mut()
            .find(|(key, _)| *key == old)
            .is_some_and(|slot| {
                slot.0 = new;
                true
            })
    }

    /// The current keys, in birth order.
    pub(crate) fn ids(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.entries.iter().map(|(id, _)| *id)
    }

    /// Empties the table, handing back every **live** incarnation's stop edges
    /// paired with its configured [`stop_grace`](RestartConfig::stop_grace) — the
    /// escalation sweep's input: the loop is exiting, so it stops every survivor
    /// crash-only (cancel → grace → abort) before its own death propagates.
    ///
    /// A child in a backoff window (no live handle) has no incarnation to stop and
    /// is dropped from the result; its pending rebuild dies with the retries queue.
    pub(crate) fn drain_live_handles(&mut self) -> SmallVec<[(ChildHandle, Duration); 4]> {
        self.entries
            .drain(..)
            .filter_map(|(_, child)| child.handle.map(|handle| (handle, child.config.stop_grace)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use futures::stream::AbortHandle;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use super::{Child, ChildHandle, Children, Spawned, WatchInstaller, WatchOutcome};
    use crate::{
        mailbox::ActorId,
        restart::{RestartConfig, RestartPolicy, RestartTracker},
    };

    /// A `ChildHandle` around throwaway stop edges — enough for the table tests,
    /// which never actually stop anything.
    fn handle(id: ActorId) -> ChildHandle {
        let (abort, _registration) = AbortHandle::new_pair();
        ChildHandle {
            id,
            cancel: CancellationToken::new(),
            abort,
        }
    }

    /// A no-op watch installer — the table tests never install a watch, so a
    /// closure that claims success without touching a mailbox is enough.
    fn noop_installer() -> WatchInstaller {
        Box::new(|_reg| WatchOutcome::Installed)
    }

    /// A `Child` whose factory rebuilds the same id forever. The table never
    /// calls the factory, so a fixed handle is the whole of what it needs.
    fn child_entry(id: ActorId) -> Child {
        Child {
            factory: Box::new(move || Spawned {
                handle: handle(id),
                install_watch: noop_installer(),
            }),
            handle: Some(handle(id)),
            config: RestartConfig::new(RestartPolicy::Permanent),
            tracker: RestartTracker::new(Instant::now()),
        }
    }

    /// The table is a keyed sequence, not a set: insert/lookup/remove work by id,
    /// and the surviving entries stay in **birth order** across a removal (a later
    /// card's `RestForOne` restarts every child born after the failed one, so the
    /// order is load-bearing, not incidental).
    #[test]
    fn children_insert_lookup_remove_preserve_birth_order() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(1)));
        children.insert(ActorId::new(2), child_entry(ActorId::new(2)));
        children.insert(ActorId::new(3), child_entry(ActorId::new(3)));

        assert!(children.get_mut(ActorId::new(2)).is_some());
        assert!(children.remove(ActorId::new(2)).is_some(), "entry returned");
        assert!(children.get_mut(ActorId::new(2)).is_none());

        let ids: Vec<_> = children.ids().collect();
        assert_eq!(
            ids,
            [ActorId::new(1), ActorId::new(3)],
            "birth order survives removal"
        );
    }

    /// A miss is an `Option`, never a panic: the loop looks children up by the id
    /// on an arriving death notice, and a notice for an id the table never held
    /// (an unsupervised watch edge) is ordinary, not a programmer bug.
    #[test]
    fn lookup_and_remove_of_an_absent_id_are_none() {
        let mut children = Children::new();
        assert!(children.get_mut(ActorId::new(9)).is_none());
        assert!(children.remove(ActorId::new(9)).is_none());
        assert_eq!(children.ids().count(), 0);
    }

    /// `get_mut` hands out the REAL entry, so a mutation through it is visible on
    /// the next lookup — the loop records restart accounting this way.
    #[test]
    fn get_mut_mutations_are_visible_on_the_next_lookup() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(1)));

        let entry = children.get_mut(ActorId::new(1)).expect("just inserted");
        entry.handle = None;
        entry.config.max_restarts = 42;

        let again = children.get_mut(ActorId::new(1)).expect("still there");
        assert!(again.handle.is_none(), "the backoff window is recorded");
        assert_eq!(again.config.max_restarts, 42);
    }

    /// A rebuilt child is a NEW actor with a NEW `ActorId`, so its table key has
    /// to move — but its birth POSITION must not. `rekey` exists precisely
    /// because `remove` + `insert` would append the entry at the end and silently
    /// re-order the sibling list a later `RestForOne` depends on.
    #[test]
    fn rekey_replaces_the_key_in_place() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(1)));
        children.insert(ActorId::new(2), child_entry(ActorId::new(2)));
        children.insert(ActorId::new(3), child_entry(ActorId::new(3)));
        children
            .get_mut(ActorId::new(2))
            .expect("present")
            .config
            .max_restarts = 7;

        assert!(children.rekey(ActorId::new(2), ActorId::new(20)));

        assert!(children.get_mut(ActorId::new(2)).is_none(), "old key gone");
        assert_eq!(
            children
                .get_mut(ActorId::new(20))
                .expect("reachable under the new key")
                .config
                .max_restarts,
            7,
            "the SAME entry moved, not a fresh one",
        );
        assert_eq!(
            children.ids().collect::<Vec<_>>(),
            [ActorId::new(1), ActorId::new(20), ActorId::new(3)],
            "the rebuilt child keeps its birth position",
        );
    }

    /// Re-keying an id the table does not hold changes nothing and says so — the
    /// loop can race a rebuild against an `unsupervise` that already removed the
    /// entry, and that race must not resurrect it.
    #[test]
    fn rekey_of_an_absent_id_is_a_reported_no_op() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(1)));

        assert!(!children.rekey(ActorId::new(9), ActorId::new(90)));

        assert_eq!(children.ids().collect::<Vec<_>>(), [ActorId::new(1)]);
        assert!(children.get_mut(ActorId::new(90)).is_none());
    }

    /// The factory is an erased rebuild edge, so it must actually be callable and
    /// re-callable — `FnMut`, not `FnOnce`. A table whose factory could only run
    /// once would supervise exactly one restart.
    #[test]
    fn the_factory_can_be_invoked_repeatedly() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(5)));
        let entry = children.get_mut(ActorId::new(1)).expect("present");

        assert_eq!((entry.factory)().handle.id(), ActorId::new(5));
        assert_eq!(
            (entry.factory)().handle.id(),
            ActorId::new(5),
            "the rebuild edge survives its first use",
        );
    }

    /// The handle exposes the child's identity — the key the loop re-keys on
    /// after a rebuild — and nothing else; a sender would pin the child's mailbox
    /// open and defeat ref-count-driven stop.
    #[test]
    fn child_handle_reports_its_id() {
        assert_eq!(handle(ActorId::new(3)).id(), ActorId::new(3));
    }

    /// The stop edges are shared with the running child, not private copies: the
    /// supervisor cancels and aborts THROUGH this handle, so a clone of it must
    /// drive the same token and the same abort registration.
    #[tokio::test]
    async fn child_handle_clone_shares_the_stop_edges() {
        let original = handle(ActorId::new(1));
        let cloned = original.clone();

        assert!(!original.cancel.is_cancelled());
        cloned.cancel.cancel();
        assert!(
            original.cancel.is_cancelled(),
            "a cloned handle must cancel the SAME token",
        );

        assert!(!original.abort.is_aborted());
        cloned.abort.abort();
        assert!(
            original.abort.is_aborted(),
            "a cloned handle must abort the SAME task",
        );
    }

    /// The escalation sweep's input: `drain_live_handles` hands back every LIVE
    /// incarnation's stop edges paired with its `stop_grace`, empties the table,
    /// and drops any backoff-window child (no live handle) from the result.
    #[test]
    fn drain_live_handles_returns_live_edges_with_grace_and_empties() {
        let mut children = Children::new();
        let mut alive = child_entry(ActorId::new(1));
        alive.config = alive.config.with_stop_grace(Duration::from_secs(7));
        children.insert(ActorId::new(1), alive);
        let mut backoff = child_entry(ActorId::new(2));
        backoff.handle = None; // in a backoff window: no live incarnation
        children.insert(ActorId::new(2), backoff);

        let drained = children.drain_live_handles();

        assert_eq!(drained.len(), 1, "only the live child yields a handle");
        assert_eq!(drained[0].0.id(), ActorId::new(1), "the live child's id");
        assert_eq!(
            drained[0].1,
            Duration::from_secs(7),
            "the handle carries the child's own grace",
        );
        assert_eq!(children.ids().count(), 0, "the table is emptied");
    }

    /// A `Child` carries its own tuning, so two children of one supervisor can
    /// disagree on policy — the per-child config is not a supervisor-wide one.
    #[test]
    fn children_keep_independent_configs() {
        let mut children = Children::new();
        let mut strict = child_entry(ActorId::new(1));
        strict.config = RestartConfig::new(RestartPolicy::Never).with_stop_grace(Duration::ZERO);
        children.insert(ActorId::new(1), strict);
        children.insert(ActorId::new(2), child_entry(ActorId::new(2)));

        let first = children.get_mut(ActorId::new(1)).expect("present");
        assert_eq!(first.config.policy, RestartPolicy::Never);
        assert_eq!(first.config.stop_grace, Duration::ZERO);
        let second = children.get_mut(ActorId::new(2)).expect("present");
        assert_eq!(second.config.policy, RestartPolicy::Permanent);
    }
}
