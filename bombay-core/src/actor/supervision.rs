//! Supervision runtime types (card #196): the loop-owned child table and the
//! erased rebuild edge.
//!
//! The factory closure is the feature's ONLY `dyn`. It is the one place the
//! child's concrete type is in scope — spawn, watch-install, and any registry
//! rebinding all live inside it — so erasing there is what lets a supervisor
//! hold children of several types in one homogeneous table without the
//! supervisor's own type growing a parameter per child.
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

use futures::{future::BoxFuture, stream::AbortHandle};
use smallvec::SmallVec;
use tokio_util::sync::CancellationToken;

use crate::{
    mailbox::ActorId,
    restart::{RestartConfig, RestartTracker},
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

/// The erased rebuild edge: spawn a fresh incarnation, install the supervisor's
/// watch edge on it, and hand back the new [`ChildHandle`].
///
/// `FnMut`, not `FnOnce` — a child is rebuilt as many times as its budget
/// allows. The future is boxed because edge installation awaits (the
/// registration rides the child's bounded mailbox); that is **one box per
/// rebuild**, never per message.
///
/// Infallible on purpose: the only failure the install can report is
/// [`ActorNotLinked`](crate::error::ActorNotLinked), which depends on how the
/// *supervisor* was spawned and so cannot vary between rebuilds — it is checked
/// once, where the factory is built. A child that spawns but then fails
/// `on_start` is not a factory failure: it is a live actor that dies, and it
/// reports itself through the ordinary death notice.
pub type RebuildFactory = Box<dyn FnMut() -> BoxFuture<'static, ChildHandle> + Send>;

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
}

/// A child-table operation shipped over the supervisor's own mailbox.
///
/// The table is task-owned, so *all* mutation goes through the loop and arrives
/// in mailbox FIFO order — there is no lock to take and no ordering rule for a
/// caller to get wrong.
#[expect(
    clippy::exhaustive_enums,
    reason = "the supervision verb set is deliberately closed so the supervised \
              run-loop is a total match; new arms are added under their driving cards"
)]
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
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use futures::{FutureExt, stream::AbortHandle};
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use super::{Child, ChildHandle, Children};
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

    /// A `Child` whose factory rebuilds the same id forever. The table never
    /// calls the factory, so a fixed handle is the whole of what it needs.
    fn child_entry(id: ActorId) -> Child {
        Child {
            factory: Box::new(move || async move { handle(id) }.boxed()),
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
    #[tokio::test]
    async fn the_factory_can_be_invoked_repeatedly() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(5)));
        let entry = children.get_mut(ActorId::new(1)).expect("present");

        assert_eq!((entry.factory)().await.id(), ActorId::new(5));
        assert_eq!(
            (entry.factory)().await.id(),
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
