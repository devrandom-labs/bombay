//! Death-watch primitives (card #195): the `LinkDied` notice, the watch
//! registration `Signal` carries, and the `Watchers` guard whose `Drop` is the
//! death event. Death travels on a dedicated UNBOUNDED channel so the guard's
//! non-blocking notify (from `Drop`, which cannot await) can never lose a
//! notice — see the design doc for the Erlang/Akka grounding.

use crate::{error::ActorStopReason, mailbox::ActorId};
use smallvec::SmallVec;

/// A death notice: the actor `id` stopped for `reason`; `linked` is `true` iff
/// the edge was installed by `link` (propagating) rather than `watch` (notify).
///
/// Monomorphic on purpose — this is why watcher lists need no `dyn`.
#[derive(Clone, Debug)]
pub struct LinkDied {
    /// The actor that died.
    pub id: ActorId,
    /// Why it stopped.
    pub reason: ActorStopReason,
    /// Whether the edge was a `link` (propagate) vs a `watch` (notify only).
    pub linked: bool,
}

/// The sender half of a watcher's UNBOUNDED link channel. One concrete type for
/// all watchers (the payload is monomorphic), so a watched actor stores a
/// homogeneous list of these.
pub type LinkSender = flume::Sender<LinkDied>;
/// The receiver half, drained by a `Watch` actor's run-loop.
pub type LinkReceiver = flume::Receiver<LinkDied>;

/// A watch registration in transit on the message mailbox: "notify `watcher` on
/// my death, over `link_tx`; `linked` decides propagate-vs-notify."
#[derive(Clone, Debug)]
pub struct WatchReg {
    /// The watcher's identity (also the unwatch key).
    pub watcher: ActorId,
    /// The watcher's link channel to deliver `LinkDied` on.
    pub link_tx: LinkSender,
    /// `true` for a `link` edge (propagating), `false` for `watch`.
    pub linked: bool,
}

/// The set of watchers a (watched) actor must notify when it stops, owned by the
/// actor's task. Its `Drop` fires the notifications — so death is delivered on
/// EVERY exit path (normal return, panic unwind, `Abortable` cancellation),
/// since `Drop` runs on all of them.
pub struct Watchers {
    me: ActorId,
    list: SmallVec<[(ActorId, LinkSender, bool); 1]>,
    reason: Option<ActorStopReason>,
}

impl Watchers {
    pub(crate) fn new(me: ActorId) -> Self {
        Self {
            me,
            list: SmallVec::new(),
            reason: None,
        }
    }

    /// Registers a watcher. `link` twice for the same watcher installs two edges;
    /// dedup is intentionally not done (idempotency is a caller concern, and a
    /// duplicate simply delivers twice — bounded by watch-count, never a leak).
    pub(crate) fn push(&mut self, watcher: ActorId, link_tx: LinkSender, linked: bool) {
        self.list.push((watcher, link_tx, linked));
    }

    /// Removes every edge for `watcher` (the `unwatch` path). Linear scan — a
    /// watcher list is small; a map would buy nothing here.
    pub(crate) fn remove(&mut self, watcher: ActorId) {
        self.list.retain(|(id, _, _)| *id != watcher);
    }

    /// Records the graceful stop reason. If never called (hard kill), `Drop`
    /// defaults to `Killed`.
    pub(crate) fn set_reason(&mut self, reason: ActorStopReason) {
        self.reason = Some(reason);
    }
}

impl Drop for Watchers {
    fn drop(&mut self) {
        let reason = self.reason.take().unwrap_or(ActorStopReason::Killed);
        for (_, tx, linked) in self.list.drain(..) {
            // Unbounded channel: send only fails if the watcher itself is gone,
            // in which case the edge is stale and correctly dropped.
            let _ = tx.try_send(LinkDied {
                id: self.me,
                reason: reason.clone(),
                linked,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_of_watchers_notifies_every_edge_with_its_linked_flag() {
        let (tx_a, rx_a) = flume::unbounded();
        let (tx_b, rx_b) = flume::unbounded();
        let mut guard = Watchers::new(ActorId::new(42));
        guard.push(ActorId::new(1), tx_a, false); // a watched
        guard.push(ActorId::new(2), tx_b, true); // b linked
        guard.set_reason(ActorStopReason::Normal);
        drop(guard);

        let a = rx_a.try_recv().expect("a notified");
        assert_eq!(a.id, ActorId::new(42));
        assert!(!a.linked, "watch edge carries linked=false");
        assert!(a.reason.is_normal());

        let b = rx_b.try_recv().expect("b notified");
        assert!(b.linked, "link edge carries linked=true");
    }

    #[test]
    fn drop_without_set_reason_reports_killed() {
        // Abortable drops the guard without a graceful reason => Killed.
        let (tx, rx) = flume::unbounded();
        let mut guard = Watchers::new(ActorId::new(7));
        guard.push(ActorId::new(1), tx, false);
        drop(guard); // no set_reason

        let n = rx.try_recv().expect("notified on kill path");
        assert!(matches!(n.reason, ActorStopReason::Killed));
    }
}
