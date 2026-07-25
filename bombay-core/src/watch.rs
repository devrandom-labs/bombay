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
    /// `true` iff the dying actor's `on_stop` failed — returned `Err`, panicked,
    /// or exceeded the notice grace (#196). `false` whenever `on_stop` never ran
    /// at all (kill path, startup failure): nothing was cleaned up, so nothing
    /// failed to clean up.
    ///
    /// A flag rather than a distinct [`reason`](Self::reason): "it died AND left
    /// a lock or file handle stranded" is extra information about the same death,
    /// and a supervisor may escalate on it instead of restarting — folding it
    /// into the reason would erase why the actor actually stopped.
    pub cleanup_failed: bool,
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

/// One installed edge in a watcher set: who to notify, over which link channel,
/// and whether the edge propagates (`link`) or merely notifies (`watch`).
struct Edge {
    watcher: ActorId,
    tx: LinkSender,
    linked: bool,
}

/// What the dying actor's `on_stop` did, as the watcher set knows it (#196).
///
/// **This is a naming device, not a behavioural distinction.** Only
/// [`Failed`](Self::Failed) reaches the wire (as
/// [`LinkDied::cleanup_failed`](LinkDied::cleanup_failed)); `NotAttempted` and
/// `Succeeded` collapse to the same `false`, and a bool defaulting to `false`
/// would behave identically on every path today — including the kill path,
/// which holds because nothing armed the flag, not because two states are told
/// apart. What the enum buys is that the teardown's arm-then-disarm protocol
/// (`assume_cleanup_failed` before the hook,
/// [`record_cleanup_succeeded`](Watchers::record_cleanup_succeeded) only on an
/// observed `Ok`) reads as three named states rather than two writes to a bool
/// whose initial `false` means something different from its later `false`. If a
/// consumer ever needs the distinction — or needs to say *how* cleanup failed —
/// the shape is already here; nothing needs it now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cleanup {
    /// `on_stop` never started: a hard kill before teardown, or a startup
    /// failure. Nothing was cleaned up, so nothing failed to clean up.
    NotAttempted,
    /// `on_stop` started and was never observed to succeed — it returned `Err`,
    /// panicked, blew the teardown's notice grace, or was interrupted mid-flight
    /// by a kill. Pessimistic on purpose: several of those paths never
    /// run another line of teardown, so the flag has to be armed BEFORE the hook
    /// and cleared only by an observed success.
    Failed,
    /// `on_stop` returned `Ok(())`.
    Succeeded,
}

/// The set of watchers a (watched) actor must notify when it stops, owned by the
/// actor's task. Its `Drop` fires the notifications — so death is delivered on
/// EVERY exit path (normal return, a caught handler panic delivered as
/// `Panicked`, `Abortable` kill), since `Drop` runs on all of them.
pub struct Watchers {
    me: ActorId,
    list: SmallVec<[Edge; 1]>,
    reason: Option<ActorStopReason>,
    cleanup: Cleanup,
}

impl Watchers {
    pub(crate) fn new(me: ActorId) -> Self {
        Self {
            me,
            list: SmallVec::new(),
            reason: None,
            cleanup: Cleanup::NotAttempted,
        }
    }

    /// Registers a watcher from a [`WatchReg`]. Duplicates are intentionally
    /// kept: repeated `watch` edges match Erlang (repeated `monitor/2` calls are
    /// independent monitors), and a duplicate `link` edge — where Erlang would
    /// keep a single link — just delivers a duplicate notice whose first `Break`
    /// wins (recorded OTP divergence; dedup would cost a scan on every apply).
    /// Single apply path shared by the run-loop and the teardown drain, so both
    /// stay FIFO-consistent.
    pub(crate) fn apply(&mut self, reg: WatchReg) {
        self.list.push(Edge {
            watcher: reg.watcher,
            tx: reg.link_tx,
            linked: reg.linked,
        });
    }

    /// Removes **every** edge for `watcher` (the `unwatch` path) — watch and
    /// link edges alike, coarser than Erlang's per-monitor `demonitor`. Linear
    /// scan — a watcher list is small; a map would buy nothing here.
    pub(crate) fn remove(&mut self, watcher: ActorId) {
        self.list.retain(|edge| edge.watcher != watcher);
    }

    /// Records the graceful stop reason. If never called (hard kill), `Drop`
    /// defaults to `Killed`.
    pub(crate) fn set_reason(&mut self, reason: ActorStopReason) {
        self.reason = Some(reason);
    }

    /// Arms the cleanup-failure flag BEFORE `on_stop` is awaited; only
    /// [`record_cleanup_succeeded`](Self::record_cleanup_succeeded) disarms it.
    ///
    /// Pessimistic by design: a hook can leave without ever returning to the
    /// teardown code — a kill drops the whole lifecycle future mid-hook, and the
    /// `timeout` call itself panics on a runtime with no timer. Both of those
    /// leave resources unreleased, and neither gets a chance to set a flag
    /// afterwards, so the flag is set in advance and cleared on success.
    // `const` here but not on `set_reason` next door: overwriting an
    // `Option<ActorStopReason>` drops the old value, and that destructor is not
    // const-evaluable (E0493). A fieldless enum has no destructor to run.
    pub(crate) const fn assume_cleanup_failed(&mut self) {
        self.cleanup = Cleanup::Failed;
    }

    /// Records the observed `Ok(())` from `on_stop`, disarming
    /// [`assume_cleanup_failed`](Self::assume_cleanup_failed).
    pub(crate) const fn record_cleanup_succeeded(&mut self) {
        self.cleanup = Cleanup::Succeeded;
    }

    /// The bit the notices will carry — read by the teardown so the backlog it
    /// answers *after* dropping this guard reports the same outcome the guard's
    /// own notices did, rather than a second, drifting copy of it.
    pub(crate) const fn cleanup_failed(&self) -> bool {
        matches!(self.cleanup, Cleanup::Failed)
    }
}

impl Drop for Watchers {
    fn drop(&mut self) {
        let reason = self.reason.take().unwrap_or(ActorStopReason::Killed);
        // Read before the drain: `list.drain` borrows `self` mutably, and every
        // notice carries the same collapsed outcome anyway.
        let cleanup_failed = self.cleanup_failed();
        let me = self.me;
        for edge in self.list.drain(..) {
            // Unbounded channel: send only fails if the watcher itself is gone,
            // in which case the edge is stale and correctly dropped.
            let _ = edge.tx.try_send(LinkDied {
                id: me,
                reason: reason.clone(),
                linked: edge.linked,
                cleanup_failed,
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
        guard.apply(WatchReg {
            watcher: ActorId::new(1),
            link_tx: tx_a,
            linked: false,
        }); // a watched
        guard.apply(WatchReg {
            watcher: ActorId::new(2),
            link_tx: tx_b,
            linked: true,
        }); // b linked
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
        // Abortable drops the guard without a graceful reason => Killed. This is
        // also the canonical pin for the kill path's `cleanup_failed` bit: one
        // drop event carries both invariants, so they are asserted together.
        let (tx, rx) = flume::unbounded();
        let mut guard = Watchers::new(ActorId::new(7));
        guard.apply(WatchReg {
            watcher: ActorId::new(1),
            link_tx: tx,
            linked: false,
        });
        drop(guard); // no set_reason

        let n = rx.try_recv().expect("notified on kill path");
        assert!(matches!(n.reason, ActorStopReason::Killed));
        assert!(
            !n.cleanup_failed,
            "the kill path never runs on_stop, so no cleanup can have failed"
        );
    }

    #[test]
    fn record_cleanup_succeeded_disarms_the_assumption() {
        // The other half of the pessimistic protocol (#196): the flag is armed
        // before `on_stop` is awaited, so a successful hook MUST clear it or every
        // clean shutdown would libel itself. `NotAttempted` and `Succeeded` both
        // collapse to `false` on the wire, but only this transition proves the
        // clear path exists.
        let (tx, rx) = flume::unbounded();
        let mut guard = Watchers::new(ActorId::new(11));
        guard.apply(WatchReg {
            watcher: ActorId::new(1),
            link_tx: tx,
            linked: false,
        });
        guard.set_reason(ActorStopReason::Normal);
        guard.assume_cleanup_failed();
        assert!(guard.cleanup_failed(), "armed before the hook");
        guard.record_cleanup_succeeded();
        assert!(!guard.cleanup_failed(), "an observed Ok disarms it");
        drop(guard);

        let n = rx.try_recv().expect("notified");
        assert!(
            !n.cleanup_failed,
            "a hook that returned Ok must not be reported as a failed cleanup"
        );
    }

    #[test]
    fn assumed_cleanup_failure_rides_every_notice() {
        // Two edges, not one: a `Drop` that stamped only the first notice would
        // pass a single-edge test while leaving later watchers misinformed.
        let (tx_a, rx_a) = flume::unbounded();
        let (tx_b, rx_b) = flume::unbounded();
        let mut guard = Watchers::new(ActorId::new(9));
        guard.apply(WatchReg {
            watcher: ActorId::new(1),
            link_tx: tx_a,
            linked: true,
        });
        guard.apply(WatchReg {
            watcher: ActorId::new(2),
            link_tx: tx_b,
            linked: false,
        });
        guard.set_reason(ActorStopReason::Normal);
        guard.assume_cleanup_failed();
        drop(guard);

        let a = rx_a.try_recv().expect("a notified");
        assert!(
            a.cleanup_failed,
            "cleanup_failed must ride the first notice"
        );
        assert!(a.reason.is_normal());
        let b = rx_b.try_recv().expect("b notified");
        assert!(b.cleanup_failed, "cleanup_failed must ride EVERY notice");
        assert!(
            b.reason.is_normal(),
            "original reason preserved — a failed cleanup is not a different death"
        );
    }
}
