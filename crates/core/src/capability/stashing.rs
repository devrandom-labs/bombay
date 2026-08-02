//! Bounded deferral: the [`Stashing`] capability, its required
//! [`StashPolicy`] capacity seam, and the [`Replay`] loop hook the
//! [`Shell`](super::Shell) drains in-step (ADR-0022).

use crate::{
    mailbox::Capacity,
    stash::{Stash, StashFull},
};

use super::Actor;

/// The loop hook a capability set exposes for **in-step replay**.
///
/// After each user [`handle`](Actor::handle), the [`Shell`](super::Shell) drains this until
/// it yields `None` — that is how a [`Stashing`] capability replays deferred
/// messages ahead of the mailbox backlog (ADR-0022). It is the "participation"
/// half of a capability (the loop servicing it each step), distinct from the
/// "access" half ([`Ctx::cap`](super::Ctx::cap)).
///
/// The design point of stage 2 (ADR-0026): `Shell<A>` holds `A::Caps` as an
/// opaque set, so it cannot *discover* a stash generically — a blanket impl is
/// coherence-infeasible (E0119) and specialization is unstable. So the one
/// `derive(Provide)` that already reads the cap-set
/// fields ALSO emits this impl: forget-proof, single `spawn`, single `Shell`.
/// `()` and every non-stashing set yield `None`; users never hand-write it.
pub trait Replay<M> {
    /// The next message due for in-step replay, or `None` when drained.
    fn next_replay(&mut self) -> Option<M>;
}

impl<M> Replay<M> for () {
    fn next_replay(&mut self) -> Option<M> {
        None
    }
}

/// Bounded deferral capability (ADR-0022 semantics) — the `capability`-surface
/// successor to the removed `StashActor`/`Stashed` pair.
///
/// A field of a cap-set struct: `cx.cap::<Stashing<Msg>>()` inside a handler
/// defers a message the current state cannot accept ([`stash`](Stashing::stash))
/// and releases the batch ([`unstash_all`](Stashing::unstash_all)); the
/// [`Shell`](super::Shell) replays the released messages in-step, ahead of the backlog, in
/// arrival order. Capacity comes from a required [`StashPolicy`] (Args-sourced),
/// wired in the cap set's hand-written [`CapSet::build`](super::CapSet::build). The buffer holds bare
/// messages, so a non-empty stash never pins the actor alive (`Collected`
/// reachable — ADR-0020).
pub struct Stashing<M> {
    stash: Stash<M>,
}

impl<M> Stashing<M> {
    /// Builds an empty stash bounded to `cap` (held + ready together).
    #[must_use]
    pub const fn bounded(cap: Capacity) -> Self {
        Self {
            stash: Stash::bounded(cap),
        }
    }

    /// Defers `msg`.
    ///
    /// # Errors
    ///
    /// [`StashFull`] (carrying `msg` back intact) when the stash is at capacity.
    pub fn stash(&mut self, msg: M) -> Result<(), StashFull<M>> {
        self.stash.stash(msg)
    }

    /// Queues every currently-held message for in-step replay, in arrival
    /// order, ahead of the mailbox backlog. Snapshot semantics: a message
    /// stashed *during* the replay wait for the next call (ADR-0022 D2).
    pub fn unstash_all(&mut self) {
        self.stash.unstash_all();
    }

    /// Messages currently deferred (held + awaiting replay).
    #[must_use]
    pub fn len(&self) -> usize {
        self.stash.len()
    }

    /// `true` when nothing is deferred.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stash.is_empty()
    }

    /// Drives one replay step. Crate-private: only the framework drains, via
    /// the [`Replay`] impl below (the derived hook forwards here) — never the
    /// user (ADR-0022 forget-proofness).
    pub(crate) fn pop_ready(&mut self) -> Option<M> {
        self.stash.pop_ready()
    }
}

impl<M> Replay<M> for Stashing<M> {
    fn next_replay(&mut self) -> Option<M> {
        self.pop_ready()
    }
}

/// The required, testable capacity seam for a [`Stashing`] capability.
///
/// One item — the bound, sourced from the actor's own spawn `Args` (never a
/// `SpawnConfig` field, ADR-0022 D8). Plugged as a type on the cap set's
/// [`CapSet::build`](super::CapSet::build); bounded deferral is the point, so there is no default.
pub trait StashPolicy<A: Actor> {
    /// The stash capacity for this actor, derived from its spawn args.
    fn capacity(args: &A::Args) -> Capacity;
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroUsize;

    use super::{Replay, Stashing};
    use crate::mailbox::Capacity;

    fn cap(n: usize) -> Capacity {
        Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("valid")
    }

    /// The plain-actor floor: `()` never replays anything.
    #[test]
    fn unit_caps_replay_is_always_none() {
        let mut unit = ();
        assert_eq!(<() as Replay<u32>>::next_replay(&mut unit), None);
    }

    /// The cap's `Replay` impl drains `ready` in arrival order after an
    /// `unstash_all`, then reports empty — the loop hook the `Shell` calls.
    #[test]
    fn stashing_replay_drains_ready_in_arrival_order() {
        let mut s = Stashing::<u32>::bounded(cap(4));
        assert!(s.is_empty());
        s.stash(1).expect("slot 1");
        s.stash(2).expect("slot 2");
        assert!(!s.is_empty(), "two messages are deferred");
        assert_eq!(s.len(), 2);
        // Nothing is due for replay until unstash_all snapshots the batch.
        assert_eq!(Replay::next_replay(&mut s), None, "held is not yet ready");
        s.unstash_all();
        assert_eq!(Replay::next_replay(&mut s), Some(1));
        assert_eq!(Replay::next_replay(&mut s), Some(2));
        assert_eq!(Replay::next_replay(&mut s), None);
        assert!(s.is_empty());
    }

    /// Overflow refuses loudly with the exact message back — never drops,
    /// never panics (ADR-0022 bounded-with-handback).
    #[test]
    fn stashing_overflow_hands_the_exact_message_back() {
        let mut s = Stashing::<u32>::bounded(cap(1));
        s.stash(10).expect("fits");
        let full = s.stash(20).expect_err("at capacity");
        assert_eq!(full.capacity().get(), 1);
        assert_eq!(full.msg(), 20, "the rejected message comes back intact");
    }
}
