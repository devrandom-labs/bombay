//! Bounded deferral buffer: the two-queue `Stash<M>` primitive + its typed
//! overflow handback `StashFull<M>` (card #224, ADR-0022). Design:
//! docs/superpowers/specs/2026-07-30-224-bounded-stash-design.md.
//!
//! This module is now just the **primitive**: the ADR-0022 *surface* is the
//! [`Stashing`](crate::capability::Stashing) capability on the `capability` module
//! (ADR-0026 stage 2, card #279), which wraps this buffer and is serviced
//! in-step by the loop. The old `StashActor`/`Stashed<S>` trait+wrapper pair
//! is removed.
//!
//! Research anchor: conditional synchronization for fixed-interface actors
//! (De Koster et al., AGERE! 2016, §4.2; Briot–Guerraoui–Löhr, ACM Computing Surveys 1998);
//! replay preserves arrival order, overflow refuses loudly (guaranteed
//! delivery — silent drop is the one forbidden outcome).

use std::collections::VecDeque;

use crate::mailbox::Capacity;

/// A bounded, single-producer deferral buffer.
///
/// `stash` defers a message the current state cannot accept; `unstash_all`
/// snapshots everything held for front-of-line replay by the
/// [`Stashing`](crate::capability::Stashing) capability — ahead of the mailbox
/// backlog, in stash-arrival order.
#[derive(Debug)]
pub struct Stash<M> {
    /// `stash()` pushes here (back). Waits for an `unstash_all`.
    held: VecDeque<M>,
    /// `unstash_all()` moves `held` here; replay pops from the front.
    ready: VecDeque<M>,
    /// Bounds `held.len() + ready.len()`.
    cap: Capacity,
}

/// Overflow: the stash is at capacity. Carries the rejected message back in
/// full — never dropped, never panicked (the `TellError` handback precedent).
#[derive(thiserror::Error, Debug)]
#[error("stash full (capacity {})", .cap.get())]
pub struct StashFull<M> {
    msg: M,
    cap: Capacity,
}

impl<M> StashFull<M> {
    /// Recovers the rejected message. Total — overflow never consumes it.
    #[must_use]
    pub fn msg(self) -> M {
        self.msg
    }

    /// The capacity that was hit.
    #[must_use]
    pub const fn capacity(&self) -> Capacity {
        self.cap
    }
}

impl<M> Stash<M> {
    /// Builds an empty stash bounded to `cap` messages. Crate-private: a
    /// stash exists only inside a [`Stashing`](crate::capability::Stashing)
    /// capability (forget-proof by construction).
    pub(crate) const fn bounded(cap: Capacity) -> Self {
        Self {
            held: VecDeque::new(),
            ready: VecDeque::new(),
            cap,
        }
    }

    /// Messages currently deferred (held + awaiting replay).
    #[must_use]
    #[expect(
        clippy::manual_saturating_arithmetic,
        reason = "the house arithmetic rule bans saturating_* in capacity paths \
                  (restart.rs documents the ceiling policy); checked_add + explicit \
                  MAX is the canonical shape — an unreachable overflow reads as full"
    )]
    pub fn len(&self) -> usize {
        // Both queues are bounded by `cap`, but per the arithmetic-safety
        // rule the sum is still checked: an (unreachable) overflow reads as
        // "at capacity", never as a small number.
        self.held
            .len()
            .checked_add(self.ready.len())
            .unwrap_or(usize::MAX)
    }

    /// `true` when nothing is deferred.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.held.is_empty() && self.ready.is_empty()
    }

    /// Defers `msg`.
    ///
    /// # Errors
    ///
    /// [`StashFull`] (carrying `msg` back) when `len() == capacity`.
    pub fn stash(&mut self, msg: M) -> Result<(), StashFull<M>> {
        if self.len() >= self.cap.get() {
            return Err(StashFull { msg, cap: self.cap });
        }
        self.held.push_back(msg);
        Ok(())
    }

    /// Queues every currently-held message for replay, in stash order,
    /// ahead of the mailbox backlog. **Snapshot semantics:** messages stashed
    /// *during* the replay wait for the next call — a replayed message cannot
    /// re-enter its own batch. A handler that re-stashes a message and calls
    /// `unstash_all` on every replay of it livelocks itself (user bug; same
    /// class as an actor `tell`-ing itself forever).
    pub fn unstash_all(&mut self) {
        self.ready.append(&mut self.held);
    }

    /// Pops the next message due for replay. Crate-private: only the
    /// [`Stashing`](crate::capability::Stashing) capability drives replay.
    pub(crate) fn pop_ready(&mut self) -> Option<M> {
        self.ready.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroUsize;

    use super::*;

    fn cap(n: usize) -> Capacity {
        Capacity::new(NonZeroUsize::new(n).expect("test capacity nonzero"))
            .expect("test capacity valid")
    }

    /// Invariant 1: the bound covers held + ready together, from the
    /// constructor parameter — not a global.
    #[test]
    fn capacity_bounds_held_plus_ready() {
        let mut stash = Stash::bounded(cap(2));
        stash.stash(1u32).expect("slot 1");
        assert!(!stash.is_empty(), "one held message means non-empty");
        stash.unstash_all(); // 1 moves to ready — still counts
        stash.stash(2u32).expect("slot 2");
        let err = stash.stash(3u32).expect_err("cap 2 must refuse the 3rd");
        assert_eq!(err.msg(), 3, "the rejected message comes back intact");
        assert_eq!(stash.len(), 2);
    }

    /// Invariant 2: overflow is a typed handback — exact message recovered,
    /// nothing dropped, nothing panicked.
    #[test]
    fn overflow_hands_the_exact_message_back() {
        let mut stash = Stash::bounded(cap(1));
        stash.stash(10u32).expect("fits");
        let err = stash.stash(20u32).expect_err("full");
        assert_eq!(err.capacity().get(), 1);
        assert_eq!(err.msg(), 20);
        // The buffer itself is untouched by the refusal.
        stash.unstash_all();
        assert_eq!(stash.pop_ready(), Some(10));
        assert_eq!(stash.pop_ready(), None);
    }

    /// D2 snapshot semantics: a message stashed after `unstash_all` waits in
    /// `held`; it is NOT part of the draining batch.
    #[test]
    fn unstash_is_a_snapshot_not_a_live_view() {
        let mut stash = Stash::bounded(cap(4));
        stash.stash(1u32).expect("held 1");
        stash.stash(2u32).expect("held 2");
        stash.unstash_all();
        stash.stash(3u32).expect("held 3 — mid-replay stash");
        assert_eq!(stash.pop_ready(), Some(1));
        assert_eq!(stash.pop_ready(), Some(2));
        assert_eq!(stash.pop_ready(), None, "3 must wait for the next unstash");
        stash.unstash_all();
        assert_eq!(stash.pop_ready(), Some(3));
    }

    /// Replay order is stash-arrival order (FIFO), across multiple
    /// stash/unstash rounds.
    #[test]
    fn replay_order_is_arrival_order() {
        let mut stash = Stash::bounded(cap(4));
        stash.stash(1u32).expect("1");
        stash.stash(2u32).expect("2");
        stash.unstash_all();
        stash.stash(3u32).expect("3");
        stash.unstash_all(); // 3 joins BEHIND the already-ready 1, 2
        let drained: Vec<u32> = std::iter::from_fn(|| stash.pop_ready()).take(4).collect();
        assert_eq!(drained, vec![1, 2, 3]);
        assert!(stash.is_empty());
    }
}
