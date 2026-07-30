//! Bounded deferral: `Stashed<S>` composition over a two-queue `Stash<M>`
//! (card #224). Design: docs/superpowers/specs/2026-07-30-224-bounded-stash-design.md,
//! ADR-0022.
//!
//! Research anchor: conditional synchronization for fixed-interface actors
//! (De Koster et al., AGERE! 2016, §4.2; Briot–Guerraoui–Löhr, ACM Computing Surveys 1998);
//! replay preserves arrival order, overflow refuses loudly (guaranteed
//! delivery — silent drop is the one forbidden outcome).

use core::{any::type_name, future::Future};
use std::collections::VecDeque;

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::{Capacity, Mailboxed},
    message::Msg,
};

/// A bounded, single-producer deferral buffer.
///
/// `stash` defers a message the current state cannot accept; `unstash_all`
/// snapshots everything held for front-of-line replay by [`Stashed`]'s handle
/// wrapper — ahead of the mailbox backlog, in stash-arrival order.
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
    /// stash exists only inside a [`Stashed`] (forget-proof by construction).
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
    /// [`Stashed`] handle wrapper drives replay.
    pub(crate) fn pop_ready(&mut self) -> Option<M> {
        self.ready.pop_front()
    }
}

/// Opt-in actor shape with deferral: [`Actor`]'s hooks, plus the stash as a
/// `handle` parameter.
///
/// Implement this instead of `Actor`, then spawn `Stashed::<Self>` — the
/// wrapper owns the buffer and drives replay; there is no wiring to forget.
pub trait StashActor: Mailboxed<Msg: Msg> + Sized + Send + 'static {
    /// The argument passed to [`on_start`](StashActor::on_start).
    type Args: Send;
    /// The actor's own domain error, kept typed end to end.
    type Error: ReplyError;

    /// Stash capacity, from the actor's own constructor input. Required and
    /// explicit — bounded is the point; there is no global default. Ignore
    /// `args` for a type-fixed bound, or thread it through for a
    /// spawn-tunable one. Never a `SpawnConfig` field (spec D8).
    fn stash_capacity(args: &Self::Args) -> Capacity;

    /// Builds the actor state. See [`Actor::on_start`].
    fn on_start(
        args: Self::Args,
        actor_ref: ActorRef<Stashed<Self>>,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    /// Handles one message; `stash` defers what the current state cannot
    /// accept ([`Stash::stash`]) and releases it ([`Stash::unstash_all`]).
    /// See [`Actor::handle`] for `stop` and error semantics.
    fn handle(
        &mut self,
        msg: Self::Msg,
        actor_ref: ActorRef<Stashed<Self>>,
        stash: &mut Stash<Self::Msg>,
        stop: &mut bool,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// See [`Actor::on_panic`]. No stash access: the state is poisoned and
    /// the stash dies with the incarnation (spec D6).
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Stashed<Self>>,
        err: PanicError,
    ) -> impl Future<Output = ActorStopReason> + Send {
        let _ = actor_ref;
        async move { ActorStopReason::Panicked(err) }
    }

    /// See [`Actor::on_stop`]. No stash access: whatever is still deferred
    /// at stop is dropped (spec D6) — a stashed ask's reply port drops with
    /// it and the asker sees the usual typed ask-side error.
    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Stashed<Self>>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let _ = (actor_ref, reason);
        async { Ok(()) }
    }
}

/// The only way to have a stash: framework-owned composition over user state.
///
/// A `Stashed<S>` is a plain [`Actor`] — every existing verb (spawn, tell,
/// ask, watch, supervise-as-child, timers, `Recipient`) works unchanged.
#[derive(Debug)]
pub struct Stashed<S: StashActor> {
    state: S,
    stash: Stash<S::Msg>,
}

impl<S: StashActor> Mailboxed for Stashed<S> {
    type Msg = S::Msg;
}

impl<S: StashActor> Actor for Stashed<S> {
    type Args = S::Args;
    type Error = S::Error;

    fn name() -> &'static str {
        // The user's type is the interesting name in logs, not the wrapper's.
        type_name::<S>()
    }

    async fn on_start(args: S::Args, actor_ref: ActorRef<Self>) -> Result<Self, S::Error> {
        let cap = S::stash_capacity(&args);
        let state = S::on_start(args, actor_ref).await?;
        Ok(Self {
            state,
            stash: Stash::bounded(cap),
        })
    }

    /// The whole replay mechanism (spec D3): run the user handler, then drain
    /// `ready` — still inside the current `handle_message` step, so replayed
    /// messages run ahead of the entire mailbox backlog, in stash-arrival
    /// order, under the step's own strong `actor_ref` (no upgrade, no
    /// drain-window hazard). A replayed handler's `Err`/panic/`stop` routes
    /// exactly as a delivered message's would.
    async fn handle(
        &mut self,
        msg: S::Msg,
        actor_ref: ActorRef<Self>,
        stop: &mut bool,
    ) -> Result<(), S::Error> {
        S::handle(
            &mut self.state,
            msg,
            actor_ref.clone(),
            &mut self.stash,
            stop,
        )
        .await?;
        while !*stop {
            let Some(m) = self.stash.pop_ready() else {
                break;
            };
            S::handle(&mut self.state, m, actor_ref.clone(), &mut self.stash, stop).await?;
        }
        Ok(())
    }

    async fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> ActorStopReason {
        S::on_panic(&mut self.state, actor_ref, err).await
    }

    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), S::Error> {
        S::on_stop(&mut self.state, actor_ref, reason).await
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
        let drained: Vec<u32> = std::iter::from_fn(|| stash.pop_ready()).collect();
        assert_eq!(drained, vec![1, 2, 3]);
        assert!(stash.is_empty());
    }
}
