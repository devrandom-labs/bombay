//! The ADR-0025 deadline plane's capability seats: the [`DeadlineHook`]
//! loop-participation trait, the sealed [`DeadlineCx`] contexts
//! ([`ByState`]/[`ByPhase`]), THE one [`DeadlinePolicy`] seat (ADR-0028),
//! and the [`Deadlined`] capability.

use core::{future::Future, marker::PhantomData};

use tokio::time::Instant;

use crate::actor::{Flow, WeakActorRef, sealed};

use super::{Actor, Never, PhasePolicy, Shell, Step};

/// The loop hook a capability set exposes for the **ADR-0025 deadline
/// plane** (ADR-0026 stage 4, card #281).
///
/// The participation half of [`Deadlined`] (and of `Phased`), as
/// [`Replay`](super::Replay) is of [`Stashing`](super::Stashing).
///
/// Every iteration, each of the three loop shapes re-reads
/// [`next_deadline`](DeadlineHook::next_deadline) through the [`Shell`]'s
/// runtime bridge and arms one guarded `sleep_until` arm — above the
/// mailbox arm, below every housekeeping arm; fires once per value. Expiry
/// is delivered to [`on_deadline`](DeadlineHook::on_deadline) at a turn
/// boundary under the same `catch_unwind`/poisoning treatment as `handle`,
/// crash domain `PanicReason::OnDeadline` (handler-like, restart-eligible).
///
/// Derive-emitted (never hand-written by users): a [`Deadlined`] field
/// forwards to its [`DeadlinePolicy`]; a deadline-less set stays disabled.
/// `()` — the plain-actor floor — declares no deadline.
pub trait DeadlineHook<A: Actor> {
    /// The next instant this actor needs waking; `None` = arm disabled.
    fn next_deadline(&self, actor: &A) -> Option<Instant>;

    /// Expiry delivery at a turn boundary.
    ///
    /// # Errors
    ///
    /// Returns [`A::Error`](Actor::Error) if the plugged policy fails — a
    /// controlled crash, exactly as a handler `Err`.
    fn on_deadline(
        &mut self,
        actor: &mut A,
        actor_ref: WeakActorRef<Shell<A>>,
    ) -> impl Future<Output = Result<Flow, A::Error>> + Send;
}

impl<A: Actor> DeadlineHook<A> for () {
    fn next_deadline(&self, _: &A) -> Option<Instant> {
        None
    }

    async fn on_deadline(
        &mut self,
        _: &mut A,
        _: WeakActorRef<Shell<A>>,
    ) -> Result<Flow, A::Error> {
        Ok(Flow::Continue)
    }
}

/// What a deadline is computed FROM — the sealed context a capability
/// curates for its [`DeadlinePolicy`] seat (ADR-0028; sealed until the
/// machine-algebra card opens capability authorship).
///
/// The anti-god-object law rides here: a seat sees exactly what its
/// context declares — [`ByState`] exposes actor state, [`ByPhase`] the
/// phase clock — never a grab-bag.
pub trait DeadlineCx: sealed::Sealed {
    /// The served actor.
    type Actor: Actor;
    /// The transition vocabulary of [`on_deadline`](DeadlinePolicy::on_deadline)'s
    /// [`Step`] verdict: [`Never`] for phase-less contexts (`Step<Never>`
    /// IS `Flow` — `Goto` is unconstructible, ADR-0029), the machine's
    /// phase for [`ByPhase`].
    type Phase: Copy + PartialEq + Send + 'static;
    /// The capability-curated window passed (by value — `Copy`) beside
    /// the actor: `()` for [`ByState`] (the actor IS the view),
    /// [`PhaseView`] for [`ByPhase`] (the phase clock).
    type View: Copy + Send + 'static;
}

/// [`Deadlined`]'s context: the slot reads **actor state**.
///
/// The quinn `poll_timeout` shape — no set/cancel verbs, nothing to
/// forget, nothing to race; magnitudes live in the actor's own state,
/// sourced from its spawn `Args`. This is #241's non-phased consumer
/// path (`last_activity + T` over the same slot).
pub struct ByState<A> {
    marker: PhantomData<A>,
}

impl<A: Actor> sealed::Sealed for ByState<A> {}
impl<A: Actor> DeadlineCx for ByState<A> {
    type Actor = A;
    type Phase = Never;
    type View = ();
}

/// [`Phased`](super::Phased)'s context: the view is the **phase clock**.
///
/// Slot and reaction receive a [`PhaseView`] (committed phase + entry
/// instant) and speak the machine's own transition verb,
/// [`Step<Phase>`](Step).
pub struct ByPhase<P> {
    marker: PhantomData<P>,
}

impl<P: PhasePolicy> sealed::Sealed for ByPhase<P> {}
impl<P: PhasePolicy> DeadlineCx for ByPhase<P> {
    type Actor = P::Actor;
    type Phase = P::Phase;
    type View = PhaseView<P>;
}

/// The [`ByPhase`] slot's read window: the committed phase and its entry
/// instant (the deadline anchor, reset on phase CHANGE only — D4).
pub struct PhaseView<P: PhasePolicy> {
    /// The committed phase.
    pub phase: P::Phase,
    /// When it was entered — the anchor a seat adds its magnitude to
    /// (`entered_at.checked_add(d)`; an overflowing sum is beyond
    /// representable time, i.e. no deadline).
    pub entered_at: Instant,
}

impl<P: PhasePolicy> Clone for PhaseView<P> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<P: PhasePolicy> Copy for PhaseView<P> {}

impl<P: PhasePolicy> core::fmt::Debug for PhaseView<P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PhaseView")
            .field("entered_at", &self.entered_at)
            .finish_non_exhaustive()
    }
}

/// THE deadline-plane policy seat (ADR-0028).
///
/// ONE context-generic trait serving [`Deadlined`] (`Cx = ByState<A>`)
/// and [`Phased`](super::Phased) (`Cx = ByPhase<P>`) — the relocated ADR-0025 pair,
/// unified.
///
/// The pair lives in one trait ON PURPOSE: the silent
/// declared-deadline/defaulted-reaction pair is unrepresentable — you
/// cannot implement the slot without its reaction (#196 no-default
/// precedent). Opting OUT is the explicit named [`NoTimeout`] plug.
pub trait DeadlinePolicy<Cx: DeadlineCx>: Send + 'static {
    /// Builds the seat from the spawn args — the D8 magnitude channel
    /// (`Args`-tunable per instance, never `SpawnConfig`).
    fn build(args: &<Cx::Actor as Actor>::Args) -> Self
    where
        Self: Sized;

    /// The next instant this actor needs waking, read from actor state
    /// and the context's view; `None` = no deadline. Keep it a **pure
    /// function of `(self, actor, view)`** — the loop re-reads it every
    /// iteration (the declarative slot; re-arming is implicit).
    fn next_deadline(&self, actor: &Cx::Actor, view: Cx::View) -> Option<Instant>;

    /// Reacts to expiry at a turn boundary, in the context's transition
    /// vocabulary ([`Step<Cx::Phase>`](Step) — literally `Flow` when the
    /// context is phase-less, ADR-0029). Takes a [`WeakActorRef`] by drain-window
    /// necessity (ADR-0025): a deadline fire carries no message to mint a
    /// strong ref from; self-sends degrade (`upgrade` → `None`).
    ///
    /// # Errors
    ///
    /// Returns the actor's error to crash controlled, exactly as a
    /// handler `Err` (crash domain `PanicReason::OnDeadline`).
    fn on_deadline(
        &self,
        actor: &mut Cx::Actor,
        view: Cx::View,
        actor_ref: WeakActorRef<Shell<Cx::Actor>>,
    ) -> impl Future<Output = Result<Step<Cx::Phase>, <Cx::Actor as Actor>::Error>> + Send;
}

/// The explicit "this machine has no deadlines" seat.
///
/// The slot is constantly `None`, so the loop's deadline arm never arms
/// and the (unreachable) reaction is `Stay`. A named opt-out, never a
/// silent default (#196 `OneForOne` precedent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoTimeout;

impl<P: PhasePolicy> DeadlinePolicy<ByPhase<P>> for NoTimeout {
    fn build(_: &<P::Actor as Actor>::Args) -> Self {
        Self
    }

    fn next_deadline(&self, _: &P::Actor, _: PhaseView<P>) -> Option<Instant> {
        None
    }

    async fn on_deadline(
        &self,
        _: &mut P::Actor,
        _: PhaseView<P>,
        _: WeakActorRef<Shell<P::Actor>>,
    ) -> Result<Step<P::Phase>, <P::Actor as Actor>::Error> {
        // Unreachable in practice: a constantly-None slot never fires.
        Ok(Step::Continue)
    }
}

/// The deadline capability (ADR-0026 stage 4) — the ADR-0025 plane's user
/// seat, `Cx = ByState` (ADR-0028).
///
/// Plugged as a cap-set field, it puts the actor on the loop's deadline
/// arm: the loop re-reads the policy's declarative slot every iteration
/// and delivers expiry through it. Carries the `Args`-built policy
/// instance (strategy-as-plugged-type, ADR-0026 constraint 5). Orthogonal
/// to loop shape: plain, watching, and supervising sets can all carry it.
pub struct Deadlined<DP> {
    policy: DP,
}

impl<DP> Deadlined<DP> {
    /// Builds the capability from the spawn args (the seat's D8 magnitude
    /// channel).
    #[must_use]
    pub fn build<A: Actor>(args: &A::Args) -> Self
    where
        DP: DeadlinePolicy<ByState<A>>,
    {
        Self {
            policy: DP::build(args),
        }
    }
}

impl<A: Actor, DP: DeadlinePolicy<ByState<A>>> DeadlineHook<A> for Deadlined<DP> {
    fn next_deadline(&self, actor: &A) -> Option<Instant> {
        self.policy.next_deadline(actor, ())
    }

    async fn on_deadline(
        &mut self,
        actor: &mut A,
        actor_ref: WeakActorRef<Shell<A>>,
    ) -> Result<Flow, A::Error> {
        // `Step<Never> = Flow` (ADR-0029): the policy's verdict IS the
        // hook's — the #290 adapter is gone.
        self.policy.on_deadline(actor, (), actor_ref).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{Nameless, dead_weak};
    use super::DeadlineHook;
    use crate::actor::Flow;

    /// The `()` floor: a capability-less set declares no deadline and
    /// its expiry hook is inert — the arm stays disabled for plain
    /// actors.
    #[tokio::test]
    async fn unit_caps_declare_no_deadline() {
        let mut nameless = Nameless;
        assert_eq!(
            <() as DeadlineHook<Nameless>>::next_deadline(&(), &nameless),
            None,
        );
        let out = <() as DeadlineHook<Nameless>>::on_deadline(&mut (), &mut nameless, dead_weak())
            .await
            .expect("the unit hook is infallible");
        assert_eq!(out, Flow::Continue, "the unit hook keeps running");
    }
}
