//! The phase machine (ADR-0024, seats declared per ADR-0028):
//! [`PhasePolicy`] + [`Phased`], the sealed deferral seats
//! ([`NoDefer`]/[`Bounded`]) over the [`PhaseBuffer`] storage seam, and
//! the [`Admission`] loop hook servicing the gate.

use core::{future::Future, marker::PhantomData};

use tokio::time::Instant;

use crate::actor::{Flow, WeakActorRef, sealed};

use super::{
    Actor, ByPhase, DeadlineHook, DeadlinePolicy, Deferred, Disposition, Never, Overflow,
    PhaseView, Replay, Shell, StashPolicy, Stashing, Step,
};

/// A [`Phased`] machine's stash storage seam — sealed; the two
/// implementors are [`Stashing`] (the [`Bounded`] seat) and [`NoStash`]
/// (the [`NoDefer`] seat: a ZST with nothing to hold).
pub trait PhaseBuffer<M>: sealed::Sealed + Send + 'static {
    /// Pops the next released message, oldest first (the in-step replay
    /// drain — [`Replay`] semantics).
    fn next_ready(&mut self) -> Option<M>;
    /// Releases everything held (the D4 transition effect).
    fn release_all(&mut self);
}

impl<M: Send + 'static> sealed::Sealed for Stashing<M> {}
impl<M: Send + 'static> PhaseBuffer<M> for Stashing<M> {
    fn next_ready(&mut self) -> Option<M> {
        self.pop_ready()
    }

    fn release_all(&mut self) {
        self.unstash_all();
    }
}

/// The [`NoDefer`] machine's stash type: zero-sized, holds nothing — a
/// defer-path allocation is unrepresentable, not merely unexercised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoStash;

impl sealed::Sealed for NoStash {}
impl<M: Send + 'static> PhaseBuffer<M> for NoStash {
    fn next_ready(&mut self) -> Option<M> {
        None
    }

    fn release_all(&mut self) {}
}

/// One [`DeferSeat::admit_defer`] verdict — how the seat routed a gated
/// `Defer`.
#[derive(Debug)]
pub enum DeferOutcome<M, Ph> {
    /// Stashed; the loop continues.
    Absorbed,
    /// The stash was full and the overflow hook redelivered the message
    /// to `handle` (D6 default: visible-but-unrefused shedding).
    Redeliver(M),
    /// The overflow hook absorbed it; apply this step.
    Handled(Step<Ph>),
}

/// The deferral seat of a [`Phased`] machine (ADR-0028).
///
/// Sealed, core-provided: [`NoDefer`] or [`Bounded`]. The seat owns the
/// gate's `Defer` token type, the stash storage type, and the defer
/// routing; plugging it (or not) is the declaration the gate's verdict
/// type enforces.
pub trait DeferSeat<P: PhasePolicy>: sealed::Sealed + Send + 'static {
    /// The gate's `Defer` payload — [`Never`] (uninhabited: cannot defer)
    /// or [`Deferred`].
    type Token: Send + 'static;
    /// The stash storage — [`NoStash`] (ZST) or [`Stashing`].
    type Stash: PhaseBuffer<<P::Actor as Actor>::Msg>;

    /// Carves the storage from the spawn args.
    fn build_stash(args: &<P::Actor as Actor>::Args) -> Self::Stash;

    /// Routes a gated `Defer`: stash it, or hand the overflow to
    /// [`PhasePolicy::on_defer_full`] (D6). Statically unreachable for
    /// [`NoDefer`] — the token is uninhabited.
    ///
    /// # Errors
    ///
    /// Returns the actor's error if the overflow hook fails — a
    /// controlled crash, exactly as a handler `Err`.
    fn admit_defer(
        token: Self::Token,
        actor: &mut P::Actor,
        phase: P::Phase,
        msg: <P::Actor as Actor>::Msg,
        stash: &mut Self::Stash,
    ) -> impl Future<Output = DeferRouted<P>> + Send;
}

/// [`DeferSeat::admit_defer`]'s outcome, named so the hook's RPITIT stays
/// readable: the routing verdict or the actor's own error.
pub type DeferRouted<P> = Result<
    DeferOutcome<<<P as PhasePolicy>::Actor as Actor>::Msg, <P as PhasePolicy>::Phase>,
    <<P as PhasePolicy>::Actor as Actor>::Error,
>;

/// The explicit "this machine never defers" seat.
///
/// No token, no stash, no bound — an undeclared `Defer` is a COMPILE
/// error, and no buffer type exists in the machine. A named opt-out,
/// never a silent default.
///
/// The law, pinned: a `NoDefer` machine's gate cannot spell `Defer` —
/// its verdict type is `Disposition<Never>` and [`Deferred`] does not
/// fit the uninhabited token:
///
/// ```compile_fail,E0053
/// use bombay::actor::Flow;
/// use bombay::capability::{
///     Actor, CapSet, Ctx, Deferred, Disposition, NoDefer, NoTimeout, PhasePolicy,
/// };
/// use bombay::message::Msg;
///
/// #[derive(Debug)]
/// struct Ping;
/// impl Msg for Ping {}
///
/// struct A;
/// #[derive(bombay_macros::Provide)]
/// struct ACaps {
///     phased: bombay::capability::Phased<APolicy>,
/// }
/// impl CapSet<A> for ACaps {
///     fn build(args: &()) -> Self {
///         Self { phased: bombay::capability::Phased::build(args) }
///     }
/// }
/// impl Actor for A {
///     type Msg = Ping;
///     type Args = ();
///     type Error = core::convert::Infallible;
///     type Caps = ACaps;
///     async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Self::Error> {
///         Ok(Self)
///     }
///     async fn handle(&mut self, _: Ping, _: Ctx<'_, Self>) -> Result<Flow, Self::Error> {
///         Ok(Flow::Continue)
///     }
/// }
///
/// struct APolicy;
/// impl PhasePolicy for APolicy {
///     type Actor = A;
///     type Phase = ();
///     type Deferral = NoDefer;   // declared: never defers
///     type Timeout = NoTimeout;
///     fn initial(_: &()) {}
///     fn gate((): (), _: &Ping) -> Disposition<Deferred> {
///         Disposition::Defer(Deferred)   // E0053: the declared token is `Never`
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoDefer;

impl sealed::Sealed for NoDefer {}
impl<P: PhasePolicy> DeferSeat<P> for NoDefer {
    type Token = Never;
    type Stash = NoStash;

    fn build_stash(_: &<P::Actor as Actor>::Args) -> NoStash {
        NoStash
    }

    async fn admit_defer(
        token: Never,
        _: &mut P::Actor,
        _: P::Phase,
        _: <P::Actor as Actor>::Msg,
        _: &mut NoStash,
    ) -> Result<DeferOutcome<<P::Actor as Actor>::Msg, P::Phase>, <P::Actor as Actor>::Error> {
        match token {}
    }
}

/// The bounded deferral seat.
///
/// Gate `Defer`s go into a [`Stashing`] whose capacity is the plugged
/// [`StashPolicy`]'s (REUSED — the bound is spelled once, ADR-0022:
/// deferral without a bound is the rejected design); overflow routes
/// through [`PhasePolicy::on_defer_full`] with the message INTACT (D6).
pub struct Bounded<SP> {
    marker: PhantomData<SP>,
}

impl<SP> sealed::Sealed for Bounded<SP> {}
impl<P: PhasePolicy, SP: StashPolicy<P::Actor> + Send + 'static> DeferSeat<P> for Bounded<SP> {
    type Token = Deferred;
    type Stash = Stashing<<P::Actor as Actor>::Msg>;

    fn build_stash(args: &<P::Actor as Actor>::Args) -> Self::Stash {
        Stashing::bounded(SP::capacity(args))
    }

    async fn admit_defer(
        Deferred: Deferred,
        actor: &mut P::Actor,
        phase: P::Phase,
        msg: <P::Actor as Actor>::Msg,
        stash: &mut Self::Stash,
    ) -> Result<DeferOutcome<<P::Actor as Actor>::Msg, P::Phase>, <P::Actor as Actor>::Error> {
        match stash.stash(msg) {
            Ok(()) => Ok(DeferOutcome::Absorbed),
            Err(full) => {
                let refused = full.msg();
                Ok(
                    match P::on_defer_full(actor, phase, refused, stash).await? {
                        Overflow::Redeliver(m) => DeferOutcome::Redeliver(m),
                        Overflow::Handled(step) => DeferOutcome::Handled(step),
                    },
                )
            }
        }
    }
}

/// The stash type a [`PhasePolicy`]'s deferral seat declares —
/// [`NoStash`] for [`NoDefer`], [`Stashing`] for [`Bounded`].
pub type StashOf<P> = <<P as PhasePolicy>::Deferral as DeferSeat<P>>::Stash;

/// The gate token a [`PhasePolicy`]'s deferral seat declares —
/// [`Never`] for [`NoDefer`] (a `Defer` verdict cannot be constructed),
/// [`Deferred`] for [`Bounded`].
pub type TokenOf<P> = <<P as PhasePolicy>::Deferral as DeferSeat<P>>::Token;

/// The phase-machine policy — ONE machine with DECLARED seats.
///
/// ADR-0028, correcting ADR-0026 constraint 5's boundary: the unit is
/// the machine plus its *required* core; the deferral and deadline seats
/// are plugged strategies, present exactly when declared.
///
/// The `capability`-surface seat of ADR-0024's `FsmActor` (D1–D10 semantics
/// preserved). A phase policy is actor-specific — it gates the actor's
/// closed menu — so the served actor is the [`Actor`](PhasePolicy::Actor)
/// associated type, and `Phased<P>` stays a one-parameter field type.
///
/// The silent-pair law (#196) holds structurally in both directions:
/// a `Defer` verdict without the [`Bounded`] seat does not compile (the
/// token is uninhabited); a deadline slot without its reaction does not
/// exist (the pair shares one [`DeadlinePolicy`] trait); and opting out
/// is the explicit named plug ([`NoDefer`] / [`NoTimeout`](super::NoTimeout)), never a
/// silent default. The seat declarations ARE the static declaration
/// table (#243/#286-inferable).
///
/// [`on_defer_full`]: PhasePolicy::on_defer_full
pub trait PhasePolicy: Send + Sized + 'static {
    /// The served actor (whose menu the gate classifies).
    type Actor: Actor;
    /// The phase NAME — a plain tag enum (`gen_statem`'s name/data split,
    /// D2); phase DATA stays in the actor. `Copy` keeps `Step<Phase>`
    /// unconditionally `Copy`.
    type Phase: Copy + PartialEq + Send + 'static;
    /// The deferral seat: [`NoDefer`] (no token, no stash, no bound) or
    /// [`Bounded`] over the reused [`StashPolicy`].
    type Deferral: DeferSeat<Self>;
    /// The deadline seat: [`NoTimeout`](super::NoTimeout) (slot constantly disarmed) or a
    /// plugged [`DeadlinePolicy`] strategy over [`ByPhase`] (`Self` is
    /// legal for the one-struct style).
    type Timeout: DeadlinePolicy<ByPhase<Self>>;

    /// The phase the machine starts in. (There is no policy instance:
    /// every core item is a static declaration; instance-tunable
    /// magnitudes — the D8 channel — ride the SEATS' `build(args)`.)
    fn initial(args: &<Self::Actor as Actor>::Args) -> Self::Phase;

    /// The whole admission protocol in one declarative place (D5) — a
    /// static declaration table (#243-derivable). `handle` never sees a
    /// message its phase declared away. The verdict type carries the
    /// deferral declaration: [`Disposition`] (= `Disposition<Never>`)
    /// for a [`NoDefer`] machine, `Disposition<Deferred>` for [`Bounded`].
    fn gate(phase: Self::Phase, msg: &<Self::Actor as Actor>::Msg) -> Disposition<TokenOf<Self>>;

    /// A declaratively-deferred message found the stash at capacity (D6).
    /// The message is handed back INTACT; the default redelivers it to
    /// `handle` (visible-but-unrefused shedding) — override for a loud
    /// typed refusal. Stash access per D6b (a handler-plane hook).
    /// Reachable only from the [`Bounded`] seat; dead code under
    /// [`NoDefer`].
    ///
    /// # Errors
    ///
    /// A returned `Err` is a controlled crash, exactly as a handler's.
    fn on_defer_full(
        actor: &mut Self::Actor,
        phase: Self::Phase,
        msg: <Self::Actor as Actor>::Msg,
        stash: &mut Stashing<<Self::Actor as Actor>::Msg>,
    ) -> impl Future<Output = DeferVerdict<Self>> + Send {
        let _ = (actor, phase, stash);
        async move { Ok(Overflow::Redeliver(msg)) }
    }
}

/// [`PhasePolicy::on_defer_full`]'s outcome, named so the hook's RPITIT
/// stays readable: the [`Overflow`] verdict or the actor's own error.
pub type DeferVerdict<P> = Result<
    Overflow<<<P as PhasePolicy>::Actor as Actor>::Msg, <P as PhasePolicy>::Phase>,
    <<P as PhasePolicy>::Actor as Actor>::Error,
>;

/// The phase capability (ADR-0026 stage 4, card #281) — ADR-0024's
/// machine as one plugged unit, riding the ADR-0025 plane.
///
/// Owns the machine state the framework must observe: the committed
/// phase, its entry instant (the deadline anchor), the pending
/// transition, and the bounded phase stash (ADR-0022 two-queue snapshot,
/// the [`Stashing`] surface reused). **Embeds the deadline seat**: a
/// `Phased` field IS the cap set's deadline participation — plugging a
/// separate [`Deadlined`](super::Deadlined) beside it is rejected (one deadline seat per
/// actor; the loop has one arm).
///
/// Transition effects run on phase CHANGE only, in D4 order: switch →
/// reset the entry instant → release the stash; deadline cancel/re-arm is
/// IMPLICIT (the loop re-reads [`next_deadline`](DeadlineHook) from the
/// new phase — that is the whole point of the declarative plane). The
/// released batch replays in-step, re-gated in the NEW phase, ahead of
/// the mailbox backlog.
pub struct Phased<P: PhasePolicy> {
    timeout: P::Timeout,
    phase: P::Phase,
    pending: Option<P::Phase>,
    entered_at: Instant,
    stash: StashOf<P>,
}

impl<P: PhasePolicy> Phased<P> {
    /// Builds the machine from the spawn args: plugged timeout seat,
    /// initial phase, the deferral seat's storage (a ZST for [`NoDefer`]
    /// — no buffer exists), entry clock started now.
    #[must_use]
    pub fn build(args: &<P::Actor as Actor>::Args) -> Self {
        Self {
            timeout: <P::Timeout as DeadlinePolicy<ByPhase<P>>>::build(args),
            phase: P::initial(args),
            pending: None,
            entered_at: Instant::now(),
            stash: <P::Deferral as DeferSeat<P>>::build_stash(args),
        }
    }

    /// The committed phase. A [`goto`](Phased::goto) in the current
    /// handler is not yet visible here — phases change at step
    /// boundaries (D3).
    #[must_use]
    pub const fn phase(&self) -> P::Phase {
        self.phase
    }

    /// Requests a transition, committed by the framework only after the
    /// current handler returns `Ok` (D3: a mid-handler panic never
    /// observes a half-switched phase). Last call wins within one
    /// handler; `goto(current)` commits to a no-op (D3's
    /// `Goto(current) ≡ Stay`).
    pub const fn goto(&mut self, next: P::Phase) {
        self.pending = Some(next);
    }

    /// Applies a policy hook's step at its boundary (the hook return IS
    /// the boundary).
    fn apply(&mut self, step: Step<P::Phase>) -> Flow {
        match step {
            Step::Stay => Flow::Continue,
            Step::Stop => Flow::Stop,
            Step::Goto(next) => {
                self.commit_to(next);
                Flow::Continue
            }
        }
    }

    /// The D4 transition effects, on phase CHANGE only: switch → reset
    /// the deadline anchor → release the stash (replayed by the Shell's
    /// in-step drain, re-gated in the new phase). Deadline cancel/re-arm
    /// is implicit via the declarative slot.
    fn commit_to(&mut self, next: P::Phase) {
        if next != self.phase {
            self.phase = next;
            self.entered_at = Instant::now();
            self.stash.release_all();
        }
    }

    /// Commits a pending [`goto`](Phased::goto), if any.
    fn commit_pending(&mut self) {
        if let Some(next) = self.pending.take() {
            self.commit_to(next);
        }
    }
}

impl<P, SP> Phased<P>
where
    P: PhasePolicy<Deferral = Bounded<SP>>,
    SP: StashPolicy<P::Actor> + Send + 'static,
{
    /// The phase stash — D5's manual escape hatch (`stash`/`unstash_all`
    /// for release timing that is not transition-shaped), same bounded
    /// buffer the gate defers into. Exists only on a [`Bounded`] machine:
    /// a [`NoDefer`] machine has no buffer to reach.
    pub const fn stash(&mut self) -> &mut Stashing<<P::Actor as Actor>::Msg> {
        &mut self.stash
    }
}

impl<P: PhasePolicy> Replay<<P::Actor as Actor>::Msg> for Phased<P> {
    fn next_replay(&mut self) -> Option<<P::Actor as Actor>::Msg> {
        self.stash.next_ready()
    }
}

impl<P: PhasePolicy> DeadlineHook<P::Actor> for Phased<P> {
    /// The plugged seat's slot over the phase clock ([`PhaseView`]) —
    /// the ADR-0025 declarative slot as a pure function of machine state.
    /// Structurally `None` under [`NoTimeout`](super::NoTimeout).
    fn next_deadline(&self, actor: &P::Actor) -> Option<Instant> {
        self.timeout.next_deadline(
            actor,
            PhaseView {
                phase: self.phase,
                entered_at: self.entered_at,
            },
        )
    }

    async fn on_deadline(
        &mut self,
        actor: &mut P::Actor,
        actor_ref: WeakActorRef<Shell<P::Actor>>,
    ) -> Result<Flow, <P::Actor as Actor>::Error> {
        let view = PhaseView {
            phase: self.phase,
            entered_at: self.entered_at,
        };
        let step = self.timeout.on_deadline(actor, view, actor_ref).await?;
        Ok(self.apply(step))
    }
}

/// The in-step admission hook a capability set exposes — the third
/// loop-participation trait, servicing [`Phased`]'s gate on EVERY
/// delivered or replayed message (ADR-0026 stage 4).
///
/// The [`Shell`] routes each message through
/// [`admit`](Admission::admit) before the user handler and calls
/// [`commit`](Admission::commit) after a delivered handler returns `Ok`
/// (never after `Err`/panic — D3). `()` and non-phased sets deliver
/// everything and commit nothing. Derive-emitted; users never hand-write
/// it.
pub trait Admission<A: Actor> {
    /// Classifies one message: hand it to the handler, or absorb it
    /// (deferred, ignored, or consumed by the overflow hook — whose step
    /// may stop the actor).
    ///
    /// # Errors
    ///
    /// Returns [`A::Error`](Actor::Error) if the overflow hook fails — a
    /// controlled crash, exactly as a handler `Err`.
    fn admit(
        &mut self,
        actor: &mut A,
        msg: A::Msg,
    ) -> impl Future<Output = Result<Admitted<A::Msg>, A::Error>> + Send;

    /// Commits a pending transition after a delivered handler's `Ok`.
    fn commit(&mut self);
}

/// One [`Admission::admit`] verdict.
#[derive(Debug)]
pub enum Admitted<M> {
    /// Deliver to the user handler.
    Deliver(M),
    /// Absorbed by the cap set; the step's continuation decision.
    Absorbed(Flow),
}

impl<A: Actor> Admission<A> for () {
    async fn admit(&mut self, _: &mut A, msg: A::Msg) -> Result<Admitted<A::Msg>, A::Error> {
        Ok(Admitted::Deliver(msg))
    }

    fn commit(&mut self) {}
}

impl<P: PhasePolicy> Admission<P::Actor> for Phased<P> {
    async fn admit(
        &mut self,
        actor: &mut P::Actor,
        msg: <P::Actor as Actor>::Msg,
    ) -> Result<Admitted<<P::Actor as Actor>::Msg>, <P::Actor as Actor>::Error> {
        match P::gate(self.phase, &msg) {
            Disposition::Deliver => Ok(Admitted::Deliver(msg)),
            Disposition::Ignore => Ok(Admitted::Absorbed(Flow::Continue)),
            Disposition::Defer(token) => {
                // The seat routes it (stash, or D6 overflow). Statically
                // unreachable under NoDefer: the token is uninhabited.
                match <P::Deferral as DeferSeat<P>>::admit_defer(
                    token,
                    actor,
                    self.phase,
                    msg,
                    &mut self.stash,
                )
                .await?
                {
                    DeferOutcome::Absorbed => Ok(Admitted::Absorbed(Flow::Continue)),
                    DeferOutcome::Redeliver(m) => Ok(Admitted::Deliver(m)),
                    DeferOutcome::Handled(step) => Ok(Admitted::Absorbed(self.apply(step))),
                }
            }
        }
    }

    fn commit(&mut self) {
        self.commit_pending();
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroUsize;

    use super::super::fixtures::{M, MMsg, MPolicy, Ph};
    use super::{Admission, Admitted, Overflow, PhasePolicy, Stashing};
    use crate::mailbox::Capacity;

    /// D6 default: an un-overridden `on_defer_full` hands the INTACT
    /// message back as `Redeliver` — visible-but-unrefused shedding,
    /// never a silent drop.
    #[tokio::test]
    async fn default_on_defer_full_redelivers_the_intact_message() {
        let mut actor = M;
        let mut stash = Stashing::<MMsg>::bounded(
            Capacity::new(NonZeroUsize::new(1).expect("nonzero")).expect("valid"),
        );
        let out = <MPolicy as PhasePolicy>::on_defer_full(
            &mut actor,
            Ph::A,
            MMsg::GotoBThenOk,
            &mut stash,
        )
        .await
        .expect("the default is infallible");
        assert!(
            matches!(out, Overflow::Redeliver(MMsg::GotoBThenOk)),
            "the default verdict redelivers the exact message",
        );
    }

    /// The `()` floor: a capability-less set admits everything and
    /// commits nothing.
    #[tokio::test]
    async fn unit_caps_admit_everything() {
        let mut unit = ();
        let out = <() as Admission<M>>::admit(&mut unit, &mut M, MMsg::GotoBThenOk)
            .await
            .expect("the unit admission is infallible");
        assert!(
            matches!(out, Admitted::Deliver(MMsg::GotoBThenOk)),
            "the floor delivers everything",
        );
        <() as Admission<M>>::commit(&mut unit);
    }
}
