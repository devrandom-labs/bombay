//! The distilled actor surface (ADR-0026, stage 1 — card #278): ONE
//! [`Actor`] trait, capabilities as plugged types on [`Actor::Caps`],
//! access through the one typed window [`Ctx`].
//!
//! Stage-1 shape: the new surface runs ON the existing runtime through the
//! internal [`Shell`] adapter (which implements the shipped
//! [`actor::Actor`](crate::actor::Actor) and drives the untouched run
//! loop). The [`Handle`] alias names the seam; it collapses when a later
//! stage teaches the loop to drive this trait natively.
//!
//! Compile-time laws (ADR-0026 constraints):
//! - capability access is gated by [`Provide`] bounds — a capability the
//!   set does not declare is a **compile error**, and no runtime-checked
//!   accessor exists (constraint 1);
//! - the seam is **open**: any crate defines a capability type and a
//!   cap-set struct providing it (constraint 2; encoding per the
//!   ADR-0026 Addendum — `#[derive(bombay_macros::Provide)]` emits the
//!   per-field impls, and duplicate capability fields are rejected by
//!   coherence, E0119).

use core::{any::type_name, future::Future, marker::PhantomData, ops::ControlFlow};

use tokio::time::Instant;

use crate::{
    actor::{
        ActorRef, Flow, LinkReact, PreparedActor, SpawnConfig, SupervisedReact, WeakActorRef,
        sealed,
    },
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::{ActorId, Capacity, Mailboxed},
    message::Msg,
    restart::SupervisionStrategy,
    stash::Stash,
};

/// The typed overflow handback of the [`Stashing`] capability — re-exported
/// so a stashing actor imports its whole surface from [`caps`](crate::caps).
pub use crate::stash::StashFull;

/// The deadline plane's instant type (tokio's, paused-clock testable) —
/// re-exported so a [`DeadlinePolicy`] and the derive-emitted
/// [`DeadlineHook`] impls name it without a direct tokio dependency.
pub use tokio::time::Instant as DeadlineInstant;

/// The one user trait of the distilled surface.
///
/// Identity + behavior only: everything else — deferral, phases,
/// deadlines, watching — is a
/// capability TYPE plugged into [`Caps`](Actor::Caps) (`()` for a plain
/// actor) and reached through [`Ctx::cap`]. Policies ride the capability
/// types as required trait impls; nothing about a capability can be
/// half-implemented (ADR-0026 constraint 5).
pub trait Actor: Sized + Send + 'static {
    /// The closed message menu (#114 — the slot-size tripwire rides the
    /// [`Msg`] bound exactly as on the shipped trait).
    type Msg: Msg;
    /// The argument passed to [`init`](Actor::init) to build the state.
    type Args: Send;
    /// The actor's own domain error, kept typed end to end.
    type Error: ReplyError;
    /// The capability set: `()` for plain actors, otherwise a named
    /// struct built by [`CapSet::build`] whose fields are capability
    /// types (one [`Provide`] impl per field — derive-generated).
    ///
    /// Bounded [`Replay<Self::Msg>`](Replay) so the loop can service in-step
    /// replay uniformly — `()` and non-stashing sets yield `None`, a set with
    /// a [`Stashing`] field drains it — [`SelectRunner<Self>`](SelectRunner)
    /// so every cap set names its run-loop shape at compile time (stage 3),
    /// [`DeadlineHook<Self>`](DeadlineHook) so every loop shape can poll
    /// the ADR-0025 deadline plane uniformly (stage 4) — `()` and
    /// deadline-less sets stay disabled — and [`Admission<Self>`](Admission)
    /// so a [`Phased`] gate classifies every delivered or replayed message
    /// before the handler — `()` and non-phased sets deliver everything.
    /// The derive emits all of them alongside `Provide`, so none is a
    /// separate thing to forget.
    type Caps: CapSet<Self>
        + Replay<Self::Msg>
        + SelectRunner<Self>
        + DeadlineHook<Self>
        + Admission<Self>;

    /// A human-readable name for logs/tracing. Defaults to the type name.
    #[must_use]
    fn name() -> &'static str {
        type_name::<Self>()
    }

    /// Builds the actor state. Runs to completion before any message is
    /// handled. The capability set is already built (from the same
    /// `args`) and reachable through `cx`.
    fn init(
        args: Self::Args,
        cx: Ctx<'_, Self>,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    /// Handles one message. Same continuation contract as the shipped
    /// trait ([ADR-0023]): `Ok(Flow::Continue)` keeps running,
    /// `Ok(Flow::Stop)` stops cleanly after this handler, `Err` is a
    /// controlled crash.
    ///
    /// [ADR-0023]: https://github.com/devrandom-labs/bombay/blob/main/docs/adr/0023-handler-stop-return-value-not-out-param.md
    fn handle(
        &mut self,
        msg: Self::Msg,
        cx: Ctx<'_, Self>,
    ) -> impl Future<Output = Result<Flow, Self::Error>> + Send;

    /// Observes a caught panic and names the terminal stop reason —
    /// semantics identical to the shipped hook (`&mut self` is
    /// poisoned; stop-only).
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Shell<Self>>,
        err: PanicError,
    ) -> impl Future<Output = ActorStopReason> + Send {
        let _ = actor_ref;
        async move { ActorStopReason::Panicked(err) }
    }

    /// Terminal cleanup — semantics identical to the shipped hook
    /// (time-bounded by the runtime; resource release only on the
    /// poisoned path).
    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Shell<Self>>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let _ = (actor_ref, reason);
        async { Ok(()) }
    }
}

/// A capability set, built from the spawn args before
/// [`Actor::init`] runs.
///
/// Implemented by `()` (plain actors) and by user cap-set structs. The
/// per-field access impls are [`Provide`]; a derive emits those — this
/// `build` stays hand-written in stage 1 (it needs policy knowledge a
/// derive cannot infer; a build-generating derive is card #243).
pub trait CapSet<A: Actor>: Send + 'static {
    /// Builds the set from the actor's spawn args.
    fn build(args: &A::Args) -> Self;
}

impl<A: Actor> CapSet<A> for () {
    fn build(_: &A::Args) -> Self {}
}

/// "This capability set provides capability `C`" — the open seam.
///
/// Per the ADR-0026 Addendum: implemented once per cap-set FIELD, on
/// the user's own struct, so any crate can participate and duplicate
/// capability fields are unrepresentable (overlapping impls, E0119).
pub trait Provide<C> {
    /// Exclusive access to the capability's state.
    fn provide(&mut self) -> &mut C;
}

/// The loop hook a capability set exposes for **in-step replay**.
///
/// After each user [`handle`](Actor::handle), the [`Shell`] drains this until
/// it yields `None` — that is how a [`Stashing`] capability replays deferred
/// messages ahead of the mailbox backlog (ADR-0022). It is the "participation"
/// half of a capability (the loop servicing it each step), distinct from the
/// "access" half ([`Ctx::cap`]).
///
/// The design point of stage 2 (ADR-0026): `Shell<A>` holds `A::Caps` as an
/// opaque set, so it cannot *discover* a stash generically — a blanket impl is
/// coherence-infeasible (E0119) and specialization is unstable. So the one
/// [`derive(Provide)`](bombay_macros::Provide) that already reads the cap-set
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

/// Bounded deferral capability (ADR-0022 semantics) — the `caps`-surface
/// successor to the removed `StashActor`/`Stashed` pair.
///
/// A field of a cap-set struct: `cx.cap::<Stashing<Msg>>()` inside a handler
/// defers a message the current state cannot accept ([`stash`](Stashing::stash))
/// and releases the batch ([`unstash_all`](Stashing::unstash_all)); the
/// [`Shell`] replays the released messages in-step, ahead of the backlog, in
/// arrival order. Capacity comes from a required [`StashPolicy`] (Args-sourced),
/// wired in the cap set's hand-written [`CapSet::build`]. The buffer holds bare
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
/// [`CapSet::build`]; bounded deferral is the point, so there is no default.
pub trait StashPolicy<A: Actor> {
    /// The stash capacity for this actor, derived from its spawn args.
    fn capacity(args: &A::Args) -> Capacity;
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
/// use bombay::caps::{
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
///     phased: bombay::caps::Phased<APolicy>,
/// }
/// impl CapSet<A> for ACaps {
///     fn build(args: &()) -> Self {
///         Self { phased: bombay::caps::Phased::build(args) }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

/// The loop hook a capability set exposes for the **ADR-0025 deadline
/// plane** — the participation half of [`Deadlined`] (and of `Phased`), as
/// [`Replay`] is of [`Stashing`] (ADR-0026 stage 4, card #281).
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
    /// [`Step`] verdict: [`Never`] for phase-less contexts (`Step<Never> ≅
    /// Flow` — `Goto` is unconstructible), the machine's phase for
    /// [`ByPhase`].
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

/// [`Phased`]'s context: the view is the **phase clock**.
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
/// and [`Phased`] (`Cx = ByPhase<P>`) — the relocated ADR-0025 pair,
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
    /// vocabulary ([`Step<Cx::Phase>`](Step) — `Flow`-isomorphic when the
    /// context is phase-less). Takes a [`WeakActorRef`] by drain-window
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
        Ok(Step::Stay)
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
        // `Step<Never> ≅ Flow` — the phase-less context's Goto is
        // uninhabited, so the adapter is total.
        Ok(match self.policy.on_deadline(actor, (), actor_ref).await? {
            Step::Stay => Flow::Continue,
            Step::Stop => Flow::Stop,
            Step::Goto(never) => match never {},
        })
    }
}

/// The uninhabited type with two structural jobs (ADR-0028).
///
/// As a phase: [`Step<Never>`](Step) has no constructible `Goto` — it is
/// isomorphic to [`Flow`] (a plain actor is a one-phase machine), which is
/// how one [`DeadlinePolicy`] trait serves both [`Deadlined`] and
/// [`Phased`]. As a defer token: [`Disposition<Never>`](Disposition) has
/// no constructible `Defer` — a machine that plugged [`NoDefer`] cannot
/// even SPELL a deferral; the law is the type, not a convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Never {}

/// The [`Bounded`] deferral seat's gate token: `Disposition::Defer(Deferred)`.
///
/// Constructible only because the seat is plugged — its existence in a
/// gate's verdict type IS the deferral declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deferred;

/// Per-phase message admission — the P trio (ADR-0024 D5, P PLDI 2013
/// `defer`/`ignore` made payload-capable): what the framework does with a
/// message BEFORE the handler is consulted.
///
/// Generic over the deferral token `D` (default [`Never`]): a gate whose
/// policy plugged [`NoDefer`] returns plain `Disposition` and cannot
/// construct `Defer`; a [`Bounded`] policy's gate returns
/// `Disposition<Deferred>` (ADR-0028).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition<D = Never> {
    /// Hand it to `handle` now.
    Deliver,
    /// Stash it; re-gate on every phase change (P `defer`). Carries the
    /// plugged seat's token — the declaration rides the verdict type.
    Defer(D),
    /// Drop it deliberately, by declaration (P `ignore` — recorded
    /// intent, never a silent loss).
    Ignore,
}

/// A policy hook's transition decision — `Flow` (ADR-0023) plus one
/// variant, `Copy`, zero-box (ADR-0024 D3).
///
/// `Goto(current)` is deliberately a no-op (`gen_statem` `next_state` to
/// the same state): no unstash, no deadline reset. In `handle`, the
/// transition verb is [`Phased::goto`] instead — recorded there,
/// committed by the framework only after the handler returns `Ok`, so
/// D3's commit-after-Ok law holds with no `Step` return channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<Ph> {
    /// Stay in the current phase (`gen_statem` `keep_state`).
    Stay,
    /// Transition (no-op when already there).
    Goto(Ph),
    /// Stop cleanly (reason `Normal`), like `Flow::Stop`.
    Stop,
}

/// The [`PhasePolicy::on_defer_full`] verdict — ADR-0024 D6's overflow
/// handback, never a silent drop.
#[derive(Debug)]
pub enum Overflow<M, Ph> {
    /// Deliver the overflowed message to `handle` after all — the
    /// default: visible-but-unrefused shedding.
    Redeliver(M),
    /// The hook absorbed it (typically a loud typed refusal); apply this
    /// step.
    Handled(Step<Ph>),
}

/// The phase-machine policy — ONE machine with DECLARED seats.
///
/// ADR-0028, correcting ADR-0026 constraint 5's boundary: the unit is
/// the machine plus its *required* core; the deferral and deadline seats
/// are plugged strategies, present exactly when declared.
///
/// The `caps`-surface seat of ADR-0024's `FsmActor` (D1–D10 semantics
/// preserved). A phase policy is actor-specific — it gates the actor's
/// closed menu — so the served actor is the [`Actor`](PhasePolicy::Actor)
/// associated type, and `Phased<P>` stays a one-parameter field type.
///
/// The silent-pair law (#196) holds structurally in both directions:
/// a `Defer` verdict without the [`Bounded`] seat does not compile (the
/// token is uninhabited); a deadline slot without its reaction does not
/// exist (the pair shares one [`DeadlinePolicy`] trait); and opting out
/// is the explicit named plug ([`NoDefer`] / [`NoTimeout`]), never a
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
    /// The deadline seat: [`NoTimeout`] (slot constantly disarmed) or a
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
/// separate [`Deadlined`] beside it is rejected (one deadline seat per
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
    /// Structurally `None` under [`NoTimeout`].
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

/// The death-reaction policy seat of the [`Watching`] capability — the
/// relocated `on_link_died` hook (ADR-0026 stage 3, card #280).
///
/// Parameterized by the actor so a policy can react through `&mut A`
/// (record, mutate state); the loop's delivery rules are unchanged (a
/// notice arriving after the loop's stop decision is dropped, #266).
/// Policies ride the [`Watching`] TYPE — chosen by name, never inherited.
pub trait WatchPolicy<A: Actor>: Send + 'static {
    /// Reacts to the death of a watched/linked actor. `Break(reason)`
    /// stops the watcher with that reason; `Continue` keeps it running.
    ///
    /// # Errors
    ///
    /// Returns [`A::Error`](Actor::Error) if the reaction fails — a
    /// controlled crash, exactly as a handler `Err`.
    fn on_link_died(
        actor: &mut A,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, A::Error>> + Send;
}

/// The NAMED OTP propagation policy — semantics byte-identical to the
/// removed `Watch::on_link_died` default.
///
/// A **linked abnormal** death propagates
/// ([`LinkDied`](ActorStopReason::LinkDied) carrying the original reason);
/// a watch-only (`linked == false`) death, or any normal death, is
/// observed and the actor continues. Chosen by writing
/// `Watching<OtpPropagation>` — never inherited silently (card #280).
pub struct OtpPropagation;

impl<A: Actor> WatchPolicy<A> for OtpPropagation {
    async fn on_link_died(
        _actor: &mut A,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, A::Error> {
        Ok(if linked && !reason.is_normal() {
            ControlFlow::Break(ActorStopReason::LinkDied {
                id,
                reason: Box::new(reason),
            })
        } else {
            ControlFlow::Continue(())
        })
    }
}

/// The watching capability (ADR-0026 stage 3).
///
/// Plugged as a cap-set field, it makes the actor **link-reactive** — the
/// loop drains its link channel and dispatches deaths to `WP`. Zero
/// runtime state (the watchers set stays loop-owned): the policy rides
/// the type, strategy-as-type (ADR-0026 constraint 5).
pub struct Watching<WP> {
    policy: PhantomData<WP>,
}

impl<WP> Watching<WP> {
    /// Builds the (stateless) watching capability.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            policy: PhantomData,
        }
    }
}

impl<WP> Default for Watching<WP> {
    fn default() -> Self {
        Self::new()
    }
}

/// A restart-set strategy named as a TYPE — the [`Supervising`] plug.
///
/// There is deliberately no default marker: bounded supervision names its
/// strategy by construction (the shipped `OneForOne` trait default is
/// dropped, card #280).
pub trait Strategy: Send + 'static {
    /// The runtime strategy this marker names.
    const STRATEGY: SupervisionStrategy;
}

/// A failed child is rebuilt alone; siblings never observe it.
pub struct OneForOne;

/// A failed child restarts itself and every YOUNGER sibling (ADR-0014).
pub struct RestForOne;

/// A failed child restarts the whole set (ADR-0014).
pub struct OneForAll;

impl Strategy for OneForOne {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::OneForOne;
}

impl Strategy for RestForOne {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::RestForOne;
}

impl Strategy for OneForAll {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::OneForAll;
}

/// The supervising capability (ADR-0026 stage 3).
///
/// Plugged as a cap-set field, it runs the actor on the three-arm
/// supervised loop — children are registered via the handle's `supervise`
/// verb, rebuilt under their per-child
/// [`RestartConfig`](crate::restart::RestartConfig) (path unchanged) and
/// this set-level strategy. Requires [`Watching`] in the same set
/// (compile-time law — [`HasSupervising`] bounds [`HasWatching`]). Zero
/// runtime state: the strategy rides the type.
pub struct Supervising<SS: Strategy> {
    strategy: PhantomData<SS>,
}

impl<SS: Strategy> Supervising<SS> {
    /// Builds the (stateless) supervising capability.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            strategy: PhantomData,
        }
    }
}

impl<SS: Strategy> Default for Supervising<SS> {
    fn default() -> Self {
        Self::new()
    }
}

/// "This cap set watches, with policy [`Policy`](HasWatching::Policy)".
///
/// The loop-participation half of [`Watching`], as [`Replay`] is of
/// [`Stashing`]. Derive-emitted from a `Watching<WP>` field; the
/// associated type (never a free impl parameter) is what keeps the
/// [`Shell`]'s conditional impls coherent (spike-280, no E0207/E0119).
pub trait HasWatching<A: Actor> {
    /// The declared death-reaction policy.
    type Policy: WatchPolicy<A>;
}

/// "This cap set supervises" — the loop-participation half of
/// [`Supervising`].
///
/// The supertrait IS the composition law (ADR-0026 constraint 3):
/// supervising-without-watching is unsatisfiable, so an invalid stack
/// does not compile. Derive-emitted from a `Supervising<SS>` field.
pub trait HasSupervising<A: Actor>: HasWatching<A> {
    /// The declared restart-set strategy.
    type Strat: Strategy;
}

/// Names the run-loop shape for a cap set (ADR-0026 stage 3 — the
/// compile-time loop selection, spike-280).
///
/// Derive-emitted alongside [`Replay`]: a `Supervising` field selects
/// [`SupervisedRun`], else a `Watching` field selects [`LinkedRun`], else
/// [`PlainRun`]; `()` selects [`PlainRun`] via the core impl below.
///
/// The associated type is deliberately UNBOUNDED here: the "this shape is
/// actually runnable for this actor" obligation
/// (`Runner: RunKind<A>`) sits on the ONE [`spawn`], discharging at the
/// concrete actor — which is what lets the derive-emitted impl stay
/// generic over `A`.
pub trait SelectRunner<A: Actor> {
    /// The selected loop-shape marker.
    type Runner;
}

impl<A: Actor> SelectRunner<A> for () {
    type Runner = PlainRun;
}

/// Loop-shape marker: the one-arm plain message loop.
pub struct PlainRun;

/// Loop-shape marker: the two-arm linked loop (mailbox + link channel).
pub struct LinkedRun;

/// Loop-shape marker: the three-arm supervised loop (mailbox + link
/// channel + restart-backoff queue).
pub struct SupervisedRun;

/// The one typed window a handler sees.
///
/// Capability access + the actor's own handle. Its reachable surface is
/// exactly what [`Actor::Caps`] declares — there is deliberately NO
/// runtime-checked accessor (ADR-0026 constraint 1).
pub struct Ctx<'a, A: Actor> {
    caps: &'a mut A::Caps,
    self_ref: &'a Handle<A>,
}

impl<A: Actor> Ctx<'_, A> {
    /// Reaches capability `C`. Compile-gated: exists only when the cap
    /// set provides `C`.
    ///
    /// ```compile_fail
    /// use bombay::caps::{Actor, Ctx};
    /// use bombay::actor::Flow;
    ///
    /// struct NoCap;
    /// #[derive(bombay_macros::Msg)]
    /// enum M { Ping }
    /// impl bombay::mailbox::Mailboxed for NoCap { type Msg = M; }
    ///
    /// impl Actor for NoCap {
    ///     type Msg = M;
    ///     type Args = ();
    ///     type Error = core::convert::Infallible;
    ///     type Caps = ();
    ///     async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Self::Error> { Ok(NoCap) }
    ///     async fn handle(&mut self, _: M, mut cx: Ctx<'_, Self>) -> Result<Flow, Self::Error> {
    ///         let _ = cx.cap::<u32>(); // COMPILE ERROR: `()` provides nothing
    ///         Ok(Flow::Continue)
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn cap<C>(&mut self) -> &mut C
    where
        A::Caps: Provide<C>,
    {
        self.caps.provide()
    }

    /// The actor's own handle (tell/ask/timers on the stage-1 seam type).
    #[must_use]
    pub const fn self_ref(&self) -> &Handle<A> {
        self.self_ref
    }
}

/// The stage-1 seam: a `caps` actor's runtime handle is an
/// [`ActorRef`] over its [`Shell`]. The alias collapses when the loop
/// drives [`Actor`] natively in a later ADR-0026 stage.
pub type Handle<A> = ActorRef<Shell<A>>;

/// Internal adapter wrapping a [`caps::Actor`](Actor) into the shipped
/// runtime trait.
///
/// Holds user state + the built capability set; inherits the loop's
/// poisoning, drain-window, and stop semantics unchanged. Public only
/// because [`Handle`] names it; its fields are not.
pub struct Shell<A: Actor> {
    user: A,
    caps: A::Caps,
}

impl<A: Actor> Mailboxed for Shell<A> {
    type Msg = A::Msg;
}

impl<A: Actor> crate::actor::Actor for Shell<A> {
    type Args = A::Args;
    type Error = A::Error;

    fn name() -> &'static str {
        A::name()
    }

    async fn on_start(args: A::Args, actor_ref: ActorRef<Self>) -> Result<Self, A::Error> {
        let mut caps = A::Caps::build(&args);
        let user = A::init(
            args,
            Ctx {
                caps: &mut caps,
                self_ref: &actor_ref,
            },
        )
        .await?;
        // An init-time `Phased::goto` commits here — before any message —
        // rather than dangling until the first handled step.
        caps.commit();
        Ok(Self { user, caps })
    }

    /// One delivered message, then in-step replay (ADR-0022): run the
    /// [`step`](Shell::step) (admission → user handler → transition
    /// commit), then drain the cap set's [`Replay`] queue in the same step
    /// — ahead of the whole mailbox backlog, in stash-arrival order, under
    /// this step's strong `actor_ref` (no upgrade, no drain-window
    /// hazard). Replayed messages re-enter admission, so a [`Phased`] gate
    /// re-classifies them in the CURRENT phase (a re-deferred message goes
    /// to `held`, never back into the draining batch — the snapshot
    /// bound). A replayed handler's `Err`/panic/`Flow::Stop` routes
    /// exactly as a delivered message's would; `Flow::Stop` abandons the
    /// rest of the batch. For a plain actor (`Caps = ()`) admission is a
    /// pass-through and the drain loop never enters.
    async fn handle(&mut self, msg: A::Msg, actor_ref: ActorRef<Self>) -> Result<Flow, A::Error> {
        if self.step(msg, &actor_ref).await? == Flow::Stop {
            return Ok(Flow::Stop);
        }
        while let Some(m) = self.caps.next_replay() {
            if self.step(m, &actor_ref).await? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }

    /// The loop's deadline arm reads the cap set's declarative slot — the
    /// runtime bridge of the ADR-0025 plane onto the [`Deadlined`]
    /// capability (stage 4). A plain set (`Caps = ()`) reports `None` and
    /// the arm stays disabled.
    fn next_deadline(&self) -> Option<Instant> {
        self.caps.next_deadline(&self.user)
    }

    /// Expiry rides the cap set's hook, then the same in-step replay drain
    /// as [`handle`](Self::handle) — a phase timeout may release a stash
    /// batch that must replay ahead of the backlog, re-gated. The drain
    /// needs a strong ref for the replayed handlers' `Ctx`; a deadline
    /// fire has no message to mint one from, so in the drain window
    /// (`upgrade` fails) the released batch waits for the next delivered
    /// step — and dies with the incarnation if none comes (ADR-0022 D6).
    async fn on_deadline(&mut self, actor_ref: WeakActorRef<Self>) -> Result<Flow, A::Error> {
        if self
            .caps
            .on_deadline(&mut self.user, actor_ref.clone())
            .await?
            == Flow::Stop
        {
            return Ok(Flow::Stop);
        }
        let Some(strong) = actor_ref.upgrade() else {
            return Ok(Flow::Continue);
        };
        while let Some(m) = self.caps.next_replay() {
            if self.step(m, &strong).await? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }

    async fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> ActorStopReason {
        A::on_panic(&mut self.user, actor_ref, err).await
    }

    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), A::Error> {
        A::on_stop(&mut self.user, actor_ref, reason).await
    }
}

impl<A: Actor> Shell<A> {
    /// One handler step under admission (stage 4): the cap set classifies
    /// the message first — a [`Phased`] gate defers/ignores/sheds without
    /// the handler ever seeing it — then a delivered message runs the user
    /// handler, and a pending [`Phased::goto`] commits ONLY after its `Ok`
    /// (D3: an `Err`/panic never observes a half-switched phase).
    async fn step(&mut self, msg: A::Msg, actor_ref: &ActorRef<Self>) -> Result<Flow, A::Error> {
        match self.caps.admit(&mut self.user, msg).await? {
            Admitted::Absorbed(flow) => Ok(flow),
            Admitted::Deliver(m) => {
                let flow = A::handle(
                    &mut self.user,
                    m,
                    Ctx {
                        caps: &mut self.caps,
                        self_ref: actor_ref,
                    },
                )
                .await?;
                self.caps.commit();
                Ok(flow)
            }
        }
    }
}

impl<A: Actor> sealed::Sealed for Shell<A> {}

/// The stage-3 analogue of stage 2's derive-emitted [`Replay`]: the
/// `Shell` is link-reactive exactly when the cap set declares
/// [`Watching`], and the reaction IS the declared policy, reached through
/// the [`HasWatching::Policy`] associated type with `&mut user` access —
/// no free policy parameter anywhere (spike-280, O2).
impl<A: Actor> LinkReact for Shell<A>
where
    A::Caps: HasWatching<A>,
{
    fn on_link_died(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, A::Error>> + Send {
        <<A::Caps as HasWatching<A>>::Policy as WatchPolicy<A>>::on_link_died(
            &mut self.user,
            id,
            reason,
            linked,
        )
    }
}

impl<A: Actor> SupervisedReact for Shell<A>
where
    A::Caps: HasSupervising<A>,
{
    fn strategy() -> SupervisionStrategy {
        <A::Caps as HasSupervising<A>>::Strat::STRATEGY
    }
}

/// Runs a selected loop shape (ADR-0026 stage 3, spike-280).
///
/// Each marker spawns the [`Shell`] onto its [`PreparedActor`] floor path.
/// The obligation `SelectRunner::Runner: RunKind<A>` is discharged at the
/// one [`spawn`] — monomorphized; the "branch" is trait resolution, not
/// code.
pub trait RunKind<A: Actor> {
    /// Spawns the actor onto this loop shape.
    fn spawn_with(config: SpawnConfig, args: A::Args) -> Handle<A>;
}

impl<A: Actor> RunKind<A> for PlainRun {
    fn spawn_with(config: SpawnConfig, args: A::Args) -> Handle<A> {
        let prepared = PreparedActor::<Shell<A>>::new(config);
        let handle = prepared.actor_ref().clone();
        let _join = prepared.spawn(args);
        handle
    }
}

impl<A: Actor> RunKind<A> for LinkedRun
where
    A::Caps: HasWatching<A>,
{
    fn spawn_with(config: SpawnConfig, args: A::Args) -> Handle<A> {
        let (prepared, link_rx) = PreparedActor::<Shell<A>>::new_linked(config);
        let handle = prepared.actor_ref().clone();
        let _join = prepared.spawn_linked_task(args, link_rx);
        handle
    }
}

impl<A: Actor> RunKind<A> for SupervisedRun
where
    A::Caps: HasSupervising<A>,
{
    fn spawn_with(config: SpawnConfig, args: A::Args) -> Handle<A> {
        let (prepared, link_rx) = PreparedActor::<Shell<A>>::new_linked(config);
        let handle = prepared.actor_ref().clone();
        let _join = prepared.spawn_supervised_task(args, link_rx);
        handle
    }
}

/// Spawns a `caps` actor with the default [`SpawnConfig`] — the ONE
/// ergonomic entry.
///
/// The loop shape is selected from [`Actor::Caps`] at compile time
/// (monomorphized, no runtime branch): plain sets run the one-arm loop,
/// [`Watching`] sets the linked loop, [`Supervising`] sets the supervised
/// loop (ADR-0026 stage 3).
///
/// A `Supervising` set without `Watching` does not compile — the
/// composition law rides the [`HasSupervising`] supertrait (and the
/// derive rejects it with a readable error):
///
/// ```compile_fail
/// #[derive(bombay_macros::Provide)]
/// struct RogueCaps {
///     supervising: bombay::caps::Supervising<bombay::caps::OneForOne>,
/// }
/// ```
#[must_use]
pub fn spawn<A: Actor>(args: A::Args) -> Handle<A>
where
    <A::Caps as SelectRunner<A>>::Runner: RunKind<A>,
{
    spawn_with(SpawnConfig::default(), args)
}

/// Spawns with an explicit [`SpawnConfig`] (mailbox capacity + stop
/// grace); loop shape selected exactly as [`spawn`].
#[must_use]
pub fn spawn_with<A: Actor>(config: SpawnConfig, args: A::Args) -> Handle<A>
where
    <A::Caps as SelectRunner<A>>::Runner: RunKind<A>,
{
    <<A::Caps as SelectRunner<A>>::Runner as RunKind<A>>::spawn_with(config, args)
}

#[cfg(test)]
mod tests {
    use core::any::type_name;

    use super::{Actor, CapSet, Ctx, Flow, Shell};
    use crate::mailbox::Mailboxed;

    struct Nameless;

    #[derive(Debug)]
    enum NoMsg {}
    impl crate::message::Msg for NoMsg {}

    impl Mailboxed for Nameless {
        type Msg = NoMsg;
    }

    impl Actor for Nameless {
        type Msg = NoMsg;
        type Args = ();
        type Error = core::convert::Infallible;
        type Caps = ();

        async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }

        async fn handle(&mut self, msg: NoMsg, _: Ctx<'_, Self>) -> Result<Flow, Self::Error> {
            match msg {}
        }
    }

    /// Kills the `Actor::name` default's ""/"xyzzy" mutants: the default
    /// is exactly the type name.
    #[test]
    fn caps_actor_name_defaults_to_type_name() {
        assert_eq!(<Nameless as Actor>::name(), type_name::<Nameless>());
    }

    /// Kills the `Shell::name` mutants: the wrapper reports the USER
    /// type's name, not its own.
    #[test]
    fn shell_forwards_the_user_type_name() {
        assert_eq!(
            <Shell<Nameless> as crate::actor::Actor>::name(),
            type_name::<Nameless>()
        );
    }

    /// The unit cap set builds from any args — the plain-actor floor.
    #[test]
    fn unit_capset_builds() {
        <() as CapSet<Nameless>>::build(&());
    }

    /// Stage-3 (card #280): the Watching/Supervising capabilities and the
    /// compile-time loop selection — behavior tests for the pieces the
    /// spike (spike-280) proved only compile.
    mod watching_supervising {
        use core::any::type_name;
        use core::convert::Infallible;
        use core::ops::ControlFlow;

        use super::super::{
            Actor, CapSet, Ctx, Flow, HasSupervising, HasWatching, OneForAll, OneForOne,
            OtpPropagation, PlainRun, Replay, RestForOne, SelectRunner, Shell, Strategy,
            Supervising, WatchPolicy, Watching,
        };
        use crate::{
            actor::{LinkReact, SupervisedReact},
            error::ActorStopReason,
            mailbox::{ActorId, Mailboxed},
            message::Msg,
            restart::SupervisionStrategy,
        };

        /// A recording watcher: its policy pushes every notice into the
        /// actor's own state — the shape the ported equivalence suites'
        /// recording hooks need (`&mut A` access from a policy).
        struct Rec {
            seen: Vec<(ActorId, bool)>,
        }

        #[derive(Debug)]
        struct RecMsg;
        impl Msg for RecMsg {}
        impl Mailboxed for Rec {
            type Msg = RecMsg;
        }

        struct RecPolicy;

        impl WatchPolicy<Rec> for RecPolicy {
            async fn on_link_died(
                actor: &mut Rec,
                id: ActorId,
                _reason: ActorStopReason,
                linked: bool,
            ) -> Result<ControlFlow<ActorStopReason>, Infallible> {
                actor.seen.push((id, linked));
                Ok(ControlFlow::Continue(()))
            }
        }

        /// Hand-written cap set (what the derive emits, spelled out — the
        /// derive's own emission is unit-tested in `bombay_macros`).
        struct RecCaps {
            watching: Watching<RecPolicy>,
        }

        impl CapSet<Rec> for RecCaps {
            fn build((): &()) -> Self {
                Self {
                    watching: Watching::new(),
                }
            }
        }
        impl super::super::Provide<Watching<RecPolicy>> for RecCaps {
            fn provide(&mut self) -> &mut Watching<RecPolicy> {
                &mut self.watching
            }
        }
        impl<M> Replay<M> for RecCaps {
            fn next_replay(&mut self) -> Option<M> {
                None
            }
        }
        impl<A: Actor> super::super::Admission<A> for RecCaps {
            async fn admit(
                &mut self,
                _: &mut A,
                msg: A::Msg,
            ) -> Result<super::super::Admitted<A::Msg>, A::Error> {
                Ok(super::super::Admitted::Deliver(msg))
            }
            fn commit(&mut self) {}
        }
        impl<A: Actor> super::super::DeadlineHook<A> for RecCaps {
            fn next_deadline(&self, _: &A) -> Option<tokio::time::Instant> {
                None
            }
            async fn on_deadline(
                &mut self,
                _: &mut A,
                _: crate::actor::WeakActorRef<super::super::Shell<A>>,
            ) -> Result<super::super::Flow, A::Error> {
                Ok(super::super::Flow::Continue)
            }
        }
        impl<A: Actor> HasWatching<A> for RecCaps
        where
            RecPolicy: WatchPolicy<A>,
        {
            type Policy = RecPolicy;
        }
        impl<A: Actor> SelectRunner<A> for RecCaps {
            type Runner = super::super::LinkedRun;
        }

        impl Actor for Rec {
            type Msg = RecMsg;
            type Args = ();
            type Error = Infallible;
            type Caps = RecCaps;

            async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
                Ok(Self { seen: Vec::new() })
            }

            async fn handle(&mut self, _: RecMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
                Ok(Flow::Continue)
            }
        }

        /// A supervising actor whose cap set names `OneForAll` — the
        /// strategy-read probe.
        struct Sup;

        impl Mailboxed for Sup {
            type Msg = RecMsg;
        }

        struct SupCaps {
            watching: Watching<OtpPropagation>,
            supervising: Supervising<OneForAll>,
        }

        impl CapSet<Sup> for SupCaps {
            fn build((): &()) -> Self {
                Self {
                    watching: Watching::new(),
                    supervising: Supervising::new(),
                }
            }
        }
        impl super::super::Provide<Watching<OtpPropagation>> for SupCaps {
            fn provide(&mut self) -> &mut Watching<OtpPropagation> {
                &mut self.watching
            }
        }
        impl super::super::Provide<Supervising<OneForAll>> for SupCaps {
            fn provide(&mut self) -> &mut Supervising<OneForAll> {
                &mut self.supervising
            }
        }
        impl<M> Replay<M> for SupCaps {
            fn next_replay(&mut self) -> Option<M> {
                None
            }
        }
        impl<A: Actor> super::super::Admission<A> for SupCaps {
            async fn admit(
                &mut self,
                _: &mut A,
                msg: A::Msg,
            ) -> Result<super::super::Admitted<A::Msg>, A::Error> {
                Ok(super::super::Admitted::Deliver(msg))
            }
            fn commit(&mut self) {}
        }
        impl<A: Actor> super::super::DeadlineHook<A> for SupCaps {
            fn next_deadline(&self, _: &A) -> Option<tokio::time::Instant> {
                None
            }
            async fn on_deadline(
                &mut self,
                _: &mut A,
                _: crate::actor::WeakActorRef<super::super::Shell<A>>,
            ) -> Result<super::super::Flow, A::Error> {
                Ok(super::super::Flow::Continue)
            }
        }
        impl<A: Actor> HasWatching<A> for SupCaps
        where
            OtpPropagation: WatchPolicy<A>,
        {
            type Policy = OtpPropagation;
        }
        impl<A: Actor> HasSupervising<A> for SupCaps
        where
            OtpPropagation: WatchPolicy<A>,
        {
            type Strat = OneForAll;
        }
        impl<A: Actor> SelectRunner<A> for SupCaps {
            type Runner = super::super::SupervisedRun;
        }

        impl Actor for Sup {
            type Msg = RecMsg;
            type Args = ();
            type Error = Infallible;
            type Caps = SupCaps;

            async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
                Ok(Self)
            }

            async fn handle(&mut self, _: RecMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
                Ok(Flow::Continue)
            }
        }

        /// The NAMED OTP policy carries the exact semantics of the removed
        /// `Watch::on_link_died` default: a **linked abnormal** death
        /// propagates as `Break(LinkDied)` carrying the original reason;
        /// a watch-only (`linked == false`) abnormal death and a linked
        /// normal death are observed and continue. Port of the removed
        /// `default_hook_breaks_on_linked_abnormal_and_continues_otherwise`.
        #[tokio::test]
        async fn otp_propagation_breaks_on_linked_abnormal_and_continues_otherwise() {
            let mut sup = Sup;
            let id = ActorId::from_raw_for_test(1);

            let out = <OtpPropagation as WatchPolicy<Sup>>::on_link_died(
                &mut sup,
                id,
                ActorStopReason::Killed,
                true,
            )
            .await
            .expect("infallible policy");
            match out {
                ControlFlow::Break(ActorStopReason::LinkDied { id: died, reason }) => {
                    assert_eq!(died, id, "the notice's id rides the stop reason");
                    assert!(
                        matches!(*reason, ActorStopReason::Killed),
                        "the ORIGINAL reason is preserved, got {reason:?}",
                    );
                }
                other => panic!("linked + abnormal must propagate, got {other:?}"),
            }

            let out = <OtpPropagation as WatchPolicy<Sup>>::on_link_died(
                &mut sup,
                id,
                ActorStopReason::Killed,
                false,
            )
            .await
            .expect("infallible policy");
            assert!(
                matches!(out, ControlFlow::Continue(())),
                "watch (linked=false) + abnormal is notify-only, got {out:?}",
            );

            let out = <OtpPropagation as WatchPolicy<Sup>>::on_link_died(
                &mut sup,
                id,
                ActorStopReason::Normal,
                true,
            )
            .await
            .expect("infallible policy");
            assert!(
                matches!(out, ControlFlow::Continue(())),
                "linked + normal does not propagate, got {out:?}",
            );
        }

        /// `Shell`'s sealed `LinkReact` impl dispatches the loop's death
        /// notice INTO the cap set's declared policy with `&mut user`
        /// access — the stage-3 analogue of the stage-2 replay-drain test.
        #[tokio::test]
        async fn shell_dispatches_on_link_died_to_the_declared_policy() {
            let mut shell = Shell {
                user: Rec { seen: Vec::new() },
                caps: RecCaps::build(&()),
            };
            let id = ActorId::from_raw_for_test(7);

            let out = <Shell<Rec> as LinkReact>::on_link_died(
                &mut shell,
                id,
                ActorStopReason::Killed,
                true,
            )
            .await
            .expect("recording policy is infallible");

            assert!(
                matches!(out, ControlFlow::Continue(())),
                "the recording policy continues, got {out:?}",
            );
            assert_eq!(
                shell.user.seen,
                vec![(id, true)],
                "the policy observed the notice through &mut user state",
            );
        }

        /// `Shell`'s sealed `SupervisedReact` impl reads the strategy off
        /// the `Supervising` cap's TYPE — the re-pointed `kind.rs`
        /// strategy read.
        #[test]
        fn shell_strategy_reads_the_supervising_caps_type() {
            assert_eq!(
                <Shell<Sup> as SupervisedReact>::strategy(),
                SupervisionStrategy::OneForAll,
                "the strategy is the cap set's named type, not a default",
            );
        }

        /// The three strategy markers name exactly their runtime strategy —
        /// kills value-swap mutants on the `STRATEGY` consts.
        #[test]
        fn strategy_markers_name_their_runtime_strategy() {
            assert_eq!(
                <OneForOne as Strategy>::STRATEGY,
                SupervisionStrategy::OneForOne
            );
            assert_eq!(
                <RestForOne as Strategy>::STRATEGY,
                SupervisionStrategy::RestForOne
            );
            assert_eq!(
                <OneForAll as Strategy>::STRATEGY,
                SupervisionStrategy::OneForAll
            );
        }

        /// The plain-actor floor: `()` selects the one-arm loop shape.
        #[test]
        fn unit_capset_selects_the_plain_runner() {
            assert_eq!(
                type_name::<<() as SelectRunner<super::Nameless>>::Runner>(),
                type_name::<PlainRun>(),
                "a capability-less set runs the plain message loop",
            );
        }
    }

    /// Stage-4 (card #281): the `Deadlined` capability — the ADR-0025
    /// plane's user seat — and the `DeadlineHook` participation half the
    /// `Shell` bridges to the loop's deadline arm.
    mod deadlined {
        use core::convert::Infallible;
        use core::time::Duration;

        use tokio::time::Instant;

        use futures::stream::AbortHandle;
        use tokio_util::sync::CancellationToken;

        use super::super::{
            Actor, ByState, CapSet, Ctx, DeadlineHook, DeadlinePolicy, Deadlined, Flow, Never,
            Replay, SelectRunner, Shell, Step,
        };
        use crate::{
            actor::{Actor as RuntimeActor, ActorRef, WeakActorRef},
            mailbox::{ActorId, Capacity, Mailbox, Mailboxed},
            message::Msg,
        };

        /// A drain-window weak ref (its strong parent is dropped at
        /// return): exactly the ref shape the hook must tolerate.
        fn dead_weak<A: crate::actor::Actor>() -> WeakActorRef<A> {
            let cap = Capacity::try_from(1usize).expect("valid test capacity");
            let (tx, _rx) = Mailbox::<A>::bounded(cap, ActorId::from_raw_for_test(9));
            let (abort, _reg) = AbortHandle::new_pair();
            let strong = ActorRef::new(
                ActorId::from_raw_for_test(9),
                tx,
                CancellationToken::new(),
                abort,
                None,
            );
            strong.downgrade()
        }

        /// An actor whose deadline is a pure function of its own state —
        /// the quinn `poll_timeout` shape the policy reads.
        struct Idler {
            due: Option<Instant>,
            fires: u32,
        }

        #[derive(Debug)]
        struct IdleMsg;
        impl Msg for IdleMsg {}
        impl Mailboxed for Idler {
            type Msg = IdleMsg;
        }

        struct IdlePolicy;

        impl DeadlinePolicy<ByState<Idler>> for IdlePolicy {
            fn build((): &()) -> Self {
                Self
            }
            fn next_deadline(&self, actor: &Idler, (): ()) -> Option<Instant> {
                actor.due
            }
            async fn on_deadline(
                &self,
                actor: &mut Idler,
                (): (),
                _: WeakActorRef<Shell<Idler>>,
            ) -> Result<Step<Never>, Infallible> {
                actor.fires = actor.fires.saturating_add(1);
                actor.due = None;
                Ok(Step::Stop)
            }
        }

        /// Hand-written cap set (what the derive emits, spelled out).
        struct IdleCaps {
            deadlined: Deadlined<IdlePolicy>,
        }

        impl CapSet<Idler> for IdleCaps {
            fn build(args: &()) -> Self {
                Self {
                    deadlined: Deadlined::build(args),
                }
            }
        }
        impl super::super::Provide<Deadlined<IdlePolicy>> for IdleCaps {
            fn provide(&mut self) -> &mut Deadlined<IdlePolicy> {
                &mut self.deadlined
            }
        }
        impl<M> Replay<M> for IdleCaps {
            fn next_replay(&mut self) -> Option<M> {
                None
            }
        }
        impl<A: Actor> super::super::Admission<A> for IdleCaps {
            async fn admit(
                &mut self,
                _: &mut A,
                msg: A::Msg,
            ) -> Result<super::super::Admitted<A::Msg>, A::Error> {
                Ok(super::super::Admitted::Deliver(msg))
            }
            fn commit(&mut self) {}
        }
        impl<A: Actor> DeadlineHook<A> for IdleCaps
        where
            IdlePolicy: DeadlinePolicy<ByState<A>>,
        {
            fn next_deadline(&self, actor: &A) -> Option<Instant> {
                DeadlineHook::next_deadline(&self.deadlined, actor)
            }
            async fn on_deadline(
                &mut self,
                actor: &mut A,
                actor_ref: WeakActorRef<Shell<A>>,
            ) -> Result<Flow, A::Error> {
                DeadlineHook::on_deadline(&mut self.deadlined, actor, actor_ref).await
            }
        }
        impl<A: Actor> SelectRunner<A> for IdleCaps {
            type Runner = super::super::PlainRun;
        }

        impl Actor for Idler {
            type Msg = IdleMsg;
            type Args = ();
            type Error = Infallible;
            type Caps = IdleCaps;

            async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
                Ok(Self {
                    due: None,
                    fires: 0,
                })
            }

            async fn handle(&mut self, _: IdleMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
                Ok(Flow::Continue)
            }
        }

        /// The `()` floor: a capability-less set declares no deadline and
        /// its expiry hook is inert — the arm stays disabled for plain
        /// actors.
        #[tokio::test]
        async fn unit_caps_declare_no_deadline() {
            let mut nameless = super::Nameless;
            assert_eq!(
                <() as DeadlineHook<super::Nameless>>::next_deadline(&(), &nameless),
                None,
            );
            let out = <() as DeadlineHook<super::Nameless>>::on_deadline(
                &mut (),
                &mut nameless,
                dead_weak(),
            )
            .await
            .expect("the unit hook is infallible");
            assert_eq!(out, Flow::Continue, "the unit hook keeps running");
        }

        /// `Shell`'s runtime `next_deadline` forwards to the cap set's
        /// declared policy — a pure read of user state through the bridge.
        #[tokio::test]
        async fn shell_forwards_next_deadline_to_the_declared_policy() {
            let due = Instant::now() + Duration::from_secs(5);
            let mut shell = Shell {
                user: Idler {
                    due: Some(due),
                    fires: 0,
                },
                caps: IdleCaps::build(&()),
            };
            assert_eq!(
                RuntimeActor::next_deadline(&shell),
                Some(due),
                "the bridge reads the policy's pure function of user state",
            );
            shell.user.due = None;
            assert_eq!(
                RuntimeActor::next_deadline(&shell),
                None,
                "no declared deadline disables the arm",
            );
        }

        /// `Shell`'s runtime `on_deadline` dispatches expiry INTO the
        /// declared policy with `&mut user` access, and the policy's
        /// `Flow` decision rides back out — the stage-4 analogue of the
        /// stage-3 link-dispatch test.
        #[tokio::test]
        async fn shell_dispatches_on_deadline_to_the_declared_policy() {
            let mut shell = Shell {
                user: Idler {
                    due: Some(Instant::now()),
                    fires: 0,
                },
                caps: IdleCaps::build(&()),
            };
            let flow = RuntimeActor::on_deadline(&mut shell, dead_weak())
                .await
                .expect("the recording policy is infallible");
            assert_eq!(flow, Flow::Stop, "the policy's Flow decision rides out");
            assert_eq!(
                shell.user.fires, 1,
                "the policy observed expiry through &mut user state",
            );
        }
    }

    /// Stage-4 (card #281): the `Phased` machine's unit-level laws that
    /// need `Shell` internals — commit-after-Ok (D3) and the D6 default
    /// overflow verdict. Behavior over the real loop lives in
    /// `tests/caps_phased.rs` and the equivalence oracle.
    mod phased {
        use core::num::NonZeroUsize;

        use futures::stream::AbortHandle;
        use tokio_util::sync::CancellationToken;

        use super::super::{
            Actor, Admission, CapSet, Ctx, Disposition, Flow, Overflow, PhasePolicy, Phased, Shell,
            Stashing,
        };
        use crate::{
            actor::{ActorRef, WeakActorRef},
            mailbox::{ActorId, Capacity, Mailbox, Mailboxed},
            message::Msg,
        };

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Ph {
            A,
            B,
        }

        #[derive(Debug)]
        enum MMsg {
            GotoBThenFail,
            GotoBThenOk,
        }
        impl Msg for MMsg {}

        struct M;
        impl Mailboxed for M {
            type Msg = MMsg;
        }

        struct MPolicy;
        impl PhasePolicy for MPolicy {
            type Actor = M;
            type Phase = Ph;
            type Deferral = super::super::NoDefer;
            type Timeout = super::super::NoTimeout;
            fn initial((): &()) -> Ph {
                Ph::A
            }
            fn gate(_: Ph, _: &MMsg) -> Disposition {
                Disposition::Deliver
            }
        }

        struct MCaps {
            phased: Phased<MPolicy>,
        }
        impl CapSet<M> for MCaps {
            fn build(args: &()) -> Self {
                Self {
                    phased: Phased::build(args),
                }
            }
        }
        impl super::super::Provide<Phased<MPolicy>> for MCaps {
            fn provide(&mut self) -> &mut Phased<MPolicy> {
                &mut self.phased
            }
        }
        impl super::super::Replay<MMsg> for MCaps {
            fn next_replay(&mut self) -> Option<MMsg> {
                super::super::Replay::next_replay(&mut self.phased)
            }
        }
        impl super::super::DeadlineHook<M> for MCaps {
            fn next_deadline(&self, actor: &M) -> Option<tokio::time::Instant> {
                super::super::DeadlineHook::next_deadline(&self.phased, actor)
            }
            async fn on_deadline(
                &mut self,
                actor: &mut M,
                actor_ref: WeakActorRef<Shell<M>>,
            ) -> Result<Flow, &'static str> {
                super::super::DeadlineHook::on_deadline(&mut self.phased, actor, actor_ref).await
            }
        }
        impl Admission<M> for MCaps {
            async fn admit(
                &mut self,
                actor: &mut M,
                msg: MMsg,
            ) -> Result<super::super::Admitted<MMsg>, &'static str> {
                Admission::admit(&mut self.phased, actor, msg).await
            }
            fn commit(&mut self) {
                Admission::commit(&mut self.phased);
            }
        }
        impl<A: Actor> super::super::SelectRunner<A> for MCaps {
            type Runner = super::super::PlainRun;
        }

        impl Actor for M {
            type Msg = MMsg;
            type Args = ();
            type Error = &'static str;
            type Caps = MCaps;

            async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, &'static str> {
                Ok(Self)
            }

            async fn handle(
                &mut self,
                msg: MMsg,
                mut cx: Ctx<'_, Self>,
            ) -> Result<Flow, &'static str> {
                cx.cap::<Phased<MPolicy>>().goto(Ph::B);
                match msg {
                    MMsg::GotoBThenFail => Err("bang"),
                    MMsg::GotoBThenOk => Ok(Flow::Continue),
                }
            }
        }

        /// A strong ref plus the receiver that keeps its channel open for
        /// the test's life (the `actor_ref.rs` `build_ref_with_rx` shape).
        fn strong_ref() -> (
            ActorRef<Shell<M>>,
            crate::mailbox::MailboxReceiver<Shell<M>>,
        ) {
            let cap = Capacity::try_from(1usize).expect("valid test capacity");
            let (tx, rx) = Mailbox::<Shell<M>>::bounded(cap, ActorId::from_raw_for_test(11));
            let (abort, _reg) = AbortHandle::new_pair();
            let actor_ref = ActorRef::new(
                ActorId::from_raw_for_test(11),
                tx,
                CancellationToken::new(),
                abort,
                None,
            );
            (actor_ref, rx)
        }

        /// D3 commit-after-Ok, on the `Shell::step` code order itself: a
        /// handler that requests a transition and then FAILS never
        /// commits it (an unwind takes the same post-await path), while
        /// the same request followed by `Ok` does.
        #[tokio::test]
        async fn a_failing_handler_never_commits_its_pending_goto() {
            let mut shell = Shell {
                user: M,
                caps: MCaps::build(&()),
            };
            let (actor_ref, _rx) = strong_ref();

            let out =
                crate::actor::Actor::handle(&mut shell, MMsg::GotoBThenFail, actor_ref.clone())
                    .await;
            assert_eq!(out, Err("bang"), "the controlled crash rides out");
            assert_eq!(
                shell.caps.phased.phase(),
                Ph::A,
                "a failed handler's goto is NEVER committed (D3)",
            );

            let out = crate::actor::Actor::handle(&mut shell, MMsg::GotoBThenOk, actor_ref).await;
            assert_eq!(out, Ok(Flow::Continue));
            assert_eq!(
                shell.caps.phased.phase(),
                Ph::B,
                "the same goto commits after Ok",
            );
        }

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
                matches!(out, super::super::Admitted::Deliver(MMsg::GotoBThenOk)),
                "the floor delivers everything",
            );
            <() as Admission<M>>::commit(&mut unit);
        }
    }

    mod stashing {
        use core::num::NonZeroUsize;

        use super::super::{Replay, Stashing};
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
}
