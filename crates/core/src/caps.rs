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
    /// a [`Stashing`] field drains it — and [`SelectRunner<Self>`](SelectRunner)
    /// so every cap set names its run-loop shape at compile time (stage 3).
    /// The derive emits both alongside `Provide`, so neither is a separate
    /// thing to forget.
    type Caps: CapSet<Self> + Replay<Self::Msg> + SelectRunner<Self>;

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
        Ok(Self { user, caps })
    }

    /// One delivered message, then in-step replay (ADR-0022): run the user
    /// handler, then drain the cap set's [`Replay`] queue in the same step —
    /// ahead of the whole mailbox backlog, in stash-arrival order, under this
    /// step's strong `actor_ref` (no upgrade, no drain-window hazard). A
    /// replayed handler's `Err`/panic/`Flow::Stop` routes exactly as a
    /// delivered message's would; `Flow::Stop` abandons the rest of the batch.
    /// For a plain actor (`Caps = ()`) the drain loop never enters.
    async fn handle(&mut self, msg: A::Msg, actor_ref: ActorRef<Self>) -> Result<Flow, A::Error> {
        if A::handle(
            &mut self.user,
            msg,
            Ctx {
                caps: &mut self.caps,
                self_ref: &actor_ref,
            },
        )
        .await?
            == Flow::Stop
        {
            return Ok(Flow::Stop);
        }
        while let Some(m) = self.caps.next_replay() {
            if A::handle(
                &mut self.user,
                m,
                Ctx {
                    caps: &mut self.caps,
                    self_ref: &actor_ref,
                },
            )
            .await?
                == Flow::Stop
            {
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
