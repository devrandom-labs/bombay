//! spike-280 — compile-time loop selection from `Caps` (card #280, ADR-0026
//! open risk ii). Stable Rust, zero deps. The compiler is the judge.
//!
//! Obligations (each proven by code in this file or a red-case cfg):
//!
//! - **O1** ONE `spawn`, three loop shapes, selected from `A::Caps` via an
//!   associated marker type (`SelectRunner::Runner`) — fully monomorphized,
//!   no runtime branch anywhere in the selection path.
//! - **O2** The watch reaction is dispatched through `HasWatching::Policy`
//!   (an associated type), so no impl carries an unconstrained type
//!   parameter (the E0207 wall) and `Shell<A>`'s conditional impls stay
//!   coherent (no E0119).
//! - **O3** `Supervising` requires `Watching` **by supertrait**
//!   (`HasSupervising<A>: HasWatching<A>`): an invalid stack (supervising
//!   without watching) fails to compile at the spawn site — red case R1.
//! - **O4** The `watch`/`link`/`supervise` verbs are inherent methods on
//!   `Handle<A>` gated by `HasWatching`/`HasSupervising` bounds: calling
//!   them on a plain-caps handle fails to compile — red case R3.
//! - **O5** Cross-cap composition: one cap-set struct with
//!   `Stashing + Watching + Supervising` — the previously-unrepresentable
//!   deferring supervisor — compiles and spawns onto the supervised shape.
//! - **O6** The lifecycle futures the floor builds remain `Send` with the
//!   policy dispatch inlined (guarded by `assert_send`), and the loop fns
//!   with the NEW bounds (`LinkReact`/`SupervisedReact` replacing
//!   `Watch`/`Supervisor`) typecheck when instantiated at `Shell<A>`.
//!
//! Derive note: everything marked `// [derive-emitted]` is what
//! `#[derive(Provide)]` will emit in the real implementation (stage-2
//! precedent: it already string-matches cap fields to emit `Replay`); here
//! it is hand-written, exactly as spike-278 hand-wrote the `Provide` impls.

use core::future::Future;
use core::marker::PhantomData;
use core::ops::ControlFlow;

// ════════════════════════ runtime stand-ins (shapes mirror bombay) ═══════

/// Mirrors `ActorStopReason` where the spike needs to tell normal from not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Normal,
    LinkDied(u64),
}

/// Mirrors `restart::SupervisionStrategy` (the runtime enum the loop reads).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisionStrategy {
    OneForOne,
    RestForOne,
    OneForAll,
}

/// Mirrors `watch::LinkDied` (the notice the linked/supervised loops drain).
pub struct LinkDied {
    pub id: u64,
    pub reason: StopReason,
    pub linked: bool,
}

/// Mirrors `ActorRef<RtA>` — parameterized by the RUNTIME actor type
/// (`Shell<A>` for caps actors), exactly as `Handle<A> = ActorRef<Shell<A>>`
/// does in stage 1.
pub struct ActorRef<RtA>(PhantomData<RtA>);

impl<RtA> ActorRef<RtA> {
    #[must_use]
    pub const fn stub() -> Self {
        Self(PhantomData)
    }
}

/// Mirrors the shipped runtime trait `actor::Actor` — the trait the loops
/// drive. In the spike `on_start` is sync (asyncness there is not the risk
/// being probed); `handle` is async because the policy-dispatch Send
/// inference rides it.
pub trait RtActor: Sized + Send + 'static {
    type Msg: Send + 'static;
    type Args: Send + 'static;
    type Error: Send + core::fmt::Debug + 'static;

    fn on_start(args: Self::Args) -> Self;

    fn handle(&mut self, msg: Self::Msg) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

// ═══════════════ the sealed internal seams that REPLACE Watch/Supervisor ═

mod sealed {
    /// Seals `LinkReact`/`SupervisedReact`: outside crates can NAME them (they
    /// appear in public floor bounds) but never implement them — they are the
    /// loop's internal dispatch seam, not a user surface. `Shell` is their
    /// only implementor.
    pub trait Sealed {}
}

/// Replaces `trait Watch` at the loops' bound sites (`kind.rs:299/340`):
/// "this runtime actor can react to a link death". Sealed — implemented
/// ONLY by `Shell<A> where A::Caps: HasWatching<A>`, never by users.
pub trait LinkReact: RtActor + sealed::Sealed {
    fn on_link_died(
        &mut self,
        id: u64,
        reason: StopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<StopReason>, Self::Error>> + Send;
}

/// Replaces `trait Supervisor` at the loops' bound sites
/// (`kind.rs:394/418/513`): a link-reactive runtime actor with a restart-set
/// strategy. Sealed, `Shell`-only, like `LinkReact`.
pub trait SupervisedReact: LinkReact {
    fn strategy() -> SupervisionStrategy;
}

// ═══════════════════════════ the three loops (bound shapes mirror kind.rs) ═

/// Mirrors `run_message_loop<A: Actor>` — bound unchanged by stage 3.
pub async fn run_message_loop<RtA: RtActor>(
    state: &mut RtA,
    mailbox: &mut Vec<RtA::Msg>,
) -> StopReason {
    while let Some(msg) = mailbox.pop() {
        if state.handle(msg).await.is_err() {
            return StopReason::Normal;
        }
    }
    StopReason::Normal
}

/// Mirrors `run_linked_message_loop<A: Watch>` with the ONE stage-3 change:
/// the bound is `LinkReact` and the death arm calls the sealed seam
/// (`kind.rs:353` re-pointed). Everything else about the real loop's body is
/// untouched by stage 3.
pub async fn run_linked_message_loop<RtA: LinkReact>(
    state: &mut RtA,
    mailbox: &mut Vec<RtA::Msg>,
    link_rx: &mut Vec<LinkDied>,
) -> StopReason {
    while let Some(n) = link_rx.pop() {
        match state.on_link_died(n.id, n.reason, n.linked).await {
            Ok(ControlFlow::Break(reason)) => return reason,
            Ok(ControlFlow::Continue(())) => {}
            Err(_e) => return StopReason::Normal,
        }
    }
    run_message_loop(state, mailbox).await
}

/// Mirrors `run_supervised_message_loop<A: Supervisor>` with the TWO stage-3
/// changes: bound `SupervisedReact`, and the strategy read (`kind.rs:418`)
/// goes through the sealed seam.
pub async fn run_supervised_message_loop<RtA: SupervisedReact>(
    state: &mut RtA,
    mailbox: &mut Vec<RtA::Msg>,
    link_rx: &mut Vec<LinkDied>,
) -> StopReason {
    let _strategy: SupervisionStrategy = RtA::strategy();
    run_linked_message_loop(state, mailbox, link_rx).await
}

// ═══════════════════════════════════ the caps surface (mirrors caps.rs) ═══

/// Mirrors `caps::Actor` with the stage-3 `Caps` bound: `SelectRunner<Self>`
/// joins `CapSet + Replay`, so every caps actor names its loop shape at
/// compile time (derive-emitted, like `Replay` in stage 2).
pub trait CapsActor: Sized + Send + 'static {
    type Msg: Send + 'static;
    type Args: Send + 'static;
    type Error: Send + core::fmt::Debug + 'static;
    type Caps: CapSet<Self> + Replay<Self::Msg> + SelectRunner<Self>;

    fn init(args: Self::Args) -> Self;

    fn handle(&mut self, msg: Self::Msg) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait CapSet<A: CapsActor>: Send + 'static {
    fn build(args: &A::Args) -> Self;
}

impl<A: CapsActor> CapSet<A> for () {
    fn build(_: &A::Args) -> Self {}
}

/// Mirrors stage-2 `caps::Replay` verbatim.
pub trait Replay<M> {
    fn next_replay(&mut self) -> Option<M>;
}

impl<M> Replay<M> for () {
    fn next_replay(&mut self) -> Option<M> {
        None
    }
}

/// Mirrors stage-2 `caps::Provide` verbatim (the open seam).
pub trait Provide<C> {
    fn provide(&mut self) -> &mut C;
}

// ─────────────────────────────────────────────── the Stashing cap (stage 2) ─

/// Stand-in for the shipped `caps::Stashing<M>` — enough state to make the
/// `Replay` forwarding real rather than vacuous.
pub struct Stashing<M> {
    ready: Vec<M>,
}

impl<M> Stashing<M> {
    #[must_use]
    pub const fn new() -> Self {
        Self { ready: Vec::new() }
    }

    pub(crate) fn pop_ready(&mut self) -> Option<M> {
        self.ready.pop()
    }
}

impl<M> Default for Stashing<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Replay<M> for Stashing<M> {
    fn next_replay(&mut self) -> Option<M> {
        self.pop_ready()
    }
}

// ──────────────────────────────────────────── the Watching cap (stage 3) ───

/// The watch-reaction policy seat — the relocated `Watch::on_link_died`.
/// Parameterized by the actor so a custom policy can record into `&mut A`
/// (what the ported equivalence suites need).
pub trait WatchPolicy<A: CapsActor>: Send + 'static {
    fn on_link_died(
        actor: &mut A,
        id: u64,
        reason: StopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<StopReason>, A::Error>> + Send;
}

/// The NAMED OTP default (card #280: "chosen, not defaulted") — byte-for-byte
/// the semantics of the shipped `Watch::on_link_died` default
/// (`actor/mod.rs:170`): a linked abnormal death propagates, anything else
/// is observed.
pub struct OtpPropagation;

impl<A: CapsActor> WatchPolicy<A> for OtpPropagation {
    async fn on_link_died(
        _actor: &mut A,
        id: u64,
        reason: StopReason,
        linked: bool,
    ) -> Result<ControlFlow<StopReason>, A::Error> {
        Ok(if linked && reason != StopReason::Normal {
            ControlFlow::Break(StopReason::LinkDied(id))
        } else {
            ControlFlow::Continue(())
        })
    }
}

/// The watching capability: policy rides the type (strategy-as-type,
/// ADR-0026 constraint 5). No runtime state — the watchers set stays
/// loop-owned exactly as today.
pub struct Watching<WP>(PhantomData<WP>);

impl<WP> Watching<WP> {
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

/// "This cap set watches, with policy `Self::Policy`" — the associated type
/// is what dodges E0207: no impl anywhere mentions a free `WP` parameter.
/// [derive-emitted] from a `Watching<WP>` field.
pub trait HasWatching<A: CapsActor> {
    type Policy: WatchPolicy<A>;
}

// ──────────────────────────────────────── the Supervising cap (stage 3) ────

/// Strategy-as-type: the restart-set strategy a `Supervising` cap names.
/// There is deliberately NO default (card #280: required-by-construction —
/// the shipped `OneForOne` default is dropped).
pub trait StrategySel: Send + 'static {
    const STRATEGY: SupervisionStrategy;
}

pub struct OneForOne;
pub struct RestForOne;
pub struct OneForAll;

impl StrategySel for OneForOne {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::OneForOne;
}
impl StrategySel for RestForOne {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::RestForOne;
}
impl StrategySel for OneForAll {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::OneForAll;
}

/// The supervising capability: the strategy rides the type.
pub struct Supervising<SS: StrategySel>(PhantomData<SS>);

impl<SS: StrategySel> Supervising<SS> {
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

/// "This cap set supervises" — **O3 lives here**: the supertrait
/// `HasWatching<A>` makes supervising-without-watching unsatisfiable, so an
/// invalid stack dies at the spawn site (red case R1).
/// [derive-emitted] from a `Supervising<SS>` field.
pub trait HasSupervising<A: CapsActor>: HasWatching<A> {
    type Strat: StrategySel;
}

// ═══════════════════ Shell: the ONE adapter, conditionally loop-capable ═══

pub struct Shell<A: CapsActor> {
    user: A,
    caps: A::Caps,
}

impl<A: CapsActor> RtActor for Shell<A> {
    type Msg = A::Msg;
    type Args = A::Args;
    type Error = A::Error;

    fn on_start(args: A::Args) -> Self {
        let caps = A::Caps::build(&args);
        let user = A::init(args);
        Self { user, caps }
    }

    /// Mirrors the shipped stage-2 `Shell::handle`: user handler, then the
    /// in-step replay drain. Loop-shape-agnostic — this is why `Stashing`
    /// composes with every loop for free.
    async fn handle(&mut self, msg: A::Msg) -> Result<(), A::Error> {
        self.user.handle(msg).await?;
        while let Some(m) = self.caps.next_replay() {
            self.user.handle(m).await?;
        }
        Ok(())
    }
}

impl<A: CapsActor> sealed::Sealed for Shell<A> {}

/// The stage-3 analogue of stage 2's "derive emits Replay": `Shell` is
/// link-reactive exactly when the cap set declares `Watching`, and the
/// reaction IS the declared policy. Conditional impl — no overlap with
/// anything (O2: the policy is reached via the associated type, so there is
/// no unconstrained `WP` parameter and no E0207).
impl<A: CapsActor> LinkReact for Shell<A>
where
    A::Caps: HasWatching<A>,
{
    fn on_link_died(
        &mut self,
        id: u64,
        reason: StopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<StopReason>, Self::Error>> + Send {
        <<A::Caps as HasWatching<A>>::Policy as WatchPolicy<A>>::on_link_died(
            &mut self.user,
            id,
            reason,
            linked,
        )
    }
}

impl<A: CapsActor> SupervisedReact for Shell<A>
where
    A::Caps: HasSupervising<A>,
{
    fn strategy() -> SupervisionStrategy {
        <A::Caps as HasSupervising<A>>::Strat::STRATEGY
    }
}

// ═══════════════════════ the floor (mirrors PreparedActor, re-bounded) ════

/// Send-guard: the real floor hands these futures to `tokio::spawn`, which
/// requires `Send`. If policy dispatch ever un-Sends the lifecycle future,
/// THIS line is where the spike breaks (O6).
fn assert_send<F: Future + Send>(f: F) -> F {
    f
}

/// Mirrors `PreparedActor<A>` — kept, re-bounded: the plain block keeps
/// `RtActor` (unchanged), the linked/supervised blocks trade
/// `A: Watch`/`A: Supervisor` for the sealed seams (`spawn.rs:207/278`).
pub struct PreparedActor<RtA: RtActor>(PhantomData<RtA>);

impl<RtA: RtActor> PreparedActor<RtA> {
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }

    pub fn run(self, args: RtA::Args) -> impl Future<Output = StopReason> {
        assert_send(async move {
            let mut state = RtA::on_start(args);
            let mut mailbox = Vec::new();
            run_message_loop(&mut state, &mut mailbox).await
        })
    }
}

impl<RtA: LinkReact> PreparedActor<RtA> {
    pub fn run_linked(self, args: RtA::Args) -> impl Future<Output = StopReason> {
        assert_send(async move {
            let mut state = RtA::on_start(args);
            let mut mailbox = Vec::new();
            let mut link_rx = Vec::new();
            run_linked_message_loop(&mut state, &mut mailbox, &mut link_rx).await
        })
    }
}

impl<RtA: SupervisedReact> PreparedActor<RtA> {
    pub fn run_supervised(self, args: RtA::Args) -> impl Future<Output = StopReason> {
        assert_send(async move {
            let mut state = RtA::on_start(args);
            let mut mailbox = Vec::new();
            let mut link_rx = Vec::new();
            run_supervised_message_loop(&mut state, &mut mailbox, &mut link_rx).await
        })
    }
}

// ═══════════════════════════ O1: the compile-time loop selector ═══════════

/// Names the loop shape for a cap set. The associated type is deliberately
/// UNBOUNDED here; the `Runner: RunKind<A>` obligation is discharged at the
/// one `spawn` (where `A` is concrete), which is what lets the
/// [derive-emitted] impl stay generic over `A` without proving the cap
/// bounds for every possible actor.
pub trait SelectRunner<A: CapsActor> {
    type Runner;
}

/// The plain-actor floor: `()` selects the one-arm loop.
impl<A: CapsActor> SelectRunner<A> for () {
    type Runner = PlainRun;
}

pub struct PlainRun;
pub struct LinkedRun;
pub struct SupervisedRun;

/// One marker per loop shape; `spawn` is monomorphized through this — the
/// "branch" is trait resolution, not code.
pub trait RunKind<A: CapsActor> {
    fn spawn(args: A::Args) -> Handle<A>;
}

impl<A: CapsActor> RunKind<A> for PlainRun {
    fn spawn(args: A::Args) -> Handle<A> {
        let prepared = PreparedActor::<Shell<A>>::new();
        let _fut = prepared.run(args); // real code: tokio::spawn(fut)
        ActorRef::stub()
    }
}

impl<A: CapsActor> RunKind<A> for LinkedRun
where
    A::Caps: HasWatching<A>,
{
    fn spawn(args: A::Args) -> Handle<A> {
        let prepared = PreparedActor::<Shell<A>>::new();
        let _fut = prepared.run_linked(args);
        ActorRef::stub()
    }
}

impl<A: CapsActor> RunKind<A> for SupervisedRun
where
    A::Caps: HasSupervising<A>,
{
    fn spawn(args: A::Args) -> Handle<A> {
        let prepared = PreparedActor::<Shell<A>>::new();
        let _fut = prepared.run_supervised(args);
        ActorRef::stub()
    }
}

pub type Handle<A> = ActorRef<Shell<A>>;

/// THE one spawn (O1). The `where` clause is the single compile gate: it
/// resolves the cap set's named `Runner` and demands it be runnable for this
/// concrete actor — which transitively demands `HasWatching`/`HasSupervising`
/// exactly when the linked/supervised shapes were selected. Monomorphized;
/// zero runtime branches.
pub fn spawn<A: CapsActor>(args: A::Args) -> Handle<A>
where
    <A::Caps as SelectRunner<A>>::Runner: RunKind<A>,
{
    <<A::Caps as SelectRunner<A>>::Runner as RunKind<A>>::spawn(args)
}

// ═══════════════════════════════ O4: cap-gated verbs on the handle ════════

impl<A: CapsActor> ActorRef<Shell<A>>
where
    A::Caps: HasWatching<A>,
{
    /// Mirrors `ActorRef::watch` (`actor_ref.rs:202` block) — gated on the
    /// WATCHER's cap; the target stays universally watchable, as today.
    pub fn watch<B: CapsActor>(&self, _target: &Handle<B>) {}

    /// Mirrors `ActorRef::link` (`actor_ref.rs:254`) — both peers must watch.
    pub fn link<B: CapsActor>(&self, _peer: &Handle<B>)
    where
        B::Caps: HasWatching<B>,
    {
    }
}

impl<A: CapsActor> ActorRef<Shell<A>>
where
    A::Caps: HasSupervising<A>,
{
    /// Mirrors `ActorRef::supervise` (`actor_ref.rs:333` block).
    pub fn supervise<C: CapsActor>(&self, _child: &Handle<C>) {}

    pub fn stop_child(&self, _id: u64) {}
}

// ═══════════════════════════════════════════ proof actors (green cases) ═══

pub struct Job;

// ---- plain: Caps = () — the stage-1 floor, untouched by stage 3 ----------

pub struct Plain;

impl CapsActor for Plain {
    type Msg = Job;
    type Args = ();
    type Error = core::convert::Infallible;
    type Caps = ();

    fn init((): ()) -> Self {
        Self
    }

    async fn handle(&mut self, _msg: Job) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ---- watching-only, custom recording policy (what the ported suites do) --

/// A watcher that records notices into its own state — proves a policy
/// reaches `&mut A` (the equivalence suites' recording hooks need this).
pub struct Recorder {
    pub seen: Vec<u64>,
}

pub struct RecordingPolicy;

/// Concrete-actor policy impl (NOT generic) — proves the [derive-emitted]
/// `HasWatching` where-clause resolves for actor-specific policies, not
/// just the generic `OtpPropagation`.
impl WatchPolicy<Recorder> for RecordingPolicy {
    async fn on_link_died(
        actor: &mut Recorder,
        id: u64,
        _reason: StopReason,
        _linked: bool,
    ) -> Result<ControlFlow<StopReason>, core::convert::Infallible> {
        actor.seen.push(id);
        Ok(ControlFlow::Continue(()))
    }
}

pub struct RecorderCaps {
    pub watching: Watching<RecordingPolicy>,
}

impl CapSet<Recorder> for RecorderCaps {
    fn build((): &()) -> Self {
        Self {
            watching: Watching::new(),
        }
    }
}

// [derive-emitted] from the `watching: Watching<RecordingPolicy>` field:
impl Provide<Watching<RecordingPolicy>> for RecorderCaps {
    fn provide(&mut self) -> &mut Watching<RecordingPolicy> {
        &mut self.watching
    }
}
// [derive-emitted] no Stashing field → the None replay:
impl<M> Replay<M> for RecorderCaps {
    fn next_replay(&mut self) -> Option<M> {
        None
    }
}
// [derive-emitted] Watching field, policy bound via where-clause:
impl<A: CapsActor> HasWatching<A> for RecorderCaps
where
    RecordingPolicy: WatchPolicy<A>,
{
    type Policy = RecordingPolicy;
}
// [derive-emitted] Watching, no Supervising → the linked shape:
impl<A: CapsActor> SelectRunner<A> for RecorderCaps {
    type Runner = LinkedRun;
}

impl CapsActor for Recorder {
    type Msg = Job;
    type Args = ();
    type Error = core::convert::Infallible;
    type Caps = RecorderCaps;

    fn init((): ()) -> Self {
        Self { seen: Vec::new() }
    }

    async fn handle(&mut self, _msg: Job) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ---- O5: the deferring supervisor (Stashing + Watching + Supervising) ----

/// The composition the trait tiers made UNREPRESENTABLE (ADR-0026's decisive
/// fact: no `impl Watch for Stashed<S>`): a supervisor that defers work
/// while rebuilding a child. Under caps it is just three fields.
pub struct DeferringSup;

pub struct DeferringSupCaps {
    pub stash: Stashing<Job>,
    pub watching: Watching<OtpPropagation>,
    pub supervising: Supervising<OneForAll>,
}

impl CapSet<DeferringSup> for DeferringSupCaps {
    fn build((): &()) -> Self {
        Self {
            stash: Stashing::new(),
            watching: Watching::new(),
            supervising: Supervising::new(),
        }
    }
}

// [derive-emitted] per-field Provide:
impl Provide<Stashing<Job>> for DeferringSupCaps {
    fn provide(&mut self) -> &mut Stashing<Job> {
        &mut self.stash
    }
}
impl Provide<Watching<OtpPropagation>> for DeferringSupCaps {
    fn provide(&mut self) -> &mut Watching<OtpPropagation> {
        &mut self.watching
    }
}
impl Provide<Supervising<OneForAll>> for DeferringSupCaps {
    fn provide(&mut self) -> &mut Supervising<OneForAll> {
        &mut self.supervising
    }
}
// [derive-emitted] Stashing field → forwarding replay (stage-2 shape):
impl Replay<Job> for DeferringSupCaps {
    fn next_replay(&mut self) -> Option<Job> {
        self.stash.next_replay()
    }
}
// [derive-emitted] Watching field:
impl<A: CapsActor> HasWatching<A> for DeferringSupCaps
where
    OtpPropagation: WatchPolicy<A>,
{
    type Policy = OtpPropagation;
}
// [derive-emitted] Supervising field (supertrait `HasWatching` satisfied by
// the impl above — remove the Watching field and THIS is what R1 breaks):
impl<A: CapsActor> HasSupervising<A> for DeferringSupCaps
where
    OtpPropagation: WatchPolicy<A>,
{
    type Strat = OneForAll;
}
// [derive-emitted] Supervising present → the supervised shape:
impl<A: CapsActor> SelectRunner<A> for DeferringSupCaps {
    type Runner = SupervisedRun;
}

impl CapsActor for DeferringSup {
    type Msg = Job;
    type Args = ();
    type Error = core::convert::Infallible;
    type Caps = DeferringSupCaps;

    fn init((): ()) -> Self {
        Self
    }

    async fn handle(&mut self, _msg: Job) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ═══════════════════════════════════════════════ green proof (compiles) ═══

/// O1/O5/O4 green: the ONE spawn serves all three shapes, monomorphized; the
/// verbs exist exactly where the caps allow them. Never executed — the
/// compiler passing this fn IS the proof.
pub fn green_proof() {
    // O1: three shapes, one entry.
    let plain: Handle<Plain> = spawn::<Plain>(());
    let recorder: Handle<Recorder> = spawn::<Recorder>(());
    let defsup: Handle<DeferringSup> = spawn::<DeferringSup>(());

    // O4 green half: watcher verbs on watching handles.
    recorder.watch(&plain);
    defsup.watch(&recorder);
    defsup.link(&recorder); // both sides watch — ok
    defsup.supervise(&plain);
    defsup.stop_child(7);

    // A supervised handle is also a watching handle (supertrait chain).
    let _ = &defsup;
}

// ══════════════════════════════════════════════════ red cases (cfg-gated) ═

/// R1 (O3): Supervising WITHOUT Watching — the invalid stack. With
/// `--cfg r1_supervising_without_watching` this must FAIL to compile:
/// `HasSupervising`'s supertrait `HasWatching` is unimplementable (no
/// Watching field → no [derive-emitted] HasWatching impl), so the spawn
/// gate rejects it.
#[cfg(r1_supervising_without_watching)]
pub mod r1 {
    use super::{
        CapSet, CapsActor, Handle, HasSupervising, Job, OneForOne, Replay, SelectRunner,
        SupervisedRun, Supervising, spawn,
    };

    pub struct Rogue;

    pub struct RogueCaps {
        pub supervising: Supervising<OneForOne>,
    }

    impl CapSet<Rogue> for RogueCaps {
        fn build((): &()) -> Self {
            Self {
                supervising: Supervising::new(),
            }
        }
    }
    impl<M> Replay<M> for RogueCaps {
        fn next_replay(&mut self) -> Option<M> {
            None
        }
    }
    // [derive-emitted] Supervising field present — but there is NO
    // HasWatching impl to satisfy the supertrait, which is the point.
    impl<A: CapsActor> HasSupervising<A> for RogueCaps {
        type Strat = OneForOne;
    }
    impl<A: CapsActor> SelectRunner<A> for RogueCaps {
        type Runner = SupervisedRun;
    }

    impl CapsActor for Rogue {
        type Msg = Job;
        type Args = ();
        type Error = core::convert::Infallible;
        type Caps = RogueCaps;

        fn init((): ()) -> Self {
            Self
        }

        async fn handle(&mut self, _msg: Job) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    pub fn must_not_compile() {
        let _h: Handle<Rogue> = spawn::<Rogue>(());
    }
}

/// R3 (O4): watch verb on a PLAIN handle. With `--cfg r3_watch_on_plain`
/// this must FAIL to compile: `()` has no `HasWatching`, so the verb block
/// does not exist for `Handle<Plain>`.
#[cfg(r3_watch_on_plain)]
pub mod r3 {
    use super::{Handle, Plain, spawn};

    pub fn must_not_compile() {
        let plain: Handle<Plain> = spawn::<Plain>(());
        let other: Handle<Plain> = spawn::<Plain>(());
        plain.watch(&other);
    }
}
