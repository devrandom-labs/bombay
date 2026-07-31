//! Candidate (b) for card #277: ONE actor trait, capabilities as plugged
//! TYPES, one context, one handle type.
//!
//! What this spike proves/measures (ergonomics + compile-time enforcement):
//! - one trait (`Actor`) covers plain / deferring / phased / watching
//!   actors — no StashActor/FsmActor/Watch/Supervisor trait family, no
//!   empty marker impls, no wrapper type leaking into signatures;
//! - capabilities are TYPES on `type Caps` (Joel's strategy-as-type):
//!   `()`, `Stashing`, `Phased<P>`, `Watching<P>` — policies are separate
//!   plugged units, required by construction (no defaults to forget);
//! - capability ACCESS is compile-gated: `cx.stash()` only exists when
//!   the cap set provides it (trait-bound accessor — misuse is a compile
//!   error, demonstrated in tests/compile_gate.rs);
//! - ONE `Ref<A>` handle: internally weak, upgrade-per-use — the
//!   strong/weak prose rule becomes a non-question at the API surface
//!   (liveness pinning is a runtime-model concern, noted not modeled);
//! - ONE `spawn` entry: capability set decides the loop shape at compile
//!   time (modeled by a single dispatcher here).
//!
//! The runtime here is a MODEL (synchronous dispatcher driving the same
//! gate/defer/replay/deadline semantics pinned by ADR-0024/0025) — enough
//! to run behavior tests on the walkthroughs; the real loop is not
//! re-modeled.

use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::sync::mpsc;
use std::time::Duration;

// ---------------------------------------------------------------- core --

/// Handler continuation (ADR-0023 unchanged).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Continue,
    Stop,
}

/// Per-state admission (ADR-0024 unchanged).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Deliver,
    Defer,
    Ignore,
}

/// THE one user trait. Everything else is a plugged capability type.
pub trait Actor: Sized + Send + 'static {
    type Msg: Send + 'static;
    type Args: Send;
    type Error: Debug + Send + 'static;
    /// The capability set — a tuple of capability TYPES. `()` = plain.
    /// Each capability carries its policy as an associated type the user
    /// implements as a unit (strategy-as-type; nothing defaulted away).
    type Caps: CapSet<Self>;

    fn init(
        args: Self::Args,
        cx: &mut Ctx<Self>,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn handle(
        &mut self,
        msg: Self::Msg,
        cx: &mut Ctx<Self>,
    ) -> impl Future<Output = Result<Flow, Self::Error>> + Send;
}

// -------------------------------------------------------- capabilities --

/// A capability set: builds its runtime state from the spawn args.
pub trait CapSet<A: Actor>: Sized + Send + 'static {
    fn build(args: &A::Args) -> Self;
}

impl<A: Actor> CapSet<A> for () {
    fn build(_: &A::Args) -> Self {}
}

/// Bounded deferral capability (ADR-0022 semantics).
pub struct Stashing<A: Actor> {
    held: VecDeque<A::Msg>,
    ready: VecDeque<A::Msg>,
    cap: usize,
}

/// The stash policy: one required item — the bound (never defaulted).
pub trait StashPolicy<A: Actor>: Send + 'static {
    fn capacity(args: &A::Args) -> usize;
}

impl<A: Actor> Stashing<A> {
    fn bounded(cap: usize) -> Self {
        Self {
            held: VecDeque::new(),
            ready: VecDeque::new(),
            cap,
        }
    }
    pub fn stash(&mut self, msg: A::Msg) -> Result<(), A::Msg> {
        if self.held.len() + self.ready.len() >= self.cap {
            return Err(msg);
        }
        self.held.push_back(msg);
        Ok(())
    }
    pub fn unstash_all(&mut self) {
        self.ready.append(&mut self.held);
    }
    fn pop_ready(&mut self) -> Option<A::Msg> {
        self.ready.pop_front()
    }
}

/// Phase capability (ADR-0024 semantics over the ADR-0025 plane). The
/// whole machine — states, admission, deadlines, timeout reaction — is
/// ONE plugged policy type: nothing about it can be half-implemented.
pub struct Phased<P: PhasePolicy> {
    state: P::State,
    entered_at_virtual_ms: u64,
    /// Two queues, ADR-0022 snapshot semantics: `Defer` pushes to `held`;
    /// a state change moves `held` → `ready`; replay pops `ready` only.
    /// A message re-deferred DURING replay goes back to `held` — the
    /// batch is a snapshot, so replay always terminates. (The first
    /// draft of this model used one queue and livelocked instantly —
    /// an executable re-proof of why ADR-0022 mandates the split.)
    held: VecDeque<P::Msg>,
    ready: VecDeque<P::Msg>,
    stash_cap: usize,
}

pub trait PhasePolicy: Send + 'static {
    type State: Copy + PartialEq + Send + 'static;
    type Msg: Send + 'static;
    type Args;
    fn initial(args: &Self::Args) -> Self::State;
    fn stash_capacity(args: &Self::Args) -> usize;
    fn gate(state: &Self::State, msg: &Self::Msg) -> Disposition;
    fn state_timeout(state: &Self::State) -> Option<Duration>;
}

/// Death-watch capability: the reaction policy is the plugged type —
/// no `impl Watch for X {}` ceremony, no separate trait tier.
pub struct Watching<P: WatchPolicy> {
    pub notices: Vec<(u64, bool)>,
    _p: std::marker::PhantomData<P>,
}

pub trait WatchPolicy: Send + 'static {
    /// true = propagate (stop), false = absorb. (Model of on_link_died.)
    fn on_death(id: u64, abnormal: bool, linked: bool) -> bool;
}

/// OTP default as a NAMED policy the user chooses explicitly.
pub struct OtpPropagation;
impl WatchPolicy for OtpPropagation {
    fn on_death(_: u64, abnormal: bool, linked: bool) -> bool {
        linked && abnormal
    }
}

// Tuple cap-sets for the shapes the walkthroughs need. (A real impl
// generates these via macro over arities, axum/bevy-style.)
impl<A: Actor, SP: StashPolicy<A>> CapSet<A> for (Stashing<A>, std::marker::PhantomData<SP>) {
    fn build(args: &A::Args) -> Self {
        (
            Stashing::bounded(SP::capacity(args)),
            std::marker::PhantomData,
        )
    }
}

impl<A, P> CapSet<A> for Phased<P>
where
    A: Actor<Msg = P::Msg, Args = P::Args>,
    P: PhasePolicy,
{
    fn build(args: &A::Args) -> Self {
        Self {
            state: P::initial(args),
            entered_at_virtual_ms: 0,
            held: VecDeque::new(),
            ready: VecDeque::new(),
            stash_cap: P::stash_capacity(args),
        }
    }
}

impl<A: Actor, P: WatchPolicy> CapSet<A> for Watching<P> {
    fn build(_: &A::Args) -> Self {
        Self {
            notices: Vec::new(),
            _p: std::marker::PhantomData,
        }
    }
}

// ------------------------------------------------------------- context --

/// The ONE context: owns the capability state, borrow-split from `self`
/// by construction (framework state lives here, user state in the actor).
pub struct Ctx<A: Actor> {
    pub caps: A::Caps,
    outbox: Vec<A::Msg>,
    virtual_now_ms: u64,
}

impl<A: Actor> Ctx<A> {
    /// Self-send (models tell-to-self; the one Ref is elsewhere).
    pub fn send_self(&mut self, msg: A::Msg) {
        self.outbox.push(msg);
    }
}

/// Compile-gated accessors: these exist ONLY for cap sets that provide
/// the capability — `cx.stash()` on a plain actor is a COMPILE ERROR.
impl<A, SP> Ctx<A>
where
    A: Actor<Caps = (Stashing<A>, std::marker::PhantomData<SP>)>,
    SP: StashPolicy<A>,
{
    pub fn stash(&mut self) -> &mut Stashing<A> {
        &mut self.caps.0
    }
}

impl<A, P> Ctx<A>
where
    A: Actor<Caps = Phased<P>, Msg = P::Msg, Args = P::Args>,
    P: PhasePolicy,
{
    pub fn phase(&self) -> P::State {
        self.caps.state
    }
    /// The transition verb — gen_statem/ADR-0024 rules applied by the
    /// framework: no-op on same state; release + re-deadline on change.
    pub fn goto(&mut self, next: P::State) {
        if next != self.caps.state {
            self.caps.state = next;
            self.caps.entered_at_virtual_ms = self.virtual_now_ms;
            // release: deferred messages re-enter delivery (drained by
            // the dispatcher's replay loop, re-gated in the new state)
        }
    }
}

// ------------------------------------------------------ handle + spawn --

/// THE one handle type. Internally weak in the real design; here a
/// plain sender. `tell` is fallible (dead target) — one rule, no
/// strong/weak choice at the surface.
pub struct Ref<A: Actor> {
    tx: mpsc::Sender<A::Msg>,
}

impl<A: Actor> Clone for Ref<A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<A: Actor> Ref<A> {
    pub fn tell(&self, msg: A::Msg) -> Result<(), A::Msg> {
        self.tx.send(msg).map_err(|e| e.0)
    }
}

/// ONE spawn shape (modeled synchronously): builds caps from args, runs
/// init, returns the driver + handle. Capability-specific loop features
/// are selected by the caps type at compile time.
pub struct Spawned<A: Actor> {
    pub actor: A,
    pub cx: Ctx<A>,
    rx: mpsc::Receiver<A::Msg>,
    pub handle: Ref<A>,
    pub stopped: bool,
}

pub async fn spawn<A: Actor>(args: A::Args) -> Result<Spawned<A>, A::Error> {
    let (tx, rx) = mpsc::channel();
    let caps = A::Caps::build(&args);
    let mut cx = Ctx {
        caps,
        outbox: Vec::new(),
        virtual_now_ms: 0,
    };
    let actor = A::init(args, &mut cx).await?;
    Ok(Spawned {
        actor,
        cx,
        rx,
        handle: Ref { tx },
        stopped: false,
    })
}

impl<A: Actor> Spawned<A> {
    /// Drives every queued message through the plain path (no phase).
    pub async fn drain(&mut self) -> Result<(), A::Error> {
        while let Ok(msg) = self.rx.try_recv() {
            if self.stopped {
                return Ok(());
            }
            if self.step(msg).await? == Flow::Stop {
                self.stopped = true;
            }
        }
        Ok(())
    }

    async fn step(&mut self, msg: A::Msg) -> Result<Flow, A::Error> {
        // Borrow-split Q1: user state (&mut self.actor) and framework
        // state (&mut self.cx) are DISJOINT fields — this compiles, and
        // is the answer to the audit's Q1.
        let flow = self.actor.handle(msg, &mut self.cx).await?;
        // self-sends re-enter the queue
        for m in self.cx.outbox.drain(..) {
            let _ = self.handle.tell(m);
        }
        Ok(flow)
    }
}

/// Stash-capability driving: replay released messages after each step.
impl<A, SP> Spawned<A>
where
    A: Actor<Caps = (Stashing<A>, std::marker::PhantomData<SP>)>,
    SP: StashPolicy<A>,
{
    pub async fn drain_stashed(&mut self) -> Result<(), A::Error> {
        while let Ok(msg) = self.rx.try_recv() {
            if self.stopped {
                return Ok(());
            }
            if self.step(msg).await? == Flow::Stop {
                self.stopped = true;
            }
            while let Some(m) = self.cx.caps.0.pop_ready() {
                if self.step(m).await? == Flow::Stop {
                    self.stopped = true;
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Compile-gate proof: requesting a capability the cap set does not
/// provide is a COMPILE error, not a runtime surprise.
///
/// ```compile_fail
/// use spike_277_b::{Actor, Ctx, Flow, spawn};
/// struct Plain;
/// impl Actor for Plain {
///     type Msg = (); type Args = (); type Error = std::convert::Infallible;
///     type Caps = ();                       // no stash capability...
///     async fn init((): (), _: &mut Ctx<Self>) -> Result<Self, Self::Error> { Ok(Plain) }
///     async fn handle(&mut self, (): (), cx: &mut Ctx<Self>) -> Result<Flow, Self::Error> {
///         cx.stash();                       // ...so this DOES NOT COMPILE
///         Ok(Flow::Continue)
///     }
/// }
/// ```
pub struct CompileGateProof;

/// Phased driving: gate → deliver/defer/ignore, replay-on-transition,
/// virtual-clock deadline (models the ADR-0025 plane).
impl<A, P> Spawned<A>
where
    A: Actor<Caps = Phased<P>, Msg = P::Msg, Args = P::Args>,
    P: PhasePolicy,
{
    pub async fn drain_phased(&mut self) -> Result<(), A::Error> {
        while let Ok(msg) = self.rx.try_recv() {
            if self.stopped {
                return Ok(());
            }
            self.step_phased(msg).await?;
            self.replay().await?;
        }
        Ok(())
    }

    /// Advances the virtual clock; fires the phase deadline if due
    /// (loop-owned, fires-once — the plane model).
    pub async fn advance(&mut self, ms: u64, on_timeout: fn(P::State) -> Option<P::Msg>)
    where
        P::Msg: Send,
    {
        self.cx.virtual_now_ms += ms;
        if let Some(t) = P::state_timeout(&self.cx.caps.state) {
            let due = self.cx.caps.entered_at_virtual_ms + u64::try_from(t.as_millis()).unwrap();
            if self.cx.virtual_now_ms >= due
                && let Some(m) = on_timeout(self.cx.caps.state)
            {
                let _ = self.handle.tell(m);
            }
        }
    }

    async fn step_phased(&mut self, msg: A::Msg) -> Result<(), A::Error> {
        match P::gate(&self.cx.caps.state, &msg) {
            Disposition::Deliver => {
                let before = self.cx.caps.state;
                let flow = self.actor.handle(msg, &mut self.cx).await?;
                if flow == Flow::Stop {
                    self.stopped = true;
                }
                if before != self.cx.caps.state {
                    self.cx.caps.unstash_for_replay();
                }
                for m in self.cx.outbox.drain(..) {
                    let _ = self.handle.tell(m);
                }
            }
            Disposition::Defer => {
                if self.cx.caps.held.len() + self.cx.caps.ready.len() >= self.cx.caps.stash_cap {
                    // overflow-handback modeled as redelivery to handle
                    let _ = self.actor.handle(msg, &mut self.cx).await?;
                } else {
                    self.cx.caps.held.push_back(msg);
                }
            }
            Disposition::Ignore => {}
        }
        Ok(())
    }

    async fn replay(&mut self) -> Result<(), A::Error> {
        while let Some(m) = self.cx.caps.pop_replay() {
            if self.stopped {
                return Ok(());
            }
            self.step_phased(m).await?;
        }
        Ok(())
    }
}

impl<P: PhasePolicy> Phased<P> {
    fn unstash_for_replay(&mut self) {
        self.ready.append(&mut self.held);
    }
    fn pop_replay(&mut self) -> Option<P::Msg> {
        self.ready.pop_front()
    }
}
