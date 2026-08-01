//! The local actor spine (card #116): the `Actor` trait, its lifecycle hooks,
//! the run-loop that drives it, and the spawn entry points.
//!
//! Send-saturated for now; the cfg-gated `MaybeSend` relaxation for
//! single-threaded client builds is a dedicated later sweep (#9). The `ActorRef`
//! here is a **minimal scaffold** — ref-count-driven stop, `Recipient` erasure,
//! and the `tell`/`ask` builders are #117/#118.

use core::{any::type_name, future::Future, ops::ControlFlow};

use tokio::time::Instant;

use crate::{
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::{ActorId, Mailboxed},
    message::Msg,
    restart::SupervisionStrategy,
};

mod actor_ref;
mod kind;
mod pipe;
mod recipient;
mod spawn;
mod supervision;
mod timer;

pub use self::{
    actor_ref::{ActorRef, WeakActorRef},
    recipient::{Recipient, RecipientAskRequest, ReplyRecipient, WeakRecipient},
    spawn::{DEFAULT_MAILBOX_CAPACITY, PreparedActor, RunResult, SpawnConfig},
    timer::TimerHandle,
};

// Test-local stand-ins for the removed spawn verbs (stage 3, card #280),
// shared by the in-crate unit tests of pipe/timer/request.
#[cfg(test)]
pub(crate) use spawn::test_verbs;

// The supervision types stay OFF the public API — the `supervise` verb returns a
// bare `ActorId`, and `SuperviseReg`/`SupervisionOp` only ride inside the
// `ControlSignal::Supervision(Box<SupervisionOp>)` variant, exactly as `WatchReg`
// rides `ControlSignal::Watch(Box<WatchReg>)` without being re-exported.
// `SupervisionOp` needs a `pub(crate)` re-export only because `mailbox` (outside
// this module) names it in that variant; `ChildHandle`/`SuperviseReg` are used
// solely within `actor` and reach their definitions directly.
pub(crate) use self::supervision::SupervisionOp;

/// The handler's continuation decision: keep running, or stop cleanly
/// (reason `Normal`) after the current message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    /// Keep the actor running; poll the mailbox for the next message.
    Continue,
    /// Stop after this handler: reason `Normal`, backlog abandoned,
    /// `on_stop` runs. Deliberately carries no reason — a self-stop's
    /// only honest reason is `Normal` (ADR-0023).
    Stop,
}

/// A single-writer, identity-agnostic unit of concurrency: owned state behind a
/// mailbox, driven by one task that handles messages sequentially.
///
/// > Staged supersession (ADR-0026): new code can use the distilled
/// > [`caps::Actor`](crate::caps::Actor) surface; this trait remains fully
/// > supported while the migration stages land.
///
/// `Actor` is a subtrait of [`Mailboxed`] (the mailbox is keyed on the actor),
/// and its message type is bounded `: Msg` so every actor's `Msg` gets the
/// compile-time slot-size tripwire (card #114).
///
/// # Panics & poisoning
///
/// A panic in `handle` is caught and routed to [`on_panic`](Actor::on_panic);
/// the actor then **stops** (there is no resume). After a panic `&mut self` is
/// **poisoned** (torn state): [`on_stop`](Actor::on_stop) still runs and may do
/// reason-independent resource release only — it must **never** flush or derive
/// from domain fields, which are torn.
pub trait Actor: Mailboxed<Msg: Msg> + Sized + Send + 'static {
    /// The argument passed to [`on_start`](Actor::on_start) to build the state.
    type Args: Send;
    /// The actor's own domain error, kept typed end to end.
    type Error: ReplyError;

    /// A human-readable name for logs/tracing. Defaults to the type name.
    #[must_use]
    fn name() -> &'static str {
        type_name::<Self>()
    }

    /// Builds (or hydrates) the actor state. Runs to completion before any
    /// message is handled; messages that arrive meanwhile wait in the mailbox.
    fn on_start(
        args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    /// Handles one message. The return value is the continuation decision:
    /// `Ok(Flow::Continue)` keeps the actor running; `Ok(Flow::Stop)` stops it
    /// cleanly (reason `Normal`) after this handler, before any further mailbox
    /// poll; a returned `Err` is a controlled crash (routed to `on_panic`, then
    /// stop). The three outcomes are one exhaustive value — signalling stop
    /// *and* crash at once is unrepresentable.
    fn handle(
        &mut self,
        msg: Self::Msg,
        actor_ref: ActorRef<Self>,
    ) -> impl Future<Output = Result<Flow, Self::Error>> + Send;

    /// The next instant this actor needs waking, as a pure function of its
    /// current state — the ADR-0025 declarative deadline slot (the quinn
    /// `poll_timeout` shape). Re-read by the loop every iteration, so state
    /// changes take effect at the next step boundary; `None` disables the
    /// arm entirely (a disabled arm registers nothing with the timer wheel —
    /// a `Sleep` registers lazily on first poll).
    ///
    /// This default is the runtime FLOOR every loop polls uniformly; the
    /// user seat is the [`caps::Deadlined`](crate::caps::Deadlined) /
    /// [`caps::Phased`](crate::caps::Phased) capability (ADR-0026), which
    /// [`caps::Shell`](crate::caps::Shell) bridges here.
    #[must_use]
    fn next_deadline(&self) -> Option<Instant> {
        None
    }

    /// Expiry delivery, at a turn boundary (ADR-0025): runs under the same
    /// `catch_unwind`/poisoning treatment as [`handle`](Actor::handle), in
    /// the `PanicReason::OnDeadline` crash domain — handler-like and
    /// restart-eligible, NOT a lifecycle hook.
    ///
    /// Takes a [`WeakActorRef`] by drain-window necessity, not style: a
    /// deadline fire carries no message to mint a strong ref from, and a
    /// loop-held sender would keep the mailbox open forever and defeat
    /// `Collected` (ADR-0020). Deadlines keep firing through the drain
    /// window; `Flow` decisions work unchanged there — only self-sends
    /// degrade (`upgrade` returns `None`), the `on_panic`/`on_stop` family.
    ///
    /// Fires once per value: after firing for deadline `d`, the arm
    /// re-enables only when [`next_deadline`](Actor::next_deadline) reports
    /// a value ≠ `d` — a hook that leaves its deadline unchanged cannot
    /// busy-loop the biased select.
    fn on_deadline(
        &mut self,
        actor_ref: WeakActorRef<Self>,
    ) -> impl Future<Output = Result<Flow, Self::Error>> + Send {
        let _ = actor_ref;
        async { Ok(Flow::Continue) }
    }

    /// Observes a caught panic and names the terminal stop reason. Infallible
    /// and stop-only — it cannot resume the actor. `&mut self` is poisoned.
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> impl Future<Output = ActorStopReason> + Send {
        let _ = actor_ref;
        async move { ActorStopReason::Panicked(err) }
    }

    /// Terminal cleanup. A returned `Err` is logged/surfaced, **never**
    /// unwrapped, and the original `reason` is preserved. On the poisoned
    /// (post-panic) path, do resource release only — never read domain fields.
    ///
    /// # Time-bounded
    ///
    /// This hook is **bounded**: watchers must learn of the death promptly, so
    /// the runtime waits a fixed grace (5 s) and then **drops this future where
    /// it is parked**. Cleanup past that point does not happen — code after an
    /// `.await` that outlives the grace never runs. The death notice then
    /// reports `cleanup_failed`, exactly as for a returned `Err` or a panic. Do
    /// blocking-free, bounded work here; hand anything open-ended to a task that
    /// outlives the actor.
    ///
    /// # Runtime
    ///
    /// That bound is a `tokio::time::timeout`, so **every** actor now needs a
    /// runtime with the TIME driver enabled (`Builder::enable_time`, or
    /// `enable_all` / `#[tokio::main]`) — not just the actors that use the
    /// opt-in send timeouts. On a timer-less runtime the teardown itself panics:
    /// the join handle yields `Err` and the [`RunResult`] is lost.
    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let _ = (actor_ref, reason);
        async { Ok(()) }
    }
}

/// Seals [`LinkReact`]/[`SupervisedReact`]: the traits stay nameable (they
/// appear in public floor bounds) but only this crate implements them —
/// they are the run-loop's internal dispatch seams, not a user surface.
/// The one production implementor is [`caps::Shell`](crate::caps::Shell),
/// conditional on its cap set (ADR-0026 stage 3, card #280).
pub(crate) mod sealed {
    /// The seal. Unnameable outside the crate.
    pub trait Sealed {}
}

/// The linked run-loop's dispatch seam: a runtime actor that can react to
/// a link-death notice (ADR-0026 stage 3).
///
/// Replaces the removed `Watch` trait at the loop and floor bound sites —
/// but is **sealed**: user code declares watching by plugging
/// [`caps::Watching`](crate::caps::Watching) into its cap set, never by
/// implementing this.
pub trait LinkReact: Actor + sealed::Sealed {
    /// Reacts to the death of a watched/linked actor — the semantics seat
    /// of [`caps::WatchPolicy`](crate::caps::WatchPolicy). Delivery rules
    /// are the loop's (a post-break notice is dropped by design, #266).
    ///
    /// # Errors
    ///
    /// Returns [`Actor::Error`] if the plugged policy fails; the shipped
    /// [`OtpPropagation`](crate::caps::OtpPropagation) policy is infallible.
    fn on_link_died(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send;
}

/// The supervised run-loop's dispatch seam: a link-reactive runtime actor
/// with a restart-set strategy (ADR-0026 stage 3).
///
/// Replaces the removed `Supervisor` trait at the loop and floor bound
/// sites; sealed like [`LinkReact`]. There is deliberately NO default
/// strategy — the cap set names one
/// ([`caps::Supervising`](crate::caps::Supervising),
/// required-by-construction).
pub trait SupervisedReact: LinkReact {
    /// The restart-set strategy for this supervisor's children.
    #[must_use]
    fn strategy() -> SupervisionStrategy;
}

// The former `Spawn`/`SpawnLinked`/`SpawnSupervised` verb traits and the
// `Watch`/`Supervisor` capability tiers are GONE (ADR-0026 stage 3, card
// #280): the ONE `caps::spawn` selects the loop shape from the cap set at
// compile time, and watching/supervising are the `caps::Watching`/
// `caps::Supervising` capability types. The [`PreparedActor`] floor below
// remains the expert path to deterministic lifecycle driving.
