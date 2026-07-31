//! The local actor spine (card #116): the `Actor` trait, its lifecycle hooks,
//! the run-loop that drives it, and the spawn entry points.
//!
//! Send-saturated for now; the cfg-gated `MaybeSend` relaxation for
//! single-threaded client builds is a dedicated later sweep (#9). The `ActorRef`
//! here is a **minimal scaffold** — ref-count-driven stop, `Recipient` erasure,
//! and the `tell`/`ask` builders are #117/#118.

use core::{any::type_name, future::Future, ops::ControlFlow};

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

/// Opt-in capability: an actor that **watches** others and reacts to their death.
///
/// Only actors spawned via `spawn_linked` (added in a later slice) receive death
/// notices; a plain actor is still *watchable* (passive) but cannot itself watch.
/// `Watch` is strictly less authority than a supervisor (restart) — watching is
/// "get notified", supervising is "rebuild".
pub trait Watch: Actor {
    /// Reacts to the death of a watched/linked actor.
    ///
    /// Default = OTP semantics: a **linked** (`linked == true`) **abnormal**
    /// death propagates (`Break`); a `watch` (notify-only) death, or any normal
    /// death, is observed and the actor continues. Override to trap (return
    /// `Continue` for a linked abnormal death) or to react programmatically.
    ///
    /// A notice that arrives after the run-loop has already taken its stop
    /// decision (e.g. a target dying after the mailbox-closed `Collected`
    /// break) is dropped by design and never reaches this hook: a stopping
    /// actor observes nothing further — Erlang parity, where a `DOWN`
    /// delivered to an already-dead process is dropped, and delivering
    /// post-break would violate finish-current-then-stop (card #266).
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if a custom override fails; the default hook is
    /// infallible.
    fn on_link_died(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        async move {
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
}

/// Ergonomic spawn entry points, provided for every [`Actor`].
///
/// Spawns onto the current tokio runtime and returns the [`ActorRef`]; the actor
/// stops via `Signal::Stop`, [`ActorRef::stop`], [`ActorRef::kill`], a handler
/// crash, or startup failure (ref-count-driven stop is #117).
///
/// The runtime must have the TIME driver enabled — teardown bounds
/// [`on_stop`](Actor::on_stop) with a timer, and panics without one.
pub trait Spawn: Actor {
    /// Spawns with the default [`SpawnConfig`].
    #[must_use]
    fn spawn(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_with_config(SpawnConfig::default(), args)
    }

    /// Spawns with an explicit [`SpawnConfig`] (mailbox capacity + `on_stop`
    /// notice grace).
    #[must_use]
    fn spawn_with_config(config: SpawnConfig, args: Self::Args) -> ActorRef<Self> {
        let prepared = PreparedActor::<Self>::new(config);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn(args);
        actor_ref
    }
}

impl<A: Actor> Spawn for A {}

/// Ergonomic linked-spawn entry points, provided for every [`Watch`] actor.
///
/// A linked actor is spawned with its own UNBOUNDED link channel, so it can
/// `watch`/`link` others and its [`on_link_died`](Watch::on_link_died) hook fires
/// when a watched actor stops. A `Watch` actor spawned via the plain [`Spawn`]
/// path has no link channel and cannot watch.
pub trait SpawnLinked: Watch {
    /// Spawns a linked actor with the default [`SpawnConfig`].
    #[must_use]
    fn spawn_linked(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_linked_with_config(SpawnConfig::default(), args)
    }

    /// Spawns a linked actor with an explicit [`SpawnConfig`] (mailbox capacity
    /// + `on_stop` notice grace).
    #[must_use]
    fn spawn_linked_with_config(config: SpawnConfig, args: Self::Args) -> ActorRef<Self> {
        let (prepared, link_rx) = PreparedActor::<Self>::new_linked(config);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn_linked_task(args, link_rx);
        actor_ref
    }
}

impl<A: Watch> SpawnLinked for A {}

/// Authority marker: an [`Actor`] cannot watch, a [`Watch`] actor observes a
/// peer's death, a `Supervisor` **rebuilds** dead children under a restart
/// policy.
///
/// Restart *policy* (whether a given child comes back) stays per-child,
/// supplied at `supervise` time. The *strategy* (which siblings share the
/// failed child's fate) is a property of the supervisor — the seat #196
/// reserved (card #199, ADR-0014).
pub trait Supervisor: Watch {
    /// The restart-set strategy for this supervisor's children.
    ///
    /// Defaults to [`OneForOne`](SupervisionStrategy::OneForOne) — the 2a
    /// behavior: a failed child is rebuilt alone and siblings never observe
    /// it. Override to cycle sets ([`RestForOne`](SupervisionStrategy::RestForOne)
    /// / [`OneForAll`](SupervisionStrategy::OneForAll)).
    #[must_use]
    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForOne
    }
}

/// Ergonomic supervised-spawn entry points, provided for every [`Supervisor`].
///
/// A supervisor is spawned **linked** (it owns a link channel, so its children's
/// deaths reach it) and runs the three-arm supervised loop: the message mailbox,
/// the link channel, and the restart-backoff queue. Children are registered
/// after spawn via `ActorRef::supervise` (a later card); a supervisor with no
/// children behaves exactly as a `spawn_linked` [`Watch`] actor.
pub trait SpawnSupervised: Supervisor {
    /// Spawns a supervisor with the default [`SpawnConfig`].
    #[must_use]
    fn spawn_supervised(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_supervised_with_config(SpawnConfig::default(), args)
    }

    /// Spawns a supervisor with an explicit [`SpawnConfig`] (mailbox capacity
    /// + `on_stop` notice grace).
    #[must_use]
    fn spawn_supervised_with_config(config: SpawnConfig, args: Self::Args) -> ActorRef<Self> {
        let (prepared, link_rx) = PreparedActor::<Self>::new_linked(config);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn_supervised_task(args, link_rx);
        actor_ref
    }
}

impl<A: Supervisor> SpawnSupervised for A {}

#[cfg(test)]
mod watch_trait_tests {
    use super::*;
    use crate::mailbox::ActorId;
    use core::ops::ControlFlow;

    struct W;
    #[derive(Debug)]
    struct M;
    impl crate::message::Msg for M {}
    impl crate::mailbox::Mailboxed for W {
        type Msg = M;
    }
    impl Actor for W {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(W)
        }
        async fn handle(&mut self, _: M, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            Ok(Flow::Continue)
        }
    }
    impl Watch for W {}

    /// The default `on_link_died` hook is OTP-shaped: it `Break`s only for a
    /// **linked** *and* **abnormal** death, and `Continue`s for a notify-only
    /// (`linked == false`) death or any normal death. Fails if the default
    /// collapses to one arm (always break / always continue).
    #[tokio::test]
    async fn default_hook_breaks_on_linked_abnormal_and_continues_otherwise() {
        let mut w = W;

        let out = w
            .on_link_died(ActorId::from_raw_for_test(1), ActorStopReason::Killed, true)
            .await
            .expect("infallible default hook");
        assert!(
            matches!(out, ControlFlow::Break(ActorStopReason::LinkDied { .. })),
            "linked + abnormal must propagate, got {out:?}",
        );

        let out = w
            .on_link_died(
                ActorId::from_raw_for_test(1),
                ActorStopReason::Killed,
                false,
            )
            .await
            .expect("infallible default hook");
        assert!(
            matches!(out, ControlFlow::Continue(())),
            "watch (linked=false) + abnormal is notify-only, got {out:?}",
        );

        let out = w
            .on_link_died(ActorId::from_raw_for_test(1), ActorStopReason::Normal, true)
            .await
            .expect("infallible default hook");
        assert!(
            matches!(out, ControlFlow::Continue(())),
            "linked + normal does not propagate, got {out:?}",
        );
    }
}

#[cfg(test)]
mod supervisor_trait_tests {
    use super::*;
    use crate::restart::SupervisionStrategy;

    struct DefaultSup;
    struct AllSup;
    #[derive(Debug)]
    struct M2;
    impl crate::message::Msg for M2 {}
    macro_rules! actor_boilerplate {
        ($t:ty) => {
            impl crate::mailbox::Mailboxed for $t {
                type Msg = M2;
            }
            impl Actor for $t {
                type Args = ();
                type Error = core::convert::Infallible;
                async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                    unreachable!("trait-surface test: never spawned")
                }
                async fn handle(&mut self, _: M2, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
                    Ok(Flow::Continue)
                }
            }
            impl Watch for $t {}
        };
    }
    actor_boilerplate!(DefaultSup);
    actor_boilerplate!(AllSup);
    impl Supervisor for DefaultSup {}
    impl Supervisor for AllSup {
        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::OneForAll
        }
    }

    /// The strategy seat #196 reserved: a supervisor property with the 2a
    /// default, overridable per supervisor TYPE. `RestartConfig` (per-child)
    /// carries no strategy field — the card's compile-visible invariant
    /// `strategy_is_supervisor_property_not_child` is held structurally by
    /// this being the only strategy surface in the crate.
    #[test]
    fn strategy_is_supervisor_property_with_one_for_one_default() {
        assert_eq!(
            DefaultSup::supervision_strategy(),
            SupervisionStrategy::OneForOne,
            "default preserves 2a behavior",
        );
        assert_eq!(
            AllSup::supervision_strategy(),
            SupervisionStrategy::OneForAll
        );
    }
}
