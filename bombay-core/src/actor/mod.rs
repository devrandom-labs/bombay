//! The local actor spine (card #116): the `Actor` trait, its lifecycle hooks,
//! the run-loop that drives it, and the spawn entry points.
//!
//! Send-saturated for now; the cfg-gated `MaybeSend` relaxation for
//! single-threaded client builds is a dedicated later sweep (#9). The `ActorRef`
//! here is a **minimal scaffold** — ref-count-driven stop, `Recipient` erasure,
//! and the `tell`/`ask` builders are #117/#118.

use core::{any::type_name, future::Future, ops::ControlFlow};

use crate::{
    actor::spawn::default_capacity,
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::{ActorId, Capacity, Mailboxed},
    message::Msg,
};

mod actor_ref;
mod kind;
mod recipient;
mod spawn;

pub use self::{
    actor_ref::{ActorRef, WeakActorRef},
    recipient::{Recipient, RecipientAskRequest, ReplyRecipient, WeakRecipient},
    spawn::{DEFAULT_MAILBOX_CAPACITY, PreparedActor, RunResult},
};

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

    /// Handles one message. Set `*stop = true` to stop the actor cleanly after
    /// this handler returns `Ok`. A returned `Err` is treated as a controlled
    /// crash (routed to `on_panic`, then stop).
    fn handle(
        &mut self,
        msg: Self::Msg,
        actor_ref: ActorRef<Self>,
        stop: &mut bool,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

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
pub trait Spawn: Actor {
    /// Spawns with the [`DEFAULT_MAILBOX_CAPACITY`](spawn::DEFAULT_MAILBOX_CAPACITY).
    #[must_use]
    fn spawn(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_with_capacity(default_capacity(), args)
    }

    /// Spawns with an explicit mailbox `capacity`.
    #[must_use]
    fn spawn_with_capacity(capacity: Capacity, args: Self::Args) -> ActorRef<Self> {
        let prepared = PreparedActor::<Self>::new(capacity);
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
    /// Spawns a linked actor with the
    /// [`DEFAULT_MAILBOX_CAPACITY`](spawn::DEFAULT_MAILBOX_CAPACITY).
    #[must_use]
    fn spawn_linked(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_linked_with_capacity(default_capacity(), args)
    }

    /// Spawns a linked actor with an explicit mailbox `capacity`.
    #[must_use]
    fn spawn_linked_with_capacity(capacity: Capacity, args: Self::Args) -> ActorRef<Self> {
        let (prepared, link_rx) = PreparedActor::<Self>::new_linked(capacity);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn_linked_task(args, link_rx);
        actor_ref
    }
}

impl<A: Watch> SpawnLinked for A {}

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
        async fn handle(
            &mut self,
            _: M,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
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
            .on_link_died(ActorId::new(1), ActorStopReason::Killed, true)
            .await
            .expect("infallible default hook");
        assert!(
            matches!(out, ControlFlow::Break(ActorStopReason::LinkDied { .. })),
            "linked + abnormal must propagate, got {out:?}",
        );

        let out = w
            .on_link_died(ActorId::new(1), ActorStopReason::Killed, false)
            .await
            .expect("infallible default hook");
        assert!(
            matches!(out, ControlFlow::Continue(())),
            "watch (linked=false) + abnormal is notify-only, got {out:?}",
        );

        let out = w
            .on_link_died(ActorId::new(1), ActorStopReason::Normal, true)
            .await
            .expect("infallible default hook");
        assert!(
            matches!(out, ControlFlow::Continue(())),
            "linked + normal does not propagate, got {out:?}",
        );
    }
}
