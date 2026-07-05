//! The local actor spine (card #116): the `Actor` trait, its lifecycle hooks,
//! the run-loop that drives it, and the spawn entry points.
//!
//! Send-saturated for now; the cfg-gated `MaybeSend` relaxation for
//! single-threaded client builds is a dedicated later sweep (#9). The `ActorRef`
//! here is a **minimal scaffold** — ref-count-driven stop, `Recipient` erasure,
//! and the `tell`/`ask` builders are #117/#118.

use core::{any::type_name, future::Future};

use crate::{
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::Mailboxed,
    message::Msg,
};

mod actor_ref;

pub use self::actor_ref::{ActorRef, WeakActorRef};

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
