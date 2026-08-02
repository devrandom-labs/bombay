//! The one user trait of the distilled surface ([`Actor`]) and the cap-set
//! seams it hangs off: [`CapSet`] (build) and [`Provide`] (access).

use core::{any::type_name, future::Future};

use crate::{
    actor::{Flow, WeakActorRef},
    error::{ActorStopReason, PanicError, ReplyError},
    message::Msg,
};

use super::{Admission, Ctx, DeadlineHook, Replay, SelectRunner, Shell};

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
    /// a [`Stashing`](super::Stashing) field drains it — [`SelectRunner<Self>`](SelectRunner)
    /// so every cap set names its run-loop shape at compile time (stage 3),
    /// [`DeadlineHook<Self>`](DeadlineHook) so every loop shape can poll
    /// the ADR-0025 deadline plane uniformly (stage 4) — `()` and
    /// deadline-less sets stay disabled — and [`Admission<Self>`](Admission)
    /// so a [`Phased`](super::Phased) gate classifies every delivered or replayed message
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
    /// `Ok(Flow::Stop(Normal))` stops cleanly after this handler, `Err` is a
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

#[cfg(test)]
mod tests {
    use core::any::type_name;

    use super::super::fixtures::Nameless;
    use super::{Actor, CapSet};

    /// Kills the `Actor::name` default's ""/"xyzzy" mutants: the default
    /// is exactly the type name.
    #[test]
    fn caps_actor_name_defaults_to_type_name() {
        assert_eq!(<Nameless as Actor>::name(), type_name::<Nameless>());
    }

    /// The unit cap set builds from any args — the plain-actor floor.
    #[test]
    fn unit_capset_builds() {
        <() as CapSet<Nameless>>::build(&());
    }
}
