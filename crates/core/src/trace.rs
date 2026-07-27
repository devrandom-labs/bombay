//! Actor-lifecycle instrumentation (card #209): the `tracing` feature's SINGLE
//! cfg surface. Call sites are cfg-free — this module swaps the real
//! implementation for inert no-ops when the feature is off, so an off build
//! compiles every span and event out (gate check `bombay-tracing-off`).
//!
//! Span model (spec D5/D6): one root `actor.lifecycle` span per actor that
//! `follows_from` the spawn site (the actor OUTLIVES its spawner — a link, not
//! a parent), with `stop.reason` recorded at teardown; one `actor.handle` span
//! per message, parented to the caller's span captured at enqueue.

#[cfg(feature = "tracing")]
mod imp {
    use core::future::Future;
    use core::time::Duration;

    use tracing::Instrument as _;

    use crate::{actor::Actor, error::ActorStopReason, error::PanicError, id::ActorId};

    pub use tracing::Span;

    /// Caller-side trace context captured at enqueue time.
    ///
    /// The sender's current span rides the mailbox envelope so the handler's
    /// span parents to it and cross-actor traces stitch into one tree. A ZST
    /// when `tracing` is off.
    pub struct SendContext {
        caller: tracing::Span,
    }

    impl SendContext {
        /// Captures the sender's current span (zero-cost with no subscriber).
        #[must_use]
        pub fn capture() -> Self {
            Self {
                caller: tracing::Span::current(),
            }
        }

        /// The per-message `actor.handle` span: parented to the captured caller
        /// span when enabled, else contextually to the lifecycle span.
        #[expect(
            dead_code,
            reason = "wired by the caller-span propagation slice of #209"
        )]
        pub(crate) fn handle_span<A: Actor>(&self) -> Span {
            if self.caller.is_disabled() {
                tracing::debug_span!(
                    "actor.handle",
                    actor.name = A::name(),
                    msg.kind = core::any::type_name::<A::Msg>(),
                )
            } else {
                tracing::debug_span!(
                    parent: &self.caller,
                    "actor.handle",
                    actor.name = A::name(),
                    msg.kind = core::any::type_name::<A::Msg>(),
                )
            }
        }
    }

    /// The per-actor root span. `parent: None` is load-bearing: without it the
    /// span would contextually parent to the spawn site, nesting the actor's
    /// whole lifetime under it.
    pub fn lifecycle_span<A: Actor>(id: ActorId) -> Span {
        let span = tracing::info_span!(
            parent: None,
            "actor.lifecycle",
            actor.name = A::name(),
            actor.id = ?id,
            stop.reason = tracing::field::Empty,
        );
        span.follows_from(tracing::Span::current().id());
        span
    }

    /// Attaches `span` to `fut`, entered per-poll (never across an await).
    pub fn instrument<F: Future>(fut: F, span: Span) -> tracing::instrument::Instrumented<F> {
        fut.instrument(span)
    }

    /// Records the terminal stop reason onto the current (lifecycle) span.
    ///
    /// MUST run directly inside the instrumented lifecycle future with no
    /// nested span entered: `record` on a span that never declared the field
    /// is a SILENT no-op in tracing, so recording from under a child span
    /// drops the field without any error. Kill and startup-failure paths never
    /// reach this, so their lifecycle spans close with `stop.reason` empty —
    /// deliberate (the span itself still marks the lifetime).
    pub fn record_stop_reason(reason: &ActorStopReason) {
        tracing::Span::current().record("stop.reason", tracing::field::display(reason));
    }

    pub fn spawned() {
        tracing::trace!("actor spawned");
    }

    pub fn on_start_ok() {
        tracing::trace!("actor started");
    }

    pub fn on_start_failed(err: &PanicError) {
        tracing::error!(?err, "on_start failed");
    }

    pub fn on_stop_ok(reason: &ActorStopReason) {
        tracing::trace!(%reason, "actor stopped");
    }

    pub fn on_stop_failed<E: core::fmt::Debug>(reason: &ActorStopReason, err: &E) {
        tracing::error!(%reason, ?err, "on_stop returned an error");
    }

    pub fn on_stop_panicked(reason: &ActorStopReason) {
        tracing::error!(%reason, "on_stop panicked");
    }

    pub fn on_stop_abandoned(reason: &ActorStopReason, grace: Duration) {
        tracing::error!(%reason, ?grace, "on_stop exceeded the notice grace and was abandoned");
    }
}

#[cfg(not(feature = "tracing"))]
mod imp {
    use core::future::Future;
    use core::time::Duration;

    use crate::{actor::Actor, error::ActorStopReason, error::PanicError, id::ActorId};

    /// Inert stand-in for `tracing::Span` in an off build.
    pub struct Span;

    /// Inert stand-in: a ZST with the same API as the tracing-on version.
    pub struct SendContext;

    impl SendContext {
        #[must_use]
        pub const fn capture() -> Self {
            Self
        }

        #[expect(
            clippy::unused_self,
            reason = "mirrors the tracing-on API so call sites stay cfg-free"
        )]
        #[expect(
            dead_code,
            reason = "wired by the caller-span propagation slice of #209"
        )]
        #[expect(
            clippy::extra_unused_type_parameters,
            reason = "mirrors the tracing-on signature so call sites stay cfg-free"
        )]
        pub(crate) const fn handle_span<A: Actor>(&self) -> Span {
            Span
        }
    }

    #[expect(
        clippy::extra_unused_type_parameters,
        reason = "mirrors the tracing-on signature so call sites stay cfg-free"
    )]
    pub const fn lifecycle_span<A: Actor>(_id: ActorId) -> Span {
        Span
    }

    pub const fn instrument<F: Future>(fut: F, _span: Span) -> F {
        fut
    }

    pub const fn record_stop_reason(_reason: &ActorStopReason) {}
    pub const fn spawned() {}
    pub const fn on_start_ok() {}
    pub const fn on_start_failed(_err: &PanicError) {}
    pub const fn on_stop_ok(_reason: &ActorStopReason) {}
    pub const fn on_stop_failed<E: core::fmt::Debug>(_reason: &ActorStopReason, _err: &E) {}
    pub const fn on_stop_panicked(_reason: &ActorStopReason) {}
    pub const fn on_stop_abandoned(_reason: &ActorStopReason, _grace: Duration) {}
}

pub use imp::SendContext;
pub use imp::{
    instrument, lifecycle_span, on_start_failed, on_start_ok, on_stop_abandoned, on_stop_failed,
    on_stop_ok, on_stop_panicked, record_stop_reason, spawned,
};
