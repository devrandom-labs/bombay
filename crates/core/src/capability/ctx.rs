//! The one typed window a handler sees: [`Ctx`].

use super::{Actor, Handle, Provide};

/// The one typed window a handler sees.
///
/// Capability access + the actor's own handle. Its reachable surface is
/// exactly what [`Actor::Caps`] declares — there is deliberately NO
/// runtime-checked accessor (ADR-0026 constraint 1).
pub struct Ctx<'a, A: Actor> {
    pub(super) caps: &'a mut A::Caps,
    pub(super) self_ref: &'a Handle<A>,
}

impl<A: Actor> Ctx<'_, A> {
    /// Reaches capability `C`. Compile-gated: exists only when the cap
    /// set provides `C`.
    ///
    /// ```compile_fail
    /// use bombay::capability::{Actor, Ctx};
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
