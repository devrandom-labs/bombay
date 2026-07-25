//! Typed error domains for the local actor spine (card #113).
//!
//! One variant = one failure domain; retryability is a *method*, never a
//! caller's guess (CLAUDE rule #3). The send path is split into two honest
//! types instead of kameo's single `SendError`:
//!
//! * [`TellError`] — fire-and-forget *delivery* failures. The message never
//!   reached the actor, so it is always handed back.
//! * [`AskError`] — a `tell` (which may fail as a [`TellError`]) followed by
//!   awaiting a reply (which may fail three further ways). Composes
//!   [`TellError`] via [`AskError::Deliver`], so an ask that fails to deliver
//!   is *literally* a delivery failure — no duplicated variants.
//!
//! This split makes illegal states unrepresentable: a `tell` caller cannot
//! even name `Timeout`/`Handler`, and whether the message is returned is
//! encoded in the type rather than left to `Option<M>`.

use std::{any::Any, fmt, sync::Arc};

use downcast_rs::{DowncastSync, impl_downcast};

/// A fire-and-forget delivery failure: the message never reached the actor.
///
/// Both variants carry the undelivered message `M` back to the caller — there
/// is nothing to lose into the void, so [`TellError::msg`] is total.
#[derive(thiserror::Error, Debug)]
pub enum TellError<M = ()> {
    /// The target actor is not alive (a stale slab key: never started, or
    /// stopped). **Terminal** — retrying can only spin.
    #[error("actor not alive")]
    ActorNotAlive(M),
    /// The actor's mailbox is full. **Retryable** — this is backpressure;
    /// nothing was delivered, so re-sending the returned message is safe.
    #[error("mailbox full")]
    MailboxFull(M),
    /// The blocking send waited its full deadline without a mailbox slot
    /// freeing. **Retryable** — the timed send owns the message for the whole
    /// wait (guaranteed handback, ADR-0008), so nothing was delivered and
    /// re-sending is safe. Distinct from [`MailboxFull`](Self::MailboxFull):
    /// the caller was *willing to wait* and the saturation outlasted it.
    #[error("send timed out")]
    SendTimeout(M),
}

impl<M> TellError<M> {
    /// `true` for the retry-safe variants, [`MailboxFull`](Self::MailboxFull)
    /// and [`SendTimeout`](Self::SendTimeout) — both mean the message bounced
    /// undelivered off a saturated mailbox.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::MailboxFull(_) | Self::SendTimeout(_))
    }

    /// `true` for the single terminal variant, [`ActorNotAlive`](Self::ActorNotAlive).
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::ActorNotAlive(_))
    }

    /// Recovers the undelivered message. Total — every delivery failure
    /// carries the message back (the module doc's "nothing to lose into the
    /// void" guarantee), so no `Option` is needed.
    #[must_use]
    pub fn msg(self) -> M {
        match self {
            Self::ActorNotAlive(m) | Self::MailboxFull(m) | Self::SendTimeout(m) => m,
        }
    }

    /// Re-types the carried message, preserving the variant.
    pub fn map_msg<N>(self, f: impl FnOnce(M) -> N) -> TellError<N> {
        match self {
            Self::ActorNotAlive(m) => TellError::ActorNotAlive(f(m)),
            Self::MailboxFull(m) => TellError::MailboxFull(f(m)),
            Self::SendTimeout(m) => TellError::SendTimeout(f(m)),
        }
    }
}

/// A request/reply failure: a delivery ([`TellError`]) followed by awaiting a
/// reply, which can fail three further ways a `tell` never can.
///
/// `E` is the actor's own domain error — kept *composed and un-erased* (the
/// opposite of ractor's `Box<dyn Error>`), so a nexus `Conflict` reaches the
/// caller typed and distinct from backpressure. Defaults to [`Infallible`] for
/// actors whose handlers cannot fail.
#[derive(thiserror::Error, Debug)]
pub enum AskError<M = (), E = Infallible> {
    /// The delivery half failed exactly as a `tell` would; carries `M` back.
    #[error(transparent)]
    Deliver(TellError<M>),
    /// The message was delivered but no reply arrived in time. **Transient**,
    /// but not retryable: the message is already in the actor.
    #[error("reply timed out")]
    Timeout,
    /// The actor accepted the message, then died before replying (its reply
    /// port was dropped). Distinct from `ActorNotAlive` (it *was* alive) and
    /// `Timeout` (no deadline elapsed).
    #[error("interrupted before reply")]
    Interrupted,
    /// The handler replied with its own domain error `E` (e.g. nexus
    /// `Conflict`). Never retryable — a retry would corrupt single-writer.
    #[error(transparent)]
    Handler(E),
}

impl<M, E> AskError<M, E> {
    /// `true` only for delivery backpressure. A `Timeout` is deliberately *not*
    /// retryable (the message is already in the actor), and a `Handler` domain
    /// error must never be re-driven as backpressure (rule #3).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Deliver(inner) if inner.is_retryable())
    }

    /// `true` only when the underlying delivery failure is terminal.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Deliver(inner) if inner.is_terminal())
    }

    /// Recovers the undelivered message: `Some` only for a `Deliver` failure
    /// (never enqueued); `None` for every reply-side failure (already in the
    /// actor, so handing it back would duplicate it).
    #[must_use]
    pub fn msg(self) -> Option<M> {
        match self {
            Self::Deliver(inner) => Some(inner.msg()),
            Self::Timeout | Self::Interrupted | Self::Handler(_) => None,
        }
    }

    /// Recovers the handler's domain error: `Some` only for `Handler`.
    #[must_use]
    pub fn err(self) -> Option<E> {
        match self {
            Self::Handler(e) => Some(e),
            Self::Deliver(_) | Self::Timeout | Self::Interrupted => None,
        }
    }

    /// Re-types the carried message (delivery failures only), preserving the
    /// variant; reply-side failures pass through untouched.
    pub fn map_msg<N>(self, f: impl FnOnce(M) -> N) -> AskError<N, E> {
        match self {
            Self::Deliver(inner) => AskError::Deliver(inner.map_msg(f)),
            Self::Timeout => AskError::Timeout,
            Self::Interrupted => AskError::Interrupted,
            Self::Handler(e) => AskError::Handler(e),
        }
    }

    /// Re-types the handler domain error (the `Handler` variant only),
    /// preserving every other variant.
    pub fn map_err<F>(self, f: impl FnOnce(E) -> F) -> AskError<M, F> {
        match self {
            Self::Deliver(inner) => AskError::Deliver(inner),
            Self::Timeout => AskError::Timeout,
            Self::Interrupted => AskError::Interrupted,
            Self::Handler(e) => AskError::Handler(f(e)),
        }
    }
}

impl<M, E> From<TellError<M>> for AskError<M, E> {
    fn from(err: TellError<M>) -> Self {
        Self::Deliver(err)
    }
}

/// The empty error type for actors whose handlers cannot fail.
///
/// A local re-export placeholder until the message/reply cards settle the
/// canonical spot; `core::convert::Infallible` has no inhabitants, so an
/// `AskError<M, Infallible>` provably never carries a `Handler`.
pub use core::convert::Infallible;

/// The bound on any value stored type-erased as a caught panic payload.
///
/// `Send + Sync` (via [`DowncastSync`]) is what lets [`PanicError`] share the
/// payload behind a plain `Arc` — no `Mutex`, no lock on downcast. `Debug` is
/// for reporting; `'static` (via `DowncastSync`) enables the downcast. Every
/// sane error type satisfies this for free through the blanket impl. `Display`
/// and `serde` are deliberately *not* required here — arbitrary panic payloads
/// cannot guarantee them; the Zenoh tier adds serde behind its feature.
pub trait ReplyError: DowncastSync + fmt::Debug {}
impl<T> ReplyError for T where T: fmt::Debug + Send + Sync + 'static {}
impl_downcast!(sync ReplyError);

/// Which phase of an actor's life produced a panic.
///
/// The distinction is load-bearing for supervision: restarting an actor that
/// panicked *during startup* just re-panics it (a crash loop), so a supervisor
/// treats a lifecycle-hook failure differently from a message-handler panic.
#[derive(thiserror::Error, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PanicReason {
    /// A message handler unwound during execution.
    #[error("message handler")]
    HandlerPanic,
    /// The `on_start` lifecycle hook failed.
    #[error("on_start hook")]
    OnStart,
    /// The `on_stop` lifecycle hook failed.
    #[error("on_stop hook")]
    OnStop,
    /// The `on_panic` lifecycle hook itself failed.
    #[error("on_panic hook")]
    OnPanic,
    /// The `on_link_died` lifecycle hook itself failed.
    #[error("on_link_died hook")]
    OnLinkDied,
}

impl PanicReason {
    /// `true` if the panic occurred in a lifecycle hook rather than a message
    /// handler — the "refuse to restart-storm" signal for a supervisor.
    #[must_use]
    pub const fn is_lifecycle_hook(self) -> bool {
        !matches!(self, Self::HandlerPanic)
    }
}

/// A caught panic, turned from an un-handleable unwind into an inspectable
/// value so a supervisor can decide restart/escalate.
///
/// The payload is stored behind a plain `Arc<dyn ReplyError>` — `Arc` so a
/// single death reason fans out to every watcher without cloning an
/// un-cloneable payload, and *no* `Mutex` because the [`ReplyError`] `Sync`
/// bound already makes the shared payload thread-safe.
#[derive(thiserror::Error, Clone, Debug)]
#[error("actor panicked ({reason}): {err:?}")]
pub struct PanicError {
    err: Arc<dyn ReplyError>,
    reason: PanicReason,
}

impl PanicError {
    /// Wraps an already-typed payload with the phase that produced it.
    #[must_use]
    pub fn new(err: Box<dyn ReplyError>, reason: PanicReason) -> Self {
        Self {
            err: Arc::from(err),
            reason,
        }
    }

    /// The phase that panicked.
    #[must_use]
    pub const fn reason(&self) -> PanicReason {
        self.reason
    }

    /// Recovers a concrete payload by type, cloned. `None` on a type mismatch.
    #[must_use]
    pub fn downcast<T: ReplyError + Clone>(&self) -> Option<T> {
        self.err.downcast_ref::<T>().cloned()
    }

    /// Calls `f` with the payload viewed as a `&str`, if it is one — the common
    /// case, since most panics carry a `&str` or `String` message.
    pub fn with_str<R>(&self, f: impl FnOnce(&str) -> R) -> Option<R> {
        self.err
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| self.err.downcast_ref::<String>().map(String::as_str))
            .map(f)
    }
    /// Builds a `PanicError` from a caught unwind payload (`catch_unwind` yields
    /// `Box<dyn Any + Send>`), tagging it with the phase that produced it.
    ///
    /// The common payloads — `&'static str` and `String` — are recovered as a
    /// string. An arbitrary payload cannot be recovered as its concrete type
    /// from `dyn Any` without naming it, so it is recorded as a stable
    /// placeholder string (still inspectable via [`with_str`](Self::with_str)).
    #[must_use]
    pub fn from_panic_any(payload: Box<dyn Any + Send>, reason: PanicReason) -> Self {
        let err: Box<dyn ReplyError> = match payload.downcast::<String>() {
            Ok(message) => Box::new(*message),
            Err(not_a_string) => match not_a_string.downcast::<&'static str>() {
                Ok(message) => Box::new(*message),
                Err(_unknown) => Box::new("non-string panic payload"),
            },
        };
        Self::new(err, reason)
    }
}

/// Why an actor stopped. Exhaustive (no `#[non_exhaustive]`, rule #3) and
/// `Clone` because a death reason fans out to every watcher.
///
/// Variant *production* is split across cards; the two variants that need
/// not-yet-built types are deferred: `LinkDied { id: ActorId, .. }` (#120/#121)
/// and `PeerDisconnected` (the Zenoh remote tier).
#[derive(thiserror::Error, Clone, Debug)]
pub enum ActorStopReason {
    /// The actor finished its work and shut down cleanly.
    #[error("stopped normally")]
    Normal,
    /// The actor was killed — a hard stop with no cleanup.
    #[error("killed")]
    Killed,
    /// A `watch`/`link` found the target already dead (or the registration was
    /// discarded by the target's teardown), so the notice is synthetic and the
    /// target's true stop reason is unknowable — the Erlang `noproc` analog.
    /// Abnormal on purpose: a linked default hook propagates it, exactly as a
    /// non-`normal` Erlang exit signal terminates a non-trapping linked peer.
    #[error("already dead when watched")]
    AlreadyDead,
    /// The actor's code panicked mid-execution.
    #[error(transparent)]
    Panicked(PanicError),
    /// A supervisor is deliberately cycling the actor.
    #[error("supervisor restart")]
    SupervisorRestart,
    /// A supervisor gave up on a child (a restart budget tripped) and is
    /// escalating by stopping itself — the microreboot ladder's next rung is
    /// whoever watches this supervisor (#196).
    #[error("restart limit exceeded for child {child:?} after {rebuilds} rebuilds")]
    RestartLimitExceeded {
        /// The child whose budget tripped.
        child: crate::mailbox::ActorId,
        /// Lifetime failures observed for that child.
        rebuilds: u32,
    },
    /// A supervisor refused to restart a child because the child died in a
    /// **lifecycle hook** (`on_start` above all): re-running that hook is a
    /// knowable crash loop, so the supervisor escalates *immediately*, without
    /// consuming any restart budget (#196).
    ///
    /// A distinct failure domain from [`RestartLimitExceeded`](Self::RestartLimitExceeded),
    /// not a `rebuilds: 0` special case of it: that variant is a budget trip
    /// *after* repeated rebuilds and its remediation is "investigate the
    /// flakiness or tune the limits"; this is a refusal to rebuild even once and
    /// its remediation is "fix the hook". One variant per failure domain (CLAUDE
    /// rule #3) — folding them would make either the `rebuilds` count or the
    /// "budget tripped" story a lie. Abnormal, like every escalation, so the
    /// supervisor's own watcher propagates it up the microreboot ladder.
    #[error("child {child:?} died in a lifecycle hook; restart refused")]
    ChildLifecycleFailed {
        /// The child whose lifecycle hook failed.
        child: crate::mailbox::ActorId,
    },
    /// A watched/linked actor died and this actor is propagating that death
    /// (a linked abnormal exit, or an explicit `Break` from `on_link_died`).
    /// `reason` is boxed (large-variant discipline — it nests a stop reason).
    #[error("linked actor {id:?} died: {reason}")]
    LinkDied {
        /// The identity of the actor that died.
        id: crate::mailbox::ActorId,
        /// Why the linked actor stopped.
        reason: Box<Self>,
    },
}

impl ActorStopReason {
    /// `true` for an *expected* stop (leave it dead / it is being cycled),
    /// `false` for an abnormal one (kill, panic). The one bit a supervisor
    /// branches on.
    #[must_use]
    pub const fn is_normal(&self) -> bool {
        matches!(self, Self::Normal | Self::SupervisorRestart)
    }
}

/// A [`Registry::register`](crate::registry::Registry::register) collision:
/// the name is already held by a **live** actor.
///
/// A bare struct, not an enum variant — register has exactly one failure
/// domain (CLAUDE rule: a single-domain fallible op returns its bare error).
/// Dead incumbents never produce this: their name is reclaimed atomically by
/// the same `register` call. Terminal for *this* name — retrying without an
/// intervening unregister or incumbent death only spins.
#[derive(thiserror::Error, Clone, Copy, Debug, PartialEq, Eq)]
#[error("name is already registered to a live actor")]
pub struct NameTaken;

/// A [`watch`](crate::actor::ActorRef::watch)/[`link`](crate::actor::ActorRef::link)
/// call on a handle whose actor was **not** spawned via `spawn_linked`
///
/// — it has no link channel to receive death notices on, so it cannot watch.
///
/// A caller mistake (spawn a `Watch` actor via plain `spawn`), surfaced as a
/// typed `Result` rather than a panic. A compile-time typestate (a
/// `LinkedActorRef` witness returned by `spawn_linked`) exists on stable and
/// was rejected on cost — handle bifurcation infects `Recipient`, the registry,
/// and #121 — not possibility: ADR-0011.
#[derive(thiserror::Error, Clone, Copy, Debug, PartialEq, Eq)]
#[error("actor was not spawned linked; it cannot watch")]
pub struct ActorNotLinked;

/// A [`Registry::lookup`](crate::registry::Registry::lookup) type conflict:
/// the name resolves to a **live** actor of a different `Actor` type than the
/// one requested.
///
/// Distinct from absence (`Ok(None)`) — the name is genuinely occupied, the
/// caller asked with the wrong type. Dead incumbents never produce this: a
/// dead entry reads as absent on every path, whatever its type.
#[derive(thiserror::Error, Clone, Copy, Debug, PartialEq, Eq)]
#[error("name is registered to an actor of a different type")]
pub struct WrongActorType;

#[cfg(test)]
mod tests {
    use super::*;

    /// Delivery failures classify by *retry safety*, not by name. `MailboxFull`
    /// is backpressure — the message bounced, nothing was delivered, so a retry
    /// is safe. `ActorNotAlive` is terminal — the target is gone; a retry loop
    /// would spin forever. A blind retry loop must be able to tell them apart
    /// from the type alone (CLAUDE rule #3).
    #[test]
    fn tell_error_classifies_retry_safety() {
        let full: TellError<u8> = TellError::MailboxFull(1);
        assert!(full.is_retryable(), "backpressure is retryable");
        assert!(!full.is_terminal(), "backpressure is not terminal");

        let gone: TellError<u8> = TellError::ActorNotAlive(1);
        assert!(!gone.is_retryable(), "a dead actor is never retryable");
        assert!(gone.is_terminal(), "a dead actor is terminal");
    }

    /// Card #118 (deferred from #113): a blocking send that waits its whole
    /// deadline without a slot freeing is `SendTimeout(M)`. The timed send owns
    /// the message for the entire wait (guaranteed handback, ADR-0008), so the
    /// message was **definitely never delivered**: retryable, not terminal, and
    /// the exact message comes back — through `msg` and through `map_msg`.
    #[test]
    fn send_timeout_classifies_retryable_with_msg_back() {
        let timed_out: TellError<u8> = TellError::SendTimeout(7);
        assert!(
            timed_out.is_retryable(),
            "never delivered, so a retry is safe"
        );
        assert!(
            !timed_out.is_terminal(),
            "saturation is transient, not terminal"
        );
        assert_eq!(timed_out.msg(), 7, "the exact message is handed back");

        let retyped = TellError::SendTimeout(7u8).map_msg(u16::from);
        assert!(
            matches!(retyped, TellError::SendTimeout(7u16)),
            "map_msg preserves the variant and value, got {retyped:?}"
        );
    }

    /// `TellError::msg` is *total* (the module doc's claim): every variant
    /// carries the undelivered message, so recovery never needs an `Option`.
    #[test]
    fn tell_error_msg_recovers_from_every_variant() {
        assert_eq!(TellError::ActorNotAlive(1u8).msg(), 1);
        assert_eq!(TellError::MailboxFull(2u8).msg(), 2);
        assert_eq!(TellError::SendTimeout(3u8).msg(), 3);
    }

    /// A stand-in domain error, e.g. the shape a nexus aggregate's own
    /// `thiserror` enum takes (optimistic-concurrency `Conflict`, …). A proper
    /// `Error` (so it can sit behind `#[error(transparent)]`) and `Clone` (so it
    /// can be recovered from a type-erased [`PanicError`]).
    #[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
    #[error("optimistic-concurrency conflict")]
    struct Conflict;

    /// The reply half adds three failures a `tell` can never have, and *only*
    /// backpressure — reached via [`AskError::Deliver`] — is retryable. In
    /// particular a `Timeout` is **not** retryable (the message is already in
    /// the actor; a re-send would double-process), and a `Handler` domain
    /// error (where a nexus `Conflict` lives) must never be retried as
    /// backpressure or the single-writer guarantee is corrupted (rule #3).
    #[test]
    fn ask_error_classifies_retry_safety() {
        let full: AskError<u8, Conflict> = AskError::Deliver(TellError::MailboxFull(1));
        assert!(full.is_retryable(), "delivery backpressure stays retryable");
        assert!(!full.is_terminal());

        let gone: AskError<u8, Conflict> = AskError::Deliver(TellError::ActorNotAlive(1));
        assert!(!gone.is_retryable());
        assert!(
            gone.is_terminal(),
            "a dead actor is terminal through Deliver"
        );

        for reply_side in [
            AskError::<u8, Conflict>::Timeout,
            AskError::<u8, Conflict>::Interrupted,
            AskError::<u8, Conflict>::Handler(Conflict),
        ] {
            assert!(
                !reply_side.is_retryable(),
                "reply-side failures never retry"
            );
            assert!(
                !reply_side.is_terminal(),
                "reply-side failures are not terminal"
            );
        }
    }

    /// Whether the message comes back is encoded in the variant, not an
    /// `Option<M>` guess: delivery failures return `Some(M)` (never enqueued);
    /// reply-side failures return `None` (already in the actor — handing it back
    /// would duplicate it). The domain error is recoverable only from `Handler`.
    #[test]
    fn ask_error_recovers_message_and_error() {
        assert_eq!(
            AskError::<u8, Conflict>::Deliver(TellError::MailboxFull(9)).msg(),
            Some(9)
        );
        assert_eq!(
            AskError::<u8, Conflict>::Deliver(TellError::ActorNotAlive(9)).msg(),
            Some(9)
        );
        assert_eq!(AskError::<u8, Conflict>::Timeout.msg(), None);
        assert_eq!(AskError::<u8, Conflict>::Interrupted.msg(), None);
        assert_eq!(AskError::<u8, Conflict>::Handler(Conflict).msg(), None);

        assert_eq!(
            AskError::<u8, Conflict>::Handler(Conflict).err(),
            Some(Conflict)
        );
        assert_eq!(AskError::<u8, Conflict>::Timeout.err(), None);
        assert_eq!(
            AskError::<u8, Conflict>::Deliver(TellError::MailboxFull(9)).err(),
            None
        );
    }

    /// An ask *is* a tell then a wait, so a delivery failure converts into an
    /// `AskError` with a bare `?` — no per-variant re-mapping.
    #[test]
    fn ask_error_composes_from_tell_error() {
        fn deliver() -> Result<(), TellError<u8>> {
            Err(TellError::MailboxFull(3))
        }
        fn ask() -> Result<(), AskError<u8, Conflict>> {
            deliver()?;
            Ok(())
        }
        assert!(matches!(
            ask(),
            Err(AskError::Deliver(TellError::MailboxFull(3)))
        ));
    }

    /// `@bug` — a nexus optimistic-concurrency `Conflict` is a *domain* answer,
    /// surfaced as `Handler(Conflict)`, and must classify as **not retryable**.
    /// This test FAILS if `Conflict` is ever conflated with a retryable code:
    /// a caller's retry loop would silently re-drive the conflict as
    /// backpressure and corrupt the single-writer guarantee (rule #3).
    #[test]
    fn conflict_is_domain_not_retryable() {
        let conflict: AskError<u8, Conflict> = AskError::Handler(Conflict);
        assert!(
            !conflict.is_retryable(),
            "a domain Conflict must never retry"
        );
        assert!(
            !conflict.is_terminal(),
            "a Conflict is a live answer, not a dead actor"
        );
        assert_eq!(
            conflict.err(),
            Some(Conflict),
            "the typed error survives, un-erased"
        );
    }

    /// The message payload can be re-typed on failure without collapsing the
    /// variant — the reactivation layer (#20) uses this to re-wrap a returned
    /// message. Reply-side failures have no message, so `map_msg` is a no-op there.
    #[test]
    fn map_msg_retypes_carried_message() {
        let mapped = TellError::MailboxFull(7u8).map_msg(|m| u32::from(m) + 1);
        assert!(matches!(mapped, TellError::MailboxFull(8u32)));

        let via_ask: AskError<u8, Conflict> = AskError::Deliver(TellError::ActorNotAlive(7));
        assert_eq!(via_ask.map_msg(u32::from).msg(), Some(7u32));

        // Reply-side: nothing to map, variant preserved.
        let timeout: AskError<u8, Conflict> = AskError::Timeout;
        assert!(matches!(timeout.map_msg(u32::from), AskError::Timeout));
    }

    /// The domain error can be re-typed independently of the message — used at
    /// the boundary where a caller adapts an aggregate's error into its own.
    #[test]
    fn map_err_retypes_handler_error() {
        let mapped = AskError::<u8, Conflict>::Handler(Conflict).map_err(|_| "conflict");
        assert_eq!(mapped.err(), Some("conflict"));

        let full: AskError<u8, Conflict> = AskError::Deliver(TellError::MailboxFull(1));
        assert!(matches!(
            full.map_err(|_| "x"),
            AskError::Deliver(TellError::MailboxFull(1))
        ));
    }

    /// A supervisor restarting a startup-panicking actor just re-panics it —
    /// an instant crash loop. So `PanicReason` distinguishes a *lifecycle-hook*
    /// failure (safe to refuse restart) from a plain message-handler panic.
    #[test]
    fn panic_reason_flags_lifecycle_hooks() {
        assert!(PanicReason::OnStart.is_lifecycle_hook());
        assert!(PanicReason::OnStop.is_lifecycle_hook());
        assert!(PanicReason::OnPanic.is_lifecycle_hook());
        assert!(
            !PanicReason::HandlerPanic.is_lifecycle_hook(),
            "a handler panic is runtime, not lifecycle"
        );
    }

    /// The one bit every supervisor branches on: was this an *expected* stop
    /// (leave it dead) or an abnormal one? `SupervisorRestart` counts as normal
    /// (the supervisor is deliberately cycling it); `Killed` does not (operator
    /// pulled the plug) and neither does a panic.
    #[test]
    fn stop_reason_is_normal_classification() {
        assert!(ActorStopReason::Normal.is_normal());
        assert!(ActorStopReason::SupervisorRestart.is_normal());
        assert!(!ActorStopReason::Killed.is_normal());
        assert!(
            !ActorStopReason::Panicked(PanicError::new(
                Box::new("boom"),
                PanicReason::HandlerPanic
            ))
            .is_normal()
        );
    }

    /// A panic payload is genuinely arbitrary, so it is stored type-erased and
    /// recovered by trying known types. The overwhelmingly common panic — a
    /// string — is recoverable as `&str`, and the phase is preserved verbatim.
    #[test]
    fn panic_error_recovers_str_and_reason() {
        let panic = PanicError::new(Box::new(String::from("kaboom")), PanicReason::OnStart);
        assert_eq!(panic.with_str(str::to_owned), Some(String::from("kaboom")));
        assert_eq!(panic.reason(), PanicReason::OnStart);
    }

    /// A non-string payload is recovered by concrete type — this is what a
    /// panic-probe test asserts on (rule #8: the specific value, not a Debug
    /// substring). A mismatched type yields `None`.
    #[test]
    fn panic_error_downcasts_to_concrete_type() {
        let panic = PanicError::new(Box::new(Conflict), PanicReason::HandlerPanic);
        assert_eq!(panic.downcast::<Conflict>(), Some(Conflict));
        assert_eq!(panic.with_str(str::to_owned), None, "not a string payload");
    }

    /// A death reason fans out to every watcher, so `PanicError` is `Clone` —
    /// and the clone shares the same `Arc`'d payload rather than duplicating it.
    #[test]
    fn panic_error_clone_shares_payload() {
        let original = PanicError::new(Box::new(Conflict), PanicReason::OnPanic);
        let cloned = original.clone();
        assert_eq!(cloned.downcast::<Conflict>(), Some(Conflict));
        assert_eq!(cloned.reason(), PanicReason::OnPanic);
        // original still usable — clone did not consume it.
        assert_eq!(original.reason(), PanicReason::OnPanic);
    }

    /// A caught panic arrives as `Box<dyn Any + Send>` from `catch_unwind`. The two
    /// common payloads — `&'static str` and `String` — are recovered as a string;
    /// the phase is preserved. This is the loop's bridge from an unwind to a value.
    #[test]
    fn from_panic_any_recovers_string_payloads() {
        let from_str = PanicError::from_panic_any(Box::new("boom"), PanicReason::HandlerPanic);
        assert_eq!(from_str.with_str(str::to_owned), Some(String::from("boom")));
        assert_eq!(from_str.reason(), PanicReason::HandlerPanic);

        let from_string =
            PanicError::from_panic_any(Box::new(String::from("kaboom")), PanicReason::OnStart);
        assert_eq!(
            from_string.with_str(str::to_owned),
            Some(String::from("kaboom"))
        );
        assert_eq!(from_string.reason(), PanicReason::OnStart);
    }

    /// A non-string panic payload (an arbitrary type) cannot be recovered as its
    /// concrete type from `dyn Any` without knowing it, so `from_panic_any` records
    /// a stable placeholder string and preserves the phase. The placeholder must be
    /// a recoverable `&str`, so a supervisor can still log *something*.
    #[test]
    fn from_panic_any_records_placeholder_for_non_string_payload() {
        let panic = PanicError::from_panic_any(Box::new(42_u64), PanicReason::OnPanic);
        assert_eq!(panic.reason(), PanicReason::OnPanic);
        assert_eq!(
            panic.with_str(str::to_owned),
            Some(String::from("non-string panic payload")),
        );
    }

    /// Display strings are public surface (they show up in logs and `?` chains),
    /// so pin them. `Deliver`/`Handler` are transparent — they delegate to the
    /// inner error's own message rather than inventing a wrapper line.
    #[test]
    fn error_display_messages_are_stable() {
        assert_eq!(
            TellError::<()>::ActorNotAlive(()).to_string(),
            "actor not alive"
        );
        assert_eq!(TellError::<()>::MailboxFull(()).to_string(), "mailbox full");

        assert_eq!(
            AskError::<(), Conflict>::Timeout.to_string(),
            "reply timed out"
        );
        assert_eq!(
            AskError::<(), Conflict>::Interrupted.to_string(),
            "interrupted before reply"
        );
        assert_eq!(
            AskError::<(), Conflict>::Deliver(TellError::MailboxFull(())).to_string(),
            "mailbox full",
            "Deliver is transparent — shows the delivery reason, not a wrapper"
        );
        assert_eq!(
            AskError::<(), Conflict>::Handler(Conflict).to_string(),
            "optimistic-concurrency conflict",
            "Handler is transparent — the domain error's own message"
        );

        assert_eq!(ActorStopReason::Normal.to_string(), "stopped normally");
        assert_eq!(ActorStopReason::Killed.to_string(), "killed");
        assert_eq!(
            ActorStopReason::SupervisorRestart.to_string(),
            "supervisor restart"
        );
        assert_eq!(PanicReason::OnStart.to_string(), "on_start hook");
    }

    #[test]
    fn on_link_died_is_a_lifecycle_hook() {
        // A hook panic must not be treated as a restartable handler crash (slice 2).
        assert!(PanicReason::OnLinkDied.is_lifecycle_hook());
    }

    /// The synthetic link-to-dead reason (Erlang's `noproc` analog) is its own
    /// failure domain: a watcher/supervisor must be able to distinguish "the
    /// target was already dead when the edge was installed (true reason
    /// unknowable)" from a real hard [`Killed`]. It is abnormal (a linked
    /// default hook must propagate it, as Erlang's non-normal `noproc` does).
    #[test]
    fn already_dead_is_abnormal_and_distinct_from_killed() {
        assert!(!ActorStopReason::AlreadyDead.is_normal());
        assert_eq!(
            ActorStopReason::AlreadyDead.to_string(),
            "already dead when watched"
        );
        assert!(
            !matches!(ActorStopReason::AlreadyDead, ActorStopReason::Killed),
            "one variant per failure domain: already-dead is not a kill",
        );
    }

    /// A supervisor that gave up on a child stops ITSELF, and that stop must be
    /// abnormal: the microreboot ladder's next rung is whoever watches the
    /// supervisor, and a linked default hook only propagates a non-normal death.
    /// Were this classified normal, an exhausted restart budget would stop the
    /// supervisor silently and the failure would end there.
    #[test]
    fn restart_limit_exceeded_is_abnormal() {
        let reason = ActorStopReason::RestartLimitExceeded {
            child: crate::mailbox::ActorId::new(7),
            rebuilds: 6,
        };
        assert!(
            !reason.is_normal(),
            "an escalating supervisor is an abnormal stop — its own watcher must propagate"
        );
        assert_eq!(
            reason.to_string(),
            "restart limit exceeded for child ActorId(7) after 6 rebuilds",
        );
    }

    /// A lifecycle-hook escalation is a DISTINCT failure domain from a budget
    /// trip (#196): it refuses to rebuild even once, carries no rebuild count,
    /// and is abnormal so the supervisor's own watcher propagates it. Fails if
    /// it is ever conflated with [`ActorStopReason::RestartLimitExceeded`].
    #[test]
    fn child_lifecycle_failed_is_abnormal_and_distinct_from_a_budget_trip() {
        let reason = ActorStopReason::ChildLifecycleFailed {
            child: crate::mailbox::ActorId::new(3),
        };
        assert!(
            !reason.is_normal(),
            "a hook escalation is an abnormal stop — its watcher must propagate"
        );
        assert_eq!(
            reason.to_string(),
            "child ActorId(3) died in a lifecycle hook; restart refused",
        );
        assert!(
            !matches!(reason, ActorStopReason::RestartLimitExceeded { .. }),
            "one variant per failure domain: a hook refusal is not a budget trip",
        );
    }

    #[test]
    fn link_died_is_abnormal() {
        // LinkDied must be able to propagate: it is NOT a normal stop.
        let reason = ActorStopReason::LinkDied {
            id: crate::mailbox::ActorId::new(1),
            reason: Box::new(ActorStopReason::Killed),
        };
        assert!(!reason.is_normal());
    }
}
