//! The shared verdict vocabulary of the capability surface: [`Never`],
//! [`Normal`], [`Deferred`], [`Disposition`], the ONE [`Step`] family (with
//! its [`Flow`] handler corner), and [`Overflow`] (ADR-0024 / ADR-0028 /
//! ADR-0029).

/// The uninhabited type with two structural jobs (ADR-0028).
///
/// As a phase: [`Step<Never>`](Step) has no constructible `Goto` — a plain
/// actor is a one-phase machine, which is how one
/// [`DeadlinePolicy`](super::DeadlinePolicy) trait serves both [`Deadlined`](super::Deadlined) and
/// [`Phased`](super::Phased), and how [`Flow`] is a corner of [`Step`] rather than
/// its own dialect (ADR-0029). As a defer token: [`Disposition<Never>`](Disposition) has
/// no constructible `Defer` — a machine that plugged [`NoDefer`](super::NoDefer) cannot
/// even SPELL a deferral; the law is the type, not a convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Never {}

/// The [`Bounded`](super::Bounded) deferral seat's gate token: `Disposition::Defer(Deferred)`.
///
/// Constructible only because the seat is plugged — its existence in a
/// gate's verdict type IS the deferral declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deferred;

/// Per-phase message admission — the P trio (ADR-0024 D5, P PLDI 2013
/// `defer`/`ignore` made payload-capable): what the framework does with a
/// message BEFORE the handler is consulted.
///
/// Generic over the deferral token `D` (default [`Never`]): a gate whose
/// policy plugged [`NoDefer`](super::NoDefer) returns plain `Disposition` and cannot
/// construct `Defer`; a [`Bounded`](super::Bounded) policy's gate returns
/// `Disposition<Deferred>` (ADR-0028).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition<D = Never> {
    /// Hand it to `handle` now.
    Deliver,
    /// Stash it; re-gate on every phase change (P `defer`). Carries the
    /// plugged seat's token — the declaration rides the verdict type.
    Defer(D),
    /// Drop it deliberately, by declaration (P `ignore` — recorded
    /// intent, never a silent loss).
    Ignore,
}

/// The one spellable stop reason at the handler and seat corners of
/// [`Step`] (ADR-0029) — a unit witness, not an
/// [`ActorStopReason`](crate::error::ActorStopReason).
///
/// ADR-0023's law survives as a type: a self-stop's only honest reason is
/// `Normal`, so the reason position holds a type with exactly one
/// inhabitant — fabricating `Killed`/`Panicked`/`LinkDied` is
/// unrepresentable, not discouraged. The run loop discharges this marker
/// to `ActorStopReason::Normal` at its single consumption point.
///
/// The law, pinned both ways: a handler-corner verdict cannot carry a
/// runtime reason —
///
/// ```compile_fail,E0308
/// use bombay::actor::Flow;
/// use bombay::error::ActorStopReason;
///
/// let _: Flow = Flow::Stop(ActorStopReason::Killed);
/// ```
///
/// while the one honest reason is exactly spellable:
///
/// ```
/// use bombay::actor::{Flow, Normal};
///
/// let _: Flow = Flow::Stop(Normal);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Normal;

/// THE verdict family (ADR-0029): the one type every reaction answers in.
///
/// "Keep going, switch behavior, or stop?" — typed `become` (Agha):
/// become(same) / closed-menu become / become(⊥) with an exit code.
///
/// Both parameters default to the plain-actor corner, and each corner is
/// carved by what its parameters make unconstructible:
///
/// - [`Flow`] = `Step<Never, Normal>` — handlers, [`DeadlineHook`](super::DeadlineHook),
///   [`Admitted::Absorbed`](super::Admitted::Absorbed): no `Goto`, no reason but [`Normal`];
/// - `Step<Ph>` = `Step<Ph, Normal>` — policy seats
///   ([`DeadlinePolicy`](super::DeadlinePolicy), [`Overflow::Handled`](Overflow::Handled)): no reason but
///   [`Normal`];
/// - `Step<Never, ActorStopReason>` — the watch corner
///   ([`WatchPolicy`](super::WatchPolicy)): `Stop` PROPAGATES a reason that already
///   exists (the peer's death); no `Goto`.
///
/// `Copy` follows the parameters: seat verdicts stay `Copy`; only the
/// watch corner (boxed nested reason) is move-only.
///
/// `Goto(current)` is deliberately a no-op (`gen_statem` `next_state` to
/// the same state): no unstash, no deadline reset. In `handle`, the
/// transition verb is [`Phased::goto`](super::Phased::goto) instead — recorded there,
/// committed by the framework only after the handler returns `Ok`, so
/// D3's commit-after-Ok law holds with no `Step` return channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<Ph = Never, R = Normal> {
    /// Keep the current behavior (`gen_statem` `keep_state`); poll for
    /// the next event.
    Continue,
    /// Transition (no-op when already there).
    Goto(Ph),
    /// Stop after this reaction, with the corner's reason: the [`Normal`]
    /// witness at the handler/seat corners (backlog abandoned, `on_stop`
    /// runs — ADR-0023), a carried
    /// [`ActorStopReason`](crate::error::ActorStopReason) at the watch corner.
    Stop(R),
}

/// The handler plane's corner of [`Step`] (ADR-0023 name, ADR-0029
/// shape): `Continue | Stop(Normal)` — `Goto` is uninhabited and no
/// reason but [`Normal`] is spellable.
///
/// Re-exported at [`actor::Flow`](crate::actor::Flow), where ADR-0023 put it.
pub type Flow = Step;

/// The [`PhasePolicy::on_defer_full`](super::PhasePolicy::on_defer_full) verdict — ADR-0024 D6's overflow
/// handback, never a silent drop.
#[derive(Debug)]
pub enum Overflow<M, Ph> {
    /// Deliver the overflowed message to `handle` after all — the
    /// default: visible-but-unrefused shedding.
    Redeliver(M),
    /// The hook absorbed it (typically a loud typed refusal); apply this
    /// step.
    Handled(Step<Ph>),
}
