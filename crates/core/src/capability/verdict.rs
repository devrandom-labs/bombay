//! The shared verdict vocabulary of the capability surface: [`Never`],
//! [`Deferred`], [`Disposition`], [`Step`], [`Overflow`] (ADR-0024 /
//! ADR-0028). Card #297 folds the remaining dialects into this family.

/// The uninhabited type with two structural jobs (ADR-0028).
///
/// As a phase: [`Step<Never>`](Step) has no constructible `Goto` — it is
/// isomorphic to [`Flow`](crate::actor::Flow) (a plain actor is a one-phase machine), which is
/// how one [`DeadlinePolicy`](super::DeadlinePolicy) trait serves both [`Deadlined`](super::Deadlined) and
/// [`Phased`](super::Phased). As a defer token: [`Disposition<Never>`](Disposition) has
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

/// A policy hook's transition decision — `Flow` (ADR-0023) plus one
/// variant, `Copy`, zero-box (ADR-0024 D3).
///
/// `Goto(current)` is deliberately a no-op (`gen_statem` `next_state` to
/// the same state): no unstash, no deadline reset. In `handle`, the
/// transition verb is [`Phased::goto`](super::Phased::goto) instead — recorded there,
/// committed by the framework only after the handler returns `Ok`, so
/// D3's commit-after-Ok law holds with no `Step` return channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<Ph> {
    /// Stay in the current phase (`gen_statem` `keep_state`).
    Stay,
    /// Transition (no-op when already there).
    Goto(Ph),
    /// Stop cleanly (reason `Normal`), like `Flow::Stop`.
    Stop,
}

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
