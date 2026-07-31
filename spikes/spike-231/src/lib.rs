//! Spike for card #231: the nexus-shaped aggregate lifecycle
//! (Loading -> Ready -> Draining) written both ways:
//!
//! - `agg_idiom` — the raw `match self.phase` idiom over the shipped
//!   `StashActor`/`Stashed` surface, with manual transition bookkeeping.
//! - `agg_fsm` — the same actor against `fsm`, a mock of the proposed
//!   `FsmActor`/`Fsm<S>` wrapper (candidate shape (b)).
//!
//! `tests/equivalence.rs` drives both through the same scripts and asserts
//! identical probe sequences (the #266 mode-blind-oracle pattern).

pub mod agg_fsm;
pub mod agg_idiom;
pub mod fsm;

/// Observable outcomes, emitted by both variants. The equivalence oracle
/// compares `Vec<Probe>` sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// Replay event folded into state (Loading).
    Applied(u64),
    /// Command executed (Ready).
    Processed(u64),
    /// Command refused (Draining).
    Refused(u64),
    /// Command shed because the stash was full (Loading).
    ShedFull(u64),
    /// The Loading state timeout fired.
    LoadTimedOut,
    /// Drain accepted; snapshot flush started.
    DrainStarted,
    /// Snapshot flush completed (Draining); actor stops after this.
    Snapshotted,
    /// A stale state-timeout was observed by user code where it should have
    /// been impossible. The oracle asserts this NEVER appears.
    StaleTimeoutLeaked,
}

pub type ProbeTx = tokio::sync::mpsc::UnboundedSender<Probe>;
pub type ProbeRx = tokio::sync::mpsc::UnboundedReceiver<Probe>;

#[must_use]
pub fn probe_channel() -> (ProbeTx, ProbeRx) {
    tokio::sync::mpsc::unbounded_channel()
}
