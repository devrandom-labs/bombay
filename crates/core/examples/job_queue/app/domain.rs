//! Shared domain vocabulary: jobs, typed refusals, the stats/report
//! shapes, and the checked event-counter bump.

use std::time::Duration;

use bombay::ActorId;

/// What a job does to the worker that runs it.
#[derive(Debug, Clone)]
pub enum JobKind {
    /// Completes after simulated async work.
    Ok(Duration),
    /// The worker's handler returns `Err` — a controlled crash.
    Fail,
    /// The worker's handler panics — caught, routed to `on_panic`.
    Poison,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u64,
    pub kind: JobKind,
}

/// Typed `Submit` rejection — the app-level layer, distinct from mailbox
/// backpressure (which a client sees as an ask timeout under load).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SubmitError {
    #[error("job queue at capacity")]
    QueueFull,
    #[error("dispatcher is draining")]
    Draining,
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub retried: u64,
    pub rebuilds: u64,
    /// Current roster, one entry per live slot.
    pub worker_ids: Vec<ActorId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainReport {
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub retried: u64,
    pub rebuilds: u64,
}

/// Stats counters only ever increment by one; overflowing u64 that way is a
/// programmer bug, not a data limit.
#[expect(
    clippy::expect_used,
    reason = "u64 event-counter overflow is a programmer bug, not a data limit"
)]
pub(crate) const fn bump(counter: &mut u64) {
    *counter = counter.checked_add(1).expect("u64 event counter overflow");
}
