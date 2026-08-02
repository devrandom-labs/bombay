//! The job-queue mini-app — bombay's M1 compositional exit gate (card #218).
//!
//! Producer → `Dispatcher` (supervisor) → `Worker` children. At-least-once:
//! no submitted job is lost across a worker crash and rebuild — every job
//! completes at least once or is recorded failed, and the queue is empty at
//! drain. Exactly-once is impossible without idempotence (a worker can crash
//! after the work, before the ack).
//!
//! Shared verbatim by the runnable demo (`main.rs`) and the gate test
//! (`tests/app_job_queue.rs`) via `#[path]` inclusion — one file per
//! actor (card #292), this module the one re-export door.

mod audit;
mod dispatcher;
mod domain;
mod intake;
mod overseer;
mod worker;

pub use audit::{AuditLog, AuditMsg};
pub use dispatcher::{
    AppError, DISPATCHER_NAME, Dispatcher, DispatcherCaps, DispatcherConfig, DispatcherMsg,
};
pub(crate) use domain::bump;
pub use domain::{DrainReport, Job, JobKind, Stats, SubmitError};
pub use intake::{Intake, IntakeCaps, IntakeMsg, IntakePolicy};
pub use overseer::{Overseer, OverseerCaps, OverseerMsg, RecordDeath};
pub use worker::{
    Done, DrainGrace, Worker, WorkerArgs, WorkerCaps, WorkerError, WorkerMsg, WorkerPhase,
    WorkerPhases,
};

use bombay::capability;

pub struct App {
    /// Bootstrap handle kept alive for the demo; tests resolve via the registry.
    #[allow(dead_code, reason = "public bootstrap handle used by the demo binary")]
    pub dispatcher: capability::Handle<Dispatcher>,
    pub overseer: capability::Handle<Overseer>,
}

/// Wires the whole spine through the ONE spawn (stage 3): the overseer's
/// `Watching` cap selects the linked loop, the dispatcher's `Supervising`
/// cap the supervised loop — no per-shape spawn verbs.
#[expect(
    clippy::expect_used,
    reason = "the Watching cap structurally guarantees the link channel; init must succeed"
)]
pub async fn start(cfg: DispatcherConfig) -> App {
    let overseer = capability::spawn::<Overseer>(());
    let dispatcher = capability::spawn::<Dispatcher>(cfg);
    overseer
        .watch(&dispatcher)
        .await
        .expect("the overseer's Watching cap provides its link channel");
    // Ensure `init` (registry registration + worker supervision) has
    // completed before returning, so callers can resolve the dispatcher by name.
    let _ = dispatcher
        .ask(|reply| DispatcherMsg::Stats { reply })
        .await
        .expect("dispatcher init completed");
    App {
        dispatcher,
        overseer,
    }
}
