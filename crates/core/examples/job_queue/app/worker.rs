//! The `Worker` actor: one job at a time, ack via self-pipe; its phase
//! machine (Serving → Draining) declares `NoDefer` and the `DrainGrace`
//! timeout seat (card #281).

use std::time::Duration;

use bombay::{
    ActorId,
    actor::{Flow, Recipient, WeakActorRef},
    capability,
    error::{ActorStopReason, PanicError, TellError},
};
use flume::Sender;

use super::{Job, JobKind};

/// Worker → dispatcher ack.
#[derive(Debug, Clone)]
pub struct Done {
    pub slot: usize,
    pub job_id: u64,
}

#[allow(
    dead_code,
    reason = "Drain is exercised by the integration tests (card #281 walking skeleton)"
)]
#[derive(Debug, bombay_macros::Msg)]
pub enum WorkerMsg {
    Run(Job),
    WorkDone {
        job_id: u64,
    },
    /// Finish the in-flight job, refuse new ones, then stop (card #281:
    /// the phased-worker walking skeleton).
    Drain,
}

impl From<Job> for WorkerMsg {
    fn from(job: Job) -> Self {
        Self::Run(job)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("job {0} failed")]
    JobFailed(u64),
    #[error("ack lost: dispatcher unreachable")]
    AckLost(#[source] TellError<Done>),
}

pub struct WorkerArgs {
    pub slot: usize,
    pub dispatcher: Recipient<Done>,
    pub stopped_tx: Option<Sender<ActorId>>,
    /// How long a draining worker waits for its in-flight job before
    /// abandoning it — the Draining phase's deadline (card #281), an
    /// `Args`-tunable magnitude (ADR-0024 D8).
    pub drain_grace: Duration,
    /// Optional refusal tape: a job refused while Draining is reported
    /// here (loud, never a silent drop).
    pub refused_tx: Option<Sender<u64>>,
}

/// The worker's operational phases (card #281 walking skeleton).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerPhase {
    /// Accepting and running jobs.
    Serving,
    /// Finishing the in-flight job; new jobs are refused loudly.
    Draining,
}

/// The worker's phase machine as ONE declaration with its seats spelled
/// out (ADR-0028): every message is DELIVERED in every phase — a Draining
/// worker must refuse a new job loudly (the asker learns), never defer or
/// silently ignore it — so the deferral seat is `NoDefer` (no stash
/// exists, and the gate's verdict type cannot even spell `Defer`); the
/// one deadline (the in-flight drain grace) rides the plugged
/// [`DrainGrace`] seat.
pub struct WorkerPhases;

impl capability::PhasePolicy for WorkerPhases {
    type Actor = Worker;
    type Phase = WorkerPhase;
    type Deferral = capability::NoDefer;
    type Timeout = DrainGrace;

    fn initial(_: &WorkerArgs) -> WorkerPhase {
        WorkerPhase::Serving
    }

    fn gate(_: WorkerPhase, _: &WorkerMsg) -> capability::Disposition {
        // Deliberately all-Deliver, written down: refusal is the
        // handler's job (loud), and the in-flight ack must always land.
        capability::Disposition::Deliver
    }
}

/// The Draining grace as a plugged timeout seat (D8: the magnitude rides
/// `Args` into the seat's `build`).
pub struct DrainGrace {
    grace: Duration,
}

impl capability::DeadlinePolicy<capability::ByPhase<WorkerPhases>> for DrainGrace {
    fn build(args: &WorkerArgs) -> Self {
        Self {
            grace: args.drain_grace,
        }
    }

    fn next_deadline(
        &self,
        _: &Worker,
        view: capability::PhaseView<WorkerPhases>,
    ) -> Option<tokio::time::Instant> {
        match view.phase {
            WorkerPhase::Serving => None,
            WorkerPhase::Draining => view.entered_at.checked_add(self.grace),
        }
    }

    /// The in-flight job outlived the drain grace: abandon it and stop —
    /// the dispatcher's at-least-once bookkeeping re-queues it.
    async fn on_deadline(
        &self,
        _: &mut Worker,
        _: capability::PhaseView<WorkerPhases>,
        _: WeakActorRef<capability::Shell<Worker>>,
    ) -> Result<capability::Step<WorkerPhase>, WorkerError> {
        Ok(capability::Step::Stop)
    }
}

#[derive(bombay_macros::Provide)]
pub struct WorkerCaps {
    phased: capability::Phased<WorkerPhases>,
}

impl capability::CapSet<Worker> for WorkerCaps {
    fn build(args: &WorkerArgs) -> Self {
        Self {
            phased: capability::Phased::build(args),
        }
    }
}

pub struct Worker {
    slot: usize,
    dispatcher: Recipient<Done>,
    stopped_tx: Option<Sender<ActorId>>,
    refused_tx: Option<Sender<u64>>,
    /// A job is in flight (its `WorkDone` self-pipe has not landed yet).
    busy: bool,
}

impl capability::Actor for Worker {
    type Msg = WorkerMsg;
    type Args = WorkerArgs;
    type Error = WorkerError;
    type Caps = WorkerCaps;

    async fn init(args: WorkerArgs, _: capability::Ctx<'_, Self>) -> Result<Self, WorkerError> {
        Ok(Self {
            slot: args.slot,
            dispatcher: args.dispatcher,
            stopped_tx: args.stopped_tx,
            refused_tx: args.refused_tx,
            busy: false,
        })
    }

    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<capability::Shell<Self>>,
        _: ActorStopReason,
    ) -> Result<(), WorkerError> {
        if let Some(tx) = &self.stopped_tx {
            let _ = tx.send(actor_ref.id());
        }
        Ok(())
    }

    #[expect(
        clippy::panic,
        reason = "a poison job simulates an actor bug on purpose; the loop catches it"
    )]
    async fn handle(
        &mut self,
        msg: WorkerMsg,
        mut cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, WorkerError> {
        let phase = cx.cap::<capability::Phased<WorkerPhases>>().phase();
        match msg {
            // A draining worker refuses new work LOUDLY — the refusal is
            // taped, never silently dropped; the in-flight job (if any)
            // still completes below.
            WorkerMsg::Run(job) if phase == WorkerPhase::Draining => {
                if let Some(tx) = &self.refused_tx {
                    let _ = tx.send(job.id);
                }
                Ok(Flow::Continue)
            }
            WorkerMsg::Run(job) => match job.kind {
                JobKind::Poison => panic!("poison job {id}", id = job.id),
                JobKind::Fail => Err(WorkerError::JobFailed(job.id)),
                JobKind::Ok(work) => {
                    let job_id = job.id;
                    self.busy = true;
                    // never block the turn on the work itself
                    cx.self_ref().pipe_to_self(
                        async move { tokio::time::sleep(work).await },
                        move |outcome: Result<(), PanicError>| {
                            // simulated work cannot panic; a real app would
                            // route Err into its own failure message
                            drop(outcome);
                            WorkerMsg::WorkDone { job_id }
                        },
                    );
                    Ok(Flow::Continue)
                }
            },
            WorkerMsg::WorkDone { job_id } => {
                self.busy = false;
                self.dispatcher
                    .tell(Done {
                        slot: self.slot,
                        job_id,
                    })
                    .await
                    .map_err(WorkerError::AckLost)?;
                // The FlushDone-style self-pipe closes the drain: once the
                // in-flight ack has landed, a draining worker stops.
                Ok(if phase == WorkerPhase::Draining {
                    Flow::Stop
                } else {
                    Flow::Continue
                })
            }
            WorkerMsg::Drain => {
                if self.busy {
                    cx.cap::<capability::Phased<WorkerPhases>>()
                        .goto(WorkerPhase::Draining);
                    Ok(Flow::Continue)
                } else {
                    Ok(Flow::Stop)
                }
            }
        }
    }
}
