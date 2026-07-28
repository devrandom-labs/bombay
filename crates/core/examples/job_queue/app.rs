//! The job-queue mini-app — bombay's M1 compositional exit gate (card #218).
//!
//! Producer → `Dispatcher` (supervisor) → `Worker` children. At-least-once:
//! no submitted job is lost across a worker crash and rebuild — every job
//! completes at least once or is recorded failed, and the queue is empty at
//! drain. Exactly-once is impossible without idempotence (a worker can crash
//! after the work, before the ack).
//!
//! Shared verbatim by the runnable demo (`main.rs`) and the gate test
//! (`tests/app_job_queue.rs`) via `#[path]` inclusion.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use bombay::{
    ActorId,
    actor::{
        Actor, ActorRef, Recipient, Spawn as _, SpawnLinked as _, SpawnSupervised as _, Supervisor,
        Watch, WeakActorRef,
    },
    error::{ActorStopReason, NameTaken, PanicError, TellError},
    mailbox::Mailboxed,
    registry::Registry,
    reply::ReplySender,
    restart::RestartConfig,
};
use core::ops::ControlFlow;

// ---------------------------------------------------------------- domain ----

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
const fn bump(counter: &mut u64) {
    *counter = counter.checked_add(1).expect("u64 event counter overflow");
}

// ---------------------------------------------------------------- worker ----

/// Worker → dispatcher ack.
#[derive(Debug, Clone)]
pub struct Done {
    pub slot: usize,
    pub job_id: u64,
}

#[derive(Debug, bombay_macros::Msg)]
pub enum WorkerMsg {
    Run(Job),
    WorkDone { job_id: u64 },
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
}

pub struct Worker {
    slot: usize,
    dispatcher: Recipient<Done>,
}

impl Mailboxed for Worker {
    type Msg = WorkerMsg;
}

impl Actor for Worker {
    type Args = WorkerArgs;
    type Error = WorkerError;

    async fn on_start(args: WorkerArgs, _: ActorRef<Self>) -> Result<Self, WorkerError> {
        Ok(Self {
            slot: args.slot,
            dispatcher: args.dispatcher,
        })
    }

    #[expect(
        clippy::panic,
        reason = "a poison job simulates an actor bug on purpose; the loop catches it"
    )]
    async fn handle(
        &mut self,
        msg: WorkerMsg,
        actor_ref: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), WorkerError> {
        match msg {
            WorkerMsg::Run(job) => match job.kind {
                JobKind::Poison => panic!("poison job {id}", id = job.id),
                JobKind::Fail => Err(WorkerError::JobFailed(job.id)),
                JobKind::Ok(work) => {
                    let job_id = job.id;
                    // never block the turn on the work itself
                    actor_ref.pipe_to_self(
                        async move { tokio::time::sleep(work).await },
                        move |outcome: Result<(), PanicError>| {
                            // simulated work cannot panic; a real app would
                            // route Err into its own failure message
                            drop(outcome);
                            WorkerMsg::WorkDone { job_id }
                        },
                    );
                    Ok(())
                }
            },
            WorkerMsg::WorkDone { job_id } => self
                .dispatcher
                .tell(Done {
                    slot: self.slot,
                    job_id,
                })
                .await
                .map_err(WorkerError::AckLost),
        }
    }
}

// ------------------------------------------------------------ dispatcher ----

pub const DISPATCHER_NAME: &str = "dispatcher";

#[allow(dead_code, reason = "Stats variant is used by the integration tests")]
#[derive(Debug, bombay_macros::Msg)]
pub enum DispatcherMsg {
    Submit {
        job: Job,
        reply: ReplySender<(), SubmitError>,
    },
    Done(Done),
    Retry(Job),
    WorkerReplaced {
        slot: usize,
        id: ActorId,
        worker: Recipient<Job>,
    },
    Drain {
        reply: ReplySender<DrainReport>,
    },
    /// Self-signal: all children detached+stopped, reply and stop.
    FinishStop,
    Stats {
        reply: ReplySender<Stats>,
    },
}

impl From<Done> for DispatcherMsg {
    fn from(done: Done) -> Self {
        Self::Done(done)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("registry name collision")]
    Registry(#[from] NameTaken),
    #[error("supervise registration failed")]
    Supervise(#[source] TellError<()>),
}

pub struct DispatcherConfig {
    pub workers: usize,
    pub queue_cap: usize,
    pub retry_cap: u32,
    pub retry_backoff: Duration,
    pub restart: RestartConfig,
    pub registry: Arc<Registry>,
}

pub struct Dispatcher {
    registry: Arc<Registry>,
    queue_cap: usize,
    retry_cap: u32,
    retry_backoff: Duration,
    pending: VecDeque<Job>,
    /// In-flight job per worker slot — the at-least-once bookkeeping.
    outstanding: HashMap<usize, Job>,
    roster: HashMap<usize, (ActorId, Recipient<Job>)>,
    retries: HashMap<u64, u32>,
    /// Retry timers armed but not yet re-entered as `Retry` messages.
    pending_retries: u32,
    stats: Stats,
    draining: bool,
    stopping: bool,
    drain_reply: Option<ReplySender<DrainReport>>,
}

impl Mailboxed for Dispatcher {
    type Msg = DispatcherMsg;
}

impl Actor for Dispatcher {
    type Args = DispatcherConfig;
    type Error = AppError;

    async fn on_start(cfg: DispatcherConfig, actor_ref: ActorRef<Self>) -> Result<Self, AppError> {
        cfg.registry.register(DISPATCHER_NAME, &actor_ref)?;
        for slot in 0..cfg.workers {
            // WEAK capture is mandatory: the factory lives in this actor's own
            // child table — a strong ref would be a self-cycle and the
            // dispatcher could never ref-count-stop.
            let disp: WeakActorRef<Self> = actor_ref.downgrade();
            let done_port: Recipient<Done> = actor_ref.recipient::<Done>();
            actor_ref
                .supervise(cfg.restart, move || {
                    let worker = Worker::spawn(WorkerArgs {
                        slot,
                        dispatcher: done_port.clone(),
                    });
                    if let Some(strong_disp) = disp.upgrade() {
                        // best-effort: a full mailbox loses the roster update —
                        // wart #3, evidence on #225
                        let _ = strong_disp
                            .tell(DispatcherMsg::WorkerReplaced {
                                slot,
                                id: worker.id(),
                                worker: worker.recipient::<Job>(),
                            })
                            .try_send();
                    }
                    worker
                })
                .await
                .map_err(AppError::Supervise)?;
        }
        Ok(Self {
            registry: cfg.registry,
            queue_cap: cfg.queue_cap,
            retry_cap: cfg.retry_cap,
            retry_backoff: cfg.retry_backoff,
            pending: VecDeque::new(),
            outstanding: HashMap::new(),
            roster: HashMap::new(),
            retries: HashMap::new(),
            pending_retries: 0,
            stats: Stats::default(),
            draining: false,
            stopping: false,
            drain_reply: None,
        })
    }

    async fn handle(
        &mut self,
        msg: DispatcherMsg,
        actor_ref: ActorRef<Self>,
        stop: &mut bool,
    ) -> Result<(), AppError> {
        match msg {
            DispatcherMsg::Submit { job, reply } => {
                if self.draining {
                    let _ = reply.send_err(SubmitError::Draining);
                } else if self.pending.len() >= self.queue_cap {
                    let _ = reply.send_err(SubmitError::QueueFull);
                } else {
                    bump(&mut self.stats.submitted);
                    self.pending.push_back(job);
                    self.dispatch();
                    let _ = reply.send(());
                }
            }
            DispatcherMsg::Done(Done { slot, job_id }) => {
                // guard against a stale ack from a pre-rebuild incarnation
                if self.outstanding.get(&slot).is_some_and(|j| j.id == job_id) {
                    self.outstanding.remove(&slot);
                    self.retries.remove(&job_id);
                    bump(&mut self.stats.completed);
                }
                self.dispatch();
                self.finish_drain_if_quiet(&actor_ref).await;
            }
            DispatcherMsg::Retry(job) => {
                #[expect(
                    clippy::expect_used,
                    reason = "pending retry counter underflow is structurally impossible"
                )]
                {
                    self.pending_retries = self
                        .pending_retries
                        .checked_sub(1)
                        .expect("pending retry counter underflow");
                }
                self.pending.push_front(job);
                self.dispatch();
            }
            DispatcherMsg::WorkerReplaced { slot, id, worker } => {
                let rebuilt = self.roster.insert(slot, (id, worker)).is_some();
                if rebuilt {
                    bump(&mut self.stats.rebuilds);
                    self.requeue_outstanding(slot, &actor_ref);
                }
                self.stats.worker_ids = self
                    .roster
                    .values()
                    .map(|(actor_id, _)| *actor_id)
                    .collect();
                self.dispatch();
                self.finish_drain_if_quiet(&actor_ref).await;
            }
            DispatcherMsg::Stats { reply } => {
                let _ = reply.send(self.stats.clone());
            }
            DispatcherMsg::Drain { reply } => {
                self.draining = true;
                self.drain_reply = Some(reply);
                self.finish_drain_if_quiet(&actor_ref).await;
            }
            DispatcherMsg::FinishStop => {
                if let Some(reply) = self.drain_reply.take() {
                    let _ = reply.send(DrainReport {
                        submitted: self.stats.submitted,
                        completed: self.stats.completed,
                        failed: self.stats.failed,
                        retried: self.stats.retried,
                        rebuilds: self.stats.rebuilds,
                    });
                }
                *stop = true;
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _: WeakActorRef<Self>, _: ActorStopReason) -> Result<(), AppError> {
        // reason-independent resource release only (post-panic safe)
        self.registry.unregister(DISPATCHER_NAME);
        Ok(())
    }
}

impl Watch for Dispatcher {}
impl Supervisor for Dispatcher {}

impl Dispatcher {
    /// Hand pending jobs to idle slots. `try_tell` keeps this handler
    /// non-blocking (a worker's mailbox holds at most `Run` + `WorkDone`, so a
    /// full mailbox means the worker is mid-rebuild — the job goes back to
    /// pending and the next `WorkerReplaced`/`Done` re-triggers dispatch).
    fn dispatch(&mut self) {
        let idle: Vec<usize> = self
            .roster
            .keys()
            .copied()
            .filter(|slot| !self.outstanding.contains_key(slot))
            .collect();
        for slot in idle {
            let Some(job) = self.pending.pop_front() else {
                break;
            };
            let worker = &self.roster[&slot].1;
            match worker.try_tell(job.clone()) {
                Ok(()) => {
                    self.outstanding.insert(slot, job);
                }
                Err(_) => {
                    // worker dead or full: the job stays pending; rebuild or
                    // ack traffic will re-trigger dispatch
                    self.pending.push_front(job);
                }
            }
        }
    }

    /// A slot's worker died mid-job: re-queue its outstanding job with a
    /// delay, or record it failed past the retry cap. Nothing is dropped
    /// silently.
    #[expect(
        clippy::expect_used,
        reason = "u32 retry counter overflow is a programmer bug, not a data limit"
    )]
    fn requeue_outstanding(&mut self, slot: usize, actor_ref: &ActorRef<Self>) {
        let Some(job) = self.outstanding.remove(&slot) else {
            return;
        };
        let attempts = self.retries.entry(job.id).or_insert(0);
        *attempts = attempts.checked_add(1).expect("u32 retry counter overflow");
        if *attempts > self.retry_cap {
            self.retries.remove(&job.id);
            bump(&mut self.stats.failed);
        } else {
            bump(&mut self.stats.retried);
            self.pending_retries = self
                .pending_retries
                .checked_add(1)
                .expect("pending retry counter overflow");
            // dropping the handle detaches: the timer still fires
            let _detached = actor_ref.send_after(self.retry_backoff, DispatcherMsg::Retry(job));
        }
    }

    /// Drain complete? Detach-and-stop every worker FIRST — a supervisor
    /// stopping `Normal` does NOT stop its children (verified: only the
    /// escalation path calls `stop_surviving_children`; `spawn.rs`'s
    /// supervised lifecycle runs no child sweep) — then self-signal
    /// `FinishStop`. The mailbox is FIFO, so the stop signals are processed
    /// before the final message; the reply and `*stop = true` happen in the
    /// `FinishStop` arm.
    async fn finish_drain_if_quiet(&mut self, actor_ref: &ActorRef<Self>) {
        if !self.draining
            || self.stopping
            || !self.pending.is_empty()
            || !self.outstanding.is_empty()
            || self.pending_retries != 0
        {
            return;
        }
        self.stopping = true;
        let ids: Vec<ActorId> = self.roster.values().map(|(id, _)| *id).collect();
        for id in ids {
            // enqueues the detach+stop signal into our own mailbox; an error
            // here means our own mailbox is gone — nothing left to stop
            let _ = actor_ref.stop_child(id).await;
        }
        // best-effort self-signal: if the mailbox is momentarily full, the
        // message occupying it re-enters this method and retries (the
        // `stopping` flag keeps the child stops one-shot — reset it so the
        // retry path can re-send FinishStop)
        if actor_ref
            .tell(DispatcherMsg::FinishStop)
            .try_send()
            .is_err()
        {
            self.stopping = false;
        }
    }
}

// -------------------------------------------------------------- overseer ----

/// Watches the dispatcher from outside — the app-level death-watch consumer.
#[derive(Debug, bombay_macros::Msg)]
pub enum OverseerMsg {
    /// Did the dispatcher die, and was it a normal stop?
    Observed {
        reply: ReplySender<Option<(ActorId, bool)>>,
    },
}

pub struct Overseer {
    seen: Option<(ActorId, bool)>,
}

impl Mailboxed for Overseer {
    type Msg = OverseerMsg;
}

impl Actor for Overseer {
    type Args = ();
    type Error = core::convert::Infallible;

    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { seen: None })
    }

    async fn handle(
        &mut self,
        msg: OverseerMsg,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        let OverseerMsg::Observed { reply } = msg;
        let _ = reply.send(self.seen);
        Ok(())
    }
}

impl Watch for Overseer {
    async fn on_link_died(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        _linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        self.seen = Some((id, reason.is_normal()));
        Ok(ControlFlow::Continue(()))
    }
}

// ------------------------------------------------------------- bootstrap ----

pub struct App {
    /// Bootstrap handle kept alive for the demo; tests resolve via the registry.
    #[allow(dead_code, reason = "public bootstrap handle used by the demo binary")]
    pub dispatcher: ActorRef<Dispatcher>,
    pub overseer: ActorRef<Overseer>,
}

/// Wires the whole spine: linked overseer, supervised dispatcher (which
/// registers itself and supervises its workers in `on_start`), watch edge.
#[expect(
    clippy::expect_used,
    reason = "spawn_linked structurally guarantees the link channel; on_start must succeed"
)]
pub async fn start(cfg: DispatcherConfig) -> App {
    let overseer = Overseer::spawn_linked(());
    let dispatcher = Dispatcher::spawn_supervised(cfg);
    overseer
        .watch(&dispatcher)
        .await
        .expect("overseer was spawned linked");
    // Ensure `on_start` (registry registration + worker supervision) has
    // completed before returning, so callers can resolve the dispatcher by name.
    let _ = dispatcher
        .ask(|reply| DispatcherMsg::Stats { reply })
        .await
        .expect("dispatcher on_start completed");
    App {
        dispatcher,
        overseer,
    }
}
