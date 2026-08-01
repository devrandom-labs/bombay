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
    actor::{Flow, Recipient, SpawnConfig, WeakActorRef},
    caps,
    error::{ActorStopReason, NameTaken, PanicError, TellError},
    mailbox::Capacity,
    registry::Registry,
    reply::ReplySender,
    restart::RestartConfig,
};
use core::{convert::Infallible, ops::ControlFlow};
use flume::Sender;

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
    pub stopped_tx: Option<Sender<ActorId>>,
}

pub struct Worker {
    slot: usize,
    dispatcher: Recipient<Done>,
    stopped_tx: Option<Sender<ActorId>>,
}

impl caps::Actor for Worker {
    type Msg = WorkerMsg;
    type Args = WorkerArgs;
    type Error = WorkerError;
    type Caps = ();

    async fn init(args: WorkerArgs, _: caps::Ctx<'_, Self>) -> Result<Self, WorkerError> {
        Ok(Self {
            slot: args.slot,
            dispatcher: args.dispatcher,
            stopped_tx: args.stopped_tx,
        })
    }

    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<caps::Shell<Self>>,
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
        cx: caps::Ctx<'_, Self>,
    ) -> Result<Flow, WorkerError> {
        match msg {
            WorkerMsg::Run(job) => match job.kind {
                JobKind::Poison => panic!("poison job {id}", id = job.id),
                JobKind::Fail => Err(WorkerError::JobFailed(job.id)),
                JobKind::Ok(work) => {
                    let job_id = job.id;
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
            WorkerMsg::WorkDone { job_id } => self
                .dispatcher
                .tell(Done {
                    slot: self.slot,
                    job_id,
                })
                .await
                .map(|()| Flow::Continue)
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
    pub worker_stopped_tx: Option<Sender<ActorId>>,
    /// Per-worker `on_stop` notice grace, wired into each worker's
    /// [`SpawnConfig`] (card #257).
    pub worker_grace: Duration,
    /// Optional audit sink on the caps surface (card #278 walking
    /// skeleton): every accepted submission is recorded there.
    pub audit: Option<caps::Handle<AuditLog>>,
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
    drain_reply: Option<ReplySender<DrainReport>>,
    audit: Option<caps::Handle<AuditLog>>,
}

/// The dispatcher's capability set (stage 3, card #280): it watches (the
/// named OTP policy) and supervises its worker fleet under `OneForOne` —
/// authority declared as plugged caps, not marker impls. The derive emits
/// the access seams, the replay hook, and the supervised loop selection.
#[derive(bombay_macros::Provide)]
pub struct DispatcherCaps {
    watching: caps::Watching<caps::OtpPropagation>,
    supervising: caps::Supervising<caps::OneForOne>,
}

impl caps::CapSet<Dispatcher> for DispatcherCaps {
    fn build(_: &DispatcherConfig) -> Self {
        Self {
            watching: caps::Watching::new(),
            supervising: caps::Supervising::new(),
        }
    }
}

impl caps::Actor for Dispatcher {
    type Msg = DispatcherMsg;
    type Args = DispatcherConfig;
    type Error = AppError;
    type Caps = DispatcherCaps;

    async fn init(cfg: DispatcherConfig, cx: caps::Ctx<'_, Self>) -> Result<Self, AppError> {
        let self_ref = cx.self_ref();
        cfg.registry.register(DISPATCHER_NAME, self_ref)?;
        let worker_stopped_tx = cfg.worker_stopped_tx.clone();
        let worker_grace = cfg.worker_grace;
        for slot in 0..cfg.workers {
            // WEAK capture is mandatory: the factory lives in this actor's own
            // child table — a strong ref would be a self-cycle and the
            // dispatcher could never ref-count-stop.
            let disp: WeakActorRef<caps::Shell<Self>> = self_ref.downgrade();
            let done_port: Recipient<Done> = self_ref.recipient::<Done>();
            let stopped_tx = worker_stopped_tx.clone();
            self_ref
                .supervise(cfg.restart, move || {
                    let worker = caps::spawn_with::<Worker>(
                        SpawnConfig {
                            on_stop_grace: worker_grace,
                            ..Default::default()
                        },
                        WorkerArgs {
                            slot,
                            dispatcher: done_port.clone(),
                            stopped_tx: stopped_tx.clone(),
                        },
                    );
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
            drain_reply: None,
            audit: cfg.audit,
        })
    }

    async fn handle(
        &mut self,
        msg: DispatcherMsg,
        cx: caps::Ctx<'_, Self>,
    ) -> Result<Flow, AppError> {
        let flow = match msg {
            DispatcherMsg::Submit { job, reply } => {
                if self.draining {
                    let _ = reply.send_err(SubmitError::Draining);
                } else if self.pending.len() >= self.queue_cap {
                    let _ = reply.send_err(SubmitError::QueueFull);
                } else {
                    bump(&mut self.stats.submitted);
                    let job_id = job.id;
                    self.pending.push_back(job);
                    self.dispatch();
                    let _ = reply.send(());
                    if let Some(audit) = &self.audit {
                        // Best-effort audit trail: a dead/full sink never
                        // blocks intake (same posture as the roster wart).
                        let _ = audit.tell(AuditMsg::Recorded { job_id }).await;
                    }
                }
                Flow::Continue
            }
            DispatcherMsg::Done(Done { slot, job_id }) => {
                // guard against a stale ack from a pre-rebuild incarnation
                if self.outstanding.get(&slot).is_some_and(|j| j.id == job_id) {
                    self.outstanding.remove(&slot);
                    self.retries.remove(&job_id);
                    bump(&mut self.stats.completed);
                }
                self.dispatch();
                self.finish_drain_if_quiet()
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
                Flow::Continue
            }
            DispatcherMsg::WorkerReplaced { slot, id, worker } => {
                let rebuilt = self.roster.insert(slot, (id, worker)).is_some();
                if rebuilt {
                    bump(&mut self.stats.rebuilds);
                    self.requeue_outstanding(slot, cx.self_ref());
                }
                self.stats.worker_ids = self
                    .roster
                    .values()
                    .map(|(actor_id, _)| *actor_id)
                    .collect();
                self.dispatch();
                self.finish_drain_if_quiet()
            }
            DispatcherMsg::Stats { reply } => {
                let _ = reply.send(self.stats.clone());
                Flow::Continue
            }
            DispatcherMsg::Drain { reply } => {
                self.draining = true;
                self.drain_reply = Some(reply);
                self.finish_drain_if_quiet()
            }
        };
        Ok(flow)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<caps::Shell<Self>>,
        _: ActorStopReason,
    ) -> Result<(), AppError> {
        // reason-independent resource release only (post-panic safe)
        self.registry.unregister(DISPATCHER_NAME);
        Ok(())
    }
}

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
    fn requeue_outstanding(&mut self, slot: usize, actor_ref: &caps::Handle<Self>) {
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

    /// Drain complete? Reply and stop directly (`Flow::Stop`). The supervisor's
    /// own exit now tears down any remaining children (ADR-0019), so the app no
    /// longer needs to detach-and-stop each worker before finishing the drain.
    fn finish_drain_if_quiet(&mut self) -> Flow {
        if !self.draining
            || !self.pending.is_empty()
            || !self.outstanding.is_empty()
            || self.pending_retries != 0
        {
            return Flow::Continue;
        }
        if let Some(reply) = self.drain_reply.take() {
            let _ = reply.send(DrainReport {
                submitted: self.stats.submitted,
                completed: self.stats.completed,
                failed: self.stats.failed,
                retried: self.stats.retried,
                rebuilds: self.stats.rebuilds,
            });
        }
        Flow::Stop
    }
}

// ------------------------------------------------------------ intake -------

/// Front door for submissions (card #224, migrated to the caps surface at
/// card #279).
///
/// A plain [`caps::Actor`] carrying a [`caps::Stashing`] capability: it defers
/// `Submit`s during maintenance and forwards them on `Resume` — the
/// walking-skeleton demonstration of the bounded stash as a capability.
pub struct Intake {
    dispatcher: caps::Handle<Dispatcher>,
    maintenance: bool,
}

#[allow(
    dead_code,
    reason = "Pause/Resume are exercised by the integration tests"
)]
#[derive(Debug, bombay_macros::Msg)]
pub enum IntakeMsg {
    Submit {
        job: Job,
        reply: ReplySender<(), SubmitError>,
    },
    Pause,
    Resume,
}

/// The intake's capability set: bounded deferral, nothing else. `#[derive]`
/// emits the `Provide` access seam AND the `Replay` loop hook — the stash is
/// serviced automatically, impossible to forget.
#[derive(bombay_macros::Provide)]
pub struct IntakeCaps {
    stash: caps::Stashing<IntakeMsg>,
}

/// Stash capacity, threaded from the spawn args (never a `SpawnConfig` field,
/// ADR-0022 D8).
pub struct IntakePolicy;

impl caps::StashPolicy<Intake> for IntakePolicy {
    fn capacity((_, cap): &<Intake as caps::Actor>::Args) -> Capacity {
        *cap
    }
}

impl caps::CapSet<Intake> for IntakeCaps {
    fn build(args: &<Intake as caps::Actor>::Args) -> Self {
        Self {
            stash: caps::Stashing::bounded(<IntakePolicy as caps::StashPolicy<Intake>>::capacity(
                args,
            )),
        }
    }
}

impl caps::Actor for Intake {
    type Msg = IntakeMsg;
    type Args = (caps::Handle<Dispatcher>, Capacity);
    type Error = Infallible;
    type Caps = IntakeCaps;

    async fn init((dispatcher, _): Self::Args, _: caps::Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self {
            dispatcher,
            maintenance: false,
        })
    }

    async fn handle(
        &mut self,
        msg: IntakeMsg,
        mut cx: caps::Ctx<'_, Self>,
    ) -> Result<Flow, Infallible> {
        match msg {
            IntakeMsg::Pause => self.maintenance = true,
            IntakeMsg::Resume => {
                self.maintenance = false;
                cx.cap::<caps::Stashing<IntakeMsg>>().unstash_all();
            }
            submit @ IntakeMsg::Submit { .. } if self.maintenance => {
                // Full stash = shed load with the same typed refusal the
                // dispatcher uses for a full queue: the asker learns NOW.
                if let Err(overflow) = cx.cap::<caps::Stashing<IntakeMsg>>().stash(submit)
                    && let IntakeMsg::Submit { reply, .. } = overflow.msg()
                {
                    let _ = reply.send_err(SubmitError::QueueFull);
                }
            }
            IntakeMsg::Submit { job, reply } => {
                // Forward: the dispatcher answers the original asker directly.
                if self
                    .dispatcher
                    .tell(DispatcherMsg::Submit { job, reply })
                    .await
                    .is_err()
                {
                    // Dispatcher gone: nothing to answer with — the dropped
                    // reply port surfaces the typed ask-side error.
                }
            }
        }
        Ok(Flow::Continue)
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

/// The overseer's death reaction, a NAMED policy (stage 3): record who died
/// and whether it was a normal stop; never propagate.
pub struct RecordDeath;

impl caps::WatchPolicy<Overseer> for RecordDeath {
    async fn on_link_died(
        actor: &mut Overseer,
        id: ActorId,
        reason: ActorStopReason,
        _linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, Infallible> {
        actor.seen = Some((id, reason.is_normal()));
        Ok(ControlFlow::Continue(()))
    }
}

/// The overseer's capability set: watching with the recording policy.
#[derive(bombay_macros::Provide)]
pub struct OverseerCaps {
    watching: caps::Watching<RecordDeath>,
}

impl caps::CapSet<Overseer> for OverseerCaps {
    fn build((): &()) -> Self {
        Self {
            watching: caps::Watching::new(),
        }
    }
}

impl caps::Actor for Overseer {
    type Msg = OverseerMsg;
    type Args = ();
    type Error = Infallible;
    type Caps = OverseerCaps;

    async fn init((): (), _: caps::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Self { seen: None })
    }

    async fn handle(
        &mut self,
        msg: OverseerMsg,
        _: caps::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        let OverseerMsg::Observed { reply } = msg;
        let _ = reply.send(self.seen);
        Ok(Flow::Continue)
    }
}

// ------------------------------------------------------------- bootstrap ----

pub struct App {
    /// Bootstrap handle kept alive for the demo; tests resolve via the registry.
    #[allow(dead_code, reason = "public bootstrap handle used by the demo binary")]
    pub dispatcher: caps::Handle<Dispatcher>,
    pub overseer: caps::Handle<Overseer>,
}

/// Wires the whole spine through the ONE spawn (stage 3): the overseer's
/// `Watching` cap selects the linked loop, the dispatcher's `Supervising`
/// cap the supervised loop — no per-shape spawn verbs.
#[expect(
    clippy::expect_used,
    reason = "the Watching cap structurally guarantees the link channel; init must succeed"
)]
pub async fn start(cfg: DispatcherConfig) -> App {
    let overseer = caps::spawn::<Overseer>(());
    let dispatcher = caps::spawn::<Dispatcher>(cfg);
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

// -------------------------------------------------------------- audit ----

/// The audit sink's closed menu.
#[allow(
    dead_code,
    reason = "Count and the recorded job id are exercised by the integration tests"
)]
#[derive(Debug, bombay_macros::Msg)]
pub enum AuditMsg {
    /// A submission was accepted by the dispatcher.
    Recorded { job_id: u64 },
    /// How many acceptances have been recorded?
    Count { reply: ReplySender<u64> },
}

/// Append-only audit trail on the distilled caps surface (ADR-0026 stage
/// 1, card #278): ONE trait impl, `Caps = ()`, spawned via
/// [`caps::spawn`] — the walking-skeleton demonstration that a plain
/// caps actor composes with the existing app unchanged.
pub struct AuditLog {
    entries: u64,
}

impl caps::Actor for AuditLog {
    type Msg = AuditMsg;
    type Args = ();
    type Error = Infallible;
    type Caps = ();

    async fn init((): (), _: caps::Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { entries: 0 })
    }

    async fn handle(&mut self, msg: AuditMsg, _: caps::Ctx<'_, Self>) -> Result<Flow, Infallible> {
        match msg {
            AuditMsg::Recorded { job_id: _ } => bump(&mut self.entries),
            AuditMsg::Count { reply } => {
                let _ = reply.send(self.entries);
            }
        }
        Ok(Flow::Continue)
    }
}
