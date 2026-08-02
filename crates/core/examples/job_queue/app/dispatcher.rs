//! The `Dispatcher` supervisor: at-least-once bookkeeping over a
//! supervised worker fleet — `Watching` + `Supervising` capabilities,
//! `OneForOne` restarts, retry backoff, drain protocol.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use bombay::{
    ActorId,
    actor::{Flow, Normal, Recipient, SpawnConfig, WeakActorRef},
    capability,
    error::{ActorStopReason, NameTaken, TellError},
    registry::Registry,
    reply::ReplySender,
    restart::RestartConfig,
};
use flume::Sender;

use super::{
    AuditLog, AuditMsg, Done, DrainReport, Job, Stats, SubmitError, Worker, WorkerArgs, bump,
};

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
    /// Optional audit sink on the capability surface (card #278 walking
    /// skeleton): every accepted submission is recorded there.
    pub audit: Option<capability::Handle<AuditLog>>,
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
    audit: Option<capability::Handle<AuditLog>>,
}

/// The dispatcher's capability set (stage 3, card #280): it watches (the
/// named OTP policy) and supervises its worker fleet under `OneForOne` —
/// authority declared as plugged capabilities, not marker impls. The derive emits
/// the access seams, the replay hook, and the supervised loop selection.
#[derive(bombay_macros::Provide)]
pub struct DispatcherCaps {
    watching: capability::Watching<capability::OtpPropagation>,
    supervising: capability::Supervising<capability::OneForOne>,
}

impl capability::CapSet<Dispatcher> for DispatcherCaps {
    fn build(_: &DispatcherConfig) -> Self {
        Self {
            watching: capability::Watching::new(),
            supervising: capability::Supervising::new(),
        }
    }
}

impl capability::Actor for Dispatcher {
    type Msg = DispatcherMsg;
    type Args = DispatcherConfig;
    type Error = AppError;
    type Caps = DispatcherCaps;

    async fn init(cfg: DispatcherConfig, cx: capability::Ctx<'_, Self>) -> Result<Self, AppError> {
        let self_ref = cx.self_ref();
        cfg.registry.register(DISPATCHER_NAME, self_ref)?;
        let worker_stopped_tx = cfg.worker_stopped_tx.clone();
        let worker_grace = cfg.worker_grace;
        for slot in 0..cfg.workers {
            // WEAK capture is mandatory: the factory lives in this actor's own
            // child table — a strong ref would be a self-cycle and the
            // dispatcher could never ref-count-stop.
            let disp: WeakActorRef<capability::Shell<Self>> = self_ref.downgrade();
            let done_port: Recipient<Done> = self_ref.recipient::<Done>();
            let stopped_tx = worker_stopped_tx.clone();
            self_ref
                .supervise(cfg.restart, move || {
                    let worker = capability::spawn_with::<Worker>(
                        SpawnConfig {
                            on_stop_grace: worker_grace,
                            ..Default::default()
                        },
                        WorkerArgs {
                            slot,
                            dispatcher: done_port.clone(),
                            stopped_tx: stopped_tx.clone(),
                            // The app never drains workers individually
                            // (the supervisor's teardown covers them); the
                            // grace mirrors the stop grace.
                            drain_grace: worker_grace,
                            refused_tx: None,
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
        cx: capability::Ctx<'_, Self>,
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
        _: WeakActorRef<capability::Shell<Self>>,
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
    fn requeue_outstanding(&mut self, slot: usize, actor_ref: &capability::Handle<Self>) {
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

    /// Drain complete? Reply and stop directly (`Flow::Stop(Normal)`). The supervisor's
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
        Flow::Stop(Normal)
    }
}
