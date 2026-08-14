//! At-least-once job accounting through stable supervised workers.

use std::collections::VecDeque;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bombay_framework::prelude::behavior::SendAlgebra;
use bombay_framework::prelude::*;
use tokio::time::Instant;

const DISPATCHER: QueueAddr = QueueAddr(1);
const REPORTER: QueueAddr = QueueAddr(2);
const READY: QueueAddr = QueueAddr(3);
const ADMISSIONS: QueueAddr = QueueAddr(4);
const QUERY_REPORTER: QueueAddr = QueueAddr(5);
const DEADLINE_PROBE: QueueAddr = QueueAddr(6);
const WORKERS: usize = 3;
const DRAIN_TIMER: behavior::TimerId = behavior::TimerId(0);
const DRAIN_GRACE: Duration = Duration::from_millis(100);
const RETRY_BASE: Duration = Duration::from_millis(5);
const RETRY_CAP: Duration = Duration::from_millis(40);
const MAINTENANCE_CAPACITY: usize = 4;

static DEADLINE_REACHED: OnceLock<AtomicBool> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct QueueAddr(u64);

impl Address for QueueAddr {
    type Nonce = u64;

    fn birth(self, nonce: u64) -> Self {
        Self(self.0 * 257 + nonce + 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobKind {
    Complete,
    Fail,
    PoisonOnce,
    Hang,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Job {
    id: u64,
    kind: JobKind,
    attempt: u32,
}

#[derive(Debug)]
enum QueueMessage {
    Submit {
        job: Job,
        reply_to: Recipient<AdmissionCollector>,
    },
    EnterMaintenance,
    Resume,
    Query {
        reply_to: Recipient<Reporter>,
    },
    Done {
        slot: usize,
        job_id: u64,
        failed: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    Accepted { job_id: u64 },
    Refused { job: Job, reason: RefusalReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefusalReason {
    MaintenanceCapacity,
    Draining,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Report {
    accepted: u64,
    completed: u64,
    failed: u64,
    retried: u64,
    abandoned: Vec<Job>,
}

#[derive(Debug)]
struct Poisoned;

struct Worker {
    slot: usize,
}

impl Behavior for Worker {
    type Addr = QueueAddr;
    type Msg = Job;
    type Event = User<QueueAddr, Job>;
    type Sends = Vec<Delivery<JobQueue>>;
    type Ph = Never;
    type Error = Poisoned;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        let job = event.message;
        if matches!(job.kind, JobKind::PoisonOnce) && job.attempt == 0 {
            return Err(Poisoned);
        }
        if matches!(job.kind, JobKind::Hang) {
            return Ok(Actions::cont());
        }
        Ok(Actions {
            sends: vec![Delivery::new(
                Recipient::global(DISPATCHER),
                QueueMessage::Done {
                    slot: self.slot,
                    job_id: job.id,
                    failed: matches!(job.kind, JobKind::Fail),
                },
            )],
            creates: Vec::new(),
            become_: Step::Continue,
        })
    }
}

fn worker(slot: usize) -> Worker {
    Worker { slot }
}

fn worker_nonce(slot: usize) -> u64 {
    u64::try_from(slot).expect("worker slot fits the address nonce")
}

type QueueSends = SendProduct<
    SendProduct<Vec<Delivery<ProxyBehavior>>, Vec<Delivery<Reporter>>>,
    Vec<Delivery<AdmissionCollector>>,
>;

struct QueueKernel;

impl Behavior for QueueKernel {
    type Addr = QueueAddr;
    type Msg = QueueMessage;
    type Event = User<QueueAddr, QueueMessage>;
    type Sends = QueueSends;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<Worker>;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type QueueSupervisor = Supervisor<QueueKernel, Worker>;

struct JobQueue {
    supervisor: QueueSupervisor,
    expected: u64,
    pending: VecDeque<Job>,
    held: VecDeque<Job>,
    slots: Vec<WorkerSlot>,
    retries: Vec<RetrySlot>,
    accepted: u64,
    completed: u64,
    failed: u64,
    retried: u64,
    phase: QueuePhase,
    ready_sent: bool,
    abandoned: Vec<Job>,
}

enum QueuePhase {
    Accepting,
    Maintenance,
    Draining {
        timer_generation: behavior::TimerGeneration,
    },
}

enum WorkerSlot {
    Installing,
    Idle,
    Busy(Job),
}

struct RetrySlot {
    generation: behavior::TimerGeneration,
    armed: Option<Job>,
}

fn retry_timer(slot: usize) -> behavior::TimerId {
    behavior::TimerId(u64::try_from(slot).expect("worker slot fits a timer identity") + 1)
}

fn retry_slot(id: behavior::TimerId) -> Option<usize> {
    let slot =
        id.0.checked_sub(1)
            .and_then(|slot| usize::try_from(slot).ok())?;
    (slot < WORKERS).then_some(slot)
}

fn retry_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(3);
    RETRY_BASE.saturating_mul(1_u32 << exponent).min(RETRY_CAP)
}

impl JobQueue {
    fn new(expected: u64) -> Self {
        Self {
            supervisor: Supervisor::new(
                QueueKernel,
                worker_nonce,
                WORKERS,
                worker,
                Strategy::OneForOne,
                RestartPolicy::Permanent,
                8,
                Duration::MAX,
            ),
            expected,
            pending: VecDeque::new(),
            held: VecDeque::new(),
            slots: (0..WORKERS).map(|_| WorkerSlot::Installing).collect(),
            retries: (0..WORKERS)
                .map(|_| RetrySlot {
                    generation: behavior::TimerGeneration(0),
                    armed: None,
                })
                .collect(),
            accepted: 0,
            completed: 0,
            failed: 0,
            retried: 0,
            phase: QueuePhase::Accepting,
            ready_sent: false,
            abandoned: Vec::new(),
        }
    }

    fn accept(
        &mut self,
        job: Job,
        reply_to: Recipient<AdmissionCollector>,
    ) -> Delivery<AdmissionCollector> {
        let outcome = match self.phase {
            QueuePhase::Accepting => {
                self.accepted += 1;
                self.pending.push_back(job);
                Admission::Accepted { job_id: job.id }
            }
            QueuePhase::Maintenance if self.held.len() < MAINTENANCE_CAPACITY => {
                self.accepted += 1;
                self.held.push_back(job);
                Admission::Accepted { job_id: job.id }
            }
            QueuePhase::Maintenance => Admission::Refused {
                job,
                reason: RefusalReason::MaintenanceCapacity,
            },
            QueuePhase::Draining { .. } => Admission::Refused {
                job,
                reason: RefusalReason::Draining,
            },
        };
        Delivery::new(reply_to, outcome)
    }

    fn enter_maintenance(&mut self) {
        if matches!(self.phase, QueuePhase::Accepting) {
            self.phase = QueuePhase::Maintenance;
        }
    }

    fn resume(&mut self) {
        if matches!(self.phase, QueuePhase::Maintenance) {
            self.pending.extend(self.held.drain(..));
            self.phase = QueuePhase::Accepting;
        }
    }

    fn complete(&mut self, slot: usize, job_id: u64, failed: bool) {
        let WorkerSlot::Busy(job) = core::mem::replace(&mut self.slots[slot], WorkerSlot::Idle)
        else {
            panic!("only an occupied worker can report completion");
        };
        assert_eq!(job.id, job_id, "completion must name the outstanding job");
        if failed {
            self.failed += 1;
        } else {
            self.completed += 1;
        }
    }

    fn retry(&mut self, event: &WorkerStopped<QueueAddr>) -> Option<behavior::ScheduleAfter> {
        let slot = usize::try_from(event.proxy).expect("proxy nonce fits a worker slot");
        let previous = core::mem::replace(&mut self.slots[slot], WorkerSlot::Installing);
        if event.outcome.is_ok() {
            return None;
        }
        if let WorkerSlot::Busy(mut job) = previous {
            job.attempt += 1;
            self.retried += 1;
            let retry = &mut self.retries[slot];
            assert!(retry.armed.is_none(), "one slot owns at most one retry");
            retry.generation = behavior::TimerGeneration(
                retry
                    .generation
                    .0
                    .checked_add(1)
                    .expect("retry generation exhausted"),
            );
            retry.armed = Some(job);
            return Some(behavior::ScheduleAfter {
                id: retry_timer(slot),
                generation: retry.generation,
                after: retry_delay(job.attempt),
            });
        }
        None
    }

    fn release_retry(&mut self, elapsed: behavior::TimerElapsed) -> bool {
        let Some(slot) = retry_slot(elapsed.id) else {
            return false;
        };
        let retry = &mut self.retries[slot];
        if retry.generation != elapsed.generation {
            return false;
        }
        let Some(job) = retry.armed.take() else {
            return false;
        };
        self.pending.push_front(job);
        true
    }

    fn dispatch(&mut self) -> Vec<Delivery<ProxyBehavior>> {
        let mut sends = Vec::new();
        for slot in 0..self.slots.len() {
            if self.retries[slot].armed.is_none()
                && matches!(self.slots[slot], WorkerSlot::Idle)
                && let Some(job) = self.pending.pop_front()
            {
                self.slots[slot] = WorkerSlot::Busy(job);
                sends.push(Delivery::new(
                    Recipient::child(worker_nonce(slot)),
                    ProxyCommand::Forward(job),
                ));
            }
        }
        sends
    }

    fn drained(&self) -> bool {
        matches!(self.phase, QueuePhase::Draining { .. })
            && self.completed + self.failed + self.abandoned.len() as u64 == self.accepted
            && self.pending.is_empty()
            && self.retries.iter().all(|retry| retry.armed.is_none())
            && self
                .slots
                .iter()
                .all(|slot| matches!(slot, WorkerSlot::Idle))
    }

    fn abandon_outstanding(&mut self) {
        self.abandoned.extend(self.pending.drain(..));
        self.abandoned.extend(
            self.retries
                .iter_mut()
                .filter_map(|retry| retry.armed.take()),
        );
        for slot in &mut self.slots {
            if let WorkerSlot::Busy(job) = core::mem::replace(slot, WorkerSlot::Idle) {
                self.abandoned.push(job);
            }
        }
    }

    fn report(&self) -> Report {
        Report {
            accepted: self.accepted,
            completed: self.completed,
            failed: self.failed,
            retried: self.retried,
            abandoned: self.abandoned.clone(),
        }
    }
}

type JobQueueEvent =
    behavior::TimedEvent<behavior::ShutdownProtocol<<QueueSupervisor as Behavior>::Event>>;
type JobQueueSends =
    SendProduct<<QueueSupervisor as Behavior>::Sends, ServiceSends<behavior::ScheduleAfter>>;

type QueueDispatchPath = behavior::Inner<behavior::Inner<behavior::Inner<behavior::Own>>>;
type QueueReportPath = behavior::Inner<behavior::Inner<behavior::Own>>;
type JobQueueDispatchPath = behavior::Inner<QueueDispatchPath>;
type JobQueueReportPath = behavior::Inner<QueueReportPath>;
type JobQueueActions =
    Actions<QueueAddr, Never, JobQueueSends, <QueueSupervisor as Behavior>::Birth>;

impl Behavior for JobQueue {
    type Addr = QueueAddr;
    type Msg = QueueMessage;
    type Event = JobQueueEvent;
    type Sends = JobQueueSends;
    type Ph = Never;
    type Error = Never;
    type Birth = <QueueSupervisor as Behavior>::Birth;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        self.supervisor.init().map(|actions| {
            actions.map_sends(|behavior| SendProduct::new(behavior, ServiceSends::empty()))
        })
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event {
            behavior::TimedEvent::Inner(behavior::ShutdownProtocol::Inner(event)) => {
                self.transition_supervision(event)
            }
            behavior::TimedEvent::Inner(behavior::ShutdownProtocol::ShutdownRequested(_)) => {
                Ok(self.begin_drain())
            }
            behavior::TimedEvent::Elapsed(elapsed) => Ok(self.timer_elapsed(elapsed)),
        }
    }
}

impl JobQueue {
    fn transition_supervision(
        &mut self,
        event: <QueueSupervisor as Behavior>::Event,
    ) -> behavior::BehaviorActed<Self> {
        let mut retry_schedule = None;
        match &event {
            SupervisionEvent::Inner(User {
                message: QueueMessage::Submit { job, reply_to },
                ..
            }) => {
                let admission = self.accept(*job, *reply_to);
                let mut actions = behavior::delegate_transition(&mut self.supervisor, event)?;
                actions.sends.send(admission);
                return Ok(self.finish_supervision(actions));
            }
            SupervisionEvent::Inner(User {
                message: QueueMessage::EnterMaintenance,
                ..
            }) => self.enter_maintenance(),
            SupervisionEvent::Inner(User {
                message: QueueMessage::Resume,
                ..
            }) => self.resume(),
            SupervisionEvent::Inner(User {
                message: QueueMessage::Query { reply_to },
                ..
            }) => {
                let report = Delivery::new(*reply_to, self.report());
                let mut actions = behavior::delegate_transition(&mut self.supervisor, event)?;
                actions.sends.send(report);
                return Ok(self.finish_supervision(actions));
            }
            SupervisionEvent::Inner(User {
                message:
                    QueueMessage::Done {
                        slot,
                        job_id,
                        failed,
                    },
                ..
            }) => self.complete(*slot, *job_id, *failed),
            SupervisionEvent::WorkerStopped(stopped) => retry_schedule = self.retry(stopped),
            SupervisionEvent::WorkerCreationResolved(resolved) => {
                let slot = usize::try_from(resolved.proxy).expect("proxy nonce fits a worker slot");
                if resolved.result.is_ok() {
                    self.slots[slot] = WorkerSlot::Idle;
                }
            }
            SupervisionEvent::ChildStopped(_) | SupervisionEvent::CreationResolved(_) => {}
        }

        let actions = behavior::delegate_transition(&mut self.supervisor, event)?;
        let mut actions = self.finish_supervision(actions);
        if let Some(schedule) = retry_schedule {
            actions.sends.send(schedule);
        }
        Ok(actions)
    }

    fn finish_supervision(
        &mut self,
        mut actions: Actions<
            QueueAddr,
            Never,
            <QueueSupervisor as Behavior>::Sends,
            <QueueSupervisor as Behavior>::Birth,
        >,
    ) -> JobQueueActions {
        let dispatch = self.dispatch();
        // A proxy drops forwards while its worker is installing. Dispatch only
        // after the typed worker-creation result marks that slot available.
        for delivery in dispatch {
            actions.sends.send::<_, QueueDispatchPath>(delivery);
        }
        if !self.ready_sent && self.accepted == self.expected {
            self.ready_sent = true;
            actions.sends.send::<_, QueueReportPath>(Delivery::new(
                Recipient::<Reporter>::global(READY),
                self.report(),
            ));
        }
        if self.drained() {
            actions.sends.send::<_, QueueReportPath>(Delivery::new(
                Recipient::<Reporter>::global(REPORTER),
                self.report(),
            ));
            actions.become_ = Step::Stop(Exit::Normal);
        }
        actions.map_sends(|behavior| SendProduct::new(behavior, ServiceSends::empty()))
    }

    fn begin_drain(&mut self) -> JobQueueActions {
        if matches!(self.phase, QueuePhase::Draining { .. }) {
            return Actions::cont();
        }
        let generation = behavior::TimerGeneration(0);
        self.pending.extend(self.held.drain(..));
        self.phase = QueuePhase::Draining {
            timer_generation: generation,
        };
        let mut sends = <JobQueueSends as behavior::SendAlgebra>::empty();
        let become_ = if self.drained() {
            sends.send::<_, JobQueueReportPath>(Delivery::new(
                Recipient::<Reporter>::global(REPORTER),
                self.report(),
            ));
            Step::Stop(Exit::Normal)
        } else {
            sends.send(behavior::ScheduleAfter {
                id: DRAIN_TIMER,
                generation,
                after: DRAIN_GRACE,
            });
            Step::Continue
        };
        Actions::new(sends, Vec::new(), become_)
    }

    fn grace_elapsed(&mut self, elapsed: behavior::TimerElapsed) -> JobQueueActions {
        let QueuePhase::Draining { timer_generation } = self.phase else {
            return Actions::cont();
        };
        if elapsed.id != DRAIN_TIMER || elapsed.generation != timer_generation {
            return Actions::cont();
        }
        self.abandon_outstanding();
        let mut sends = <JobQueueSends as behavior::SendAlgebra>::empty();
        sends.send::<_, JobQueueReportPath>(Delivery::new(
            Recipient::<Reporter>::global(REPORTER),
            self.report(),
        ));
        Actions::new(sends, Vec::new(), Step::Stop(Exit::Normal))
    }

    fn timer_elapsed(&mut self, elapsed: behavior::TimerElapsed) -> JobQueueActions {
        if elapsed.id == DRAIN_TIMER {
            return self.grace_elapsed(elapsed);
        }
        if !self.release_retry(elapsed) {
            return Actions::cont();
        }
        let mut sends = <JobQueueSends as behavior::SendAlgebra>::empty();
        for delivery in self.dispatch() {
            sends.send::<_, JobQueueDispatchPath>(delivery);
        }
        Actions::new(sends, Vec::new(), Step::Continue)
    }
}

struct Reporter;

struct DeadlineProbe;

impl Behavior for DeadlineProbe {
    type Addr = QueueAddr;
    type Msg = Never;
    type Event = User<QueueAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's deadline reaction returns the behavior error domain"
)]
fn deadline_reached(_probe: &mut DeadlineProbe) -> Result<behavior::Become<QueueAddr>, Never> {
    DEADLINE_REACHED
        .get()
        .expect("deadline marker initialized")
        .store(true, Ordering::SeqCst);
    Ok(Step::Stop(Exit::Normal))
}

impl Behavior for Reporter {
    type Addr = QueueAddr;
    type Msg = Report;
    type Event = User<QueueAddr, Report>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Report;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        Err(event.message)
    }
}

struct AdmissionCollector {
    expected: usize,
    outcomes: Vec<Admission>,
}

impl Behavior for AdmissionCollector {
    type Addr = QueueAddr;
    type Msg = Admission;
    type Event = User<QueueAddr, Admission>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Vec<Admission>;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        self.outcomes.push(event.message);
        if self.outcomes.len() == self.expected {
            Err(core::mem::take(&mut self.outcomes))
        } else {
            Ok(Actions::cont())
        }
    }
}

type QueueEndpoint = ActorRef<QueueAddr, MailboxAnchor<<JobQueue as Behavior>::Event>>;
type ProxyBehavior = Proxy<Worker>;
type ProxyEndpoint = ActorRef<QueueAddr, MailboxAnchor<<ProxyBehavior as Behavior>::Event>>;
type WorkerEndpoint = ActorRef<QueueAddr, MailboxAnchor<<Worker as Behavior>::Event>>;
type ReporterEndpoint = ActorRef<QueueAddr, MailboxAnchor<<Reporter as Behavior>::Event>>;
type AdmissionEndpoint =
    ActorRef<QueueAddr, MailboxAnchor<<AdmissionCollector as Behavior>::Event>>;
type DeadlineEndpoint =
    ActorRef<QueueAddr, MailboxAnchor<<Deadline<DeadlineProbe> as Behavior>::Event>>;

type QueueIncarnation = IncarnationEndpoint<QueueAddr, QueueEndpoint>;
type ProxyIncarnation = IncarnationEndpoint<QueueAddr, ProxyEndpoint>;
type WorkerIncarnation = IncarnationEndpoint<QueueAddr, WorkerEndpoint>;
type ReporterIncarnation = IncarnationEndpoint<QueueAddr, ReporterEndpoint>;
type AdmissionIncarnation = IncarnationEndpoint<QueueAddr, AdmissionEndpoint>;
type DeadlineIncarnation = IncarnationEndpoint<QueueAddr, DeadlineEndpoint>;

#[derive(Clone, Default)]
struct Routes {
    queues: AddressRouter<QueueAddr, QueueIncarnation>,
    proxies: AddressRouter<QueueAddr, ProxyIncarnation>,
    workers: AddressRouter<QueueAddr, WorkerIncarnation>,
    reporters: AddressRouter<QueueAddr, ReporterIncarnation>,
    admissions: AddressRouter<QueueAddr, AdmissionIncarnation>,
    deadlines: AddressRouter<QueueAddr, DeadlineIncarnation>,
}

macro_rules! register_with {
    ($behavior:ty, $endpoint:ty, $field:ident) => {
        impl EndpointRegistry<$behavior, $endpoint> for Routes {
            type Error = AddressInUse<QueueAddr>;
            type Registration = <AddressRouter<QueueAddr, $endpoint> as EndpointRegistry<
                $behavior,
                $endpoint,
            >>::Registration;

            fn register(
                &self,
                address: QueueAddr,
                endpoint: $endpoint,
            ) -> Result<Self::Registration, Self::Error> {
                EndpointRegistry::<$behavior, $endpoint>::register(&self.$field, address, endpoint)
            }
        }
    };
}

register_with!(JobQueue, QueueIncarnation, queues);
register_with!(ProxyBehavior, ProxyIncarnation, proxies);
register_with!(Worker, WorkerIncarnation, workers);
register_with!(Reporter, ReporterIncarnation, reporters);
register_with!(AdmissionCollector, AdmissionIncarnation, admissions);
register_with!(Deadline<DeadlineProbe>, DeadlineIncarnation, deadlines);

macro_rules! route_with {
    ($behavior:ty, $endpoint:ty, $field:ident) => {
        impl DeliveryRouter<$behavior> for Routes {
            type Error = <AddressRouter<QueueAddr, $endpoint> as DeliveryRouter<$behavior>>::Error;

            async fn deliver(
                &self,
                from: QueueAddr,
                delivery: Delivery<$behavior>,
            ) -> Result<(), Self::Error> {
                self.$field.deliver(from, delivery).await
            }
        }
    };
}

route_with!(JobQueue, QueueIncarnation, queues);
route_with!(ProxyBehavior, ProxyIncarnation, proxies);
route_with!(Worker, WorkerIncarnation, workers);
route_with!(Reporter, ReporterIncarnation, reporters);
route_with!(AdmissionCollector, AdmissionIncarnation, admissions);

#[allow(
    clippy::too_many_lines,
    reason = "the example keeps one complete admission-to-drain scenario readable in execution order"
)]
async fn run_batch(system: &System<Routes>, jobs: &[Job]) -> Report {
    let deadline_marker = DEADLINE_REACHED.get_or_init(|| AtomicBool::new(false));
    deadline_marker.store(false, Ordering::SeqCst);
    let deadline = system
        .spawn(Actor::new(
            DEADLINE_PROBE,
            Deadline::new(
                DeadlineProbe,
                behavior::TimerId(10_000),
                Some(Instant::now()),
                deadline_reached,
            ),
        ))
        .expect("the deadline probe address is vacant");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), deadline.outcome())
            .await
            .expect("the production deadline must fire and retire"),
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    assert!(
        deadline_marker.load(Ordering::SeqCst),
        "the Behavior deadline reaction must execute through production"
    );

    let reporter = system
        .spawn(Actor::new(REPORTER, Reporter))
        .expect("the reporter address is vacant");
    let ready = system
        .spawn(Actor::new(READY, Reporter))
        .expect("the readiness address is vacant");
    let query_reporter = system
        .spawn(Actor::new(QUERY_REPORTER, Reporter))
        .expect("the query reporter address is vacant");
    let admissions = system
        .spawn(Actor::new(
            ADMISSIONS,
            AdmissionCollector {
                expected: jobs.len(),
                outcomes: Vec::new(),
            },
        ))
        .expect("the admission collector address is vacant");
    let queue = system
        .spawn(Actor::new(DISPATCHER, JobQueue::new(jobs.len() as u64)))
        .expect("the dispatcher address is vacant");

    assert!(
        queue
            .actor_ref()
            .send(QueueAddr(0), QueueMessage::EnterMaintenance)
            .await
            .is_ok(),
        "maintenance is an ordinary ordered queue command"
    );

    for (index, job) in jobs.iter().enumerate() {
        if index == MAINTENANCE_CAPACITY.min(jobs.len()) {
            assert!(
                queue
                    .actor_ref()
                    .send(QueueAddr(0), QueueMessage::Resume)
                    .await
                    .is_ok(),
                "resume is an ordinary ordered queue command"
            );
        }
        assert!(
            queue
                .actor_ref()
                .send(
                    QueueAddr(0),
                    QueueMessage::Submit {
                        job: *job,
                        reply_to: Recipient::global(ADMISSIONS),
                    },
                )
                .await
                .is_ok(),
            "accepted ingress must not drop a job"
        );
    }
    if jobs.len() <= MAINTENANCE_CAPACITY {
        assert!(
            queue
                .actor_ref()
                .send(QueueAddr(0), QueueMessage::Resume)
                .await
                .is_ok(),
            "resume releases the complete held prefix"
        );
    }

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), ready.outcome())
            .await
            .expect("queue must publish readiness after accepting the batch"),
        TaskOutcome::Returned(Err(bombay::RunError::Behavior(_)))
    ));
    assert!(
        queue
            .actor_ref()
            .send(
                QueueAddr(0),
                QueueMessage::Query {
                    reply_to: Recipient::global(QUERY_REPORTER),
                },
            )
            .await
            .is_ok(),
        "report query is ordinary typed queue ingress"
    );
    let TaskOutcome::Returned(Err(bombay::RunError::Behavior(query_report))) =
        tokio::time::timeout(Duration::from_secs(1), query_reporter.outcome())
            .await
            .expect("query must reply from its mailbox turn")
    else {
        panic!("query reporter must receive one turn-consistent snapshot");
    };
    assert_eq!(query_report.accepted, jobs.len() as u64);
    assert!(query_report.completed + query_report.failed <= query_report.accepted);
    queue
        .actor_ref()
        .request_shutdown()
        .expect("the queue accepts a typed drain request");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), queue.outcome())
            .await
            .expect("queue must finish its drain"),
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    let TaskOutcome::Returned(Err(bombay::RunError::Behavior(admission_outcomes))) =
        tokio::time::timeout(Duration::from_secs(1), admissions.outcome())
            .await
            .expect("collector must receive every admission outcome")
    else {
        panic!("collector must receive every admission outcome");
    };
    assert_eq!(admission_outcomes.len(), jobs.len());
    assert!(
        admission_outcomes
            .iter()
            .all(|outcome| matches!(outcome, Admission::Accepted { .. }))
    );
    match tokio::time::timeout(Duration::from_secs(1), reporter.outcome())
        .await
        .expect("reporter must receive final accounting")
    {
        TaskOutcome::Returned(Err(bombay::RunError::Behavior(report))) => report,
        _ => panic!("reporter must publish the final queue accounting"),
    }
}

async fn run() {
    let system = local_system!(
        mailbox = MailboxConfig::bounded(32),
        routes = Routes::default(),
    );
    let jobs = (0..20)
        .map(|id| Job {
            id,
            kind: match id {
                7 | 11 => JobKind::PoisonOnce,
                13 => JobKind::Fail,
                17 => JobKind::Hang,
                _ => JobKind::Complete,
            },
            attempt: 0,
        })
        .collect::<Vec<_>>();

    let report = run_batch(&system, &jobs).await;
    assert_eq!(
        report,
        Report {
            accepted: 20,
            completed: 18,
            failed: 1,
            retried: 2,
            abandoned: vec![Job {
                id: 17,
                kind: JobKind::Hang,
                attempt: 0,
            }],
        }
    );

    // A second complete run at the same logical addresses proves the first
    // root outcome followed transitive proxy and worker retirement.
    assert_eq!(
        run_batch(
            &system,
            &[Job {
                id: 100,
                kind: JobKind::Complete,
                attempt: 0,
            }],
        )
        .await,
        Report {
            accepted: 1,
            completed: 1,
            failed: 0,
            retried: 0,
            abandoned: Vec::new(),
        }
    );
    println!("job queue drained without loss: {report:?}");
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    run().await;
}

#[cfg(test)]
mod tests {
    use super::{
        Admission, Job, JobKind, JobQueue, MAINTENANCE_CAPACITY, QueueAddr, QueuePhase, RETRY_CAP,
        RefusalReason, WorkerSlot, retry_delay, retry_timer,
    };
    use bombay::behavior::{Crash, Recipient, TimerElapsed, TimerGeneration, WorkerStopped};

    fn job(id: u64) -> Job {
        Job {
            id,
            kind: JobKind::Complete,
            attempt: 0,
        }
    }

    fn failed_stop(slot: usize) -> WorkerStopped<QueueAddr> {
        WorkerStopped::new(
            u64::try_from(slot).unwrap(),
            u64::try_from(slot).unwrap(),
            Err(Crash::Failed),
            tokio::time::Instant::now(),
        )
    }

    #[test]
    fn failed_attempt_is_armed_until_its_matching_timer_elapses() {
        let mut queue = JobQueue::new(0);
        queue.slots[0] = WorkerSlot::Busy(job(7));

        let schedule = queue.retry(&failed_stop(0)).expect("failed work retries");

        assert_eq!(schedule.id, retry_timer(0));
        assert_eq!(schedule.generation, TimerGeneration(1));
        assert_eq!(schedule.after, retry_delay(1));
        assert!(queue.pending.is_empty());
        assert_eq!(queue.retries[0].armed.unwrap().attempt, 1);

        assert!(queue.release_retry(TimerElapsed::new(schedule.id, schedule.generation)));
        assert_eq!(queue.pending.front().unwrap().attempt, 1);
        assert!(queue.retries[0].armed.is_none());
    }

    #[test]
    fn stale_and_duplicate_timer_events_cannot_release_an_attempt_twice() {
        let mut queue = JobQueue::new(0);
        queue.slots[0] = WorkerSlot::Busy(job(7));
        let first = queue.retry(&failed_stop(0)).unwrap();
        assert!(queue.release_retry(TimerElapsed::new(first.id, first.generation)));
        queue.pending.clear();
        queue.slots[0] = WorkerSlot::Busy(job(8));
        let second = queue.retry(&failed_stop(0)).unwrap();

        assert_ne!(first.generation, second.generation);
        assert!(!queue.release_retry(TimerElapsed::new(first.id, first.generation)));
        assert!(queue.pending.is_empty());
        assert_eq!(queue.retries[0].armed.unwrap().id, 8);
        assert!(queue.release_retry(TimerElapsed::new(second.id, second.generation)));
        assert!(!queue.release_retry(TimerElapsed::new(second.id, second.generation)));
        assert_eq!(
            queue.pending.iter().map(|job| job.id).collect::<Vec<_>>(),
            [8]
        );
    }

    #[test]
    fn retry_waits_for_both_timer_and_replacement_in_either_order() {
        let mut timer_first = JobQueue::new(0);
        timer_first.slots[0] = WorkerSlot::Busy(job(1));
        let schedule = timer_first.retry(&failed_stop(0)).unwrap();
        assert!(timer_first.release_retry(TimerElapsed::new(schedule.id, schedule.generation)));
        assert!(timer_first.dispatch().is_empty());
        timer_first.slots[0] = WorkerSlot::Idle;
        assert_eq!(timer_first.dispatch().len(), 1);

        let mut replacement_first = JobQueue::new(0);
        replacement_first.slots[0] = WorkerSlot::Busy(job(2));
        let schedule = replacement_first.retry(&failed_stop(0)).unwrap();
        replacement_first.slots[0] = WorkerSlot::Idle;
        assert!(replacement_first.dispatch().is_empty());
        assert!(
            replacement_first.release_retry(TimerElapsed::new(schedule.id, schedule.generation))
        );
        assert_eq!(replacement_first.dispatch().len(), 1);
    }

    #[test]
    fn retry_backoff_grows_and_saturates_without_overflow() {
        assert_eq!(retry_delay(1), std::time::Duration::from_millis(5));
        assert_eq!(retry_delay(2), std::time::Duration::from_millis(10));
        assert_eq!(retry_delay(3), std::time::Duration::from_millis(20));
        assert_eq!(retry_delay(4), RETRY_CAP);
        assert_eq!(retry_delay(u32::MAX), RETRY_CAP);
    }

    #[test]
    fn abandonment_moves_every_owned_job_exactly_once() {
        let mut queue = JobQueue::new(4);
        queue.phase = QueuePhase::Draining {
            timer_generation: bombay::behavior::TimerGeneration(0),
        };
        queue.accepted = 4;
        queue.pending.extend([job(1), job(2)]);
        queue.slots = vec![WorkerSlot::Busy(job(3))];
        queue.retries = vec![super::RetrySlot {
            generation: TimerGeneration(1),
            armed: Some(job(4)),
        }];

        queue.abandon_outstanding();

        assert_eq!(queue.abandoned, [job(1), job(2), job(4), job(3)]);
        assert!(queue.pending.is_empty());
        assert!(queue.retries[0].armed.is_none());
        assert!(matches!(queue.slots.as_slice(), [WorkerSlot::Idle]));
        assert!(queue.drained());
    }

    #[test]
    fn completed_work_cannot_also_be_abandoned() {
        let mut queue = JobQueue::new(1);
        queue.phase = QueuePhase::Draining {
            timer_generation: bombay::behavior::TimerGeneration(0),
        };
        queue.accepted = 1;
        queue.slots = vec![WorkerSlot::Busy(job(7))];

        queue.complete(0, 7, false);
        queue.abandon_outstanding();

        assert_eq!(queue.completed, 1);
        assert!(queue.abandoned.is_empty());
        assert!(queue.drained());
    }

    #[test]
    fn draining_refuses_a_new_job_without_changing_accepted_accounting() {
        let mut queue = JobQueue::new(0);
        queue.phase = QueuePhase::Draining {
            timer_generation: bombay::behavior::TimerGeneration(0),
        };

        let refusal = queue.accept(job(9), Recipient::global(QueueAddr(99)));

        assert_eq!(queue.accepted, 0);
        assert!(queue.pending.is_empty());
        assert_eq!(
            refusal.message,
            Admission::Refused {
                job: job(9),
                reason: RefusalReason::Draining,
            }
        );
    }

    #[test]
    fn maintenance_admits_exact_capacity_then_returns_overflow_payload() {
        let mut queue = JobQueue::new(0);
        queue.enter_maintenance();

        for id in 0..MAINTENANCE_CAPACITY as u64 {
            let response = queue.accept(job(id), Recipient::global(QueueAddr(99)));
            assert_eq!(response.message, Admission::Accepted { job_id: id });
        }
        let overflow = job(99);
        let response = queue.accept(overflow, Recipient::global(QueueAddr(99)));

        assert_eq!(queue.accepted, MAINTENANCE_CAPACITY as u64);
        assert_eq!(queue.held.len(), MAINTENANCE_CAPACITY);
        assert_eq!(
            response.message,
            Admission::Refused {
                job: overflow,
                reason: RefusalReason::MaintenanceCapacity,
            }
        );
    }

    #[test]
    fn resume_places_held_fifo_before_later_accepted_work() {
        let mut queue = JobQueue::new(0);
        queue.enter_maintenance();
        queue.accept(job(1), Recipient::global(QueueAddr(99)));
        queue.accept(job(2), Recipient::global(QueueAddr(99)));

        queue.resume();
        queue.accept(job(3), Recipient::global(QueueAddr(99)));

        assert_eq!(queue.pending, [job(1), job(2), job(3)]);
        assert!(queue.held.is_empty());
    }

    #[test]
    fn shutdown_promotes_already_admitted_maintenance_work() {
        let mut queue = JobQueue::new(0);
        queue.enter_maintenance();
        queue.accept(job(1), Recipient::global(QueueAddr(99)));
        queue.accept(job(2), Recipient::global(QueueAddr(99)));

        let _actions = queue.begin_drain();

        assert_eq!(queue.pending, [job(1), job(2)]);
        assert!(queue.held.is_empty());
        assert!(matches!(queue.phase, QueuePhase::Draining { .. }));
        assert_eq!(queue.accepted, 2);
    }

    #[test]
    fn maintenance_commands_are_idempotent_and_resume_cannot_leave_draining() {
        let mut queue = JobQueue::new(0);
        queue.enter_maintenance();
        queue.enter_maintenance();
        assert!(matches!(queue.phase, QueuePhase::Maintenance));
        queue.resume();
        queue.resume();
        assert!(matches!(queue.phase, QueuePhase::Accepting));
        let _actions = queue.begin_drain();
        queue.resume();
        assert!(matches!(queue.phase, QueuePhase::Draining { .. }));
    }

    #[test]
    fn reporting_snapshot_does_not_mutate_queue_accounting() {
        let mut queue = JobQueue::new(0);
        queue.accepted = 3;
        queue.completed = 1;
        queue.failed = 1;
        queue.retried = 2;
        queue.abandoned.push(job(9));

        let before = queue.report();
        let snapshot = queue.report();
        let after = queue.report();

        assert_eq!(snapshot, before);
        assert_eq!(after, before);
    }

    #[test]
    fn stale_grace_generation_is_inert() {
        let mut queue = JobQueue::new(1);
        queue.phase = QueuePhase::Draining {
            timer_generation: bombay::behavior::TimerGeneration(4),
        };
        queue.accepted = 1;
        queue.slots = vec![WorkerSlot::Busy(job(5))];

        let actions = queue.grace_elapsed(bombay::behavior::TimerElapsed {
            id: super::DRAIN_TIMER,
            generation: bombay::behavior::TimerGeneration(3),
        });

        assert!(matches!(actions.become_, bombay::behavior::Step::Continue));
        assert!(queue.abandoned.is_empty());
        assert!(matches!(queue.slots.as_slice(), [WorkerSlot::Busy(_)]));
    }

    #[tokio::test]
    async fn queue_retries_failures_and_accounts_graceful_drain() {
        tokio::time::timeout(std::time::Duration::from_secs(2), super::run())
            .await
            .expect("the queue must wait for replacement realization without stalling");
    }
}
