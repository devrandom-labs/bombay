# Job-Queue Compositional Example + M1 Exit Gate (#218) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One job-queue mini-app wiring the whole M1 spine (spawn → registry → supervision/restart → death-watch → ask/tell → timers → pipe → drain), shipped as the flagship runnable example AND a gate-checked integration test proving the at-least-once invariant — plus a wart-capture pipeline filing every surfaced gap as an M1 issue.

**Architecture:** Push + ack + re-queue. `Dispatcher` (an `Actor + Watch + Supervisor`) holds pending/outstanding job state and a worker roster; `Worker` children crash on `Fail`/`Poison` jobs and are rebuilt by supervision; the factory closure `try_send`s `WorkerReplaced` back to the dispatcher (source-verified: `on_link_died` never fires for supervised children, and the factory is the only scope holding a rebuilt child's ref). An `Overseer: Watch` actor observes the dispatcher's death from outside. Spec: `docs/superpowers/specs/2026-07-28-218-job-queue-exit-gate-design.md`.

**Tech Stack:** `bombay` (crates/core), `bombay_macros::Msg` derive (dev-dep), tokio, `thiserror`, `bombay::test_support::terminate_bound`.

**Branch:** `feat/218-job-queue` (spec already committed).

**Verified API facts this plan relies on** (from source, 2026-07-28):
- `ActorRef::supervise(config: impl Into<RestartConfig>, factory: FnMut() -> ActorRef<A> + Send + 'static) -> Result<ActorId, TellError<()>>` — `actor_ref.rs:361`; first incarnation spawned inline; the loop drops the strong ref the factory returns, so the factory must anchor liveness (our roster `Recipient` + the in-flight `WorkerReplaced` message do this).
- Supervisor's `on_link_died` fires ONLY for non-child peers (`kind.rs:470-486` — child table consumes the notice). No rebuild hook exists (`kind.rs:759-789`).
- `RestartConfig::new(policy)` + const builders `.with_min_backoff/.with_max_restarts/.with_max_total/...` (`restart.rs:167-227`).
- `pipe_to_self(future, mapper: FnOnce(Result<T, PanicError>) -> A::Msg)` (`pipe.rs:77`).
- `send_after(delay, msg) -> TimerHandle` (`timer.rs:113`); dropping the handle detaches (timer still fires).
- `Registry::new/register/lookup/unregister` (`registry.rs:90-169`); `lookup` returns `Result<Option<ActorRef<A>>, WrongActorType>`.
- `ask(|reply| Msg) → AskRequest` with `.timeout(..)`/`.no_timeout()`, default 5 s; handler replies `reply.send(v)` / `reply.send_err(e)` (surfaces as the handler-error variant of `AskError`).
- `tell(msg)` awaits (backpressure) or `.try_send()` (sync fail-fast); `Recipient::try_tell` likewise.
- `watch` exists only on `ActorRef<A: Watch>` spawned linked; notify-only (`linked=false`).
- `terminate_bound()` from `bombay::test_support` (15 s native); integration tests wrap every terminal await.
- `bombay_macros` is a dev-dependency of package `bombay` → usable in `examples/` and `tests/`.

---

## Task 1: Wart pipeline — log file, up-front issues, CLAUDE.md rule, card bullets

**Files:**
- Create: `docs/warts/218-example-warts.md`
- Modify: `CLAUDE.md` (working-method section)

- [ ] **Step 1: Create the wart log with the three warts already surfaced by design research**

Write `docs/warts/218-example-warts.md`:

```markdown
# Wart log — card #218 (job-queue compositional example)

Every friction point hit while building the example lands here IMMEDIATELY,
then gets triaged into an M1 GitHub issue at the next phase boundary. An entry
is closed only when it carries its issue number. Severity: `blocker` (spine
cannot express the app) / `boilerplate` / `paper-cut`.

| # | Severity | Wart | Issue |
|---|----------|------|-------|
| 1 | boilerplate | Hand-written `Mailboxed` + `Actor` impls for every actor; only `Msg` has a derive, and it must be named as `bombay_macros::Msg` (dev-dep, no re-export through `bombay`). ~40 lines of ceremony per trivial actor. | TBD-in-step-3 |
| 2 | boilerplate | A supervisor cannot observe a child rebuild: `on_link_died` never fires for supervised children (kind.rs consumes the notice) and no hook delivers the fresh `ActorRef`/`ActorId` — the factory closure is the only seam, forcing the factory-`try_send`-to-self `WorkerReplaced` pattern with a mandatory `WeakActorRef` capture (strong capture = self-cycle, actor never ref-count-stops). | TBD-in-step-3 |
| 3 | paper-cut | Roster/rebuild bookkeeping (`WorkerReplaced`) travels through the ordinary bounded mailbox and competes with user backlog — a full mailbox silently loses the roster update. Evidence for #225 (control-signal lane). | comment on #225 in step 4 |
```

- [ ] **Step 2: Add the walking-skeleton rule to CLAUDE.md**

In `CLAUDE.md`, in the "Working method: cards-driven + TDD" numbered list, append a new numbered item after item 6:

```markdown
7. **The compositional example grows with every card (walking skeleton).** The
   job-queue app (`crates/core/examples/job_queue/`, card #218) is the M1
   compositional proof. Every subsequent feature card MUST carry an explicit
   checklist bullet — "extend the job-queue app + its integration test
   (`crates/core/tests/app_job_queue.rs`) with this feature" — added when the
   card is picked up, shipped or explicitly deferred like any other bullet.
   Warts surfaced while working on examples are logged in `docs/warts/` and
   filed as M1 issues at each phase boundary; a card does not close with
   unfiled warts.
```

- [ ] **Step 3: File the two up-front wart issues**

First check what #217 already covers: `gh issue view 217 --repo devrandom-labs/bombay`. #217 is the syn-3/manyhow migration of `bombay_macros` — infra, not new derives — so file both issues (adjust the first body if #217 turns out to already promise an Actor derive):

```bash
gh issue create --repo devrandom-labs/bombay \
  --title "macros: actor boilerplate reducer — derive(Actor) / actor! for the closed menu" \
  --milestone "M1 · Foundation: actor core" \
  --label foundation --label runtime \
  --body "$(cat <<'EOF'
Surfaced by #218 (job-queue example, wart log docs/warts/218-example-warts.md #1).

Declaring a trivial actor costs ~40 lines: manual `Mailboxed` impl, manual `Actor` impl with full `on_start`/`handle` signatures, plus a `bombay_macros::Msg` derive that must be spelled fully-qualified because `bombay` does not re-export it.

## Scope — one invariant per bullet
- [ ] Decide the reduction shape (`#[derive(Actor)]` on the state struct + attribute for the menu, vs an `actor!` item macro) — record as ADR.
- [ ] `bombay` re-exports the derive(s) so users write one import path.
- [ ] The `Msg` slot-size tripwire (#114) is preserved through the new surface.
- [ ] The job-queue example (#218) is rewritten on the new surface and its line count drop is recorded.

Blocked by #217 (syn 3 / manyhow migration lands first so the macro crate is not written twice).
EOF
)"
```

```bash
gh issue create --repo devrandom-labs/bombay \
  --title "core(supervision): no way to observe a child rebuild (fresh ActorId) from the supervisor actor" \
  --milestone "M1 · Foundation: actor core" \
  --label foundation --label runtime \
  --body "$(cat <<'EOF'
Surfaced by #218 (job-queue example, wart log docs/warts/218-example-warts.md #2).

A supervisor's `on_link_died` never fires for its own supervised children (the restart table consumes the notice, `actor/kind.rs` `dispatch_death`), and `supervise` returns only the FIRST incarnation's `ActorId` — no hook or message delivers a rebuilt child's fresh `ActorRef`/`ActorId`. Any supervisor that routes work to children (a dispatcher, a pool) must smuggle the new ref out of the factory closure via a `WeakActorRef` self-`try_send` — mandatory weak capture (strong = self-cycle, the supervisor never ref-count-stops), best-effort delivery through the user mailbox.

## Scope — one invariant per bullet
- [ ] Design the observation seam (an `on_child_rebuilt(old_id, new_id)`-style hook on `Supervisor`, vs the factory receiving context, vs a first-class routing primitive in #228) — record as ADR; #228 (worker pool) is the first consumer.
- [ ] The job-queue example's `WorkerReplaced` pattern is replaced by (or documented as) the sanctioned pattern.

Related: #225 (roster updates should not compete with user backlog), #228.
EOF
)"
```

- [ ] **Step 4: Cross-file — add both issues to project board #4, note evidence on #225, fill issue numbers into the wart log**

```bash
gh project item-add 4 --owner devrandom-labs --url <issue-1-url>
gh project item-add 4 --owner devrandom-labs --url <issue-2-url>
gh issue comment 225 --repo devrandom-labs/bombay --body "Evidence from #218 (wart log #3): the dispatcher's factory-to-self \`WorkerReplaced\` roster update rides the ordinary bounded mailbox and competes with user backlog — a full mailbox silently drops it (best-effort \`try_send\`). Exactly the class of supervision-adjacent signal this card's control lane should carry."
```

Replace the two `TBD-in-step-3` cells in `docs/warts/218-example-warts.md` with the real issue numbers.

- [ ] **Step 5: Update #218's card checklist**

```bash
gh issue comment 218 --repo devrandom-labs/bombay --body "Scope addendum (one invariant per bullet): - [ ] all warts surfaced while building the example are filed as M1 issues and listed in docs/warts/218-example-warts.md (none open as TBD). Up-front warts filed: <issue-1>, <issue-2>, evidence comment on #225."
```

- [ ] **Step 6: Commit**

```bash
git add docs/warts/218-example-warts.md CLAUDE.md
git commit -m "docs(warts): wart pipeline + walking-skeleton rule; up-front warts filed [#218]"
```

---

## Task 2: Failing gate test — sequence/protocol (red)

**Files:**
- Create: `crates/core/tests/app_job_queue.rs`
- Create: `crates/core/examples/job_queue/main.rs` (stub so the example target exists)

The app module is shared verbatim between the example and the test via `#[path]` inclusion — one source of truth, both compiled against the real public API.

- [ ] **Step 1: Create the example stub**

`crates/core/examples/job_queue/main.rs`:

```rust
//! Runnable job-queue demo — bombay's M1 compositional example (card #218).
//! Run: `cargo run -p bombay --example job_queue`

mod app;

#[tokio::main]
async fn main() {
    unimplemented!("demo narration lands in Task 6");
}
```

(`app.rs` does not exist yet — that is the red state.)

- [ ] **Step 2: Write the sequence/protocol test**

`crates/core/tests/app_job_queue.rs`:

```rust
//! Card #218 — the M1 exit gate: the four cross-cutting test categories
//! applied at APP level over the job-queue mini-app.
//!
//! The app under test is the real example (`examples/job_queue/app.rs`),
//! included by path so the demo and the gate compile the same code.

#[path = "../examples/job_queue/app.rs"]
mod app;

use std::{future::IntoFuture, sync::Arc, time::Duration};

use app::{DISPATCHER_NAME, Dispatcher, DispatcherConfig, DispatcherMsg, DrainReport, Job, JobKind};
use bombay::{
    registry::Registry,
    restart::{RestartConfig, RestartPolicy},
    test_support::terminate_bound,
};
use tokio::time::timeout;

const WORK: Duration = Duration::from_millis(5);

fn config(registry: &Arc<Registry>) -> DispatcherConfig {
    DispatcherConfig {
        workers: 2,
        queue_cap: 8,
        retry_cap: 2,
        retry_backoff: Duration::from_millis(5),
        restart: RestartConfig::new(RestartPolicy::Permanent)
            .with_min_backoff(Duration::from_millis(1))
            .with_max_restarts(50)
            .with_max_total(200),
        registry: Arc::clone(registry),
    }
}

/// Every terminal await is bounded — a hung app flow must fail the test, not
/// stall the mutants/MIRI lanes.
async fn bounded<F: IntoFuture>(fut: F) -> F::Output {
    timeout(terminate_bound(), fut)
        .await
        .expect("app flow must resolve within the terminate bound")
}

fn ok_job(id: u64) -> Job {
    Job { id, kind: JobKind::Ok(WORK) }
}

#[tokio::test]
async fn sequence_submit_stats_drain_reports_exact_counts() {
    let registry = Arc::new(Registry::new());
    let app = app::start(config(&registry)).await;
    // clients resolve the dispatcher by NAME — the registry seam is load-bearing
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    for id in 0..8u64 {
        bounded(dispatcher.ask(|reply| DispatcherMsg::Submit { job: ok_job(id), reply }))
            .await
            .expect("submit accepted under cap");
    }

    let stats = bounded(dispatcher.ask(|reply| DispatcherMsg::Stats { reply }))
        .await
        .expect("stats reply");
    assert_eq!(stats.submitted, 8);

    let report = bounded(dispatcher.ask(|reply| DispatcherMsg::Drain { reply }).no_timeout())
        .await
        .expect("drain reply");
    assert_eq!(
        report,
        DrainReport { submitted: 8, completed: 8, failed: 0, retried: 0, rebuilds: 0 },
        "every submitted job completed exactly once on the happy path",
    );
    drop(app);
}
```

- [ ] **Step 3: Run to verify it fails for the right reason**

```bash
cargo test -p bombay --test app_job_queue 2>&1 | head -30
```

Expected: compile error — `examples/job_queue/app.rs` not found (`#[path]` include). NOT a wrong-API error in the test body itself.

---

## Task 3: The app — types, messages, Worker, Dispatcher, Overseer (green)

**Files:**
- Create: `crates/core/examples/job_queue/app.rs`

Before coding, verify three unconfirmed details and note results in the wart log if surprising:
1. `rg -n 'enum AskError' crates/core/src/error.rs -A 25` — exact handler-error and timeout variant names (plan assumes `Handler(E)` and a reply-timeout variant; adjust test matches in Task 5 to the real names).
2. `rg -n 'derive' crates/core/src/restart.rs | head -5` — `RestartConfig: Clone` (needed: one config per `supervise` call). If not `Clone`, construct it per-slot from `DispatcherConfig` fields instead.
3. `rg -n 'impl.*Debug.*Recipient|derive' crates/core/src/actor/recipient.rs | head` — `Recipient: Debug` (needed by `#[derive(Debug)]` on `DispatcherMsg`). If absent, hand-write `impl fmt::Debug for DispatcherMsg`.

- [ ] **Step 1: Write the full app module**

`crates/core/examples/job_queue/app.rs`:

```rust
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
        Actor, ActorRef, Recipient, Spawn as _, SpawnLinked as _, SpawnSupervised as _,
        Supervisor, Watch, WeakActorRef,
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
fn bump(counter: &mut u64) {
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
        Ok(Self { slot: args.slot, dispatcher: args.dispatcher })
    }

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
                .tell(Done { slot: self.slot, job_id })
                .await
                .map_err(WorkerError::AckLost),
        }
    }
}

// ------------------------------------------------------------ dispatcher ----

pub const DISPATCHER_NAME: &str = "dispatcher";

#[derive(Debug, bombay_macros::Msg)]
pub enum DispatcherMsg {
    Submit { job: Job, reply: ReplySender<(), SubmitError> },
    Done(Done),
    Retry(Job),
    WorkerReplaced { slot: usize, id: ActorId, worker: Recipient<Job> },
    Drain { reply: ReplySender<DrainReport> },
    Stats { reply: ReplySender<Stats> },
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
    stats: Stats,
    draining: bool,
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
                .supervise(cfg.restart.clone(), move || {
                    let worker = Worker::spawn(WorkerArgs {
                        slot,
                        dispatcher: done_port.clone(),
                    });
                    if let Some(disp) = disp.upgrade() {
                        // best-effort: a full mailbox loses the roster update —
                        // wart #3, evidence on #225
                        let _ = disp
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
            stats: Stats::default(),
            draining: false,
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
                self.maybe_finish_drain(stop);
            }
            DispatcherMsg::Retry(job) => {
                self.pending.push_front(job);
                self.dispatch();
            }
            DispatcherMsg::WorkerReplaced { slot, id, worker } => {
                let rebuilt = self.roster.insert(slot, (id, worker)).is_some();
                if rebuilt {
                    bump(&mut self.stats.rebuilds);
                    self.requeue_outstanding(slot, &actor_ref);
                }
                self.stats.worker_ids = self.roster.values().map(|(id, _)| *id).collect();
                self.dispatch();
                self.maybe_finish_drain(stop);
            }
            DispatcherMsg::Stats { reply } => {
                let _ = reply.send(self.stats.clone());
            }
            DispatcherMsg::Drain { reply } => {
                self.draining = true;
                self.drain_reply = Some(reply);
                self.maybe_finish_drain(stop);
            }
        }
        Ok(())
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), AppError> {
        // reason-independent resource release only (post-panic safe)
        self.registry.unregister(DISPATCHER_NAME);
        Ok(())
    }
}

impl Watch for Dispatcher {}
impl Supervisor for Dispatcher {}

impl Dispatcher {
    /// Hand pending jobs to idle slots. `try_tell` keeps this handler
    /// non-blocking (a worker's mailbox holds at most Run + WorkDone, so a
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
            let Some(job) = self.pending.pop_front() else { break };
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
    fn requeue_outstanding(&mut self, slot: usize, actor_ref: &ActorRef<Self>) {
        let Some(job) = self.outstanding.remove(&slot) else { return };
        let attempts = self.retries.entry(job.id).or_insert(0);
        *attempts = attempts.checked_add(1).expect("u32 retry counter overflow");
        if *attempts > self.retry_cap {
            self.retries.remove(&job.id);
            bump(&mut self.stats.failed);
        } else {
            bump(&mut self.stats.retried);
            // dropping the handle detaches: the timer still fires
            let _detached = actor_ref.send_after(self.retry_backoff, DispatcherMsg::Retry(job));
        }
    }

    fn maybe_finish_drain(&mut self, stop: &mut bool) {
        if !self.draining || !self.pending.is_empty() || !self.outstanding.is_empty() {
            return;
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
        *stop = true;
    }
}

// -------------------------------------------------------------- overseer ----

/// Watches the dispatcher from outside — the app-level death-watch consumer.
#[derive(Debug, bombay_macros::Msg)]
pub enum OverseerMsg {
    /// Did the dispatcher die, and was it a normal stop?
    Observed { reply: ReplySender<Option<(ActorId, bool)>> },
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
    pub dispatcher: ActorRef<Dispatcher>,
    pub overseer: ActorRef<Overseer>,
}

/// Wires the whole spine: linked overseer, supervised dispatcher (which
/// registers itself and supervises its workers in `on_start`), watch edge.
pub async fn start(cfg: DispatcherConfig) -> App {
    let overseer = Overseer::spawn_linked(());
    let dispatcher = Dispatcher::spawn_supervised(cfg);
    overseer
        .watch(&dispatcher)
        .await
        .expect("overseer was spawned linked");
    App { dispatcher, overseer }
}
```

Notes for the implementer:
- If `ActorId` is not `Copy`, change `map(|(id, _)| *id)` to `.map(|(id, _)| id.clone())` and `reply.send(self.seen)` to `reply.send(self.seen.clone())`.
- If `Recipient` is not `Debug`, hand-write `impl fmt::Debug for DispatcherMsg` (log this as a wart — derives blocked by handle types).
- If `WeakActorRef::upgrade` has a different name, `rg -n 'fn upgrade' crates/core/src/actor/actor_ref.rs`.
- Any API mismatch discovered here goes in the wart log immediately.

- [ ] **Step 2: Run the sequence test to green**

```bash
cargo test -p bombay --test app_job_queue -- sequence 2>&1 | tail -15
```

Expected: PASS. If it hangs, the drain never completed — check `maybe_finish_drain` is called from `Done` and that `dispatch` assigns before `Submit` replies.

- [ ] **Step 3: Verify supervisor teardown stops the workers (read, don't assume)**

```bash
rg -n 'stop_surviving_children|teardown|fn run_supervised' crates/core/src/actor/kind.rs | head -20
```

Read the supervised-loop teardown path: confirm a supervisor stopping with `Normal` stops its remaining children. If it does NOT, this is a **blocker-class wart** (drained app leaks live workers) — log it, file the issue, and surface to Joel before working around it.

- [ ] **Step 4: Commit**

```bash
git add crates/core/examples/job_queue crates/core/tests/app_job_queue.rs
git commit -m "feat(examples): job-queue app + sequence/protocol gate test [#218]"
```

---

## Task 4: Lifecycle test — crash, rebuild, re-queue, retry-cap (red → green)

**Files:**
- Modify: `crates/core/tests/app_job_queue.rs` (append)

- [ ] **Step 1: Append the lifecycle test**

```rust
#[tokio::test]
async fn lifecycle_crash_rebuild_requeue_no_job_lost() {
    let registry = Arc::new(Registry::new());
    let app = app::start(config(&registry)).await;
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    let before = bounded(dispatcher.ask(|reply| DispatcherMsg::Stats { reply }))
        .await
        .expect("stats reply");
    assert_eq!(before.worker_ids.len(), 2, "both slots announced via WorkerReplaced");

    // 6 completable jobs + 2 always-crashing jobs (one Err, one panic)
    for id in 0..6u64 {
        bounded(dispatcher.ask(|reply| DispatcherMsg::Submit { job: ok_job(id), reply }))
            .await
            .expect("submit accepted");
    }
    for job in [
        Job { id: 100, kind: JobKind::Fail },
        Job { id: 101, kind: JobKind::Poison },
    ] {
        bounded(dispatcher.ask(|reply| DispatcherMsg::Submit { job, reply }))
            .await
            .expect("submit accepted");
    }

    let report = bounded(dispatcher.ask(|reply| DispatcherMsg::Drain { reply }).no_timeout())
        .await
        .expect("drain reply");

    // retry_cap = 2: each crashing job runs 3 times (initial + 2 retries)
    // then is recorded failed. Every crash rebuilds the worker (Permanent).
    assert_eq!(
        report,
        DrainReport { submitted: 8, completed: 6, failed: 2, retried: 4, rebuilds: 6 },
        "at-least-once: no job lost, crashing jobs retried to cap then recorded",
    );

    // the dispatcher stopped normally after drain; the overseer must see it
    let dispatcher_id = dispatcher.id();
    let mut observed = None;
    for _ in 0..200 {
        observed = bounded(app.overseer.ask(|reply| app::OverseerMsg::Observed { reply }))
            .await
            .expect("overseer reply");
        if observed.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        observed,
        Some((dispatcher_id, true)),
        "death-watch: overseer observes the dispatcher's normal death",
    );
}
```

Fresh-`ActorId` check: append inside the test, before the drain, a stats poll asserting rebuild visibility:

```rust
    // wait until the crashes have produced at least one rebuild, then assert
    // the roster carries a FRESH ActorId (a rebuilt child is a new actor)
    let mut after = None;
    for _ in 0..200 {
        let stats = bounded(dispatcher.ask(|reply| DispatcherMsg::Stats { reply }))
            .await
            .expect("stats reply");
        if stats.rebuilds > 0 {
            after = Some(stats);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let after = after.expect("a crash must produce a rebuild within the bound");
    assert!(
        after.worker_ids.iter().any(|id| !before.worker_ids.contains(id)),
        "a rebuilt worker must carry a fresh ActorId: before {:?} after {:?}",
        before.worker_ids,
        after.worker_ids,
    );
```

(Place the poll block between the submits and the drain ask.)

- [ ] **Step 2: Run it**

```bash
cargo test -p bombay --test app_job_queue -- lifecycle 2>&1 | tail -20
```

Expected: PASS. Likely first failures and their meanings:
- `rebuilds` off by N → the initial `WorkerReplaced` from `supervise`'s inline first spawn counted as a rebuild; the `is_some()` guard in the handler must make first-insert NOT count.
- Hang at drain → a crashed job neither retried nor failed: check `requeue_outstanding` runs on the `rebuilt` branch and that `Retry` re-enters `dispatch`.
- `completed = 7` → stale-ack guard missing (`Done` from a pre-rebuild incarnation double-counted).

- [ ] **Step 3: Commit**

```bash
git add crates/core/tests/app_job_queue.rs
git commit -m "test(examples): lifecycle gate — crash, rebuild, re-queue, retry cap [#218]"
```

---

## Task 5: Defensive-boundary test (red → green)

**Files:**
- Modify: `crates/core/tests/app_job_queue.rs` (append)

- [ ] **Step 1: Confirm the real `AskError` variant names**

```bash
rg -n 'pub enum AskError' -A 30 crates/core/src/error.rs
```

The test below writes `AskError::Handler(..)` for a `send_err` reply and a timeout variant spelled `AskError::ReplyTimeout { .. }` — replace both `matches!` patterns with the real names found here, and assert the classification methods (`is_retryable`/`is_terminal`) per the doc comments on those variants (#113 decided timeouts are NOT retryable).

- [ ] **Step 2: Append the boundary test**

```rust
#[tokio::test]
async fn boundary_queue_full_draining_and_timeout_classified() {
    use bombay::error::AskError;

    let registry = Arc::new(Registry::new());
    // one slow worker + tiny queue so rejection layers are reachable
    let mut cfg = config(&registry);
    cfg.workers = 1;
    cfg.queue_cap = 2;
    let _app = app::start(cfg).await;
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    let slow = Duration::from_millis(500);
    // job 0 is dispatched immediately (outstanding); 1 and 2 fill pending
    for id in 0..3u64 {
        bounded(dispatcher.ask(|reply| DispatcherMsg::Submit {
            job: Job { id, kind: JobKind::Ok(slow) },
            reply,
        }))
        .await
        .expect("accepted up to cap");
    }

    // pending == queue_cap → typed app-level rejection, not a mailbox error
    let err = bounded(dispatcher.ask(|reply| DispatcherMsg::Submit { job: ok_job(9), reply }))
        .await
        .expect_err("queue is full");
    assert!(
        matches!(err, AskError::Handler(SubmitError::QueueFull)),
        "expected typed QueueFull, got {err:?}",
    );

    // a zero ask deadline elapses before any reply: timeout classified terminal
    let err = bounded(
        dispatcher
            .ask(|reply| DispatcherMsg::Stats { reply })
            .timeout(Duration::ZERO),
    )
    .await
    .expect_err("zero deadline cannot be met");
    assert!(err.is_terminal(), "#113: ask timeouts are not retryable, got {err:?}");

    // drain while the slow jobs are still in flight, then submit → Draining
    let drain_dispatcher = dispatcher.clone();
    let drain = tokio::spawn(async move {
        drain_dispatcher
            .ask(|reply| DispatcherMsg::Drain { reply })
            .no_timeout()
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await; // drain accepted, jobs still running
    let err = bounded(dispatcher.ask(|reply| DispatcherMsg::Submit { job: ok_job(10), reply }))
        .await
        .expect_err("draining rejects new work");
    assert!(
        matches!(err, AskError::Handler(SubmitError::Draining)),
        "expected typed Draining, got {err:?}",
    );

    let report = bounded(drain).await.expect("drain task").expect("drain reply");
    assert_eq!(report.completed, 3, "in-flight and pending jobs finish during drain");
}
```

Import fix: this test needs `app::SubmitError` — add `SubmitError` to the `use app::{...}` list at the top of the file.

- [ ] **Step 3: Run it**

```bash
cargo test -p bombay --test app_job_queue -- boundary 2>&1 | tail -15
```

Expected: PASS. If the `Draining` assert sees `QueueFull`: the drain ask hadn't been handled within 50 ms — raise the sleep to 100 ms (still « the 500 ms job).

- [ ] **Step 4: Commit**

```bash
git add crates/core/tests/app_job_queue.rs
git commit -m "test(examples): defensive-boundary gate — QueueFull, Draining, timeout class [#218]"
```

---

## Task 6: Linearizability test — concurrent producers (red → green)

**Files:**
- Modify: `crates/core/tests/app_job_queue.rs` (append)

- [ ] **Step 1: Append the concurrency test (real overlap: multi-thread flavor + Barrier)**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linear_concurrent_producers_no_loss_no_phantom() {
    let registry = Arc::new(Registry::new());
    let mut cfg = config(&registry);
    cfg.workers = 3;
    cfg.queue_cap = 1024; // accept everything; loss-accounting is the subject
    let _app = app::start(cfg).await;
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    const PRODUCERS: usize = 8;
    const PER_PRODUCER: u64 = 25;
    let barrier = Arc::new(tokio::sync::Barrier::new(PRODUCERS));
    let mut producers = Vec::new();
    for p in 0..PRODUCERS {
        let dispatcher = dispatcher.clone();
        let barrier = Arc::clone(&barrier);
        producers.push(tokio::spawn(async move {
            barrier.wait().await; // all producers submit concurrently
            let mut accepted = 0u64;
            for i in 0..PER_PRODUCER {
                let id = u64::try_from(p).expect("small") * 1000 + i;
                let kind = if i % 10 == 3 {
                    JobKind::Fail // ~10% of jobs crash their worker
                } else {
                    JobKind::Ok(Duration::from_millis(1))
                };
                dispatcher
                    .ask(|reply| DispatcherMsg::Submit { job: Job { id, kind }, reply })
                    .no_timeout()
                    .await
                    .expect("submit accepted under a 1024 cap");
                accepted += 1;
            }
            accepted
        }));
    }

    let mut submitted = 0u64;
    for producer in producers {
        submitted += bounded(producer).await.expect("producer task");
    }

    let report = bounded(dispatcher.ask(|reply| DispatcherMsg::Drain { reply }).no_timeout())
        .await
        .expect("drain reply");
    assert_eq!(report.submitted, submitted, "dispatcher accounted every accepted submit");
    assert_eq!(
        report.completed + report.failed,
        submitted,
        "at-least-once accounting: no lost job, no phantom job",
    );
}
```

`accepted += 1` in a test loop bounded by `PER_PRODUCER` is fine (test code, bounded).

- [ ] **Step 2: Run the full gate test file**

```bash
cargo test -p bombay --test app_job_queue 2>&1 | tail -15
```

Expected: all 4 tests PASS. If the linear test trips the supervisor's restart budget (`RestartLimitExceeded` → drain ask dies): ~20 crashing jobs × 3 attempts = ~60 rebuilds over 3 slots ≈ 20 consecutive per slot — the `config()` helper's `with_max_restarts(50).with_max_total(200)` covers it; if it still trips, raise those two numbers in `config()` and log a wart (restart accounting surprising under bursty crash load).

- [ ] **Step 3: Commit**

```bash
git add crates/core/tests/app_job_queue.rs
git commit -m "test(examples): linearizability gate — concurrent producers, no loss [#218]"
```

---

## Task 7: Runnable demo narration + README

**Files:**
- Modify: `crates/core/examples/job_queue/main.rs` (replace stub body)
- Modify: `README.md`

- [ ] **Step 1: Check whether a fmt subscriber is available to examples**

```bash
rg -n 'tracing-subscriber' crates/core/Cargo.toml Cargo.toml
```

If `tracing-subscriber` is NOT a dev-dependency: add it to `crates/core/Cargo.toml` `[dev-dependencies]` with its version declared at the workspace root per the shared convention (`tracing-subscriber = { workspace = true }` + root `[workspace.dependencies]` entry, latest version), and log a wart (`paper-cut`: no out-of-the-box way to see bombay's own tracing story from an example). `cargo-hakari` is not wired in this repo — skip it.

- [ ] **Step 2: Write the demo**

Replace `crates/core/examples/job_queue/main.rs`:

```rust
//! Runnable job-queue demo — bombay's M1 compositional example (card #218).
//!
//! Twenty jobs through a supervised worker pool: two poison (panic), one
//! failing (typed error), the rest complete. Watch the restarts in the
//! tracing output, then read the drain report.
//!
//! Run: `cargo run -p bombay --example job_queue`

mod app;

use std::{sync::Arc, time::Duration};

use app::{DISPATCHER_NAME, Dispatcher, DispatcherConfig, DispatcherMsg, Job, JobKind, OverseerMsg};
use bombay::{
    registry::Registry,
    restart::{RestartConfig, RestartPolicy},
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init(); // bombay only emits; the app subscribes

    let registry = Arc::new(Registry::new());
    let app = app::start(DispatcherConfig {
        workers: 3,
        queue_cap: 32,
        retry_cap: 2,
        retry_backoff: Duration::from_millis(50),
        restart: RestartConfig::new(RestartPolicy::Permanent)
            .with_min_backoff(Duration::from_millis(10))
            .with_max_restarts(20),
        registry: Arc::clone(&registry),
    })
    .await;

    // clients never hold the spawn handle — they resolve by name
    let dispatcher = registry
        .lookup::<Dispatcher>(DISPATCHER_NAME)
        .expect("registered under the dispatcher type")
        .expect("dispatcher is alive");

    for id in 0..20u64 {
        let kind = match id {
            7 | 11 => JobKind::Poison,
            13 => JobKind::Fail,
            _ => JobKind::Ok(Duration::from_millis(20)),
        };
        dispatcher
            .ask(|reply| DispatcherMsg::Submit { job: Job { id, kind }, reply })
            .await
            .expect("submit accepted");
    }

    let report = dispatcher
        .ask(|reply| DispatcherMsg::Drain { reply })
        .no_timeout()
        .await
        .expect("drain reply");
    println!("drained: {report:?}");

    // give the death notice a moment to reach the overseer
    tokio::time::sleep(Duration::from_millis(100)).await;
    let observed = app
        .overseer
        .ask(|reply| OverseerMsg::Observed { reply })
        .await
        .expect("overseer reply");
    println!("overseer saw dispatcher exit: {observed:?}");
}
```

- [ ] **Step 3: Run the demo and check the narrative**

```bash
cargo run -p bombay --example job_queue
```

Expected: warn-level restart lines from bombay's own tracing for the poison/fail crashes, then
`drained: DrainReport { submitted: 20, completed: 17, failed: 3, retried: 6, rebuilds: 9 }`
(3 crashing jobs × 3 attempts each) and `overseer saw dispatcher exit: Some((ActorId(..), true))` — exact `ActorId` rendering per its `Debug` impl.

- [ ] **Step 4: README — public-API-changed case (new flagship example)**

In `README.md`, after the "Using bombay" code block's closing paragraph (the `#[derive(bombay_macros::Msg)]` note, line ~70), add:

```markdown
For the whole spine composed — supervised workers, crash-and-rebuild, re-queued
jobs, name lookup, timers, pipes, and a drained shutdown — run the flagship
example: `cargo run -p bombay --example job_queue` (source:
[`crates/core/examples/job_queue/`](crates/core/examples/job_queue/), gate test:
[`crates/core/tests/app_job_queue.rs`](crates/core/tests/app_job_queue.rs)).
```

- [ ] **Step 5: Commit**

```bash
git add crates/core/examples/job_queue/main.rs README.md crates/core/Cargo.toml Cargo.toml
git commit -m "feat(examples): job-queue demo narration + README pointer [#218]"
```

(Drop the two Cargo.toml paths if Step 1 needed no dep change.)

---

## Task 8: Phase-boundary wart triage

**Files:**
- Modify: `docs/warts/218-example-warts.md`

- [ ] **Step 1: Sweep the implementation for unlogged friction**

Re-read the diff (`git diff origin/main...HEAD -- crates/core`) asking: what was awkward that a downstream user will also hit? Candidates already predicted by the plan — log whichever actually materialized, plus anything new:
- `Recipient`/`ReplySender` blocking `#[derive(Debug)]` on menus (if hit in Task 3).
- No join-handle/`wait_for_shutdown` from the ergonomic `spawn_supervised` path (quiescence had to be observed via the Overseer poll loop).
- Anything discovered in the Task 3 teardown verification (Step 3).

- [ ] **Step 2: File one M1 issue per new wart** (same shape as Task 1 Step 3: title `core(...)`/`docs(...)`, milestone `M1 · Foundation: actor core`, body citing `docs/warts/218-example-warts.md`, one invariant per bullet, added to board #4). Fill issue numbers into the wart table — zero `TBD` cells may remain.

- [ ] **Step 3: Commit**

```bash
git add docs/warts/218-example-warts.md
git commit -m "docs(warts): phase-boundary triage — all #218 warts filed [#218]"
```

---

## Task 9: Gate, PR, card close-out

- [ ] **Step 1: Format + tracked-file check**

```bash
cargo fmt --all
git status --short   # everything intended must be TRACKED (flake sees git tree only)
git add -A && git status --short
```

- [ ] **Step 2: Run the single gate**

```bash
nix flake check 2>&1 | tail -25
```

Expected: green. Notes: examples are not mutated by the mutants gate (it runs over the library) — `mutants-baseline.json` needs no entry for app code; if the gate complains about an untracked file, `git add` it (a check over an untracked file passes vacuously).

- [ ] **Step 3: Commit any gate fixes, push, open the PR**

```bash
git push -u origin feat/218-job-queue
gh pr create --repo devrandom-labs/bombay --title "test(core): M1 exit gate — job-queue compositional app + integration test [#218]" --body "$(cat <<'EOF'
Closes #218.

The M1 compositional proof: one job-queue mini-app (`crates/core/examples/job_queue/`) wiring the whole spine — spawn_supervised → registry lookup → supervision restart under induced Fail/Poison crashes → factory-driven WorkerReplaced re-queue → death-watch via an Overseer → ask/tell with typed rejections → send_after retry backoff → pipe_to_self work → drained shutdown — and a gate-checked integration test (`crates/core/tests/app_job_queue.rs`) applying the four cross-cutting categories at APP level: sequence/protocol, lifecycle (at-least-once across rebuilds, fresh ActorId asserted), defensive boundary (QueueFull / Draining / timeout classification), linearizability (8 concurrent producers, completed + failed == submitted).

Spec: docs/superpowers/specs/2026-07-28-218-job-queue-exit-gate-design.md
Design revision recorded there: supervisors cannot observe child rebuilds from on_link_died — the factory try_send pattern is the workaround, filed as a wart issue.

Warts filed (docs/warts/218-example-warts.md): <list issue numbers>.
Walking-skeleton rule added to CLAUDE.md: every subsequent feature card extends this app + test.
EOF
)"
```

- [ ] **Step 4: Card close-out checklist** — on merge: every #218 bullet either named-shipped (file/test) or deferred-to-named-card in a closing comment; wart-table has zero TBD; board Status → Done.

---

## Self-review (done at plan-writing time)

- **Spec coverage:** wart protocol → Tasks 1/8; app + revision → Task 3; four categories → Tasks 2/4/5/6; demo + README → Task 7; CLAUDE.md rule → Task 1; gate discipline → Task 9. No spec section unimplemented.
- **Known unknowns made explicit** (verify steps, not placeholders): `AskError` variant names (Task 5 Step 1), `RestartConfig: Clone` / `Recipient: Debug` / `ActorId: Copy` (Task 3 preamble), supervisor teardown of children (Task 3 Step 3), fmt subscriber dep (Task 7 Step 1). Each has a concrete fallback in place.
- **Type consistency:** `Recipient<Done>` (worker→dispatcher ack), `Recipient<Job>` (dispatcher→worker run) used consistently; `ReplySender<(), SubmitError>` + `send_err` matches the spec's revision note; `DrainReport` fields match both the struct and every assert.
