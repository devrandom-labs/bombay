# Design: `job_queue` compositional example + M1 exit gate (card #218)

**Date:** 2026-07-28
**Card:** [#218](https://github.com/devrandom-labs/bombay/issues/218) — test(core): M1 exit gate — compositional end-to-end app + integration test
**Status:** approved (decisions recorded on the card, 2026-07-28)

## Problem

Every test in `crates/core/tests/` is per-module. Nothing proves the rebuilt spine —
spawn, registry, supervision, death-watch, ask/tell, timers, pipe — composes as a
real application. Per-module green and compositional green are different
guarantees; the second is what downstream consumers (bombay-nexus, agency) rely
on. M1 has no exit gate; this is it.

## Decisions already fixed on the card

- Mini-app is a **job queue** (producer + workers + supervisor). Worker-pool
  rejected (pre-empts #228's real primitive); mini-bank rejected (weak fit for
  the upcoming mailbox/QoS cards).
- Ships as **both** the flagship runnable `crates/core/examples/` app and a
  gate-checked integration test over the same wiring.
- Lands **now**, before the remaining M1 feature cards; those cards then extend
  this app (walking-skeleton process rule, added to CLAUDE.md with this card).
- Scope is app A only. Satellite examples (health-monitor, pipeline) are #216
  bullets, landed after as their own small PRs.

## Architecture (approach 1: push + ack + re-queue)

At-least-once job delivery. The invariant the whole app exists to prove:
**no submitted job is lost across a worker crash and supervisor rebuild** —
every job is completed at least once or explicitly recorded failed, and the
queue is empty at drain. Exactly-once is impossible without idempotence (a
worker can crash after the work, before the ack); the test asserts ≥1×, never
exactly-once.

Rejected alternatives: pull/work-claiming (same ack complexity, wastes the
watch/supervision surface — a rebuilt worker just polls again, nothing to
re-route); minimal-linear with accepted job loss (cannot carry the recovery
invariant, too weak for an exit gate).

### Components

| Component | Traits / spawn path | State |
|---|---|---|
| `Dispatcher` | `Actor + Watch + Supervisor` (`OneForOne`), `spawn_supervised`, registered in `Registry` as `"dispatcher"` | `VecDeque<Job>` pending; `HashMap<ActorId, Job>` outstanding (in-flight per worker); roster `Vec<(ActorId, Recipient<WorkerMsg>)>`; `draining: bool`; stats counters; per-job retry counts |
| `Worker` × N | plain `Actor` (watched, does not watch), children via `supervise(config, factory)` with explicit `RestartPolicy::Permanent` | worker slot index; `Recipient<Done>` back to dispatcher |
| `Overseer` | `Actor + Watch`, `spawn_linked`, `watch`es (notify-only) the dispatcher | last observed death: `Option<(ActorId, bool /* normal */)>` |
| Producer / client | not an actor — plain tokio task in `main` / test body; finds dispatcher through `Registry` | — |

**Design revision (2026-07-28, source-verified against `actor/kind.rs`):** a
supervisor's `on_link_died` hook never fires for its own supervised children —
the restart table consumes the death notice (`kind.rs` `dispatch_death`), and
no rebuild hook delivers the fresh incarnation's `ActorRef`/`ActorId` back to
the supervisor; the factory closure is the only scope holding it. Roster
maintenance and outstanding-job re-queue therefore ride a
`WorkerReplaced { slot, id, worker }` message that the factory `try_send`s to
the dispatcher on **every** incarnation (initial and rebuilt). The factory
captures a `WeakActorRef<Dispatcher>` — a strong capture would be a self-cycle
(the factory lives in the dispatcher's own child table), so the dispatcher
would never ref-count-stop. Death-watch stays load-bearing through the
`Overseer`, which observes the dispatcher's death from outside. Both
underlying gaps are filed as warts (rebuild-observation hole; roster update
competing with user backlog → #225 evidence).

`Job { id: u64, payload: JobKind }`, `JobKind::{ Ok(Duration), Fail, Poison }`:
`Ok` completes after simulated async work, `Fail` makes the worker return
`Err` (controlled crash), `Poison` makes it panic (caught, `on_panic`). Both
crash kinds route to supervisor rebuild.

### Messages (closed menus, `#[derive(Msg)]`)

```text
DispatcherMsg:
  Submit { job, reply: ReplySender<(), SubmitError> }        // ask; typed rejection via send_err
  Done(Done { slot, job_id })                                // worker ack; tell via Recipient<Done>
  Retry { job }                                              // send_after re-entry
  WorkerReplaced { slot, id, worker: Recipient<Job> }        // factory try_send, every incarnation
  Drain  { reply: ReplySender<DrainReport> }                 // shutdown protocol
  Stats  { reply: ReplySender<Stats> }                       // observability ask

WorkerMsg:
  Run(Job)                                                   // via From<Job> (Recipient<Job> roster)
  WorkDone { job_id, outcome }                               // pipe_to_self re-entry

OverseerMsg:
  Observed { reply: ReplySender<Option<(ActorId, bool)>> }   // did dispatcher die, normally?
```

### Data flow

- **Happy path:** client `ask(Submit)` → dispatcher assigns to an idle worker
  (`Recipient::tell(Run)`) or queues → worker runs the job off-turn via
  `pipe_to_self(do_work(job), WorkDone)` → on `WorkDone` the worker tells
  `Done` → dispatcher clears outstanding, hands the worker its next job.
- **Failure path:** `Poison` (handler panic) / `Fail` (handler `Err`) → worker
  dies → supervisor machinery rebuilds it from the factory (fresh `ActorId`)
  → the factory `try_send`s `WorkerReplaced` → the dispatcher swaps the roster
  entry and re-queues that slot's outstanding job via
  `send_after(backoff, Retry)`. Retries are capped per job; past the cap the
  job is recorded failed in stats — no infinite poison loop, nothing silently
  dropped.
- **Drain:** `Drain` sets `draining` (further `Submit` rejected typed), waits
  for pending + outstanding to empty, `stop_child`s each worker, sets
  `*stop = true`, replies with a `DrainReport`. Registry entry removed in
  `on_stop`.

### Feature-coverage map (what makes each seam load-bearing)

| Spine seam | Where it is load-bearing |
|---|---|
| `spawn_supervised` / `supervise` / `RestartPolicy` / `OneForOne` | worker rebuild under induced crash |
| factory re-entry + `WeakActorRef` | `WorkerReplaced` roster swap + outstanding-job re-queue |
| `spawn_linked` / `watch` / `on_link_died` (Watch) | `Overseer` observing dispatcher death from outside |
| `ask` + `.timeout()`, `tell`, `try_send` | Submit/Stats/Drain vs Done/Run/Retry |
| `pipe_to_self` | worker async work re-entry |
| `send_after` + `TimerHandle` | retry backoff |
| `Recipient` erasure | dispatcher's worker roster |
| `Registry` | client lookup by name; removal on stop |
| bounded mailbox backpressure | producer → dispatcher under load |
| `on_panic` / error taxonomy | `Poison` vs `Fail` crash kinds |
| drain / `stop_child` / ref-count semantics | shutdown protocol |

## Error handling

- `SubmitError` is the typed domain error on `Submit`: `QueueFull` when
  pending ≥ cap — distinct from mailbox backpressure (which the client also
  observes via `ask.timeout()` under load; two different rejection layers, both
  surfaced) — and `Draining` for submit-after-drain.
- Worker `Err` and worker panic both exercised; both are crashes to the
  supervisor.
- Retry-cap exhaustion is recorded in `Stats`/`DrainReport`, never dropped.
- App error enums follow the shared rules: `thiserror`, one variant per failure
  domain, no sentinels.

## Files

- `crates/core/examples/job_queue.rs` — runnable demo: installs a `tracing`
  fmt subscriber, submits ~20 jobs including `Fail`/`Poison`, narrates
  restarts, drains, prints the report. `cargo run -p bombay --example job_queue`.
- `crates/core/tests/app_job_queue.rs` — the gate-checked integration test
  (four cross-cutting categories at APP level, below).
- `docs/warts/218-example-warts.md` — the wart log (tracked; see protocol).
- CLAUDE.md — walking-skeleton process rule added (every subsequent feature
  card carries an "extend the job-queue app + its integration test" bullet).
- README — per the per-card README rule: example added to the usage section
  pointer; no coverage numbers.

## Testing (the four categories, app-level)

1. **Sequence/protocol** — submit → done → stats → drain as one multi-step
   flow; exact counts asserted (`assert_eq!`, not `contains`).
2. **Lifecycle** — induced crash (`Fail` and `Poison`) → worker rebuilt (fresh
   `ActorId` observed) → job re-run; every job completed ≥ 1× or recorded
   failed; queue empty at drain.
3. **Defensive boundary** — submit past queue cap → typed `QueueFull`; submit
   after drain → typed rejection; tiny `ask` timeout under load → `AskError`
   classified correctly.
4. **Linearizability/isolation** — concurrent producers (`tokio::spawn` +
   `Barrier`, real overlap) → drain → `completed + failed == submitted`
   exactly (no loss, no phantom).

Discipline: every await bounded (`terminate_bound()` — mutants/MIRI lanes must
not hang); new public/`pub(crate)` fns get `mutants-baseline.json` entries;
proptests (if any) named `prop_*`; test asserts specific values and can fail.

## Wart-capture protocol

Surfacing implementation gaps is a *goal* of this card, not a side effect.

- **During implementation:** every friction point or gap is appended
  immediately to `docs/warts/218-example-warts.md`: what hurt, minimal repro
  sketch, severity (`blocker` / `boilerplate` / `paper-cut`).
- **At each plan-phase boundary:** triage entries → `gh issue create` on
  milestone M1, labels as fits (`foundation`/`runtime`/…), added to project
  board #4. One wart = one issue, one-invariant-per-bullet scope. The wart-log
  entry is then annotated with its issue number — nothing lives only in the
  file.
- **Card #218 gains the checklist bullet:** "all surfaced warts filed as M1
  issues (numbers listed)" — the card cannot close `COMPLETED` with silent
  warts (#166 discipline).
- **Known wart #1 (file up-front, before coding):** extreme boilerplate —
  hand-written `Mailboxed` + `Actor` impls, no derive beyond `Msg`. Check #217
  (bombay_macros syn-3 migration) first; if not covered there, file a separate
  M1 card "macros: actor boilerplate reducer (`#[derive(Actor)]` / `actor!`)"
  blocked-by #217.
- **Blocker-class wart** (the spine cannot express the app): stop, file the
  issue, surface to Joel before hacking around it.

## Out of scope

- Worker-pool primitive (#228), stash (#224), control-signal lane (#225),
  receive-timeout (#241), attach_stream (#230) — the app deliberately
  hand-rolls around their absence. Each absence is logged as a wart
  cross-referencing the existing card, and the "extend the app" bullet lands on
  those cards, not here.
- Satellite examples B (health-monitor) and C (pipeline) — #216.
- Any remote/Zenoh surface (#67 is the M2 analog of this gate).
