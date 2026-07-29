# #248 — queued supervise registration must not orphan its child

**Status:** approved (Joel, 2026-07-29) · **Card:** [#248] · **Amends:** ADR-0019

## Problem

`supervise()` spawns the child inline in the caller's task and enqueues
`Signal::Supervision(Box<SupervisionOp::Add(SuperviseReg)>)` into the
supervisor's bounded mailbox. If the supervisor exits before the loop dequeues
that op, the child is never inserted into the task-owned table:

- the ADR-0019 epilogue sweep (`teardown_children`, `actor/kind.rs:496`)
  covers the TABLE only — the queued child is invisible to it;
- the dropped mailbox drops the boxed op holding the only `ChildHandle`;
  `ChildHandle` has no `Drop`, so nothing stops the child — a live orphan,
  contradicting ADR-0019 ("supervisor exit ⇒ children provably stopped").

Three exit paths, different constraints:

| Path | Mailbox in epilogue? | Covered by |
|---|---|---|
| Graceful stop (`stop()`, ref-count) | yes — returned in `SupervisedLifecycleResult::Graceful` | **drain** (mechanism 1) |
| Kill (`kill()` → `Err(Aborted)`) | no — dropped inside the aborted future | **drop-guard** (mechanism 2) |
| Startup failure | yes, but `startup_failed` needs the queued Watch regs (`reject_queued_watchers`) — destructive pre-drain would eat them | **drop-guard** (mechanism 2) |

## Decision: both mechanisms, split by path

Guard-only was rejected: on the graceful path it fires during `finish_actor`'s
backlog sweep — an abort is *issued* but never joined, so the supervisor's own
death notice can precede the child's death, breaking ADR-0019's graceful
ordering invariant, and the child gets a brutal abort instead of the
cancel → grace → join a table child gets. Drain-only was rejected: the kill
path drops the mailbox inside the `Abortable` region, out of the epilogue's
reach. Each mechanism covers exactly what the other cannot.

### Mechanism 1 — graceful-path drain into the table (primary)

In `run_lifecycle_supervised` (`actor/spawn.rs:630`), restructure the
epilogue: match `lifecycle_result` FIRST to recover `mailbox_rx` (and
`watchers`) on the `Graceful` arm, then drain the backlog **before**
`pending_aborts.clear()` + `teardown_children`, routing every signal with the
loop's own FIFO semantics:

- `Supervision(Add(armed))` → disarm, `children.insert(id, child)`, then
  `install_child_watch` (insert-before-watch, the #196 ordering) so the
  child's death notice reaches the supervisor's `link_rx` and
  `teardown_children` can *join* it instead of burning the full grace.
  `WatchOutcome::Full`/`Closed` reuse the existing abort/synthesize handling.
- `Supervision(Remove(id))` → `children.remove(id)` — the caller took
  ownership; the child must NOT be swept. (Discarding the op would tear down
  a child the caller was promised.)
- `Supervision(Stop(id))` → remove + cancel + `PendingAbort` into
  `pending_aborts`; the immediately following `clear()` performs the abort —
  grace truncated at supervisor exit, exactly the documented `PendingAbort`
  contract.
- `Watch(reg)`/`Unwatch(id)` → applied to `watchers`, same as
  `apply_raced_registrations` — earlier, but `finish_actor`'s later sweeps
  still close the on_stop windows for later arrivals.
- `Message`/`Stop` → discarded (unchanged).

`install_child_watch` needs a `SupervisorRef`; clone `sup_link_tx` once more
before the `Abortable` region for epilogue use (the loop's copy moves into
`SupervisedState`).

Queued children then flow through `teardown_children` identically to table
children: cancel, per-child grace, join, before the supervisor's death is
announced. The ordering invariant holds with no special case.

### Mechanism 2 — drop-guard on the queued registration (backstop)

`SupervisionOp::Add` carries an **armed** registration: a newtype wrapper
(`supervision.rs`, beside `PendingAbort`) holding `Option<SuperviseReg>` with

- `disarm(self) -> SuperviseReg` (take; used by `apply_supervision_op` and the
  mechanism-1 drain), and
- `Drop`: if still armed, `cancel.cancel()` then `abort.abort()` on the first
  incarnation's handle. Abort is what guarantees termination (a hung handler
  ignores cancel; `Drop` cannot await a grace). The child's own epilogue lives
  outside its `Abortable` region (ADR-0019), so its death is still announced
  and its `MailboxReceiver` drop answers its watchers.

This fires wherever the op is dropped unconsumed: the kill path (mailbox
dropped with the aborted future), the startup-failure path, the
`apply_raced_registrations` discard arm (`spawn.rs:411`, plain/linked
lifecycles), and any future leak. Precedent: `PendingAbort`
(`supervision.rs:71`) — same Drop-aborts shape, same idempotent-abort
argument.

Kill-path ordering: `Abortable` drops the inner future (and mailbox) when it
yields `Err(Aborted)`, so guards abort queued children *before* the epilogue
sweeps the table — "a brutal kill is brutal" (ADR-0019); nothing is orphaned.

## Non-goals

- No change to `supervise()`'s inline-spawn-then-enqueue shape (it is what
  lets `supervise` return the child's id from a tell).
- No generation counters, no ack protocol, no supervisor-side spawn — rejected
  in ADR-0019's alternatives; this card closes its residual window only.

## Tests (TDD — failing first)

1. Probe (exists as the card's hypothesis): graceful `stop()` immediately
   after `supervise` — no `quiesce()` — child provably dead (mailbox closed)
   before the supervisor's `RunResult` resolves. Deterministic under paused
   current-thread: no await point between `supervise` returning and `stop()`,
   so the op is still queued.
2. Kill variant: `kill()` with the registration still queued — child dead,
   `RunResult::Killed`.
3. FIFO drain semantics: queued `Add(A)` then `Remove(A)` at stop — A is NOT
   torn down (ownership transferred); queued `Add(A)` then `Stop(A)` — A is
   torn down.
4. Guard unit test (`supervision.rs`): dropping an armed registration cancels
   and aborts the first incarnation; a disarmed one does not.
5. The two `quiesce()` barriers in the #245 lifecycle tests
   (`spawn.rs:4356`, `spawn.rs:4401`) are REMOVED — the invariant must hold
   whether the ops were applied or still queued.
6. Job-queue app (walking-skeleton rule): extend shutdown to stop the
   supervisor with a just-supervised worker if the app can express the race
   deterministically; otherwise defer on the card with reasoning.

## ADR

Amend `docs/adr/0019-supervisor-subtree-teardown.md`: replace the "Known
residual window" section with the resolution (drain + guard, path table
above), and record the Remove/Stop drain semantics.
