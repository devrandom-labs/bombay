# #248 — queued supervise registration must not orphan its child

## Context

Spec: `docs/superpowers/specs/2026-07-29-248-queued-registration-teardown-design.md`
(read it first — it holds the path table and the rejected alternatives).
Amends ADR-0019. Approved mechanism: **both** —

1. **Graceful-path drain**: `run_lifecycle_supervised`'s epilogue recovers the
   mailbox on the `Graceful` arm and drains queued `Supervision` ops into the
   child table BEFORE `teardown_children`, so queued children get the full
   cancel → grace → join sweep and ADR-0019's ordering invariant (children
   provably dead before the supervisor's own death notice) holds.
2. **Drop-guard backstop**: `SupervisionOp::Add` carries an armed registration;
   dropping it unconsumed cancels + aborts the first incarnation. Covers the
   kill path (mailbox dropped inside the `Abortable`), the startup-failure
   path, and the `apply_raced_registrations` discard arm.

Invariants that must hold:

- ADR-0019 graceful ordering: every child (table or queued) is dead or
  provably aborted before the supervisor's `RunResult` resolves.
- Kill path: `kill()` with queued registrations orphans nothing;
  `RunResult::Killed`; supervisor `on_stop` skipped (unchanged).
- **Failed-send contract preserved** (`actor_ref.rs:355-401`, test
  `supervise_on_a_dead_supervisor_reports_actor_not_alive` at
  `spawn.rs:3959`): when `supervise`'s mailbox send fails, the anchored child
  KEEPS RUNNING unsupervised. The guard must be explicitly disarmed on the
  `SendError` handback (flume hands the signal back) — never abort there.
- #196 insert-before-watch ordering preserved in the drain (insert, then
  `install_child_watch`).
- FIFO drain semantics: `Remove` detaches (child NOT swept), `Stop` stops via
  `pending_aborts` (the following `clear()` truncates grace), `Watch`/
  `Unwatch` reach `watchers` as in `apply_raced_registrations`.
- All new/changed non-test functions get `mutants-baseline.json` entries
  (Unaccounted otherwise); non-Default returns → `known_zero_viable`.
- TDD: write each failing test, watch it fail (`cargo check` compiles it;
  actual red/green runs happen in Claude-driven `nix flake check`), then
  implement.

All steps are SEQUENTIAL — they share `supervision.rs`, `kind.rs`,
`spawn.rs`. No parallel fan-out.

## Steps

1. **Guard type** — `crates/core/src/actor/supervision.rs` (beside
   `PendingAbort`, line ~71). New crate-private newtype `ArmedReg` holding
   `Option<SuperviseReg>`:
   - `pub(crate) fn new(reg: SuperviseReg) -> Self`
   - `pub(crate) fn disarm(mut self) -> SuperviseReg` — takes the reg out;
     the impossible already-disarmed case panics via `expect` with an
     `#[expect(clippy::expect_used, reason = ...)]` (programmer bug: disarm
     is called at most once per armed value — mirror the crate's existing
     expect-with-reason style, e.g. `kind.rs` cycle_count_down).
   - `Drop`: if still `Some` and `child.handle` is `Some`, `cancel.cancel()`
     then `abort.abort()` (cancel first so a cooperative child that wins the
     race still sees a graceful edge; abort guarantees termination — Drop
     cannot await a grace). Comment cites #248 + ADR-0019 amendment.
   - Change `SupervisionOp::Add(SuperviseReg)` → `Add(ArmedReg)`. Update the
     two `kind.rs` test constructions of `SupervisionOp::Add(SuperviseReg
     { ... })` (~lines 1380 and 1430) to wrap with `ArmedReg::new(...)`, and
     add `ArmedReg` to the import lists in `actor_ref.rs` (~line 22) and
     `kind.rs` (~line 22).
   - Unit tests in `supervision.rs`'s test mod (reuse the existing mock
     `handle(id)`/`child_entry(id)` helpers, ~line 417): (a) dropping an
     armed reg cancels + aborts the handle (assert via the
     `CancellationToken::is_cancelled` and an `AbortRegistration`-backed
     probe as the existing `ChildHandle` tests do, see
     `child_handle_clone_shares_the_stop_edges` ~line 630); (b) a disarmed
     reg's drop touches neither edge.
   Expected outcome: compiles; both tests express the guard contract.

2. **Arm at the send site, disarm on handback** —
   `crates/core/src/actor/actor_ref.rs:361-402` (`supervise`). Wrap the reg:
   `SupervisionOp::Add(ArmedReg::new(reg))`. On `Err(send_err)` from the
   mailbox send, recover the signal from the error, `disarm()` the reg, and
   drop it plainly — preserving the documented continue-unsupervised
   semantics (update the doc comment at line ~355 to say the handback is
   deliberate). If the flume error type in our mailbox wrapper does not hand
   the signal back, STOP and report blocked — do not weaken the contract.

3. **Disarm at the apply site** — `crates/core/src/actor/kind.rs:907-918`
   (`apply_supervision_op`): `Add(armed)` → `let reg = armed.disarm();` then
   the existing insert + `install_child_watch` body, unchanged.

4. **Epilogue drain (mechanism 1)** — `crates/core/src/actor/spawn.rs:630-740`
   (`run_lifecycle_supervised`):
   - Before the `Abortable` region, clone one extra `sup_link_tx` for the
     epilogue (the existing clone moves into `SupervisedState`).
   - Restructure the epilogue: match `lifecycle_result` FIRST. On
     `Ok(Graceful(state, weak, mut mailbox_rx, mut watchers, reason))`:
     call a new `pub(super)` helper in `kind.rs` (name it
     `drain_queued_supervision` — simple name, Joel's preference), signature
     roughly `(mailbox_rx: &mut MailboxReceiver<A>, children: &mut Children,
     watchers: &mut Watchers, pending_aborts: &mut DelayQueue<PendingAbort>,
     sup_id: ActorId, sup_link_tx: LinkSender)` — `SupervisorRef`'s fields
     are private to `kind.rs` and it has no constructor, so the helper takes
     the raw pieces and builds the `SupervisorRef` itself inside `kind.rs`
     (do NOT widen `SupervisorRef`'s API). Drain `mailbox_rx.drain()`:
     - `Signal::Supervision(op)` → match `*op`: `Add(armed)` → disarm,
       `children.insert`, `install_child_watch` (reuse it — Full/Closed
       outcomes already abort + synthesize the death onto the link channel,
       which the sweep will join); `Remove(id)` → `children.remove(id)`
       (no cycle bookkeeping — the loop is gone, the cycle died with it);
       `Stop(id)` → remove, cancel, `pending_aborts.insert(PendingAbort::new(h),
       child.config.stop_grace)`.
     - `Signal::Watch(reg)` → `watchers.apply(*reg)`; `Signal::Unwatch(id)` →
       `watchers.remove(id)`.
     - `Signal::Message{..}` / `Signal::Stop` → discard.
   - THEN `pending_aborts.clear()` (aborts any drained `Stop` children,
     grace truncated — the documented `PendingAbort` contract), then
     `teardown_children(&mut children, &link_rx).await`, then `finish_actor`
     with the recovered pieces.
   - `StartupFailed` and `Err(Aborted)` arms: `pending_aborts.clear()` +
     `teardown_children` as today (no drain — spec explains why); the guard
     covers their queued ops.
   - Update the epilogue's doc comment (spawn.rs:613-629) — it currently
     says the sweep covers the table; it now covers table ∪ queued on the
     graceful path, guard elsewhere. Also fix the stale comment at
     `spawn.rs:406-411` (`apply_raced_registrations`): the discard is now
     safe because the armed op's drop stops the child.
   - NOTE: `apply_raced_registrations` runs inside `finish_actor` AFTER the
     drain already emptied the backlog; its `Supervision` arm stays for
     later-arriving ops (a `supervise` racing `on_stop`) — those are
     guard-covered; say so in its comment.

5. **Lifecycle tests** — `crates/core/src/actor/spawn.rs` (the #245 test
   mod, ~4330-4420):
   - Remove BOTH `quiesce().await` calls + their `TODO(#248)` comments
     (lines ~4354-4356 and ~4399-4401). The tests now assert the invariant
     whether ops were applied or queued.
   - New test `supervisor_stop_with_queued_registration_tears_down_child`:
     same shape as `supervisor_normal_stop_tears_down_children` but call
     `sup.stop()` on the very next line after `supervise_worker` returns —
     no `recv_id`, no await between (paused current-thread ⇒ the op is
     deterministically still queued). Assert child mailbox closes
     (`await_closed`) before `join` resolves `Stopped{Normal}`.
   - New test `supervisor_kill_with_queued_registration_orphans_nothing`:
     same, with `sup.kill()`; assert child closed + `RunResult::Killed`.
   - New test `queued_remove_detaches_instead_of_sweeping`: supervise A
     (quiesce so Add applies), then `unsupervise(A)` + `stop()` back-to-back
     with no await between them (Remove still queued) — assert A's sender
     stays OPEN after the supervisor joins (child kept running; then stop it
     manually to end the test). This quiesce is LEGITIMATE (the subject
     needs A in the table first) — comment it citing #248's resolution.
   - New test `queued_stop_still_stops_child`: supervise A, quiesce,
     `stop_child(A)` + `stop()` back-to-back — assert A closed before join.
   - Extend `supervise_on_a_dead_supervisor_reports_actor_not_alive`
     (~spawn.rs:3959): after the `ActorNotAlive` error, additionally assert
     the anchored child's mailbox sender is still OPEN (the disarm-on-handback
     path must not abort it) — the failed-send contract currently has no
     regression test; then stop the child to end the test.

6. **ADR amendment** — `docs/adr/0019-supervisor-subtree-teardown.md`:
   replace the "Known residual window" section (lines 99-106) with a
   "Resolution (#248)" section: the path table from the spec, the two
   mechanisms, the Remove/Stop drain semantics, and the failed-send
   disarm rule.

7. **Mutants baseline** — `mutants-baseline.json` (repo root; follow the
   existing entry shape): add entries for `ArmedReg::new`, `disarm`, the
   `Drop` impl, and `drain_queued_supervision`. `disarm` returns
   `SuperviseReg` (non-Default) → `known_zero_viable`; `ArmedReg::new`
   likely also `known_zero_viable` (returns the non-Default wrapper). Check
   how the two path-keyed sections are structured before editing (floors map
   + known_zero_viable LIST — update BOTH or the gate fails closed).
   `run_lifecycle_supervised` and `supervise` are already in
   `known_zero_viable` (entries ~145/199) — no floor change for their
   changed bodies; leave `apply_supervision_op`'s floor (2) unless the gate
   complains.

8. **Job-queue app** (walking-skeleton rule, CLAUDE.md item 7) — DEFERRED.
   Pre-flight CHECK confirmed the app supervises workers inside
   `Dispatcher::on_start` and awaits a `Stats` ask before returning
   (`examples/job_queue/app.rs:300-320`, `app.rs:474-485`) — no natural
   supervise+stop back-to-back site; forcing one would be a contrived race.
   Do NOT touch the app. Claude records the deferral on the card at close.

## Verification (per step and final)

- `cargo check -p bombay` and `cargo clippy -p bombay --lib` after each step
  — NEVER run `cargo test`/`cargo nextest` (sandboxed shell hangs test
  binaries; verified). `cargo fmt` before finishing.
- Tests run in Claude-driven `nix flake check` after your handoff — write
  them to compile; red/green confirmation is Claude's job.

## Out of scope

- `supervise()`'s inline-spawn shape, ack protocols, generation counters
  (rejected in ADR-0019).
- The #195/#196 watch-registration races (already fixed; do not touch
  `watch_installer` or `WatchOutcome`).
- Any file outside: `crates/core/src/actor/{supervision.rs,kind.rs,
  spawn.rs,actor_ref.rs}`, the ADR, `mutants-baseline.json`, the job-queue
  app + its integration test.
- No lint-level changes, no `clippy.toml` edits, no README change (no
  public-API change; Claude handles coverage-baseline doc).
