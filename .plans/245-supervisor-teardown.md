# Card #245 — supervisor teardown sweep: signal, bounded join, abort

Spec: `docs/superpowers/specs/2026-07-28-245-supervisor-teardown-design.md`
ADR: `docs/adr/0019-supervisor-subtree-teardown.md`
Read both BEFORE editing. When this plan and the spec disagree, STOP and report blocked.

## Context

- Invariant: a supervisor's exit, for ANY reason (`Normal`, `Killed`, peer-death
  propagation, own-handler panic, escalation), stops every remaining supervised
  child: cancel → join (death notice observed on the supervisor's own `link_rx`
  via the already-installed watch edges) bounded per child by its `stop_grace`
  (graces concurrent) → abort stragglers → post-abort confirmation bounded by
  `ON_STOP_NOTICE_GRACE`. Never orphan, never wedge.
- One non-skippable sweep site: the supervised lifecycle epilogue in
  `crates/core/src/actor/spawn.rs`. The escalation-path sweep inside
  `dispatch_death` (`crates/core/src/actor/kind.rs:480`) is removed.
- `Abortable` boundary (supervised lifecycle ONLY): wraps prologue
  (`start_actor`) + message loop. `children` and `pending_aborts` are created
  OUTSIDE the abortable future and borrowed into it, so they survive `kill()`.
  `state`/`watchers`/`mailbox_rx` stay inside and drop on abort (that IS the
  kill semantics: `on_stop` skipped, watchers notified by
  `MailboxReceiver::drop` as today).
- Engineering rules bite here: no bare arithmetic on counts/durations
  (checked_*), thiserror-only errors (none expected to change), bounded awaits
  in every test, `let ... else`, imports at top, comments only for the why.
- Rust only, stable toolchain, edition 2024. NEVER relax any clippy lint.

## Steps

All steps SEQUENTIAL unless marked. Steps 1–3 touch the same two files —
do NOT parallelize them.

### Step 1 — kind.rs: teardown_children (replaces stop_surviving_children)

File: `crates/core/src/actor/kind.rs`.

Replace `stop_surviving_children` (line ~499) with:

```rust
async fn teardown_children(children: &mut Children, link_rx: &LinkReceiver)
```

Behavior (the join loop — line-level design is yours, this contract is not):

1. `children.drain_live_handles()` → set of `(ChildHandle, Duration)` — if
   empty, return immediately.
2. Cancel every handle (`handle.cancel.cancel()`), all up front.
3. Join phase: await death notices on `link_rx` for the drained ids.
   Per-child deadline = its own grace, measured from the cancel; graces run
   concurrently (overall bound = max grace, not sum). A notice for an id not
   in the pending set (peer, duplicate) is discarded. When a child's deadline
   fires before its notice: `handle.abort.abort()`.
4. Post-abort confirmation: keep draining `link_rx` for the aborted ids,
   bounded overall by `ON_STOP_NOTICE_GRACE` (import from `spawn`; hoist the
   const to `pub(super)` if needed). If the bound expires with notices still
   missing, emit a `trace` event (follow the existing `trace::` module
   pattern in this file/spawn.rs) and return — a non-yielding child must not
   wedge the supervisor's exit.

Notes:
- `link_rx.recv_async()` never yields `Err` here for the same reason as the
  loop's arm (the caller still holds a sender clone) — but the epilogue no
  longer holds `sup_link_tx` by construction unless you pass it; simplest is
  to treat `Err` as "channel drained, nothing more will arrive" and break to
  the abort phase. Decide locally, document with a one-line why.
- Update the doc comment: this is the single sweep site, join semantics,
  bounds, and the kill/no-wedge rationale (cite ADR-0019).

Also in this step: `dispatch_death` (~line 470) — remove the
`stop_surviving_children(children).await;` call so the `Some(Break)` arm just
breaks with the reason. Rewrite its doc comment (lines ~443–462): both Break
sources now rely on the lifecycle epilogue sweep; the #195 peer-death path no
longer leaves children untouched (ADR-0019 contract revision). Remove the
now-stale `#[expect(clippy::too_many_arguments, ...)]` only if the signature
actually shrank; otherwise leave it.

Existing unit test `stop_surviving_children_cancels_and_aborts_live_ones`
(~line 1425): rework to call `teardown_children` with a link channel; it must
still assert cancel-then-abort for a cancel-ignoring child (straggler leg).

Verification: `cargo check -p bombay` + `cargo clippy -p bombay --lib`.

### Step 2 — spawn.rs: Abortable boundary + epilogue sweep

File: `crates/core/src/actor/spawn.rs`.

`run_lifecycle_supervised` (~line 593) restructures to:

1. Capture `sup_id` / `sup_link_tx` as today (before anything else).
2. Create `children`, `pending_aborts` OUTSIDE the abortable region.
   `retries`, `cycle`, `rng` stay inside it.
3. Build the abortable region: an async block running `start_actor` + the
   existing `run_supervised_message_loop` call, borrowing
   `&mut children` / `&mut pending_aborts` into `SupervisedState` as today.
   Its output: enough to run the epilogue on the graceful path — i.e.
   `(state, watchers, mailbox_rx, reason)` — or the startup-failure result.
   Wrap it in `Abortable::new(..., abort_registration)`.
4. Epilogue (always runs):
   - Drain `pending_aborts`: abort every entry (their cancel already fired;
     grace truncated at supervisor exit — one-line why comment).
   - `teardown_children(&mut children, &link_rx).await`
   - Match the abortable result:
     - `Ok(graceful output)` → `finish_actor(state, weak, mailbox_rx,
       watchers, reason).await` exactly as today (startup-failure path also
       preserved exactly as today — it must run the sweep too, since
       `on_start` may have spawned children before failing).
     - `Err(Aborted)` → `RunResult::Killed`.

`run_supervised` (~line 268): the `Abortable::new(lifecycle, ...)` +
`.unwrap_or(RunResult::Killed)` wrapper is removed — pass
`abort_registration` into `run_lifecycle_supervised` instead and await it
directly. `PreparedActor` destructuring adjusts accordingly. Update the doc
comments on `run_supervised`/`spawn_supervised_task` (~lines 261–298): kill
aborts message service; the epilogue still sweeps children; `on_stop` still
skipped on kill. Do NOT touch `run` (~164) or `run_linked` (~227).

Order matters in the epilogue: `pending_aborts` drain first, then
`teardown_children`, then `finish_actor` — children are dead (or provably
wedged) before the supervisor's own death is announced on graceful paths.

Verification: `cargo check -p bombay` + `cargo clippy -p bombay --lib`.

### Step 3 — lifecycle tests (spawn.rs tests module)

Write these FAILING FIRST if you can stage it; at minimum, verify each one
fails when the sweep call is commented out (note in your final report whether
you did).

All test awaits BOUNDED (existing `bounded` helper). Use the existing
supervised-test scaffolding (`spawn_supervised`, seeded rng seams). One
invariant per test:

1. `supervisor_normal_stop_tears_down_children`: supervisor with 2 live
   children; supervisor stops via ref-count/stop with reason `Normal`; assert
   both children provably dead (their `on_stop` observed / watch notice
   received / `try_tell` fails closed) before the supervisor's `RunResult`
   resolves.
2. `supervisor_kill_tears_down_children`: same but `kill()`; children dead;
   result `RunResult::Killed`; supervisor `on_stop` NOT run.
3. `teardown_joins_early_not_grace_sleep`: paused clock
   (`start_paused = true`); child stops promptly on cancel with a LARGE grace
   (e.g. 60s); assert the sweep completes without advancing anywhere near the
   grace (join semantics, not the old blind sleep). CAUTION (memory: paused
   clock): never spin on `yield_now` with a wrapping timeout — use the time
   driver (`tokio::time` advances) and bounded awaits.
4. `teardown_aborts_cancel_ignoring_child`: child ignores cancellation (loops
   over a pending future not wired to its cancel token); assert it is aborted
   at its grace and the supervisor still exits within bound.
5. `kill_during_on_stop` existing test (~line 2728,
   `kill_during_on_stop_marks_cleanup_failed`): only touch it if it exercises
   a SUPERVISED actor; it uses plain `spawn`, so expected NO change. Confirm
   and state so in your report.
6. Escalation-path regression: an existing test covers children being stopped
   on escalation — confirm it still passes with the sweep moved to the
   epilogue (it should, the sweep now runs later but before the task ends).

Mutants baseline: `teardown_children` is a new fn and the old
`stop_surviving_children` entry must be renamed/replaced in the mutants
baseline (`mutants-baseline.json` — BOTH path-keyed sections if present:
floors map AND known_zero_viable list; memory says missing entries fail the
gate as Unaccounted). Mirror the shape of neighboring entries.

Verification: `cargo check -p bombay --tests` + `cargo clippy -p bombay --tests`.
Do NOT run the tests — no `cargo test`/`cargo nextest` (sandboxed; they hang).
Tests run in the unsandboxed `nix flake check` gate driven afterwards.

### Step 4 — job-queue example + integration test (walking skeleton)

Files: `crates/core/examples/job_queue/app.rs`,
`crates/core/tests/app_job_queue.rs`. PARALLEL-SAFE with nothing (depends on
Steps 1–2 semantics); run after Step 2 compiles.

1. `finish_drain_if_quiet` (~line 465): remove the manual roster
   `stop_child` loop and the `FinishStop` self-signal. When quiet: send the
   `DrainReport` reply and set stop directly (the `DispatcherMsg::FinishStop`
   arm's body moves here; delete the variant and the `stopping` flag if
   nothing else uses them). Delete the workaround comment (lines ~458–464)
   — replace with one line citing ADR-0019 (supervisor exit sweeps children).
2. Worker teardown observability seam: give workers a way to report their
   stop that the integration test can assert — e.g. an
   `flume::Sender<ActorId>` (or equivalent) carried in the worker config,
   fired in the worker's `on_stop`. Keep it example-level and minimal.
3. `app_job_queue.rs`: extend the drain test — after drain completes and the
   dispatcher has terminated (existing join/termination handle), assert EVERY
   worker's stop signal was received (bounded recv per worker). This is the
   invariant the workaround could never prove: workers actually stopped after
   drain, torn down by the supervisor, not by the app.
4. `docs/warts/218-example-warts.md` row 4: annotate resolved-by #245 (keep
   the row, add "resolved by #245 — supervisor exit sweeps children,
   ADR-0019" to the wart text or issue column per the table's style).

Verification: `cargo check -p bombay --examples --tests` + clippy same scope.

### Step 5 — README + coverage baseline

- `README.md`: public behavior changed — one salient line in the relevant
  supervision bullet: supervisor stop/kill now tears down its children
  (bounded join). No card numbers in README.
- `docs/testing/coverage-baseline.md`: tests moved/added — update per its
  existing format.

Verification: none beyond fmt (`cargo fmt` before finishing — the fmt gate is
strict).

## Verification (final, before reporting done)

- `cargo fmt`
- `cargo check -p bombay --all-targets`
- `cargo clippy -p bombay --all-targets`
- Do NOT run tests or `nix flake check` — the controller runs the flake gate
  (unsandboxed) after review.

## Out of scope — do not touch

- Plain/linked lifecycles (`run`, `run_linked`, `run_lifecycle`,
  `run_lifecycle_linked`), `finish_actor`.
- `clippy.toml`, any `[lints]` table, lint levels anywhere.
- `fuzz/` workspace, benches, `.github/`.
- kameo sibling checkout (`~/Code/devrandom/kameo`) — read-only reference,
  never a build input.
- No commits — the controller commits.
