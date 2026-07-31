# Card #267 — drain-window mint equivalence for supervise/stop_child/unsupervise

Test-only card. Follow-up to #266 (PR #269): the equivalence oracle covered the
watch/link verb family; the supervision verbs issued from a drain-window minted
handler ref are the deliberately-deferred untested surface.

## Context — verified mechanics (all anchors checked 2026-07-31)

**The invariant (ADR-0010):** a handler-context `supervise`/`supervise_cloned`/
`stop_child`/`unsupervise` behaves identically whether the handler's `ActorRef`
is the steady-state shared upgrade or a drain-window mint
(`crates/core/src/actor/kind.rs:262-270` — the mint carries the loop's own cold
`link_tx` and the dequeued `self_sender`).

**Why this differs from #266's watch/link:** watch/link ops target the OTHER
actor's control lane (its loop is running and applies them mid-script).
Supervision ops target the issuing supervisor's OWN control lane — and the
supervisor's loop is inside the handler while the script runs. Ops queued by a
handler are applied only AFTER that handler returns. Every choreography below
is built around this.

Verified facts the tests lean on (do not re-derive; they were read this session):

1. **Control-first merge beats lane closure** (`crates/core/src/mailbox.rs:459-499`):
   `recv` try-polls the control lane before latching user-lane disconnect. So in
   the drain window, an op the handler queued is ALWAYS served and applied by
   the live loop arm (`kind.rs:475-480` → `apply_supervision_op`,
   `kind.rs:961-1004`) before the `Closed → Collected` break. Consequence: a
   supervision op from the mint CANNOT race the `Collected` break — the
   reachable race is via the stop flag (fact 2). Document this as a positive
   finding in the test-file doc comment.
2. **Stop-flag bypasses the last poll** (`kind.rs:1078`): `stop = true` makes
   `handle_mailbox_step` return `Break(Normal)` directly — no further `recv`.
   A queued `Add` then lands in the graceful epilogue's
   `drain_queued_supervision` (`kind.rs:1026-1063`), which applies it
   (insert + watch edge, #248 mechanism 1), after which `teardown_children`
   (`kind.rs:538`) sweeps the child. This is the mint-path re-assertion of the
   #248 never-orphaned invariant.
3. **Guard semantics** (`crates/core/src/actor/supervision.rs`): `ArmedReg::Drop`
   (l.122) cancels **and immediately aborts** an unapplied first incarnation;
   `PendingAbort::Drop` (l.79) aborts (idempotent — no-op on an
   already-stopped child). Asserting a child's exact `RunResult::Stopped
   { reason: Normal }` therefore distinguishes installed-then-swept (graceful
   cancel) from guard-aborted.
4. **Biased select order in the supervised loop** (`kind.rs:419-489`): death arm,
   then `retries` (disabled while empty), then `pending_aborts`, then mailbox.
   An already-expired retry deadline is served BEFORE a ready mailbox poll —
   Test A's rebuild-in-drain-mode determinism depends on this.
5. **Verb plumbing** (`crates/core/src/actor/actor_ref.rs`): `supervise` (l.377)
   spawns the first incarnation inline in the caller's task and queues
   `SupervisionOp::Add(ArmedReg)` on the control lane; `unsupervise` (l.458) →
   `Remove` (detach, child keeps running); `stop_child` (l.488) → `Stop`
   (cancel now, deferred abort after `stop_grace` via `pending_aborts`);
   `supervise_cloned` (l.515) delegates to `supervise`.
6. **Teardown sweep** (`kind.rs:538-609`): cancels every live table child, joins
   their death notices on the supervisor's `link_rx`, per-child bound =
   `stop_grace`. A missing watch edge means the notice never arrives and the
   sweep stalls for the full grace — so give children `stop_grace` = 60 s
   (≫ the suite `TERMINATE` bound): if install ever skips the watch edge, the
   test FAILS by bounded-await instead of passing slowly (falsifiability, the
   #266 `on_stop_grace` pattern at `drain_equivalence.rs:1216-1219`).
7. **`SendError`-handback disarm (#248) is UNREACHABLE from a mint**: the mint's
   own sender keeps both lanes open while the handler holds it, so
   `send_control` from a live handler to its own lane cannot observe
   `ControlClosed`. Card bullet 2's "(handback disarm unaffected by ref shape)"
   closes by documenting this unreachability in the test-file doc comment, not
   by a test that cannot be constructed.

**Oracle discipline (copy from `crates/core/tests/drain_equivalence.rs`, #266):**
ONE mode-blind `run_script(mode, script)`; `Mode` may influence ONLY
(a) whether an external strong supervisor ref is held across the run and
(b) enqueue-before-run vs enqueue-after-run. Full-trace `assert_eq!` against a
`vec![..]` literal, then steady vs drain. Ids map to roles, never compared raw.
Test-local `ReasonKind` mirror with exhaustive match (no `_` arm). Every await
wrapped in `bounded()` under `TERMINATE = terminate_bound()` (#148). Oneshot
gates only — no sleeps. Tests are NOT proptests: no `prop_` prefix.

## Steps — all SEQUENTIAL (every step edits the same new test file)

### Step 1 — new test file + fixtures
File: `crates/core/tests/drain_supervision_equivalence.rs` (new; sibling of
`drain_equivalence.rs`, same discipline; a new file rather than extending
because the fixtures are Supervisor-shaped and the #266 file is already 1380
lines). Doc comment: card #267, the invariant, and findings 1 and 7 above.

Fixtures:
- `ReasonKind` + `bounded()` + `TERMINATE` + `cap()`/`config()` helpers —
  copied verbatim in shape from `drain_equivalence.rs` (test binaries cannot
  share modules; duplication is the accepted cost, as between the existing
  integration-test files).
- `Child` actor: trivial handler with one ask variant
  `Ping { reply: ReplySender<u32, Infallible> }` answering a constant (the
  liveness probe). No `Watch` impl needed (being watched is universal).
- `Incarnations` slot: `Arc<Mutex<Vec<(ActorRef<Child>, JoinHandle<RunResult<Child>>)>>>`.
  The child factory (`FnMut() -> ActorRef<Child>`) builds each incarnation via
  `PreparedActor::<Child>::new(config(4))`, pushes `(ref clone, join handle)`
  into the slot, and returns the ref. The stored ref is the child's liveness
  anchor (ADR-0003: the supervisor never pins a child); the join handle gives
  the test exact per-incarnation `RunResult` assertions. The factory captures
  ONLY the slot — never any supervisor ref (kameo #171).
- `SupScript` actor implementing `Supervisor` (default strategy) + `Watch`
  (default hooks) + `on_stop` pushing `Finished(ReasonKind)` into the trace.
  Msg enum: one variant per script step (see tests) — each handler invocation
  performs its verbs against its handler-context `ActorRef` and records
  outcomes into the shared trace `Arc<Mutex<Vec<TraceEvent>>>`.
- `TraceEvent`: `SuperviseOk(bool)`, `StopChildOk(bool)`, `UnsuperviseOk(bool)`,
  `Finished(ReasonKind)`. (Child fates are asserted via the slot's
  `RunResult`s + started-count, not via the trace — `on_stop` is skipped on
  kill so an event log under-reports.)
- `Mode { Steady, Drain }` + `start_supervisor(mode, ...)` mirroring #266's
  `start_watcher`: `PreparedActor::<SupScript>::new_linked(config(8))` +
  `spawn_supervised_task` (`spawn.rs:322`); Drain enqueues ALL script messages
  before spawning the task and holds no external ref; Steady holds the ref and
  enqueues after.
- Child `RestartConfig`: policy `Permanent`, `min_backoff` **zero** (Test A's
  determinism needs the retry deadline already expired at handler return —
  fact 4), `stop_grace` 60 s (fact 6). Use the actual `RestartConfig`
  construction API (`with_*` setters exist; check the code, not this plan).

Verification: `cargo check -p bombay --tests` compiles (test bodies may be
`todo!()`-free stubs only if a later step fills them — prefer landing each test
complete per step).

### Step 2 — Test A: supervise install + working restart edge, paired-run
Script messages: `[Supervise, Park]`.
- `Supervise` handler: `actor_ref.supervise(cfg, factory)` → record
  `SuperviseOk(result.is_ok())`. Return (op is queued; the loop applies `Add`
  when the handler returns, BEFORE delivering `Park` — fact 1's control-first
  merge).
- `Park` handler: signal `entered`, park on `release` (the #266 gate pattern).
- Test choreography (both modes, mode-blind): await `entered` → incarnation 1
  exists in the slot → `kill()` it via its stored ref → `bounded(join₁)` →
  assert `RunResult::Killed` (the death notice is queued on the supervisor's
  `link_rx` before the join resolves — the #266 discipline) → send `release`.
- After release, in BOTH modes the loop: biased death arm fires (notice ready)
  → `dispatch_death` → table hit → Permanent + Killed → retry scheduled at
  now+0 → next select: retries arm READY before the mailbox arm (fact 4) →
  rebuild → incarnation 2. Drain then breaks `Collected` on the next mailbox
  poll and the epilogue sweep cancels incarnation 2; Steady parks in `recv`
  until the test drops the external ref, then collects and sweeps identically.
- Test: await slot growth to 2 via `bounded` on a `started` notification
  (give `Child::on_start` a `flume`/`tokio` unbounded sender captured by the
  factory args to signal each start — deterministic, no polling), then (Steady
  only per the mode knob: drop the external ref; this is knob (a), allowed),
  `bounded(sup_join)` → assert `Stopped { reason: Collected }` →
  `bounded(join₂)` → assert `Stopped { reason: Normal }` (swept by teardown —
  proves table entry + watch edge live from the DRAIN-minted supervise).
- Assert trace == `vec![SuperviseOk(true), Finished(Collected)]` for both runs,
  then `steady == drain`. Assert started-count == 2 in both.

### Step 3 — Test B: stop_child equivalence, paired-run
Script messages: `[Supervise, StopChild, Park]`.
- `StopChild` handler: `actor_ref.stop_child(id)` (id captured from the
  `Supervise` step via actor state) → record `StopChildOk(result.is_ok())`.
- Interleaving guaranteed by fact 1: `Add` applies between handlers 1 and 2,
  `Stop` applies between handlers 2 and 3 (cancel now + `PendingAbort` armed
  with the 60 s grace). By the time `Park` parks, the child is cancelled.
- Test: await `entered` → `bounded(join₁)` → assert
  `Stopped { reason: Normal }` (graceful cancel — the child finished stopping
  BEFORE the supervisor may exit, so the epilogue's `PendingAbort` drop-abort
  (fact 3) is a proven no-op, not a race) → `release` → (Steady: drop ref) →
  `bounded(sup_join)` → `Stopped { reason: Collected }`.
- Assert trace == `vec![SuperviseOk(true), StopChildOk(true),
  Finished(Collected)]`, steady == drain, started-count == 1 (a stopped child
  is never rebuilt — the edge was dropped before the death could route).

### Step 4 — Test C: unsupervise equivalence (detach, child survives), paired-run
Script messages: `[Supervise, Unsupervise, Park]`.
- `Unsupervise` handler: `actor_ref.unsupervise(id)` → record
  `UnsuperviseOk(result.is_ok())`.
- Test: await `entered` → assert child alive via `bounded(child.ask(Ping))`
  round-trip == the constant → `release` → (Steady: drop ref) →
  `bounded(sup_join)` → `Stopped { reason: Collected }` → **the detached child
  survives the supervisor's death**: `ask(Ping)` round-trips again AFTER the
  supervisor join → then cleanup: `kill()` the child, `bounded(join₁)` →
  `Killed`, and assert started-count == 1 (never rebuilt: the supervisor is
  gone and the edge was dropped).
- Assert trace == `vec![SuperviseOk(true), UnsuperviseOk(true),
  Finished(Collected)]`, steady == drain.

### Step 5 — Test D: supervise races the supervisor's own stop (stop-flag → epilogue drain), paired-run
Script messages: `[SuperviseAndStop]` (single message).
- Handler: `actor_ref.supervise(cfg, factory)` → record `SuperviseOk(..)` →
  set `*stop = true` → return. The loop breaks `Normal` WITHOUT another poll
  (fact 2); the queued `Add` is applied by `drain_queued_supervision` in the
  graceful epilogue, then `teardown_children` sweeps the just-installed child.
- Test (both modes): `bounded(join₁)` → assert `Stopped { reason: Normal }`
  (installed-then-swept; a dropped-armed registration would abort — fact 3 —
  and a skipped watch-edge install would stall the sweep past `TERMINATE` —
  fact 6: the child is never orphaned and never guard-aborted) →
  (Steady: drop ref) → `bounded(sup_join)` → assert
  `Stopped { reason: Normal }` (the flag's reason, not `Collected`).
- Assert trace == `vec![SuperviseOk(true), Finished(Normal)]`, steady == drain,
  started-count == 1.
- Doc comment on this test records finding 1: the `Collected`-break race is
  unreachable from the mint path (control-first merge), so the stop-flag path
  is THE reachable mint-path race and this pin is exhaustive for it.

### Step 6 — coverage baseline
Append the new test file's coverage to `docs/testing/coverage-baseline.md`
following its existing per-file format (test-only card: README untouched,
`mutants-baseline.json` untouched — zero production-code changes).

## Verification (K3 runs ONLY these — never cargo test/nextest: sandboxed test
binaries hang)
- `cargo check -p bombay --tests`
- `cargo clippy -p bombay --tests -- -D warnings`
- `cargo fmt --all` (then `cargo fmt --all -- --check`)
Tests themselves run in `nix flake check`, driven by the controller after
commit (untracked files are invisible to the flake — the controller stages
first).

## Out of scope — do NOT touch
- ANY production source (`crates/core/src/**`). If a test cannot be written
  without a production change, STOP and report `blocked` — that is a plan
  defect, not something to fix inline.
- `README.md`, `mutants-baseline.json`, the job-queue example/test (its
  supervision lives in steady-state `on_start`; the walking-skeleton bullet is
  closed as an explicit deferral on the card — the #248 precedent).
- `drain_equivalence.rs` (#266's file stays untouched).
- Lint levels, `clippy.toml`, `[lints]` — never relax; `#[expect]` with
  `reason` only if genuinely required.
