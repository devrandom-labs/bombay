# 218-job-queue — compositional example app + M1 exit-gate test

## Context

Card #218: one job-queue mini-app proving bombay's rebuilt actor spine composes
(spawn_supervised → registry → supervision restart → death-watch → ask/tell →
timers → pipe → drain), shipped as a runnable example AND an integration test.

**The complete design and the FULL code for every file is in
`docs/superpowers/plans/2026-07-28-218-job-queue-exit-gate.md` — read it FIRST
(Tasks 2–7 there contain the exact code blocks). This plan tells you what to
build and in what order; that document tells you the code.** Spec (rationale):
`docs/superpowers/specs/2026-07-28-218-job-queue-exit-gate-design.md`.

Invariants that must hold:
- At-least-once: no submitted job lost across worker crash + rebuild; every job
  completed ≥1× or recorded failed; queue empty at drain. NEVER assert
  exactly-once for a crashing job.
- The factory closure captures `WeakActorRef<Dispatcher>` — weak is MANDATORY
  (strong = self-cycle through the child table; dispatcher never
  ref-count-stops).
- Supervisor `on_link_died` NEVER fires for supervised children (verified:
  `crates/core/src/actor/kind.rs` dispatch_death) — do NOT "fix" the
  `WorkerReplaced` pattern into an on_link_died handler; it cannot work.
- Engineering rules apply: thiserror one-variant-per-domain, no `|_|` that
  discards a source error (wrap with `#[source]`), checked arithmetic on
  counters via the `bump` helper, no `unwrap` in app logic (`expect` only for
  structurally-impossible states, with a message saying why).
- Every test await is bounded with `timeout(terminate_bound(), ..)` via the
  `bounded` helper — an unbounded await hangs the mutants/MIRI lanes.

Verification facts you must respect (sandbox): you CANNOT run tests — `cargo
test`/`cargo nextest` binaries hang in this sandbox. Verify with `cargo check`
+ `cargo clippy` only; the controller runs the real tests afterward.

The plan document marks several API details as verify-first (exact `AskError`
variant names; `RestartConfig: Clone`; `Recipient: Debug`; `ActorId: Copy`;
`WeakActorRef::upgrade` name). Verify each against the source with `rg` before
using it and adapt the code to what the source actually says. If an adaptation
reveals genuine friction (e.g. a handle type blocks `#[derive(Debug)]` on a
message enum), APPEND a row to `docs/warts/218-example-warts.md` (severity +
one-line wart, issue cell `pending`) — do not file issues yourself.

## Steps

1. **Create `crates/core/examples/job_queue/app.rs`** — SEQUENTIAL (everything
   depends on it). Code: plan doc Task 3 Step 1 (complete module: JobKind/Job/
   SubmitError/Stats/DrainReport/bump, Done/WorkerMsg/WorkerError/Worker,
   DispatcherMsg/AppError/DispatcherConfig/Dispatcher + dispatch/
   requeue_outstanding/maybe_finish_drain, OverseerMsg/Overseer, App/start).
   Apply the verify-first adaptations noted above.
   Verify: `cargo check -p bombay --example job_queue` will fail only for the
   missing `main.rs` — so create the two-line stub from plan doc Task 2 Step 1
   in the same step, then the check must PASS.

2. **Create `crates/core/tests/app_job_queue.rs`** — SEQUENTIAL, depends on
   step 1. All four tests: plan doc Task 2 Step 2 (sequence), Task 4 Step 1
   (lifecycle incl. the fresh-ActorId poll block placed between submits and
   drain), Task 5 Step 2 (boundary — first do Task 5 Step 1: verify real
   `AskError` variant names with `rg -n 'pub enum AskError' -A 30
   crates/core/src/error.rs` and use those names; also confirm the timeout
   classification methods before asserting `is_terminal`), Task 6 Step 1
   (linearizability). Include `SubmitError` in the `use app::{...}` list.
   Verify: `cargo check -p bombay --tests` PASSES.

3. **Replace `crates/core/examples/job_queue/main.rs` with the demo** —
   SEQUENTIAL, depends on step 1 (disjoint from step 2's file but needs the
   tracing-subscriber decision): first run plan doc Task 7 Step 1 (`rg -n
   'tracing-subscriber' crates/core/Cargo.toml Cargo.toml`); if absent, add it
   as a workspace-root `[workspace.dependencies]` entry (latest version) +
   `crates/core/Cargo.toml` `[dev-dependencies]` `tracing-subscriber = {
   workspace = true }`, and append the paper-cut wart row. Then write the demo
   from Task 7 Step 2. Verify: `cargo check -p bombay --example job_queue`
   PASSES.

4. **README pointer** — PARALLEL OK with step 3 after step 1 (disjoint file).
   Add the paragraph from plan doc Task 7 Step 4 after the
   `#[derive(bombay_macros::Msg)]` note (~line 70 of README.md).
   Verify: none (prose).

5. **Teardown fact-check** — PARALLEL OK with steps 2–4 (read-only). Plan doc
   Task 3 Step 3: read the supervised-loop teardown in
   `crates/core/src/actor/kind.rs` and answer: does a supervisor stopping with
   reason `Normal` stop its remaining children? Report the answer with
   file:line evidence in your final output. If NO, append a `blocker` wart row
   and say so loudly in the final report — do NOT work around it.

6. **Final pass** — SEQUENTIAL, after all: `cargo clippy -p bombay --examples
   --tests 2>&1 | tail -30` — fix warnings in YOUR new files only (never touch
   lint config, never `#[allow]` without a `reason`). `cargo fmt --all`.

## Verification

- `cargo check -p bombay --example job_queue` — PASS
- `cargo check -p bombay --tests` — PASS
- `cargo clippy -p bombay --examples --tests` — no warnings from the new files
- `cargo fmt --all` run
- DO NOT run `cargo test` / `cargo nextest` / `nix flake check` (sandbox
  hang); the controller runs them after review.
- DO NOT commit — the controller commits after review.

## Out of scope

- Anything under `crates/core/src/` (library code) — if the app cannot be
  expressed without a core change, STOP and report blocked.
- `clippy.toml`, `[lints]`, `mutants-baseline.json`, CI workflows, flake.
- Filing GitHub issues (append wart rows only).
- `docs/superpowers/**` (the plan/spec are read-only inputs).
- Deleting or renaming any existing file.
