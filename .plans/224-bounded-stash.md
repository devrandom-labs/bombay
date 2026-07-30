# 224 — Bounded stash: `Stashed<S>` composition

## Context

Card #224. Full task-by-task plan with ALL code blocks:
`docs/superpowers/plans/2026-07-30-224-bounded-stash.md` — read it first; this
file adds the execution constraints and step ordering. Design spec (decisions
D1–D8, rejected alternatives, stop-fate table):
`docs/superpowers/specs/2026-07-30-224-bounded-stash-design.md`. ADR content is
given verbatim in the plan (Task 6).

Invariants that must hold (violating any = stop and report blocked):

- `crates/core/src/actor/kind.rs` and every run loop: **zero changes**. The
  replay mechanism lives entirely inside `Stashed::handle` (plan Task 2).
- `Stash::bounded` and `Stash::pop_ready` are `pub(crate)` — a stash must be
  impossible to construct or drain outside the wrapper.
- Overflow never panics, never drops: `StashFull<M>` carries the message back
  (`TellError<M>` precedent, `crates/core/src/error.rs:27`).
- No bare arithmetic in capacity paths (checked_add; overflow reads as full,
  never as small).
- Replay order: user handler first, then drain `ready` front-first in the
  same `handle` call, `while !*stop`.
- Clippy config is law: no `#[allow]` without `reason`, no lint relaxation,
  cognitive-complexity 9 / 5 args / 80 lines per fn. If `Stashed::handle`
  trips a limit, extract a private `async fn drain_ready` — same semantics.
- Test assertions from the plan are fixed. If an API name in a test differs
  from reality (the plan flags `try_send_message` and the ask-closure form as
  the two likely spots), adjust the plumbing to the real API — never weaken or
  reshape an assert.

## Steps

1. **Task 1 — `Stash<M>` + `StashFull<M>` + unit tests + lib.rs wiring.**
   SEQUENTIAL (foundation). Files: create `crates/core/src/stash.rs`, modify
   `crates/core/src/lib.rs`. Code verbatim from plan Task 1.
   Verify: `cargo check -p bombay` + `cargo clippy -p bombay --lib -- -D warnings`.
2. **Task 2 — `StashActor` + `Stashed<S>` wrapper.** SEQUENTIAL — depends on
   step 1 (same file `stash.rs`). Code verbatim from plan Task 2.
   Verify: `cargo clippy -p bombay --lib -- -D warnings`.
3. **Tasks 3+4+5 — integration tests.** SEQUENTIAL — one file
   (`crates/core/tests/stash.rs`), written in plan order (Gate actor + 3
   ordering tests, then 3 stop-fate tests, then the supervised-restart test +
   `Boom` variant). Code from plan Tasks 3–5.
   Verify: `cargo check -p bombay --tests` (compiles; do NOT run tests — see
   Verification).
4. **Task 6 — ADR-0022 + ADR index row.** PARALLEL OK with step 5 (disjoint
   files: `docs/adr/0022-bounded-stash.md`, `docs/adr/README.md`). ADR text
   verbatim from plan Task 6. sonic-suitable.
5. **Task 8 — README bullet + coverage baseline.** PARALLEL OK with step 4
   (disjoint files: `README.md`, `docs/testing/coverage-baseline.md`).
   Bullet text from plan Task 8; coverage entry follows that file's existing
   per-module format. sonic-suitable.
6. **Task 9 — job-queue intake gate.** SEQUENTIAL — depends on step 2. Files:
   `crates/core/examples/job_queue/app.rs`, `.../main.rs` (**mandatory** —
   the demo submits directly at `main.rs:58,76`; route those through the
   intake, else `dead_code` fails the flake gate),
   `crates/core/tests/app_job_queue.rs`. `Intake` code from plan Task 9 with
   one pre-authorized plumbing fix: the overflow rejection is
   `reply.send_err(SubmitError::QueueFull)` (`send` sends Ok — precedent
   `app.rs:346`). The new app test must be REAL code built on that file's
   existing helpers — the plan gives the scenario + fixed asserts (deferred
   ask A completes Ok before post-resume B; both Ok; bounded awaits; keep the
   test on real time, NOT `start_paused` — the deferred ask rides the 5 s
   default ask deadline). Holding A's pending ask needs the ActorRef cloned
   into a `tokio::spawn`.
   Verify: `cargo check -p bombay --examples --tests`.
7. **Task 7 (mutants baseline) and plan Task 10 (flake gate, push, PR) are
   NOT yours** — Claude drives both unsandboxed after your handoff. Skip
   them.

While appending the ADR-0022 index row (step 4): the index is missing rows
0017–0019 though the files exist — add those three rows too, same format
(small pre-existing gap, in scope for the index edit only).

Commit per plan task with the plan's exact commit messages (Tasks 1–6, 8–9).
`cargo fmt` before every commit.

## Verification

- Per step: `cargo check` / `cargo clippy` as listed above. **NEVER run
  `cargo test` or `cargo nextest` — sandboxed test binaries hang
  uninterruptibly here.** All test EXECUTION happens in Claude's
  `nix flake check` after you finish; your job is that everything compiles
  clean and the test code faithfully matches the plan.
- Final output line exactly: `<<<KIMI-DONE: done|blocked>>>`.

## Out of scope — do NOT touch

- `crates/core/src/actor/kind.rs`, `spawn.rs`, `actor_ref.rs`, `mailbox.rs`
  (read freely, modify never).
- `mutants-baseline.json` (Claude's step).
- `clippy.toml`, any `[lints]` table, any workflow file.
- No new dependencies. No `unstash(n)`. No `Watch`/`Supervisor` impl for
  `Stashed<S>` (spec L3).
