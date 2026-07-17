# Actor-loop bolero + MIRI net (#164) — execution plan

> Execution: the bolero target + oracle is tightly-coupled async code, built **inline via TDD** (I hold the context). Independent adjuncts (corpus seeds, mailbox closed-half, fuzz.yml/flake wiring, explicit MIRI tests) are delegated to subagents. Spec: `docs/superpowers/specs/2026-07-17-164-actor-loop-fuzz-design.md`.

**Branch:** `test/164-117-loop-invariants` (already created). Closes #164 on merge (PR1 of 2).

## Grounded API facts (read from source, not assumed)
- `PreparedActor::<A>::new(cap)` → `.actor_ref() -> &ActorRef`, `.run(args).await -> RunResult<A>`, `.spawn(args) -> JoinHandle`.
- Enqueue msg: `actor_ref.tell(msg).await`. In-band stop: `actor_ref.mailbox_sender().send(Signal::Stop).await`. Out-of-band: `actor_ref.stop()` (cancel). Kill: `actor_ref.kill()` → `RunResult::Killed`.
- `RunResult`: `StartupFailed(PanicError)` | `Stopped { actor, reason: ActorStopReason }` | `Killed`.
- Test helpers in `spawn.rs` tests: `cap(n)`, `bounded(fut)`, `terminate_bound()`.
- `Actor` trait: `type Args/Msg/Error`, `on_start(args, ref)`, `handle(msg, ref, &mut stop)`, `on_stop(weak, reason)`, `name()`.
- Fuzz target pattern: `fuzz/tests/mailbox.rs` — `check!().with_type::<(u16, Vec<Op>)>().for_each(...)`; `Op: TypeGenerator`.

## Deterministic oracle (the design)
Enqueue the whole Op script, then `run().await`. Termination is guaranteed by always dropping external refs before `run` (or by an in-band `Stop`/cancel), so the loop always ends:
- **cancel-before-run** (`CancelStop` present) → `handled == 0`, `Normal` (cancel short-circuits the first `recv`).
- **in-band `Stop`** at position k → `handled == (messages before k)`, `Normal`; messages after abandoned.
- **else drain-then-close** (refs dropped) → `handled == all queued messages`, `Normal`.

---

## Task 1 — verify-first spike: minimal target drives the loop + replays
- [ ] Add a `fuzz/tests/actor_loop.rs` with a fuzz-local `Actor` (counts handled messages into a shared counter; trivial `on_start`/`handle`/`on_stop`), a `current_thread` runtime built inside the `check!` closure, and ONE case: enqueue 2 msgs, drop refs, run, assert `handled == 2` and `Stopped{Normal}`. Bound every await with `terminate_bound`.
- [ ] Register the target in `fuzz/Cargo.toml` (mirror `mailbox`'s `[[test]]` entry).
- [ ] Run `cd fuzz && nix develop --command cargo test actor_loop` — confirm it passes (replay/bounded-random mode). This proves bolero can drive `PreparedActor::run` on current_thread. If it can't, STOP and report.

## Task 2 — the Op vocabulary + oracle (TDD, inline)
- [ ] `Op = Send(u64) | StopInBand | CancelStop`. Generate `(u16 cap_seed, Vec<Op>)`. Drive: build prepared, clone ref, apply each Op (Send→tell, StopInBand→mailbox_sender().send(Signal::Stop), CancelStop→record + call `stop()` before run), then `drop` external refs, then `run`.
- [ ] Compute the deterministic expected `(handled_count, reason)` from the ops per the oracle above — as a SMALL predicate, not a re-encoding of the loop (only the three stop-mode branches).
- [ ] Assert `RunResult::Stopped { reason: Normal, .. }` and `handled == expected`. No panic, no hang.
- [ ] Run the target; iterate until green over bounded-random.

## Task 3 — the harder invariants (TDD, inline)
- [ ] **RunResult matches path**: a second fuzz-local actor whose `on_start` fails → assert `StartupFailed` regardless of ops (separate small target or a param).
- [ ] **Stop reason preserved through failing `on_stop`**: a fuzz-local actor whose `on_stop` returns `Err`/panics → the `reason` still equals what the ops imply.
- [ ] **No-strong-self-ref (the falsification anchor)**: the drain-then-close case with a non-empty backlog is exactly this; make it an explicit assertion in the oracle path.

## Task 4 — falsify (required)
- [ ] Delete `drop(actor_ref)` at `spawn.rs:165`; run `cargo test actor_loop` in `fuzz/`; confirm the target goes RED (hang→`terminate_bound` fires, or wrong handled-count). Revert exactly (`git diff` clean on spawn.rs). Record the failure evidence.

## Task 5 — explicit MIRI-run cases (delegate)
- [ ] Add ~3 `#[tokio::test]` cases to `bombay-core/src/actor/spawn.rs` tests covering the three stop modes with exact assertions (mirroring the oracle), so the loop invariants are MIRI-covered by the existing `cargo miri test -p bombay-core --lib` lane. At least one `multi_thread` (tasks>workers) for #172's seed leg. Spot-check one under `cargo miri test`.

## Task 6 — corpus seeds + deterministic replay (delegate)
- [ ] Commit ≥1 seed per target under `fuzz/tests/__fuzz__/actor_loop/corpus/` and `.../mailbox_state_machine/corpus/` (bolero writes corpus files; generate one by running the target once, or hand-author a minimal input bolero accepts).
- [ ] Falsify replay: a seed that trips a temporarily-injected bug goes red on `bombay-fuzz-replay`; revert.
- [ ] Correct `flake.nix:192`'s "deterministic corpus-replay" comment to be true now that seeds exist.

## Task 7 — mailbox closed-half (delegate)
- [ ] In `fuzz/tests/mailbox.rs`: make `Op::DropTx` able to drop the *sending* handle (not just a tail clone) and add dropping `rx`; assert `TrySendError::Closed` hands the signal back, `is_closed() == true`, `WeakMailboxSender::upgrade() -> None`. Update the target's stale "MIRI cannot drive tokio" / "never Closed" comments.

## Task 8 — wiring + gate
- [ ] Add `actor_loop` to `.github/workflows/fuzz.yml`'s matrix and confirm the in-gate `bombay-fuzz-replay` (flake) picks it up.
- [ ] `nix flake check` green; `cd fuzz && cargo test` green.

## Task 9 — PR
- [ ] Push; open PR closing #164 (map every scope bullet incl. the two #168-audit sub-bullets). Note PR2 (#117) follows.
