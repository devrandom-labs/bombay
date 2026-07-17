# Automated net over the actor loop (#164)

**Card:** [#164](https://github.com/devrandom-labs/bombay/issues/164) — "#149's unshipped half": a bolero target + MIRI-run tests over the actor loop and its stop modes. First of two PRs; PR2 closes #117 (its orphaned concurrency tests + triomphe/ADR note), for which this is a recorded dependency.

## Problem

The actor loop (`spawn.rs::run_lifecycle` → `kind.rs::run_message_loop`) sits in the one gap where **both** automated tools are blind:
- **Unfuzzed** — `fuzz/tests/mailbox.rs` imports only `bombay_core::mailbox`; `spawn.rs`/`kind.rs` are unreached. #149's 3.5M executions largely re-verified flume against a `VecDeque` oracle.
- **Near-unmutatable** — cargo-mutants replaces a fn body with `Default::default()`, and `Capacity`/`ActorId`/`RunResult`/`ActorRef` have no `Default` by design, so the loop yields only **4 viable mutants** (measured; confirmed by the #171 baseline: `spawn.rs` and `kind.rs` viable counts are tiny). "Zero survivors" there is a claim over a 4-mutant sample.

Its net today is hand-written tests alone. This card adds an automated net.

## The loop's semantics (asserted, from the source — `kind.rs:16-55`, `spawn.rs:138-185`)

- **on_start fails/panics** → `RunResult::StartupFailed(PanicError{OnStart})`; the loop never runs.
- **In-band `Signal::Stop`** (FIFO): everything queued ahead was already handled; Stop → `ActorStopReason::Normal`; anything queued after is abandoned.
- **Cancel token / all-senders-dropped**: "finish-current-then-stop, no drain" — the in-flight handler completes, the next `recv` observes the stop → `Normal`; the backlog is abandoned.
- **Handler returns `Err` or panics** → `catch_unwind`, `on_panic` observes, loop breaks with the resulting reason.
- **`on_stop` fails/panics** → logged (`log_on_stop_outcome`), the `reason` is **preserved** in `RunResult::Stopped { reason }`.
- **The loop holds only a WEAK self-ref** (`spawn.rs:165` `drop(actor_ref)`): dropping the last external `ActorRef` closes the mailbox and stops the actor (kameo issue #171; deleting that `drop` is the card's falsification target).

## MIRI resolution (verify-first, settled)

MIRI **does** drive the async loop: `spawn.rs`'s `#[tokio::test]` cases (current_thread and multi_thread) drive `PreparedActor::run`, the miri lane runs `cargo miri test -p bombay-core --lib -- --skip prop_` over exactly them, and that lane is green (miri-sweep on #181). ADR-0005 already documented that "MIRI cannot drive tokio" is false.

What keeps a **bolero** target off the isolation lane is **corpus filesystem I/O** (the same reason the lane `--skip prop_`), **not** tokio. So:
- The loop's invariants get **explicit `#[tokio::test]` cases in `bombay-core`**, MIRI-run for free by the existing lane — no new MIRI wiring.
- The **bolero target lives in `fuzz/`** for input breadth (fuzz lane only).
- The stale `fuzz/tests/mailbox.rs` comment "MIRI cannot drive tokio" is corrected to name the real constraint (corpus I/O).

## Architecture

Three pieces, one shared real surface (`PreparedActor::run`):

### 1. `fuzz/tests/actor_loop.rs` — the bolero target

`check!().with_type::<(u16, Vec<Op>)>()` where `Op ∈ { Send(u64), StopInBand, CancelStop, DropExternalRefs, DropReceiver }`. Each iteration builds a fuzz-local actor, drives the **real** `PreparedActor::run` on a `current_thread` runtime with **every await `terminate_bound`-wrapped** (a message-dropping change would otherwise hang → cargo-mutants TIMEOUT, per #148), and asserts **property invariants keyed on the stop mode** — deliberately NOT a counting oracle (a full oracle would re-encode the loop's drain logic, the "test the reimplementation" anti-pattern):

- **No strong self-ref held** — `DropExternalRefs` with an empty backlog stops the actor (`Normal`). Direct falsification of the `drop(actor_ref)` deletion.
- **Enqueued-before-drop still handled** — a `Send` before `DropExternalRefs` is handled before the all-senders-gone stop.
- **Drain-or-abandon boundary** — messages before `StopInBand` are handled; after it, abandoned.
- **RunResult matches path** — on_start-fail actor → `StartupFailed`; else `Stopped` with a reason consistent with the trigger.
- **Stop reason preserved through a failing `on_stop`** — an actor whose `on_stop` errors/panics still yields the loop's `reason` verbatim.
- **No panic escapes, no hang.**

Determinism: `current_thread` tokio is single-threaded and cooperative, so each `(cap, ops)` → one deterministic outcome — a sound fuzz oracle.

### 2. `bombay-core` explicit MIRI-run cases (extend `spawn.rs` tests)

A handful of hand-written `#[tokio::test]` cases covering the same stop-mode sequences the bolero target explores, so the loop's invariants are MIRI-covered by the existing lane. The `multi_thread` ones give schedule exploration under `-Zmiri-many-seeds` (#172's leg, if they qualify tasks>workers).

### 3. Two #168-audit sub-bullets (same `fuzz/` surface)

- **Corpus seeds** — commit ≥1 seed per target under `fuzz/tests/__fuzz__/<target>/corpus/` (today only `.gitkeep` → `bombay-fuzz-replay` is bounded-random, not deterministic). Falsify: a seed that trips a temporarily-injected bug goes red on replay. Correct `flake.nix:192`'s "deterministic corpus-replay" comment.
- **Mailbox closed-half** — make `Op::DropTx` able to drop the *sending* handle (not just a tail clone) and drop `rx`; assert `TrySendError::Closed`, `is_closed() == true`, `WeakMailboxSender::upgrade() → None` — today structurally unreachable in `mailbox_state_machine`.

### Wiring

Add `actor_loop` to `fuzz.yml`'s matrix (#152) and the in-gate `bombay-fuzz-replay` replay (`flake.nix`).

## Falsification (required, per #149/#150/#152 precedent)

Delete `drop(actor_ref)` at `spawn.rs:165`, confirm **the bolero target itself** (and the explicit no-strong-self-ref test) go red, then revert.

## Out of scope

- A full counting oracle over handled-count (re-encodes production logic).
- `RunResult::Killed` — kill/abort is a spawn-level `JoinHandle::abort` concern that drops the whole future, not a `run_lifecycle` outcome; the target covers `StartupFailed` vs `Stopped`.
- #117's orphaned concurrency tests (tell-vs-drop, upgrade races) and the triomphe/ADR note — PR2.

## Verify-first (implementation step 1)

Confirm empirically in the devshell before building the target:
1. A minimal `check!` target in `fuzz/` can drive `PreparedActor::run` on a `current_thread` runtime and replay a committed seed (`cargo test` in `fuzz/`).
2. The explicit `#[tokio::test]` loop cases pass under `cargo miri test -p bombay-core --lib` (spot-check one, since the full lane is slow).
