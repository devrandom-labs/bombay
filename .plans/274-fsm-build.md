# 274 — build the deadline plane (ADR-0025), then FsmActor/Fsm<S> (ADR-0024)

> **SUPERSEDED IN PART by ADR-0026 (#277, 2026-07-31).** S2/S3 as written
> are void: `next_deadline`/`on_deadline` do NOT land on `Actor` — they
> live in the `Deadlined` capability, so the plane part is BLOCKED ON
> ADR-0026 stage 1 (`CapSet`/`Ctx` machinery + the open-`Has` spike gate)
> and the loops query the cap set, not the actor. Part 2 (S6–S11) is
> replaced by stage 4 (`Phased`). Semantics, tests, and oracle ports in
> this plan remain the correct content; re-seat them per ADR-0026 when
> the stage-4 card is planned.

## Context

Design is FIXED by:
- `docs/adr/0025-framework-event-plane-deadlines.md` (plane: next_deadline/on_deadline, arm placement, fires-once guard, WeakActorRef rule)
- `docs/superpowers/specs/2026-07-31-274-framework-event-plane-design.md` (P-D1..P-D7, loop algorithm, model properties P1–P5)
- `docs/adr/0024-fsm-behavior-switching.md` + `docs/superpowers/specs/2026-07-31-231-become-fsm-design.md` (D1–D10, trait surface, wrapper algorithm; D7 amended — NO epochs, NO timer task, NO Signal variant)

Invariants that must hold throughout:
- `Fsm<S>::Msg == S::Msg` — no envelope, no new `Signal`/`ControlSignal` variant anywhere.
- No existing inter-arm select relation changes; new deadline arm sits ABOVE the mailbox arm, BELOW link/retries/aborts.
- Fires-once-per-value: after firing for value `d`, re-arm only when `next_deadline() != Some(d)`.
- `on_deadline`/`on_state_timeout` take `WeakActorRef` (drain-window: no message to mint a strong ref from).
- `PanicReason::OnDeadline` is NOT in `is_lifecycle_hook` (handler-like, restart-eligible).
- House rules: RPITIT (`fn -> impl Future + Send`), no bare arithmetic on counts/durations in prod paths, thiserror, every `#[allow]` has a reason, all `use` at top.
- SANDBOX: NEVER run `cargo test`/`cargo nextest` — verification per step is `cargo check -p bombay` + `cargo clippy -p bombay --lib` ONLY. Tests are AUTHORED here and RUN by the controller via `nix flake check` after.

## Steps

### Part 1 — the plane

**S1. `PanicReason::OnDeadline`** — SEQUENTIAL (root)
File: `crates/core/src/error.rs`
- Add variant after `OnLinkDied` (~line 248): `/// The `on_deadline` hook failed.` `#[error("on_deadline hook")] OnDeadline,`
- Do NOT touch `is_lifecycle_hook` (line 262–267).
- Extend the classification test block (lines ~661–672): `assert!(!PanicReason::OnDeadline.is_lifecycle_hook(), "a deadline hook is ordinary state processing — restart-eligible, unlike OnLinkDied");`
Expected: compiles; the classification is pinned by an assertion, not silence.

**S2. `Actor` trait: the two plane methods** — SEQUENTIAL, depends S1
File: `crates/core/src/actor/mod.rs` (trait `Actor`, lines ~68–139)
- After `handle`, add:
  - `#[must_use] fn next_deadline(&self) -> Option<tokio::time::Instant> { None }` — doc: declarative deadline, pure function of state, re-read by the loop after every state-touching step (quinn `poll_timeout` shape, ADR-0025); `None` = disabled.
  - `fn on_deadline(&mut self, actor_ref: WeakActorRef<Self>) -> impl Future<Output = Result<Flow, Self::Error>> + Send { let _ = actor_ref; async { Ok(Flow::Continue) } }` — doc: turn-boundary delivery, same catch_unwind/poisoning as `handle`, crash domain `PanicReason::OnDeadline`; Weak by drain-window necessity (cite ADR-0025); fires-once-per-value contract.
- Import `tokio::time::Instant` if not present (top-of-file use).
Expected: compiles; no existing impl breaks (defaults).

**S3. `kind.rs`: deadline arm in all three loops** — SEQUENTIAL, depends S2
File: `crates/core/src/actor/kind.rs`
- Add `async fn handle_deadline<A: Actor>(state: &mut A, self_ref: &WeakActorRef<A>) -> ControlFlow<ActorStopReason>` mirroring `handle_link_died` (lines 340–367): `AssertUnwindSafe(state.on_deadline(self_ref.clone())).catch_unwind().await`; `Ok(Ok(Flow::Continue)) -> Continue(())`, `Ok(Ok(Flow::Stop)) -> Break(Normal)`, `Ok(Err(e)) -> Break(Panicked(PanicError::new(Box::new(e), PanicReason::OnDeadline)))`, unwind -> `Break(Panicked(from_panic_any(payload, OnDeadline)))`.
- Shared arming shape (per loop-local variables, NOT a struct unless clippy arg-budget forces one): `let deadline = state.next_deadline(); let armed = deadline.is_some() && deadline != last_fired; let due = deadline.unwrap_or_else(Instant::now);` with `let mut last_fired: Option<Instant> = None;` before each loop.
- `run_message_loop` (lines 157–172): convert body to `tokio::select! { biased; () = tokio::time::sleep_until(due), if armed => { last_fired = deadline; handle_deadline(...) }  poll = poll_mailbox(&handles.cancel, mailbox_rx) => { handle_mailbox_step(...) } }`. Doc-comment: first select in this loop; cancel observation (inside poll_mailbox) delayed ≤ one hook turn when a deadline is due — bounded, pinned by test; disabled arm registers nothing with the timer wheel (Sleep registers lazily on first poll) — the structural argument the card's disabled-arm bullet accepts.
- `run_linked_message_loop` (lines 299–335): insert the deadline arm BETWEEN the link arm and the mailbox arm.
- `run_supervised_message_loop` (lines 394–490): insert BETWEEN the `pending_aborts` arm and the mailbox arm.
- Choice recorded in a comment: v1 recreates `sleep_until(due)` per iteration (O(1) wheel ops, Varghese & Lauck; simplest correct form); pinned `Sleep::reset` is the named optimization if S5's bench shows pain.
Expected: compiles clippy-clean (cognitive-complexity 9 / 80-line fn limits — extract helpers as needed, e.g. a `fn poll_deadline(...)`-style shared helper is acceptable if signatures stay ungeneric over loop internals).

**S4. Plane tests** — depends S3. PARALLEL OK with S5 (disjoint files).
File: NEW `crates/core/tests/deadline_plane.rs` (+ unit tests inside `kind.rs` where loop internals are needed)
Port the model properties as integration tests against real actors (all `#[tokio::test(start_paused = true)]` where timing matters; guard timeouts must OUTLAST the longest in-test timer):
- P1a prompt-under-saturation: actor with `next_deadline = start+10ms`, handler sleeps 1 ms virtual, 50 messages pre-queued (mailbox capacity ≥ 50 via `SpawnConfig`): hook fires after ~10 handled, not 50. Probe channel pattern (`tests/stash.rs` precedent).
- P2 disabled: `next_deadline = None` actor, 5 messages, stop: zero fires.
- P3 fires-once: hook leaves an already-due fixed deadline unchanged: exactly one fire, actor still serves a subsequent tell.
- P4 sliding: `next_deadline = last_activity + 20ms` (actor records `Instant::now()` in handle): touches at 15/30 ⇒ single fire at 50 (assert via captured `Instant`).
- P5 turn boundary: deadline due mid-30ms-handler: probe order Handled → HandlerDone → Fired.
- Drain window: spawn, tell N messages, drop ALL external strong refs (keep only a `WeakActorRef` + probe), deadline due during backlog drain: hook fires, receives a Weak that fails upgrade, `Flow::Stop` from the hook stops with `Normal` before backlog exhaustion.
- Panic-in-hook: hook panics ⇒ actor stops, watcher sees `Panicked` with `PanicReason::OnDeadline` (watch-notice pattern from existing supervision tests).
- Cancel-delay bound: token-stop (`ActorRef::stop`) issued while a deadline is due ⇒ actor stops after at most one hook turn (assert the hook ran ≤ 1 time then Normal stop).
- Ordering pins (one test per loop flavor): with BOTH a ready death notice and a due deadline, `on_link_died` runs first (linked/supervised); with a due deadline and a queued message, the hook runs first (all three).
- P1b lives as a doc-comment on the arm (the counter-model justification), NOT as a shipped starved arm.
Expected: files compile under `cargo check --tests` — DO NOT run them.

**S5. Cost bench** — depends S3. PARALLEL OK with S4.
File: `crates/core/Cargo.toml` (+ NEW `crates/core/benches/deadline_arm.rs`)
- Criterion bench, two cases over the plain loop's actor: `disabled` (next_deadline None) and `armed-far-future`: measure per-message throughput delta. Follow `benches/timers.rs` harness shape; measurement-vs-setup separation per test rules; use `std::hint::black_box` (criterion::black_box is deprecated in-repo).
Expected: compiles; numbers land in the PR body (controller runs the bench).

### Part 2 — Fsm on the plane

**S6. `fsm.rs` module** — depends S2 (not S3). PARALLEL OK with S3 (disjoint files); if dispatched sequentially, order S3 first.
Files: NEW `crates/core/src/fsm.rs`; `crates/core/src/lib.rs` (add `pub mod fsm;` next to `pub mod stash;`, line ~45)
Implement the #231 spec trait surface as amended (spec § "Trait surface"; `State: Copy + PartialEq + Send + 'static`; `on_state_timeout` takes `WeakActorRef<Fsm<Self>>`; **policy items REQUIRED, not defaulted** — Joel's strategy-pattern decision, 2026-07-31: policies are explicit like `SupervisionStrategy` (#196 no-default precedent), only mechanics keep defaults), reusing `crate::stash::Stash` (pub(crate) `bounded`/`pop_ready` — `Fsm` is in-crate):
- `pub enum Step<St> { Stay, Goto(St), Stop }` (derive Debug/Clone/Copy/PartialEq/Eq), `pub enum Disposition { Deliver, Defer, Ignore }` (same derives).
- `pub trait FsmActor`: Args/Error/State; **REQUIRED (no default bodies)**: `initial_state(&Args) -> State`, `stash_capacity(&Args) -> Capacity`, `gate(state: &State, msg: &Msg) -> Disposition` (static — a declaration table, #243-derivable; even "deliver everything" is now written), `state_timeout(&self, state: &State) -> Option<Duration>` (**takes `&self`** so deadline magnitudes are Args-tunable per instance — the D8 constructor-input channel, never SpawnConfig; document: keep it a pure function of (self, state)), `on_start(Args, ActorRef<Fsm<Self>>)`, `handle(&mut self, &State, Msg, ActorRef<Fsm<Self>>, &mut Stash<Msg>) -> Result<Step<State>, Error>`, `on_state_timeout(&mut self, &State, WeakActorRef<Fsm<Self>>, &mut Stash<Msg>) -> Result<Step<State>, Error>` (required — declaring a deadline forces writing its reaction; the silent declared-timeout-defaulted-handler pair is unrepresentable). **DEFAULTED (safe mechanics)**: `on_defer_full` (delegate to `self.handle(...)` — the handback), `on_stop`/`on_panic` passthroughs. All RPITIT.
- **`spawn_fsm` sugar**: `impl<S: FsmActor> ... fn spawn_fsm(args: S::Args) -> ActorRef<Fsm<S>>` (blanket, mirroring `Spawn`; plus `spawn_fsm_with_config(SpawnConfig, args)`) so call sites never name `Fsm<S>`.
- `pub struct Fsm<S: FsmActor> { data: S, state: S::State, entered_at: tokio::time::Instant, stash: Stash<S::Msg> }` — NO epoch field, NO timer handle.
- `impl Mailboxed for Fsm<S> { type Msg = S::Msg; }` — the closed-menu constraint made structural.
- `impl Actor for Fsm<S>`: `name() = type_name::<S>()`; `on_start` (initial_state, `entered_at = Instant::now()`, `Stash::bounded(cap)`); `handle` per the #231 spec wrapper algorithm (gate → Deliver/Defer-with-`on_defer_full`-overflow/Ignore; apply Step; replay loop popping `ready`, re-gating in the CURRENT state — re-deferred goes to `held`); `next_deadline(&self) = self.data.state_timeout(&self.state).map(|d| self.entered_at + d)`; `on_deadline` routes to `S::on_state_timeout(&mut self.data, &self.state, actor_ref, &mut self.stash)`, applies the Step, runs the same replay loop; `on_stop`/`on_panic` passthrough.
- `transition(&mut self, next)`: only when `next != self.state`: `self.state = next; self.entered_at = Instant::now(); self.stash.unstash_all();` — deadline cancel/re-arm is IMPLICIT via next_deadline (that is the whole point; say so in the doc).
- `Instant::now()` in transition: use `tokio::time::Instant::now()` (paused-clock testable).
Expected: compiles clippy-clean; no public item leaks loop internals.

**S7. Fsm unit tests (in `fsm.rs` `#[cfg(test)]`)** — SEQUENTIAL after S6 + S3.
- Goto(current) ≡ Stay: no unstash, `entered_at` unchanged (assert deadline value stable via `next_deadline()`).
- Transition order: stash released and replayed IN the new state, ahead of mailbox backlog, arrival order.
- Gate: Deliver/Defer/Ignore each observed; `handle` never sees a Defer/Ignore message (probe).
- Defer overflow → `on_defer_full` gets the INTACT message (assert payload equality); default delegates to handle.
- Stale-timeout unrepresentable: state_timeout(Loading)=30s, transition to Ready at t=10s, advance past 30s ⇒ `on_state_timeout` NEVER fires (next_deadline changed at transition; fires-once + value change cover it). This replaces the epoch tests.
- Commit-after-Ok: handler panics (after mutating a scratch field but before returning Goto) ⇒ on_stop probe observes the PRE-transition state tag.
- Spawn/tell/Recipient mint on `Fsm<A>` works (one smoke test).
Expected: compiles; not run here.

**S8. Equivalence oracle port** — depends S6. PARALLEL OK with S9, S10 (disjoint files).
File: NEW `crates/core/tests/fsm_equivalence.rs`
Port the #231 spike oracle (spec § "Spike record"): idiom variant = `StashActor` + manual phase field + manual unstash (+ its LoadDeadline via `send_after` and manual guards), fsm variant = `FsmActor` (gate + state_timeout). Same 6 scenarios, same probe-sequence assertions (S1 happy path with replay-ahead-of-backlog; S2 overflow shed; S3 deadline fires (paused); S4 stale-deadline invisibility — idiom forges a late LoadDeadline tell, fsm proves non-delivery via the S7 mechanism instead (mode-adapted, both must end with identical probe sequences); S5 drain-during-loading refusals; S6 deadline cancelled on timely transition). Collect-guard must outlast 3× the longest timer.
Expected: compiles.

**S9. Allocation guards** — depends S6. PARALLEL OK with S8, S10.
File: NEW `crates/core/tests/alloc_fsm.rs` (one-test binary, `#[global_allocator] CountingAlloc` — `tests/alloc_exact.rs` precedent; needs `test-support` feature wiring identical to that file's harness block in Cargo.toml)
- Warm up; measure gross allocs: one Stay message vs one Goto message (empty stash, no timeout in target state): assert equal (transition adds 0).
- One Goto INTO a state WITH a state_timeout: assert equal as well — arming is `next_deadline` arithmetic, NO task, NO allocation (this is the plane's improvement over the mock's measured 3).
Expected: compiles.

**S10. Job-queue walking skeleton** — depends S6. PARALLEL OK with S8, S9.
Files: `crates/core/examples/job_queue/app.rs` (Worker, lines ~121–200), `crates/core/examples/job_queue/main.rs`, `crates/core/tests/app_job_queue.rs`
- Convert `Worker` to `FsmActor` with `State { Serving, Draining }`: Serving handles jobs as today; a new `Drain` message → `Goto(Draining)`; gate: in Draining, incoming job messages are DELIVERED and refused with the existing typed refusal (NOT Ignored — the asker must learn); a `FlushDone`-style self-pipe (existing pipe seam) → `Step::Stop`.
- Extend `app_job_queue.rs`: a drained worker refuses a new job with the typed error AND completes the in-flight one first (probe/reply assertions, existing test idioms).
- Keep the diff minimal — this demonstrates the feature, it does not redesign the app.
Expected: example + test compile.

**S11. Wiring** — SEQUENTIAL, last.
- `mutants-baseline.json`: run NOTHING; add entries by the established shape for every new fn (`error.rs` variant fns none; `kind.rs::handle_deadline`; every `fsm.rs` fn incl. trait defaults with bodies; `next_deadline` impls). Non-Default-return fns → `known_zero_viable` section per its README/conventions in the file. Controller will run `cargo mutants --list` and correct.
- `README.md`: one public-API bullet for `fsm::{FsmActor, Fsm, Step, Disposition}` + `Actor::{next_deadline, on_deadline}` and a ~10-line usage sketch; keep under the README rules (no card numbers).
- `docs/testing/coverage-baseline.md`: add the new test files with one-line descriptions.
Expected: `cargo check -p bombay` + `cargo clippy -p bombay --lib` clean at the end.

## Verification (per step and final)

- After EVERY step: `cargo check -p bombay` (workspace member) and `cargo clippy -p bombay --lib`.
- After S4/S8/S9/S10: `cargo check -p bombay --tests --examples` (compile only).
- NEVER `cargo test`/`nextest`/`miri`/`mutants` — sandboxed; the controller runs `nix flake check` (git-adds new files first) and the bench.
- Final output line: `<<<KIMI-DONE: done|blocked>>>`.

## Out of scope

- #241's surface (verbs/menu message for receive-timeout) — the plane only.
- Derive/macros (#243). Hierarchical states, named timeouts.
- Any change to `Signal`, `ControlSignal`, the control lane, link channel, ADR-0018 timers, or existing arm relations.
- `clippy.toml` / `[lints]` — never touched.
- `cargo hakari` (not wired in this repo).
