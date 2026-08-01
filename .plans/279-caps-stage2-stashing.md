# 279 — caps stage 2: `Stashing` capability (ports ADR-0022; removes `StashActor`/`Stashed`)

## Context

ADR-0026 stage 2. Stage 1 (#278) shipped the caps machinery in
`crates/core/src/caps.rs` (`Actor`/`Ctx`/`CapSet`/`Provide`, `Shell<A>`
adapter on the untouched loop, `#[derive(Provide)]`, `spawn`/`spawn_with`).
This card relocates ADR-0022 bounded deferral onto that surface as the
`Stashing<M>` capability and deletes the old `StashActor`/`Stashed<S>` trait+
wrapper pair.

**The one design point — decided (option A, "derive-driven replay"):**
`Shell<A>` runs on the untouched loop and holds `A::Caps` as an *opaque*
`CapSet`, so its `handle` cannot generically discover a stash. Making the set
walkable for the loop needs a uniform hook; a blanket impl is coherence-
infeasible (E0119) and specialization is unstable, so the `#[derive(Provide)]`
that already reads the cap-set fields *also* emits the loop hook. This keeps
ONE `Shell`, ONE `spawn`, and forget-proof auto-replay (ADR-0022's core
guarantee — silent forever-defer stays unrepresentable). The tower-`Layer` +
selector loop is the leading candidate for stage-3 loop-selection and is
recorded as such; it is out of scope here.

Invariants that MUST hold (violating any = stop):
- ADR-0022 semantics preserved verbatim: two-queue snapshot (`held`→`ready`),
  in-step replay ahead of the mailbox backlog in arrival order, bounded
  refusal-with-handback (`StashFull<M>`, message never dropped/panicked),
  non-pinning (a non-empty stash never blocks `Collected`), D6 stop-fate
  (every terminal path drops the remainder; only `unstash_all` rescues),
  restart → fresh stash from `Args`.
- Constraint 1 (ADR-0026): NO runtime-checked cap accessor. Access is
  `cx.cap::<Stashing<M>>()`, compile-gated by `Provide`.
- `Stash<M>`/`StashFull<M>` KEPT (buffer primitive + typed handback). Only
  `StashActor` + `Stashed<S>` are removed.
- `Stash::bounded`/`Stash::pop_ready` stay `pub(crate)`; `Stashing::pop_ready`
  stays `pub(crate)` (only the framework drains — the derived hook reaches it
  through the in-core `Replay` impl on `Stashing`, never a `pub` drain).
- No bare arithmetic in count paths (checked; overflow reads as full).
- Zero changes to `actor/`, `mailbox.rs`, the run loop. Replay lives entirely
  in `Shell::handle`.
- House rules: RPITIT, thiserror, all `use` at top, every `#[allow]`/`#[expect]`
  reasoned. No `#[deprecated]` (deny-warnings gate); the old surface is
  DELETED, not deprecated.
- SANDBOX: never `cargo test`/`nextest`/`miri`/`mutants` (hangs). Per-step:
  `cargo check` + `cargo clippy --lib`. Gate = `nix flake check`, FOREGROUND
  (background masks the exit — #278 gotcha), after `git add`.

## Design (the shapes)

In `caps.rs`:
- `pub trait Replay<M> { fn next_replay(&mut self) -> Option<M>; }` — the loop
  hook. `impl<M> Replay<M> for ()` returns `None` (plain-actor floor).
- `pub struct Stashing<M> { stash: crate::stash::Stash<M> }` wrapping the kept
  buffer. Public API: `stash(msg) -> Result<(), StashFull<M>>`, `unstash_all()`,
  `len()`, `is_empty()`; `pub(crate) fn pop_ready(&mut self) -> Option<M>`.
  Constructed by `bounded(cap: Capacity)`.
- `pub trait StashPolicy<A: Actor> { fn capacity(args: &A::Args) -> Capacity; }`
  — the required, testable capacity seam (Args-sourced; never `SpawnConfig`, D8).
- `impl<M> Replay<M> for Stashing<M> { fn next_replay(&mut self) -> Option<M> {
  self.pop_ready() } }` — in-core, so it reaches `pub(crate) pop_ready`.
- Re-export `pub use crate::stash::StashFull;`.
- `Shell::handle`: run the user handler once; if `Flow::Stop`, stop; else
  `while let Some(m) = self.caps.next_replay() { … A::handle(m) … }`, stopping
  on any replayed `Flow::Stop` — the exact old `Stashed::handle` shape.
  Bound `A::Caps: Replay<A::Msg>` on the `impl actor::Actor for Shell<A>` and
  on `spawn`/`spawn_with`.

In `derive_provide.rs` — extend `#[derive(Provide)]` (still ONE derive, so
nothing to forget) to ALSO emit a `Replay` impl:
- For each field whose type's final path segment is `Stashing` with generic
  arg `M`: emit `impl ::bombay::caps::Replay<#M> for #Ident { fn next_replay(
  &mut self) -> Option<#M> { ::bombay::caps::Replay::next_replay(&mut self.#f) } }`.
- If NO stash field: emit `impl<__M> ::bombay::caps::Replay<__M> for #Ident {
  fn next_replay(&mut self) -> Option<__M> { None } }`.
- Never both (a stash field ⇒ concrete impl only, no blanket ⇒ no E0119).

## Steps

1. **caps.rs — `Replay` + `Stashing` + `StashPolicy` + `Shell` drain.**
   SEQUENTIAL (root). Add unit tests: `Replay for ()` is None; `Stashing`
   stash/unstash/overflow-handback/pop_ready ordering; `Stashing: Replay`
   drains ready in arrival order. Verify: `cargo check -p bombay`,
   `cargo clippy -p bombay --lib`.
2. **derive_provide.rs — emit `Replay`.** SEQUENTIAL — depends on 1 (path
   `::bombay::caps::Replay` must exist). Extend the parser to capture stash
   fields (final segment `Stashing`, its `<M>`); extend `ToTokens`. Unit tests:
   a stash field emits `impl … Replay < … > for`; a no-stash struct emits the
   `__M` blanket; a two-cap struct (one `Stashing<T>`, one non-stash) emits
   exactly one concrete Replay. Add a COMPILE-ONLY success doctest and keep the
   E0119 duplicate-field `compile_fail`. Verify: `cargo check -p bombay_macros`.
3. **stash.rs — delete `StashActor`/`Stashed`.** SEQUENTIAL — after 1
   (Stashing now owns the cap role). Remove the trait, the wrapper, their
   `#[cfg(test)]` tests, and their doc. KEEP `Stash<M>`/`StashFull<M>` and
   their four unit tests. Update the module doc to "buffer primitive; the
   `Stashing` cap (caps.rs) is the ADR-0022 surface". Verify: `cargo check -p
   bombay --lib`.
4. **tests port → `crates/core/tests/caps_stashing.rs`.** SEQUENTIAL — after
   1–3. Rewrite `Gate` and `StopProbe` onto `caps::Actor` + a `Stashing<GateMsg>`
   cap (a `GateCaps` struct with `#[derive(Provide)]`, a `GatePolicy: StashPolicy`,
   hand-written `CapSet::build`). Port ALL nine behaviors verbatim in intent:
   `replay_runs_before_backlog_in_arrival_order`, `no_stale_replay_after_batch_drains`,
   `replayed_stop_abandons_rest_of_batch`, `stashed_messages_do_not_pin_refcount_stop`,
   `inband_stop_drops_stash`, `kill_drops_stash`, `restart_gets_a_fresh_stash`,
   name-is-user-type (now `Shell<Gate>`), on_stop-delegates. Add the **pinned
   one-queue livelock re-proof** as a documented unit test (a one-queue model
   deadlocks; the two-queue snapshot terminates — mirrors ADR-0026's re-proof).
   Delete `crates/core/tests/stash.rs`. Verify: `cargo check -p bombay --tests`.
5. **Migrate the job-queue `Intake`.** SEQUENTIAL — after 1–3. `app.rs`: `Intake`
   becomes a `caps::Actor` with `type Caps = IntakeCaps` (holds
   `Stashing<IntakeMsg>`); `handle` uses `cx.cap::<Stashing<IntakeMsg>>()` for
   `stash`/`unstash_all`; keep the same Pause/Resume/overflow-handback logic.
   `main.rs` + `app_job_queue.rs`: `Stashed::<Intake>::spawn((disp, cap))` →
   `caps::spawn::<Intake>(...)`; drop `stash::Stashed` imports. The existing
   app-test stash scenario (deferred ask A completes before post-resume B)
   MUST still pass unchanged in intent. Verify: `cargo check -p bombay
   --examples --tests`.
6. **Wiring.** SEQUENTIAL, last. `mutants-baseline.json`: entries for every new
   fn (`Stashing::*`, `Replay::next_replay` impls, derive helpers) per the
   two-section shape (non-Default returns → known_zero_viable). README: rewrite
   the stash bullet to the `Stashing` cap. `docs/testing/coverage-baseline.md`:
   replace the `stash (#224)` section (drop StashActor/Stashed lines, add the
   `Stashing` cap + `caps_stashing.rs`). `docs/adr/0022-bounded-stash.md`:
   append a dated amendment note — "superseded by the `Stashing` capability
   (ADR-0026 stage 2, #279); semantics preserved invariant-by-invariant; the
   composition/forget-proofness now rides the derived `Replay` hook".

## Verification

- Per step: `cargo check` / `cargo clippy -p bombay --lib` as listed.
  After 4/5: `cargo check -p bombay --tests --examples`.
- Gate (I drive, unsandboxed, FOREGROUND after `git add`): `nix flake check`,
  and confirm doctests execute — `cargo test --doc -p bombay` runs ≥1
  `compile_fail` (the O2 `Ctx::cap`), `-p bombay_macros` runs baseline+ the
  duplicate-field E0119 doctest.

## Out of scope

- Run-loop changes; the `Shell` collapse (native caps loop) — later stage.
- The tower-`Layer`/selector loop — stage-3 loop-selection design note only.
- `Watching`/`Supervising` (#280), `Deadlined`/`Phased` (#281), error
  consolidation (#282). Cross-cap composition test (stashing + watching) lands
  at stage 3.
- `clippy.toml`/`[lints]`, hakari, workflows.
