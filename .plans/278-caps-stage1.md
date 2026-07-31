# 278 — stage 1: caps machinery (one-trait Actor, Ctx/CapSet/Provide, derive, one spawn)

## Context

ADR-0026 stage 1, gate ALREADY PASSED (ADR-0026 Addendum: encoding =
derive-on-named-struct, per-field `Provide<C>` impls; spike on branch
`spikes/277`, `spikes/spike-278/`). Design fixed by:
- `docs/adr/0026-core-distillation-one-trait-caps.md` (+ Addendum) — the
  five constraints are LAW, esp. constraint 1: NO runtime-checked
  capability accessor may exist anywhere.
- `docs/superpowers/specs/2026-07-31-277-core-distillation-design.md`
  (surface sketch + invariant mapping).

Stage-1 strategy: the NEW surface runs ON the existing runtime via an
internal adapter — `Shell<A>` implements the shipped `Actor`
(`crates/core/src/actor/mod.rs:68`) and `Mailboxed`; the run loop is NOT
touched in this stage. Old surface stays fully working and warning-free.

Invariants that must hold:
- Constraint 1: no `try_get`/`Option`-returning cap accessor exists.
- The #114 slot tripwire is preserved (`Msg` bound unchanged;
  `message.rs:18`).
- Poisoning/drain-window semantics unchanged (they live in the untouched
  loop; `Shell` only forwards).
- NO `#[deprecated]` attributes on the old surface — the clippy gate is
  deny-warnings and existing code/examples still use it. "Deprecated in
  place" = doc-comments pointing at `caps`, nothing more, this stage.
- House rules: RPITIT (`fn -> impl Future + Send`), thiserror, checked
  arithmetic in count paths, all `use` at top, every `#[allow]` reasoned.
- SANDBOX: NEVER run `cargo test`/`nextest`/miri/mutants. Per-step
  verification = `cargo check -p bombay -p bombay_macros` +
  `cargo clippy -p bombay --lib` ONLY. Tests are AUTHORED, not run;
  the controller runs `nix flake check`.

## Steps

**S1. `caps` module — the surface + adapter.** SEQUENTIAL (root).
Files: NEW `crates/core/src/caps.rs` (single file this stage; split only
if clippy's 80-line/fn or cognitive-complexity limits force helpers out);
`crates/core/src/lib.rs` (add `pub mod caps;` beside `pub mod stash;`).
Implement (names final per spec):

- `pub trait Actor: Sized + Send + 'static` with
  `type Msg: crate::message::Msg`, `type Args: Send`,
  `type Error: crate::error::ReplyError`, `type Caps: CapSet<Self>`;
  required `fn init(args: Self::Args, cx: Ctx<'_, Self>) -> impl Future<Output = Result<Self, Self::Error>> + Send`
  and `fn handle(&mut self, msg: Self::Msg, cx: Ctx<'_, Self>) -> impl Future<Output = Result<crate::actor::Flow, Self::Error>> + Send`;
  defaulted `name()` (type_name), `on_stop`, `on_panic` — signatures and
  doc-semantics copied from `actor/mod.rs:76,101,131` (WeakActorRef
  params reference `Shell<Self>`; see Handle note below).
  NOTE: no `Mailboxed` impl is asked of the user — the merge falls out
  of the adapter (Shell implements it). This is the "menu declarations
  merge" of ADR-0026 for stage 1; the derive half is S2.
- `pub trait CapSet<A: Actor>: Send + 'static { fn build(args: &A::Args) -> Self; }`
  + `impl<A: Actor> CapSet<A> for ()`.
- `pub trait Provide<C> { fn provide(&mut self) -> &mut C; }` — THE open
  seam (Addendum). No other accessor path may exist (constraint 1).
- `pub struct Ctx<'a, A: Actor> { /* pub(crate): caps: &'a mut A::Caps, self_ref: &'a Handle<A> */ }`
  with `pub fn cap<C>(&mut self) -> &mut C where A::Caps: Provide<C>`,
  `pub fn self_ref(&self) -> &Handle<A>`. NOTHING else public.
- `pub struct Shell<A: caps::Actor>` (fields `pub(crate)`: `user: A`,
  `caps: A::Caps`) + `pub type Handle<A> = crate::actor::ActorRef<Shell<A>>`
  (alias documented as the stage-1 seam; collapses when the loop drives
  `caps::Actor` natively in a later stage).
  `impl Mailboxed for Shell<A> { type Msg = A::Msg; }`;
  `impl crate::actor::Actor for Shell<A>`: `on_start` = `A::Caps::build(&args)`
  then `A::init(args, Ctx{..})` (Ctx borrows the freshly built caps and
  the actor_ref param); `handle` = `A::handle(&mut self.user, msg, Ctx{..})`;
  `name`/`on_panic`/`on_stop` forward to the `caps::Actor` items.
- `pub async fn spawn<A: caps::Actor>(args: A::Args) -> Handle<A>` and
  `spawn_with(config: crate::actor::SpawnConfig, args) -> Handle<A>` —
  thin over the existing blanket `Spawn` (`actor/mod.rs:196,203`) on
  `Shell<A>`. (They are sync fns today upstream; mirror the existing
  signatures — do NOT invent async spawn if `Spawn::spawn` is sync.)
Expected: compiles clippy-clean; zero changes inside `actor/`, `stash/`,
`mailbox.rs`.

**S2. `#[derive(CapSet)]` in bombay_macros.** PARALLEL OK with S3/S4
(different crate).
Files: NEW `crates/macros/src/derive_capset.rs`; `crates/macros/src/lib.rs`
(register, following `derive_msg.rs`'s structure: `mod` + re-export +
doc with a `compile_fail` doctest).
- On a named struct: emit `impl<A> bombay::caps::CapSet<A> for TheStruct`
  — WRONG shape: the derive cannot know `A`. Instead emit exactly what
  the Addendum spike hand-wrote: the derive takes the actor type via
  attribute `#[capset(actor = MyActor, args = MyArgs)]`? NO — keep it
  simpler and fully general: derive emits (a) one
  `impl bombay::caps::Provide<FieldTy> for TheStruct` per field
  (E0119 rejects duplicate field types — the O3 feature), and (b) an
  inherent `fn build_from(parts: (FieldTy, ...)) -> Self`? NO — decision:
  the derive emits ONLY the `Provide` impls (the open seam, the part
  users must not hand-write); `CapSet::build` remains a hand-written
  impl this stage (it needs policy knowledge the derive cannot infer).
  Doc this split explicitly; a `build`-generating attribute is future
  work under #243. Keep the derive ~mirror of `derive_msg.rs` in size.
- Derive doc carries TWO doctests: one success (struct with two distinct
  cap fields, `cap::<C>()` works) and one `compile_fail` (duplicate
  field type → E0119).
Expected: `cargo check -p bombay_macros` + the doctests compile as
written (execution is the controller's gate; see S5 verification note).

**S3. Tests.** After S1; PARALLEL OK with S2/S4 (new files only).
Files: unit tests in `caps.rs` `#[cfg(test)]` + NEW
`crates/core/tests/caps_stage1.rs`.
- Port spike proofs in-repo: O1 (third-party cap module + user struct
  with hand-written Provide impls composes with a core-provided ZST demo
  cap — stage 1 ships no real caps, define a tiny `Counter`-style demo
  cap inside the test), O2 as a `compile_fail` DOCTEST on `Ctx::cap`
  (in `caps.rs` — doctests are the crate's, tests/ cannot host
  compile_fail), O4 policy-from-args.
- W1 walkthrough as an integration test: plain caps::Actor (1 trait
  impl + derive(Msg) only), `caps::spawn`, tell + ask round-trip
  (`ReplySender` in the menu, `tests/` idioms from existing files),
  ref-count stop still yields `Collected` semantics via the old
  machinery (spawn, drop handle, observe via watcher? keep simple:
  tell/ask + explicit `stop()` + is_alive assertions).
- Adapter forwarding: `on_stop` runs with the right reason on
  `stop()` (probe channel); panic in `handle` reaches `on_panic`
  (PanicReason::HandlerPanic — unchanged domain).
- Constraint-1 pin: a test-file comment + grep-style assertion is NOT a
  test; instead the `compile_fail` doctest IS the pin, and S5 verifies
  doctests actually execute in the gate (the #170 lesson).
Expected: `cargo check -p bombay --tests` compiles; NOT run.

**S4. Walking skeleton.** After S1; PARALLEL OK with S2/S3.
Files: `crates/core/examples/job_queue/app.rs`, `main.rs`,
`crates/core/tests/app_job_queue.rs`.
- Add a small plain `AuditLog` actor on the NEW surface
  (`caps::Actor`, `Caps = ()`): receives `Audited { job_id }` tells from
  the Dispatcher at submit time (Dispatcher holds a `caps::Handle<AuditLog>`
  or an erased `Recipient` — use `Recipient` if `Handle` type in
  Dispatcher fields reads poorly), answers an `ask` returning the audit
  count. Extend `app_job_queue.rs`: submissions produce matching audit
  entries (exact count assertion).
- Keep the diff minimal; do not migrate existing actors this stage.
Expected: example + app test compile.

**S5. Wiring.** SEQUENTIAL, last.
- `mutants-baseline.json`: entries for every new fn (`caps.rs`,
  `derive_capset.rs` helpers) per the file's established two-section
  shape (floors map + zero-viable list); non-Default returns →
  known_zero_viable.
- `README.md`: one new "capabilities (staged)" bullet + 8-line
  `caps::Actor` example; mark the caps surface as the ADR-0026 staged
  direction, old surface still supported.
- `docs/testing/coverage-baseline.md`: new test files, one line each.
- Doc-comment breadcrumbs ("superseded by `caps` per ADR-0026, staged")
  on `StashActor`, `Watch`, `Supervisor`, `Spawn*` trait docs — plain
  doc text ONLY, no `#[deprecated]`.
Expected final: `cargo check -p bombay -p bombay_macros` +
`cargo clippy -p bombay --lib` clean.

## Verification

- Per step: `cargo check -p bombay -p bombay_macros`;
  `cargo clippy -p bombay --lib`. After S3/S4:
  `cargo check -p bombay --tests --examples`.
- NEVER cargo test/nextest/miri/mutants (sandbox hangs) — controller
  runs `nix flake check` after `git add` (untracked files are invisible
  to it) AND verifies the compile_fail doctests EXECUTE
  (`cargo test --doc -p bombay 2>&1 | grep -c compile_fail` ≥ 2 — the
  #170 vacuous-tripwire lesson).
- Final line: `<<<KIMI-DONE: done|blocked>>>`.

## Out of scope

- Real capabilities (Stashing #279, Watching/Supervising #280,
  Deadlined/Phased #281) — stage 1 ships machinery + demo/test caps only.
- Run-loop changes, spawn-surface removal, `#[deprecated]` attributes,
  error-surface changes (#282), `clippy.toml`/`[lints]`, hakari.
- The `Shell` collapse (native caps loop) — a later stage.
