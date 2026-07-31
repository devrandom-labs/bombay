# 271 — `handle` returns `Result<Flow, Error>`; drop `stop: &mut bool`

## Context

ADR-0023 (`docs/adr/0023-handler-stop-return-value-not-out-param.md` — READ IT
FIRST) decided: `Actor::handle` and `StashActor::handle` drop the
`stop: &mut bool` out-param and return `Result<Flow, Self::Error>`. This plan
is the whole migration. Invariants that must hold at the end:

- `Ok(Flow::Stop)` ≡ today's `*stop = true`: the actor stops with
  `ActorStopReason::Normal` **after the current handler**, before any further
  mailbox poll; backlog abandoned; `on_stop` runs.
- `Err(e)` stays a controlled crash (routed to `on_panic`, then stop) — `?`
  inside handlers keeps working unchanged.
- `Flow` is fieldless, exhaustive (NO `#[non_exhaustive]` — house rule), and
  carries NO reason payload (ADR-0023 Decision, last bullet — do not "improve"
  this).
- `Stashed<S>` replay short-circuits on `Ok(Flow::Stop)` exactly where it
  breaks on `*stop` today: a replayed handler's Stop/Err routes identically to
  a delivered message's.
- Semantics of `Signal::Stop`, `ActorRef::stop`, `kill` are UNTOUCHED.
- No test's assertion weakens; fixtures migrate mechanically
  (`_: &mut bool` param removed, `Ok(())` → `Ok(Flow::Continue)`,
  `*stop = true; Ok(())` → `Ok(Flow::Stop)`).
- Clippy config untouched; no new `#[allow]`; all `use` imports at file top.

## Steps

1. **`crates/core/src/actor/mod.rs` — `Flow` + trait + in-file fixtures.**
   SEQUENTIAL (everything depends on this).
   - Add above the `Actor` trait:
     ```rust
     /// The handler's continuation decision: keep running, or stop cleanly
     /// (reason `Normal`) after the current message.
     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
     pub enum Flow {
         /// Keep the actor running; poll the mailbox for the next message.
         Continue,
         /// Stop after this handler: reason `Normal`, backlog abandoned,
         /// `on_stop` runs. Deliberately carries no reason — a self-stop's
         /// only honest reason is `Normal` (ADR-0023).
         Stop,
     }
     ```
   - `Actor::handle` (currently `mod.rs:78-83`): drop the `stop: &mut bool`
     param; output becomes `Result<Flow, Self::Error>`.
   - Rewrite the `handle` doc (currently `mod.rs:75-77`) around the return
     protocol: `Ok(Flow::Continue)` keeps running; `Ok(Flow::Stop)` stops
     cleanly (`Normal`) after this handler; `Err` = controlled crash. Note the
     three outcomes are one exhaustive value — signalling stop *and* crash is
     unrepresentable (this replaces the old "after this handler returns `Ok`"
     prose).
   - Migrate the two in-file test fixtures (`mod.rs:292-299` and the
     `actor_boilerplate!` macro at `mod.rs:365-372`).
   - Verify: `cargo check -p bombay` (lib + this file's unit tests compile via
     `cargo check -p bombay --profile test` is NOT needed separately —
     all-targets comes in step 10).

2. **`crates/core/src/actor/kind.rs:1067-1093` — `handle_message`.**
   SEQUENTIAL — depends on step 1.
   - Delete `let mut stop = false;` and the `&mut stop` arg.
   - Map: `Ok(Ok(Flow::Continue)) => ControlFlow::Continue(())`,
     `Ok(Ok(Flow::Stop)) => ControlFlow::Break(ActorStopReason::Normal)`;
     `Ok(Err(_))` / `Err(_)` arms unchanged.
   - Update the one doc mention of "handler-set `stop = true`" at
     `kind.rs:214` to name `Flow::Stop`.
   - `use` the `Flow` item at file top (`use super::Flow;` or via the existing
     `super::` import group).

3. **`crates/core/src/stash.rs` — `StashActor` + `Stashed<S>` replay.**
   SEQUENTIAL — depends on step 1 (same-file trait + wrapper + fixtures).
   - `StashActor::handle` (`stash.rs:149-155`): drop `stop` param, return
     `Result<Flow, Self::Error>`; fix its doc line (`:146-148`).
   - `Stashed<S>::handle` (`stash.rs:219-240`) becomes:
     ```rust
     async fn handle(
         &mut self,
         msg: S::Msg,
         actor_ref: ActorRef<Self>,
     ) -> Result<Flow, S::Error> {
         if S::handle(&mut self.state, msg, actor_ref.clone(), &mut self.stash).await?
             == Flow::Stop
         {
             return Ok(Flow::Stop);
         }
         while let Some(m) = self.stash.pop_ready() {
             if S::handle(&mut self.state, m, actor_ref.clone(), &mut self.stash).await?
                 == Flow::Stop
             {
                 return Ok(Flow::Stop);
             }
         }
         Ok(Flow::Continue)
     }
     ```
     Keep the spec-D3 doc comment (`:213-218`), updating the "`stop`" word to
     `Flow::Stop`.
   - Migrate this file's own test fixtures.

4. **`crates/core/src/actor/spawn.rs` — 41 fixture sites.**
   PARALLEL OK after steps 1-3 (disjoint from 5-9). Mechanical: remove the
   param; `*stop = true` sites (e.g. `:1243`, `:1658`, `:1727`, `:2437`,
   `:4233`) become `Ok(Flow::Stop)` returns — CAREFUL at `:4233`
   (`Cmd::StopNormally => *stop = true` inside a match: restructure so the
   arm yields `Flow::Stop` and the handler returns it; other arms yield
   `Flow::Continue`). Doc comment at `:1632` updates. sonic-suitable EXCEPT
   the `:4229` region (judgment: match restructure).
   Verify: `cargo check -p bombay --all-targets` compiles this file's tests.

5. **Remaining `crates/core/src` fixture files.** PARALLEL OK (disjoint):
   `request.rs` (3), `registry.rs` (2), `timer.rs` (2), `recipient.rs` (2),
   `pipe.rs` (3), `actor_ref.rs` (1). All `_: &mut bool` noise — pure
   mechanical, sonic-suitable.

6. **`crates/core/tests/` — 55 sites, 13 files.** PARALLEL OK (disjoint from
   src). Mechanical, sonic-suitable. Files: `invariants.rs` (12),
   `tracing_capture.rs` (11), `dst_races.rs` (11), `drain_equivalence.rs` (8),
   `control_lane.rs` (3), `stash.rs` (3),
   `drain_supervision_equivalence.rs` (2), `app_job_queue.rs` (1), the four
   `alloc_*.rs` (1 each). `dst_races.rs:614-648` has a `SelfStop`-style
   fixture: its `*stop = true` becomes `Ok(Flow::Stop)`. Assertions must NOT
   change — if any test asserts on the flag itself rather than observable
   stop behavior, STOP and report as blocked (none is known to).

7. **`crates/core/examples/job_queue/app.rs` — the walking skeleton.**
   PARALLEL OK. The one *designed* (non-fixture) migration:
   - `finish_drain_if_quiet(&mut self, stop: &mut bool)` (`app.rs:474-491`)
     becomes `fn finish_drain_if_quiet(&mut self) -> Flow` (returns
     `Flow::Stop` where it wrote `*stop = true`, else `Flow::Continue`).
   - In `handle` (`app.rs:335-402`): the match yields the arm's `Flow`
     (`Submit`/`Retry`/`Stats` arms yield `Flow::Continue`; `Done`,
     `WorkerReplaced`, `Drain` arms yield `self.finish_drain_if_quiet()`),
     and the handler ends `Ok(flow)`. Keep the doc at `:471-473` accurate.

8. **`crates/core/benches/` — 9 sites, 5 files.** PARALLEL OK. Mechanical:
   `supervision_vs_kameo.rs` (5), `timers.rs` (1), `request_vs_kameo.rs` (1),
   `watcher_fanout.rs` (1), `registry_vs_kameo.rs` (1). sonic-suitable.

9. **`fuzz/` workspace — 4 sites.** PARALLEL OK. `fuzz/tests/actor_loop.rs`
   (3), `fuzz/tests/registry.rs` (1). SEPARATE workspace: root `cargo check`
   does NOT cover it. Verify with
   `cargo check --manifest-path fuzz/Cargo.toml --tests`; if that errors on
   the sandbox (network/offline), report the compile check as deferred — the
   flake gate (`bombay-fuzz-replay`) covers it. Do NOT touch `fuzz/Cargo.lock`
   (no dep change).

10. **`README.md` — public API doc.** SEQUENTIAL after 1 (content depends on
    final names only). Two touches, nothing else:
    - The usage example (`README.md:42-46`): new signature, ends
      `Ok(Flow::Continue)`.
    - The **Actor** bullet (`README.md:80`): mention `handle` returns
      `Flow` (`Continue`/`Stop`) — one clause, no new section.

11. **`mutants-baseline.json` reconciliation.** SEQUENTIAL — last.
    Run `cargo mutants --list` (list only — NEVER `cargo mutants` proper, it
    runs tests) and diff mutant paths for the changed fns (`handle_message`,
    `Stashed::handle`, any `Flow`-matching arms) against the baseline's two
    path-keyed sections (floors map + zero-viable list). Add/adjust entries
    for genuinely new paths; do not demote existing floors. If a judgment
    call arises (Collapse vs Unaccounted), report it in findings instead of
    guessing.

## Verification

Sandbox rule: **NO test execution** (`cargo test`/`nextest` hang in this
sandbox). Allowed:

- `cargo check -p bombay --all-targets` — must pass clean.
- `cargo clippy -p bombay --all-targets -- -D warnings` — the flake gates lib
  scope, but nothing you add may warn anywhere.
- `cargo check --manifest-path fuzz/Cargo.toml --tests` (best-effort, see
  step 9).
- `cargo fmt` before finishing.

Tests + the full gate (`nix flake check`) run controller-side after review.

## Out of scope

- `Signal`, `ControlSignal`, `ActorRef::stop`/`kill`, cancel-token plumbing,
  the run-loop select shapes — untouched.
- No new tests, no deleted assertions, no reason payload on `Flow`, no
  `#[non_exhaustive]`, no clippy/config changes, no `docs/adr/*` edits, no
  `fuzz/Cargo.lock` churn, no README restructure beyond step 10.
- `on_link_died` / `Watch` / supervision semantics — untouched.
