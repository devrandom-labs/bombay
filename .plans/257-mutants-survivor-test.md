# Kill the surviving mutant `trace.rs::imp::child_teardown_abandoned`

## Context

The mutants gate is RED on `main` and every recent PR (verified: scheduled
`mutants` runs failed 2026-07-29 and 2026-07-30; green 2026-07-28). It is not a
required check, so auto-merge let the drift through. Three defects; **two are
already fixed by Claude** in `mutants-baseline.json` on this branch
(`feat/257-injectable-on-stop-grace`):

- `poll_mailbox` (Collapse: 0 viable / 4 total, all unviable) → demoted
  `floors → known_zero_viable`. DONE.
- `<impl Drop for PendingAbort>::drop` (Unaccounted: 1 viable, caught) →
  registered in `floors: 1`. DONE.

**This plan covers the third — the only one needing real code:** the mutant
`crates/core/src/trace.rs:172` `replace imp::child_teardown_abandoned with ()`
is **MISSED**. `child_teardown_abandoned` is floored at 1 in the baseline
(added by #245, `3daf74a`) but **no test anywhere catches it** — a vacuous
floor. `rg teardown_abandoned crates/core/tests` returns nothing. This is the
exact "shipped the wiring, dropped the invariant" trap in CLAUDE.md item #3.
The fix is to write the missing test. **Do NOT demote it to
`known_zero_viable`** — it is a real, catchable invariant; demoting to go green
would re-commit the #149 sin.

### Invariants that MUST hold

1. **The test must actually kill the mutant.** `child_teardown_abandoned`'s
   ONLY observable effect is a `tracing::error!` event
   (`crates/core/src/trace.rs:171-173`, message
   `"child teardown notice missing after abort; abandoning"`, fields
   `child.id`, `grace`). A test that does not **capture tracing** passes on both
   the real body and the `()` mutant — vacuous. The test MUST install a tracing
   subscriber and assert the event fired. Claude will falsify this by re-running
   the mutant (below); a test that cannot fail is a rejected deliverable.

2. **Feature gating.** The mutant exists only under `feature = "tracing"`
   (default-on; `crates/core/Cargo.toml:12` `default = ["tracing"]`). Under
   `not(tracing)`, `child_teardown_abandoned` is already an empty
   `const fn` (`trace.rs:256`) — no viable mutant. The test MUST be
   `#[cfg(feature = "tracing")]`.

3. **In-crate only.** `teardown_children` is `pub(super)`
   (`crates/core/src/actor/kind.rs:529`) — unreachable from integration tests.
   The test MUST live in `crates/core/src/actor/kind.rs`'s existing
   `#[cfg(test)] mod tests` block (where `teardown_children_cancels_and_aborts_live_ones`
   at line 1639 already lives).

4. **Determinism — no flake.** Use `#[tokio::test(start_paused = true)]`. Do NOT
   spin/`yield_now` waiting for the clock (see the paused-clock-spin hazard: a
   yield-spin under a paused clock pins the timer and it never fires). Let the
   runtime auto-advance virtual time while the sweep parks on `recv_async`.

### The trigger (deterministic)

`teardown_children` (`kind.rs:529-619`) reaches the abandonment trace at
`kind.rs:595-599` (timeout branch) when a child it aborted never confirms its
death within `on_stop_grace`. Drive it with a **silent** child — a live
`ChildHandle` whose backing task NEVER sends a `LinkDied` — and keep the
`link_tx` alive so the channel never closes:

1. join phase (`kind.rs:557-584`): the child's `stop_grace` = `Duration::ZERO`,
   so `grace_futs` fires at once → child is aborted → moved to `aborted`;
   `pending` empties → loop exits.
2. post-abort (`kind.rs:590-618`): `aborted` is non-empty; `recv_async` parks
   forever (silent child, `link_tx` held). Virtual time auto-advances past
   `sleep(on_stop_grace)` → timeout branch (`595-599`) →
   `trace::child_teardown_abandoned(id, on_stop_grace)` → break.

Contrast the existing `mock_child` (`kind.rs:1178`) which spawns a task that
DOES send `LinkDied` on abort — that path removes the child from `aborted`
before the deadline, so it must NOT be used here. The existing `handle(id)`
helper (`kind.rs:1165`) already builds exactly the silent handle needed (abort
pair + fresh `CancellationToken`, no task).

## Steps

1. **[SEQUENTIAL — single file]** In `crates/core/src/actor/kind.rs`, inside
   `#[cfg(test)] mod tests`, add a minimal in-module tracing capture (the
   integration `mod capture` in `tests/tracing_capture.rs` is not reachable from
   a unit test). Keep it small: a `tracing_subscriber::Layer` (dev-dep,
   `Cargo.toml:62`) that records, per event, `level` + the `message` field +
   the `child.id` + `grace` fields into an `Arc<Mutex<Vec<..>>>`. Provide an
   `install()` returning `(store, guard)` where `guard` is the
   `tracing::subscriber::DefaultGuard` from `set_default(...)` (mirror
   `capture::install` in `tests/tracing_capture.rs`). Gate helper + test
   `#[cfg(feature = "tracing")]`. If you prefer, instead lift the existing
   `capture` module into `src/test_support.rs` behind the `test-support`
   feature and reuse it — either is acceptable; the minimal local capture is
   simpler and lower-risk. Do not add new non-dev dependencies.

2. **[SEQUENTIAL — same file, depends on step 1]** Add the test:

   ```rust
   /// A child that is aborted at supervisor exit but never confirms its death
   /// (a non-yielding child produces no notice) must not wedge the sweep: after
   /// `on_stop_grace` the supervisor traces the missing notice and proceeds.
   /// Kills the `child_teardown_abandoned` -> () mutant (trace.rs:172).
   #[cfg(feature = "tracing")]
   #[tokio::test(start_paused = true)]
   async fn teardown_traces_abandonment_when_a_child_never_confirms_death() { ... }
   ```

   - `let (link_tx, link_rx) = flume::unbounded();` and **bind `link_tx` for the
     whole test** (do not drop it) so the channel stays open.
   - Build `Children::new()` with ONE live child: reuse the `child(config,
     started)` helper (`kind.rs:1211`) with
     `RestartConfig::new(RestartPolicy::Permanent).with_stop_grace(Duration::ZERO)`,
     then set `entry.handle = Some(handle(child_id))` (silent handle,
     `kind.rs:1165`). Insert under `child_id = ActorId::from_raw_for_test(1)`.
   - `let (store, _guard) = install();` BEFORE the await (current-thread runtime
     under `start_paused`, so the thread-local subscriber captures the sweep's
     events).
   - `teardown_children(&mut children, &link_rx, on_stop_grace).await;` with
     `on_stop_grace = Duration::from_secs(1)` (or reuse
     `DEFAULT_ON_STOP_NOTICE_GRACE` if non-zero).
   - Assertions (exact, not `contains` on a bag):
     - exactly ONE captured event whose message ==
       `"child teardown notice missing after abort; abandoning"`;
     - that event's level == `"ERROR"`;
     - its `child.id` field == `format!("{child_id:?}")`;
     - its `grace` field is present (non-empty);
     - `children.ids().count() == 0` (the sweep still empties the table).

## Verification

Kimi runs ONLY (tests hang under the inherited sandbox — do NOT run
`cargo test`/`nextest`):

```bash
cargo check  -p bombay --all-features
cargo clippy -p bombay --all-targets --all-features
```

Both must be clean (no warnings — the lint config is deny-level). Report the
tail of each. **Claude** then drives, unsandboxed:

- `nix flake check` (runs the test suite + clippy + fmt gate).
- Falsifiability / mutant-kill proof: `nix develop -c cargo mutants -p bombay
  --file crates/core/src/trace.rs --function child_teardown_abandoned` (or
  temporarily stub the body to `{}`) and confirm the new test now appears as
  **caught**, not missed. A test that stays green when the body is `()` is
  vacuous and gets sent back.

## Out of scope

- Do NOT edit `mutants-baseline.json` (Claude already curated it — the
  `child_teardown_abandoned: 1` floor stays; this test satisfies it).
- Do NOT touch `crates/core/src/trace.rs` or `teardown_children`'s production
  logic. This is a test-only change.
- Do NOT demote `child_teardown_abandoned` to `known_zero_viable`.
- Do NOT relax any lint level, `#[allow]`, or `clippy.toml`.
- No README / job-queue-app change: this is a coverage/robustness fix with no
  public-API surface (CLAUDE.md item #4 "no API change, no coverage-file move"
  case — the walking-skeleton bullet, item #7, is N/A here; stated explicitly
  so it is not silently trimmed).
```
