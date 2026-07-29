# 253 — `Collected` stop reason: ref-count death is LeaveDead under every policy

## Context

Spec: `docs/superpowers/specs/2026-07-29-253-collected-stop-design.md` (read it first).
Card: devrandom-labs/bombay#253. Branch: `feat/253-collected-stop` (already checked out).

Bug being fixed: all-senders-gone (ref-count) stop is collapsed into
`ActorStopReason::Normal` by the run-loops, and `should_restart(Permanent, Normal) =
Restart` — so a supervised child nobody anchors rebuild-churns: with default budgets
`consecutive` climbs (uptime ≈ 0 never earns `reset_after`), trips `max_restarts = 5`
in ~3 s, and the supervisor kills itself (`RestartLimitExceeded`) plus every sibling
(ADR-0019 teardown). Unbudgeted configs churn forever.

Decision (Joel, on the card): ref-count death gets its own reason,
`ActorStopReason::Collected`, classified normal (`is_normal() == true`), and
`should_restart` returns `LeaveDead` for it under **every** policy. Collection is not
failure (no OTP precedent exists — OTP has no ref-count death; actor-GC literature
treats collection of an unreachable actor as invisible).

Invariants that must hold:

- Explicit graceful stops stay `Normal`: cancel-token stop (`stop()`), in-band
  `Signal::Stop`, handler-set `stop = true`. ONLY the all-senders-gone path becomes
  `Collected`.
- ADR-0003 drain-then-stop is untouched: queued messages still self-pin and are
  handled before the collected stop.
- `AlreadyDead` stays abnormal and restart-worthy (no change).
- No `Default` for `RestartPolicy`/`RestartConfig` appears anywhere (#196 decision).
- Engineering rules: exhaustive matches (no `non_exhaustive`, no `_ =>` catch-alls on
  `ActorStopReason`), thiserror, no lint relaxation, every `#[expect]` has a reason.
- `ActorStopReason` is a public enum with exhaustive matching downstream — the
  compiler will list every match site that must gain a `Collected` arm. Extend each
  one deliberately (they are compile tripwires by design); never silence with `_`.

## Steps

All steps are SEQUENTIAL — they touch overlapping files (`error.rs`, `kind.rs`,
`restart.rs`) and each later step compiles only after the earlier ones.

### Step 1 — `error.rs`: the `Collected` variant

File: `crates/core/src/error.rs`.

Add to `ActorStopReason` (place it right after `Normal`):

```rust
/// Every strong handle was dropped and the queue drained — no message can
/// ever arrive again, so the actor collected itself (ref-count-driven stop,
/// ADR-0003). Collection is not failure: it is classified normal, links do
/// not propagate it, and a supervisor leaves a collected child dead under
/// every policy (ADR-0020) — restarting an actor nobody can reach would
/// make garbage collection observable.
#[error("collected: every strong ref was dropped")]
Collected,
```

Update `is_normal`:

```rust
matches!(self, Self::Normal | Self::SupervisorRestart | Self::Collected)
```

Extend `error.rs` unit tests: `Collected.is_normal()` is true; a Display assert
`ActorStopReason::Collected.to_string() == "collected: every strong ref was dropped"`
next to the existing `Normal` Display test (error.rs:776). If the test module has an
exhaustive-match variant tripwire, extend it.

Expected outcome: `cargo check -p bombay --all-targets` FAILS listing every
non-exhaustive match on `ActorStopReason` — that list is the worklist for Step 2/5.

### Step 2 — extend every exhaustive match (compile back to green)

Fix each site the compiler names. Known ones:

- `crates/core/src/restart.rs` tests: `all_reasons()` becomes `[ActorStopReason; 9]`
  (add `ActorStopReason::Collected`) and its tripwire `match` gains the arm.
- Pre-flight CHECK found no other crate-level exhaustive `match` (others use
  `matches!`); fix anything else the compiler flags.

### Step 2b — the exact-`Normal` sweep (enumerated by prewalk)

Sites where a **ref-count (drop-last-ref) stop** asserts exact `Normal` and must
flip to `ActorStopReason::Collected`:

- `crates/core/src/actor/spawn.rs` ~line 954
  (`ref-count stop is a clean normal stop` assert in the drop-last-ref test): flip
  the `RunResult::Stopped { reason: ActorStopReason::Normal, .. }` matcher to
  `Collected`, and update the assert message.
- `crates/core/src/actor/spawn.rs` ~line 997
  (`queued_message_is_handled_even_if_last_ref_drops_first`): same flip — this test
  IS the spec's drain-then-stop invariant (message handled, then collected), so
  extend its doc comment with one line instead of writing a duplicate Test D.
- `fuzz/tests/actor_loop.rs` — the oracle is a RUNTIME assertion and goes stale
  (pre-flight CHECK finding). Two tests:
  - `actor_loop_state_machine` (~line 289): the non-kill branch currently expects
    `Normal` always. New oracle:
    `Kill` present → `RunResult::Killed`; else `StopInBand` or `CancelStop`
    present → `Stopped { reason: Normal, .. }` (a queued `Signal::Stop` is still
    drained before the mailbox reports closed, and a cancel fires out-of-band);
    else (pure drop-refs fall-through) → `Stopped { reason: Collected, .. }`.
  - `stop_reason_preserved_through_panicking_on_stop` (~line 353): same
    `Normal`-vs-`Collected` split on `StopInBand|CancelStop` presence (Kill already
    filtered out).
  Update each test's doc-comment oracle description to match. Touch nothing else in
  `fuzz/`.

Sites that KEEP exact `Normal` (their stop comes from `Signal::Stop` / `stop()` /
constructed `LinkDied` notices, not ref-count): `crates/core/tests/invariants.rs`
(all sites send `Stop`), `crates/core/tests/dst_races.rs`, `watch.rs` tests,
`actor/mod.rs:327`. Verify each site you touch actually ends via drop-last-ref
before flipping it.

While in `restart.rs`, adjust the decision-table tests to the NEW semantics (this is
the TDD red→green pair for Step 3 — semantically these asserts fail against the old
table, the compiler just forces them to land together):

- `permanent_restarts_on_every_reason` → rename to
  `permanent_restarts_on_every_reason_except_collected`; iterate `all_reasons()`,
  assert `Restart` for every reason EXCEPT `Collected`, and assert `LeaveDead` for
  `Collected` explicitly.
- `transient_splits_every_reason_between_dead_and_restart`: add
  `ActorStopReason::Collected` to the `leave_dead` list (the variant-coverage assert
  at the bottom keeps the split exhaustive).
- `never_always_leaves_dead`: unchanged (Collected now included via `all_reasons`).
- NEW test pinning the policy-independence:

```rust
/// #253/ADR-0020: ref-count collection is not a failure — no policy rebuilds
/// a collected child, exactly as no policy restarts past a lifecycle panic.
#[test]
fn collected_leaves_dead_under_every_policy() {
    for policy in [
        RestartPolicy::Permanent,
        RestartPolicy::Transient,
        RestartPolicy::Never,
    ] {
        assert_eq!(
            should_restart(policy, &ActorStopReason::Collected),
            RestartVerdict::LeaveDead,
            "{policy:?}",
        );
    }
}
```

### Step 3 — `restart.rs`: the `should_restart` arm

File: `crates/core/src/restart.rs`, `should_restart` (line ~81).

Add after the lifecycle-hook carve-out, before the policy match:

```rust
// Ref-count collection is not a failure: nobody can reach the actor again,
// so no policy rebuilds it (#253, ADR-0020).
if matches!(reason, ActorStopReason::Collected) {
    return RestartVerdict::LeaveDead;
}
```

Update the fn doc comment: it currently says only the lifecycle-hook panic
short-circuits every policy — now `Collected` does too (one sentence, citing
ADR-0020). Also extend the `RestartPolicy::Permanent` variant doc ("Rebuild on every
exit…") with: being collected is the caller dropping the actor, not the actor
exiting — `Permanent` does not resurrect it.

### Step 4 — `kind.rs`: split the collapsed stop (`MailboxPoll`)

File: `crates/core/src/actor/kind.rs`.

Add a crate-private enum + one polling helper (a helper fn taking
`Option<Option<_>>` would trip the denied `clippy::option_option`, so the mapping
lives inside the helper that does the awaiting):

```rust
/// One poll of the mailbox arm with the two stop causes kept distinct — the
/// split #253 needs: cancellation is a graceful stop (`Normal`), a closed
/// mailbox is ref-count collection (`Collected`, ADR-0020). Flattening them
/// into one `None` is exactly the bug that made collection restart-worthy.
pub(super) enum MailboxPoll<A: Mailboxed> {
    /// The cancel token fired: an out-of-band graceful stop.
    Cancelled,
    /// The mailbox closed: every strong sender is gone and the queue is
    /// drained (ADR-0003 drain-then-stop already happened).
    Closed,
    /// An ordinary signal.
    Signal(Signal<A>),
}

/// Awaits the next mailbox event under the cancel token and names which of
/// the three outcomes happened. The one place the
/// `run_until_cancelled(recv())` nesting is interpreted.
async fn poll_mailbox<A: Mailboxed>(
    cancel: &CancellationToken,
    mailbox_rx: &mut MailboxReceiver<A>,
) -> MailboxPoll<A> {
    match cancel.run_until_cancelled(mailbox_rx.recv()).await {
        None => MailboxPoll::Cancelled,
        Some(None) => MailboxPoll::Closed,
        Some(Some(signal)) => MailboxPoll::Signal(signal),
    }
}
```

Change `handle_mailbox_step` to take `poll: MailboxPoll<A>` instead of
`signal: Option<Signal<A>>`:

```rust
let next = match poll {
    MailboxPoll::Cancelled => return ControlFlow::Break(ActorStopReason::Normal),
    MailboxPoll::Closed => return ControlFlow::Break(ActorStopReason::Collected),
    MailboxPoll::Signal(next) => next,
};
```

(keep the existing `match next { … }` body unchanged; update the fn doc: the two
stop causes are now distinct, `Signal::Stop` and handler-set stop stay `Normal`).

Three call sites move to the helper:

- `run_message_loop`: `let poll = poll_mailbox(&handles.cancel, mailbox_rx).await;`
  then `handle_mailbox_step(state, self_ref, handles, watchers, poll)`.
- `run_linked_message_loop`: the mailbox select arm becomes
  `poll = poll_mailbox(&handles.cancel, mailbox_rx) => { … handle_mailbox_step(…, poll) … }`.
- `run_supervised_message_loop`: the mailbox arm becomes

```rust
poll = poll_mailbox(&handles.cancel, mailbox_rx) => {
    match poll {
        MailboxPoll::Signal(Signal::Supervision(op)) => apply_supervision_op(
            children,
            &supervisor,
            &mut SetCycleCtx::new(retries, pending_aborts, cycle),
            *op,
        ),
        other => {
            if let ControlFlow::Break(reason) =
                handle_mailbox_step(state, self_ref, handles, watchers, other).await
            {
                return reason;
            }
        }
    }
}
```

No other behavior change in the loops.

### Step 5 — `kind.rs`: trace the quiet death

`handle_child_death`'s `LeaveDead` arm becomes:

```rust
RestartVerdict::LeaveDead => {
    // #253/ADR-0020: a collected child is left dead SILENTLY by policy —
    // the trace event is the only witness (the #244 observability concern).
    if matches!(notice.reason, ActorStopReason::Collected) {
        trace::child_collected(notice.id);
    }
    ControlFlow::Continue(())
}
```

File `crates/core/src/trace.rs`: add to the tracing-ON `imp`:

```rust
pub fn child_collected(child: ActorId) {
    tracing::debug!(child.id = ?child, "supervised child collected (all refs dropped); left dead");
}
```

and the matching no-op to the tracing-OFF half (generated by the
`inert_trace_surface!` macro — extend the macro body the same way the other
`ActorId`-taking no-ops are listed). Three spots total: tracing-on `imp`, the
inert macro body, and the module's `pub use`/re-export list if fns are exported
individually — check and cover all that exist.

### Step 6 — integration tests: the probe, flipped to pinned behavior

File: `crates/core/src/actor/spawn.rs`, existing supervised test module (reuse its
local helpers — `supervise_worker` etc. near line 3808 — and its actor types; follow
the module's paused-clock style). Repo rules: every await bounded, exact asserts,
tests must be able to fail.

Test A — budgeted config (the card's reproduction, now asserting the fix):

```rust
/// #253/ADR-0020: an anchorless Permanent child ref-count-collects ONCE and is
/// left dead — no rebuild churn, no RestartLimitExceeded, the supervisor and
/// its siblings survive. On pre-#253 main this escalated within ~3s of virtual
/// time (consecutive trips max_restarts=5).
#[tokio::test(start_paused = true)]
async fn collected_permanent_child_is_left_dead_and_supervisor_survives() { … }
```

Shape: spawn a supervisor (`spawn_supervised`), `supervise` with
`RestartConfig::new(RestartPolicy::Permanent)` and a factory that increments an
`Arc<AtomicU32>` spawn counter and returns a fresh child WITHOUT storing any strong
ref anywhere. Advance the paused clock far past every backoff the old churn would
have used (e.g. `tokio::time::sleep(Duration::from_secs(300)).await`). Assert:

- spawn counter == 1 (exactly one incarnation ever);
- the supervisor is still alive (e.g. a bounded `ask`/`tell` round-trip succeeds, or
  `is_alive()` — match how sibling tests probe supervisor liveness);
- no `RestartLimitExceeded` was observed by a watcher on the supervisor (install a
  watch edge before the scenario; assert no death notice arrives within a bounded
  window).

Test B — unbudgeted config, same scenario, same asserts:

```rust
#[tokio::test(start_paused = true)]
async fn collected_child_does_not_churn_even_unbudgeted() { … }
```

with `RestartConfig::new(RestartPolicy::Permanent)
.with_max_restarts(u32::MAX).with_max_total(u32::MAX)` — pins the card's "budgeted
and unbudgeted configs both" bullet (pre-#253 this config was an infinite loop).

Test C — the split's boundary pair (put beside the existing death-notice tests that
assert `notice.reason.is_normal()`, ~line 2400-2900; follow their harness):

- explicit `stop()` (cancel path): watcher's notice is exactly
  `ActorStopReason::Normal` (`assert!(matches!(notice.reason, ActorStopReason::Normal))`);
- dropping the last strong ref: notice is exactly `ActorStopReason::Collected`.

Test D — drain-then-stop: covered by flipping
`queued_message_is_handled_even_if_last_ref_drops_first` (Step 2b); no new test.

Factory note (pre-flight CHECK): the existing `supervise_worker` helper's factory
ANCHORS workers (stores strong senders) — do not reuse it for Tests A/B. Call
`sup.supervise(cfg, factory)` directly with a factory that retains nothing.

Sibling assert (pre-flight CHECK): Tests A and B also supervise ONE anchored
sibling child (any policy, factory keeps a strong ref in an `Arc<Mutex<…>>`
anchor) and assert after the scenario that the sibling is still alive — pins
"the subtree survives", not just the supervisor.

Also sweep: any existing test that asserts an exact `ActorStopReason::Normal` for a
ref-count stop (drop-last-ref path) flips to `Collected`; asserts on `is_normal()`
stay untouched. Do NOT touch tests where `Normal` comes from `stop()`/`Signal::Stop`.

### Step 7 — tracing capture test

File: `crates/core/tests/tracing_capture.rs`. Follow the file's existing
capture-and-assert pattern (it already tests restart events with anchor
scaffolding): scenario = supervised Permanent child, factory anchors nothing, drop
refs, bounded wait; assert the `child_collected` event is captured with the child's
id, and that NO `restart_scheduled` event fires for that death.

### Step 8 — docs + baseline

1. NEW `docs/adr/0020-collected-stop-not-restart-worthy.md` — follow the ADR house
   style (`docs/adr/README.md` index gets a row too). Content from the spec:
   context (the two-invariant collision, the ~3 s subtree bomb with default tuning,
   infinite churn unbudgeted), verified facts (loop collapse of the two stop causes;
   `should_restart` table; registry #119 holds weak handles so `Permanent` never
   could keep an unreferenced named service alive), decision (`Collected`, normal,
   LeaveDead under every policy; the `MailboxPoll` split), consequences (unanchored
   Permanent child now dies quietly ONCE — stated honestly, with the
   `child_collected` trace event as the witness; anchoring stays the app's job), and
   the #196 note: the no-default-policy/budget decision SURVIVES — budgets now guard
   only genuine crash loops.
2. `crates/core/src/actor/actor_ref.rs` `supervise` doc block (lines ~326-361):
   rewrite the "**An unanchored child is actively fatal**" paragraph — an unanchored
   child now collects once and is left dead under every policy
   (`ActorStopReason::Collected`, ADR-0020); the supervisor keeps running. Keep the
   anchoring guidance (a dead-but-wanted child is still an app bug) and fix the
   error-section sentence that references the old behavior.
3. `README.md`: public API changed — the "public API at a glance" area gains/updates
   one bullet for `ActorStopReason::Collected` (ref-count stop is its own reason;
   supervisors never rebuild from it). Keep README lean (no card numbers in prose
   beyond the usual style).
4. `docs/testing/coverage-baseline.md`: add the new tests to the relevant section.
5. `mutants-baseline.json` (pre-flight CHECK corrections applied):
   - `kind.rs::handle_mailbox_step` is currently in `known_zero_viable`, NOT
     `floors` — KEEP it there (do not invent a floor).
   - NEW `kind.rs::poll_mailbox`: add to `floors` with floor 1 (its arm-swap
     mutants are killed by the Step 6 boundary tests). Claude re-measures at the
     `nix build .#mutants` gate and adjusts if needed.
   - NEW trace `child_collected`: mirror how `child_escalated` is classified
     (check both sections; trace fns follow one pattern — copy it).
   - Touched fns already in `floors` (`error.rs::ActorStopReason::is_normal`,
     `kind.rs::handle_child_death`, `restart.rs::should_restart` if listed): leave
     floors as-is; new tests can only raise caught counts, never lower them.
   - A new fn missing from both sections fails the gate as Unaccounted.

## Verification

Sandbox rule: NEVER run tests (`cargo test`/`cargo nextest`) — they hang in this
sandbox. Compile-and-lint only; the test suite runs later in the unsandboxed
`nix flake check` driven by Claude.

After every step:

- `cargo check -p bombay --all-targets` — must pass (Step 1 is expected RED until
  Step 2 lands; land 1+2 together before checking).
- `cargo clippy -p bombay` — zero warnings, no new `#[allow]`.
- `cargo fmt` before finishing (the fmt gate is strict).

Final: re-read the spec's Tests section and confirm every listed invariant has a
test with an exact assert.

## Out of scope

- NO commits — Claude reviews and commits.
- No change to `AlreadyDead`, set-cycles (#199), teardown (ADR-0019), `Children`,
  backoff math, budgets, or any `Default`.
- No new public API beyond the enum variant; `MailboxPoll`/`poll_mailbox` stay
  `pub(super)`/private.
- No job-queue app/example changes (walking-skeleton bullet is deferred on the card
  with reasoning — the deterministic in-app scenario doesn't exist yet, wart #3).
- No clippy config, lint level, or `clippy.toml` changes.
- In `fuzz/`, touch ONLY the two oracle assertions named in Step 2b (they are
  runtime-stale, not compile-broken); nothing else.
