# Injectable `on_stop` grace — kill the `cfg!(miri)` production fork (card #257)

**Status:** design approved 2026-07-30. Implementation delegated to kimi (repo rule:
Claude plans/reviews, kimi writes code).

## Problem

`ON_STOP_NOTICE_GRACE` (`crates/core/src/actor/spawn.rs:57`) is a hardcoded `5 s`
in production, forked to `10 min` under `cfg!(miri)`. Two defects:

1. **Untestable production branching.** No test can assert which arm of a
   `cfg!(miri)` `const` compiles — the MIRI arm is dead weight to every non-MIRI
   build and invisible to the test suite.
2. **The bound is mandatory and un-tunable.** Every actor gets the same 5 s
   `on_stop` grace; an actor with a legitimately slower cleanup cannot raise it,
   and a test cannot lower it to observe the bound quickly.

The `cfg!(miri)` arm was meant as a stopgap; `spawn.rs:66-67` says the fix is "an
injectable grace on the config surface #196 is growing". #196 closed without
carrying that deferral forward (anti-pattern #166). This card is that deferral.

## Design

### 1. `SpawnConfig` — the spawn-path config surface

A new public struct groups the mailbox capacity and the `on_stop` grace:

```rust
/// Per-spawn configuration for a `PreparedActor`.
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    /// Mailbox capacity (bounded).
    pub capacity: Capacity,
    /// How long a dying actor's `on_stop` may run before its death notices go
    /// out anyway (card #196). See `finish_actor` for the crash-only semantics.
    pub on_stop_grace: Duration,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        Self {
            capacity: default_capacity(),          // 64, the current DEFAULT_MAILBOX_CAPACITY
            on_stop_grace: DEFAULT_ON_STOP_NOTICE_GRACE, // Duration::from_secs(5) — NO cfg!(miri)
        }
    }
}
```

Ergonomics for the common test case use struct-update:
`SpawnConfig { capacity: cap(4), ..Default::default() }`. No builder methods
(YAGNI — add at the second concrete need).

The `ON_STOP_NOTICE_GRACE` `const` is **renamed** to `DEFAULT_ON_STOP_NOTICE_GRACE`
and loses its `cfg!(miri)` arm entirely:

```rust
/// The default `on_stop` grace (card #196), used by `SpawnConfig::default`.
/// Per-actor overridable via `SpawnConfig::on_stop_grace`.
pub(super) const DEFAULT_ON_STOP_NOTICE_GRACE: Duration = Duration::from_secs(5);
```

Production now contains **no** `cfg!(miri)` branch on the grace (grep-guard bullet).

### 2. `PreparedActor` carries the grace

`PreparedActor<A>` gains an `on_stop_grace: Duration` field. Its two constructors
take a `SpawnConfig` instead of a bare `Capacity`:

```rust
impl<A: Actor> PreparedActor<A> {
    pub fn new(config: SpawnConfig) -> Self { /* uses config.capacity, stores config.on_stop_grace */ }
}
impl<A: Watch> PreparedActor<A> {
    pub fn new_linked(config: SpawnConfig) -> (Self, LinkReceiver) { /* same */ }
}
```

### 3. Threading the grace to the two teardown call sites

The stored grace flows to both bounded-teardown sites; the `const` is no longer
read directly by either:

- **`spawn.rs` `finish_actor`** (`:486`, `:498`) — gains an `on_stop_grace: Duration`
  parameter, used in `tokio::time::timeout(on_stop_grace, stop_fut)` and the
  `trace::on_stop_abandoned(&reason, on_stop_grace)` call. `run_lifecycle`,
  `run_lifecycle_linked`, and `run_lifecycle_supervised` thread the field through.
- **`kind.rs` `teardown_children`** (`:588`, `:594`, `:608`) — gains an
  `on_stop_grace: Duration` parameter for the post-abort confirmation sleep and
  its two `trace::child_teardown_abandoned` calls. `run_lifecycle_supervised`
  passes the supervisor's grace. (This is the supervisor's *own* post-abort
  confirmation bound; each child's cancel→abort grace remains the child's own
  `stop_grace`, unchanged.)

`kind.rs` drops its `use crate::actor::spawn::ON_STOP_NOTICE_GRACE;` import.

### 4. Public ergonomic spawn surface (`actor/mod.rs`)

The three `_with_capacity` methods migrate to `_with_config`, and the zero-arg
forms use `SpawnConfig::default()`:

| before | after |
|---|---|
| `spawn(args)` | `spawn(args)` → `spawn_with_config(SpawnConfig::default(), args)` |
| `spawn_with_capacity(cap, args)` | `spawn_with_config(config, args)` |
| `spawn_linked_with_capacity(cap, args)` | `spawn_linked_with_config(config, args)` |
| `spawn_supervised_with_capacity(cap, args)` | `spawn_supervised_with_config(config, args)` |

This is a public-API change → README "public API at a glance" and the usage
example update (CLAUDE.md item 4, the main case).

### 5. MIRI reconciliation (card bullet 3)

Key finding: the two abandonment tests that exercise the grace bound —
`death_notice_within_grace_of_hanging_on_stop` and
`supervised_kill_during_on_stop_is_bounded` — run under
`#[tokio::test(start_paused = true)]`. tokio auto-advances the paused virtual
clock to the next timer once every task is idle, so the bound fires
deterministically in ~zero real/interpreted time **regardless of its magnitude**.
These are MIRI-safe with the plain 5 s default. The old blanket `cfg!(miri)`
production fork was therefore broader than the tests actually required.

Plan: production default stays plain 5 s. **Run the MIRI lane after migration.**
If any lib test spuriously abandons a legitimately-progressing `on_stop` under a
*non-paused* MIRI runtime, that specific test injects a large grace via its
`SpawnConfig.on_stop_grace`, sourced from a dedicated test-support MIRI-scaled
value:

```rust
// test_support.rs — only added if the MIRI lane proves it is needed.
/// A generous `on_stop` grace for tests that run a non-trivial `on_stop` under a
/// non-paused runtime, MIRI-scaled for the interpreter's virtual clock.
/// Kept DISTINCT from `terminate_bound` (a harness deadline) so a hook grace and
/// a harness deadline never fire on the same instant — see `terminate_bound`.
pub const fn on_stop_grace() -> Duration {
    if cfg!(miri) { Duration::from_mins(10) } else { Duration::from_secs(5) }
}
```

`terminate_bound` stays a separate test-support value: it is the *harness*
fail-fast deadline (3× grace), not a *hook* grace. Its existing doc comment
already explains why identical magnitudes are wrong. Bullet 3 resolves as
"documents why it stays separate" — and, if MIRI proves the injection
unnecessary, `on_stop_grace()` is not added at all and the PR body says so.

### 6. Tests (TDD — failing first)

1. **Config honored** — `spawn_with_explicit_grace_observes_that_bound`
   (`spawn.rs` unit test, `#[tokio::test(start_paused = true)]`): spawn a
   hanging-`on_stop` actor with a *small* custom grace
   (`SpawnConfig { on_stop_grace: SMALL, ..Default::default() }`), assert the death
   notice arrives at `SMALL` with `cleanup_failed = true`. FAILS if the config is
   ignored and the 5 s default is used (the outer `SMALL + ε` bound expires with no
   notice). This is the core new invariant — asserts the *explicit* grace, not the
   default.
2. **No production MIRI fork** — a `#[test]` (or the existing
   `mutants-baseline`/source-guard mechanism) asserting `spawn.rs` source has no
   `cfg!(miri)` associated with the grace const. Grep-level per the card.
3. **Regression** — the two existing abandonment tests migrate to `SpawnConfig`
   and keep asserting `DEFAULT_ON_STOP_NOTICE_GRACE`-relative bounds unchanged.

### 7. Job-queue walking-skeleton wiring (card bullet 4)

Extend `crates/core/examples/job_queue/` + `crates/core/tests/app_job_queue.rs`:
spawn one actor through `SpawnConfig` with a non-default `on_stop_grace` and assert
it still drains cleanly. If the app has no actor whose `on_stop` is observable
enough to make this meaningful, defer with an explicit reason in the PR body
(per the walking-skeleton rule).

## Call-site migration (mechanical)

~93 `PreparedActor::new*` / `spawn_*_with_capacity` call sites across
`spawn.rs`, `mod.rs`, `timer.rs`, and `tests/{invariants,control_lane,tracing_capture,dst_races,app_job_queue}.rs`
rewrite from `new(cap)` → `new(SpawnConfig { capacity: cap, ..Default::default() })`
and `_with_capacity(cap, args)` → `_with_config(SpawnConfig { capacity: cap, ..Default::default() }, args)`.
Behavior-preserving: every migrated site keeps the 5 s default grace.

## Non-goals

- No per-message or dynamic (post-spawn) grace changes — grace is fixed at spawn.
- No change to a child's per-child `stop_grace` (cancel→abort), which is distinct
  from this inside-out `on_stop` notice bound (`spawn.rs:55` distinction stands).
- No `#[non_exhaustive]` on `SpawnConfig` (repo phase rule: exhaustive matching).

## Gate

`nix flake check` (single gate) + the MIRI lane (`miri.yml`) must be green. Verify
the MIRI lane explicitly — it is the empirical input to §5.
