# Injectable `on_stop` Grace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: implement task-by-task, TDD, commit per task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the actor `on_stop` grace configurable per spawn via a `SpawnConfig`, deleting the `cfg!(miri)` fork from the production grace `const` (card #257).

**Architecture:** A new `SpawnConfig { capacity, on_stop_grace }` (Default = 64 / 5 s, no MIRI branch) is stored on `PreparedActor` and threaded to the two bounded-teardown sites (`finish_actor` in `spawn.rs`, `teardown_children` in `kind.rs`). The `ON_STOP_NOTICE_GRACE` const is renamed `DEFAULT_ON_STOP_NOTICE_GRACE`, loses `cfg!(miri)`, and is read only by `SpawnConfig::default`. MIRI safety is preserved because the abandonment tests use `start_paused` (magnitude-independent); a test-support scaled grace is added only if the MIRI lane proves it needed.

**Tech Stack:** Rust (edition 2024), tokio, futures `Abortable`, `nix flake check` gate, `miri.yml` lane.

**Reference spec:** `docs/superpowers/specs/2026-07-30-injectable-on-stop-grace-design.md`

---

## File Structure

- `crates/core/src/actor/spawn.rs` — `SpawnConfig` type, `DEFAULT_ON_STOP_NOTICE_GRACE`, `PreparedActor` grace field + constructor signatures, `finish_actor` grace param, `run_lifecycle*` threading, new unit test.
- `crates/core/src/actor/kind.rs` — `teardown_children` grace param; drop the `ON_STOP_NOTICE_GRACE` import.
- `crates/core/src/actor/mod.rs` — public `Spawn`/`SpawnLinked`/`SpawnSupervised` `_with_capacity` → `_with_config`; re-export `SpawnConfig`.
- `crates/core/src/test_support.rs` — (conditional) `on_stop_grace()` MIRI-scaled helper.
- `crates/core/examples/job_queue/*`, `crates/core/tests/app_job_queue.rs` — walking-skeleton wiring.
- Call-site migration: `timer.rs`, `tests/{invariants,control_lane,tracing_capture,dst_races}.rs`.
- `README.md` — public-API bullet + example.

---

## Task 1: `SpawnConfig` + rename const + store grace (behavior-preserving migration)

Big atomic signature change: introduces the type, renames the const (no `cfg!(miri)`), stores the grace on `PreparedActor` **without yet reading it in teardown**, and migrates every call site so the crate compiles and the existing suite stays green at the 5 s default. The grace field is deliberately unused by `finish_actor`/`teardown_children` in this task — Task 2 wires it in, TDD-style.

**Files:**
- Modify: `crates/core/src/actor/spawn.rs` (const ~57-71, `PreparedActor` ~122-148 + 201-221, `default_capacity` stays)
- Modify: `crates/core/src/actor/mod.rs` (ergonomic traits ~173-264, re-exports ~30)
- Modify: `crates/core/src/actor/timer.rs`, `crates/core/tests/{invariants,control_lane,tracing_capture,dst_races,app_job_queue}.rs`

- [ ] **Step 1: Replace the forked const with a plain default**

In `spawn.rs`, replace the entire `ON_STOP_NOTICE_GRACE` const (lines 45-71) with:

```rust
/// The default grace a dying actor's `on_stop` gets before its death notices go
/// out anyway (card #196). Per-actor overridable via
/// [`SpawnConfig::on_stop_grace`]; read only by [`SpawnConfig::default`].
///
/// The notices must carry `on_stop`'s outcome (`LinkDied::cleanup_failed`), so the
/// hook runs *first* — but a watcher must never be stranded behind a user hook
/// that never returns. Past this bound the hook's future is dropped and the death
/// is announced with `cleanup_failed = true`. OTP's shape: a child's `terminate/2`
/// runs before exit signals propagate, bounded by the child spec's `shutdown`.
///
/// Distinct from a supervisor's `stop_grace`, which bounds cancel→abort from the
/// *outside*; this bounds notice delay from the *inside*.
pub(super) const DEFAULT_ON_STOP_NOTICE_GRACE: Duration = Duration::from_secs(5);
```

- [ ] **Step 2: Add the `SpawnConfig` type**

In `spawn.rs`, after `DEFAULT_MAILBOX_CAPACITY`/`default_capacity` and the new const, add:

```rust
/// Per-spawn configuration for a [`PreparedActor`]: the bounded mailbox capacity
/// and the [`on_stop`](Actor::on_stop) notice grace (card #257).
///
/// Build the common case with struct-update over [`SpawnConfig::default`]:
/// `SpawnConfig { capacity, ..Default::default() }`.
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    /// The bounded mailbox capacity.
    pub capacity: Capacity,
    /// How long `on_stop` may run before the death notices go out anyway; see
    /// [`DEFAULT_ON_STOP_NOTICE_GRACE`].
    pub on_stop_grace: Duration,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        Self {
            capacity: default_capacity(),
            on_stop_grace: DEFAULT_ON_STOP_NOTICE_GRACE,
        }
    }
}
```

- [ ] **Step 3: Store the grace on `PreparedActor` and take a `SpawnConfig`**

Add the field to the struct (~122):

```rust
pub struct PreparedActor<A: Actor> {
    actor_ref: ActorRef<A>,
    mailbox_rx: MailboxReceiver<A>,
    abort_registration: AbortRegistration,
    on_stop_grace: Duration,
}
```

Change `new` (~138) to take a `SpawnConfig`:

```rust
    pub fn new(config: SpawnConfig) -> Self {
        let id = next_actor_id();
        let (mailbox_tx, mailbox_rx) = Mailbox::<A>::bounded(config.capacity, id);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(id, mailbox_tx, CancellationToken::new(), abort_handle, None);
        Self {
            actor_ref,
            mailbox_rx,
            abort_registration,
            on_stop_grace: config.on_stop_grace,
        }
    }
```

Change `new_linked` (~201) identically — take `config: SpawnConfig`, use `config.capacity` in `Mailbox::bounded`, set `on_stop_grace: config.on_stop_grace` in the returned `Self`.

- [ ] **Step 4: Migrate the ergonomic spawn surface in `mod.rs`**

Re-export `SpawnConfig` (add to the `spawn::{...}` use at ~30):

```rust
    spawn::{DEFAULT_MAILBOX_CAPACITY, PreparedActor, RunResult, SpawnConfig},
```

Rewrite the three `_with_capacity` methods to `_with_config` and the zero-arg forms to use `SpawnConfig::default()`. `Spawn` (~176):

```rust
    fn spawn(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_with_config(SpawnConfig::default(), args)
    }

    fn spawn_with_config(config: SpawnConfig, args: Self::Args) -> ActorRef<Self> {
        let prepared = PreparedActor::<Self>::new(config);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn(args);
        actor_ref
    }
```

`SpawnLinked` (~202):

```rust
    fn spawn_linked(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_linked_with_config(SpawnConfig::default(), args)
    }

    fn spawn_linked_with_config(config: SpawnConfig, args: Self::Args) -> ActorRef<Self> {
        let (prepared, link_rx) = PreparedActor::<Self>::new_linked(config);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn_linked_task(args, link_rx);
        actor_ref
    }
```

`SpawnSupervised` (~250):

```rust
    fn spawn_supervised(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_supervised_with_config(SpawnConfig::default(), args)
    }

    fn spawn_supervised_with_config(config: SpawnConfig, args: Self::Args) -> ActorRef<Self> {
        let (prepared, link_rx) = PreparedActor::<Self>::new_linked(config);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn_supervised_task(args, link_rx);
        actor_ref
    }
```

Remove the now-unused `default_capacity` import from `mod.rs` if the compiler flags it (it is now only used inside `spawn.rs`). Keep `DEFAULT_MAILBOX_CAPACITY` if still referenced in docs.

- [ ] **Step 5: Migrate every remaining call site mechanically**

Apply this transformation everywhere across `spawn.rs` (tests), `timer.rs`, and `tests/{invariants,control_lane,tracing_capture,dst_races,app_job_queue}.rs`:

- `PreparedActor::<T>::new(CAP)` → `PreparedActor::<T>::new(SpawnConfig { capacity: CAP, ..Default::default() })`
- `PreparedActor::<T>::new_linked(CAP)` → `PreparedActor::<T>::new_linked(SpawnConfig { capacity: CAP, ..Default::default() })`
- `T::spawn_with_capacity(CAP, args)` → `T::spawn_with_config(SpawnConfig { capacity: CAP, ..Default::default() }, args)`
- `T::spawn_linked_with_capacity(CAP, args)` → `T::spawn_linked_with_config(SpawnConfig { capacity: CAP, ..Default::default() }, args)`
- `T::spawn_supervised_with_capacity(CAP, args)` → `T::spawn_supervised_with_config(SpawnConfig { capacity: CAP, ..Default::default() }, args)`

Add `SpawnConfig` to each file's `bombay::{...}` / `crate::actor::{...}` / `super::{...}` import as needed. Find them with:

```bash
rg -n "new\(cap|new_linked\(|_with_capacity\(" crates/core/src crates/core/tests
```

- [ ] **Step 6: Verify the whole suite compiles and passes unchanged (safety net)**

Run: `nix develop --command cargo nextest run -p bombay`
Expected: PASS — behavior is unchanged, every actor still gets the 5 s default grace. This green run is the refactor's safety net (no new behavior yet).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(spawn): SpawnConfig surface + plain DEFAULT_ON_STOP_NOTICE_GRACE [#257]"
```

---

## Task 2: Thread the grace into teardown (TDD — honored-grace invariant)

**Files:**
- Test: `crates/core/src/actor/spawn.rs` (`tests` module — reuse `HangingStop` / `watch_before_start`)
- Modify: `crates/core/src/actor/spawn.rs` (`finish_actor` ~470-517, `run_lifecycle*` ~523-778, `PreparedActor::run*` destructures)
- Modify: `crates/core/src/actor/kind.rs` (`teardown_children` ~530-616)

- [ ] **Step 1: Write the failing test**

Add to the `spawn.rs` `tests` module (near `death_notice_within_grace_of_hanging_on_stop`). It spawns a hanging-`on_stop` actor with a **small** custom grace and asserts the notice arrives at that grace — not the 5 s default:

```rust
/// `@bug` Config (card #257): an explicit `SpawnConfig.on_stop_grace` is the bound
/// a hanging `on_stop` is abandoned at — NOT the 5 s default. FAILS while teardown
/// ignores the field and reads the default: the outer `SMALL + ε` bound then
/// expires with no notice, because the default 5 s grace has not elapsed.
#[tokio::test(start_paused = true)]
async fn spawn_with_explicit_grace_observes_that_bound() {
    const SMALL: Duration = Duration::from_millis(50);
    let prepared = PreparedActor::<HangingStop>::new(SpawnConfig {
        capacity: cap(4),
        on_stop_grace: SMALL,
    });
    let link_rx = watch_before_start(&prepared);
    let actor_ref = prepared.actor_ref().clone();
    let (entered_tx, _entered_rx) = tokio::sync::oneshot::channel();

    let _join = prepared.spawn(entered_tx);
    actor_ref.stop();
    drop(actor_ref);

    // Bound just past the SMALL grace but far below the 5 s default: only a teardown
    // that honors the config lets the notice arrive inside this window.
    let notice = tokio::time::timeout(SMALL + Duration::from_millis(10), link_rx.recv_async())
        .await
        .expect("the notice must arrive at the explicit grace, not the 5 s default")
        .expect("the link channel is still open");
    assert!(
        notice.cleanup_failed,
        "an abandoned on_stop counts as a failed cleanup",
    );
    assert!(
        notice.reason.is_normal(),
        "the recorded stop reason survives the abandoned hook, got {:?}",
        notice.reason,
    );
}
```

Ensure `SpawnConfig` is in the test module's `use super::{...}` (add alongside `DEFAULT_MAILBOX_CAPACITY`). Confirm `HangingStop` and `watch_before_start` exist in this module (they are used by `death_notice_within_grace_of_hanging_on_stop`); if `HangingStop` lives in a nested `mod`, hoist the test next to it.

- [ ] **Step 2: Run the test to verify it FAILS**

Run: `nix develop --command cargo nextest run -p bombay spawn_with_explicit_grace_observes_that_bound`
Expected: FAIL — `finish_actor` still uses `DEFAULT_ON_STOP_NOTICE_GRACE`, so the notice is scheduled at 5 s (virtual) and the `SMALL + 10ms` `timeout` elapses first: the `.expect("the notice must arrive...")` panics.

- [ ] **Step 3: Give `finish_actor` a grace parameter**

Change the signature (~470) and the two use sites (~486, ~498):

```rust
async fn finish_actor<A: Actor>(
    mut state: A,
    weak: WeakActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
    mut watchers: Watchers,
    reason: ActorStopReason,
    on_stop_grace: Duration,
) -> RunResult<A> {
```

Inside, replace `ON_STOP_NOTICE_GRACE` with `on_stop_grace`:

```rust
    match tokio::time::timeout(on_stop_grace, stop_fut).await {
        Ok(stop_result) => { /* unchanged */ }
        Err(_elapsed) => {
            trace::on_stop_abandoned(&reason, on_stop_grace);
        }
    }
```

Update the doc comment references to `[`ON_STOP_NOTICE_GRACE`]` → the parameter / `DEFAULT_ON_STOP_NOTICE_GRACE`.

- [ ] **Step 4: Thread the grace through the lifecycles**

`run_lifecycle` (~523) and `run_lifecycle_linked` (~548) each gain `on_stop_grace: Duration` and pass it to `finish_actor(..., reason, on_stop_grace)`. `run_lifecycle_supervised` (~646) gains `on_stop_grace: Duration`, passes it to the graceful `finish_actor(...)` call (~759) **and** to the three `teardown_children(...)` calls (~758, ~770, ~775).

`PreparedActor::run` (~166), `run_linked` (~229), and `run_supervised` (~273) destructure `self` — add `on_stop_grace` to each destructure and pass it into the corresponding `run_lifecycle*` call. Example for `run`:

```rust
        let Self {
            actor_ref,
            mailbox_rx,
            abort_registration,
            on_stop_grace,
        } = self;
        // ...
            let lifecycle = run_lifecycle(args, actor_ref, mailbox_rx, on_stop_grace);
```

- [ ] **Step 5: Give `teardown_children` a grace parameter**

In `kind.rs`, change the signature (~530) and the three `ON_STOP_NOTICE_GRACE` uses (~588, ~594, ~608):

```rust
pub(super) async fn teardown_children(
    children: &mut Children,
    link_rx: &LinkReceiver,
    on_stop_grace: Duration,
) {
```

```rust
        let mut bound = std::pin::pin!(sleep(on_stop_grace));
        // ...
                    trace::child_teardown_abandoned(*id, on_stop_grace); // both call sites
```

Remove `use crate::actor::spawn::ON_STOP_NOTICE_GRACE;` (~17). Add `use core::time::Duration;` if not already imported in `kind.rs`. Update the doc comment `[`ON_STOP_NOTICE_GRACE`]` references (~526, ~585) to name the parameter.

- [ ] **Step 6: Run the test to verify it PASSES**

Run: `nix develop --command cargo nextest run -p bombay spawn_with_explicit_grace_observes_that_bound`
Expected: PASS — the notice now arrives at `SMALL` (virtual).

- [ ] **Step 7: Run the full suite (no regressions)**

Run: `nix develop --command cargo nextest run -p bombay`
Expected: PASS — the migrated abandonment tests still hold at the 5 s default.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(spawn): injectable on_stop grace honored by teardown [#257]"
```

---

## Task 3: Source guard — no `cfg!(miri)` on the grace

**Files:**
- Test: `crates/core/src/actor/spawn.rs` (`tests` module)

- [ ] **Step 1: Write the guard test**

A regression guard asserting the production source no longer forks the grace on MIRI (card bullet 2, grep-level). Reads its own source at compile time:

```rust
/// Guard (card #257): the production grace carries NO `cfg!(miri)` fork — the
/// stopgap the config surface replaced. FAILS if the fork is reintroduced.
#[test]
fn grace_const_has_no_miri_fork() {
    let src = include_str!("spawn.rs");
    let idx = src
        .find("DEFAULT_ON_STOP_NOTICE_GRACE: Duration")
        .expect("the default grace const is defined here");
    // The const's definition line must resolve to a plain literal, not a cfg fork.
    let line_end = src[idx..].find(';').expect("const has a terminating ;");
    let def = &src[idx..idx + line_end];
    assert!(
        !def.contains("cfg!(miri)"),
        "the production on_stop grace must not fork on MIRI; inject via SpawnConfig instead:\n{def}",
    );
}
```

- [ ] **Step 2: Run it — verify PASS**

Run: `nix develop --command cargo nextest run -p bombay grace_const_has_no_miri_fork`
Expected: PASS (Task 1 already removed the fork; this locks it out).

Note: this guard cannot be "fail-first" here because the fork was removed in Task 1 as part of the compile-restoring migration. Its value is regression protection. If a reviewer wants to see it red, temporarily re-add `if cfg!(miri) { ... }` to the const and re-run.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(spawn): guard against reintroducing the cfg!(miri) grace fork [#257]"
```

---

## Task 4: MIRI lane reconciliation (empirical — card bullet 3)

**Files:**
- (conditional) Modify: `crates/core/src/test_support.rs`
- (conditional) Modify: whichever test the MIRI lane flags

- [ ] **Step 1: Run the MIRI lane locally**

Run the MIRI leg the way `miri.yml` does (check the workflow for the exact invocation; it runs the lib tests, skipping `prop_*`):

```bash
rg -n "cargo.*miri" .github/workflows/miri.yml
# then run that command, e.g.:
# nix develop --command cargo +nightly miri nextest run -p bombay -E 'not test(/prop_/)'
```

Expected: identify any test that spuriously reports `cleanup_failed` / abandons a legitimately-progressing `on_stop` under the non-paused virtual clock now that the default is a real 5 s.

- [ ] **Step 2a: If the lane is GREEN — document and stop**

No test needs an injected grace: the abandonment tests use `start_paused` and are magnitude-independent. Record in the PR body: "MIRI lane green with the plain 5 s default; no test-support grace injected. `terminate_bound` stays a separate harness deadline (card bullet 3 = documented separation)." Skip to Task 5.

- [ ] **Step 2b: If a test is RED — add the scaled helper and inject it**

Add to `test_support.rs`:

```rust
/// A generous `on_stop` grace for a test whose `on_stop` does non-trivial work
/// under a NON-paused runtime, MIRI-scaled for the interpreter's 5 µs/basic-block
/// virtual clock (card #257). Kept DISTINCT from [`terminate_bound`] (a harness
/// deadline) so a hook grace and a harness deadline never fire on the same instant
/// — see `terminate_bound`.
#[must_use]
pub const fn on_stop_grace() -> Duration {
    if cfg!(miri) {
        Duration::from_mins(10)
    } else {
        Duration::from_secs(5)
    }
}
```

In the flagged test, spawn through `SpawnConfig { on_stop_grace: test_support::on_stop_grace(), ..Default::default() }`. Re-run the MIRI lane to green. Record which test and why in the PR body.

- [ ] **Step 3: Commit (only if Step 2b ran)**

```bash
git add -A
git commit -m "test-support(spawn): MIRI-scaled on_stop_grace for non-paused teardown tests [#257]"
```

---

## Task 5: Job-queue walking-skeleton wiring (card bullet 4)

**Files:**
- Modify: `crates/core/examples/job_queue/` (spawn one actor with a custom grace)
- Modify: `crates/core/tests/app_job_queue.rs`

- [ ] **Step 1: Inspect the app for an actor with an observable `on_stop`**

Run: `rg -n "spawn|on_stop|SpawnConfig|PreparedActor" crates/core/examples/job_queue crates/core/tests/app_job_queue.rs`
Decide whether one actor's shutdown is observable enough to make a custom grace meaningful.

- [ ] **Step 2a: If yes — wire it**

Spawn that actor via `SpawnConfig { capacity, on_stop_grace: <non-default>, ..Default::default() }` and extend `app_job_queue.rs` to assert it still drains cleanly under the custom grace (reuse `terminate_bound()` for the harness deadline). Keep the existing app assertions intact.

- [ ] **Step 2b: If no — defer with a reason**

Do not force it. Record in the PR body: "Job-queue wiring deferred — no app actor has an `on_stop` observable enough for a grace assertion; filed as a follow-up note." (The card bullet permits an explicit, reasoned defer.)

- [ ] **Step 3: Run the app integration test**

Run: `nix develop --command cargo nextest run -p bombay --test app_job_queue`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "example(job_queue): spawn with a custom on_stop grace [#257]"
```

---

## Task 6: README + final gate

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the public-API surface**

In the "public API at a glance" section, replace the `spawn_with_capacity` family with the `SpawnConfig` / `_with_config` surface, and update any usage example that constructed a `PreparedActor::new(capacity)` to `PreparedActor::new(SpawnConfig { capacity, ..Default::default() })`. Add one line noting `on_stop_grace` is now per-spawn tunable.

- [ ] **Step 2: Run the single gate**

Run: `nix flake check`
Expected: PASS (build + clippy + fmt + tests). Fix any clippy/fmt fallout (run `cargo fmt` first).

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs(readme): SpawnConfig + per-spawn on_stop grace [#257]"
```

---

## Self-Review Notes

- **Spec coverage:** §1 SpawnConfig → Task 1; §2 PreparedActor field → Task 1; §3 threading → Task 2; §4 ergonomic surface → Task 1 Step 4; §5 MIRI → Task 4; §6 tests → Tasks 2/3; §7 job-queue → Task 5; README → Task 6. All covered.
- **TDD honesty:** the only genuinely-new behavior (honored grace) is fail-first in Task 2. The migration (Task 1) and guard (Task 3) are refactor/regression and rely on the existing green suite + a documented manual falsifiability check.
- **Type consistency:** `SpawnConfig`, `on_stop_grace`, `DEFAULT_ON_STOP_NOTICE_GRACE`, `_with_config` used identically across tasks.
- **Engineering rules:** no `saturating_*`/bare arithmetic added; `SpawnConfig` not `#[non_exhaustive]` (phase rule); no new `cfg!(miri)` in production; grace is a plain `Duration` field, no `Option`/sentinel.
