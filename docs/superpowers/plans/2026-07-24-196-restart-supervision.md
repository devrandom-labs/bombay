# Restart & Supervision (slice 2a) Implementation Plan — card #196

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `Supervisor` actor rebuilds dead children under an explicit per-child `RestartPolicy` with exponential backoff, two give-up counters, and escalation over the #195 death edge.

**Architecture:** Pure restart arithmetic lives in a new `restart.rs` (policy table, config, tracker — no async, mutants-friendly). The supervision runtime is a third loop variant in `kind.rs`/`spawn.rs` selecting over mailbox + link channel + a `DelayQueue` retry arm, with a loop-owned `Children` table beside `Watchers`. Two prerequisite fixes land first: startup failures must reach watchers as `Panicked(OnStart)` (not synthetic `AlreadyDead`), and teardown must run a bounded `on_stop` *before* death notices so `LinkDied` can carry `cleanup_failed`.

**Tech Stack:** flume (existing), tokio-util `CancellationToken` + **`DelayQueue`** (enable `time` feature), **`fastrand`** (new dep, seedable jitter), smallvec.

**Spec:** `docs/superpowers/specs/2026-07-24-196-restart-supervision-design.md` — invariant numbers below refer to its list of 21.

**Ground rules (from CLAUDE.md / memory — non-negotiable):**
- TDD: every step writes the failing test first and *watches it fail*.
- `nix flake check` only sees **tracked** files — `git add` new files before trusting it.
- New/renamed fns need `mutants-baseline.json` entries (else `Unaccounted`).
- Proptests must be named `prop_*` (MIRI sweep skips them by prefix).
- Test awaits are always bounded (`tokio::time::timeout`) — unbounded awaits turned #148/#179 mutants sweeps red with TIMEOUT, not survivors.
- No Claude attribution in commits. Conventional commits with scope.
- `cargo fmt` before every commit (fmt gate is strict).
- fuzz/ workspace has its own Cargo.lock — a new bombay-core dep breaks flake check until it is updated.

---

## Task 0: Branch + dependency wiring

**Files:**
- Modify: `Cargo.toml` (workspace deps)
- Modify: `bombay-core/Cargo.toml`
- Modify: `fuzz/Cargo.lock` (regenerate)

- [ ] **Step 1: Branch**

```bash
git checkout -b feat/196-restart-supervision
```

- [ ] **Step 2: Workspace deps** — in root `Cargo.toml` `[workspace.dependencies]`, extend the existing `tokio-util` line and add `fastrand` with a why-comment (house style):

```toml
# Cooperative cancellation for the actor run-loop (card #116, absorbs #55).
# CancellationToken::run_until_cancelled drives graceful stop without a select!.
# `time` (card #196): DelayQueue serves the supervisor's per-child restart
# deadlines — N items, each with a deadline, yield the next expired — replacing
# a hand-rolled min-scan we would otherwise have to mutation-test ourselves.
tokio-util = { version = "0.7", features = ["time"] }
# Restart-backoff jitter (card #196): zero-dep and SEEDABLE — DST tests seed the
# generator and assert exact jittered delays instead of disabling jitter and
# leaving the jitter path untested.
fastrand = "2"
```

- [ ] **Step 3:** In `bombay-core/Cargo.toml` `[dependencies]` add `fastrand = { workspace = true }` (tokio-util line already exists).

- [ ] **Step 4: Verify build + fuzz lockfile**

```bash
cargo build -p bombay-core
(cd fuzz && cargo update -p bombay-core 2>/dev/null || cargo generate-lockfile)
git add Cargo.toml Cargo.lock bombay-core/Cargo.toml fuzz/Cargo.lock
```

Expected: clean build. (Skip `cargo hakari` — not wired in bombay, memory `hakari-not-wired-in-bombay`.)

- [ ] **Step 5: Commit** — `chore(deps): tokio-util time feature + fastrand for #196 restart scheduling`

---

## Task 1: Startup failure carries its true reason (spec invariant 8 `startup_failure_carries_true_reason`, @bug)

Today `MailboxReceiver::drop` (`mailbox.rs:370`) answers queued `Signal::Watch` with synthetic `AlreadyDead` on *every* teardown, including startup failure — so `Panicked(OnStart)` evaporates and a supervisor would crash-loop an unstartable child. Fix: a `pub(crate)` drain-with-reason helper, called from both lifecycle startup-failure paths; `Drop` keeps `AlreadyDead` for the genuinely-unknowable case.

**Files:**
- Modify: `bombay-core/src/mailbox.rs` (helper + refactor `Drop` through it)
- Modify: `bombay-core/src/actor/spawn.rs` (`run_lifecycle`, `run_lifecycle_linked` startup-failure paths)
- Test: `bombay-core/src/actor/spawn.rs` tests module

- [ ] **Step 1: Write the failing test** (in `spawn.rs` `mod tests`; reuse the module's existing probe-actor pattern — an actor whose `on_start` returns `Err`):

```rust
/// @bug #196: a child whose `on_start` fails must deliver its TRUE reason
/// (`Panicked(OnStart)`) to already-queued watchers — not synthetic
/// `AlreadyDead`, which is restart-worthy and would crash-loop a supervisor
/// against a child that can never start.
#[tokio::test]
async fn startup_failure_answers_queued_watchers_with_on_start_reason() {
    let prepared = PreparedActor::<FailingStart>::new(cap(4));
    let (link_tx, link_rx) = flume::unbounded();
    prepared
        .actor_ref()
        .mailbox_sender()
        .try_send(Signal::Watch(Box::new(WatchReg {
            watcher: ActorId::new(1),
            link_tx,
            linked: false,
        })))
        .expect("fresh mailbox has capacity");

    let result = tokio::time::timeout(Duration::from_secs(5), prepared.run(()))
        .await
        .expect("startup failure is prompt");
    assert!(matches!(result, RunResult::StartupFailed(_)));

    let notice = link_rx.try_recv().expect("watcher must be notified");
    match notice.reason {
        ActorStopReason::Panicked(err) => {
            assert_eq!(err.reason(), PanicReason::OnStart);
        }
        other => panic!("expected Panicked(OnStart), got {other:?}"),
    }
}
```

`FailingStart` probe (add beside the module's existing probes if none fails on start):

```rust
struct FailingStart;
#[derive(Debug)]
struct NoMsg;
impl crate::message::Msg for NoMsg {}
impl Mailboxed for FailingStart {
    type Msg = NoMsg;
}
impl Actor for FailingStart {
    type Args = ();
    type Error = &'static str;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Err("refuses to start")
    }
    async fn handle(&mut self, _: NoMsg, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
        Ok(())
    }
}
```

(`&'static str` already implements `ReplyError` via the blanket — the panic-payload path in `error.rs` relies on it.)

- [ ] **Step 2: Run, verify it fails for the right reason**

```bash
cargo nextest run -p bombay-core startup_failure_answers_queued
```

Expected: FAIL — `expected Panicked(OnStart), got AlreadyDead`.

- [ ] **Step 3: Implement.** In `mailbox.rs`, extract the drain into a reason-taking helper and route `Drop` through it:

```rust
impl<A: Mailboxed> MailboxReceiver<A> {
    /// Drains the backlog, answering every still-queued [`Signal::Watch`] with a
    /// death notice carrying `reason` (card #196: the startup-failure path knows
    /// the TRUE reason — `Panicked(OnStart)`; `Drop` falls back to
    /// [`AlreadyDead`](ActorStopReason::AlreadyDead), where the reason is
    /// genuinely unknowable). Also releases the `self_sender` cycle (#195 duty 1).
    pub(crate) fn reject_queued_watchers(&mut self, reason: &ActorStopReason) {
        for signal in self.rx.drain() {
            if let Signal::Watch(reg) = signal {
                let _ = reg.link_tx.try_send(LinkDied {
                    id: self.me,
                    reason: reason.clone(),
                    linked: reg.linked,
                });
            }
        }
    }
}

impl<A: Mailboxed> Drop for MailboxReceiver<A> {
    fn drop(&mut self) {
        self.reject_queued_watchers(&ActorStopReason::AlreadyDead);
    }
}
```

(Keep the existing doc comment on `Drop`, amended: reason is `AlreadyDead` *here*; the startup path pre-empts with the true reason.)

In `spawn.rs`, both lifecycles' startup-failure paths call it before returning. `run_lifecycle`:

```rust
    } = match start_actor(args, actor_ref).await {
        Ok(started) => started,
        Err(failed) => {
            // #196: queued watchers get the TRUE startup reason, not the
            // Drop-path's synthetic AlreadyDead.
            if let RunResult::StartupFailed(err) = &failed {
                mailbox_rx
                    .reject_queued_watchers(&ActorStopReason::Panicked(err.clone()));
            }
            return failed;
        }
    };
```

Mirror the same block in `run_lifecycle_linked`.

- [ ] **Step 4: Run the test + the whole crate** (the #195 test `dropping_receiver_notifies_queued_watch_regs_already_dead` must still pass — the Drop path is unchanged behavior):

```bash
cargo nextest run -p bombay-core
```

Expected: all PASS.

- [ ] **Step 5: Commit** — `fix(spawn): startup failure answers queued watchers with Panicked(OnStart), not AlreadyDead (#196)`

---

## Task 2: `LinkDied.cleanup_failed` + `Watchers` plumbing

**Files:**
- Modify: `bombay-core/src/watch.rs`
- Test: `bombay-core/src/watch.rs` tests module

- [ ] **Step 1: Failing test** (extend the existing `watch.rs` tests):

```rust
#[test]
fn set_cleanup_failed_rides_every_notice() {
    let (tx, rx) = flume::unbounded();
    let mut guard = Watchers::new(ActorId::new(9));
    guard.apply(WatchReg { watcher: ActorId::new(1), link_tx: tx, linked: true });
    guard.set_reason(ActorStopReason::Normal);
    guard.set_cleanup_failed();
    drop(guard);

    let n = rx.try_recv().expect("notified");
    assert!(n.cleanup_failed, "cleanup_failed must ride the notice");
    assert!(n.reason.is_normal(), "original reason preserved — a failed cleanup is not a different death");
}
```

- [ ] **Step 2: Run** `cargo nextest run -p bombay-core set_cleanup_failed` — FAIL: no field/method.

- [ ] **Step 3: Implement.** `LinkDied` gains the field (update its doc: set when the dying actor's `on_stop` returned `Err` or exceeded the notice grace; `false` on the kill path where `on_stop` never runs):

```rust
pub struct LinkDied {
    pub id: ActorId,
    pub reason: ActorStopReason,
    pub linked: bool,
    /// `true` iff the dying actor's `on_stop` failed (returned `Err`, panicked,
    /// or exceeded the notice grace) — "it died AND did not clean up" (#196).
    pub cleanup_failed: bool,
}
```

`Watchers` gains `cleanup_failed: bool` (init `false`) + setter beside `set_reason`:

```rust
    /// Records that `on_stop` failed; stamped onto every outgoing notice.
    pub(crate) fn set_cleanup_failed(&mut self) {
        self.cleanup_failed = true;
    }
```

`Drop` stamps `cleanup_failed: self.cleanup_failed` into each notice.

- [ ] **Step 4: Fix all construction sites** — `LinkDied` is built in `watch.rs::Drop`, `mailbox.rs::reject_queued_watchers`, `actor_ref.rs` (link-to-dead path ~line 289), and test literals. Whole-repo sweep (memory `enum-variant-removal-grep-scope`: includes `examples/`, `fuzz/`):

```bash
rg -l "LinkDied \{" --type rust | xargs -I{} echo {}   # visit each; synthetic paths use cleanup_failed: false
cargo nextest run -p bombay-core
```

Expected: all PASS (kill-path test `drop_without_set_reason_reports_killed` asserts `cleanup_failed == false` — add that assertion to it).

- [ ] **Step 5: Commit** — `feat(watch): LinkDied carries cleanup_failed (#196)`

---

## Task 3: Teardown reorder — bounded `on_stop` *before* death notices

`finish_actor` (`spawn.rs:254`) currently drops `watchers` before `on_stop`. New order: drain raced regs → run `on_stop` bounded by `ON_STOP_NOTICE_GRACE` → stamp `cleanup_failed` on Err/panic/timeout → drop `watchers` (notices fire). Kill path untouched (lifecycle future dropped; guard fires `Killed`, `cleanup_failed=false`). The supervisor-side `stop_grace` (Task 5) is a *different* knob: it bounds cancel→abort externally; this constant bounds notice delay internally.

**Files:**
- Modify: `bombay-core/src/actor/spawn.rs` (`finish_actor` + a `pub(crate) const ON_STOP_NOTICE_GRACE: Duration = Duration::from_secs(5);`)
- Test: `bombay-core/src/actor/spawn.rs` tests module

- [ ] **Step 1: Two failing tests** (both `start_paused` — deterministic time):

```rust
/// #196 invariant 18: on_stop Err ⇒ notice carries cleanup_failed=true.
#[tokio::test(start_paused = true)]
async fn on_stop_error_marks_cleanup_failed_on_notice() {
    // FailingStop: on_stop returns Err("cleanup failed"); handle sets *stop = true on first msg.
    let prepared = PreparedActor::<FailingStop>::new(cap(4));
    let (link_tx, link_rx) = flume::unbounded();
    prepared.actor_ref().mailbox_sender().try_send(Signal::Watch(Box::new(WatchReg {
        watcher: ActorId::new(1), link_tx, linked: false,
    }))).expect("capacity");
    let actor_ref = prepared.actor_ref().clone();
    let join = prepared.spawn(());
    actor_ref.stop();
    drop(actor_ref);
    tokio::time::timeout(Duration::from_secs(30), join).await.expect("bounded").expect("join");

    let n = link_rx.try_recv().expect("notified");
    assert!(n.cleanup_failed);
    assert!(n.reason.is_normal(), "reason stays Normal — cleanup failure is a flag, not a reason");
}

/// #196 invariant 19: a HANGING on_stop delays the death notice by at most
/// ON_STOP_NOTICE_GRACE — never unboundedly.
#[tokio::test(start_paused = true)]
async fn death_notice_within_grace_of_hanging_on_stop() {
    // HangingStop: on_stop = std::future::pending().
    let prepared = PreparedActor::<HangingStop>::new(cap(4));
    let (link_tx, link_rx) = flume::unbounded();
    prepared.actor_ref().mailbox_sender().try_send(Signal::Watch(Box::new(WatchReg {
        watcher: ActorId::new(1), link_tx, linked: false,
    }))).expect("capacity");
    let actor_ref = prepared.actor_ref().clone();
    let _join = prepared.spawn(());
    actor_ref.stop();
    drop(actor_ref);

    // Auto-advance carries us past the grace; the notice must arrive.
    let n = tokio::time::timeout(ON_STOP_NOTICE_GRACE + Duration::from_secs(1), async {
        link_rx.recv_async().await
    })
    .await
    .expect("notice within grace")
    .expect("channel open");
    assert!(n.cleanup_failed, "an abandoned on_stop counts as failed cleanup");
}
```

Probe actors:

```rust
struct FailingStop;
impl Actor for FailingStop {
    type Args = (); type Error = &'static str;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(Self) }
    async fn handle(&mut self, _: NoMsg, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> { Ok(()) }
    async fn on_stop(&mut self, _: WeakActorRef<Self>, _: ActorStopReason) -> Result<(), Self::Error> {
        Err("cleanup failed")
    }
}
struct HangingStop;
impl Actor for HangingStop {
    type Args = (); type Error = &'static str;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(Self) }
    async fn handle(&mut self, _: NoMsg, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> { Ok(()) }
    async fn on_stop(&mut self, _: WeakActorRef<Self>, _: ActorStopReason) -> Result<(), Self::Error> {
        std::future::pending().await
    }
}
```

(Each needs its own `Mailboxed` impl over `NoMsg`.)

- [ ] **Step 2: Run** — first FAILS (`cleanup_failed` false: notices fired before `on_stop` ran), second FAILS (notice arrives immediately, before grace — assertion on `cleanup_failed` false).

- [ ] **Step 3: Reorder `finish_actor`:**

```rust
async fn finish_actor<A: Actor>(
    mut state: A,
    weak: WeakActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
    mut watchers: Watchers,
    reason: ActorStopReason,
) -> RunResult<A> {
    for signal in mailbox_rx.drain() {
        match signal {
            Signal::Watch(reg) => watchers.apply(*reg),
            Signal::Unwatch(id) => watchers.remove(id),
            Signal::Message { .. } | Signal::Stop => {}
        }
    }
    watchers.set_reason(reason.clone());

    // #196 reorder (revises #195): run a BOUNDED on_stop first so the death
    // notices can carry its outcome (`cleanup_failed`). Notices are delayed by
    // at most ON_STOP_NOTICE_GRACE — never unboundedly behind a hanging user
    // hook, which is the property the old notify-first order protected. OTP
    // shape: terminate/2 runs before exit signals, bounded by `shutdown`.
    let stop_fut = AssertUnwindSafe(state.on_stop(weak.clone(), reason.clone())).catch_unwind();
    match tokio::time::timeout(ON_STOP_NOTICE_GRACE, stop_fut).await {
        Ok(stop_result) => {
            if !matches!(&stop_result, Ok(Ok(()))) {
                watchers.set_cleanup_failed();
            }
            log_on_stop_outcome::<A>(&reason, stop_result);
        }
        Err(_elapsed) => {
            // Crash-only: the hook exceeded its bound and is abandoned (its
            // future is dropped); death must still be announced.
            watchers.set_cleanup_failed();
        }
    }
    drop(watchers); // fires the notifications — now WITH the cleanup outcome

    RunResult::Stopped { actor: state, reason }
}
```

(`timeout(..)` returning `Err` drops `stop_fut` — borrow of `state` ends there; the subsequent move of `state` into `RunResult` is legal.)

- [ ] **Step 4: Update the #195 ordering artifacts.** Find every test/comment asserting notify-before-`on_stop`:

```bash
rg -n "before on_stop|notifications before" bombay-core/src/ bombay-core/tests/
```

Update each to the new invariant ("within grace"). Run the full crate:

```bash
cargo nextest run -p bombay-core
```

Expected: all PASS.

- [ ] **Step 5: Commit** — `feat(spawn): bounded on_stop before death notices; notices carry cleanup outcome (#196)`

---

## Task 4: `restart.rs` — policy table (`should_restart`)

New top-level module, pure sync. Add `pub mod restart;` to `lib.rs` (after `registry`).

**Files:**
- Create: `bombay-core/src/restart.rs`
- Modify: `bombay-core/src/lib.rs`
- Test: inline `mod tests`

- [ ] **Step 1: Failing tests** — the spec's decision table, one test per row-class + the lifecycle-hook carve-out (spec invariants 2–7 pure halves):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ActorStopReason, PanicError, PanicReason};

    fn panicked(reason: PanicReason) -> ActorStopReason {
        ActorStopReason::Panicked(PanicError::new(Box::new("boom"), reason))
    }

    #[test]
    fn permanent_restarts_on_every_reason() {
        for reason in [
            ActorStopReason::Normal,
            ActorStopReason::SupervisorRestart,
            ActorStopReason::Killed,
            ActorStopReason::AlreadyDead,
            panicked(PanicReason::HandlerPanic),
        ] {
            assert_eq!(
                should_restart(RestartPolicy::Permanent, &reason),
                RestartVerdict::Restart,
                "Permanent must restart on {reason:?}",
            );
        }
    }

    #[test]
    fn transient_leaves_normal_and_supervisor_restart_dead() {
        assert_eq!(should_restart(RestartPolicy::Transient, &ActorStopReason::Normal), RestartVerdict::LeaveDead);
        assert_eq!(should_restart(RestartPolicy::Transient, &ActorStopReason::SupervisorRestart), RestartVerdict::LeaveDead);
    }

    #[test]
    fn transient_restarts_on_abnormal() {
        for reason in [ActorStopReason::Killed, ActorStopReason::AlreadyDead, panicked(PanicReason::HandlerPanic)] {
            assert_eq!(should_restart(RestartPolicy::Transient, &reason), RestartVerdict::Restart, "{reason:?}");
        }
    }

    #[test]
    fn never_always_leaves_dead() {
        for reason in [ActorStopReason::Normal, ActorStopReason::Killed, panicked(PanicReason::HandlerPanic)] {
            assert_eq!(should_restart(RestartPolicy::Never, &reason), RestartVerdict::LeaveDead, "{reason:?}");
        }
    }

    /// #196: a lifecycle-hook panic (`on_start` above all) is a guaranteed crash
    /// loop — restart is knowably wrong; escalate regardless of policy.
    #[test]
    fn lifecycle_hook_panic_escalates_under_every_policy() {
        for policy in [RestartPolicy::Permanent, RestartPolicy::Transient, RestartPolicy::Never] {
            assert_eq!(
                should_restart(policy, &panicked(PanicReason::OnStart)),
                RestartVerdict::Escalate,
                "{policy:?}",
            );
        }
    }

    #[test]
    fn nested_link_died_classified_by_outer_variant() {
        let reason = ActorStopReason::LinkDied {
            id: crate::mailbox::ActorId::new(3),
            reason: Box::new(ActorStopReason::Killed),
        };
        assert_eq!(should_restart(RestartPolicy::Transient, &reason), RestartVerdict::Restart);
    }
}
```

- [ ] **Step 2: Run** `cargo nextest run -p bombay-core restart::` — FAIL: module absent.

- [ ] **Step 3: Implement:**

```rust
//! Restart policy & accounting (card #196): WHEN to rebuild a dead child, how
//! to SPACE attempts, and when to GIVE UP. Pure and synchronous on purpose —
//! the async loop only consumes verdicts, so this module mutation-tests clean.

use crate::error::{ActorStopReason, PanicReason};

/// Per-child restart policy — explicit at every `supervise` call, NEVER
/// defaulted (spec: OTP `permanent` / k8s `Always` / Akka *stop* disagree
/// across the whole range ⇒ the choice is not defaultable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestartPolicy {
    /// Rebuild on every exit, normal or abnormal (a server: exiting is a bug).
    Permanent,
    /// Rebuild on abnormal exit only; a normal stop is the actor's own decision.
    Transient,
    /// Never rebuild; supervision is observation only.
    Never,
}

/// What the supervisor does with one death notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartVerdict {
    Restart,
    LeaveDead,
    /// A lifecycle-hook failure: restarting is a guaranteed crash loop —
    /// bypass backoff and counters, escalate now.
    Escalate,
}

/// The spec's decision table. Lifecycle-hook panics short-circuit every policy.
#[must_use]
pub fn should_restart(policy: RestartPolicy, reason: &ActorStopReason) -> RestartVerdict {
    if let ActorStopReason::Panicked(err) = reason {
        if err.reason().is_lifecycle_hook() {
            return RestartVerdict::Escalate;
        }
    }
    match policy {
        RestartPolicy::Permanent => RestartVerdict::Restart,
        RestartPolicy::Transient if reason.is_normal() => RestartVerdict::LeaveDead,
        RestartPolicy::Transient => RestartVerdict::Restart,
        RestartPolicy::Never => RestartVerdict::LeaveDead,
    }
}
```

- [ ] **Step 4: Track + run** — `git add bombay-core/src/restart.rs && cargo nextest run -p bombay-core restart::` — PASS.

- [ ] **Step 5: Commit** — `feat(restart): RestartPolicy + should_restart decision table (#196)`

---

## Task 5: `restart.rs` — `Jitter`, `RestartConfig` (no `Default`), builder

**Files:**
- Modify: `bombay-core/src/restart.rs`

- [ ] **Step 1: Failing tests:**

```rust
    #[test]
    fn jitter_clamps_to_percent() {
        assert_eq!(Jitter::percent(20).as_percent(), 20);
        assert_eq!(Jitter::percent(150).as_percent(), 100, "clamped, not rejected — a magnitude, not semantics");
    }

    #[test]
    fn config_from_bare_policy_uses_documented_tuning() {
        let cfg: RestartConfig = RestartPolicy::Transient.into();
        assert_eq!(cfg.policy, RestartPolicy::Transient);
        assert_eq!(cfg.max_restarts, 5);
        assert_eq!(cfg.max_total, 100);
        assert_eq!(cfg.min_backoff, Duration::from_millis(100));
        assert_eq!(cfg.max_backoff, Duration::from_secs(30));
        assert_eq!(cfg.jitter, Jitter::percent(20));
        assert_eq!(cfg.reset_after, Duration::from_secs(60));
        assert_eq!(cfg.stop_grace, Duration::from_secs(5));
    }

    #[test]
    fn builder_overrides_stick() {
        let cfg = RestartConfig::new(RestartPolicy::Permanent)
            .with_max_backoff(Duration::from_secs(5))
            .with_max_restarts(2);
        assert_eq!(cfg.max_backoff, Duration::from_secs(5));
        assert_eq!(cfg.max_restarts, 2);
        assert_eq!(cfg.policy, RestartPolicy::Permanent);
    }
```

- [ ] **Step 2: Run** — FAIL (types absent).

- [ ] **Step 3: Implement** (public fields — this is config, not an invariant-bearing type; NO `Default` impl, spec "Policy has no default"):

```rust
/// Jitter as an integer percent (0..=100) of the computed delay — keeps the
/// config `Eq`/`Hash` and clear of float pedantry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Jitter(u8);

impl Jitter {
    #[must_use]
    pub const fn percent(p: u8) -> Self {
        Self(if p > 100 { 100 } else { p })
    }
    #[must_use]
    pub const fn as_percent(self) -> u8 {
        self.0
    }
}

/// Restart tuning for one supervised child. Deliberately NO `Default` impl:
/// the policy is caller-stated semantics (see the spec's "Policy has no
/// default; tuning does") — a future `derive(Default)` here is a spec
/// violation, not a convenience.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RestartConfig {
    pub policy: RestartPolicy,
    pub max_restarts: u32,
    pub max_total: u32,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub jitter: Jitter,
    pub reset_after: Duration,
    pub stop_grace: Duration,
}

impl RestartConfig {
    /// Documented tuning defaults around an EXPLICIT policy. `stop_grace = 5s`
    /// is OTP's child-spec `shutdown` default; the rest are unsourced starting
    /// points (re-tuned under #199's DST measurements).
    #[must_use]
    pub const fn new(policy: RestartPolicy) -> Self {
        Self {
            policy,
            max_restarts: 5,
            max_total: 100,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            jitter: Jitter::percent(20),
            reset_after: Duration::from_secs(60),
            stop_grace: Duration::from_secs(5),
        }
    }
    #[must_use] pub const fn with_max_restarts(mut self, n: u32) -> Self { self.max_restarts = n; self }
    #[must_use] pub const fn with_max_total(mut self, n: u32) -> Self { self.max_total = n; self }
    #[must_use] pub const fn with_min_backoff(mut self, d: Duration) -> Self { self.min_backoff = d; self }
    #[must_use] pub const fn with_max_backoff(mut self, d: Duration) -> Self { self.max_backoff = d; self }
    #[must_use] pub const fn with_jitter(mut self, j: Jitter) -> Self { self.jitter = j; self }
    #[must_use] pub const fn with_reset_after(mut self, d: Duration) -> Self { self.reset_after = d; self }
    #[must_use] pub const fn with_stop_grace(mut self, d: Duration) -> Self { self.stop_grace = d; self }
}

impl From<RestartPolicy> for RestartConfig {
    fn from(policy: RestartPolicy) -> Self {
        Self::new(policy)
    }
}
```

(`use core::time::Duration;` at top of module.)

- [ ] **Step 4: Run** — PASS. **Step 5: Commit** — `feat(restart): Jitter + RestartConfig, policy explicit with no Default (#196)`

---

## Task 6: `restart.rs` — backoff arithmetic

**Files:**
- Modify: `bombay-core/src/restart.rs`

- [ ] **Step 1: Failing tests** (boundaries per test-quality rule: `n=0`, `1`, cap crossover, `u32::MAX`; jitter bounds via seeded rng + a `prop_` for MIRI-skip):

```rust
    #[test]
    fn backoff_grows_exponentially_from_min() {
        let cfg = RestartConfig::new(RestartPolicy::Transient).with_jitter(Jitter::percent(0));
        assert_eq!(base_backoff(&cfg, 1), Duration::from_millis(100));
        assert_eq!(base_backoff(&cfg, 2), Duration::from_millis(200));
        assert_eq!(base_backoff(&cfg, 3), Duration::from_millis(400));
    }

    #[test]
    fn backoff_caps_at_max_and_survives_huge_n() {
        let cfg = RestartConfig::new(RestartPolicy::Transient);
        assert_eq!(base_backoff(&cfg, 10), Duration::from_secs(30), "past the cap");
        assert_eq!(base_backoff(&cfg, u32::MAX), Duration::from_secs(30), "overflow = explicit cap branch");
        assert_eq!(base_backoff(&cfg, 0), cfg.min_backoff, "n=0 degenerate = min");
    }

    #[test]
    fn jittered_backoff_is_seeded_and_bounded() {
        let cfg = RestartConfig::new(RestartPolicy::Transient); // 20% jitter
        let mut rng = fastrand::Rng::with_seed(42);
        let base = base_backoff(&cfg, 3); // 400ms
        let d = jittered_backoff(&cfg, 3, &mut rng);
        assert!(d >= base && d <= base + base / 5, "within +20%: {d:?}");
        let mut rng2 = fastrand::Rng::with_seed(42);
        assert_eq!(d, jittered_backoff(&cfg, 3, &mut rng2), "same seed ⇒ same delay (DST contract)");
    }

    proptest::proptest! {
        /// MIRI-skipped by prefix (memory: prop_ contract).
        #[test]
        fn prop_backoff_monotone_until_cap(n in 0_u32..64) {
            let cfg = RestartConfig::new(RestartPolicy::Transient);
            proptest::prop_assert!(base_backoff(&cfg, n) <= base_backoff(&cfg, n.saturating_add(1)));
            proptest::prop_assert!(base_backoff(&cfg, n) <= cfg.max_backoff);
        }
    }
```

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement:**

```rust
/// `min_backoff * 2^(n-1)`, capped at `max_backoff` by EXPLICIT branch — the
/// cap is a semantic ceiling, so overflow routes to it deliberately (never
/// `saturating_*`, which would make the same value an accident).
#[must_use]
pub fn base_backoff(cfg: &RestartConfig, consecutive: u32) -> Duration {
    let exp = consecutive.saturating_sub(1);
    let Some(factor) = 1_u32.checked_shl(exp) else {
        return cfg.max_backoff;
    };
    match cfg.min_backoff.checked_mul(factor) {
        Some(d) if d < cfg.max_backoff => d,
        _ => cfg.max_backoff,
    }
}

/// Adds `0..=jitter%` of the base, from a SEEDABLE rng (DST asserts exact
/// delays under a fixed seed instead of disabling jitter).
#[must_use]
pub fn jittered_backoff(cfg: &RestartConfig, consecutive: u32, rng: &mut fastrand::Rng) -> Duration {
    let base = base_backoff(cfg, consecutive);
    let pct = u64::from(rng.u8(0..=cfg.jitter.as_percent()));
    let extra_nanos = (base.as_nanos() / 100).saturating_mul(u128::from(pct));
    base + Duration::from_nanos(u64::try_from(extra_nanos).unwrap_or(u64::MAX))
}
```

*Note on the two `saturating_` uses:* `saturating_sub(1)` on `n=0` and the jitter magnitude are display/magnitude paths, not size/offset computations — record the reasoning in a comment; if clippy's restriction lint objects, switch to explicit `if`.

- [ ] **Step 4: Run** module + fmt. **Step 5: Commit** — `feat(restart): exponential backoff with cap and seeded jitter (#196)`

---

## Task 7: `restart.rs` — `RestartTracker` (two counters + reset)

**Files:**
- Modify: `bombay-core/src/restart.rs`

- [ ] **Step 1: Failing tests** (pure — `Instant` passed in, no clock reads; spec invariants 9/10/11 arithmetic halves):

```rust
    fn t0() -> Instant { Instant::now() } // tests only compare relative offsets

    #[test]
    fn consecutive_limit_escalates() {
        let cfg = RestartConfig::new(RestartPolicy::Transient).with_max_restarts(2);
        let mut tr = RestartTracker::new(t0());
        let now = t0();
        assert!(matches!(tr.record_failure(&cfg, now), GiveUp::No { attempt: 1 }));
        assert!(matches!(tr.record_failure(&cfg, now), GiveUp::No { attempt: 2 }));
        assert!(matches!(tr.record_failure(&cfg, now), GiveUp::Yes { rebuilds: 3 }));
    }

    #[test]
    fn healthy_uptime_resets_consecutive_only() {
        let cfg = RestartConfig::new(RestartPolicy::Transient).with_max_restarts(2).with_max_total(4);
        let mut tr = RestartTracker::new(t0());
        let start = t0();
        assert!(matches!(tr.record_failure(&cfg, start), GiveUp::No { attempt: 1 }));
        tr.record_started(start);
        let healthy = start + cfg.reset_after + Duration::from_secs(1);
        assert!(matches!(tr.record_failure(&cfg, healthy), GiveUp::No { attempt: 1 }), "consecutive reset by healthy uptime");
    }

    /// Spec invariant 11: slow drip — consecutive resets every time, the
    /// never-reset lifetime budget still trips. Fails if only one counter exists.
    #[test]
    fn slow_drip_exhausts_lifetime_budget() {
        let cfg = RestartConfig::new(RestartPolicy::Transient).with_max_restarts(5).with_max_total(3);
        let mut tr = RestartTracker::new(t0());
        let mut now = t0();
        for expected_total in 1..=3_u32 {
            let verdict = tr.record_failure(&cfg, now);
            assert!(matches!(verdict, GiveUp::No { attempt: 1 }), "drip #{expected_total}: {verdict:?}");
            tr.record_started(now);
            now += cfg.reset_after + Duration::from_secs(1); // always "healthy"
        }
        assert!(matches!(tr.record_failure(&cfg, now), GiveUp::Yes { rebuilds: 4 }), "lifetime budget trips");
    }
```

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement:**

```rust
use tokio::time::Instant; // paused-clock aware (start_paused), unlike std's

/// One child's give-up accounting. `consecutive` answers "did this incarnation
/// work?" (reset on healthy uptime); `total` answers "is this child worth
/// having at all?" (NEVER reset — catches the slow drip that resets
/// `consecutive` every cycle). Escalate when either trips.
#[derive(Debug, Clone, Copy)]
pub struct RestartTracker {
    consecutive: u32,
    total: u32,
    started: Instant,
}

/// Verdict of one recorded failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GiveUp {
    /// Rebuild; `attempt` (≥1, consecutive) feeds [`jittered_backoff`].
    No { attempt: u32 },
    /// A budget tripped: `rebuilds` = lifetime failures observed.
    Yes { rebuilds: u32 },
}

impl RestartTracker {
    #[must_use]
    pub const fn new(started: Instant) -> Self {
        Self { consecutive: 0, total: 0, started }
    }

    /// Records an incarnation start (arms the healthy-uptime reset clock).
    pub fn record_started(&mut self, now: Instant) {
        self.started = now;
    }

    /// Records a death; both counters via `checked_add` — a counter overflow in
    /// a LIMIT path must trip the limit, not wrap or silently cap.
    pub fn record_failure(&mut self, cfg: &RestartConfig, now: Instant) -> GiveUp {
        if now.saturating_duration_since(self.started) > cfg.reset_after {
            self.consecutive = 0;
        }
        let (Some(consecutive), Some(total)) =
            (self.consecutive.checked_add(1), self.total.checked_add(1))
        else {
            return GiveUp::Yes { rebuilds: self.total };
        };
        self.consecutive = consecutive;
        self.total = total;
        if consecutive > cfg.max_restarts || total > cfg.max_total {
            GiveUp::Yes { rebuilds: total }
        } else {
            GiveUp::No { attempt: consecutive }
        }
    }
}
```

(`saturating_duration_since` is tokio-`Instant`'s panic-free elapsed — an uptime comparison, not a size computation.)

- [ ] **Step 4: Run + fmt.** **Step 5: Commit** — `feat(restart): RestartTracker — consecutive + lifetime budgets with healthy-uptime reset (#196)`

---

## Task 8: `ActorStopReason::RestartLimitExceeded`

**Files:**
- Modify: `bombay-core/src/error.rs`
- Whole-repo match sweep (memory `enum-variant-removal-grep-scope`)

- [ ] **Step 1: Failing test** (in `error.rs` tests):

```rust
#[test]
fn restart_limit_exceeded_is_abnormal() {
    let reason = ActorStopReason::RestartLimitExceeded {
        child: crate::mailbox::ActorId::new(7),
        rebuilds: 6,
    };
    assert!(!reason.is_normal(), "an escalating supervisor is an abnormal stop — its own watcher must propagate");
}
```

- [ ] **Step 2: Run** — FAIL (no variant). **Step 3: Implement** — add to `ActorStopReason` (after `SupervisorRestart`):

```rust
    /// A supervisor gave up on a child (a restart budget tripped) and is
    /// escalating by stopping itself — the microreboot ladder's next rung is
    /// whoever watches this supervisor (#196).
    #[error("restart limit exceeded for child {child:?} after {rebuilds} rebuilds")]
    RestartLimitExceeded {
        /// The child whose budget tripped.
        child: crate::mailbox::ActorId,
        /// Lifetime failures observed for that child.
        rebuilds: u32,
    },
```

`is_normal()` unchanged (variant falls in the `false` arm — verify the `matches!` covers it, it does by exclusion).

- [ ] **Step 4: Whole-repo exhaustive-match sweep** — `ActorStopReason` is exhaustively matched (`no #[non_exhaustive]` rule):

```bash
cargo build --workspace --all-targets 2>&1 | rg "non-exhaustive patterns" -A3
rg -n "match .*reason|ActorStopReason::" --type rust src/ bombay-core/ examples/ fuzz/ actors/ | rg -v "//" | head -40
```

Fix every non-exhaustive site (run-loop `should_restart` sites come later; existing sites mostly treat it via `_ | is_normal()` — each new arm is deliberate). Then `cargo nextest run -p bombay-core` + `cargo build --workspace --all-targets` (examples + fuzz compile — a broken example aborts the mutants baseline build).

- [ ] **Step 5: Commit** — `feat(error): ActorStopReason::RestartLimitExceeded (#196)`

---

## Task 9: Supervision plumbing types — `ChildHandle`, `SuperviseReg`, `Children`, `Signal::Supervision`

**Files:**
- Create: `bombay-core/src/actor/supervision.rs` (`mod supervision;` in `actor/mod.rs`, `pub use` the public types)
- Modify: `bombay-core/src/mailbox.rs` (one new `Signal` variant)
- Test: inline

- [ ] **Step 1: Failing tests** (pure table ops — insertion order, remove semantics, take-for-retry):

```rust
    #[test]
    fn children_insert_lookup_remove_preserve_birth_order() {
        let mut children = Children::new();
        children.insert(child_entry(1));
        children.insert(child_entry(2));
        children.insert(child_entry(3));
        assert!(children.get_mut(ActorId::new(2)).is_some());
        children.remove(ActorId::new(2));
        assert!(children.get_mut(ActorId::new(2)).is_none());
        let ids: Vec<_> = children.ids().collect();
        assert_eq!(ids, [ActorId::new(1), ActorId::new(3)], "birth order survives removal (RestForOne seam, #199)");
    }
```

(`child_entry(n)` helper builds a `Child` with a no-op factory and a dummy handle around `ActorId::new(n)`.)

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement** `supervision.rs`:

```rust
//! Supervision runtime types (card #196): the loop-owned child table and the
//! erased rebuild edge. The factory closure is the feature's ONLY `dyn` — it is
//! the one place the child's concrete type is in scope (spawn + watch-install +
//! registry rebinding all live inside it). It must never capture a strong
//! `ActorRef` of supervisor OR child (ADR-0003: a strong ref here pins liveness
//! and makes ref-count-driven stop unreachable — kameo #171).

use crate::{mailbox::ActorId, restart::{RestartConfig, RestartTracker}};
use futures::{future::BoxFuture, stream::AbortHandle};
use smallvec::SmallVec;
use tokio_util::sync::CancellationToken;

/// Non-generic handle to one child incarnation: stop edges only, no sender —
/// holding a sender would pin the child's mailbox open.
#[derive(Debug, Clone)]
pub struct ChildHandle {
    pub(crate) id: ActorId,
    pub(crate) cancel: CancellationToken,
    pub(crate) abort: AbortHandle,
}

impl ChildHandle {
    #[must_use]
    pub const fn id(&self) -> ActorId {
        self.id
    }
}

/// The erased rebuild edge: spawns a fresh incarnation, installs the
/// supervisor's watch edge on it, returns the new handle. Boxed future because
/// edge installation is async (bounded fallback path).
pub(crate) type RebuildFactory = Box<dyn FnMut() -> BoxFuture<'static, ChildHandle> + Send>;

/// One supervised child in the loop-owned table.
pub(crate) struct Child {
    pub(crate) factory: RebuildFactory,
    /// `None` while a rebuild waits out its backoff in the DelayQueue.
    pub(crate) handle: Option<ChildHandle>,
    pub(crate) config: RestartConfig,
    pub(crate) tracker: RestartTracker,
}

/// A supervise registration in transit on the supervisor's own mailbox: the
/// first incarnation was already spawned in the CALLER's task (that is what
/// makes `supervise` a tell that can return the `ActorId`).
pub(crate) struct SuperviseReg {
    pub(crate) child: Child,
}

/// Loop-owned, beside `Watchers` — recovery bookkeeping must not live in the
/// `&mut self` that a fault can tear (crash-only, applied to ourselves).
/// Insertion order IS birth order (the #199 RestForOne seam).
pub(crate) struct Children {
    entries: SmallVec<[(ActorId, Child); 4]>,
}

impl Children {
    pub(crate) fn new() -> Self {
        Self { entries: SmallVec::new() }
    }
    /// Key = the child's CURRENT incarnation id; re-keyed on rebuild.
    pub(crate) fn insert(&mut self, child: Child) {
        let id = child.handle.as_ref().map_or(ActorId::new(u64::MAX), ChildHandle::id);
        self.entries.push((id, child));
    }
    pub(crate) fn get_mut(&mut self, id: ActorId) -> Option<&mut Child> {
        self.entries.iter_mut().find(|(k, _)| *k == id).map(|(_, c)| c)
    }
    pub(crate) fn remove(&mut self, id: ActorId) -> Option<Child> {
        let idx = self.entries.iter().position(|(k, _)| *k == id)?;
        Some(self.entries.remove(idx).1)
    }
    pub(crate) fn rekey(&mut self, old: ActorId, new: ActorId) {
        if let Some(entry) = self.entries.iter_mut().find(|(k, _)| *k == old) {
            entry.0 = new;
        }
    }
    pub(crate) fn ids(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.entries.iter().map(|(k, _)| *k)
    }
}
```

*(Adjust `insert` to take the id explicitly if the `u64::MAX` sentinel offends — `insert(id: ActorId, child: Child)` is cleaner and is what the loop has in hand. Prefer that; the test uses it accordingly.)*

`Signal` in `mailbox.rs` gains ONE variant (no Debug derive exists on `Signal`, so the un-Debug factory is fine — verified):

```rust
    /// A supervision-table operation for the supervisor's loop (card #196).
    /// Boxed: the reg embeds config + factory and must not bloat the hot
    /// `Message` slot (#114 budget).
    Supervision(Box<crate::actor::supervision::SupervisionOp>),
```

with, in `supervision.rs`:

```rust
/// Table operations shipped over the supervisor's own mailbox — the table is
/// task-owned, so ALL mutation goes through the loop (no lock, no ordering rule).
pub(crate) enum SupervisionOp {
    Add(SuperviseReg),
    /// Drop the edge; child keeps running unwatched.
    Remove(ActorId),
    /// Drop the edge AND stop the child (cancel → stop_grace → abort) — OTP
    /// `terminate_child/2`. Without it, `kill()` fights the policy.
    Stop(ActorId),
}
```

Non-supervised loops treat `Signal::Supervision(_)` as a no-op arm (`kind.rs` `handle_mailbox_step`: ignore-and-continue, mirroring the pre-#195 `LinkDied` seam comment).

- [ ] **Step 4:** `git add bombay-core/src/actor/supervision.rs`, build workspace (exhaustive `Signal` matches in `kind.rs`/`spawn.rs`/tests now need the new arm — add ignore arms), run crate tests. PASS.

- [ ] **Step 5: Commit** — `feat(supervision): Children table, ChildHandle, SupervisionOp signal (#196)`

---

## Task 10: `Supervisor` + `SpawnSupervised` + the supervised loop (rebuild on death)

The heart. `run_supervised_message_loop` = linked loop + (a) death notices for child ids consult the table instead of `on_link_died`, (b) a `DelayQueue<ActorId>` arm fires due rebuilds, (c) `Supervision` ops mutate the table.

**Files:**
- Modify: `bombay-core/src/actor/mod.rs` (traits)
- Modify: `bombay-core/src/actor/kind.rs` (loop)
- Modify: `bombay-core/src/actor/spawn.rs` (`new_supervised` / `run_supervised` / `spawn_supervised_task`, lifecycle)
- Modify: `bombay-core/src/actor/supervision.rs` (decision glue)
- Test: `bombay-core/tests/supervision.rs` (new integration file — `git add` it!)

- [ ] **Step 1: Failing test** (spec invariants 1 + 4 — rebuild-not-resume, via the public API end to end):

```rust
//! Integration: supervised rebuild semantics (#196).
use bombay_core::{/* public exports: Actor, Watch, Supervisor, SpawnSupervised, RestartPolicy, ... */};

/// Counter child: `Bump` mutates state then panics on command. Its Args carry
/// an mpsc reporting each incarnation's on_start (fresh-state proof).
#[tokio::test(start_paused = true)]
async fn panicked_child_is_rebuilt_fresh_with_new_id() {
    let (started_tx, started_rx) = flume::unbounded::<u64>(); // ActorId::as_u64 per on_start
    let sup = Sup::spawn_supervised(());
    let child_id = tokio::time::timeout(D5, sup.supervise::<Crasher, _>(
        bombay_core::RestartPolicy::Transient.into(),
        move || Crasher::spawn(CrasherArgs { started: started_tx.clone() }),
    )).await.expect("bounded").expect("registered");

    let first = tokio::time::timeout(D5, started_rx.recv_async()).await.expect("bounded").expect("first incarnation");
    // kill it abnormally: Crasher panics on its first message; reach it by registry-free direct send is
    // not possible without a ref — so CrasherArgs also carries a oneshot the child uses to send its own
    // ActorRef out on on_start. Panic it via tell(Boom).
    crasher_ref.tell(Boom).send().await.expect("delivered");

    let second = tokio::time::timeout(D30, started_rx.recv_async()).await.expect("rebuilt within backoff").expect("second incarnation");
    assert_ne!(first, second, "a rebuilt child is a NEW actor (new ActorId), never the torn one");
    assert_ne!(second, child_id.as_u64(), "and not the registered first incarnation either");
}
```

(Concretize the probe pair — `Sup` is a unit `Supervisor` with default hooks; `Crasher` panics in `handle`. The test file defines both fully; `D5`/`D30` are `Duration` consts. `start_paused` auto-advance covers the 100ms backoff without real sleeping.)

- [ ] **Step 2: Run** — FAIL: traits/entry points absent.

- [ ] **Step 3: Implement, in order:**

**(a) `actor/mod.rs` traits** (after `SpawnLinked`):

```rust
/// Authority marker: `Actor` cannot watch, `Watch` observes death, `Supervisor`
/// rebuilds. No methods in slice 2a — restart policy is per-CHILD, supplied at
/// `supervise` time; #199 lands `supervision_strategy()` here (its named seat).
pub trait Supervisor: Watch {}

/// Spawn entry for supervisors: a supervised actor runs the three-arm loop
/// (mailbox + link channel + restart DelayQueue).
pub trait SpawnSupervised: Supervisor {
    #[must_use]
    fn spawn_supervised(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_supervised_with_capacity(default_capacity(), args)
    }
    #[must_use]
    fn spawn_supervised_with_capacity(capacity: Capacity, args: Self::Args) -> ActorRef<Self> {
        let (prepared, link_rx) = PreparedActor::<Self>::new_linked(capacity);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn_supervised_task(args, link_rx);
        actor_ref
    }
}
impl<A: Supervisor> SpawnSupervised for A {}
```

**(b) `spawn.rs`:** `spawn_supervised_task` mirrors `spawn_linked_task` → `run_lifecycle_supervised` → same `start_actor`/`finish_actor` bookends, driving the new loop with a `Children::new()` + `DelayQueue` + a seeded-from-entropy `fastrand::Rng` (test seam: `#[cfg(test)]` constructor takes a seed).

**(c) `kind.rs` supervised loop** (sketch — the select over three arms; `biased` first death, then timer, then mailbox):

```rust
pub(super) async fn run_supervised_message_loop<A: Supervisor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    handles: &LoopHandles,
    watchers: &mut Watchers,
    channels: LinkedLoopChannels<'_, A>,
    children: &mut Children,
    retries: &mut DelayQueue<ActorId>,
    rng: &mut fastrand::Rng,
) -> ActorStopReason {
    let LinkedLoopChannels { mailbox_rx, link_rx } = channels;
    let mut link_open = true;
    loop {
        tokio::select! {
            biased;
            death = link_rx.recv_async(), if link_open => match death {
                Ok(notice) if children.get_mut(notice.id).is_some() => {
                    match handle_child_death(children, retries, rng, notice) {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(reason) => return reason, // escalation
                    }
                }
                Ok(notice) => {
                    if let ControlFlow::Break(reason) = handle_link_died(state, notice).await {
                        return reason;
                    }
                }
                Err(_) => link_open = false,
            },
            expired = retries.next(), if !retries.is_empty() => {
                if let Some(expired) = expired {
                    rebuild_child(children, expired.into_inner()).await;
                }
            }
            maybe = handles.cancel.run_until_cancelled(mailbox_rx.recv()) => {
                match handle_mailbox_step(state, self_ref, handles, watchers, maybe.flatten()).await {
                    // Signal::Supervision ops are intercepted HERE (before the
                    // shared step) and applied to `children`.
                    ControlFlow::Continue(()) => {}
                    ControlFlow::Break(reason) => return reason,
                }
            }
        }
    }
}
```

`handle_child_death` is **sync + pure-arming** (mutants lesson: decision separate from polling):

```rust
fn handle_child_death(
    children: &mut Children,
    retries: &mut DelayQueue<ActorId>,
    rng: &mut fastrand::Rng,
    notice: LinkDied,
) -> ControlFlow<ActorStopReason> {
    let child = children.get_mut(notice.id).expect("caller checked membership");
    child.handle = None;
    match should_restart(child.config.policy, &notice.reason) {
        RestartVerdict::LeaveDead => ControlFlow::Continue(()),
        RestartVerdict::Escalate => ControlFlow::Break(escalation_reason(notice.id, child)),
        RestartVerdict::Restart => match child.tracker.record_failure(&child.config, Instant::now()) {
            GiveUp::Yes { rebuilds } => ControlFlow::Break(ActorStopReason::RestartLimitExceeded {
                child: notice.id,
                rebuilds,
            }),
            GiveUp::No { attempt } => {
                let delay = jittered_backoff(&child.config, attempt, rng);
                retries.insert(notice.id, delay);
                ControlFlow::Continue(())
            }
        },
    }
}
```

`rebuild_child`: run `factory()`, `children.rekey(old, new)` + `handle = Some(..)` + `tracker.record_started(Instant::now())`. Escalation's child-stop sweep is Task 12.

- [ ] **Step 4: Run the integration test** — PASS; then whole crate. (Loop fn count grows — check the 80-line/5-arg clippy caps; the channel/table params are grouped in structs as the existing `LinkedLoopChannels` pattern requires.)

- [ ] **Step 5: Commit** — `feat(supervision): Supervisor trait + supervised loop — rebuild on child death with backoff (#196)`

---

## Task 11: `ActorRef::supervise` / `supervise_cloned` — factory wrapper + no-pin proofs

**Files:**
- Modify: `bombay-core/src/actor/actor_ref.rs`
- Test: `bombay-core/tests/supervision.rs`

- [ ] **Step 1: Failing tests** — spec invariant 21 (no strong child ref) + policy table end-to-end (invariants 2/3/5):

```rust
/// Invariant 21: the supervisor holds only weak-ish edges — dropping the last
/// USER ref to a supervised child still triggers ref-count-driven stop.
#[tokio::test(start_paused = true)]
async fn supervised_child_still_refcount_stops() { /* supervise Quiet child; drop user ref; bounded-await its Stopped notice via a watcher */ }

/// Invariants 2+3: Permanent rebuilds a normal exit, Transient leaves it dead.
#[tokio::test(start_paused = true)]
async fn normal_exit_restart_depends_on_policy() { /* two children, one per policy, both self-stop; assert one respawn, one silence over a bounded window */ }

/// Invariant 5: Never never rebuilds, even on panic.
#[tokio::test(start_paused = true)]
async fn never_policy_leaves_panicked_child_dead() { /* ... */ }
```

(Write the three fully in the file — same probe pattern as Task 10's test.)

- [ ] **Step 2: Run** — FAIL (`supervise` absent).

- [ ] **Step 3: Implement** on `impl<S: Supervisor> ActorRef<S>`:

```rust
    /// Registers a supervised child. The FIRST incarnation is spawned here, in
    /// the caller's task — that is what lets this be a tell returning the
    /// `ActorId`. The closure re-runs per rebuild inside the supervisor's loop
    /// (spawn + optional registry re-binding live in it). The wrapper captures
    /// the supervisor's `ActorId` + `link_tx` clone ONLY — a strong self-ref
    /// here would pin the supervisor's own liveness (ADR-0003 / kameo #171).
    ///
    /// # Errors
    /// [`TellError`] if the supervisor's mailbox is closed/full-on-timeout;
    /// [`ActorNotLinked`]-class misuse is impossible (S: Supervisor ⇒ linked).
    pub async fn supervise<A, F>(
        &self,
        config: impl Into<RestartConfig>,
        mut factory: F,
    ) -> Result<ActorId, TellError<()>>
    where
        A: Actor,
        F: FnMut() -> ActorRef<A> + Send + 'static,
    {
        let config = config.into();
        let watcher = self.id();
        let link_tx = self
            .link_tx()
            .expect("S: Supervisor is spawned via spawn_supervised, which always creates the link channel")
            .clone();

        let wrapper = move || -> ChildHandle {
            let child = factory();
            install_watch_edge(&child, watcher, &link_tx);
            let handle = ChildHandle {
                id: child.id(),
                cancel: child.cancel_token().clone(),
                abort: child.abort_handle().clone(),
            };
            drop(child); // never pin the child either
            handle
        };
        // First incarnation now, in the caller's task:
        let mut wrapper = wrapper;
        let first = wrapper();
        let id = first.id();
        let child = Child {
            factory: Box::new(move || Box::pin(std::future::ready(wrapper()))),
            handle: Some(first),
            tracker: RestartTracker::new(Instant::now()),
            config,
        };
        self.mailbox_sender()
            .send(Signal::Supervision(Box::new(SupervisionOp::Add(SuperviseReg { child }))))
            .await
            .map_err(|_| TellError::ActorStopped)?; // exact variant per error.rs
        Ok(id)
    }

    pub async fn supervise_cloned<A: Actor>(
        &self,
        config: impl Into<RestartConfig>,
        args: A::Args,
    ) -> Result<ActorId, TellError<()>>
    where
        A::Args: Clone,
    {
        self.supervise(config, move || A::spawn(args.clone())).await
    }
```

`install_watch_edge` (in `supervision.rs`): builds `WatchReg { watcher, link_tx, linked: false }` and `try_send`s it — fresh mailbox, capacity ≥ 1, FIFO-first ⇒ deterministic success unless the user's closure leaked the ref into a flood; that fallback path sends bounded (`timeout(stop_grace, send)`), and on timeout kills the incarnation (`cancel` + `abort`) and returns a tombstone handle the tracker counts as an immediate failure. *(Implement the fallback exactly so; it is the audited anti-stall rule — no unbounded `watch().await` anywhere on the supervise path.)* Note the factory's `BoxFuture` stays: the fallback leg is async even though the happy path is ready-immediately.

- [ ] **Step 4: Run** — all supervision tests PASS; whole crate green.

- [ ] **Step 5: Commit** — `feat(actor-ref): supervise/supervise_cloned — caller-side first spawn, pin-free edges (#196)`

---

## Task 12: `unsupervise`, `stop_child`, escalation sweep

**Files:**
- Modify: `bombay-core/src/actor/actor_ref.rs` (two verbs)
- Modify: `bombay-core/src/actor/supervision.rs` + `kind.rs` (op handling, escalation stop sweep)
- Test: `bombay-core/tests/supervision.rs`

- [ ] **Step 1: Failing tests** — spec invariants 17, 20, 12, 13, 14, 16:

```rust
/// Invariant 17: unsupervise then die ⇒ no rebuild (#195 Unwatch-race carry-forward:
/// enforcement is the Children lookup, so even a late queued notice finds nothing).
#[tokio::test(start_paused = true)]
async fn unsupervised_child_death_does_not_rebuild() { /* supervise, unsupervise, panic child, assert no second on_start over bounded window */ }

/// Invariant 20: stop_child stops AND removes — no rebuild under Permanent.
#[tokio::test(start_paused = true)]
async fn stop_child_is_terminal_even_for_permanent() { /* ... */ }

/// Invariants 12+13+14: budget trip ⇒ siblings stopped without cooperation
/// (hanging on_stop child), supervisor dies RestartLimitExceeded, ITS watcher
/// hears it (the ladder's next rung).
#[tokio::test(start_paused = true)]
async fn escalation_stops_siblings_and_notifies_supervisors_watcher() { /* max_restarts=1 crasher + hanging sibling + outer watcher on the supervisor */ }

/// Invariant 16: a tell to the supervisor lands while a child waits out backoff.
#[tokio::test(start_paused = true)]
async fn supervisor_serves_messages_during_backoff() { /* min_backoff=30s; tell + ask roundtrip before rebuild fires */ }
```

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement:**

```rust
    /// Drops the supervision edge; the child keeps running, unwatched.
    pub async fn unsupervise(&self, id: ActorId) -> Result<(), TellError<()>> {
        self.send_supervision_op(SupervisionOp::Remove(id)).await
    }

    /// Drops the edge AND stops the child (cancel → stop_grace → abort) as one
    /// verb — OTP `terminate_child/2`. `kill()` alone fights the policy
    /// (`Killed` is abnormal ⇒ `Transient`/`Permanent` rebuild it).
    pub async fn stop_child(&self, id: ActorId) -> Result<(), TellError<()>> {
        self.send_supervision_op(SupervisionOp::Stop(id)).await
    }
```

Loop-side op handling (`kind.rs` intercept before `handle_mailbox_step`): `Remove` → `children.remove(id)` (also `retries` entry purge via `DelayQueue::remove` keyed map — keep the `delay_queue::Key` inside `Child` when queued); `Stop` → remove + `stop_child_handle(handle, stop_grace).await`.

`stop_child_handle` (in `supervision.rs`) — the crash-only sweep both verbs and escalation share:

```rust
/// External, cooperation-free stop: cancel (graceful window) → stop_grace →
/// abort. Bounded by construction — the crash-only power-off rule.
pub(crate) async fn stop_child_handle(handle: &ChildHandle, grace: Duration) {
    handle.cancel.cancel();
    tokio::time::sleep(grace).await;
    handle.abort.abort();
}
```

*(Escalation calls it per live child — sequential is fine at this fan-out; #199's set engine revisits.)* Escalation in the loop's `Break(RestartLimitExceeded)` path: before returning the reason, sweep `children` and stop every live handle; the reason then flows to `finish_actor` → `Watchers::drop` → the supervisor's own watcher (invariant 12 needs zero new code beyond the sweep).

- [ ] **Step 4: Run** — PASS; full crate; `cargo build --workspace --all-targets`.

- [ ] **Step 5: Commit** — `feat(supervision): unsupervise + stop_child + escalation sweep (#196)`

---

## Task 13: Remaining invariant tests (6, 8-timing, 9, 10, 11, 15)

**Files:**
- Test: `bombay-core/tests/supervision.rs`

- [ ] **Step 1..4:** One test per remaining spec invariant, each written failing-first against a deliberately broken expectation, then confirmed green (they exercise paths built above; "failing first" here = assert the *specific* value, run, and only accept green with the exact expected numbers):

```rust
/// Invariant 6: AlreadyDead rebuilds under Transient and burns budget.
#[tokio::test(start_paused = true)]
async fn already_dead_notice_rebuilds_and_counts() { /* supervise a child that is ALREADY stopped before the edge lands: factory spawns, then immediately cancel+abort before returning — reg queued at a dead mailbox ⇒ synthetic AlreadyDead ⇒ assert a rebuild attempt happens */ }

/// Invariant 8 (timing): deadlines are min·2^(n-1) capped, exact under seed.
#[tokio::test(start_paused = true)]
async fn backoff_deadlines_exact_under_paused_clock() { /* jitter 0; crasher with max_restarts=4; record Instant::now() at each on_start; assert gaps 100ms/200ms/400ms */ }

/// Invariant 9: healthy uptime resets the ladder to min_backoff.
#[tokio::test(start_paused = true)]
async fn healthy_uptime_resets_backoff_ladder() { /* fail twice, run healthy past reset_after, fail again ⇒ gap back to 100ms */ }

/// Invariant 10: budget trip reason carries child + rebuild count.
#[tokio::test(start_paused = true)]
async fn budget_trip_reason_names_child_and_count() { /* outer watcher asserts RestartLimitExceeded { child, rebuilds: max+1 } */ }

/// Invariant 11 (integration): slow drip trips max_total.
#[tokio::test(start_paused = true)]
async fn slow_drip_trips_lifetime_budget_end_to_end() { /* max_total=2, reset_after tiny; child dies each time after healthy uptime */ }

/// Invariant 15: OneForOne isolation — sibling ids stable across a rebuild.
#[tokio::test(start_paused = true)]
async fn sibling_survives_one_for_one_rebuild() { /* two children; crash one; sibling's ActorId unchanged + still answers */ }
```

(Full bodies in-file; every await bounded; auto-advance does the time travel.)

- [ ] **Step 5: Commit** — `test(supervision): invariant sweep — backoff timing, budgets, isolation (#196)`

---

## Task 14: Gate & card hygiene

**Files:**
- Modify: `mutants-baseline.json` (entries for every new fn: `should_restart`, `base_backoff`, `jittered_backoff`, `RestartTracker::*`, `Children::*`, `handle_child_death`, `rebuild_child`, `stop_child_handle`, `install_watch_edge`, `supervise*`, `reject_queued_watchers`, builder `with_*` — mark `known_zero_viable` only where genuinely non-observable)
- Modify: `README.md` (public-API-changed case: `Supervisor`/`SpawnSupervised`/`supervise`/`stop_child`/`RestartPolicy`/`RestartConfig` bullet + one usage line)
- Modify: `docs/testing/coverage-baseline.md` (new test file + counts)
- Create: `docs/adr/0012-restart-accounting-counters-not-window.md`, `docs/adr/0013-virtual-actors-not-core.md` (content = the spec's ADR paragraphs, ADR template as 0011)

- [ ] **Step 1:** ADRs + README + coverage baseline. **Step 2:** mutants baseline entries; then targeted sweep with the mandatory timeout:

```bash
nix build .#mutants 2>/dev/null || cargo mutants -p bombay-core --file 'bombay-core/src/restart.rs' --file 'bombay-core/src/actor/supervision.rs' --timeout 60
```

Expected: zero viable survivors, zero TIMEOUTs (a TIMEOUT = an unbounded test await — fix the test, memory `card-148`/`#179`).

- [ ] **Step 3: The single gate** — everything tracked first:

```bash
cargo fmt --all
git add -A
nix flake check
```

Expected: green, with real derivations logged (`building '...drv'` — silent = cached).

- [ ] **Step 4: Commit + PR**

```bash
git commit -m "core(supervision): restart — explicit RestartPolicy, backoff+budgets, escalation (#196)"
git push -u origin feat/196-restart-supervision   # HTTPS if SSH times out (memory)
gh pr create --repo devrandom-labs/bombay --title "core(supervision): restart — RestartPolicy, backoff, budgets, escalation (#196)" --body-file <(...)
```

PR body: per-invariant checklist mapping test names ↔ spec numbers; the two prerequisite behavior changes (startup reason, teardown reorder) called out; deferrals: none new (all on #199). No Claude attribution. Merge only on green `Nix Flake Check` (ruleset).

---

## Self-review notes (done at write time)

- **Spec coverage:** 21 invariants → Tasks 1 (inv 8-startup), 2–3 (18, 19), 4–7 (2–7, 9–11 pure), 10 (1, 4), 11 (2, 3, 5, 21), 12 (12, 13, 14, 16, 17, 20), 13 (6, 8, 9, 10, 11, 15). Wiring (deps, DelayQueue, fastrand, ADRs, README, mutants) → Tasks 0, 14. Invariant numbering follows the spec list.
- **Known judgment calls surfaced to the implementer:** `Children::insert` id-explicit form preferred; `TellError` variant name must match `error.rs` (check before use); the `Instant::now()` calls inside loop glue are tokio-time (paused-aware) — never `std::time::Instant`.
- **Clippy caps:** the supervised loop adds params — group in structs (existing `LinkedLoopChannels` pattern) to stay ≤ 5 args; keep fns ≤ 80 lines by extracting `handle_child_death`/`rebuild_child`/op-handling as free fns (also what makes them mutation-testable).
