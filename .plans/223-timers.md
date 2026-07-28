# Card #223 — timer surface: `send_after` / `send_interval` + `TimerHandle`

## Context

Design record (read first): `docs/superpowers/specs/2026-07-28-223-timers-design.md`.
Precedent module (read second, copy its idioms): `crates/core/src/actor/pipe.rs`
(weak-hold task, ExitGuard test pattern, fate-table doc comments).

Invariants that must hold (from the spec, enforced by the tests below):

1. Fired message = ordinary `A::Msg` menu variant through the bounded mailbox.
2. Armed timer holds ONLY a weak handle — never pins ref-count stop (ADR-0003).
   This is a deliberate deviation from kameo v0.22.2 (which pins via a strong
   clone).
3. Cancel is sleep-phase-atomic: cancel that lands before sleep expiry NEVER
   delivers and reaps the task; once the sleep completes, the send runs to
   completion un-cancellable (`CancellationToken` + `select!`; NEVER
   `JoinHandle::abort` — aborting mid-send hits flume's indeterminate-cancel
   window, ADR-0008).
4. Full mailbox = backpressure: fire awaits capacity via the ordinary awaiting
   `tell`; delivered late, never lost. Strong ref held only for the send.
5. Interval arms the next tick only AFTER the prior tick's message is enqueued
   (arm-after-enqueue). No burst catch-up. Fresh message per tick via `FnMut`.
6. Interval self-reaps when the target dies (upgrade failure or closed-mailbox
   send error exits the loop).
7. Nothing fallible in the public surface: scheduling returns `TimerHandle`,
   not `Result`. Later failures are drop-plus-trace fates.
8. Dropping `TimerHandle` detaches (timer fires anyway). `cancel()` idempotent.

Engineering rules bite here:
- No bare arithmetic anywhere (there is none needed — do not add tick counters
  that do arithmetic in production paths).
- `tracing` calls go through `crate::trace` ONLY — call sites stay cfg-free.
  New trace fns must be added to BOTH halves of `crates/core/src/trace.rs`
  (the `#[cfg(feature = "tracing")]` impl AND the `inert_trace_surface!` macro
  in the off half) and to the `pub use` list at the bottom.
- Tests: every await bounded (use `terminate_bound()` from
  `crate::test_support`); no yield-spin loops under `start_paused` (known
  pitfall: a yield spin pins the clock and wrapping timeouts never fire —
  sleep-poll loops are fine, they let auto-advance run).
- All `use` imports at top of file. `let ... else` over `if let ... else`.
  Comments only for why, never what.

## Steps

### Step 1 — trace events (SEQUENTIAL, first)

**Modify:** `crates/core/src/trace.rs`

Three new events, non-generic, keyed by `ActorId` (both `ActorRef` and the
erased `Recipient` expose `id()` — follow the `restart_scheduled(child: ActorId, ...)`
style, not the generic `pipe_result_dropped::<A>()` style):

In the `#[cfg(feature = "tracing")]` half (after `pipe_result_dropped`):

```rust
pub fn timer_cancelled(target: ActorId) {
    tracing::trace!(target.id = ?target, "timer cancelled before fire");
}

pub fn timer_fire_dropped(target: ActorId) {
    tracing::debug!(target.id = ?target, "timer fired after target stopped; message dropped");
}

pub fn timer_factory_panicked(target: ActorId) {
    tracing::error!(target.id = ?target, "send_interval message factory panicked; timer stopped");
}
```

In the `inert_trace_surface!` macro (off half), same signatures as
`pub const fn` no-ops:

```rust
pub const fn timer_cancelled(_target: ActorId) {}
pub const fn timer_fire_dropped(_target: ActorId) {}
pub const fn timer_factory_panicked(_target: ActorId) {}
```

Add all three to the `pub use imp::{...}` list at the bottom of the file.

Verify: `cargo check -p bombay` and `cargo check -p bombay --no-default-features`
(if `tracing` is a default feature; otherwise `--features tracing` and bare).
Both must compile.

### Step 2 — failing tests for `send_after` core (SEQUENTIAL)

**Create:** `crates/core/src/actor/timer.rs` (module with a `#[cfg(test)] mod tests`
containing the tests below and only enough non-test skeleton to name the API),
**Modify:** `crates/core/src/actor/mod.rs` (add `mod timer;` next to `mod pipe;`
and `timer::TimerHandle` to the `pub use self::{...}` block).

Reuse the `Sink`/`SinkMsg`/`wait_for_seen`/`ExitGuard` shapes from
`pipe.rs` tests — copy them into `timer.rs`'s test module and trim to what the
timer tests need (a `Tick(u32)` + `Read(ReplySender<Vec<u32>>)` menu is
enough; call the actor `Sink` again, module-local).

Write these tests FIRST, watch each fail (unresolved names), then implement:

```rust
/// Invariant 1: the delayed message arrives as the exact menu value, through
/// the mailbox, after the delay — and not before.
#[tokio::test(start_paused = true)]
async fn send_after_fires_exact_value_after_delay() {
    let actor_ref = Sink::spawn(());
    let _handle = actor_ref.send_after(Duration::from_secs(10), SinkMsg::Tick(42));

    // Before the deadline nothing may arrive: drain the runtime briefly at
    // a time strictly inside the delay window.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let seen = actor_ref.ask(|reply| SinkMsg::Read(reply)).await.expect("alive");
    assert_eq!(seen, Vec::<u32>::new(), "nothing may fire before the deadline");

    // Cross the deadline; the tick must land.
    let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 1))
        .await
        .expect("fired tick must arrive once the deadline passes");
    assert_eq!(seen, vec![42], "the exact scheduled value round-trips");
}

/// Invariant 3 + reaping: cancel before the deadline — advancing far past the
/// deadline afterwards delivers NOTHING and the task exits.
#[tokio::test(start_paused = true)]
async fn cancel_before_fire_never_delivers_and_reaps_task() {
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
    let actor_ref = Sink::spawn(());
    let handle = actor_ref.send_after_probed(
        Duration::from_secs(10),
        SinkMsg::Tick(1),
        ExitGuard(Some(exit_tx)),
    );
    handle.cancel();

    tokio::time::timeout(terminate_bound(), exit_rx)
        .await
        .expect("cancelled timer task must exit within the bound")
        .expect("guard dropped, not leaked");
    tokio::time::sleep(Duration::from_secs(60)).await; // far past the deadline
    let seen = actor_ref.ask(|reply| SinkMsg::Read(reply)).await.expect("alive");
    assert_eq!(seen, Vec::<u32>::new(), "a cancelled timer must never deliver");
}

/// Invariant 3, fired edge: cancel AFTER the deadline is a no-op — the
/// message still arrives (it is ordinary mail by then).
#[tokio::test(start_paused = true)]
async fn cancel_after_fire_is_noop() {
    let actor_ref = Sink::spawn(());
    let handle = actor_ref.send_after(Duration::from_secs(1), SinkMsg::Tick(7));
    let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 1))
        .await
        .expect("tick arrives");
    handle.cancel(); // idempotent + late: both must be harmless
    handle.cancel();
    assert_eq!(seen, vec![7]);
}

/// Invariant 2: an actor whose ONLY remaining tie is an armed timer still
/// ref-count-stops (the timer holds a weak handle; kameo deviation).
#[tokio::test]
async fn armed_timer_does_not_pin_refcount_stop() {
    let actor_ref = Sink::spawn(());
    let _handle = actor_ref.send_after(Duration::from_secs(3600), SinkMsg::Tick(1));
    let weak = actor_ref.downgrade();
    drop(actor_ref);

    tokio::time::timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("with only an armed timer left, the actor must ref-count-stop");
}

/// Invariant 7 fate: target dead at fire — no panic, no delivery, task exits.
#[tokio::test(start_paused = true)]
async fn dead_target_at_fire_drops_cleanly() {
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
    let actor_ref = Sink::spawn(());
    let _handle = actor_ref.send_after_probed(
        Duration::from_secs(10),
        SinkMsg::Tick(1),
        ExitGuard(Some(exit_tx)),
    );
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    tokio::time::timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actor stops");

    // Cross the deadline; the task must exit cleanly (upgrade fails, drop+trace).
    tokio::time::timeout(terminate_bound(), exit_rx)
        .await
        .expect("timer task must exit after firing at a dead target")
        .expect("guard dropped, not leaked");
}
```

Notes for the implementer:
- `send_after_probed` is `#[cfg(test)]`-only sugar on `ActorRef<A>` (defined in
  Step 3) that threads an `ExitGuard` into the timer task so tests can observe
  task exit — mirror how `pipe.rs` tests capture the guard in the mapper, but
  since `send_after` takes a value (no closure), the probe variant is needed.
  Keep it `#[cfg(test)]` in `timer.rs`; it is NOT public API.
- `start_paused` + `sleep` is safe (auto-advance); do NOT add yield-spin loops.

Run: `cargo nextest run -p bombay timer` → all new tests FAIL to compile
(missing `send_after`). That is the required starting state.

### Step 3 — implement `send_after` + `TimerHandle` (SEQUENTIAL)

**Modify:** `crates/core/src/actor/timer.rs`

Shape (the *what*; line-level *how* is yours):

```rust
//! send_after / send_interval (card #223): the sanctioned non-pinning timer
//! surface. Design record: docs/superpowers/specs/2026-07-28-223-timers-design.md,
//! ADR-0018.

use core::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, ActorRef, Recipient, WeakActorRef, WeakRecipient},
    id::ActorId,
    trace,
};

/// Cancel handle for a scheduled send. Dropping it detaches (the timer still
/// fires); cancellation is only ever explicit.
#[derive(Debug)]
pub struct TimerHandle {
    token: CancellationToken,
}

impl TimerHandle {
    /// Cancels the timer. Idempotent. Wins iff it lands before the sleep
    /// expires: a cancelled-before-fire timer never delivers; once the sleep
    /// has completed the send is committed and `cancel` is a no-op.
    pub fn cancel(&self) {
        self.token.cancel();
    }
}
```

Private target abstraction so `ActorRef` and `Recipient` share one task body
(one trait, two impls, no public surface — generalizes `spawn_pipe`'s weak
pattern to erased targets):

```rust
/// A weak, upgradeable delivery target. `fire` upgrades, sends with
/// backpressure, and reports whether the target can ever accept again.
trait WeakTarget: Send + 'static {
    type M: Send + 'static;
    fn id(&self) -> ActorId;
    async fn fire(&self, msg: Self::M) -> core::ops::ControlFlow<()>;
}

impl<A: Actor> WeakTarget for WeakActorRef<A> { /* upgrade → tell(msg).await */ }
impl<M: Send + 'static> WeakTarget for WeakRecipient<M> { /* upgrade → tell(msg).await */ }
```

`fire` fate handling (both impls): upgrade failure → `trace::timer_fire_dropped(self.id())`,
`Break(())`. Send error (mailbox closed in the kill race) → same trace, `Break(())`.
Success → `Continue(())`. The strong ref must live only across the send await.

The one-shot task:

```rust
fn spawn_once<W: WeakTarget>(weak: W, delay: Duration, msg: W::M) -> TimerHandle {
    let token = CancellationToken::new();
    let task_token = token.clone();
    let _join = tokio::spawn(async move {
        tokio::select! {
            biased;
            () = task_token.cancelled() => {
                trace::timer_cancelled(weak.id());
                return;
            }
            () = tokio::time::sleep(delay) => {}
        }
        // Fired: from here the send is committed — cancellation no longer
        // applies (aborting mid-send would hit flume's indeterminate-cancel
        // window, ADR-0008).
        let _ = weak.fire(msg).await;
    });
    TimerHandle { token }
}
```

`biased` + cancelled-first: when cancel and deadline race to the same instant,
cancel wins deterministically — the paused-clock tests rely on that edge being
fixed, not scheduler-dependent.

Public verbs:

```rust
impl<A: Actor> ActorRef<A> {
    /// Delivers `msg` to this actor after `delay`, as an ordinary menu
    /// message through the mailbox. The armed timer holds only a weak
    /// handle — it never keeps the actor alive (ADR-0003). Full mailbox at
    /// fire time = backpressure: delivered late, never lost.
    #[must_use = "dropping the handle detaches the timer; keep it to be able to cancel"]
    pub fn send_after(&self, delay: Duration, msg: A::Msg) -> TimerHandle {
        spawn_once(self.downgrade(), delay, msg)
    }
}

impl<M: Send + 'static> Recipient<M> {
    /// [`ActorRef::send_after`] for the erased tell-side handle.
    #[must_use = "dropping the handle detaches the timer; keep it to be able to cancel"]
    pub fn send_after(&self, delay: Duration, msg: M) -> TimerHandle {
        spawn_once(self.downgrade(), delay, msg)
    }
}
```

(Check the exact inherent-impl bounds `recipient.rs` uses for `Recipient<M>`
methods and match them — do not invent stricter ones.)

`send_after_probed` (test-only, next to the tests):

```rust
#[cfg(test)]
impl<A: Actor> ActorRef<A> {
    /// Test seam: like `send_after` but holds `probe` inside the timer task so
    /// tests observe task exit via the guard's Drop.
    fn send_after_probed<G: Send + 'static>(
        &self,
        delay: Duration,
        msg: A::Msg,
        probe: G,
    ) -> TimerHandle { /* same as send_after but the spawned future owns `probe` */ }
}
```

Simplest honest implementation: make `spawn_once` take an optional probe, or
duplicate the small task body in the probed variant — your call; keep the
production path free of test-only branches if you can do so without
duplication, otherwise prefer a `#[cfg(test)]` duplicate of the task body over
polluting `spawn_once`.

Run: `cargo nextest run -p bombay timer` → Step 2 tests PASS.
Then `cargo fmt` and commit: `feat(timer): send_after + TimerHandle — non-pinning delayed send [#223]`

### Step 4 — failing tests for `send_interval` (SEQUENTIAL)

**Modify:** `crates/core/src/actor/timer.rs` (test module)

```rust
/// Interval delivers fresh messages per tick at the period cadence.
#[tokio::test(start_paused = true)]
async fn interval_ticks_arrive_with_fresh_messages() {
    let actor_ref = Sink::spawn(());
    let mut n = 0u32;
    let handle = actor_ref.send_interval(Duration::from_secs(1), move || {
        n += 1;
        SinkMsg::Tick(n)
    });
    let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 3))
        .await
        .expect("three ticks arrive");
    handle.cancel();
    assert_eq!(seen[..3], [1, 2, 3], "factory runs once per tick, in order");
}

/// Invariant 5 (arm-after-enqueue) + invariant 4 boundary: with consumption
/// blocked and the mailbox full, ticks do NOT pile up beyond the structural
/// bound (one in the handler + capacity queued + one awaiting enqueue). No
/// burst catch-up after the stall clears.
#[tokio::test(start_paused = true)]
async fn interval_does_not_overlap_or_burst_when_mailbox_full() {
    // Gated sink with capacity 1: handler blocks until the gate opens.
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let actor_ref = GatedSink::spawn_with_capacity(gate_rx, 1);
    let mut n = 0u32;
    let handle = actor_ref.send_interval(Duration::from_secs(1), move || {
        n += 1;
        GatedMsg::Tick(n)
    });

    // 10 periods pass while consumption is blocked. Arm-after-enqueue means
    // at most: 1 tick in the blocked handler + 1 queued + 1 awaiting
    // capacity = ticks 1..=3 ever created; a free-running/bursting interval
    // would have created ~10.
    tokio::time::sleep(Duration::from_secs(10)).await;
    gate_tx.send(()).expect("handler is waiting on the gate");

    let seen = tokio::time::timeout(terminate_bound(), gated_wait_for_seen(&actor_ref, 3))
        .await
        .expect("blocked ticks drain after the gate opens");
    handle.cancel();
    assert!(
        seen.len() <= 4,
        "arm-after-enqueue bounds queued ticks structurally, got {}: {seen:?}",
        seen.len(),
    );
    assert_eq!(seen[..3], [1, 2, 3], "ticks stay ordered, none replayed or skipped-then-burst");
}

/// Invariant 6: the interval loop reaps itself when the target dies.
#[tokio::test(start_paused = true)]
async fn interval_self_reaps_on_target_death() {
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
    let actor_ref = Sink::spawn(());
    let _handle = actor_ref.send_interval_probed(
        Duration::from_secs(1),
        || SinkMsg::Tick(1),
        ExitGuard(Some(exit_tx)),
    );
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    tokio::time::timeout(terminate_bound(), exit_rx)
        .await
        .expect("interval task must reap itself once the target is gone")
        .expect("guard dropped, not leaked");
    drop(weak);
}

/// D7 containment: a panicking factory kills only the timer task (traced);
/// the actor keeps running.
#[tokio::test(start_paused = true)]
async fn interval_factory_panic_kills_timer_not_actor() {
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
    let actor_ref = Sink::spawn(());
    let _handle = actor_ref.send_interval_probed(
        Duration::from_secs(1),
        || -> SinkMsg { panic!("factory boom") },
        ExitGuard(Some(exit_tx)),
    );
    tokio::time::timeout(terminate_bound(), exit_rx)
        .await
        .expect("timer task must exit on factory panic")
        .expect("guard dropped, not leaked");
    assert!(actor_ref.is_alive(), "a factory panic must never touch the actor");
}
```

`GatedSink` is a second module-local test actor: `Args = oneshot::Receiver<()>`,
menu `enum GatedMsg { Tick(u32), Read(ReplySender<Vec<u32>>) }`, handler takes
the gate on the FIRST `Tick` only (same `Option<Receiver>` take-pattern as
`GatedB` in `pipe.rs`), records every tick. `spawn_with_capacity(args, n)` —
use `PreparedActor::new(Capacity::try_from(n).expect("valid test capacity"))`
+ `.spawn(args)` (see `spawn.rs` tests around line 3868 for the working
pattern; wrap it in a small helper in the test module).
`gated_wait_for_seen` = `wait_for_seen` for the second actor (or make
`wait_for_seen` generic over a closure that reads — your call, DRY it).

Run: `cargo nextest run -p bombay timer` → new tests fail (missing
`send_interval`). Required starting state.

### Step 5 — implement `send_interval` (SEQUENTIAL)

**Modify:** `crates/core/src/actor/timer.rs`

```rust
fn spawn_interval<W, F>(weak: W, period: Duration, mut make_msg: F) -> TimerHandle
where
    W: WeakTarget,
    F: FnMut() -> W::M + Send + 'static,
{
    let token = CancellationToken::new();
    let task_token = token.clone();
    let _join = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = task_token.cancelled() => {
                    trace::timer_cancelled(weak.id());
                    return;
                }
                () = tokio::time::sleep(period) => {}
            }
            // Fresh message per tick; a panicking factory kills only this
            // task (traced), never the actor (spec D7).
            let Ok(msg) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut make_msg))
            else {
                trace::timer_factory_panicked(weak.id());
                return;
            };
            // Arm-after-enqueue: the next sleep starts only after this send
            // completed (backpressure included) — no burst catch-up.
            if weak.fire(msg).await.is_break() {
                return;
            }
        }
    });
    TimerHandle { token }
}
```

Public verbs on `ActorRef<A>` (`F: FnMut() -> A::Msg + Send + 'static`) and
`Recipient<M>` (`F: FnMut() -> M + Send + 'static`), same doc-comment style as
`send_after`, both `#[must_use]`. Plus `#[cfg(test)] send_interval_probed`
mirroring `send_after_probed`.

Clippy note: `catch_unwind(AssertUnwindSafe(&mut make_msg))` — if the closure
call needs an explicit invocation wrapper, `|| make_msg()` inside
`AssertUnwindSafe` is fine; keep the banned-method list in mind (no
`std::thread::spawn`, no `std::process::exit`).

Run: `cargo nextest run -p bombay timer` → Step 4 tests PASS.
`cargo fmt`, commit: `feat(timer): send_interval — arm-after-enqueue periodic send [#223]`

### Step 6 — Recipient smoke tests (PARALLEL OK with Step 7 after Step 5; same file as Steps 2/4 though — treat as SEQUENTIAL in practice)

**Modify:** `crates/core/src/actor/timer.rs` (test module)

```rust
/// The erased tell-side handle gets the same verbs: fire through a
/// `Recipient<u32>` (menu conversion via `From`), cancel still works.
#[tokio::test(start_paused = true)]
async fn recipient_send_after_fires_through_erasure() {
    let actor_ref = Sink::spawn(());
    let recipient: Recipient<u32> = actor_ref.recipient();
    let _handle = recipient.send_after(Duration::from_secs(1), 5u32);
    let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 1))
        .await
        .expect("erased tick arrives");
    assert_eq!(seen, vec![5]);
}

/// Recipient interval + cancel-before-fire on the erased path.
#[tokio::test(start_paused = true)]
async fn recipient_interval_and_cancel() {
    let actor_ref = Sink::spawn(());
    let recipient: Recipient<u32> = actor_ref.recipient();
    let mut n = 0u32;
    let handle = recipient.send_interval(Duration::from_secs(1), move || {
        n += 1;
        n
    });
    let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 2))
        .await
        .expect("two erased ticks arrive");
    handle.cancel();
    assert_eq!(seen[..2], [1, 2]);
}
```

Requires `SinkMsg: From<u32>` in the test module (map to `Tick`) — the
`Recipient` conversion boundary (ADR-0004). Check `actor_ref.recipient::<M>()`
signature in `recipient.rs:238` and use it exactly.

Run: `cargo nextest run -p bombay timer` → red first (missing `From`), then
green. Commit: `test(timer): recipient-path smoke coverage [#223]`

### Step 7 — bench: armed-timer cost (PARALLEL OK after Step 5 — disjoint files)

**Create:** `crates/core/benches/timers.rs`
**Modify:** `crates/core/Cargo.toml` (add a `[[bench]]` entry matching the
existing seven — same `harness` setting)

Copy the harness style from `crates/core/benches/channels.rs` (same criterion
setup, same iteration discipline: setup outside the measured closure). Two
measurements, names exactly:

1. `arm_send_after_10k` — measured body: arm 10_000 `send_after` timers with a
   long delay (e.g. 3600s) against one spawned `Sink`-style bench actor;
   measure arming cost only (the timers never fire inside the measurement).
   Drop handles after each iteration outside the timing loop if the harness
   allows; leaking armed timers across iterations is acceptable for a
   long-delay bench but note it in a comment.
2. `arm_delay_queue_insert_10k` — baseline: `tokio_util::time::DelayQueue`,
   10_000 `insert(i, 3600s)` calls, measured the same way.

These two numbers go into ADR-0018's D3 section verbatim (the card requires
the tradeoff recorded "with numbers").

Run: `cargo bench -p bombay --bench timers -- --quick` (or nextest-equivalent
smoke: `cargo check --benches -p bombay` at minimum must pass).
Commit: `bench(timer): armed-timer cost vs DelayQueue insert baseline [#223]`

### Step 8 — ADR-0018 (PARALLEL OK after Step 7 — needs the bench numbers)

**Create:** `docs/adr/0018-timer-surface.md`

Distill from `docs/superpowers/specs/2026-07-28-223-timers-design.md` — same
section style as `docs/adr/0017-pipe-to-self-not-reentrancy.md` (Context /
Options considered / Decision / Consequences / Fate table). Must contain:

- D3 per-task-vs-shared-wheel decision WITH the two bench numbers from Step 7.
- D5 cancel model (sleep-phase-atomic, why not Pekko generation filtering,
  ADR-0008 flume link).
- D6 full-mailbox policy (backpressure, deliver late, never lost).
- D7 arm-after-enqueue interval semantics (vs Pekko fixed-rate burst / Orleans
  arm-after-processing).
- The kameo v0.22.2 strong-ref deviation (cite `src/request/tell.rs:133-141`).
- Deferred: receive-timeout (follow-up card), named-key timers, message
  deadlines. The fate table from the spec, verbatim.

Commit: `docs(adr): ADR-0018 timer surface [#223]`

### Step 9 — README bullet + coverage baseline (SEQUENTIAL, last content step)

**Modify:** `README.md` — add ONE bullet to the public-API-at-a-glance section:
timers (`send_after` / `send_interval` on `ActorRef` and `Recipient`,
`TimerHandle::cancel`, non-pinning, cancel-before-fire guaranteed).
**Modify:** `docs/testing/coverage-baseline.md` — add the `timer.rs` test
inventory (the tests from Steps 2/4/6, one line each).

Commit: `docs: README timer bullet + coverage baseline [#223]`

### Step 10 — mutants baseline + full gate (SEQUENTIAL, final)

1. `git add -A` first (nix checks source from the git tree — untracked files
   pass vacuously).
2. Run `nix build .#mutants 2>&1 | tail -40` — it will list Unaccounted
   mutants for the new fns.
3. Add every new fn to `mutants-baseline.json` `floors` (keys look like
   `crates/core/src/actor/timer.rs::ActorRef<A>::send_after`; copy the exact
   names from the gate output). If a fn's mutants are all caught, its floor is
   the caught count; a fn with structurally-unkillable mutants goes to
   `known_zero_viable` ONLY with a comment-worthy reason (prefer killing).
   Trace fns from Step 1: the tracing-on half fns get entries like the
   existing `pipe_result_dropped` ones (check how those are keyed and mirror).
4. Re-run `nix build .#mutants` until the gate is green.
5. `nix flake check` — the single gate; must be green.

Commit: `chore(mutants): baseline entries for timer surface [#223]`

## Verification (must all pass; quote output)

```
cargo nextest run -p bombay timer     # all timer tests green
cargo nextest run -p bombay           # nothing else broken
cargo check --benches -p bombay
nix build .#mutants                   # baseline gate green
nix flake check                       # THE gate
```

## Out of scope — do NOT touch

- `kind.rs` / `spawn.rs` run loop (receive-timeout is a separate card).
- `pipe.rs` (shared shape is precedent, not a refactor target — do not
  generalize `spawn_pipe` to take tokens).
- Named-key timers, message deadlines, immediate-first-tick options.
- No new error variants, no `Result` in the timer API.
- No lint relaxation, no `clippy.toml` edits, no `#[allow]` without `reason`.
- Do not commit `Cargo.lock` changes unless a dependency actually changed
  (none should — `tokio-util` and `tokio` are already deps).

## Execution tier

k3 (design judgment in the `WeakTarget` trait, select semantics, paused-clock
tests). Not prewalk. Fix rounds continue the same session with `-c`.
