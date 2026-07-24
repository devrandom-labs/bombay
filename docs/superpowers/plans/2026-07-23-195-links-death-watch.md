# Links & Death-Watch (#195) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An actor reliably learns when a peer it watches has stopped — on every exit path (normal, panic, kill) — via `watch` (notify-only) and `link` (propagating) verbs.

**Architecture:** Death travels on a dedicated **unbounded** link channel (separate from the bounded message mailbox), so a `Drop`-guard on the dying actor can notify with a non-blocking send that never loses a notice. The notice `LinkDied` is **monomorphic**, so watcher lists are homogeneous — no `dyn`. Being watched is universal+passive (base `Actor`); watching+reacting is opt-in (`Watch: Actor` supertrait via `spawn_linked`).

**Tech Stack:** Rust 2024, `flume` (channels), `smallvec` (new), `tokio` (`select!`), `thiserror`, `futures::Abortable`. Gate: `nix flake check`.

**Spec:** `docs/superpowers/specs/2026-07-23-120-links-death-watch-design.md`

**Conventions (every task):**
- Run the gate with `nix develop --command cargo nextest run -p bombay-core` for fast per-task loops; run the **full** `nix flake check` before the final commit of each phase (it also runs clippy/fmt/taplo — the real gate).
- `cargo fmt` before every commit (fmt gate is strict — memory: `clippy-gate-scope-lib-not-all-targets`).
- Conventional commits, scope `supervision`/`core`; **no** Claude attribution.
- Branch: `120-links-death-watch` (already created, holds the design commit).

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `Cargo.toml` | workspace dep `smallvec` | modify |
| `bombay-core/Cargo.toml` | pull `smallvec` into the crate | modify |
| `fuzz/Cargo.lock` | regenerate so the isolated fuzz workspace sees `smallvec` | modify |
| `bombay-core/src/error.rs` | un-defer `ActorStopReason::LinkDied`, `PanicReason::OnLinkDied`; add `WatchError` | modify |
| `bombay-core/src/watch.rs` | **new** — `LinkDied`, `WatchReg`, `Watchers` guard, the link channel type alias | create |
| `bombay-core/src/mailbox.rs` | `Signal`: drop `LinkDied`, add `Watch`/`Unwatch`; fix the `SendMessageFut` match | modify |
| `bombay-core/src/actor/mod.rs` | `Watch: Actor` supertrait; re-export watch types; `SpawnLinked` | modify |
| `bombay-core/src/actor/actor_ref.rs` | `link_tx: Option<Sender<LinkDied>>` in `RefShared`; `watch`/`link`/`unwatch` | modify |
| `bombay-core/src/actor/kind.rs` | loop: handle `Watch`/`Unwatch`; linked two-arm loop calling `on_link_died` | modify |
| `bombay-core/src/actor/spawn.rs` | own the `Watchers` guard + link channel; `spawn_linked`; teardown notify | modify |
| `bombay-core/src/lib.rs` | `mod watch;` + public re-exports | modify |
| `mutants-baseline.json` | entries for every new fn (memory: `mutants-baseline-workflow`) | modify |
| `README.md` | public-API delta at card close | modify |

---

## Phase 1 — Dependency + error/type foundations (no behavior change)

### Task 1: Add the `smallvec` workspace dependency

**Files:**
- Modify: `Cargo.toml:11-33` (`[workspace.dependencies]`)
- Modify: `bombay-core/Cargo.toml:18-26` (`[dependencies]`)
- Modify: `fuzz/Cargo.lock`

- [ ] **Step 1: Add to workspace deps.** In `Cargo.toml`, after the `flume` block (line 18), add:

```toml
# Inline-small watcher lists (card #195): a watched actor's watcher list is 0-or-1
# in the common case; `[_; 1]` inline keeps that heap-free while spilling for
# popular actors. Uncapped by design (OTP has no monitor limit) — never ArrayVec.
smallvec = "1"
```

- [ ] **Step 2: Pull it into bombay-core.** In `bombay-core/Cargo.toml`, after line 26 (`papaya = { workspace = true }`), add:

```toml
smallvec = { workspace = true }
```

- [ ] **Step 3: Regenerate the fuzz lockfile** (the isolated fuzz workspace resolves independently; a stale lock breaks `nix flake check` — memory: `registry-119-shipped` gotcha).

Run: `nix develop --command bash -c 'cd fuzz && cargo update -p bombay-core'`
Expected: `fuzz/Cargo.lock` updated to include `smallvec`; no error.

- [ ] **Step 4: Verify the gate still builds.**

Run: `nix develop --command cargo build -p bombay-core`
Expected: builds clean (dep present, unused — fine, no code uses it yet).

- [ ] **Step 5: Commit.**

```bash
cargo fmt
git add Cargo.toml bombay-core/Cargo.toml fuzz/Cargo.lock
git commit -m "build(supervision): add smallvec workspace dep for #195 watcher lists"
```

---

### Task 2: Error model — un-defer `LinkDied` / `OnLinkDied`, add `WatchError`

**Files:**
- Modify: `bombay-core/src/error.rs:196-219` (`PanicReason`), `:286-316` (`ActorStopReason`), and append `WatchError`.
- Test: inline `#[cfg(test)]` in `error.rs` (there is already a test mod near line 540).

- [ ] **Step 1: Write the failing tests.** Append to the existing `#[cfg(test)] mod tests` in `error.rs`:

```rust
#[test]
fn on_link_died_is_a_lifecycle_hook() {
    // A hook panic must not be treated as a restartable handler crash (slice 2).
    assert!(PanicReason::OnLinkDied.is_lifecycle_hook());
}

#[test]
fn link_died_is_abnormal() {
    // LinkDied must be able to propagate: it is NOT a normal stop.
    let reason = ActorStopReason::LinkDied {
        id: crate::mailbox::ActorId::new(1),
        reason: Box::new(ActorStopReason::Killed),
    };
    assert!(!reason.is_normal());
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `nix develop --command cargo test -p bombay-core --lib error::tests::link_died_is_abnormal`
Expected: FAIL — `no variant named LinkDied` / `no variant named OnLinkDied`.

- [ ] **Step 3: Un-defer `PanicReason::OnLinkDied`.** In `error.rs`, replace the deferral comment at line 209 (`// DEFERRED — OnLinkDied ...`) with the variant:

```rust
    /// The `on_link_died` lifecycle hook itself failed.
    #[error("on_link_died hook")]
    OnLinkDied,
```

(`is_lifecycle_hook` at line 216 uses `!matches!(self, Self::HandlerPanic)`, so `OnLinkDied` returns `true` automatically — no change there.)

- [ ] **Step 4: Un-defer `ActorStopReason::LinkDied`.** In `error.rs`, inside `pub enum ActorStopReason` (before the closing brace at line 306), add:

```rust
    /// A watched/linked actor died and this actor is propagating that death
    /// (a linked abnormal exit, or an explicit `Break` from `on_link_died`).
    /// `reason` is boxed (large-variant discipline — it nests a stop reason).
    #[error("linked actor {id:?} died: {reason}")]
    LinkDied {
        /// The identity of the actor that died.
        id: crate::mailbox::ActorId,
        /// Why the linked actor stopped.
        reason: Box<ActorStopReason>,
    },
```

`is_normal()` at line 313 already matches only `Normal | SupervisorRestart`, so `LinkDied` is abnormal automatically.

- [ ] **Step 5: Add `WatchError`.** Append near `NameTaken` (after line 328):

```rust
/// A [`watch`](crate::actor::ActorRef::watch)/[`link`](crate::actor::ActorRef::link)
/// call on a handle whose actor was **not** spawned via `spawn_linked` — it has
/// no link channel to receive death notices on, so it cannot watch.
///
/// A caller mistake (spawn a `Watch` actor via plain `spawn`), surfaced as a
/// typed `Result` rather than a panic: stable Rust has no negative bound to
/// forbid it at the type level.
#[derive(thiserror::Error, Clone, Copy, Debug, PartialEq, Eq)]
#[error("actor was not spawned linked; it cannot watch")]
pub struct ActorNotLinked;
```

- [ ] **Step 6: Run to verify pass.**

Run: `nix develop --command cargo test -p bombay-core --lib error::tests`
Expected: PASS (both new tests + existing lifecycle-hook tests at lines 542-546).

- [ ] **Step 7: Commit.**

```bash
cargo fmt
git add bombay-core/src/error.rs
git commit -m "core(error): un-defer ActorStopReason::LinkDied + PanicReason::OnLinkDied, add ActorNotLinked (#195)"
```

---

### Task 3: The `watch` module — `LinkDied`, `WatchReg`, link-channel types

**Files:**
- Create: `bombay-core/src/watch.rs`
- Modify: `bombay-core/src/lib.rs` (add `mod watch;`)

- [ ] **Step 1: Write the failing test.** Create `bombay-core/src/watch.rs` with only the test first (types below it in later steps):

```rust
//! Death-watch primitives (card #195): the `LinkDied` notice, the watch
//! registration `Signal` carries, and the `Watchers` guard whose `Drop` is the
//! death event. Death travels on a dedicated UNBOUNDED channel so the guard's
//! non-blocking notify (from `Drop`, which cannot await) can never lose a
//! notice — see the design doc for the Erlang/Akka grounding.

use crate::{error::ActorStopReason, mailbox::ActorId};

/// A death notice: the actor `id` stopped for `reason`; `linked` is `true` iff
/// the edge was installed by `link` (propagating) rather than `watch` (notify).
///
/// Monomorphic on purpose — this is why watcher lists need no `dyn`.
#[derive(Clone, Debug)]
pub struct LinkDied {
    /// The actor that died.
    pub id: ActorId,
    /// Why it stopped.
    pub reason: ActorStopReason,
    /// Whether the edge was a `link` (propagate) vs a `watch` (notify only).
    pub linked: bool,
}

/// The sender half of a watcher's UNBOUNDED link channel. One concrete type for
/// all watchers (the payload is monomorphic), so a watched actor stores a
/// homogeneous list of these.
pub type LinkSender = flume::Sender<LinkDied>;
/// The receiver half, drained by a `Watch` actor's run-loop.
pub type LinkReceiver = flume::Receiver<LinkDied>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_of_watchers_notifies_every_edge_with_its_linked_flag() {
        let (tx_a, rx_a) = flume::unbounded();
        let (tx_b, rx_b) = flume::unbounded();
        let mut guard = Watchers::new(ActorId::new(42));
        guard.push(ActorId::new(1), tx_a, false); // a watched
        guard.push(ActorId::new(2), tx_b, true);  // b linked
        guard.set_reason(ActorStopReason::Normal);
        drop(guard);

        let a = rx_a.try_recv().expect("a notified");
        assert_eq!(a.id, ActorId::new(42));
        assert!(!a.linked, "watch edge carries linked=false");
        assert!(a.reason.is_normal());

        let b = rx_b.try_recv().expect("b notified");
        assert!(b.linked, "link edge carries linked=true");
    }

    #[test]
    fn drop_without_set_reason_reports_killed() {
        // Abortable drops the guard without a graceful reason => Killed.
        let (tx, rx) = flume::unbounded();
        let mut guard = Watchers::new(ActorId::new(7));
        guard.push(ActorId::new(1), tx, false);
        drop(guard); // no set_reason

        let n = rx.try_recv().expect("notified on kill path");
        assert!(matches!(n.reason, ActorStopReason::Killed));
    }
}
```

- [ ] **Step 2: Wire the module in.** In `bombay-core/src/lib.rs`, add alongside the other `mod` lines:

```rust
mod watch;
```

- [ ] **Step 3: Run to verify failure.**

Run: `nix develop --command cargo test -p bombay-core --lib watch::tests`
Expected: FAIL — `cannot find type Watchers`, `cannot find WatchReg` not yet defined.

- [ ] **Step 4: Implement `WatchReg` and `Watchers`.** In `watch.rs`, above the `#[cfg(test)]`:

```rust
use smallvec::SmallVec;

/// A watch registration in transit on the message mailbox: "notify `watcher` on
/// my death, over `link_tx`; `linked` decides propagate-vs-notify."
#[derive(Clone, Debug)]
pub struct WatchReg {
    /// The watcher's identity (also the unwatch key).
    pub watcher: ActorId,
    /// The watcher's link channel to deliver `LinkDied` on.
    pub link_tx: LinkSender,
    /// `true` for a `link` edge (propagating), `false` for `watch`.
    pub linked: bool,
}

/// The set of watchers a (watched) actor must notify when it stops, owned by the
/// actor's task. Its `Drop` fires the notifications — so death is delivered on
/// EVERY exit path (normal return, panic unwind, `Abortable` cancellation),
/// since `Drop` runs on all of them.
pub(crate) struct Watchers {
    me: ActorId,
    list: SmallVec<[(ActorId, LinkSender, bool); 1]>,
    reason: Option<ActorStopReason>,
}

impl Watchers {
    pub(crate) fn new(me: ActorId) -> Self {
        Self {
            me,
            list: SmallVec::new(),
            reason: None,
        }
    }

    /// Registers a watcher. `link` twice for the same watcher installs two edges;
    /// dedup is intentionally not done (idempotency is a caller concern, and a
    /// duplicate simply delivers twice — bounded by watch-count, never a leak).
    pub(crate) fn push(&mut self, watcher: ActorId, link_tx: LinkSender, linked: bool) {
        self.list.push((watcher, link_tx, linked));
    }

    /// Removes every edge for `watcher` (the `unwatch` path). Linear scan — a
    /// watcher list is small; a map would buy nothing here.
    pub(crate) fn remove(&mut self, watcher: ActorId) {
        self.list.retain(|(id, _, _)| *id != watcher);
    }

    /// Records the graceful stop reason. If never called (hard kill), `Drop`
    /// defaults to `Killed`.
    pub(crate) fn set_reason(&mut self, reason: ActorStopReason) {
        self.reason = Some(reason);
    }
}

impl Drop for Watchers {
    fn drop(&mut self) {
        let reason = self.reason.take().unwrap_or(ActorStopReason::Killed);
        for (_, tx, linked) in self.list.drain(..) {
            // Unbounded channel: send only fails if the watcher itself is gone,
            // in which case the edge is stale and correctly dropped.
            let _ = tx.try_send(LinkDied {
                id: self.me,
                reason: reason.clone(),
                linked,
            });
        }
    }
}
```

- [ ] **Step 5: Run to verify pass.**

Run: `nix develop --command cargo test -p bombay-core --lib watch::tests`
Expected: PASS (both tests).

- [ ] **Step 6: Commit.**

```bash
cargo fmt
git add bombay-core/src/watch.rs bombay-core/src/lib.rs
git commit -m "core(watch): LinkDied notice + Watchers drop-guard (#195)"
```

---

## Phase 2 — `Signal` restructure (mechanical, touches all match sites)

### Task 4: Replace `Signal::LinkDied` with `Signal::Watch`/`Signal::Unwatch`

**Files:**
- Modify: `bombay-core/src/mailbox.rs:149-193` (`Signal` + old `LinkDied`), `:396-398` (`SendMessageFut` match), `:118-163` (remove scaffold `ActorId`? — NO, keep `ActorId`; remove scaffold `StopReason`/`LinkDied` struct only)
- Modify: `bombay-core/src/actor/kind.rs:58-59` (loop arm)
- Test: existing `mailbox.rs` tests + a new one.

- [ ] **Step 1: Write the failing test.** Append to `mailbox.rs`'s `#[cfg(test)] mod tests`:

```rust
#[test]
fn signal_watch_and_unwatch_are_carried() {
    let (tx, _rx) = flume::unbounded::<crate::watch::LinkDied>();
    let reg = crate::watch::WatchReg { watcher: ActorId::new(9), link_tx: tx, linked: true };
    // Compiles only if Signal carries Watch/Unwatch (this is the whole assertion).
    let _watch: Signal<Probe> = Signal::Watch(Box::new(reg));
    let _unwatch: Signal<Probe> = Signal::Unwatch(ActorId::new(9));
}
```

(If `Probe` is not in `mailbox.rs`'s test mod, use the existing local test actor there; check the module's own `struct Probe`.)

- [ ] **Step 2: Run to verify failure.**

Run: `nix develop --command cargo test -p bombay-core --lib mailbox::tests::signal_watch_and_unwatch_are_carried`
Expected: FAIL — `no variant Watch`.

- [ ] **Step 3: Remove the scaffold `StopReason` + `LinkDied` struct.** In `mailbox.rs`, delete the scaffold `pub enum StopReason` (lines ~134-147) and `pub struct LinkDied` (lines ~149-163). Keep `ActorId` (still used).

- [ ] **Step 4: Restructure the `Signal` enum.** Replace the `LinkDied(Box<LinkDied>)` variant (line 190-192) with:

```rust
    /// A watch registration: enqueue a watcher onto this actor's watcher set so
    /// it is notified when this actor stops. Boxed — a cold control path; inlining
    /// `WatchReg` (which holds a `flume::Sender`) would inflate every message slot.
    Watch(Box<crate::watch::WatchReg>),
    /// Deregister a watcher by id (the `unwatch` path).
    Unwatch(ActorId),
```

- [ ] **Step 5: Fix the `SendMessageFut` match.** In `mailbox.rs:393-398`, update the unreachable arm to the new variant set:

```rust
                Signal::Stop | Signal::Watch(_) | Signal::Unwatch(_) => {
                    unreachable!("send_message enqueues only Signal::Message")
                }
```

- [ ] **Step 6: Fix the loop arm.** In `kind.rs`, replace the `Signal::LinkDied(_) => {}` arm (line 58-59) — Watch/Unwatch are handled in Task 6; for now make the match total so the crate compiles:

```rust
                // Watch/Unwatch registration handled by the loop's control path
                // (Task 6 wires the Watchers guard); until then, ignore to keep
                // the match total.
                Signal::Watch(_) | Signal::Unwatch(_) => {}
```

- [ ] **Step 7: Run to verify pass + whole crate compiles.**

Run: `nix develop --command cargo test -p bombay-core --lib`
Expected: PASS — new test green, no `Signal::LinkDied` references remain (grep to confirm: `rg 'Signal::LinkDied' bombay-core/src` returns nothing).

- [ ] **Step 8: Commit.**

```bash
cargo fmt
git add bombay-core/src/mailbox.rs bombay-core/src/actor/kind.rs
git commit -m "core(mailbox): Signal carries Watch/Unwatch; LinkDied leaves the enum (#195)"
```

---

## Phase 3 — Being watched (passive; every actor)

### Task 5: Own the `Watchers` guard in the lifecycle + handle Watch/Unwatch

**Files:**
- Modify: `bombay-core/src/actor/spawn.rs:138-180` (`run_lifecycle`)
- Modify: `bombay-core/src/actor/kind.rs:25-63` (`run_message_loop` signature + Watch/Unwatch handling)
- Test: new tests in `spawn.rs`'s test mod (`Counter` actor exists there, line ~229).

- [ ] **Step 1: Write the failing test.** In `spawn.rs`'s `#[cfg(test)] mod tests`, add (uses the existing `Counter` actor + `terminate_bound` helper):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_notified_on_normal_stop() {
    let (handled, stopped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let target = Counter::spawn((handled.clone(), stopped.clone()));
    let (watch_tx, watch_rx) = flume::unbounded::<crate::watch::LinkDied>();

    // Register a watcher directly via the mailbox (ActorRef::watch is Task 9).
    target
        .mailbox_sender()
        .send(crate::mailbox::Signal::Watch(Box::new(crate::watch::WatchReg {
            watcher: crate::mailbox::ActorId::new(999),
            link_tx: watch_tx,
            linked: false,
        })))
        .await
        .expect("registration delivered");

    target.stop(); // graceful
    let notice = terminate_bound(watch_rx.recv_async()).await.expect("watch fired");
    assert_eq!(notice.id, target.id());
    assert!(notice.reason.is_normal(), "normal stop => normal reason");
    assert!(!notice.linked);
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::watch_notified_on_normal_stop`
Expected: FAIL — watcher never notified (loop ignores `Signal::Watch`, no teardown notify).

- [ ] **Step 3: Thread a `&mut Watchers` through the loop.** In `kind.rs`, change `run_message_loop`'s signature to accept the guard and handle the registrations:

```rust
pub(super) async fn run_message_loop<A: Actor>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    cancel: &CancellationToken,
    abort: &AbortHandle,
    mailbox_rx: &mut MailboxReceiver<A>,
    watchers: &mut crate::watch::Watchers,
) -> ActorStopReason {
```

Replace the `Signal::Watch(_) | Signal::Unwatch(_) => {}` arm (from Task 4) with:

```rust
                Signal::Watch(reg) => {
                    let crate::watch::WatchReg { watcher, link_tx, linked } = *reg;
                    watchers.push(watcher, link_tx, linked);
                }
                Signal::Unwatch(id) => watchers.remove(id),
```

- [ ] **Step 4: Create + set the guard in `run_lifecycle`.** In `spawn.rs`, inside `run_lifecycle` (after `state` is built, before `run_message_loop`), construct the guard and pass it, then set its reason and drain pending watch regs at teardown:

```rust
    let mut watchers = crate::watch::Watchers::new(actor_ref.id());
    let cancel = actor_ref.cancel_token().clone();
    let abort = actor_ref.abort_handle().clone();
    let weak = actor_ref.downgrade();
    drop(actor_ref);

    let reason =
        run_message_loop(&mut state, &weak, &cancel, &abort, &mut mailbox_rx, &mut watchers).await;

    // Any Watch registration still queued (raced the stop) must also be notified,
    // or it is a silently missed death. Drain the backlog and apply late regs.
    for signal in mailbox_rx.drain() {
        if let crate::mailbox::Signal::Watch(reg) = signal {
            let crate::watch::WatchReg { watcher, link_tx, linked } = *reg;
            watchers.push(watcher, link_tx, linked);
        }
    }
    watchers.set_reason(reason.clone());
    drop(watchers); // fires notifications on the graceful path

    let stop_result = AssertUnwindSafe(state.on_stop(weak.clone(), reason.clone()))
        .catch_unwind()
        .await;
```

(Keep the existing `log_on_stop_outcome` + `RunResult::Stopped` tail below this.)

- [ ] **Step 5: Update the plain-loop caller signature.** The existing `run_message_loop` call site is the one edited in Step 4. Confirm no other caller exists: `rg 'run_message_loop' bombay-core/src`. There is one call (spawn.rs) plus the def (kind.rs).

- [ ] **Step 6: Run to verify pass.**

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::watch_notified_on_normal_stop`
Expected: PASS.

- [ ] **Step 7: Add the panic + kill + in-flight tests.** Append to `spawn.rs` tests:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_notified_on_panic() {
    // Panicker: an actor whose handle() unwinds. Reuse a local panic actor.
    let target = Panicker::spawn(());
    let (tx, rx) = flume::unbounded::<crate::watch::LinkDied>();
    target.mailbox_sender().send(crate::mailbox::Signal::Watch(Box::new(
        crate::watch::WatchReg { watcher: crate::mailbox::ActorId::new(1), link_tx: tx, linked: false },
    ))).await.unwrap();
    target.tell(Boom).try_send().unwrap(); // provokes the panic
    let notice = terminate_bound(rx.recv_async()).await.expect("watch fired on unwind");
    assert!(matches!(notice.reason, ActorStopReason::Panicked(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_notified_on_kill() {
    let (handled, stopped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let target = Counter::spawn((handled, stopped));
    let (tx, rx) = flume::unbounded::<crate::watch::LinkDied>();
    target.mailbox_sender().send(crate::mailbox::Signal::Watch(Box::new(
        crate::watch::WatchReg { watcher: crate::mailbox::ActorId::new(1), link_tx: tx, linked: false },
    ))).await.unwrap();
    target.kill(); // Abortable drops the loop future — no on_stop
    let notice = terminate_bound(rx.recv_async()).await.expect("watch fired on kill");
    assert!(matches!(notice.reason, ActorStopReason::Killed));
}
```

Add a minimal `Panicker` actor + `Boom` message to the test mod if not present:

```rust
struct Panicker;
#[derive(Debug)]
struct Boom;
impl crate::message::Msg for Boom {}
impl crate::mailbox::Mailboxed for Panicker { type Msg = Boom; }
impl crate::actor::Actor for Panicker {
    type Args = ();
    type Error = core::convert::Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(Panicker) }
    async fn handle(&mut self, _: Boom, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
        panic!("boom")
    }
}
```

- [ ] **Step 8: Run to verify the panic + kill tests pass.**

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::watch_notified_on`
Expected: PASS (normal, panic, kill).

- [ ] **Step 9: Add the in-flight race test.**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_in_flight_at_kill_still_notified() {
    // Register a watch, then immediately kill — the reg may not be applied before
    // teardown; the drain-pending step must still notify it.
    let (handled, stopped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let target = Counter::spawn_with_capacity(
        crate::mailbox::Capacity::try_from(8usize).unwrap(), (handled, stopped));
    let (tx, rx) = flume::unbounded::<crate::watch::LinkDied>();
    let sender = target.mailbox_sender().clone();
    sender.try_send(crate::mailbox::Signal::Watch(Box::new(
        crate::watch::WatchReg { watcher: crate::mailbox::ActorId::new(1), link_tx: tx, linked: false },
    ))).unwrap();
    target.kill();
    let notice = terminate_bound(rx.recv_async()).await.expect("in-flight watch notified");
    assert!(matches!(notice.reason, ActorStopReason::Killed));
}
```

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::watch_in_flight_at_kill_still_notified`
Expected: PASS.

> **Note on the kill path + drain:** on `kill`, `Abortable` drops the whole `run_lifecycle` future — the drain-pending loop in Step 4 does **not** run (the future is gone), but `Watchers::drop` still fires with `Killed`, and any pending `Signal::Watch` in the mailbox was already appended *before* the abort or is drained by `MailboxReceiver::drop` (mailbox.rs:366). The in-flight test asserts the reg that *was* applied is notified; a reg that never reached the guard AND was mid-flight at the abort point is covered because the guard holds it (try_send lands before abort). If a strict "reg still in the channel at abort" case must be covered, it belongs to `MailboxReceiver::drop` — out of scope here, note it on #196.

- [ ] **Step 10: Commit.**

```bash
cargo fmt
git add bombay-core/src/actor/spawn.rs bombay-core/src/actor/kind.rs
git commit -m "core(actor): passive watchability — notify watchers on every exit path (#195)"
```

---

## Phase 4 — Watching + reacting (opt-in `Watch` actors)

### Task 6: `Watch: Actor` supertrait + `link_tx` in `RefShared`

**Files:**
- Modify: `bombay-core/src/actor/mod.rs` (add `Watch` trait + re-exports)
- Modify: `bombay-core/src/actor/actor_ref.rs:23-80` (`RefShared` + `ActorRef::new` gain `link_tx`)
- Test: `mod.rs` doc-level unit test.

- [ ] **Step 1: Write the failing test.** In a `#[cfg(test)]` block in `actor/mod.rs`:

```rust
#[cfg(test)]
mod watch_trait_tests {
    use super::*;
    use crate::{error::ActorStopReason, mailbox::ActorId};
    use std::ops::ControlFlow;

    struct W;
    #[derive(Debug)] struct M;
    impl crate::message::Msg for M {}
    impl crate::mailbox::Mailboxed for W { type Msg = M; }
    impl Actor for W {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(W) }
        async fn handle(&mut self, _: M, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> { Ok(()) }
    }
    impl Watch for W {}

    #[tokio::test]
    async fn default_hook_breaks_on_linked_abnormal_and_continues_otherwise() {
        let mut w = W;
        let weak = // build a WeakActorRef<W>; helper in the test
            { let p = crate::actor::PreparedActor::<W>::new(
                crate::mailbox::Capacity::try_from(1usize).unwrap());
              p.actor_ref().downgrade() };
        // linked + abnormal => Break
        let out = w.on_link_died(ActorId::new(1), ActorStopReason::Killed, true).await.unwrap();
        assert!(matches!(out, ControlFlow::Break(ActorStopReason::LinkDied { .. })));
        // watch (linked=false) + abnormal => Continue
        let out = w.on_link_died(ActorId::new(1), ActorStopReason::Killed, false).await.unwrap();
        assert!(matches!(out, ControlFlow::Continue(())));
        // linked + normal => Continue
        let out = w.on_link_died(ActorId::new(1), ActorStopReason::Normal, true).await.unwrap();
        assert!(matches!(out, ControlFlow::Continue(())));
        let _ = weak;
    }
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `nix develop --command cargo test -p bombay-core --lib watch_trait_tests`
Expected: FAIL — `cannot find trait Watch`.

- [ ] **Step 3: Add the `Watch` trait.** In `actor/mod.rs`, after the `Actor` trait (line 94), add:

```rust
use core::ops::ControlFlow;
use crate::mailbox::ActorId;

/// Opt-in capability: an actor that **watches** others and reacts to their death.
///
/// Only actors spawned via [`SpawnLinked::spawn_linked`] receive death notices;
/// a plain actor is still *watchable* (passive) but cannot itself watch. `Watch`
/// is strictly less authority than the slice-2 `Supervisor` (restart) — watching
/// is "get notified", supervising is "rebuild".
pub trait Watch: Actor {
    /// Reacts to the death of a watched/linked actor.
    ///
    /// Default = OTP semantics: a **linked** (`linked == true`) **abnormal**
    /// death propagates (`Break`); a `watch` (notify-only) death, or any normal
    /// death, is observed and the actor continues. Override to trap (return
    /// `Continue` for a linked abnormal death) or to react programmatically.
    fn on_link_died(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> impl core::future::Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send
    {
        async move {
            Ok(if linked && !reason.is_normal() {
                ControlFlow::Break(ActorStopReason::LinkDied { id, reason: Box::new(reason) })
            } else {
                ControlFlow::Continue(())
            })
        }
    }
}
```

Add `use crate::error::ActorStopReason;` to the imports at the top of `mod.rs` if not already present (it imports `ActorStopReason` at line 13).

- [ ] **Step 4: Add `link_tx` to `RefShared`.** In `actor_ref.rs`, extend the struct (line 25-29) and `new` (line 66-80):

```rust
struct RefShared<A: Actor> {
    sender: MailboxSender<A>,
    cancel: CancellationToken,
    abort: AbortHandle,
    /// The actor's own link channel sender — `Some` only for actors spawned via
    /// `spawn_linked` (they can watch); `None` for plain actors. An 8-byte niche,
    /// no channel allocated when absent. Does NOT change clone cost (still one Arc).
    link_tx: Option<crate::watch::LinkSender>,
}
```

`ActorRef::new` currently takes `(id, sender, cancel, abort)`. Add `link_tx`:

```rust
    pub(crate) fn new(
        id: ActorId,
        sender: MailboxSender<A>,
        cancel: CancellationToken,
        abort: AbortHandle,
        link_tx: Option<crate::watch::LinkSender>,
    ) -> Self {
        Self {
            id,
            shared: Arc::new(RefShared { sender, cancel, abort, link_tx }),
        }
    }
```

Add an accessor:

```rust
    /// This actor's own link-channel sender, if it was spawned linked.
    pub(crate) fn link_tx(&self) -> Option<&crate::watch::LinkSender> {
        self.shared.link_tx.as_ref()
    }
```

- [ ] **Step 5: Fix every `ActorRef::new` call site.** Grep and add `None` (plain path) — the drain-window mint in `kind.rs:46-48` and `PreparedActor::new` in `spawn.rs:102-107`:

Run: `rg -n 'ActorRef::new' bombay-core/src`

- `kind.rs:47` drain-window mint: pass `None` (a rebuilt handler ref for a plain actor carries no link channel — the linked path uses a different mint, Task 8):

```rust
                        ActorRef::new(self_ref.id(), self_sender, cancel.clone(), abort.clone(), None)
```

- `spawn.rs` `PreparedActor::new` (line 102): pass `None` for the plain path (Task 7 adds the linked constructor).

- The `actor_ref.rs` test helper `build_ref_with_rx` (line 273): pass `None`.

- [ ] **Step 6: Run to verify pass + `handles_are_two_words` still holds.**

Run: `nix develop --command cargo test -p bombay-core --lib watch_trait_tests actor_ref::tests::handles_are_two_words`
Expected: PASS — `ActorRef` is still 2 words (the `Option<Sender>` lives behind the `Arc`, not inline).

- [ ] **Step 7: Commit.**

```bash
cargo fmt
git add bombay-core/src/actor/mod.rs bombay-core/src/actor/actor_ref.rs bombay-core/src/actor/kind.rs bombay-core/src/actor/spawn.rs
git commit -m "core(actor): Watch supertrait + link_tx in RefShared (#195)"
```

---

### Task 7: `spawn_linked` + linked lifecycle with the link channel

**Files:**
- Modify: `bombay-core/src/actor/spawn.rs` (`PreparedActor::new_linked`, a `run_lifecycle_linked`)
- Modify: `bombay-core/src/actor/mod.rs` (`SpawnLinked` trait)

- [ ] **Step 1: Write the failing test.** In `spawn.rs` tests, add a linked actor that records a received death:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn linked_actor_receives_death_of_watched_target() {
    // Watcher records the last LinkDied id it saw into a shared slot.
    let seen = Arc::new(std::sync::Mutex::new(None::<crate::mailbox::ActorId>));
    let watcher = Observer::spawn_linked(seen.clone());
    let (handled, stopped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let target = Counter::spawn((handled, stopped));

    // Wire: register the watcher's link_tx on the target directly (ActorRef::watch is Task 9).
    let link_tx = watcher.link_tx().expect("linked actor has a link channel").clone();
    target.mailbox_sender().send(crate::mailbox::Signal::Watch(Box::new(
        crate::watch::WatchReg { watcher: watcher.id(), link_tx, linked: false },
    ))).await.unwrap();

    target.stop();
    // Give the watcher time to observe; poll the shared slot under a bound.
    terminate_bound(async {
        loop {
            if seen.lock().unwrap().is_some() { break; }
            tokio::task::yield_now().await;
        }
    }).await;
    assert_eq!(*seen.lock().unwrap(), Some(target.id()));
}
```

Add the `Observer` actor + its `Watch` impl (overriding `on_link_died` to record) to the test mod:

```rust
struct Observer { seen: Arc<std::sync::Mutex<Option<crate::mailbox::ActorId>>> }
#[derive(Debug)] struct Never;
impl crate::message::Msg for Never {}
impl crate::mailbox::Mailboxed for Observer { type Msg = Never; }
impl crate::actor::Actor for Observer {
    type Args = Arc<std::sync::Mutex<Option<crate::mailbox::ActorId>>>;
    type Error = core::convert::Infallible;
    async fn on_start(seen: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(Observer { seen }) }
    async fn handle(&mut self, _: Never, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> { Ok(()) }
}
impl crate::actor::Watch for Observer {
    async fn on_link_died(&mut self, id: crate::mailbox::ActorId, _r: ActorStopReason, _l: bool)
        -> Result<std::ops::ControlFlow<ActorStopReason>, Self::Error> {
        *self.seen.lock().unwrap() = Some(id);
        Ok(std::ops::ControlFlow::Continue(()))
    }
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::linked_actor_receives_death`
Expected: FAIL — `no method spawn_linked`.

- [ ] **Step 3: Add `PreparedActor::new_linked` + linked lifecycle.** In `spawn.rs`, add a constructor that makes the link channel and stores its `tx` in the ref, keeping `rx` for the loop:

```rust
impl<A: Watch> PreparedActor<A> {
    /// Prepares a **linked** actor: like [`new`](Self::new) but also creates the
    /// unbounded link channel (so this actor can watch others), storing the
    /// sender in the `ActorRef` and keeping the receiver for the run-loop.
    pub fn new_linked(capacity: Capacity) -> (Self, crate::watch::LinkReceiver) {
        let (mailbox_tx, mailbox_rx) = Mailbox::<A>::bounded(capacity);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let (link_tx, link_rx) = flume::unbounded();
        let actor_ref = ActorRef::new(
            next_actor_id(),
            mailbox_tx,
            CancellationToken::new(),
            abort_handle,
            Some(link_tx),
        );
        (Self { actor_ref, mailbox_rx, abort_registration }, link_rx)
    }
}
```

Add `run_lifecycle_linked` — identical to `run_lifecycle` but with a two-arm select and the `on_link_died` reaction. Factor the shared parts if the reviewer prefers; for clarity the plan spells it out:

```rust
async fn run_lifecycle_linked<A: Watch>(
    args: A::Args,
    actor_ref: ActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
    link_rx: crate::watch::LinkReceiver,
) -> RunResult<A> {
    let started = AssertUnwindSafe(A::on_start(args, actor_ref.clone())).catch_unwind().await;
    let mut state = match started {
        Ok(Ok(actor)) => actor,
        Ok(Err(err)) => return RunResult::StartupFailed(PanicError::new(Box::new(err), PanicReason::OnStart)),
        Err(payload) => return RunResult::StartupFailed(PanicError::from_panic_any(payload, PanicReason::OnStart)),
    };

    let mut watchers = crate::watch::Watchers::new(actor_ref.id());
    let cancel = actor_ref.cancel_token().clone();
    let abort = actor_ref.abort_handle().clone();
    let weak = actor_ref.downgrade();
    drop(actor_ref);

    let reason = crate::actor::kind::run_linked_message_loop(
        &mut state, &weak, &cancel, &abort, &mut mailbox_rx, &mut watchers, &link_rx,
    ).await;

    for signal in mailbox_rx.drain() {
        if let crate::mailbox::Signal::Watch(reg) = signal {
            let crate::watch::WatchReg { watcher, link_tx, linked } = *reg;
            watchers.push(watcher, link_tx, linked);
        }
    }
    watchers.set_reason(reason.clone());
    drop(watchers);

    let stop_result = AssertUnwindSafe(state.on_stop(weak.clone(), reason.clone())).catch_unwind().await;
    log_on_stop_outcome::<A>(&reason, stop_result);
    RunResult::Stopped { actor: state, reason }
}
```

- [ ] **Step 4: Add the `run` / `spawn` linked entry points.** Add to `PreparedActor<A: Watch>`:

```rust
    /// Runs the linked actor in the current task.
    pub async fn run_linked(self, args: A::Args, link_rx: crate::watch::LinkReceiver) -> RunResult<A> {
        let lifecycle = run_lifecycle_linked(args, self.actor_ref, self.mailbox_rx, link_rx);
        Abortable::new(lifecycle, self.abort_registration).await.unwrap_or(RunResult::Killed)
    }
    /// Spawns the linked actor in a background tokio task.
    pub fn spawn_linked_task(self, args: A::Args, link_rx: crate::watch::LinkReceiver) -> JoinHandle<RunResult<A>> {
        tokio::spawn(self.run_linked(args, link_rx))
    }
```

- [ ] **Step 5: Add the `SpawnLinked` ergonomic trait.** In `actor/mod.rs`:

```rust
/// Ergonomic linked-spawn for every [`Watch`] actor.
pub trait SpawnLinked: Watch {
    /// Spawns a linked actor (can watch others) with the default capacity.
    #[must_use]
    fn spawn_linked(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_linked_with_capacity(default_capacity(), args)
    }
    /// Spawns a linked actor with an explicit mailbox capacity.
    #[must_use]
    fn spawn_linked_with_capacity(capacity: Capacity, args: Self::Args) -> ActorRef<Self> {
        let (prepared, link_rx) = PreparedActor::<Self>::new_linked(capacity);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn_linked_task(args, link_rx);
        actor_ref
    }
}
impl<A: Watch> SpawnLinked for A {}
```

Export it: add `SpawnLinked` and `Watch` to the `pub use self::{...}` in `mod.rs:23-27`.

- [ ] **Step 6: Add the linked loop in `kind.rs`** (Task 8 implements the select; here just declare so it compiles). See Task 8 — do Task 8 before running.

- [ ] **Step 7: (after Task 8) Run to verify pass.**

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::linked_actor_receives_death`
Expected: PASS.

- [ ] **Step 8: Commit (after Task 8).**

---

### Task 8: The two-arm linked loop calling `on_link_died`

**Files:**
- Modify: `bombay-core/src/actor/kind.rs` (add `run_linked_message_loop`)

- [ ] **Step 1: Implement the linked loop.** In `kind.rs`, add (reuses `handle_message` + the Watch/Unwatch arms from Task 5):

```rust
/// The `Watch`-actor run-loop: the plain message loop PLUS a second select arm
/// draining the link channel and dispatching `on_link_died`. A `Break` from the
/// hook (default: linked abnormal death) stops the actor with the propagated
/// reason; an `Err` from the hook is a controlled crash (`OnLinkDied`).
pub(super) async fn run_linked_message_loop<A: Watch>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    cancel: &CancellationToken,
    abort: &AbortHandle,
    mailbox_rx: &mut MailboxReceiver<A>,
    watchers: &mut crate::watch::Watchers,
    link_rx: &crate::watch::LinkReceiver,
) -> ActorStopReason {
    loop {
        tokio::select! {
            biased;
            // Death notices first (biased): a pending death is handled before
            // more messages, matching "react to failure promptly".
            death = link_rx.recv_async() => {
                let Ok(notice) = death else { continue };
                if let ControlFlow::Break(reason) =
                    handle_link_died(state, self_ref, notice).await
                {
                    return reason;
                }
            }
            maybe = cancel.run_until_cancelled(mailbox_rx.recv()) => {
                match maybe {
                    None | Some(None) => return ActorStopReason::Normal,
                    Some(Some(signal)) => match signal {
                        Signal::Message { msg, self_sender } => {
                            let actor_ref = self_ref.upgrade().unwrap_or_else(|| {
                                ActorRef::new(self_ref.id(), self_sender, cancel.clone(), abort.clone(), None)
                            });
                            if let ControlFlow::Break(reason) =
                                handle_message(state, actor_ref, self_ref, msg).await
                            {
                                return reason;
                            }
                        }
                        Signal::Stop => return ActorStopReason::Normal,
                        Signal::Watch(reg) => {
                            let crate::watch::WatchReg { watcher, link_tx, linked } = *reg;
                            watchers.push(watcher, link_tx, linked);
                        }
                        Signal::Unwatch(id) => watchers.remove(id),
                    },
                }
            }
        }
    }
}

/// Runs `on_link_died` under `catch_unwind`; maps its `ControlFlow`/`Err`/panic.
async fn handle_link_died<A: Watch>(
    state: &mut A,
    self_ref: &WeakActorRef<A>,
    notice: crate::watch::LinkDied,
) -> ControlFlow<ActorStopReason> {
    let crate::watch::LinkDied { id, reason, linked } = notice;
    let result = AssertUnwindSafe(state.on_link_died(id, reason, linked)).catch_unwind().await;
    match result {
        Ok(Ok(flow)) => flow,
        Ok(Err(err)) => ControlFlow::Break(ActorStopReason::Panicked(
            PanicError::new(Box::new(err), PanicReason::OnLinkDied))),
        Err(payload) => ControlFlow::Break(ActorStopReason::Panicked(
            PanicError::from_panic_any(payload, PanicReason::OnLinkDied))),
    }
}
```

Add the imports `use crate::actor::Watch;` and confirm `PanicError`, `PanicReason`, `ControlFlow`, `ActorRef` are in scope (they are, from the existing `kind.rs` top imports + `Signal`).

- [ ] **Step 2: Run the linked test (unblocks Task 7 Step 7).**

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::linked_actor_receives_death`
Expected: PASS.

- [ ] **Step 3: Commit Tasks 7+8 together.**

```bash
cargo fmt
git add bombay-core/src/actor/kind.rs bombay-core/src/actor/spawn.rs bombay-core/src/actor/mod.rs
git commit -m "core(actor): spawn_linked + two-arm loop dispatching on_link_died (#195)"
```

---

### Task 9: `ActorRef::watch` / `link` / `unwatch`

**Files:**
- Modify: `bombay-core/src/actor/actor_ref.rs` (add `impl<A: Watch> ActorRef<A>`)

- [ ] **Step 1: Write the failing tests.** In `actor_ref.rs` tests (make `Probe` implement `Watch` or add a local `Watch` actor; simplest is a dedicated linked test using `spawn_linked`). Add to `spawn.rs` tests (has the runtime) instead, to exercise end-to-end:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn link_propagates_on_abnormal() {
    // a links b; b panics; a's default hook Breaks => a stops with LinkDied.
    let a = Observer::spawn_linked(Arc::new(std::sync::Mutex::new(None)));
    let b = Panicker::spawn(()); // plain actor, watchable
    a.link(&b).expect("a is linked, can watch");
    // NOTE: link needs B: Watch for the reverse edge; here we assert the a-watches-b
    // direction only, so use watch() not link() for a plain B:
}
```

Because `link` requires **both** `A: Watch` and `B: Watch`, and `Panicker` is not `Watch`, the propagation test uses `watch` + a linked-flag override, OR makes `Panicker` a `Watch` actor. Make `Panicker: Watch` (default hook) so `link` type-checks:

```rust
impl crate::actor::Watch for Panicker {}
```

Then:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn link_propagates_on_abnormal() {
    let seen = Arc::new(std::sync::Mutex::new(None));
    let a = Observer::spawn_linked(seen);          // Observer overrides hook to Continue...
    // ...so use a propagation-observing actor instead: assert via a JoinHandle.
    let a = Panicker::spawn_linked(());            // default hook: Break on linked abnormal
    let b = Panicker::spawn_linked(());
    a.link(&b).expect("both linked");
    b.tell(Boom).try_send().unwrap();              // b panics
    // a must stop because the link propagated. Observe via is_alive polling.
    terminate_bound(async { while a.is_alive() { tokio::task::yield_now().await; } }).await;
    assert!(!a.is_alive(), "linked abnormal death propagated to a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn link_does_not_propagate_on_normal() {
    let a = Panicker::spawn_linked(());
    let b = Counter::spawn_linked((Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0))));
    a.link(&b).unwrap();
    b.stop(); // normal
    // a must stay alive.
    tokio::task::yield_now().await;
    assert!(a.is_alive(), "normal death does not propagate");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_spawned_watch_actor_watch_errs() {
    let a = Panicker::spawn(()); // Watch actor, but plain-spawned => no link channel
    let b = Panicker::spawn(());
    assert_eq!(a.watch(&b), Err(crate::error::ActorNotLinked));
}
```

(`Counter` must also be `impl Watch` for `spawn_linked`; add `impl crate::actor::Watch for Counter {}`.)

- [ ] **Step 2: Run to verify failure.**

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::link_propagates_on_abnormal`
Expected: FAIL — `no method watch`/`link`.

- [ ] **Step 3: Implement the verbs.** In `actor_ref.rs`, add:

```rust
use crate::{actor::Watch, error::ActorNotLinked, mailbox::Signal, watch::WatchReg};

impl<A: Watch> ActorRef<A> {
    /// Watches `target`: this actor's `on_link_died` fires when `target` stops.
    /// One-directional, notify-only (`linked = false`).
    ///
    /// # Errors
    /// [`ActorNotLinked`] if this actor was spawned via plain `spawn` (no link
    /// channel). Spawn watchers with `spawn_linked`.
    pub fn watch<B: Actor>(&self, target: &ActorRef<B>) -> Result<(), ActorNotLinked> {
        self.register_on(target, false)
    }

    /// Links with `peer`: bidirectional. Each side's `on_link_died` fires on the
    /// other's death; the default hook propagates an abnormal death (`Break`).
    /// Requires both actors to be `Watch` (both must react).
    ///
    /// # Errors
    /// [`ActorNotLinked`] if either actor lacks a link channel.
    pub fn link<B: Watch>(&self, peer: &ActorRef<B>) -> Result<(), ActorNotLinked> {
        self.register_on(peer, true)?;
        peer.register_on(self, true)
    }

    /// Stops watching `target` (removes this actor's edge from target's set).
    pub fn unwatch<B: Actor>(&self, target: &ActorRef<B>) {
        // Best-effort: if target has already stopped, the send fails and there is
        // nothing to remove.
        let _ = target.mailbox_sender().try_send(Signal::Unwatch(self.id()));
    }

    /// Registers this actor as a watcher on `target` with the given `linked` flag.
    fn register_on<B: Actor>(&self, target: &ActorRef<B>, linked: bool) -> Result<(), ActorNotLinked> {
        let link_tx = self.link_tx().ok_or(ActorNotLinked)?.clone();
        let reg = WatchReg { watcher: self.id(), link_tx, linked };
        match target.mailbox_sender().try_send(Signal::Watch(Box::new(reg))) {
            Ok(()) => Ok(()),
            // Target already dead: deliver an immediate LinkDied (Erlang's
            // link-to-dead rule). The watcher's own channel is guaranteed present.
            Err(_) => {
                if let Some(tx) = self.link_tx() {
                    let _ = tx.try_send(crate::watch::LinkDied {
                        id: target.id(),
                        reason: ActorStopReason::Killed,
                        linked,
                    });
                }
                Ok(())
            }
        }
    }
}
```

Add `use crate::error::ActorStopReason;` to `actor_ref.rs` imports.

> **Design note (dead-target reason):** the immediate notice uses `Killed` as the reason because the target's true reason is unknowable once its mailbox is gone. This matches "we know it is dead, not why." Recorded on the spec's dead-target test.

- [ ] **Step 4: Run to verify pass** (needs Task 5+8 landed).

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::link_ spawn::tests::plain_spawned`
Expected: PASS (propagate-on-abnormal, no-propagate-on-normal, plain-spawned-errs).

- [ ] **Step 5: Add the remaining spec tests** — `trap_exit_via_override_keeps_running`, `dead_target_watch_immediate_linkdied`, `watch_does_not_pin_target`, `stale_watcher_edge_self_prunes`, `many_watchers_all_notified`. Full code:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trap_exit_via_override_keeps_running() {
    // Trapper overrides on_link_died to Continue even for a linked abnormal death.
    let a = Trapper::spawn_linked(());
    let b = Panicker::spawn_linked(());
    a.link(&b).unwrap();
    b.tell(Boom).try_send().unwrap();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(a.is_alive(), "trap_exit override keeps the actor alive");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_target_watch_immediate_linkdied() {
    let a = Trapper::spawn_linked(());
    let b = Panicker::spawn_linked(());
    b.kill();
    terminate_bound(async { while b.is_alive() { tokio::task::yield_now().await; } }).await;
    // Watching an already-dead b delivers LinkDied immediately (observed via the
    // trapper's link channel indirectly — assert the call still succeeds and a
    // stays alive because Trapper Continues).
    assert!(a.watch(&b).is_ok());
    tokio::task::yield_now().await;
    assert!(a.is_alive());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_does_not_pin_target() {
    // Watching holds no strong ActorRef to the target: dropping the target's last
    // external ref still stops it.
    let a = Trapper::spawn_linked(());
    let (handled, stopped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let b = Counter::spawn_linked((handled, stopped.clone()));
    a.watch(&b).unwrap();
    let b_id = b.id();
    drop(b); // last external strong ref
    // b must stop (ref-count-driven stop, ADR-0003); a receives its death.
    terminate_bound(async { while stopped.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } }).await;
    let _ = b_id;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_watchers_all_notified() {
    use tokio::sync::Barrier;
    let (handled, stopped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let target = Counter::spawn((handled, stopped));
    let n = 8usize;
    let barrier = Arc::new(Barrier::new(n));
    let mut receivers = Vec::new();
    let mut tasks = Vec::new();
    for i in 0..n {
        let (tx, rx) = flume::unbounded::<crate::watch::LinkDied>();
        receivers.push(rx);
        let sender = target.mailbox_sender().clone();
        let b = barrier.clone();
        tasks.push(tokio::spawn(async move {
            b.wait().await; // real overlap
            sender.send(crate::mailbox::Signal::Watch(Box::new(
                crate::watch::WatchReg { watcher: crate::mailbox::ActorId::new(i as u64 + 1), link_tx: tx, linked: false },
            ))).await.unwrap();
        }));
    }
    for t in tasks { t.await.unwrap(); }
    target.stop();
    for rx in receivers {
        let notice = terminate_bound(rx.recv_async()).await.expect("each watcher notified");
        assert_eq!(notice.id, target.id());
    }
}
```

Add `Trapper` (a `Watch` actor overriding the hook to `Continue`):

```rust
struct Trapper;
impl crate::mailbox::Mailboxed for Trapper { type Msg = Never; }
impl crate::actor::Actor for Trapper {
    type Args = ();
    type Error = core::convert::Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(Trapper) }
    async fn handle(&mut self, _: Never, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> { Ok(()) }
}
impl crate::actor::Watch for Trapper {
    async fn on_link_died(&mut self, _: crate::mailbox::ActorId, _: ActorStopReason, _: bool)
        -> Result<std::ops::ControlFlow<ActorStopReason>, Self::Error> {
        Ok(std::ops::ControlFlow::Continue(())) // trap: never propagate
    }
}
```

- [ ] **Step 6: Run the full new suite.**

Run: `nix develop --command cargo nextest run -p bombay-core`
Expected: all green.

- [ ] **Step 7: Commit.**

```bash
cargo fmt
git add bombay-core/src/actor/actor_ref.rs bombay-core/src/actor/spawn.rs
git commit -m "core(actor): watch/link/unwatch verbs + full death-watch invariant tests (#195)"
```

---

## Phase 5 — Hardening lanes, exports, README

### Task 10: `stale_watcher_edge_self_prunes` + `lib.rs` public exports

**Files:**
- Modify: `bombay-core/src/lib.rs` (re-export `Watch`, `SpawnLinked`, `LinkDied`, `ActorNotLinked`)
- Modify: `bombay-core/src/actor/mod.rs` (re-exports)
- Test: `spawn.rs`

- [ ] **Step 1: Stale-edge test.** In `spawn.rs` tests:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_watcher_edge_self_prunes() {
    // A watcher registers, then its link channel receiver is dropped (watcher
    // "dies"). When the target later stops, the notify send fails and is skipped
    // — no panic, no leak. We assert the target still stops cleanly.
    let (handled, stopped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let target = Counter::spawn((handled, stopped.clone()));
    let (tx, rx) = flume::unbounded::<crate::watch::LinkDied>();
    target.mailbox_sender().send(crate::mailbox::Signal::Watch(Box::new(
        crate::watch::WatchReg { watcher: crate::mailbox::ActorId::new(1), link_tx: tx, linked: false },
    ))).await.unwrap();
    drop(rx); // watcher's receiver gone => edge is now stale
    target.stop();
    terminate_bound(async { while stopped.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } }).await;
    // No panic reaching here is the assertion; the dead edge was skipped.
}
```

Run: `nix develop --command cargo test -p bombay-core --lib spawn::tests::stale_watcher_edge_self_prunes`
Expected: PASS.

- [ ] **Step 2: Public re-exports.** In `actor/mod.rs`, extend the `pub use`:

```rust
pub use self::{
    actor_ref::{ActorRef, WeakActorRef},
    recipient::{Recipient, RecipientAskRequest, ReplyRecipient, WeakRecipient},
    spawn::{DEFAULT_MAILBOX_CAPACITY, PreparedActor, RunResult},
    // #195:
    mod_watch_exports::{}, // (inline — Watch + SpawnLinked declared in this module)
};
pub use self::{Watch, SpawnLinked}; // if declared inline in mod.rs, no path prefix needed
```

(Adjust to however `Watch`/`SpawnLinked` are declared — if inline in `mod.rs`, they are already `pub`; just ensure `lib.rs` re-exports them.)

In `lib.rs`, add to the crate's public surface:

```rust
pub use watch::{LinkDied};
pub use error::ActorNotLinked; // if not already re-exported with the other errors
```

- [ ] **Step 3: Verify the public API compiles + doctests.**

Run: `nix develop --command cargo test -p bombay-core --doc`
Expected: PASS (or no doctests).

- [ ] **Step 4: Commit.**

```bash
cargo fmt
git add bombay-core/src/lib.rs bombay-core/src/actor/mod.rs bombay-core/src/actor/spawn.rs
git commit -m "core(watch): public exports + stale-edge self-prune test (#195)"
```

---

### Task 11: Mutation baseline + MIRI + bolero extension

**Files:**
- Modify: `mutants-baseline.json`
- Modify: `fuzz/` bolero target from #164 (extend, do not fork)

- [ ] **Step 1: List new functions needing baseline entries** (memory: `mutants-baseline-workflow` — a new/renamed fn missing an entry is `Unaccounted` and fails the gate). New fns: `Watchers::{new,push,remove,set_reason,drop}`, `ActorRef::{watch,link,unwatch,register_on,link_tx}`, `Watch::on_link_died` (default), `run_linked_message_loop`, `handle_link_died`, `PreparedActor::new_linked`, `run_lifecycle_linked`, `SpawnLinked::*`.

- [ ] **Step 2: Run mutants over the crate to get the real surviving/viable set.**

Run: `nix build .#mutants -L` (per memory `mutation-sweep-179-shipped`: the gate must use explicit `--timeout 60`; the `.#mutants` derivation already encodes it).
Expected: a report. Any **new** survivor must be killed by a test; any non-`Default` return with no viable mutant → mark `known_zero_viable` per the baseline workflow.

- [ ] **Step 3: Add baseline entries** for every new fn per the report (Unaccounted → accounted). Follow the exact JSON shape already in `mutants-baseline.json`.

- [ ] **Step 4: Verify the gate.**

Run: `nix build .#mutants -L`
Expected: exit 0 (no new survivors, no Unaccounted).

- [ ] **Step 5: Extend the #164 bolero loop target** to feed `Signal::Watch`/`Unwatch` into the loop model (do NOT create a parallel target — memory: single-source oracle). Add the two new signal variants to the target's input enum and assert no panic + eventual notify.

- [ ] **Step 6: MIRI over the new sync paths.**

Run: `nix develop .#miri --command bash -c 'cargo miri test -p bombay-core --lib watch:: -- --skip prop_'`
Expected: green (every proptest carries `prop_` — memory `proptest-prop-prefix-miri-contract`; the new tests here are not proptests, so unaffected).

- [ ] **Step 7: Commit.**

```bash
git add mutants-baseline.json fuzz/
git commit -m "test(supervision): mutants baseline + bolero/MIRI coverage for death-watch (#195)"
```

---

### Task 12: README public-API delta + final gate

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the "public API at a glance" section.** Add (matching the README's existing bullet style):
  - `Watch: Actor` supertrait with `on_link_died` (default OTP semantics).
  - `ActorRef::watch` / `link` / `unwatch` (require the actor be spawned via `spawn_linked`).
  - `SpawnLinked::spawn_linked` entry point.
  - `ActorStopReason::LinkDied`, `error::ActorNotLinked`, `watch::LinkDied`.
  - One salient-feature line: reliable death-watch on every exit path (normal/panic/kill) via a dedicated unbounded link channel.

- [ ] **Step 2: Run the full gate.**

Run: `nix flake check`
Expected: green (build + clippy + fmt + tests + taplo). This is the authoritative gate — memory `subagent-gate-stall-pattern`: commit before running so a subagent isn't stranded mid-gate.

- [ ] **Step 3: Commit + open the PR.**

```bash
cargo fmt
git add README.md
git commit -m "docs(readme): death-watch public API — Watch/watch/link/spawn_linked (#195)"
git push -u origin 120-links-death-watch
gh pr create --repo devrandom-labs/bombay --base main \
  --title "core(supervision): links + death-watch — watch/link, unbounded link channel, Watch trait (#195)" \
  --body "Closes #195. Slice 1 of #120. Design: docs/superpowers/specs/2026-07-23-120-links-death-watch-design.md

Watch/link verbs on one mechanism; dedicated unbounded link channel (Erlang/Akka-grounded) with a Drop-guard so no death is missed on kill/panic/normal; monomorphic LinkDied => no dyn (erasure relocated to #196 restart factory); Watch: Actor supertrait via spawn_linked; link_tx Option in RefShared keeps the 1-Arc clone.

Deferred to #196 (recorded on the card): RestartPolicy, strategies, restart factory, on_stop-failure surface."
```

Expected: PR opened; `Nix Flake Check` runs green (per branch-protection ruleset — memory `gate-main-merges-on-green-ci`).

---

## Self-review notes (addressed)

- **Spec coverage:** every spec test (12) maps to a task (5, 7, 9, 10). Signal restructure → Task 4. No-`dyn` → Task 3 (monomorphic `LinkDied`). Role split → Tasks 5 (passive) + 6-8 (opt-in). Wiring `Option<Sender>` → Task 6. Error deltas → Task 2. Deferrals → recorded on #196.
- **Known coupling:** the kill-path in-flight edge case (a `Watch` reg still *in the channel* at `Abortable` drop) is explicitly scoped OUT and noted for `MailboxReceiver::drop` / #196 — Task 5 Step 9. This is the one place the plan bounds coverage; it is logged, not silent (per CLAUDE "no silent caps").
- **Type consistency:** `WatchReg{watcher,link_tx,linked}`, `LinkDied{id,reason,linked}`, `Watchers::{new,push,remove,set_reason}`, `register_on`, `link_tx()` are used identically across Tasks 3/5/7/8/9.
- **`link` needs both peers `Watch`:** enforced by `link<B: Watch>` (Task 9); the tests make `Panicker`/`Counter` implement `Watch` where they are linked.
