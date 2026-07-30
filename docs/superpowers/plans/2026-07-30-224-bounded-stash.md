# Bounded Stash (`Stashed<S>`) Implementation Plan — card #224

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A bounded, framework-owned deferral buffer (`Stash`) delivered through a composition wrapper (`Stashed<S: StashActor>`) with in-step, arrival-order replay — per spec `docs/superpowers/specs/2026-07-30-224-bounded-stash-design.md` (v2).

**Architecture:** One new module `crates/core/src/stash.rs` holds everything: `Stash<M>` (two-queue buffer), `StashFull<M>` (typed overflow handback), `StashActor` (opt-in trait mirroring `Actor` with a `&mut Stash` handle param), and `Stashed<S>` (the wrapper whose `Actor::handle` runs the user handler then drains `ready` in the same step). **`kind.rs` and all loops are untouched.** Integration tests live in `crates/core/tests/stash.rs`.

**Tech Stack:** Rust stable (edition 2024), tokio (`start_paused` tests), thiserror, existing `bombay` types (`Capacity`, `ActorRef`, `PreparedActor`, supervision). Gate: `nix flake check`. Test runner: `nix develop --command cargo nextest run -p bombay`.

**Rules that bite here:** flake checks only see **tracked** files (`git add` before trusting a check); no raw `cargo` outside `nix develop`; conventional commits with scope + `[#224]`; no Claude attribution; `cargo fmt` before every commit; every terminal await in tests bounded by `terminate_bound()`.

---

### Task 1: `Stash<M>` + `StashFull<M>` (pure struct, unit-tested)

**Files:**
- Create: `crates/core/src/stash.rs`
- Modify: `crates/core/src/lib.rs` (register module)

- [ ] **Step 1: Write the failing unit tests + module skeleton**

Create `crates/core/src/stash.rs`:

```rust
//! Bounded deferral: `Stashed<S>` composition over a two-queue `Stash<M>`
//! (card #224). Design: docs/superpowers/specs/2026-07-30-224-bounded-stash-design.md,
//! ADR-0022.
//!
//! Research anchor: conditional synchronization for fixed-interface actors
//! (De Koster et al., AGERE! 2016, §4.2; Briot–Guerraoui–Löhr, ACM CSur 1998);
//! replay preserves arrival order, overflow refuses loudly (guaranteed
//! delivery — silent drop is the one forbidden outcome).

use std::collections::VecDeque;

use crate::mailbox::Capacity;

/// A bounded, single-producer deferral buffer. `stash` defers a message the
/// current state cannot accept; `unstash_all` snapshots everything held for
/// front-of-line replay by [`Stashed`]'s handle wrapper — ahead of the
/// mailbox backlog, in stash-arrival order.
#[derive(Debug)]
pub struct Stash<M> {
    /// `stash()` pushes here (back). Waits for an `unstash_all`.
    held: VecDeque<M>,
    /// `unstash_all()` moves `held` here; replay pops from the front.
    ready: VecDeque<M>,
    /// Bounds `held.len() + ready.len()`.
    cap: Capacity,
}

/// Overflow: the stash is at capacity. Carries the rejected message back in
/// full — never dropped, never panicked (the `TellError` handback precedent).
#[derive(thiserror::Error, Debug)]
#[error("stash full (capacity {})", .cap.get())]
pub struct StashFull<M> {
    msg: M,
    cap: Capacity,
}

impl<M> StashFull<M> {
    /// Recovers the rejected message. Total — overflow never consumes it.
    #[must_use]
    pub fn msg(self) -> M {
        self.msg
    }

    /// The capacity that was hit.
    #[must_use]
    pub const fn capacity(&self) -> Capacity {
        self.cap
    }
}

impl<M> Stash<M> {
    /// Builds an empty stash bounded to `cap` messages. Crate-private: a
    /// stash exists only inside a [`Stashed`] (forget-proof by construction).
    pub(crate) fn bounded(cap: Capacity) -> Self {
        Self {
            held: VecDeque::new(),
            ready: VecDeque::new(),
            cap,
        }
    }

    /// Messages currently deferred (held + awaiting replay).
    #[must_use]
    pub fn len(&self) -> usize {
        // Both queues are bounded by `cap`, but per the arithmetic-safety
        // rule the sum is still checked: an (unreachable) overflow reads as
        // "at capacity", never as a small number.
        self.held.len().checked_add(self.ready.len()).unwrap_or(usize::MAX)
    }

    /// `true` when nothing is deferred.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.held.is_empty() && self.ready.is_empty()
    }

    /// Defers `msg`.
    ///
    /// # Errors
    ///
    /// [`StashFull`] (carrying `msg` back) when `len() == capacity`.
    pub fn stash(&mut self, msg: M) -> Result<(), StashFull<M>> {
        if self.len() >= self.cap.get() {
            return Err(StashFull { msg, cap: self.cap });
        }
        self.held.push_back(msg);
        Ok(())
    }

    /// Queues every currently-held message for replay, in stash order,
    /// ahead of the mailbox backlog. **Snapshot semantics:** messages stashed
    /// *during* the replay wait for the next call — a replayed message cannot
    /// re-enter its own batch. A handler that re-stashes a message and calls
    /// `unstash_all` on every replay of it livelocks itself (user bug; same
    /// class as an actor `tell`-ing itself forever).
    pub fn unstash_all(&mut self) {
        self.ready.append(&mut self.held);
    }

    /// Pops the next message due for replay. Crate-private: only the
    /// [`Stashed`] handle wrapper drives replay.
    pub(crate) fn pop_ready(&mut self) -> Option<M> {
        self.ready.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroUsize;

    use super::*;

    fn cap(n: usize) -> Capacity {
        Capacity::new(NonZeroUsize::new(n).expect("test capacity nonzero"))
            .expect("test capacity valid")
    }

    /// Invariant 1: the bound covers held + ready together, from the
    /// constructor parameter — not a global.
    #[test]
    fn capacity_bounds_held_plus_ready() {
        let mut stash = Stash::bounded(cap(2));
        stash.stash(1u32).expect("slot 1");
        stash.unstash_all(); // 1 moves to ready — still counts
        stash.stash(2u32).expect("slot 2");
        let err = stash.stash(3u32).expect_err("cap 2 must refuse the 3rd");
        assert_eq!(err.msg(), 3, "the rejected message comes back intact");
        assert_eq!(stash.len(), 2);
    }

    /// Invariant 2: overflow is a typed handback — exact message recovered,
    /// nothing dropped, nothing panicked.
    #[test]
    fn overflow_hands_the_exact_message_back() {
        let mut stash = Stash::bounded(cap(1));
        stash.stash(10u32).expect("fits");
        let err = stash.stash(20u32).expect_err("full");
        assert_eq!(err.capacity().get(), 1);
        assert_eq!(err.msg(), 20);
        // The buffer itself is untouched by the refusal.
        stash.unstash_all();
        assert_eq!(stash.pop_ready(), Some(10));
        assert_eq!(stash.pop_ready(), None);
    }

    /// D2 snapshot semantics: a message stashed after `unstash_all` waits in
    /// `held`; it is NOT part of the draining batch.
    #[test]
    fn unstash_is_a_snapshot_not_a_live_view() {
        let mut stash = Stash::bounded(cap(4));
        stash.stash(1u32).expect("held 1");
        stash.stash(2u32).expect("held 2");
        stash.unstash_all();
        stash.stash(3u32).expect("held 3 — mid-replay stash");
        assert_eq!(stash.pop_ready(), Some(1));
        assert_eq!(stash.pop_ready(), Some(2));
        assert_eq!(stash.pop_ready(), None, "3 must wait for the next unstash");
        stash.unstash_all();
        assert_eq!(stash.pop_ready(), Some(3));
    }

    /// Replay order is stash-arrival order (FIFO), across multiple
    /// stash/unstash rounds.
    #[test]
    fn replay_order_is_arrival_order() {
        let mut stash = Stash::bounded(cap(4));
        stash.stash(1u32).expect("1");
        stash.stash(2u32).expect("2");
        stash.unstash_all();
        stash.stash(3u32).expect("3");
        stash.unstash_all(); // 3 joins BEHIND the already-ready 1, 2
        let drained: Vec<u32> = std::iter::from_fn(|| stash.pop_ready()).collect();
        assert_eq!(drained, vec![1, 2, 3]);
        assert!(stash.is_empty());
    }
}
```

Register the module in `crates/core/src/lib.rs` — add after the existing `pub mod restart;` line:

```rust
pub mod stash;
```

- [ ] **Step 2: Run the unit tests, verify they pass (struct + tests land together; the failing-first evidence is the compile failure before this task)**

```bash
git add crates/core/src/stash.rs crates/core/src/lib.rs
nix develop --command cargo nextest run -p bombay stash::
```

Expected: 4 tests PASS (`capacity_bounds_held_plus_ready`, `overflow_hands_the_exact_message_back`, `unstash_is_a_snapshot_not_a_live_view`, `replay_order_is_arrival_order`).

- [ ] **Step 3: fmt + clippy the new module**

```bash
nix develop --command cargo fmt
nix develop --command cargo clippy -p bombay --lib -- -D warnings
```

Expected: clean. (`Capacity::new` returns `Option` — the test helper's double-expect is test-only code.)

- [ ] **Step 4: Commit**

```bash
git add crates/core/src/stash.rs crates/core/src/lib.rs
git commit -m "core(stash): bounded two-queue Stash + StashFull typed handback [#224]"
```

---

### Task 2: `StashActor` trait + `Stashed<S>` wrapper

**Files:**
- Modify: `crates/core/src/stash.rs` (append below `Stash`)

- [ ] **Step 1: Append the trait + wrapper + in-step replay**

Append to `crates/core/src/stash.rs` (above the `tests` module). Extend the existing `use` block at the top of the file to:

```rust
use core::{any::type_name, future::Future};
use std::collections::VecDeque;

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::{Capacity, Mailboxed},
    message::Msg,
};
```

Then the new items:

```rust
/// Opt-in actor shape with deferral: [`Actor`]'s hooks, plus the stash as a
/// `handle` parameter. Implement this instead of `Actor`, then spawn
/// `Stashed::<Self>` — the wrapper owns the buffer and drives replay; there
/// is no wiring to forget.
pub trait StashActor: Mailboxed<Msg: Msg> + Sized + Send + 'static {
    /// The argument passed to [`on_start`](StashActor::on_start).
    type Args: Send;
    /// The actor's own domain error, kept typed end to end.
    type Error: ReplyError;

    /// Stash capacity, from the actor's own constructor input. Required and
    /// explicit — bounded is the point; there is no global default. Ignore
    /// `args` for a type-fixed bound, or thread it through for a
    /// spawn-tunable one. Never a `SpawnConfig` field (spec D8).
    fn stash_capacity(args: &Self::Args) -> Capacity;

    /// Builds the actor state. See [`Actor::on_start`].
    fn on_start(
        args: Self::Args,
        actor_ref: ActorRef<Stashed<Self>>,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    /// Handles one message; `stash` defers what the current state cannot
    /// accept ([`Stash::stash`]) and releases it ([`Stash::unstash_all`]).
    /// See [`Actor::handle`] for `stop` and error semantics.
    fn handle(
        &mut self,
        msg: Self::Msg,
        actor_ref: ActorRef<Stashed<Self>>,
        stash: &mut Stash<Self::Msg>,
        stop: &mut bool,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// See [`Actor::on_panic`]. No stash access: the state is poisoned and
    /// the stash dies with the incarnation (spec D6).
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Stashed<Self>>,
        err: PanicError,
    ) -> impl Future<Output = ActorStopReason> + Send {
        let _ = actor_ref;
        async move { ActorStopReason::Panicked(err) }
    }

    /// See [`Actor::on_stop`]. No stash access: whatever is still deferred
    /// at stop is dropped (spec D6) — a stashed ask's reply port drops with
    /// it and the asker sees the usual typed ask-side error.
    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Stashed<Self>>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let _ = (actor_ref, reason);
        async { Ok(()) }
    }
}

/// The only way to have a stash: framework-owned composition over user state.
/// A `Stashed<S>` is a plain [`Actor`] — every existing verb (spawn, tell,
/// ask, watch, supervise-as-child, timers, `Recipient`) works unchanged.
#[derive(Debug)]
pub struct Stashed<S: StashActor> {
    state: S,
    stash: Stash<S::Msg>,
}

impl<S: StashActor> Mailboxed for Stashed<S> {
    type Msg = S::Msg;
}

impl<S: StashActor> Actor for Stashed<S> {
    type Args = S::Args;
    type Error = S::Error;

    fn name() -> &'static str {
        // The user's type is the interesting name in logs, not the wrapper's.
        type_name::<S>()
    }

    async fn on_start(args: S::Args, actor_ref: ActorRef<Self>) -> Result<Self, S::Error> {
        let cap = S::stash_capacity(&args);
        let state = S::on_start(args, actor_ref).await?;
        Ok(Self {
            state,
            stash: Stash::bounded(cap),
        })
    }

    /// The whole replay mechanism (spec D3): run the user handler, then drain
    /// `ready` — still inside the current `handle_message` step, so replayed
    /// messages run ahead of the entire mailbox backlog, in stash-arrival
    /// order, under the step's own strong `actor_ref` (no upgrade, no
    /// drain-window hazard). A replayed handler's `Err`/panic/`stop` routes
    /// exactly as a delivered message's would.
    async fn handle(
        &mut self,
        msg: S::Msg,
        actor_ref: ActorRef<Self>,
        stop: &mut bool,
    ) -> Result<(), S::Error> {
        S::handle(&mut self.state, msg, actor_ref.clone(), &mut self.stash, stop).await?;
        while !*stop {
            let Some(m) = self.stash.pop_ready() else { break };
            S::handle(&mut self.state, m, actor_ref.clone(), &mut self.stash, stop).await?;
        }
        Ok(())
    }

    async fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> ActorStopReason {
        S::on_panic(&mut self.state, actor_ref, err).await
    }

    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), S::Error> {
        S::on_stop(&mut self.state, actor_ref, reason).await
    }
}
```

- [ ] **Step 2: Compile check (the integration tests in Task 3 are the behavior gate)**

```bash
nix develop --command cargo clippy -p bombay --lib -- -D warnings
```

Expected: clean compile. If clippy flags the `handle` fn length or arg count, the split point is extracting the replay `while` into a private `async fn drain_ready` on `Stashed<S>` — same semantics, do not change behavior.

- [ ] **Step 3: fmt + commit**

```bash
nix develop --command cargo fmt
git add crates/core/src/stash.rs
git commit -m "core(stash): StashActor trait + Stashed<S> wrapper with in-step replay [#224]"
```

---

### Task 3: Integration — replay order, snapshot, mid-batch stop

**Files:**
- Create: `crates/core/tests/stash.rs`

- [ ] **Step 1: Write the failing integration tests**

Create `crates/core/tests/stash.rs`:

```rust
//! Bounded-stash behavior through the public API (card #224): replay order
//! vs the mailbox backlog, snapshot semantics end to end, stop-mode fates,
//! and restart hygiene. Every terminal await is bounded.

use core::{convert::Infallible, num::NonZeroUsize};
use std::sync::{Arc, Mutex};

use tokio::time::timeout;

use bombay::{
    actor::{ActorRef, PreparedActor, Spawn, SpawnConfig},
    mailbox::{Capacity, Mailboxed, Signal},
    message::Msg,
    reply::ReplySender,
    stash::{Stash, StashActor, Stashed},
    test_support::terminate_bound,
};

fn cap(n: usize) -> Capacity {
    Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("valid")
}

/// The coffee-shop actor: `Item`s are stashed until `Open` flips the state;
/// serves are recorded on an external tape so post-mortem asserts work after
/// any stop mode.
struct Gate {
    open: bool,
    tape: Arc<Mutex<Vec<u32>>>,
}

#[derive(Debug)]
enum GateMsg {
    Open,
    Item(u32),
    /// Served like `Item`, then stops the actor (mid-batch stop probe).
    ItemThenStop(u32),
    Read(ReplySender<Vec<u32>>),
}

impl Msg for GateMsg {}
impl Mailboxed for Gate {
    type Msg = GateMsg;
}

impl StashActor for Gate {
    type Args = Arc<Mutex<Vec<u32>>>;
    type Error = Infallible;

    fn stash_capacity(_: &Self::Args) -> Capacity {
        cap(8)
    }

    async fn on_start(tape: Self::Args, _: ActorRef<Stashed<Self>>) -> Result<Self, Infallible> {
        Ok(Self { open: false, tape })
    }

    async fn handle(
        &mut self,
        msg: GateMsg,
        _: ActorRef<Stashed<Self>>,
        stash: &mut Stash<GateMsg>,
        stop: &mut bool,
    ) -> Result<(), Infallible> {
        match msg {
            GateMsg::Open => {
                self.open = true;
                stash.unstash_all();
            }
            item @ (GateMsg::Item(_) | GateMsg::ItemThenStop(_)) if !self.open => {
                stash.stash(item).expect("test stash sized for the scenario");
            }
            GateMsg::Item(n) => self.tape.lock().expect("tape").push(n),
            GateMsg::ItemThenStop(n) => {
                self.tape.lock().expect("tape").push(n);
                *stop = true;
            }
            GateMsg::Read(reply) => drop(reply.send(self.tape.lock().expect("tape").clone())),
        }
        Ok(())
    }
}

fn tape() -> Arc<Mutex<Vec<u32>>> {
    Arc::new(Mutex::new(Vec::new()))
}

fn read(tape: &Arc<Mutex<Vec<u32>>>) -> Vec<u32> {
    tape.lock().expect("tape").clone()
}

/// Spawns a `Stashed<Gate>` with the message sequence pre-queued before the
/// loop starts — a deterministic mailbox, no racing sends.
fn spawn_prequeued(
    msgs: Vec<GateMsg>,
    tape: Arc<Mutex<Vec<u32>>>,
) -> ActorRef<Stashed<Gate>> {
    let prepared = PreparedActor::<Stashed<Gate>>::new(SpawnConfig {
        capacity: cap(16),
        ..SpawnConfig::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    for msg in msgs {
        actor_ref
            .mailbox_sender()
            .try_send_message(msg)
            .expect("pre-queue fits the mailbox");
    }
    let _join = prepared.spawn(tape);
    actor_ref
}

/// Invariant 3 — the load-bearing ordering test. Queue [1, 2, Open, 3]:
/// 1 and 2 are stashed; Open unstashes; 3 sits in the mailbox backlog behind
/// Open. Replay runs in-step, so serves land [1, 2, 3]. A tail-reinject
/// implementation would serve [3, 1, 2]; a FIFO-breaking stash would permute
/// 1 and 2. Fails on either.
#[tokio::test]
async fn replay_runs_before_backlog_in_arrival_order() {
    let t = tape();
    let actor_ref = spawn_prequeued(
        vec![
            GateMsg::Item(1),
            GateMsg::Item(2),
            GateMsg::Open,
            GateMsg::Item(3),
        ],
        Arc::clone(&t),
    );
    let seen = timeout(terminate_bound(), async {
        loop {
            let seen = actor_ref
                .ask(GateMsg::Read)
                .await
                .expect("alive while draining");
            if seen.len() >= 3 {
                return seen;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all three serves within the bound");
    assert_eq!(seen, vec![1, 2, 3], "stash replays first, in arrival order");
}

/// D2 snapshot end to end: after the first Open's batch drains, the stash
/// must be EMPTY — a later serve must not drag along any stale replay, and
/// order stays [1, 2].
#[tokio::test]
async fn message_stashed_after_snapshot_waits_for_next_unstash() {
    let t = tape();
    // Close-after-open variant: reuse Gate but drive it so 2 arrives while
    // closed again. Simplest deterministic driver: two rounds.
    let actor_ref = spawn_prequeued(vec![GateMsg::Item(1), GateMsg::Open], Arc::clone(&t));
    let first = timeout(terminate_bound(), async {
        loop {
            let seen = actor_ref.ask(GateMsg::Read).await.expect("alive");
            if seen.len() >= 1 {
                return seen;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first replay lands");
    assert_eq!(first, vec![1]);
    // Round 2: the gate is open now, so Item(2) serves directly — this
    // asserts the stash is EMPTY after its snapshot drained (nothing stale
    // replays alongside).
    actor_ref.tell(GateMsg::Item(2)).await.expect("deliver 2");
    let second = timeout(terminate_bound(), async {
        loop {
            let seen = actor_ref.ask(GateMsg::Read).await.expect("alive");
            if seen.len() >= 2 {
                return seen;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second serve lands");
    assert_eq!(second, vec![1, 2], "no stale replay, no reorder");
}

/// D6 mid-batch stop: a replayed message that sets `stop` ends the batch —
/// the rest of the stash is never served. Queue [ItemThenStop(1), Item(2),
/// Open]: both stash; replay serves 1 (stop) and must NOT serve 2.
#[tokio::test]
async fn replayed_stop_abandons_rest_of_batch() {
    let t = tape();
    let actor_ref = spawn_prequeued(
        vec![
            GateMsg::ItemThenStop(1),
            GateMsg::Item(2),
            GateMsg::Open,
        ],
        Arc::clone(&t),
    );
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor stops after the replayed stop");
    assert_eq!(read(&t), vec![1], "batch ends at stop; 2 is abandoned");
}
```

- [ ] **Step 2: Run, verify current failures are only where expected**

```bash
git add crates/core/tests/stash.rs
nix develop --command cargo nextest run -p bombay --test stash
```

Expected: **compile errors only if Task 2 diverged** (fix Task 2, not the tests). Once compiling, all 3 tests must PASS — if `replay_runs_before_backlog_in_arrival_order` fails with `[3, 1, 2]`-style order, the replay loop is not in-step; fix `Stashed::handle`, never the assertion.

Note: if `mailbox_sender().try_send_message(msg)` does not exist under that exact name, find the real pre-queue verb with `grep -n "fn try_send\|fn send_message" crates/core/src/mailbox.rs` and use it — `control_lane.rs` pre-queues with plain `tell` before spawn, which is also acceptable (`bounded(actor_ref.tell(msg)).await`).

- [ ] **Step 3: fmt + commit**

```bash
nix develop --command cargo fmt
git add crates/core/tests/stash.rs
git commit -m "test(stash): replay-before-backlog order, snapshot, mid-batch stop [#224]"
```

---### Task 4: Stop-fate integration — Collected/pin, in-band Stop, kill

**Files:**
- Modify: `crates/core/tests/stash.rs` (append)

- [ ] **Step 1: Append the three stop-fate tests**

```rust
/// Invariants 4 + 7: a non-empty stash does not pin. Drop every external
/// ref while a message sits stashed → the actor ref-count-stops (Collected)
/// within the bound, and the stashed message is never served.
#[tokio::test]
async fn stashed_messages_do_not_pin_refcount_stop() {
    let t = tape();
    let actor_ref = spawn_prequeued(vec![GateMsg::Item(1)], Arc::clone(&t));
    // Sync: wait until the message was taken (and stashed) so the drop below
    // races nothing.
    let seen = timeout(terminate_bound(), actor_ref.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive");
    assert_eq!(seen, Vec::<u32>::new(), "1 is stashed, not served");
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("non-empty stash must not keep the actor alive");
    assert_eq!(read(&t), Vec::<u32>::new(), "deferred message dies deferred");
}

/// Invariant 5: in-band `Signal::Stop` with a non-empty stash — the actor
/// stops Normal and the stashed message is never served (Stop abandons the
/// queued backlog; the deferred backlog ranks no higher, spec D6).
#[tokio::test]
async fn inband_stop_drops_stash() {
    let t = tape();
    let actor_ref = spawn_prequeued(vec![GateMsg::Item(1)], Arc::clone(&t));
    timeout(terminate_bound(), actor_ref.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive — 1 is stashed");
    timeout(
        terminate_bound(),
        actor_ref.mailbox_sender().send(Signal::Stop),
    )
    .await
    .expect("send within bound")
    .expect("stop enqueued");
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("in-band stop lands");
    assert_eq!(read(&t), Vec::<u32>::new(), "stash dropped on Stop");
}

/// Invariant 6: `kill()` with a non-empty stash — hard abort, stashed
/// message never served.
#[tokio::test]
async fn kill_drops_stash() {
    let t = tape();
    let actor_ref = spawn_prequeued(vec![GateMsg::Item(1)], Arc::clone(&t));
    timeout(terminate_bound(), actor_ref.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive — 1 is stashed");
    actor_ref.kill();
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("kill lands");
    assert_eq!(read(&t), Vec::<u32>::new(), "stash dropped on kill");
}
```

- [ ] **Step 2: Run + verify PASS**

```bash
nix develop --command cargo nextest run -p bombay --test stash
```

Expected: all 6 tests PASS. `ask(GateMsg::Read)` — if the ask builder takes a closure (`.ask(|reply| GateMsg::Read(reply))`, the `timer.rs` shape), use that form in all tests; check with `grep -n "fn ask" crates/core/src/actor/actor_ref.rs`.

- [ ] **Step 3: fmt + commit**

```bash
nix develop --command cargo fmt
git add crates/core/tests/stash.rs
git commit -m "test(stash): stop-fate matrix — Collected non-pinning, in-band Stop, kill [#224]"
```

---

### Task 5: Restart hygiene — no stale stash across incarnations

**Files:**
- Modify: `crates/core/tests/stash.rs` (append)

- [ ] **Step 1: Append the supervised-restart test**

Extend the `GateMsg` enum in this test file with a panic trigger (add the variant to the existing enum and a `Boom` arm to `Gate::handle`):

```rust
// in GateMsg:
    /// Panics the handler (restart probe).
    Boom,

// in Gate::handle's match, alongside the other arms (place BEFORE the
// `if !self.open` guard arm so it always fires):
    GateMsg::Boom => panic!("gate boom"),
```

Then append (imports to add at the top of the file:
`use bombay::actor::{Supervisor, SpawnSupervised, Watch};`
`use bombay::restart::{RestartConfig, RestartPolicy};`
`use bombay::test_support::set_supervisor_rng_seed;`
`use core::time::Duration;`):

```rust
/// Minimal supervisor: exists only to own the `Stashed<Gate>` child.
struct Sup;

#[derive(Debug)]
struct SupMsg;
impl Msg for SupMsg {}
impl Mailboxed for Sup {
    type Msg = SupMsg;
}
impl bombay::actor::Actor for Sup {
    type Args = ();
    type Error = Infallible;
    async fn on_start(
        (): (),
        _: ActorRef<Self>,
    ) -> Result<Self, Infallible> {
        Ok(Sup)
    }
    async fn handle(
        &mut self,
        _: SupMsg,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Infallible> {
        Ok(())
    }
}
impl Watch for Sup {}
impl Supervisor for Sup {}

/// Invariant 8: restart = new incarnation from Args; a stale stash must not
/// leak across incarnations. Incarnation 0 stashes 1 then panics; the
/// rebuilt incarnation is Opened and must serve NOTHING from the old stash.
#[tokio::test(start_paused = true)]
async fn restart_gets_a_fresh_stash() {
    set_supervisor_rng_seed(Some(7));
    let t = tape();
    let sup_ref = Sup::spawn_supervised(());

    let spawned: Arc<Mutex<Vec<ActorRef<Stashed<Gate>>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let factory_tape = Arc::clone(&t);
    let factory_spawned = Arc::clone(&spawned);
    let config = RestartConfig::new(RestartPolicy::Permanent)
        .with_min_backoff(Duration::from_millis(1))
        .with_max_backoff(Duration::from_millis(1));
    timeout(
        terminate_bound(),
        sup_ref.supervise(config, move || {
            let child = Stashed::<Gate>::spawn(Arc::clone(&factory_tape));
            factory_spawned.lock().expect("spawned").push(child.clone());
            child
        }),
    )
    .await
    .expect("supervise within bound")
    .expect("supervisor alive");

    // Incarnation 0: stash 1 (closed gate), sync, then crash it.
    let inc0 = spawned.lock().expect("spawned")[0].clone();
    timeout(terminate_bound(), inc0.tell(GateMsg::Item(1)))
        .await
        .expect("tell within bound")
        .expect("delivered");
    timeout(terminate_bound(), inc0.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive — 1 is stashed");
    timeout(terminate_bound(), inc0.tell(GateMsg::Boom))
        .await
        .expect("tell within bound")
        .expect("boom delivered");

    // Paused clock: advance past the 1 ms backoff until incarnation 1 exists.
    let inc1 = timeout(terminate_bound(), async {
        loop {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if let Some(child) = spawned.lock().expect("spawned").get(1).cloned() {
                return child;
            }
        }
    })
    .await
    .expect("rebuild within bound");

    // Open the fresh incarnation: a leaked stash would now serve 1.
    timeout(terminate_bound(), inc1.tell(GateMsg::Open))
        .await
        .expect("tell within bound")
        .expect("open delivered");
    let seen = timeout(terminate_bound(), inc1.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive");
    assert_eq!(
        seen,
        Vec::<u32>::new(),
        "a stale stash must not survive restart (fresh incarnation from Args)"
    );
}
```

- [ ] **Step 2: Run + verify PASS**

```bash
nix develop --command cargo nextest run -p bombay --test stash
```

Expected: all 7 tests PASS. If `Sup::spawn_supervised` needs different args or the seed helper differs, model on `crates/core/tests/control_lane.rs` (working supervised tests) — adjust plumbing, never the final assert.

- [ ] **Step 3: fmt + commit**

```bash
nix develop --command cargo fmt
git add crates/core/tests/stash.rs
git commit -m "test(stash): restart builds a fresh stash — no cross-incarnation leak [#224]"
```

---

### Task 6: ADR-0022

**Files:**
- Create: `docs/adr/0022-bounded-stash.md`
- Modify: `docs/adr/README.md` (append index row, matching its existing format)

- [ ] **Step 1: Write the ADR**

```markdown
# ADR-0022 — Bounded stash: `Stashed<S>` composition, in-step replay

**Status:** Accepted (2026-07-30) — implemented under card #224.

## Context

A fixed-interface actor must handle every menu variant in every state or drop
it; multi-phase protocols hand-roll unbounded `Vec<Msg>` buffers per actor.
The literature names the need (conditional synchronization: Briot–Guerraoui–
Löhr 1998; De Koster et al. 2016 §4.2) and the family-correct shape for a
fixed-interface FIFO actor: a separate runtime-owned buffer whose replay
preserves arrival order (SALSA's partial-message mailbox), with guaranteed
delivery — overflow refuses loudly, silent drop is forbidden.

## Decision

- **Composition, not a trait accessor:** `Stashed<S: StashActor>` owns the
  buffer; user state composes in; `handle` receives `&mut Stash`. A stash
  cannot exist outside the wrapper (`Stash::bounded` is crate-private), so
  the v1 field-plus-accessor forget-trap (silent forever-defer) is
  unrepresentable. Compile-time enforcement over runtime discipline
  (ADR-0015 precedent).
- **In-step replay:** `Stashed::handle` runs the user handler, then drains
  the ready queue in the same `handle_message` step — ahead of the mailbox
  backlog, in stash-arrival order, under the step's strong `actor_ref`.
  Zero changes to `kind.rs`/the loops. Top-of-loop drains are unsound in the
  drain window (`WeakActorRef::upgrade` → `None` while queued messages still
  self-pin, ADR-0003/0010).
- **Two queues, snapshot unstash:** `stash()` → `held`; `unstash_all()`
  moves `held` → `ready`; replay pops `ready`. Mid-replay stashes wait for
  the next unstash.
- **Non-pinning:** the stash holds bare messages (no `self_sender`); a
  non-empty stash never keeps an actor alive (`Collected` reachable —
  ADR-0020, timer precedent ADR-0018).
- **Bounded with typed handback:** capacity from `stash_capacity(&args)`
  (constructor input, never `SpawnConfig`); overflow returns
  `StashFull<M>` carrying the message back (`TellError` precedent).

## Stop-fate table

Only `unstash_all` rescues a deferred message. Every terminal path drops the
remainder: Collected, in-band Stop, out-of-band stop(), kill(), panic/`Err`,
and a mid-batch stop/crash. Restart rebuilds from `Args` via
`Stashed::on_start` → structurally fresh stash each incarnation.

## Consequences

- A second trait surface (`StashActor`) mirrors `Actor`'s hooks; use-site
  type is `ActorRef<Stashed<S>>`.
- Stash access is `handle`-only — hook-triggered replay is impossible by
  design (follow-up card if ever needed). `Stashed<S>` as a *watcher*
  (`Watch`/`Supervisor` impls) is deferred to first concrete use; it is
  fully watchable/supervisable as a target today.
- Self-inflicted livelock (re-stash + unstash every replay) remains a user
  bug, documented on `unstash_all`.

Design record: docs/superpowers/specs/2026-07-30-224-bounded-stash-design.md.
```

- [ ] **Step 2: Append the ADR-0022 row to `docs/adr/README.md`'s index, in the same style as the 0021 row.**

- [ ] **Step 3: Commit**

```bash
git add docs/adr/0022-bounded-stash.md docs/adr/README.md
git commit -m "docs(adr): ADR-0022 bounded stash — Stashed<S> composition [#224]"
```

---

### Task 7: Mutants baseline

**Files:**
- Modify: `mutants-baseline.json`

- [ ] **Step 1: List the new mutant sites**

```bash
nix develop --command cargo mutants --list -p bombay -f crates/core/src/stash.rs
```

Expected: a list of mutants for `Stash::{bounded,len,is_empty,stash,unstash_all,pop_ready}`, `StashFull::{msg,capacity}`, and the `Stashed`/`StashActor` impls.

- [ ] **Step 2: Add baseline entries**

Follow the file's existing two-section structure (path-keyed floors map + zero-viable list — see the `crates/core/src/actor/spawn.rs` entries as the format reference). Every new fn gets an entry; fns whose only mutants are caught by the Task 1–5 tests get their floor; genuinely unviable mutants (e.g. `name()` string swap) go to the zero-viable list with a reason.

- [ ] **Step 3: Validate the gate locally, then commit**

```bash
git add mutants-baseline.json
nix build .#mutants 2>&1 | tail -5
```

Expected: the mutants check passes (look for the derivation actually running — `building '...drv'` — a silent pass may be cache; and never mask the exit code with a bare `| tail`, check `$?` or `set -o pipefail`).

```bash
git commit -m "chore(mutants): baseline entries for stash module [#224]"
```

---

### Task 8: README bullet + coverage baseline

**Files:**
- Modify: `README.md` (public-API section)
- Modify: `docs/testing/coverage-baseline.md`

- [ ] **Step 1: Add one public-API bullet to `README.md`** in the *public API at a glance* section, alongside the existing actor-surface bullets:

```markdown
- **Bounded stash** — defer messages your current state can't accept:
  implement `StashActor` (handler receives `&mut Stash`), spawn
  `Stashed::<You>`; `unstash_all` replays ahead of the mailbox backlog in
  arrival order; overflow hands the message back (`StashFull`), and a
  stashed message never keeps a dying actor alive.
```

- [ ] **Step 2: Record the new test file/coverage movement in `docs/testing/coverage-baseline.md`** following its existing per-module format (new module `stash.rs`: 4 unit + `tests/stash.rs`: 7 integration).

- [ ] **Step 3: Commit**

```bash
git add README.md docs/testing/coverage-baseline.md
git commit -m "docs: README stash bullet + coverage baseline [#224]"
```

---

### Task 9: Walking skeleton — job-queue intake gate

**Files:**
- Modify: `crates/core/examples/job_queue/app.rs` (new `Intake` section)
- Modify: `crates/core/examples/job_queue/main.rs` (wire intake in front of the dispatcher if the demo flow submits directly)
- Modify: `crates/core/tests/app_job_queue.rs` (new test)

- [ ] **Step 1: Add the `Intake` actor to `app.rs`**

A `Stashed<Intake>` front door for submissions: during maintenance it stashes
`Submit`s (askers wait on their reply ports); `Resume` unstashes and forwards
to the dispatcher — a real two-phase deferral in the compositional app.

```rust
// ------------------------------------------------------------ intake -------

/// Front door for submissions (card #224): a `Stashed` actor that defers
/// `Submit`s during maintenance and forwards them on `Resume` — the
/// walking-skeleton demonstration of the bounded stash.
pub struct Intake {
    dispatcher: ActorRef<Dispatcher>,
    maintenance: bool,
}

#[derive(Debug, bombay_macros::Msg)]
pub enum IntakeMsg {
    Submit {
        job: Job,
        reply: ReplySender<(), SubmitError>,
    },
    Pause,
    Resume,
}

impl bombay::stash::StashActor for Intake {
    type Args = (ActorRef<Dispatcher>, Capacity);
    type Error = Infallible;

    fn stash_capacity((_, cap): &Self::Args) -> Capacity {
        *cap
    }

    async fn on_start(
        (dispatcher, _): Self::Args,
        _: ActorRef<bombay::stash::Stashed<Self>>,
    ) -> Result<Self, Infallible> {
        Ok(Self {
            dispatcher,
            maintenance: false,
        })
    }

    async fn handle(
        &mut self,
        msg: IntakeMsg,
        _: ActorRef<bombay::stash::Stashed<Self>>,
        stash: &mut bombay::stash::Stash<IntakeMsg>,
        _: &mut bool,
    ) -> Result<(), Infallible> {
        match msg {
            IntakeMsg::Pause => self.maintenance = true,
            IntakeMsg::Resume => {
                self.maintenance = false;
                stash.unstash_all();
            }
            submit @ IntakeMsg::Submit { .. } if self.maintenance => {
                // Full stash = shed load with the same typed refusal the
                // dispatcher uses for a full queue: the asker learns NOW.
                if let Err(overflow) = stash.stash(submit) {
                    if let IntakeMsg::Submit { reply, .. } = overflow.msg() {
                        drop(reply.send(Err(SubmitError::QueueFull)));
                    }
                }
            }
            IntakeMsg::Submit { job, reply } => {
                // Forward: the dispatcher answers the original asker directly.
                if self
                    .dispatcher
                    .tell(DispatcherMsg::Submit { job, reply })
                    .await
                    .is_err()
                {
                    // Dispatcher gone: nothing to answer with — the dropped
                    // reply port surfaces the typed ask-side error.
                }
            }
        }
        Ok(())
    }
}
```

Adjust names to the file's actual imports (`ReplySender`, `SubmitError::QueueFull` — verify the exact variant with `grep -n "enum SubmitError" -A 6 crates/core/examples/job_queue/app.rs`; if the shed-load variant is named differently, use that one).

- [ ] **Step 2: Add the integration test to `crates/core/tests/app_job_queue.rs`**

Model the harness/setup on the file's existing tests (reuse its builders). The
new test, in that file's style:

```rust
/// #224 walking skeleton: pause intake, submit while paused (deferred, asker
/// still waiting), resume — the deferred submissions complete in order,
/// ahead of a post-resume submission.
#[tokio::test]
async fn intake_defers_submissions_during_maintenance() {
    // 1. Bring up the app as the neighboring tests do, plus an Intake in
    //    front: Stashed::<Intake>::spawn((dispatcher_ref, cap(8))).
    // 2. tell(IntakeMsg::Pause).
    // 3. ask Submit job A — hold the pending reply future (do NOT await it
    //    to completion yet; wrap in tokio::spawn).
    // 4. tell(IntakeMsg::Resume); then ask Submit job B.
    // 5. Await both replies within terminate_bound(): A completes Ok before
    //    B (replay-before-backlog through the full app stack).
}
```

(The comment lines are the test's spec — write them as real code against the
file's existing helpers; the neighboring `supervise`/drain tests show every
needed builder. The assert is the completion order A-then-B plus both `Ok`.)

- [ ] **Step 3: Run the app tests + example build**

```bash
nix develop --command cargo nextest run -p bombay --test app_job_queue
nix develop --command cargo build -p bombay --examples
```

Expected: PASS / clean build.

- [ ] **Step 4: fmt + commit**

```bash
nix develop --command cargo fmt
git add crates/core/examples/job_queue/ crates/core/tests/app_job_queue.rs
git commit -m "example(job_queue): Stashed intake gate — maintenance-mode deferral [#224]"
```

---

### Task 10: Gate, push, PR

- [ ] **Step 1: Full gate (tracked files only count — verify nothing is untracked first)**

```bash
git status --short   # expect: clean
set -o pipefail; nix flake check 2>&1 | tail -20
```

Expected: all checks pass. A silent instant pass on a check that should have rebuilt = cached; confirm the test derivations logged `building`.

- [ ] **Step 2: Push + PR**

```bash
git push -u origin feat/224-bounded-stash
gh pr create --repo devrandom-labs/bombay \
  --title "core(stash): bounded stash — Stashed<S> composition with in-step replay [#224]" \
  --body "$(cat <<'EOF'
Closes #224.

Bounded deferral per the v2 design spec (docs/superpowers/specs/2026-07-30-224-bounded-stash-design.md) + ADR-0022:

- `Stash<M>`: two-queue bounded buffer; `StashFull<M>` typed overflow handback (message recovered, never dropped/panicked).
- `StashActor` + `Stashed<S>`: framework-owned composition — a stash cannot exist unwired (forget-trap unrepresentable); replay runs in-step, ahead of the mailbox backlog, in arrival order. `kind.rs` untouched.
- Non-pinning (Collected reachable with a non-empty stash); stop-fate matrix asserted per mode (Stop / stop() / kill / Collected / panic); restart gets a structurally fresh stash.
- Walking skeleton: job-queue intake gate defers submissions during maintenance.

Checklist mapping: every card invariant has a named test in crates/core/src/stash.rs (unit) or crates/core/tests/stash.rs (integration); wiring = ADR-0022, mutants baseline, README bullet, coverage baseline, job-queue extension.
EOF
)"
```

- [ ] **Step 3: Watch CI (`Nix Flake Check`)** — a red in ~10-20 s on a new branch is the known GitHub 503 flake-input eval issue; rerun before diagnosing.

---

## Self-review notes (kept honest)

- Task 3 Step 2 and Task 4 Step 2 carry **explicit adjust-the-plumbing-not-the-assert escape hatches** for the two API shapes most likely to differ (`try_send_message`, ask-closure form) — verified names where possible, flagged where not.
- Task 5's supervised harness mirrors `control_lane.rs` (verified: `supervise(config, factory)` at `actor_ref.rs:377`, `RestartConfig::new(...).with_min_backoff(...)` at `control_lane.rs:213`).
- Task 9 Step 2 is deliberately spec-shaped rather than fully-coded: it must reuse `app_job_queue.rs`'s existing builders, which the implementer reads first. The assert (A completes Ok before B, both Ok) is fixed.
