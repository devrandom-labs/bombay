# Actor trait, lifecycle hooks & run-loop (#116) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the local actor spine in `bombay-core` — the `Actor` trait, its lifecycle hooks (`on_start`/`handle`/`on_panic`/`on_stop`), the run-loop that drives it, and the spawn entry points — Send-saturated, with a minimal `ActorRef` scaffold.

**Architecture:** `on_start` (builds state) → `loop { CancellationToken::run_until_cancelled(mailbox.recv()) }` → `on_stop`, with four `catch_unwind` sites tagging `PanicReason`. No startup buffer (the bounded flume mailbox is the FIFO buffer), no `select!` (a plain `match` on `run_until_cancelled`), no `VecDeque`. `&mut self` is kept and **poisoned-on-panic** (discard, never resume). Graceful stop via `CancellationToken`/`Signal::Stop`; hard kill via `futures::Abortable` (skips `on_stop`).

**Tech Stack:** Rust (edition 2024, stable), `tokio` + `tokio-util` (`CancellationToken`), `futures` (`Abortable`, `catch_unwind`), `flume` (mailbox, already shipped), `thiserror`/`downcast-rs` (errors, already shipped).

**Spec:** `docs/superpowers/specs/2026-07-05-116-actor-trait-loop-design.md`

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `Cargo.toml` (root) | add `tokio-util` to `[workspace.dependencies]` | 1 |
| `bombay-core/Cargo.toml` | add `tokio-util`, `futures` deps | 1 |
| `bombay-core/src/error.rs` | add `PanicError::from_panic_any` | 2 |
| `bombay-core/src/actor/mod.rs` | the `Actor` trait + `Spawn` ext-trait + re-exports | 3, 11 |
| `bombay-core/src/actor/actor_ref.rs` | minimal `ActorRef`/`WeakActorRef` scaffold | 3 |
| `bombay-core/src/actor/kind.rs` | run-loop (`ActorBehaviour` helpers) | 4–10 |
| `bombay-core/src/actor/spawn.rs` | `PreparedActor`, `RunResult`, `run`/`spawn` | 4–11 |
| `bombay-core/src/lib.rs` | `pub mod actor;` | 3 |

**Clippy note (god-level bar, all new code):** functions ≤ 80 lines, cognitive-complexity ≤ 9, ≤ 5 args, **no** `unwrap`/`expect`/`panic`/`unreachable` in library code. Where a provably-safe `expect` is unavoidable (the default-capacity constant), use `#[expect(clippy::expect_used, reason = "…")]` on that line and cover it with a unit test. Run `cargo fmt` before every commit (the fmt gate is strict). The clippy gate checks lib + bins only, so `#[cfg(test)]` code is currently ungated — but write it clean anyway.

**Test harness note:** all tests live in `#[cfg(test)] mod tests` at the bottom of each file. Behavioral tests use `#[tokio::test]`; deterministic ordering tests use `flavor = "current_thread"`; real-overlap tests use `flavor = "multi_thread"` + `tokio::sync::Barrier`. Run a single test with `cargo test -p bombay-core <name>`.

---

## Task 1: Add dependencies

**Files:**
- Modify: `Cargo.toml` (root `[workspace.dependencies]`)
- Modify: `bombay-core/Cargo.toml`

- [ ] **Step 1: Add `tokio-util` to the workspace dependency list**

In root `Cargo.toml`, inside `[workspace.dependencies]`, add after the `flume` line:

```toml
# Cooperative cancellation for the actor run-loop (card #116, absorbs #55).
# CancellationToken::run_until_cancelled drives graceful stop without a select!.
tokio-util = "0.7"
```

- [ ] **Step 2: Add the deps to `bombay-core`**

In `bombay-core/Cargo.toml`, under `[dependencies]`, add:

```toml
tokio-util = { workspace = true }
futures = { workspace = true }
```

(`futures` is already declared in the root `[workspace.dependencies]` as `futures = "0.3"`; `tokio-util` was added in Step 1.)

- [ ] **Step 3: Verify it builds and regenerate the workspace-hack**

Run: `nix develop --command cargo build -p bombay-core`
Expected: builds clean (no code uses the new deps yet).

Then regenerate hakari (mirrors nexus; required after any dependency change):

Run: `nix develop --command cargo hakari generate`
Expected: updates the workspace-hack crate (may be a no-op).

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml bombay-core/Cargo.toml
git add -A   # picks up any hakari-generated changes
git commit -m "build(core): add tokio-util + futures for the #116 run-loop"
```

---

## Task 2: `PanicError::from_panic_any`

The run-loop's `catch_unwind` yields `Box<dyn Any + Send>`; this converts it into an inspectable `PanicError`. `error.rs` already earmarks this in a `DEFERRED` comment. The overwhelmingly common panic payloads are `&'static str` and `String`; other types are recorded with a placeholder (the concrete type is not recoverable from `dyn Any` without knowing it).

**Files:**
- Modify: `bombay-core/src/error.rs` (the `impl PanicError` block, ~line 215; and its `tests` module)

- [ ] **Step 1: Write the failing test**

In `bombay-core/src/error.rs`, inside `mod tests`, add:

```rust
/// A caught panic arrives as `Box<dyn Any + Send>` from `catch_unwind`. The two
/// common payloads — `&'static str` and `String` — are recovered as a string;
/// the phase is preserved. This is the loop's bridge from an unwind to a value.
#[test]
fn from_panic_any_recovers_string_payloads() {
    let from_str = PanicError::from_panic_any(Box::new("boom"), PanicReason::HandlerPanic);
    assert_eq!(from_str.with_str(str::to_owned), Some(String::from("boom")));
    assert_eq!(from_str.reason(), PanicReason::HandlerPanic);

    let from_string =
        PanicError::from_panic_any(Box::new(String::from("kaboom")), PanicReason::OnStart);
    assert_eq!(from_string.with_str(str::to_owned), Some(String::from("kaboom")));
    assert_eq!(from_string.reason(), PanicReason::OnStart);
}

/// A non-string panic payload (an arbitrary type) cannot be recovered as its
/// concrete type from `dyn Any` without knowing it, so `from_panic_any` records
/// a stable placeholder string and preserves the phase. The placeholder must be
/// a recoverable `&str`, so a supervisor can still log *something*.
#[test]
fn from_panic_any_records_placeholder_for_non_string_payload() {
    let panic = PanicError::from_panic_any(Box::new(42_u64), PanicReason::OnPanic);
    assert_eq!(panic.reason(), PanicReason::OnPanic);
    assert_eq!(
        panic.with_str(str::to_owned),
        Some(String::from("non-string panic payload")),
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `nix develop --command cargo test -p bombay-core from_panic_any`
Expected: FAIL — `no function or associated item named 'from_panic_any' found`.

- [ ] **Step 3: Implement `from_panic_any`**

In `bombay-core/src/error.rs`, add `use std::any::Any;` — the file already has `use std::{fmt, sync::Arc};`, so change it to:

```rust
use std::{any::Any, fmt, sync::Arc};
```

Then inside `impl PanicError`, replace the `// DEFERRED — from_panic_any …` comment with:

```rust
    /// Builds a `PanicError` from a caught unwind payload (`catch_unwind` yields
    /// `Box<dyn Any + Send>`), tagging it with the phase that produced it.
    ///
    /// The common payloads — `&'static str` and `String` — are recovered as a
    /// string. An arbitrary payload cannot be recovered as its concrete type
    /// from `dyn Any` without naming it, so it is recorded as a stable
    /// placeholder string (still inspectable via [`with_str`](Self::with_str)).
    #[must_use]
    pub fn from_panic_any(payload: Box<dyn Any + Send>, reason: PanicReason) -> Self {
        let err: Box<dyn ReplyError> = match payload.downcast::<String>() {
            Ok(message) => Box::new(*message),
            Err(payload) => match payload.downcast::<&'static str>() {
                Ok(message) => Box::new(*message),
                Err(_unknown) => Box::new("non-string panic payload"),
            },
        };
        Self::new(err, reason)
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `nix develop --command cargo test -p bombay-core from_panic_any`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add bombay-core/src/error.rs
git commit -m "core(error): PanicError::from_panic_any — bridge catch_unwind to a value (#116)"
```

---

## Task 3: `Actor` trait + minimal `ActorRef`/`WeakActorRef`

Introduce the `actor` module, the trait, and the minimal ref scaffold. This task compiles the trait + ref together (they are mutually referential) and unit-tests the ref's `downgrade`/`upgrade`/`id`.

**Files:**
- Create: `bombay-core/src/actor/mod.rs`
- Create: `bombay-core/src/actor/actor_ref.rs`
- Modify: `bombay-core/src/lib.rs`

- [ ] **Step 1: Write the module skeleton (compile target for the test)**

Create `bombay-core/src/actor/mod.rs`:

```rust
//! The local actor spine (card #116): the `Actor` trait, its lifecycle hooks,
//! the run-loop that drives it, and the spawn entry points.
//!
//! Send-saturated for now; the cfg-gated `MaybeSend` relaxation for
//! single-threaded client builds is a dedicated later sweep (#9). The `ActorRef`
//! here is a **minimal scaffold** — ref-count-driven stop, `Recipient` erasure,
//! and the `tell`/`ask` builders are #117/#118.

use core::{any::type_name, future::Future};

use crate::{
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::Mailboxed,
    message::Msg,
};

mod actor_ref;

pub use self::actor_ref::{ActorRef, WeakActorRef};

/// A single-writer, identity-agnostic unit of concurrency: owned state behind a
/// mailbox, driven by one task that handles messages sequentially.
///
/// `Actor` is a subtrait of [`Mailboxed`] (the mailbox is keyed on the actor),
/// and its message type is bounded `: Msg` so every actor's `Msg` gets the
/// compile-time slot-size tripwire (card #114).
///
/// # Panics & poisoning
///
/// A panic in `handle` is caught and routed to [`on_panic`](Actor::on_panic);
/// the actor then **stops** (there is no resume). After a panic `&mut self` is
/// **poisoned** (torn state): [`on_stop`](Actor::on_stop) still runs and may do
/// reason-independent resource release only — it must **never** flush or derive
/// from domain fields, which are torn.
pub trait Actor: Mailboxed<Msg: Msg> + Sized + Send + 'static {
    /// The argument passed to [`on_start`](Actor::on_start) to build the state.
    type Args: Send;
    /// The actor's own domain error, kept typed end to end.
    type Error: ReplyError;

    /// A human-readable name for logs/tracing. Defaults to the type name.
    #[must_use]
    fn name() -> &'static str {
        type_name::<Self>()
    }

    /// Builds (or hydrates) the actor state. Runs to completion before any
    /// message is handled; messages that arrive meanwhile wait in the mailbox.
    fn on_start(
        args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    /// Handles one message. Set `*stop = true` to stop the actor cleanly after
    /// this handler returns `Ok`. A returned `Err` is treated as a controlled
    /// crash (routed to `on_panic`, then stop).
    fn handle(
        &mut self,
        msg: Self::Msg,
        actor_ref: ActorRef<Self>,
        stop: &mut bool,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Observes a caught panic and names the terminal stop reason. Infallible
    /// and stop-only — it cannot resume the actor. `&mut self` is poisoned.
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> impl Future<Output = ActorStopReason> + Send {
        let _ = actor_ref;
        async move { ActorStopReason::Panicked(err) }
    }

    /// Terminal cleanup. A returned `Err` is logged/surfaced, **never**
    /// unwrapped, and the original `reason` is preserved. On the poisoned
    /// (post-panic) path, do resource release only — never read domain fields.
    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let _ = (actor_ref, reason);
        async { Ok(()) }
    }
}
```

- [ ] **Step 2: Write the minimal `ActorRef` scaffold with a failing unit test**

Create `bombay-core/src/actor/actor_ref.rs`:

```rust
//! The minimal handle to a running actor (card #116 scaffold).
//!
//! Each field is independently cheap to clone and shares state, so no outer
//! `Arc` is needed here — the Arc/Weak ref-count semantics (last strong drop
//! stops the actor), `Recipient` erasure, and the `tell`/`ask` builders are
//! #117/#118. #116 exposes only what the hooks, spawn, and loop need.

use core::fmt;

use futures::stream::AbortHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    actor::Actor,
    mailbox::{ActorId, MailboxSender, WeakMailboxSender},
};

/// A cloneable handle to a running actor: enqueue signals, stop it gracefully,
/// or kill it. Does **not** (yet) drive ref-count shutdown — see the module doc.
pub struct ActorRef<A: Actor> {
    id: ActorId,
    mailbox: MailboxSender<A>,
    cancel: CancellationToken,
    abort: AbortHandle,
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            mailbox: self.mailbox.clone(),
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("id", &self.id)
            .field("actor", &A::name())
            .finish_non_exhaustive()
    }
}

impl<A: Actor> ActorRef<A> {
    pub(crate) fn new(
        id: ActorId,
        mailbox: MailboxSender<A>,
        cancel: CancellationToken,
        abort: AbortHandle,
    ) -> Self {
        Self { id, mailbox, cancel, abort }
    }

    /// The actor's scaffold identity (replaced by the AID in #121).
    #[must_use]
    pub const fn id(&self) -> ActorId {
        self.id
    }

    /// The sender half of the actor's mailbox — used to enqueue `Signal`s. The
    /// ergonomic `tell`/`ask` builders wrap this in #118.
    #[must_use]
    pub const fn mailbox_sender(&self) -> &MailboxSender<A> {
        &self.mailbox
    }

    /// The loop's graceful-cancellation token (loop-internal).
    pub(crate) const fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Requests a graceful, out-of-band stop: the in-flight message finishes,
    /// then the actor stops and `on_stop` runs. Queued messages are abandoned.
    pub fn stop(&self) {
        self.cancel.cancel();
    }

    /// Hard-kills the actor: the task is aborted at its next await point,
    /// `on_stop` does **not** run, and any in-flight message is dropped.
    pub fn kill(&self) {
        self.abort.abort();
    }

    /// Downgrades to a non-pinning [`WeakActorRef`].
    #[must_use]
    pub fn downgrade(&self) -> WeakActorRef<A> {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox.downgrade(),
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        }
    }
}

/// A non-pinning handle to an actor. [`upgrade`](WeakActorRef::upgrade) yields a
/// strong [`ActorRef`] only while the actor's mailbox is still open.
pub struct WeakActorRef<A: Actor> {
    id: ActorId,
    mailbox: WeakMailboxSender<A>,
    cancel: CancellationToken,
    abort: AbortHandle,
}

impl<A: Actor> Clone for WeakActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            mailbox: self.mailbox.clone(),
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakActorRef")
            .field("id", &self.id)
            .field("actor", &A::name())
            .finish_non_exhaustive()
    }
}

impl<A: Actor> WeakActorRef<A> {
    /// The actor's scaffold identity.
    #[must_use]
    pub const fn id(&self) -> ActorId {
        self.id
    }

    /// Upgrades to a strong [`ActorRef`], or `None` if the actor's mailbox has
    /// closed (every strong sender dropped).
    #[must_use]
    pub fn upgrade(&self) -> Option<ActorRef<A>> {
        self.mailbox.upgrade().map(|mailbox| ActorRef {
            id: self.id,
            mailbox,
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use crate::mailbox::{Capacity, Mailbox, Mailboxed};

    // A minimal Actor purely to key the mailbox/ref. `on_start`/`handle` are
    // never called in this task's tests (no loop yet) — they exist so the type
    // satisfies `Actor`.
    struct Probe;
    struct ProbeMsg;
    impl Msg for ProbeMsg {}
    impl Mailboxed for Probe {
        type Msg = ProbeMsg;
    }
    impl Actor for Probe {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Probe)
        }
        async fn handle(
            &mut self,
            _: ProbeMsg,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    use crate::message::Msg;

    fn build_ref() -> (ActorRef<Probe>, super::super::actor_ref::WeakActorRef<Probe>) {
        let cap = Capacity::try_from(4usize).expect("valid capacity");
        let (tx, _rx) = Mailbox::<Probe>::bounded(cap);
        let (abort, _reg) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(ActorId::new(7), tx, CancellationToken::new(), abort);
        let weak = actor_ref.downgrade();
        (actor_ref, weak)
    }

    /// Lifecycle: a weak ref upgrades while the mailbox is open, and returns
    /// `None` once every strong sender (incl. the one inside `ActorRef`) drops.
    #[tokio::test]
    async fn weak_upgrades_while_open_then_none_after_drop() {
        let (actor_ref, weak) = build_ref();
        assert_eq!(weak.id(), ActorId::new(7));
        assert!(weak.upgrade().is_some(), "mailbox open -> upgradable");

        drop(actor_ref);
        assert!(
            weak.upgrade().is_none(),
            "all strong senders dropped -> not upgradable",
        );
    }
}
```

Note: the `use crate::message::Msg;` placement above is intentionally awkward to avoid an unused-import warning ordering issue; when implementing, put **all** `use` at the top of the `tests` module (house rule #6). The final tidy form is:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use crate::{
        mailbox::{ActorId, Capacity, Mailbox, Mailboxed},
        message::Msg,
    };
    // … struct Probe / impls / build_ref / the test …
}
```

- [ ] **Step 3: Wire the module into the crate**

In `bombay-core/src/lib.rs`, add `pub mod actor;` in alphabetical order (before `pub mod error;`):

```rust
pub mod actor;
pub mod error;
pub mod mailbox;
pub mod message;
pub mod reply;
```

- [ ] **Step 4: Run the test to verify it passes (and the module compiles)**

Run: `nix develop --command cargo test -p bombay-core weak_upgrades_while_open`
Expected: PASS.

If the associated-type-bound supertrait `Mailboxed<Msg: Msg>` fails to parse on the pinned toolchain, fall back to a `where` clause: declare `pub trait Actor: Mailboxed + Sized + Send + 'static where Self::Msg: Msg` — semantically identical. (Associated-type bounds are stable since Rust 1.79; the repo pins ≥ 1.85, so the inline form should work.)

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/mod.rs bombay-core/src/actor/actor_ref.rs bombay-core/src/lib.rs
git commit -m "core(actor): Actor trait + minimal ActorRef/WeakActorRef scaffold (#116)"
```

---

## Task 4: Walking skeleton — the run-loop + `PreparedActor::run`

Build the minimal loop that: runs `on_start`, drains the mailbox handling messages, stops on `Signal::Stop`, and runs `on_stop`. This is the biggest task (the pieces are mutually dependent); subsequent tasks add one guarantee each against this skeleton.

**Files:**
- Create: `bombay-core/src/actor/kind.rs`
- Create: `bombay-core/src/actor/spawn.rs`
- Modify: `bombay-core/src/actor/mod.rs` (declare the two submodules + re-exports)

- [ ] **Step 1: Write the failing behavioral test**

Create `bombay-core/src/actor/spawn.rs` with just a `tests` module for now (the impl comes in Step 3):

```rust
#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use crate::{
        actor::{ActorRef, PreparedActor, RunResult, WeakActorRef},
        error::ActorStopReason,
        mailbox::{Capacity, Mailboxed, Signal},
        message::Msg,
    };

    /// Counts handled messages and records whether `on_stop` ran, via shared
    /// atomics the test inspects — the SUT is the real loop, not a reimpl.
    struct Counter {
        handled: Arc<AtomicU32>,
        stopped: Arc<AtomicU32>,
    }
    struct Tick;
    impl Msg for Tick {}
    impl Mailboxed for Counter {
        type Msg = Tick;
    }
    impl crate::actor::Actor for Counter {
        type Args = (Arc<AtomicU32>, Arc<AtomicU32>);
        type Error = core::convert::Infallible;

        async fn on_start(
            (handled, stopped): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self { handled, stopped })
        }

        async fn handle(
            &mut self,
            _: Tick,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            _: ActorStopReason,
        ) -> Result<(), Self::Error> {
            self.stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn cap(n: usize) -> Capacity {
        Capacity::try_from(n).expect("valid test capacity")
    }

    /// Sequence: two messages then a `Stop` — both are handled (FIFO, before the
    /// stop), `on_stop` runs exactly once, and the outcome is a normal stop.
    #[tokio::test]
    async fn handles_queued_messages_then_stops_normally() {
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Counter>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        actor_ref.mailbox_sender().send(Signal::Message(Tick)).await.expect("send 1");
        actor_ref.mailbox_sender().send(Signal::Message(Tick)).await.expect("send 2");
        actor_ref.mailbox_sender().send(Signal::Stop).await.expect("stop");

        let outcome = prepared.run((Arc::clone(&handled), Arc::clone(&stopped))).await;

        assert_eq!(handled.load(Ordering::SeqCst), 2, "both messages handled before stop");
        assert_eq!(stopped.load(Ordering::SeqCst), 1, "on_stop ran once");
        assert!(
            matches!(outcome, RunResult::Stopped { reason: ActorStopReason::Normal, .. }),
            "clean normal stop",
        );
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `nix develop --command cargo test -p bombay-core handles_queued_messages_then_stops_normally`
Expected: FAIL — `PreparedActor`, `RunResult` not found.

- [ ] **Step 3: Implement the run-loop (`kind.rs`)**

Create `bombay-core/src/actor/kind.rs`:

```rust
//! The actor run-loop (card #116): drive `on_start` → message loop → `on_stop`,
//! with a `catch_unwind` around each hook so a panic becomes an inspectable
//! `PanicError` instead of tearing down the task.

use std::{ops::ControlFlow, panic::AssertUnwindSafe};

use futures::FutureExt;

use crate::{
    actor::{Actor, ActorRef},
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{MailboxReceiver, Signal},
};

/// Runs the message loop until a stop condition, returning the stop reason.
///
/// `state` is the live actor; `actor_ref` is its strong self-handle (kept strong
/// in #116 — ref-count-driven stop is #117). The loop finishes any in-flight
/// handler before observing a graceful stop ("finish-current-then-stop, no
/// drain").
pub(crate) async fn run_message_loop<A: Actor>(
    state: &mut A,
    actor_ref: &ActorRef<A>,
    mailbox_rx: &mut MailboxReceiver<A>,
) -> ActorStopReason {
    let cancel = actor_ref.cancel_token();
    loop {
        match cancel.run_until_cancelled(mailbox_rx.recv()).await {
            // Token cancelled (out-of-band graceful stop).
            None => return ActorStopReason::Normal,
            // All senders dropped (unreachable in #116 — the loop holds one).
            Some(None) => return ActorStopReason::Normal,
            Some(Some(signal)) => match signal {
                Signal::Message(msg) => {
                    if let ControlFlow::Break(reason) =
                        handle_message(state, actor_ref, msg).await
                    {
                        return reason;
                    }
                }
                // In-band graceful stop (FIFO): everything queued ahead was
                // already handled above.
                Signal::Stop => return ActorStopReason::Normal,
                // Nothing produces LinkDied pre-#120; ignore and keep running.
                Signal::LinkDied(_) => {}
            },
        }
    }
}

/// Handles one message under `catch_unwind`. `Continue` keeps looping; `Break`
/// carries the terminal stop reason.
async fn handle_message<A: Actor>(
    state: &mut A,
    actor_ref: &ActorRef<A>,
    msg: A::Msg,
) -> ControlFlow<ActorStopReason> {
    let mut stop = false;
    let result = AssertUnwindSafe(state.handle(msg, actor_ref.clone(), &mut stop))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(())) if stop => ControlFlow::Break(ActorStopReason::Normal),
        Ok(Ok(())) => ControlFlow::Continue(()),
        // A returned Err is a controlled crash: observe via on_panic, then stop.
        Ok(Err(err)) => {
            let panic = PanicError::new(Box::new(err), PanicReason::HandlerPanic);
            ControlFlow::Break(run_on_panic(state, actor_ref, panic).await)
        }
        // The handler unwound: catch, observe via on_panic, then stop.
        Err(payload) => {
            let panic = PanicError::from_panic_any(payload, PanicReason::HandlerPanic);
            ControlFlow::Break(run_on_panic(state, actor_ref, panic).await)
        }
    }
}

/// Runs `on_panic` (infallible, stop-only) under `catch_unwind`; if the hook
/// itself panics, that becomes the terminal reason instead.
async fn run_on_panic<A: Actor>(
    state: &mut A,
    actor_ref: &ActorRef<A>,
    err: PanicError,
) -> ActorStopReason {
    let weak = actor_ref.downgrade();
    match AssertUnwindSafe(state.on_panic(weak, err)).catch_unwind().await {
        Ok(reason) => reason,
        Err(payload) => {
            ActorStopReason::Panicked(PanicError::from_panic_any(payload, PanicReason::OnPanic))
        }
    }
}
```

- [ ] **Step 4: Implement `spawn.rs` (`PreparedActor`, `RunResult`, the lifecycle driver)**

At the **top** of `bombay-core/src/actor/spawn.rs` (above the `tests` module), add:

```rust
//! Spawn entry points (card #116): prepare an actor, then run it in the current
//! task or a background tokio task. Kill is uniform across both via
//! `futures::Abortable` wrapping the whole lifecycle (so a hard kill skips
//! `on_stop`).

use std::{
    fmt,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::{
    FutureExt,
    stream::{AbortHandle, AbortRegistration, Abortable},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, ActorRef, kind::run_message_loop},
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver},
};

/// The default mailbox capacity for the ergonomic spawn path (4 cache-lines'
/// worth of slots is a sane starting point; tune with `spawn_with_capacity`).
pub const DEFAULT_MAILBOX_CAPACITY: usize = 64;

/// Monotonic scaffold id source (#121 replaces this with the AID).
static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);

fn next_actor_id() -> ActorId {
    // Relaxed is sufficient: correctness needs only that each `fetch_add` returns
    // a distinct value. Uniqueness is a property of atomic increment alone and
    // requires no happens-before with any other memory (CLAUDE rule #5).
    ActorId::new(NEXT_ACTOR_ID.fetch_add(1, Ordering::Relaxed))
}

/// The total outcome of running an actor to completion in the current task.
pub enum RunResult<A: Actor> {
    /// Ran and stopped. If `reason` is [`ActorStopReason::Panicked`], `actor` is
    /// **poisoned** (torn state): resource-release only, never read domain fields.
    Stopped {
        /// The final actor state.
        actor: A,
        /// Why it stopped.
        reason: ActorStopReason,
    },
    /// `on_start` returned `Err` or panicked — no actor was produced.
    StartupFailed(PanicError),
    /// Hard-killed via [`ActorRef::kill`] — `on_stop` was skipped, state dropped.
    Killed,
}

impl<A: Actor> fmt::Debug for RunResult<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped { reason, .. } => {
                f.debug_struct("Stopped").field("reason", reason).finish_non_exhaustive()
            }
            Self::StartupFailed(err) => f.debug_tuple("StartupFailed").field(err).finish(),
            Self::Killed => f.write_str("Killed"),
        }
    }
}

/// An actor initialized and ready to run, with its [`ActorRef`] available before
/// the loop starts (so callers can pre-send messages).
#[must_use = "a prepared actor must be run or spawned"]
pub struct PreparedActor<A: Actor> {
    actor_ref: ActorRef<A>,
    mailbox_rx: MailboxReceiver<A>,
    abort_registration: AbortRegistration,
}

impl<A: Actor> fmt::Debug for PreparedActor<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedActor").field("actor_ref", &self.actor_ref).finish_non_exhaustive()
    }
}

impl<A: Actor> PreparedActor<A> {
    /// Prepares an actor with a mailbox of the given `capacity`.
    pub fn new(capacity: Capacity) -> Self {
        let (mailbox_tx, mailbox_rx) = Mailbox::<A>::bounded(capacity);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let actor_ref =
            ActorRef::new(next_actor_id(), mailbox_tx, CancellationToken::new(), abort_handle);
        Self { actor_ref, mailbox_rx, abort_registration }
    }

    /// The handle to the actor, usable before the loop starts.
    #[must_use]
    pub const fn actor_ref(&self) -> &ActorRef<A> {
        &self.actor_ref
    }

    /// Runs the actor in the current task until it stops. Aborts (hard kill)
    /// short-circuit to [`RunResult::Killed`], skipping `on_stop`.
    pub async fn run(self, args: A::Args) -> RunResult<A> {
        let lifecycle = run_lifecycle(args, self.actor_ref, self.mailbox_rx);
        Abortable::new(lifecycle, self.abort_registration)
            .await
            .unwrap_or(RunResult::Killed)
    }

    /// Spawns the actor in a background tokio task.
    pub fn spawn(self, args: A::Args) -> JoinHandle<RunResult<A>> {
        tokio::spawn(self.run(args))
    }
}

/// `on_start` (catch) → message loop → `on_stop` (catch; Err logged, reason
/// preserved). Returns `StartupFailed` if `on_start` fails, else `Stopped`.
async fn run_lifecycle<A: Actor>(
    args: A::Args,
    actor_ref: ActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
) -> RunResult<A> {
    let started = AssertUnwindSafe(A::on_start(args, actor_ref.clone())).catch_unwind().await;
    let mut state = match started {
        Ok(Ok(actor)) => actor,
        Ok(Err(err)) => {
            return RunResult::StartupFailed(PanicError::new(Box::new(err), PanicReason::OnStart));
        }
        Err(payload) => {
            return RunResult::StartupFailed(PanicError::from_panic_any(
                payload,
                PanicReason::OnStart,
            ));
        }
    };

    let reason = run_message_loop(&mut state, &actor_ref, &mut mailbox_rx).await;

    let weak = actor_ref.downgrade();
    let stop_result =
        AssertUnwindSafe(state.on_stop(weak, reason.clone())).catch_unwind().await;
    log_on_stop_outcome::<A>(&reason, stop_result);

    RunResult::Stopped { actor: state, reason }
}

/// Logs a failed/panicked `on_stop` without altering the preserved stop reason
/// and without unwrapping (a double-panic on the shutdown path can abort the
/// process — std `Drop` docs).
fn log_on_stop_outcome<A: Actor>(
    reason: &ActorStopReason,
    stop_result: Result<Result<(), A::Error>, Box<dyn std::any::Any + Send>>,
) {
    match stop_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            eprintln!("[bombay] on_stop for {} returned an error: {err:?} (stop reason: {reason})", A::name());
        }
        Err(_payload) => {
            eprintln!("[bombay] on_stop for {} panicked (stop reason: {reason})", A::name());
        }
    }
}
```

Note on `eprintln!`: the god-level bar bans `clippy::print_stderr` in library code. The tracing wiring is a later concern (the `tracing` feature is repurposed in #66). For #116, wrap the two `eprintln!` lines with `#[expect(clippy::print_stderr, reason = "diagnostic-only until the tracing feature lands (#66); on_stop failure must be surfaced, never swallowed")]` on the enclosing `match` arms, or gate them behind `#[cfg(...)]`. Simplest: put the attribute on the `log_on_stop_outcome` function:

```rust
#[expect(
    clippy::print_stderr,
    reason = "diagnostic-only surface until the tracing feature lands (#66); \
              an on_stop failure must be surfaced, never swallowed"
)]
fn log_on_stop_outcome<A: Actor>(/* … */) { /* … */ }
```

- [ ] **Step 5: Declare the submodules + re-exports in `mod.rs`**

In `bombay-core/src/actor/mod.rs`, update the module declarations and re-exports:

```rust
mod actor_ref;
mod kind;
mod spawn;

pub use self::{
    actor_ref::{ActorRef, WeakActorRef},
    spawn::{DEFAULT_MAILBOX_CAPACITY, PreparedActor, RunResult},
};
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `nix develop --command cargo test -p bombay-core handles_queued_messages_then_stops_normally`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/
git commit -m "core(actor): run-loop walking skeleton — on_start → loop → on_stop (#116)"
```

---

## Task 5: Graceful stop via `CancellationToken` finishes the in-flight message

**Files:**
- Test: `bombay-core/src/actor/spawn.rs` (`tests` module)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `spawn.rs`:

```rust
/// Lifecycle: `stop()` (out-of-band cancel) while a handler is mid-flight lets
/// that handler finish, then stops and runs `on_stop`. The queued-behind message
/// is abandoned (finish-current-then-stop, no drain).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_finishes_in_flight_then_stops() {
    use tokio::sync::oneshot;

    struct Slow {
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
        handled: Arc<AtomicU32>,
    }
    struct Work;
    impl Msg for Work {}
    impl Mailboxed for Slow {
        type Msg = Work;
    }
    impl crate::actor::Actor for Slow {
        type Args = (oneshot::Sender<()>, oneshot::Receiver<()>, Arc<AtomicU32>);
        type Error = core::convert::Infallible;
        async fn on_start(
            (entered, release, handled): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self { entered: Some(entered), release: Some(release), handled })
        }
        async fn handle(&mut self, _: Work, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            if let Some(release) = self.release.take() {
                let _ = release.await;
            }
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let handled = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<Slow>::new(cap(8));
    let actor_ref = prepared.actor_ref().clone();
    // Two messages: the first blocks until released; the second must be abandoned.
    actor_ref.mailbox_sender().send(Signal::Message(Work)).await.expect("send 1");
    actor_ref.mailbox_sender().send(Signal::Message(Work)).await.expect("send 2");

    let run = tokio::spawn(prepared.run((entered_tx, release_rx, Arc::clone(&handled))));

    entered_rx.await.expect("handler entered");   // handler #1 is mid-flight
    actor_ref.stop();                              // cancel while in-flight
    release_tx.send(()).expect("release handler"); // let handler #1 finish

    let outcome = run.await.expect("run task");
    assert_eq!(handled.load(Ordering::SeqCst), 1, "only the in-flight message finished; the queued one was abandoned");
    assert!(matches!(outcome, RunResult::Stopped { reason: ActorStopReason::Normal, .. }));
}
```

- [ ] **Step 2: Run the test to verify it passes (the skeleton already supports cancel)**

Run: `nix develop --command cargo test -p bombay-core cancel_finishes_in_flight_then_stops`
Expected: PASS — the loop already awaits `handle` outside the cancellation wrapper, so the in-flight handler finishes before the next `run_until_cancelled` observes the cancel.

If it FAILS (e.g. the queued message is also handled), the bug is that cancellation was checked in the wrong place; the `run_until_cancelled(mailbox_rx.recv())` in `kind.rs` must wrap **only the recv**, never the `handle`. Fix in `kind.rs`.

- [ ] **Step 3: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/spawn.rs
git commit -m "core(actor): test graceful cancel finishes in-flight then stops (#116)"
```

---

## Task 6: `stop: &mut bool` stops the actor after the handler returns

**Files:**
- Test: `bombay-core/src/actor/spawn.rs` (`tests` module)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module:

```rust
/// Sequence: a handler that sets `*stop = true` stops the actor cleanly after it
/// returns `Ok` — a following queued message is never handled.
#[tokio::test]
async fn stop_flag_stops_after_current_handler() {
    struct Once {
        handled: Arc<AtomicU32>,
    }
    struct Go;
    impl Msg for Go {}
    impl Mailboxed for Once {
        type Msg = Go;
    }
    impl crate::actor::Actor for Once {
        type Args = Arc<AtomicU32>;
        type Error = core::convert::Infallible;
        async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { handled })
        }
        async fn handle(&mut self, _: Go, _: ActorRef<Self>, stop: &mut bool) -> Result<(), Self::Error> {
            self.handled.fetch_add(1, Ordering::SeqCst);
            *stop = true;
            Ok(())
        }
    }

    let handled = Arc::new(AtomicU32::new(0));
    let prepared = PreparedActor::<Once>::new(cap(8));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref.mailbox_sender().send(Signal::Message(Go)).await.expect("send 1");
    actor_ref.mailbox_sender().send(Signal::Message(Go)).await.expect("send 2");

    let outcome = prepared.run(Arc::clone(&handled)).await;
    assert_eq!(handled.load(Ordering::SeqCst), 1, "stopped after the first handler; second never ran");
    assert!(matches!(outcome, RunResult::Stopped { reason: ActorStopReason::Normal, .. }));
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `nix develop --command cargo test -p bombay-core stop_flag_stops_after_current_handler`
Expected: PASS — the skeleton's `handle_message` already returns `Break(Normal)` when `stop` is set.

- [ ] **Step 3: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/spawn.rs
git commit -m "core(actor): test *stop flag stops after the current handler (#116)"
```

---

## Task 7: Messages sent during `on_start` are handled after, in order

**Files:**
- Test: `bombay-core/src/actor/spawn.rs` (`tests` module)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module:

```rust
/// Sequence (no startup buffer): messages that arrive while `on_start` is still
/// running wait in the bounded mailbox and are handled *after* start, in FIFO
/// order — the ordering guarantee comes from the flume channel, not a buffer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_during_on_start_are_handled_after_in_order() {
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    struct Recorder {
        seen: Arc<Mutex<Vec<u32>>>,
    }
    struct N(u32);
    impl Msg for N {}
    impl Mailboxed for Recorder {
        type Msg = N;
    }
    impl crate::actor::Actor for Recorder {
        type Args = (oneshot::Receiver<()>, Arc<Mutex<Vec<u32>>>);
        type Error = core::convert::Infallible;
        async fn on_start(
            (gate, seen): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            let _ = gate.await; // block startup until the test has enqueued messages
            Ok(Self { seen })
        }
        async fn handle(&mut self, N(n): N, _: ActorRef<Self>, stop: &mut bool) -> Result<(), Self::Error> {
            self.seen.lock().expect("lock").push(n);
            if n == 2 {
                *stop = true;
            }
            Ok(())
        }
    }

    let (gate_tx, gate_rx) = oneshot::channel();
    let seen = Arc::new(Mutex::new(Vec::new()));

    let prepared = PreparedActor::<Recorder>::new(cap(8));
    let actor_ref = prepared.actor_ref().clone();
    let run = tokio::spawn(prepared.run((gate_rx, Arc::clone(&seen))));

    // Enqueue BEFORE releasing on_start — these must be buffered by the mailbox.
    actor_ref.mailbox_sender().send(Signal::Message(N(0))).await.expect("send 0");
    actor_ref.mailbox_sender().send(Signal::Message(N(1))).await.expect("send 1");
    actor_ref.mailbox_sender().send(Signal::Message(N(2))).await.expect("send 2");
    gate_tx.send(()).expect("release on_start");

    run.await.expect("run task");
    assert_eq!(*seen.lock().expect("lock"), vec![0, 1, 2], "handled after start, in FIFO order");
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `nix develop --command cargo test -p bombay-core messages_during_on_start_are_handled_after_in_order`
Expected: PASS — `on_start` is awaited fully before the loop starts; the mailbox holds the early messages in order.

- [ ] **Step 3: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/spawn.rs
git commit -m "core(actor): test on-start messages handled after, in order (no buffer) (#116)"
```

---

## Task 8: `on_start` failure — `Err` and panic both yield `StartupFailed`

The `on_start` panic test is the card's **"fails under `panic = abort`"** pin: under `panic = "abort"` the process would abort instead of producing a `StartupFailed`, so this test only passes under `panic = "unwind"`.

**Files:**
- Test: `bombay-core/src/actor/spawn.rs` (`tests` module)

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
/// Lifecycle: `on_start` returning `Err` produces `StartupFailed` (no actor, no
/// message ever handled) — tagged as an `OnStart`-phase panic reason.
#[tokio::test]
async fn on_start_error_yields_startup_failed() {
    #[derive(Debug)]
    struct Boom;
    struct NeverStarts;
    struct Never;
    impl Msg for Never {}
    impl Mailboxed for NeverStarts {
        type Msg = Never;
    }
    impl crate::actor::Actor for NeverStarts {
        type Args = ();
        type Error = Boom;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Err(Boom)
        }
        async fn handle(&mut self, _: Never, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let outcome = PreparedActor::<NeverStarts>::new(cap(4)).run(()).await;
    let RunResult::StartupFailed(err) = outcome else {
        panic!("expected StartupFailed, got {outcome:?}");
    };
    assert_eq!(err.reason(), crate::error::PanicReason::OnStart);
}

/// Defensive: a panic in `on_start` is CAUGHT (not a process abort) and becomes
/// `StartupFailed` with the `OnStart` reason and the recoverable message.
///
/// This is the card's `panic = "unwind"` pin: under `panic = "abort"` the
/// process aborts here instead, and the test cannot pass.
#[tokio::test]
async fn on_start_panic_is_caught_as_startup_failed() {
    struct PanicsOnStart;
    struct Never;
    impl Msg for Never {}
    impl Mailboxed for PanicsOnStart {
        type Msg = Never;
    }
    impl crate::actor::Actor for PanicsOnStart {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            panic!("startup boom")
        }
        async fn handle(&mut self, _: Never, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let outcome = PreparedActor::<PanicsOnStart>::new(cap(4)).run(()).await;
    let RunResult::StartupFailed(err) = outcome else {
        panic!("expected StartupFailed, got {outcome:?}");
    };
    assert_eq!(err.reason(), crate::error::PanicReason::OnStart);
    assert_eq!(err.with_str(str::to_owned), Some(String::from("startup boom")));
}
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `nix develop --command cargo test -p bombay-core on_start_`
Expected: PASS (both) — the skeleton's `run_lifecycle` already maps `Err`/panic in `on_start` to `StartupFailed`.

- [ ] **Step 3: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/spawn.rs
git commit -m "core(actor): test on_start Err/panic → StartupFailed (unwind pin) (#116)"
```

---

## Task 9: Handler panic — `on_stop` runs with `Panicked`, poison contract, post-panic send fails

Three guarantees in one task (they share the panic setup): (a) `on_stop` runs after a handler panic and sees `Panicked`; (b) the poison contract — `on_stop` must not flush torn state, verified by a spy; (c) after the panic-stop the mailbox is closed, so a later send fails.

**Files:**
- Test: `bombay-core/src/actor/spawn.rs` (`tests` module)

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
/// A handler that panics mid-mutation, with an `on_stop` spy that records the
/// reason it received and whether it observed torn state. Shared across the
/// three panic guarantees below.
mod panic_probe {
    use super::*;
    use std::sync::Mutex;

    pub(super) struct Torn {
        pub(super) counter: u32,
        pub(super) stop_reason: Arc<Mutex<Option<ActorStopReason>>>,
        pub(super) counter_at_stop: Arc<Mutex<Option<u32>>>,
    }
    pub(super) struct Explode;
    impl Msg for Explode {}
    impl Mailboxed for Torn {
        type Msg = Explode;
    }
    impl crate::actor::Actor for Torn {
        type Args = (Arc<Mutex<Option<ActorStopReason>>>, Arc<Mutex<Option<u32>>>);
        type Error = core::convert::Infallible;
        async fn on_start(
            (stop_reason, counter_at_stop): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self { counter: 0, stop_reason, counter_at_stop })
        }
        async fn handle(&mut self, _: Explode, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
            self.counter = 99;   // torn write BEFORE the panic
            panic!("handler boom");
        }
        async fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            reason: ActorStopReason,
        ) -> Result<(), Self::Error> {
            // Records the reason and the poisoned field value — a real on_stop
            // must NOT persist `self.counter` (torn); this spy only records it so
            // the test can assert the loop DID run on_stop with the torn state
            // present (the contract is "don't flush", enforced by review + this
            // documented probe).
            *self.stop_reason.lock().expect("lock") = Some(reason);
            *self.counter_at_stop.lock().expect("lock") = Some(self.counter);
            Ok(())
        }
    }
}

/// `@bug` Lifecycle: after a handler panic, `on_stop` STILL runs and receives
/// `ActorStopReason::Panicked` (OTP `terminate` precedent). Fails if the loop
/// skips `on_stop` on the panic path.
#[tokio::test]
async fn on_stop_runs_after_panic_with_panicked_reason() {
    use panic_probe::*;
    use std::sync::Mutex;

    let stop_reason: Arc<Mutex<Option<ActorStopReason>>> = Arc::new(Mutex::new(None));
    let counter_at_stop = Arc::new(Mutex::new(None));

    let prepared = PreparedActor::<Torn>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref.mailbox_sender().send(Signal::Message(Explode)).await.expect("send");

    let outcome = prepared.run((Arc::clone(&stop_reason), Arc::clone(&counter_at_stop))).await;

    assert!(
        matches!(&outcome, RunResult::Stopped { reason: ActorStopReason::Panicked(_), .. }),
        "panic → Stopped with Panicked, got {outcome:?}",
    );
    let recorded = stop_reason.lock().expect("lock").clone();
    assert!(
        matches!(recorded, Some(ActorStopReason::Panicked(_))),
        "on_stop ran and saw Panicked, got {recorded:?}",
    );
}

/// `@bug` Defensive (poison contract): the field mutated just before the panic
/// (`counter = 99`) IS still visible to `on_stop` (proving the state is torn, not
/// rolled back) — which is exactly why a real `on_stop` must NOT flush it. This
/// pins that the loop surfaces torn state to `on_stop` rather than silently
/// discarding before cleanup, so the "don't flush" contract is meaningful.
#[tokio::test]
async fn on_stop_after_panic_observes_torn_state() {
    use panic_probe::*;
    use std::sync::Mutex;

    let stop_reason = Arc::new(Mutex::new(None));
    let counter_at_stop: Arc<Mutex<Option<u32>>> = Arc::new(Mutex::new(None));

    let prepared = PreparedActor::<Torn>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref.mailbox_sender().send(Signal::Message(Explode)).await.expect("send");
    let _ = prepared.run((Arc::clone(&stop_reason), Arc::clone(&counter_at_stop))).await;

    assert_eq!(
        *counter_at_stop.lock().expect("lock"),
        Some(99),
        "on_stop sees the torn (pre-panic-mutated) field — hence must not flush it",
    );
}
```

Add the post-panic-send test (uses the simpler `Counter`-style actor that panics):

```rust
/// `@bug` Lifecycle: once a handler panic stops the actor, its mailbox receiver
/// is dropped, so a later `send` fails (the actor is gone). Fails if teardown
/// leaves the receiver alive on the panic path.
#[tokio::test]
async fn send_after_handler_panic_fails() {
    struct Bomb;
    struct Trigger;
    impl Msg for Trigger {}
    impl Mailboxed for Bomb {
        type Msg = Trigger;
    }
    impl crate::actor::Actor for Bomb {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Bomb)
        }
        async fn handle(&mut self, _: Trigger, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
            panic!("boom")
        }
    }

    let prepared = PreparedActor::<Bomb>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    let handle = prepared.spawn(());
    actor_ref.mailbox_sender().send(Signal::Message(Trigger)).await.expect("send trigger");

    let outcome = handle.await.expect("run task");
    assert!(matches!(outcome, RunResult::Stopped { reason: ActorStopReason::Panicked(_), .. }));

    let resend = actor_ref.mailbox_sender().send(Signal::Message(Trigger)).await;
    assert!(resend.is_err(), "the actor's mailbox is closed after the panic-stop");
}
```

Add the `handle`-returns-`Err` test (exercises the `Ok(Err(err))` arm of `handle_message`, which no panic test hits — a returned error is NOT an unwind):

```rust
/// Lifecycle: a handler that RETURNS `Err` (not a panic) is a controlled crash —
/// it stops the actor with `Panicked(HandlerPanic)` and runs `on_stop`. This is
/// the only test that exercises the `Ok(Err(_))` arm of the loop's dispatch.
#[tokio::test]
async fn handle_returning_err_stops_as_panicked() {
    #[derive(Debug)]
    struct Nope;
    struct Failer;
    struct Do;
    impl Msg for Do {}
    impl Mailboxed for Failer {
        type Msg = Do;
    }
    impl crate::actor::Actor for Failer {
        type Args = ();
        type Error = Nope;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Failer)
        }
        async fn handle(&mut self, _: Do, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
            Err(Nope)
        }
    }

    let prepared = PreparedActor::<Failer>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref.mailbox_sender().send(Signal::Message(Do)).await.expect("send");
    let outcome = prepared.run(()).await;

    let RunResult::Stopped { reason: ActorStopReason::Panicked(err), .. } = outcome else {
        panic!("expected Stopped/Panicked, got {outcome:?}");
    };
    assert_eq!(err.reason(), crate::error::PanicReason::HandlerPanic);
}
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `nix develop --command cargo test -p bombay-core -- on_stop_runs_after_panic on_stop_after_panic_observes_torn_state send_after_handler_panic handle_returning_err_stops_as_panicked`
Expected: PASS (all four) — the skeleton runs `on_stop` on every non-kill exit, maps a returned `Err` to `Panicked(HandlerPanic)`, and the `run` future drops `mailbox_rx` on completion.

- [ ] **Step 3: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/spawn.rs
git commit -m "core(actor): test on_stop-after-panic, poison contract, post-panic send (#116)"
```

---

## Task 10: Hard kill skips `on_stop` and drops the in-flight message

**Files:**
- Test: `bombay-core/src/actor/spawn.rs` (`tests` module)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module:

```rust
/// Lifecycle: `kill()` while a handler is mid-flight aborts the task at its next
/// await point — the handler never completes, `on_stop` does NOT run, and the
/// outcome is `Killed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_skips_on_stop_and_drops_in_flight() {
    use tokio::sync::oneshot;

    struct Blocker {
        entered: Option<oneshot::Sender<()>>,
        finished: Arc<AtomicU32>,
        stopped: Arc<AtomicU32>,
    }
    struct Block;
    impl Msg for Block {}
    impl Mailboxed for Blocker {
        type Msg = Block;
    }
    impl crate::actor::Actor for Blocker {
        type Args = (oneshot::Sender<()>, Arc<AtomicU32>, Arc<AtomicU32>);
        type Error = core::convert::Infallible;
        async fn on_start(
            (entered, finished, stopped): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self { entered: Some(entered), finished, stopped })
        }
        async fn handle(&mut self, _: Block, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            std::future::pending::<()>().await; // never completes until aborted
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn on_stop(&mut self, _: WeakActorRef<Self>, _: ActorStopReason) -> Result<(), Self::Error> {
            self.stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let (entered_tx, entered_rx) = oneshot::channel();
    let finished = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));

    let prepared = PreparedActor::<Blocker>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    actor_ref.mailbox_sender().send(Signal::Message(Block)).await.expect("send");
    let handle = prepared.spawn((entered_tx, Arc::clone(&finished), Arc::clone(&stopped)));

    entered_rx.await.expect("handler entered");  // handler is now parked forever
    actor_ref.kill();                            // hard abort

    let outcome = handle.await.expect("join");
    assert!(matches!(outcome, RunResult::Killed), "kill → Killed, got {outcome:?}");
    assert_eq!(finished.load(Ordering::SeqCst), 0, "in-flight handler dropped, never finished");
    assert_eq!(stopped.load(Ordering::SeqCst), 0, "on_stop skipped on hard kill");
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `nix develop --command cargo test -p bombay-core kill_skips_on_stop_and_drops_in_flight`
Expected: PASS — `Abortable` wraps the whole lifecycle, so `kill()` drops the future mid-handler; `run` maps the `Aborted` to `RunResult::Killed`.

- [ ] **Step 3: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/spawn.rs
git commit -m "core(actor): test hard kill skips on_stop and drops in-flight (#116)"
```

---

## Task 11: `Spawn` ext-trait + `DEFAULT_MAILBOX_CAPACITY` + concurrent ordering

**Files:**
- Modify: `bombay-core/src/actor/mod.rs` (add the `Spawn` trait)
- Modify: `bombay-core/src/actor/spawn.rs` (add `default_capacity`, re-export via mod)
- Test: `bombay-core/src/actor/spawn.rs` (`tests` module)

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `spawn.rs`:

```rust
/// The ergonomic spawn path uses the default mailbox capacity; pin the constant
/// and that `default_capacity()` yields exactly it (guards a wrong default).
#[test]
fn default_capacity_is_64() {
    assert_eq!(DEFAULT_MAILBOX_CAPACITY, 64);
    assert_eq!(super::default_capacity().get(), 64);
}

/// Linearizability / single-writer: many senders race messages at one actor from
/// the same instant; the actor handles them sequentially, so the total count is
/// exact (none lost or double-counted) despite real concurrency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_senders_single_writer_exact_count() {
    use crate::actor::Spawn;
    use tokio::sync::{Barrier, oneshot};

    const SENDERS: u32 = 8;
    const PER_SENDER: u32 = 50;

    struct Sink {
        count: u32,
        done_at: u32,
        done: Option<oneshot::Sender<u32>>,
    }
    struct Bump;
    impl Msg for Bump {}
    impl Mailboxed for Sink {
        type Msg = Bump;
    }
    impl crate::actor::Actor for Sink {
        type Args = (u32, oneshot::Sender<u32>);
        type Error = core::convert::Infallible;
        async fn on_start((done_at, done): Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { count: 0, done_at, done: Some(done) })
        }
        async fn handle(&mut self, _: Bump, _: ActorRef<Self>, stop: &mut bool) -> Result<(), Self::Error> {
            self.count += 1;
            if self.count == self.done_at {
                if let Some(done) = self.done.take() {
                    let _ = done.send(self.count);
                }
                *stop = true;
            }
            Ok(())
        }
    }

    let (done_tx, done_rx) = oneshot::channel();
    let total = SENDERS * PER_SENDER;
    let actor_ref = Sink::spawn_with_capacity(cap(4), (total, done_tx));

    let start = Arc::new(Barrier::new(SENDERS as usize));
    let mut tasks = Vec::new();
    for _ in 0..SENDERS {
        let sender = actor_ref.mailbox_sender().clone();
        let start = Arc::clone(&start);
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            for _ in 0..PER_SENDER {
                sender.send(Signal::Message(Bump)).await.expect("send");
            }
        }));
    }

    let final_count = done_rx.await.expect("actor finished");
    assert_eq!(final_count, total, "single writer counted every message exactly once");
    for task in tasks {
        task.await.expect("sender task");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `nix develop --command cargo test -p bombay-core -- default_capacity_is_64 concurrent_senders_single_writer_exact_count`
Expected: FAIL — `Spawn`, `default_capacity` not found.

- [ ] **Step 3: Add `default_capacity` to `spawn.rs`**

In `bombay-core/src/actor/spawn.rs`, add below the `DEFAULT_MAILBOX_CAPACITY` const:

```rust
/// The default capacity as a validated [`Capacity`]. Infallible for the fixed
/// constant 64 (in `1..=Capacity::MAX`); the `expect` is proven by
/// `default_capacity_is_64` and can never trip at runtime.
pub(crate) fn default_capacity() -> Capacity {
    #[expect(
        clippy::expect_used,
        reason = "DEFAULT_MAILBOX_CAPACITY (64) is a compile-time-valid capacity; \
                  the conversion is infallible and pinned by a unit test"
    )]
    Capacity::try_from(DEFAULT_MAILBOX_CAPACITY).expect("64 is a valid capacity")
}
```

- [ ] **Step 4: Add the `Spawn` ext-trait to `mod.rs`**

In `bombay-core/src/actor/mod.rs`, add after the `Actor` trait (and update the `use` for `Capacity` + `spawn` items):

At the top, extend the imports:

```rust
use crate::{
    actor::spawn::{PreparedActor, default_capacity},
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::{Capacity, Mailboxed},
    message::Msg,
};
```

Then add:

```rust
/// Ergonomic spawn entry points, provided for every [`Actor`]. Spawns onto the
/// current tokio runtime and returns the [`ActorRef`]; the actor stops via
/// `Signal::Stop`, [`ActorRef::stop`], [`ActorRef::kill`], a handler crash, or
/// startup failure (ref-count-driven stop is #117).
pub trait Spawn: Actor {
    /// Spawns with the [`DEFAULT_MAILBOX_CAPACITY`](spawn::DEFAULT_MAILBOX_CAPACITY).
    #[must_use]
    fn spawn(args: Self::Args) -> ActorRef<Self> {
        Self::spawn_with_capacity(default_capacity(), args)
    }

    /// Spawns with an explicit mailbox `capacity`.
    #[must_use]
    fn spawn_with_capacity(capacity: Capacity, args: Self::Args) -> ActorRef<Self> {
        let prepared = PreparedActor::<Self>::new(capacity);
        let actor_ref = prepared.actor_ref().clone();
        let _join = prepared.spawn(args);
        actor_ref
    }
}

impl<A: Actor> Spawn for A {}
```

Ensure `Spawn` is re-exported: in the `pub use self::{…}` block, the trait is defined in `mod.rs` itself so it is already `pub`. No re-export needed, but confirm `Spawn` is reachable as `crate::actor::Spawn`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `nix develop --command cargo test -p bombay-core -- default_capacity_is_64 concurrent_senders_single_writer_exact_count`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add bombay-core/src/actor/
git commit -m "core(actor): Spawn ext-trait + default capacity + concurrent single-writer test (#116)"
```

---

## Task 12: Full gate, coverage doc, final commit

**Files:**
- Modify: `docs/testing/coverage-baseline.md` (add the `actor` module)

- [ ] **Step 1: Run the full test suite for the crate**

Run: `nix develop --command cargo test -p bombay-core`
Expected: PASS — all existing (error/mailbox/message/reply) tests plus every new `actor` test.

- [ ] **Step 2: Update the coverage baseline**

In `docs/testing/coverage-baseline.md`, add a row/section for `bombay-core/src/actor/` (mod.rs / actor_ref.rs / kind.rs / spawn.rs) noting the categories covered: sequence (queued-then-stop, stop-flag, on-start ordering), lifecycle (cancel, kill, on_start failure, panic → on_stop), defensive (poison contract, on_start panic caught), linearizability (concurrent single-writer). Match the file's existing format (read it first to mirror the columns/wording).

- [ ] **Step 3: Run the single authoritative gate**

Run: `nix develop --command cargo fmt --check && nix flake check`
Expected: PASS — build + clippy (god-level bar) + fmt + tests all green.

Common gate failures and fixes:
- **clippy `print_stderr`** in `log_on_stop_outcome` → confirm the `#[expect(clippy::print_stderr, reason = …)]` is present.
- **clippy `expect_used`** in `default_capacity` → confirm the `#[expect(clippy::expect_used, reason = …)]` is present.
- **clippy cognitive-complexity / >80 lines** in `run_lifecycle` → it is already split into `run_message_loop`/`handle_message`/`run_on_panic`/`log_on_stop_outcome`; if it still trips, extract the `on_start` match into a `start_actor` helper.
- **`missing_docs`** → every `pub` item (trait, methods, `RunResult` variants + fields, `PreparedActor`, `Spawn`) needs a doc comment; the code above includes them — don't drop any.
- **fmt** → re-run `cargo fmt`.

- [ ] **Step 4: Final commit**

```bash
git add docs/testing/coverage-baseline.md
git commit -m "docs(testing): record #116 actor spine coverage baseline"
```

- [ ] **Step 5: Push and open the PR** (only when the user asks — main is protected; PR gated on green `Nix Flake Check`)

```bash
git push -u origin core/116-actor-trait-loop
# then, when asked:
gh pr create --repo devrandom-labs/bombay --base main \
  --title "core(actor): rebuild Actor trait, lifecycle hooks & run-loop (#116)" \
  --body "Implements #116 per docs/superpowers/specs/2026-07-05-116-actor-trait-loop-design.md. …"
```

---

## Self-Review Checklist (run before handing off)

- **Spec coverage:** trait (T3) · `&mut self`/poison (T9) · loop no-buffer/`run_until_cancelled` (T4/T5/T7) · three stop paths (T4 Signal::Stop, T5 cancel, T10 kill) · four catch_unwind (T4 handle/on_stop, T8 on_start, T9 on_panic) · `from_panic_any` (T2) · `handle` Err→Panicked (T9 `handle_returning_err_stops_as_panicked`) · minimal ActorRef (T3) · `RunResult`/spawn (T4/T11) · deps + hakari (T1) · coverage doc (T12).
- **Placeholder scan:** none — every step has concrete code/commands.
- **Type consistency:** `run_message_loop` (kind.rs) ↔ called in `run_lifecycle` (spawn.rs); `RunResult::{Stopped{actor,reason},StartupFailed,Killed}` used identically in impl + tests; `cancel_token()`/`downgrade()`/`mailbox_sender()` names match between `actor_ref.rs` and `kind.rs`/`spawn.rs`.
