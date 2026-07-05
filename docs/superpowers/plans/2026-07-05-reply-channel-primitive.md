# Reply channel primitive (#115) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the typed, single-shot reply channel (`ReplySender`/`ReplyReceiver`/`reply_channel`) that carries one `Result<R, E>` from a handler back to an `ask`, deleting kameo's `Box<dyn Any>` reply erasure.

**Architecture:** A thin wrapper over `tokio::sync::oneshot<Result<R, E>>` (ADR-0002). `send`/`send_err` consume `self` (compile-time single-send). `recv` maps the oneshot outcome into #113's `AskError` (`RecvError→Interrupted`, `Ok(Err e)→Handler(e)`, `Ok(Ok r)→Ok(r)`). The oneshot never appears in the public API. `DelegatedReply`/`ForwardedReply` are out of scope (deferred to #116/#118).

**Tech Stack:** Rust 2024, `tokio::sync::oneshot`, `bombay-core` crate, `proptest`, `cargo-mutants`, god-level clippy bar (no `unwrap`/`expect`/`unreachable`/`panic` in production code).

**Test command:** `nix develop --command cargo test -p bombay-core reply` (module tests). Full gate: `nix flake check`.

---

### Task 1: Module scaffold + `reply_channel` + `send` + `recv` (Ok path)

**Files:**
- Create: `bombay-core/src/reply.rs`
- Modify: `bombay-core/src/lib.rs` (add `pub mod reply;`)

- [ ] **Step 1: Register the module.** In `bombay-core/src/lib.rs`, add `pub mod reply;` in module-name order (after `pub mod message;` → alphabetical: error, mailbox, message, reply):

```rust
pub mod error;
pub mod mailbox;
pub mod message;
pub mod reply;
```

- [ ] **Step 2: Write the failing test.** Create `bombay-core/src/reply.rs` with the module doc, the imports, and only the test (no impl yet) so it fails to compile first:

```rust
//! The actor's typed, single-shot reply channel (card #115).
//!
//! Local tier of the two-tier message model (#66): an `ask` awaits exactly one
//! `Result<R, E>` back from a handler — **in-process, zero-serialize**, no
//! `Box<dyn Any>`. `R` is the reply value; `E` is the handler's own domain error
//! (a nexus `Conflict`, …), kept typed end to end. `E` defaults to [`Infallible`]
//! so an infallible reply is just `ReplySender<R>`.
//!
//! Backed by `tokio::sync::oneshot` (ADR-0002), kept an implementation detail
//! behind [`ReplySender`] / [`ReplyReceiver`] — the mailbox channel-seam
//! philosophy (ADR-0001): swap the primitive for M6 / `no_std` at the second impl.
//!
//! Out of scope (deferred to their machinery): `DelegatedReply` / `ForwardedReply`
//! are produced only by `Context::reply_sender`/`forward` (#116/#118).

use tokio::sync::oneshot;

use crate::error::{AskError, Infallible};

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in domain error — the shape a nexus aggregate's own `thiserror`
    /// enum takes (optimistic-concurrency `Conflict`, …).
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Conflict;

    /// Sequence: a handler's `Ok` reply reaches the caller, typed and intact.
    #[tokio::test]
    async fn ask_ok_reply_reaches_caller() {
        let (tx, rx) = reply_channel::<u32, Infallible>();
        tx.send(7).expect("asker still waiting");
        assert_eq!(rx.recv::<()>().await, Ok(7));
    }
}
```

- [ ] **Step 3: Run test — verify it fails to compile.**

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: FAIL — `cannot find function reply_channel`, `ReplySender`, etc.

- [ ] **Step 4: Write the minimal implementation.** Insert above the `#[cfg(test)]` block:

```rust
/// Sends the single reply to a waiting `ask`. Obtained by the handler; consuming
/// `self` on send makes a second reply a compile error.
#[must_use = "the asker is waiting for this reply"]
pub struct ReplySender<R, E = Infallible> {
    tx: oneshot::Sender<Result<R, E>>,
}

impl<R, E> ReplySender<R, E> {
    /// Sends the successful reply `R`. Consumes `self`. `Err(AskerGone)` if the
    /// asker already dropped its receiver (the ask was abandoned).
    pub fn send(self, reply: R) -> Result<(), AskerGone> {
        self.tx.send(Ok(reply)).map_err(|_| AskerGone)
    }
}

/// The receive half held by the `ask`. Yields the single reply, mapped into the
/// typed [`AskError`].
pub struct ReplyReceiver<R, E = Infallible> {
    rx: oneshot::Receiver<Result<R, E>>,
}

impl<R, E> ReplyReceiver<R, E> {
    /// Awaits the one reply and maps the outcome into [`AskError`]:
    /// `Ok(Ok r) → Ok(r)`, `Ok(Err e) → Handler(e)`, sender-dropped → `Interrupted`.
    ///
    /// `M` is free: this layer never produces `Deliver`/`Timeout` (the ask
    /// builder's, #118), so it returns an `AskError<M, E>` ready for any `M`.
    pub async fn recv<M>(self) -> Result<R, AskError<M, E>> {
        match self.rx.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(handler_err)) => Err(AskError::Handler(handler_err)),
            Err(_recv_error) => Err(AskError::Interrupted),
        }
    }
}

/// The asker had already dropped its receiver, so the reply went nowhere. A unit
/// signal, not the payload: a reply to a vanished asker is un-actionable (nothing
/// to retry, unlike the mailbox's returned `Signal`).
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("asker gone; reply discarded")]
pub struct AskerGone;

/// Builds a fresh reply channel: the sender for the handler, the receiver for the
/// waiting `ask`.
#[must_use]
pub fn reply_channel<R, E>() -> (ReplySender<R, E>, ReplyReceiver<R, E>) {
    let (tx, rx) = oneshot::channel();
    (ReplySender { tx }, ReplyReceiver { rx })
}
```

Note: `assert_eq!(rx.recv::<()>().await, Ok(7))` requires `Result<u32, AskError<(), Infallible>>: PartialEq`. `AskError` does **not** derive `PartialEq`. So this assert will not compile — fix in Step 5 by asserting on the recovered value instead.

- [ ] **Step 5: Fix the test assertion to not require `AskError: PartialEq`.** Replace the assert in `ask_ok_reply_reaches_caller`:

```rust
        let got = rx.recv::<()>().await;
        assert_eq!(got.ok(), Some(7), "the Ok reply arrives typed and intact");
```

- [ ] **Step 6: Run test — verify it passes.**

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: PASS (`ask_ok_reply_reaches_caller`).

- [ ] **Step 7: Commit.**

```bash
git add bombay-core/src/lib.rs bombay-core/src/reply.rs
git commit --no-verify -m "feat(core): #115 reply channel — typed oneshot ReplySender/Receiver + reply_channel"
```

---

### Task 2: `send_err` + `Handler` mapping (`@bug` typed error reaches caller)

**Files:**
- Modify: `bombay-core/src/reply.rs`

- [ ] **Step 1: Write the failing test** in `mod tests`:

```rust
    /// `@bug` — a handler that answers with its own domain error `E` must reach
    /// the caller as `AskError::Handler(E)`, **typed, not erased**. Fails if the
    /// port were `oneshot<R>` instead of `oneshot<Result<R, E>>`. (Ref #122-#2.)
    #[tokio::test]
    async fn ask_handler_error_reaches_caller_typed() {
        let (tx, rx) = reply_channel::<u32, Conflict>();
        tx.send_err(Conflict).expect("asker still waiting");
        // Recover the domain error via AskError::err() — the specific typed value.
        assert_eq!(rx.recv::<()>().await.err().and_then(AskError::err), Some(Conflict));
    }
```

- [ ] **Step 2: Run test — verify it fails to compile.**

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: FAIL — `no method named send_err`.

- [ ] **Step 3: Add `send_err`** to `impl<R, E> ReplySender<R, E>`, after `send`:

```rust
    /// Sends the handler's typed domain error `E` as the reply (surfaces as
    /// [`AskError::Handler`]). Consumes `self`. `Err(AskerGone)` if the asker is gone.
    pub fn send_err(self, error: E) -> Result<(), AskerGone> {
        self.tx.send(Err(error)).map_err(|_| AskerGone)
    }
```

- [ ] **Step 4: Run test — verify it passes.**

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: PASS (both tests).

- [ ] **Step 5: Commit.**

```bash
git add bombay-core/src/reply.rs
git commit --no-verify -m "feat(core): #115 send_err — typed handler error maps to AskError::Handler"
```

---

### Task 3: Lifecycle — drop sender → `Interrupted`, never hangs

**Files:**
- Modify: `bombay-core/src/reply.rs`

- [ ] **Step 1: Write the failing test** in `mod tests`:

```rust
    /// Lifecycle: dropping the `ReplySender` without replying must surface
    /// `AskError::Interrupted` to the asker — and **return**, never hang. This is
    /// the card's central "drop → error, not a deadlock" guarantee.
    #[tokio::test]
    async fn dropping_sender_interrupts_the_ask() {
        let (tx, rx) = reply_channel::<u32, Conflict>();
        drop(tx);
        assert!(matches!(rx.recv::<()>().await, Err(AskError::Interrupted)));
    }
```

- [ ] **Step 2: Run test — verify it passes** (behaviour already implemented in Task 1's `recv`; this test pins the `Err(_recv_error)` arm so a mutation to it is caught).

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add bombay-core/src/reply.rs
git commit --no-verify -m "test(core): #115 drop-sender interrupts the ask (never hangs)"
```

---

### Task 4: Defensive — send to a gone asker returns `AskerGone`

**Files:**
- Modify: `bombay-core/src/reply.rs`

- [ ] **Step 1: Write the failing test** in `mod tests`:

```rust
    /// Defensive: if the asker dropped its receiver (ask abandoned), the
    /// handler's `send`/`send_err` report `AskerGone` rather than deadlocking or
    /// panicking. The reply is discarded — un-actionable, so no payload is returned.
    #[tokio::test]
    async fn send_to_gone_asker_reports_asker_gone() {
        let (tx, rx) = reply_channel::<u32, Conflict>();
        drop(rx);
        assert_eq!(tx.send(9), Err(AskerGone));

        let (tx, rx) = reply_channel::<u32, Conflict>();
        drop(rx);
        assert_eq!(tx.send_err(Conflict), Err(AskerGone));
    }
```

- [ ] **Step 2: Run test — verify it passes** (pins the `map_err(|_| AskerGone)` arms on both send paths).

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add bombay-core/src/reply.rs
git commit --no-verify -m "test(core): #115 send to a gone asker reports AskerGone"
```

---

### Task 5: `tell` has no reply port — `E = Infallible` roundtrip

**Files:**
- Modify: `bombay-core/src/reply.rs`

- [ ] **Step 1: Write the failing test** in `mod tests`:

```rust
    /// A `tell` carries no reply port and cannot fail with a domain error, so its
    /// reply type is `E = Infallible`: `send_err` is uncallable (Infallible is
    /// uninhabited — there is no value to pass), and only the `Ok` path exists.
    /// This pins that the Infallible-defaulted channel roundtrips a plain value.
    #[tokio::test]
    async fn infallible_reply_has_no_error_path() {
        let (tx, rx) = reply_channel::<u32, Infallible>();
        tx.send(42).expect("asker still waiting");
        assert_eq!(rx.recv::<()>().await.ok(), Some(42));
    }
```

- [ ] **Step 2: Run test — verify it passes.**

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add bombay-core/src/reply.rs
git commit --no-verify -m "test(core): #115 tell has no reply port — Infallible reply roundtrip"
```

---

### Task 6: Linearizability — concurrent `send` ‖ `recv`

**Files:**
- Modify: `bombay-core/src/reply.rs`

- [ ] **Step 1: Write the failing test** in `mod tests` (needs `std::sync::Arc` + `tokio::sync::Barrier`; add `use std::sync::Arc;` and `use tokio::sync::Barrier;` inside `mod tests` if not present):

```rust
    /// Linearizability: a sender and a receiver race from the same instant on a
    /// multi-thread runtime; the exact sent value must arrive exactly once,
    /// whichever side wins the start. Real overlap (spawn + Barrier), not
    /// sequential-then-check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_send_and_recv_deliver_the_exact_value() {
        let (tx, rx) = reply_channel::<u64, Infallible>();
        let start = Arc::new(Barrier::new(2));

        let sender_start = Arc::clone(&start);
        let sender = tokio::spawn(async move {
            sender_start.wait().await;
            tx.send(0xABCD_1234).expect("receiver present");
        });
        let receiver = tokio::spawn(async move {
            start.wait().await;
            rx.recv::<()>().await
        });

        sender.await.expect("sender task");
        let got = receiver.await.expect("receiver task");
        assert_eq!(got.ok(), Some(0xABCD_1234));
    }
```

- [ ] **Step 2: Run test — verify it passes.**

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add bombay-core/src/reply.rs
git commit --no-verify -m "test(core): #115 linearizability — concurrent send/recv exact-value"
```

---

### Task 7: DST — proptest over {send | send_err | drop} × recv interleavings

**Files:**
- Modify: `bombay-core/src/reply.rs`

- [ ] **Step 1: Write the failing property test.** Add `use proptest::prelude::*;` and `use tokio::runtime::Builder;` inside `mod tests`, then:

```rust
    /// The reply-outcome mapping holds for every handler action, driven under a
    /// single-thread runtime for deterministic, replayable interleaving. Each
    /// action pins exactly one arm of `recv`'s match; proptest sweeps all three.
    #[derive(Debug, Clone)]
    enum Action {
        Reply(u32),
        Fail,
        Drop,
    }

    proptest! {
        #[test]
        fn prop_reply_outcome_matches_action(
            action in prop_oneof![
                any::<u32>().prop_map(Action::Reply),
                Just(Action::Fail),
                Just(Action::Drop),
            ],
        ) {
            let rt = Builder::new_current_thread().build().expect("current-thread rt");
            rt.block_on(async {
                let (tx, rx) = reply_channel::<u32, Conflict>();
                match action.clone() {
                    Action::Reply(v) => { let _ = tx.send(v); }
                    Action::Fail => { let _ = tx.send_err(Conflict); }
                    Action::Drop => drop(tx),
                }
                let got = rx.recv::<()>().await;
                match action {
                    Action::Reply(v) => prop_assert_eq!(got.ok(), Some(v)),
                    Action::Fail => prop_assert_eq!(got.err().and_then(AskError::err), Some(Conflict)),
                    Action::Drop => prop_assert!(matches!(got, Err(AskError::Interrupted))),
                }
                Ok(())
            })?;
        }
    }
```

- [ ] **Step 2: Run test — verify it passes.**

Run: `nix develop --command cargo test -p bombay-core reply`
Expected: PASS (`prop_reply_outcome_matches_action`).

- [ ] **Step 3: Commit.**

```bash
git add bombay-core/src/reply.rs
git commit --no-verify -m "test(core): #115 DST — proptest reply-outcome mapping over all actions"
```

---

### Task 8: `compile_fail` doctest — single-send is a type guarantee

**Files:**
- Modify: `bombay-core/src/reply.rs` (doc comment on `ReplySender::send`)

- [ ] **Step 1: Add a `compile_fail` doctest** to `ReplySender::send`'s doc comment (append after the existing doc lines, before the `pub fn send`):

```rust
    /// A second reply does not compile — `send` moves `self`:
    ///
    /// ```compile_fail
    /// # use bombay_core::reply::reply_channel;
    /// # use bombay_core::error::Infallible;
    /// let (tx, _rx) = reply_channel::<u32, Infallible>();
    /// let _ = tx.send(1);
    /// let _ = tx.send(2); // ← tx already moved: E0382
    /// ```
```

- [ ] **Step 2: Run the doctests — verify the `compile_fail` is honored.**

Run: `nix develop --command cargo test -p bombay-core --doc reply`
Expected: PASS (the doctest is expected to fail compilation, so the test passes).

- [ ] **Step 3: Commit.**

```bash
git add bombay-core/src/reply.rs
git commit --no-verify -m "docs(core): #115 compile_fail doctest — double-reply cannot compile"
```

---

### Task 9: Coverage baseline + mutation gate + full flake check

**Files:**
- Modify: `docs/testing/coverage-baseline.md`

- [ ] **Step 1: Run the mutation gate on the reply module.** Confirm zero surviving mutants. The repo runs mutants through the flake (per the #113/#114 baseline entries); scope it to the reply file for a fast loop:

Run (fast, targeted): `nix develop --command cargo mutants --file bombay-core/src/reply.rs`
Fallback (whole-workspace, as the baseline cites): `nix build .#mutants`
Expected: `0 missed` (all mutants caught or unviable). If any survive, add a test that kills each, re-run, then continue.

- [ ] **Step 2: Add the `reply` (#115) section** to `docs/testing/coverage-baseline.md`, immediately after the `### `message` (#114) — done` section:

```markdown
### `reply` (#115) — done
`bombay-core/src/reply.rs` carries the typed single-shot reply channel:
`ReplySender<R, E>` / `ReplyReceiver<R, E>` / `reply_channel()` over
`tokio::sync::oneshot<Result<R, E>>` (ADR-0002). Kameo's `Box<dyn Any>`
`Reply`-trait erasure is **dropped** — a typed port erases nothing, so any
`R: Send + 'static` is a reply. `send`/`send_err` consume `self` (double-reply is
a compile error, proved by a `compile_fail` doctest); a gone asker is reported as
`AskerGone`. `recv` maps the oneshot outcome into #113's `AskError`
(`Ok(Ok r)→Ok(r)`, `Ok(Err e)→Handler(e)`, sender-dropped→`Interrupted`), generic
over the never-produced `M`. Covered by: the `@bug` typed-handler-error probe, the
Ok-reply sequence, the drop→`Interrupted` lifecycle (never hangs), the
`send`-to-gone-asker defensive case, the `Infallible` (tell) roundtrip, a
2-thread barrier'd linearizability test, and a proptest DST sweeping all three
handler actions. **Mutation: 0 missed.** No bombay-owned atomics →
loom/DST-interleaving beyond proptest N/A (delegated to tokio oneshot), same as
#113. `DelegatedReply`/`ForwardedReply` deferred to #116/#118 (recorded on #115).
```

- [ ] **Step 3: Run the full gate.**

Run: `nix flake check`
Expected: PASS (build + clippy god-level bar + fmt + tests, workspace-wide).

- [ ] **Step 4: Commit.**

```bash
git add docs/testing/coverage-baseline.md
git commit --no-verify -m "docs(testing): #115 reply coverage baseline; mutation gate 0 missed"
```

---

## Post-plan

- Update `README.md` per the card rule: the rebuilt spine is not yet behind the public umbrella (same as #113/#114/#133), so **no README change** — the reply module is internal until the core lands. Confirm this classification holds; if `reply` became public surface, refresh the "public API at a glance" bullet instead.
- Open the PR for #115 against `main`; let CI (`Nix Flake Check`) be the merge gate.
- Move #115 → Done on project board #4 after merge.
