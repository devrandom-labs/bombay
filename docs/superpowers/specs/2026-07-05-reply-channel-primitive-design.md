# Reply channel primitive — design (card #115)

**Status:** approved 2026-07-05 · epic #122 · follows the per-card rigor contract there.

## What this is

The in-process, typed, **single-shot** channel that carries exactly one answer
from a message handler back to the `ask` that is waiting for it. Local tier of
the two-tier message model (#66) — **zero serialization**, no `Box<dyn Any>`.

This is the reply *transport* and its failure semantics, nothing more. *Who*
mints a channel and embeds the sender (`Context`, the per-`Msg`-variant reply
port) is #116/#118; #115 is the primitive they build on and can be tested in
full isolation.

## Re-scope from the card title (approved)

The card is titled "Reply + Delegated/Forwarded + ReplySender". `DelegatedReply`
and `ForwardedReply` are **deferred to the cards that give them meaning**:

- In the kameo reference they are produced *only* by `Context::reply_sender()` /
  `reply()` / `spawn()` / `forward()`, and `forward` needs `ActorRef<B>` +
  `Message<M>`. None of that exists until #116 (actor/Context/loop), #117
  (ActorRef), #118 (request).
- Shipping them now would be dead-until-later types testable only for
  construction/`Debug` — a violation of YAGNI ("add at the second concrete use",
  CLAUDE rule 4) and "test the real SUT" (rule 8).

They land **wired and tested** with their machinery. Recorded on the issue.

## The reference, and why it is replaced

`src/reply.rs` (~1,048 LOC) is built entirely around **type erasure**: a `Reply`
trait with `downcast_ok` / `into_any_err` implemented for ~40 std types plus a
derive macro, feeding `BoxReplySender = oneshot::Sender<Result<BoxReply,
BoxSendError>>`. That erasure exists for exactly one reason — kameo's single
shared `Signal<A>` envelope cannot name a per-message reply type, so the reply
payload is erased to `Box<dyn Any>` and re-typed caller-side.

The #114 message model fixes the reply shape (a typed reply port embedded in the
requesting `Msg` variant). With a per-variant port the reply type is named
directly, so **there is nothing to erase**. The entire `Reply` trait, its std
impls, and the derive are **dropped**: any `R: Send + 'static` is a reply.

## Public surface — `bombay-core/src/reply.rs`

The reply channel is `oneshot::Sender/Receiver<Result<R, E>>` where `R` is the
reply value and `E` is the handler's own domain error (a nexus `Conflict`, …),
kept typed and un-erased end to end. `E` defaults to `Infallible` so an
infallible reply is just `ReplySender<R>`.

```rust
pub struct ReplySender<R, E = Infallible> { /* wraps oneshot::Sender<Result<R, E>> */ }

impl<R, E> ReplySender<R, E> {
    /// Sends the successful reply. Consumes `self`, so a second reply is a
    /// *compile* error. Returns the reply back if the asker already vanished
    /// (its receiver was dropped) — non-lossy (rule 3), the caller may ignore it.
    pub fn send(self, reply: R) -> Result<(), R>;

    /// Sends the handler's typed domain error as the reply.
    pub fn send_err(self, error: E) -> Result<(), E>;
}

pub struct ReplyReceiver<R, E = Infallible> { /* wraps oneshot::Receiver<Result<R, E>> */ }

impl<R, E> ReplyReceiver<R, E> {
    /// Awaits the single reply and maps it into #113's `AskError`:
    ///   Ok(Ok(r))  -> Ok(r)
    ///   Ok(Err(e)) -> Err(AskError::Handler(e))
    ///   RecvError  -> Err(AskError::Interrupted)   // sender dropped, no reply
    ///
    /// `M` is free: the reply layer provably never produces `Deliver`/`Timeout`
    /// (those are the ask builder's, #118), so it hands back an
    /// `AskError<M, E>` for any `M` the caller wants, ready to return with no
    /// re-mapping — while the type still says "I cannot raise a delivery fault".
    pub async fn recv<M>(self) -> Result<R, AskError<M, E>>;
}

/// The sender/receiver pair over one fresh oneshot.
pub fn reply_channel<R, E>() -> (ReplySender<R, E>, ReplyReceiver<R, E>);
```

### Design decisions

1. **Primitive: `tokio::sync::oneshot`** (see ADR-0002). Already a workspace dep,
   purpose-built one-shot, drop-detection on both ends — exactly the "drop →
   `Interrupted`" and "asker-gone → reply handed back" semantics. Kept an impl
   detail behind our wrappers (the mailbox channel-seam philosophy: do not expose
   it; trait-ify at the *second* impl for M6 / `no_std`). Its single `Arc` alloc
   is the card's "1 allocation" reply floor.

2. **Single-send is a type guarantee, not a runtime guard.** `send`/`send_err`
   take `self` by value; a second call fails to compile. Strictly stronger than
   kameo's runtime "second send is a no-op". The card's `reply_port_single_send`
   becomes a `compile_fail` doctest (use-after-move).

3. **`recv` reuses `AskError<M, E>` without lying.** It only ever fills
   `Interrupted` / `Handler`, both `M`-agnostic, so #118 gets a ready-to-return
   error and there is no second overlapping error type. Rejected alternative: a
   narrow `ReplyFault<E> = {Handler | Interrupted}` widened by #118 — honest but
   redundant against #113's canonical `AskError`.

4. **`send`/`send_err` hand the payload back on a dead asker.** `Result<(), R>` /
   `Result<(), E>` rather than kameo's `let _ = ...`. Dropping the reply when the
   asker is gone is *correct* (no one is listening), but surfacing it lets a
   caller observe/measure "asker vanished" — non-lossy per rule 3, ignorable with
   `let _`.

5. **`E = Infallible` default.** Matches #113's `AskError` default. A `tell`
   carries no reply port and cannot fail with a domain error; `ReplySender<R,
   Infallible>` cannot even name `send_err` (Infallible is uninhabited).

## Test net (TDD — every test written failing first)

The four cross-cutting categories (rule 7) come first, paired with the card's
named cases:

- **Sequence/protocol** — `ask_ok_reply_reaches_caller`: `send(reply)` →
  `recv().await == Ok(reply)`. `@bug ask_handler_error_reaches_caller_typed`:
  `send_err(Conflict)` → `recv().await == Err(AskError::Handler(Conflict))`,
  the *specific* typed value; fails if the port were `oneshot<R>` not
  `oneshot<Result<R, E>>`.
- **Lifecycle** — drop the `ReplySender` without sending → `recv().await ==
  Err(AskError::Interrupted)`, and it *returns* (never hangs). This is the
  card's central "drop → error, not a hang" guarantee.
- **Defensive boundary** — `reply_port_single_send`: `compile_fail` doctest that
  a second `send` after move does not compile. `tell_has_no_reply_port`: with
  `E = Infallible`, `send_err` is uncallable and a tell mints no channel.
  `send`-to-a-dropped-receiver returns `Err(reply)` (payload handed back).
- **Linearizability** — barrier'd concurrent `send` ‖ `recv` (`tokio::spawn` +
  `Barrier`), asserting the exact sent value arrives exactly once.
- **DST** — proptest over the interleavings of {`send` | `send_err` | drop} ×
  `recv`, asserting the mapping table holds for every ordering.
- **Mutation** — `cargo-mutants`, **zero surviving mutants** in `reply.rs`.
- **loom** — N/A: no bombay-owned atomics/ordering here (delegated to tokio
  oneshot), same rationale as #113.

## Definition of done

Rebuilt in-place in `bombay-core` (no #61 quarantine header); god-level clippy
bar clean; the test net above green; DST seeds pass; zero surviving mutants;
`nix flake check` green. `DelegatedReply`/`ForwardedReply` explicitly deferred to
#116/#118 (recorded on #115). `docs/testing/coverage-baseline.md` gains a
`reply` (#115) section; ADR-0002 records the oneshot choice.
