# #145 — `Recipient` / `ReplyRecipient` type-erased fan-in — design

**Card:** devrandom-labs/bombay#145 (sub-task of #117, split from PR #144 which
landed the ref-model). **Milestone:** M1 · Foundation. **Depends on:** #144
(ref-model: `ActorRef`/`WeakActorRef`, `Signal::Message { msg, self_sender }`,
`ActorRef::tell`). **Design-of-record for implementation:** this spec + a new
**ADR-0004** authored alongside the code (mirrors how ADR-0003 was written under
#117).

## Problem

Fan-in wants a homogeneous collection — `Vec<Recipient<M>>` — that holds handles
to **heterogeneous actor types** and lets a caller broadcast one message `M` to
all of them. kameo expresses this as `Recipient<M>` erasing the actor `A` while
fixing `M`, because a kameo actor impls `Message<M>` for *many* `M`.

Bombay deliberately rejected that shape (the closed-menu decision, #114): an
actor has **one** closed `A::Msg` enum, handled by
`handle(&mut self, msg: A::Msg, …)`. There is no per-message `Message<M>` trait.
So "fan-in over a shared `M`" must be re-grounded in a way that preserves:

1. the **closed-per-actor `Msg` menu** (each actor keeps its own enum), and
2. the **by-value, zero-box send path** (no `Box<dyn>` for the *message* — the
   #122 concern where one boxed-vs-inline decision is a 256× queue-memory swing).

## Decision: conversion-boundary erasure (Option A)

`Recipient<M>` targets any actor `A` whose menu can be built from `M`:
**`A::Msg: From<M>`**. The erased send does `A::Msg::from(m)` (by value) then
`send_message` — the message is converted, never boxed.

### Why A over "shared-enum only" (`A::Msg = M`)

- **Heterogeneous menus.** A `Ledger` (menu `LedgerCmd`) and an `Audit` (menu
  `AuditCmd`) can share a `Recipient<Tick>` though their command sets differ —
  the real shape of independent aggregates that happen to understand a common
  signal.
- **One actor joins many groups.** Keyed on a *conversion*, an actor whose menu
  impls `From<Tick>` **and** `From<Report>` is reachable through both
  `Recipient<Tick>` and `Recipient<Report>`. Shared-enum caps an actor at exactly
  one group forever (the one whose `M` is its whole menu).
- **Superset for free.** Shared-enum is just the blanket identity `From<T> for T`,
  so choosing A does not foreclose it.

### Why not frunk / `HList` / `Coproduct`

- `HList` *preserves and recovers* each element's concrete type at compile time
  and is fixed-length — the **opposite** of what a runtime `Vec<Recipient<M>>`
  needs (erase the type behind one interface; add/remove members at runtime).
- `Coproduct` *could* auto-derive the `From<M>` conversions, but only by
  replacing the hand-written closed menu with a generic coproduct type — which
  fights #114's size-tripwire derive, the exhaustive-`match` discipline in
  `handle`, and self-documenting menus. The `From` impl it would save is ~3 lines
  and is frequently **lossy on purpose** (`Tick → LedgerCmd::Post`), a domain
  decision a generic derive should not guess.
- The heterogeneity lives **inside the vtable**, not in the collection's type. A
  plain trait object is the whole mechanism — no type-level machinery.

## Architecture

New module `bombay-core/src/actor/recipient.rs`; `actor/mod.rs` gains
`mod recipient;` and `pub use recipient::{Recipient, WeakRecipient};`.

### The erased seam (private)

```rust
trait ErasedRecipient<M>: Send + Sync {
    fn tell(&self, msg: M) -> BoxFuture<'_, Result<(), TellError<M>>>; // awaits capacity
    fn try_tell(&self, msg: M) -> Result<(), TellError<M>>;            // non-blocking
    fn id(&self) -> ActorId;
    fn is_alive(&self) -> bool;
    fn downgrade(&self) -> WeakRecipient<M>;
}

trait ErasedWeakRecipient<M>: Send + Sync {
    fn upgrade(&self) -> Option<Recipient<M>>;
    fn id(&self) -> ActorId;
}
```

- `M` is the **trait's** type parameter, not a method generic, so
  `dyn ErasedRecipient<M>` is object-safe (`tell(msg: M)` takes the trait's own
  `M` by value).
- The blanket impls are on the **ref handles**, monomorphized per actor:
  `impl<A: Actor, M> ErasedRecipient<M> for ActorRef<A> where A::Msg: From<M>,
  M: Clone + Send + 'static` and the weak counterpart on `WeakActorRef<A>`.
  Erasing on `ActorRef` (not a bare `MailboxSender`) gives `downgrade` a natural
  composition (`ActorRef::downgrade → WeakActorRef → WeakRecipient`) and lets the
  `Recipient` **narrow** the exposed surface.

### The public handles

```rust
pub struct Recipient<M>     { inner: Arc<dyn ErasedRecipient<M>> }     // Clone via Arc::clone
pub struct WeakRecipient<M> { inner: Arc<dyn ErasedWeakRecipient<M>> }
```

`Recipient` exposes `tell` / `try_tell` / `id` / `is_alive` / `downgrade` —
deliberately **not** `stop` / `kill`. A recipient is a messaging handle, not a
lifecycle handle; erasure is used here as encapsulation, not merely dispatch.
Both carry a hand-written `Debug` (names the struct + `id` + the `M` type name),
matching the `ActorRef` precedent so the impl can't be stubbed to an empty
formatter.

### Construction

```rust
impl<A: Actor> ActorRef<A> {
    #[must_use]
    pub fn recipient<M>(&self) -> Recipient<M>
    where A::Msg: From<M>, M: Clone + Send + 'static;
}
// plus `impl<A: Actor, M> From<ActorRef<A>> for Recipient<M>` (bounds as above) for `.into()`
```

Primary spelling: `ledger.recipient::<Tick>()`. Internally
`Recipient { inner: Arc::new(self.clone()) as Arc<dyn ErasedRecipient<M>> }`.

## Data flow — the send path

`Recipient::<M>::try_tell(m)`:

1. `let converted = A::Msg::from(m.clone());` — the **only** clone, and the
   reason `M: Clone` is required (see below).
2. `sender.try_send(Signal::Message { msg: converted, self_sender: sender.clone() })`
   — the converted `A::Msg` lives **inline** in the signal, moved into the flume
   slot by value. No heap box for the message.
3. On `TrySendError::Full` → `TellError::MailboxFull(m)`; on
   `TrySendError::Closed` → `TellError::ActorNotAlive(m)` — the **retained
   original `m`** is handed back, preserving the `error.rs` contract that a
   `TellError<M>` never loses the message into the void.

`Recipient::<M>::tell(m)` mirrors this over the async `send_message`; it only
fails `ActorNotAlive` (it awaits capacity rather than reporting `Full`).

### The `M: Clone` handback consequence (recorded)

Under erasure there is no `A::Msg → M` to recover the original message once
converted (`A` is erased; `A::Msg` is unnameable). Keeping the typed-`M`
handback therefore requires cloning `M` before conversion. This is the honest
price of **"zero-box message + typed handback + erasure — pick all three"**;
kameo only avoids it by erasing the message as `Box<dyn Any>` and downcasting on
failure — exactly the hot-path box bombay forbids. The cost is a non-issue for
the primary use case: broadcasting one `M` to N actors already implies `M: Clone`.

## Zero-box invariant, stated precisely

- **The message never hits the heap.** The converted `A::Msg` is stored inline
  in `Signal::Message` and moved into the queue slot by value (the invariant the
  card protects).
- **`try_tell` is fully allocation-free.**
- The async `tell` boxes **the future** (`futures::future::BoxFuture`, already a
  crate dependency) — the unavoidable cost of `dyn` async dispatch. It boxes the
  future, **never the message**, and is absent from `try_tell`. Both are provided:
  `try_tell` as the zero-alloc fan-out primitive, `tell` for parity with
  `ActorRef::tell`. The `try_tell` zero-alloc claim is asserted structurally here
  and **counted** by #151's counting allocator later.

## Error handling

Reuses `TellError<M>` verbatim — no new error type. `MailboxFull` (retryable
backpressure) and `ActorNotAlive` (terminal) both carry the original `M` back.
No `Box<dyn Error>`, no erasure of the typed message.

## `ReplyRecipient` — deferred to #118 (design recorded)

`ReplyRecipient` is **out of implementation scope for #145** and lands with #118.
The reason is structural, not stylistic: an ask-capable recipient needs a **reply
port threaded through the handler**, and that port does not exist yet —
`Signal::Message` has no reply field, `handle` returns `Result<(), E>` with no
channel, and ADR-0003 explicitly states "#118 will extend `Signal::Message` with
the reply port." Building it now would design-and-pre-empt the core of #118 blind.

Anticipated shape (recorded in ADR-0004 so #118 slots it in): `ReplyRecipient<M,
R, E>` erasing `A` where `A::Msg: From<M>`, backed by the #115 reply-channel
primitive; `ask(m) -> Result<R, AskError<M, E>>`. `R`/`E` are explicit params
(bombay has no `Message<M>::Reply` associated type to source them from).

The deferral is noted on card #145.

## Testing (TDD — write failing first; the 4 cross-cutting categories)

Tests run against a raw `MailboxReceiver` (recv-and-inspect the delivered
`Signal`), like the existing `actor_ref.rs` tests — deterministic, no run-loop
needed. A helper builds an `ActorRef<A>` via the `pub(crate)` `new` plus a
retained receiver.

- **Headline / fan-in (sequence + object-safety):** a `Vec<Recipient<Tick>>` over
  **two different actor types with different menus** (`LedgerCmd`, `AuditCmd`);
  broadcast one `Tick`; assert each mailbox received its **own converted variant**
  by exact value (`assert_eq!`, never match-any). This is the proof that erasure
  + heterogeneous dispatch works.
- **Conversion:** a `From<Tick>` mapping to a specific variant; assert that exact
  variant arrives (guards the `.into()` call being real).
- **Handback (defensive boundary):** full mailbox → `try_tell` returns
  `MailboxFull(original)` carrying the exact `M`; dropped receiver → `try_tell`
  and `tell` return `ActorNotAlive(original)`.
- **Lifecycle:** `downgrade` → `upgrade` is `Some` while a strong sender lives,
  `None` after every strong ref drops; `id` is preserved through erasure **and**
  through downgrade/upgrade round-trip.
- **Debug** format is stable (pins the hand-written impl).

The exhaustive #117 finalization matrix (bench/mutation/property/fuzz/MIRI/DST +
exact-memory/no-leak) is owned by sibling cards #146–#152 and is out of scope for
#145's own PR.

## Scope-of-done

- [ ] `Recipient<M>` + `WeakRecipient<M>` in `actor/recipient.rs`, exported.
- [ ] `ErasedRecipient<M>` / `ErasedWeakRecipient<M>` blanket impls on
      `ActorRef<A>` / `WeakActorRef<A>`.
- [ ] `ActorRef::recipient::<M>()` + `From<ActorRef<A>>`.
- [ ] `tell` (async, boxed future) + `try_tell` (zero-alloc) + `id` /
      `is_alive` / `downgrade`; `Clone` + `Debug` on both handles.
- [ ] Tests above, TDD, all green.
- [ ] ADR-0004 authored (conversion-boundary erasure, `M: Clone` handback,
      frunk rejected, `ReplyRecipient` deferral + anticipated shape).
- [ ] README public-API bullet updated if user-visible; else coverage-baseline.
- [ ] `nix flake check` green.
