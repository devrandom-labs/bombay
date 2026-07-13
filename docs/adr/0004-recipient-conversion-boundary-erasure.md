# ADR-0004 — `Recipient` conversion-boundary type-erased fan-in

**Status:** Accepted (2026-07-13) — implemented under card #145 (sub-task of #117)

## Context

Fan-in needs a homogeneous `Vec<Recipient<M>>` over heterogeneous actors. kameo's
`Recipient<M>` erases the actor and fixes `M` because a kameo actor impls
`Message<M>` for many `M`. Bombay rejected that (closed-menu decision #114): an
actor has one closed `A::Msg` enum, handled by `handle(&mut self, msg: A::Msg, …)`.
So fan-in must be re-grounded without breaking (1) the closed per-actor menu and
(2) the by-value, zero-box message path (the #122 concern where boxed-vs-inline is
a 256× queue-memory swing).

## Decision

`Recipient<M>` targets any actor where **`A::Msg: From<M>`**. A private
`ErasedRecipient<M>` trait object (`Arc<dyn …>`) erases the actor; the blanket
impl on `ActorRef<A>` converts `M -> A::Msg` **by value** then enqueues via the
mailbox. `WeakRecipient<M>` mirrors it over `WeakActorRef<A>`.

- **Conversion boundary, not shared enum.** Superset of "shared menu" (identity
  `From<T> for T`), lets one actor join many groups (`From<Tick>` **and**
  `From<Report>`), and expresses genuinely different menus sharing a signal.
  Shared-enum would cap an actor at exactly one group forever (the one whose `M`
  is its whole menu).
- **`M: Clone` typed handback (the consequence).** Erasure leaves no
  `A::Msg -> M`, so keeping the `TellError<M>` "message never lost" guarantee
  requires cloning `M` before conversion (one clone, not doubled). Honest price of
  "zero-box message + typed handback + erasure — pick all three"; kameo only
  dodges it by boxing the message as `Box<dyn Any>` (the hot-path box we forbid).
  Free for the broadcast use case, which already implies `M: Clone`.
- **No frunk / `HList` / `Coproduct`.** `HList` preserves and recovers each type
  (opposite of erasure) and is fixed-length (a fan-in `Vec` is dynamic).
  `Coproduct` could auto-derive the `From` impls but only by replacing the
  hand-written closed menu — fighting #114's size tripwire, exhaustive `match`,
  and readable menus. The heterogeneity lives in the vtable, not the collection
  type; a plain trait object is the whole mechanism.
- **Erase on `ActorRef`, narrow the surface.** `Recipient` exposes
  `tell`/`try_tell`/`id`/`is_alive`/`downgrade` — not `stop`/`kill`. Erasure as
  encapsulation, and `downgrade` composes via `ActorRef::downgrade`. The erased
  impls call the inherent methods via `Self::` (inherent shadows the trait method
  in path syntax) — never `self.id()`, which would recurse into the trait.
- **`tell` boxes the future.** `try_tell` is fully alloc-free; the async `tell`
  boxes the future (`BoxFuture`) — the unavoidable `dyn` async cost, never the
  message. `MailboxSender::try_send_message` (the non-blocking sibling of
  `send_message`) backs `try_tell`.

## `ReplyRecipient` — deferred to #118

An ask-capable recipient needs a reply port threaded through the handler, which
does not exist: `Signal::Message` has no reply field, `handle` returns
`Result<(), E>`, and ADR-0003 states "#118 will extend `Signal::Message` with the
reply port." Building it now pre-empts #118 blind. Anticipated shape:
`ReplyRecipient<M, R, E>` erasing `A` where `A::Msg: From<M>`, backed by the #115
reply-channel primitive, `ask(m) -> Result<R, AskError<M, E>>` (`R`/`E` explicit —
bombay has no `Message<M>::Reply` to source them from).

## Consequences

- New public API: `Recipient<M>`, `WeakRecipient<M>`, `ActorRef::recipient::<M>()`,
  `From<ActorRef<A>>`, `MailboxSender::try_send_message`.
- `M: Clone + Send + 'static` and `A::Msg: From<M>` are the enrolment bounds.
- The #117 finalization matrix (bench/mutation/property/fuzz/MIRI/DST +
  exact-memory/no-leak) for this code is owned by #146–#152.
