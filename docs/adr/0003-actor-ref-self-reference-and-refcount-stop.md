# ADR-0003 — `ActorRef` self-reference & ref-count-driven stop

**Status:** Accepted (2026-07-06) — implemented under card #117

## Context

The actor run-loop must stop when the **last external strong `ActorRef` is
dropped** (ref-count-driven shutdown — the #116 "all-senders-gone" loop arm,
deliberately left unreachable in #116 because the loop held a strong self-ref).

To make that arm reachable, the loop must **not** hold a strong sender to its own
mailbox while parked on `recv` — otherwise it keeps itself alive forever. But
`Actor::handle(&mut self, msg, actor_ref: ActorRef<Self>, stop)` (the #116
contract) hands the handler a **strong** self-address by value. So once the loop
holds only a weak self-ref, we need a *source* for the strong ref `handle`
requires — and that source must survive the interval between a message being
enqueued and the loop dequeuing it.

### Verified facts (primary source, not lore)

1. **`flume` 0.12 `WeakSender::upgrade`** (`src/lib.rs:886-905`): `fetch_update`s
   `sender_count`; returns `None` when `count == 0` (*"all senders are closed
   already → don't increase the sender count"*). A weak sender **cannot** mint a
   strong one once the strong count reaches 0.
2. **`tokio` 1.52 `chan::Tx::upgrade`** (`src/sync/mpsc/chan.rs:156-167`):
   identical — `if tx_count == 0 { return None }` then a CAS. This is a
   **universal channel invariant**, not a flume quirk: allowing it would let a
   caller *resurrect* a channel after it signalled closed.
3. **`flume` 0.12 `Sender::drop`** (`src/lib.rs:860-866`): the `count: 1→0`
   transition calls `disconnect_all()`, which fires the parked receiver's hooks →
   `recv()` wakes with `None`. **The sender-count is itself a zero-cost async
   "last handle dropped" signal.**
4. **The #114 slot-budget tripwire measures `A::Msg`, not `Signal<A>`**
   (`message.rs`): the static-assert is on `size_of::<Self>()` of the domain
   message. An envelope field on `Signal::Message` costs the user **nothing**
   against their 256 B budget.
5. **`mailbox.rs` has zero `actor::` dependencies** — it is a pure, domain-
   agnostic channel seam (ADR-0001), and **43 of the 83** `Signal::Message`
   construction sites are at that raw layer (mailbox unit tests, bench, example)
   with no `Actor`/`ActorRef` in scope.

### The decisive usage pattern

The "message enqueued just before the last ref drops" case is **not** a rare
race — it is the single most common call pattern:

```rust
let a = A::spawn(args);
a.tell(x);   // fire-and-forget
drop(a);     // release the handle immediately
```

If that was the last ref: `x` enqueues, `sender_count → 0`, `recv()` returns
`Some(x)`, but `upgrade()` now returns `None`. Any design that abandons the
un-upgradable message would **silently discard `x` on basic usage**. Therefore a
queued message must **pin the actor alive until it is handled** — this is a
load-bearing correctness property, not an optimization.

## Options considered

- **A — embed the full `ActorRef` in `Signal::Message`** (kameo 0.21's model;
  it downgrades the loop's self-ref via `into_downgrade()` after `on_start` and
  carries a strong `ActorRef` in every mailbox signal). Correct and proven, but
  forces `mailbox.rs` to depend upward on `actor::ActorRef` — a layering
  regression for the pure seam (fact 5) — and carries the fattest slot
  (~40 B/message).
- **E — embed only `MailboxSender<A>` in `Signal::Message`** *(chosen)*. The
  key insight: **only the flume `Sender` gates liveness**; `id`/`cancel`/`abort`
  are cheap non-liveness clones the loop already holds. So the message carries
  just the sender (~8 B, one `Arc` pointer, stays inside `mailbox.rs`'s own
  types) and the loop reassembles the strong `ActorRef` from the message's
  sender + its own cold fields.
- **B — separate liveness channel** (loop holds a strong mailbox sender always
  and mints handler refs freely; a dedicated second per-actor channel signals
  "last handle dropped"). Keeps the envelope pristine (0 of 83 sites change) and
  the seam pure, but adds a second per-actor primitive **and** a bespoke
  "drain-the-queue-on-ref-count-stop" policy that is a *different* mechanism from
  the graceful `stop()` path — machinery the finalized card (#117) did not buy,
  and it contradicts the finalized `is_alive() = !sender.is_closed()` wording.
- **C — upgrade a weak sender while only the receiver is alive.** Ruled out:
  impossible in flume **and** tokio by design (facts 1-2); it is a channel-
  resurrection anti-pattern.
- **D — loop holds weak, abandon any un-upgradable message.** Ruled out: breaks
  the everyday `tell; drop` pattern (see above).

## Decision

Adopt **E**. Concretely:

- `Signal::Message { msg: A::Msg, self_sender: MailboxSender<A> }` — the domain
  message plus a **strong** clone of the sender that enqueued it. The strong
  clone keeps `sender_count ≥ 1` while the message is queued, so `recv()` returns
  `None` only when the queue is empty **and** no external refs remain
  (drain-then-stop, for free, via the mailbox).
- The run-loop holds **no** strong self-sender during the message loop. It gets
  each handler's strong `ActorRef<Self>` by reassembling
  `{ id, mailbox: self_sender, cancel, abort }` from the dequeued signal plus its
  own cold fields. It retains a `WeakMailboxSender` only to build the
  `WeakActorRef` that `on_start` (downgrade), `on_panic`, and `on_stop` receive.
- `ActorRef::tell` becomes the entry point that stamps `self.mailbox.clone()`
  into the signal (the ergonomic ask/tell **builders** — timeout, backpressure —
  remain #118).

### Consequences

- Ref-count-driven stop goes live: the #116 "all-senders-gone" arm is now
  reachable, and dropping the last strong `ActorRef` stops the actor.
- **`triomphe` (a leaner `Arc`) is moot here (#117).** `ActorRef` hand-rolls no
  `Arc`-based refcount to slim down: liveness is delegated entirely to flume's
  `sender_count` (the decisive usage pattern above), and `id` is `Copy`. The
  only `Arc`s a ref holds are `cancel` (a `CancellationToken`) and `abort` (an
  `AbortHandle`) — cold, non-liveness clones off the hot path, and each is a
  third-party type whose internals `triomphe` cannot reach. There is no
  bombay-owned refcount for a leaner `Arc` to optimize, so `triomphe` is not
  adopted. (Recorded to close #117's "evaluate triomphe" bullet as examined, not
  merely skipped.)
- Stop semantics split intuitively: **explicit** `stop()` / `Signal::Stop`
  abandons the queued backlog (finish-current-then-stop, per #116); **running out
  of references** drains the queue first (queued work self-pins, so it is handled
  before the actor stops).
- `mailbox.rs` stays free of `actor::` deps (only its own `MailboxSender<A>`
  appears in the envelope).
- Migration: the 83 `Signal::Message(_)` sites move to the two-field form;
  actor-level sends funnel through the new `ActorRef::tell` entry point.
- `#118` will extend `Signal::Message` with the reply port; `self_sender` and the
  reply port are the same category of thing (channel handles in the envelope).
