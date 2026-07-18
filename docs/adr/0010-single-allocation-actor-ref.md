# ADR-0010 — Single-allocation `ActorRef`: one RMW per clone

**Status:** Accepted (2026-07-18) — card #186; amends ADR-0003 (the handle
layout), leaves its liveness model intact. The card's bench gates both passed,
measured same-session before/after on the M-series machine:

- **Registry same-name (parity-or-better required):** 478 → 192 µs (−59%),
  flipping a 1.79× kameo loss into a 1.33× bombay win; every registry read
  group now wins (lookup_hit 1.59×, distinct-names 5.0×, churn 1.25×). Full
  table in `benches/registry_vs_kameo.rs`.
- **Tell path (no-regress required):** improved instead — tell_pipeline −5.6%,
  tell_contended −5.3%, ask −17.9% (the loop-side 5→2 RMW win outweighs the
  one added pointer indirection). Full table in `benches/request_vs_kameo.rs`.
- **Watcher fan-out (#147):** roundtrip −13…−17% across widths; the dispatch
  arm (bare `MailboxSender`s, no `ActorRef` in the timed region) moved only
  within noise, as expected.

## Context

ADR-0003 shaped `ActorRef<A>` as four independent fields — `id` (`Copy`),
`mailbox: MailboxSender<A>` (flume sender), `cancel: CancellationToken`
(Arc-backed), `abort: AbortHandle` (Arc-backed) — reasoning that "each field is
independently cheap to clone, so no outer `Arc` is needed". The #119
`registry_vs_kameo` bench (PR #185, measured 2026-07-18, M-series) falsified
the *aggregate*: cloning one `ActorRef` touches **three separately-shared
cachelines** —

1. `WeakMailboxSender::upgrade` / `Sender::clone` — a CAS loop on flume's
   shared `sender_count` (flume 0.12 `src/lib.rs:886-905`: `fetch_update`,
   cannot blind-`fetch_add` from possibly-zero),
2. `CancellationToken::clone` — one Arc RMW,
3. `AbortHandle::clone` — one Arc RMW,

and under 4 reader threads resolving the *same* actor those three cachelines
ping-pong: bombay lost the same-name contended lookup 1.79× to kameo's plain
`Mutex<HashMap>` (419 µs vs 234 µs / 4k lookups) even though the papaya map is
lock-free. Control: with *distinct* actors per reader the identical code hit
30.4 M/s (3.2× its same-name ceiling) and beat kameo 1.60× — the map is not
the bottleneck; the handle shape is.

The clone cost lands on every fan-out surface: registry lookup (#119),
`Recipient`/`ReplyRecipient` erasure (#145/#118), watcher fan-out (#147), and
the run-loop's per-message self-ref lift (ADR-0003).

## Options considered

- **A — status quo** (three independently-shared fields). Rejected on the
  measurement above.
- **B — outer `Arc` owns liveness** (kameo's model, slimmed): queued messages
  pin via an `Arc<RefShared<A>>` in `Signal::Message` instead of a flume
  sender. Rejected: `RefShared` holds `CancellationToken`/`AbortHandle`, so
  embedding it in `Signal` drags lifecycle machinery into `mailbox.rs` — the
  pure channel seam ADR-0001/ADR-0003 (fact 5) deliberately keep free of
  actor-level types — and re-migrates every `Signal::Message` construction
  site. It also re-derives ADR-0003's rejected Design A with no additional
  win: flume's `sender_count` already provides the drain-then-stop signal for
  free.
- **C — split shape**: hot `MailboxSender` inline + one `Arc` for the cold
  pair (`cancel`, `abort`). Clone = flume CAS + 1 Arc RMW = **2** contended
  RMWs. Halves the problem instead of solving it; rejected because D reaches
  1 RMW at essentially the same complexity.
- **D — one shared allocation, flume keeps liveness** *(chosen)* — layout
  below.
- **`triomphe::Arc`** (re-examined per ADR-0003's note): still rejected —
  `triomphe` deliberately has no weak references, and the weak handle is
  load-bearing here (registry entries, the loop's self-ref, `WeakRecipient`).

## Decision

Adopt **D**:

```rust
struct RefShared<A: Actor> {          // one heap allocation per actor
    sender: MailboxSender<A>,         // THE external strong flume sender
    cancel: CancellationToken,
    abort: AbortHandle,
}
pub struct ActorRef<A: Actor>     { id: ActorId, shared: Arc<RefShared<A>> }
pub struct WeakActorRef<A: Actor> { id: ActorId, shared: Weak<RefShared<A>> }
```

- **Clone = `id` copy + 1 `Arc` RMW** (one contended cacheline). `downgrade`
  = 1 weak RMW. `WeakActorRef::upgrade` = 1 CAS (`std::sync::Weak::upgrade`).
  `id()` stays inline on both handles — free, and on the weak handle it is the
  post-mortem tombstone.
- **Liveness authority is unchanged: flume's `sender_count`** (ADR-0003).
  Every strong `ActorRef` shares the *one* sender inside `RefShared`, so all
  external handles together contribute exactly 1 to `sender_count`; dropping
  the last `ActorRef` drops `RefShared`, which drops that sender — the
  `count 1→0` transition still wakes the parked receiver (`recv() → None`).
  Queued messages still self-pin via the strong `self_sender` clone each
  `Signal::Message` carries; `mailbox.rs` and the envelope are untouched.
- **The loop's self-ref lift becomes upgrade-or-mint.** The run-loop still
  holds only a weak self-ref. Per message it first tries
  `weak.upgrade()` — 1 CAS, zero alloc, the steady-state path while any
  external strong ref lives — and only in the **drain window** (external refs
  gone, backlog still pinned by queued `self_sender`s) mints a fresh
  `ActorRef` from the dequeued sender plus the loop's own `cancel`/`abort`
  copies (one small allocation per drained message, bounded by queue depth,
  cold by construction; a handler that stashes such a ref keeps the actor
  alive exactly as before, via the sender inside it). `with_sender` is
  subsumed by the plain constructor; `WeakActorRef` sheds its `cancel`/
  `abort` fields (they existed only for that reassembly).

### The one semantic change

`WeakActorRef::upgrade` now answers *"does an external strong handle still
exist?"* (Arc strong count) instead of *"is flume's `sender_count` non-zero?"*.
The two diverge only in the drain window: previously a weak upgrade there
could hand out a strong sender and extend a dying actor's life; now it returns
`None`. Downstream: a draining actor reads as dead to the registry
(`ErasedEntry::is_alive` short-circuits on the failed upgrade), so its name is
reclaimable from the moment the last external ref drops rather than after the
backlog drains. This is the *intended* reading of "dead reads as absent" — an
actor no external handle can reach is dying, and nothing should be able to
resurrect it (the channel-resurrection anti-pattern ADR-0003 fact 2 already
rules out at the flume layer).

## Consequences

- `size_of::<ActorRef<A>>()` drops 32 → 16 bytes (id + one pointer); a clone
  allocates nothing (pinned by an alloc-exact test, #151 seam); one extra
  16-byte-header allocation per *actor* (`RefShared`).
- One pointer indirection on the `tell`/`ask` hot path (`&self.shared.sender`)
  — priced by `request_vs_kameo` (must not regress the 1.6–1.8× win);
  `tell`/`ask`/`mailbox_sender` lose `const` (no const `Arc` deref).
- Per-message loop cost drops from 5 contended RMWs (2 in `with_sender` + 3 in
  the handler-arg clone) to 2 (1 upgrade CAS + 1 Arc RMW), steady-state.
- Registry lookup pays 1 CAS + 1 `is_closed` load per hit instead of 1 CAS
  loop + 2 Arc RMWs — the #186 bench target.
- The full #117 verification matrix must be re-run (MIRI sweep + many-seeds,
  mutants baseline regenerated, alloc-exact) — the ref-model's memory shape
  changed.
- ADR-0003's *"no outer Arc"* consequence is superseded by this record; its
  liveness model (Design E, sender-in-envelope, drain-then-stop) stands.
