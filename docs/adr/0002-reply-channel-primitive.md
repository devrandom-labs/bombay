# ADR-0002 — Reply channel primitive

**Status:** Accepted (2026-07-05) — implemented under card #115

## Context

An `ask` needs exactly one typed answer back from a handler. The reply channel
is therefore a **one-shot, single-producer/single-consumer** carrier of a single
`Result<R, E>` (reply value, or the handler's typed domain error), on the local
tier — **in-process, zero serialization**. Unlike the mailbox (ADR-0001, a
long-lived MPSC), a reply channel is created per `ask`, used once, and dropped.

### Requirements (what actually gates the choice)

1. **One value, consume-on-send** — the reply is sent at most once. Enforcing
   single-send by *moving* the sender (compile-time) beats a runtime guard.
2. **Drop-detection on both ends** — *mandatory*. The two central failures are
   "handler dropped the sender without replying" (→ the asker must get
   `AskError::Interrupted`, never hang) and "asker dropped the receiver" (→ the
   handler's `send` should report the reply went nowhere). The primitive must
   surface both, not deadlock.
3. **Async receive** — the asker awaits the reply inside the ask builder (#118),
   which layers a timeout around it.
4. **Typed payload, no `T: Default`/`Clone` wart** — carries `Result<R, E>` by
   move; the reply value cannot be required to `Default`.
5. **Already in the tree / low cost** — a per-`ask` channel is on the hot path;
   the floor is one allocation.

## Options considered

| Candidate | one-shot | drop-detect both ends | async recv | extra dep | verdict |
|---|---|---|---|---|---|
| **tokio::sync::oneshot** | ✓ purpose-built | ✓ (`RecvError`; `send` → `Err(t)`) | ✓ | none (already used) | **pick** |
| futures::channel::oneshot | ✓ | ✓ (`Canceled`; `Sender::is_canceled`) | ✓ | none (dep) | equivalent; second choice |
| flume::bounded(1) | ✗ MPMC forced to 1 | partial | ✓ | none (mailbox dep) | wrong shape — cloneable senders, no move-consume |
| async-channel (cap 1) | ✗ MPMC | partial | ✓ | new dep | wrong shape |
| Hand-rolled `Arc<Mutex<Option<_>>>` + Notify | — | manual | ✓ | none | reinvents oneshot; more unsafe surface |

`tokio::sync::oneshot` and `futures::channel::oneshot` are the only two that are
*actually* one-shot (single value, `send(self)` consumes, both-ends drop
detection). Both are already dependencies. tokio's is chosen because bombay-core
already depends on `tokio::sync`, the runtime is tokio everywhere on the server
(ADR-0001), and its `Sender::send(self) -> Result<(), T>` hands the undelivered
value back — exactly requirement 2's "asker gone → reply returned".

## Decision

**`tokio::sync::oneshot`, behind thin `ReplySender<R, E>` / `ReplyReceiver<R, E>`
wrappers.** The oneshot is an implementation detail — it never appears in the
public API — mirroring the mailbox channel-seam philosophy (ADR-0001): do not
pre-abstract; trait-ify at the *second* impl, when the M6 / `no_std` client needs
a non-tokio one-shot. Single-send is enforced by consuming `self` in
`send`/`send_err` (a second send fails to compile). `recv` maps the oneshot
outcome into #113's `AskError` (`RecvError → Interrupted`, `Ok(Err e) →
Handler(e)`, `Ok(Ok r) → Ok(r)`).

This is a lighter decision than ADR-0001: the shape (one-shot) rules out all but
two candidates, both already present, so no benchmark is warranted — the mailbox
(a hot long-lived queue) earned one; a per-`ask` one-shot on the two purpose-built
options does not.

## Consequences

- No new dependency; `bombay-core` already pulls `tokio` with the `sync` feature.
- The oneshot stays wrapped; swapping it for an Embassy / `no_std` one-shot at M6
  is a seam-local change behind `ReplySender`/`ReplyReceiver`.
- `recv` produces `AskError`, coupling the reply layer to #113's error model —
  intended: `AskError::Interrupted`/`Handler` were designed there for exactly
  this await.
- `ReplyError`-style type erasure (kameo's `Box<dyn Any>` reply path) is **not**
  reintroduced; a typed port has nothing to erase (spec: card #115).

## References

Card #115 (this primitive) · ADR-0001 (mailbox channel; the seam philosophy) ·
#113 (`AskError` the reply await maps into) · #114 (typed per-variant reply port)
· #118 (ask builder — owns the timeout + `Deliver` around `recv`) · epic #122.
