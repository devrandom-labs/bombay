# ADR-0008 — `SendTimeout(M)`: guaranteed handback via bounded retry, not cancelled park

**Status:** Accepted (2026-07-18) — implemented under card #118 (variant deferred from #113)

## Context

`tell(msg).timeout(d)` needs a contract for the deadline firing. The obvious
implementation — `tokio::time::timeout` around the channel's send future,
dropped at the deadline — cannot say whether the message was delivered:

- flume's `SendFut` moves the item into a shared wait-hook immediately; the
  receiver claims it via `fire_recv`/`try_take` under the channel lock.
- Cancelling the future (`Drop` → `reset_hook`, `flume-0.12.0/src/async.rs`)
  removes the hook from the wait-queue **without reporting** whether the
  receiver had already taken the item. There is no recovery API (`SendFut`
  exposes no `into_item`).
- So "timed out" would mean *maybe delivered*: handing back a clone and
  retrying could double-deliver into a single-writer actor — a correctness
  violation, not an ergonomics wart.

tokio's `mpsc::send_timeout` avoids this by reserving a permit *before* moving
the value, which is why it can return `SendTimeoutError(T)` with a hard
not-delivered guarantee. Bombay's mailbox is flume (ADR-0001, measured 2–3×
throughput); that choice forecloses the permit design.

flume's own `send_timeout` has the right contract but is the blocking-thread
API — unusable on an async runtime.

## Options considered

1. **Cancelled park (`timeout(SendFut)`), variant `SendTimeout` without `M`,
   not retryable** — keeps real queue position (FIFO wakeup fairness), but the
   caller learns nothing actionable: delivery indeterminate, no message back,
   retry unsafe. Mirrors `AskError::Timeout`, yet unlike an ask there is no
   reply to observe later — the caller is permanently uncertain.
2. **Cancelled park + cloned handback (`M: Clone`)** — rejected outright: the
   clone can duplicate an already-delivered message on retry.
3. **Deadline-bounded `try_send` retry loop (chosen)** — the sender owns the
   message for the entire wait; `SendTimeout(M)` is a hard "never delivered",
   retryable, exact message returned, no `Clone` bound.

## Decision

Option 3. The wait is a `try_send` loop with exponential backoff (100 µs
doubling to a 10 ms ceiling, capped at the deadline; constants in
`bombay-core/src/request.rs`). A zero deadline still makes exactly one
delivery attempt. The typestate removes `try_send` from the timed request —
a timeout on an instantaneous send is meaningless, so it does not compile.

## Consequences

- `TellError::SendTimeout(M)` classifies **retryable** and `TellError::msg`
  stays total — every delivery failure hands the exact message back.
- **No queue position.** Parked untimed senders sit in flume's wait-queue and
  win freed slots ahead of the polling timed sender. Under sustained
  saturation timed tells bias toward timing out — acceptable: a
  deadline-bearing caller has declared it would rather fail than wait, and
  saturation-outlasting-the-deadline is precisely the signal.
- Worst-case discovery latency for a freed slot is one backoff period (≤10 ms)
  rather than an immediate wakeup.
- One boxed `Sleep` per *timed* tell (the timer needs a pinned home — the same
  price tokio's `Interval` pays). The un-timed `tell().await` path stays
  allocation-free.
- If flume ever grows item recovery on `SendFut` (or a reserve/permit API),
  revisit option 1's fairness with option 3's contract and supersede this ADR.
