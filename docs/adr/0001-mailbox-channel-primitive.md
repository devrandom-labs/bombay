# ADR-0001 — Mailbox channel primitive

**Status:** Accepted (2026-07-04) — evidence below; implemented under card #133

## Context

The actor mailbox is an **async, bounded MPSC**: one consumer (the run-loop
`select`s over `recv`), many producers (`ActorRef` clones). Card #112 shipped a
mailbox on `tokio::sync::mpsc` **without the channel survey its own rigor
contract required** ("*decide the primitive… no commitment before the survey*").
Card #133 corrects that: evaluate the real candidates on the axes that actually
gate the mailbox, and record the decision here.

### Requirements (what actually gates the choice)

1. **Async-native `recv`** — *mandatory*. The run-loop awaits in a `select`. A
   sync channel would need an async waker bolted on, adding back the cost it
   saved — so pure-sync designs are **not viable mailboxes**.
2. **Bounded** — *mandatory*. Backpressure is a design decision (#112); we
   deleted unbounded (a memory footgun). Rules out unbounded-only designs.
3. **Light idle memory per actor** — an agent runtime (agency) may hold *many*
   actors; a preallocated ring reserves `cap × size_of::<Signal>()` **even when
   idle**, which a lazily-grown channel does not.
4. **`no_std` / executor-agnostic** — decides whether we get **one** mailbox
   across the tokio server **and** the M6 / embedded client (`no_std` + Embassy,
   where tokio cannot run), or **two behind the executor seam**.
5. **loom / shuttle testable** — would **un-defer the concurrency DST** punted
   from #112 (tokio's mpsc internals are loom-opaque to us).
6. **Maturity / maintenance.**

### Why not "an actor system without tokio"?

- **Server / desktop:** no reason to avoid tokio — it's mature, Zenoh (our
  transport) is built on it, rolling our own executor is waste. tokio is the
  right commitment there.
- **M6 lite-bombay / embedded / WASM:** a real non-tokio frontier —
  `zenoh-pico`, single-thread, `no_std` + Embassy. **Both** worlds still need
  *async*, so sync lock-free queues are out in *either* case; the live question
  is whether one **async** primitive spans both (req. 4).

### Capacity models (they differ, and it matters — req. 3)

| Model | Channels | Memory |
|---|---|---|
| Preallocated fixed ring | thingbuf, LMAX disruptor | `cap × slot` reserved up front, **even idle** |
| Bounded, lazily grown | tokio::mpsc, flume, async-channel, crossbeam | tracks occupancy — light idle |
| Unbounded | Vyukov intrusive, `unbounded()` variants | grows without limit — **fails req. 2** |

## Options considered

| Candidate | async | off-tokio | `no_std` | loom | capacity model | verdict |
|---|---|---|---|---|---|---|
| **tokio::sync::mpsc** | ✓ | ✗ | ✗ | internal only | lazy bounded | server-only |
| **flume** | ✓ | ✓ | ✗ (std) | — | lazy bounded | server + std-async |
| **async-channel** | ✓ | ✓ | ✗ (std) | — | lazy bounded | server + std-async |
| **thingbuf** | ✓ | ✓ | **✓** | **✓** | preallocated ring | **spans both worlds; idle-mem tax** |
| crossbeam-channel | ✗ (sync) | — | ✗ | — | lazy bounded | ceiling ref — not a mailbox |
| Vyukov intrusive MPSC | ✗ (sync) | — | ✓ | — | unbounded | ref — fails req. 1 & 2 |
| LMAX disruptor | ✗ (sync) | — | ✗ | — | preallocated ring | ref — fails req. 1 |

## Evidence (measured — `cargo bench --bench channels`, aarch64, 4 workers)

Throughput (higher = better); one shared tokio multi-thread runtime, `u64`
payload, `CAP = 256`, warm-up 1 s / measure 2 s:

| Channel | Uncontended (1→1) | Contended (4→1) |
|---|---|---|
| crossbeam *(sync ceiling, not a mailbox)* | 53.2 M/s | — |
| **flume** | **25.7 M/s** | **14.8 M/s** |
| async-channel | 18.1 M/s | 11.1 M/s |
| **tokio::mpsc** *(v1 default)* | 13.4 M/s | 4.9 M/s |
| thingbuf | 8.6 M/s | ~4.1 M/s |

Findings:
- **tokio::mpsc — the un-surveyed v1 default — is second-slowest, ~3× slower
  than flume under contention.** The concrete cost of skipping #112's survey.
- **flume is the throughput winner**, executor-agnostic, and **move-based** (no
  extra trait bounds on the element).
- **thingbuf is the slowest *and* requires `T: Default`** (a preallocated ring
  initialises its slots). `Signal<A>` has no sensible `Default` (a message enum
  cannot default) — so thingbuf's `no_std`/loom edge comes with a real fit
  problem against our by-value design.

Caveat: this measures *raw channel* throughput with a small payload; real actor
workloads are usually handler/IO-bound, so the absolute gap matters less than
the *ordering* and the qualitative axes.

## Decision

**flume, behind a channel seam.** It is measurably the fastest async candidate
(≈2× tokio uncontended, ≈3× contended), executor-agnostic (works on the tokio
server *and* off-tokio), move-based (no `Default`/`Clone` wart, unlike thingbuf),
lazily-bounded (light idle memory — req. 3), and actively maintained. The **seam**
(a thin trait over the channel) lets the M6 / `no_std` client swap in an Embassy
or `no_std` channel later, and hosts the deterministic impl that carries the
loom/shuttle DST deferred from #112.

`no_std` reality: none of the *std* async channels (flume/async-channel/tokio)
run on embedded; thingbuf can, but is slowest and `Default`-constrained. So the
embedded channel is a **separate, seam-local decision at M6** — not forced now.

## Consequences

- Add `flume` as a real (non-dev) workspace dependency; the mailbox wraps it.
- Introduce a **channel seam** (trait) now — it was going to be needed for the
  executor split (#9 MaybeSend) and the DST anyway; this pulls it forward.
- tokio stays the runtime (tasks, `AbortHandle`, timers); only the *mailbox
  queue* changes from `tokio::sync::mpsc` to `flume`.
- Re-run the #112 mailbox test net + mutation gate against the flume-backed impl.

## References

Card #133 (this evaluation) · #112 (the skipped survey) · epic #122 · #19
(channel-handler strategy) · the deferred loom/shuttle DST (#112 → #116/#120) ·
bench: `bombay-core/benches/channels.rs`.
