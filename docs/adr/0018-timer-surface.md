# ADR-0018 — Timer surface: `send_after` / `send_interval` + `TimerHandle`

**Status:** Accepted (2026-07-28) — decided under card #223 (design record:
[`docs/superpowers/specs/2026-07-28-223-timers-design.md`](../superpowers/specs/2026-07-28-223-timers-design.md))

## Context

bombay-core runs timers internally (restart-backoff `DelayQueue`, stop-notice
grace) but exposes no user timer surface. Every actor needing a heartbeat,
retry, or protocol deadline hand-rolls `tokio::spawn` holding a **strong**
`ActorRef`, which pins the actor against ref-count stop (ADR-0003) and
re-invents an untested primitive per call site. This card closes the same
failure mode ADR-0017 closed for the ask case.

## Options considered

- **A — sender-side, weak-hold task per timer** *(chosen).* Schedule delivery
  from any `ActorRef` or `Recipient`; the timer task holds a weak handle, so it
  never pins the target. Cancellation is sleep-phase-atomic via
  `CancellationToken`. The fired message travels through the ordinary bounded
  mailbox with normal backpressure.
- **B — shared `DelayQueue` service.** Rejected: a dedicated timer wheel would
  need an owner (a global static task, a per-actor wheel, or run-loop surgery),
  and the per-entry cost is not the bottleneck for the API in scope. The
  baseline numbers below show that while a `DelayQueue` insert is cheaper,
  wrapping and owning it would not remove the task-per-fire dispatch that the
  chosen design already pays for. Deferred to named-key timers if the
  scheduling surface becomes hot enough to justify the extra complexity.
- **C — drop-cancels handle.** Rejected: the surveyed field (ractor, coerce)
  mostly ignores the return value of `tokio::spawn` timers; explicit cancellation
  is the common case, and tying cancellation to `Drop` would make accidental
  cancellation the default when callers discard the handle. Dropping detaches
  instead.

## Decision

1. **Public API:** `ActorRef::send_after(delay, msg)` and
   `ActorRef::send_interval(period, make_msg)`; identical verbs on
   `Recipient<M>` (where `M: Clone + Send + 'static`). All return a
   `TimerHandle`. `TimerHandle::cancel()` is idempotent and explicit; dropping
   the handle detaches.
2. **Ordinary mailbox delivery.** The fired message is a normal `A::Msg` through
   the bounded mailbox, so it competes fairly with other senders and respects
   backpressure.
3. **Weak target hold.** The timer task captures only `WeakActorRef<A>` /
   `WeakRecipient<M>`; it upgrades-or-drops at fire. This is a deliberate
   deviation from kameo v0.22.2, which clones a strong `actor_ref` into the
   pending `send_after` signal (`src/request/tell.rs:133-141`).
4. **One spawned task per timer.** tokio multiplexes all sleep entries onto one
   hierarchical timing wheel per runtime, so N timers are N wheel entries plus
   N small task allocations, not N OS timers. The measured cost is recorded in
   D3.
5. **Sleep-phase-atomic cancellation.** `select!(token.cancelled(), sleep(delay))`
   with `biased` so a cancel that lands before the sleep expires never
   delivers; once the sleep completes the send is committed. No
   `JoinHandle::abort` mid-send (ADR-0008).
6. **Backpressure, not drop.** A full mailbox at fire time makes the timer task
   await capacity; the message is delivered late, never lost. The strong ref is
   held only for the send await.
7. **Interval arm-after-enqueue.** The next tick's sleep starts only after the
   prior tick's message is enqueued. This avoids burst catch-up after stalls
   and keeps the bounded mailbox as the structural cap.
8. **Self-reaping interval.** An interval exits the loop on upgrade failure or
   a closed-mailbox send error. A panicking `make_msg` kills only the timer task
   and is traced; the actor is untouched.
9. **Nothing fallible in the public surface.** Scheduling returns `TimerHandle`,
   not `Result`. Later failures are drop-plus-trace fates.

## Consequences

- `TimerHandle` joins the public API; `tokio-util` is already a dependency, so
  no new crates are introduced.
- Three new trace events are added to `trace.rs`: `timer_cancelled`,
  `timer_fire_dropped`, and `timer_factory_panicked`.
- The non-pinning contract keeps ADR-0003 intact for scheduled messages.

### D3 — per-task-vs-shared-wheel cost

Measured on the local machine (M4 Pro, 2026-07-28) with `cargo bench -p bombay --bench timers -- --quick`:

| benchmark | time (10 000 entries) | throughput |
|---|---|---|
| `arm_send_after_10k` | **4.90 ms** (≈ 490 ns / timer) | 2.04 Melem/s |
| `arm_delay_queue_insert_10k` | **0.31 ms** (≈ 31 ns / insert) | 31.9 Melem/s |

A `DelayQueue` insert is roughly **16× faster** than arming a `send_after` timer,
because `send_after` pays for one spawned task per timer. However, a shared
`DelayQueue` service would still need to dispatch each fire to the mailbox (the
same send cost as the chosen design), plus its own wheel and ownership. The
per-task design is the honest baseline: simple, self-contained, and it matches
the existing `pipe_to_self` pattern. If the scheduling surface becomes hot, a
named-key timer layer can later introduce a shared wheel without changing the
public `TimerHandle` shape.

### D5 — cancel model

Cancel wins only if it lands before the sleep expires. The alternative, Pekko's
strong generation-filtering guarantee, would require a `TimerMsg{key,
generation, owner}` envelope and a receiver-side interceptor that only works for
self-scheduling. That would either pollute the closed menu or require mailbox
interception surgery, breaking the any-target scope of D1. The weak model is the
only one that generalizes to arbitrary targets and keeps the surface clean.

### D6 — full mailbox policy

Timer fire is the fact "deadline elapsed". Dropping it on a full mailbox would
break every protocol-deadline recipe built on top. The timer task holds the
upgraded strong ref only for the `tell(..).await` and waits for capacity like
any sender. This is consistent with the ADR-0017 pipe fate table and the
bounded-queue/demand-driven-pull argument.

### D7 — interval semantics

`send_interval(period, make_msg)` arms the next tick only after the prior tick's
message is enqueued. This sits between Pekko fixed-rate (arms regardless of
consumption, can burst after stalls) and Orleans arm-after-processing (period
measured from callback-Task resolution, which would require an ask-shaped tick).
Enqueue-arming needs no reply channel and keeps the mailbox's capacity as the
structural bound. The first tick fires at `t = period`; no immediate-tick option.

## Fate table

| Event | Outcome |
|---|---|
| Fire, target alive | Ordinary menu message; FIFO from enqueue time |
| Fire, mailbox full | Await capacity; delivered late, never lost (strong ref held only for the send) |
| Fire, target dead / drain window | Upgrade fails; drop + `timer_fire_dropped` trace; task exits clean |
| Kill race (strongs alive, mailbox closed) | `tell` error swallowed + trace; no panic |
| `cancel()` before sleep expiry | Never delivers; task reaped; `timer_cancelled` trace |
| `cancel()` after fire | No effect; message is ordinary mail |
| Handle dropped | Timer fires anyway (detach) |
| Interval: target dies | Loop exits (self-reaping) |
| Interval: `make_msg` panics | Timer task dies; traced; actor untouched |

## Deferred

- **Receive-timeout** (idle T, reset on message): follow-up card, not this PR.
  Pekko separates `setReceiveTimeout` from timers for the same reason.
- **Named-key timers** (Pekko `startSingleTimer` replace/debounce + strong cancel
  filtering): buildable receiver-side on top of `TimerHandle` without unbuilding
  anything.
- **Message deadlines** (Timed Rebeca `deadline(t)` expiry-at-dequeue): future
  direction.
