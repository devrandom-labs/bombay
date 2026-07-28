# Card #223 — timer surface: `send_after` / `send_interval` (design)

**Date:** 2026-07-28 · **Card:** [#223](https://github.com/devrandom-labs/bombay/issues/223) · **ADR:** ADR-0018 (to be recorded with this card's PR)

## Problem

bombay-core runs timers internally (restart-backoff `DelayQueue`, stop-notice
grace) but exposes no user timer surface. Every actor needing a heartbeat,
retry, or protocol deadline hand-rolls `tokio::spawn` holding a **strong**
`ActorRef` — which pins the actor against ref-count stop (ADR-0003) and
re-invents an untested primitive per call site. Same failure mode ADR-0017
closed for the ask case; this card closes it for the timed case.

## Decisions (each grounded; sources at bottom)

### D1 — Delayed send is a sender-side, target-agnostic primitive

`send_after(delay, msg)` schedules delivery to ANY `ActorRef<A>` /
`Recipient<M>`; self-send is the dominant special case, not the mechanism.
Formal grounding: Timed Rebeca's `after(t)` — a delayed send is an ordinary
message carrying a release time, placed in the receiver's bag [TR-2011,
OTA-2016]; Real-Time Maude's `dly(m, τ)` in-flight delayed message [RTM-2007].
The primitive belongs to the sender; nothing about it is self-specific.

### D2 — Timer fire rides the ordinary mailbox

Fired message = ordinary `A::Msg` menu variant through the bounded mailbox.
Unanimous across semantic treatments: Timed Rebeca (same bag, time-tagged),
Orleans (reminders delivered "just like any other message", timer callbacks
are ordinary turns), Pekko (timer envelope through normal mailbox), Erlang
(`{timeout, ...}` in the normal queue). The Isolated Turn Principle supplies
the reason: in-turn blocking waits violate liveness, so time must arrive as
turns [43Y-2016]. Known cost — a timeout stuck behind a long queue — is
EEP-76's problem, whose remedy is in-queue priority insertion, not a side
channel; that layer is card #225's territory, not this card's.

### D3 — One task per timer on tokio's shared wheel (no hand-rolled shared wheel)

Each timer = one spawned task awaiting `sleep`. tokio multiplexes all sleep
entries onto **one hierarchical timing wheel per runtime**
(`tokio/src/runtime/time/mod.rs`, tokio 1.53.1; the 1.38 sharding was
reverted in tokio PR #7226 for contention regression), so N timer tasks are N
wheel entries + N small task allocations — not N OS timers. A hand-rolled
shared `DelayQueue` service would need an owner (bombay has no
system/`ActorSystem` object; a global static task is a hidden global; a
per-actor wheel is run-loop surgery, the direction ADR-0017 already
rejected) and would not even dedupe wheel entries — `DelayQueue` runs its own
second wheel and registers one `Sleep` with the runtime. Field precedent for
task-per-timer: ractor (`time.rs` spawns per timer), coerce
(`scheduled_notify`), kameo v0.22.2 oracle (`TellRequest::send_after`).
Bench in this card records armed-timer cost numbers into ADR-0018.

### D4 — Weak target hold: an armed timer never pins (oracle deviation)

The timer task captures only `WeakActorRef<A>` / `WeakRecipient<M>`;
upgrade-or-exit at fire. ADR-0003 ref-count stop stays authoritative: an
actor whose only remaining tie is an armed timer still stops. **Deliberate
deviation from the oracle:** kameo v0.22.2's `send_after` clones a strong
`actor_ref` into the pending signal (`src/request/tell.rs:133-141`), so a
pending kameo timer pins the actor — exactly the liveness hole this card
exists to close. No surveyed crate (ractor, coerce, riker) holds weak refs;
none is refcount-driven, so only bombay pays the pinning cost.

### D5 — Cancel: explicit handle, drop = detach, sleep-phase-atomic guarantee

- `TimerHandle::cancel()`, idempotent, via `CancellationToken`. Dropping the
  handle detaches (timer fires anyway). Drop-cancels exists nowhere in the
  surveyed field (ractor `JoinHandle` detaches, actix `SpawnHandle` is
  `Copy`, coerce drop leaks the loop); ignored-return is the dominant use.
- **Guarantee (weak, structural):** cancel and fire are atomic alternatives.
  The task `select`s the token against `sleep`; cancel that lands before
  sleep expiry never delivers and reaps the task. Once the sleep completes
  the timer counts as fired: the send runs to completion un-cancellable, and
  the delivered message is ordinary mail — indistinguishable from any racing
  sender (ADR-0017 already guarantees no cross-sender ordering).
- Why not Pekko's strong guarantee ("message from the previous timer is not
  received, even if it was already enqueued"): that mechanism is a
  `TimerMsg{key, generation, owner}` envelope plus a dequeue-side
  interceptor (`TimerSchedulerImpl.scala`) — receiver-side machinery that
  only works for self-scheduling, breaks the any-target scope (D1), and
  pollutes the closed menu or forces mailbox interception surgery. Erlang
  documents the weak model explicitly (`cancel_timer` "does not tell you if
  the time-out message has arrived at its destination yet") and it is the
  only model that generalizes to arbitrary targets. Strong filtering remains
  buildable receiver-side later (named-timer layer, see Deferred).
- No `abort()`: aborting a task mid-`send` hits flume's indeterminate-cancel
  window (ADR-0008) — the message may or may not be enqueued with no way to
  know. Scoping cancellation to the sleep phase keeps that window closed by
  construction. Same shape independently arrived at by coerce
  (`scheduled_notify`: `select!(token.cancelled(), sleep → notify)`).

### D6 — Full mailbox: backpressure, deliver late, never lose

Timer task holds the upgraded strong ref only for the send duration and
awaits capacity like any sender (`tell(..).await`). A timer message is the
fact "deadline elapsed" — the fact stays true delivered late; dropping it
silently breaks every protocol-deadline recipe built on top. Consistent with
the ADR-0017 pipe fate table and the bounded-queue/demand-driven-pull
argument (Reactive Streams, cited on card #230). Rejected: drop+trace
(loses deadlines), bounded-retry (policy knobs for no clarity), priority
side-lane (EEP-76's answer lives in #225's control lane; timer fire is data,
not a control signal).

### D7 — Interval: fresh message per tick, arm-after-enqueue, self-reaping

`send_interval(period, make_msg: FnMut() -> Msg)`. Loop:
`select(token, sleep(period))` → upgrade → `tell(make_msg()).await` →
re-arm. First tick at `t = period` (no immediate tick; Orleans `dueTime`
flexibility is YAGNI until a second concrete need).

Non-overlap semantics: **next tick arms only after the prior tick's message
is enqueued** (card text). This sits between the field's poles — Pekko
fixed-rate arms regardless of consumption and can burst after stalls;
Orleans arms after *processing* (period measured from callback-Task
resolution, "prevents successive calls from overlapping"). True
processing-coupling would need an ask-shaped tick (reply channel per tick).
Enqueue-arming needs none, and the bounded mailbox caps pile-up structurally:
at most `capacity` ticks can ever be queued, and mailbox FIFO already
serializes their processing. Burst catch-up (tokio `interval` default,
ractor/actix) is explicitly not used — a stalled interval resumes at cadence,
missed ticks are not replayed.

Reaping: upgrade failure or closed-mailbox send error exits the loop — an
interval on a dead target self-cleans, no cancel required. `make_msg` panic
kills only the timer task and is traced (same containment as the ADR-0017
pipe mapper).

### D8 — Nothing fallible in the public surface

Scheduling always succeeds: the caller holds a strong ref at call time, so
there is no dead-target arm at schedule. Later failures (target died, mailbox
closed in kill race) are the spec'd drop-plus-trace fates below — no new
error types, no `Result` in the API.

## API

```rust
// crates/core/src/actor/timer.rs
impl<A: Actor> ActorRef<A> {
    pub fn send_after(&self, delay: Duration, msg: A::Msg) -> TimerHandle;
    pub fn send_interval<F>(&self, period: Duration, make_msg: F) -> TimerHandle
    where F: FnMut() -> A::Msg + Send + 'static;
}
impl<M: Msg> Recipient<M> {
    pub fn send_after(&self, delay: Duration, msg: M) -> TimerHandle;
    pub fn send_interval<F>(&self, period: Duration, make_msg: F) -> TimerHandle
    where F: FnMut() -> M + Send + 'static;
}

/// Erased, `Send + Sync`. Drop = detach. Single owner (not `Clone`) — YAGNI.
pub struct TimerHandle { /* CancellationToken */ }
impl TimerHandle {
    /// Idempotent. Wins iff it lands before the sleep expires (D5).
    pub fn cancel(&self);
}
```

`send_after` owns the message value until fire; `send_interval` calls the
factory once per tick.

## Fate table

| Event | Outcome |
|---|---|
| Fire, target alive | Ordinary menu message; FIFO from enqueue time |
| Fire, mailbox full | Await capacity; delivered late, never lost (strong ref held only for the send) |
| Fire, target dead / drain window | Upgrade fails; drop + `timer_target_dead` trace; task exits clean |
| Kill race (strongs alive, mailbox closed) | `tell` error swallowed + trace; no panic |
| `cancel()` before sleep expiry | Never delivers; task reaped; `timer_cancelled` trace |
| `cancel()` after fire | No effect; message is ordinary mail |
| Handle dropped | Timer fires anyway (detach) |
| Interval: target dies | Loop exits (self-reaping) |
| Interval: `make_msg` panics | Timer task dies; traced; actor untouched |

## Tests (TDD — failing first; paused clock; no yield-spins)

1. `send_after` fires: exact menu value arrives via mailbox (paused clock).
2. Cancel-before-fire: cancel, advance clock past deadline, assert no
   delivery AND task exit (ExitGuard oneshot pattern from `pipe.rs` tests).
3. Dead target at fire: no panic, no leak, task exits (ExitGuard).
4. Non-pinning: actor whose only tie is an armed `send_after` ref-count-stops
   when the last external strong drops (pipe test shape).
5. Cancel-then-fire race: paused clock drives cancel and expiry
   deterministically to both edges; either full delivery or none.
6. Interval non-overlap boundary: mailbox full blocks tick N's enqueue;
   assert tick N+1 not armed until N enqueued (also the D6 boundary test).
7. Interval self-reaps on target death; task exit asserted.
8. `Recipient` variants: same paths smoke-tested through erased handles.

Mutants-baseline entries for every new fn; bench `benches/`: armed-timer cost
(N=10k armed `send_after` tasks vs `DelayQueue::insert` baseline) — numbers
recorded in ADR-0018.

## Deferred (named, per card rule)

- **Receive-timeout** (idle T, reset on message): follow-up card, filed
  before this card's PR. Run-loop idle arm with its own invariants; Pekko
  separates `setReceiveTimeout` from timers for the same reason.
- **Named-key timers** (Pekko `startSingleTimer` replace/debounce +
  strong cancel filtering): not built; buildable receiver-side atop
  `TimerHandle` without unbuilding anything. Noted in ADR-0018.
- **Message deadlines** (Timed Rebeca `deadline(t)` expiry-at-dequeue): out
  of scope; noted as future direction in ADR-0018.

## Sources

- [TR-2011] Aceto et al., *Modelling and Simulation of Asynchronous
  Real-Time Systems using Timed Rebeca*, EPTCS 58, 2011. arXiv:1108.0228.
  Journal: Sci. Comput. Program. 89 (2014), DOI 10.1016/j.scico.2014.01.008.
- [OTA-2016] Sirjani, Khamespanah, *On Time Actors*, LNCS 9660, 2016,
  DOI 10.1007/978-3-319-30734-3_25.
- [43Y-2016] De Koster, Van Cutsem, De Meuter, *43 Years of Actors*,
  AGERE! 2016, DOI 10.1145/3001886.3001890 (Isolated Turn Principle; the
  paper itself says nothing about timers — its bearing is the liveness half).
- [RTM-2007] Ölveczky, Meseguer, *Semantics and pragmatics of Real-Time
  Maude*, HOSC 20(1–2), 2007, DOI 10.1007/s10990-007-9001-5 (`dly(m, τ)`).
- Orleans: MSR-TR-2014-41 §2.5/§2.7; grain-timer docs
  (learn.microsoft.com/dotnet/orleans/grains/timers-and-reminders).
- Pekko `TimerSchedulerImpl.scala` (apache/pekko, actor-typed internal) —
  generation-counter mechanism; interaction-patterns docs.
- Erlang ERTS `erlang` module docs — `start_timer`/`cancel_timer`
  semantics; EEP-76 (priority messages).
- kameo v0.22.2 `src/request/tell.rs:123-161` (oracle `send_after`).
- ractor `ractor/src/time.rs`; coerce `actor/mod.rs` `scheduled_notify`,
  `scheduler/timer.rs`; actix `actor.rs`/`utils.rs`; xtra
  `BREAKING-CHANGES.md` (0.6 removed timers); riker `system/timer.rs`.
- tokio 1.53.1 `runtime/time/mod.rs` + `wheel/mod.rs`; tokio PR #7226
  (sharding revert). tokio-util `DelayQueue` (own wheel, Copy-key ABA).
- bombay: ADR-0003 (ref-count stop), ADR-0008 (flume cancel indeterminacy),
  ADR-0017 + `pipe.rs` (weak-pipe primitive, fate-table precedent).
