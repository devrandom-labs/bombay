# #253 — ref-count death is not restart-worthy: the `Collected` stop reason

Probe card #253 (adversarial invariant-attack, standing process from #251):
`RestartPolicy::Permanent` (restart on ANY death, including normal —
`restart.rs:87`) composed with ADR-0003 ref-count liveness (an actor nobody
holds a strong ref to self-collects with a Normal-family reason) produces
rebuild churn for an unanchored supervised child.

## Findings (reproduced from source, 2026-07-29)

The chain, verified:

1. All-senders-gone and cancel-token stop are collapsed by
   `handle_mailbox_step` into one `ActorStopReason::Normal`
   (`kind.rs` — `signal: None → Break(Normal)`; the callers `.flatten()`
   the `Option<Option<Signal>>` from `cancel.run_until_cancelled(recv())`,
   erasing which of the two happened).
2. `should_restart(Permanent, Normal) = Restart` (`restart.rs:87`).
3. `spawn_child` (`actor_ref.rs:509`) drops the strong ref by design; the
   watch installer's transient sender dies at install. An anchorless
   incarnation therefore collects with uptime ≈ 0.
4. Uptime ≈ 0 never earns `reset_after` (default 60 s), so `consecutive`
   climbs monotonically: attempts at 100/200/400/800/1600 ms (+jitter), then
   the 6th death trips `max_restarts = 5` → `GiveUp::Yes` →
   `RestartLimitExceeded` → the supervisor breaks → ADR-0019 teardown kills
   every sibling.

**Blast radius under default tuning: dropping the last strong ref to one
`Permanent` child — a non-failure event — collapses the whole supervision
subtree in ~3 s.** With an unbudgeted config (`max_restarts = max_total =
u32::MAX`) it is instead an infinite churn loop at `max_backoff` cadence
(default 30 s/incarnation, a fresh `ActorId` + mailbox + task each cycle).

Already known as a **user trap** (doc block written under #196,
`actor_ref.rs:335-348`: "An unanchored child is actively fatal … MUST have a
liveness anchor"); `tracing_capture.rs` tests carry anchor scaffolding to
dodge it. The probe converts the trap into a semantic decision.

Facts that decided the semantics:

- OTP is silent: no ref-count death exists there (`Permanent` fidelity is not
  at stake). Actor-GC literature (Pony/ORCA quiescence collection) treats
  collection of an unreachable actor as semantically invisible by
  construction — collection is not failure.
- The "Permanent keeps my named service alive" counter-argument is moot in
  this codebase: the registry (#119, ADR-0009) holds **weak** handles only, so
  a service reachable only by name ref-count-stops today under *any* policy.
  A durable service is anchored by the app holding a strong ref — already the
  documented rule.
- Restart-on-collection punishes the non-failure with maximal blast radius
  (subtree teardown), the worst possible composition of two individually
  correct invariants.

## Decision (Joel, 2026-07-29)

**Ref-count death gets its own stop reason and is `LeaveDead` under every
policy.** "Nobody can reach this actor" is collection, not failure; a
supervisor observes it, never rebuilds from it. Harden-our-design (#251): the
resolution makes supervision coherent with ADR-0003 rather than retreating
from either invariant.

## Design

### 1. New stop reason: `ActorStopReason::Collected`

- `error.rs`: new variant `Collected`, doc: every strong handle was dropped
  and the queue drained, so no message can ever arrive again — the actor is
  collected (ADR-0003 drain-then-stop). `#[error("collected: every strong ref
  was dropped")]`. Enum is exhaustive (repo rule: no `non_exhaustive`), so
  downstream matches break loudly — intended.
- `is_normal()` returns `true` for `Collected` (expected stop): links do not
  propagate it, `Transient` already leaves it dead, watcher-side asserts on
  `is_normal()` keep holding.

### 2. Split the collapsed stop in the run-loops

`handle_mailbox_step` takes the poll result as a small crate-private enum
(`Option<Option<_>>` would trip the denied `clippy::option_option`) — e.g.
`MailboxPoll::{Cancelled, Closed, Signal(Signal<A>)}`, mapped at each caller
from `cancel.run_until_cancelled(mailbox_rx.recv())`:

- `Cancelled` (cancel token fired — graceful out-of-band stop) →
  `Break(Normal)` (unchanged);
- `Closed` (mailbox closed: all strong senders gone, queue drained) →
  `Break(Collected)`;
- `Signal(signal)` → as today (`Signal::Stop` and handler-set `stop` stay
  `Normal`).

All three loops (plain / linked / supervised) route through the shared step,
so the split lands once. The supervised loop's mailbox arm passes the same
unflattened value.

### 3. Restart decision table

`should_restart`: `Collected → LeaveDead` under **every** policy (arm placed
with the lifecycle-hook carve-out, before the policy match — it is
policy-independent like `Escalate`). `Permanent`'s doc updates: "exiting is a
bug" — but *being collected is the caller's decision*, not an exit.

`handle_child_death` needs no structural change (LeaveDead path exists);
add `trace::child_collected(id)` on that path so the quiet death is
observable (the #244 concern: without a trace event this is invisible).

### 4. No churn leak via `AlreadyDead` synthesis

The watch-installer holds a strong sender until `try_send(Signal::Watch)`
runs (both first-incarnation and rebuild paths install synchronously while
that sender is live), so a child cannot collect *before* its watch edge
lands: `WatchOutcome::Closed → AlreadyDead → Restart` cannot recur as a
collection-churn loop. No change needed; the spec records the reasoning.

### 5. Card bullet — restart budget defaults (#196 "no default policy")

The #196 decision **survives**: with collection no longer restart-worthy, the
budgets guard only genuine crash loops, which is what they were tuned for.
No default-policy is introduced; recorded in the ADR.

### 6. Docs

- **ADR-0020** — "ref-count death is not restart-worthy (`Collected`)":
  context (the collision), the GC-not-failure argument, the registry-weak
  fact, consequences (unanchored `Permanent` child is now *left dead
  silently* — the flip side is stated honestly), amendment note pointing at
  ADR-0003 and the #196 restart design.
- `actor_ref.rs` `supervise` doc block (335-348) rewritten: an unanchored
  child is no longer fatal; it collects once and is left dead (`Collected`),
  the supervisor keeps running. Anchoring guidance stays (a dead-but-wanted
  child is still a bug in the app).
- README public-API section: `ActorStopReason::Collected` + one line on the
  semantics (public API changed → per-card README rule).
- `docs/testing/coverage-baseline.md` updated.

## Tests (TDD — each written failing first where the behavior flips)

One invariant per test:

1. `should_restart(policy, Collected) == LeaveDead` for all three policies
   (unit, decision table).
2. `Collected.is_normal()` is `true` (unit).
3. `all_reasons()` compile-tripwire arrays extended (forced by the compiler);
   `Transient` split table gains `Collected` in the leave-dead half;
   `permanent_restarts_on_every_reason` becomes "every reason **except**
   `Collected`".
4. **The probe reproduction, flipped to the pinned behavior** (paused-clock
   integration): `supervise(Permanent, anchorless factory)` → exactly one
   incarnation ever (factory call count == 1 after advancing time past all
   backoff deadlines), supervisor still alive, child entry retained dead.
   Fails on current main (which escalates `RestartLimitExceeded` ~3 s in).
5. Same scenario, **unbudgeted** config (`max_restarts`/`max_total` =
   `u32::MAX`): no churn — pins both budget shapes per the card.
6. Boundary pair on the split (plain actor): explicit `stop()` (cancel path)
   still reports `Normal` to its watcher; dropping the last ref reports
   `Collected`. The split must not misclassify graceful stops.
7. ADR-0003 drain-then-stop preserved: `tell` + `drop(ref)` → message is
   handled, then the death notice is `Collected`.
8. Trace: `child_collected` event emitted on the supervised leave-dead path
   (tracing_capture).

Mutants: new/changed fns get `mutants-baseline.json` entries; existing
refcount-stop tests asserting exact `Normal` are swept to `Collected`
(`is_normal()` asserts survive as-is).

## Walking-skeleton bullet (job-queue app)

The app already contains the exact hazard: a worker is anchored only by the
dispatcher's roster `Recipient<Job>`, and the factory's roster update is
best-effort (`try_send`, wart #3) — a lost update leaves that worker
anchorless, which on current main is the subtree bomb. Extension decided at
plan time: if a deterministic app-level scenario exists (e.g. dispatcher
dropping a roster entry through an existing verb), extend
`app_job_queue.rs`; otherwise defer to the wart-#3 follow-up with that
reasoning recorded on the card (the core integration tests above cover the
invariant either way).

## Out of scope

- No change to `AlreadyDead` semantics (stays abnormal, restart-worthy).
- No change to ADR-0003 liveness, `Children` table, set-cycles (#199), or
  teardown (ADR-0019).
- No restart-policy default introduced (#196 decision stands).
- No supervisor-pins-child redesign (rejected: kameo #171, ADR-0003).
