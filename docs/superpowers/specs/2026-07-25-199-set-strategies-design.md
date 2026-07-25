# Restart-set strategies — design (card #199, slice 2b of the supervision epic)

**Status:** draft 2026-07-25 · epic #122 · parent #120 · extends
[`2026-07-24-196-restart-supervision-design.md`](2026-07-24-196-restart-supervision-design.md)
(slice 2a). This is the delta spec that card's "Deferred to slice 2b" section
contracts for.

## What this is

Slice 2a rebuilds the **failed child only** (`OneForOne`) and escalates by
supervisor death. This slice adds the middle rungs of the escalation ladder —
`RestForOne` and `OneForAll` — the strategies that stop and rebuild a **set** of
children as one recovery action, plus the heterogeneous-children proof and the
DST storm burden deferred from #196.

The design was **derived, not ported**: distilled to its underlying
data-structure/coordination problem, candidate mechanisms were built into an
executable discrete-event model and storm-tested against each other; each design
element below is retained only because disabling it reproduces a concrete
failure (counterexample seeds/scenarios in the companion model,
[`2026-07-25-199-cycle-model.rs`](2026-07-25-199-cycle-model.rs)). Where the
result agrees with OTP it is noted; where it deliberately differs, the reason is
stated.

## Research grounding

Four primary-source investigations preceded the design. Findings that shaped it:

### OTP (`lib/stdlib/src/supervisor.erl`, erlang/otp master)

- **Set-teardown is fully synchronous and sequential.** On a `one_for_all` /
  `rest_for_one` restart, `restart_multiple_children/3` runs
  `terminate_children` to completion, then `start_children`. `shutdown/1`
  monitors each child, sends the exit signal, and **blocks in a targeted
  `receive` for that child's `DOWN`** — one child at a time. The supervisor
  processes nothing else during teardown.
- **Echo-immunity is a by-product of that blocking, plus `unlink_flush/2`**,
  which drains the deliberate `{'EXIT', Pid, _}` from the supervisor's own
  mailbox so `handle_info`'s EXIT clause never sees a death the supervisor
  itself caused.
- **Ordering:** terminate in reverse start order, restart in start order
  (source comments on `terminate_children` / `start_children`).
- **Accounting:** `add_restart` is called **once** per triggering death, in
  `restart/2`, before the strategy fans out — a set restart counts once, never
  once per member.
- **No spacing.** Restarts are immediate; OTP's stock supervisor has no backoff
  (2a's spacing remains a deliberate bombay departure).

### Akka (Classic + Typed, doc.akka.io + akka/akka source)

- Classic `AllForOneStrategy.processFailure` applies the directive to all
  siblings **synchronously inside the parent's handling of the `Failed` system
  message**, with self + children suspended (dispatcher stops delivering) for
  the duration.
- Akka avoids the echo **architecturally**: failure flows *up to the owner* as a
  `Failed` system message; death-watch `Terminated` is a separate channel. But
  the parent still tracks which children it is deliberately recreating
  (`ChildrenContainer.Recreation`) — i.e. Akka *also* carries expected-stop
  state; separate channels alone do not remove it.
- **Counter-evidence, recorded honestly:** Akka **Typed removed all-for-one**
  (and `Escalate`); supervision is per-actor (`Behaviors.supervise`), and set
  reactions are built explicitly by users via `watch` + bubble-up. The modern
  Akka design voted against built-in set strategies. bombay keeps them because
  (a) its failure channel already flows child→owner (OTP/Classic-shaped), and
  (b) dropping the middle rungs pushes set-coordination onto every consumer of
  the core (the nexus runner above all) — the capability does not disappear,
  it just stops being a primitive.

### Literature (Armstrong 2003; Candea & Fox HotOS'03; Candea et al. OSDI'04; Bronson et al. HotOS'21)

- **There is no stated consensus mechanism.** "Stopped and then restarted" has
  exactly one supporting sentence (Armstrong §5.2.3, AND-supervision); nothing
  in the surveyed literature compares synchronous vs concurrent teardown or
  names the deliberate-death re-entrancy hazard. This spec's coordination rules
  are therefore **derived** (and model-checked), not cited.
- Armstrong's Shutdown protocol (thesis ch. 6) forecloses the echo structurally:
  deliberate termination carries a distinct exit reason (`shutdown`) and a
  blocking per-child acknowledgment.
- Microreboot grounds the **set concept itself**: recovery groups are the
  transitive closure of dependents, "microrebooted together", and simultaneity
  exists to deny survivors a view of a member's inconsistent intermediate state
  — the actual argument for cycling a set rather than staggering members.
- Metastable-failure theory grounds the storms the DST work must reproduce:
  recovery machinery itself can be the sustaining feedback loop.

### Ecosystem (crate survey)

- **Second reference oracle:** `ractor-supervisor` 0.1.9 (MIT, active;
  `ractor` ecosystem) implements exactly OneForOne/OneForAll/RestForOne with
  meltdown counters — cross-check target alongside the kameo fork. `bastion`
  (abandoned 2022) only for a second opinion on `rest_for_one` ordering.
- **No new dependencies.** Set-teardown barriers come from what the tree
  already has (`DelayQueue`, `join_all`); loom/shuttle/turmoil/madsim all
  require leaving tokio's clock and are rejected for DST — the house pattern
  (current-thread + `start_paused` + seeded `fastrand`) already provides
  deterministic storms. State-machine crates (statig/sm) are overkill for a
  three-variant enum.

## The distilled problem

Strip the actor vocabulary and this is a classic coordination problem:

> An **order-maintained sequence** of entries, each with a fresh-per-incarnation
> identity. Asynchronous death events arrive on a multiplexed select. One event
> may trigger an **action over a computed subset**: stop the subset
> (async, grace-bounded), then rebuild its members in order. The action itself
> *causes* death events, which must not re-trigger it (the **echo**), and the
> loop must keep serving throughout (no OTP-style blocking).

The subsets per strategy:

| strategy | subset of the birth-ordered sequence |
|---|---|
| `OneForOne` | `{failed}` (2a path, unchanged) |
| `RestForOne` | the **suffix** from the failed child |
| `OneForAll` | the whole sequence — i.e. the suffix from 0 |

**The suffix-nesting lemma** (load-bearing): every restart subset is a suffix of
the birth order, and any two suffixes are **nested**. Two consequences:

1. The strategies really are the containment rungs the card claims
   (`OneForOne ⊂ RestForOne ⊂ OneForAll ⊂ supervisor-death) — microreboot's
   "progressively larger subsets" is a theorem of the design, not a slogan.
2. A second trigger arriving **mid-cycle** can only demand a subset that
   contains the active one (younger members are already cycling; their deaths
   are absorbed and can never re-trigger). Overlap therefore has a well-defined
   resolution: **widen the active cycle** — no queue, no second cycle.

## Mechanism

### Data structure: unchanged, now justified

The loop-owned `Children` table stays `SmallVec<[(ActorId, Child); 4]>`. The
discriminating operation is *suffix-from-element* (`RestForOne`), which on a
vector is a native `[from..]` slice because **birth order is index order** — on
any map it is extra structure. Small fan-out keeps the linear id scan cheaper
than hashing. #196 chose this shape for `rekey`'s in-place order preservation;
the set strategies are why that order is load-bearing.

### New state: one flag and one small coordinator

```rust
struct Child {
    // ... 2a fields unchanged ...
    /// Member of the active set-cycle: its death is expected and absorbed.
    cycling: bool,
}

/// At most one active cycle per supervisor (loop-owned, beside `Children`).
enum CycleState {
    Idle,
    /// Teardown in flight: `awaiting` = live members whose deaths are pending.
    Tearing { awaiting: u32, backoff: Duration },
    /// Torn down; the rebuild deadline is armed in `retries` under `key`.
    Waiting { key: delay_queue::Key },
}
```

No generation counter: fresh `ActorId`s per incarnation (no ABA), the nesting
lemma (no overlapping cycles), and rebuild-timer invalidation (below) together
cover everything a fence token would.

### The cycle algorithm

1. **Trigger** (Idle, Supervised death, verdict `Restart`, set strategy):
   `record_failure` **once, on the failed child only** (OTP parity: one
   recovery action per triggering death; siblings' counters untouched). On
   `GiveUp::Yes` escalate exactly as 2a. Otherwise compute the subset, flag
   every member `cycling`, supersede any member's solo backoff deadline — 2a
   discards `DelayQueue` keys for solo retries, so supersession is a
   drop-at-fire check on the `cycling` flag (model-verified), not a removal —
   **cancel live members in reverse birth order**,
   defer their hard-aborts onto `pending_aborts` (existing arm), and count
   `awaiting` = live members. `awaiting == 0` ⇒ arm the rebuild immediately.
2. **Absorb** (death of a `cycling` member): decrement `awaiting`; at zero, arm
   one rebuild deadline in the `retries` `DelayQueue` at the trigger's jittered
   backoff → `Waiting`. The death's reason is irrelevant — a member that
   crashes *while* being torn down is still just torn down (OTP's `shutdown/1`
   treats a crash-during-shutdown `DOWN` the same way). This absorption is the
   echo suppression: it keys on supervisor-local state only.
3. **Rebuild** (the deadline fires): for every `cycling` member in **birth
   order**: clear the flag; if policy is not `Never`, run the factory, `rekey`
   in place, install the watch edge (watch-after-rekey, the #196 hazard fix,
   unchanged), `record_started`. `Never` members are left dead, entries
   retained (they were stopped with the set — a set cycle has no half-alive
   survivors — but temporary children are not rebuilt; OTP parity). → `Idle`.
4. **Widen** (Supervised death mid-cycle — under `RestForOne` necessarily an
   *older* sibling; under `OneForAll` unreachable): run the trigger step again
   over the wider suffix. Flags are recomputed idempotently, already-cancelled
   members are not re-cancelled (`CancellationToken` is idempotent), `awaiting`
   is recomputed from the flags, and — **critically** — any armed rebuild
   deadline is removed (`DelayQueue::remove(key)`) before the new one is armed.
5. **Removal mid-cycle** (`unsupervise`/`stop_child` naming a cycling member):
   remove the entry as 2a does, and if the member was being awaited, decrement
   `awaiting` (checking the zero transition). The removed child was already
   cancelled; that cannot be revoked — `unsupervise` mid-cycle detaches the
   entry but the incarnation still dies. Documented on the method.

### Why widen, not queue (and not OTP's block)

Three serialization disciplines were modeled head-to-head
(`cycle_model.rs`, 499-seed adversarial storms per mode, ~60 events each):

- **Block (OTP-faithful):** correct, but forfeits the responsive-supervisor
  property 2a fought for (`supervisor_keeps_serving_messages_during_backoff`),
  and bombay *cannot* buy OTP's echo-immunity with it anyway — deaths arrive on
  the shared link channel regardless, so the absorb state is needed either way.
  Blocking buys nothing; rejected.
- **Queue (serialize-by-deferral):** correct in all storms, but produces double
  churn (elder death mid-cycle: juniors rebuilt, then immediately re-torn by the
  queued trigger — 5 rebuilds where 3 suffice), needs a pending-trigger queue,
  and its post-cycle drain silently drops queued deaths whose entry the cycle
  already rebuilt — dropping their budget evidence with them.
- **Widen (coalesce):** correct in all storms, single churn (3 rebuilds on the
  representative case; ~4% fewer total rebuilds across identical storm suites:
  1965 vs 2043), honest budget accounting (every real trigger is charged), no
  queue state. Chosen. → **ADR-0014**.

Each supporting element is load-bearing by counterexample, reproduced
deterministically in the model:

| element removed | failure reproduced |
|---|---|
| `cycling` absorb flag | `awaiting` never drains — cycle wedges forever |
| `awaiting` adjust on removal | `unsupervise` mid-cycle wedges the cycle |
| rebuild-timer removal on widen | stale deadline fires mid-teardown of the widened cycle and rebuilds a member whose old incarnation is **still running** — two live incarnations of one logical child (the half-alive set) |
| solo-retry supersession | a pre-cycle backoff deadline rebuilds one member mid-teardown |

### Strategy is a supervisor property

```rust
pub enum SupervisionStrategy {
    OneForOne,
    RestForOne,
    OneForAll,
}

pub trait Supervisor: Watch {
    /// The restart-set strategy — a property of the SUPERVISOR (the seat #196
    /// reserved). Default preserves 2a behavior for every existing caller.
    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForOne
    }
}
```

Per-child `RestartConfig` carries **no** strategy field (compile-visible; the
card's `strategy_is_supervisor_property_not_child`). Policy/tuning stay
per-child; *which siblings share a fate* is the supervisor's topology-level
decision — mixing them would let two children disagree about the set they are
in. An associated `const` was considered and rejected: a method keeps the trait
object-safe-irrelevant surface identical to 2a's marker style and needs no
`where` bounds at use sites; it is monomorphized to a constant anyway.

### Accounting and ordering rules (model-pinned)

- One `record_failure` per triggering death, on the failed child (a set cycle is
  one recovery action). Sibling counters untouched; sibling `record_started`
  re-arms their uptime clocks on rebuild.
- Teardown cancels in reverse birth order; rebuild runs in birth order (OTP
  parity; `Children`'s insertion order is birth order).
- The rebuild delay is the **trigger's** jittered backoff (attempt = the failed
  child's consecutive count). A widen re-arms at the *new* trigger's backoff.
- A cycling member's death reason is diagnostic only — never fed to
  `should_restart` (its rebuild was already decided). A member crashing in a
  lifecycle hook *during rebuild* is a fresh Supervised death of the new
  incarnation and escalates via the 2a path — unchanged.

### Escalation interplay

Unchanged from 2a: `GiveUp::Yes` on the trigger, or a lifecycle-hook death,
breaks the loop with `RestartLimitExceeded` / `ChildLifecycleFailed`; the
escalation sweep (`stop_surviving_children` → `drain_live_handles`) stops every
survivor crash-only. Members already mid-teardown are already cancelled — the
sweep's cancel/abort on them is idempotent. A cycle in flight at escalation
simply never rebuilds: the swept table is empty, pending deadlines fall on
table-misses.

## Public API delta

- `SupervisionStrategy` enum (exported).
- `Supervisor::supervision_strategy()` with `OneForOne` default.
- No other public-surface change: `supervise`/`unsupervise`/`stop_child`,
  `RestartConfig`, and the spawn entry points are untouched. README gains the
  strategy bullet + one example line.

## Invariants — TDD, each written failing first

Card checkboxes, plus the model-discovered ones (†):

1. `one_for_all_restarts_all_children` — one failure cycles the whole set;
   every non-`Never` member rebuilt with a fresh `ActorId`.
2. `one_for_all_stop_order_reverse_start_order` — cancel order is reverse
   birth, rebuild order is birth.
3. `rest_for_one_restarts_failed_plus_younger` — suffix only; older siblings
   keep ids and never observe a stop.
4. `rest_for_one_of_last_child_equals_one_for_one`† — degenerate suffix.
5. `never_children_excluded_from_set_restarts` — stopped with the set, not
   rebuilt, entry retained.
6. `set_restart_counts_once_against_budget` — trigger's counters +1; siblings 0.
7. `supervisor_signals_heterogeneous_children` (sequence) — two child types
   through the erased factory edges.
8. `strategy_is_supervisor_property_not_child` — compile-visible seat.
9. `sibling_death_during_teardown_is_absorbed`† — no policy re-entry, no
   counter mutation, cycle completes.
10. `unsupervise_during_cycle_completes_cycle_without_rebuilding_removed`† —
    the wedge counterexample as a regression test.
11. `widen_supersedes_armed_rebuild_deadline`† — elder death in the Waiting
    window: exactly one rebuild wave, no half-alive member (the I7 hazard).
12. `solo_backoff_deadline_superseded_by_cycle`† — a pre-cycle pending retry
    never rebuilds mid-teardown.
13. `supervisor_serves_messages_during_set_teardown`† — the responsiveness
    property that rejected the OTP block, asserted directly.

## DST burden (the card's heaviest boxes)

Extends `bombay-core/tests/dst_races.rs` house patterns (seeded, `start_paused`,
fail-fast bounds); no new tooling (survey verdict):

- `dst_restart_storm_deterministic` — N children failing concurrently under
  seeded `fastrand` + paused clock: identical seed ⇒ identical interleaving of
  backoff deadlines and rebuild waves; distinct seeds ⇒ the jitter actually
  varies. The #100-class storm, replayable.
- `dst_concurrent_link_unlink_die` — seeded interleavings of
  `supervise`/`unsupervise`/child-death under set strategies; asserts no
  rebuild for removed entries (the #195 `Unwatch`-race, wider window).
- `dst_backoff_distribution_measured` — seeded runs surface real restart-delay
  distributions; #196's unsourced tuning defaults confirmed or re-tuned; the
  spec's "expected to move" note resolved on the card.

The storm scenarios' expected values are cross-checked against the design model
(`2026-07-25-199-cycle-model.rs`) — sim and SUT must tell the same story for
the same schedule.

## Verification

- `cargo-mutants`: zero new survivors; baseline entries for every new fn;
  explicit `--timeout 60` (#179).
- MIRI: new tests join the two-leg lane; proptests named `prop_*` (#192).
- The model itself is committed as a design artifact, not a test lane — the
  production DST tests are the enforcement; the model is the oracle they were
  derived from.

## ADRs produced

- **ADR-0014** — set-cycle coordination: non-blocking widen/coalesce over
  OTP-blocking and queue-serialization; suffix-nesting lemma; echo absorption
  via the `cycling` flag; rebuild-timer invalidation on widen. Counterexample
  table above.

## Open questions

None blocking. Recorded: Akka Typed's removal of all-for-one is the strongest
argument against this card and is answered (channel shape + consumer burden),
not ignored; if the nexus runner later shows set strategies unused, shrinking
the surface is a one-card removal, not a redesign.
