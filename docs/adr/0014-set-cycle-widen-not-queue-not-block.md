# ADR-0014: Set-restart cycles coalesce by widening — not OTP's block, not a trigger queue

**Status:** accepted · card #199 (restart-set strategies, #120 slice 2b)

## Context

`OneForAll`/`RestForOne` stop and rebuild a *set* of children as one recovery
action. The teardown itself produces death notices on the supervisor's link
channel — the same channel that triggers restarts — so a deliberate sibling
stop is indistinguishable, by channel alone, from a fresh failure (the echo).
And a *real* second failure can arrive while a cycle is in flight. Three
serialization disciplines were candidates:

- **Block (OTP-faithful).** `supervisor.erl` pins the supervisor inside a
  targeted `receive` per child (`shutdown/1`) and flushes the deliberate EXITs
  (`unlink_flush/2`); nothing else is processed mid-teardown. Correct — but it
  forfeits 2a's responsive-supervisor property, and bombay cannot buy OTP's
  echo-immunity with it anyway: deaths land on the shared link channel whether
  or not the loop blocks, so absorb state is needed regardless. Blocking buys
  nothing here.
- **Queue.** Defer mid-cycle Supervised deaths until the cycle closes. Correct
  in all modeled storms, but: double churn (a queued elder trigger re-tears the
  juniors the cycle just rebuilt — 5 rebuilds where 3 suffice), a pending-
  trigger queue to carry, and the post-cycle drain silently drops queued deaths
  whose entry was rebuilt meanwhile — dropping their budget evidence.
- **Widen.** Process the mid-cycle trigger immediately by recomputing the
  subset over the active cycle. Chosen.

## Decision

Every restart subset is a **suffix** of the birth-ordered child table
(`OneForAll` = the suffix from 0), and any two suffixes are nested — so a
mid-cycle trigger can only *grow* the active cycle (younger members are already
cycling; their deaths are absorbed and cannot re-trigger). Widening is
therefore well-defined: re-flag the wider suffix (idempotent), cancel the newly
included members (CancellationToken is idempotent), recompute `awaiting`, and
**remove any armed rebuild deadline** (`DelayQueue::remove`) before arming the
new one at the new trigger's backoff.

Echo suppression is one `cycling: bool` per child entry: a cycling member's
death is absorbed (counts the teardown down), never fed to `should_restart`.
No generation counter — fresh `ActorId`s per incarnation (no ABA) plus the
nesting lemma (no overlapping cycles) plus deadline removal cover everything a
fence token would.

## Evidence

Executable discrete-event model
(`docs/superpowers/specs/2026-07-25-199-cycle-model.rs`): 499-seed adversarial
storms per discipline, all invariant-clean for queue and widen; widen ~4% less
churn overall (1965 vs 2043 rebuilds) and 3-vs-5 on the deterministic
representative case. Every supporting element is load-bearing by reproduced
counterexample: no absorb flag → cycle wedges (awaiting never drains); no
awaiting-adjust on `unsupervise` mid-cycle → cycle wedges; no deadline removal
on widen → a stale timer fires mid-teardown and rebuilds a member whose old
incarnation still runs (two live incarnations of one logical child); no
solo-retry supersession → a pre-cycle backoff deadline rebuilds one member
mid-teardown.

## Consequences

- The supervisor keeps serving its mailbox throughout teardown and backoff —
  the property that rejected the OTP block — at the cost of one flag and a
  three-state coordinator (`Idle`/`Tearing`/`Waiting`).
- A widened cycle re-arms at the *newest* trigger's backoff; bounded by fan-out
  (each widen consumes an older sibling; the chain ends at the whole set).
- `unsupervise` mid-cycle detaches the entry but cannot revoke the cancel
  already sent — the incarnation still dies; documented on the method.
- OTP parity is kept where it matters: count-once accounting, reverse-birth
  teardown / birth-order rebuild, crash-during-teardown absorbed.
