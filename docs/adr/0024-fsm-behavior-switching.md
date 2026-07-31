# ADR-0024: Behavior switching — ship `FsmActor`/`Fsm<S>` with declarative admission

Date: 2026-07-31 · Status: accepted (design; build is a follow-up card) · Card: #231
· Amended 2026-07-31 by [ADR-0025](0025-framework-event-plane-deadlines.md): D7's
epoch-stamped delivery is superseded by the loop-owned declarative deadline
plane (staleness becomes unrepresentable; no epoch machinery, no `Signal`
variant, no timer task).
· Amended 2026-07-31 by [ADR-0026](0026-core-distillation-one-trait-caps.md):
the SURFACE moves — `FsmActor`/`Fsm<S>` becomes the `Phased<P: PhasePolicy>`
capability, and D8's "`StashActor` stays" is superseded by the staged
migration to `Stashing` (both under ADR-0026's semantics-preservation
mapping; every D1–D10 semantic stands).

## Context

Actors with operational phases (the nexus aggregate runner:
Loading → Ready → Draining, per the integration contract `bombay-nexus#4`)
express state fine with `match self.phase`, but the framework cannot observe
transitions — so per-transition bookkeeping (release the stash, cancel/arm
per-state deadlines, guard stale timeouts) is manual, per-edge, and fails
silently when a site is missed. Card #231 decides: ship an FSM helper
(`become`, Agha's third primitive) or document the idiom.

Constraints from the card: the closed menu stays (behavior switching changes
handling, never the message set; `Recipient<M>` handles stay valid across
states); zero allocation on the transition path; the `&mut self` poisoning
model (#116) holds unchanged.

Full design + spike record:
[`docs/superpowers/specs/2026-07-31-231-become-fsm-design.md`](../superpowers/specs/2026-07-31-231-become-fsm-design.md).

## Verified facts

- **Both-ways spike, not prose** (the #199 lesson): the lifecycle actor was
  built twice — `StashActor` + manual bookkeeping vs a mock
  `FsmActor`/`Fsm<S>` — and driven through a 6-scenario mode-blind
  equivalence oracle (#266 pattern). Observable behavior identical.
- **The idiom carries 7 manual transition obligations** (enumerated in the
  spec). Mutation runs on 4 of them: 3 caught by the oracle (forgotten
  unstash → 2 scenarios fail; forgotten release on the second exit edge →
  1 fails — a bug actually committed in the idiom's first draft; naive
  stale-deadline arm → spurious stop while Ready) and 1 — timer cancel on
  the second exit edge — **escapes the oracle entirely**.
- **The helper concentrates the residual risk into 1 declaration site**
  (a forgotten `Defer` declaration), which the same oracle catches in 3
  scenarios at once.
- **Transition cost is zero**: gross-alloc delta (CountingAlloc, #207
  pattern) — `Goto` = `Stay` = 2 (both are the test probe channel).
  Arming a state timeout measured 3 **on the mock's public-envelope
  `send_after` path** (per state entry, never per message); the shipped
  (non-envelope) mechanism's arm cost is a build-card measurement.
- **User LOC**: 124 (helper) vs 106 (idiom) — a small ceremony premium (the
  gate fn + hook overrides), not a reduction; the win is the failure-class
  removal, not brevity.
- **Transition semantics have two independent production anchors**
  (primary-source-verified): gen_statem — postponed events retried and state
  timeouts cancelled **only on state change**; `next_state` to the same
  state does neither — and P (PLDI 2013; AWS S3/EBS/DynamoDB/EC2 per
  Brooker & Desai, CACM 2025), whose declarative per-state `defer`/`ignore`
  skips events in the queue "until the machine transitions to a
  non-deferred state".

## Research grounding

Agha (MIT Press 1986; CACM 1990): `become` is one of the actor model's three
primitives — a plain actor is the one-state degenerate case — and
next-behavior designation enables pipelining. De Koster et al. (AGERE! 2016):
aggregating state change into one transition point is a taxonomy-level
granularity axis that shrinks the reasoning space. Where the two production
traditions disagree — gen_statem's *imperative* `postpone` vs P's
*declarative* `defer` — the spike decided for P: imperative deferral is
itself a per-arm forgettable step the wrapper cannot absorb; declarative
admission removes it. Rejected alternatives (cited in the spec): active-object
`await` guards (De Boer et al., CSUR 50(5) 2017 — cooperative interleaving
breaks run-to-completion `&mut self` + poisoning), mailbox types (Fowler et
al., ICFP 2023 — typestate fights the closed menu; related work), statechart
hierarchy (Harel 1987 — YAGNI at one consumer), Pekko behavior-return
(allocates code per transition; our transition is data).

## Options considered

**A. Idiom-only** — document `match self.state` + manual bookkeeping. Zero
new API, but the 7 manual transition obligations remain the user's, one
of them invisible even to a deliberately adversarial test suite; the
rehydration deadline must be a public menu variant (slot cost, stale-guard
arms). Rejected: the card's own failure class ("what if users forget",
ADR-0022 precedent) survives intact.

**B. Wrapper with imperative postpone (gen_statem-shaped)** — value-return
`Step`, automatic unstash/timeout on transition, but deferral stays a manual
`stash.stash()` call per arm. Strictly dominated by C: same cost, one more
forgettable class.

**C. Wrapper with declarative admission (P-shaped)** *(chosen)* — B plus
`fn gate(&State, &Msg) -> Disposition { Deliver | Defer | Ignore }`:
admission declared once, enforced by the wrapper; `handle` never sees a
message its state declared away (sole qualified exception: at stash
capacity the `on_defer_full` handback — consumer-controlled — delivers by
default). `Disposition` is a domain enum, not a bool
(bool-blindness; and it recovers P's `ignore` as declared intent). Manual
stash stays as the escape hatch.

**D. gen_statem-style derive** — per-state handler blocks via macro.
Deferred, not rejected: a derive can only generate what C's trait expresses;
it is syntax for #243, not a semantic candidate.

## Decision

Ship **C**: `FsmActor` + `Fsm<S>`, a framework-owned composition wrapper
(the `Stashed<S>` pattern — `Fsm<S>` implements `Actor`, every verb works
unchanged). Load-bearing points (full surface in the spec):

- `enum Step<St> { Stay, Goto(St), Stop }` — `Flow` (ADR-0023) + one
  variant, `Copy`, zero-box; the state switch commits only after `Ok`, so
  poisoning holds by construction. `Goto(current)` ≡ `Stay`.
- State NAME/DATA split (gen_statem): `type State: Copy + PartialEq +
  Send + 'static` tag enum held by the wrapper (the `Copy` bound makes
  `Step<State>` unconditionally `Copy`); data stays in `self`.
- On state *change* only: epoch bump → cancel state timeout → switch →
  release stash → arm new timeout → replay re-gated **in the new state**,
  ahead of the mailbox backlog (ADR-0022 snapshot semantics).
- State timeouts are declared (`state_timeout`), epoch-stamped, and
  delivered to an `on_state_timeout` hook; stale firings are dropped by the
  wrapper — unrepresentable in user code. **`Fsm<S>::Msg == S::Msg`** is a
  hard constraint (no public envelope; #114 tripwires and `Recipient`
  minting untouched); the internal delivery mechanism is the build card's
  first decision, under a recorded constraint: ADR-0021's control lane is
  deliberately non-generic and consumed by the run-loop (never routed to
  `Actor::handle`), so a control-lane timeout is a new lane/routing shape,
  not ADR-0021 as-is — an epoch-only event needs no `A::Msg` payload, so
  non-genericity is satisfiable; the routing into the wrapper is the open
  question.
- Defer overflow routes the intact message to `on_defer_full` (handback,
  ADR-0022 precedent) — silent drop stays unrepresentable.
- The two handler-plane hooks (`on_state_timeout`, `on_defer_full`) take
  `&mut Stash` like `handle` — a deliberate, recorded extension of
  ADR-0022's "stash access is `handle`-only" consequence (which was scoped
  to the terminal hooks and anticipated this follow-up). `on_stop`/`on_panic`
  stay stash-less.
- The shape lattice `Actor ⊂ StashActor ⊂ FsmActor` is realized by
  composition; `StashActor` stays (harden-never-migrate; the
  gen_server-beside-gen_statem precedent). No base-trait inversion.

## Consequences

- **Build is NOT this card**: a follow-up build card implements the surface,
  ports the spike's oracle + falsifiability mutations as in-repo tests, adds
  mutants-baseline entries, and extends the job-queue walking skeleton with a
  phased worker. First build decision: D7 timeout plumbing (control-lane per
  ADR-0021 vs run-loop).
- #241 (receive-timeout) is orthogonal by construction — gen_statem's
  *event* timeout (reset on any message) vs this card's *state* timeout
  (cancelled by transition); they must not merge.
- #243 (derive) gains a fixed semantic target; any macro is sugar over
  `FsmActor`.
- Accepted wart: Rust exhaustiveness cannot see the gate, so gated-away
  `(state, msg)` pairs still need a catch-all `handle` arm
  (`_ => Ok(Step::Stay)`, ~2 lines); a future derive could absorb it.
- README/public-API docs change only when the build card ships (no API
  change from this card).
