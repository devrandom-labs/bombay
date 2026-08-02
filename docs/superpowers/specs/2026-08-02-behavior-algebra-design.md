# Behavior algebra — one object, capabilities as layers (card #295)

**Decision date:** 2026-08-02 · **ADR:** 0030 (this pass, algebra only) ·
**Predecessors:** ADR-0026 (constraint 2's openness obligation),
ADR-0028 (two-layer law, names this card), ADR-0029 (one verdict family —
precondition, shipped)

## Problem

The capability layer is three sealed hooks (`Admission`/`Replay` on the
message plane, `DeadlineHook` on the event plane) projected onto three
hand-written `tokio::select!` loops (`PlainRun`/`LinkedRun`/`SupervisedRun`,
picked by `SelectRunner`). The projections work, but:

- **Testability is external-only.** The loop is one 111KB async file;
  asserting an interleaving ("deadline fires after link-death but before
  the backlog") requires a spawned actor, a real runtime, a paused clock,
  and luck. The logic and the scheduling are welded together.
- **The arm order is a comment, not a theorem.** Link > deadline > mailbox
  is hand-maintained in three places ("no existing inter-arm relation
  changes").
- **Every new event source is a new hand-written loop.** A capability
  needing a fourth arm (M3 liveliness supervision is exactly this) means
  editing `kind.rs`, not adding a brick.

Joel's directive for this card, recorded verbatim in spirit: **everything
is a layer; extreme testability everywhere; better code, not just working
code.** Third-party openness is NOT a goal of this pass — the algebra
stays sealed like today's hooks; unsealing is a one-line decision left on
the card.

## Decision

### The one object: `Behavior`

Renamed from the cards' "machine algebra" phrasing (Joel, this session):
the object is Agha's *behavior* — receive one communication, designate the
replacement — plus its event alphabet. Precision caveat carried in the
ADR: our `Behavior` bundles the sources, so it is slightly more than
Agha's one-communication function.

```rust
trait Behavior {
    type Event;
    type Error;
    async fn step(&mut self, ev: Self::Event) -> Result<Step<Never, ActorStopReason>, Self::Error>;
}
```

- `&mut self` IS the fold accumulator — state lives in the behavior,
  exactly as actor state lives in the actor.
- The behavior-level verdict is the watch corner of the one family
  (ADR-0029): continue or stop-with-reason, `Goto` unconstructible.
  Richer verdicts exist inside the stack (a `Phased` layer's inner step
  speaks `Step<P::Phase>`) and are erased at each layer boundary — a
  corner-to-corner identity, not an adapter (what #297 bought).
- Events, not calls: a layer that adds a source extends the event
  alphabet as a sum (`Deadline | Inner(E)`). The step is total over its
  alphabet; there is no second hook surface.

### The transformer: a capability is a layer

The tower pair, literally: each capability is a type constructor
`C<B>: Behavior where B: Behavior` (as `Timeout<S>: Service`), plus a
thin assembly trait (tower `Layer`) for #286's builder:

```rust
trait Capability<B: Behavior> {
    type Out: Behavior;
    fn apply(self, inner: B) -> Self::Out;
}
```

Closure under composition is structural (`Out: Behavior`). Both traits
are **sealed this pass**.

### The five capabilities mapped

| capability | today's projection | as a layer |
|---|---|---|
| base (plain actor) | `PlainRun` loop | events = `Msg \| Stop`; step = user `handle` |
| `Stashing` | `Replay` hook + in-step drain | adds replay events; batch atomicity preserved (below) |
| `Deadlined` | `DeadlineHook` | adds `Deadline` event; routes it to the policy, forwards the rest |
| `Phased` | `Admission` + `Replay` + `DeadlineHook` | wraps step (gate → deliver/defer/absorb; commit-after-`Ok`); its seats add their events |
| `Watching` | `LinkReact` + `LinkedRun` | adds `LinkDied` event; routes to `WatchPolicy` |
| `Supervising` | `SupervisedReact` + 2 extra arms | adds retry-queue/pending-abort events; reactions restart *children* (outer fold over child folds) — stays ONE kind of thing, demonstrated by the prototype, not asserted |

### Laws in signatures, not prose

1. **Commit-after-Ok** — a wrapping layer is the caller of
   `inner.step(ev).await?`; its commit line is syntactically after the
   `?`. An `Err` cannot reach it.
2. **One deadline arm as min-fold** — a time-armed source exposes its
   wake instant; the runner folds `min` into ONE timer arm. Law stated in
   ADR-0030; encoding rides the deferred select work.
3. **No-silent-drop** — total classification (`Deliver | Defer |
   Absorb(flow)`); `Defer` without a declared seat stays uncompilable
   (ADR-0028 `Disposition<D = Never>`, unchanged).
4. **Priority = stack order** — outer layers' events outrank inner ones;
   `Supervising<Watching<Deadlined<Base>>>` derives today's arm order.
5. **Agha floor as upper bound** — each capability must be derivable from
   typed-become + merged sources; anything that isn't gets named in the
   ADR as accidental structure.
6. **Composition laws survive sealing** — `Supervising ⇒ Watching`,
   `Phased ⊥ Stashing/Deadlined` remain trait-bound-enforced exactly as
   post-#290; sealing means they only need to bind our five layers.

### Replay batch atomicity: preserved by construction

Today `Shell::handle` drains the replay queue inside one select arm — no
deadline or death can interleave mid-batch. Resolution, default-conservative:
the `Stashing`/`Phased` layer drains its batch **inside its own step**, so
atomicity is structural and today's traces are reproduced exactly. The
prototype's trace-equality suite is the check; if it surfaces any other
divergence of this class, the divergence is recorded in ADR-0030 and
resolved in favor of current behavior.

## The executable prototype (essence-fold)

A synchronous ~50-line fold, committed as a bombay integration-test module
(public API only, so #298 lifts it verbatim as `bombay-matrix`'s frozen
reference):

```rust
fn run<B: Behavior>(b: &mut B, events: impl IntoIterator<Item = B::Event>) -> ActorStopReason
```

The model's `Behavior` is the SYNC projection of the trait above (`fn
step`, no `async`) — the prototype tests the algebra's composition and
semantics, not its scheduling; the async spelling returns with the real
select work. Interleaving is abstracted as the input trace — the trace generator plays
scheduler. Model layers for all five capabilities. The falsification list
(each item a test that can fail):

1. **Expressibility** — all five capabilities as layers over the one
   `step`; no capability needs a second hook surface.
2. **Trace equality** — fold vs the CURRENT `Shell`/loop machinery on a
   #266-style scenario script (messages, gotos, deferrals, deadline
   fires, link deaths, stop reasons), observations compared exactly.
3. **New-brick proof (model grade)** — a rate-limit admission layer
   written outside the model module against its public items, zero model
   changes. Proves the closure property for US; the out-of-crate proof
   stays on the card, contingent on the unseal decision.
4. **Typed-become audit** — the Agha-floor check as an assertion.

## Scope: ships now / stays open

**This pass (one PR):** ADR-0030 (algebra only, sealed-first) + the
prototype module + its tests. Card #295 stays OPEN.

- Bullet 1 (ADR) — ships, with the open-select ENCODING an explicit open
  door pending #298's monomorphization-slope data (required pre-reading,
  Joel's card comment).
- Bullet 2 (open source-set select) — deferred on-card, gated on #298.
- Bullet 3 (unseal `DeadlineCx`) — deferred AND now optional: sealed-first
  is Joel's recorded decision this session.
- Bullet 4 (out-of-crate proof) — deferred with bullet 3; model-grade
  proof ships now.
- Bullet 5 (oracles as theorems) — prototype-grade trace equality ships
  now; the full oracle-over-derived-loop rides the implementation pass.

No public API change this pass → no job-queue app extension (stated on
the PR per the walking-skeleton rule).

## Testing strategy (the point of the whole card)

- **Per-brick unit tests, no runtime**: each model layer tested alone —
  events in, verdicts out; microseconds, deterministic.
- **Whole-actor tests, still no runtime**: the fold takes ANY
  interleaving as a list — death-mid-replay, deadline-then-message —
  table-driven.
- **Laws as property tests**: priority-order, commit-after-Ok,
  min-fold — proptest over generated traces and stacks (boundaries
  included per house rules).
- **Trace equality vs the live machinery**: the one place tokio appears;
  #266 oracle discipline.

## Alternatives rejected

- **B — contribution record over a fixed spine**: today's `CapSet`/derive
  with more seats; openness per-seat, not structural; fails "everything
  is a layer".
- **C — effect commands (free-monad shape)**: step returns commands an
  interpreter runs; pays alloc/`dyn` on the hot path and demotes laws
  from signatures to interpreter convention.
- **Unsealed-first**: forces every composition law to bind strangers'
  code now, for openness nobody needs yet. Sealed-first keeps the whole
  lego/testability payoff and defers that bill, possibly forever.

## Risks, stated

- **Monomorphization slope unknown** — every stack is its own type; #298
  measures compile time / binary size across the 24-point lattice BEFORE
  the select encoding is designed.
- **Type-error legibility** — nested wrappers degrade diagnostics;
  iterate until fixed (Joel), with the builder card #286 as the main
  ergonomic front door.
- **Silent semantic drift** — the class §"replay batch atomicity"
  exemplifies; defense is the trace-equality suite, and any oracle gap is
  a bug class we already police (#149 lesson: green lanes over the wrong
  surface).
