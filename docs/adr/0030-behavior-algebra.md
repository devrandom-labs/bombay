# ADR-0030: The Behavior algebra — one object, capabilities as layers

Date: 2026-08-02 · Status: accepted (algebra; encodings deferred, see Open doors) · Card: #295 · Spec: `docs/superpowers/specs/2026-08-02-behavior-algebra-design.md` · Predecessors: ADR-0026 (constraint 2), ADR-0028 (two-layer law), ADR-0029 (one verdict family — precondition)

## Context

The capability layer is three sealed hooks (`Admission`/`Replay`, `DeadlineHook`) projected onto three hand-written select loops (`PlainRun`/`LinkedRun`/`SupervisedRun`). The projections work, but the logic and the scheduling are welded: interleavings are testable only through a spawned runtime, the arm order is a comment maintained in three places, and every new event source is a new hand-written loop. ADR-0028 named this card as the successor that implements the algebra those hooks project.

Directives recorded from the design session: everything is a layer; extreme testability everywhere; third-party openness is NOT a goal — the algebra ships sealed, unsealing stays a one-line decision on the card.

## Decision

### The one object: `Behavior`

Renamed from the cards' "machine" (Joel, design session): the object is Agha's *behavior* plus its event alphabet (precision caveat: Agha's behavior is the one-communication function alone; ours bundles the sources).

```rust
trait Behavior {
    type Event;
    type Ph;      // the become-menu still exposed upward (Never when erased)
    type Error;
    async fn step(&mut self, ev: Self::Event)
        -> Result<Step<Self::Ph, ActorStopReason>, Self::Error>;
}
```

State lives in `&mut self` — the fold accumulator. Events, not calls: a layer that adds a source extends the alphabet as a sum type; the step is total over its alphabet; there is no second hook surface. Only a fully erased behavior (`Ph = Never`) is runnable.

### The transformer: a capability is a layer

The tower pair, literally: each capability is a type constructor `C<B>: Behavior where B: Behavior` (as `Timeout<S>: Service`), plus a thin assembly trait for #286's builder (tower `Layer`):

```rust
trait Capability<B: Behavior> { type Out: Behavior; fn apply(self, inner: B) -> Self::Out; }
```

Closure under composition is structural (`Out: Behavior`). Both traits are SEALED — the laws bind only the five core layers.

### The five capabilities as layers

Base = handler floor · `Stashing`/`Deadlined`/`Watching` = source-adding layers routing their own events · `Phased` = both planes (gate wraps the step; its seats add events) · `Supervising` = source-adding layer whose reactions restart CHILDREN — the outer fold over child folds, still ONE kind of thing (demonstrated by the prototype's model, not asserted).

### Laws in signatures

1. Commit-after-Ok: a layer is the caller of `inner.step(ev).await?` — commit is syntactically after the `?`.
2. One deadline arm as min-fold over source wakes (encoding deferred).
3. No-silent-drop: total classification; `Defer` without a seat stays uncompilable (ADR-0028, unchanged).
4. Priority = stack order: outer layers' events outrank inner ones; `Supervising<Watching<Deadlined<Base>>>` derives today's arm order (encoding deferred with the select work).
5. Agha floor as upper bound: anything not derivable from typed-become + merged sources is accidental structure.

### Typed-become audit — findings (prototype pass)

- The pending-goto side channel (`Ctx::goto` + `commit()` + the D3 code-order proof) is accidental structure: with the verdict carrying `Goto` (ADR-0029's family used as designed), committing a phase on `Err` is UNREPRESENTABLE — D3's test shrinks from a code-order proof to a tautology. The real-surface migration of this finding rides the implementation pass (and reshapes with #286's handler API).
- The two-queue stash (held vs released) re-derives itself in the model's `Phased` drain — it is essential structure, not an implementation artifact.
- Replay-batch atomicity is preserved BY CONSTRUCTION: the layer drains its batch inside its own `step`, so no outer event interleaves — resolving the drain-as-source question in favor of current behavior.

## Alternatives rejected

- Contribution record over a fixed spine (today's CapSet with more seats): openness per-seat, not structural; fails "everything is a layer".
- Effect commands (free-monad): alloc/`dyn` on the hot path; laws demoted from signatures to interpreter convention.
- Unsealed-first: forces every law to bind strangers' code for openness nobody needs yet.

## Open doors (each gated, none silent)

- The async merged-select encoding over an open source set: gated on #298's monomorphization-slope measurements (required pre-reading, card comment). Stays on #295.
- Unsealing `Behavior`/`Capability`/`DeadlineCx` + the out-of-crate capability proof: contingent on Joel's unseal decision; model-grade closure proof ships in this pass.
- The full oracle-over-derived-loop (#266 6-scenario, 24-point lattice): rides the implementation pass; this pass ships model-vs-real equality for plain/phased/deadline scenarios.

## Consequences

- The prototype (`crates/core/tests/behavior_algebra/`) is the executable spec and doubles as bombay-matrix's frozen reference (#298).
- No public API change in this pass; the run loop is untouched.
- The implementation pass derives the three loops from the algebra and must keep the #266-family oracles green unchanged.
