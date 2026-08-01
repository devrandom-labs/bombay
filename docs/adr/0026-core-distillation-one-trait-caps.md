# ADR-0026: Core distillation — one `Actor` trait, capabilities as plugged types

Date: 2026-07-31 · Status: accepted (design; migration is staged follow-up
cards) · Card: #277 · Amends the *surface* (never the semantics) of
ADR-0022/0024/0025; gates #274.

## Context

The core grew by accretion: every capability added either a trait tier
(`Watch` ⊂ `Supervisor`), a parallel actor-shape trait + wrapper
(`StashActor`/`Stashed`, planned `FsmActor`/`Fsm`), free verbs, or config —
four extension paradigms. Joel's direction (2026-07-31): deep holistic
distillation — **extreme flexibility AND extreme compile-time safety**,
fewer functions, less user cognitive load, no god-object collapse.

## Verified facts (audit, 2026-07-31; full data in the #277 spec)

- **Surface baseline**: ≈80 named public items, ≈175–180 user-touchable
  entries, 10 traits, 18 trait methods (28 with the planned `FsmActor`),
  29 error variants. Obligations: plain actor 3 trait impls; supervised
  pair **8 impls counting derives** (Dispatcher 5 incl. **two empty
  marker impls**, Worker 3); planned phased actor 7 required methods.
- **Hook-family duplication ×4**: `on_start`/`handle`/`on_panic`/`on_stop`
  declared in `Actor`, `StashActor`, planned `FsmActor`, plus `Stashed`'s
  forwarding impl — drifting only by the wrapper type leaking into
  `actor_ref` and appended params (`handle` arity 3→4→5).
- **The flexibility hole — the decisive fact**: the paradigms do not
  compose. There is **no `impl Watch for Stashed<S>`** (verified,
  `stash.rs`): a deferring actor cannot itself watch or supervise; the
  planned `Fsm<S>` has the same hole. "A supervisor that defers while
  rebuilding a child" is UNREPRESENTABLE today. The tier system is not
  flexibility; it is a set of fixed, non-composable rungs.
- **14 public start-driving entry points** over 3 real start-kinds (6
  `Spawn*` trait verbs + 8 `PreparedActor` methods incl. the three
  current-task `run*` variants); the ref-type rule
  (strong iff a message exists to mint from) is sound but enforced by
  prose across five documents.
- **Candidate spike green** (`spike-277-b`, stable Rust): ONE trait with
  `type Caps` covers plain/deferring/phased/watching with behavioral
  parity on defer-replay, phase-gating, and deadline scenarios; capability
  misuse is a **compile error** (proven by a `compile_fail` doctest);
  user-state vs framework-state borrows split trivially (disjoint
  fields). Metrics vs baseline: plain 3→**1** impl; deferring 3→**2** and
  the spawn-the-wrapper trap is gone; phased 3 impls/7 methods →
  **2 impls/6 methods** with the machine as one coherent unit; watching
  needs **zero** extra impls (policy chosen by name).
- **Bonus re-proof**: a one-queue model of the phase stash livelocked
  instantly — ADR-0022's two-queue snapshot rule is load-bearing and
  survives distillation untouched.

## Research grounding

Mechanism (production Rust, docs-verified): axum's `Handler` (plain fns;
blanket impls over extractor-typed params) and bevy's `SystemParam`
(`Query`/`Res`/`Local` as typed capability requests, third-party-extensible
via derive) prove params/slots-as-capabilities at scale; tower's `Service`
sets the minimal-surface bar and its poll_ready/call panic contract warns
that fewest items ≠ fewest obligations — obligations must move into types,
not into prose. Constraint: associated-type **defaults** are unstable
(rust-lang #29661), so a pure slot design taxes every actor with every
slot declaration; the chosen hybrid avoids that (one `Caps` type, `()` for
plain). Semantics corpus unchanged from ADR-0024/0025 (Agha; De Koster
AGERE! 2016; P PLDI 2013; Timed Rebeca SCP 2014).

## Options considered

**A. Status-quo-plus** — keep the lattice, tighten (required policy items,
`spawn_fsm` sugar). Rejected: leaves the ×4 hook duplication, the empty
marker impls, the 11-entry spawn surface, and above all the composition
hole — the paradigms still don't compose.

**B. One trait + `Caps` as plugged capability types** *(chosen — the
spiked hybrid)*: `Actor` keeps only identity + behavior (`Msg`, `Args`,
`Error`, `Caps`, `init`, `handle`, + defaulted `on_stop`/`on_panic`/
`name`); every capability is a type in `Caps` carrying its policy as a
plugged trait impl (strategy-as-type, required by construction); access
is compile-gated through the one `Ctx`.

**C. Pure GAT/slot surface** — every capability an associated type on
`Actor`. Rejected as a *separate* option: without associated-type
defaults (unstable) every actor declares every slot. Its intent is
subsumed by B (`Caps` IS a slot; `()` is the default the language won't
give us per-slot).

**D. Pure axum-style free-fn handlers** (no trait; handler registered at
spawn, params via tuple blanket impls). Rejected: adds the coherence
marker + arity-macro machinery for marginal gain over B, and loses the
natural home for `Args`/`Error`/lifecycle defaults. B can adopt D's
param sugar later without semantic change.

## Decision

**B**, under five hard constraints (Joel's anti-god-object law):

1. **`Ctx` is a typed window, never a god object.** Its reachable surface
   is exactly what `Caps` declares, compile-gated. **No runtime-checked
   capability accessor, ever** — no `try_get::<Cap>() -> Option<_>`. The
   moment capability access can fail at runtime, the design has failed.
2. **The capability system is OPEN — a design target with a named
   obstacle, NOT spike-verified.** The spike proved compile-gating only
   for the CLOSED encoding (exact `Caps = tuple` equality bounds). A
   naive open `Has<C>` over tuples is coherence-infeasible on stable
   (`impl<A,B> Has<A> for (A,B)` and `impl<A,B> Has<B> for (A,B)` unify
   at `(X,X)` → E0119; no specialization). The viable open encodings —
   frunk-style indexed `Has<C, Index>`, or bevy's actual model
   (derive-on-named-struct cap sets, tuples only as core-provided sugar)
   — MUST be spiked as stage 1's first gate. Openness remains the law;
   its encoding is stage 1's proof obligation.
3. **Composition rules are compile-time law.** Cap requirements ride
   bounds (`Supervising` requires `Watching`); invalid stacks do not
   compile. The empty-marker-impl ritual dies.
4. **The expert floor stays.** `PreparedActor`, mailbox primitives, and
   the run-loop seams remain public-as-today beneath the ergonomic
   surface; distillation applies to what typical users must touch, not
   what experts may touch.
5. **Capabilities are small separate units** — `Stashing`+`StashPolicy`,
   `Phased`+`PhasePolicy` (states, gate, deadlines, timeout reaction as
   ONE unit), `Watching`+`WatchPolicy`, `Supervising`+strategy. One
   *entry* trait, many plugged parts; the 10-item `FsmActor` shape is
   explicitly the rejected direction.

Surface relocations (semantics byte-identical, declaration point moves):

- ADR-0025's `next_deadline`/`on_deadline` move **off `Actor` into a
  first-class `Deadlined<DP: DeadlinePolicy>` capability** — the loop
  asks the cap set; plain actors carry no deadline items. This is a
  deliberate **narrowing** of ADR-0025's every-actor seat (its "no
  narrower seam than the trait" reasoning predates the cap machinery,
  which IS the narrower seam), not a byte-identical move: the plane's
  *semantics* (arm placement, fires-once, WeakActorRef rule,
  turn-boundary delivery) are untouched, and #241's recorded
  non-phased consumer path re-routes through `Deadlined` (a
  receive-timeout wrapper composes it; `Phased` requires/embeds it).
- ADR-0024's `FsmActor` becomes `Phased<P: PhasePolicy>` — D1–D10
  semantics preserved verbatim (Step/Disposition/gen_statem-P transition
  rules, required policy items, `&self`-tunable magnitudes); the
  hook-ref drift disappears structurally (policy signatures are fixed
  per capability, not re-declared per trait tier).
- ADR-0022's `StashActor`/`Stashed` becomes `Stashing`+`StashPolicy` —
  two-queue snapshot, bounded refusal-with-handback, D6 terminal-hook
  rules all preserved; the spawn-the-wrapper trap is gone.
- `Watch`/`Supervisor`/the three `Spawn*` traits collapse into
  `Watching`/`Supervising` caps + **one `spawn`** (+ `spawn_with`
  config variant); loop shape is selected by `Caps` at compile time
  (monomorphized — no runtime branch). `ActorRef`/`WeakActorRef` **both
  stay**: the strong/weak split carries liveness-pinning semantics
  (ADR-0003/0010/0020) and is not surface noise — what dies is its
  *re-litigation per hook signature*.

## Consequences

- **Migration is staged follow-up cards** (pre-1.0; our own unshipped
  surface — harden-never-migrate protects semantics, which are preserved
  invariant-by-invariant in the spec's mapping table): (1) core trait +
  `Caps`/`Ctx`/derive machinery + plain path; (2) `Stashing`; (3)
  `Watching`/`Supervising` + spawn collapse; (4) `Phased` on the
  ADR-0025 plane — **replaces #274 part 2**; #274's plane part (loop
  arm) proceeds under the relocated declaration; (5) delivery-error
  consolidation (the 3×-spelled TellError/AskError/PipeAskError family)
  as its own audited card.
- **#243 (derive) is re-targeted**: menu derive (merging `Msg`+
  `Mailboxed` into one declaration, keeping the #114 tripwire),
  per-state `gate` exhaustiveness, and the custom-capability derive.
- The `.plans/274-fsm-build.md` plan is revised against stage (4);
  ADR-0024/0025 receive surface-amendment notes (their semantics
  sections stand).
- Allocation profile unchanged: caps are plain monomorphized structs, no
  boxing on any hot path; #207 guards re-assert during migration.
- **Open risks, named** (each a gate on its stage): (i) stage 1 — the
  open-`Has` encoding (E0119; indexed vs struct-derive) must spike green
  before anything else builds on it — **RESOLVED, see Addendum**; (ii) stage 3 — the supervised loop
  (3-arm, DelayQueue, teardown per ADR-0019) was NOT modeled in the
  spike, and the compile-time loop-selection mechanism (type-driven
  dispatch from `Caps`) is unspecified — stage 3 designs it with the
  drain/supervision equivalence suites as its oracle; (iii) spike crates
  are preserved on the `spikes/277` branch until their dependent stage
  lands (session scratchpads are not durable evidence).

## Addendum (2026-07-31) — stage-1 gate result: struct-derive encoding

The open-encoding spike (`spike-278`, preserved on the `spikes/277`
branch) is green on stable for the **bevy-shaped encoding**: named-struct
cap sets with one derive-generated `Provide<FieldType>` impl per field,
plus `CapSet::build(&Args)`. Proof obligations, each a passing test:

- **O1 openness**: a capability defined wholly outside core (a
  rate-limiter with its own policy trait) composes with core caps on a
  user struct through the one public seam — zero core changes.
- **O2 gating**: `Ctx::cap::<C>()` is bounded on `Caps: Provide<C>` — a
  non-providing set is a compile error (`compile_fail` doctest).
- **O3 duplicates**: two fields of one cap type produce overlapping
  `Provide` impls — E0119 rejects duplicate capabilities by construction.
- **O4 strategy-as-type**: policies (`StashPolicy`, `RatePolicy`) remain
  plugged types feeding behavior from `Args`.

**Decided**: constraint 2's encoding is derive-on-named-struct;
`Provide<C>` is the open seam; frunk-style indexed `Has` is unnecessary
(tuples stay core-provided sugar for the common closed combos, exactly
as the main text allows). The E0119 hazard becomes a *feature* at O3:
the same coherence rule that killed the naive encoding now rejects
duplicate caps for free.

## Addendum 2 (2026-08-01) — stage-3 gate result: one door, sealed seams

Risk ii is resolved (spike-280, preserved in-tree under
`spikes/277/spike-280` until the stage lands): loop selection is a
derive-emitted `SelectRunner<A> { type Runner }` on the cap set — markers
`PlainRun`/`LinkedRun`/`SupervisedRun`, each `impl RunKind<A>` calling its
`PreparedActor` floor path — with the `Runner: RunKind<A>` obligation
discharged at the ONE `caps::spawn` (monomorphized; no runtime branch).
The loops re-bound onto **sealed** `LinkReact`/`SupervisedReact` seams
whose only implementor is `Shell<A>`, conditional on derive-emitted
`HasWatching<A>` (policy as associated type — no E0207) and
`HasSupervising<A>: HasWatching<A>` (the supertrait IS constraint 3).

**Deviation, decided by Joel (2026-08-01): ONE DOOR.** The migration
consequence "equivalence suites re-run unchanged" was dropped — keeping
`Watch`/`Supervisor` as a parallel public tier solely so those suites
compile unchanged is two ways of doing the same thing. The five traits
(`Watch`, `Supervisor`, `Spawn`, `SpawnLinked`, `SpawnSupervised`) are
deleted; the #266/#267 oracle suites are ported to the caps surface
**semantics-preserving** (same choreography, same trace assertions).
Constraint 4 is amended accordingly: `PreparedActor`, the mailbox
primitives, and the run-loop seams remain the public expert floor, but
the *capability tiers* above them exist only as caps.
