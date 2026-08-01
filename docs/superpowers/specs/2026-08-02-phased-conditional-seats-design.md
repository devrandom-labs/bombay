# Phased conditional seats + one deadline policy (card #290)

**Decision date:** 2026-08-02 · **ADR:** 0028 · **Wart:** `docs/warts/281-phased-mandatory-ceremony.md`

## Problem

`PhasePolicy` charges every machine for machinery it may never use: an
all-`Deliver` gate still writes `stash_capacity` (the Worker's ceremonial
`Capacity::new(NonZeroUsize::MIN)`) and still gets a live stash allocated;
an all-`None` deadline table would still write `on_phase_timeout`. The
required-items rule is right (#196: no silent declared/defaulted pairs);
its unconditionality is the wart. Separately, the codebase carries **two
bespoke deadline seats** (`DeadlinePolicy` for `Deadlined`, plus the
`phase_deadline`/`on_phase_timeout` pair inside `PhasePolicy`) and **two
verdict dialects** (`Flow`, `Step`) for one sentence: keep going or stop.

## Decision (shape A2, amended by the essence discussion)

Two layers, one algebra-shadow, recorded in ADR-0028:

- **Capabilities** own loop participation on two planes: the **message
  plane** (`Admission`/`Replay` — service-wrapper shaped, tower-isomorphic)
  and the **event plane** (`DeadlineHook` — select-arm shaped, no tower
  analog). Sealed, core-provided (unsealing = the follow-up "machine
  algebra" card).
- **Policies** are pure, context-generic strategy seats plugged into
  capabilities (tower `retry::Policy` precedent). A policy is welded to a
  *context shape*, never to a capability.

Conciseness is the acceptance gate: **the diff must delete more than it
adds** (ledger below). Extraction from observed repetition, no invented
abstraction: both instantiations of everything unified already exist.

## The surface

### One uninhabited type, two jobs

```rust
pub enum Never {}
```

- `Step<Never> ≅ Flow` — a plain actor is a one-phase machine; `Goto` is
  unconstructible. Adapters convert at the capability boundary; handler
  `Flow` (ADR-0023) is untouched.
- `Disposition<Never>` — a gate that cannot defer. `Disposition<D = Never>`
  keeps existing non-deferring signatures literally unchanged
  (`-> caps::Disposition`); the `Defer` variant becomes `Defer(D)`.

### One deadline policy, context-generic

```rust
pub trait DeadlineCx { /* SEALED until the unseal card */
    type Actor: Actor;
    type Phase: Copy + PartialEq + Send + 'static;
    type View<'a>;   // what next_deadline may read
    type Fire<'a>;   // what the reaction receives
}
pub struct ByState<A>;   // Actor=A,        Phase=Never,    View=&'a A,            Fire=()
pub struct ByPhase<P>;   // Actor=P::Actor, Phase=P::Phase, View=PhaseView<P>,     Fire=PhaseFire<'a,P>

pub trait DeadlinePolicy<Cx: DeadlineCx>: Send + 'static {
    fn build(args: &ArgsOf<Cx>) -> Self;
    fn next_deadline(&self, view: Cx::View<'_>) -> Option<Instant>;   // pure of (self, view)
    async fn on_deadline(&self, actor: &mut Cx::Actor, fire: Cx::Fire<'_>,
        actor_ref: WeakActorRef<Shell<Cx::Actor>>) -> Result<Step<Cx::Phase>, ErrorOf<Cx>>;
}
```

- `PhaseView<P>`: `{ phase, entered_at }` (Copy — no borrow). The policy
  anchors (`entered_at.checked_add(d)`); overflow = beyond representable
  time = no deadline (unchanged).
- `PhaseFire<'a, P>`: `{ phase, stash: &'a mut StashOf<P> }`.
- **Silent-pair law, structural:** the pair lives in ONE trait — you
  cannot implement `next_deadline` without `on_deadline`. Plugging
  `NoTimeout` (below) is an explicit, named choice (#196 `OneForOne`
  precedent: named strategy ≠ silent default).
- ADR-0025 plane semantics unchanged: the loop re-reads the slot every
  iteration; fires-once; turn-boundary delivery; `WeakActorRef`;
  `PanicReason::OnDeadline`. A left phase's timeout stays unrepresentable
  (view changes with the phase; slot re-read).
- Old `DeadlinePolicy<A>` (2 static fns, `Flow`) migrates to
  `DeadlinePolicy<ByState<A>>`; `Deadlined<DP>` gains a `policy: DP` field
  and `build(args)` (was `PhantomData` + `new()`); its `DeadlineHook`
  adapts `Step<Never> → Flow` (the `Goto` arm is `match never {}`).

### PhasePolicy: core + two declared seats

```rust
pub trait PhasePolicy: Send + Sized + 'static {
    type Actor: Actor;
    type Phase: Copy + PartialEq + Send + 'static;
    type Deferral: DeferSeat<Self>;              // NoDefer | Bounded<SP: StashPolicy>
    type Timeout:  DeadlinePolicy<ByPhase<Self>>; // NoTimeout | a seat (Self is legal)
    fn build(args: &Args) -> Self;
    fn initial(args: &Args) -> Self::Phase;
    fn gate(phase: Self::Phase, msg: &Msg) -> Disposition<TokenOf<Self>>;
    // on_defer_full stays HERE, defaulted (Redeliver) — ADR-0024 D6's one
    // legitimate default; dead (unreachable) under NoDefer.
}
```

- `DeferSeat` is **sealed**, core-provided: `NoDefer` (Token=`Never`,
  Stash=`NoStash` ZST — **no buffer exists**) or `Bounded<SP>` (Token=
  `Deferred` unit, Stash=`Stashing<Msg>`, capacity via the **reused**
  `StashPolicy` — `stash_capacity` deleted, not moved). The seat owns the
  admit-defer path; `NoDefer`'s is `match token {}` — compiled out.
- `NoTimeout` (core): `next_deadline` = `None` constant; the arm never
  arms; reaction body unreachable.
- `Phased<P>` fields: `policy, timeout: P::Timeout, phase, pending,
  entered_at, stash: StashOf<P>`. `Replay` is **reused** as the stash
  drain bound (`NoStash` yields `None`); `unstash_all` rides a small
  sealed `PhaseBuffer` bound. `Phased::stash()` exists only for
  `Deferral = Bounded<SP>` (where-clause), so a never-defers machine has
  no phantom escape hatch.
- D1–D10 semantics preserved verbatim: gate-before-handler, commit-after-
  `Ok`, D4 transition effects (switch → reset anchor → release stash),
  in-step re-gated replay, `Goto(current)` no-op.

## Delete ledger (the gate)

| metric | before | after |
|---|---|---|
| deadline seat traits | 2 (`DeadlinePolicy`, pair-in-`PhasePolicy`) | **1** (`DeadlinePolicy<Cx>`) |
| verdicts at policy seats | `Flow` + `Step` | **`Step` only** (`Flow ≅ Step<Never>` at the boundary) |
| `PhasePolicy` required fns | 6 | **3** (+2 one-line type picks) |
| deferral bound spelling | `stash_capacity` duplicating `StashPolicy` | **reuse** `StashPolicy` |
| never-defers machine | 6-line `Capacity::MIN` ceremony + live stash alloc | `type Deferral = NoDefer;` + **zero alloc** |
| new public types | — | `Never`, `NoDefer`, `Bounded<SP>`, `NoTimeout`, `ByState`, `ByPhase`, view/fire structs |

If the net core diff exceeds the new-types column's inherent cost, the
design is wrong — stop and revisit.

## Explicitly not in scope

- `WatchPolicy`'s `ControlFlow<ActorStopReason>` — the third verdict
  dialect; folding it touches #266 delivery semantics. Noted in ADR-0028.
- The `Machine → Machine` algebra (open capabilities, static merged
  select) — own card, after #286; `DeadlineCx` stays sealed until then.
- Canned seats (`StopOnDeadline`) — at the second concrete use.
- Collapsing handler `Flow` into `Step<Never>` — pure churn, open door.

## Test plan (TDD, failing first)

1. **Compile-law probes** (`compile_fail` doctests / trybuild): a
   `NoDefer` gate returning `Defer` does not compile; a timeout seat
   cannot supply `next_deadline` without `on_deadline` (trait shape).
2. **`alloc_phased.rs` tightened:** `NoDefer` machine build performs zero
   stash allocation — a falsifiable assertion the old design fails.
3. **`phase_equivalence.rs`:** the 6-scenario oracle green **unchanged**
   for a `Bounded` + timed machine (semantics-preserving migration).
4. **`caps_phased.rs` / `caps_deadline` tests:** migrated to the new
   signatures, same trace assertions; `ByState` idle-timeout behavior
   byte-identical.
5. **Job-queue Worker:** `NoDefer` + `DrainGrace` seat; ceremony deleted;
   `app_job_queue.rs` green unchanged; wart doc updated with the fix.

## Migration list

`crates/core/src/caps.rs` (seats, `Deadlined`, `Phased`, `Admission`),
`crates/macros/src/derive_provide.rs` (only if hook forwarding names
change), tests above, `examples/job_queue/app.rs`, mutants baseline
(new/renamed fns), README public-API bullets, coverage baseline.
