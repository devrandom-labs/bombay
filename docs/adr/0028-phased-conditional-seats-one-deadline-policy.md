# ADR-0028: Phased seats are declared, and there is one deadline policy

Date: 2026-08-02 · Status: accepted · Card: #290 (wart of #281) · Spec: `docs/superpowers/specs/2026-08-02-phased-conditional-seats-design.md`

## Context

`PhasePolicy` (ADR-0026 stage 4) bundled three seats unconditionally: the
gate, the deferral pair (`stash_capacity` + `on_defer_full`), and the
deadline pair (`phase_deadline` + `on_phase_timeout`). A machine that
never defers still wrote a ceremonial `Capacity::new(NonZeroUsize::MIN)`
and still carried a live stash allocation; a machine with no deadlines
would still write `on_phase_timeout`. The required-items rule is
deliberate (#196: a declared deadline with a defaulted reaction is a
silent pair); the wart (`docs/warts/281-phased-mandatory-ceremony.md`) is
that the requirement was not conditional on the declaration.

Auditing the file surfaced the deeper duplication the wart was a symptom
of: **two bespoke deadline seats** (`DeadlinePolicy<A>` for `Deadlined`;
the pair inside `PhasePolicy`) whose only real differences are what the
slot is computed *from* and the verdict dialect (`Flow` vs `Step`), plus
a third dialect (`ControlFlow<ActorStopReason>`) at `WatchPolicy`. The
"re-arm" difference is illusory — both slots are re-read declaratively
every loop iteration (ADR-0025 plane).

## Decision

### The two-layer law (framing, binding on future caps work)

- A **capability** is one unit contributing to two planes: a *service
  wrapper* on the message plane (`Admission`/`Replay`: admit-before,
  commit-after-`Ok` — tower-middleware-isomorphic) and an *event source +
  reaction* on the event plane (`DeadlineHook`: select-arm shaped, no
  tower analog — a `Service` is driven only by calls; a loop selects over
  sources). Capabilities stay sealed/core-provided until the machine-
  algebra card (`Machine → Machine`, filed as the successor to ADR-0026
  constraint 2's openness obligation).
- A **policy** is a pure strategy seat a capability consumes, welded to a
  *context shape*, never to a capability (tower `retry::Policy`
  precedent). This is the corrected reading of ADR-0026 constraint 5:
  the "one unit" is middleware + its *required* seats; optional seats are
  plugged, exactly as `Retry` does not demand a load-shed policy.

### One uninhabited type, two structural laws

`pub enum Never {}`. `Step<Never> ≅ Flow` (Stay↔Continue, Stop↔Stop,
`Goto` unconstructible) — a plain actor is a one-phase machine, so policy
seats speak **one verdict** (`Step`) and capabilities adapt at the
boundary; handler `Flow` (ADR-0023) is untouched. `Disposition<D = Never>`
makes an undeclared `Defer` *uncompilable* (the variant's payload is the
declaration); the type-param default keeps non-deferring signatures
spelled `Disposition` as today.

### One deadline policy, generic over a sealed context

`DeadlinePolicy<Cx: DeadlineCx>` carries `build` / `next_deadline`
(pure of `(self, view)`) / `on_deadline` (returns `Step<Cx::Phase>`).
Core provides two contexts: `ByState<A>` (view `&A`, `Phase = Never` —
the old `Deadlined` seat) and `ByPhase<P>` (view `{phase, entered_at}`,
fire carries `&mut` stash — the old phase pair). `Deadlined<DP>` stores
the policy instance and adapts `Step<Never> → Flow`. ADR-0025 plane
semantics are unchanged (re-read every iteration, fires-once,
turn-boundary, `WeakActorRef`, `PanicReason::OnDeadline`).

### PhasePolicy declares its seats

`type Deferral: DeferSeat<Self>` — sealed, core-provided: `NoDefer`
(token `Never`, stash `NoStash` ZST: **no buffer exists**) or
`Bounded<SP: StashPolicy>` (the deferral bound **reuses** `StashPolicy`;
`stash_capacity` is deleted, not moved). `type Timeout:
DeadlinePolicy<ByPhase<Self>>` — `NoTimeout` (slot constantly `None`) or
a real seat (`Self` legal). `on_defer_full` stays on `PhasePolicy`,
defaulted (ADR-0024 D6's one legitimate default), unreachable under
`NoDefer`.

The silent-pair law survives structurally in both directions: the
deadline pair lives in ONE trait (cannot write the slot without the
reaction); `NoDefer`/`NoTimeout` are explicit named choices (#196
`OneForOne` precedent — a named strategy is not a silent default); a
`Defer` verdict without a deferral seat does not compile.

## Why this shape

- **Extraction, not invention:** every unification has both instantiations
  already in the codebase (tower's own history: `Service` was extracted
  from observed duplication). The acceptance gate is the spec's delete
  ledger — the diff must remove more than it adds.
- **Conditionality must be reachable through `P` uniformly:** one blanket
  `impl DeadlineHook for Phased<P>` exists; without specialization the
  seat information must ride associated items. Declared seat types are
  the only stable-Rust encoding that also keeps the machine ONE unit.
- **D1–D10 preserved verbatim** — this is a seat relocation; the #266-
  style equivalence oracle must pass unchanged for a fully-seated machine.

## Alternatives rejected

- **A1 (marker + self-impl):** `type Timeout = Timed` forcing
  `Self: TimeoutPolicy` — same conditionality, but no reusable seats and
  an extra forwarding shim; diverges from the plugged-policy house style.
- **B (seats on the field type):** `Phased<P, NoDefer, Timed>` — moves the
  declaration away from the policy it constrains; doubles public surface.
- **C (defer to #243/#286 generation):** leaves the wart at the
  hand-written expert floor and the law enforced by generated convention;
  the Worker ceremony would persist until #286.

## Consequences

- `DeadlinePolicy<A>` users (in-tree only, 3 days old) migrate to
  `DeadlinePolicy<ByState<A>>`; `Deadlined::new()` → `Deadlined::build(args)`.
- The job-queue Worker drops the `Capacity::MIN` ceremony for
  `type Deferral = NoDefer;` and a `DrainGrace` timeout seat; a
  never-defers machine's stash type is zero-sized — no buffer exists, so
  a defer-path allocation is unrepresentable (type-level pin; the old
  design's `VecDeque` was lazily allocated, so the claim is structural,
  not a measured delta).
- Open doors, deliberately not taken now — each with its card: folding
  `WatchPolicy`'s `ControlFlow` dialect + handler `Flow` into the one
  `Step` family is **card #297** (a precondition of #295: the algebra
  needs one verdict, not three); unsealing `DeadlineCx`/capabilities is
  **card #295** (the machine algebra); canned seats (`StopOnDeadline`)
  wait for the second concrete use, no card until then.
