# 277 audit — working notes (in-flight state, not a deliverable)

Status 2026-07-31: inventory + duplication sweeps running (two agents);
research leg DONE. Spikes + metrics + ADR pending.

## Candidate-pattern research (primary-source-verified 2026-07-31)

- **tower `Service`** (docs.rs/tower-service): 5 items — Response/Error/Future
  + poll_ready + call. The minimal-surface bar. CAUTION for the ledger:
  minimalism hides a sharp contract — "implementations are permitted to
  panic if call is invoked without obtaining Poll::Ready from poll_ready".
  Fewest items ≠ fewest obligations; obligations moved into prose.
- **axum `Handler`** (docs.rs/axum): users write plain async fns; blanket
  impls over tuples (≤16 params) of extractor types (FromRequestParts /
  last-param FromRequest) do the wiring; a marker type param works around
  coherence. Capabilities = parameter TYPES.
- **bevy `SystemParam`** (docs.rs/bevy_ecs): the production embodiment of
  params-as-capabilities: Query/Res/ResMut/Local/Commands; `Local<T>` =
  per-system state (analog: per-actor stash/phase); #[derive(SystemParam)]
  composes custom param structs; unsafe trait registers access for the
  scheduler. Type-safe zero-cost; users never implement it manually.
- **Associated type defaults: STILL UNSTABLE** (rust-lang #29661,
  RFC 2532). On pinned-stable, candidate (c) GAT/slot traits cannot
  default slots → every actor declares every slot → boilerplate tax.
  Weighs against (c) unless slots ride a derive.

## Candidate sketches to spike (job-queue Worker slice, both-ways rule)

- (a) status-quo-plus: current lattice, policy items required (per parked
  #274 plan amendments).
- (b) extractor-style single surface: ONE spawn API taking state + a plain
  async handler fn; capabilities requested via typed params (stash,
  phase tag, deadline ctl) with blanket impls bevy/axum-style; lifecycle
  hooks become optional registrations or one small trait. Key questions
  for the spike: coherence workarounds on stable; &mut state + param
  borrows coexisting (bevy solves via World splitting — we have one
  struct); RPITIT/Send bounds; how on_stop/on_panic surface.
- (c) capability slots as associated types on ONE Actor trait
  (type Deferral: DeferralPolicy = NoStash — BLOCKED by #29661 on
  stable; variant: slots without defaults, or derive-filled).

## Ledger dimensions (fixed by card #277)

user LOC | items-to-know | forgettable obligations | compiler-enforced vs
prose | allocation deltas | invariant mapping (poisoning, drain window,
ADR-0020/21/22/23/25, zero-alloc paths).

## Duplication-map results (sweep 1 done, 2026-07-31 — full data in session; anchors verified file:line)

1. **Four-way hook duplication**: on_start/handle/on_panic/on_stop declared
   3× (Actor mod.rs:82-139, StashActor stash.rs:141-178, planned FsmActor
   spec:211-229) + a 4th forwarding copy in Stashed's Actor impl
   (stash.rs:194-248). Drift axes: wrapper type inside actor_ref
   (Self → Stashed<Self> → Fsm<Self> — the WRAPPER LEAKS into user
   signatures) + trailing params. `stash_capacity` byte-identical in two
   traits. In-tree docs acknowledge ("See Actor::on_start").
2. **Two capability paradigms for one conceptual slot**: trait-subtyping
   (Watch ⊂ Supervisor, mod.rs:147-258; ADR-0011 authority-typing) vs
   composition wrapper (Stashed, planned Fsm; ADR-0022 forget-proofing) +
   free verbs (timers/watch/supervise on ActorRef) + config structs. Four
   paradigms coexist.
3. **Ref-type rule** is sound and fully recorded (strong iff
   message-driven live turn; Weak iff poisoned/terminal/message-less;
   none for on_link_died) — but doc-split: ADR-0025 amends #231 spec's
   on_state_timeout in a NOTE, spec body still says ActorRef (spec:222 vs
   140-144). The RULE could be a type, not prose.
4. **~11 public spawn entry points** over 3 real start-kinds (Spawn/
   SpawnLinked/SpawnSupervised × default/config + PreparedActor
   new/new_linked × run/spawn × 3 tiers). Axis of variation is uniform
   (task-placement × capability-tier × config) — the surface is not.
5. **handle arity 3→4→5** monotonic per capability tier; FSM adds two
   more 4-arg hooks.
6. **Delivery failures spelled 3×**: TellError(3) / AskError::Deliver(4) /
   PipeAskError(7 flattened) — deliberate split-not-erase (error.rs:1-16)
   but the largest error-surface duplication. 9 public error types total.

## Inventory baseline (sweep 2 done, 2026-07-31)

- **≈80 named public items; ≈175–180 user-touchable entries** (items +
  methods). 10 public traits; 18 trait methods shipped (FsmActor plan →
  28). 29 error variants across 5 enums + 3 bare structs.
- Obligation walkthroughs (impls / methods / types / decisions):
  plain ask actor **3/2/2/4**; stash actor **3/3/2/4**; FSM (planned)
  **3/7/3/≥6**; supervised pair **7/4/≥4/≥5** — incl. TWO EMPTY marker
  impls (`impl Watch for D {}`, `impl Supervisor for D {}`) = pure
  ceremony, and the liveness-anchor decision (ADR-0020 collected-child)
  as a hidden obligation.
- Spawn surface: ~11 entries over 3 kinds; leaks: `LinkReceiver`/
  `LinkSender` returned by public `new_linked`; several pub-in-private
  items (spawn_pipe, supervision internals).
- Msg/Mailboxed are 2 of the 3 impls every task pays (marker + one assoc
  type) — candidate for merging into one declaration (derive already
  planned in #243).

## Candidate (b) spike design — extractor-style single surface (next step)

Scratchpad crate `spike-277-b`, path-dep on crates/core NOT required
(model the surface standalone like spike-274-loop; bombay dep only if
reusing Capacity). Model the USER-FACING layer only; runtime = thin fake
loop (the real loop invariants are already ADR-pinned — this spike
measures ERGONOMICS + compile-time safety, not loop semantics).

Surface sketch to spike:
```rust
// ONE user trait (or even zero: a fn + registration)
trait Actor: Sized + Send + 'static {
    type Msg: Msg; type Args: Send; type Error: ReplyError;
    // ONE lifecycle entry: setup returns Self (today's on_start)
    async fn init(ctx: Init<'_, Self>) -> Result<Self, Self::Error>;
    // ONE handle; capabilities = typed params requested via Caps tuple
    type Caps: CapSet<Self>;   // () | (Stash,) | (Phase<St>, Stash) | ...
    async fn handle(&mut self, msg: Self::Msg, cx: Ctx<'_, Self>)
        -> Result<Flow, Self::Error>;
}
// Ctx<'_, A> exposes: self_ref() -> Ref (ONE ref type: upgradeable-
// by-construction, absorbing the ActorRef/WeakActorRef prose rule),
// cx.stash() IF Caps includes Stash (compile error otherwise),
// cx.phase()/cx.goto() IF Caps includes Phase<St>, deadline decl via
// Phase; death notices as a Msg-adjacent enum? (see open Q3)
```
Variant to also spike: axum-style — handle as PLAIN async fn with typed
params `async fn handle(&mut W, Msg, Phase<'_, St>, Stash<'_, Msg>)`
via blanket impls over param tuples (bevy SystemParam precedent; check
coherence + &mut-self-plus-params borrows — bevy splits World; we have
one struct → params must borrow from a Ctx owned OUTSIDE self. This is
the key compile-feasibility question the spike must answer.)

Metrics to produce (vs baseline above): same 4 walkthroughs on the
candidate; table LOC / impls / methods / decisions / compile-enforced.

Open questions the spike must answer:
- Q1: can capability params borrow-split against &mut self cleanly?
- Q2: does ONE Ref type (internally weak, upgrade-on-use) preserve the
  ADR-0003/0010/0020 semantics without the two-type split? (tell via
  upgrade-or-SendError — check drain-window mint needs)
- Q3: do Watch/Supervisor collapse into capabilities (Caps) too —
  killing the empty marker impls and the trait/wrapper paradigm split?
- Q4: spawn: ONE spawn(args) with capability-driven loop selection
  (compile-time: Caps decides linked/supervised loop) — kills the
  11-entry spawn surface?

## Open threads

- Two Explore sweeps (inventory; duplication map) → results feed the
  metrics baseline for candidate (a).
- Spikes in scratchpad crate(s) path-dep on crates/core.
- Deliverables: distillation ADR (0026?) + spec + revised #274 plan.
