# Card #277 — core distillation: one trait, plugged capabilities

Date: 2026-07-31 · Status: approved design (migration = staged cards) ·
ADR: [ADR-0026](../../adr/0026-core-distillation-one-trait-caps.md) ·
Gates #274.

## The distilled surface (normative sketch; stage-1 card finalizes names)

```rust
pub trait Actor: Sized + Send + 'static {
    type Msg: Msg;                    // menu; #114 tripwire via derive
    type Args: Send;
    type Error: ReplyError;
    type Caps: CapSet<Self>;          // () = plain; tuples compose

    async fn init(args: Self::Args, cx: &mut Ctx<Self>) -> Result<Self, Self::Error>;
    async fn handle(&mut self, msg: Self::Msg, cx: &mut Ctx<Self>)
        -> Result<Flow, Self::Error>;
    // defaulted mechanics (safe passthroughs): name, on_stop, on_panic
}

// capabilities (each its own module; policies REQUIRED, strategy-as-type)
Stashing<SP: StashPolicy<A>>          // ADR-0022 semantics
Phased<P: PhasePolicy>                // ADR-0024 semantics on the ADR-0025 plane
Watching<WP: WatchPolicy>             // on_link_died seat; OTP default BY NAME
Supervising<SS: SupervisionStrategy>  // requires Watching in its bounds

// ONE ergonomic spawn (loop shape chosen by Caps at compile time,
// monomorphized); PreparedActor low-level layer unchanged beneath it.
pub async fn spawn<A: Actor>(args: A::Args) -> ActorRef<A>;
pub async fn spawn_with<A: Actor>(cfg: SpawnConfig, args: A::Args) -> ActorRef<A>;
```

Compile-time laws (ADR-0026 constraints): `Ctx` exposes exactly what
`Caps` provides (no runtime-checked accessor, ever); `CapSet`/`Has<C>`
public + derivable (third-party caps, bevy-`SystemParam` precedent);
invalid stacks don't compile; the expert floor (`PreparedActor`, mailbox,
loop seams) stays; capabilities stay small separate units.

## Invariant-preservation mapping (every shipped guarantee → new seat)

| Invariant (source) | Where it lives after distillation |
|---|---|
| Poisoning: panic ⇒ stop, `&mut self` torn, terminal hooks resource-release-only (#116, mod.rs docs) | Unchanged — `handle`/hooks still run under catch_unwind in the same loop; `on_stop`/`on_panic` stay defaulted mechanics on the one trait, stash-less (ADR-0022 D6) |
| Drain window: strong ref minted only from a dequeued message's `self_sender`; `Collected` preserved (ADR-0003/0010/0020) | Unchanged — `ActorRef`/`WeakActorRef` both KEPT; hook ref-params fixed per capability policy (no per-tier re-litigation); deadline delivery keeps the Weak rule (ADR-0025) |
| In-band `Stop` FIFO vs control-lane overtake (ADR-0021) | Untouched — lanes are loop plumbing, invisible to the surface |
| Two-queue snapshot stash, bounded, refuse-with-handback (ADR-0022) | `Stashing` cap verbatim (livelock re-proof recorded: one-queue model deadlocks — the split is load-bearing) |
| `Flow` return-value stop (ADR-0023) | Unchanged on `handle` |
| FSM semantics D1–D10: Step/Disposition, state-change-only effects, replay-in-new-state, required policy items, `&self` magnitudes (ADR-0024 + amendments) | `Phased<P: PhasePolicy>` — the machine as ONE plugged unit; gate/exhaustiveness wart still targeted by #243 derive |
| Deadline plane: arm placement, fires-once, turn-boundary delivery, WeakActorRef, no epochs (ADR-0025) | Semantics untouched; `next_deadline`/`on_deadline` RELOCATE from `Actor` onto the cap machinery (plain actors carry zero deadline items) — surface amendment note added to ADR-0025 at stage 4 |
| Closed menu + #114 slot tripwire; `Recipient` valid across states | `type Msg` + menu derive (merges today's `Msg`+`Mailboxed` impls); `Fsm`-analog `Phased` keeps `Msg` identity (no envelope) |
| Zero-alloc paths (#207) & tell/ask builders (ADR-0007/0008) | Untouched; caps are monomorphized plain structs; #207 guards re-assert per migration stage |
| Supervision semantics: strategies, restart accounting, teardown (ADR-0012/0014/0019/0020, #196/#199) | `Supervising` cap wraps the same loop machinery; `SupervisionStrategy` remains the no-default policy precedent |
| Watch semantics incl. designed-lost notices (#266) and OTP default | `Watching<WatchPolicy>`; the OTP default becomes a NAMED policy (`OtpPropagation`) — chosen, not silently inherited |

## Metrics (audit baseline → candidate, spike-verified)

| task | today (impls/methods) | distilled | notes |
|---|---|---|---|
| plain ask actor | 3 / 2 | **1 / 2** | Msg+Mailboxed absorbed by derive |
| deferring | 3 / 3 + wrapper-spawn trap | **2 / 3** | trap structurally gone |
| phased (planned) | 3 / 7 | **2 / 6** | machine one unit |
| watching/supervising | +2 empty impls + 3 spawn verbs | **0 extra impls, 1 spawn** | policy by name |
| capability composition | wrapper×trait tiers DO NOT compose (no `impl Watch for Stashed`) | any `Caps` tuple | the audit's decisive hole |

Baseline totals: ≈80 items / ≈175–180 entries / 10 traits / 29 error
variants (inventory 2026-07-31, session record). Spike: `spike-277-b`
(ephemeral scratchpad; this spec + ADR are the durable record) — 5
walkthrough tests + 1 `compile_fail` doctest, all green, stable
toolchain.

## Migration stages (each its own card, filed at close of #277)

1. Core: one-trait + `CapSet`/`Ctx`/`Has` machinery + menu derive +
   plain path + ONE spawn (old surface deprecated in place, not broken).
2. `Stashing` (ports ADR-0022 tests; removes `StashActor`/`Stashed`).
3. `Watching`/`Supervising` + spawn collapse (removes `Watch`/
   `Supervisor`/three `Spawn*` traits; equivalence suites re-run).
4. `Phased` on the ADR-0025 plane (replaces parked #274 part 2; #274's
   plane part proceeds with the relocated declaration; job-queue
   walking skeleton + oracle port land here).
5. Delivery-error consolidation (TellError/AskError/PipeAskError 3×
   spelling — separate audited decision).
   #243 re-targeted: menu derive, per-state gate exhaustiveness,
   custom-cap derive.

## Open items deliberately NOT decided here

- Exact naming (`Caps` vs `Capabilities`, `Ctx` vs `Context`) — stage 1.
- Tuple-arity generation mechanics (macro vs hand impls to arity N).
- Whether `init`'s `Ctx` access is full or restricted — stage 1 decides
  with the registration-race rules (#248) in view.
