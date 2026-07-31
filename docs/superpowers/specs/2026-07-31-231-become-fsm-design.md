# Card #231 — behavior switching / FSM helper (`become` for the closed menu)

Date: 2026-07-31 · Status: approved design (build is a follow-up card) ·
ADR: [ADR-0024](../../adr/0024-fsm-behavior-switching.md)

## Problem

Actors move through operational phases (the nexus aggregate runner:
Loading → Ready → Draining), and what a message *means* depends on the phase.
Rust expresses the states fine (`match self.phase`), but the framework cannot
see transitions — so the bookkeeping that must happen at every transition
(release deferred messages, cancel/arm per-state deadlines, guard stale
timeouts) is manual, scattered, and fails silently when forgotten. This is the
same failure class that killed the stash v1 accessor design ("what if users
forget", ADR-0022 / spec D-series for #224).

The card asks: ship an FSM helper, or document the idiom? Constraints: the
closed menu stays (message set = actor-level enum; behavior switching changes
*handling*, never the menu; `Recipient<M>` handles stay valid across states);
no allocation on the transition path (no Pekko-style behavior objects); the
`&mut self` poisoning model (#116) must hold unchanged.

## Decision summary

**Ship `FsmActor` + `Fsm<S>`** — a framework-owned composition wrapper (the
`Stashed<S>` house pattern) with **value-return transitions** and **P-style
declarative message admission**. Decided by a both-ways spike against the
nexus-shaped lifecycle, not by prose (the #199 lesson): identical observable
behavior, zero transition allocations, and the idiom's seven manual
transition obligations collapse to one declaration site that the equivalence
oracle catches three ways.

## Research corpus (all primary-source-verified 2026-07-31)

**Adopted:**

- G. Agha, *Actors: A Model of Concurrent Computation in Distributed
  Systems*, MIT Press 1986; and CACM 33(9) 1990 — `become` is one of the
  three primitive operations; a plain actor is the one-state degenerate case;
  pipelining-via-become. Grounds the lattice direction (§ "Shape lattice").
- De Koster, Van Cutsem, De Meuter, *43 Years of Actors*, AGERE! 2016,
  DOI 10.1145/3001886.3001890 — state-change granularity as a top-level
  taxonomy axis: one `become`-shaped transition point "severely coarsens the
  granularity of side-effecting operations", shrinking the reasoning space.
- Desai, Gupta, Jackson, Qadeer, Rajamani, Zufferey, *P: Safe Asynchronous
  Event-Driven Programming*, PLDI 2013 (pp. 321-331) — state-machine actors
  with **declarative per-state `defer` and `ignore`**. Semantics verified
  against the P manual (p-org.github.io/P/manual/statemachines/): a deferred
  event's dequeue is skipped "until the machine transitions to a
  non-deferred state"; `ignore` is declared drop ("short hand for
  `on E do { }`"). Production evidence: AWS S3/EBS/DynamoDB/EC2 teams verify
  designs in P (Brooker & Desai, CACM 2025, per the p-org/P README).
- Erlang/OTP `gen_statem` (erlang.org/doc/apps/stdlib/gen_statem.html,
  verified 2026-07-31) — as a *production data point*, not a ceiling:
  postponed events are retried **only on a state change**
  (`NextState =/= State`); a state timeout is cancelled **only on a state
  change**; `next_state` to the same state retries nothing. P and gen_statem
  agree on these transition rules; where they disagree (imperative
  `postpone` per event vs declarative `defer` per state), the spike decided
  for P's declarative shape (§ "Spike record").

**Considered and rejected:**

- Active objects / cooperative `await` guards — De Boer, Serbanescu, Hähnle,
  Henrio, Rochas et al., *A Survey of Active Object Languages*, ACM Computing
  Surveys 50(5) 2017, DOI 10.1145/3122848. Deferral by suspending the
  *activation* (Creol/ABS `await`) instead of the *message*. Rejected:
  interleaved activations at await points break bombay's run-to-completion
  single-writer `&mut self` model and the poisoning contract (#116).
- Mailbox types — Fowler, Attard, Sowul, Trinder, Gay, *Special Delivery:
  Programming with Mailbox Types*, ICFP 2023, DOI 10.1145/3607832 (calculus:
  de'Liguoro & Padovani 2018). Static typing of state-dependent mailbox
  protocols. Recorded as the static-verification frontier; adopting it means
  typestate machinery that fights the closed-menu dynamic dispatch. Related
  work, not adopted.
- Harel statecharts (*Statecharts: A Visual Formalism for Complex Systems*,
  Science of Computer Programming 8, 1987) — hierarchical/nested states. YAGNI at the
  first consumer (3 flat states); add at the second concrete use per the API
  rules.
- Pekko Typed behavior-return (pekko.apache.org/docs/pekko/current/typed/fsm.html)
  — returning a *behavior object* allocates code per transition. Bombay's
  closed menu means only the state *name* changes; `Step` is plain data.
  This is the anti-pattern bound the card set, confirmed avoidable (0 allocs).

## Decisions

- **D1 — Ship the helper as composition, not a base-trait change.**
  `Fsm<S: FsmActor>` implements `Actor` (exactly as `Stashed<S>` does): every
  existing verb — spawn/tell/ask/watch/supervise/timers/`Recipient` — works
  unchanged. `FsmActor` is the description the wrapper consumes, not a new
  runtime citizen.
- **D2 — gen_statem's name/data split.** `type State: Copy + PartialEq +
  Send + 'static` is a plain tag enum the wrapper owns and passes to `handle`
  by reference; state *data* stays in `self`. The `Copy` bound is what makes
  `Step<State>`'s derived `Copy` unconditional (a derived `Copy` on a generic
  enum is conditional on the parameter). Payload-carrying state enums
  (the fused Rust idiom) remain possible per-actor but are not what the
  wrapper observes.
- **D3 — Transition is a value return.**
  `enum Step<St> { Stay, Goto(St), Stop }` — `Flow` (ADR-0023) plus one
  variant, `Copy`, zero-box. `Goto(s)` with `s == current` ≡ `Stay`
  (gen_statem `next_state`-to-same-state; verified). The state switch commits
  only after the handler returns `Ok` — a mid-handler panic never observes a
  half-switched state; the poisoning model holds by construction.
- **D4 — Transition effects, on state *change* only, in this order:** bump
  epoch → cancel state timeout → switch state → release stash → arm the new
  state's timeout → replay released messages **re-gated and handled in the
  new state**, ahead of the entire mailbox backlog, in stash-arrival order
  (ADR-0022 snapshot semantics bound the replay: a re-deferred message goes
  to `held`, never back into the draining batch).
- **D5 — Declarative admission (the P trio), payload-capable.**
  `fn gate(&State, &Msg) -> Disposition` with
  `enum Disposition { Deliver, Defer, Ignore }` (default `Deliver`).
  `Defer` → wrapper stashes; `Ignore` → declared drop (P precedent — this is
  recorded intent, not a silent loss). The gate takes `&Msg`, so admission
  can be data-dependent — strictly more general than P's per-event-type
  defer. Manual `stash`/`unstash_all` stay available in `handle` (gen_statem
  hybrid) for release timing that is not transition-shaped.
- **D6 — Overflow is a handback hook, never a silent drop.** A `Defer` that
  finds the stash at capacity routes the message — intact — to
  `on_defer_full(&mut self, &State, msg, …) -> Result<Step, E>`. Default
  implementation delivers to `handle` (visible-but-unrefused shedding);
  consumers override for loud typed refusal (the `TellError`/`StashFull`
  handback precedent). This is the one qualified exception to "handle never
  sees a gated-away message": at capacity, the default hands it back through
  a hook the consumer controls.
- **D6b — Stash access extends to the two handler-plane hooks.** ADR-0022's
  consequence "stash access is `handle`-only" was scoped to the *terminal*
  hooks (`on_stop`/`on_panic` — poisoned or stopping state), and anticipated
  a follow-up if live hooks ever needed access. `on_state_timeout` and
  `on_defer_full` are handler-plane (they receive control while the actor is
  live and return `Step`), so they take `&mut Stash` exactly as `handle`
  does — a deliberate, recorded extension of ADR-0022, not a silent
  reversal. The terminal hooks stay stash-less.
- **D7 — State timeouts are framework events, not menu variants.**
  `fn state_timeout(&State) -> Option<Duration>` declares them; firing
  delivers to `fn on_state_timeout(…) -> Result<Step, E>`. Every arming is
  stamped with the wrapper's transition **epoch**; a timeout that
  fired-and-queued before its cancellation carries a stale epoch and is
  dropped by the wrapper — **staleness is unrepresentable in user code**.
  `Fsm<S>::Msg == S::Msg` is a hard constraint: no public envelope, no slot
  growth (#114 tripwires untouched), `Recipient` minting unchanged. The
  internal delivery mechanism is the build card's first decision, under two
  recorded constraints: (i) ADR-0021's control lane is deliberately
  **non-generic** (one concrete `ControlSignal` for every actor — no
  `A::Msg` in any payload) and its signals are consumed by the run-loop, not
  routed to `Actor::handle`, so a control-lane timeout is a *new lane/routing
  shape*, not ADR-0021 as-is; (ii) an epoch-only event needs no message
  payload, so non-genericity is satisfiable — the open question is the
  routing into the wrapper. The spike's public envelope was a mock
  compromise and is **not** the shipped design; its `send_after`-based arm
  cost does not transfer (see Spike record).
- **D8 — The shape lattice; no hierarchy inversion.**
  `Actor ⊂ StashActor ⊂ FsmActor` conceptually (Agha: `become` is primitive;
  a plain actor is the one-state case) — realized by composition, each rung
  erasing into `Actor`. `StashActor` **stays**: harden-never-migrate, it is
  the simpler surface for non-transition-shaped deferral, and the field keeps
  gen_server alongside the strictly-more-general gen_statem for the same
  reason. Making `FsmActor` the base would tax every trivial actor with
  ceremony (unit state, `Step` returns, an unused stash).
- **D9 — Flat states only; no named timeouts.** Statecharts hierarchy and
  gen_statem named/generic timeouts are cut (YAGNI; second-concrete-use
  rule). Cancellation-by-transition covers the consumer's needs.
- **D10 — Receive-timeout (#241) is orthogonal and stays its own card.**
  gen_statem's *event* timeout ("any event cancels it") is #241's
  reset-on-message idle timer; this card ships only the *state* timeout
  (cancelled by transition, not by traffic). The two must not be merged.

## Trait surface (spec of record)

Verified compiling and behaviorally equivalent to the idiom in the spike
(mock-mechanics caveat: D7 envelope note). Written `async fn` for brevity;
the build uses the house RPITIT style
(`fn … -> impl Future<Output = …> + Send`, the #9 `MaybeSend`-ready pattern
of `Actor`/`StashActor`).

```rust
/// Per-state message admission (the P trio).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition { Deliver, Defer, Ignore }

/// The handler's transition decision — `Flow` (ADR-0023) plus one variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<St> { Stay, Goto(St), Stop }

pub trait FsmActor: Mailboxed<Msg: Msg> + Sized + Send + 'static {
    type Args: Send;
    type Error: ReplyError;
    /// State NAME (tag enum); state DATA stays in `self` (D2).
    type State: Copy + PartialEq + Send + 'static;

    fn initial_state(args: &Self::Args) -> Self::State;
    fn stash_capacity(args: &Self::Args) -> Capacity;          // ADR-0022: explicit, bounded
    fn gate(state: &Self::State, msg: &Self::Msg) -> Disposition { Disposition::Deliver }
    fn state_timeout(state: &Self::State) -> Option<Duration> { None }

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Fsm<Self>>)
        -> Result<Self, Self::Error>;
    async fn handle(&mut self, state: &Self::State, msg: Self::Msg,
                    actor_ref: ActorRef<Fsm<Self>>, stash: &mut Stash<Self::Msg>)
        -> Result<Step<Self::State>, Self::Error>;
    async fn on_state_timeout(&mut self, state: &Self::State,
                    actor_ref: ActorRef<Fsm<Self>>, stash: &mut Stash<Self::Msg>)
        -> Result<Step<Self::State>, Self::Error>;   // default: Ok(Step::Stay)
    async fn on_defer_full(&mut self, state: &Self::State, msg: Self::Msg,
                    actor_ref: ActorRef<Fsm<Self>>, stash: &mut Stash<Self::Msg>)
        -> Result<Step<Self::State>, Self::Error>;   // default: deliver to handle
    async fn on_stop(&mut self, actor_ref: WeakActorRef<Fsm<Self>>, reason: ActorStopReason)
        -> Result<(), Self::Error>;                  // passthrough default
    async fn on_panic(&mut self, actor_ref: WeakActorRef<Fsm<Self>>, err: PanicError)
        -> ActorStopReason;                          // passthrough default
}

pub struct Fsm<S: FsmActor> { /* data, state, epoch, timer, stash */ }
impl<S: FsmActor> Actor for Fsm<S> { /* the wrapper */ }
```

The shipped build reuses `bombay::stash::Stash` (its `pub(crate)` constructor
is reachable in-crate); the spike's `FsmStash` re-implementation was
mock-only.

### Wrapper algorithm (one `Actor::handle` step of `Fsm<S>`)

```text
on user message m:
  match S::gate(&state, &m):
    Deliver -> step = S::handle(&mut data, &state, m, ref, &mut stash)?
    Defer   -> stash.stash(m); on StashFull(m) -> step = S::on_defer_full(.., m, ..)?
    Ignore  -> step = Stay
on state-timeout event with epoch e:
  if e != current epoch -> drop (stale; user never observes)   // D7
  else step = S::on_state_timeout(&mut data, &state, ..)?
apply(step):
  Stay -> (); Stop -> return Flow::Stop
  Goto(next) if next != state:                                  // D3/D4
    epoch += 1; cancel state timer; state = next;
    stash.unstash_all(); arm state_timeout(state) stamped with epoch
then replay loop (within the SAME step, ADR-0022):
  while let Some(m) = stash.pop_ready():
    re-gate m in the CURRENT state (Defer -> back to held; Ignore -> drop;
    Deliver -> handle), apply(step) as above
return Flow::Continue
```

## Spike record

Setup (ephemeral session scratchpad, `spike-231/`; this spec is
self-contained — the trait surface, wrapper algorithm, scenario list, and
metrics below are the durable record): a standalone crate path-depending on
`crates/core` with the `test-support` feature, `cargo test` inside
`nix develop`. Two variants of
the same nexus-shaped lifecycle actor — `agg_idiom.rs` (`StashActor` +
`match self.phase` + manual bookkeeping) and `agg_fsm.rs` (mock
`FsmActor`/`Fsm<S>`) — driven by one script vocabulary through a mode-blind
equivalence oracle (the #266 pattern) comparing observable probe sequences.

Scenarios (all 6 green on the final surface): happy path with replay-ahead-of-
backlog ordering; bounded-stash loud shedding; rehydration deadline firing
(paused clock); stale-deadline race invisibility; drain-during-Loading
releasing deferred commands into refusal; deadline cancellation on timely
transition.

**Metrics:**

| Measure | idiom (a) | helper (b, final) |
|---|---|---|
| Observable behavior | — | identical, 6/6 scenarios |
| User-side LOC (tokei, code) | 106 | 124 (+18: gate + hook overrides) |
| Menu variants | 5 (deadline must be public) | 4 |
| Transition gross allocs (CountingAlloc, #207 pattern) | n/a | **Goto = Stay = 2** (both probe-channel; transition adds **0**) |
| State-timeout arm | same both ways | 3 **on the mock's public-envelope `send_after` path** (per state entry, never per message); the shipped (non-envelope, D7) mechanism's arm cost is unmeasured — a build-card measurement |

**The idiom's manual transition obligations (7, as marked `[F#]` in the
spike's `agg_idiom.rs`):** [F1] carry the timer handle as a field; [F2] arm
the rehydration deadline in `on_start`; [F3] unstash on the Loading→Ready
edge; [F4] cancel the deadline on that edge; [F4-dup] cancel it again on the
Loading→Draining edge (per-EDGE, not per-state); [F5] stale-deadline guard
arms in every other phase; [F6] unstash on the Loading→Draining edge.

**Falsifiability (mutation runs — 4 of the 7 mutated; each row below is a
run, not an inference):**

| Omitted step | Caught by |
|---|---|
| idiom [F3] unstash on Loading→Ready | s1, s2 |
| idiom [F6] unstash on Loading→Draining edge | s5 (this bug was actually committed in the idiom's first draft) |
| idiom [F5] naive stale-deadline arm | s4 (spurious stop while Ready) |
| idiom [F4-dup] timer cancel on second exit edge | **nothing — escapes the suite** |
| helper: forgotten `Defer` declaration | s1, s2, s5 (three at once) |

F1/F2/F4 were not mutation-run (F1 is structural — without the field, F4
cannot compile; F2's omission fails s3 by inspection). The ledger: idiom =
7 scattered obligations, 3 of 4 mutated ones caught, one invisible even to
this deliberately adversarial oracle; helper = 1 declaration site, caught
three ways. Concentration, not just reduction, is the safety property.

## Recorded warts

- **Gate/exhaustiveness wart:** `handle`'s `match (state, msg)` cannot see
  the gate, so pairs declared `Defer`/`Ignore` still require a catch-all arm
  (~2 lines of `_ => Ok(Step::Stay)` ceremony). Accepted; a derive (#243) could
  eventually generate the match and absorb it.
- **Mock envelope wart (not shipped):** the spike delivered state timeouts
  via a public `FsmMsg<M>` envelope, which leaked `tell(FsmMsg::User(..))`
  into user code. D7 forbids this in the build.

## Follow-ups

- **Build card (filed from this card):** implement `FsmActor`/`Fsm<S>` per
  D1–D10; first decision = D7 plumbing (control-lane vs run-loop); port the
  spike's 6-scenario oracle + falsifiability mutations as in-repo tests;
  mutants-baseline entries for every new fn; extend the job-queue walking
  skeleton (worker gains a 3-phase lifecycle) + `app_job_queue.rs`.
- #241 (receive-timeout) proceeds independently (D10) but should compose
  with `Fsm<S>` (an idle timer on a phased actor is coherent).
- #243 (derive) may later add sugar over `FsmActor` (per-state handler
  blocks, gate generation) — semantics are fixed here; the derive is syntax.
