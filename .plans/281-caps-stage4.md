# 281 — caps stage 4: `Deadlined` + `Phased` on the ADR-0025 plane

> Re-seats `.plans/274-fsm-build.md` (semantics/tests/oracle) onto the caps
> surface per ADR-0026. Content sources: that plan's S-steps, ADR-0024 D1–D10,
> ADR-0025, spike-277-b (the phased-cap encoding), spike-231 (the oracle),
> spike-274-loop (P1–P5). This file records the in-card design decisions the
> card left open ("the deadline arm hangs off a cap-set hook — design it
> in-card").

## The seat design (decided here)

**No 4th loop shape.** `Deadlined` is orthogonal to shape (#280 comment), so
the mechanism is the stage-2 `Replay` precedent, twice:

1. **Runtime floor**: the internal `actor::Actor` trait gains the two
   ADR-0025 plane methods as defaults (`next_deadline() -> Option<Instant>`
   = `None`; `on_deadline(WeakActorRef) -> Result<Flow, E>` =
   `Ok(Flow::Continue)`). All three loops in `kind.rs` poll them uniformly
   via one guarded `sleep_until` arm (plan S3 placement: link → [retries →
   aborts] → deadline → mailbox; fires-once via `last_fired`). This is NOT a
   reversal of ADR-0026's relocation: the USER seat is the capability; the
   runtime trait is the loop's internal floor (post one-door, caps is the
   door), and a raw floor actor pays only a `None` read per iteration.
2. **Cap bridge**: a new loop-participation trait on the cap set —
   `DeadlineHook<A>` (`next_deadline(&self, &A)` / `on_deadline(&mut self,
   &mut A, WeakActorRef<Shell<A>>)`) — added to the `Actor::Caps` bound next
   to `Replay`/`SelectRunner`. `()` = disabled. `Shell` forwards the runtime
   methods to it. Derive-emitted, either/or (no overlap):
   - `Deadlined<DP>` field → generic impl forwarding to
     `DP: DeadlinePolicy<A>` (`next_deadline(&A)` pure-fn-of-state, quinn
     shape; magnitudes live in actor state from `Args` — the #241 path);
   - `Phased<P>` field → concrete impl (over `P::Actor`) forwarding to the
     cap (`entered_at + policy.phase_deadline(phase)`), routing expiry to
     `PhasePolicy::on_phase_timeout` and applying the returned `Step`.
   - **Both fields in one set = parse-time reject** ("Phased embeds
     Deadlined; one deadline seat per set") — the friendly half; E0119
     remains the law for hand-written sets.

**`Phased<P: PhasePolicy>`** (spike-277-b encoding + D1–D10):

- `PhasePolicy` is ONE unit with `type Actor: Actor` (a phase policy is
  actor-specific — it gates the actor's menu) and `type Phase: Copy +
  PartialEq + Send + 'static`. Required items: `build(&Args)` (the D8
  magnitude channel — policy instance is Args-built), `initial(&Args)`,
  `stash_capacity(&Args)`, `gate(Phase, &Msg) -> Disposition`,
  `phase_deadline(&self, Phase) -> Option<Duration>`,
  `on_phase_timeout(&mut A, Phase, WeakActorRef<Shell<A>>, &mut Stashing<Msg>)
  -> Result<Step<Phase>, E>`. Defaulted (safe mechanics):
  `on_defer_full(...) -> Result<Overflow<Msg, Phase>, E>` returning
  `Overflow::Redeliver(msg)` — D6's deliver-to-handle default expressed as a
  verdict the framework interprets (the policy cannot call `handle` itself).
- The cap owns the machine state: `policy: P`, `phase`, `pending:
  Option<Phase>`, `entered_at: Instant`, `stash: Stashing<Msg>` (ADR-0022
  two-queue snapshot, reused).
- **Transition is `cx.cap::<Phased<P>>().goto(next)` recording `pending`;
  commit happens only after the handler returns `Ok`** — D3's
  commit-after-Ok preserved with an imperative verb (a mid-handler panic
  never observes a half-switched phase). `Goto(current)` ≡ no-op at commit.
  `phase()` reads the committed phase. Commit order (D4): switch →
  `entered_at = now` → `unstash_all` (deadline cancel/re-arm implicit via
  `next_deadline`). Commit also runs after `init` (an init-time `goto` must
  not dangle).
- **Admission**: second participation trait `Admission<A>`
  (`admit(&mut self, &mut A, msg, &Handle<A>) -> Result<Admitted<Msg>, E>` +
  `commit(&mut self, &mut A)`), also on the `Caps` bound; `()` = deliver.
  `Shell::handle` runs every message — fresh or replayed — through
  `admit` → `Deliver(m)` calls `A::handle` then `commit`; `Absorbed(flow)`
  covers Defer (stash), Ignore (declared drop), and an overflow handled by
  `on_defer_full`'s `Step`. Replay stays the stage-2 `next_replay` drain, so
  released messages re-gate in the current phase ahead of the backlog
  (ADR-0022; the flat-loop shape of the spike wrapper).
- **Deadline-turn replay**: `Shell::on_deadline` drains `next_replay` after
  the hook via `weak.upgrade()`; in the drain window (no strong ref, no
  message to mint from) the released batch waits for the next step — and
  dies with the incarnation if none comes (ADR-0022 D6 stash-dies-with-actor).

`Step<Ph> { Stay, Goto(Ph), Stop }` returns from the two policy hooks (its
D3 value-return home on this surface); `Disposition { Deliver, Defer,
Ignore }` unchanged; `Overflow<M, Ph> { Redeliver(M), Handled(Step<Ph>) }`
is the D6 verdict.

## Step map (from `.plans/274-fsm-build.md`, re-seated)

- S1 `PanicReason::OnDeadline` + classification pin — unchanged.
- S2 → runtime-floor defaults on `actor::Actor` (not the user trait) +
  `DeadlineHook`/`Deadlined`/`DeadlinePolicy` in `caps.rs` + `Shell`
  forwarding + derive emission.
- S3 loop arms in `kind.rs` — unchanged shape (v1 recreates `sleep_until`
  per iteration; pinned `Sleep::reset` is the named optimization if S5
  shows pain).
- S4 `tests/deadline_plane.rs` on the CAPS surface (P1a/P2/P3/P4/P5,
  drain-window fire, OnDeadline classification pin, cancel-delay bound,
  ordering pins per loop flavor; P1b stays a doc-comment counter-model).
- S5 `benches/deadline_arm.rs` disabled vs armed-far-future.
- S6/S7 `Phased` + unit tests (Goto=Stay, transition order, gate trio,
  defer overflow, stale-timeout unrepresentable, commit-after-Ok, smoke).
- S8 `tests/phase_equivalence.rs` — idiom = caps actor with `Stashing` +
  manual bookkeeping (incl. a public deadline variant via `send_after`);
  fsm = `Phased` actor; 6 scenarios, mode-blind probes.
- S9 `tests/alloc_phased.rs` — Goto=Stay gross allocs; arming allocates 0.
- S10 job-queue: `Worker` gains Serving/Draining phases.
- S11 mutants baseline / README / coverage baseline.
