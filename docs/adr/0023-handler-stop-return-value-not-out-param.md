# ADR-0023: Handler stop signalling — return-value `Flow`, not `stop: &mut bool`

Date: 2026-07-31 · Status: accepted · Card: #259

## Context

`Actor::handle` (`crates/core/src/actor/mod.rs`) and `StashActor::handle`
(`crates/core/src/stash.rs`) take `stop: &mut bool`; a handler sets
`*stop = true` to stop the actor cleanly after the current message. The shape
was carried from kameo at the fork and never evaluated for this crate — no ADR
records why an out-param boolean beats a return value or a context verb. Card
#259 is the evaluation.

## Verified facts

- **The out-param is kameo's *internal* plumbing, not its public API.** In the
  reference oracle (kameo `v0.22.2`, `src/actor.rs:252-260`), `stop: &mut bool`
  is threaded through `on_message`/`handle_dyn` — framework plumbing — and the
  user-facing surface is `Context::stop()` (the doc on line 251 says exactly
  that). Bombay's reply redesign (#115/#118) dropped `Context`, which silently
  promoted the internal out-param into the public trait. The signature was
  never *designed* to be public — upstream keeps it hidden.
- **One run-loop consumer, one composition reader.** `kind.rs::handle_message`
  initializes the flag, runs the handler under `catch_unwind`, and maps
  `Ok(()) if stop → Break(ActorStopReason::Normal)`; `Stashed<S>::handle`
  additionally *reads* it to gate replay (`while !*stop`). Every other
  occurrence — 129 `&mut bool` sites total (src 60, tests 55, examples 5,
  benches 9) — is an impl signature, and 114 of them are `_: &mut bool`:
  signature noise in the common case.
- **The Err-vs-stop precedence is prose-and-match-arm-order only.** A handler
  that sets `*stop = true` *and* returns `Err` crashes; the flag is silently
  discarded. The trait doc does imply this (stop is conditioned on "after
  this handler returns `Ok`", `mod.rs:75-77`), but the type happily expresses
  the contradictory pair, the enforcement is one match-arm ordering in
  `handle_message`, and no test pins it — all 12 fixtures that set
  `*stop = true` return `Ok` (verified in the #259 audit). Documented but
  structurally unenforced and untested.
- **The trait surface already speaks return-value flow.**
  `Watch::on_link_died` returns
  `Result<ControlFlow<ActorStopReason>, Self::Error>` — the analogous
  three-outcome decision (continue / stop-with-reason / crash) expressed as a
  value. `handle` is the odd one out.
- **The cancel token is *not* an equivalent mechanism** (this kills the
  "delete the parameter, use `actor_ref.stop()`" option, C2 below).
  `run_until_cancelled` (tokio-util 0.7.19,
  `src/sync/cancellation_token.rs:300-313`) is observed only at the mailbox
  arm, and the linked/supervised loops poll their `biased;` link → retries →
  deferred-abort arms first (`kind.rs::run_linked_message_loop`,
  `run_supervised_message_loop`). A token stop set inside a handler therefore
  still dispatches every ready death notice to `on_link_died` before the loop
  observes it. Today's flag makes the handler's decision and the loop's break
  coincide within one step — nothing is observed in between; the token cannot
  express that coupling, so replacing the flag with it would be an observable
  *semantic change* (post-decision hook dispatches), not a refactor. (#266
  pins notice-drops after the *loop's* break decision; the token option would
  not violate that letter — the loop would simply decide later — which is
  precisely why it is a weakening.) Step-synchronous stop is expressible only
  inside the handler step itself; the design question is only its *shape*.
- **Composition must observe the decision synchronously.** `Stashed<S>`
  (ADR-0022) replays stashed messages *within* the current `handle_message`
  step and short-circuits replay on stop (`while !*stop`,
  `stash.rs::Stashed::handle`). Any handler-wrapper composition needs the
  inner handler's stop decision in-step; a side-effect on shared runtime state
  (token, context flag) hides it.

## Research grounding

In the actor model the response to a message *includes designating the
behavior for the next message* — the handler's result determines the actor's
continuation (G. Agha, *Actors: A Model of Concurrent Computation in
Distributed Systems*, MIT Press, 1986: an actor's response to a communication
comprises the communications it sends, the actors it creates, and the
replacement behavior it designates). A return value is the direct encoding of
that semantics; an out-param is an encoding of it through mutable shared
state. Both shapes exist in production frameworks (return-tuple and
returned-behavior styles vs. context-verb styles); per the house rule,
implementations are design-space evidence, not authority — the decision below
rests on this crate's own invariants.

## Options considered

**A. Keep `stop: &mut bool`, document it.** Zero churn. But: the common case
(never stopping) pays permanent signature noise (`_: &mut bool` in nearly
every impl); the Err-discards-stop precedence stays unenforceable by
construction; the public API keeps exposing what upstream designed as hidden
plumbing; and `*stop = true; …; *stop = false` (un-stopping) remains
expressible with no meaning. Documentation would explain the wart, not remove
it.

**B. Return `Result<Flow, Self::Error>`, `enum Flow { Continue, Stop }`.**
The three run-loop outcomes become the three inhabitants of the return type —
`Ok(Flow::Continue)`, `Ok(Flow::Stop)`, `Err(e)` — one value per outcome,
exhaustively matched at the single consumer. The Err-vs-stop ambiguity becomes
*unrepresentable* rather than documented. Consistent with `on_link_died`.
Cost: every handler return site states its lifecycle effect explicitly
(`Ok(Flow::Continue)`) — A's signature noise moved to return position, in
exchange for a compiler-checked decision.

**C. `ctx.stop()` — reintroduce a context parameter.** Rejected: #115/#118
deliberately dropped kameo's `Context` (reply ports and request builders
replaced it; ADR-0007/0008); resurrecting a struct to carry one boolean is the
unused-abstraction shape the API rules ban, and as a side-effect mechanism it
shares C2's composition problem.

**C2. Delete the mechanism; in-handler stop = `actor_ref.stop()`.** Rejected
on semantics, not taste: the token is observed after the biased housekeeping
arms (see Verified facts), so it cannot express "stop before observing
anything further" — the step-synchronous coupling today's flag provides (see
Verified facts; a semantic change, not a refactor) — and `Stashed` replay could not
see the decision without peeking `pub(crate)` loop state.

**D. Return `Result<ControlFlow<()>, Self::Error>` (std vocabulary).**
Same structure as B, worse reading: `Ok(ControlFlow::Break(()))` at every
stop site, and a unit `Break` next to `on_link_died`'s reason-carrying
`Break` invites the wrong generalization (see Decision, last bullet).

## Decision

**B.** `Actor::handle` and `StashActor::handle` drop the `stop` parameter and
return `Result<Flow, Self::Error>`:

```rust
/// The handler's continuation decision: keep running, or stop cleanly
/// (reason `Normal`) after this message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Continue,
    Stop,
}
```

- `Flow` lives in `actor/` and is exported alongside the trait; it is
  exhaustive (no `#[non_exhaustive]`, house rule) and fieldless.
- `handle_message` maps `Ok(Flow::Stop) → Break(ActorStopReason::Normal)` —
  byte-for-byte today's semantics: finish the current handler, stop with
  `Normal`, abandon the backlog, run `on_stop`.
- `Stashed<S>::handle` short-circuits replay on `Ok(Flow::Stop)` — replay
  semantics (ADR-0022) unchanged.
- **`Flow::Stop` deliberately carries no reason.** `on_link_died`'s
  `ControlFlow<ActorStopReason>` *propagates a reason that already exists*
  (the peer's death); a handler stopping itself has exactly one honest reason,
  `Normal` — letting it fabricate `Killed`/`Panicked`/`LinkDied` would lie to
  watchers and supervisors (`is_normal()` drives link propagation and restart
  verdicts). The two types differ because their authority differs.
- Zero-box/closed-menu model untouched: `Flow` is a return value on the
  stack — it never rides a queued envelope, so the #114 slot-size tripwires
  and the #207 allocation guards are out of scope by construction.

## Consequences

- **Breaking signature change, pre-1.0, owned here** (the ADR-0021 precedent).
  The mechanical migration (129 sites incl. benches, overwhelmingly fixtures swapping
  `_: &mut bool` for `Ok(Flow::Continue)`) is follow-up card #271 — this card
  (#259) decides only.
- The follow-up card carries the walking-skeleton bullet: the job-queue
  dispatcher's `finish_drain_if_quiet(&mut self, stop: &mut bool)` becomes
  `finish_drain_if_quiet(&mut self) -> Flow`, and the drain arms return it —
  the match yields the flow value instead of mutating through three arms.
- `Actor::handle`'s trait doc is rewritten around the return protocol in the
  follow-up; the undocumented Err-precedence note dies with the flag.
- The in-band `Signal::Stop` (FIFO, ADR-0021), the out-of-band token stop
  (`ActorRef::stop`), and `kill` are all untouched — this ADR only reshapes
  the *step-synchronous* stop surface.
