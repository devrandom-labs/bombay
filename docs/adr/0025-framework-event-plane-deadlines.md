# ADR-0025: Framework-event plane — loop-owned declarative deadlines

Date: 2026-07-31 · Status: accepted (amends ADR-0024 D7) · Cards: #274, #241
· Amended 2026-07-31 by [ADR-0026](0026-core-distillation-one-trait-caps.md):
the SEAT of Decision 1's two methods narrows from every-actor `Actor`
defaults to the `Deadlined` capability (the cap machinery is the "narrower
seam" this ADR said did not exist). All plane semantics — arm placement,
fires-once, WeakActorRef rule, turn-boundary delivery — stand unchanged.

## Context

ADR-0024 left one mechanism open (D7): how a state-timeout event reaches
`Fsm<S>` without a public envelope. Designing that delivery *per feature* is
how the crate has been growing: link deaths got a dedicated channel (#195),
watch/supervision got the control lane (ADR-0021), in-band stop got a
`Signal` variant, and receive-timeout (#241) was next in line for its own
plumbing. Joel's direction: stop plumbing per-feature — design the plane.

The plane in question is the third of three: (1) the **user plane** (menu
messages + in-band `Stop`; bounded, event-ordered), (2) the **runtime-ops
plane** (control lane; loop-consumed, never user-visible), and (3) the
**framework-event plane** — events the framework originates that reach
*actor logic through hooks* (today: link deaths via `on_link_died`; wanted:
state timeouts #274, receive-timeout #241, M3 liveliness events). This ADR
designs the time-driven half of plane (3) once.

## Verified facts

- **The supervised loop already runs loop-owned deadline arms** — the
  restart-backoff `DelayQueue` and deferred-abort arms
  (`kind.rs::run_supervised_message_loop`, arms 2–3), including the
  documented empty-queue spin hazard and its disable-guard idiom. A deadline
  arm extends an in-crate pattern; it is not new architecture.
- **Biased placement is structural, not stylistic** (executable model,
  P1a/P1b): an arm *below* the always-ready mailbox arm starves until the
  backlog fully drains (counter-model: fire after 50/50 queued messages);
  *above* it, the fire lands the step after it comes due (~10/50). A
  deadline arm below the mailbox is a deadline that does not work under
  load.
- **Executable model green on all five properties** (scratchpad
  `spike-274-loop`, pure-tokio model of the loop shape; the durable
  property list lives in the design spec): prompt-under-saturation,
  disabled-arm-no-spin, fires-once-per-value (a hook that leaves its
  deadline unchanged fires exactly once — spin-proof by construction),
  sliding deadline fires at exactly `last_activity + T` under the paused
  clock, and expiry mid-handler is observed only at the step boundary.
- **The declarative-deadline shape is established production Rust**
  (sans-IO driving, docs verified 2026-07-31): quinn's
  `Connection::poll_timeout() -> Option<Instant>` ("the next time at which
  `handle_timeout` should be called", re-consulted after every
  state-changing call) and smoltcp's `Interface::poll_at()` (soft-deadline
  semantics). The state machine *declares* its next deadline; the driver
  owns the clock and delivers expiry.
- **Cost model** (tokio timer = hierarchical timing wheel; Varghese &
  Lauck, IEEE/ACM ToN 5(6) 1997, the O(1)-ops structure): a `Sleep`
  registers with the driver lazily on first poll, so a **disabled arm costs
  a struct construction and no wheel entry**; an armed arm re-registers
  per loop iteration — O(1), constants measured at build; the build may
  hold a pinned `Sleep` and `reset()` only on deadline change.
- **Race-freedom by ownership**: the deadline is checked and fired by the
  same task that runs handlers, so cancel-on-transition is a synchronous
  slot update. The fired-and-queued-before-cancel race that forced
  ADR-0024's epoch stamping **cannot occur** — the epoch machinery was an
  artifact of message-style delivery (the spike mock), not of the problem.
- **#241 fits the plane with no loop-tracked state** (model P4): a wrapper
  that records `last_activity` in `handle` and declares
  `next_deadline = last_activity + T` gets exact reset-on-message
  semantics. Its card's own constraint ("idle detection lives in the run
  loop… NOT a detached task") is satisfied by the plane; its verb/delivery
  surface remains #241's decision.

## Research grounding

Semantics: the **Isolated Turn Principle** (De Koster, Van Cutsem,
De Meuter, *43 Years of Actors*, AGERE! 2016, DOI 10.1145/3001886.3001890)
outlaws blocking/preemption within a turn — time is observable **only at
turn boundaries**, which is exactly where the arm fires (model P5).
**Timed Rebeca** (Reynisson et al., *Modelling and simulation of
asynchronous real-time systems using Timed Rebeca*, Science of Computer
Programming 89, 2014) formalizes actor time this way: non-preemptive message servers,
time advancing between servings, deadlines first-class. Active-object
duration guards (De Boer et al., ACM Computing Surveys 50(5), 2017) advance
time between activations in the cooperative family. Mechanism: the Rust
sans-IO pattern above (quinn/smoltcp) is the production embodiment.
Field note, one line: other ecosystems ship both delivery models
(runtime-checked deadlines and queued timer messages) — there is no single
external convention to defer to, so the decision rests on this crate's own
structure (biased select) and the ITP.

## Options considered

**A. Per-feature delivery (status quo trajectory).** State timeout as a
`Signal`/control variant + epoch filtering (ADR-0024 D7's sketch), then
receive-timeout as its own recv-arm mechanism (#241's sketch), then M3
events ad hoc. Rejected: two mechanisms for the same concept one card
apart; every `Signal` variant ripples through drain paths, receiver-`Drop`,
fuzz targets and MIRI models; and the epoch machinery exists only to patch
a race this option itself creates.

**B. Message-style delivery on the user lane** (`Signal::StateTimeout`,
sender-less). Event-order semantics, minimal loop change — but: pays the
epoch/staleness machinery, adds a `Signal` variant (same ripple), delivers
late behind a saturated backlog (a deadline that queues is a worse
deadline), and does nothing for #241 (which cannot be a queued message —
idle detection needs the loop).

**C. Loop-owned declarative deadline plane** *(chosen)* — one guarded
`sleep_until` arm in each loop, driven by an actor-declared deadline,
delivering through one hook. Epoch-free, task-free, `Signal`-free; serves
#274 and #241 with one mechanism; the quinn/smoltcp shape.

## Decision

1. **`Actor` gains two defaulted methods** (the plane's whole public
   surface):
   ```rust
   /// The next instant this actor needs waking, as a pure function of
   /// current state. `None` = no deadline. Re-read by the loop after
   /// every state-touching step (quinn `poll_timeout` shape).
   fn next_deadline(&self) -> Option<tokio::time::Instant> { None }

   /// Expiry delivery, at a turn boundary, under the same catch_unwind
   /// and poisoning treatment as `handle`. Default: keep running.
   fn on_deadline(&mut self, actor_ref: WeakActorRef<Self>)
       -> impl Future<Output = Result<Flow, Self::Error>> + Send;
   ```
   A panic/`Err` in the hook is a controlled crash tagged with a new
   `PanicReason::OnDeadline` variant (one variant, one failure domain),
   classified **handler-like, NOT `is_lifecycle_hook`** — a deadline hook
   is ordinary state processing (restart-eligible under supervision),
   unlike `OnLinkDied`'s escalate-without-restart. The `matches!` list in
   `error.rs::is_lifecycle_hook` does not change; the build pins this
   with a test (adding a variant compiles either way — silence is not a
   decision).

   **The hook takes `WeakActorRef`, not `ActorRef` — a drain-window
   necessity, not a style choice.** `handle`'s strong ref is minted from
   the dequeued message's `self_sender` when `self_ref.upgrade()` fails
   (the ADR-0003/0010 drain window); a deadline fire carries no message,
   so no mint source exists, and a loop-held sender would keep the
   mailbox open forever and kill `Collected` (ADR-0020). Deadlines
   therefore **keep firing through the drain window** (the arm sits above
   the mailbox arm, so a due deadline fires before the backlog finishes
   draining); inside the hook, transitions and `Flow` decisions work
   unchanged — only self-sends degrade (upgrade returns `None`), exactly
   as in `on_panic`/`on_stop`, whose signatures this follows.
2. **One deadline arm per loop** (plain, linked, supervised), placed
   **immediately above the mailbox arm and below every existing
   housekeeping arm** (link → [retries → aborts] → deadline → mailbox).
   Minimal perturbation: **no existing inter-arm relation changes**; the
   new arm introduces its own relations — below link/retries/aborts (a
   ready death notice or due rebuild beats a due deadline) and above the
   mailbox (the one P1 proves necessary) — and the build's ordering pins
   must cover all of them, not just deadline-before-mailbox. The plain
   loop is today a straight `poll_mailbox().await` with no select; it
   gains its **first** `select!` here. Consequence recorded: cancellation
   is observed inside the mailbox arm (`run_until_cancelled`), so a due
   deadline delays cancel observation by at most one hook turn — bounded,
   and pinned by a build test.
3. **Fires-once-per-value guard** (model P3): after firing for deadline
   value `d`, the arm re-enables only when `next_deadline()` reports a
   value ≠ `d`. A hook that leaves its deadline unchanged cannot busy-loop
   the biased select — spin-proof by construction, the same hazard class
   the `DelayQueue` arms guard with `is_empty()`.
4. **The loop re-reads `next_deadline()` each iteration.** State changes
   only in steps the loop itself runs, so this is exact; no set/cancel
   verbs exist, hence nothing to forget and nothing to race. Control-lane
   signals and watch registrations are **not** activity and reset nothing
   (they never touch actor state).
5. **Consumers:** `Fsm<S>` (#274) implements `next_deadline` from
   `(state, entered_at)` and overrides `on_deadline` to run
   `FsmActor::on_state_timeout` (whose `actor_ref` parameter becomes
   `WeakActorRef`, following the hook rule above) — ADR-0024's epoch
   clause is superseded (staleness is unrepresentable, not filtered).
   #241 builds its reset-on-message surface on the same slot (model P4);
   its API shape stays on that card — with one edge the plane
   **pre-decides**: on a same-instant tie between a due deadline and a
   queued message, the deadline fires first (biased arm order; the
   reverse placement starves, P1b) — "message wins the tie" is not
   implementable on this plane, and #241's ADR records that rather than
   choosing it. Future framework events (M3 liveliness) join the plane
   rather than adding plumbing.
6. **Untouched:** ADR-0018's `send_after`/`send_interval` (user-plane
   delayed *messages* — a different thing than deadlines); the link-notice
   channel (the plane's death-event lane, shipped in #195 — folding it in
   would be migration for no gain).

## Consequences

- **ADR-0024 amendment:** D7's "epoch-stamped, wrapper-filtered" mechanism
  and its open plumbing question are resolved by this ADR; an amendment
  note is added to ADR-0024 and the #231 spec rather than rewriting them.
- **Build (#274)** implements the plane + `Fsm<S>` on top of it: ports the
  model's P1–P5 as in-repo tests, re-runs the drain/supervision
  equivalence suites unchanged, adds ordering pins for the new arm, and
  measures the armed-arm per-iteration cost (pinned-`Sleep`-vs-recreate is
  a build decision, recorded with numbers).
- **#241 shrinks** from "design idle plumbing" to "surface + policy over
  the plane".
- The base `Actor` trait grows two defaulted methods — the cost weighed in
  ADR-0024's hook analysis (no narrower seam exists than the trait; the
  run loop is generic over `Actor`). `tokio::time::Instant` enters the
  trait surface (already implied by the crate's hard tokio dependency:
  the TIME driver is mandatory since the on_stop bound).
- Every actor's loop evaluates one extra guard per iteration; a disabled
  arm registers nothing with the timer wheel (lazy registration).
