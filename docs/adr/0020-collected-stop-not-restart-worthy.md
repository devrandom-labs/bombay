# ADR-0020: Ref-count death is not restart-worthy (`Collected`)

Date: 2026-07-29 · Status: accepted · Card: #253 · Spec:
`docs/superpowers/specs/2026-07-29-253-collected-stop-design.md`

## Context

Two accepted invariants collided:

1. **ADR-0003** makes actor liveness ref-count-driven: the run-loop holds only a
   weak self-ref, so when every strong `ActorRef` drops, the mailbox closes and the
   actor stops (after draining queued messages).
2. **#196** makes `RestartPolicy::Permanent` restart on every exit, including a
   normal stop — "exiting is a bug".

The run-loop collapsed both the cancel-token graceful stop and the all-senders-gone
ref-count stop into one `ActorStopReason::Normal`. `should_restart(Permanent, Normal)
= Restart`, so an unanchored supervised child collected itself, was rebuilt, the
rebuild also collected immediately, and `consecutive` climbed monotonically. With
default tuning (`max_restarts = 5`, `reset_after = 60 s`) the supervisor hit
`RestartLimitExceeded` and escalated, killing its whole subtree in ~3 s of virtual
time. With unbudgeted configs (`max_restarts = max_total = u32::MAX`) it churned
forever.

## Verified facts

- The collapse happened in `kind.rs::handle_mailbox_step`: the two `None` cases
  of `cancel.run_until_cancelled(mailbox_rx.recv())` were flattened into one
  `Option<Signal<A>>`, erasing whether the cancel token fired or the mailbox closed.
- `should_restart` mapped `(Permanent, Normal)` → `Restart`; `Transient` left
  `Normal` dead.
- `spawn_child` drops the strong ref by design; the watch installer's transient
  sender dies after install. An anchorless incarnation therefore collected with
  uptime ≈ 0.
- The registry (#119, ADR-0009) holds **weak** handles only, so a named service
  reachable only by name already ref-count-stopped today under any policy. A durable
  service must be anchored by the app holding a strong ref — already the rule.

## Research grounding

- **OTP has no ref-count death.** `Permanent` fidelity to OTP is not at stake:
  there is no "all senders dropped" event in Erlang/OTP to classify.
- **Actor-GC literature treats collection of an unreachable actor as
  semantically invisible.** Kafura, Washabaugh, Nelson, "Garbage Collection of
  Actors," OOPSLA/ECOOP 1990 (joint conference, Ottawa, pp. 126-134) (an actor
  becomes garbage when it cannot receive messages and cannot affect the observable state). Plyukhin & Agha, "A Scalable Algorithm
  for Decentralized Actor Termination Detection," LMCS 18(1), 2022. Plyukhin,
  Agha, Montesi, "CRGC: Fault-Recovering Actor Garbage Collection in Pekko," PLDI
  2025 (TLA+ soundness/completeness). A supervisor observing collection as a
  failure and rebuilding the actor makes the GC observable, violating the
  literature's invisibility invariant.
- **Empirical blast radius.** Restarting on collection converted a non-failure
  (nobody holds the actor) into the maximal blast radius: subtree teardown via
  `RestartLimitExceeded`. This is the worst possible composition of two
  individually correct invariants.

## Decision

1. **New stop reason.** `ActorStopReason::Collected`: every strong handle was
   dropped and the queue drained — no message can ever arrive again. It is
   classified **normal** (`is_normal() == true`), so links do not propagate it and
   `Transient` already leaves it dead.
2. **Split the collapsed stop in the run-loops.** Introduce `MailboxPoll`
   (`Cancelled`, `Closed`, `Signal`) in `kind.rs`, mapped from
   `cancel.run_until_cancelled(mailbox_rx.recv())` once:
   - `Cancelled` → `Break(Normal)`;
   - `Closed` → `Break(Collected)`;
   - `Signal` → unchanged (`Signal::Stop` and handler-set `stop` stay `Normal`).
3. **`LeaveDead` under every policy.** `should_restart` short-circuits every
   policy for `Collected`, exactly as it does for a lifecycle-hook panic:
   `Collected → RestartVerdict::LeaveDead`. `Permanent` does not resurrect an
   unreachable actor.
4. **Observability.** `handle_child_death` traces `child_collected(child_id)` on
   the leave-dead path so the quiet death is not invisible (#244).

## Engineering choices

- **No `Default` for `RestartPolicy` or `RestartConfig`.** The #196 decision
  survives: with collection no longer restart-worthy, the budgets guard only
  genuine crash loops, which is what they were tuned for. No default policy is
  introduced.
- **`MailboxPoll` is `pub(super)`.** The collapse is fixed inside `kind.rs`; the
  public enum `ActorStopReason` gains exactly one variant.
- **Exhaustive matches.** `ActorStopReason` stays exhaustive (no `#[non_exhaustive]`);
  every match site is updated deliberately. The compiler is the tripwire.

## Alternatives rejected

- **Introduce a default policy.** Surveys show mature supervisors disagree: OTP
  defaults to `permanent`, Kubernetes pods to `Always`, Akka Typed to stop. The
  choice belongs to the caller's semantics, not the framework.
- **Keep restart-on-collection and document the trap.** Tried: the #196 doc block
  already called an unanchored child "actively fatal." The probe showed the trap
  scales to subtree death in seconds; documentation is not enough.
- **Supervisor pins the child.** Rejected in kameo #171 and by ADR-0003: holding
  a strong ref in the supervisor table would make ref-count-driven stop
  unreachable, violating the actor-GC invariant the other way.

## Consequences

- An unanchored `Permanent` child now dies **quietly once** and is left dead.
  The supervisor keeps running; siblings survive.
- The `child_collected` trace event is the witness. Without it the death would be
  silent (the #244 observability concern).
- Anchoring stays the app's job. A dead-but-wanted child is still an app bug; the
  change is that supervision no longer turns that bug into a restart storm that
  kills the subtree.
- `RestartPolicy::Permanent`'s meaning sharpens: "this actor exiting is a bug"
  applies to the actor's own decision to stop. Being collected is the caller
  dropping all refs, not the actor exiting.

## Resolution (#253)

Implemented in `error.rs` (new variant + `is_normal`), `kind.rs`
(`MailboxPoll`/`poll_mailbox` + `child_collected` trace), `restart.rs`
(`should_restart` carve-out), `spawn.rs` (probe and boundary tests),
`tracing_capture.rs` (trace event test), `actor_ref.rs` (doc rewrite), and this
ADR.
