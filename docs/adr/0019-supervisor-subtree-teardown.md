# ADR-0019: Supervisor exit tears down its subtree — signal, bounded join, abort

Date: 2026-07-28 · Status: accepted · Card: #245 · Spec:
`docs/superpowers/specs/2026-07-28-245-supervisor-teardown-design.md`

## Context

A supervisor leaving its message loop with reason `Normal`, `Killed`, a
peer-death propagation, or an own-handler panic left its supervised children
running (wart #218/#4). The only sweep site was the escalation arm of
`dispatch_death`. On `kill()` no epilogue could run at all: `Abortable`
wrapped the entire supervised lifecycle.

## Decision

1. **Contract.** A supervisor's exit, for any reason, stops every remaining
   supervised child: cancel (graceful stop signal), then **join** — death
   confirmed via the supervisor's already-installed watch edge on its
   `link_rx` — bounded per child by its `stop_grace`, graces concurrent;
   stragglers are aborted at the bound. Children are never orphaned.
2. **One sweep site.** The sweep runs in the supervised lifecycle epilogue,
   after the loop exits, before `finish_actor`. The `dispatch_death`
   escalation sweep is removed in favor of it. The #195 peer-death path,
   which previously left children untouched by design, now sweeps too —
   deliberate contract revision.
3. **Abortable boundary.** In the supervised lifecycle only, `Abortable`
   wraps prologue + message loop, not the epilogue: `kill()` aborts message
   service, and the sweep still runs. The child table (and the deferred-
   abort queue) live outside the abortable region so they survive the
   abort; actor state, watchers, and the mailbox stay inside — kill skips
   `on_stop` automatically, watchers keep the #195 notice-on-drop path, and
   on kill the death announcement precedes the sweep (graceful paths sweep
   first). Post-abort death confirmation is bounded by
   `ON_STOP_NOTICE_GRACE` — tokio abort is not preemptive, so a
   non-yielding child must not wedge the supervisor's exit. Plain and
   linked lifecycles are unchanged.

## Research grounding

The contract is the convergent answer of three research programs; the
mechanism details are engineering choices where the literature is silent.

- **Structured concurrency.** Parent completion joins children; cancellation
  is signal-then-still-join; no variant permits a child to outlive its
  scope. Lee & Palsberg, "Featherweight X10: A Core Calculus for
  Async-Finish Parallelism," PPoPP 2010 (join half, with proofs). Brockbernd,
  Koval, van Deursen, Kulahcioglu Ozkan, "Understanding Concurrency Bugs in
  Real-World Programs with Kotlin Coroutines," ECOOP 2024, LIPIcs 313
  (peer-reviewed statement of both halves; cancellation cooperative).
  Leijen, "Structured Asynchrony with Algebraic Effects," TyDe 2017 (scoped
  cancellation). Elizarov et al., "Kotlin Coroutines: Design and
  Implementation," Onward! 2021 (design lineage; full text not
  independently fetched — semantics verified via the ECOOP 2024 study).
- **Actor-GC theory.** A live-but-orphaned actor is garbage only when it can
  no longer affect observable behavior, and a correct runtime must then
  collect it; absent such a collector, orphaning is unprincipled. Kafura,
  Washabaugh, Nelson, "Garbage Collection of Actors," ICDCS 1990. Plyukhin &
  Agha, "A Scalable Algorithm for Decentralized Actor Termination
  Detection," LMCS 18(1), 2022. Plyukhin, Agha, Montesi, "CRGC:
  Fault-Recovering Actor Garbage Collection in Pekko," PLDI 2025 (TLA+
  soundness/completeness). Bombay has no actor GC → teardown is mandatory.
- **Erlang formal semantics.** Coq-mechanized Core Erlang: a `'normal'` exit
  does not kill linked processes; only abnormal reasons propagate — the
  link layer alone cannot express supervisor teardown, which is behavior-
  layer. Bereczky, Horpácsi, Thompson, "A Formalisation of Core Erlang, a
  Concurrent Actor Language," Acta Cybernetica 26, 2024 (arXiv:2311.10482).
- **Empirical.** Unterminated actors and mis-sequenced teardown are a
  quantified bug class: IncorrectTermination 8.1 % of symptoms,
  ExplicitLifeCycle 12.4 % of root causes across 186 real Akka bugs.
  Bagherzadeh, Fireman, Shawesh, Khatchadourian, "Actor Concurrency Bugs,"
  OOPSLA 2020.

## Engineering choices the literature does not settle

- **Bounded grace → abort.** No fetched source formalizes a drain-timeout
  between cancel and forced kill (OTP's `shutdown` value and Akka's stop
  ordering are unformalized implementation lore; Akka's unbounded join
  admits a wedged teardown in its own docs). The per-child `stop_grace`
  bound keeps the sweep crash-only and the exit latency `max(grace)`.
- **Abortable boundary placement.** Implementation mechanics; corroborated
  by the reference oracle — kameo v0.22.2 runs an unconditional post-loop
  children-shutdown then waits for children's mailboxes to close on every
  exit path including kill, because its `Abortable` wraps only the inner
  message loop (`src/actor/spawn.rs:257-261` at tag v0.22.2; machinery
  absent in the v0.21.0 bombay forked from).

## Alternatives rejected

- **Documented leak + explicit `stop_children` API.** Manual teardown is the
  empirically documented wrong answer (OOPSLA 2020 root-cause data); every
  consumer re-implements the job-queue workaround.
- **Orphan + automatic collection (CRGC-like).** Principled per the GC line
  but requires a collector bombay does not have; a dataspace-native
  liveliness variant is an M3 conversation, not an M1 dependency.
- **Drop-guard sweep (coerce-style detached `tokio::spawn` from `Drop`).**
  Fire-and-forget: no join, reaper lost on runtime shutdown; kill path gets
  "signals sent, hopefully" instead of the proven contract.

## Resolution (#248)

The residual window is closed by **both** of the following mechanisms, split by
exit path so each covers exactly what the other cannot:

| Path | Mailbox in epilogue? | Mechanism |
|---|---|---|
| Graceful stop (`stop()`, ref-count) | yes | **drain** queued ops into the child table before the sweep |
| Kill (`kill()` → `Err(Aborted)`) | no — dropped inside the aborted future | **drop-guard** on each queued `Add` registration |
| Startup failure | yes, but `startup_failed` needs queued Watch regs | **drop-guard** (draining would eat the watchers) |

1. **Graceful-path drain.** `run_lifecycle_supervised` restructures its epilogue:
   on the `Graceful` arm it drains the supervisor's mailbox **before**
   `pending_aborts.clear()` and `teardown_children`. The drain routes queued
   `Signal::Supervision` ops with the same FIFO semantics the loop uses:
   - `Add` → disarm, `children.insert`, then `install_child_watch` (insert-before-watch,
     #196), so the following sweep can join the child's death instead of burning the
     full grace.
   - `Remove` → `children.remove(id)`: the child is detached and keeps running; it
     is **not** swept.
   - `Stop` → remove, cancel, and schedule a `PendingAbort` on `pending_aborts`; the
     immediately-following `clear()` truncates the grace and aborts the child.
   - `Watch`/`Unwatch` → applied to `watchers` as in `apply_raced_registrations`.
   - `Message` and `Signal::Stop` are discarded.

   The cloned `sup_link_tx` used by the drain is captured before the `Abortable`
   region, because the loop consumes the original copy.

2. **Drop-guard backstop.** `SupervisionOp::Add` now carries an `ArmedReg`: a
   newtype wrapping `Option<SuperviseReg>`. It is disarmed when the loop applies the
   op (`apply_supervision_op`) and when `supervise` gets a mailbox-closed handback
   (so the anchored child keeps running unsupervised). If still armed when dropped,
   the guard cancels and aborts the first incarnation's handle. This covers the kill
   path, startup-failure path, and any `Supervision` op that races a stop and is
   discarded by `apply_raced_registrations`.

Both mechanisms preserve the ADR-0019 invariant: every child (table or queued) is
provably dead or aborted before the supervisor's `RunResult` resolves on graceful
paths; on the kill path the supervisor's own death announcement precedes the child
abort, but nothing is orphaned. The failed-send contract is also preserved: a
`supervise` on a dead supervisor returns `ActorNotAlive`, and the anchored child is
explicitly disarmed so it continues running unsupervised.

## Consequences

- Supervisor exit latency now includes the sweep: bounded by the largest
  live child grace (join completes early when children confirm death — the
  old sweep always slept the full grace).
- `kill()` on a supervisor no longer skips teardown; kill during a hung
  `on_stop` is abandoned by `ON_STOP_NOTICE_GRACE` instead of instant drop.
- The job-queue example drops its manual drain workaround; wart #218/#4
  resolved by this card.
