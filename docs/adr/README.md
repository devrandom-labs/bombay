# Architecture Decision Records (ADRs)

Durable records of **why** we picked what — the decision, the options we weighed,
the evidence, and the consequences we accepted. An ADR is written when a choice
is (a) hard to reverse, (b) shapes downstream cards, or (c) would otherwise be
re-litigated from memory. It captures the *reasoning at decision time*, not a
running status.

Distilled, still-true knowledge lives in [`docs/`](../); ADRs are the audit trail
behind it. If an ADR is later overturned, add a new ADR that supersedes it and
flip the old one's status to `Superseded by ADR-NNNN` — never rewrite history.

## Format

Each ADR is `NNNN-kebab-title.md` with: **Status** · **Context** · **Options
considered** (with the evidence — benchmarks, primary sources) · **Decision** ·
**Consequences**. Keep it to the essentials; link to code/benches/cards.

Status is one of: `Proposed` · `Accepted` · `Superseded by ADR-NNNN` · `Rejected`.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-mailbox-channel-primitive.md) | Mailbox channel primitive | Accepted |
| [0002](0002-reply-channel-primitive.md) | Reply channel primitive | Accepted |
| [0003](0003-actor-ref-self-reference-and-refcount-stop.md) | `ActorRef` self-reference & ref-count-driven stop | Accepted |
| [0004](0004-recipient-conversion-boundary-erasure.md) | Recipient conversion-boundary erasure | Accepted |
| [0005](0005-loom-shuttle-na-miri-for-ref-model.md) | loom/shuttle N/A; MIRI for the ref-model | Accepted |
| [0006](0006-mutation-viable-ratchet.md) | Mutation gate: viable-count ratchet, not unviable ratio | Accepted |
| [0007](0007-request-builders-hand-rolled-not-bon.md) | ask/tell request builders: hand-rolled `IntoFuture`, not `bon` | Accepted |
| [0008](0008-send-timeout-guaranteed-handback.md) | `SendTimeout(M)`: guaranteed handback via bounded retry | Accepted |
| [0009](0009-registry-papaya-no-trait-seam.md) | Registry: papaya directly, no trait seam | Accepted |
| [0010](0010-single-allocation-actor-ref.md) | Single-allocation `ActorRef`: one RMW per clone | Accepted |
| [0011](0011-watch-capability-runtime-result-not-typestate.md) | Watch capability: runtime `Result`, not a typestate handle | Accepted |
| [0012](0012-restart-accounting-counters-not-window.md) | Restart accounting: two counters, not a sliding time window | Accepted |
| [0013](0013-virtual-actors-not-in-core.md) | Virtual-actor lazy reactivation stays out of the core | Accepted |
| [0014](0014-set-cycle-widen-not-queue-not-block.md) | Set-restart cycles coalesce by widening | Accepted |
| [0015](0015-actorid-process-local-pure-name.md) | `ActorId`: process-local pure name; restart mints a new incarnation | Accepted |
| [0016](0016-tracing-direct-instrumentation.md) | Observability: direct `tracing` instrumentation, no observer seam | Accepted |
| [0017](0017-pipe-to-self-not-reentrancy.md) | Pipe-to-self: detached weak pipe, not reentrancy | Accepted |
| [0018](0018-timer-surface.md) | Timer surface: `send_after` / `send_interval` + `TimerHandle` | Accepted |
| [0019](0019-supervisor-subtree-teardown.md) | Supervisor exit tears down its subtree — signal, bounded join, abort | Accepted |
| [0020](0020-collected-stop-not-restart-worthy.md) | Ref-count death is not restart-worthy (`Collected`) | Accepted |
| [0021](0021-control-signal-lane.md) | Control-signal lane: watch/supervision never queue behind user backlog | Accepted |
| [0022](0022-bounded-stash.md) | Bounded stash: `Stashed<S>` composition, in-step replay | Accepted |
| [0023](0023-handler-stop-return-value-not-out-param.md) | Handler stop signalling: return-value `Flow`, not `stop: &mut bool` | Accepted |
| [0024](0024-fsm-behavior-switching.md) | Behavior switching: `FsmActor`/`Fsm<S>` wrapper, declarative admission | Accepted (D7 amended by 0025) |
| [0025](0025-framework-event-plane-deadlines.md) | Framework-event plane: loop-owned declarative deadlines | Accepted |
