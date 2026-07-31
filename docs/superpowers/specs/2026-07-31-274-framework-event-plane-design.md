# Cards #274/#241 — the framework-event plane: loop-owned declarative deadlines

Date: 2026-07-31 · Status: approved design ·
ADR: [ADR-0025](../../adr/0025-framework-event-plane-deadlines.md)
(amends [ADR-0024](../../adr/0024-fsm-behavior-switching.md) D7)

## Problem

ADR-0024 left D7 open: how a state-timeout event reaches `Fsm<S>` with
`Fsm<S>::Msg == S::Msg` (no public envelope). Answering it per-feature is
the trajectory the crate was on (dedicated link channel #195, control lane
ADR-0021, `Signal::Stop`, next: receive-timeout #241 plumbing). The
holistic question: **how do framework-originated, time-driven events reach
actor logic at all?** Design that once; #274 and #241 become consumers.

## The three planes

| Plane | Carrier | Consumer | Status |
|---|---|---|---|
| User | bounded mailbox (menu + `Stop`) | `Actor::handle` | designed (ADR-0001/0021, #114) |
| Runtime ops | unbounded control lane | the run loop | designed (ADR-0021) |
| Framework events | *(this spec)* — deadlines: loop-owned slot; deaths: link channel (#195, kept) | hooks (`on_link_died`, `on_deadline`) | **designed here** (time-driven half) |

## Decisions

- **P-D1 — Declarative deadline, quinn shape.** `Actor::next_deadline(&self)
  -> Option<tokio::time::Instant>` (default `None`): a pure function of
  current state, re-read by the loop after every state-touching step. No
  set/cancel verbs — nothing to forget, nothing to race (the same
  forget-proofing argument as the #231 gate).
- **P-D2 — One hook.** `Actor::on_deadline(&mut self,
  actor_ref: WeakActorRef<Self>) -> Result<Flow, Self::Error>` (default
  `Ok(Flow::Continue)`), run at a turn boundary under the same
  `catch_unwind`/poisoning treatment as `handle`; crash domain
  `PanicReason::OnDeadline`, classified handler-like (NOT
  `is_lifecycle_hook` — restart-eligible; pinned by a build test).
  **Weak, not strong, by drain-window necessity**: `handle`'s strong ref
  is minted from the dequeued message's `self_sender` when
  `self_ref.upgrade()` fails; a deadline fire has no message, and a
  loop-held sender would kill `Collected` (ADR-0003/0010/0020). Deadlines
  keep firing through the drain window; transitions and `Flow` work
  unchanged there, self-sends degrade (upgrade `None`) — the
  `on_panic`/`on_stop` signature family. `FsmActor::on_state_timeout`'s
  ref parameter becomes `WeakActorRef` accordingly (#231 spec amended).
- **P-D3 — Arm placement**: one guarded `sleep_until` arm per loop,
  immediately above the mailbox arm, below all existing housekeeping arms
  (link → [retries → aborts] → **deadline** → mailbox). **No existing
  inter-arm relation changes**; the new arm's own relations — below each
  housekeeping arm, above the mailbox (P1) — all get build-time ordering
  pins. The plain loop (today a straight `poll_mailbox().await`, no
  select) gains its **first** `select!` here; since cancellation is
  observed inside the mailbox arm, a due deadline delays cancel
  observation by at most one hook turn (bounded; pinned by a build test).
- **P-D4 — Fires-once-per-value**: after firing for value `d`, the arm
  re-enables only when `next_deadline() != Some(d)`. Spin-proof by
  construction (P3) — the `DelayQueue` `is_empty()` guard's sibling.
- **P-D5 — Not activity**: control-lane signals and watch registrations
  reset nothing (they never touch actor state). Only handler steps do.
- **P-D6 — Consumers**: `Fsm<S>` derives `next_deadline` from
  `(state, entered_at)` and routes `on_deadline` →
  `FsmActor::on_state_timeout`; **epochs are deleted from the ADR-0024
  design** (staleness unrepresentable: cancel is a synchronous slot
  update in the owning task). #241 expresses reset-on-message as
  `last_activity + T` over the same slot (P4); its verb/menu surface stays
  on #241 — except the same-instant tie, which the plane pre-decides:
  a due deadline fires before a simultaneously-queued message (biased arm
  order; the reverse starves, P1b). #241's ADR records that, it does not
  choose it.
- **P-D7 — Untouched**: ADR-0018 timers (delayed user *messages*); the
  link channel (the plane's death-event lane; folding = migration for no
  gain).

## Loop algorithm (one iteration, any of the three loops)

```text
deadline = actor.next_deadline()
armed    = deadline.is_some() && deadline != last_fired
select! { biased;
  <existing housekeeping arms unchanged: link / retries / aborts>
  () = sleep_until(deadline), if armed:
      last_fired = deadline
      run on_deadline under catch_unwind      // Flow::Stop | Err | panic
                                              //   route exactly as handle's
  poll = poll_mailbox(...):
      <handle_mailbox_step, unchanged>
}
```

Rust cost note (build measures): a `Sleep` registers with tokio's timer
wheel lazily on first poll — a disabled arm creates a struct, registers
nothing. An armed arm re-registers per iteration (O(1); hierarchical
timing wheel, Varghese & Lauck, IEEE/ACM ToN 5(6) 1997); the build decides
recreate-per-iteration vs pinned `Sleep::reset`-on-change, with numbers.

## Executable model record

Scratchpad `spike-274-loop` (pure tokio, paused clock, current_thread;
ephemeral — this table is the durable record and the build card ports it).
Model: biased select {deadline arm (guarded, fires-once), mailbox arm},
`ModelActor { handle, next_deadline, on_deadline }`.

| # | Property | Result |
|---|---|---|
| P1a | Arm above mailbox: deadline due at +10 ms with 50×1 ms backlog fires after ~10 handled messages | green (fired after 10) |
| P1b | **Counter-model**: arm below mailbox starves until the backlog drains — fired only after all 50 | green (placement is structural) |
| P2 | `next_deadline = None` disables the arm: 0 fires, loop drains and exits (no spin) | green |
| P3 | Hook leaves an already-due deadline unchanged: exactly 1 fire, loop still serves traffic | green |
| P4 | Sliding deadline (`last_activity + 20 ms`), touches at t=15/t=30: fire at exactly t=50 (virtual) | green |
| P5 | Deadline due mid-handler (30 ms step, due at 10 ms): observed only after `HandlerDone` | green |

## Amendment trace

- ADR-0024 D7: "epoch-stamped … wrapper-filtered" + open plumbing question
  → superseded by P-D1/P-D4/P-D6 (amendment note added in place).
- #231 spec D7: same amendment note added in place; the spike's 3-alloc
  arm figure (mock envelope `send_after`) is now moot — the plane arms no
  timer task at all.
- Card #274: first-decision checkbox resolved by ADR-0025; epoch
  invariants replaced by plane invariants (card body updated).
- Card #241: mechanism provided by the plane; card scope shrinks to
  surface + policy (comment added).

## Follow-ups

- Build (#274): plane first (trait methods + three loop arms + P1–P5
  ports + equivalence-suite re-run + new-arm ordering pins + armed-arm
  cost measurement), then `Fsm<S>` on top of it.
- #241: API design over the plane (its own ADR; menu-message vs hook
  delivery is that card's call).
- M3 liveliness events: join the plane; no new plumbing.
