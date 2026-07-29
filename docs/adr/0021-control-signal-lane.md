# ADR-0021: Control-signal lane — watch/supervision ops never queue behind user backlog

Date: 2026-07-29 · Status: accepted · Card: #225 · Plan:
`.plans/225-control-lane.md`

## Context

Before this card, ONE bounded flume channel carried every `Signal` variant
(`Message`, `Stop`, `Watch`, `Unwatch`, `Supervision`). Consequence: watch
registration and supervision ops `.await`ed capacity behind the user backlog —
supervision reacted slowest exactly when the system was loaded. Evidence: the
`supervise`/`watch` sends in `actor/actor_ref.rs` were capacity-blocking
`send().await`s, and the supervisor's watch-installer treated a flooded child
mailbox as a failed incarnation (`WatchOutcome::Full`). Wart 3 of the #218
job-queue example (`docs/warts/218-example-warts.md`) is the same disease one
level up: bookkeeping that rides the bounded mailbox competes with user
backlog.

## Research grounding

- **Erlang/OTP 28, EEP-76 "Priority Messages".** OTP 28 added an opt-in
  priority lane: messages sent to a *priority alias* bypass the regular
  message queue and are received ahead of it — the ecosystem's own
  acknowledgement that some signals must not queue behind a backlog
  (`erlang.org/blog/highlights-otp-28`, `system/doc/reference_manual`
  processes chapter, github.com/erlang/otp).
- **Apache Pekko `ControlAwareMailbox`.** "Very useful if an actor needs to be
  able to receive control messages immediately no matter how many other
  messages are already in the mailbox" (pekko.apache.org/docs/pekko/1.3/
  mailboxes.html) — exactly this card's motivation, as a first-class mailbox
  type.
- **CAF (C++ Actor Framework).** Urgent messages are put into a *different
  queue of the receiver's mailbox* (CAF 0.18 docs, Message Passing) — the
  two-queue shape, in C++.
- **ractor.** Four message classes — signals, stop, supervision — are all
  served ahead of ordinary actor messages, which are "the lowest priority of
  the 4 message types" (github.com/slawlor/ractor README). The closest Rust
  sibling, and it made supervision first-class-priority by design.
- **In-tree precedent.** The unbounded link channel + `biased;` select arm
  (`actor/kind.rs`, `run_linked_message_loop`) already runs a
  control-before-messages merge for death notices, with a closed-arm latch to
  keep a ready `Err` from spinning the biased select. This ADR generalizes
  that shape.
- **The `fastpass` sibling repo** (`../fastpass`) distilled the recv-side
  two-channel biased merge into a standalone crate and drove it with a
  property suite (P1 priority, P2 per-lane FIFO, P3 progress, P4 exactly-once,
  P5 no-lost-wakeup, P6 teardown drain, P7 zero-alloc hot path — green on the
  flume variant). It also hosts a lock-free variant and an anti-starvation
  aging cap, both research-only and explicitly **out of scope** here (they
  land only after their own loom + MIRI hardening).

## Decision

1. **Split the envelope.** `Signal<A>` keeps only `Message` and `Stop` (the
   user lane, bounded). A new **non-generic** `ControlSignal` carries
   `Watch(Box<WatchReg>)`, `Unwatch(ActorId)`, `Supervision(Box<SupervisionOp>)`
   — no payload mentions `A::Msg`, so every actor's control lane is one
   concrete type. This is a breaking change to the public `Signal` enum
   (pre-1.0, owned here).
2. **A second, UNBOUNDED channel.** `Mailbox::bounded` now builds
   `flume::bounded(cap)` (user) + `flume::unbounded()` (control).
   `MailboxSender` carries `tx` + `ctl_tx`; `MailboxReceiver` carries `rx` +
   `ctl_rx`; `WeakMailboxSender` carries both weaks and upgrades both-or-`None`.
   Because every strong handle carries both halves, the lanes share a sender
   count and disconnect together — `is_closed` checks one and speaks for both,
   and ref-count-driven stop (ADR-0003) is untouched: only `Signal::Message`
   embeds a strong `self_sender`; control sends never pin the actor.
3. **The merge lives inside `MailboxReceiver::recv`.** Policy: control
   `try_recv`, then user `try_recv`, else a `biased;` select over both
   `recv_async` futures, control arm first, with a per-lane close latch (the
   link-arm guard pattern) so a disconnected-and-drained arm cannot spin the
   select. `None` only when BOTH lanes are closed and drained. `recv` stays
   cancel-safe — it sits under `run_until_cancelled` and the loops' biased
   selects, and flume buffers items in shared state, so dropping the losing
   arm's future loses nothing (pinned empirically in the fastpass repo,
   `edge_cases::recv_future_cancellation_loses_nothing`). The three run-loops
   keep their `select!` shapes; `poll_mailbox` maps the new `Recv` enum into a
   new `MailboxPoll::Control` arm.
4. **Send sites.** `watch`/`link`/`unwatch`/`supervise`/`unsupervise`/
   `stop_child` and the one-shot `WatchInstaller` now use the synchronous
   `send_control`. The verb **signatures stay `async`** so the card does not
   break its own API twice; the bodies no longer await (there is no capacity
   to wait for).
5. **Teardown moves lanes intact.** The hard-kill path (receiver `Drop` /
   `reject_queued_watchers`) drains the control lane and answers every queued
   `Watch` with the synthetic death notice (#195's obligation); the graceful
   supervised teardown drains it through the same APPLY logic the loop uses
   (`Add` → insert + watch, `Remove` → detach, `Stop` → cancel + deferred
   abort), never the reject path. User-lane drains still release the queued
   messages' `self_sender` pins.

## The accepted relaxations and semantic changes

- **No cross-lane total order.** A control signal enqueued after a user
  message MAY overtake it — that is the feature (cf. EEP-76). User
  FIFO-per-sender is untouched, and the control lane is FIFO *within itself*
  (watch-then-unwatch = no edge; reversed = edge stays).
- **`Stop` stays on the user lane.** A `Stop` must never overtake the messages
  queued before it — "handle everything I already sent, then stop" is the
  whole contract of an in-band stop. Control signals may overtake a `Stop`.
- **`WatchOutcome::Full` is gone.** Pre-split, a flooded child's bounded
  mailbox failed the watch install — the child was killed and synthesized as
  an immediate failed incarnation. On the unbounded lane the install always
  lands, so a flooded child is now watched (late) instead of failed. This is
  an intended IMPROVEMENT (registration is never lost to user backlog — the
  exact point of the card), not incidental fallout.
- **The unbounded lane is rate-bounded, not structurally bounded.** Each
  `watch`/`unwatch`/`supervise` call enqueues exactly one op; `Watchers::apply`
  deliberately keeps duplicate edges (Erlang-style independent monitors), so
  there is no dedup to bound growth. The lane is caller-floodable like any
  unbounded queue — the same trust class as the unbounded link channel. The
  flood test in `crates/core/tests/control_lane.rs` is load-bearing: a
  sustained 10k-op flood grows the lane without panic, keeps intra-lane FIFO,
  and the user lane still drains under the recv policy.
- **`Message` slot grew by one `flume::Sender` (8 B, measured aarch64).**
  `MailboxSender` now carries the control half, so the `self_sender` embedded
  in every `Message` slot is two Arc words (16 B) instead of one. The
  allocation PROFILE is the invariant the #207 guard binaries pin, and it is
  unchanged: zero per-message heap boxes (an Arc clone allocates nothing), and
  the slot-size tripwires in `mailbox.rs` were re-based to the measured sizes
  (`Signal<Probe>` = 32 B, `Signal<Small>` = 40 B, `ControlSignal` = 16 B).

## Measured evidence

Control-delivery latency vs user-queue depth (new `control_delivery_latency`
criterion arm in `crates/core/benches/mailbox.rs`, Apple M4 Pro, one
`send_control` + one `recv` against a pre-filled user lane):

| user-lane depth | latency |
|---|---|
| 0 | 77.2 ns |
| 64 | 78.1 ns |
| 1024 | 80.1 ns |
| at-cap (64/64) | 78.3 ns |

Flat (<4% spread) — the P1 property: control latency is independent of
user-queue depth. Pre-split, the `at-cap` point did not exist: the send
*blocked*. The fastpass repo measured the same shape on its standalone flume
merge (`control_latency_under_backlog`, `cargo bench -p fastpass --bench
twolane`) and holds the property suite green; its one-struct reference variant
(12–31 ns across depths 0–8192) marks the floor a future lock-free backend
could approach — out of scope here.

## Alternatives rejected

- **Priority INSIDE one channel** (a two-tier queue slot or a skip-list): keeps
  one channel but re-implements flume internals we deliberately do not own
  (ADR-0001's seam), and breaks the clean "unbounded control / bounded user"
  split — the two lanes genuinely have different backpressure contracts.
- **Bounded control lane**: reintroduces the exact failure (a flooded child
  unwatchable) with a second capacity to tune. Supervision traffic is
  caller-rate-bounded; the link channel already set the trust-class precedent.
- **Aging cap / anti-starvation counter** (fastpass's P3 machinery): the user
  lane here is drained by the same loop that applies control ops, so a control
  flood already yields to the user lane whenever the flood pauses; a strict
  control-first bias is the documented policy. If a hostile control-flood
  scenario ever materializes, the fastpass aging cap is the researched
  upgrade path.
- **Loop-arm merge** (a fourth `select!` arm per run-loop): rejected early —
  three loop shapes would each fork, the DST seed tests would fork with them,
  and the cancel-safety reasoning would be repeated per loop. Merging inside
  `recv` keeps every loop's `select!` shape byte-identical.

## Consequences

- `watch`/`link`/`unwatch`/`supervise`/`unsupervise`/`stop_child` resolve
  immediately against a full-mailbox peer; their docs no longer promise
  "ordinary backpressure".
- `Signal` loses three variants — breaking, pre-1.0, exhaustive matches are
  the tripwire; `Recv` and `ControlClosed` are new public types.
- A watch or supervision op reaches a loaded actor in ~80 ns regardless of
  backlog depth; the #218 wart-3 class of "supervision reacts slowest under
  load" is closed at the mailbox layer. (`WorkerReplaced` itself still rides
  the user lane by design — it is a user message; #244 tracks it.)
- `nix flake check` gates the suite; MIRI covers the extended
  `prop_fifo_roundtrip_single_sender` (control interleavings) by prefix
  contract.
