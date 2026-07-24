# Links & death-watch — design (card #120, slice 1 of the supervision epic)

**Status:** draft 2026-07-23 · epic #122 · follows the per-card rigor contract there.

## What this is

The **death-watch** half of #120: an actor learns, reliably, when another actor
it watches has stopped — on **every** exit path (normal, panic, kill). This is
the mechanism that makes the run-loop's currently-no-op `Signal::LinkDied` arm
(`kind.rs:59`) real.

It ships two verbs on one mechanism:

- **`watch(&target)`** — one-directional notify-only (Erlang `monitor/2`).
- **`link(&peer)`** — bidirectional, propagating (Erlang `link/1` + `trap_exit`).

## Scope — slice 1 of #120, NOT the whole card

#120 as written bundles four subsystems (link graph, death-watch, `Supervisor`
trait, restart policy/strategies, ~2,588 LOC in the kameo oracle). Per CLAUDE
rule 3 and card #166, that is sliced. **This card is slice 1: links + death-watch
delivery + the `Watch` reaction hook.** It deliberately does **not** build:

- **Restart** — `RestartPolicy`, `should_restart`, intensity limits (MaxR/MaxT) → **slice 2** (`Supervisor: Watch`).
- **Supervision strategies** — `OneForOne`/`OneForAll`/`RestForOne` → **slice 2**.
- **The restart factory** (`SpawnFactory`, re-run `on_start` from `Args`) — this is where the surviving `dyn` erasure lives → **slice 2**.
- **Remote death-watch** — per-actor Zenoh liveliness token (#8's remote tier) → later, gated on the Zenoh layer.

Slice 2 gets its own card + spec. Deferrals are recorded here and must land on
the target card, not in silence (#166).

## The reference, and why it diverges

kameo's `src/links.rs` (548 LOC) + `src/supervision.rs` (2,040 LOC) are the
oracle. bombay's death-watch diverges from it in three load-bearing ways, each
grounded below:

1. **Edge storage** — kameo hangs `Links(Arc<Mutex<LinksInner>>)` off every
   `ActorRef` and takes a hand-ordered **two-lock** on `link` (`actor_ref.rs:922`).
   bombay stores each actor's watcher list **task-local** and installs edges by
   **message**, so there is no lock and no lock-ordering rule.
2. **Delivery channel** — kameo routes death through the child's **generic**
   `Signal<A>` mailbox (bounded), which forces `Box<dyn SignalMailbox>` erasure
   and can lose a notice when the mailbox is full. bombay gives death its **own
   unbounded, non-generic channel**, which removes the erasure and the loss.
3. **Storage cost** — kameo's `Arc<Mutex<Links>>` in `ActorRef` regresses the
   single-allocation clone (#186/ADR-0010). bombay adds only an
   `Option<Sender>` field to the existing one shared allocation — clone stays
   1 Arc RMW.

## Research grounding (primary sources)

The two canonical actor systems **converge** on the design below; this is not a
preference. Sources: the Erlang/OTP reference manual (Processes / Signals) and
the Akka message-delivery-reliability docs.

- **Death is a separate delivery primitive, not a user message.** Erlang:
  exit/`DOWN` signals are "received asynchronously and automatically," becoming a
  message only at reception. Akka: *"internal system messages have their own
  mailboxes."* → **separate link channel**, not the message mailbox.
- **That channel is unbounded; neither system caps monitors/links.** Erlang
  mailboxes grow until OOM; Akka's system queue is `UnboundedMessageQueueSemantics`.
  → **unbounded link channel**, no out-degree cap.
- **The unbounded-mailbox wart is a *user-message* problem** (unbounded
  producers; `pobox`, load-shedding exist for that). Death notices are
  **one-shot per source**, so the stream is finite by construction — the
  pathology does not apply. bombay is strictly better than Erlang here: the
  **user mailbox stays bounded** (where floods are real), only the **death
  channel is unbounded** (where the stream is provably finite).
- **Default `link` propagates.** Erlang: a non-`normal` exit from a linked peer
  terminates the receiver unless it traps. → `on_link_died` default = `Break` on
  `linked && abnormal`, `Continue` otherwise; overriding the hook is `trap_exit`.

## Mechanism

### Two channels per (watcher) actor

```rust
// message mailbox (bounded) — inbound control + data, drained by the loop
enum Signal<A: Mailboxed> {
    Message { msg: A::Msg, self_sender: MailboxSender<A> },
    Stop,
    Watch(WatchReg),        // "notify me when you die" — carries watcher id + link_tx + linked flag
    Unwatch(ActorId),
}

// link channel (UNBOUNDED) — inbound death events. NON-generic payload.
struct LinkDied { id: ActorId, reason: ActorStopReason, linked: bool }
```

`LinkDied` **leaves** `Signal` (it is `Signal::LinkDied(Box<..>)` today,
`mailbox.rs:192`) and becomes the link channel's payload — **unboxed**, since it
no longer shares a slot with the hot `Message` variant. The scaffold `StopReason`
(`mailbox.rs:142`) is retired; the notice carries the real
`ActorStopReason` (`error.rs:293`), the #113/#120 unification the scaffold
anticipated.

### No `dyn` — the erasure #122-#10 predicted does not apply here

`LinkDied` is monomorphic (`ActorId` + `ActorStopReason` are both non-generic),
so the sender into a watcher's link channel is a single concrete type,
`flume::Sender<LinkDied>`, regardless of the watcher's actor type. A watched
actor's list is therefore **homogeneous**:

```rust
// (watcher id, its link channel, was-this-a-link-edge) — the `linked` flag is
// recorded per-edge at install and stamped onto each notice at death.
watchers: SmallVec<[(ActorId, flume::Sender<LinkDied>, bool); 1]>   // no Box<dyn>
```

**This revises the #122-#10 note** (*"erasure lives HERE… `Box<dyn SignalMailbox>`
edges"*). That erasure is a consequence of kameo routing death through the
generic `Signal<A>` mailbox; giving death a non-generic channel removes the
generic from the payload and the `dyn` with it. Erasure does not vanish from the
feature — it **relocates to slice 2's restart factory** (re-running `on_start`
from `Args` over a heterogeneous child set genuinely needs an erased closure,
kameo's `SpawnFactory`). Stop/kill need no `dyn` either (the non-generic
`CancellationToken`/`AbortHandle` already on `ActorRef`).

### Container: `SmallVec<[_; 1]>`, uncapped

- **Uncapped** (spills to heap) — matches OTP (no monitor/link limit). A hard cap
  would make death-observation an exhaustible resource: a watcher silently unable
  to register is a missed death, the worst bug in this subsystem. `ArrayVec`
  (fixed cap, `Err` past N) is therefore **wrong** unless a watcher-count limit
  is a deliberate product decision — it is not.
- **`[_; 1]` inline** covers the 0-or-1-watcher majority with **zero heap ever**;
  the empty inline `SmallVec` is what makes *universal watchability* free.
- New workspace dep `smallvec`; requires a `fuzz/Cargo.lock` bump (the #119
  gotcha) or `nix flake check` breaks.
- **Recorded OTP divergence:** repeated `watch` edges match Erlang (repeated
  `monitor/2` calls create independent monitors), but repeated `link` edges
  diverge — Erlang keeps *at most one* link per process pair ("there can only
  be one link between two processes", Reference Manual, Processes). bombay keeps
  duplicates for both verbs: a duplicate link delivers a duplicate notice, whose
  first `Break` wins — harmless, and dedup would cost a scan on the hot apply
  path. `unwatch` is likewise coarser than `demonitor`: it removes **every**
  edge for that watcher id, watch and link alike.

### Delivery: drop-guard + unbounded channel = no missed death

Rust has **no async `Drop`** and the message mailbox is **bounded**, so a notice
emitted from a graceful path would be skipped on kill (`Abortable` drops the
loop future, `spawn.rs:125`) and could fail to enqueue on a full mailbox. Both
are closed by:

1. A watched actor's watcher list lives in a **guard owned by the task**, so its
   `Drop` runs on **every** exit path — normal return, panic unwind, and
   `Abortable` cancellation.
2. Each notify is a **non-blocking send into the watcher's unbounded link
   channel**, which cannot fail for lack of room. A dead watcher's receiver is
   gone → `send` returns `Err` → the edge is skipped (self-pruning stale edge).

```rust
struct Watchers {
    me: ActorId,
    list: SmallVec<[(ActorId, flume::Sender<LinkDied>, bool); 1]>,
    reason: Option<ActorStopReason>,   // set by the loop before a graceful exit; None => Killed
}
impl Drop for Watchers {
    fn drop(&mut self) {
        let reason = self.reason.take().unwrap_or(ActorStopReason::Killed);
        for (_, tx, linked) in self.list.drain(..) {
            let _ = tx.try_send(LinkDied { id: self.me, reason: reason.clone(), linked });
        }
    }
}
```

**In-flight race (handled in TWO places — review-driven correction):**
`Signal::Watch` rides the bounded message mailbox, so it can be queued but not
yet applied when the target dies. Two windows exist, and the graceful teardown
drain closes only the first:

1. **Queued at loop exit (graceful):** `finish_actor` drains the mailbox and
   applies pending `Watch`/`Unwatch` before firing the guard — those watchers
   get the real reason.
2. **Accepted after the drain snapshot** — during `on_stop` (the mailbox stays
   open until the receiver drops), or still queued at an `Abortable` kill (the
   drain never runs). A successful `send` here previously vanished in
   `MailboxReceiver::drop`'s leak-fix drain: a silently missed death, the exact
   #100-class hang. Fix: the receiver carries its actor's id
   (`Mailbox::bounded(cap, id)`), and its `Drop` answers every drained
   `Signal::Watch` with a synthetic
   `LinkDied { id, reason: AlreadyDead, linked }` — the receiver's drop is the
   last code that ever sees those registrations, on both windows.

Named tests: `watch_in_flight_at_kill_still_notified` (queued-at-kill, notified
by the receiver drop) and `watch_accepted_during_on_stop_still_notified`
(graceful window).

**Synthetic reason = `AlreadyDead`, not `Killed` (review-driven correction):**
Erlang deliberately delivers a distinct `noproc` for link/monitor-to-dead — the
target's true reason is unknowable and must not be conflated with a real hard
kill (one variant per failure domain; slice 2's supervisor branches on reason).
`ActorStopReason::AlreadyDead` is abnormal (`is_normal() == false`), so a linked
default hook propagates it exactly as Erlang's non-`normal` `noproc` exit signal
terminates a non-trapping linked peer.

### Role split: watchable is universal, watching is opt-in

| role | who | needs | cost when unused |
|---|---|---|---|
| **being watched** (target) | *any* actor | accept `Signal::Watch`; notify list at teardown | empty inline `SmallVec` — **free** |
| **watching + reacting** (watcher) | *only* actors that `watch`/`link` | a link channel + `on_link_died` | **nothing** — no channel, no hook |

Being watched is passive machinery in every actor's loop (no trait method).
Watching is the opt-in **`Watch: Actor`** supertrait, entered via
**`spawn_linked`**:

```rust
trait Watch: Actor {
    fn on_link_died(&mut self, id: ActorId, reason: ActorStopReason, linked: bool)
        -> impl Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send
    {
        // default: OTP semantics. link + abnormal => die; else observe and continue.
        async move {
            Ok(if linked && !reason.is_normal() {
                ControlFlow::Break(ActorStopReason::LinkDied { id, reason })
            } else {
                ControlFlow::Continue(())
            })
        }
    }
}
```

- `Watch` is **not** `Supervisor`. Watching (get notified) is strictly less
  authority than supervising (restart). Slice 2's `Supervisor: Watch` adds
  restart *policy* on top. Capability stack: `Actor ⊂ Watch ⊂ Supervisor` (POLA).
- The #116 note's reason to quarantine `on_link_died` off base `Actor` (preserve
  `dyn Actor` object-safety) is **moot**: base `Actor` is already `Sized` and
  non-object-safe (its hooks return `impl Future`), and no `dyn Actor` exists.
  `on_link_died` is placed on `Watch` because **not every actor watches**, not
  for object-safety.
- Overriding `on_link_died` to return `Continue` for a linked abnormal death **is
  `trap_exit`**.

### Watcher-side statelessness: `linked` rides the notice

Whether a death should propagate (`link`) or merely notify (`watch`) is decided
per-edge, recorded on the **target's** watcher entry when the edge is installed,
and **stamped onto the `LinkDied` notice** (`linked: bool`). The watcher then
reacts from the notice alone — it keeps **no out-edge set**. `link` needs no
process-wide `trap_exit` flag; the finer per-edge granularity is a deliberate
superset of Erlang's process-level flag.

### Wiring: `Option<Sender>` in the one shared allocation

For external `a.watch(&b)`, B needs A's `link_tx`, so it must be reachable from
`ActorRef<A>`. Only watcher actors have a link channel, so:

```rust
struct RefShared<A: Actor> {
    sender: MailboxSender<A>,
    cancel: CancellationToken,
    abort:  AbortHandle,
    link_tx: Option<flume::Sender<LinkDied>>,   // None for plain actors; Some for Watch actors
}
```

- **Clone stays 1 Arc RMW** — `RefShared` is still one allocation; adding a field
  does not add an Arc (#186/ADR-0010 intact).
- **Leaf actors pay 8 bytes** (a niche-optimized `None`) and **allocate no
  channel** — the channel allocation is now perfectly correlated with use.
- **`watch` is one hop** — read `link_tx`, send `Signal::Watch` to the target.
- **The verbs are `async`** (`watch`/`link`/`unwatch`) and register with
  `mailbox_sender().send(..).await`, **not** `try_send`. This was a review-driven
  correction: `try_send` fails identically on a *full* mailbox (target alive, just
  busy) and a *closed* one (target dead), so a `try_send`-based `register_on`
  synthesized a spurious `LinkDied { Killed }` under ordinary backpressure —
  causing the caller's default linked hook to `Break` and **self-terminate on a
  busy peer**. `send(..).await` waits for capacity (true backpressure, the design's
  stated watch semantics) and returns `Err` *only* on closed → the synthetic
  link-to-dead notice fires solely for a genuinely dead target. (kameo's `link`
  is async for the same reason.)
- `watch(&b)` bounds `A: Watch`; `link(&b)` bounds `A: Watch` **and** `B: Watch`
  (both react). `link` **pre-checks both `link_tx` are `Some` before either
  registration** (no half-link: a plain-spawned `Watch` peer can't leave a live
  one-directional propagating edge behind an `Err`). A `send` to an already-dead
  peer yields an immediate synthetic `LinkDied` to the live side (Erlang's
  link-to-dead rule).
- A `Watch` actor mistakenly plain-`spawn`ed has `link_tx = None`; `watch`
  returns **`Err(ActorNotLinked)`**, never panics. A compile-time typestate
  (a `LinkedActorRef` witness returned by `spawn_linked` — no negative bounds
  needed) was evaluated and **rejected on cost**, not possibility: handle
  bifurcation infects `Recipient`/registry/#121 — see ADR-0011.

### Loop shape

Two loop bodies sharing the per-signal step helpers (kameo already carries loop
variants in `kind.rs`):

- **plain `spawn` (`A: Actor`)** — one-arm loop over the message mailbox; handles
  `Message`/`Stop`/`Watch`/`Unwatch`; teardown notifies its watcher list. No link
  channel.
- **`spawn_linked` (`A: Watch`)** — two-arm `select!` over `{ message mailbox,
  link channel }`; the link-channel arm calls `on_link_died` and applies its
  `ControlFlow`. Everything the plain loop does, plus reaction.

`on_link_died` runs under `catch_unwind` like the other hooks; a panic in it
becomes `PanicReason::OnLinkDied` (the deferred variant at `error.rs:209`, added
here) → terminal `Panicked`.

## Error / reason model deltas (owned here)

- `ActorStopReason::LinkDied { id: ActorId, reason: Box<ActorStopReason> }` —
  un-defer the variant reserved at `error.rs:290`. `Box` the nested reason
  (large-variant discipline, as kameo does).
- `ActorStopReason::AlreadyDead` — the synthetic link-to-dead reason (Erlang
  `noproc`): distinct failure domain from `Killed`, abnormal so it propagates.
- `PanicReason::OnLinkDied` — un-defer (`error.rs:209`); `is_lifecycle_hook()`
  returns `true` for it (a hook panic must not restart-storm in slice 2).
- New `WatchError::ActorNotLinked` (thiserror; single failure domain).
- `ActorStopReason::is_normal()` already treats `SupervisorRestart` as normal;
  `LinkDied` is **abnormal** (it must be able to propagate).

## Carry-forward from #116 addressed

- **`on_stop`-fails-on-Normal surfaced only via `eprintln!`** (`spawn.rs:190`):
  slice 1 does **not** change this surface (a supervisor reacting to it is a
  slice-2 concern); recorded so it is not silently considered "handled here."

## Testing (TDD — write failing first; the 4 cross-cutting categories first)

Sequence / lifecycle / defensive-boundary / linearizability, per the bombay
testing contract. Named tests, one invariant each:

1. `watch_notified_on_normal_stop` *(sequence)* — target stops normally; watcher
   receives `LinkDied` with `reason == Normal`, `linked == false`.
2. `watch_notified_on_panic` *(lifecycle)* — target dies by handler panic;
   watcher still receives `LinkDied` (guard `Drop` fires on unwind).
3. `@bug watch_notified_on_kill` *(lifecycle)* — target hard-killed (`Abortable`
   drops the loop, no `on_stop`); watcher **still** receives `LinkDied`
   (`reason == Killed`). FAILS if delivery hangs off the graceful path.
4. `watch_in_flight_at_kill_still_notified` *(sequence)* — `Watch` queued but not
   yet applied when the target is killed; teardown drains pending `Watch` and
   notifies. FAILS on the missed-death race.
5. `link_propagates_on_abnormal` *(sequence)* — `a.link(&b)`; b panics; a's
   default `on_link_died` returns `Break` → a stops with `LinkDied`.
6. `link_does_not_propagate_on_normal` *(sequence)* — linked peer stops
   normally; the survivor keeps running.
7. `trap_exit_via_override_keeps_running` *(sequence)* — a `Watch` actor
   overriding `on_link_died` to `Continue` survives a linked abnormal death.
8. `watch_does_not_pin_target` *(defensive)* — watching holds no strong
   `ActorRef`; the target still stops when its last external strong ref drops
   (ADR-0003 intact). Verified with `#151`'s `CountingAlloc` / leak check.
9. `dead_target_watch_immediate_linkdied` *(defensive)* — `watch`/`link` on an
   already-dead peer delivers `LinkDied` at once (Erlang link-to-dead rule).
10. `stale_watcher_edge_self_prunes` *(lifecycle)* — a watcher dies first; the
    target's later notify `send` fails and drops the edge (no leak).
11. `plain_spawned_watch_actor_watch_errs` *(defensive)* — a `Watch` actor
    plain-`spawn`ed returns `Err(ActorNotLinked)` from `watch`, no panic.
12. `many_watchers_all_notified` *(linearizability)* — N concurrent watchers on
    one target (real overlap via `tokio::spawn` + `Barrier`); each receives
    exactly one `LinkDied`. Exercises the `SmallVec` spill.

**Heavier lanes** (per the card): extend `#164`'s bolero loop target and `#117`'s
race probes rather than forking them (the oracle stays single-source); MIRI over
the new sync paths (`#150` lane); `cargo-mutants` zero **new** survivors under the
viable-ratchet gate (ADR-0006), each new fn entered in `mutants-baseline.json`.
loom on the watcher-list/notify interleaving is **noted** but bombay uses MIRI
many-seeds not loom for the ref-model (ADR-0005) — same approach here.

## Deferrals (must land on the named card, per #166)

| deferred | to |
|---|---|
| `RestartPolicy`, `should_restart`, intensity limits | slice 2 — `Supervisor: Watch` |
| `OneForOne`/`OneForAll`/`RestForOne` strategies | slice 2 |
| restart factory (`SpawnFactory`) + its `dyn` erasure | slice 2 |
| `on_stop`-failure programmatic surface | slice 2 (supervisor reaction) |
| per-actor Zenoh liveliness token (remote death-watch) | Zenoh layer (#8 remote tier) |

## Public-API surface added (README delta at card close)

- `trait Watch: Actor` with `on_link_died` (default OTP semantics).
- `ActorRef<A: Watch>::watch(&self, &ActorRef<B: Watch>) -> Result<(), WatchError>`
  and `link(..)`, `unwatch(..)`.
- `spawn_linked` entry point (requires `A: Watch`).
- `ActorStopReason::LinkDied`, `WatchError`.
