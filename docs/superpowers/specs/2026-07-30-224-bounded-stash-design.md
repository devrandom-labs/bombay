# Card #224 — Bounded stash: deferred message handling (design)

**Status:** Draft for review · **Branch:** `feat/224-bounded-stash` · **Base:** `main@ab575f6` (#225 + #257 merged)

## Problem

A fixed-interface (closed `Msg` menu) actor must handle every variant in every
state or drop it. Multi-phase protocols ("buffer commands until state replayed"
— the nexus-adapter shape) today hand-roll a `Vec<Msg>` inside actor state:
unbounded, untested, re-invented per actor. Card #224 builds the deferral
buffer once, in core, bounded and tested.

## Research grounding (primary sources, no framework lore)

- **De Koster, Van Cutsem, De Meuter — *43 Years of Actors* (AGERE! 2016,
  DOI 10.1145/3001886.3001890), §4.2 + Table 2.** Reception order follows
  interface flexibility: *"In the case of a fixed interface, it makes sense to
  process messages in the same order they arrived in the inbox."* Flexible
  interfaces get out-of-order reception, which *"facilitate[s] what is known as
  'conditional synchronisation'"*. Bombay is fixed-interface → its family
  ordering is FIFO; stash must recover conditional synchronization *without*
  breaking arrival order.
- **Varela — SALSA partial messages (via De Koster §3.2):** *"These partial
  messages are stored in a separate mailbox"* — the fixed-interface deferral
  template is a **separate runtime-owned buffer**, not interleaved user state.
- **Briot, Guerraoui, Löhr — ACM CSur 30(3) 1998:** the underlying need is
  **conditional synchronization** — a state-dependent (not type-dependent)
  acceptance gate; a type system cannot express it, a runtime buffer can.
- **Karmani & Agha, actor-model survey:** the model's one hard law is
  **guaranteed delivery** (fairness). Reordering is legal; *silent drop is not*.
  → bounded stash overflow must refuse loudly and hand the message back.
- **Gordon — *Actor Capabilities for Message Ordering* (arXiv 2502.07958,
  2025):** the static frontier (session-typed actor refs ensuring an actor is
  *"always prepared to handle any message that may arrive"*). Stash is the
  dynamic stand-in; the static version is future/KERI-era work, out of scope.

## Decisions

### D1 — Stash is a field of the actor; opt-in via a default-`None` accessor

The buffer lives **in actor state** (`self.tray: Stash<Msg>`), reached by the
loop through a defaulted method on `Actor`:

```rust
/// Deferral buffer accessor. Default: no stash. Override to opt in.
fn stash_slot(&mut self) -> Option<&mut Stash<Self::Msg>> { None }
```

- `handle`'s signature is **unchanged** (no 4th param, no `Context` — #118's
  rejection of `Context` stands).
- No forked loop: `handle_mailbox_step` (`kind.rs:209`) is the shared
  chokepoint of the plain/linked/supervised loops; making *it* stash-aware
  covers all three. (A `Watch`-style subtrait + separate loop was rejected:
  Rust has no specialization, so a subtrait cannot be detected from the shared
  generic loop — it would force a `spawn_stashing` × {plain,linked,supervised}
  matrix.)
- Non-stashing actors: the default returns `None`; the replay branch is
  statically dead and costs one predicted branch, no clone, no alloc.

### D2 — `Stash<M>`: two queues, snapshot unstash

```rust
pub struct Stash<M> {
    held:  VecDeque<M>, // stash() pushes back
    ready: VecDeque<M>, // unstash_all() moves held → ready; loop drains front
    cap:   Capacity,    // bounds held.len() + ready.len() (reuse mailbox::Capacity)
}
```

- `stash(msg) -> Result<(), StashFull<M>>` — bounded push.
- `unstash_all()` — **snapshot**: appends `held` to `ready`. Messages stashed
  *during* a replay land in `held` and wait for the next `unstash_all` — a
  replayed message cannot re-enter its own batch by stashing alone.
- Single producer (the actor, mid-`handle`), single consumer (the loop):
  plain `&mut`, no sync, no channel.

### D3 — Replay driver: the loop, inside the same message step (the load-bearing mechanic)

Replay runs in `handle_mailbox_step`'s `Signal::Message` arm, **immediately
after `handle_message` returns `Continue`**, draining `ready` front-first
through `handle_message`, cloning the step's live `actor_ref` per replayed
message.

Why this exact placement — verified against the current tree:

- The arm already holds a strong `actor_ref` in **both** liveness regimes
  (`kind.rs:257-265`): steady state upgrades the external allocation; the
  drain window mints one from the dequeued message's `self_sender`
  (ADR-0003/0010). That ref's sender keeps `sender_count ≥ 1` for the whole
  batch.
- **Any other placement is unsound.** A top-of-loop drain would need
  `WeakActorRef::upgrade`, which is keyed on the *external* handle refcount and
  documented to return `None` in the drain window while queued messages still
  self-pin (`actor_ref.rs:587-592`). A replayed bare `A::Msg` carries no
  `self_sender` fallback → no handler ref → breaks the `tell; drop` pattern
  ADR-0003 exists to protect. (This bug was caught in design review of an
  earlier sketch.)
- Consequence, for free: replay is always driven by a live handler, so a live
  sender always exists — "spontaneous replay" is structurally impossible, not
  merely forbidden.

Ordering guarantee: the batch drains **before the loop polls the mailbox
again** → replayed messages run ahead of the entire mailbox backlog and any
new arrivals, in stash-arrival order. Priority becomes:
**control > in-step replay > user backlog > new arrivals.**

Replayed messages behave exactly like delivered messages: own `stop` flag
(`Break(Normal)` honored), errors/panics route through `on_panic`, each gets a
handle span (tagged as replay; exact trace shape is an implementation detail
of `trace.rs`).

### D4 — Stash does not pin the actor

Stash holds bare `A::Msg` — no `self_sender`, no strong ref. An actor whose
only unfinished business is a non-empty stash ref-count-stops (`Collected`,
ADR-0020), exactly like an armed timer (ADR-0018 precedent). No immortal
actor: with no external refs no handler can run, so `unstash_all` can never
fire, and held messages drop with the state.

### D5 — Overflow: `StashFull<M>`, total handback

```rust
#[derive(thiserror::Error, Debug)]
#[error("stash full (capacity {cap})")]
pub struct StashFull<M> { /* msg: M, cap: Capacity */ }
```

Follows the `TellError<M>` precedent (`error.rs:27`): payload carried back in
full (`msg()` total), never in `Display`, never panics, never drops —
guaranteed delivery pushed back to the only party who can decide (the
handler: reply with an error, shed load, or escalate). Capacity hit =
`Result`, house rule.

### D6 — Stop-fate table: stash never outlives its incarnation

Only `unstash_all` rescues deferred messages. Every terminal path drops
what remains (held + undrained ready):

| Stop path | Reason | Stash fate |
|---|---|---|
| Ref-count collection (mailbox closed) | `Collected` | dropped (D4: never pinned) |
| In-band `Signal::Stop` | `Normal` | dropped — Stop abandons the queued backlog (ADR-0003); the *deferred* backlog ranks no higher |
| Out-of-band `stop()` (cancel token) | `Normal` | dropped |
| `kill()` (abort) | `Killed` | dropped with the task |
| Handler/hook panic or `Err` | `Panicked` | dropped with the poisoned state |
| Replayed message sets `stop` / panics | as above | remainder of batch + held dropped |

Between steps `ready` is empty (a batch fully drains within its step), so no
stop path can observe a half-replayed batch except by interrupting the step
itself (kill/panic) — covered above.

### D7 — Restart hygiene is structural

Supervised restart = new incarnation built from `Args` via `on_start`. The
stash is a state field, so it dies with the old incarnation and the new one
constructs fresh — a stale stash *cannot* leak across incarnations. Asserted
by test anyway (card checkbox; #149 lesson).

### D8 — Capacity is the actor's constructor parameter, not a `SpawnConfig` field

`Stash::bounded(cap)` is called wherever the actor builds its state (normally
`on_start`). Not a global, and deliberately **not** a field on `SpawnConfig`
(`spawn.rs:77`, landed with #257: `{ capacity, on_stop_grace }`): the stash is
opt-in per actor *type* (D1), so a spawn-site knob would exist for every
actor — including the non-stashing majority — and would hand the spawner
control over a buffer whose very existence is the actor's internal choice.
An actor that wants a spawn-site-tunable stash threads the capacity through
its own `Args`.

## Documented limitations

- **L1 — replay is message-step-driven.** `unstash_all` called from
  `on_link_died` (or any non-`handle` hook) takes effect at the next
  message-driven step, not immediately: the only sound replay site is inside a
  message step (D3). Documented on `unstash_all`; if a real use case needs
  hook-triggered replay, that is a follow-up card.
- **L2 — self-inflicted livelock is possible.** A handler that stashes a
  message and calls `unstash_all` on every delivery of it replays forever
  without yielding to the mailbox. Same class as an actor `tell`-ing itself in
  a loop: a user logic bug, documented, not runtime-prevented.

## Invariant → test map (card checkboxes)

| # | Card invariant | Test (all TDD: fail first) |
|---|---|---|
| 1 | Bounded API, capacity as constructor param | unit: `Stash::bounded(cap)`; cap enforced across `held + ready` |
| 2 | Overflow typed handback | unit: cap-th+1 `stash` returns `StashFull`; `assert_eq!` recovered msg == sent msg |
| 3 | Replay order (among stashed AND vs backlog) | integration: stash A,B; queue backlog D behind trigger T; `unstash_all` in T → handled order exactly `[T, A, B, D]` |
| 4 | Graceful stop, ref-count | non-empty stash, drop all refs → stops `Collected` within bound (also proves invariant 7) |
| 5 | Graceful stop, in-band `Signal::Stop` | stash msg, send Stop → stops `Normal`, stashed msg never handled (probe counter) |
| 6 | `kill()` | stash msg, kill → `Killed`, stashed never handled |
| 7 | Liveness/pinning | = test 4: stash does not pin (Collected reachable) |
| 8 | Panic + restart: no stale stash | supervised actor stashes, panics, restarts → new incarnation's stash empty (probe via ask) |
| — | Snapshot semantics (D2) | unit: stash during replay lands in `held`, not the draining batch |
| — | Mid-batch stop (D6 last row) | replayed msg sets `stop` → remaining batch never handled |

Proptests (if any) use the `prop_` prefix (MIRI-sweep contract).

## Wiring (card checkboxes)

- ADR-0022: bounded stash — field-in-actor + loop-driven in-step front-replay,
  non-pinning, stop-fate table.
- `mutants-baseline.json` entries for every new fn (`Stash::*`, `stash_slot`,
  the replay drain helper).
- README public-API bullet (`Stash`, `StashFull`, `Actor::stash_slot`).
- Walking skeleton: extend `crates/core/examples/job_queue/` + its integration
  test (`crates/core/tests/app_job_queue.rs`) — e.g. a maintenance/pause phase
  that stashes `Submit` until resumed. Shipped or explicitly deferred on-card.

## File layout

- `crates/core/src/stash.rs` — new module: `Stash<M>`, `StashFull<M>` + unit tests.
- `crates/core/src/actor/mod.rs` — `Actor::stash_slot` defaulted method.
- `crates/core/src/actor/kind.rs` — replay drain in `handle_mailbox_step`'s
  `Signal::Message` arm (after `Continue`), conditional `actor_ref` clone
  gated on `stash_slot().is_some()`.
- `crates/core/src/lib.rs` — module + re-exports.
- `docs/adr/0022-bounded-stash.md`.

## Out of scope (explicit)

- `unstash(n)` partial replay — card scope is `unstash_all`-shaped; add at
  second concrete use (YAGNI).
- FSM-ergonomics coupling (gen_statem-style postpone-on-transition) — the
  paired design lands with the FSM card, not here.
- Static ordering capabilities (Gordon 2025) — type-system project, M3+/KERI era.
