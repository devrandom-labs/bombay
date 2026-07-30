# Card #224 — Bounded stash: deferred message handling (design)

**Status:** Draft v2 for review · **Branch:** `feat/224-bounded-stash` · **Base:** `main@ab575f6` (#225 + #257 merged)

v2: design review killed the v1 field-plus-accessor shape (silent forget-trap,
see Rejected alternatives) in favor of a framework-owned composition wrapper.

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
  → bounded stash overflow must refuse loudly and hand the message back; and no
  design may allow a message to be deferred forever by accident (the v1 trap).
- **Gordon — *Actor Capabilities for Message Ordering* (arXiv 2502.07958,
  2025):** the static frontier (session-typed actor refs ensuring an actor is
  *"always prepared to handle any message that may arrive"*). Stash is the
  dynamic stand-in; the static version is future/KERI-era work, out of scope.

## Decisions

### D1 — Framework-owned composition: `Stashed<S>` wraps user state

The stash is owned, wired, and replayed by a framework wrapper type. The user
never declares the buffer, never wires it, and receives it as a `handle`
parameter — **there is nothing to forget**:

```rust
/// The only way to have a stash. Owns the buffer; user state composes in.
pub struct Stashed<S: StashActor> {
    state: S,
    stash: Stash<S::Msg>,
}

/// Opt-in actor shape with deferral. Mirrors `Actor`'s hooks; `handle`
/// additionally receives the stash.
pub trait StashActor: Mailboxed<Msg: Msg> + Sized + Send + 'static {
    type Args: Send;
    type Error: ReplyError;

    /// Stash capacity, from the actor's own constructor input. Required —
    /// bounded is explicit, never a global default (card constraint).
    fn stash_capacity(args: &Self::Args) -> Capacity;

    fn on_start(args: Self::Args, actor_ref: ActorRef<Stashed<Self>>)
        -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn handle(&mut self, msg: Self::Msg, actor_ref: ActorRef<Stashed<Self>>,
              stash: &mut Stash<Self::Msg>, stop: &mut bool)
        -> impl Future<Output = Result<(), Self::Error>> + Send;

    // on_panic / on_stop: defaulted, mirroring `Actor`'s defaults (no stash
    // access — see D6 for the fate of a non-empty stash at stop).
}

impl<S: StashActor> Mailboxed for Stashed<S> { type Msg = S::Msg; }
impl<S: StashActor> Actor for Stashed<S> { /* framework impl, once — see D3 */ }
```

Usage:

```rust
impl StashActor for Shop { /* stash.stash(msg)? ... stash.unstash_all() */ }
let shop = Stashed::<Shop>::spawn(args);   // ActorRef<Stashed<Shop>>
```

Properties:

- **Forget-proof by construction.** `Stash::bounded` is module-private: a
  stash cannot exist outside a `Stashed`, and inside one the replay wiring is
  framework code, written and tested once. Compile-time enforcement over
  runtime discipline — the #206/ADR-0015 house precedent.
- **Zero core surgery.** `Stashed<S>` is a plain `Actor`; `kind.rs` and every
  loop are untouched. All existing machinery — spawn paths, `Recipient`,
  registry, timers, watch (as a watched/supervised *target*), supervision as a
  *child* — works on `ActorRef<Stashed<S>>` unchanged.
- **Zero cost for non-users.** Plain actors don't pass through any new branch;
  the feature lives entirely in a separate type.
- Costs, stated: a second trait surface mirroring `Actor`'s hooks, and
  `ActorRef<Stashed<Shop>>` type noise at use sites.

### D2 — `Stash<M>`: two queues, snapshot unstash

```rust
pub struct Stash<M> {
    held:  VecDeque<M>, // stash() pushes back
    ready: VecDeque<M>, // unstash_all() moves held → ready; replay pops front
    cap:   Capacity,    // bounds held.len() + ready.len() (reuse mailbox::Capacity)
}
```

- `stash(msg) -> Result<(), StashFull<M>>` — bounded push.
- `unstash_all()` — **snapshot**: appends `held` to `ready`. Messages stashed
  *during* a replay land in `held` and wait for the next `unstash_all` — a
  replayed message cannot re-enter its own batch by stashing alone.
- Single producer (the handler), single consumer (`Stashed::handle`'s replay
  loop): plain `&mut`, no sync, no channel.
- Constructor `pub(crate)`; only `Stashed::on_start` builds one.

### D3 — Replay runs inside `Stashed::handle`, after the user handler returns

```rust
// impl Actor for Stashed<S> — the whole replay mechanism:
async fn handle(&mut self, msg, actor_ref, stop) -> Result<(), S::Error> {
    S::handle(&mut self.state, msg, actor_ref.clone(), &mut self.stash, stop).await?;
    while !*stop {
        let Some(m) = self.stash.pop_ready() else { break };
        S::handle(&mut self.state, m, actor_ref.clone(), &mut self.stash, stop).await?;
    }
    Ok(())
}
```

Everything the card demands falls out of running *inside the current
`handle_message` step*, inherited from the loop with no loop changes:

- **Ordering.** The batch drains before `Stashed::handle` returns, hence
  before the loop polls the mailbox again (`kind.rs:209`): replayed messages
  run ahead of the entire mailbox backlog and any new arrivals, in
  stash-arrival order. Effective priority:
  **control > in-step replay > user backlog > new arrivals.**
- **Liveness.** The step's `actor_ref` is strong and valid in both regimes —
  steady state (upgraded external allocation) and drain window (minted from
  the dequeued message's `self_sender`, `kind.rs:257-265`, ADR-0003/0010) —
  and it outlives the whole batch. No upgrade, no minting, no unsound window.
- **Failure routing.** An `Err` from a replayed handler propagates out of
  `Stashed::handle` → controlled crash → `on_panic` → stop. A panic unwinds
  into the loop's existing `catch_unwind`. A replayed handler setting `stop`
  ends the batch; the loop then breaks `Normal`. Identical to delivered
  messages, by construction.

### D4 — Stash does not pin the actor

The stash holds bare `A::Msg` — no `self_sender`, no strong ref. An actor
whose only unfinished business is a non-empty stash ref-count-stops
(`Collected`, ADR-0020), like an armed timer (ADR-0018 precedent). No
immortal actor: with no external refs no handler can run, so `unstash_all`
can never fire, and held messages drop with the state.

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
`Result`, house rule. Whether `?` works directly is the user's choice via a
`From<StashFull<M>>` impl on their error type.

### D6 — Stop-fate table: stash never outlives its incarnation

Only `unstash_all` rescues deferred messages. Every terminal path drops what
remains (held + undrained ready):

| Stop path | Reason | Stash fate |
|---|---|---|
| Ref-count collection (mailbox closed) | `Collected` | dropped (D4: never pinned) |
| In-band `Signal::Stop` | `Normal` | dropped — Stop abandons the queued backlog (ADR-0003); the *deferred* backlog ranks no higher |
| Out-of-band `stop()` (cancel token) | `Normal` | dropped |
| `kill()` (abort) | `Killed` | dropped with the task |
| Handler/hook panic or `Err` | `Panicked` | dropped with the poisoned state |
| Replayed message sets `stop` / errs / panics | as above | remainder of batch + held dropped |

Between steps `ready` is empty (a batch fully drains within its step), so no
stop path can observe a half-replayed batch except by interrupting the step
itself (kill/panic) — covered above. A stashed **ask** dropped this way drops
its reply port; the asker observes the same typed ask-side error as for any
dropped reply port — typed, never silent.

### D7 — Restart hygiene is structural

Supervised restart = new incarnation built from `Args` via
`Stashed::on_start`, which constructs a **fresh** `Stash`. A stale stash
cannot leak across incarnations. Asserted by test anyway (card checkbox;
#149 lesson).

### D8 — Capacity: `stash_capacity(&args)`, not `SpawnConfig`

Capacity comes from the actor's own constructor input via the required
`stash_capacity` trait method — type-fixed (ignore `args`, return a
constant) or spawn-tunable (thread it through `Args`). Deliberately **not**
a field on `SpawnConfig` (`spawn.rs:77`, `{ capacity, on_stop_grace }`): the
stash exists only for `Stashed<S>` actors, so a universal spawn-site knob
would be dead for the non-stashing majority and would hand the spawner
control over a buffer whose existence is the actor's internal choice.

## Rejected alternatives (why v2)

- **v1: user-declared field + defaulted `Actor::stash_slot` accessor + loop
  drain.** Fatal: forgetting the accessor override compiles clean and defers
  messages *forever* — silent loss, the one thing the model forbids. Also
  required `kind.rs` surgery (drain hook + conditional clone).
- **Loop-owned buffer + 4th `handle` param / `Context` on `Actor` itself.**
  Breaks every existing handler; re-opens #118's rejected `Context`.
- **Top-of-loop drain (any loop-owned variant).** Unsound in the drain
  window: `WeakActorRef::upgrade` is keyed on the *external* refcount and
  returns `None` while queued messages still self-pin
  (`actor_ref.rs:587-594`); a bare replayed msg has no `self_sender`
  fallback → no handler ref on the everyday `tell; drop` pattern.
- **`Stashing: Actor` subtrait detected by the shared loop.** No
  specialization in Rust → would force a separate spawn path ×
  {plain, linked, supervised} matrix.

## Scope limits

- **L1 — stash access is `handle`-only.** No other hook receives
  `&mut Stash`, so hook-triggered replay is impossible *by design* (a
  guarantee, not a wart). If a real use case needs e.g. unstash-on-link-death,
  that is a follow-up card.
- **L2 — self-inflicted livelock is possible.** A handler that re-stashes a
  message and calls `unstash_all` on every replay of it keeps the batch alive
  forever without yielding to the mailbox. Same class as an actor `tell`-ing
  itself in a loop: a user logic bug, documented on `unstash_all`, not
  runtime-prevented.
- **L3 — `Stashed<S>` as a *watcher* (impl `Watch`/`Supervisor` for
  `Stashed<S>`) is deferred** until a concrete use case (YAGNI). It is fully
  watchable and supervisable as a target/child today, which is what the card's
  invariants need.

## Invariant → test map (card checkboxes; all TDD, fail first)

| # | Card invariant | Test |
|---|---|---|
| 1 | Bounded API, capacity as constructor param | unit: cap enforced across `held + ready`; `stash_capacity` threading via `Args` |
| 2 | Overflow typed handback | unit: cap-th+1 `stash` returns `StashFull`; `assert_eq!` recovered msg == sent msg |
| 3 | Replay order (among stashed AND vs backlog) | integration on `Stashed<X>`: stash A,B; queue backlog D behind trigger T; `unstash_all` in T → handled order exactly `[T, A, B, D]` |
| 4 | Graceful stop, ref-count | non-empty stash, drop all refs → stops `Collected` within bound (also proves invariant 7) |
| 5 | Graceful stop, in-band `Signal::Stop` | stash msg, send Stop → stops `Normal`, stashed msg never handled (probe counter) |
| 6 | `kill()` | stash msg, kill → `Killed`, stashed never handled |
| 7 | Liveness/pinning | = test 4: stash does not pin (Collected reachable) |
| 8 | Panic + restart: no stale stash | supervised `Stashed<X>` child stashes, panics, restarts → new incarnation's stash empty (probe via ask) |
| — | Snapshot semantics (D2) | unit + integration: stash during replay lands in `held`, not the draining batch |
| — | Mid-batch stop (D6 last row) | replayed msg sets `stop` → remaining batch never handled |

Forget-mode needs no test: it is unrepresentable (D1). Proptests (if any) use
the `prop_` prefix (MIRI-sweep contract).

## Wiring (card checkboxes)

- ADR-0022: bounded stash — `Stashed<S>` composition, in-step replay,
  non-pinning, stop-fate table.
- `mutants-baseline.json` entries for every new fn (`Stash::*`,
  `Stashed`/`StashActor` impls).
- README public-API bullet (`Stashed`, `StashActor`, `Stash`, `StashFull`).
- Walking skeleton: extend `crates/core/examples/job_queue/` + its integration
  test (`crates/core/tests/app_job_queue.rs`) — e.g. a maintenance/pause phase
  as `Stashed<Queue>` stashing `Submit` until resumed. Shipped or explicitly
  deferred on-card.

## File layout

- `crates/core/src/stash.rs` — new module: `Stash<M>`, `StashFull<M>`,
  `StashActor`, `Stashed<S>` + its `Mailboxed`/`Actor` impls + unit tests.
- `crates/core/src/lib.rs` — module + re-exports.
- `crates/core/src/actor/kind.rs` — **no changes.**
- `docs/adr/0022-bounded-stash.md`.

## Out of scope (explicit)

- `unstash(n)` partial replay — card scope is `unstash_all`-shaped; add at
  second concrete use (YAGNI).
- FSM-ergonomics coupling (gen_statem-style postpone-on-transition) — the
  paired design lands with the FSM card, not here.
- Static ordering capabilities (Gordon 2025) — type-system project, M3+/KERI era.
