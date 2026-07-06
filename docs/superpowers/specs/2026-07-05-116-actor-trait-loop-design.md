# Actor trait, lifecycle hooks & run-loop — design (card #116)

**Status:** approved 2026-07-05 · epic #122 · follows the per-card rigor contract there.

## What this is

The local actor **spine**: the `Actor` trait (`on_start` / `handle` / `on_panic`
/ `on_stop`), the run-loop that drives it (`kind.rs`), and the spawn entry points
(`spawn.rs`). It is the identity-agnostic body that runs whatever bears the
identity — it survives the identity-first inversion (#121) unchanged in spirit.

This card builds on the shipped primitives — `mailbox` (#112), `message`/`Msg`
(#114), `reply` (#115), `error` (#113) — and introduces a **minimal `ActorRef`
scaffold** that #117 later expands. It deliberately does **not** build:
supervision/links/restart (#120), the full `ActorRef`/`Recipient`/counting
(#117), the `tell`/`ask` request builders (#118), or identity (#121).

## Scope decisions (approved)

Two forks the card's comments left implicit, decided with the user:

1. **`MaybeSend` (#9) is deferred.** #116 ships **Send-saturated** (`Actor`,
   `Args`, `Msg`, `Error`: `Send`; every hook `impl Future + Send`), matching the
   already-shipped mailbox (`Mailboxed::Msg: Send + 'static`). The cfg-gated
   `MaybeSend`/`MaybeSync` relaxation threads through every signature, so it is
   retro-applied across mailbox + actor + ref + registry in **one coherent #9
   sweep** — not partially here.

2. **Minimal `ActorRef` scaffold.** #116 builds only what the hooks, spawn, and
   loop need. Ref-count-driven-stop (last strong drop stops the actor),
   `Recipient` erasure, and the `tell`/`ask` entry points are **#117/#118**.

## The reference, and why it is replaced

`src/actor.rs` + `src/actor/kind.rs` + `src/actor/spawn.rs` (~1,983 LOC) are
built around three concerns the rebuilt loop **amputates**, because they belong
to later cards:

- **Supervision coordination** (`CoordinationState`, `OneForOne`/`OneForAll`/
  `RestForOne`, restart intensity) → #120.
- **Link death-watch** (`handle_link_died`, `notify_links`, sibling fan-out) →
  #120.
- **Console monitoring** (`ActorMonitor`, `set_running`/`set_stopping`) → console
  feature, out of the core spine.

Stripped of those, the loop is roughly 1/5th the size: `on_start` → loop →
`on_stop`, with four `catch_unwind` sites.

## The `Actor` trait — `bombay-core/src/actor/mod.rs`

Send-saturated, exactly as finalized on the card:

```rust
pub trait Actor: Mailboxed<Msg: Msg> + Sized + Send + 'static {
    type Args: Send;
    type Error: ReplyError;
    fn name() -> &'static str { type_name::<Self>() }

    fn on_start(args: Self::Args, actor_ref: ActorRef<Self>)
        -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn handle(&mut self, msg: Self::Msg, actor_ref: ActorRef<Self>, stop: &mut bool)
        -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn on_panic(&mut self, actor_ref: WeakActorRef<Self>, err: PanicError)  // infallible, stop-only
        -> impl Future<Output = ActorStopReason> + Send
        { async move { ActorStopReason::Panicked(err) } }

    fn on_stop(&mut self, actor_ref: WeakActorRef<Self>, reason: ActorStopReason)
        -> impl Future<Output = Result<(), Self::Error>> + Send { async { Ok(()) } }
        // Err logged + surfaced; NEVER unwrapped; &mut self is POISONED after a panic
}
```

### Actor ↔ mailbox coupling

The shipped mailbox is keyed on `A: Mailboxed` (`Signal<A>`,
`Mailbox::<A>::bounded`). Rather than refactor the just-shipped mailbox, **`Actor`
is a subtrait of `Mailboxed`**, and the `: Msg` slot-budget tripwire is pulled in
via an associated-type bound: `Mailboxed<Msg: Msg>`. This resolves the decision
`message.rs` explicitly deferred to #116 ("whether `Actor::Msg` bounds `: Msg`")
— **yes**, so every actor's message type gets the compile-time size tripwire.

- `Mailboxed::Msg` itself stays `Send + 'static` (**no #112 change**).
- `type Msg` is **not** re-declared on `Actor` (a second `type Msg` would
  shadow-clash); the message type surfaces as `A::Msg` through the supertrait.

### The three finalized failure decisions (locked on the card, restated)

- **`on_panic` is infallible, stop-only.** No `Result`, no resume. It only names
  the terminal `ActorStopReason`; it cannot veto the stop.
- **supervision + links leave the base trait** (`supervision_strategy` /
  `on_link_died` → a `Supervisor: Actor` supertrait in #120). Keeps the base
  trait lean.
- **`on_stop` `Err` is logged + surfaced, never unwrapped.** A double-panic on
  the shutdown path "will likely abort the program" (std `Drop` docs); unwrapping
  would also replace the real cause with a second panic, blinding watchers. The
  original stop `reason` is preserved for death-watch.

## `&mut self` — kept, poisoned-on-panic

The actor instance lives as a plain `&mut self` field in the loop's
`ActorBehaviour<A>`, owned by the task. We do **not** adopt ractor's
framework-owned `State`; the single-writer boundary is enforced structurally by
the loop (one task, one mailbox, sequential `handle`).

`&mut A` is not `UnwindSafe`, so each hook is wrapped in
`AssertUnwindSafe(...).catch_unwind()`. This is correct, not a smell: the danger
of `AssertUnwindSafe` is *witnessing* torn state (resuming on it), and we never
do — the loop breaks to shutdown the instant a panic is caught, and no resume
path exists.

**Poison contract (baked onto the trait, #122-#12):** after a handler panic —

1. the loop catches it → `on_panic(&mut self, …)` yields the stop reason;
2. `on_stop(&mut self, …, Panicked)` **still runs** (OTP `terminate` precedent);
3. but `&mut self` is **poisoned** (std `Mutex`-poison analogy): `on_stop` may do
   **reason-independent resource release only** — never flush/derive/persist from
   domain fields, which are torn and would corrupt the event log;
4. then `self` is dropped. Never resumed, never reused.

*Loop-guaranteed:* zero further `handle` calls hit torn `self`; `Panicked` is
passed to `on_stop`. *Contract (user code, can't be statically enforced):* don't
read torn domain fields on the poisoned path — covered by a spy test. The
recovery (re-fold committed events) is the durability layer's job (#12/#120),
*above* this discarded `self`.

## The run-loop — `bombay-core/src/actor/kind.rs`

Lifecycle, top to bottom:

```
on_start(args) ─► loop { run_until_cancelled(recv) } ─► on_stop ─► drop self
  catch_unwind         catch_unwind per handle          catch_unwind
```

**No startup buffer, no `select!`, no `VecDeque`** (decided against kameo):

- **No startup buffer.** `on_start` *builds* the state (`-> Result<Self>`), so no
  message is handleable until it completes. We `await on_start` fully, then loop.
  Early messages wait in the **bounded flume mailbox** (already FIFO) and drain
  after start. A separate `VecDeque` would double-buffer *and* be an unbounded
  side-queue — contradicting the mailbox card's bounded-only / backpressure
  principle. Slow hydration (the nexus event-replay case) correctly backpressures
  senders instead of ballooning a side queue.

- **`run_until_cancelled`, not `select!`.** tokio-util 0.7.18 provides
  `CancellationToken::run_until_cancelled<F>(&self, fut: F) -> Option<F::Output>`.
  The loop body is a plain `match` — no macro, no biased/fairness footguns, no
  per-branch cancellation-safety reasoning, and `Future`-generic so it survives
  the M6/executor-seam swap:

  ```rust
  loop {
      match cancel.run_until_cancelled(mailbox_rx.recv()).await {
          None            => break ActorStopReason::Normal,   // token cancelled (out-of-band graceful)
          Some(None)      => break ActorStopReason::Normal,   // all senders gone
          Some(Some(sig)) => match sig {
              Signal::Message(m) => { /* AssertUnwindSafe(handle(m)).catch_unwind(); break if stop/err */ }
              Signal::Stop       => break ActorStopReason::Normal, // in-band graceful (FIFO)
              Signal::LinkDied(_)=> { /* no-op: nothing produces this pre-#120; continue */ }
          }
      }
  }
  ```

  Because `handle(m).await` runs **outside** the cancellation wrapper, an
  in-flight handler always finishes before either stop is observed — the
  "finish-current-then-stop, **no drain**" guarantee. Queued-behind messages are
  abandoned (nexus re-drives them).

### Three ways to stop

| Trigger | Mechanism | `on_stop`? | In-flight msg |
|---|---|---|---|
| `Signal::Stop` (in-band, FIFO) | mailbox | ✅ runs | finishes |
| `CancellationToken` (out-of-band) | `run_until_cancelled` | ✅ runs | finishes |
| **kill** (hard) | `futures::Abortable` + `AbortHandle` | ❌ skipped | dropped mid-await |

`futures::Abortable` (not `JoinHandle::abort`) wraps the whole lifecycle future,
so kill works uniformly for both `spawn` (background task) and `run` (current
task, for deterministic tests). The `AbortHandle` lives in the `ActorRef`.

### Four `catch_unwind` sites → `PanicReason`

`on_start` → `OnStart`; `handle` → `HandlerPanic`; `on_panic` → `OnPanic`;
`on_stop` → `OnStop` (all already in `error.rs`). A caught panic becomes a
`PanicError` via a **new `PanicError::from_panic_any(Box<dyn Any + Send>,
PanicReason)`** — the one addition to `error.rs`, already earmarked in its
`DEFERRED` comment for #116.

### Two failure-routing decisions (card implicit — follow kameo)

- **`handle` returns `Err(E)`** → treated as a controlled crash:
  `Panicked(PanicError::new(Box::new(e), HandlerPanic))` → `on_panic` → stop. The
  reply to the asker already went through the embedded port; a returned `Err` is
  the handler escalating a fatal condition. (Matches kameo's `on_message`
  `Ok(Err)` → `Panicked` arm.)
- **`stop: &mut bool`** set true by a handler → after the handler returns `Ok`,
  the loop breaks with `Normal`.

## Minimal `ActorRef` scaffold — `bombay-core/src/actor/actor_ref.rs`

Each field is independently cheap-clone and shares state, so **no outer `Arc`** in
#116 (the Arc/Weak *counting* semantics are #117):

```rust
pub struct ActorRef<A: Actor> {          // #[derive(Clone)]
    id: ActorId,                          // mailbox's scaffold ActorId
    mailbox: MailboxSender<A>,            // flume sender
    cancel: CancellationToken,            // graceful stop
    abort: AbortHandle,                   // hard kill
}
pub struct WeakActorRef<A: Actor> {       // holds WeakMailboxSender; upgrade() fails once senders gone
    id: ActorId,
    mailbox: WeakMailboxSender<A>,
    cancel: CancellationToken,
    abort: AbortHandle,
}
```

**#116 methods only:** `id()`, `downgrade()` / `upgrade()`, `stop()` (cancels the
token → graceful), `kill()` (abort → skips `on_stop`), `mailbox_sender()` (to
enqueue `Signal`s). The ergonomic `tell` / `ask` and ref-count-driven-stop are
**#117/#118** — not here.

## Spawn — `bombay-core/src/actor/spawn.rs`

- **`RunResult<A>`** — the honest, total outcome of running an actor. `run()`
  can't return `Result<(A, ActorStopReason), PanicError>`: the **kill** path drops
  the state mid-flight (no `A`), and startup failure produces no `A` either.

  ```rust
  pub enum RunResult<A: Actor> {
      /// Ran and stopped. If `reason` is `Panicked`, `actor` is POISONED
      /// (torn state — resource-release only; never read domain fields).
      Stopped { actor: A, reason: ActorStopReason },
      /// `on_start` returned `Err` or panicked; no actor was produced.
      StartupFailed(PanicError),
      /// Hard-killed via `kill()`; `on_stop` was skipped, state dropped.
      Killed,
  }
  ```

- `PreparedActor<A>`: created before running; holds `actor_ref` + `mailbox_rx` +
  `AbortRegistration`. Methods:
  - `.actor_ref() -> &ActorRef<A>` — usable before the loop starts (pre-send).
  - `.run(args) -> RunResult<A>` — runs in the current task (deterministic tests).
  - `.spawn(args) -> JoinHandle<RunResult<A>>`.
- Convenience (`Spawn: Actor` blanket ext-trait): `A::spawn(args) -> ActorRef<A>`
  (default cap); `A::spawn_with_capacity(cap, args) -> ActorRef<A>`.
  `DEFAULT_MAILBOX_CAPACITY = 64`.
- **Return contract:** `on_start` panic/`Err` → `StartupFailed`. Handler panic →
  `Stopped { actor: torn A, reason: Panicked }` (poisoned). `on_stop` panic →
  logged/surfaced, **original `reason` preserved** in `Stopped`. Kill → `Killed`.

## Testing (TDD — write failing first)

Rule #7 categories + rule #8 (`@bug` FAILS while the bug exists). Handler-panic
probes use an `on_stop`/reason spy, **not** `should_panic` (handler panics are
actor-internal).

| Test | Category | Note |
|---|---|---|
| `messages_during_on_start_handled_after_in_order` | sequence | flume FIFO, no buffer |
| `stop_flag_in_handle_stops_normally_after_ok` | sequence | `*stop = true` |
| `graceful_stop_finishes_current_then_stops` | lifecycle | cancel → in-flight completes, `on_stop` runs |
| `kill_skips_on_stop_and_drops_in_flight` | lifecycle | abort path |
| `@bug on_stop_runs_after_panic_with_panicked_reason` | lifecycle | spy sees `Panicked` |
| `@bug on_stop_after_panic_does_not_flush_torn_state` | defensive | poison contract |
| `@bug handler_panic_stops_actor_and_send_fails` | lifecycle | post-panic `send` → `SendError` |
| `on_start_panic_caught_as_panicerror` | defensive | **fails under `panic = abort`** (card's pin) |
| `on_start_error_never_handles_messages` | lifecycle | `Err` from `on_start` |
| `concurrent_senders_single_writer_ordering` | linearizability | real overlap (`spawn` + `Barrier`) |

**Deferred:** `client_tier…single_threaded` → #9; full `rehydrate_from_log` →
#120 (#116 covers the *discard* half via the poison test).

## Dependencies & README

- New `bombay-core` dep: `tokio-util` (`CancellationToken`, absorbs #55).
  `futures` (Abortable) is already available; add if not.
- `PanicError::from_panic_any` added to `error.rs`.
- README: `bombay-core` is not yet public API (the spine settles at the end of
  #112–#121), so no user-facing README change; coverage moves go to
  `docs/testing/coverage-baseline.md`.

## Cross-links

message model #114 · mailbox #112 · reply #115 · error #113 · ActorRef #117 ·
request builders #118 · supervision/links #120 · identity #121 · `MaybeSend` #9 ·
cancellation #55.
