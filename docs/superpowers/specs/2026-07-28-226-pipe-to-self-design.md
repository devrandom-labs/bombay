# Pipe-to-self — sanctioned non-blocking ask-from-handler (card #226)

**Date:** 2026-07-28
**Card:** [#226](https://github.com/devrandom-labs/bombay/issues/226)
**Status:** approved design, pre-implementation
**ADR:** 0017 (pipe-not-reentrancy) — written alongside implementation

## Problem

bombay-core documents the ask-in-handler ban (`request.rs` — a handler that
`ask(..).await`s another actor is the bounded-mailbox cycle deadlock, the
canonical high-level deadlock of Torres Lopez et al., arXiv:1706.07372) but
ships no sanctioned alternative. Users hand-roll `tokio::spawn` + strong
`ActorRef` clone + `tell`, which (a) pins the actor against ref-count stop
(ADR-0003 violation), (b) loses failure correlation, (c) is re-invented
untested per call site.

## Research grounding (primary sources, verified)

- **Pekko** `ActorContext.pipeToSelf[Value](future)(mapResult: Try[Value] => T): Unit`
  — returns `Unit`, mapper receives `Try[Value]` (typed failure), documented
  thread-safe. Verified against the live scaladoc 2026-07-28.
- **kameo v0.22 oracle** (`src/message.rs:229`) `Context::spawn` — detached
  `tokio::spawn`, no cancel handle, "the spawned task continues running even
  if the actor stops". Verified in the sibling checkout.
- **De Koster et al., *43 Years of Actors*, AGERE! 2016** — ITP liveness: no
  blocking inside a turn; blocking futures deadlock-prone, non-blocking
  promises endorsed (footnote 2, citing Miller et al. TGC 2005).

The converged shape across all three: fire the future, end the turn, result
re-enters as an ordinary message, failure typed to the mapper, fire-and-forget.

## Decisions (user-approved)

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | Panic fate | Mapper receives `Result<T, PanicError>` | Pekko `Try[Value]` precedent; card: "silent drop of a panic is a silent failure". Actor decides. |
| 2 | Full mailbox at resolution | Await capacity (backpressure) | House stance ("backpressure, not a failure" — watch registration awaits the same way). Strong ref held only for the send duration. |
| 3 | Return value | `()` — fire-and-forget | Pekko returns `Unit`; card scope has no cancel invariant; second-use rule. Actor death already cancels delivery via failed weak upgrade. |
| 4 | Verb home | `ActorRef::pipe_to_self` only | Handler already receives strong `ActorRef<Self>`; verb-on-handle is the crate grammar (tell/ask/watch/link). `WeakActorRef` stays minimal. |
| 5 | Reentrancy | Rejected (ADR-0017) | Orleans-style interleaving tears `&mut self` across await points of interleaved turns — breaks the single-writer poisoning model. |
| 6 | Run-loop lane | Rejected | A `FuturesUnordered` arm in the run loop avoids the per-pipe spawn but bypasses the mailbox (ordering semantics fork), requires kind.rs/spawn.rs surgery, and shares nothing with #223. |

## Public API

```rust
impl<A: Actor> ActorRef<A> {
    /// Runs `future` in a detached task and delivers its result to this actor
    /// as an ordinary message: the sanctioned non-blocking alternative to
    /// `ask(..).await` inside a handler.
    pub fn pipe_to_self<T, F, M>(&self, future: F, mapper: M)
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
        M: FnOnce(Result<T, PanicError>) -> A::Msg + Send + 'static;
}
```

Ask-another-actor becomes:

```rust
let b = b_ref.clone();
actor_ref.pipe_to_self(
    async move { b.ask(Query { .. }).await },   // block owns its clone; borrow internal
    MyMsg::QueryDone,                            // FnOnce(Result<..>) -> menu variant
);
```

## Mechanism

```text
pipe_to_self(future, mapper):
    weak = self.downgrade()                      // strong ref never enters the task
    tokio::spawn(async move {
        out = AssertUnwindSafe(future).catch_unwind().await
              .map_err(|payload| PanicError::from_panic_any(payload, PanicReason::PipedFuture))
        Some(strong) = weak.upgrade() else return   // dead or drain window: drop
        msg = mapper(out)
        _ = strong.tell(msg).await               // closed-mailbox error swallowed
    })
```

- **Non-pinning by construction:** the task's closure type contains only
  `WeakActorRef<A>`; no code path can hold the actor alive while the future is
  pending. This is structural, not discipline — stronger than the Pekko/kameo
  precedents can express.
- **Drain-window drop for free:** `WeakActorRef::upgrade` already answers
  `None` once external strong refs are gone (ADR-0010), so "actor dying ⇒
  result dropped" needs no new mechanism.
- **Closed menu preserved:** the mapper returns `A::Msg` — compile-checked
  menu variant, no erased re-entry path.
- **Monomorphized end-to-end:** future + mapper inline into the spawned task;
  no `Box<dyn Fn>`, no dyn dispatch. The one allocation is `tokio::spawn`'s
  task box (unavoidable for a detached task; kameo pays the same).
- **Zero cost on the actor hot loop:** the result is an ordinary mailbox
  message — no new select arm, no run-loop change.

## Semantics table (ADR-0017 fate table)

| Event | Outcome |
|---|---|
| Future resolves, actor alive | `mapper(Ok(t))` delivered through mailbox (FIFO with other senders from enqueue time) |
| Future panics | `mapper(Err(PanicError))` delivered; the panic never touches the actor (not its turn) |
| Actor dead / drain window before resolution | Upgrade fails; result dropped silently (spec'd drop — no dead-letter queue exists yet); task exits clean |
| Kill race: external strongs alive, mailbox closed | `tell` returns closed-send error; swallowed; no panic in the detached task |
| Mailbox full at resolution | Task awaits capacity holding the upgraded strong ref (ordinary backpressure); pins only for the send duration |
| Mapper panics | Kills only the pipe task (a mapper panic cannot be re-mapped); documented sharp edge, `tracing` event under the feature |
| Ordering vs other messages | None guaranteed — the pipe is an ordinary racing sender; documented |

## `PanicReason` delta

New variant `PanicReason::PipedFuture` — a piped-future panic is neither a
lifecycle hook nor the actor's message turn (`is_lifecycle_hook() == false`).
Enum-variant addition: grep the whole repo (examples/tests/fuzz), per the
#196-era lesson.

## Shared primitive (#223 seam)

The task body — *weak-hold, resolve, upgrade-or-drop, send, swallow-closed* —
is the "non-pinning delayed self-send" primitive the timers card (#223) and
attach_stream (#230) need. Per the second-use rule it ships here as a
`pub(crate)` helper (in `actor/actor_ref.rs` or a small `actor/pipe.rs`),
**not** a premature trait abstraction; #223 generalizes it (any-target send,
cancel handle) at its second concrete use.

## Known compile-time limit (recorded as fact)

The ask-in-handler *ban itself* is not type-enforceable: a handler can await
any future, and Rust has no effect system to mark "inside a turn". The ban
stays documentary; `pipe_to_self` is the sanctioned escape hatch it points to.
Every surveyed framework shares this limit.

## Tests (card invariants → tests, TDD order)

1. **Round-trip:** actor pipes a real future; mapped variant arrives through
   the mailbox and mutates state; asserted via ask.
2. **Non-pinning:** actor whose only tie is an in-flight (never-resolving)
   pipe + no external strong refs ⇒ ref-count stop fires (weak-ref test,
   mirrors the existing ADR-0003 suite).
3. **Dead-before-resolution:** actor stopped, pipe then resolves ⇒ no panic,
   task exits, result dropped (asserted via completion of the detached task +
   no delivery).
4. **Liveness overlap:** actor A pipes an `ask` to actor B; A handles other
   messages *while* B's reply is pending — real overlap via `Barrier`-style
   sequencing (B blocks on a barrier until A proves it processed another
   message), not sequential-then-check.
5. **Panic surfacing:** piped future panics ⇒ mapper receives
   `Err(PanicError)` with `PanicReason::PipedFuture`, actor keeps running.
6. **Kill race:** kill with external strong held, pipe resolves into closed
   mailbox ⇒ swallowed cleanly.
7. **Doc invariant:** `request.rs` ask-ban text points at `pipe_to_self`.

Wiring: ADR-0017, mutants-baseline entries for new fns, README public-API
bullet.

## Rejected alternatives (for ADR-0017)

- **Orleans reentrancy** (`[Reentrant]`/`[AlwaysInterleave]`): interleaving
  turns on one actor breaks single-writer poisoning — `&mut self` observed
  torn across the await points of interleaved turns. Road not taken.
- **Run-loop `FuturesUnordered` lane:** see decision 6.
- **Cancel/abort handle:** no card invariant needs it; YAGNI until a second
  concrete use (#230 stream attachment owns lifecycle control).
- **`pipe_ask` sugar in this card:** the ask case pays a triple-nested
  `Result<Result<R, AskError<M, E>>, PanicError>` at the mapper plus a manual
  target clone. Real UX cost, Pekko solves it with a second verb
  (`context.ask`). Deferred to **#239** (filed, on the board) — #226 ships the
  primitive only.
