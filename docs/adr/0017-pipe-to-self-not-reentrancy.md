# ADR-0017 — Pipe-to-self: detached weak pipe, not reentrancy

**Status:** Accepted (2026-07-28) — decided under card #226 (design record:
[`docs/superpowers/specs/2026-07-28-226-pipe-to-self-design.md`](../superpowers/specs/2026-07-28-226-pipe-to-self-design.md))

## Context

bombay-core documents the ask-in-handler ban (`request.rs` — a handler that
`ask(..).await`s another actor is the bounded-mailbox cycle deadlock, the
canonical high-level deadlock of Torres Lopez et al., arXiv:1706.07372) but
ships no sanctioned alternative. Users hand-roll `tokio::spawn` + strong
`ActorRef` clone + `tell`, which (a) pins the actor against ref-count stop
(ADR-0003 violation), (b) loses failure correlation, (c) is re-invented
untested per call site.

## Options considered

- **A — detached weak pipe** *(chosen).* Spawn a task that captures only a
  `WeakActorRef`, `catch_unwind`s the piped future, upgrades-or-drops at
  resolution, and delivers the mapped `A::Msg` through the ordinary mailbox.
  Non-pinning by construction, panic surfaced typed via `PanicError`,
  closed-menu preserved, zero run-loop surgery.
- **B — run-loop `FuturesUnordered` lane.** Rejected: bypasses the mailbox,
  which forks ordering semantics,
  requires `kind.rs`/`spawn.rs` surgery, and shares nothing with the later
  timers card (#223).
- **C — Orleans-style reentrancy** (`[Reentrant]` / `[AlwaysInterleave]`).
  Rejected: interleaving turns on one actor breaks single-writer poisoning —
  `&mut self` would be observed torn across the await points of interleaved
  turns.

## Decision

1. **Detached weak pipe.** `ActorRef::pipe_to_self(future, mapper)` runs
   `future` off-turn and re-enters its result as an ordinary message.
2. **Weak-ref-only while pending.** The task captures only a
   `WeakActorRef<A>`; no strong ref may be held between spawn and resolution.
   This makes in-flight pipes non-pinning structurally, not by discipline.
3. **Panic surfaced, not swallowed.** A panic in the piped future reaches
   `mapper` as `Err(PanicError)` with `PanicReason::PipedFuture` — the actor
   decides, the actor itself is untouched.
4. **Backpressure = capacity await.** A resolving pipe waits for mailbox
   capacity like any sender, holding the upgraded strong ref only for the send
   duration.
5. **Kill-race + drain-window drops are spec'd, not silent.** If the actor is
   gone at resolution, the result drops; if the mailbox closed between upgrade
   and send, the closed-send error is swallowed. Both paths emit `tracing`
   breadcrumbs.
6. **`pipe_ask` sugar.** A second verb flattens the ask error union once, so
   the common "ask another actor from a handler" case does not force a
   triple-nested `Result` on the call site. `pipe_ask` delegates to the same
   primitive and inherits its non-pinning / drop semantics.

## Consequences

- The ask-in-handler ban now points to a sanctioned escape hatch.
- `PanicReason` gains `PipedFuture`, which `is_lifecycle_hook()` correctly
  classifies as `false` after flipping the predicate from
  `!matches!(Self::HandlerPanic)` to a positive match on the lifecycle-hook
  variants.
- No new public trait or run-loop arm; the shared primitive (`spawn_pipe`)
   ships as `pub(crate)` here and generalizes at its second concrete use
  (#223 / #230).

## Fate table

### `pipe_to_self`

| Event | Outcome |
|---|---|
| Future resolves, actor alive | `mapper(Ok(t))` delivered through mailbox (FIFO with other senders from enqueue time) |
| Future panics | `mapper(Err(PanicError{reason: PipedFuture}))` delivered; the panic never touches the actor |
| Actor dead / drain window before resolution | Upgrade fails; result dropped silently; task exits clean |
| Kill race: external strongs alive, mailbox closed | `tell` returns closed-send error; swallowed; no panic in the detached task |
| Mailbox full at resolution | Task awaits capacity holding the upgraded strong ref (backpressure); pins only for the send duration |
| Mapper panics | Kills only the pipe task; result dropped; `tracing` error event records it |
| Ordering vs other messages | None guaranteed — the pipe is an ordinary racing sender |

### `pipe_ask`

| Event | Outcome |
|---|---|
| Target alive, replies | `mapper(Ok(R))` delivered through mailbox |
| Target not alive at delivery | `mapper(Err(PipeAskError::TargetDead))` |
| Target mailbox full (non-waiting path) | `mapper(Err(PipeAskError::MailboxFull))` |
| Delivery deadline expires | `mapper(Err(PipeAskError::SendTimeout))` |
| Reply deadline expires | `mapper(Err(PipeAskError::ReplyTimeout))` |
| Target dies after accepting | `mapper(Err(PipeAskError::Interrupted))` |
| Target handler returns domain error `E` | `mapper(Err(PipeAskError::Handler(E)))` un-erased |
| Piped ask future unwinds | `mapper(Err(PipeAskError::Panicked(PanicError{reason: PipedFuture})))` |

Flatten is variant-lossless with respect to `AskError`/`TellError`, but the
undelivered message payload a `Deliver` failure carries is **structurally
dropped**: fire-and-forget has no caller to hand it back to.
