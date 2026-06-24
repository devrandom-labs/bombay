# Behavioural invariants of the surviving kameo core (card #74)

> Compiled from the `tests/features/**` scenarios. Each invariant is what a `Then`
> asserts; every one is grounded in source (citations live in the feature files). Items
> the source did **not** let us pin are listed under *Open invariants* and carry
> `@review-semantics` in the features — they are scenarios to resolve at wiring, never
> guessed assertions.

## Reading this doc

- **Confirmed** = asserted by a scenario, traceable to `file:line`.
- **Bug** (`@bug`) = scenario MUST FAIL today; reproduces a real defect (asserts the *desired*
  behaviour so it stays red until source is fixed — source is NOT changed in this phase).

## Decisions resolved (2026-06)

All semantic open questions were resolved as a **tests-only** exercise — no source was
changed; defects are captured as `@bug` probes for a later fix pass.

- **Topic/pattern wildcards (broker + message_queue): glob is the contract.** `*` uses the
  `glob` separator `/`, so it spans `.` — `order.*` *does* match `order.created.urgent`.
  Asserted as-is; AMQP/MQTT segment semantics are explicitly NOT promised.
- **`message_queue` malformed Topic key: two `@bug` probes** (fix later, both bind + publish):
  `@bug message_queue.rs:591` (bind must reject with `AmqpError::InvalidRoutingKey`) and
  `@bug message_queue.rs:707` (publish must return an error, not `unwrap()`-panic).
- **`ActorPool::new(0)`: panic is the contract.** Zero workers is a programmer bug → panic
  per rule 4; asserted as the documented contract, not migrated to `Result`.
- **`pubsub` Spawned/SpawnedWithTimeout never prunes a dead subscriber: defect.**
  `@bug pubsub.rs:125` asserts the subscriber *is* pruned (fails today; broker/message_bus
  already self-prune via a spawned `Unsubscribe`).
- **`on_stop` is not called when `on_start` fails: defensible start-paired contract**, asserted
  as-is (not a bug). **`ActorId` counter overflow:** unreachable on 64-bit, asserted only as
  "never panics on any reachable call; all 2^64 ids unique".

Still genuinely open (wiring-phase, not semantics): scheduler Burst/Delay catch-up counts,
and pool dispatch tie-break determinism — both flagged as `# NOTE` in their features.

---

## Core

### actor lifecycle (`src/actor.rs`, `actor/spawn.rs`, `actor/kind.rs`)
- Pre-startup external messages are buffered (VecDeque) and replayed front-to-back in send order; messages sent from *within* `on_start` bypass the buffer and run first.
- Default `on_panic` → `Break(Panicked)`; `Continue` keeps the actor alive, `Break(reason)` stops with that reason.
- **`on_panic` that itself errors or panics** → the actor stops with `Panicked(PanicError{reason: OnPanic})` (the hook is run under `catch_unwind`; both its `Err` and an unwinding panic map to `PanicReason::OnPanic`, `kind.rs:401-414`).
- `on_stop` runs exactly once on both kill (`Killed`) and graceful stop (`Normal`).
- **`on_stop` failure is a side-channel, not a spawn-future error:** an `on_stop` that returns `Err` or panics becomes `PanicError{reason: OnStop}` delivered via `shutdown_result = Err(..)` **and** the global actor-error hook, yet the spawn future **still resolves `Ok((actor, reason))`** (`spawn.rs:253-285`) — derived/best-effort teardown never fails the actor's recorded stop.
- Default `on_link_died`: stops for Killed/Panicked/LinkDied; continues for `Normal` and `SupervisorRestart`.
- `spawn` uses a bounded mailbox capacity 64; `spawn_with_mailbox` honours the supplied mailbox.
- `on_start` Err **and** panic are both surfaced as a startup error (reason `Panicked`) and never enter the run loop.
- `spawn_in_thread` panics on a current-thread runtime.
- RAII: dropping the last strong `ActorRef` closes the mailbox → `Break(Normal)`; weak refs don't keep it alive.
- `spawn_link` links before spawning (race-free by construction).
- **Resolved (asserted):** `on_stop` is NOT called when `on_start` fails — start-paired contract.

### actor_ref (`src/actor/actor_ref.rs`)
- `ask` returns the handler reply on its own reply channel (no cross-delivery under concurrency); `tell` is fire-and-forget.
- `is_alive` = `!mailbox.is_closed()`. **Surprise:** `WeakActorRef::is_alive` uses a *different* predicate (`!shutdown_result.initialized()`).
- `upgrade` → `Some` while a strong ref exists, `None` after all strong refs drop; send-to-dead → `ActorNotRunning`.
- `is_current` is false outside the actor task, true inside its handler.
- self-link / self-unlink are no-ops (early return on equal id).
- Eq/Hash/Ord are purely id-based; clone bumps strong count, downgrade bumps weak count.
- `Recipient`/`ReplyRecipient` type-erase but preserve id; `wait_for_startup`/`wait_for_shutdown` fan out to all concurrent waiters.

### actor_id (`src/actor/id.rs`)
- `generate()` returns the pre-increment value of one `AtomicUsize` `fetch_add(1, Relaxed)` → strictly increasing, contiguous, unique sequentially **and** concurrently (uniqueness needs only atomicity, not ordering — Relaxed suffices); concurrent generation collectively covers a gapless range.
- `to_bytes`/`from_bytes` round-trips losslessly; without `remote`, encoding is exactly the 8 LE bytes of `sequence_id`; serde serializes as those bytes and deserializes them back.
- Display = `#{sequence_id}` and Debug = `ActorId({sequence_id})` without `remote` (the `@{peer}`/`@local` and two-field forms are remote-gated).
- **`<8` bytes is a latent panic, NOT `MissingSequenceID` (corrected):** `from_bytes` slice-indexes `bytes[0..8]` *before* `try_into`, so a short/empty slice panics with "range end index 8 out of range" — the `MissingSequenceID` map_err and the serde `invalid_length` mapping (`id.rs:218-221`) are unreachable dead code. The author clearly intended a clean error (the variant + mapping exist), so this is captured as `@bug:id.rs:140-143` / `@bug:id.rs:218-221` probes (verified empirically 2026-06); the fix is a length check before slicing (a separate fix-pass card, not #74).
- Eq/Hash/Ord derive over `sequence_id` (no-remote).
- **Resolved (asserted):** counter overflow is unreachable on 64-bit — assert only "never panics
  on any reachable call; all ids unique". **Out of scope:** cross-peer ordering (`remote` feature).

### mailbox (`src/mailbox.rs`)
- FIFO per sender; no global cross-sender order asserted (tokio mpsc contract).
- `push_front` drains entirely before the channel; all four recv variants pop `front` first; `recv_many` never mixes front + channel in one call.
- Bounded backpressure: `send` parks at capacity, unblocks exactly when `recv` frees a slot; `try_send` → `Full(signal)`; unbounded never returns `Full`, only `Closed`.
- `capacity`/`max_capacity` = `Some(n)` bounded / `None` unbounded.
- Closing the receiver makes `send` return the un-sent signal; buffered signals still drain before `None`.
- Weak senders don't keep the channel alive.
- **Error-domain split (load-bearing):** `signal_startup_finished` maps full→`MailboxFull`, closed→`ActorNotRunning` (capacity ≠ dead-actor, per rule 3).

### message (`src/message.rs`)
- Sequential single-writer handling with exclusive `&mut A`; `ctx.stop()` defers shutdown until after the current reply is delivered.
- `reply_sender()`/`reply()` take the channel so the dispatcher does not double-reply.
- `ctx.spawn` detached: reply delivered to ask caller; on tell, error → global hook (`PanicReason::OnMessage`), and `on_panic` is NOT called.
- `forward` hands the original reply channel to the target (ask) or tells; dead target → `ActorNotRunning` to caller; `try_forward` fails fast with `MailboxFull` and restores `ctx.reply`; `blocking_forward` waits for capacity.
- Handler `Err`: ask → caller `HandlerError`; tell → routed via `into_any_err` to on_panic.

### reply (`src/reply.rs`)
- `Result`: `to_result`/`into_value` identity, `into_any_err` boxes Err only; infallible types have `Error = Infallible`.
- `ReplySender::send` maps Ok→boxed value, Err→`HandlerError`; single-use (consumed by value).
- `DelegatedReply` is return-only: `to_result`/`into_value` panic (`unimplemented!`); only `into_any_err` (→ None) is real.
- `ForwardedReply`: Forwarded(Ok) reports no error, Forwarded(Err) reports the SendError; `into_value` on Forwarded(Ok) is `unreachable!` (dispatch-bug indicator); wrong-type `downcast_ok` panics (documented misuse boundary).

### request/ask (`src/request/ask.rs`)
- `mailbox_timeout` → `Timeout(Some(msg))` (never enqueued, handed back); `reply_timeout` → `Timeout(None)` (already enqueued). **This Some/None split is load-bearing.**
- Both set: `send()` awaits capacity before starting the reply clock.
- Closed mailbox → `ActorNotRunning(msg)`; full bounded `try_send` → `MailboxFull(msg)` (the bounded(1) two-send drain race is already fixed in-tree).
- A handler panic / kill mid-ask resolves the caller to a `SendError` — **never a hang**.
- Concurrent asks each answered exactly once with no cross-talk.

### request/tell (`src/request/tell.rs`)
- No reply; `Result<(), SendError<M>>`. Bounded `send()` waits for capacity; `try_send()` never waits; unbounded `send()` ignores `mailbox_timeout`.
- `send_after` returns an abortable `JoinHandle`; `abort()` before fire prevents delivery; deferred send to a stopped actor → `ActorNotRunning`.
- Concurrent tells: every `Ok` recorded once, every `MailboxFull` not recorded; bounded `send()` backpressures rather than dropping.

### registry (`src/registry.rs`, local only)
- `insert` returns `bool` — `false` on duplicate name with **no overwrite** (existing entry survives); it does NOT return `NameAlreadyRegistered` (that's the remote path).
- `get::<A>` → `Ok(None)` absent, `Ok(Some)` present+type-match, `Err(BadActorType)` present+wrong-type.
- Registration outlives the actor: a stopped actor's entry stays present (no liveness observation); messaging it → `ActorNotRunning`; re-register requires removing the dead entry first.
- Empty and very-long names are valid distinct keys.
- Mutex-serialised: concurrent same-name insert elects exactly one winner; get-during-remove returns `Some`/`None`, never `BadActorType`, never panics.

### links (`src/links.rs`)
- Supervised child notifies parent WITH `mailbox_rx` + siblings (restart path); unsupervised actor notifies siblings WITHOUT `mailbox_rx`.
- **Death-watch deadlock prevention:** `set_children_parent_shutdown` sets `parent_shutdown=true` (Release); a child loading it (Acquire) drops `mailbox_rx` instead of queuing — the exact Release/Acquire pairing. A restarted instance resets the flag and clears stale children.
- Death notification is once-only (`mem::take`/`drain`); notifying a dead target is swallowed; no links ⇒ no notification.

### supervision (`src/supervision.rs`)
- Policy × exit matrix: Permanent always; Transient only abnormal (panic/error), not Normal; Never never.
- Strategy: OneForOne = failed child only; OneForAll = all; RestForOne = failed + younger siblings in spawn order.
- Intensity: exceed `max_restarts` in window ⇒ stop; count resets after window.
- **GAPs now covered:** `SupervisorRestart` bypasses Permanent/Transient but NOT Never; `restart_limit(0)` never restarts even the first time (`0 >= 0`); window=ZERO resets every failure; window=MAX never resets; on_start-fail-during-restart surfaces `OnStart`.

### error (`src/error.rs`)
- `map_msg` rewrites only message-bearing variants; `map_err` only HandlerError; both preserve the variant tag.
- `boxed`→`downcast` round-trips; `try_downcast` wrong-type → recoverable `Err`; infallible `downcast` panics on wrong-type (programmer bug).
- `unwrap_msg` returns the message only for `ActorNotRunning`/`MailboxFull`/`Timeout(Some)` and **panics** otherwise (incl. `Timeout(None)` — the message, not the variant tag, is the discriminator); `unwrap_err` returns the error only for `HandlerError` and panics otherwise (`error.rs:171-188`; programmer-bug panics, rule 4).
- `PanicError` exposes payload from `&'static str` and `String`; `with_downcast_ref` mismatch → None; poisoned mutex still readable via `get_ref` with no second panic.
- **`PanicError` serde round-trip is LOSSY (and non-idempotent):** `Serialize` emits `err = self.to_string()` (the Display) + `reason`; `Deserialize` rebuilds the inner payload as a `String` (`error.rs:564-661`). The concrete payload type is erased; a value-less Display drops the payload entirely; and because Display is `"{reason}: {payload}"`, each round-trip re-prefixes the reason (`"R: boom"` → `"R: R: boom"`). Defensible wire contract (a peer cannot reconstruct arbitrary Rust types), pinned as the actual behaviour — not a faithful round-trip.
- `ActorStopReason::is_normal` true only for Normal; `PanicReason` lifecycle-hook vs message-processing classification, with `Next` in neither set.
- `BadActorType` and `NameAlreadyRegistered` are distinct, non-interchangeable failure domains.

---

## Actors (`kameo_actors`)

### Cross-module routing-semantics matrix (the subtle part)
| | dedupe on register/subscribe | prunes dead subscriber on |
|---|---|---|
| **broker** | never (Vec push; N patterns = N deliveries) | `ActorNotRunning` only |
| **pubsub** | **overwrites by ActorId** (replaces filter) | `ActorNotRunning` **and** `ActorStopped` |
| **message_bus** | never (Vec push) | `ActorNotRunning` only |
| **message_queue** | **dedups by ActorId** per (queue, TypeId) — re-consume is a no-op (`message_queue.rs:761`) | `ActorNotRunning` only (`:434/:440/:449`; Spawned/SpawnedWithTimeout self-cancel `:457-467`) |

- Spawned / SpawnedWithTimeout: **broker and message_bus self-prune** (the spawned task sends an `Unsubscribe`/`Unregister` on `ActorNotRunning`, `broker.rs:227` / `message_bus.rs:218`); **pubsub does NOT** — it drops the task result (`pubsub.rs:125-131`), so a dead subscriber is never pruned → captured as `@bug pubsub.rs:125`.
- `MailboxFull`/`Timeout`/`HandlerError` never prune anywhere.

### broker (`actors/src/broker.rs`)
- glob `Pattern` matching: separator `/`, `require_literal_separator: true` — `*` matches within one segment, does NOT cross `/`; `.` is an ordinary char. **Resolved:** glob is the contract — `*` spans `.` (asserted), AMQP segment semantics NOT promised.
- DeliveryStrategy: Guaranteed blocks until accept; BestEffort `try_send` skips full mailbox without prune/panic; TimedDelivery bounds per-recipient wait; Spawned retries a full mailbox indefinitely; SpawnedWithTimeout abandons after timeout.
- Unsubscribe `Some(topic)` prunes one pattern; `None` prunes from all; emptying a pattern drops the key.

### pubsub (`actors/src/pubsub.rs`)
- `publish` clones to every subscriber whose filter (default `|_| true`) returns true; a false filter skips delivery entirely and does NOT remove the subscriber (so a dead-but-filtered-out subscriber is never pruned).
- Zero-subscriber publish is a graceful no-op; all 5 strategies covered.

### message_bus (`actors/src/message_bus.rs`)
- Routing by `TypeId`: publish of `M` reaches only recipients registered for exactly `M`'s TypeId; `downcast_ref::<Recipient<M>>()` is sound because the bucket key is that TypeId.
- Unregister removes only the (actor, type) registration; emptying a bucket drops the key.

### message_queue (`actors/src/message_queue.rs`) — exemplar
- Direct = exact routing-key match; Fanout = all bound queues; Topic = glob; Headers = x-match all/any over message headers (HeadersRequired if absent).
- Default (empty-name) exchange auto-binds a queue to its own name.
- QueueBind validates queue existence, exchange existence, duplicate bindings, and Headers `x-match` value.
- **BasicConsume dedups by `actor_id` per `(queue, TypeId)`** — re-consuming with the same actor is a no-op (`message_queue.rs:761`), unlike broker/message_bus (no dedup) and pubsub (overwrite); see the routing matrix above.
- **Delete cascades:** `QueueDelete`/`ExchangeDelete` (and `basic_cancel`) tear down the entity and its dependent bindings/consumers (`message_queue.rs:370-408`); `if_unused` gates deletion on having no bindings/consumers.
- **`@bug` message_queue.rs:707 + message_queue.rs:591** — a malformed Topic `routing_key` (e.g. `"[unclosed"`) is accepted at bind (`:591` has no glob validation) and `Pattern::new(..).unwrap()` panics the run-loop at publish (`:707`). Two probes assert the desired fix on both sides (bind rejects with `AmqpError::InvalidRoutingKey`; publish returns an error, no panic). **Note:** `AmqpError::InvalidRoutingKey` does **not** exist in the current enum (9 variants, `message_queue.rs:212-231`) — the fix must add it. Until then the probes cannot compile against `master`, which keeps them red exactly as a `@bug` probe must be.
- **Resolved:** Topic wildcard = glob — `log.*` *does* deliver `log.warn.detail` (asserted), not AMQP segment semantics.

### pool (`actors/src/pool.rs`)
- Least-connections routing: `next_worker` selects min `Arc::weak_count`; an in-flight `Dispatch` holds a `Weak<()>` alive, so load = in-flight messages, not mailbox depth.
- A single Dispatch reaches exactly one worker; reply is `WorkerReply::Forwarded` (pool forwards the reply channel).
- Broadcast fans to every worker, returns `Vec<Result>` of length = worker count, in worker order.
- **Broadcast `panic!` guard (`pool.rs:396-404`)** — the broadcast map calls `panic!("reset err infallible called on a SendError::HandlerError")` for the `HandlerError` arm, but broadcast uses a *tell* (no reply channel), which can only yield `ActorNotRunning`/`MailboxFull`/`Timeout` — so the panic arm is **unreachable defensive code** (same shape as `reply.rs:535`'s `unreachable!`). Pinned as `@review-semantics` (healthy-path result + guard reachability), NOT a `@bug` probe: it does not fire on any reachable input, so a probe would pass green — the forbidden "green test documenting a bug".
- Worker replacement: `on_link_died` rebuilds the dead worker via factory at the **same index** and **re-links** it; unknown id is a no-op.
- Dispatch retry: catches `ActorNotRunning`, advances to next worker, retries up to `workers.len()`; total exhaustion returns `Err(ActorNotRunning(msg))` carrying the original message (at-most-once, explicit error — never silent loss/dup).
- `new(0)`/`new_async(0)` panic via `assert_ne!(size, 0)`. **Resolved:** panic is the contract (zero-size = programmer bug, rule 4).

### scheduler (`actors/src/scheduler.rs`)
- `SetTimeout` deadline = `Instant::now() + duration` captured at construction; fires exactly once; returns an `AbortHandle`. `SetInterval` anchored at construction; loops tick→upgrade→tell.
- Self-termination: each interval tick checks `upgrade()` and matches `ActorNotRunning | ActorStopped` → returns cleanly (no panic/error) when the target is gone.
- `abort()` cancels before fire and stops further interval firings; `start_delay` defers the first tick; `Duration::ZERO` fires immediately.
- Each timer is an independent JoinSet task; `next()` drains only finished tasks without disturbing live ones.
- **Open:** tokio interval first-tick-immediate counts; Burst vs Delay catch-up counts (need paused-clock wiring + tokio docs).

---

## Console

### poller (`console/src/poller.rs`)
- Framing: client sends single byte `0x00`; reply = 4-byte **big-endian** u32 length + MessagePack `Message::Snapshot`.
- `MAX_FRAME_BYTES = 67_108_864 (64 MiB)`; guard is `len > MAX` so the boundary is **inclusive** (== MAX accepted, MAX+1 / `0xFFFFFFFF` rejected as `InvalidData` before allocation).
- Zero-length frame passes the size gate but fails MessagePack decode → reconnect; truncated prefix/payload → `UnexpectedEof` → reconnect; invalid payload leaves the shared slot unchanged.
- Connect failure → `Disconnected { error, since }` + fixed **5s** backoff (not exponential).

### tui (`console/src/tui.rs`) — pure helpers
- `spark_height(max=0) → 1` (baseline, not 0); `actor_rate(dt None or 0) → 0`; `backpressure_style`/`mailbox_bar(cap=0) → ratio 0.0` (normal style, not red); counter-reset delta saturates to 0.
- `short_type_name` splits at first `<` then last `::` (`"a::b::Foo<X>" → "Foo"`, empty→empty); `centered_rect` clamps size to area (never overflows); `mailbox_bar` ratio clamped ≤ 1.0.
- `rate_context(snapshot, prev)`: `dt = captured_at.duration_since(prev).ok()` — a reversed/non-monotonic clock (current < prev) → `Err` → `None`, so `actor_rate` then yields 0 (no negative/wrapped dt, no panic); `prev None` → empty map + `None` dt.
- `severity(actor) ∈ 1..=5`: Stopped 5, Restarting 4, **Running with a handler in-flight ≥ STUCK_THRESHOLD (5s) also 4** (ties with Restarting), Stopping 3, Starting 2, Running (fresh/idle) 1 — higher = surfaced first by a descending State sort.
- `compare`/`sort_actors`: every non-`Id` column composes its key with `.then(a.id.0.cmp(&b.id.0))`, so ties break by ascending id (total, deterministic). `sort_actors` applies `if desc { ord.reverse() }` to the **whole** Ordering, so a descending sort reverses the id tiebreak too (it is not a stable ascending secondary key).
- `sparkline_line(samples, max, width)`: `cols = width*2`; data left-zero-padded / right-clipped to `cols`, chunked by 2 ⇒ **exactly `width` braille cells** for any input; only the last `width*2` samples are shown (older scroll off); `width==0` ⇒ empty line.
- `braille(left, right)`: `bits = LEFT[left.min(4)] | RIGHT[right.min(4)] ≤ 0xFF`, so `0x2800 + bits` is always a valid `U+2800..=U+28FF` cell — heights are clamped with `.min(4)` and the `' '` fallback (`:1584`) is unreachable.
- `color_rgb(color)`: total over every `Color`; `Rgb(r,g,b)` verbatim, the named-ANSI set mapped, and any **unlisted/Reset/default** variant → `FG (205,205,212)` — so the fade math always has a concrete triple.
- `fade_toward_bg(color, factor)`: per-channel lerp `(target + (c-target)*factor) as u8` toward `BG = Rgb(18,18,22)`; the `as u8` **truncates toward zero** (does not round), so e.g. `Rgb(205,205,205)` faded 0.5 → `Rgb(111,111,113)` (the `.5` channels drop, never round up).
- `detect_deadlocks`: functional wait-for graph; self-/2-/multi-cycle each emitted once; cycles normalized to lowest-id start and sorted; dangling target → no panic, no cycle.
- **Corrected vs brief:** these helpers do NOT divide-by-zero — they short-circuit ratio to 0.0 (encoded as the invariant, not an error scenario). `fade_toward_bg` factor 0.5 is now **pinned** with exact u8-truncation values (`Rgb(205,205,205)` → `111/111/113`), and `braille`/`color_rgb`/`sparkline_line`/`severity`/`rate_context`/`compare` (previously absent from this inventory) are now itemised above.

### server / wire (`src/console/server.rs`, `wire.rs`, `registry.rs`)
- `seq` = process-global `SEQ.fetch_add(1)` per snapshot — strictly monotonic, distinct across concurrent clients.
- Actor **membership list** captured atomically under one registry lock (per-actor fields read live after).
- Request byte value ignored (any byte ⇒ one snapshot); pipelined bytes ⇒ one snapshot each with increasing seq; encode error closes the connection with no partial frame.
- **Corrected vs brief:** the server never reads a client-supplied length (only a 1-byte trigger), so there is no server-side oversized/garbage-length path — the 64 MiB cap is purely client-side (two `@boundary` scenarios pin this: `server_wire.feature` "The request byte value is ignored" and "The server applies no frame-size cap because it never reads a client length").
