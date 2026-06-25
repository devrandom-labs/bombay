# Bombay property & model specification (card #74, Phase 2)

> **Phase 2 deliverable: property-based & model-based scenarios, still spec-only.** Where
> Phase 1 pinned behaviour with hand-picked *examples*, Phase 2 states the **laws** those
> examples are instances of — universally quantified over generated inputs (`@property`)
> and checked against a reference model under any interleaving (`@model`). No step
> definitions, no generators coded, no source changed. Wiring (proptest / a model checker)
> is Phase 3.

## Why a second kind

An example proves a point; a property proves the *rule*. `mailbox.feature` asserts that a
`bounded(2)` mailbox blocks the 3rd send — Phase 2 asserts FIFO holds for **every** capacity
and message count, against a `VecDeque` oracle. Example-based scenarios catch the cases we
thought of; property/model scenarios catch the cases we didn't. They pair: a failing
property shrinks to a minimal counter-example that becomes a new Phase-1 example.

## Two new tags (a property still carries one cross-cutting tag too)

- `@property` — a law `∀ inputs. invariant(SUT(inputs))`. Wired with `proptest` (or
  `quickcheck`). The scenario names the law; the generator strategy + the oracle live in a
  `# GEN:` / `# ORACLE:` note.
- `@model` — behaviour checked against a **reference model** under concurrency: generate a
  random op sequence (and interleaving), run it on both the SUT and a simple in-memory model,
  assert observable equivalence (linearizability / refinement). Wired with a model checker
  (e.g. `stateright`) or a hand-rolled linearizability check (`loom` for exhaustive
  interleavings of small cases).

Every property/model scenario ALSO keeps exactly one Phase-1 category tag
(`@sequence`/`@lifecycle`/`@boundary`/`@linearizability`) so coverage stays auditable, plus
`@timing` where a paused clock is needed.

## Authoring rules

1. **State the law, not an instance.** The `Given` quantifies (`Given any capacity c in
   {1,2,7,64,1024}`); the `Then` is the invariant that must hold for all of them.
2. **Generators include boundaries (rule 8).** Every `# GEN:` lists the strategy AND the
   boundary values it must include: `0, 1, MAX-1, MAX`; empty / max / max+1 for collections
   and strings. A property whose generator skips boundaries is rejected at review.
3. **Name the oracle.** `@model` scenarios cite the reference model explicitly in `# ORACLE:`
   (e.g. "a `VecDeque` per sender"); `@property` round-trip laws cite the inverse function.
4. **Cross-reference Phase 1.** Each scenario ends with `# Generalizes:` naming the example
   scenario(s) it subsumes, so the two layers stay linked.
5. **Facts only.** A law is asserted only where source/spec guarantees it for all inputs. If
   a law holds only "usually" (timing, scheduler fairness) say so in `# GEN:` and prefer a
   `@model` refinement check over an exact-count `@property`.
6. **Bugs stay bugs.** A property that exposes a Phase-1 `@bug` (e.g. "for ANY malformed glob
   key, publish must not panic") carries the same `@bug:<file:line>` and must fail today.

## Layout

Property scenarios live beside their Phase-1 examples as `*.properties.feature`:

```
tests/features/core/mailbox.feature            (Phase 1, examples)
tests/features/core/mailbox.properties.feature (Phase 2, laws)   ← this phase
```

## Property catalog

Compiled from `tests/features/**/*.properties.feature` (21 files, **112 laws** = 81 `@property` + 31 `@model`). Each line: the
law, its kind (`@property` / `@model`), and the oracle. `@bug` = the universal form of a Phase-1
bug probe — must fail today. Generator strategies (with boundary values) live in each scenario's
`# GEN:` note.

### Core (`src/`)

**mailbox** — 5 `@property`, 2 `@model`
- FIFO holds for any capacity c and count n — oracle: `VecDeque` push/pop.
- `try_send` Full iff at capacity, for any c, k∈[0,c].
- unbounded never Full for any n.
- `push_front` drains before channel for any two batches — oracle: front-deque ⧺ channel FIFO.
- after close, every send returns the un-sent signal; buffered drain first.
- `@model` concurrent senders preserve per-sender FIFO under any interleaving — oracle: per-sender `VecDeque`, history must be a valid linearization.
- `@model` strong-sender count refines an integer counter under any clone/drop interleaving — closed ⇔ 0.

**actor_id** — 5 `@property`, 1 `@model`
- `from_bytes ∘ to_bytes == id` for any id — oracle: inverse fn.
- rejects any byte slice len<8; accepts any len==8 — oracle: `u64::from_le_bytes`.
- `generate()` strictly increasing per call.
- Eq/Hash/Ord agree with `sequence_id` for any pair.
- `@model` N concurrent `generate()` → distinct, gap-free `[N0,N0+k)` — oracle: atomic counter handing the range out once.

**actor_lifecycle** — 4 `@property`, 2 `@model`
- `on_link_died` Continue iff Normal/SupervisorRestart else Break, over every reason.
- any panic → Break(Panicked) wrapping that error.
- startup buffer replays any pre-start sequence in send order — oracle: `VecDeque`.
- internal on_start sends precede all buffered externals.
- `@model` all spawn variants reach the same running state for any valid mailbox.
- `@model` actor stops exactly when last strong ref drops; no upgrade after — oracle: integer strong-count.

**actor_ref** — 2 `@property`, 3 `@model`
- Eq/Hash/Ord id-based & consistent for any pair.
- tell to a stopped actor → `ActorNotRunning` for any message.
- `@model` strong/weak counts + upgrade-iff-strong>0 refine integer counters under any interleaving.
- `@model` N concurrent asks each get their own reply, no cross-talk — oracle: per-oneshot identity.
- `@model` any W startup waiters all resolve once, none before on_start completes.

**message** — 4 `@property`, 1 `@model`
- any single-sender sequence handled in send order.
- forward round-trips any target reply v to the caller.
- handler Err(e) on ask → caller `HandlerError(e)`.
- handler Err(e) on tell → on_panic path, never a caller.
- `@model` any ask/tell mix applies each command once — oracle: sequential fold (single-writer).

**reply** — 4 `@property`, 1 `@model`
- Ok(v)/Err(e) round-trip through to_result/into_value/into_any_err for any v/e.
- `ReplySender::send` wires Ok→boxed value, Err→`HandlerError`.
- any infallible-reply type → Ok(self), Error=Infallible.
- `from_ok/from_err/from_result` preserve any value/error.
- `@model` N concurrent forwarded asks each downcast to their own value, no panic/cross-talk.

**request_ask** — 2 `@property`, 2 `@model` (`@timing`)
- reply Ok iff handler delay d < reply_timeout t, else Timeout(None), for any d/t.
- no capacity ⇒ mailbox_timeout fires first, always Timeout(Some(msg)).
- `@model` N concurrent asks → bijection of replies (no cross-talk) for any N.
- `@model` among N asks with t_i, caller i fails iff d ≥ t_i, independently.

**request_tell** — 1 `@property`, 3 `@model`
- `try_send` Ok iff k<c else MailboxFull, for any capacity.
- `@model` every Ok recorded once, every MailboxFull never — recorded set == Ok set.
- `@model` bounded send under backpressure delivers every message exactly once.
- `@model` (`@timing`) `send_after` delivers once after delay, or never if aborted before fire.

**registry** — 1 `@property`, 2 `@model`
- `get::<B>` on a type-A ref (B≠A) is always `BadActorType`.
- `@model` refines `Map<Name,Ref>` with insert-NO-overwrite under any op sequence — oracle: first-wins map.
- `@model` for any name inserted by K concurrent threads, exactly one wins.

**links** — 1 `@property`, 2 `@model`
- on one death each of N linked siblings gets exactly one `on_link_died`, no `mailbox_rx`, for any N.
- `@model` parent_shutdown Release/Acquire: no `mailbox_rx` queued once flag observed true — oracle: `AtomicBool`, forbidden state = queue-after-true.
- `@model` K children dying simultaneously each notify parent once with own `mailbox_rx`.

**supervision** — 2 `@property`, 1 `@model` (`@timing`)
- restart iff predicate(policy, exit_kind, reason) for ALL combos incl Never×SupervisorRestart edge.
- restarted set == strategy's defined index-subset for any ordered child set (OneForOne/OneForAll/RestForOne).
- `@model` child stops as soon as >max failures fall within window, for any burst and (max,w) — oracle: sliding-window counter; edges max=0, w=ZERO/MAX.

**error** — 4 `@property`
- map_msg/map_err preserve the variant tag for every variant.
- `boxed ∘ downcast == identity` for every variant/type; wrong-type `try_downcast` recoverable Err, no panic.
- flatten hoists any inner HandlerError domain to the matching outer variant.

### Actors (`actors/src/`)

**broker** — 5 `@property`, 1 `@model` — glob is the contract (separator `/`, `*` spans `.`)
- receives iff `glob(p).matches(t)` — oracle: the `glob` crate.
- delivery count == matching (subscriber,pattern) registrations (no dedup).
- pruned iff strategy surfaces `ActorNotRunning`; full/slow never pruned.
- `Unsubscribe(None)` clears all patterns, `Some(p)` only p.
- `@model` concurrent publishes refine per-(subscriber,pattern) counters, no loss/dup.

**pubsub** — 4 `@property`, 2 `@model`
- receives iff `filter(msg)` true; false filter never prunes.
- a message reaches exactly `{s : s.filter(m)}`.
- only `ActorNotRunning`/`ActorStopped` prunes.
- `@property @bug:pubsub.rs:125` — Spawned/SpawnedWithTimeout prunes a stopped subscriber for ANY message (fails today).
- `@model` subscriber-set size == distinct ActorIds (re-subscribe overwrites) — oracle: `HashMap<ActorId,Filter>`.
- `@model` concurrent publishes refine per-subscriber counters; filter called once per message.

**message_bus** — 3 `@property`, 1 `@model`
- a publish of M reaches exactly recipients of `TypeId(M)`, none of any other type (no dedup).
- dead recipient pruned iff `ActorNotRunning`, scoped to type M only.
- full/slow never pruned.
- `@model` concurrent multi-type publishes refine per-(type,recipient) counters, no cross-talk.

**message_queue** — 6 `@property`, 1 `@model` — Topic = glob
- Direct delivers iff `routing_key == binding key` (set-deduped per queue).
- Fanout delivers to all bound queues, key ignored.
- Topic delivers iff `glob(binding).matches(key)` — oracle: the `glob` crate.
- Headers all (args ⊆ headers) / any (args ∩ headers ≠ ∅); empty headers ⇒ `HeadersRequired`.
- `@property @bug:message_queue.rs:591` — ANY non-compilable Topic key rejected at bind (fails today).
- `@property @bug:message_queue.rs:707` — ANY non-compilable Topic key ⇒ publish returns error, no panic (fails today).
- `@model` concurrent fanout publishes refine a per-queue counter (== N).

**pool** — 3 `@property`, 2 `@model`
- `new`/`new_async` panic iff size==0; any n>0 builds n live workers.
- Broadcast returns exactly N results in worker order, all Ok on a healthy pool.
- pool size stays N after any death sequence; replacement at same index, re-linked.
- `@model` dispatch selects argmin(load) live worker; total exhaustion ⇒ `Err(ActorNotRunning(msg))` — oracle: integer per-worker load (= in-flight `Weak`s).
- `@model` concurrent dispatches refine a total-handled counter == M, at-most-once.

**scheduler** — 3 `@property`, 2 `@model` (`@timing`)
- SetTimeout fires once at ~construction+d; abort before d ⇒ never.
- SetInterval fires k times by `start_delay + k*period`.
- both return a usable `AbortHandle` for any d/p.
- `@model` an interval to a stopped target terminates cleanly (no panic) for any period.
- `@model` independent concurrent timers each fire on their own schedule, no cross-talk.

### Console

**poller** — 4 `@property`
- `decode ∘ encode == identity` for any Snapshot — oracle: inverse fn (compare re-encoded bytes; `Snapshot` has no `PartialEq`).
- length L accepted iff L ≤ `MAX_FRAME_BYTES`, rejected `InvalidData` iff L > MAX (inclusive gate); boundaries 0/1/MAX±1/0xFFFFFFFF.
- oversized L rejected before any allocation.
- any non-msgpack byte string decodes to error, never panic.

**tui** — 12 `@property`, 1 `@model` — pure helpers
- `spark_height(v,max)` ∈ [1,4], max==0 ⇒ 1.
- `actor_rate` ≥ 0, never panics, 0 on None/zero dt or missing prev.
- `mailbox_bar` ratio ∈ [0,1] (clamped), 10-cell bar, pct ∈ [0,100].
- `backpressure_style` bands the **unclamped** ratio (≥0.8 red / ≥0.5 yellow / else FG), cap==0 ⇒ FG. *(Note: unlike `mailbox_bar`, this ratio is NOT clamped — len>cap lands in red.)*
- `centered_rect` result always fully contained in the input area.
- `short_type_name` never panics, idempotent, ""→"".
- `fmt_short`/`fmt_ago`/`fmt_uptime` never panic for any Duration incl ZERO/MAX; `fmt_uptime` MM/SS ∈ [0,59].
- `braille(l,r)` always a single valid `U+2800..=U+28FF` cell, never panics; `braille(l,r) == braille(l.min(4), r.min(4))` ∀ l,r (the `' '` fallback is unreachable).
- `color_rgb(c)` total over every `Color`, never panics; every unlisted/Reset/default variant ⇒ `FG (205,205,212)`.
- `sparkline_line(samples,max,width)` returns **exactly `width`** braille cells ∀ inputs, never panics; only the last `width*2` samples influence the cells; `width==0` ⇒ empty line.
- `@model` `detect_deadlocks` returns exactly the real cycles, each once, normalized — oracle: reference successor-chase cycle finder.

**server_wire** — 2 `@property`, 1 `@model`
- snapshot `seq` strictly increasing (+1 single-producer) across any poll count — oracle: monotonic counter.
- uptime (monotonic `Instant`) non-decreasing across any poll sequence; `captured_at` (wall-clock `SystemTime::now()`) asserted only as a fresh/plausible stamp — it is NOT monotonic (a clock step can regress it; the client handles this, see invariants.md:201).
- `@model` membership captured under one registry lock is a consistent snapshot for any spawn/stop interleaving — oracle: live-id set model at a valid linearization point.
