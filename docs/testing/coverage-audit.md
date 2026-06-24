# Test-coverage completeness audit (card #74)

> **Scope.** This audits whether every behavioural invariant of the surviving kameo local
> core is exercised across every *relevant input class* — not merely that a scenario exists.
> Source-of-truth inputs: `docs/testing/invariants.md`, `docs/testing/properties.md`, and the
> 42 `tests/features/**/*.feature` files (409 example scenarios + 105 property/model laws).
> Every claim below is grounded in `file:line` (CLAUDE.md rule 0). This is an **audit only** —
> no source or scenario was modified.

## Method

Four read-only agents audited one area each (core lifecycle; core request/registry/
supervision; `kameo_actors`; console). Each built an invariant inventory from the two docs,
cross-checked the source modules for invariants the docs missed (rule 3 — each crate
validates at its own boundary), mapped every invariant to its covering scenario(s), and
graded the **seven input dimensions** the card names:

`B` boundary · `Z` zero/null/empty · `O` negative/overflow/wrap · `H` normal/happy ·
`E` error/failure-domains · `C` concurrency/interleaving · `L` lifecycle.

The three highest-impact findings were re-verified directly against source before
publication (see *Verified facts* at the end).

---

## Per-module coverage matrix

Legend: `●` covered with specific-value assertions · `◐` partial / range-asserted / single-variant ·
`○` gap · `—` not applicable to this invariant class. A cell is graded across the module's
invariants collectively; the *gaps* are itemised in the next section.

### Core (`src/`)

| Module | B | Z | O | H | E | C | L | Headline gaps |
|---|---|---|---|---|---|---|---|---|
| actor_lifecycle | ● | ◐ | — | ● | ● | ◐ | ● | no bounded(0) mailbox; `on_panic`-panics→`OnPanic` & `on_stop`-error side-channel untested; one vague `Then` (`:211`) |
| actor_ref | ● | ◐ | — | ● | ● | ● | ◐ | `WeakActorRef::is_alive` divergence (`actor_ref.rs:2133`) untested; `kill()` mid-handler, `attach_stream`, blocking link/unlink uncovered; 2 vague `Then`s |
| actor_id | ● | ● | ◐ | ● | ● | ● | ● | overflow unreachable on 64-bit (asserted as no-panic+unique — correct); serde `invalid_length` path untested |
| mailbox | ● | ◐ | ○ | ● | ● | ● | ● | `blocking_*`/`poll_*` recv variants untested; bounded(0); metric-slice bare arithmetic (`mailbox.rs:616,707,834`); one vague-range `Then` (`:57`) |
| message | ● | ● | — | ● | ● | ● | ● | strong; only `name()` default + StreamMessage order (deferred) open |
| reply | ● | ● | — | ● | ● | ● | — | infallible type-set only sampled (~80 types); `downcast_err` nested-`unreachable!` path untested |
| request_ask | ● | ● | — | ● | ● | ● | ● | **forward error paths missing** (`try_forward`/dead-target — invariants.md:83 promises them, only happy `forward` tested); panic/kill variant left open though source pins `ActorStopped` |
| request_tell | ● | ● | — | ● | ● | ● | ● | `blocking_send` park-then-succeed not exercised; Recipient tell variants out of scope |
| registry | ● | ● | — | ● | ● | ● | ● | `remove_by_id` "first match on duplicate id" edge untested; strong |
| links | ● | ● | — | ● | ● | ● | ● | **parent-side `send_children_shutdown`/`wait_children_closed` (`links.rs:54-78`) entirely untested & undocumented** |
| supervision | ● | ● | ◐ | ● | ● | ● | ● | `MaxRestartsExceeded{count,max}` payload never asserted; `should_restart` post-state mutation only indirect; 3 legit open `@review-semantics` |
| error | ● | — | — | ● | ● | — | ● | `unwrap_msg`/`unwrap_err` panic paths untested; `PanicError` **lossy serde round-trip** undocumented; pure algebra (no `@model`, acceptable) |

### Actors (`actors/src/`)

| Module | B | Z | O | H | E | C | L | Headline gaps |
|---|---|---|---|---|---|---|---|---|
| broker | ● | ◐ | — | ● | ● | ● | ● | empty-pattern `""` subscribe match not pinned; "twice under one pattern, one dies, prune-removes-both" nuance (`broker.rs:100`) untested |
| pubsub | ● | ● | — | ● | ● | ● | ● | `@bug:pubsub.rs:125` correct (must-fail); no plain Spawned-to-live-subscriber happy example |
| message_bus | ● | ● | — | ● | ● | ● | ● | SpawnedWithTimeout async-prune only in property GEN, no Phase-1 example; otherwise strong |
| message_queue | ◐ | ◐ | — | ● | ◐ | ● | ◐ | **BasicConsume dedup-by-id (`:761`) untested & absent from routing matrix; 4 `AmqpError` variants unreached; no MailboxFull/Timeout-no-prune scenario; QueueDelete/ExchangeDelete cascades untested** |
| pool | ● | ● | — | ● | ● | ● | ● | **Broadcast `panic!` on HandlerError (`pool.rs:402`) untested latent defect**; Dispatch tell-path (`reply_sender None`) untested; tie-break + balance correctly `@review-semantics` |
| scheduler | ● | ● | — | ● | ● | ● | ● | `Skip` MissedTickBehavior never exercised; Burst/Delay exact counts correctly open |

### Console

| Module | B | Z | O | H | E | C | L | Headline gaps |
|---|---|---|---|---|---|---|---|---|
| poller | ● | ● | ● | ● | ● | — | ● | write-timeout (`poller.rs:99`) never asserted; otherwise complete — size-gate boundaries all present in GEN |
| tui | ● | ◐ | ◐ | ● | — | — | — | **`braille`/`color_rgb`/`sparkline_line` have feature scenarios but are absent from both doc inventories & have no `@property` law**; `fade_toward_bg` factor-0.5 unpinned; `rate_context` negative-dt→0 path untested |
| server_wire | ● | ● | ○ | ● | ● | ● | ● | `Totals` rollup arithmetic (`registry.rs:442,454`) rule-2 overflow untested; grave-window `> ttl` boundary unpinned; **doc mis-tags the cap scenarios (see Doc corrections)** |

---

## Prioritised missing input-class cases (new scenarios to add)

### P0 — correctness / defect-probe gaps

1. **`message_queue` BasicConsume dedup-by-id** (`actors/src/message_queue.rs:761`,
   `if !recipients.iter().any(|reg| reg.actor_id == actor_id)`). A consumer attached twice to
   the same `(queue, type)` is stored **once** — the *opposite* of broker/message_bus
   (no-dedup) and pubsub (overwrite). No scenario, and the module is missing from the
   cross-module routing matrix (`invariants.md:133-141`). Add: dedup-on-double-consume
   example + property; add a `message_queue` row to the matrix.

2. **`message_queue` MailboxFull / Timeout never-prune** — the delivery path
   (`message_queue.rs:438-452`) prunes only on `ActorNotRunning`, like the others, but no
   scenario asserts a *full or slow* consumer survives. Add a no-prune example mirroring
   `broker.feature:135/146`.

3. **`pool` Broadcast `panic!` on `HandlerError`** (`actors/src/pool.rs:396-404`). When a
   broadcast worker's reply is `SendError::HandlerError`, the map closure calls
   `panic!("reset err infallible called on a SendError::HandlerError")` — a run-loop panic
   reachable whenever `A::Reply::Error != Infallible`. Currently undocumented and untested.
   Add a **`@bug` probe** asserting the desired behaviour (error surfaced in the `Vec`, no
   panic), or confirm-as-contract if intended.

4. **`request_ask` forward error paths** — `invariants.md:83` promises `try_forward`→
   `MailboxFull` + `ctx.reply` restore, dead-target→`ActorNotRunning`, and `blocking_forward`
   capacity-wait, but `request_ask.feature` exercises only the happy `forward` (`:66-72`).
   Add the three error/boundary forward scenarios.

5. **`links` parent-side shutdown protocol** — `send_children_shutdown` (`links.rs:54-65`)
   and `wait_children_closed` (`links.rs:67-78`) are the parent half of the death-watch
   deadlock-prevention story; only the child side (`set_children_parent_shutdown` +
   Release/Acquire) is covered. Add a `@sequence`/`@lifecycle` scenario for parent-initiated
   shutdown join.

### P1 — boundary / error-domain / overflow gaps

6. **bounded(0) zero-capacity mailbox** — tokio `bounded(0)` panics; no defensive-boundary
   scenario exists in `mailbox.feature` or `actor_lifecycle.feature` (`spawn_with_mailbox`).
   Add a `@boundary` probe.

7. **mailbox `blocking_recv` / `poll_recv` / `blocking_recv_many` / `poll_recv_many`**
   (`mailbox.rs:665-847`) — `invariants.md:71` claims "all four recv variants pop front
   first" but only `recv`/`recv_many` have scenarios. Add front-priority scenarios for the
   other recv variants and for `blocking_send`/`send_timeout` (`mailbox.rs:201-250`).

8. **`message_queue` unreached `AmqpError` variants** — `QueueInUse`, `ExchangeInUse`,
   `QueueAlreadyExists`, `ExchangeAlreadyExists` (`message_queue.rs:213-231`) plus
   `QueueDelete`/`ExchangeDelete` happy + `if_unused` paths and `QueueUnbind`-missing-exchange
   (`:654`) have no scenario. Each error variant should be reached by its own cause (rule 3).
   Include the `ExchangeDeclare` empty-name→`ExchangeAlreadyExists` oddity (`:507`).

9. **`supervision` `MaxRestartsExceeded{restart_count, max_restarts}` payload** — referenced
   only in comments (`supervision.feature:111,158`); add a `Then` asserting the carried counts,
   and one asserting `should_restart`'s post-state mutation (`links.rs:261-262`).

10. **`server_wire` `Totals` arithmetic** — `registry.rs:442` (`messages_received +=`) and
    `:454` (`REAPED_STOPPED + stopped_now`) use bare `+` (rule 2: no overflow path). Add an
    overflow/boundary property; pin the grave-window `elapsed() > ttl` exclusivity at exactly
    `ttl` (`registry.rs:470`).

11. **`actor_ref` `WeakActorRef::is_alive` divergence** (`actor_ref.rs:2133-2135`) — flagged
    as a "Surprise" in `invariants.md:56` but no scenario exercises the case where it diverges
    from `ActorRef::is_alive` (window after mailbox-close, before `shutdown_result` set).

12. **`scheduler` `Skip` MissedTickBehavior** (`scheduler.rs:172`) — only Burst/Delay are
    exercised; add a `Skip` scenario (or document it as out-of-contract).

### P2 — minor / cosmetic / sampling

13. tui `fade_toward_bg` factor-0.5 (u8-truncation midpoint) — only `0.0`/`1.0` asserted
    (`tui.feature:122`, NOTE at `:132`); `rate_context` negative-dt→`None`→rate 0
    (`tui.rs:1186`) untested; `severity`/`compare`/`sort_actors` tie-break (`tui.rs:1204-1227`)
    have no scenario.
14. `reply` infallible type-set (`reply.rs:636-761`, ~80 types incl tuple arities/atomics/
    NonZero) only sampled; `error` `unwrap_msg`/`unwrap_err` panics (`error.rs:171-188`);
    `actor_id` Display/Debug + serde `invalid_length` (`id.rs:162-237`); `request_ask` `forward`
    happy-only; `registry` `remove_by_id` duplicate-id "first match" edge.
15. Vague `Then`s to tighten (rule 8): `mailbox.feature:57` (count "1..5" not exact 5);
    `actor_lifecycle.feature:211` ("the actor stops" — omits `Break(Normal)` reason);
    `actor_ref.feature:147` ("nothing changes"); `request_ask.feature:194` ("MsgB cannot be
    enqueued" — no specific error).

---

## Invariants present in source but absent from both docs

| Source `file:line` | Invariant | Doc status |
|---|---|---|
| `actors/src/message_queue.rs:761` | BasicConsume **dedup-by-actor_id** per `(queue,type)` | absent from routing matrix & all scenarios |
| `actors/src/message_queue.rs:370-408` | `queue_delete` / `basic_cancel` auto-delete cascades | undocumented, untested |
| `actors/src/pool.rs:396-404` | Broadcast `panic!` on `HandlerError` reply | undocumented latent defect |
| `actors/src/pool.rs:346-358` | Dispatch tell-path (`reply_sender == None`) | undocumented, untested |
| `src/links.rs:54-78` | `send_children_shutdown` / `wait_children_closed` (parent shutdown) | undocumented, untested |
| `src/actor/kind.rs:402-414` | `on_panic` that itself panics → `PanicReason::OnPanic` | undocumented, untested |
| `src/actor/spawn.rs:253-282` | `on_stop` error side-channel (hook + `shutdown_result`, future still `Ok`) | undocumented, untested |
| `src/actor/actor_ref.rs:2133-2135` | `WeakActorRef::is_alive` divergent predicate | named in prose (`invariants.md:56`), untested |
| `src/error.rs:171-188` | `unwrap_msg`/`unwrap_err` panic on wrong variant | undocumented, untested |
| `src/error.rs:564-661` | `PanicError` **lossy** serde round-trip (err→Display String) | undocumented; defensible silent-lossiness, no probe |
| `src/mailbox.rs:616,707,834` | metric-slice bare arithmetic (`len-1-count..`) | rule-2 boundary unverified |
| `src/console/registry.rs:442,454` | `Totals` bare-`+` rollup | rule-2 overflow unverified |
| `console/src/tui.rs:1547,1579,1597,1880` | `sparkline_line`, `braille`, `color_rgb`, `fmt_cycle` helpers | have feature scenarios but absent from doc inventories |
| `src/actor/id.rs:218-221` | serde `Visitor` `invalid_length` mapping | undocumented, untested |

---

## Doc corrections (factual discrepancies found, verified against source)

1. **`invariants.md:198`** claims "two `@review-semantics` scenarios pin [the client-side 64
   MiB cap]." **`server_wire.feature` contains zero `@review-semantics` tags** — the two
   relevant scenarios (`:144` request-byte-ignored, `:161` no-frame-size-cap) are tagged
   `@boundary` (verified: `grep` over the file). The *behaviour* is pinned correctly
   (`server.rs:98-119` never reads a client length); only the tag name in the doc is wrong.

2. **`invariants.md:133-141` routing matrix omits `message_queue`** entirely, despite
   `message_queue` having its own register-dedup semantics (`:761`, dedup-by-id) distinct from
   all three listed modules. Add a fourth row.

3. **`message_queue` `@bug` probes reference `AmqpError::InvalidRoutingKey`, which does not
   exist** (`AmqpError` has 9 variants, `message_queue.rs:212-231`; verified). This is
   *consistent* with their must-fail intent — the fix requires adding the variant, so the
   scenarios stay red (in fact won't compile) until source is fixed. Worth a one-line note in
   `invariants.md:160` so a future implementer knows the variant must be added.

## Verified facts (re-checked against source post-audit)

- `server_wire.feature` has no `@review-semantics` tag; the cap scenarios are `@boundary`
  (`tests/features/console/server_wire.feature:144,152,161,171`).
- `AmqpError` enum (`actors/src/message_queue.rs:212-231`) = {ExchangeAlreadyExists,
  QueueAlreadyExists, ExchangeNotFound, QueueNotFound, BindingAlreadyExists, HeadersRequired,
  InvalidHeaderMatch, ExchangeInUse, QueueInUse} — **no `InvalidRoutingKey`**.
- `pool.rs:396-404` contains a live `panic!("reset err infallible called on a
  SendError::HandlerError")` in the Broadcast map.
- All three existing `@bug` probes still match their cited source lines and are written to
  fail today (assert the *desired fixed* behaviour, never a green test documenting the bug):
  `pubsub.rs:125` (Spawned/SpawnedWithTimeout `tokio::spawn` discards the tell result → dead
  subscriber never pruned), `message_queue.rs:591` (QueueBind has no glob validation),
  `message_queue.rs:707` (`Pattern::new(..).unwrap()` panics at publish).

## Audit verdict

GEN boundary coverage in the property files is **strong** — every `@property`/`@model` GEN
checked includes `0/1/MAX` (or `ZERO/MAX` for durations) and `N>c` for collections; no GEN was
found omitting a claimed boundary. The example layer's main weaknesses are (a) `message_queue`
(dedup, four error variants, delete cascades, no-prune), (b) a handful of undocumented
source invariants — two of which (`pool.rs:402` panic, the `links` parent-shutdown half) are
correctness-relevant, and (c) a small set of vague `Then`s and missing zero/blocking-variant
boundaries. No green-test-documenting-a-bug was found anywhere; all deferred items are
correctly `@review-semantics`/NOTE rather than guessed assertions.

---

---

## Addendum — scenarios authored (2026-06-24, spec-only)

Following the audit, the P0 + P1 gaps above were filled as Phase-1 example and Phase-2
property scenarios (no step definitions, no source changes). Summary of what landed:

| File | Added | Covers |
|---|---|---|
| `actors/message_queue.feature` | +11 (4 `@lifecycle`, 7 `@boundary`) | dedup-by-id, full-consumer-no-prune, queue-delete cascade, cancel→auto-delete, + 7 unreached `AmqpError` variants |
| `actors/message_queue.properties.feature` | +2 `@property` | dedup idempotence ∀k, prune-iff-ActorNotRunning ∀strategy |
| `actors/pool.feature` | +2 | Broadcast no-panic (`@review-semantics`), Dispatch tell-path (`reply_sender None`) |
| `actors/scheduler.feature` | +1 `@boundary @timing` | `Skip` MissedTickBehavior |
| `core/request_ask.feature` | +3 `@boundary`/`@sequence` | `try_forward` MailboxFull, `forward` dead-target, `blocking_forward` |
| `core/links.feature` | +4 | parent shutdown ordering, `send_children_shutdown`, `wait_children_closed`, no-children boundary |
| `core/links.properties.feature` | +1 `@property` | parent shutdown fires once-per-child, waits exactly those mailboxes |
| `core/mailbox.feature` | +5 | `blocking_recv`/`poll_recv`/`*_many` front-priority, `bounded(0)` panic, `blocking_send`/`send_timeout` |
| `core/supervision.feature` | +2 `@lifecycle` | `MaxRestartsExceeded{count,max}` payload, `should_restart` post-state mutation |
| `core/actor_ref.feature` | +1 `@lifecycle @review-semantics` | `WeakActorRef::is_alive` predicate divergence |
| `console/server_wire.feature` | +2 | grave-window `> ttl` exclusivity, `total_stopped` reap-boundary conservation |
| `console/server_wire.properties.feature` | +1 `@property` | `total_stopped` counts each stop once ∀ reap schedule |
| `docs/testing/invariants.md` | +1 matrix row | `message_queue` dedup-by-id / prune row (doc correction #2) |

**Reclassification (P0 item 3, `pool.rs:401` Broadcast panic).** On reading the source, the
`panic!` is reached only via `SendError::HandlerError`, but the broadcast uses a *tell*
(`worker.tell(..).send()`), which has no reply channel and so can only yield
`ActorNotRunning`/`MailboxFull`/`Timeout` — never `HandlerError`. The panic arm is therefore
unreachable defensive code (the same shape as `reply.rs:535`'s `unreachable!`). It was
**not** authored as a `@bug` probe: a `@bug` must fail today (rule 8), but this panic does not
fire on any reachable input, so a probe would pass green — itself the forbidden "green test
documenting a bug." It is instead a `@review-semantics` scenario pinning the healthy-path
result and flagging the guard's reachability for the wiring phase.

Doc corrections #1 (`invariants.md:198` `@review-semantics`→`@boundary` mis-tag) and #3
(`AmqpError::InvalidRoutingKey` does not yet exist) were **not** applied — they were left for
your review since you opted only into scenario authoring; both remain accurately recorded
above.

---

**Status:** P0 + P1 scenarios authored (see Addendum). Remaining open choices:
- **(A)** apply the two un-applied doc corrections (`invariants.md:198` tag fix; the
  `AmqpError::InvalidRoutingKey` note); and/or
- **(B)** author the P2 (cosmetic/sampling) cases and tighten the four vague `Then`s; and/or
- **(C)** proceed to Phase 3 (wire `cucumber` + step definitions), folding these scenarios in
  per-module as each is wired.
