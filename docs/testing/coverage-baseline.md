# Coverage baseline (card #85)

Reproducible via crane, with **two engines** selected by system:

```bash
nix build .#coverage -L            # system default: llvm-cov on Darwin, tarpaulin on Linux
nix build .#coverage-llvm -L       # force llvm-cov (any system) -> result/html/index.html
nix build .#coverage-tarpaulin -L  # force tarpaulin (Linux only) -> result/tarpaulin-report.html
```

- **`coverage-llvm`** (crane `cargoLlvmCov`) — works on every system, region/branch accurate,
  instrumented by the version-matched `llvm-cov`/`llvm-profdata` from the toolchain's
  `llvm-tools` component (`rust-toolchain.toml`).
- **`coverage-tarpaulin`** (crane `cargoTarpaulin`) — Linux-only opt-in. NOTE: its ptrace
  engine **hangs on this tokio-multi-threaded / async cucumber suite** (verified — the
  post-merge run wedged 40+ min in the test phase), so it is exposed for completeness only.
- **`coverage`** is **llvm-cov on every system** — the reliable engine that actually
  completes here; it is what the merge workflow and the numbers below use.

All run `cargo … test --workspace` with default features; non-gating (instrumentation
recompiles the world — too slow for the per-push gate). `remote` (libp2p) is off by default, so
it is never compiled or counted (M1 deletes it). On every **merge to `main`**, the
`coverage.yml` workflow rebuilds this and publishes the browsable HTML to GitHub Pages at
`…/bombay/coverage/` (and as the `coverage-html` artifact). The numbers below are from
`coverage-llvm`; tarpaulin's totals differ slightly (different instrumentation granularity).

## bombay-core — the M1 core rebuild (#112+)

The rebuilt spine lives in the `bombay-core` crate (part-by-part, epic #122), born
under the god-level bar (no #61 quarantine). It is measured by the same
`--workspace` coverage run and adds a **reproducible mutation gate**:

```bash
nix build .#mutants -L   # cargo-mutants on bombay-core; fails if any mutant survives
```

Pinned via the flake's `nixpkgs` (never `nix run nixpkgs#…`), mirroring the coverage
package. On-demand, not a per-push gate (rebuilds+tests once per mutant).

### `mailbox` (#112, redesigned #133) — done
Zero-box `Signal<A>` queue behind a **`flume`** channel (chosen on measured evidence —
ADR-0001; `flume` is isolated inside the sender/receiver wrappers = the seam).
Construction hangs off the `Mailbox::<A>::bounded(cap)` namespace (composable, no
free-floating `bounded()`). Pure transport: `send`/`try_send`/`recv`/`downgrade`/`drain`
— **no `close()`**; graceful shutdown is `Signal::Stop` + `drain` at the run-loop (#116).
18 tests: round-trip, backpressure hand-back, `Capacity` boundaries (proptest incl.
`0`/`MAX±1`/`usize::MAX`) + `CapacityError`, `MAX`-constant + `Display` guards, lifecycle
(send-after-drop / recv-none / drain-flush), `LinkDied` boxed-slot `size_of` guard +
monomorphic worst-case demo, weak death-watch, an 8-thread `Barrier` linearizability
test, and a single-sender FIFO proptest. **Mutation: 0 missed** (`nix build .#mutants`).
Criterion (`benches/mailbox.rs`, realistic ~40 B command): `tell` ≈ **5.7 ns**, send+recv
≈ **18.4 ns** (~40 % faster than the tokio v1 on the same bench). Channel eval:
`benches/channels.rs` (ADR-0001) — flume wins at both `u64` and `~40 B` payloads.

**DST posture — loom/shuttle deferred to #116/#120.** loom and shuttle can only
model-check code compiled against *their* primitives; the real `tokio::sync::mpsc` this
mailbox wraps is opaque to both ([loom](https://docs.rs/loom/latest/loom/) requires
"the code being tested specifically uses the loom replacement types";
[shuttle](https://github.com/awslabs/shuttle) requires replacing std primitives with
its equivalents). The mailbox delegates all synchronization to tokio (which loom-tests
its own channel internally), so a loom/shuttle test here would either explore nothing or
test a reimplementation (violates the "test the actual SUT" rule). The first
bombay-owned concurrency is the run-loop `select` over signals (#116) and the death-watch
push (#120) — that is where loom/shuttle land. The 8-thread linearizability test is the
mailbox's concurrency coverage until then.

### `error` (#113) — done
Typed error domains, rebuilt to diverge from kameo where the type system pays off.
The single kameo `SendError` is **split into two honest types**: `TellError<M>`
(fire-and-forget *delivery* failures — `ActorNotAlive`/`MailboxFull`, both hand the
message back) and `AskError<M, E>`, which **composes** `TellError` via `Deliver(..)` and
adds the three reply-side failures a `tell` can never have (`Timeout`, `Interrupted`,
`Handler(E)`). So a `tell` caller cannot even *name* `Timeout`/`Handler`, and whether the
message is returned is encoded in the variant, not an `Option<M>`. Retryability is a
**method** (`is_retryable`/`is_terminal`), never a caller's guess — only delivery
backpressure is retryable; a `Timeout` is not (the message is already in the actor) and a
`Handler` domain error (where a nexus `Conflict` lives) must never be re-driven as
backpressure (rule #3). `ActorStopReason` (`Normal`/`Killed`/`Panicked`/`SupervisorRestart`
+ `is_normal`) and `PanicError` complete the lifecycle side; `PanicError` holds the
type-erased payload behind a plain **`Arc<dyn ReplyError>`** (no `Mutex` — the `Send + Sync`
bound makes the shared payload thread-safe), recoverable by `downcast::<T>()` / `with_str`.
`PanicReason` distinguishes a lifecycle-hook failure from a handler panic (the supervisor's
restart-storm signal). **`downcast-rs`** + **`thiserror`** adopted (rule #3: no manual
`Display`). 13 tests: retry/terminal classification (tell + ask), the `@bug`
`conflict_is_domain_not_retryable` probe, message/error recovery, `From<TellError>`
composition, `map_msg`/`map_err`, `PanicReason`/`is_normal` classification, `PanicError`
downcast/`with_str`/clone-shares-`Arc`, and Display-message stability. **Mutation: 0 missed**
(`nix build .#mutants`; 17 caught, 12 unviable). No atomics/ordering here → loom/DST N/A.
**Deferred (tracked):** `ActorStopReason::LinkDied`/`PeerDisconnected`, `PanicReason::OnLinkDied`,
`TellError::SendTimeout`, and `serde` on `ReplyError`/`PanicError` land with their producing
cards (#120/#121, request builders #118, the Zenoh tier).

### `message` (#114) — done
`bombay-core/src/message.rs` carries the `Msg` marker trait: an actor's single closed
message type, queued **by value**, with `SLOT_BUDGET` (default 256 B / 4 cache lines)
as the per-slot byte bound. Trait covered by 3 `bombay-core` unit tests — default-256
pin, hand-override, and usability as a generic bound. Mutation testing yields no
signal for this module: `cargo-mutants` mutates function bodies only, so it
generates no mutants for a trait-const-only file — the `SLOT_BUDGET` default is
pinned by the `slot_budget_defaults_to_256` unit test instead.

The `#[derive(Msg)]` proc-macro (`macros/src/derive_msg.rs`) implements the trait and
emits a compile-time slot-size tripwire; it sits outside the `bombay-core` mutation
gate by design (proc-macros compile out-of-process, same as the "known limitation"
below) and is instead covered by: native runtime tests (`macros/tests/derive_msg.rs`)
for the generated impl on the default budget and the `#[msg(budget = N)]` override;
`parse_budget` unit tests for the attribute grammar (value present, absent,
non-integer, bare key, unknown key, duplicate within one `#[msg(...)]` and across two,
negative, and overflowing-integer rejection); direct `syn::parse_str::<DeriveMsg>`
unit tests for the generic- and union-rejection guards; and six paired `///`
doctests on the derive in `macros/src/lib.rs` — three that must keep compiling
(the initial within-budget example, the boxed-remedy, and the `#[msg(budget = N)]`
escape) plus three `compile_fail` (budget tripwire, generic rejected, union
rejected). No README change —
the rebuilt spine is not behind the umbrella yet (same as #113/#133).

### `reply` (#115) — done
`bombay-core/src/reply.rs` carries the typed single-shot reply channel:
`ReplySender<R, E>` / `ReplyReceiver<R, E>` / `reply_channel()` over
`tokio::sync::oneshot<Result<R, E>>` (ADR-0002). Kameo's `Box<dyn Any>`
`Reply`-trait erasure is **dropped** — a typed port erases nothing, so any
`R: Send + 'static` is a reply. `send`/`send_err` consume `self` (double-reply is
a compile error, proved by a `compile_fail` doctest); a gone asker is reported as
`AskerGone` (a unit signal — a reply to a vanished asker is un-actionable, so no
payload is handed back). `recv` maps the oneshot outcome into #113's `AskError`
(`Ok(Ok r)→Ok(r)`, `Ok(Err e)→Handler(e)`, sender-dropped→`Interrupted`), generic
over the never-produced `M`. Covered by 8 tests: the `@bug` typed-handler-error
probe, the Ok-reply sequence, the drop→`Interrupted` lifecycle (never hangs), the
`send`-to-gone-asker defensive case, the `Infallible` (tell) roundtrip, a 2-thread
barrier'd linearizability test, a deterministic **recv-parks-then-send-wakes** test
(the reverse ordering — exercises the oneshot waker path the buffered-value tests
skip), and a proptest sweeping all three handler actions. Benched
(`benches/reply.rs`): the typed roundtrip is **≈1.5× faster than the erased
`Box<dyn Any>` path** (21.4 µs vs 32.8 µs /1k — the box+downcast cost #115 removes;
ADR-0002). **Mutation: 0 missed** (4 mutants: `send`/`send_err` whole-body →
`Ok(())` both caught; `recv`/`reply_channel` whole-body replacements are
*unviable* — they need `R: Default` / a `Default` impl the generic types lack, so
`cargo-mutants` cannot mutate the arms of the generic `recv` individually. Those
three arms — `Ok→Ok`, `Err→Handler`, drop→`Interrupted` — are instead pinned
behaviorally by the ok/handler/drop tests + the DST). No bombay-owned atomics →
loom N/A (delegated to tokio oneshot), same as #113.
`DelegatedReply`/`ForwardedReply` deferred to #116/#118 (recorded on #115).

### `actor` (#116) — done
`bombay-core/src/actor/` carries the local actor spine: the `Actor` trait +
lifecycle hooks (`mod.rs`), the run-loop (`kind.rs`), the minimal
`ActorRef`/`WeakActorRef` handle (`actor_ref.rs`), and the spawn entry points
`PreparedActor`/`RunResult` + the `Spawn` ext-trait (`spawn.rs`). The loop is
**finish-current-then-stop, no drain**: `on_start` (caught) → a `select` over
`CancellationToken::run_until_cancelled(recv)` → `on_stop` (caught; a returned
`Err` is logged via `log_on_stop_outcome`, never unwrapped, and the stop
`reason` is preserved). Four `catch_unwind` boundaries turn a panic into an
inspectable `PanicError` instead of tearing down the task — `handle`, `on_stop`,
`on_start`, and `on_panic` — and a hard kill is a uniform `futures::Abortable`
wrap of the whole lifecycle (skips `on_stop` → `RunResult::Killed`). A `handle`
that returns `Err` is a controlled crash routed through `on_panic` exactly like a
caught unwind (both → `ActorStopReason::Panicked`). `default_capacity()` is
pinned by a unit test so its `expect` can never trip.

**14 tests** (13 in `spawn.rs`, 1 in `actor_ref.rs`), organized by the rule-#7
cross-cutting categories:
- **Sequence/protocol** — queued-messages-then-`Signal::Stop` handled in order
  then stop; `*stop = true` stops after the current handler returns; on-start
  messages handled *after* `on_start` in FIFO order (proves the no-buffer /
  mailbox-waits contract); a returned `Err` stops as `Panicked`.
- **Lifecycle** — graceful cancel finishes the in-flight handler then stops;
  hard `kill` skips `on_stop` and drops in-flight; `on_start` `Err` and
  `on_start` panic both → `RunResult::StartupFailed` (unwind pinned to the
  `on_start` boundary); a handler panic → `on_stop` runs with a `Panicked`
  reason; weak-ref upgrades while open then `None` after the strong ref drops.
- **Defensive boundary** — the poison contract: `on_stop` after a panic observes
  torn state (release-only, never reads domain fields); a post-panic `send`
  fails; the `on_start` panic is caught (unwind never escapes the pin).
- **Linearizability** — concurrent senders drive a single-writer actor to an
  exact final count (real overlap via `tokio::spawn`).

Mutation (`nix build .#mutants`) and loom/DST are not re-measured in this card;
the first bombay-owned concurrency the run-loop introduces (the `select` over
signals) is the loom/shuttle target noted under `mailbox` above. No README
change — the rebuilt spine is not behind the umbrella yet (same as #113/#115).

### `actor-ref` self-reference & ref-count stop (#117) — done
Makes the #116 "all-senders-gone" loop arm reachable (ADR-0003). The run-loop no
longer holds a strong self-ref: `spawn.rs` downgrades and drops its `ActorRef`
before the loop, which now takes a `&WeakActorRef` + the cancel token
(`kind.rs`). Each `Signal::Message` gains a `self_sender: MailboxSender<A>` — a
**strong** clone of the enqueuing sender — so a queued message pins the actor
alive until handled (drain-then-stop), and the loop lifts a strong `ActorRef`
out of the dequeued signal via `WeakActorRef::with_sender`. New public entry
points: `ActorRef::tell` / `is_alive` and `MailboxSender::send_message` /
`is_closed`; `MailboxReceiver` gains a draining `Drop`.

**+3 tests** over the #116/#112 baselines:
- **Lifecycle / ref-count stop** (`spawn.rs`) — `dropping_last_actor_ref_stops_the_actor`:
  dropping the last strong `ActorRef` closes the mailbox and stops the actor
  `Normal` (this arm hung in #116).
- **Sequence / self-pin** (`spawn.rs`, `@bug`) —
  `queued_message_is_handled_even_if_last_ref_drops_first`: the everyday
  `tell; drop` pattern; a message enqueued while a ref existed is handled even
  when the last ref drops before the loop dequeues it. FAILS under the rejected
  "loop upgrades a weak self-ref" design (Design D).
- **Lifecycle / anti-leak** (`mailbox.rs`, falsifiable) —
  `dropping_receiver_mid_backlog_frees_the_queued_message`: an `Arc` canary in
  the payload proves a receiver dropped mid-backlog frees the queued signal (and
  its embedded `self_sender`), breaking the `Shared → queue → Signal → Sender`
  cycle. Verified to FAIL with `impl Drop for MailboxReceiver` removed — it
  guards precisely that mechanism, not incidental behavior.

No README change — the rebuilt spine is not behind the umbrella yet (same as
#116); `tell`/`is_alive` are steps toward the already-documented kameo target
API (ergonomic ask/tell builders are #118).

### `recipient` type-erased fan-in (#145) — done
`bombay-core/src/actor/recipient.rs` carries `Recipient<M>` / `WeakRecipient<M>`:
type-erased, zero-box fan-in handles that broadcast one `M` to **heterogeneous**
actors whose closed menu satisfies `A::Msg: From<M>` (ADR-0004). A private
`ErasedRecipient<M>` / `ErasedWeakRecipient<M>` trait object (`Arc<dyn …>`) erases
the actor; the send converts `M -> A::Msg` **by value** — the message never boxes,
only the handle — and enqueues via `MailboxSender::try_send_message` (the new
non-blocking sibling of `send_message`). The `M: Clone` bound is the honest price
of "zero-box message + typed handback + erasure": there is no `A::Msg -> M`, so
the original `M` is cloned before conversion to hand it back on failure. Sub-task
of #117; ships the **tell-side only** — `ReplyRecipient` is deferred to #118 (no
reply port in `Signal::Message` yet), its anticipated `ReplyRecipient<M, R, E>`
shape recorded in ADR-0004. New public API: `Recipient`/`WeakRecipient`,
`ActorRef::recipient::<M>()`, `From<ActorRef<A>>`, `MailboxSender::try_send_message`.

**10 tests** (`recipient.rs`) + 1 (`mailbox.rs`, `try_send_message`):
- **Sequence / erasure** — `try_tell` and async `tell` deliver the converted
  variant; the headline `broadcast_reaches_heterogeneous_actors_as_their_own_variant`
  fans one `Tick` over a `Vec<Recipient<Tick>>` of two DIFFERENT menus and asserts
  each receives its own variant (`LedgerCmd::Post` / `AuditCmd::Record`) — the
  proof that erasure routes by the real `From` impl, not a default.
- **Defensive boundary / handback** — a full mailbox and a stopped actor hand the
  EXACT original `M` back (`MailboxFull(Tick)` / `ActorNotAlive(Tick)`);
  `try_send_message` likewise pins `Full`/`Closed` with the returned payload.
- **Lifecycle** — `downgrade` → `upgrade` is `Some` while a strong sender lives,
  `None` after all strong senders drop; `id` preserved through erasure and
  downgrade.
- **Guards** — hand-written `Debug` (names struct + id) and `is_alive` tracking.

No README change (same target-API posture as #113/#115/#116/#117). The #117
finalization matrix (bench/mutation/property/fuzz/MIRI/DST + exact-memory/no-leak)
for this code is owned by #146–#152.

## Baseline — 2026-06-29 (after #77)

Workspace line coverage **60.85% (5686/9345)** — but that blends the SUT with untested crates
and compile-time-only code. The honest per-area picture:

| Area | Line cov | Note |
|---|---|---|
| **kameo core `src/`** (the #77-wired modules) | **76.7%** (4098/5342) | the wired surface |
| in-tree `src/console/` | 95–98% (minus `demo.rs`, a non-SUT demo at 0%) | #76 |
| `console` crate — `tui.rs` | **93.24%** (1393/1494) | **#82** lifted it 73% → 93%: keystroke render scenarios for every `state_cell` arm, all sort keys + direction toggle, tree collapse/expand, the `+`/`-` poll-interval keys (with clamps), the full inspect-panel field blocks, and the focused-panel scroll edges |
| `console` crate — `poller.rs` | **82.35%** (154/187) | **#82** lifted it ~69.5% → 82%: the reconnect-backoff loop and Ok-poll interval pacing are now covered via injectable-time seams (`retry_until_some` / `pacing_sleep` / `drive_polls`) driven by a fake clock — no real sleeps. The residue is the thin `spawn_poller`/`connect_loop`/`poll_loop` delegating shells (real forever-thread + IO, not run in tests) |
| **`actors` crate** | (re-measure pending) | **#78 wired** the `broker` / `pubsub` / `message_bus` / `message_queue` modules to the SUT via cucumber BDD runners (was 0% / 0–971 at the #77 baseline); the next `coverage-llvm` run on merge refreshes the exact number. `pool` / `scheduler` remain unwired. |
| `macros` crate | ~4% | see "known limitation" below |

### The real gaps inside the #77-wired core (ranked)
| Line cov | File | Read |
|---|---|---|
| **25.0%** (6/24) | `src/request.rs` | tiny module (thin builder/`IntoFuture` glue); ~18 uncovered lines, low value |
| **46.5%** (463/995) | `src/actor/actor_ref.rs` | **biggest real gap** — 532 uncovered lines despite 22 wired scenarios. The many ask/tell/query overloads, `Recipient`/`ReplyRecipient` erasure variants, blocking variants, and error paths are under-exercised. Highest-value place to add scenarios. |
| 71.6% (303/423) | `src/request/tell.rs` | uncovered timeout/blocking/error branches |
| 72.1% (258/358) | `src/error.rs` | uncovered combinator/Display branches |
| 72.4% (184/254) | `src/actor/kind.rs` | run-loop branches |
| 76.0% (127/167) | `src/message.rs` | dispatch edges |

Well-covered (≥80%): `supervision` 95%, `spawn` 93%, `actor` 91%, `links` 89%, `reply` 89%,
`id` 88%, `registry` 87%, `request/ask` 81%, `mailbox` 81%.

### Cross-module integration (#87)
The #77 coverage above is per-module (one isolated `World` each). `tests/core_integration.rs`
adds 5 end-to-end scenarios over the subsystem INTERACTIONS: supervision × mailbox (no message
lost or duplicated across a restart under concurrent producer load), supervision × registry (the
registry entry stays resolvable and alive across a child restart), links × mailbox (`on_link_died`
fires for a dying peer while the watcher keeps draining its own mailbox), and the OneForAll /
RestForOne cascade restart-sets with in-flight messages preserved. These guard regressions that
only manifest in the interaction between modules — which line coverage of isolated scenarios
cannot catch.

### Known limitation — proc-macros read ~0%
The `macros` crate (`messages.rs` 0/437, the `derive_*`) runs at **compile time**, in a separate
process during the build of crates that USE the macros — runtime `llvm-cov` of the test binaries
cannot see it. Covering it needs expansion/`trybuild` tests, a distinct concern (not "write more
runtime scenarios"). Likewise `demo.rs` is a non-SUT demo entrypoint. `console/src/main.rs` and
the literal `event::read()` poll are now exercised by the **Tier-2 PTY smoke test** (#83, below);
note that `llvm-cov` still reports them near-0% because the test drives a *separate* compiled
process, whose instrumentation the test-binary coverage run does not aggregate — the guarantee is
behavioural (the binary boots, polls input, and quits cleanly), not a line-count bump.

### Tier-2 (PTY / "Selenium-for-terminals") — #83
`console/tests/pty_smoke.rs` drives the real `bombay-console --demo` binary through a
pseudo-terminal (`portable-pty`), re-emulates the visible screen from the raw PTY bytes with
`vt100`, and asserts on the rendered grid: dashboard renders → `?` opens the help popup (via the
real `event::read()` poll) → `Esc` dismisses → `/`+query echoes → `q` exits cleanly. This is the
only tier that reaches `main.rs` startup → the input poll → teardown, which are structurally
unreachable by the in-process `TestBackend` tier (#76/#82). Bounded + non-flaky: every wait polls
the grid until a specific string appears with a hard per-step timeout; no fixed sleeps.

## What this tells us
Wiring scenarios (#77) ≠ covering the code: the wired core is a healthy **77%**, but
`actor_ref.rs` at **46%** is the one wired module with a large hole, and four modules sit in the
low-70s. Gap-closing priority: **`actor_ref` scenarios first**, then the low-70s error/edge
branches. The `actors` 0% is the separate big hole (#78).
