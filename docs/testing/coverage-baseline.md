# Coverage baseline (card #85)

> **Scope change 2026-07-27 (#213):** the vendored kameo fork left the tree — workspace
> member `.` (`src/`), its root `tests/` BDD suites (`core_*_bdd`, `console_wire_*`),
> `examples/`, `benches/overhead.rs`, and the `actors/` utility crate are **deleted**;
> `apps/purr/` (the vendored TUI) is **deleted** too (#215: console deferred to a post-M1
> greenfield rebuild; git history keeps the reference). Coverage and
> mutation scope is now **bombay + bombay_macros (derive_msg) + mutants-gate** only.
> Sections below that describe the vendored crate's suites are historical record.

> **Audited 2026-07-17 by #168** (scope-vs-shipped sweep over #112–#117, #145–#152).
> Five claims in this document were corrected in place; each correction is a blockquote
> marked `Corrected 2026-07-17 (#168)` in the relevant section. In short:
>
> | Claim | Reality |
> |---|---|
> | the derive's `///` + `compile_fail` doctests cover the tripwire | **none run in the gate** — `bombay-doctest` selects the root fork package only (**#170**) |
> | #148 "0 survivors", and "0 survivors anywhere in the whole-package run" | one of the six named fns had **0 viable mutants**; the whole-package run was **interrupted at 141/205**, and there was **no standing mutants gate** — since fixed: **#171/#165 closed**, `nix build .#mutants` is now a standing, reproducible gate reporting **64 viable / 215 total** (see the #148 section below) |
> | the MIRI Schedules leg explores 64 schedules | all three tests are **current_thread** — nothing to permute; the `multi_thread`+`Barrier` races are excluded from that leg (**#172**) |
> | the bolero lane asserts the mailbox's FIFO/exactly-once | it asserts **flume's**, through ~50 lines of glue; and `__fuzz__/` holds **zero seeds**, so the "deterministic corpus-replay" is bounded-random (**#164**) |
> | "the PR path was confirmed to restore the corpus" | it never ran — no successful `main` run existed to restore from |
>
> **Standing lesson from that audit:** every "0 missed" / "zero survivors" line below is a
> **point-in-time observation recorded in a PR body**, not a standing property — as of the
> #168 audit (2026-07-17), nothing re-ran the mutation sweep. **#171/#165 have since closed
> this**: `nix build .#mutants` (workflow `.github/workflows/mutants.yml`) is now a standing,
> reproducible gate that fails on a survivor, a timeout, an interrupted run (fewer recorded
> outcomes than candidates), or a per-`file::function` viability collapse against a
> committed baseline, and it always reports the measured ratio rather than a bare
> pass/fail — currently **92 viable / 314 total** (measured 2026-07-18 under #186; see the #148 section below and
> `docs/adr/0006-mutation-viable-ratchet.md`). Read every green claim in this file for its
> *sample size* and its *surface*, not its colour — a green lane over the wrong surface is
> indistinguishable from a green lane over the right one, which is exactly why the gate now
> reports the ratio instead of a bare pass/fail.

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

## bombay — the M1 core rebuild (#112+)

The rebuilt spine lives in the `bombay` crate (part-by-part, epic #122), born
under the god-level bar (no #61 quarantine). It is measured by the same
`--workspace` coverage run and adds a **reproducible mutation gate**:

```bash
nix build .#mutants -L   # cargo-mutants over the workspace + crates/macros/src/derive_msg.rs, plus
                          # a mutants-gate verdict tool: fails on any survivor, any timeout,
                          # an interrupted run (fewer recorded outcomes than candidates), or a
                          # per-`file::function` viability collapse against the committed
                          # baseline; always prints the "N viable / M total" ratio
```

Pinned via the flake's `nixpkgs` (never `nix run nixpkgs#…`), mirroring the coverage
package. Quarantined off `nix flake check` (rebuild-per-mutant is too slow for the
per-push gate) — the nightly run is `.github/workflows/mutants.yml`; the gate's design
(the per-function baseline, the viability-collapse check) is recorded in
`docs/adr/0006-mutation-viable-ratchet.md`. Measured 2026-07-18 (#186, after the
single-allocation `ActorRef` restructure, ADR-0010), whole package + `derive_msg.rs`:
**92 viable / 314 total** mutants (92 caught, 0 missed, 0 timeout, 222 unviable) — the
ratchet baseline was regenerated under #186: `WeakActorRef::with_sender` is gone
(subsumed by the loop's upgrade-or-mint; its compensating control moved to
`upgrade_preserves_id_cancel_and_abort`), `ActorRef::abort_handle` joins
`known_zero_viable`, `cancel_token` moves to a caught floor of 1 (it lost `const`, so
whole-body mutants now compile), and `AskFut::poll`'s floor ratchets 1 → 3. All of
`crates/macros/src/derive_msg.rs` stays empirically 0-viable, covered by #170's compile-fail
lane. (The pre-#186 point-in-time reading, 2026-07-17: 64 viable / 215 total, 0 missed.)

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

**DST posture — loom/shuttle deferred to #120 (see the correction below).** loom and
shuttle can only model-check code compiled against *their* primitives; the real channel
this mailbox wraps is opaque to both ([loom](https://docs.rs/loom/latest/loom/) requires
"the code being tested specifically uses the loom replacement types";
[shuttle](https://github.com/awslabs/shuttle) requires replacing std primitives with
its equivalents). The mailbox delegates all synchronization to that channel, so a
loom/shuttle test here would either explore nothing or test a reimplementation (violates
the "test the actual SUT" rule). The 8-thread linearizability test is the mailbox's
concurrency coverage until then.

> **Corrected 2026-07-17 (#168).** This paragraph previously read "the real
> `tokio::sync::mpsc` this mailbox wraps" and "delegates all synchronization to tokio
> (which loom-tests its own channel internally)". **Both were false since PR #134** — the
> mailbox wraps **flume** (`mailbox.rs:200`), not tokio. The *conclusion* survives (flume
> is equally loom-opaque, and ships no loom instrumentation — ADR-0005 re-derives this
> from flume's source), but the deferral rested on a stale premise for three weeks. The
> naming of #116 as a loom/DST target is also stale: **#116 closed without it**
> (see "Mutation … and loom/DST are not re-measured in this card", below), so only #120
> and #88 still carry it. ADR-0005 chose **MIRI** for the ref-model precisely because it
> interprets flume's *real* `std::sync` atomics, which loom cannot reach.

**Control-signal lane (#225, ADR-0021).** The mailbox is now two lanes: the bounded
user lane (`Signal::Message`/`Stop`) plus an UNBOUNDED control lane
(`ControlSignal::Watch`/`Unwatch`/`Supervision`), merged control-first inside
`MailboxReceiver::recv`. Coverage: `tests/control_lane.rs` pins every card
invariant — watch and supervise ops land on a FULL mailbox before the backlog
drains (mailbox-level ordering witness + actor-level end-to-end), intra-lane
FIFO (watch→unwatch = no notice; reversed = notice), the overtake relaxation,
`Stop` still draining prior messages, the #195 teardown obligation answered on
the new lane, the graceful-teardown supervise regression (#245/#248 semantics),
and the load-bearing 10k control flood (lane grows, no panic, FIFO holds, user
lane still drains). `mailbox.rs` unit tests add the closed-lane handback, the
paired weak upgrade, overtake/FIFO/None-only-when-both-closed merges, and the
re-based slot tripwires (`MailboxSender` = two 8 B `flume::Sender`s, measured);
`prop_fifo_roundtrip_single_sender` now interleaves control signals
(MIRI `prop_` prefix contract); `benches/mailbox.rs` gains the
`control_delivery_latency` arm (flat 77–80 ns across depths {0, 64, 1024,
at-cap}, M4 Pro). The `dst_races.rs` suite gains the gated full-mailbox
`watch()` interleaving, and the fuzz targets drive `send_control`.

### `id` (#206) — done
`ActorId` extracted from `mailbox` into its own `id` module as a process-local,
unforgeable **pure-name** handle: `pub(crate) from_raw` mint (spawn path) +
`test-support`-gated `from_raw_for_test`; no public constructor, no readable
`u64`, not serializable (ADR-0015). The two leak vectors (forgery, wire-leak)
are closed **structurally by the compiler**, so evidence is compilation, not
runtime assertions: unforgeability = `pub(crate)` visibility + the public-API
review; non-serializability = `assert_not_impl_any!(ActorId: Serialize,
DeserializeOwned)` compiled in the test build (probe-verified — a temporary
`#[derive(Serialize)]` breaks the build). Runtime tests
(`tests/invariants.rs`): `i24_actor_id_root_export_and_test_ctor` (root re-export
+ `Eq`/`Copy` pure-name semantics) and `i17b_concurrent_mint_distinct_ids`
(32 barrier-released tasks, pairwise-distinct mint — real overlap). The
counter's overflow/wrap policy + loom mint model + mutation boundary sweep are
the **counter-hygiene follow-up card**, deliberately split out (the mint's
`fetch_add` logic is untouched here).

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
`crates/core/src/message.rs` carries the `Msg` marker trait: an actor's single closed
message type, queued **by value**, with `SLOT_BUDGET` (default 256 B / 4 cache lines)
as the per-slot byte bound. Trait covered by 3 `bombay` unit tests — default-256
pin, hand-override, and usability as a generic bound. Mutation testing yields no
signal for this module: `cargo-mutants` mutates function bodies only, so it
generates no mutants for a trait-const-only file — the `SLOT_BUDGET` default is
pinned by the `slot_budget_defaults_to_256` unit test instead. This absence is no
longer silent: the standing gate (`nix build .#mutants`, #171/#165) reports outcomes
**per `file::function`**, so a file contributing zero candidates shows up as such in the
run rather than being folded into an aggregate total.

The `#[derive(Msg)]` proc-macro (`crates/macros/src/derive_msg.rs`) implements the trait and
emits a compile-time slot-size tripwire; it sits outside the `bombay` mutation
gate by design (proc-macros compile out-of-process, same as the "known limitation"
below) and is instead covered by: native runtime tests (`crates/macros/tests/derive_msg.rs`)
for the generated impl on the default budget and the `#[msg(budget = N)]` override;
`parse_budget` unit tests for the attribute grammar (value present, absent,
non-integer, bare key, unknown key, duplicate within one `#[msg(...)]` and across two,
negative, and overflowing-integer rejection); direct `syn::parse_str::<DeriveMsg>`
unit tests for the generic- and union-rejection guards; and six paired `///`
doctests on the derive in `crates/macros/src/lib.rs` — three that must keep compiling
(the initial within-budget example, the boxed-remedy, and the `#[msg(budget = N)]`
escape) plus three `compile_fail` (budget tripwire, generic rejected, union
rejected). No README change —
the rebuilt spine is not behind the umbrella yet (same as #113/#133).

> **⚠️ Corrected 2026-07-17 (#168) — the doctests above do NOT run in the gate.**
> `bombay-doctest` (`flake.nix:215`) is `craneLib.cargoDocTest` with no extra args,
> i.e. `cargo test --doc --locked` — no `-p`, no `--workspace`. The root `Cargo.toml`
> is both `[workspace]` and `[package] name = "bombay"` with **no `default-members`**,
> so cargo selects the root package alone. Measured, not inferred:
> ```
> $ cargo metadata --no-deps | jq .workspace_default_members
> ["path+file:///Users/joel/Code/devrandom/bombay#0.21.0"]   # the vendored fork, only
> ```
> So **none** of the six `///` doctests — and none of the three `compile_fail` probes —
> execute under `nix flake check`. `bombay-nextest` covers the `macros` crate because
> nextest defaults to the whole workspace; `cargo test --doc` does not. The same applies
> to `crates/core/src/reply.rs:32`'s consume-once probe: **all four `compile_fail`
> doctests in the rebuilt spine are dead in the gate**, and no `trybuild` compensates.
>
> Concretely: delete the `const _: () = assert!(…)` tripwire from
> `crates/macros/src/derive_msg.rs:64-68` and the gate stays green — every in-gate
> `#[derive(Msg)]` type is within budget, and `examples/msg_budget.rs`'s tripwire demo
> is commented out (`:26-33`). `size_exactly_at_budget_compiles` guards `<=` vs `<`,
> not assert-vs-no-assert. The unit tests (`parse_budget` grammar, generic/union
> guards) DO run and are unaffected. Tracked as **#170**.

### `reply` (#115) — done
`crates/core/src/reply.rs` carries the typed single-shot reply channel:
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
`crates/core/src/actor/` carries the local actor spine: the `Actor` trait +
lifecycle hooks (`mod.rs`), the run-loop (`kind.rs`), the minimal
`ActorRef`/`WeakActorRef` handle (`actor_ref.rs`), and the spawn entry points
`PreparedActor`/`RunResult` + the `Spawn` ext-trait (`spawn.rs`). The loop is
**finish-current-then-stop, no drain**: `on_start` (caught) → a `select` over
`CancellationToken::run_until_cancelled(recv)` → `on_stop` (caught; a returned
`Err` is logged via `log_on_stop_outcome`, never unwrapped, and the stop
`reason` is preserved). Four `catch_unwind` boundaries turn a panic into an
inspectable `PanicError` instead of tearing down the task — `handle`, `on_stop`,
`on_start`, and `on_panic` — and a hard kill is a uniform `futures::Abortable`
wrap of the message-service future (skips `on_stop` → `RunResult::Killed`); for
supervised actors the lifecycle epilogue still tears down every remaining child
before the task returns. A `handle` that returns `Err` is a controlled crash
routed through `on_panic` exactly like a caught unwind (both →
`ActorStopReason::Panicked`). `default_capacity()` is pinned by a unit test so its
`expect` can never trip.

**19 tests** (18 in `spawn.rs`, 1 in `actor_ref.rs`), organized by the rule-#7
cross-cutting categories:
- **Sequence/protocol** — queued-messages-then-`Signal::Stop` handled in order
  then stop; `Ok(Flow::Stop)` stops after the current handler returns; on-start
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
signals) is the loom/shuttle target noted under `mailbox` above. This surface is,
however, swept by the standing whole-package gate landed under #171/#165 (see the
#148 section below) — its per-function ratio now covers `actor/kind.rs` and
`actor/spawn.rs` alongside every other bombay module. No README
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

### `actor-ref` single-allocation restructure (#186, ADR-0010) — done
`ActorRef` becomes inline `id` + one `Arc<RefShared{sender, cancel, abort}>`;
`WeakActorRef` becomes `id` + `std::sync::Weak`. Liveness stays flume's
`sender_count` (ADR-0003); the loop's per-message self-ref lift becomes
upgrade-or-mint (`kind.rs`), replacing `WeakActorRef::with_sender`. One
semantic change, pinned below: a weak upgrade in the drain window is `None`.
Bench evidence in the three bench headers (registry same-name −59%, flipping
1.79× loss → 1.33× win; tell path improved; watcher roundtrip −13…−17%).

**+8 tests / 1 replaced / 1 new test binary:**
- **Layout** (`actor_ref.rs`) — `handles_are_two_words`: both handles are
  exactly two words (the 1-RMW-clone claim's structural half).
- **Drain-window semantics** (`actor_ref.rs`, red-first) —
  `weak_upgrade_is_none_in_the_drain_window`: external refs gone + queued
  message still pinning ⇒ `upgrade()` is `None` while the message is still
  delivered (ADR-0003 self-pin intact).
- **Registry drain-window, both paths** (`registry.rs`, red-first) —
  `lookup_in_drain_window_reads_absent` + `register_reclaims_name_in_drain_window`:
  a draining actor reads dead on the read path and its name is reclaimable on
  the write path (one liveness rule, both directions).
- **Minted-ref wiring** (`spawn.rs`) — `drain_window_handler_ref_stops_the_actor`
  + `drain_window_handler_ref_kills_the_actor`: a handler ref minted in the
  drain window cancels/aborts the REAL loop (fresh-token/handle stubs fail).
- **Compensating control** (`actor_ref.rs`) — `upgrade_preserves_id_cancel_and_abort`
  replaces `with_sender_preserves_id_cancel_and_abort` (same invariant, new
  reassembly path: upgrade must share the original's token/handle verbatim).
- **Alloc-exactness** (`tests/alloc_clone.rs`, single-test binary, #151 seam) —
  `clone_downgrade_upgrade_allocate_nothing`: handle ops are pure refcount
  traffic (0 gross allocations, exact reclamation).

Mutation: baseline regenerated — **92 viable / 314 total, 0 missed, 0 timeout**
(see the gate paragraph above). MIRI sweep + seeds legs green on PR #188.

### `recipient` type-erased fan-in (#145) — done
`crates/core/src/actor/recipient.rs` carries `Recipient<M>` / `WeakRecipient<M>`:
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

### `actor-ref` context tests (#146) — done
First of the #117 finalization sub-issues (split from PR #144): four behaviors
that were only covered *incidentally* (invariants i12b/i19 exercise them through
`tell`) now each have a canonical, falsifiable test in their natural location
(`actor_ref.rs`). No production change; the test fixture's `ProbeMsg` gains a
`u64` payload so a delivery-failure test can pin the *exact* handed-back message
rather than a ZST. A "reap" is modelled by dropping the `MailboxReceiver` — what
the run-loop does on stop.

**+4 tests** (`actor_ref.rs`):
- **Lifecycle / non-pinning** — `weak_actor_ref_does_not_pin_channel`: after the
  sole strong `ActorRef` drops, neither a `WeakActorRef` nor a clone of it can
  `upgrade`, and the receiver observes the channel disconnected (`recv → None`).
- **Lifecycle / no-resurrection** (`@bug`) — `stale_ref_cannot_resurrect_reaped_actor`:
  a weak ref captured while alive stays `None` after a full reap (senders + receiver
  gone), re-cloning is no back door, and the `id` survives only as a tombstone.
- **Defensive boundary / handback** — `send_to_reaped_actor_returns_actor_not_alive`:
  a `tell` to a reaped actor fails `TellError::ActorNotAlive`, is `is_terminal`,
  and hands the exact undelivered `ProbeMsg(42)` back.
- **Sequence / shared liveness** — `cloned_sender_liveness_via_is_closed`:
  `is_alive`/`is_closed` read identically across cloned senders; a surviving clone
  keeps liveness true, and reaping flips every clone to closed at once. Verified
  falsifiable (stubbing `is_alive` to `true` turns it red).

### `watcher_fanout` bench (#147) — done
A fan-out bench (`benches/watcher_fanout.rs`) so a future slab/registry
optimization (#122) has a baseline to beat. It measures the **production**
send/handle path — real `MailboxSender::try_send_message` and `Actor::handle`,
never a reimplementation — with setup separated from measurement. The
link/death-watch graph (#120) is not built, so the honest fan-out is one
notification cloned to N watcher mailboxes ("a death reason fans out to every
watcher"). Two arms, sweeping width `{16, 128, 1024}`:
- **`watcher_fanout_dispatch`** — pure fan-out enqueue: clone one `Notify` into N
  fresh mailboxes via `try_send_message`, no actors running (`iter_batched_ref`
  keeps fleet construction out of the timed region). Isolates the dispatch loop
  (iterate the registry, enqueue to each, incl. the per-send `self_sender` clone).
- **`watcher_fanout_roundtrip`** — full send + handle: N spawned watchers whose
  real `handle` acks, so the producer observes every watcher processed the event.

Baseline (2026-07-13, current-thread runtime): both arms are **linear per
element** — dispatch ≈ 15 Melem/s (16→~1.0 µs, 1024→~72 µs), roundtrip ≈ 2.3
Melem/s (16→~6.6 µs, 1024→~448 µs). The flat per-element slope is exactly what a
slab/registry would need to flatten. No production change; no README change.

### `actor-ref` mutation sweep (#148) — done
`cargo-mutants` over the #117 ref-model surface —
`ActorRef::tell`/`is_alive`, `MailboxSender::send_message`/`is_closed`,
`WeakActorRef::with_sender`, and `impl Drop for MailboxReceiver`: **0 missed, 0
timeout** (21 mutants over that surface: 13 caught, 8 unviable). No production
change.

> **Corrected 2026-07-17 (#168) — read the ratio, not the colour.**
> Of the six functions named above, **`WeakActorRef::with_sender` contributed 0 viable
> mutants**: all 4 of its generated mutants are `Unviable`, because it is a pure
> field-copy returning `ActorRef<A>`, which has no `Default`, so whole-body replacement
> is cargo-mutants' only strategy and none of it compiles. "Zero survivors" over that
> function is **vacuous** — a wrong-`id` or stale-`cancel` copy would be invisible, and
> `with_sender` is the upgrade path used by `WeakActorRef::upgrade` and the self-ref
> construction in `kind.rs:40`. The "13 caught, 8 unviable" above is accurate but
> averages this away. This is #165's pattern *inside* #148's own named scope.
>
> **PR #157's separate claim of "0 survivors anywhere in the whole-package run" is not
> supported by its artifact:** `mutants.out/mutants.json` enumerates **205** candidates;
> `outcomes.json` records **141** (zero `spawn.rs` outcomes, 6 of 42 for `recipient.rs`);
> `debug.log` ends `err=interrupted phase=Test`. The run was killed at 141/205, and could
> not have exited 0 regardless — `timeout.txt` lists 5 mailbox timeouts and cargo-mutants
> exits 3 on timeout (those *are* properly deferred to #133). The scoped 21-mutant result
> above is real; the whole-package narration was not. `mutants.out/` is gitignored, so
> this shows no evidence of a complete run exists in the repo/PR/card — not that none
> ever happened. Tracked as **#171** (which also covers: there is **no standing mutants
> gate** — `flake.nix:320-322` says "On-demand, NOT a gating check", and
> `rg -i mutants .github/` is empty, so every "0 missed" in this document is a
> point-in-time PR-body observation, not a property).

> **Closed 2026-07-17 (#171/#165).** Both problems in the two blockquotes above are now
> fixed by a standing, reproducible gate: `nix build .#mutants` runs the sweep plus a
> `mutants-gate` verdict tool that fails on any survivor, any timeout, an interrupted run
> (fewer recorded outcomes than candidates — exactly the 141/205 failure mode above), or a
> per-`file::function` viability collapse against a committed `mutants-baseline.json`, and
> it always prints the "N viable / M total" ratio instead of a bare pass/fail. The nightly
> workflow is `.github/workflows/mutants.yml`; the design rationale (why a per-function
> viable-count ratchet rather than a raw survivor count) is `docs/adr/0006-mutation-viable-ratchet.md`.
> Because the ratio is now reported **per function**, a 0-viable function like
> `WeakActorRef::with_sender` is visible in the committed baseline (`known_zero_viable`)
> rather than averaged into an aggregate "13 caught, 8 unviable" that hides it — and
> `with_sender` now additionally carries a hand-written compensating test in
> `crates/core/src/actor/actor_ref.rs`, so a wrong-`id`/stale-`cancel` copy that its own 0
> viable mutants cannot catch is caught there instead. The current whole-package +
> `derive_msg.rs` measurement (2026-07-17, this branch, complete run — not interrupted) is
> **64 viable / 215 total** mutants (64 caught, 0 missed, 0 timeout, 151 unviable; 215 vs.
> the 205 candidates PR #157 attempted, because the sweep now also covers
> `crates/macros/src/derive_msg.rs`, which is empirically 0-viable and is documented as such rather
> than silently absent). The baseline floors 47 functions with viable ≥ 1 and documents 48
> functions — including all of `derive_msg.rs` — as 0-viable by design.

The interesting finding was **not** a survivor but three mutation *timeouts*: a
`… -> Ok(())` stub of a send/tell path makes a message silently vanish, so any
round-trip test that then awaited delivery hung until the harness's 20 s cap —
which `cargo-mutants` reports as a timeout (exit 3), failing the gate exactly
like a survivor. `cargo test` runs the whole binary in one process, so a single
hanging test times out the run regardless of which mutant a *different* test would
have caught. The fix is test-only: **bound the hang-prone awaits** so the mutant
is *caught* by a fast assertion instead of a timeout, matching the
`timeout(TERMINATE, run)` discipline `invariants.rs`/`dst_races.rs` already use.
Nineteen hang-prone awaits across five test modules were bounded — the
intermediate handler-gate `entered_rx`/`done_rx` oneshots and the panic-path
`run()`/`handle` awaits in `spawn.rs` (which stop via a panic, not a stop-signal,
so they were never wrapped), the erased-tell round trips in `recipient.rs`, the
`send_message` round trips in `msg_mailbox_compose.rs`, and the on-start/on-stop
gates in `dst_races.rs`.
The sweep also cut the surface's mutation wall-clock ~30 % (fast catches replace
20 s hangs). No README change (same target-API posture as #145–#147).

### `pipe_to_self` + `pipe_ask` (#226) — done
Detached weak-reference pipe primitive (`crates/core/src/actor/pipe.rs`) plus the
flat-error `pipe_ask` sugar. `pipe_to_self(future, mapper)` captures only a
`WeakActorRef` while the future is in-flight, upgrades it at resolution, maps the
`Result<T, PanicError>` to `A::Msg`, and delivers through the ordinary mailbox; a
panic in the piped future is surfaced as `PanicReason::PipedFuture` (the
`is_lifecycle_hook` predicate was flipped to a positive match so future variants
cannot silently misclassify). `pipe_ask(target, make_msg, mapper)` delegates to the
same primitive and flattens `AskError<M, E>` + `PanicError` into one
`PipeAskError<E>` union, so callers match a single closed error menu.

New public API: `ActorRef::pipe_to_self`, `ActorRef::pipe_ask`, `PipeAskError<E>`,
and `PanicReason::PipedFuture`; `spawn_pipe` stays `pub(crate)`. Two trace
breadcrumbs (`pipe_mapper_panicked`, `pipe_result_dropped`) cover the off-actor
panic and dead-target drop paths.

**11 unit tests** (`crates/core/src/actor/pipe.rs`) + **2 integration tests**
(`crates/core/tests/tracing_capture.rs`), organized by the card's invariants:
- **Round-trip / typed delivery** — `piped_future_result_arrives_as_mapped_message`
  and `pipe_ask_delivers_flat_ok` prove the happy path reaches the actor's own
  menu and `pipe_ask` collapses the nested results to `Result<R, PipeAskError<E>>`.
- **Panic surfacing** — `piped_panic_reaches_mapper_typed_and_actor_survives`
  asserts the `PanicReason::PipedFuture` attribution, the payload string, and
  that the actor stays alive.
- **Non-pinning** — `in_flight_pipe_does_not_pin_refcount_stop` and
  `in_flight_pipe_ask_does_not_pin_refcount_stop` drop the sole strong
  `ActorRef` while futures are still pending and assert the actor
  ref-count-stops within the bound (a strong capture in the pipe task would
  keep it alive forever and trip the bound).
- **Dead / killed target** — `actor_dead_before_resolution_drops_result_cleanly`
  stops the actor before the future resolves and asserts the mapper never ran
  and the detached task exited (drop-guard oneshot);
  `pipe_resolving_into_killed_mailbox_is_swallowed` resolves against a
  kill-closed mailbox with a strong ref still live and asserts the task exits
  without panicking.
- **Liveness overlap** — `actor_keeps_processing_while_piped_ask_is_pending`
  holds an inner ask open on a gated responder and proves the actor still
  answers other messages meanwhile (`no_timeout` on the inner ask keeps the
  deadline out of the liveness assertion).
- **Flatten lossless** — `flatten_maps_every_variant_distinctly` covers every
  `PipeAskError::flatten` arm as a pure function, no sleeps — the
  `Timeout`/`Interrupted` arms live here by design instead of as timing-flaky
  end-to-end tests.
- **E2E error arms** — `pipe_ask_dead_target_flattens_to_target_dead` and
  `pipe_ask_handler_error_flattens_unerased` cover the two ask error paths
  reachable without clock control (dead target at delivery; handler domain
  error, un-erased).
- **Trace breadcrumbs** — `pipe_mapper_panic_emits_one_error_event` and
  `pipe_result_dropped_after_stop_emits_one_debug_event` assert the two
  drop-path events fire exactly once.

**Mutation:** see `mutants-baseline.json` — floors set from the #226 scoped
sweep over `crates/core/src/actor/pipe.rs` (4 mutants generated).

### `request` ask/tell builders (#118) — done
The send surface: `TellRequest` (`.await` / `try_send` / `.timeout(d)`),
`AskRequest` (typed port in the message, default-5s/override/`no_timeout`),
`Ask<M, R, E>` carrier + `ReplyRecipient` (the #145 deferral), `SendTimeout(M)`
(the #113 deferral, guaranteed-handback semantics — ADR-0008; builder shape —
ADR-0007). Tests, all in-crate `request::tests` unless noted:

- **Boundary/defensive** — `try_send_full_returns_mailbox_full_with_msg`,
  `tell_timeout_on_saturated_mailbox_returns_send_timeout_with_msg`,
  `tell_timeout_zero_still_attempts_once` (zero-deadline still attempts once),
  `ask_times_out_when_target_saturated` (card-named; resolves as
  `Deliver(SendTimeout(M))`, retryable, message back),
  `ask_times_out_when_handler_never_replies` (`Timeout`, not retryable).
- **Sequence/lifecycle** — `tell_timeout_delivers_when_capacity_frees_before_deadline`,
  `ask_reply_reaches_caller_end_to_end`, `ask_handler_error_reaches_caller_typed`,
  `@bug ask_actor_dies_mid_handle_maps_interrupted` (card-named, ref #122-#3),
  `ask_default_timeout_is_five_seconds` (paused-time pin of the gen_server
  precedent), `ask_no_timeout_outlives_the_default_deadline`,
  `delegated_reply_arrives_from_a_later_handle_call` and
  `forwarded_reply_comes_from_the_second_actor` (the #115 marker types resolved
  as *structural* — no `DelegatedReply`/`ForwardedReply`/`Context` carried over),
  `actor_not_alive_unifies_terminal` (`tests/invariants.rs`; never-run +
  startup-failed legs landed, stopped legs pre-existing, passivated leg
  unassertable — no passivation exists yet).
- **Erasure** — `reply_recipient_ask_round_trips_typed_reply`,
  `reply_recipient_ask_to_reaped_actor_hands_msg_back`,
  `reply_recipient_ask_times_out_saturated_with_msg_back`.
- **Allocation (measured, not templated — #122-#11)** —
  `tests/alloc_request.rs`, a one-test binary (the `alloc_exact.rs` isolation
  rationale) over a new **gross**-allocation counter on `CountingAlloc` (live
  counters cancel transient alloc-then-free): `tell().await` = **0** heap
  allocations, the full ask round trip = **exactly 1** (the oneshot; the inline
  `pin_project` timer allocates nothing), both reclaim exactly. The harness's
  own first draft was caught by the counter (a fresh flume channel grows its
  queue on first push — warm-up now shares the channel).
- **DST/linearizability** — `cyclic_topology_never_deadlocks`
  (`tests/dst_races.rs`, card-named, seeded ×4 LCG storms, paused time): a
  capacity-1 ring under the #122-#4 discipline (handlers `try_send`+shed, never
  block) where every deadline-bearing ask resolves and the ring stays live.

Paused-time tests use tokio `start-paused` (new `test-util` dev-dep feature).
Timed sends are a bounded `try_send` retry loop, **not** a cancelled park on
flume's `SendFut` — flume cancellation cannot report delivered-vs-not
(`reset_hook` discards the hook without checking), so a cancelled park could
hand back an already-delivered message and double-deliver on retry; ADR-0008
records the analysis. No README change (same pre-public posture as #112–#117;
the vendored-kameo API the README documents is unchanged).

### `registry` local name→actor lookup (#119) — done
`crates/core/src/registry.rs`: concrete `Registry` over
`papaya::HashMap<Cow<'static, str>, Box<dyn ErasedEntry>>` (erased **weak**
handles — a registration never pins the actor), register-once decided
atomically inside `papaya::HashMap::compute`, dead entries read as absent on
every path (the one liveness rule: mailbox channel open). Trait-seam note on
the card deliberately not implemented — ADR-0009 records the evidence
(papaya 0.2.4 is green under `-Zmiri-strict-provenance` on the pinned
nightly, so the *production* impl is what every lane sees). Errors:
`NameTaken` / `WrongActorType` (bare per-op structs in `error.rs`).

Tests, all in-crate `registry::tests` (17):

- **Sequence** — `register_then_lookup_resolves_the_same_actor` (round-trip:
  the looked-up ref delivers onto the ORIGINAL receiver — same channel, not
  just same id), `unregister_frees_the_name_for_reregistration` (remove →
  absent → double-remove no-op → re-register).
- **Defensive boundary** — `register_on_live_name_is_rejected_and_keeps_incumbent`
  (dedup-on-collision; the loser must not clobber),
  `lookup_with_wrong_actor_type_errors`, `lookup_of_unknown_name_is_absent`,
  `dead_incumbent_of_another_type_reads_absent_not_type_error` (dead entries
  cannot claim a type), `registration_does_not_pin_the_actor` (THE weak-handle
  invariant: last strong drop closes the channel despite the registry entry).
- **Lifecycle** — `lookup_of_reaped_actor_is_absent`,
  `register_reclaims_a_dead_incumbents_name`,
  `name_reclaimable_when_only_receiver_is_gone` +
  `lookup_sees_receiver_reaped_actor_as_absent` (liveness = channel open, not
  "strong refs linger": these two kill the `is_some_and → is_some` mutant and
  pin read-path/write-path to the SAME rule).
- **Linearizability (real OS-thread overlap: `std::thread::scope` + `Barrier`)** —
  `concurrent_register_single_winner_on_one_name` (N racers, exactly one `Ok`,
  losers see `NameTaken`, lookup resolves the winner),
  `concurrent_reclaim_of_dead_name_single_winner` (the stale-replace decision
  is atomic — no interleaving double-replaces),
  `concurrent_lookup_during_register_churn_is_consistent` (readers racing a
  register/unregister loop only ever observe absent or THE registrant).
- **Guards** — `registry_debug_names_struct_and_entry_count`,
  `default_is_an_empty_working_registry`, `registry_is_send_and_sync`
  (compile-time property).

**MIRI**: all 17 run in the sweep leg (measured 8.8 s interpreted, strict
provenance); the three scoped-thread races are added to the many-seeds leg's
named list — std threads are OS threads under MIRI, so the tasks>workers
probe caveat (a tokio work-stealing artifact) cannot apply; measured
1.6 s + 1.6 s + 2.2 s per seed. The card's "DST over concurrent registration
races" bullet is discharged by exactly that leg: `-Zmiri-many-seeds` IS the
deterministic schedule exploration for a sync lock-free surface.

**Mutation** (scoped run, `--timeout 60`, 2026-07-18): **16 mutants — 11
caught, 5 unviable, 0 missed, 0 timeouts**; floors merged into
`mutants-baseline.json` from the gate's own `emit-baseline`
(`register` 3, `lookup` 3, `is_alive` 2, `unregister` 2, `fmt` 1; `as_any`
`known_zero_viable`, compensated by the typed-downcast tests;
`new`/`default` produced no candidates). The PR's whole-crate sweep then
caught what the scoped run could not: the first cut of the round-trip test
awaited `tell`-then-`recv` **unbounded**, so the *mailbox* mutant
`Capacity::get -> 0` (a rendezvous channel) deadlocked it into a 60 s
TIMEOUT — the exact #179 failure mode, reintroduced in a new module. All
awaits in the registry tests are now 5 s-bounded; both `Capacity::get`
mutants re-verified as fast catches.

**Bench** (`benches/registry_vs_kameo.rs`, measured 2026-07-18, M-series),
five groups, all recorded honestly. Same-name groups go to kameo (lookup_hit
24.9 ns vs 17.9 ns; 4-reader one-name 419 µs vs 234 µs; 3r+1w churn 470 µs
vs 328 µs; register/unregister 94.5 ns vs 45.9 ns): with one hot actor the
cost is the ref-model — weak-`upgrade` CAS + two Arc RMWs per hit on three
shared cachelines — not the map, and that shape belongs to ADR-0003
(follow-up card filed: single-allocation `ActorRef`, 1-RMW clone). The
**distinct-names group — the design's regime (many actors) — goes to bombay
1.60×** (131.6 µs vs 210.5 µs; 30.4 M/s vs 19.0 M/s): bombay jumps 3.2×
once readers stop sharing one actor while kameo barely moves, because its
ceiling is the one global mutex whatever the name — flat by construction as
readers/names grow. Both designs sit 2–3 orders below message-rate costs;
the structural drivers (no guard-across-`.await` deadlock class, atomic
`compute` claim, weak no-pinning entries) are what the same-name nanoseconds
buy. `scc::HashIndex` recorded as runner-up if the map itself ever measures
as the bottleneck. No README
change (same pre-public posture as #112–#118; the vendored-kameo
`ActorRegistry` bullet the README documents is unchanged).

### `timer` surface (#223) — done
`crates/core/src/actor/timer.rs` carries the sanctioned non-pinning timer
primitives: `ActorRef::send_after` / `send_interval`, `Recipient::send_after` /
`send_interval`, and `TimerHandle::cancel`. The timer task holds only a weak
handle to the target and upgrades it at fire, so an armed timer never pins
ref-count stop (ADR-0003 deviation from kameo v0.22.2). Fired messages are
ordinary `A::Msg` deliveries through the bounded mailbox, with the same
backpressure semantics as any sender. Cancellation is sleep-phase-atomic via
`CancellationToken`; `JoinHandle::abort` is never used, avoiding flume's
indeterminate-cancel window (ADR-0008). Intervals arm the next tick only after
the prior tick is enqueued (arm-after-enqueue), self-reap on target death, and
contain a panicking `make_msg` to the timer task.

**11 unit tests** (all in `crates/core/src/actor/timer.rs`):
- **Round-trip / value** — `send_after_fires_exact_value_after_delay` (paused
  clock, exact menu value, no early delivery).
- **Cancel semantics** — `cancel_before_fire_never_delivers_and_reaps_task`
  (cancel reaps the task, then advancing far past the deadline delivers
  nothing); `cancel_after_fire_is_noop` (late cancel is harmless).
- **Non-pinning / lifecycle** — `armed_timer_does_not_pin_refcount_stop`;
  `dead_target_at_fire_drops_cleanly` (no panic, no leak, task exits via
  `ExitGuard`);
- **Interval cadence / backpressure** — `interval_ticks_arrive_with_fresh_messages`
  (fresh `FnMut` per tick, ordered); `interval_does_not_overlap_or_burst_when_mailbox_full`
  (gated sink with capacity 1, arm-after-enqueue bounds queued ticks to the
  structural maximum).
- **Interval containment / reaping** — `interval_self_reaps_on_target_death`;
  `interval_factory_panic_kills_timer_not_actor` (factory panic traced, actor
  untouched).
- **Erasure** — `recipient_send_after_fires_through_erasure`;
  `recipient_interval_and_cancel` (`Recipient<u32>` round-trip via `From`).

**Mutation:** see `mutants-baseline.json` — new `floors` / `known_zero_viable`
entries for the timer surface functions and the three trace events.

**Bench** (`benches/timers.rs`, measured 2026-07-28, M4 Pro): `arm_send_after_10k`
≈ **4.90 ms** / 10 000 timers (≈ 490 ns / timer, 2.04 Melem/s); baseline
`arm_delay_queue_insert_10k` ≈ **0.31 ms** / 10 000 inserts (≈ 31 ns / insert,
31.9 Melem/s). The per-task cost is the price of not introducing a shared
owner/service; a future named-key timer layer can justify a `DelayQueue`-style
wheel if scheduling becomes hot. Recorded in ADR-0018.

No further README change beyond the timer bullet added to the public-API-at-a-glance
section; the surface is now documented there.

### `stash` (#224) — done
Bounded deferral via framework-owned composition: `Stash<M>` (two-queue buffer —
`held` behind `stash()`, `ready` behind `unstash_all()`, snapshot semantics),
`StashFull<M>` typed overflow handback (`TellError` precedent — the message
comes back, never dropped, never panicked), the opt-in `StashActor` trait
(`Actor`'s hooks + `&mut Stash` handle param + required `stash_capacity(&args)`),
and the `Stashed<S>` wrapper whose `Actor::handle` runs the user handler then
drains `ready` in the same step — replay ahead of the mailbox backlog, in
stash-arrival order, with **zero** `kind.rs`/loop changes (ADR-0022).
`Stash::bounded`/`pop_ready` are `pub(crate)`: a stash cannot exist or drain
outside the wrapper (forget-trap unrepresentable). Tests: 4 unit
(`src/stash.rs` — cap bounds held+ready, exact-message handback, snapshot
semantics, arrival-order replay across rounds) + 7 integration
(`tests/stash.rs` — replay-before-backlog `[T, A, B, D]` order, no stale
replay after a drained batch, mid-batch `stop` abandons the rest, non-pinning
`Collected` with a non-empty stash, in-band `Signal::Stop` drop, `kill()`
drop, supervised restart gets a structurally fresh stash). Walking skeleton:
`Stashed<Intake>` job-queue intake gate (`examples/job_queue`,
`tests/app_job_queue.rs::intake_defers_submissions_during_maintenance`).
**Mutation:** see `mutants-baseline.json` — entries for `Stash::*`,
`StashFull::*`, and the `Stashed`/`StashActor` impls.

### `watch` — links + death-watch (#195, slice 1 of #120) — done
The death-watch half of #120: an actor learns, on **every** exit path (normal /
panic / kill), when a watched peer stops. Two verbs on one mechanism — `watch`
(monitor, notify-only) and `link` (bidirectional, propagating) — grounded in the
Erlang/OTP + Akka convergent design (separate unbounded death channel; see the
design doc `docs/superpowers/specs/2026-07-23-120-links-death-watch-design.md`).

Shipped surface: `watch::{LinkDied, Watchers}` (+ crate-internal `WatchReg`,
`LinkSender`/`LinkReceiver`); `Signal::{Watch, Unwatch}` replacing the retired
`Signal::LinkDied`; `Watch: Actor` supertrait with the default OTP `on_link_died`;
`SpawnLinked::spawn_linked`; **async** `ActorRef::{watch, link, unwatch}`;
`ActorStopReason::LinkDied` + `PanicReason::OnLinkDied` un-deferred;
`error::ActorNotLinked`; `link_tx: Option<LinkSender>` in `RefShared` (clone stays
1 Arc RMW, #186/ADR-0010 intact). Erasure is **absent** — `LinkDied` is
monomorphic, so watcher lists are a homogeneous `SmallVec<[_; 1]>` (this revises
the #122-#10 "erasure lives here" note; the erasure relocates to slice-2 restart).

Tests (bombay lib, all TDD — written failing first):
- **unit** — `drop_of_watchers_notifies_every_edge_with_its_linked_flag`,
  `drop_without_set_reason_reports_killed` (`watch.rs`); `signal_watch_and_unwatch_are_carried`
  (`mailbox.rs`); `on_link_died_is_a_lifecycle_hook`, `link_died_is_abnormal`
  (`error.rs`); `default_hook_breaks_on_linked_abnormal_and_continues_otherwise`
  (`actor/mod.rs`).
- **delivery on every exit path** — `watch_notified_on_normal_stop`,
  `watch_notified_on_panic`, `watch_notified_on_kill`,
  `watch_in_flight_at_kill_still_notified`.
- **registration semantics** — `unwatch_queued_before_stop_suppresses_notice`
  (FIFO Unwatch honored at teardown), `stale_watcher_edge_self_prunes`,
  `many_watchers_all_notified` (8-way `Barrier` linearizability),
  `watch_does_not_pin_target` (ADR-0003: watching holds no strong ref),
  `plain_spawned_watch_actor_watch_errs`,
  `watch_full_but_alive_target_lands_immediately_no_spurious_death` (the
  registration regression guard — a busy-but-alive target takes the watch on
  the control lane (ADR-0021) and must never be mistaken for dead).
- **handler-context watch (#260)** — `drain_window_handler_watch_succeeds`
  (the #260 regression: a `watch` from INSIDE a handler in the drain window
  succeeds exactly as in steady state, because the drain-window-minted handler
  ref now carries the loop's own cold copy of `link_tx` through `LoopHandles` —
  FAILED before the fix with `Err(ActorNotLinked)`),
  `drain_window_watch_delivers_death_notice` (the `Ok` is not vacuous: the
  registration carries the SAME link channel the loop drains, proved end-to-end
  by the target's `Collected` notice reaching `on_link_died`), and
  `steady_state_handler_watch_succeeds` (the steady-state control — the
  handler's ref comes from the shared upgrade; handler-context steady-state
  watch was otherwise untested). No new function shapes, so no new
  `mutants-baseline.json` entries: the three touched functions
  (`handle_mailbox_step`, `start_actor`, `run_linked_message_loop`) were
  already keyed `known_zero_viable`, and their post-change mutants are the
  same `Default`-requiring whole-body replacements (unviable by construction).
- **drain-window watch/link equivalence (#266)** — `tests/drain_equivalence.rs`
  pins the ADR-0010 invariant that handler-context `watch`/`link` (plus the
  rest of the spec'd-equivalent verb set: `is_alive`, `tell`, `ask`,
  `unwatch`) behaves identically on the steady-state shared upgrade and on a
  drain-window mint: a strict trace oracle (ONE parameterized
  `run_script(mode, death)`, full `Vec<TraceEvent>` equality against a
  `vec![..]` literal, role-keyed never raw-`ActorId`-keyed) for graceful and
  killed target deaths, plus adversarial legs — both link edges installed
  (watcher-side kill, peer-side watcher `Collected`), a linked peer's panic
  propagating as `LinkDied { Panicked }` through the default hook, per-message
  mints registering independent duplicate edges (two watches, two notices),
  the designed-lost late notice after the loop's break decision (GREEN pin,
  Erlang `DOWN`-to-dead parity, decision 1), and the spec'd
  `pipe_to_self`/`send_after` drain-window result-drop divergences (decision
  2; steady delivery already pinned by `pipe.rs`/`timer.rs` unit tests).
  `tests/dst_races.rs` gains the seeded
  `drain_window_watch_races_target_death_and_close_equivalence` leg (4-seed
  LCG knobs; sorted notice-multiset canonicalization, exact counts per
  injection point), and `tests/app_job_queue.rs` the drain-window auditor
  walking skeleton against the real job-queue app. `Watch::on_link_died`
  documents the designed-lost rule. Test-only card — no production-code
  change beyond that doc note, so no `mutants-baseline.json` movement.
- **reaction / propagation** — `linked_actor_receives_death_of_watched_target`,
  `link_propagates_on_abnormal`, `link_does_not_propagate_on_normal`,
  `trap_exit_via_override_keeps_running`, `dead_target_watch_immediate_linkdied`.
  The three survival/propagation tests use a `Recorder` actor with an invocation
  counter **plus a post-death `Ping` round-trip** as the liveness proof (a bare
  `is_alive()` races the lazy mailbox close); each was mutation-verified during
  review (mutate the loop to always-`Break` / drop the dead-target branch → the
  named test fails).

Fuzz (in-gate `bombay-fuzz-replay`): the single #164 `actor_loop` target gained
`Op::Watch { linked }` / `Op::Unwatch`, exercising the registration signals under
random loop-op interleavings via the `#[cfg(feature = "test-support")]`
`test_support::watch_signal` seam (constructs `Signal::Watch` without exposing
`WatchReg`). Death-delivery is not asserted in the generic harness (non-deterministic
across kill/startup-panic/stop-ahead-of-Watch orderings) — deterministic delivery
stays covered by the `#[tokio::test]` cases above.

Mutation (`nix build .#mutants`, viable-ratchet vs `mutants-baseline.json`,
ADR-0006): a scoped sweep over the six #195-touched files ran **167 mutants → 34
caught / 0 missed / 133 unviable** (2026-07-23). Zero survivors; both initially-missed
mutants (`replace unwatch with ()`, `replace || with && in link`) were killed by
dedicated mutation-verified tests (`unwatch_removes_edge_so_death_delivers_no_notice`,
`link_to_plain_peer_errs_without_half_link` — the latter uses a raw-fence + graceful-stop
barrier so the kill is deterministic under the full parallel suite, not just in
isolation). Every new function carries a baseline entry (10 `floors` + 12
`known_zero_viable`); a missing entry reads as `Unaccounted` and fails the gate. MIRI: the new synchronous paths (`Watchers` guard,
`register_on`, the `Signal::Watch`/`Unwatch` arms) run under the scheduled nightly
`.#miri` lane (stable stays the per-push gate, #60/#150).

No README change (same pre-public posture as #112–#119: the README documents the
vendored-kameo API until the M1 rebuild swap; bombay's `Watch`/`spawn_linked`/
`watch`/`link` are not surfaced there yet).

### `fuzz` — bolero workspace (#149) — done
Isolated non-member `fuzz/` workspace (crate `bombay-fuzz`, own `Cargo.lock`) —
the reusable verification backbone (#150/#151/#152 build on it). `bolero::check!`
targets run on **stable** via the `bombay-fuzz-replay` flake check
(`cd fuzz && cargo test`, DefaultEngine = deterministic corpus-replay +
bounded-random); nightly sanitized fuzzing is the same targets under #152's
`fuzz.yml`, quarantined to CI env (no `fuzz/rust-toolchain.toml`).

Targets: `smoke` (wiring proof) and `mailbox_state_machine` — a model-based
differential over the **sync** mailbox surface (`try_send`/`drain`/clone/drop)
against a `VecDeque` oracle, asserting FIFO + exactly-once + capacity
backpressure. Sync-only so #151's MIRI job runs the same surface. Exact-memory /
leak assertion is deferred to #151's counting allocator, which plugs into this
same target.

> **⚠️ Corrected 2026-07-17 (#168) — this lane asserts flume's guarantees, and the
> in-gate replay replays nothing.** Two distinct problems:
>
> 1. **Wrong surface (tracked as #164).** `crates/core/src/mailbox.rs:200` is
>    `flume::bounded`, and `try_send`/`drain` are thin glue over it, so the FIFO +
>    exactly-once assertion above discriminates **flume's** ordering through ~50 lines of
>    bombay code against a `VecDeque` oracle. #152's 3,539,931 green executions largely
>    re-verified a mature crate. #149 also scoped `send_message`, `recv`, and *self-pin
>    drain-or-abandon per stop mode* — none shipped (`Signal::Stop` is an `unreachable!`
>    arm; the self-pin cycle is built via `self_sender: tx.clone()` and never asserted on).
> 2. **No corpus seeds (tracked as #164's added bullets).** `git ls-files
>    fuzz/tests/__fuzz__` returns exactly `.gitkeep` — **zero seeds**. So
>    `bombay-fuzz-replay` is **bounded-random only**, despite `flake.nix:192` calling it
>    "Deterministic corpus-replay". Corpus continuity lives in a 90-day GitHub Actions
>    artifact, not git. The flake's source filter (`flake.nix:98`) correctly keeps
>    `__fuzz__` files and currently has nothing to keep — a vacuously-correct pipeline
>    over an empty input reads exactly like a working one.
>
> The target also **cannot reach the closed state**: `Op::DropTx` pops only tail clones
> while sends use `senders.first()`, and `rx` is never dropped — so `TrySendError::Closed`
> and `is_closed() == true` are unreachable, and `Op::IsClosed` asserts only the
> trivially-true direction (the target concedes this at `fuzz/tests/mailbox.rs:59-60`).

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
runtime scenarios"). Likewise `demo.rs` is a non-SUT demo entrypoint. `apps/purr/src/main.rs` and
the literal `event::read()` poll are now exercised by the **Tier-2 PTY smoke test** (#83, below);
note that `llvm-cov` still reports them near-0% because the test drives a *separate* compiled
process, whose instrumentation the test-binary coverage run does not aggregate — the guarantee is
behavioural (the binary boots, polls input, and quits cleanly), not a line-count bump.

### Tier-2 (PTY / "Selenium-for-terminals") — #83
`apps/purr/tests/pty_smoke.rs` drives the real `purr --demo` binary through a
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

## deep-fuzz lane (#152) — nightly sanitized half of the #149 bolero harness
`.github/workflows/fuzz.yml`, scheduled nightly 03:00 UTC + PR + dispatch (a `duration`
input in seconds); **never** the flake gate. The write-once/run-both-ways payoff of #149:
the *same* `check!` targets the in-gate `bombay-fuzz-replay` check replays on stable are
recompiled here under a **pinned** nightly with ASan + sancov, becoming coverage-guided
fuzzers. Pin (`FUZZ_TOOLCHAIN`) equals miri.yml's `MIRI_TOOLCHAIN` and flake.nix's
`miriToolchain` — one nightly date repo-wide, so a bump is one review. It lives in the
workflow `env`, never a `fuzz/rust-toolchain.toml`, or a rustup user's plain
`cd fuzz && cargo test` replay would pull nightly and break #149's contract.

- **Engine** — libFuzzer + `--sanitizer address`, one leg. cesr's second AFL++/CMPLOG
  leg and `-use_value_profile=1` are deliberately **not** carried over: both buy their
  keep on CESR's exact-byte gates (code tables, magic/version prefixes), which a
  `TypeGenerator`-driven `(u16, Vec<Op>)` mailbox target does not have.
- **Matrix** — `mailbox_state_machine` only. `smoke` is excluded: it is a total function
  that cannot fail by construction, so fuzzing it would burn a nightly slot for no signal.
- **Corpus** — compounds night over night via the `corpus-<target>` artifact (90-day
  retention), restored by explicit run-id lookup, `cargo bolero reduce`-minimized
  (libFuzzer `-merge=1`) before re-upload. PR runs read the corpus but never write it.
- **Durations** — dispatch input > PR smoke (60 s) > nightly (120 s).

First run measured 2026-07-16 (PR #163, 60 s smoke depth, cold corpus): **3,539,931 runs
in 61 s** (58,031 exec/s), coverage climbing 120 → 244 edges / 121 → 1,026 features, corpus
1 → 206 inputs (7,224 b), no crash. Job wall-clock 142 s incl. the nightly toolchain +
`cargo install cargo-bolero` + sancov build; `fuzz-gate` 3 s. The PR path **skips**
Minimize/Upload, per the read-never-write rule above.

> **Corrected 2026-07-17 (#168).** This paragraph previously said the PR path "was
> confirmed to restore the corpus". **It was not.** The restore step reported `success`,
> but with zero successful `main` runs in existence, `gh run list --branch main --status
> success` returned empty, so `run_id` was empty and the download never ran. What was
> confirmed is that the step *does not error on a cold start* — which is what
> `continue-on-error: true` at `fuzz.yml:79` is there for. The skip of Minimize/Upload
> is real and correctly observed.
>
> **The corpus persistence loop has never executed.** `gh run list --workflow fuzz.yml`
> shows **4 `pull_request` runs and 0 `schedule` runs** — the 03:00 UTC cron has not yet
> come round. restore→grow→minimize→upload is *reviewed* wiring, not *exercised* wiring.
> The 3.5M executions above are real but are **60 s PR-smoke depth on a cold corpus**;
> no nightly-depth number exists yet. See also the wrong-surface correction under
> "`fuzz` — bolero workspace (#149)" above: these executions fuzzed flume, not bombay.

A crash is only half-caught here: it must be minimized and committed as a seed under
`fuzz/tests/__fuzz__/<target>/corpus/`, so the in-gate #149 replay reproduces it forever
on stable. That is what stops a nightly-only find from regressing once the lane goes quiet.

> **Sharpened 2026-07-17 (#168) — "half-caught" understates it; there is no mechanism.**
> The lane genuinely *detects* and *preserves* a crash (`fuzz.yml:130-137` `if: failure()`;
> the artifact path is correct against `cargo-bolero-0.13.4/src/test_target.rs:76-80` +
> `libfuzzer.rs:50-51`). But nothing minimizes it, and nothing commits it:
> - The **Minimize step is `if: success()`** (`fuzz.yml:111`) — it does not run on the one
>   event it would be needed for.
> - **`cargo bolero reduce` is not crash minimization.** It is a libFuzzer
>   `-merge_control_file` / `-merge_inner=1` *corpus merge*
>   (`cargo-bolero-0.13.4/src/libfuzzer.rs:80-101`). The pinned tool has **no**
>   crash-minimizing subcommand at all: `Commands = Test | Reduce | List | New |
>   BuildClusterfuzz` (`src/main.rs:30-40`). There is no `-minimize_crash` path.
> - And per the #149 correction above, `fuzz/tests/__fuzz__/` holds **zero seeds**, so
>   there is no committed corpus for a minimized crash to join.
>
> So the artifact → permanent-stable-regression-test path is an undocumented human
> procedure, improvised at 3am UTC on a nightly-only, non-notifying lane. Tracked on
> **#164** (seeds) plus a crash-triage runbook card.

Falsifiability, per the #149/#150 precedent, is checked at two levels: the *gate over the
workflow* (`bombay-actionlint`, which also shellchecks the `run:` blocks) was confirmed to
FAIL on an injected bad input at `fuzz.yml:58`, then reverted; the *lane itself* is
exercised by its own `pull_request` trigger — the numbers above come from that run, not
from a local simulation, and rising coverage is the evidence it fuzzed rather than merely
exited 0.

Standing caveats: fuzzing **samples** an input space (a green lane is evidence, not proof);
a 60/120 s budget is smoke depth, not a campaign; and `bombay-actionlint` — like every
flake check — sources from the **git tree**, so it silently passes over an *untracked*
workflow. Stage a new file before believing its green.

## MIRI lane (#150) — UB/race/leak coverage of the ref-model, incl. flume's internals
`.github/workflows/miri.yml`, scheduled nightly + PR + dispatch; **never** the flake gate
(nightly stays quarantined to this lane and #152's; reproduce locally via
`nix develop .#miri`). MIRI
interprets flume's *real* `std::sync` atomics — the only tool that reaches them, since
loom/shuttle require opt-in instrumentation flume does not ship (ADR-0005). Two legs,
both measured 2026-07-16:
- **Sweep** — full `bombay --lib`, isolation on, `--skip prop_` (proptest's
  failure-persistence file I/O is what isolation forbids): 79 passed / 0 failed /
  3 filtered, **42 s real**.
- **Schedules** — `-Zmiri-many-seeds=0..64 -Zmiri-many-seeds-keep-going` over three
  ref-model tests (last-ref-drop; receiver-drop mid-backlog; the enqueue-before-last-drop
  self-pin): 64 seeds × 3 tests, **24.6 s real**.

> **⚠️ Corrected 2026-07-17 (#168) — the Schedules leg explores a single-threaded space.**
> `-Zmiri-many-seeds` permutes MIRI's scheduling among **ready OS threads** (plus
> weak-memory read-buffering). All three tests in that leg are plain `#[tokio::test]` —
> i.e. tokio's **current_thread** runtime (`spawn.rs:316`, `spawn.rs:360`,
> `mailbox.rs:733`) — and `tokio::spawn` there stays on the same OS thread;
> `dropping_receiver_mid_backlog_frees_the_queued_message` spawns nothing at all. So 64
> seeds × 3 tests ≈ 192 near-identical executions, not schedule exploration. **The timings
> above corroborate it independently**: 24.6 s for 64×3, versus 42 s for the Sweep's 79
> tests at one seed each — the many-seeds leg does roughly one test's worth of work.
>
> The inversion is exact: the tests that *do* have real overlap — `spawn.rs:1101`
> `concurrent_senders_single_writer_exact_count` (multi_thread, 4 workers, 8 senders +
> `Barrier`), `mailbox.rs:821`, `reply.rs:181` — run in the **Sweep** at one seed each and
> are **excluded** from Schedules. Sampling flume's real interleavings was the entire
> rationale for choosing MIRI over loom (ADR-0005), and that rationale currently lands on
> the leg that does not sample. Tracked as **#172**. The Sweep leg (UB/data-race/leak over
> `--lib`) is **sound and unaffected**.
>
> Two of the three tests also assert something other than the race #150's card names:
> `dropping_last_actor_ref_stops_the_actor` asserts `handled == 0` ("no messages were sent
> before the ref dropped") rather than racing a `tell`, and the receiver-drop test is fully
> sequential (`try_send` → `drop(tx)` → `drop(rx)` → assert) rather than racing an
> in-flight send. Both are valuable tests — the leak canary in particular is falsifiable
> against bombay's own `Drop` impl — but the named windows are unexercised. Tracked as
> orphans on #117.

Falsifiability verified per the #149 precedent: a message-vanishing probe in
`send_message` makes the self-pin test FAIL (0 ≠ 1) under the lane, then reverted.
Standing caveats: MIRI **samples** schedules (a green lane is evidence, not proof), and
the #148 fail-fast bounds are MIRI-aware via `test_support::terminate_bound()` (5 s
native, 10 min under the interpreter — MIRI's virtual clock ticks 5 µs per basic block).

## Exact-memory reclamation (#151) — in-gate counting allocator
`crates/core/tests/alloc_exact.rs` — a **dedicated one-test binary** (a
`#[global_allocator]` counts its whole process, and only a lone test is
process-isolated under both nextest and plain `cargo test`) asserting the ADR-0003
`queue → Signal → Sender → Arc<Shared>` cycle reclaims to an **exact** bytes+allocs
baseline after a mid-backlog receiver drop. `CountingAlloc` lives on the `test-support`
seam (signed counters; `Relaxed` with a structural single-thread proof). A warm-up round
before the baseline excludes one-time lazy init, keeping the assertion exact with no
whitelist. Falsifiability verified: `mem::forget(rx)` fails it (+992 bytes / +11 allocs
observed), then reverted. #151's other half — the nightly MIRI leak/UB job — was
delivered by #150's `miri.yml` (the leak checker is active in the sweep; the
mid-backlog Drop test runs in both legs).

## Recipient zero-box transit (#207) — the counting allocator on the erased path
Two more **dedicated one-test binaries** (same `alloc_exact.rs` isolation
rationale) close the #168 WEAK finding the #145 finalization matrix left open:
`Recipient`/`ReplyRecipient` exist **for** zero-box message transit, but no
executable guard held it — a refactor that boxed the message would have gone red
nowhere. The `CountingAlloc` `gross_allocs` counter (the #118 seam) now guards it:

- `tests/alloc_recipient.rs` — `recipient_try_tell_is_zero_box_like_a_direct_send`:
  a `Recipient<M>` `try_tell` performs the **exact** gross-allocation count of a
  direct typed send (**0** — the message rides inline in the queue slot, only the
  handle is `Arc<dyn>`), and the `A::Msg: From<M>` conversion boundary itself is
  **0**-alloc for the representative message. Non-ZST `u64` payload so a boxed
  message is a countable heap allocation (a boxed ZST is a no-op). Falsifiability
  verified: a `Box::new(msg)` in `try_tell` fails it (0 → 1), then reverted.
- `tests/alloc_reply_recipient.rs` —
  `reply_recipient_ask_boxes_only_futures_never_the_message`: an erased ask
  allocates exactly the reply port a direct ask does (**1**) plus the two
  `dyn`-dispatch future boxes (`into_future` + `deliver`) — **3** total, message
  inline in the `Ask` carrier, never boxed. Falsifiability verified: a
  `Box::new(msg)` in `deliver` fails it (3 → 4), then reverted.

Test-only; no production change. Ties ADR-0004 (conversion-boundary erasure, the
256× queue-memory swing) to an executable check. The `WeakRecipient` /
`WeakReplyRecipient`-after-upgrade leg is deferred to **#208** (which introduces
`WeakReplyRecipient`); #207 bullet 3 lands there — the guard extends this harness
when that type ships.

## Restart-set strategies (#199) — the set-cycle coordinator
`OneForAll` / `RestForOne` (ADR-0014) add three test surfaces:

- **Decision layer (`kind.rs` unit tests)** — the pure, synchronous state machine:
  `set_trigger_flags_set_and_counts_trigger_once`, `absorbed_deaths_count_down_and_arm_rebuild`
  (an `on_stop`-panic mid-teardown is absorbed, not escalated), `elder_death_mid_tearing_widens_the_cycle`,
  `widen_during_waiting_replaces_the_armed_deadline` (the stale-deadline / half-alive hazard —
  `retries.len() == 1`), `rebuild_child_is_superseded_for_cycling_entries`,
  `removing_an_awaited_member_counts_the_teardown_down` (the wedge counterexample). Plus the
  `Children` cycle-op units in `supervision.rs` (`flag_cycle` reverse-order + widen-idempotence,
  `absorb_cycling_death`, `cycling_rebuild_ids` birth-order + `Never`-exclusion). #252 adds
  `prop_flag_cycle_awaiting_equals_newly_flagged_live_count` — a proptest pinning
  `awaiting == stops.len()` (exact count of newly flagged live suffix members, never capped)
  across table sizes 0..16 and the empty-suffix boundary, the bound the conversion's
  panic-over-sentinel `expect` relies on.
- **Behavioral (`spawn.rs` `supervised_rebuild`)** — end-to-end against the wired loop:
  OneForAll (all-rebuilt / reverse-stop-birth-rebuild / count-once-against-budget), RestForOne
  (suffix-only / last-child-degenerates-to-OneForOne), `Never`-excluded-from-set, the four
  mid-cycle races (sibling-death-absorbed, unsupervise-mid-cycle-no-wedge, widen-supersedes-deadline,
  serves-messages-during-teardown), the heterogeneous-children sequence (two actor types through
  the erased factory edges), and supervisor-exit teardown (normal stop, hard kill, early join under
  paused clock, abort of a cancel-ignoring child, bounded kill-during-`on_stop`). Each headline
  set-restart test is **bite-checked** by flipping the supervisor to the default `OneForOne` and
  confirming it fails (the #149 vacuous-green guard).
- **DST (`tests/dst_races.rs`)** — `dst_restart_storm_deterministic` (same seed + schedule ⇒
  identical `(virtual_ms, tag)` rebuild trace; keyed on logical tags, never process-global
  `ActorId`s), `dst_concurrent_link_unlink_die` (8 seeds; no rebuild after `unsupervise`, no wedge),
  `dst_backoff_distribution_measured` (delays ∈ `[base(n), base(n)·1.2]`; the #196 tuning defaults
  confirmed against the measured distribution and posted to #199). Seeded via the new
  `test_support::set_supervisor_rng_seed` seam (integration tests link the lib `not(test)`, so the
  `#[cfg(test)]` seed thread-local is unreachable; `spawn.rs::supervisor_rng` reads either seam).

The design was validated by an executable discrete-event model
(`docs/superpowers/specs/2026-07-25-199-cycle-model.rs`) before the spec — the SUT's DST expectations
cross-check against it; every coordinator element is load-bearing by a reproduced counterexample.

## Tracing feature (#209) — capture-subscriber suite

`crates/core/tests/tracing_capture.rs` — **9 tests** over the `tracing` feature's spans and
events. The whole binary is `#![cfg(feature = "tracing")]` (a `--no-default-features` build
has no surface to test); the off build is covered instead by the `bombay-tracing-off` flake
check, which compiles `-p bombay --no-default-features` and fails if the `tracing` crate
appears in the resolved normal-dep graph. A hand-rolled `CaptureLayer` over the
`tracing-subscriber` registry (installed as the *thread* default per test — current-thread
runtime, so every actor task emits on the capturing thread) records spans
(id / resolved parent / `follows_from` links / fields) and events (level / enclosing span /
fields):

- **lifecycle span identity** — `actor.lifecycle` carries `actor.name` + `actor.id` at
  creation, records `stop.reason` at teardown, is a **root** (`parent == None`), and
  `follows_from` exactly the spawn-site span;
- **`on_stop` failure events** — error / panic / grace-abandonment each emit **exactly one**
  `error!` with structured `reason` (+ `err` / `grace`) fields — the retired-`eprintln!`
  replacements can actually fail their assertions;
- **handle-span parenting** — `actor.handle` parents to the caller's span captured at
  enqueue (with `msg.kind` + `actor.name`); no caller span at enqueue → contextual fallback
  to the lifecycle span;
- **handler crash** — a handler's `Err` emits one `error!` firing *inside* that message's
  `actor.handle` span;
- **restart warn** — a seeded supervisor RNG (`set_supervisor_rng_seed`, paused clock) pins
  the scheduled-restart `warn!`'s exact `restart.attempt` / `restart.delay` / `child.id`;
- **death notice** — delivering a death notice is one `trace!` per watcher edge with
  `watcher.id` / `reason` / `cleanup_failed`.

Structural half: the two slot-size tripwires in `mailbox.rs` pin `SendContext` (the
caller-span envelope field) to at most **one word**, so the #209 context can never silently
fatten every queue slot.

## Queued-registration teardown (#248) — drain + drop-guard

Closes ADR-0019's residual window (a `supervise` registration still queued at
supervisor exit orphaned its already-spawned child). Unit half
(`supervision.rs`): an armed registration's drop cancels **and** aborts the
first incarnation; a disarmed one touches neither edge. Lifecycle half
(`spawn.rs`, paused current-thread):

- **stop with the `Add` still queued** — graceful epilogue drain inserts the
  child and the sweep joins it before `RunResult::Stopped` resolves (no
  barrier: there is no await between `supervise` returning and `stop()`);
- **kill with the `Add` still queued** — the dropped mailbox fires the
  drop-guard; child provably dead, `RunResult::Killed`, `on_stop` skipped;
- **queued `Remove` detaches** — the child survives the supervisor and still
  accepts messages (ownership transfer honored, not swept);
- **queued `Stop` still stops** — drained onto `pending_aborts`, grace
  truncated at exit;
- **failed-send handback** — `supervise` on a dead supervisor now *asserts*
  the anchored child keeps running unsupervised (the disarm path; this
  contract previously had no regression test).

The two `quiesce()` barriers the #245 lifecycle tests carried (their comments
pointed here) are **removed** — the teardown invariant holds whether the ops
were applied by the loop or drained by the epilogue.

## Ref-count collection is not restart-worthy (#253, ADR-0020)

Splits the two stop causes the run-loop had collapsed into `Normal`: a cancel-
token graceful stop stays `Normal`; an all-senders-gone ref-count stop becomes
`ActorStopReason::Collected`. `Collected` is classified normal but is
`LeaveDead` under every `RestartPolicy`, so an unanchored supervised child
collects once instead of churning the supervisor to death.

- **Decision table (`restart.rs`)** — `collected_leaves_dead_under_every_policy`
  pins `Permanent`/`Transient`/`Never` all returning `LeaveDead`;
  `permanent_restarts_on_every_reason_except_collected` and the updated
  `transient_splits_every_reason_between_dead_and_restart` keep the exhaustive
  variant coverage.
- **Classification (`error.rs`)** — `Collected.is_normal()` is `true` and its
  `Display` message is pinned.
- **Loop split (`kind.rs`)** — `MailboxPoll`/`poll_mailbox` keep `Cancelled`,
  `Closed`, and `Signal` distinct; the change is covered by the boundary tests
  below and the updated fuzz oracles.
- **Boundary tests (`spawn.rs`)** — `dropping_last_actor_ref_stops_the_actor`
  and `queued_message_is_handled_even_if_last_ref_drops_first` now assert
  `Collected`; `watch_notified_on_collected_stop` distinguishes explicit `stop()`
  (`Normal`) from ref-count drop (`Collected`).
- **Supervision probe (`spawn.rs` `supervised_rebuild`)** —
  `collected_permanent_child_is_left_dead_and_supervisor_survives` (default
  budget) and `collected_child_does_not_churn_even_unbudgeted` (`max_restarts =
  max_total = u32::MAX`) both anchor a Permanent child with no strong ref and
  assert exactly one incarnation, a surviving supervisor, no
  `RestartLimitExceeded`, and a surviving anchored sibling.
- **Observability (`tracing_capture.rs`)** —
  `collected_child_emits_debug_event_and_no_restart_scheduled` asserts the
  `child_collected` debug trace names the child and that no `restart_scheduled`
  warn fires.
- **Fuzz oracles (`fuzz/tests/actor_loop.rs`)** — the actor-loop state machine
  and the failing-`on_stop` preservation test split non-kill outcomes into
  `Normal` (graceful stop) and `Collected` (pure drop-refs fall-through).

## Drain-window supervision equivalence (#267)

Closes the supervision-verb half of the ADR-0010 mint-equivalence surface #266
left deferred: a handler-context `supervise` / `stop_child` / `unsupervise`
behaves identically whether the handler's `ActorRef` is the steady-state shared
upgrade or a drain-window mint. Unlike watch/link (ops on the OTHER actor's
lane), supervision ops target the issuing supervisor's OWN control lane while
its loop is inside the handler, so every choreography is built around
ops-applying-after-handler-return. New file
`crates/core/tests/drain_supervision_equivalence.rs` (sibling of
`drain_equivalence.rs`, same oracle discipline: ONE mode-blind runner per
scenario, `Mode` influencing only the held external ref and
enqueue-before/after, full-trace `assert_eq!` against a `vec![..]` literal then
steady-vs-drain, exact per-incarnation `RunResult` assertions via a
factory-captured incarnation slot, started-count via a start-notification
channel, every await `bounded()`, oneshot gates):

- **supervise install + restart edge** — a drain-minted `supervise` inserts
  the table entry and installs the watch edge for real: a killed Permanent
  child is rebuilt (biased death arm → zero-backoff retry → rebuild before
  the `Collected` break), and the rebuild is swept by the exit teardown.
  Runs under `start_paused`: probe-verified this card that an at-now
  `DelayQueue` deadline polls ready at the immediately following select
  iteration ONLY under a frozen virtual instant — under a real clock the
  wheel's deadline lapses ~1 ms later and the drain-mode `Closed` break
  would always win, so the paused clock is what makes the
  rebuild-before-collect ordering deterministic (the `control_lane.rs` /
  `dst_races.rs` discipline).
- **stop_child** — graceful cancel from the mint; the child joins
  `Stopped { Normal }` before the supervisor exits (the epilogue's
  `PendingAbort` drop-abort a proven no-op), never rebuilt, supervisor
  collects identically.
- **unsupervise** — detach from the mint: the child answers its liveness
  probe after the supervisor's death (the sweep only sweeps table children),
  never rebuilt.
- **supervise racing the supervisor's own stop flag** — the flag bypasses
  the last mailbox poll, so the queued `Add` is applied by the graceful
  epilogue's `drain_queued_supervision` and swept by `teardown_children`
  (installed-then-swept `Stopped { Normal }`, never guard-aborted `Killed`)
  — the #248 never-orphaned invariant re-asserted for the mint path, and
  exhaustive for the mint-path race because the control-first merge makes
  the `Collected`-break race unreachable from a handler's own queued op
  (positive finding, documented in the file header). The #248
  `SendError`-handback disarm is documented as UNREACHABLE from a mint (the
  mint's own sender keeps both lanes open), not tested — no test can
  construct it.

Test-only card: zero production-code changes, so `mutants-baseline.json` and
the README are untouched. The falsifiability caveat (the 60 s `stop_grace`
tripwire vs `TERMINATE` under real-time legs, and its paused-clock/miri
behavior) is recorded in the test-file header.
