# Coverage baseline (card #85)

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
> pass/fail — currently **64 viable / 215 total** (see the #148 section below and
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

## bombay-core — the M1 core rebuild (#112+)

The rebuilt spine lives in the `bombay-core` crate (part-by-part, epic #122), born
under the god-level bar (no #61 quarantine). It is measured by the same
`--workspace` coverage run and adds a **reproducible mutation gate**:

```bash
nix build .#mutants -L   # cargo-mutants over the workspace + macros/src/derive_msg.rs, plus
                          # a mutants-gate verdict tool: fails on any survivor, any timeout,
                          # an interrupted run (fewer recorded outcomes than candidates), or a
                          # per-`file::function` viability collapse against the committed
                          # baseline; always prints the "N viable / M total" ratio
```

Pinned via the flake's `nixpkgs` (never `nix run nixpkgs#…`), mirroring the coverage
package. Quarantined off `nix flake check` (rebuild-per-mutant is too slow for the
per-push gate) — the nightly run is `.github/workflows/mutants.yml`; the gate's design
(the per-function baseline, the viability-collapse check) is recorded in
`docs/adr/0006-mutation-viable-ratchet.md`. Measured 2026-07-17 (this branch), whole
package + `derive_msg.rs`: **64 viable / 215 total** mutants (64 caught, 0 missed, 0
timeout, 151 unviable) — the ratchet baseline is 47 floored functions (viable ≥ 1) plus 48
documented 0-viable functions in `known_zero_viable` (including `WeakActorRef::with_sender`,
which now also carries a hand-written compensating test, and all of
`macros/src/derive_msg.rs`, whose #114 derive is empirically 0-viable and is instead covered
by #170's compile-fail lane).

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
pinned by the `slot_budget_defaults_to_256` unit test instead. This absence is no
longer silent: the standing gate (`nix build .#mutants`, #171/#165) reports outcomes
**per `file::function`**, so a file contributing zero candidates shows up as such in the
run rather than being folded into an aggregate total.

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
> to `bombay-core/src/reply.rs:32`'s consume-once probe: **all four `compile_fail`
> doctests in the rebuilt spine are dead in the gate**, and no `trybuild` compensates.
>
> Concretely: delete the `const _: () = assert!(…)` tripwire from
> `macros/src/derive_msg.rs:64-68` and the gate stays green — every in-gate
> `#[derive(Msg)]` type is within budget, and `examples/msg_budget.rs`'s tripwire demo
> is commented out (`:26-33`). `size_exactly_at_budget_compiles` guards `<=` vs `<`,
> not assert-vs-no-assert. The unit tests (`parse_budget` grammar, generic/union
> guards) DO run and are unaffected. Tracked as **#170**.

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
signals) is the loom/shuttle target noted under `mailbox` above. This surface is,
however, swept by the standing whole-package gate landed under #171/#165 (see the
#148 section below) — its per-function ratio now covers `actor/kind.rs` and
`actor/spawn.rs` alongside every other bombay-core module. No README
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
> `bombay-core/src/actor/actor_ref.rs`, so a wrong-`id`/stale-`cancel` copy that its own 0
> viable mutants cannot catch is caught there instead. The current whole-package +
> `derive_msg.rs` measurement (2026-07-17, this branch, complete run — not interrupted) is
> **64 viable / 215 total** mutants (64 caught, 0 missed, 0 timeout, 151 unviable; 215 vs.
> the 205 candidates PR #157 attempted, because the sweep now also covers
> `macros/src/derive_msg.rs`, which is empirically 0-viable and is documented as such rather
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
> 1. **Wrong surface (tracked as #164).** `bombay-core/src/mailbox.rs:200` is
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
- **Sweep** — full `bombay-core --lib`, isolation on, `--skip prop_` (proptest's
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
`bombay-core/tests/alloc_exact.rs` — a **dedicated one-test binary** (a
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
