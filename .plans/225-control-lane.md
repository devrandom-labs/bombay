# 225 — control-signal lane: watch/supervision ops must not queue behind user backlog

## Context

Card #225 (M1, `qos`/`runtime`). Today ONE bounded flume carries every
`Signal` variant (`mailbox.rs:140`): `Message`, `Stop`, `Watch`, `Unwatch`,
`Supervision`. Consequence: watch registration and supervision ops `.await`
capacity behind the user backlog — supervision reacts slowest exactly when the
system is loaded. Evidence: #218 wart 3 (`docs/warts/218-example-warts.md`),
EEP-76 (Erlang/OTP 28 added a priority lane for exactly this).

**Mechanism (decided, ADR-0021 to record):** a second, UNBOUNDED flume channel
for runtime control signals, merged **inside `MailboxReceiver::recv`** with a
control-first biased select. Validated in the sibling `fastpass` research repo
(same recv-side merge shape, full property suite P1–P8 green on the flume
variant; a lock-free variant exists there as a future swappable backend — NOT
this card). Precedent in-tree: the unbounded link channel + `biased;` arm
(`actor/kind.rs:294`, `actor/kind.rs:399`).

Invariants that must hold (each gets a test, most exist as card checkboxes):

1. Watch/link registration reaches a full-mailbox actor before its backlog
   drains; same for `SupervisionOp` (latency independent of queue depth).
2. FIFO **within** the control lane (watch-then-unwatch = no edge; reversed =
   edge stays).
3. User-message FIFO-per-sender unchanged; the zero-alloc send path unchanged
   — the **allocation profile** is the invariant, not byte-identical layout:
   `MailboxSender` gains `ctl_tx`, so the embedded `self_sender` grows every
   `Message` slot by one `flume::Sender` (~8 bytes). Accepted and recorded in
   ADR-0021; the slot-size tripwire
   (`mailbox.rs:649` `cold_variants_are_boxed_so_message_slots_stay_small`)
   is updated to the new expected size, and the #207 guard binaries' alloc
   assertions must still pass after their mechanical `Recv`-match updates
   (see step 4 chase list).
4. A control signal enqueued after a user message MAY overtake it; `Stop`
   still drains messages queued before it (Stop stays on the USER lane).
5. Teardown: a queued `Watch` on the control lane is answered with a synthetic
   death notice on receiver drop — the #195 obligation moves lanes intact
   (today's logic: `mailbox.rs:349-417`).
6. Liveness/ref-count semantics untouched: only `Signal::Message` embeds a
   strong `self_sender` (ADR-0003); control sends never pin the actor. The
   control tx lives INSIDE `MailboxSender`, so sender-count/disconnect edges
   stay coupled — `Collected` stop detection is unchanged.
7. Unbounded control lane: the bound is **caller call rate**, not a structural
   dedup — `Watchers::apply` deliberately keeps duplicate edges (Erlang-style
   independent monitors, `actor/watch.rs:113-125`), so re-watch does NOT
   replace. Each `watch`/`unwatch`/`supervise` API call enqueues exactly one
   op; the lane is caller-floodable like any unbounded queue. ADR-0021 states
   this honestly (rate-bounded-by-caller-discipline, same trust class as the
   unbounded link channel); the flood test is LOAD-BEARING, not documentary:
   sustained control flood ⇒ queue grows, no panic, intra-lane FIFO holds,
   user lane still drains via recv policy.
8. Loop arm count preserved: the merge lives inside `recv`, so
   `run_message_loop` / `run_linked_message_loop` / `run_supervised_message_loop`
   keep their existing `select!` shapes — DST seed tests are extended with
   control-signal interleavings, not forked (`crates/core/tests/dst_races.rs`,
   seed seam in `test_support.rs`).
9. `recv` must stay cancel-safe (it sits under `run_until_cancelled` and
   biased selects): the internal merge uses try_recv-first + select on both
   `recv_async` futures; flume buffers items in shared state, so dropping a
   losing arm loses nothing (pinned empirically in the fastpass repo,
   `edge_cases::recv_future_cancellation_loses_nothing`).

## Steps

All steps SEQUENTIAL unless marked — the early ones all touch `mailbox.rs`.

1. **Failing tests first (TDD)** — new integration test file
   `crates/core/tests/control_lane.rs` + unit tests in `mailbox.rs`:
   - `watch_installs_before_full_backlog_drains`: fill a bounded mailbox with
     user messages, send a watch, assert the edge is installed (death notice
     later arrives) without draining the backlog first.
   - `supervise_op_applies_before_full_backlog_drains`: same for
     `SupervisionOp::Add` via `supervise` (`actor/actor_ref.rs:358`).
   - `control_lane_fifo_watch_then_unwatch`: watch→unwatch ⇒ no notice;
     unwatch→watch ⇒ notice. Asserts intra-lane FIFO.
   - `control_overtakes_earlier_user_message`: deterministic overtake witness.
   - `stop_still_drains_prior_messages`: regression for Stop-on-user-lane.
   - `queued_watch_answered_on_teardown`: port of the #195 drop test against
     the control lane (reuse `test_support.rs:81` signal builder).
   These fail to compile until step 2 — write them alongside step 2's enum
   split, then keep them red until step 4 wires the recv merge.
2. **Split the enum** (`crates/core/src/mailbox.rs:140`): new
   **non-generic** `ControlSignal` — `Watch(Box<WatchReg>)`,
   `Unwatch(ActorId)`, `Supervision(Box<SupervisionOp>)`; none of the
   payloads are generic over `A` (`actor/watch.rs:44`,
   `actor/supervision.rs:268`), and the repo rule bans unused generic
   parameters. `Signal` keeps `Message` + `Stop`. This is a BREAKING change
   to the public `Signal` enum (`lib.rs:39` re-export, used by external
   tests e.g. `dst_races.rs:574`) — pre-1.0, accepted, owned by ADR-0021.
   Chase exhaustive-match fallout across the WHOLE repo (examples, fuzz —
   `fuzz/tests/mailbox.rs` matches every arm — `request.rs:106`,
   `mailbox.rs:450`, `actor/kind.rs:251-263`, `actor/kind.rs:1018-1033`,
   `actor/spawn.rs:405-414`, `test_support.rs:81`); `cargo check --workspace`
   AND the fuzz workspace; a missed site is a compile error, fix all.
3. **Second channel in the mailbox types** (`mailbox.rs:193`):
   `Mailbox::bounded` creates `flume::bounded(cap)` + `flume::unbounded()`;
   `MailboxSender` gains `ctl_tx`, `MailboxReceiver` gains `ctl_rx`. New
   sender method `send_control(ControlSignal)` (sync, unbounded — returns
   `Result<(), ControlClosed>`-shaped error carrying the signal back, naming
   mirroring existing conventions); NO change to `send_message`
   (`mailbox.rs:253`). **`WeakMailboxSender` pairs up too**: `downgrade`
   (`mailbox.rs:291`) captures both weaks, `upgrade` (`mailbox.rs:310`)
   upgrades both-or-`None` (3 construction sites total); `is_closed`
   (`mailbox.rs:284-288`) keeps checking only `tx` — valid because the
   receiver drops both rxs together, so the disconnect edges are coupled by
   construction (document this on `is_closed`).
4. **The merge in `recv`** (`mailbox.rs:336`): return type becomes an enum the
   loops can match — `Recv<A> { Control(ControlSignal), Signal(Signal<A>) }`
   (name at K3's discretion; keep it small). Policy: control `try_recv` first,
   then user `try_recv`, else `select!` over both `recv_async` with `biased;`
   control-arm-first; closed-flag handling per lane so a disconnected arm
   cannot spin the select (mirror the link-arm guard, `actor/kind.rs:274`
   doc). `None` only when BOTH lanes are closed and empty. Step 1's tests go
   green here except teardown.
   **Chase list for the changed `recv()` return type** (mechanical `match`
   updates, alloc assertions must still hold): the #207 guard binaries
   (`alloc_request.rs:83,94-100`, `alloc_reply_recipient.rs:100,130`,
   `alloc_recipient.rs:84`), `msg_mailbox_compose.rs:53-103`,
   `examples/msg_budget.rs:87`, `fuzz/tests/mailbox.rs:106-110`, plus every
   in-crate `recv().await` destructure.
5. **Teardown moves lanes — TWO distinct paths, do not conflate**
   (`mailbox.rs:349-417`, `actor/spawn.rs:386,507`, `actor/kind.rs:1004-1035`):
   - **Hard-kill path** (receiver `Drop` / `close_now`): drain `ctl_rx`,
     answer every queued `Watch` with the synthetic death notice (today's
     `reject_queued_watchers` logic, relocated); queued `Supervision` is
     dropped HERE and only here — same as today. User-lane drain keeps
     releasing `self_sender` pins.
   - **Graceful supervised teardown**: today `drain_queued_supervision`
     (`actor/kind.rs:1004-1035`, called from the spawn.rs teardown) APPLIES
     queued ops — `Add` → `install_registration`, `Remove` →
     `children.remove`, `Stop` → `PendingAbort` — it does NOT drop them.
     That behavior must survive the lane move: the graceful path drains
     `ctl_rx` through the same apply logic (plus watch/unwatch application),
     not through the reject path. Regression test: supervise-op queued at
     graceful teardown still lands (child adopted/stopped), asserting the
     #245/#248 semantics hold on the new lane.
6. **Send-site migration**: `actor/actor_ref.rs:263` (unwatch), `:296`
   (watch), `:391`/`:469` (supervise/unsupervise) and the one-shot
   `WatchInstaller` (`actor/supervision.rs:182`) switch to `send_control`.
   The #248 `ArmedReg` disarm-on-`SendError`-handback semantics must
   survive: `send_control`'s closed-error handback carries the op so the
   existing disarm paths (`actor/actor_ref.rs:399`) keep working.
   **Deliberate semantic change — `WatchOutcome::Full` becomes unreachable**
   (`actor/supervision.rs:143-158`, matched at `actor/kind.rs:900-916`):
   today a flooded child's bounded mailbox fails the watch install (`Full` =
   failed incarnation); on the unbounded control lane the install always
   lands, so a flooded child is now watched (late) instead of failed.
   Remove the dead `Full` variant and its match arm (exhaustive-matching
   rule: no dead variants), and record the semantic change in ADR-0021 —
   this is an intended IMPROVEMENT (registration no longer lost to user
   backlog, the exact point of the card), not incidental fallout.
   **Doc sweep at the migrated sites**: `actor/actor_ref.rs:266-272`,
   `:340-342`, `:459-470` all claim ".await for mailbox capacity — ordinary
   backpressure"; false after the move — rewrite to the unbounded-lane
   contract.
7. **Loop plumbing** (`actor/kind.rs:173-263` + the three loops + `actor/spawn.rs`):
   `poll` (`actor/kind.rs:189`) maps the new `Recv` enum into `MailboxPoll` — add a
   `Control(ControlSignal<A>)` arm; `step_signal`/`apply_*` handlers move
   their Watch/Unwatch/Supervision match arms over. Loop `select!` shapes
   unchanged (invariant 8).
8. **DST + property extension** (PARALLEL OK with 9/10 — different files):
   extend `crates/core/tests/dst_races.rs` seeds to interleave control sends
   with user backlog + assert invariants 1/2/4; extend the existing user-FIFO
   proptest (`prop_*` naming — MIRI contract) with interleaved control
   signals.
9. **Zero-alloc + bench** (PARALLEL OK): #207 guard binaries get the
   mechanical `Recv`-match updates from step 4's chase list; their
   **allocation-count assertions must pass unchanged** (that is the
   invariant — the code around them may move). Update the slot-size
   tripwire (`mailbox.rs:649`) to the new expected `Message` size (grown by
   one embedded `flume::Sender`; invariant 3). Add a criterion arm to the
   existing bench (`crates/core/benches/`) measuring control-delivery
   latency vs user-queue depth {0, 64, 1024, at-cap} — flat curve expected;
   numbers go in ADR-0021.
10. **ADR-0021 + docs** (PARALLEL OK): `docs/adr/0021-control-signal-lane.md`
    — decision (in-recv two-channel biased merge), the fastpass research
    numbers + pointer to the sibling repo, EEP-76/Pekko/CAF/ractor citations
    (from the card), the deliberate ordering relaxation, the honest
    unbounded-lane bound (caller-rate, invariant 7 — NOT structural dedup),
    the `WatchOutcome::Full` semantic change (step 6), the `Message`-slot
    size growth (invariant 3), the breaking `Signal` variant removal
    (step 2), and the future fastpass swappable backend as explicitly
    out-of-scope. Update `docs/testing/coverage-baseline.md`. README: the
    `watch`/`supervise` *signatures* are unchanged, but `Signal` is public
    and loses variants — check the README's public-API bullets and touch
    only if they name `Signal` variants.
11. **Walking skeleton** (job-queue rule, CLAUDE.md item 7): extend
    `crates/core/examples/job_queue/` + `crates/core/tests/app_job_queue.rs`
    with a scenario where the dispatcher is backlogged (full mailbox) and a
    `supervise`/watch op still lands promptly — assert roster/supervision
    state updates before the backlog drains. (The #218 wart-3 `WorkerReplaced`
    user-message drop is NOT fixed here — it rides the user lane by design;
    #244 tracks it. Say so in the test comment.)
12. **mutants baseline**: add entries for every new/renamed fn
    (`send_control`, the recv merge, teardown drain helpers) to
    `mutants-baseline.json` — non-Default returns to `known_zero_viable` per
    the baseline workflow; unbounded awaits are banned in the new tests
    (every await bounded — mutants Timeout policy).

## Verification

- Per-step: `cargo check --workspace` + `cargo clippy --workspace` ONLY (K3
  never runs tests — sandbox hang; see kimi-delegate).
- Gate (Claude drives, unsandboxed): `nix flake check` — includes tests, fmt,
  mutants ratchet. `git add` new files BEFORE the gate (untracked = invisible
  to flake checks).
- MIRI lane note: new proptests carry the `prop_` prefix; new sync tests stay
  current-thread-compatible where feasible.

## Out of scope

- The lock-free fastpass backend and any feature-gated swap layer (separate
  repo/crate; lands only after its loom + MIRI hardening).
- User-level message priorities (M3+ question per the card).
- #244 (`WorkerReplaced` rides the user lane — pipe-to-self is a user
  message; unchanged here).
- Any change to `Signal::Message` slot layout, `send_message`, ask/tell
  builders, or the link channel.
