# Card #266 — adversarial invariant tests for drain-window watch/link

## Context

Follow-up to #260 / PR #265. The three #260 tests in `crates/core/src/actor/spawn.rs`
(`drain_window_handler_watch_succeeds`, `steady_state_handler_watch_succeeds`,
`drain_window_watch_delivers_death_notice`) verify the fix mechanics. This card pins
the INVARIANT (ADR-0010): **handler-context `watch`/`link` behaves identically whether
the handler's `ActorRef` is the steady-state shared upgrade or a drain-window mint.**
External ref-count liveness must be unobservable through the watch verbs.

Mint site: `crates/core/src/actor/kind.rs:262` — `self_ref.upgrade().unwrap_or_else(|| ActorRef::new(.., handles.link_tx.clone()))`.
Linked loop: `run_linked_message_loop` (`kind.rs:299`) — `biased` select, link arm first,
`link_open` flag. Break decisions: `handle_mailbox_step` (`kind.rs:215`) —
`Cancelled → Normal`, `Closed → Collected`, `Signal::Stop → Normal`.
Epilogue: `finish_actor` (`spawn.rs:501`) — drains mailbox control lane, NEVER drains
`link_rx`.

### Decisions already made (Joel, 2026-07-30 — do not relitigate)

1. **Late-notice semantics = designed-lost.** A death notice that lands on the link
   channel AFTER the loop's break decision is dropped (Erlang parity: DOWN to a dead
   process is dropped; delivering post-break would violate finish-current-then-stop).
   Pin with a GREEN test asserting non-delivery + clean stop, plus a doc note on
   `Watch::on_link_died` (`crates/core/src/actor/mod.rs:133` region). NOT an
   `#[ignore]` bug-probe.
2. **Strict equivalence oracle covers only the spec'd-equivalent verb set**:
   `is_alive`, `tell` (self + peer), `ask` (peer), `watch`, `unwatch`, `link`,
   stop, kill. `pipe_to_self` / `send_after` are EXCLUDED from the oracle — their
   drain-window result-drop is the spec'd fate (`pipe.rs:102-107`, weak-upgrade seam,
   ADR-0010/0017) — and get separate divergence-pin tests instead.
3. **`supervise`/`stop_child` from a drain-window mint: deferred** to a follow-up card
   (Claude files it; not K3's job).

### Invariants that must hold in the tests (engineering rules)

- Every test calls the real SUT through the public API (`bombay::actor::{PreparedActor, SpawnConfig, Spawn, SpawnLinked, Watch, ...}`) — integration tests in `crates/core/tests/`, NOT unit tests in `spawn.rs` (public API is sufficient; verified).
- Every await is bounded: use the file-local `bounded()` helper pattern + `test_support::terminate_bound` exactly as `crates/core/tests/dst_races.rs:62-93` does. NO unbounded `.await` on joins/oneshots (hang class from #148).
- `assert_eq!` with exact values. Exact variant assertions via full-trace equality or `assert!(matches!(..))` with the payload bound and checked — never `is_normal()` (#253: it no longer discriminates).
- Anti-gaming rule for the oracle: ONE parameterized `run_script(mode)` function; `mode` may influence ONLY (a) whether an external strong watcher ref is held across the run and (b) enqueue-before-run vs after. No other branch on `mode` anywhere. The steady trace is the oracle; `assert_eq!(steady, drain)` on the full `Vec<TraceEvent>`.
- Actor IDs differ between the two runs — traces must record ROLES, not raw `ActorId`s. Map ids to a `Role { SelfActor, Target, Peer }` via ids captured at spawn time; a notice with an unknown id maps to a distinct `Role::Unknown(())` (which would fail equality → good).
- `ActorStopReason` has NO `PartialEq` (`error.rs:341` derive is Clone+Debug). Canonicalize into a test-local `ReasonKind` enum deriving `Debug+PartialEq+Eq+Clone` mirroring ALL variants (`error.rs:342-410`): `Normal, SupervisorRestart, Collected, Killed, AlreadyDead, Panicked, LinkDied(Box<ReasonKind>), RestartLimitExceeded, ChildLifecycleFailed`. Exhaustive `match` on `ActorStopReason` (no `_` arm) so a new variant breaks compilation here.
- Choreography by oneshot gates (the `DrainWatcher` pattern, `spawn.rs:1249-1324`): `entered`/`release` channels; never sleep-based sequencing.
- `RunResult::Stopped { actor, .. }` carries final state — DROP it before waiting for a `Collected` stop of anything its fields pin (the #260 tests comment this; same discipline).
- Clippy: 80-line fn cap, cognitive complexity 9 — decompose fixtures/helpers; module-level fixture structs like the #260 cluster does.

## Steps

### Step 1 — `crates/core/tests/drain_equivalence.rs` (new file): oracle harness

SEQUENTIAL — foundation for step 2 (same file).

1a. File header: `#![cfg(test)]`-style integration test (no attr needed), imports at top
    following `dst_races.rs` conventions, local `cap()`, `bounded()` helpers.
    NOTE: `LinkReceiver` is a PRIVATE module's type (only `LinkDied` is re-exported,
    `lib.rs:53`) — do not try to import it; destructure `new_linked`'s tuple and pass
    the receiver by inference, as the #260 unit tests do.

1b. Trace types: `Role`, `ReasonKind` (as in Context), and
    `TraceEvent { IsAlive(bool), TellSelf(bool), TellPeer(bool), AskPeer(Result<u32, ()>), Watch(Role, Result<(), ()>), Unwatch(Role), Link(Role, Result<(), ()>), Notice { who: Role, reason: ReasonKind, linked: bool }, Finished(ReasonKind) }`
    — all deriving `Debug, PartialEq, Eq, Clone`. `Watch`'s `Err(())` maps `ActorNotLinked` (drop the payload; it is a unit-like error).

1c. Fixtures:
    - `EchoPeer`: `Watch`-spawned actor answering `ask` with a constant `u32` and accepting `tell`.
    - `Target`: trivial `Watch`-spawned actor (needs a link channel? NO — `watch` target may be plain-spawned like `WatchTarget` in spawn.rs; but `link` requires `B: Watch` AND a linked spawn on both sides. Use ONE `Watch`-linked-spawned `Peer` actor as both watch-target and link-peer to keep the script single-target where possible; a separate plain `Target` for the watch/unwatch steps is fine if simpler).
    - `Scripted`: the watcher. `Watch` impl records `Notice` events (Continue). Its ONE handler runs the whole script against its handler `actor_ref`, pushing `TraceEvent`s into an `Arc<Mutex<Vec<TraceEvent>>>`, gated by `entered`/`release` oneshots where the test must interleave target death.

1d. Script (graceful pair), inside the handler:
    `is_alive` → `tell` self (a second no-op message variant or a bool flag so the second delivery records nothing extra — simplest: `TellSelf` records send success only; the second handler invocation must be a no-op recorded ONCE in the trace as delivered) → `tell` peer → `ask` peer → `watch` target → `unwatch` target → `watch` target again (so a notice WILL arrive) → `link` peer → signal `entered`, park on `release`.
    Test body per mode: build actors, enqueue script message (drain: before `spawn_linked_task`, no external ref; steady: after, external ref held), await `entered`, kill/drop the target per script, join target, `release`, drive watcher to stop (steady: drop external ref timing must NOT differ observably — the script message ends with `stop = true` after the park? NO: notices must be drained after the handler returns, and `stop = true` breaks the loop BEFORE the link arm is polled (`handle_mailbox_step` returns Break directly). The handler must NOT set stop; the watcher ends by: drain mode → `Collected` after backlog empties; steady mode → test drops the external ref after collecting notices... that makes `Finished` reasons EQUAL (`Collected` both) but the drain mode's collection happens immediately while steady waits on the test — acceptable and deterministic. Both end `Collected`; `Finished(Collected)` asserted equal.)
    - The peer/target death used for the notice: graceful pair drops the last target ref → `Notice { Target, Collected, false }`; kill pair (1e) uses `target_ref.kill()` → `Notice { Target, Killed, false }` (bullet 3, inside the oracle).
    - IMPORTANT hang trap: the watcher state holds target/peer refs — `.take()` and drop them inside the handler after use (the `DrainWatcher` Option pattern), and drop the `RunResult` before joining pinned actors.

1e. Two paired tests: `graceful_script_trace_equal_steady_vs_drain` and
    `kill_script_trace_equal_steady_vs_drain` (same `run_script`, death mode is part of
    the SCRIPT (shared), not the mode). Each: `assert_eq!(steady, drain)` PLUS one
    explicit exact assertion on the notice event in the drain trace
    (`Notice { who: Role::Target, reason: ReasonKind::Killed, linked: false }` for the
    kill pair) so the test is self-documenting and cannot pass on two equally-wrong
    traces missing the notice entirely — also `assert!(trace.contains(...))` is banned;
    use position or exact expected full-trace `Vec` literal if deterministic (preferred:
    build the EXPECTED full trace as a `vec![...]` literal and assert both runs equal
    IT — strongest anti-gaming form; do this if the script is fully deterministic,
    which the gate choreography makes it).

### Step 2 — same file: dedicated adversarial tests

SEQUENTIAL — depends on step 1 (same file, reuses fixtures/types).

2a. `drain_window_link_installs_both_edges_watcher_side` — watcher (drain window, no
    external ref) links peer in handler; peer killed while watcher parked; watcher's
    recording hook gets `Notice { Peer, Killed, linked: true }`. Exact single notice.

2b. `drain_window_link_installs_both_edges_peer_side` — the reverse edge: after the
    drain-window `link`, the watcher stops normally (drain to `Collected` after
    handler); the PEER's recording hook receives `Notice { watcher-role, Collected, linked: true }`.
    This is the edge registered onto the watcher's OWN control lane mid-drain
    (`link` = `self.register_on(peer, true)` + `peer.register_on(self, true)`,
    `actor_ref.rs:254-262`) — it only fires if the watcher's loop applied the queued
    control signal. Assert exactly one notice, exact fields.

2c. `drain_window_link_propagates_peer_panic_as_link_died` (bullet 4) — DEFAULT `Watch`
    hook on the watcher (no override). Two messages queued (drain window): handler 1
    links peer + drops refs + signals; handler 2 parks on release. Test: tell peer a
    panicking message → peer dies `Panicked` → release → handler 2 returns WITHOUT
    stop → biased link arm delivers → default hook Breaks → watcher's `RunResult` is
    `Stopped { reason: ActorStopReason::LinkDied { id, reason: inner }, .. }` with
    `id == peer_id` and `inner` matching `Panicked(_)`. Exact variant via
    `assert!(matches!(...))` binding both fields.

2d. `per_message_mints_register_independent_duplicate_edges` (bullet 5) — two queued
    messages; each handler invocation watches the SAME target from its OWN mint
    (first mint dies when handler 1 returns; handler 2 forces a fresh mint — assert
    prerequisite: no external ref, enqueue both before run). Handler 2 drops the target
    ref and parks; target dies `Collected`; release; handler 2 returns (no stop);
    biased link arm drains BOTH notices (recording hook, Continue); loop then breaks
    `Collected`. Assert `notices.len() == 2` exactly, both
    `(Target, Collected, false)`, and watcher `Finished(Collected)`. A deduplicating
    mint fails at the count.

2e. `late_notice_after_break_decision_is_dropped_by_design` (bullet 6, decision 1) —
    watcher (drain window) watches target in its only handler, drops target ref,
    returns (no park, no stop). Backlog empties → loop breaks `Collected`. The
    watcher's `on_stop` (implement `Actor::on_stop` on the fixture) signals
    `stopping` and parks on `stop_release` (bounded; use a generous
    `SpawnConfig::on_stop_grace` like 60s so the park never trips the grace).
    Test: await `stopping` (proof the break decision happened) → drop target ref →
    join target (`Collected`, its guard try_sends the notice onto the watcher's
    still-open link channel) → `stop_release` → join watcher. Assert: recorded
    notices EMPTY, watcher `RunResult::Stopped { reason: Collected, .. }`, watch
    result recorded `Ok(())`. GREEN pin; comment explains the designed-lost decision
    and cites card #266 + Erlang DOWN-to-dead-process parity.

2f. Divergence pins (decision 2): `drain_window_pipe_result_dropped_by_design` and
    `drain_window_timer_message_dropped_by_design` — handler (drain window) calls
    `pipe_to_self` (ready future) / `send_after(tiny)`, records invocation count;
    backlog empties → `Collected` with count == 1 (the piped/timer message never
    arrives: weak upgrade fails, `pipe.rs:104`). First CHECK whether steady-state
    delivery for pipe/timer is already covered by `pipe.rs`/`timer.rs` unit tests —
    if yes, do NOT duplicate steady siblings; the drain pins alone document the
    divergence (cite the existing steady tests by name in the doc comment).
    Timing discipline: bound the "never arrives" side by joining the `Collected`
    RunResult, not by sleeping.

### Step 3 — `crates/core/tests/dst_races.rs`: seeded race leg (bullet 7)

PARALLEL OK with step 4/5 (disjoint files). Depends on step 1 conceptually (copy the
small `ReasonKind`/canonicalization helpers — integration tests cannot share code
across files without a common module; duplicate the ~30 lines locally or add
`crates/core/tests/common/mod.rs` ONLY if dst_races already has one — it does not, so
duplicate locally and keep it minimal).

`drain_window_watch_races_target_death_and_close_equivalence` — for
`seed in [0xDEAD_BEEF, 42, 7, 0xBAD_C0FFE]` (match file convention): an LCG (same
pattern as `cyclic_topology_never_deadlocks`, `dst_races.rs:797`) derives knobs:
number of queued watch-messages (1..=3), target death mode (drop vs kill), death
injection point (before release / after release), extra `tokio::task::yield_now`
counts. Run the SAME knob-set in steady and drain mode; canonicalize outcomes into
(sorted notice multiset by (role, reason-kind, linked), watch-result list, final
`ReasonKind`); `assert_eq!` per seed with the seed in the panic message. Every await
bounded by `terminate_bound()`. NOTE: notice ORDER relative to other events may race —
the multiset canonicalization is the oracle, exact counts still asserted (N queued
messages → N duplicate edges → N notices when death mode delivers).

### Step 4 — walking-skeleton extension (CLAUDE.md rule)

PARALLEL OK (disjoint files: `crates/core/tests/app_job_queue.rs`, maybe
`crates/core/examples/job_queue/app.rs`).

Extend the job-queue integration test with a drain-window watch use: a short-lived
"auditor" actor spawned with its job message enqueued BEFORE run and no external ref
held (drain window), whose handler `watch`es the dispatcher and records the
dispatcher's death via its recording hook; drive the app to shutdown; assert the
auditor observed the dispatcher's exact stop reason. CRITICAL choreography: a
drain-window actor with an empty backlog self-collects (`Closed → Collected`)
immediately after its handler returns — the auditor's handler MUST park on a
`release` oneshot gate (the DrainWatcher pattern) until the test has observed the
dispatcher's death, then be released so the queued notice is drained before the
auditor's own `Collected` stop. Reuse the app's existing
observed-death plumbing (`poll_observed_death`, `app_job_queue.rs:71`) as the model.
Keep it ONE new test fn; do not restructure the app. If `app.rs` genuinely cannot
host this without restructuring, STOP and report blocked — deferral is Claude's call,
not yours.

### Step 5 — doc note (decision 1)

PARALLEL OK (disjoint file: `crates/core/src/actor/mod.rs`).

On `Watch::on_link_died` (trait at `mod.rs:131`, method at `mod.rs:145`): add a short paragraph —
death notices arriving after the loop has taken its stop decision (e.g. a target dying
after the mailbox-closed `Collected` break) are dropped by design; a stopping actor
observes nothing further (Erlang parity: DOWN to a dead process). Cite card #266.
Doc-only; no signature changes; no README change (no public API change).

### Step 6 — coverage baseline

SEQUENTIAL — after steps 1-4 (needs final test names).

Update `docs/testing/coverage-baseline.md` per the CLAUDE.md rule (tests moved, no API
change): add the new file + one-line description of the invariant cluster.

## Verification (sandbox rule: NO cargo test / nextest — they hang in this sandbox)

Per step and final:

```
cargo check -p bombay --tests
cargo clippy -p bombay --tests
```

Tests themselves run in `nix flake check` (unsandboxed), driven by Claude after
handback. Remember: new files are invisible to flake checks until `git add` — Claude
handles staging; you do NOT run git commands.

## Out of scope

- NO production-code changes except the Step-5 doc comment. If a test exposes a real
  behavioral divergence (oracle failure), STOP, report `blocked` with the failing
  scenario — that is a bug discovery, and the fix is a separate card.
- No changes to `spawn.rs`/`kind.rs`/`actor_ref.rs`/`watch.rs` code.
- No `#[allow]`s without `reason`; no lint-level changes; no `clippy.toml` edits.
- No mutants-baseline edits (test-only changes; no new production fns).
- No commits, no README edits, no card/board updates.
- Supervise-from-drain-window equivalence: deferred (follow-up card, Claude files it).
