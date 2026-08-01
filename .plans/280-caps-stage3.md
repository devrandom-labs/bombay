# 280 — caps stage 3: Watching/Supervising caps + one-spawn collapse

## Context

Stage 3 of ADR-0026 (#277), on top of shipped stage 1 (#278/PR #284) and
stage 2 (#279/PR #285). Branch: `core/280-caps-stage3-watching-supervising`
off `a7ddd24` (origin/main).

**Joel's decision (2026-08-01, overrides one card bullet): ONE DOOR.** The
card's "equivalence suites re-run UNCHANGED / if they need editing, stop"
bullet conflicts with its own "remove Watch/Supervisor" headline (the suites
are external tests that `impl Watch`/`impl Supervisor` against the public
floor — verified). Joel rejected keeping the traits as a parallel public
tier ("tech debt, two ways of doing the same shit"). Resolution: the five
traits are DELETED; the suites are PORTED to the caps surface,
**semantics-preserving** — same choreography, same trace-event assertions.
The `PreparedActor` floor STAYS (card + ADR-0026 constraint 4) but is
re-bounded on sealed internal seams. Record this deviation in the PR body
and as an ADR-0026 amendment note.

**The loop-selection mechanism is SPIKE-PROVEN** (ADR-0026 open risk ii):
`scratchpad/spike-280` — GREEN_EXIT=0; R1 (Supervising-without-Watching)
and R3 (watch verb on plain handle) fail to compile as required
(exit 101 each). Mechanism summary:

- `SelectRunner<A: caps::Actor> { type Runner; }` implemented per cap-set
  type (derive-emitted, like stage-2 `Replay`; core impl for `()` →
  `PlainRun`). The assoc type is deliberately UNBOUNDED; the obligation
  `Runner: RunKind<A>` sits on the ONE `caps::spawn`, discharging at the
  concrete actor — this is what lets derive-emitted impls stay generic
  over `A`.
- Markers `PlainRun`/`LinkedRun`/`SupervisedRun` each `impl RunKind<A>`
  (with `where A::Caps: HasWatching<A>` / `HasSupervising<A>` on the
  linked/supervised ones) calling the matching `PreparedActor` floor path.
  Monomorphized; no runtime branch.
- Sealed traits `LinkReact: actor::Actor` (method `on_link_died`, same sig
  as today's hook) and `SupervisedReact: LinkReact` (assoc `fn strategy()`)
  replace `Watch`/`Supervisor` at every loop/floor/verb bound site.
  `Shell<A>` implements them conditionally
  (`where A::Caps: HasWatching<A>` / `HasSupervising<A>`) and is the only
  out-of-module implementor (sealed); in-crate test types may implement
  them directly (sealing binds only foreign crates).
- `HasWatching<A> { type Policy: WatchPolicy<A>; }` — associated type, no
  free `WP` param anywhere (E0207 dodged). Derive emits
  `impl<A: caps::Actor> HasWatching<A> for S where P: WatchPolicy<A>`;
  proven to resolve for both a generic policy (`OtpPropagation`) and a
  concrete per-actor recording policy (what ported suites need).
- `HasSupervising<A>: HasWatching<A>` — the supertrait IS the
  "Supervising requires Watching" law (compile error at spawn otherwise).
- `WatchPolicy<A> { fn on_link_died(actor: &mut A, id, reason, linked) ->
  Result<ControlFlow<ActorStopReason>, A::Error> }` (async). Ship
  `OtpPropagation` with byte-identical semantics to today's
  `Watch::on_link_died` default (`actor/mod.rs:170`): linked && abnormal →
  Break(LinkDied), else Continue.
- `Watching<WP>` zero-state (PhantomData; watchers set stays loop-owned).
  `Supervising<SS: StrategySel>` strategy-as-type via
  `StrategySel { const STRATEGY: SupervisionStrategy; }` + marker structs
  `caps::OneForOne`/`RestForOne`/`OneForAll` (no namespace clash with the
  `restart::SupervisionStrategy` enum variants — spike-proven). NO default
  strategy (required-by-construction; shipped OneForOne default dropped).

## Verified touchpoint audit (exhaustive rg, 2026-08-01)

Behavioral dispatch (2): `kind.rs:353` (`state.on_link_died(...)` — call
syntax unchanged, trait behind it changes), `kind.rs:418`
(`A::supervision_strategy()` → `A::strategy()`).

Bound sites (~9): `kind.rs:299` (`run_linked_message_loop<A: Watch>`),
`kind.rs:340` (`handle_link_died<A: Watch>`), `kind.rs:394`
(`run_supervised_message_loop<A: Supervisor>`), `kind.rs:513`
(`dispatch_death<A: Supervisor>`); `spawn.rs:207` (`impl<A: Watch>
PreparedActor`), `spawn.rs:278` (`impl<A: Supervisor> PreparedActor`),
`spawn.rs:581` (`run_lifecycle_linked<A: Watch>`), `spawn.rs:687`
(`run_lifecycle_supervised<A: Supervisor>`); `actor_ref.rs:202`
(`impl<A: Watch> ActorRef<A>` — watch/unwatch/link/unlink), `:254`
(`link<B: Watch>`), `:333` (`impl<S: Supervisor> ActorRef<S>` —
supervise/unsupervise/stop_child).

Deleted (all in `actor/mod.rs`): `Watch` (:151), `Spawn` (:197),
`SpawnLinked` (:223), `Supervisor` (:251), `SpawnSupervised` (:271) + their
blanket impls + `watch_trait_tests`/`supervisor_trait_tests` (OTP-default
test MOVES to an `OtpPropagation` test; strategy-default test dies with the
default). NOTE: `mod.rs:29` currently re-exports `Spawn` etc.? — verify the
`pub use` list and `lib.rs` prelude during S3; every removed re-export is a
README "public API changed" item.

External impl'ers to port (whole-tree rg): `tests/drain_equivalence.rs`,
`tests/drain_supervision_equivalence.rs`, `tests/caps_stashing.rs`,
`tests/control_lane.rs`, `tests/app_job_queue.rs`, `tests/dst_races.rs`,
`tests/tracing_capture.rs`, `examples/job_queue/app.rs` (+ `main.rs` doc
text). In-module test impl'ers: `spawn.rs` (many, lines ~909–6479),
`kind.rs` back half (lines 1307–2531 — **NOT yet read; read fully before
touching**), `caps.rs` tests, `mod.rs` tests.

Known consumer not in tree: fuzz workspace has its own Cargo.lock
([[registry-119-shipped]] gotcha: bombay dep changes may need
`fuzz/Cargo.lock` refresh or the flake breaks). Check `fuzz/` for uses of
the deleted traits too.

## Steps (TDD: red test first per step; sandbox verification =
`cargo check -p bombay` + `cargo clippy` via `nix develop --command`; NO
cargo test in sandbox — tests run in the foreground `nix flake check` gate)

1. **caps.rs machinery** (additive except the `Actor::Caps` bound change):
   `WatchPolicy`, `OtpPropagation`, `Watching<WP>`, `StrategySel` + three
   markers, `Supervising<SS>`, `HasWatching`, `HasSupervising`,
   `SelectRunner` (+ `()` impl), `RunKind` + three marker types, sealed
   `LinkReact`/`SupervisedReact` (live in `actor/` since the loops bound on
   them; `Shell` impls in caps.rs), conditional `Shell` impls, new
   `caps::spawn`/`spawn_with` bodies (RunKind dispatch; delete
   `RuntimeSpawn` usage). Extend `type Caps` bound with
   `+ SelectRunner<Self>`; update the hand-written cap sets in existing
   caps tests. Tests first: OtpPropagation 3-case behavior (port of
   `default_hook_breaks_on_linked_abnormal_and_continues_otherwise`),
   StrategySel consts, compile_fail doctests for R1/R3 (must EXECUTE in the
   gate — #170 rule: verify the compile_fail count went up per package).
2. **derive(Provide) extension** (bombay_macros): string-match field types
   `Watching<WP>` → emit `HasWatching` (+ where-clause `WP: WatchPolicy<A>`),
   `Supervising<SS>` → emit `HasSupervising`, and ALWAYS emit `SelectRunner`
   (Supervising present → `SupervisedRun`, else Watching → `LinkedRun`,
   else → `PlainRun`). Same alias caveat as stage 2 (final path segment
   string-match; aliases not recognized — document). Macro tests + doctests
   mirroring stage-2 derive tests.
3. **The collapse**: re-bound the 9 sites (Watch→LinkReact,
   Supervisor→SupervisedReact), re-point `kind.rs:418` to `A::strategy()`,
   delete the five traits + blanket impls + their mod.rs tests; fix
   in-module tests (loop unit tests: implement `LinkReact` directly on test
   types in-crate — real SUT, minimal churn; OTP-behavior tests use
   `OtpPropagation` via Shell). Read `kind.rs:1307-2531` FIRST.
4. **Port external suites + example** (semantics-preserving: choreography
   and trace assertions unchanged; actors become caps actors; recording
   hooks become per-actor `WatchPolicy` impls writing through `&mut A`;
   floor driving becomes `PreparedActor::<Shell<X>>::…`). Add the
   cross-cap composition test: (Stashing, Watching, Supervising) deferring
   supervisor — stash during a child's backoff window, unstash on rebuild,
   assert replay order + restart both observed. Extend job-queue app +
   `app_job_queue.rs` per the walking-skeleton rule (Overseer/Dispatcher
   become caps actors; marker impls deleted).
5. **Close-out**: mutants baseline entries for every new/renamed fn
   ([[mutants-baseline-workflow]]); README public-API section (traits
   removed, caps added — this is the main "public API changed" case);
   `docs/testing/coverage-baseline.md`; ADR-0026 amendment note (one-door
   deviation + spike result); preserve spike-280 on the `spikes/277` branch
   (risk iii); `cargo fmt`; commit; **foreground** `nix flake check`
   (never `run_in_background` — masked-exit gotcha #278/#279; untracked
   files invisible — `git add` first).

## Verification

- `nix develop --command cargo check -p bombay` + `clippy` per step.
- Final: `git add -A` then foreground `nix flake check` (all 10 checks).
- Ported oracle suites green with UNCHANGED choreography/assertions — any
  needed semantic edit means the collapse changed behavior: STOP, report.
- compile_fail doctest count increased and EXECUTING per package (#170).

## Progress (2026-08-01, mid-execution)

DONE (each `cargo check -p bombay --test <name>` green at the time):
- Step 1+2 complete: caps.rs machinery (WatchPolicy/OtpPropagation/Watching/
  Strategy+markers/Supervising/HasWatching/HasSupervising/SelectRunner/
  markers/RunKind/new spawn+spawn_with with `Runner: RunKind` where-clause;
  sealed impls for Shell) + behavior tests in caps.rs
  `watching_supervising` mod (TDD red→green); derive extension
  (HasWatching/HasSupervising/SelectRunner emission + supervising-without-
  watching syn error; 27/27 macro tests green — macro tests RUN fine in
  sandbox); `+ SelectRunner<Self>` bound live; caps_stage1.rs hand set
  updated. Clippy clean both crates (9 doc-paragraph lints fixed;
  too_many_lines fixed by extracting `emit_watch_supervise`).
- Step 3 complete: 5 traits deleted from actor/mod.rs (replaced by sealed
  LinkReact/SupervisedReact defined there; `#[cfg(test)] pub(crate) use
  spawn::test_verbs;` re-export); kind.rs bounds+strategy()+docs; spawn.rs
  floor blocks+lifecycle fns re-bounded + `test_verbs` module (TestSpawn/
  TestSpawnLinked/TestSpawnSupervised + otp_link_react!/one_for_one_react!/
  sealed_stamp! macros) + all ~27 in-module test impls swapped; actor_ref.rs
  verb blocks re-gated (watch/link → LinkReact, supervise → SupervisedReact),
  supervise_cloned DELETED (zero callers); pipe/timer/request test mods use
  test_verbs. LIB COMPILES CLEAN, zero warnings.
- Step 4 partial — ported & green: caps_stashing.rs (Sup → caps),
  control_lane.rs (Sup → caps + spawn_plain helper), dst_races.rs
  (storm_supervisor! macro → caps sets w/ strategy markers; link_unlink_storm
  generic re-bounded `S: caps::Actor + Shell<S>: SupervisedReact + Runner:
  RunKind`; TapingWatcher/RaceWatcher → caps actors w/ TapingPolicy/
  RacePolicy; spawn helpers), tracing_capture.rs (Watcher/Sup → caps,
  spawn_plain helper, floor → Shell<Sup>).

REMAINING:
- Port `examples/job_queue/app.rs` (marker impls at ~296/297 + imports line
  21 + SpawnSupervised usage) + `tests/app_job_queue.rs` (imports line 24,
  site ~495) — walking-skeleton bullet: extend app+test with a stage-3
  feature (Overseer as caps supervisor).
- Port `tests/drain_equivalence.rs` (~5 watcher actors w/ custom hooks →
  per-actor policies; floor → Shell<...>; ~1356 lines) and
  `tests/drain_supervision_equivalence.rs` (SupScript → caps supervisor;
  uses handler-context `actor_ref.supervise` → `cx.self_ref().supervise`).
  SEMANTICS-PRESERVING: same choreography + trace assertions; semantic edit
  needed ⇒ STOP.
- Cross-cap composition test (deferring supervisor) — new
  tests/caps_stage3.rs or into caps_stashing.
- R3 compile_fail doctest (watch verb on plain handle) — R1 doctest already
  on caps::spawn; verify compile_fail doctests EXECUTE per package (#170).
- `cargo fmt`; fix pre-existing-looking warnings only if mine; mutants
  baseline true-up (new fns: caps::{spawn,spawn_with,RunKind impls,
  OtpPropagation::on_link_died, Shell::{on_link_died,strategy}, Stashing…
  markers; renamed: none); README public-API update (traits/verbs gone,
  caps::Watching/Supervising/policies/markers new, supervise_cloned gone);
  docs/testing/coverage-baseline.md; ADR-0026 amendment note (one-door);
  copy spike-280 from scratchpad onto spikes/277 branch; `git add -A`;
  commit; FOREGROUND `nix flake check`. fuzz/ dir: check for uses of deleted
  traits + Cargo.lock refresh need.

## Out of scope

- `Phased`/`Deadlined` (stage 4, #281) and delivery-error consolidation
  (stage 5, #282).
- Actor-builder ergonomics ([[actor-builder-akka-message-identity]] —
  after the arc).
- Any change to loop INTERNALS (teardown, biased arms, cycle coordinator,
  Children/DelayQueue) beyond the bound/dispatch re-pointing.
- `restart.rs` / per-child `RestartConfig` path (card: unchanged).
