# Open design ledger

This ledger records the current Bombay dependency graph, selected contracts,
feature verification, and the disposition of earlier work. Entries for
removed packages are historical evidence, not current API guidance.

## Final audit closure (2026-10-02)

The 45-row [canonical queue](todo.md#canonical-execution-queue) is terminal:
25 `TEST-*` rows are `verified`, 19 `ARC-*` rows are `distilled`, and ARC-009
is `retained` by direct-composition evidence. No row is ready, active, blocked,
or feature-complete; no detailed checkbox is open. The 67 reciprocal
dependency edges have no missing ID, cycle, or unsatisfied prerequisite.

The final merged-tree `nix flake check path:. --max-jobs 1 --cores 2` passed
all 21 `aarch64-darwin` checks, including release workspace tests, strict
Clippy, docs, coverage, Loom, panic boundaries, and the public examples. The
independent full mutation package passed its baseline gate on the identical
runtime/Engine/macro source: 870 candidates, 342 caught, 524 unviable, four
classified equivalent misses, and zero timeouts. Bounded Driver and Observe
ASan fuzz runs, four-seed Observe Miri, and the retained performance package
passed at the preceding source checkpoint. The final source and public API
dispositions are in the [API audit](public-api-audit.md) and the row review
below. Publication status is a separate release dependency, recorded in the
publication manifest section; it does not reopen a canonical architecture or
test row.

## Historical recovery checkpoint (2026-10-01)

- Active user goal: finish every item in `docs/todo.md`, prove each result
  against its prior representation, distill the architecture, and pass all
  required repository gates. The 45-row queue has 19 pending rows:
  all are architecture rows awaiting final distillation; TEST-020 and TEST-022
  are verified.
  Do not mark the goal complete or narrow its scope.
- Preserve the existing dirty worktree. The user explicitly authorized
  continuing production edits above the repository's 15-path/500-line
  checkpoint in this tree. The latest measured complete tree before the
  TEST-020 resolution record has 255 changed paths, including 94 untracked;
  its physical deltas are below.
- Selected owners: Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, the pinned Timers Git revision, and Bombay-private
  Observe. The selected Behavior revision's complete `AGENTS.md` was read.
- ARC-006 implementation now uses invisible Address reservation before
  initialization interpretation, private child binding before interpretation,
  public publication only after accepted continuing source settlement, exact
  unpublished terminal custody, and exact pure-initialization panic custody.
  The prior claim-before-commit order fails the three TEST-023 regressions;
  those regressions and the Engine/child panic tests passed focused debug and
  optimized runs before the latest reservation-conflict regression was added.
- The host execution fault cleared. The reservation rejection and nested
  creation regressions passed in debug and optimized builds. The complete
  locked workspace tests, build, formatting check, strict Clippy, and diff
  check passed. ARC-006, ARC-001, ARC-002, ARC-010, and ARC-012 are
  feature-complete. ARC-010 now uses the selected published 0.20.0
  supervisor and source-preparation contracts.
  ARC-011 has since made both cancellation regressions executable and passing,
  and moved activation-task settlement into the spawned actor task. Its
  pre-fix failures and debug/release results are recorded below.

## Selected contracts

### Bombay publication manifest blocker (2026-10-02)

- After merging the already released `main` manifests, `Cargo.lock` still
  selects Behavior Core/Actors 0.20.0, Macros 0.13.0, Address 0.3.0,
  Communication 0.1.2, and the Timers git patch. The prior release job
  published `bombay-engine` 0.2.0 and `bombay-macros` 0.1.0, then failed to
  publish `bombay-rs` 0.1.1 because `bombay-machine` had a path-only
  dependency. ARC-004 has removed that crate and dependency.
- The smallest remaining blocker is the exact pinned-Nix
  `cargo package --locked -p bombay-rs --allow-dirty` command: Cargo rejects
  another path-only dependency, `bombay-macros`, before assembling the
  archive. Its published 0.1.0 version is known from the registry. The
  pre-edit failure is the inversion; the same command must advance past
  manifest validation after the correction.
- Change ledger before the manifest edit: touch `crates/bombay/Cargo.toml`,
  this ledger, and `docs/todo.md`; production source `+0/-0/net 0`, test
  source `+0/-0/net 0`, public types `+0/-0`. Reuse the existing macro crate
  and its published version by adding an explicit version requirement beside
  the local path. No new dependency, runtime abstraction, or API is added.
- Publication still needs a coordinated versioned release of the changed
  Engine, Macros, and runtime packages. A package dry run may expose the next
  exact release dependency after this manifest blocker is removed.
- Resolution: the macro path now has `version = "0.1.0"`. Repeating the exact
  package command advanced through manifest validation and produced a 198-file
  `bombay-rs` 0.1.1 archive. Tarball verification then failed while compiling
  against the already published `bombay-engine` 0.2.0 and `bombay-macros`
  0.1.0: the registry Engine still owns the older Behavior 0.14 contract,
  producing 220 compile errors against the new Behavior 0.20 runtime. This
  establishes the next release dependency: publish new, coordinated Engine
  and Macros versions before a matching `bombay-rs` archive can verify. The
  full local workspace test passed after the `main` merge, before the macro
  manifest correction; the final flake gate will cover that correction.
- Release sequencing blocker before workflow edit: the existing `Publish`
  workflow runs on every `main` source push, independently of the release-PR
  workflow. The 2026-09-17 run 35268085163 is the exact failing regression:
  it published Engine/Macros, then attempted an unreleasable runtime package
  before its dependency correction. On this branch the newly changed Engine,
  Macros, and runtime require one coordinated release PR; a normal feature
  merge must create that PR without prematurely invoking `release-plz
  release`. Expected files: `.github/workflows/release.yml`, this ledger,
  and `docs/todo.md`; production/tests `+0/-0/net 0`, public types `+0/-0`.
  Reuse the existing `release-pr.yml` merged-PR detection. The `Publish` job
  should run only for a merged `release-plz-*` PR (or deliberate manual
  dispatch), so its existing package order and registry credentials remain
  unchanged. Verify the condition against both an ordinary feature merge
  and a release PR merge using the GitHub commit-to-PR API before retention.
- Resolution: `release.yml` now reuses that exact merged-PR predicate before
  its publish step. The GitHub commit-to-PR API returned `true` for release
  merge `3e54328` and `false` for ordinary CI merge `1881950`, matching the
  intended gate; `git diff --check` passed. The workflow keeps manual
  dispatch explicit and does not alter release-plz's package order or
  credentials. This edit contributes production/tests/public API zero.
- Registry dependency audit: the sole Timers patch selects upstream commit
  `13e884d`, which follows the published `bombay-timers-v0.1.0` tag at
  `9ddb112`. The published 0.1.0 archive still has an infallible `schedule`
  that panics on counter exhaustion; the selected revision returns a typed
  `ScheduleError` with the complete rejected input. Bombay's runtime uses
  the selected contract, so a publishable Bombay archive requires a new
  Timers release and a corresponding dependency version before coordinated
  Engine/Macros/runtime publication. The registry currently has 0.1.0 only.
  This is a separate upstream release dependency, not a remaining canonical
  architecture or test queue row.

### ARC-006 terminal activation-status documentation (2026-10-02)

- State: `feature-complete` after a documentation correction in the terminal audit. The
  selected lock/patch remains Core/Actors 0.20.0 at `804b2bf`, Macros 0.13.0
  at `3f08364`, Address 0.3.0, Communication 0.1.2, private Observe, and
  Timers at `13e884da7ab41781f52337b0038060e375b00ee0`. Behavior owns
  fold/settlement custody, Address owns invisible reservation/public lease,
  Engine owns the affine activation port, and Bombay owns commit/publish
  order. The complete selected Behavior instructions, ARC-006 source/tests,
  current module/capability guide, and planning inventory were inspected.
- Exact blocker and inversion: current-facing paragraphs in
  `prd-backlog/local-runtime.md` and `prd-backlog/evidence.md` still say the
  three visibility regressions are ignored and activation order unresolved.
  Selected `local.rs` has ordinary tests for precommit invisibility, accepted
  publication, and rejected/corrupt invisibility; ARC-006 records their
  original-order failure and debug/release pass. The pre-edit claims fail
  against source and test results.
- Expected files: those two planning notes, this ledger, and `docs/todo.md`.
  Expected production/tests `+0/-0/net 0`, documentation roughly
  `+6/-3/net +3`, public types `+0/-0`. Update only the current status note;
  preserve original ACT acceptance rows as dated requirements. No activation
  implementation or extra regression is required for this wording correction.
- Resolution: the local requirements page now records ARC-006's selected
  invisible-reservation order and scopes its ACT rows to the earlier snapshot.
  The evidence table names the executed regressions and original-order
  failure. A source search finds no remaining current claim that those
  regressions are ignored; `git diff --check` passes. The locked workspace
  suite ran the ordinary tests. No executable source or public type changed:
  production/tests `+0/-0/net 0`, public types `+0/-0`. ARC-006 returns to
  `feature-complete` pending project-wide distillation.

### ARC-020 terminal template-evidence documentation (2026-10-02)

- State: `feature-complete` after a current-document audit correction. The exact lock and
  sole Timers patch select Core/Actors 0.20.0 at `804b2bf`, Macros 0.13.0
  at `3f08364`, Address 0.3.0, Communication 0.1.2, private Observe,
  and Timers at `13e884da7ab41781f52337b0038060e375b00ee0`. The
  selected Behavior instructions, 45-template/19-capability manifest,
  Actors source/tests, and Bombay live supervisor/FIFO tests were inspected.
  Behavior Actors owns policies; Bombay owns typed interpretation and live
  composition; the PRD research snapshots do not select current contracts.
- Exact blockers and inversions: normative `driver-law.md` attributes Macros
  0.13.0 to the Core/Actors commit; the Macros archive records `3f08364`.
  `prd-backlog/evidence.md` labels five templates "Blocked: missing atomic
  interpretations" although the exact manifest and runtime code show all
  19 requests interpreted; fixed supervision and FIFO have live witnesses,
  while broader dynamic/keyed policy proof remains open. The superseded
  completion navigation page still says Bombay must finish those
  interpretations. The EXEC external-work decision note claims the current
  lock is 0.17 and atomic interpreters are missing, although its experiment
  itself remains open. These are source/manifest inversions; no new runtime
  experiment is required for a documentation correction.
- Expected files: those four guides, this ledger, and `docs/todo.md`.
  Expected production/tests `+0/-0/net 0`, documentation about
  `+14/-11/net +3`, public types `+0/-0`. Correct current evidence levels and
  label the prior EXEC source hashes as a dated snapshot. Preserve all still
  open broader policy and external-work verification obligations; add no
  interpreter, template, or feature promise.
- Resolution: the normative Driver law now names the separate Core/Actors and
  Macros revisions. The template evidence appendix removes five false
  missing-interpreter blockers: fixed supervision, FIFO, and the proxy have
  live witnesses; dynamic/keyed retain explicit broader verification. The
  superseded navigation page reports the current interpreter status, and the
  EXEC external-work note labels its 0.17 hashes as a snapshot while keeping
  the separate bounded-work experiment open. Searches find no remaining
  current assertion that these atomic interpreters are missing, and
  `git diff --check` passes. The pre-edit claims fail the selected lock,
  manifest, and live tests; post-edit claims match those independent sources.
  Documentation only: production/tests `+0/-0/net 0`, public types `+0/-0`.

### Terminal audit checkpoint (2026-10-02)

- ARC-020 final minimization substage, before production edit: the exact
  blocker is four `dead_code` suppressions on Entity ingress, Entity actor
  launch, and fencing despite live production calls through
  `entity/bombay.rs`. The smallest inversion is the source caller inventory:
  each suppressed item is reached by the native Entity runtime; the former
  rationale that typed application topology cannot materialize it is false.
  Remove only those attributes and run the pinned strict all-target Clippy
  gate to prove the compiler accepts their actual callers. Expected files:
  `crates/bombay/src/launch.rs`, `crates/bombay/src/local.rs`, this ledger,
  and `docs/todo.md`; expected production source `+0/-16/net -16`, tests
  `+0/-0/net 0`, public types `+0/-0`. Reuse the existing
  `spawn_owned_entity_with`, `EntityIngress`, `LocalIngress`, `ActorRef::fence`,
  and native Entity adapter; add no wrapper, interpreter, or product. The
  private Observe module allowance has a separately measured 26-diagnostic
  need and is outside this removal.
- The normative Driver law's final verification checklist still called its
  post-audit status `feature-complete`; the canonical queue reserves
  `distilled` for that state. Its wording now names project-wide
  distillation, without changing a Driver law or executable source.
- Resolution: all four stale suppressions were removed from the existing
  Entity launch and ingress paths (`+0/-16/net -16` production source lines,
  no test or public-type changes). The native Entity adapter's real callers
  remain, and `nix develop -c cargo clippy --locked --workspace
  --all-targets -- -D warnings` passed on the resulting tree. The prior
  suppressions' “cannot materialize” rationale is disproved by those callers;
  no new production abstraction was introduced. Final suite gates continue.

- Preliminary queue closure at that checkpoint: all 45 rows had a valid
  state, 25 `TEST-*` rows were `verified`, ARC-009 was `retained`, and 19
  `ARC-*` rows awaited distillation. No checkbox was open; no row was active,
  ready, or blocked.
  The 67 dependency edges have no missing ID or cycle and all prerequisites
  satisfy their downstream rows. The canonical queue and this ledger's
  current dependency graph agree.
- Behavior ownership: the exact 0.20.0 Core/Actors archives carry
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`; the separate Macros
  0.13.0 archive carries `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`.
  The selected instructions were read. The pinned-Nix
  `cargo test --locked -p bombay-engine --test law_manifest
  engine_does_not_mirror_actor_template_laws -- --exact` passed. Schema 3
  names all 45 owner templates and 19 actor-owned capability requests;
  every source and evidence path exists in the selected Actors archive; all
  19 request leaves have typed Bombay interpretation. Source searches find
  no Bombay actor contract, second effect algebra, dynamic capability map,
  or Engine Actor-template dependency. `ActorExt` is a static composition
  extension returning owner types; Entity's `SlotEffect` owns distinct
  stable-identity lifecycle semantics.
- Module and suite scan so far: all 50 Rust files under the three runtime,
  Engine, and macro source roots were enumerated against the module boundary
  map, including the separately owned Entity and private Observe trees.
  Public crate roots, compile fixtures, examples, benchmarks, fuzz targets,
  manifests, and current guides were enumerated; obsolete current
  `LocalActors`/`LocalProtocol` spellings are absent except explicit
  supersession prose. The unsafe inventory remains Observe's proved slot
  boundary plus test instrumentation and the test-only allocation counter;
  no production `bool` field encodes semantic phase or policy. The current
  document scan corrected the exact Macros revision and 13/19 snapshot
  conflict. Full suite review and final gates continue.
- Complete worktree checkpoint after those documentation corrections:
  256 changed paths including 94 untracked. Physical diff against HEAD with
  renames expanded: production source `+7631/-5967/net +1664` (includes
  embedded unit tests), test/example/tool source `+7773/-2590/net +5183`,
  documentation `+17466/-1949/net +15517`, other files
  `+971/-177/net +794`. This is net-positive production work, not a code
  reduction claim. Terminal documentation-only stages added no public type;
  cumulative public-surface disposition remains in `public-api-audit.md`.
- Final gate progress on current source: pinned-Nix rustfmt check and strict
  all-target Clippy passed. Full locked workspace tests and independent
  Nix mutation gate are running. Do not promote architecture rows until the
  remaining audit and gates pass.
- Final current-source verification update: `nix flake check path:.
  --max-jobs 1 --cores 2` passed all 21 `aarch64-darwin` checks after the
  Entity allowance and performance-package edits. These include release
  workspace build/test, no-default/all-feature child builds, strict Clippy,
  rustfmt, warning-denied rustdoc, doctests, owner coverage floors, Entity
  and Observe Loom, Driver and actor execution production law evidence,
  unwind/abort boundaries, and all six public-example gates. The ordinary
  `nix develop -c cargo test --locked --workspace` and strict all-target
  Clippy also passed after the source edit. Pinned Miri 0.1.0 on Rust
  1.98.0-nightly passed 12 affine Observe cases across seeds 0–3; pinned
  cargo-fuzz 0.13.2 passed 2,048 Driver ASan runs at seed 20260928 and
  1,024 ASan runs each for four Observe targets at seed 20261001. The
  standalone performance package and the Observe performance harness pass.
  Stable Rust/Cargo are 1.96.0; Nix is 2.33.3. The complete mutation gate
  was still active at that checkpoint, so architecture row promotion was
  pending.
- Completed source-equivalent mutation gate: the independent pinned-Nix
  `packages.aarch64-darwin.mutants` build finished all 870 candidates in
  2h24m with 342 caught, 524 unviable, the four baseline-classified
  equivalent misses, and zero timeouts. The fail-closed `mutants-gate check`
  accepted 346 viable candidates. Its complete `outcomes.json`, four-miss
  list, and per-mutant logs were copied into ignored
  `target/mutation-evidence/mutants.out/` before Nix cleanup. The later
  `main` merge changed manifest versions, release notes/workflows, and setup
  guidance; the macro dependency correction changed only manifest
  packaging. No runtime, Engine, or macro source changed after the mutation
  snapshot. The subsequent merged-tree flake check passed all 21 checks.

#### Architecture row minimization review (final gates passed)

The terminal performance gate found a separate TEST-020 verification defect:
`nix build path:.#packages.aarch64-darwin.performance --print-out-paths
--no-link --max-jobs 1 --cores 2` ran both Entity benchmarks and then failed
because `flake.nix` tried to copy `target/criterion`, which those custom
benches do not create. The smallest failing regression is that exact package
build (exit 1 after the completed measurements). Before the flake edit, the
planned surface is `flake.nix`, this ledger, and `docs/todo.md`; production
`+0/-0/net 0`, benchmark/test source `+0/-0/net 0`, public types `+0/-0`.
Reuse the existing Driver Criterion benchmark to produce the retained
Criterion artifact, run the two Entity benchmarks unchanged, and save their
actual output with pipe failure propagation. No new benchmark or runtime
abstraction is needed.
The corrected `nix build path:.#packages.aarch64-darwin.performance
--print-out-paths --no-link --max-jobs 1 --cores 2` passed and retained
`driver.txt`, `entity-directory.txt`, `entity-lifecycle.txt`, `criterion/`,
and `environment.txt` in
`result-performance/`, a GC-rooted symlink to the completed Nix output.
The Driver Criterion benchmark collected 100 samples; Entity directory and
lifecycle custom runs each completed seven repetitions. The earlier unlinked
output was collected by Nix, so a second identical package build retained
the complete reports under the rooted link. Its timing was measured alongside
other verification jobs and is not used as a regression claim. The benchmark
harness and production code did not change; `flake.nix` now records the
actual three outputs and propagates bench failures through `tee`.
`nix fmt -- --check flake.nix` passed.

The complete source, export, caller, example, fixture, benchmark, fuzz, and
current-guide inventories were compared with each row's inversion in
`docs/todo.md`. The retained public interface and caller proof are enumerated
in `docs/public-api-audit.md`. The final gate accepted these dispositions and
promoted all 19 rows to `distilled` in the canonical queue.

| Row | Minimal remaining owner and distinction from the prior representation |
| --- | --- |
| ARC-001 | Typed shutdown authority is interpreted in Bombay; the old erased shutdown path fails the exact rejection and terminal tests. No second lifecycle policy remains. |
| ARC-002 | Address owns reservation and public lease; an external actor receives only a resolved endpoint. The former unobservable claim was removed, and lease/closure tests exercise the real boundary. |
| ARC-003 | `EntitySlot::decide` is the one pure stable-identity transition; directory installs its decision. The independent oracle and real directory traces reject duplicated classification. |
| ARC-004 | The redundant `bombay-machine` package and its public algebra are absent from workspace metadata; Entity retains only its distinct concrete slot law. The compile denial and absence law pin the boundary. |
| ARC-005 | Workspace manifests name live packages and Syn 3 is Bombay's macro parser; Syn 2 remains only through transitive `zerocopy-derive`. No local compatibility alias remains. |
| ARC-006 | Bombay privately commits initialization through Address reservation before public publication; the three formerly ignored visibility regressions and original-order inversion prove one activation transaction. |
| ARC-007 | Entity owns stable key routing, hydration, passivation, and fenced retirement; Behavior Actors templates own supervision and proxy policy. Differential traces reject substitution. |
| ARC-008 | One child occurrence owns its binding and exact settled terminal; duplicate and reversed-order inversions reject the former parallel stores. Behavior still owns the structural child product. |
| ARC-010 | All 19 selected actor capability requests have one typed Bombay interpreter; Behavior Actors retain all 45 template policies. Fixed/FIFO/proxy live witnesses and the manifest law reject an invented second actor contract. |
| ARC-011 | Bombay's actor task owns launch settlement through retirement; cancellation and parent/child terminal tests reject the earlier split task custody. No second launcher remains. |
| ARC-012 | Publication, owner cancellation, and terminal selection move through owned phases; panic/abort traces reject coordinated optional authority and preserve exact residuals. |
| ARC-013 | Actor-owned timer and observation queues interpret their typed lanes directly. The ordered effect and generation tests reject synchronization that existed solely for split ownership. |
| ARC-014 | One Entity task group owns activation, drain, and shutdown; spawn rejection is explicit. Optimized real-directory Loom and terminal traces reject detached work. |
| ARC-015 | External Entity callers get only stable identity and the installed decision interface; former mechanism exports were removed. Compile boundaries and benchmark callers prove the remaining advanced kernel surface. |
| ARC-016 | The one accepted actor facade delegates to Behavior; macro derives use typed role proofs rather than syntax strings. Renamed-crate, hygiene, and compile-denial fixtures reject spelling inference. |
| ARC-017 | Production unsafe is confined to Observe's slot publication and affine read boundary; test-only raw wakers and allocation instrumentation have separate owners. Miri, Loom, fuzz, and drop-count evidence exercise the invariant. |
| ARC-018 | `RootOrigin` and `ChildOrigin` keep source provenance in disjoint types; wrong-role and root/child compile denials reject the previous nullable nonce state. |
| ARC-019 | Bombay owns ordinary private Observe tests once; the conditional harness owns Loom/fuzz/performance compilation without duplicate ordinary execution. Test listings and coverage distinguish the owners. |
| ARC-020 | Each reachable public type has a caller, construction invariant, trait/error choice, and static denial in the API audit. Two private aliases and four now-obsolete Entity `dead_code` allowances were removed; the remaining Observe allowance has a measured keyed-API need. |

### ARC-020 terminal current-guide reconciliation (2026-10-02)

- State: `feature-complete` after two current-facing documentation corrections. The
  exact lock and sole Timers patch select Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, private Observe, and Timers at
  `13e884da7ab41781f52337b0038060e375b00ee0`. The selected Behavior
  instructions and current owner source/tests govern. The schema-3 template
  manifest's focused pinned-Nix law test passed; direct archive inspection
  found all 45 template and 19 capability source paths and evidence paths,
  and the manifest records 19 implemented interpreters.
- Exact blockers and inversions: `docs/driver-test-strategy.md` attributes
  Macros 0.13.0 to the Core/Actors revision, contrary to the Macros archive.
  The planning snapshot `docs/prd-backlog/README.md` still uses a 13/19
  missing-interpreter count under its current "What already exists" heading,
  contrary to the selected schema-3 manifest and ARC-010. Its inventory rows
  remain historical requirements rather than silently changing their original
  classification. The smallest inversions are those exact selected lock and
  manifest comparisons.
- Expected files: those two guides, this ledger, and `docs/todo.md`; a brief
  provenance line in the planning evidence may be needed to scope its earlier
  todo disposition. Expected production/tests `+0/-0/net 0`, documentation
  roughly `+10/-8/net +2`, public types `+0/-0`. Reuse the current capability
  manifest and ARC-010 record; add no implementation or new requirement.
- Resolution: Driver strategy now names the separate Macros revision. The
  planning guide's current inventory says all 19 named capabilities have typed
  interpreters, while labeling its original six-gap requirements as the dated
  snapshot. Its evidence page explicitly scopes its former `todo.md`
  disposition to that snapshot. The focused pinned-Nix template inventory
  test passed; all 45 template and 19 capability source/evidence paths exist in
  the selected Actors archive, and the old 13/19 claim is absent from current
  guidance. `git diff --check` passed. No executable code changed, so focused
  debug/release runs beyond the already executed inventory test are
  inapplicable. Production `+0/-0/net 0`; tests `+0/-0/net 0`; public types
  `+0/-0`. The final repository gates still apply.

### ARC-020 terminal owner-revision attribution (2026-10-02)

- State: `feature-complete` after one current-contract documentation correction. The exact
  `Cargo.lock` and sole Timers patch select Behavior Core/Actors 0.20.0 from
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 from
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, private Observe, and Timers at
  `13e884da7ab41781f52337b0038060e375b00ee0`. The selected Behavior
  instructions, owner API and tests, and current capability/module guidance
  remain the governing ownership map.
- Exact blocker and inversion: `docs/runtime-capability-interfaces.md` says
  Core, Actors, and Macros all come from the Core/Actors commit. The locked
  Macros archive records the separate `3f08364` revision, and this guide's
  exact-audited dependency table already gives its distinct checksum. The
  current sentence therefore contradicts the selected build contract.
- Expected files: the capability guide, this ledger, and `docs/todo.md`.
  Expected production/tests `+0/-0/net 0`, documentation `+1/-1/net 0`,
  public types `+0/-0`. Correct the attribution in place; reuse the existing
  exact dependency table and add no API or runtime mechanism.
- Resolution: the current guide now attributes Core/Actors 0.20.0 to
  `804b2bf` and Macros 0.13.0 to `3f08364`, matching the lock and its own
  dependency table. The pre-edit sentence was the failed inversion; the
  post-edit sentence has distinct owners. `git diff --check` passed. This
  documentation-only correction requires no focused Rust build; the final
  repository gates will verify the unchanged executable contracts.
  Production `+0/-0/net 0`; tests `+0/-0/net 0`; public types `+0/-0`.

### TEST-022 terminal research-probe provenance (2026-10-02)

- State: `verified`. The exact current
  lock and patch select Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, private Observe, and Timers at
  `13e884da7ab41781f52337b0038060e375b00ee0`. Behavior/Actors own
  proxy diagnostics and preparation/shutdown policy; the two retained
  research probes preserve earlier 0.19 contract failures. The 0.20 owner
  source/tests, Bombay integration evidence, and current module/capability
  documents are recorded under ARC-010 and TEST-025.
- Exact blocker: `behavior-actors-proxy-diagnostic-gap.md` and
  `fifo-preparation-shutdown-gap.md` retain useful 0.19 failure traces, but
  their openings say the old archive is currently selected, and the FIFO
  reproduction says to patch the current tree. The smallest source
  inversion is comparing those claims with `Cargo.lock`'s 0.20 registry
  checksums and the selected 0.20 integration tests; the old reproduction
  cannot be claimed for the current build.
- Expected files: the two probe notes, this ledger, and `docs/todo.md`.
  Expected production/tests `+0/-0/net 0`, documentation roughly
  `+10/-3/net +7`, public types `+0/-0`. Keep the useful dated traces and
  point current readers to the selected ARC-010/TEST-025 record; add no
  compatibility implementation or new probe.
- Resolution: both notes now identify their 0.19.0 snapshot before any
  reproduction steps. The proxy note says its `Cargo.lock` statement applies
  at capture; the FIFO note directs patch application to the historical
  snapshot. Both point readers to current 0.20.0 integration evidence.
  `rg` found no remaining claim that the current working tree selects 0.19.0;
  `git diff --check` passed. No Rust source or public API changed, so debug,
  release, and specialized executable gates are inapplicable to this
  correction. Production `+0/-0/net 0`; tests `+0/-0/net 0`; public types
  `+0/-0`; documentation only. The current full repository audit continues.

### TEST-019 terminal benchmark-description correction (2026-10-02)

- State: `active` for one terminal-audit finding. The selected lock still
  contains Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, Bombay-private Observe, and the Timers patch at
  `13e884da7ab41781f52337b0038060e375b00ee0`. Observe owns the
  retained-key/pooled-slot primitive; `observe-perf` owns only a dedicated
  performance harness. Its exact workload code, metric names, current docs,
  and TEST-019/020 evidence were inspected.
- Exact blocker and smallest inversion: `seq_retire_recreate` iterates
  distinct `key in 0..N`, creating then immediately dropping one subject;
  its doc comment says it repeats the same key. The module header says all
  workloads use disjoint key ranges, while `seq_pool_reuse` deliberately uses
  `0_u64` on every turn. A source/read comparison falsifies both statements;
  no timing experiment is needed to detect incorrect descriptions.
- Expected files: `crates/observe-perf/src/main.rs`, this ledger, and
  `docs/todo.md`. Expected executable production `+0/-0/net 0`, benchmark
  comments roughly `+3/-3/net 0`, public types `+0/-0`. Reuse both existing
  workloads and metric names; add no new measurement or threshold.
- Resolution: the module summary now distinguishes deterministic disjoint-key
  workloads from the intentional same-key pool-reuse workload, and the
  `seq_retire_recreate` call-site/function comments both describe its actual
  distinct-key loop. Source inspection confirms all executable statements,
  metric names, counts, and benchmark thresholds are unchanged; `git diff
  --check` passes. Focused debug/release semantic tests are inapplicable to
  comment-only correction. The terminal benchmark gate still runs both
  measured workloads. Executable production `+0/-0/net 0`, benchmark
  comments `+4/-3/net +1`, public types `+0/-0`. TEST-019 returns to
  `verified`.

### ARC-017 terminal unsafe-proof wording (2026-10-02)

- State: `active` for one terminal-audit finding. The selected lock remains
  Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, patched Timers at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private
  Observe. Its raw-waker test support, safe callers, Miri evidence, selected
  Behavior instructions, and current ARC-017 unsafe inventory were checked.
  This code belongs to Observe test instrumentation; no Behavior fold or
  runtime policy changes.
- Exact blocker: two `count_wake_*` and two `thread_wake_*` safety comments
  claim a `ManuallyDrop` borrow, while their bodies reconstruct an owned
  `Arc` from the raw pointer and call `std::mem::forget` after use. The
  refcount law is correct, but the written proof names a value that does not
  exist. The smallest inversion is the source inspection against those exact
  function bodies; a semantic test cannot distinguish comment text.
- Expected files: `crates/bombay/src/observe/test_support/mod.rs`, this
  ledger, and `docs/todo.md`. Expected runtime production `+0/-0/net 0`,
  test-support comments `+4/-4/net 0`, public types `+0/-0`. Reuse the
  existing static vtables, Arc protocol, bounded Miri results, and final
  terminal Miri gate. Add no unsafe operation, wrapper, or test.
- Resolution: all four comments now name the exact reconstruct/forget step.
  Source inspection shows each clone gains one reference and each borrowed
  wake leaves the existing reference count unchanged. `rg` finds no
  `ManuallyDrop` claim in the module and `git diff --check` passes. No
  executable statement changed, so focused debug/optimized runtime tests
  would mirror the unchanged implementation; the selected four-seed Miri
  receipt above remains valid and terminal verification reruns the pinned
  Miri lane. Runtime production `+0/-0/net 0`, comments `+4/-4/net 0`,
  public types `+0/-0`. ARC-017 returns to `feature-complete`.

### ARC-020 terminal example minimization (2026-10-02)

- State: `active` for one terminal-audit finding; the 19-row distillation pass
  pauses while this owning item is repaired. `Cargo.lock` still selects
  Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, private Observe, and patched Timers at
  `13e884da7ab41781f52337b0038060e375b00ee0`. The selected Behavior
  instructions, `StopOnShutdown` and `ReceiveTimeout` owner source/tests,
  Bombay `ActorExt`, the executable example, and current API/module docs were
  checked. Behavior Actors owns both wrapper policies; Bombay's extension
  methods return those exact concrete types; the example owns the choice to
  compose them.
- Exact blocker: `run_receive_timeout` in
  `examples/actor-templates/src/main.rs` composes `stop_on_shutdown` after
  `with_receive_timeout`, but that run sends only `Work` and `Open` and waits
  for its timeout terminal. Its shutdown policy cannot run. The separate
  `run_shutdown` invokes `request_shutdown` and demonstrates the same wrapper
  law correctly. Upstream `StopOnShutdown` only transforms an owned
  `ShutdownRequested` event; ordinary inner events delegate unchanged.
- Smallest differential: run the existing example under pinned Nix before
  and after deleting that one wrapper call; both runs must observe the same
  normal `Completion::Stopped`, accepted settlements, empty control/user/
  descendant products, and bounded timeout. The pre-edit source search proves
  the first run has no shutdown request; the second run retains its actual
  request. This is an example composition minimization, not a new template
  law or production design.
- Expected touched files: this ledger, `docs/todo.md`, and
  `examples/actor-templates/src/main.rs`. Expected production `+0/-0/net 0`,
  example `+0/-1/net -1`, public types `+0/-0`. Reuse Behavior Actors'
  `ReceiveTimeout`, the existing `Machine` behavior, the existing terminal
  projection, and the separate `StopOnShutdown` run. Add no wrapper,
  interpreter, product, or public API.
- Resolution: the pre-edit example passed
  `nix develop -c cargo run --locked -p bombay-example-actor-templates`.
  Removing only `.stop_on_shutdown()` failed the pinned `cargo check`/`run`
  with E0277: `EventLayer<TimerElapsed, User<MailAddr,
  ProcessorMessage>>` cannot inject the `ShutdownRequested` event required
  by `Application::run_with`'s `LaunchSystem` bound. The selected upstream
  wrapper supplies exactly that typed event lane, even though its shutdown
  transition is exercised in the second run. Restoring the wrapper returns
  the example to the pre-edit source. This is a valid static denial, so the
  proposed deletion is rejected and ARC-020 returns to `feature-complete`.
  Production, examples, and public API each have `+0/-0/net 0` from this
  probe. The terminal audit resumes; no new abstraction or cleanup was added.

### ARC-020 terminal fixture naming (2026-10-02)

- State: `active`. The lock, patch, and selected Behavior instructions remain
  the 0.20.0/0.13.0/Address 0.3.0/Communication 0.1.2/private Observe/Timers
  contracts recorded above. Behavior owns concrete `Protocol`; Address owns
  each `ActorSpace`; Bombay's syntax-only `ActorSpaces` derive emits static
  `Hosts<P>` proofs. The two current actor-space tests and their compile
  fixtures were inspected with the current module and public API guidance.
- Exact blocker: `crates/bombay/tests/actor_spaces.rs` and
  `tests/compile/pass/advanced_runtime_imports.rs` call a product of exact
  `ActorSpace<Orders>`/`ActorSpace<Payments>` fields `LocalActors`. That name
  was also the removed ambient runtime abstraction and hides what these
  fixture values own. The pre-edit source search finds both current uses,
  while the selected design has no `LocalActors` runtime owner.
- Smallest change and proof: rename only the fixture structs and local values
  to names that expose their owned spaces; keep the exact `Hosts<P>` derive
  and pointer-identity assertions. The focused positive and compile-fail
  actor-space matrix must pass unchanged. No compiler architecture or generic
  plumbing is introduced.
- Expected files: those two test files, this ledger, and `docs/todo.md`.
  Expected production `+0/-0/net 0`, tests approximately `+9/-9/net 0`,
  public types `+0/-0`. Reuse the existing Address spaces, Bombay derive,
  and test matrix. No runtime or template code is touched.
- Resolution: the fixture structs now name `OrderPaymentSpaces` and
  `OrderSpaces`; their local values and accessor name also state that they
  own/select spaces. No `LocalActors` spelling remains in those current
  fixtures. `nix develop -c cargo test --locked -p bombay-rs --test
  actor_spaces --test prelude_levels` passed all four tests plus their
  positive/negative compile cases in debug, and the matching `--release`
  command passed. `git diff --check` passed. Stage production
  `+0/-0/net 0`, test fixture replacements are line-neutral, public types
  `+0/-0`. ARC-020 returns to `feature-complete` for terminal distillation.

### TEST-020 feature verification and change ledger (2026-10-01)

- State: `active`; TEST-003/004/005/017/018/024 are terminal, so no unresolved
  dependency blocks this item. `Cargo.lock` selects Behavior Core/Actors 0.20.0
  at `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, Bombay-private Observe, and Timers patch
  `13e884da7ab41781f52337b0038060e375b00ee0`. The complete selected
  Behavior instructions and owner APIs/tests/docs were inspected for the
  prerequisite features; this item rechecked the current Driver, actor outcome,
  Observe fuzz targets, mutation gate/baseline, flake, CI, and Driver strategy.
  Behavior/Actors own folds/templates; Address, Communication, Observe, and
  Timers own primitives; Engine owns causal settlement; Bombay owns actor
  execution and local composition; cargo-mutants owns candidate generation,
  `mutants-gate` owns fail-closed results, and CI owns cadence/artifacts.
- Exact blocker: the full current mutation sweep and coverage report have not
  been reviewed against each owner law. Observe fuzz has four on-demand
  targets but no bounded retained campaign. The pinned Miri shell has no
  recurring job or stored result. Sanitizer applicability is unrecorded.
- First failing regressions: the existing CI workflow has no Observe fuzz or
  Miri step; the coverage derivation emits HTML without a floor; the mutation
  gate has a reviewed baseline but no current complete sweep. Capture those
  pre-change absences and execute the existing source mutation witnesses before
  adding verification machinery. A source change needs its own failing law,
  prior-representation failure, debug/release run, and inversion.
- Expected files: `.github/workflows/checks.yml`, `flake.nix`, bounded Observe
  campaign and seed corpus, `mutants-baseline.json` only after a complete
  sweep, this ledger, `docs/todo.md`, and current verification guidance. Any
  production owner source is conditional on a demonstrated surviving semantic
  mutant or failing law. Expected production delta `+0/-0/net 0` initially;
  test/verification code roughly `+100/-0`; public types `+0/-0`.
- Reuse the existing Driver/actor execution real-source mutation scripts,
  fail-closed `mutants-gate`, Engine fuzz campaign, Observe fuzz targets,
  isolated Observe Loom tests, pinned Miri/fuzz shells, and typed owner tests.
  Add no verification wrapper or public API until a concrete law requires it.

#### TEST-020 checkpoint: owner coverage and bounded campaigns

Mutation obligations follow the semantic owner rather than a workspace-wide
percentage. The complete candidate sweep covers both Engine and Bombay source
packages; every viable candidate must be killed or reviewed as an explicit
equivalence before a baseline can be seeded. A timeout or missed mutation is
never counted as a kill.

| Owner | Required mutation or static inversion | Independent observation |
| --- | --- | --- |
| Engine Driver and affine Environment port | Eight named real-source Driver law inversions; complete `bombay-engine` cargo-mutants candidates | Typed causal transcripts, complete settlement/error/custody traces, Engine fuzz, Driver 90% line floor |
| Bombay actor execution and outcome projection | Eight real-source kills and two affine denials; complete `bombay-rs` candidates | Panic/cancellation/replay retirement traces, exact settlement error projection, actor execution 93% and outcome 90% line floors |
| Bombay Entity, local capability interpretation, and private Observe | Every viable `bombay-rs` source candidate must be killed or explicitly classified; no baseline floor may conceal a survivor | Independent Entity oracle, production Entity/Observe Loom, four Observe fuzz grammars, affine Miri seeds, primary Observe 90% line floor |
| Bombay macro syntax owner | Caller compile-pass/fail, crate rename, hygiene, expansion, and differential witnesses; cargo-mutants does not claim a semantic macro mutation floor | Exact Behavior-owned expansion and static denials, never a second runtime semantics |
| External Behavior/Actors, Address, Communication, Timers | Locked upstream source, API, tests, and Bombay integration evidence; no first-party source mutation claim for an external crate | Exact typed effect interpretation and application traces |
| `mutants-gate` | Its own fail-closed unit inversions reject an incomplete, missing, stale, surviving, or timed-out report | Five gate tests and current complete cargo-mutants outcome/candidate identity check |

- The pre-change locked workspace/all-target coverage baseline completed:
  83.60% regions, 84.06% functions, 82.96% lines. Those totals include the
  isolated `observe-tests` path import as a second physical Observe row and
  an unexecuted performance binary, so they are accounting data rather than
  the acceptance floor. The exact owner rows were Driver 128/136 lines,
  actor execution 504/523, actor outcome 25/30, and primary Observe 495/528.
  Driver's uncovered branch is the intentionally unreachable retained-turn
  arm; actor outcome's missing branch was `SettlementFailed`.
- A new pure conversion regression supplies all three exact typed settlement
  failures and move-owned residual custody. It passed pinned debug and release
  runs. The real-source actor-execution mutation runner now kills its new
  `erase-settlement-failure` inversion in addition to seven earlier kills and
  two affine denials; the direct receipt is
  `target/test020-actor-evidence/actor-execution-law-evidence.json`. The new
  per-owner line-floor checker fails the pre-change JSON specifically on actor
  outcome (25/30, below 90%). Its floors are Driver 90%, actor execution 93%,
  actor outcome 90%, and primary Observe 90%; each maps to existing typed
  Driver/terminal/Observe laws and avoids the duplicated performance rows.
  The required Nix coverage derivation passed against the new regression:
  Driver 128/136, actor execution 504/523, actor outcome 46/51, and primary
  Observe 495/528 lines. Its JSON and floor receipt are in
  `/nix/store/010kfz1d1ih5j6lbk0rylwwj47g70095-bombay-workspace-0.1.0/`.
- Four seeded Observe fuzz targets (`ops`, `future_ops`, `promotion_ops`,
  `waker_ops`) each completed 1,024 deterministic runs at seed 20261001,
  max length 512, with per-target retained corpus and result logs. The Driver
  target completed 2,048 runs at seed 20260928, max length 4096. Both scripts
  now pass the explicit fuzz directory accepted by cargo-fuzz 0.13.2 and
  request AddressSanitizer. The old no-directory invocation failed before a
  target could start; its command is no longer used. The weekly and manual CI
  cadence retains Observe fuzz corpora/crashes and Miri results; push/PR runs
  both bounded fuzz campaigns. Pinned nightly Miri 2026-06-15 passed 12 affine
  Observe tests for each of seeds 0, 1, 2, and 3. Miri's Darwin sysroot build
  warned that `rust-objcopy` could not load `libLLVM.dylib`, but setup and all
  test executions succeeded.
- AddressSanitizer applies to the current `aarch64-apple-darwin` host and
  CI's `x86_64-unknown-linux-gnu` target under the [Rust Unstable Book target
  matrix](https://doc.rust-lang.org/beta/unstable-book/compiler-flags/sanitizer.html).
  The pinned `.#fuzz` nightly's explicit `cargo fuzz run --sanitizer address`
  commands completed all five targets without a sanitizer report. Driver and
  Observe runs each retain a toolchain/seed manifest, exact result log, corpus,
  and any crash artifact under their own ignored `artifacts/run.*/` path.
  Miri and Loom retain the distinct unsafe and concurrency laws.
- The initial complete 886-candidate cargo-mutants sweep passed its unmutated
  baseline in 59s build plus 88s tests, then its Nix scratch copy ran out of
  local disk space before the first mutant verdict. The failed result is not
  mutation evidence. Generated development artifacts were cleaned through
  pinned `cargo clean --profile dev` (31.3 GiB logical), and the coverage
  artifacts through pinned `cargo clean --target-dir target/llvm-cov-target`
  (4.5 GiB logical); free space rose from 4.3 to 21 GiB. A six-worker retry
  passed its unmutated baseline and classified 33 of 886 candidates, then was
  stopped before six independent scratch builds exhausted local disk space.
  Its partial candidate list is not a verdict. The next sweep runs
  owner-package tests with three parallel jobs, retaining the fail-closed gate.
- The first three-worker discovery reached the `axum` methods without compiling
  that optional feature, so its `launch_axum` and `run_axum` replacements were
  falsely missed. The required sweep now activates all features. That partial
  default-feature report also found ten real observable gaps in public handle
  formatting, exact numeric address conversion, `RunError` formatting/source,
  and external actor termination; focused assertions for each now pass in
  debug and optimized builds. Removing `ExternalActor::close_admission` timed
  out the old receive test; an immediate exact-payload rejection now fails that
  mutation without waiting. These partial reports are discovery evidence only.
- One replacement is a proved equivalence: `StageApplicationChildren for ()`
  returns `Ok(Children::new())`, while selected Behavior's
  `Default for Children<A, NoChildren>` calls `Self::new()` exactly. No caller
  can distinguish the cargo-mutants `Ok(Default::default())` replacement. The
  existing gate rejects this survivor, so TEST-020 also needs an exact-name
  reviewed-equivalence entry without accepting a different missed, timed-out,
  caught, or stale candidate. The smallest failing gate test presents that one
  exact missed candidate alongside an explicit equivalence and checks that a
  different candidate remains rejected. Expected additional files:
  `crates/mutants-gate/src/main.rs`, `mutants-baseline.json`, `flake.nix`, and
  this ledger; estimated tooling-source delta `+45/-5`, Bombay/Engine
  production `+0/-0`, public types `+0/-0`. Reuse cargo-mutants' exact
  candidate identity and the existing fail-closed tally; add no runtime law.
- The new gate regression first failed against the earlier verdict parser (exit
  101: it rejected the exact reviewed equivalent as a survivor). The parser now
  accepts only names enumerated in `equivalent_mutants`; it rejects a stale or
  duplicate name, a different survivor, a timeout, and a previously equivalent
  candidate that becomes caught. `emit-baseline` carries the reviewed list
  from the existing baseline and still rejects any other survivor. All six
  `mutants-gate` tests passed in debug and optimized builds, and focused
  strict Clippy and rustfmt passed. The all-feature sweep's unmutated baseline
  passed; its complete verdict remains pending.
- The complete candidate sweep exposed `LocalIngress::fail_fence` as dead
  production code: `rg` finds no call site, and the owning `EntityIngress`
  interpretation/retirement paths already publish the exact fence outcome.
  Replacing the unused method body is necessarily invisible to all runtime
  tests. The smallest regression is the current complete-source reference
  search plus existing queued-fence acknowledgement test; the source mutation
  is the failing minimization witness. Before editing production, the planned
  change is deletion of that private method and its dead-code allowance from
  `crates/bombay/src/local.rs`, about `+0/-11/net -11` source lines, zero public
  types, reusing the existing `EntityIngress` fence owners and no new policy.
- The all-feature discovery sweep also found an exact library equivalence in
  `NoChildBindings::retire_child_tasks`: its empty `Vec::new()` return and the
  proposed `vec![]` expansion are the same standard-library construction, and
  the required `RetireChildTasks` implementation cannot be removed. The exact
  candidate is reviewed in the fail-closed baseline. Direct regressions now
  observe child occurrence scopes and creation bindings, timer events through
  both runtime ports, observation start/cancel, source admission, continuing
  report disposition, ingress payload retention, and distinct executor panic
  versus cancellation classification. The source mutation sweep predates those
  tests; a final fresh sweep must confirm that each replacement is caught.
- The same sweep found that `begin_terminal_reports` has no observable effect:
  `ActionInterpreter::commit` always finishes every continuing action with
  `Discard`, which clears `TerminationSelection`, and a stopping action ends
  the Driver run. Its no-op source mutation survived. The smallest end-to-end
  regression is the continuing-report then stop test, which must retain the
  later normal terminal outcome. Before editing production, the planned
  deletion touches `crates/bombay/src/reports.rs`,
  `crates/bombay/src/application_runtime.rs`, and
  `crates/bombay/src/interpret.rs`; expected production delta is about
  `+0/-10/net -10`, public types `+0/-0`. It reuses the existing
  `TerminationSelection::finish(Discard)` law and removes the redundant trait
  method and forwarding call; no replacement abstraction is introduced.
- The discovery sweep was stopped after 333 candidates because a hanging
  mutated test consumed the whole cargo-mutants outer timeout even after
  several other tests failed. Its partial report remains evidence for targeted
  regressions, not a score. The existing Nextest `mutants` profile provides
  per-test timeouts but was not selected by the Nix commands. Both mutation
  derivations now pass `--profile mutants` to Nextest. An initial direct probe
  selected that profile and exposed a 10-second baseline timeout in compile
  contract tests under parallel execution; the profile now allows 60 seconds
  per test. The unmutated baseline must pass before trusting any mutants.
- The direct two-candidate profile probe then passed its unmutated
  all-feature baseline (323 tests, 40 seconds) and classified the previously
  timed-out `LocalInbox::drain -> None` mutation as caught after Nextest
  terminated its hanging test. Its other replacement was unviable. This
  validates the per-test timeout mechanism; the fresh all-feature complete
  sweep is running under the corrected Nix derivation.
- The first corrected Nix sweep baseline failed its existing Driver law
  manifest test because that static test treated every `--profile mutants`
  token as a Cargo build profile. The command now passes the option after
  `--` to Nextest, and `.config/nextest.toml` declares that profile. The
  static test now requires both mutation derivations to select the declared
  Nextest profile and still rejects any bare Cargo-profile token. The failure
  occurred before any mutant ran, so the sweep must restart after this test
  passes in the Nix source snapshot.
- The revised Driver law manifest test passed in debug and optimized builds,
  and strict Engine Clippy and rustfmt passed. The restarted Nix all-feature
  sweep passed its unmutated workspace baseline (60 seconds of tests); the
  883-candidate mutation verdict is still in progress.
- The full sweep found two unreviewed survivors in recursive child occurrence
  actor-space selection. The existing test compared one later lookup with the
  first occurrence, but never repeated the later lookup; a fresh space on
  every recursive call still satisfied that comparison. The regression now
  compares two later lookups' exact registration scope as well. The prior
  source mutation is the failing inversion; focused debug and optimized
  verification plus a fresh mutation run are required before closure.
- The 223-candidate discovery snapshot was stopped as free disk reached
  3.8 GiB; its only unreviewed survivors were the two recursive child-space
  replacements addressed by the repeated-lookup regression. The replacement
  run uses two mutation workers and disables Cargo incremental artifacts to
  bound scratch growth. `cargo clean -p bombay-rs` removed 3.0 GiB of rebuildable
  local package artifacts while retaining the Miri/fuzz/coverage evidence.
- In the replacement sweep, both previously missed recursive child-space
  replacements now fail the exact repeated-scope assertion in 0.011 seconds;
  one remaining candidate at that point is the reviewed empty-child-product
  equivalence and another is the reviewed empty-Vec equivalence. The full
  inventory and final gate remain pending.
- The newly reached Observe source exposed two unreviewed retained-key hasher
  survivors: deleting the word mixer and replacing XOR with OR. Map equality
  still prevents wrong values, but those changes make common distinct two-word
  keys collide and degrade the selected retained-table performance owner. A
  new owner-local test hashes `(1, 0)` and `(1, 2)` as distinct observation-key
  shapes and asserts their hashes differ. The concrete pair is a collision
  under both source replacements and distinct under the selected mixer;
  focused debug/optimized verification and real-source mutation replay remain
  required. No runtime implementation or public API was added.
- Two further mixer candidates replace XOR with AND and delete byte-slice
  writing. The two-word regression also compares `(1, 0)` with `(1, 4)` so
  AND's common-key collapse is observable; a byte-key regression compares
  equal-length distinct worker names through `Hasher::write`. These assert
  separation of concrete retained-key classes without freezing exact hash
  bytes. The active sweep predates the additions and remains discovery only.
- A later candidate reversed the partial-tail branch in byte writing; the
  existing eight-byte names did not enter that branch. The byte-key regression
  now also separates equal-length six-byte node names, which must exercise
  the retained tail. Deleting `write_u64` is covered by the two-word tuple
  regression. Both are source mutations in the current discovery snapshot,
  so the final replacement sweep must establish their actual failures.
- The same hasher inventory also found a no-op `write_usize` survivor. A
  focused retained index-key separation test now hashes two ordinary `usize`
  observation keys and must distinguish them. This completes the three owned
  input shapes (`write`, `write_u64`, and `write_usize`) without asserting a
  private hash constant.
- The ordinary sweep also enumerated thirteen candidates in the four
  `#[cfg(loom)]` synchronization shims (`lock`, `read_lock`, `write_lock`, and
  `recover`); their bodies are absent from the default Cargo build, so a
  default-run Missed verdict says nothing about their Loom behavior. The
  mutation configuration excludes only those exact path/signature classes
  from the ordinary production sweep. The separate pinned Observe Loom
  derivation compiles and checks that conditional owner; terminal verification
  must rerun it. The exclusion is conditional-scope accounting, not an
  equivalence claim, and no production default-path candidate is removed.
- Observe's completion bit is declared `1 << 0` to name its bit position.
  Cargo-mutants' `1 >> 0` replacement has the same `usize` value `1`, so
  no state transition or test can distinguish it. The exact candidate is a
  reviewed equivalence in the fail-closed baseline; the shift notation stays
  for consistency with `HAS_WAITER` and `OUTCOME_VALID` bit positions.
- Two `Waiters::retain` match-arm deletions survived because cancellation
  tests checked the outgoing wake behavior but not the inline storage restored
  after fan-out shrinks. The new owner-local test starts with two registered
  waiters, retains exactly one, then retains none; it requires `One` and
  `Empty` respectively and counts both inspected registrations. This is the
  documented no-extra-allocation waiter law, rather than an exact internal
  payload assertion. Both original source mutations are intended inversions;
  focused debug/optimized tests and final real-source replay remain pending.
- The mutable wake-migration variant `Waiters::retain_mut` has the same
  fan-out compaction law and yielded two analogous survivors. The regression
  now replays two-to-one and two-to-none through that owner path as well,
  requiring the same inline and empty representations after migration.
- `Slot::complete` yielded one additional arithmetic equivalence:
  `COMPLETED` and `OUTCOME_VALID` occupy disjoint bits, so OR and XOR set the
  identical pair of bits. The exact replacement is reviewed in the baseline.
  Replacing the no-waiter `previous & HAS_WAITER` check with OR is a real
  performance/progress defect: it takes the waiter lock even with no waiter.
  A regression holds that lock, publishes from another thread, and requires
  publication to finish before releasing the lock; the test releases and
  joins before asserting so its own failure cannot strand a thread. Focused
  debug/optimized tests and source-mutation replay remain pending.
- The no-waiter completion test passed in pinned debug and optimized builds.
  The three hash-separation tests and the combined `retain`/`retain_mut`
  compaction test also passed in both builds. Their source-mutation verdicts
  require a new sweep because the active discovery uses an earlier snapshot.
- `Slot::reset` has the same no-waiter lock-avoidance law during pool reuse.
  The discovery sweep found its `state & HAS_WAITER` to OR replacement missed.
  A second owner-local regression holds the waiter lock while resetting a
  pristine slot on another thread, releases and joins before asserting, and
  requires reset to finish without the lock. It is the exact recycler-side
  inversion of the completion regression; pinned debug and optimized tests
  passed, and the final source replay remains pending.
- `SmallMap::insert_vacant` exposed two unreviewed performance-law survivors:
  reversing the inline threshold and reducing promotion capacity from twice
  `INLINE_CAP` to `INLINE_CAP + 2`. A new owner-local test inserts exactly
  `INLINE_CAP` distinct retained keys, requires inline storage, inserts one
  more, and requires hash storage with at least twice the inline headroom.
  This measures the documented allocation and retention-scale contract
  rather than the chosen hash bytes. A third source replacement divides the
  requested capacity by two; the same headroom assertion must reject it.
  The focused test passed in pinned debug and optimized builds; final source
  mutation replay is pending.
- `Subject::drop` allowed a `<= SLOT_POOL_CAP` mutation to survive because
  previous tests did not retire one more unique unobserved subject at an
  already full pool. The new owner-local regression constructs
  `SLOT_POOL_CAP + 1` live subjects, retires them all, and requires exactly
  `SLOT_POOL_CAP` recycled slots. It tests the documented bound through
  actual subject retirement, not by assigning the pool directly. Focused
  debug and optimized tests passed; final source replay is pending.
- The discovery sweep found `RootOrigin` equality and diagnostic formatting
  unobserved: replacing equality with unconditional `true` or formatting with
  an empty success survived. The terminal owner test will compare equal and
  different root addresses and require the root's address in its diagnostic.
  The adjacent `ChildOrigin` contract will also distinguish address and nonce
  and expose both in its diagnostic. These are public identity and provenance
  laws; the test uses different concrete origins rather than copying the
  implementation's comparison expression. The discovery also found root
  equality's unconditional `false` and inverted comparison missed in its
  earlier snapshot. The old snapshot later found `ChildOrigin`'s unconditional
  `true` and conjunction-to-disjunction replacements missed too; the new
  different-address and different-nonce comparisons distinguish both. All
  three terminal owner tests passed in pinned debug and optimized builds;
  final source replay remains pending.
- `TerminationPublication`'s concrete `publish` and owner-cancellation paths
  were caught, but its `Retirement::retire` adapter survived as an empty body:
  existing tests called only `publish`. Add an owner-local test that passes a
  completed outcome through the `Retirement` trait and waits for its exact
  published termination. This proves the adapter is live without adding a
  second publication implementation. The focused test passed in pinned debug
  and optimized builds; source replay remains pending.
- `Hosts<P> for Arc<N>` returned an unobserved hosted space: a mutation could
  leak a new empty `ActorSpace` instead of preserving the shared host. An
  owner-local regression will compare the address-registration scope obtained
  through the `Arc` implementation with the original owned scope, including
  after cloning the `Arc`. The scope is the public identity used for exact
  registration, so a fresh empty space is observably wrong. Focused builds
  and source replay remain pending. Two adjacent survivors in the old snapshot
  replace `HostedActorSpaces::space` with a fresh space or make
  `resolve_logical` return `None`. Extend the same owner test to assert the
  hosted registration scope, then claim an actual actor endpoint and resolve
  that exact address through the hosted logical policy. This uses Address's
  public claim/resolve behavior and Communication's ordinary mailbox rather
  than constructing a fake resolved value. All three topology tests passed in
  pinned debug and optimized builds; source replay remains pending.
- Native Entity's `LocalEntityRuntime::join` adapter survived a replacement
  with unconditional success. The existing family tests cover a synthetic
  runtime's join failure, while the native `BombayEntityRuntime` must preserve
  Tokio's exact failed task. An attempted end-to-end hydration-panic probe
  reached the panic but stalled at family shutdown after its admission waiter
  was aborted; it did not isolate `join`, so the probe was removed without a
  production edit. The retained owner-local regression calls the concrete
  native `LocalEntityRuntime::join` adapter on a panicked Tokio task and
  requires its exact `JoinError` panic provenance. It passed in pinned debug
  and optimized builds; source replay remains pending. The generic family
  task-group law stays in its separate tests.
- The Entity directory's `is_empty` check survived unconditional `true` even
  though its inverted predicate and unconditional `false` were caught.
  Existing tests required emptiness after removal but did not call this method
  while a represented slot was active. Add that nonempty observation to the
  real dispatch/activation test, which already interprets the first activation
  effect; do not create a new fixture that drops an uninspected decision.
  The focused test passed in pinned debug and optimized builds; source replay
  remains pending.
- The sweep also mutates Observe's compiled test waker support. `FlagWake`'s
  consuming `wake` callback was caught, but its borrowed `wake_by_ref` callback
  could become a no-op while tests still passed. Add a direct `Waker::wake_by_ref`
  observation of its flag, proving the test instrumentation used to check
  asynchronous wakeups. The focused test passed in pinned debug and optimized
  builds; source replay remains pending.
- Complete discovery checkpoint: 883 candidates yielded 307 caught, 524
  unviable, 52 missed, and no timeout. The 52 old-snapshot misses are mapped
  to 13 cfg(loom)-only shim candidates excluded from the ordinary sweep, four
  exact reviewed equivalences in `mutants-baseline.json`, and 35 observable
  regressions added after that snapshot. A fresh Nix sweep started against the
  current source and found 870 candidates after the conditional exclusions.
  Complete tracked/untracked physical file checkpoint before this paragraph:
  255 changed paths including 94 untracked; production
  `+7977/-6017/net +1960`, tests/examples/tools `+7769/-2634/net +5135`,
  docs `+17058/-1931/net +15127`, other `+164/-76/net +88`.
  Public API `+0/-0` types in this TEST-020 stage. Physical source-file
  classification counts test modules in production files as production.
- The first current-source Nix sweep found the expected 870 candidates but
  stopped before mutation at its cold unmutated Nextest baseline: 392/394
  tests passed and two compile-fixture tests reached the profile's 60-second
  per-test limit while their builds overlapped. Neither test reported an
  assertion failure. Raise only the mutation profile's threshold to four
  30-second periods (120 seconds), preserving finite failure detection, then
  rerun the cold baseline and the full sweep. This changes verification
  scheduling, not a production contract or reviewed equivalence.
- Current complete tracked/untracked file-based checkpoint: 253 changed paths,
  94 untracked; production `+7244/-4060/net +3184`,
  tests/examples/tools `+7814/-4503/net +3311`, docs
  `+16889/-1948/net +14941`, other `+452/-145/net +307`.
  Public API `+0/-0` types in this TEST-020 stage. This calculation uses
  `git diff HEAD --numstat --no-renames` plus physical lines of untracked
  files; it counts test modules inside production source files as production
  and therefore differs from the prior content-based checkpoint.
- Complete tracked/untracked checkpoint after this stage's tool and test
  edits: 252 paths, including 94 untracked. Physical file classification with
  `git diff --numstat --no-renames` plus untracked text: production
  `+6822/-5953/net +869`; tests/examples/tools `+7924/-2651/net +5273`;
  docs `+16833/-1948/net +14885`; other `+271/-89/net +182`.
  Public types `+0/-0`. This checkpoint uses stricter path classification
  than ARC-020's earlier record; its cumulative totals are not stage deltas.

#### TEST-020 resolution and complete mutation receipt (2026-10-02)

- State: `verified`. The final selected-source Nix sweep passed its 394-test
  unmutated all-feature baseline and completed 870 candidates in 2 hours
  24 minutes: 342 caught, 524 unviable, four missed, zero timed out.
  `/nix/store/db5kynqpqyvkmjd3br1jfgpdjs3mn6sf-bombay-workspace-mutants-sweep-0.1.0/`
  retains the complete candidate, outcome, and diff records. The four missed
  names match the exact previously reviewed semantic equivalences in
  `mutants-baseline.json`; no unreviewed survivor remains. The 35 former
  semantic survivors from the old source snapshot were all caught, including
  the Observe hasher/waiter/pool cases, provenance equality/diagnostics,
  topology registration/resolution, native Entity join, and directory
  nonempty observation. The thirteen conditional Loom shim candidates remain
  excluded only from this ordinary build and require the separate Loom gate.
- `emit-baseline` passed and generated 206 viable-function floors and 246
  zero-viable function entries. The old three-floor/seven-zero baseline then
  failed `nix develop -c cargo run --locked --release -p mutants-gate -- check
  <sweep>/mutants.out mutants-baseline.json` on unaccounted functions, as the
  fail-closed gate must. Replacing it with the exact emitted baseline made
  that same pinned command pass with `mutation coverage: 346 viable / 870
  total`. The four reviewed-equivalence names were unchanged. This is a
  complete inventory receipt, not a relaxation of the survival rule.
- The TEST-020 regressions, gate inversions, debug/optimized owner tests,
  90%/93% owner coverage floors, five bounded ASan fuzz campaigns, and pinned
  four-seed affine Miri run are recorded above. The final independent Nix
  `mutants` derivation and terminal audit gates are running after this item;
  their results belong to terminal verification, not this resolution.
- Complete tracked and untracked physical checkpoint before the resolution
  documentation: 255 changed paths, 94 untracked; production
  `+7625/-5961/net +1664`, tests/examples/tools `+7720/-2584/net +5136`,
  docs `+17096/-1948/net +15148`, other `+1005/-165/net +840`.
  TEST-020 added zero public types and removed zero public types. Physical
  source-file classification includes test modules in production files;
  these cumulative counts are not a claim that TEST-020 added production
  capability.

### ARC-020 resolution and full-tree checkpoint (2026-10-01)

- State: `feature-complete`; TEST-020 is now active, and terminal distillation
  remains pending. The [public API inventory](public-api-audit.md) maps each
  reachable Bombay-owned type and operation to its owner/caller, construction
  invariant, traits, exact failure/custody behavior, and compiled evidence.
  `ActorSpace` closes Address's generic endpoint shape to a protocol;
  `ActorExt` returns exact Behavior Actors wrappers; `ApplicationHandle`,
  `ActorInterface`, Entity IDs/ports, and terminal projections have distinct
  authority. No root export was deleted or added. Private synonyms
  `LocalAddresses` and `OccurrenceBindings` were removed.
- Selected Behavior's architectural naming ban was reconciled by renaming
  the private actor execution/outcome and termination-observation symbols,
  `InstalledSlotFacts`, and `OutputTarget` for their actual domain ownership.
  Historical PRD snapshots retain their old names with revision notes. The
  actor-execution test passed 10 focused debug and optimized laws. The
  pinned-Nix direct mutation script and the isolated Nix derivation both
  produced seven killed real-source inversions and two affine ownership
  denials. The derivation receipt is
  `/nix/store/7ni5h8dcj2jzyqq29w7r6casmfw482sp-bombay-workspace-0.1.0/actor-execution-law-evidence.json`
  at locked Behavior revision `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`.
- The original downstream handle `Debug` compile test failed E0277 for six
  ordinary handles and then `InstalledActor`; all seven now pass with redacted
  formatting. The original discarded-policy/terminal fixture passed despite
  `#![deny(unused_must_use)]`; after adding `#[must_use]` to six `ActorExt`
  methods and `ActorRetirement`, its two intended diagnostics pass trybuild in
  debug and release. Existing external-actor and origin/role compile denials
  preserve affine authority and static provenance. Removing the broad
  private Observe allowance produced 26 unused-keyed-API diagnostics; the
  same physical source remains independently verified by `observe-tests`, so
  the allowance is retained with a precise reason.
- The locked workspace tests, build, strict all-target Clippy,
  warning-denied rustdoc, rustfmt check, and diff check passed after all
  source edits. Complete measured tree before this resolution paragraph:
  241 changed paths including 83 untracked; production
  `+6995/-6007/net +988`; standalone tests/examples/tools
  `+7529/-2653/net +4876`; docs `+16600/-1931/net +14669`;
  other `+102/-49/net +53`. Relative to the TEST-017 checkpoint, ARC-020
  adds production `+879/-778/net +101` (the private file renames count as
  delete/add), standalone tests `+109/-1/net +108`, docs
  `+348/-5/net +343`, and other `+0/-0/net 0`. Public API `+0/-0` types.

### ARC-020 feature verification and audit change ledger (2026-10-01)

- State: `active`; all 19 named queue prerequisites are satisfied. `Cargo.lock`
  selects Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, Bombay-private Observe, and the pinned Timers patch at
  `13e884da7ab41781f52337b0038060e375b00ee0`. The complete selected
  Behavior instructions, owner APIs/tests/docs for the six capabilities,
  current Bombay source/re-exports/examples/compile fixtures, and the
  normative Driver/capability/module documents were inspected during the
  prerequisite items and checked again against the current lock. Behavior
  owns Actions and actor derivation; Behavior Actors owns templates; Address,
  Communication, Observe, and Timers own their primitives; Engine owns the
  affine Driver port; Bombay owns local composition and its exact application,
  Entity, external-caller, topology, and terminal contracts.
- Exact blocker: no repository-wide post-deletion inventory yet proves that
  each remaining caller-facing item has a real owner/caller, construction
  invariant, standard traits and intentional omissions, exact custody, and
  compiled example. Current documents mix aspirational and implemented API,
  and a broad private Observe `dead_code` allowance needs a justified scope.
  Naming must follow the selected Behavior domain rule and the official Rust
  API Guidelines checklist, not mechanical renaming from a search result.
- Smallest first regression: compile the retained public entry paths from the
  actual downstream examples and compile fixtures, run warning-denied rustdoc,
  and run a public-surface inventory that fails on any unowned export or
  current guidance without compiled evidence. Before production edits, isolate
  one concrete redundant/invalid surface with a downstream compile or
  differential test and a prior-representation failure. Existing concrete
  compositions, static proofs, and standard traits are the comparison; no
  macro or new API layer is eligible on audit assertions alone.
- Initial expected files: this ledger, `docs/todo.md`, current API/module
  documents, and targeted downstream compile fixtures. Initial production
  forecast `+0/-0/net 0`, public types `+0/-0`. Any independently proven
  removal or correction will receive an updated exact blocker, file list,
  line delta, and public-type count here before its first production edit.
  Reuse all selected owners and existing application/Entity/Driver tests;
  delete only a surface that lacks a unique semantic role and caller.
- First production audit probe: `crates/bombay/src/lib.rs` applies one
  `dead_code` allowance to the entire private Observe source. This masks the
  compiler's unused-item inventory for all of its APIs. The smallest failing
  regression is removal of that blanket allowance followed by a pinned Nix
  warning-denied `cargo check -p bombay-rs`; the original representation
  always masks those warnings. Expected first edit: `lib.rs` only,
  production `+0/-4/net -4`, public types `+0/-0`. If exact private Observe
  members genuinely need an allowance because the same source serves its
  isolated Loom/fuzz/performance harness, scope the allowance to those
  members and record why; do not remove a tested API merely to silence a
  warning. The existing Observe module and harness remain the owners.
- Probe result: removing the allowance and running
  `nix develop -c env RUSTFLAGS='-D dead_code' cargo check --locked -p bombay-rs`
  produced 26 intended unused-item diagnostics, concentrated in the keyed
  `ObservationSpace` and synchronous/registration API that Bombay's private
  production composition does not call. The isolated `observe-tests` package
  publicly exposes the same physical source for the keyed, Loom, fuzz, and
  performance boundaries. Removing it from the source to silence Bombay's
  local compiler would delete separately verified capability. Splitting the
  exact shared source solely to narrow a lint would add modules and duplicate
  ownership. Retain the module-scoped allowance with its precise reason;
  normal warning-denied workspace build and rustdoc still check the whole API.
  Production delta for this probe is `+1/-1/net 0` in the reason line,
  public types `+0/-0`.
- Naming blocker isolated before the next production edit: selected Behavior
  forbids `Incarnation` as an architectural identifier. Bombay's private
  `Incarnation<B,E,R>` is an execution wrapper that owns one Driver future and
  one retirement authority; its `IncarnationOutcome` classifies driver result,
  panic, and cancellation. The names describe neither owned operation nor
  transformation. The actual semantic owner is actor execution, so use
  `ActorExecution` and `ActorExecutionOutcome`, with `actor_execution.rs` and
  `actor_outcome.rs`. The precise pre-change static inversion is that a search
  for prohibited type/file identifiers finds both in live source; the existing
  panic/cancellation/exact-failure/replay tests and production mutation gate
  must still pass afterward. Ordinary Rust rename is sufficient: no wrapper,
  trait, macro, new behavior, or public API is needed. Expected source files:
  the two renamed private modules, `lib.rs`, launch, retirement, terminal,
  termination, observation, and the Entity lifecycle test; expected tooling
  files: the exact mutation script, Engine manifest, flake, mutation baseline,
  and current path-bearing documents. Forecast production `+0/-0/net 0` lines
  apart from names; public API `+0/-0` types. Preserve Driver, terminal
  publication, and all exact outcomes; delete only the banned identifiers.
- Public-handle trait blocker before another production edit: the ordinary
  `ActorRef`, `ActorInterface`, `ExternalActor`, `ApplicationHandle`,
  `ApplicationLifecycle`, and `App` handles lack `Debug`, contrary to the Rust
  API Guidelines C-DEBUG rule. A downstream compile-only test will require
  `Debug` for those exact public types and fail with E0277 on the original
  representation. Their private mailboxes, allocation source, and shutdown
  authority are not Debug values; implement redacted Debug with the visible
  domain identity and no payload or authority leak. `App` can derive Debug
  when its owned inputs are Debug. Expected files: one new integration compile
  test and the existing actor-interface, application-runtime, and local source
  modules. Forecast production `+65/-0/net +65`, public types `+0/-0`.
  Reuse Rust's `Debug` trait and existing handle values; add no facade.
- Observation vocabulary blocker before the next production edit: selected
  Behavior forbids `Fact*` architectural identifiers. Bombay's private
  `FactQueue`, `FactSource`, `PendingFact`, and `InjectFact` actually own
  registrations for peer/child termination and their ordered event injection.
  The current source search finds those banned identifiers; that is the
  original static failing witness. Rename them to `TerminationObservations`,
  `TerminationSource`, `PendingTermination`, and `InjectTermination`, and
  rename the owned `facts` fields to `observations`. Rename the generic
  `return_fact` helper to `inject_control_event`, which states the actual
  control-lane interpretation. Run existing simultaneous-ready, cancellation,
  and exact peer/child tests in debug/release; their observable order is the
  semantic differential. Expected production files: `observation.rs`,
  `local.rs`, `launch.rs`, `application_runtime.rs`, plus any direct test
  references and current capability/module docs. Forecast production
  `+0/-0/net 0` apart from identifiers; public API `+0/-0`. Reuse Observe's
  exact observations and Behavior's `InjectEvent`; add no observation service.
- The same static naming scan also found private `InstalledSlotFacts` in
  `entity/directory.rs`. It copies a decision's evidence, phase, and activation
  identity during ordered interpretation. Rename it to
  `InstalledSlotEvidence` in that file, preserving the existing interpreter
  trace tests. Expected production delta remains identifier-only and public
  API `+0/-0`.
- Must-use blocker before the next production edit: Bombay's six `ActorExt`
  methods consume an authored Behavior and return the entire composed policy,
  while `ActorRetirement` carries the exact terminal fact; neither currently
  warns when its value is silently discarded. A downstream fixture with
  `#![deny(unused_must_use)]` will first compile when discarding those values
  and then reject both mistakes once `#[must_use]` is attached. This is a
  compiler-visible law, not a new wrapper. Expected files: `actors/actor_ext.rs`,
  `terminal.rs`, one `template_authoring` trybuild fixture and its diagnostic,
  plus current audit docs. Forecast production `+7/-0/net +7`, public types
  `+0/-0`. Reuse the owning Behavior Actors wrappers and exact Bombay terminal.
- One remaining private Entity identifier uses the generic implementation
  word `OutputTarget`: it actually owns the source of installed slot effects,
  either a mapped slot queue or a transient exact effect vector. The original
  static naming scan finds it in `entity/directory.rs`. Rename it to
  `InstalledEffectSource` in that owner only; the existing Entity directory
  trace tests prove the two variants keep their effect order and payloads.
  Forecast production `+0/-0/net 0` identifier-only, public API `+0/-0`.
- Public installed-endpoint trait blocker before the next production edit:
  `InstalledActor<B>` is the Behavior `EndpointAddress::Installed` associated
  product and is re-exported at Bombay's root, but lacks `Debug` although its
  `ActorRef` already renders the address without exposing control authority.
  Extend the downstream `public_handle_debug` compile test with a nominal
  closed Behavior; the original representation fails E0277 for
  `InstalledActor<Root>`. Add a redacted `Debug` impl that shows its exact
  recipient and no control sender. Expected production file `local.rs`,
  forecast `+15/-0/net +15`, public types `+0/-0`; test extension about
  25 lines. Reuse the existing installed endpoint, no wrapper.
- Alias deletion blocker before the next production edit: private
  `LocalAddresses<P>` in `launch.rs` is definitionally identical to the
  existing public `ActorSpace<P>` and owns no state, proof, transition, or
  separate caller contract. Direct `ActorSpace<P>` syntax already compiles in
  the downstream host fixture and application examples; changing the local
  observation/launch uses then replaying their tests is the differential.
  Delete the one-line alias and replace its use in `launch.rs` and
  `observation.rs`. Forecast production `+0/-1/net -1` lines, public API
  `+0/-0` types. Reuse Address's exact space and Bombay's `ActorSpace` closure.
- A second exact synonym is `OccurrenceBindings<Owner, Root> =
  ChildBindings<Owner, Root>` in `child_bindings.rs`. It adds no occurrence
  evidence, state, or type distinction; both source paths already instantiate
  the same `ChildOccurrences` product. The existing child/application/Entity
  tests compare exact birth binding and retirement order using that product.
  Remove only this alias and use `ChildBindings` in `application_runtime.rs`
  and `entity/bombay.rs`, then replay those tests. Forecast production
  `+0/-3/net -3` alias/doc lines, public API `+0/-0`; no new wrapper.

### TEST-017 resolution and full-tree checkpoint (2026-10-01)

- State: `verified`; ARC-020 is now active. Before the change, five Observe
  doctests were ignored and no Observe doctest law executed. Three compile-fail
  fixtures in the isolated `observe-tests` package now establish E0599 for
  forbidden publisher/affine-observation clones and E0382 for a second
  consuming completion. One positive integration test retrieves an exact
  move-only outcome after publication. The first trybuild run failed only for
  missing expected diagnostics; captured diagnostics and clean replay passed.
  Both focused debug and optimized runs passed. Bombay and Observe doctests
  now have zero ignored entries.
- README points to executable examples/tests. Current user-facing API,
  capability, and Driver sketches are explicitly schematic `text` blocks,
  with links to the compiled owners. Historical decisions and the archived
  diagnostic probe remain historical evidence. Locked workspace tests/build,
  strict all-target Clippy, formatting check, and diff check passed.
- Complete measured tree before this resolution paragraph: 232 changed paths,
  including 77 untracked. Production `+6116/-5229/net +887`; standalone
  tests/examples/tools `+7420/-2652/net +4768`; docs
  `+16252/-1926/net +14326`; other `+102/-49/net +53`. Relative to the ARC-016
  checkpoint, this stage has production `+2/-26/net -24`, standalone tests
  `+75/-0/net +75`, docs `+117/-73/net +44`, other `+1/-0/net +1`.
  Public API `+0/-0` types. The production difference is rustdoc prose only.

### TEST-017 feature verification and change ledger (2026-10-01)

- State: `active`; ARC-016 and ARC-019 are feature-complete. TEST-017
  unblocks ARC-020. `Cargo.lock` still selects Behavior Core/Actors 0.20.0
  (`804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`), Macros 0.13.0
  (`3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`), Address 0.3.0,
  Communication 0.1.2, the pinned Timers patch at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private
  Observe. The selected Behavior instructions, current owner source/tests,
  Observe code/tests/harness, README, user-facing API, public examples, and
  capability/module/Driver documents were inspected for this documentation
  evidence feature. Behavior and Behavior Actors own actor syntax/templates;
  Bombay owns the runner and examples; private Observe owns publication and
  affine observation; the isolated non-publishable `observe-tests` package
  is the only downstream-accessible compilation view of that private source.
  Address, Communication, and Timers have no documentation-test ownership
  role beyond the executable examples already selected by their contracts.
- Exact blocker: the five Observe rustdoc blocks are `ignore`, so Cargo reports
  five ignored tests and executes none of their no-clone, double-complete, or
  positive affine examples. Current README and user-facing API contain
  `rust,ignore` snippets with omitted definitions; they can appear to teach
  current syntax without any compiler check.
- Smallest prior-representation witness: the full locked workspace run lists
  exactly five ignored Observe doctests and zero executed. The new harness
  must compile-fail on publisher clone, second consuming completion, and
  affine observation clone, and compile/run a positive move-only affine
  observation. Restoring `ignore` would recover the five ignored entries
  rather than executable evidence. Existing application, Axum, actor-template,
  external-interface, and Entity examples/tests are the executable owners
  for current public snippets; any schematic fragment must say so in prose.
- Ordinary Rust comparison: `compile_fail` rustdoc inside Bombay cannot prove
  the private Observe API because an external doctest sees privacy errors.
  The existing unpublished `observe-tests` library compiles that exact source
  for downstream access, so a small trybuild matrix there proves the intended
  type errors without a second implementation or public Bombay re-export.
  Existing public examples and compile fixtures are preferred to copy-pasting
  each illustrative README fragment into a new test crate.
- Expected files: Observe rustdoc, isolated harness manifest and targeted
  fixtures/test, README and current user-facing API, this ledger, TODO, and
  any existing example needed to make a current claim executable. Forecast
  physical production source `+0/-20/net -20` doc lines, standalone tests
  `+70/-0/net +70`, public API `+0/-0` types. Reuse exact Observe source,
  `trybuild`, existing public examples, and the selected Behavior algebra;
  add no runtime wrapper, feature law, or macro.

### ARC-016 feature verification and change ledger (2026-10-01)

- State: `active`. ARC-011, ARC-015, ARC-018, TEST-012, TEST-015, and
  TEST-016 are feature-complete; ARC-016 unblocks TEST-017 and ARC-020.
- Selected contracts: root `Cargo.lock` selects Behavior Core/Actors 0.20.0
  at `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, the sole Timers `[patch]` at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private
  Observe. The complete selected Behavior instructions, owner `ChildRole` and
  generated-role API, actors' structural positions, Bombay's `ActorSpace`,
  `Hosts`, origin/retirement API, derive and actor-facade source and fixtures,
  and the current capability/module/Driver documents were inspected.
  Behavior owns actor expansion, positions, and role proofs; Bombay owns
  concrete host selection and exact terminal provenance; Address,
  Communication, Observe, and Timers do not classify source type syntax.
- Exact blockers: `ActorSpaces` skips a type alias for `ActorSpace<P>` because
  it checks the last path segment's spelling, and its token-string duplicate
  check cannot identify alias-equivalent protocols. `TerminalProjection`
  classifies root/child, retirement actor, and structural position by final
  identifier, so aliases can be rejected or routed differently despite
  identical Rust types. `#[actor]` attaches three Clippy allowances to a
  caller-authored `receive`, suppressing legitimate warnings in that body.
- Smallest regressions: a pass fixture with an aliased actor-space field must
  implement `Hosts<P>`; a pass fixture with origin/retirement aliases must
  implement the same exact `ProjectTerminal` source as the spelled type; a
  duplicate alias-equivalent host must fail through overlapping Rust impls;
  a caller-authored warning must remain visible under strict Clippy. Run
  these against the prior macro before production edits.
- Ordinary-Rust comparison: a manual `Hosts<P>` impl handles every alias but
  repeats the same protocol/field selection at each multi-protocol call site;
  `ActorSpace<P>` alone already implements `Hosts<P>`, but current `App` and
  Entity hosting require a product for multiple protocols. Manual terminal
  projection can handle aliases but duplicates structural-to-declared child
  conversion and owner proof for every variant. Keep the two syntax-only
  derives while making protocol choice explicit and delegating type identity
  and duplicate denial to Rust. Preserve the accepted actor facade's exact
  owner-macro delegation; remove allowances on authored code.
- Expected files: macro source, terminal origin owner if a typed projection
  obligation is necessary, affected `ActorSpaces`/terminal declarations,
  focused compile fixtures/tests, current guidance, this ledger, and TODO.
  Forecast production `+35/-100/net -65` (may change with compiler proof),
  public API `+0/-0` types if existing `ChildRole`/origin methods suffice;
  no new runtime state, interpreter, capability, or actor contract. Reuse
  `Hosts`, `ActorSpace`, `ProjectTerminal`, `ChildRole`, `RootOrigin`,
  `ChildOrigin`, `ActorRetirement`, and Behavior's owner macro. Delete
  string-key protocol duplicate tracking, last-name type classification,
  and broad caller-method lint allowances.

**ARC-016 resolution:** The pre-edit aliased `ActorSpace` pass fixture failed
with missing `Hosts<First>`, and the aliased root-origin test failed because
the derive required the literal `RootOrigin` identifier. `ActorSpaces` now
requires `#[actor_space(P)]` on each hosted field, emits a concrete `Hosts<P>`
impl, and lets Rust check field identity and reject duplicate impls. The
retained derive is necessary because current `App` and native Entity paths
use one concrete multi-protocol host product; hand-written impls repeat each
mapping at every application. `TerminalProjection` now selects a closed
root/application-child/structural-child/declared-child syntax case. Root and
child methods prove their distinct origin types; declared children use
Behavior's exact `ChildRole<Owner, Child = Actor>` and the named actor's
`ActorRetirement` type. Root projection uses a generated concrete
`RootOrigin<_>` assignment, so aliases retain the same static proof without
another public method or trait. No last-segment type-name parsing remains. A briefly
prototyped associated-source trait was rejected because Rust could not prove
two different child-role associated sources disjoint, producing E0119 for a
valid two-role enum; explicit child-role annotations preserve the concrete
source types without adding a public trait.

The actor facade still forwards to the locked Behavior macro. All three
receive-method lint allowances were removed experimentally. Strict workspace
Clippy then rejected legitimate mandated signatures: `unnecessary_wraps`
for `BehaviorActed`, `unused_self` for a required receiver, and
`needless_pass_by_value` for an owned message. They remain scoped only to
`receive`, with the exact contract named in the reason. An isolated downstream
Clippy probe with `self.value = self.value` failed on `self_assignment` inside
that same authored body, proving unrelated body warnings are still visible.

Aliases for space, root origin, child origin, and actor retirement compile;
the renamed downstream crate proves generic host, collision hygiene, and
qualified paths. Compile-fail fixtures reject alias-equivalent duplicate
hosts, a wrong host field, missing hosts, duplicate terminal pairs, a missing
application-child marker, a wrong child role, and a wrong retired actor.
Focused debug and optimized `actor_spaces`, `terminal_projection`,
`application_terminal_custody`, `entity_application`, `fixed_supervisor_runtime`,
and `fifo_pool_runtime` suites pass without snapshot overwrite. Full locked
workspace tests, workspace build, strict all-target Clippy, rustfmt check,
and `git diff --check` passed before the final root-proof minimization; the
focused alias and compile-fail suite passed after it. The official Rust API
Guidelines naming and conversion guidance led to deleting the proposed
identity `into_root` method. No new public type, method, or actor contract
was added.

Complete tracked/untracked checkpoint after ARC-016: 224 changed paths,
including 70 untracked paths. Physical crate source, including inline tests,
is `+6114/-5203/net +911` against HEAD, a stage net `-57` from the ARC-018
checkpoint (`+95/-152` by cumulative-count difference); standalone tests,
examples, and tools are `+7345/-2652/net +4693`; docs are
`+16135/-1853/net +14282`; other files are `+101/-49/net +52`.
Public API `+0/-0` types. This is a
production reduction, not a new capability layer. TEST-017 is newly ready
and selected next; ARC-020 still awaits TEST-017.

### ARC-002 feature verification and change ledger (2026-10-01)

- State: `feature-complete` after ARC-001 reached `feature-complete`; ARC-002 unblocks
  ARC-020. The queue's reciprocal edge remains ARC-001 -> ARC-002.
- Selected contracts: root `Cargo.lock` selects Behavior Core/Actors 0.19.0
  from `e5c703e966eba4d2a15fe2c129594f57ae2270fc`, Macros 0.13.0
  from `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`,
  Address 0.3.0, Communication 0.1.2, Timers 0.1.0 from the sole `[patch]`
  revision `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private
  Observe. The selected Behavior instructions and relevant owner API/tests/docs
  were rechecked. Behavior owns `EstablishedRecipient<P>` and exact typed
  delivery; Behavior Actors supplies no external actor template. Address owns
  optional logical lookup, claims and leases; Communication owns the external
  mailbox and exact closed-payload recovery; Observe owns the retained
  termination publication. Timers is not in this external path. Bombay's
  application allocator owns the never-wrapping origin identity.
- Exact blocker: `ExternalActor::establish` constructs a fresh private
  `AddressSpace`, immediately claims one allocated address into it, and retains
  its lease. No resolver to that space is stored, returned, or passed to any
  interpretation. The Address claim can neither be observed nor collide with
  another actor, so `ExternalActorError::Address` reports an unreachable path.
- Smallest end-to-end witness: existing external actor tests establish two
  distinct allocated origins, send through exact `EstablishedRecipient<P>`,
  recover the complete rejected payload after receiver retirement, and show
  that stale recipients cannot retarget. A source/API inspection proves there
  is no resolution consumer; attempting to retain the private claim adds no
  observable law. The after-change test must preserve those traces exactly.
- Ordinary Rust comparison: an allocated `MailAddr`, an exact established
  recipient, Communication admission/receiver, and Observe termination already
  express the external contract. There is no static gap for a wrapper, builder,
  macro, or second address space. Application-owned logical Address resolution
  would be a new capability without a consumer and is rejected here.
- Expected files: `actor_interface.rs`, its existing test, current public
  guidance, this ledger, and `docs/todo.md`; forecast production
  `+0/-13/net -13`, tests `+0/-0/net 0`, public types `+0/-0` (one error
  variant removed). Reuse the existing `ApplicationAddresses`,
  `ActorRef::external`, exact recipient, mailbox channel, admission owner,
  receiver, and termination publisher. Delete only the private Address space,
  its lease, and impossible claim rejection. This is a separate stage from
  ARC-001 and ARC-012.

**ARC-002 result:** The desired exhaustive public error match failed against
the old API with `E0004` because the private `Address` claim variant still
existed. That exact fixture now compiles. `ExternalActor::establish` no longer
creates an Address space or lease; it retains the allocator-issued `MailAddr`,
exact recipient, admission owner, affine receiver, and termination publisher.
The two-origin, exact reply, stale-recipient, complete rejected payload, and
noncloneable receiver witnesses pass in debug and optimized focused tests.
The complete locked debug workspace suite, formatting, strict all-target
Clippy, and whitespace check pass. There was no new public type; one public
error variant was removed. No other actor Address claim changed.

Complete tracked/untracked checkpoint after ARC-002: 184 changed paths,
including 49 untracked paths. Production under `src/` is
`+4593/-4543/net +50`, a task-local net `-13` from the preceding ARC-001
checkpoint; tests/examples/benches/fuzz outside those sources are
`+4869/-2373/net +2496`; docs are `+12122/-1834/net +10288`; whole tree is
`+21681/-8793/net +12888`. These cumulative counts include earlier
authorized stages and untracked research. ARC-002 is `feature-complete`;
project-wide minimization and final gates remain.


### ARC-001 feature verification and change ledger (2026-10-01)

- State: `active`; ARC-006 is feature-complete and is the sole prerequisite.
  ARC-001 must unblock ARC-002 and ARC-012. The dependency graph in
  `docs/todo.md` names these reciprocal edges.
- Selected contracts: Behavior Core/Actors 0.19.0 at
  `e5c703e966eba4d2a15fe2c129594f57ae2270fc` and Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`; Address 0.3.0;
  Communication 0.1.2; Timers 0.1.0 at the root `[patch]` revision
  `13e884da7ab41781f52337b0038060e375b00ee0`; Bombay-private Observe.
  `Cargo.lock`, the sole root patch, selected source, public algebra, tests,
  and current capability/Driver/module documents were inspected for this
  feature. The selected Behavior revision's complete `AGENTS.md` governs it.
- Ownership: Behavior and Behavior Actors own the closed shutdown request and
  typed `InjectEvent<ShutdownRequested, Here>` proof. Communication owns the
  exact `ControlSender<E>` and returns `ControlClosed<E>` with its rejected
  event; its user sender remains separate. Address owns opaque resolution and
  lease retirement. Observe retains the one-publication termination result;
  Timers owns actor-local schedules. Bombay alone owns admission closure,
  root lifecycle projection, concrete application launch, and Entity lease
  retirement. Engine has no shutdown adapter to change.
- Exact blocker: `ActorRef<P>` currently stores
  `Option<Weak<dyn ShutdownControl>>`. One private production implementation
  erases `B::Event`; `None` marks external endpoints. Protocol-indexed delivery
  references therefore carry an irrelevant lifecycle field, and root launch
  loses the event type before constructing `ApplicationLifecycle<P>`.
- Smallest end-to-end regression: compile an application boundary that keeps
  delivery as `ActorRef<P>` while statically naming lifecycle custody by its
  exact event type; compile-fail an external delivery reference used as a
  shutdown authority or one event type substituted for another. Run the
  existing first/repeated/stopped shutdown and complete termination traces in
  debug and optimized profiles. The desired two-parameter lifecycle syntax
  must fail against the current one-parameter representation before edits.
- Ordinary Rust comparison: an event-indexed lifecycle projection can carry
  `Weak<ControlSender<E>>`, while the existing protocol-indexed `ActorRef<P>`
  stays cloneable and delivery-only. `Ingress<ShutdownRequested, Here>` and
  `InjectEvent` provide the exact event transformation. No macro, extra
  mailbox, second actor contract, or trait object is needed. A Behavior-indexed
  public application handle would expose composed template actor types to
  callers, so the existing protocol and family indices gain an exact event
  index instead. Caller syntax and diagnostics must be measured first.
- Expected production files: `local.rs`, `launch.rs`,
  `application_runtime.rs`, and `entity/bombay.rs`; application boundary type
  annotations in examples/tests and current guidance will follow only when
  the focused regression proves the gap. Forecast production delta
  `+90/-100/net -10`; public types `+0/-0`. Reuse `ActorRef`,
  `InstalledActor<B>`, `ControlSender<B::Event>`, `ApplicationLifecycle`,
  `RootActor<B>`, `OwnedActor<B>`, and one Observe termination. Delete
  `ShutdownControl`, `TypedShutdownControl`, and their sole erased lane.
  This stage addresses ARC-001 only; ARC-002 and ARC-012 retain their own
  change ledgers.
- No new wrapper, builder, trait, facade, recipe, or public product is planned.
  The changed lifecycle type uniquely owns shutdown access for one installed
  root and transforms one typed shutdown request into its existing control
  lane. Its concrete application use replaces the trait object and its
  endpoint-attached optional field.

**ARC-001 result:** The desired two-parameter lifecycle compile-pass fixture
failed against the previous representation with `E0107` and no unrelated
diagnostic. The retained `ActorRef<P>` now has no shutdown field or method;
the existing `ApplicationHandle` and `ApplicationLifecycle` each name the
concrete root event sum. Root launch carries a weak view of the Environment's
existing control sender. Direct child and native Entity shutdown borrow the
sender already stored with the installed incarnation. A single private
`request_actor_shutdown` function preserves the old admission-close-before-
control-send order and the exact AlreadyStopping/AlreadyStopped distinction.
The original typed `ShutdownChild` request remains the rejected settlement
item, while application shutdown borrows its unchanged authority and creates
only the zero-sized ingress. External endpoints cannot acquire this lifecycle
projection because their `ActorRef<P>` has no constructor for it.

The compile-pass fixture now proves cloneable protocol-indexed sending,
termination observation, and event-indexed lifecycle request. Compile-fail
fixtures prove wrong root protocol, wrong lifecycle event, no shutdown on an
`ActorRef`, and no direct shutdown on `ApplicationHandle`. A root trace now
asserts accepted, repeated, and post-termination shutdown results; a local
control-lane test consumes exactly one event and proves no resend in debug and
optimized builds. The complete locked workspace tests passed in both profiles,
and strict all-target Clippy passed. Two explicit ignored ARC-011 cancellation
tests remain outside this feature.

Complete tracked/untracked checkpoint after ARC-001: 183 changed paths,
including 48 untracked paths. Production under `src/`: `+4588/-4525/net +63`
(including embedded tests); tests/examples/benches/fuzz outside those sources:
`+4856/-2371/net +2485`; docs `+11991/-1828/net +10163`;
whole tree `+21530/-8765/net +12765`. The prior ARC-006 checkpoint had
production `+4405/-4363/net +42`, so this feature's cumulative production
effect is net `+21`; it is new capability code, not code reduction. No new
public type was added; two existing public products gained an event index.
These raw file/line counts include earlier authorized stages and untracked
research. ARC-001 is `feature-complete`; final project-wide minimization is
still outstanding.


The workspace and every retained independent lock select these exact owners:

| Owner | Selected contract |
|---|---|
| Rust | 1.96.0, edition 2024 |
| Behavior Core | 0.19.0 at `e5c703e966eba4d2a15fe2c129594f57ae2270fc` |
| Behavior Actors | 0.19.0 at `e5c703e966eba4d2a15fe2c129594f57ae2270fc` |
| Behavior Macros | 0.13.0 at `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8` |
| Address | 0.3.0 at `87f7af4fd67671bbcf05c59f5da687b40ce7ba98` |
| Communication | 0.1.2 |
| Timers | 0.1.0 at `13e884da7ab41781f52337b0038060e375b00ee0` |
| Observe | Bombay-private implementation |

The exact immutable registry selections and checksums are pinned in
`Cargo.lock` and the Engine fuzz lock. The root patch table selects only the
unreleased Timers correction. A sibling checkout or a previously released
crate is evidence only and never overrides the selected build contract.

### 2026-09-30 dependency-selection stage for ARC-006 and BEH3

- The prior lock selected Behavior Core/Actors 0.17.0, Macros 0.12.0, and
  Address 0.2.0. The current root and Engine fuzz locks now select Behavior
  Core/Actors 0.19.0, Macros 0.13.0, and Address 0.3.0 while retaining
  Communication 0.1.2 and the exact Timers Git patch above. The crates.io
  sparse index publishes Behavior Core/Actors 0.19.0,
  Macros 0.13.0, and Address 0.3.0. The Behavior 0.19.0 release commit is
  `379f845`, but the selected Core and Actors registry archives identify
  `e5c703e966eba4d2a15fe2c129594f57ae2270fc`; Macros 0.13.0 identifies
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`. Their `AGENTS.md` files
  have the same SHA-256, and the source trees differ from the release commit
  only in non-production documentation or fixture-lock content. The exact
  selected `AGENTS.md` was read.
  Downloaded registry archives match sparse-index SHA-256 checksums
  `a8b275a5bc832ab117a9f9c9fe002736f642b18e35e10d9ae5af47ab4d6948e7`
  (Core), `bf1bd4a9f16f85e0bdb747a0ca784aaca0f0ffda527390af76b0e1ee88b2f0ef`
  (Actors), `c6f66dda26895533be22286fe4a062b43a524cac5ea2a3cc1a956b6c0d38a4bc`
  (Macros), and `8dfc2197b4156cc87c4021a2fa0e8767a5efb009c98d4238c4147714840fc1dc`
  (Address). Five relevant published source files match the owner release
  snapshots byte-for-byte. Both lockfiles were regenerated through the pinned
  Nix shell.
- Owner verification so far: Address 0.3.0 owns an affine, exclusive,
  non-resolvable `Reservation` from `try_reserve`; `publish` consumes it and
  retains its registration identity. Behavior 0.19.0 still requires
  uninterpreted initialization `Actions` in `HostRejected`, so reservation
  must precede initialization commitment. Behavior Actors 0.19.0 owns
  `AssignWorker::settle` over `EstablishedDelivery` and
  `ProxyOperation::settle` over `ProxyControlAdmission`, each returning the
  exact source action on lower-capability rejection. The released source,
  focused owner tests, Address reservation tests and Loom model, current
  Communication return-on-closure API, Observe code, and selected Timers queue
  are the feature-local evidence to complete before runtime implementation.
- Dependency edges remain reciprocal: Address reservation unblocks ARC-006;
  ARC-006 unblocks ARC-001 and PLACE1. The two Actors settlement operations
  unblock BEH3, which unblocks ARC-010; ARC-010 unblocks TEST-025 and DIST1.
  The dependency blocker is removed. The first locked workspace check now
  fails on 14 concrete API migration errors in `address.rs`,
  `application_runtime.rs`, and `entity/family.rs`; no production migration
  is yet claimed complete.
- Change ledger for this selection stage: smallest existing end-to-end
  regression is the three TEST-023 causal visibility cases, which failed on
  the prior claim-before-commit representation. The assignment/proxy stage
  still requires a real rejected-delivery witness before production changes.
  Expected files for dependency selection are root `Cargo.toml`, `Cargo.lock`,
  Engine fuzz `Cargo.toml` and `Cargo.lock`, this ledger, and the current
  dependency documentation. Expected production Rust delta is `+0/-0/net 0`;
  public types `+0/-0`. The later runtime stages must record their own exact
  expected source files and line deltas before their first production edit.
  Existing `LocalEnvironment`, Driver, `ActionSettlement`, Address space,
  Communication mailbox, and Behavior Actors source actions are reused; no
  Bombay-side replacement algebra or address registry is planned.

#### Behavior 0.19 installed-actor contract migration (before production)

- Derived law: a committed child result and exact-child observation carry the
  installed concrete behavior authority. Its protocol endpoint alone cannot
  authorize a behavior-specific shutdown. The published owner API requires
  `EndpointAddress::Installed<B>`, projects its message recipient separately,
  and returns `CommittedChild<B, Occurrence>` after successful installation.
  This is a migration of the existing child and control ownership, not a new
  actor policy.
- Smallest prior-representation regressions: the existing TEST-023 causal
  visibility tests fail on the old claim order; the first locked 0.19 workspace
  check fails specifically because `MailAddr` lacks installed authority,
  Bombay constructs `EstablishedCreation` from a protocol recipient, and the
  Entity request omits the owner-declared logical-protocol product. Existing
  application creation, terminal custody, and compile fixtures are the
  end-to-end syntax/trace gates for the migration. A focused static-denial
  fixture for cross-behavior installed authority is required before retaining
  a new public carrier.
- Expected source files for the compile migration: `crates/bombay/src/address.rs`,
  `local.rs`, `application_runtime.rs`, `entity/family.rs`, and `lib.rs`, plus
  focused tests and current interface documentation. Expected production
  delta `+90/-45/net +45` lines; public API `+1/-0` types at most. The
  candidate `InstalledActor<B>` owns the existing `ActorRef<B::Protocol>` and
  exact `ControlSender<B::Event>` together, with crate-private issuance. It
  transforms exact-child control inputs into the typed control lane; protocol
  recipients cannot perform that transformation. This deletes the old
  recipient-to-actor reconstruction at creation and observation sites, and
  supplies the concrete value needed to remove erased shutdown authority in
  ARC-001. Creation, observation, and shutdown of the same child are its
  concrete uses. A bare `ActorRef` or structural tuple would permit two
  behaviors sharing a protocol/event to exchange authority.
- Keep ARC-006's reservation/publication implementation and the later
  application-wide audit in separate measured stages. This migration may not
  add a second host table, child registry, effect algebra, or policy wrapper.

#### Behavior 0.19 generated-send visibility migration (before production)

- A locked workspace check now rejects the public counter example with
  `E0446`: its generated `CounterSends` is private even though the actor type
  is crate-visible. The Behavior 0.19 owner macro accepts an explicit
  visibility before named `sends` lanes and uses it for both generated send and
  settlement products. Bombay's actor facade currently parses that argument
  as a type, so it cannot forward the owner syntax. The smallest compile
  regression is the existing counter example with `sends = pub(crate) { ... }`.
- Expected files: `crates/bombay-macros/src/lib.rs`, counter and any other
  affected actor facade call sites, plus this ledger. Expected production
  delta `+7/-2/net +5` lines; public API `+0/-0` types. The existing actor
  facade parser and Behavior macro are reused; this adds no semantic product,
  behavior contract, or runtime policy.

#### ARC-006 pure-initialization panic custody stage (before production)

- Exact blocker: a panic in a child's synchronous pure initialization fold
  currently unwinds the spawned task, drops the surviving child value, and
  reaches `establish_child` as a coarse `SpawnError::Panicked`. Behavior 0.19
  owns `ChildCreationOutcome::InitializationPanicked { creation }`; Actors 0.19
  maps that exact routed child to `WorkerCreationRejection::WorkerPanicked`.
  The owner PRD section 6.4 requires Bombay to catch only the pure fold while
  the child remains owned, release its unpublished reservation, and return
  the surviving child without inventing Actions or treating it as an ordinary
  `C::Error`. Communication 0.1.2, Address 0.3, Observe, and Timers have no
  additional panic policy. The `Driver -> Incarnation -> LocalRetirement ->
  SpawnError -> establish_child` chain is Bombay's exact dependency edge.
- Smallest failing regression: a panicking child initialization mutates its
  still-owned child, then panics; the parent receives one
  `InitializationPanicked` creation settlement with its exact ID, route, kind,
  and surviving child, no committed birth or public address. A Driver-level
  inversion requires one prepared-environment retirement and the surviving
  mutated behavior. On the current representation the child task unwinds and
  parent `establish_child` panics instead.
- Expected files: `crates/bombay-engine/src/driver.rs` and its driver-law test,
  `crates/bombay/src/{outcome,launch,terminal,application_runtime}.rs`, one
  external application creation test, and current Driver/runtime contract
  documentation. Expected production delta `+85/-12/net +73` lines; public
  API `+0/-0` types (variants added to existing closed sums). Reuse the
  current Driver retirement, prepared Environment retirement, LocalResidual,
  terminal projection, routed creation, and Behavior-owned creation result.
  Delete the child's coarse panic fallback as a representation of pure-fold
  panic; retain it only for a genuinely postcommit task unwind.

## Runtime completion programme

The 2026-09-29 user-directed programme is specified in the separate
[PRD requirements inventory](prd-backlog/README.md), with source evidence and
failure contracts. The former `runtime-completion-design.md` now redirects
there. This concerns
new feature completion, not resumption of the unrelated cleanup queue.

Confirmed ownership: Bombay provides Zenoh as its sole production networking
substrate, typed identity integration, local/multicore execution
and distributed actor hosting. Mnesis owns durability; `mnesis-bombay` composes
it with Bombay. Selo is built using those three and supplies the first-class
KERI identity/authority integration from the consumer side. Bombay must not
depend on Selo or Mnesis. The final distributed product composes the stack.
Both self-hosted and Kubernetes deployments are in scope. This supersedes
earlier external-roadmap placement of Zenoh outside Bombay; no existing
runtime contract or dependency has been changed in this design stage.
The user further selected deterministic identity and distribution providers
so Bombay can be built and tested before Selo. Selo integration is a separate
downstream milestone; it does not block the core distributed-runtime proof.

### Feature-local verification

- Re-resolved the root lock and sole Timers patch: Behavior Core/Actors 0.17.0,
  Macros 0.12.0 at `435560ce7bea8ad3330ee2d42e5034f837a80602`;
  Address 0.2.0; Communication 0.1.2; private Observe; Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; Tokio 1.53.1.
  Read that Behavior revision's complete `AGENTS.md` and Bombay's ownership,
  capability, Driver-law and verification documents.
- Inspected selected Behavior action/settlement algebra, Actors' assignment,
  proxy operation, activation, initialization, customer and diagnostic actions,
  pool/proxy tests and persistence module. Confirmed the two private
  rejection-return gaps in BEH3 and the six absent Bombay interpreters.
- Inspected Address claim/lease semantics and tests; Communication mailbox
  admission/retirement and concurrent closure tests; private Observe publication
  and retirement tests; selected Timers queue, generation tests and README;
  Bombay Entity/runtime, launch, application and timer integration. Existing
  actor spawn paths are `Send` and use `tokio::spawn`; application runners
  choose current-thread execution.
- Inspected `mnesis-bombay` manifests, lock, README, execution ADR and current
  Bombay probe. Its selected graph still uses Behavior 0.9.5, published
  Bombay 0.1.0, former Entity 0.1.0 and Mnesis/store 0.3.1. That graph is not
  silently promoted to this workspace's contract.
- Inspected GitHub `devrandom-labs/selo` at
  `c7f394bf46c2e150dcdd38f36b7b0989d85ea68e`: manifest, complete file inventory,
  README and naming contract. Only `selo-naming` exists; cryptographic identity
  runtime and integration contracts remain to be implemented.
- Consulted primary Tokio, Zenoh and KERI documentation. No Zenoh, codec,
  distributed placement authority or new Selo dependency revision is selected
  for Bombay. Those implementations remain blocked on their exact contracts.

### Reciprocal feature edges

This table extends existing entries; bridge rows below are reciprocal edges
for this programme, not duplicate implementations or altered cleanup priority.
An `active` row permits its stated research/witness work, not a guessed public
API or an unmeasured production edit.

| ID | State | Blocked by | Unblocks | Next concrete evidence |
| --- | --- | --- | --- | --- |
| EXEC1 | active | — | DIST1 | Compare ordinary-Rust execution selection and caller-owned async hosting; prove two actors run concurrently while each remains serialized |
| NET1 | blocked | External: immutable Zenoh/codec selection and typed extension-seam experiment | PLACE1, DIST1, SELO1 | Two-process request/reply, exact/ambiguous delivery outcomes, bounded queues and static protocol routing |
| AUTH1 | active | — | PLACE1, DIST1, SELO1 | Typed identity admission with deterministic accepted/denied/stale/unavailable cases; prove provider substitution without inventing a Selo API |
| PLACE1 | blocked | NET1, AUTH1; external: authoritative placement/fencing contract | DIST1 | Competing activation and partition recovery reject a stale owner's durable effects |
| DIST1 | blocked | EXEC1, ARC-010, TEST-025, MNE1, NET1, AUTH1, PLACE1 | OPS1 | Assemble supervised, durable, authenticated distributed actors and recover across host failure |
| OPS1 | blocked | DIST1 | — | Same semantics on self-hosted and Kubernetes deployments, bounded telemetry, drain and compatible upgrades |
| SELO1 | blocked | AUTH1, NET1, MNE1; external: Selo KERI runtime | — | Run the same identity contract suite against Selo, then KERI rotation/delegation/revocation and full-stack deployment scenarios |

Existing-entry reciprocal additions:

- BEH3 unblocks ARC-010 (unchanged).
- ARC-006 additionally unblocks PLACE1; its local activation contract must
  precede publishing a distributed owner.
- ARC-010 additionally unblocks DIST1 and continues to unblock TEST-025.
- TEST-025 is blocked by ARC-010 and additionally unblocks DIST1; its proof is
  executable supervisor/pool behavior, not successful pure initialization.
- MNE1 additionally unblocks DIST1. Durable failover also needs PLACE1's
  resource-enforced fencing; MNE1 alone does not provide distributed ownership.
- NET1 and MNE1 additionally unblock SELO1. SELO1 does not block DIST1 or OPS1;
  those may use explicit test identity for runtime evidence, while production
  identity/security claims require a real configured implementation.

Independent local progress is eligible while Selo/transport contracts are
developed. NET1's experiment must exercise the same typed law with Zenoh and
a deterministic faulting network test host. This does not require a second
production transport. AUTH1 begins with concrete admission
scenarios and a deterministic test authority; Selo later runs the same contract
suite under SELO1. PLACE1 begins with deterministic grant/partition scenarios
and real-process static placement; production failover still requires a
verified authoritative placement/fencing implementation.
No provider registry, duplicate actor algebra, identity implementation inside
Bombay, or new consensus algorithm is authorized by this plan.

`EXEC1` now has a separate [execution ownership PRD](prds/execution-ownership.md).
Its fixed contracts, decision gates and coordinated work packages supersede the
conversational net-line-reduction target. The entry remains active for verified
research and witnesses; dependent production changes require the PRD's accepted
decision records and a feature-local pre-edit change ledger. This documentation
addition changes no feature state or dependency edge.

EXEC research checkpoint, 2026-09-29: the current `Cargo.lock` selects registry
Behavior/Behavior Actors 0.17.0 from release revision
`435560ce7bea8ad3330ee2d42e5034f837a80602`, Behavior Macros 0.12.0,
Address 0.2.0, Communication 0.1.2, Tokio 1.53.1, Bombay-private Observe,
and Timers 0.1.0 from the existing Git patch
`13e884da7ab41781f52337b0038060e375b00ee0`. The complete Behavior
revision instructions and the repository's five current owner documents were
read. The complete inherited path/hash/line manifest is held at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-exec-goal-qyt0kpye/baseline.json`
(SHA-256 `224721737a7be95bb5cd05e440b90d70e0b32b32c6312b79fd4dcc95bde11dbb`);
the tracked baseline diff and status are beside it (SHA-256
`2ebe399be199305893aed50cfb4fcde2e30339f05bd2cb16cb40860d7fe6a45f`
and `e9f4d9653e9985602a044f4534d05dd4f824a9e337d91335f0127e485c24ee07`).
These preserve the pre-EXEC dirty tree, including untracked work; no reset,
stash or source-file replacement occurred. Decision research lives under
[`docs/prds/execution-ownership/`](prds/execution-ownership/). All eight
decision gates remain open; the research notes do not select a production
interface. In particular, the selected Behavior endpoint type is indexed by
protocol while exact established shutdown needs a behavior-indexed event
sender, so DG-SHUTDOWN records an upstream-or-complete-local static contract
dependency. DG-TASK separately records dropped-startup and join custody gaps.
The reciprocal programme edges remain `EXEC1 -> DIST1` and `DIST1` blocked by
`EXEC1`; no new programme edge is inferred from these feature-local gates.
Research-only change checkpoint relative to the frozen baseline: production
`+0 / -0 / net 0`; tests `+0 / -0 / net 0`; public API `+0 types / -0 types`.
Changed task-local paths are this ledger and the eight named decision records
under `docs/prds/execution-ownership/`; all are documentation. No Bombay actor
task, projection task or observation task count changed or was claimed from an
unintegrated experiment. `ApplicationBehavior`, `ApplicationLifecycle`,
`ActorInterface`, `ChildBindings`, `ActionInterpreter`, `FactQueue`,
`LocalTimers` and the Entity task group remain semantic owners pending their
recorded individual differential gates; forwarding aliases and the exact
observer task remain deletion candidates, not implemented changes.
First bounded EXEC witness slice, before source edit: reproduce the original
dropped-root-activation waiter defect while a test-only action interpreter
holds initialization commitment. The smallest failure is that dropping the
only startup waiter releases its cancellation sender and actor `JoinHandle`;
after the interpreter gate opens, the actor can hold an Address lease and
wait indefinitely without a cleanup owner. Expected additional touched path:
`crates/bombay/src/local.rs` (existing internal test module only), plus this
ledger; expected production `+0 / -0 / net 0`, test code approximately +60
lines, public API `+0 / -0` types. Reuse its existing `ActivationProbe`,
`GatedInitializationInterpreter`, one AddressSpace and the actual
`spawn_root_with`; add no new owner, interpreter, product, wrapper or public
surface. The test must fail on the current implementation for the orphaned
lease law, then be rerun in debug and optimized builds after DG-TASK accepts
an ownership model. This slice does not authorize a production fix while that
gate remains open.
Witness checkpoint after the bounded test edit: production
`+0 / -0 / net 0`; tests `+49 / -0 / net +49` embedded in
`crates/bombay/src/local.rs`; public API `+0 types / -0 types`.
The task-local changed path set is that test-bearing source file, this ledger,
and the eight decision records (10 paths). The original implementation fails
the exact ignored witness in debug and optimized builds with an orphaned
Address lease after the startup waiter is dropped; `gate release: Ok(())`
confirms initialization could proceed. The witness remains explicitly ignored
in the default suite pending a selected custody model and must become active
before this feature can be called complete. No actor/projection/observation
task count changed in production; no retained abstraction changed owner.
Pinned-Nix formatting check passed. The default Bombay library suite passed
`171` tests with `4` ignored and `0` failures; the new EXEC regression is one
of the ignored witnesses. Running it explicitly with `--ignored` fails for
the intended orphaned-lease law in both debug and optimized profiles.
Second independent custody witness slice: the same locked actor source can
activate through the existing gated interpreter, after which a normal
`OwnedTask::finish()` waiter is polled and dropped while Tokio survives. Its
captured `ActorRef::termination()` should eventually observe the selected
cleanup; current source detaches the join handle and treats cancellation
sender closure as normal waiting, so the test should fail for a still-live
actor. Reuse the same `ActivationProbe` and interpreter; expected only
`local.rs` test additions of approximately 40 lines, production/public API
`+0`, and no new owner or policy. This is a separate await/drop edge within
DG-TASK, not permission for a production fix before its contract is accepted.

### Design-stage change ledger

- Exact scope: explain the executable-template gap and design the requested
  local, multicore, durable, transport, identity and distributed composition.
- Expected/actual edited surfaces: this ledger, the new runtime completion
  design, runtime capability guidance and module-boundary guidance (four docs).
- Production: `+0 / -0 / net 0`; tests: `+0 / -0 / net 0`;
  public API: `+0 types / -0 types`. No manifest or lock change.
- Reuse: owning Behavior actions/source settlements and Actors policies;
  existing Driver, Environment, Address, Communication, Observe, Timers,
  Entity, Tokio tasks, Mnesis execution and Selo naming where applicable.
- Before any production edit, record the chosen slice's failing witness,
  files, production delta and public-type count. Existing cumulative surface
  checkpoints remain in force. This design does not waive them.
- Inherited complete-tree baseline: 114 changed tracked paths plus 24
  untracked files. File-based counts are production `+3307 / -4079 / net -772`,
  tests/examples/verification tools `+3763 / -2273 / net +1490`, documentation
  `+7026 / -902 / net +6124`, other files `+1062 / -985 / net +77`.
  Production-file counts include embedded unit tests; these are measurement
  categories, not a claim that inherited work has been reviewed or completed.
  This stage changes only documentation; its public API delta is exactly zero.
- Final design-stage verification: `git diff --check` and local document-link
  and programme reciprocal-edge checks pass. No Rust commands were needed for
  the documentation-only edits; the prior local example runs do not establish
  any newly proposed distributed behavior.
- Final task-local delta: four documentation files, `+595 / -0 / net +595`;
  production/tests/public API remain zero. Complete tree: 114 changed tracked
  paths and 25 untracked files; production `+3307 / -4079 / net -772`,
  tests `+3763 / -2273 / net +1490`, documentation `+7621 / -902 / net +6719`,
  other files `+1062 / -985 / net +77`, using the baseline categories above.

## Current audit dependency graph (2026-10-02)

The 45-row [canonical queue](todo.md#canonical-execution-queue) is the current
dependency graph for this audit. Its `Depends on` column is the `Blocked by`
relation; for each ID, `Unblocks` is exactly the reverse set of rows naming
that ID. A complete reciprocal scan found 67 edges, no missing ID, no cycle,
and no unsatisfied prerequisite. Twenty-five `TEST-*` rows are `verified`,
ARC-009 is `retained`, and the remaining 19 `ARC-*` rows are `distilled`
after the terminal audit. The programme graph
below is a historical snapshot of different work and does not set queue state.

## Historical programme dependency graph

```text
DX53 atomic Behavior migration (distilled)
  -> DX49 application-native Entity (distilled)
       -> MNE1 durable Mnesis execution (downstream, blocked externally)
  -> BEH1 creation-settlement disposition (feature-complete upstream)
       -> DX58 Behavior 0.17 crates.io adoption (feature-complete)

BEH2 pool-worker facade resolution (feature-complete upstream)
  -> DX58 Behavior 0.17 crates.io adoption (feature-complete)

BEH3 exact atomic source-action rejection return (feature-complete upstream)
  -> ARC-010 complete Behavior Actors capability interpretation (ready)
    -> TEST-025 executable supervisor and pool policies (blocked)

DX54 Triomphe feature minimization (feature-complete, independent)

DX55 module-scoped test imports (feature-complete, independent)

DX56 Observe retained-key naming (feature-complete, independent)

DX57 Machine output consumer (removed by ARC-004)

DX59 Machine topology identity rendering (removed by ARC-004)

DX60 normative runtime contract alignment (feature-complete, independent)

DX61 Machine test transition policy (removed by ARC-004)

DX62 Observe sequential model state (feature-complete, independent)

DX63 Observe exhaustive model state (feature-complete, independent)

DX64 Driver allocation fixture input ownership (feature-complete, independent)

DX65 Driver panic injection policy (feature-complete, independent)

DX66 Driver custody failure ownership (feature-complete, independent)

DX67 Entity Loom admission algebra (feature-complete, independent)

DX68 Entity Loom activation claim (feature-complete, independent)

DX69 Entity hash-gate phase (feature-complete, independent)

DX70 Observe fuzz state ownership (feature-complete, independent)

DX71 Entity activation-gate phase (feature-complete, independent)

DX72 Entity runtime failure selection (feature-complete, independent)

DX73 Entity replacement admission completion (feature-complete, independent)

DX74 Observe waker-selection ownership (feature-complete, independent)

DX75 Observe deadline parking value (feature-complete, independent)

DX76 Observe future-registration custody (feature-complete, independent)

DX77 Observe direct-registration readiness (feature-complete, independent)
  -> DX78 local admission-closure disposition (feature-complete)
    -> DX79 shutdown-transition assertion separation (feature-complete)
      -> DX80 future-poll assertion separation (feature-complete)
        -> DX81 receive-custody assertion separation (feature-complete)
          -> DX82 Observe outcome-take assertion separation (feature-complete)
            -> DX83 Observe blocking-wait assertion separation (feature-complete)
              -> DX84 Machine receipt-wait assertion separation (removed by ARC-004)
                -> DX85 Observe thread-join assertion separation (feature-complete)
                  -> DX86 Entity passivation-join assertion separation (feature-complete)
                    -> DX87 exclusive-turn assertion separation (removed by ARC-004)
                      -> DX88 linearized-execution assertion separation (removed by ARC-004)
                        -> DX89 serialized-submission assertion separation (removed by ARC-004)
                          -> DX90 Driver-run assertion separation (feature-complete)
                            -> DX91 Driver allocation-run assertion separation (feature-complete)
                              -> DX92 tagged-ingress send assertion separation (feature-complete)
                                -> DX93 Entity family-shutdown assertion separation (feature-complete)
                                  -> DX94 Entity admission assertion separation (feature-complete)
                                    -> DX95 generic downstream-consumer audit (feature-complete)
                                      -> DX96 local fence assertion separation (feature-complete)
                                        -> DX97 local termination assertion separation (feature-complete)
                                          -> DX98 repository-complete source audit (feature-complete)
  -> TEST-001 Driver final-settlement precedence (verified)
    -> TEST-002 Driver benchmark terminal custody (verified)
  -> TEST-004 production-bound Driver inversions (verified)
    -> TEST-003 executable revision-bound Driver-law evidence (verified)
  -> TEST-005 production-bound Incarnation inversions (active)

```

DX58 is feature-complete against the published 0.17 graph. Core and Macros
require every generated birth owner to choose exact live return or retirement
custody, and Actors preserves retirement custody while later source lanes
continue in declaration order. MNE1 still requires Mnesis-Bombay to select the
eventual immutable Bombay and Behavior graph and implement its own durable
command execution contract. Bombay deliberately does not classify mailbox
admission as durable completion.

## ARC-003 feature verification and change ledger (2026-09-28)

- Selected contracts: `Cargo.lock` selects Behavior Core/Actors 0.17.0 and
  Macros 0.12.0 from `435560ce7bea8ad3330ee2d42e5034f837a80602`,
  Address 0.2.0, Communication 0.1.2, and Timers 0.1.0 from the sole root
  `[patch.crates-io]` at `13e884da7ab41781f52337b0038060e375b00ee0`.
  These are the selected registry/Git sources; the sibling Behavior checkout is
  used only at the exact selected revision for its complete `AGENTS.md`.
- Feature-local owner check: selected Behavior `transition.rs` and
  `effects/actions.rs` own actor folds and typed actions; selected Behavior
  Actors `atomic/stable_proxy`, `atomic/dynamic_supervisor`,
  `discovery/{registry,resolver}.rs`, `machine.rs`, and their algebra/proxy
  tests own actor-child policy, not stable domain-command hydration. Address
  `AddressSpace`/`Lease`, Communication mailbox and exact send returns,
  Bombay-private Observe publication, and the pinned actor-owned TimerQueue
  own their existing capability laws. Entity `lifecycle`, `directory`, and
  `runtime` own the remaining stable key, bounded waiters, reservations,
  processing fence, and exact retirement. ARC-007's recorded differential
  trace confirms this boundary. The current module-boundary and capability
  documents agree. No dependency contract or protocol consumer blocks this
  local classification change; ARC-004 and TEST-010 depend on it.
- Exact blocker: `SlotReducer::reduce` computes the successor and effects,
  while `EntitySlot::handles`, `SlotEvent::trigger`, `LifecycleEdge::endpoints`,
  and the post-reduction topology search independently classify the same
  turn. In particular, an early `FenceAcknowledged` in `Draining` with pending
  reservations performs no transition but the separate `handles` predicate
  calls it handled. The smallest regression will assert its returned
  classification, phase, and complete effects against an independent
  state/input expectation; it must fail against the existing representation
  in both debug and optimized tests before production edits.
- Planned smallest change: make the total slot reduction return state,
  ordered effects (including exact returned commands and leases), and a
  closed disposition selected in its executable branch. The directory will
  preserve that disposition at the linearization point; the machine adapter
  will carry it without a second post-reduction classification lookup. Validate
  the retained declarative topology against observed edge dispositions and an
  independent oracle. Remove the duplicate `handles` and trigger lookup.
- Expected touched production paths: `entity/lifecycle/{mod,machine,inactive,retiring}.rs`,
  `entity/{directory,runtime,mod}.rs`; expected production delta below +200
  and ideally net negative after deleting redundant classification code.
  Expected test/doc paths: lifecycle and Entity test modules, this ledger,
  `docs/todo.md`, and current source-boundary guidance if its contract changes.
  Public types expected: one concrete `SlotDecision` replacing its alias;
  remove `SlotReducer` if its generic implementation has no remaining owner;
  retain the already public phase/edge/disposition vocabulary only where it
  expresses a distinct caller law. Reuse `SlotEffectBatch`, `LinearizedExecutor`,
  `LifecycleMachine`, and existing primitive/Behavior Actors owners. Delete
  the post-reduction edge lookup and the independently matched `handles` table.
- Abstraction budget: `SlotDecision` uniquely owns the atomic return of one
  Entity turn: successor, exact owned effects, and disposition. Its unique
  event transformation is classification at the same branch that consumes the
  input. The generic `Decision<State, Effects>` cannot carry that law; the
  existing wrapper recovers it through a second table. The concrete use is
  `LocalDirectory::begin_drain`/`EntityRuntime::passivate` and the ordered
  executor output. The change deletes existing classification machinery
  rather than adding a forwarding wrapper.
- Inversion evidence: before the production edit,
  `fence_acknowledgement_before_reserved_delivery_is_ignored` failed in both
  debug and optimized builds (`SelfLoop` observed, `Ignored` expected). After
  the independent 137,561-trace oracle was added, temporarily restoring the
  old early-fence classification made that oracle fail on `FenceCurrent` for
  exactly the same law; the branch was restored and the oracle passed again.
- Result: `EntitySlot::decide` now selects successor, ordered typed effects,
  and `TransitionEvidence` together. The generic `SlotReducer` and its second
  `handles`/`trigger` classification table are removed; `LifecycleMachine`
  adapts the already-selected disposition into `LinearizedExecutor` without
  topology lookup. The declarative topology is checked against the independent
  state/input oracle, including all seven edges, exact effects, retained
  payloads, authorities, and replayed stale facts. `EntityRuntime::passivate`
  consumes the installed disposition and phase. The remaining `Base` adapter
  and topology rendering are ARC-004's separate minimization question.
- Verification: focused Entity lifecycle tests passed in debug and optimized
  builds (14/14); `nix develop -c cargo test --locked --workspace` passed;
  `nix develop -c cargo clippy --locked --workspace --all-targets -- -D warnings`
  passed; `nix develop -c cargo fmt --all -- --check` passed. Full workspace
  testing preceded the final oracle-only additions; those additions passed
  their focused debug/optimized tests and strict workspace Clippy.
- ARC-003 task-local checkpoint: seven production paths; production
  `+158 / -186 / net -28`; tests `+437 / -22 / net +415`; public API
  `+0 / -1` named types (`SlotReducer` removed, `SlotDecision` changed from an
  alias to the concrete result under its existing name). Eleven total
  task-touched paths include the queue and current design guidance. The
  complete working tree at this checkpoint, including untracked paths, has
  89 changed paths: production `+800 / -587 / net +213`, tests
  `+3371 / -1522 / net +1849`, and public API `+4 / -1` named types versus
  `HEAD`. Other files including documentation are `+5692 / -1744 / net +3948`.
  These whole-tree figures include work present before ARC-003 and use the
  first module-level `#[cfg(test)]` to separate source and test lines.
- State: `feature-complete`. ARC-004, TEST-010, and ARC-014 are unblocked;
  ARC-004 is the next queue item by priority and sequence.

## ARC-004 feature verification and change ledger (2026-09-28)

- Selected contract: the same exact `Cargo.lock` Behavior Core/Actors 0.17.0,
  Macros 0.12.0, Address 0.2.0, Communication 0.1.2, and pinned Timers
  `13e884da7ab41781f52337b0038060e375b00ee0` are in force; the root
  patch table selects only Timers. The complete Behavior instructions at
  `435560ce7bea8ad3330ee2d42e5034f837a80602` remain controlling.
- Feature-local owner check: selected Behavior `Behavior::transition` owns
  actor folds and `Actions`; selected Behavior Actors `StableProxy`,
  `DynamicSupervisor`, Registry/Resolver, and its own `Machine` own actor
  policy, keyed actor management, discovery, and receive/become/defer/stop.
  Address `AddressSpace`/`Lease`, Communication mailbox and exact return,
  Bombay-private Observe, and pinned Timers retain their capability contracts.
  Entity alone owns stable-key hydration, delivery reservations, ordered fence,
  and exact retirement. ARC-007's complete differential and ARC-003's
  independent transition oracle verify this boundary. The selected owner
  APIs/tests and Bombay's current directory, ordinary/Loom tests,
  allocation tests, error tests, benchmarks, `driver-law.md`,
  `driver-test-strategy.md`, module boundaries, and capability document have
  been inspected for this feature. No upstream dependency blocks this local
  minimization.
- Exact blocker: `bombay-machine` exports both `Reducer` and `Machine`
  transition algebras, topology composition, and three executors. Its only
  production consumer is Entity's `Base` adapter plus `LinearizedExecutor`.
  No second production caller shares its output-ordering, poison, evidence,
  and terminal laws. The `LinearizedExecutor` stores cloned latest evidence
  solely so Entity can recover the activation ID already owned by its slot
  state. The package also duplicates the locked Behavior Actors `Machine`
  name while implementing a distinct, unconsumed framework.
- Smallest failing boundary regression: a compile-fail downstream fixture
  attempts to import `bombay_machine::Decision` through Bombay's dependency
  graph. It must compile before the change, causing the intended trybuild
  failure, and fail to resolve after the production dependency is removed.
  Existing `reentrant_delivery_resolution_appends_fence_to_current_interpreter`,
  `active_dispatch_stays_under_allocation_ceiling`, the ARC-003 pure-fold
  oracle, and real-directory Loom tests are the behavioral preservation
  witnesses. Add production-bound panic and concurrent output-order inversions
  before removing the generic executor tests.
- Planned smallest change: replace Entity's generic `Base` and
  `LinearizedExecutor` with one private concrete slot whose mutex owns the
  affine `EntitySlot`, ordered pending effect batches, and one dispatch phase.
  Submit linearizes the total `decide` return; callbacks run after unlocking;
  reentrant/concurrent callers append effects to the same queue. Panic while
  transitioning poisons the slot, while a panicking effect consumer drops
  only its owned output and permits a later dispatch to resume. Read the
  current activation from the slot itself, removing cloned latest evidence.
  Keep no generic public executor without two production consumers.
- Expected production paths: `entity/{directory,mod}.rs`,
  `entity/lifecycle/{mod,machine}.rs` (rename the retained domain vocabulary to
  a truthful file), root and Bombay Cargo manifests/lock, the complete
  `crates/bombay-machine` crate, and the Entity lifecycle benchmark. Expected
  tests/bench/docs: Entity directory, Loom, allocation and error tests,
  Machine-only benchmarks/fixtures, this ledger, queue, module boundaries,
  and current capability guidance. More than 15 paths may change; the user
  has explicitly authorized expanded-surface production work in this tree.
  Expected production delta is net negative, with at most 200 new concrete
  private Entity lines and deletion of the generic crate. Expected public API
  is `+0` types and removal of the entire unowned Machine framework and
  Entity topology types. Existing Behavior, Behavior Actors, Address,
  Communication, Observe, Timers, `EntitySlot`, `SlotEffectBatch`, and
  `LocalDirectory` are reused. No new actor contract or macro is proposed.
- Abstraction budget: the private slot uniquely owns one entity key's state
  and pending ordered effects. Its event transformation is the exact
  linearized `EntitySlot::decide` turn, and its output transformation drains
  those effects without holding the slot lock. A plain `Mutex<EntitySlot>`
  cannot preserve reentrant output order after unlock; the current generic
  executor does, but carries an unnecessary public algebra, topology,
  cloning, and unrelated executor policies for one caller. The concrete
  directory tests and Loom model demonstrate this owner. The change deletes
  `Base`, `LifecycleMachine`, `LifecycleOutput`, `OutputEvidence`, and the
  workspace package rather than forwarding them.
- Prior-state proof: the downstream `bombay_machine::Decision` compile-fail
  fixture compiled before dependency removal, so trybuild failed for the
  intended boundary. It fails to resolve after removal, and trybuild passes.
- Result: Entity now owns a private `Slot` with affine state, ordered effect
  batches, and a dispatch phase. Its transition is `EntitySlot::decide`;
  callbacks run outside the slot lock. The entire `bombay-machine` package,
  generic executors/combinators, topology, Base adapter, stored evidence,
  machine-only benchmark, and obsolete tests/re-exports are removed. Retained
  lifecycle vocabulary lives in `entity/lifecycle/transition.rs`. The
  previously edited Machine receipt assertions were removed with their
  unowned package; the Entity and Driver changes already in this tree were
  preserved. No new public type was added.
- Behavioral evidence: the ARC-003 independent 137,561-trace oracle, Entity
  directory tests, optimized lifecycle and allocation tests, the real-directory
  optimized Loom tests, and the downstream compile boundary all pass. A
  panicking transition poisons its slot; a panicking effect consumer drops
  only its output and later dispatch resumes. Temporarily reversing the
  queue drain to `pop_back` made the production Loom ordering test fail with
  observed `[0, 2, 1]` against `[0, 1, 2]`; restoring FIFO passes.
- Gates: `nix develop -c cargo test --locked --workspace`,
  `nix develop -c cargo build --locked --workspace`,
  `nix develop -c cargo fmt --all -- --check`, and
  `nix develop -c cargo clippy --locked --workspace --all-targets -- -D warnings`
  passed. `nix develop -c cargo bench --locked -p bombay-rs --bench
  entity_lifecycle --no-run` passed. The optimized production Entity Loom
  command with `RUSTFLAGS='--cfg bombay_entity_loom'` passed. `git diff --check`
  passed. `nix flake check --no-build` could not evaluate
  `devShells.aarch64-darwin.miri` because its pinned nightly channel path
  `/nix/store/4c5593l3lcadd8k84dm5q0s5p34zgr89-channel-rust-nightly.toml.drv`
  was absent/invalid; evaluating all 18 `checks.aarch64-darwin` names passed.
- Complete-tree checkpoint after this stage, including untracked files:
  production `+872/-2360/net -1488`; tests `+3718/-2596/net +1122`;
  other `+5767/-1799/net +3968`; public named API `+4/-32` relative to HEAD;
  107 changed or untracked paths. Relative to the preceding ARC-003 checkpoint,
  production net fell by 1701 lines, tests net fell by 727, and 31 public
  named types were removed. This stage is `feature-complete`; TEST-021 owns
  the final absence audit and ARC-015 owns later Entity surface distillation.

## TEST-010 feature verification and change ledger (2026-09-28)

- Selected contract: `Cargo.lock` still selects Behavior Core/Actors 0.17.0
  and Macros 0.12.0 at `435560ce7bea8ad3330ee2d42e5034f837a80602`,
  Address 0.2.0, Communication 0.1.2, and the sole patched Timers 0.1.0
  revision `13e884da7ab41781f52337b0038060e375b00ee0`.
  The exact selected Behavior instructions and its fold/action and Actors
  template tests were inspected for ARC-003/004 and remain controlling.
- Feature-local owner check: Entity alone owns the stable-key lifecycle and
  `EntitySlot::decide`; Behavior/Actors own actor folds and template policies,
  Address owns leases, Communication owns exact delivery returns, Observe
  owns completion, and Timers owns generation-safe schedules. Current Entity
  source, its pure-fold and directory tests, the module boundary and capability
  documents, and the ARC-003/004 differential records were rechecked. No
  dependency edge blocks a test-only audit.
- Exact gap: the new independent 137,561-trace oracle checks complete state,
  authority, effects, and evidence, but the named edge/replay test does not
  assert that each of the seven edges is actually reached or that each relevant
  stale fact is injected at the phase where it might be misclassified. The
  smallest regression is an explicit edge witness table with
  per-edge stale and replay inversions checked against the existing independent
  expected transition, plus successor-phase coverage.
- Planned change: add only tests to
  `crates/bombay/src/entity/lifecycle/transition.rs`, then update this ledger
  and `docs/todo.md`. Expected production `+0/-0/net 0`, tests under 100 new
  lines, public API `+0/-0`. Reuse `EntitySlot::decide`, the independent oracle,
  and existing typed events/effects. No wrapper or new semantic abstraction is
  needed. A deliberate mutation of one classification must make the new test
  fail for its intended edge before accepting it.
- Result: seven explicit edge witnesses assert the independent model traverses
  the named edge. Each applicable edge is preceded by its stale-generation
  inversion, then replayed after the transition. Every input is additionally
  checked in each witness phase against complete successor state, authority,
  ordered effects, and classification. The existing 137,561-trace oracle and
  longer composition traces remain.
- Inversion: changing production `ForceDrain` stale-generation classification
  from `Ignored` to `SelfLoop` made the new focused test fail at `ForceStale`
  with exactly that discrepancy; restoring `Ignored` passes.
- Verification: four focused lifecycle transition tests passed in debug and
  optimized builds; strict workspace all-target Clippy passed; rustfmt was
  applied and `git diff --check` passed. ARC-004's complete locked workspace
  suite remains the latest full suite, with no production behavior changed in
  TEST-010.
- Task-local checkpoint: executable production `+0/-0/net 0`; tests
  `+96/-0/net +96` lines in the source test module; public API `+0/-0`.
  The complete tree remains 107 changed or untracked paths; the prior ARC-004
  checkpoint is increased only by these test lines and documentation edits.
  State: `verified`; ARC-015 remains blocked by other prerequisites.

## ARC-014 feature verification and first change ledger (2026-09-28)

- Selected versions are unchanged: `Cargo.lock` selects Behavior Core/Actors
  0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, and patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`. The exact Behavior
  instructions and owner APIs/tests were inspected for ARC-003/004 and remain
  controlling. Entity's current runtime, native adapter, family, directory,
  tests, module boundary, capability guidance, and Driver law/test strategy
  were rechecked for this feature. Behavior Actors owns actor policy; Bombay
  alone owns these Entity lifecycle tasks. No dependency contract blocks the
  local task-ownership correction.
- Ownership defect: `EntityTaskGroup` counts active tasks and idle epochs,
  while `EntityTaskOwner` separately stores native Tokio join handles.
  `BombayEntityRuntime::spawn` silently drops a task when its weak owner has
  gone. Also, `passivate` can install a drain before interpreting its fence
  without holding the admission/shutdown gate, allowing shutdown to observe
  zero tasks and then a stranded draining slot. The latter is the smallest
  end-to-end failing blocker to isolate before broader task-owner edits.
- First-stage regression: hold one admitted delivery after activation, start
  family shutdown and observe its admission closure, then call `passivate`
  before the delivery settles. It must not begin an independent drain after
  shutdown owns the family. Release delivery and require the exact
  fence/retirement trace and one completed shutdown. The prior ordering must
  fail at the passivation disposition before production changes.
  Existing family task tests, exact command custody, and production Entity
  Loom tests remain witnesses. First-stage expected paths are
  `entity/runtime.rs`, `tests/entity_runtime.rs`, this ledger, and the queue;
  executable production under 20 net lines, public API `+0/-0`. Reuse the
  existing admission gate, directory, task group, and typed effects. Do not
  add task-owner machinery until this regression passes in debug and release.
- Later ARC-014 stage: compare plain owned task records, associated task join
  values, and current count/handle collections against actual caller syntax
  and failure diagnostics. The final owner must preserve required facts when
  native scheduling cannot proceed, and must classify task panic/cancel and
  family join exactly. Record its own preproduction ledger after the first
  stage is verified.
- First-stage prior-state proof: the new causal test failed in debug and
  optimized builds with `Passivation::Begun` after shutdown had closed
  admission. Its shutdown-poll signal and held delivery establish order
  without a sleep or scheduler guess.
- First-stage result: `passivate` now holds the existing admission gate through
  directory interpretation and returns the new `ShuttingDown` disposition
  after family shutdown claims that gate. The exact delivery/fence/retirement
  trace passes; all four Entity family tests pass optimized. Strict workspace
  all-target Clippy, formatting, and `git diff --check` pass.
- First-stage task-local checkpoint: executable production `+11/-0/net +11`
  (one disposition variant plus the admission guard), tests approximately
  `+77/-0/net +77`, public named types `+0/-0` and one enum variant added.
  Complete tree: 107 changed or untracked paths; relative to the prior
  TEST-010 checkpoint, the only code additions are this runtime guard/variant
  and the family regression. The entire ARC-014 item remains active.
- Second-stage representation comparison: a plain `EntityTaskGroup` count
  cannot return the native `JoinError` and would leave the separate handle
  collection; a concrete Tokio-only group cannot represent the existing
  thread-based advanced test host. An associated task handle on the existing
  `LocalEntityRuntime` port lets one group own registration, idle epoch, and
  exact join custody without a new public scheduler trait, boxed future, or
  callback map. The port's plain `spawn` and `join` methods remain static;
  failed task joins are preserved in the generic shutdown product rather than
  dropped. The existing actor task hierarchy and Observe epoch primitive are
  reused. No macro is involved.
- Second-stage blocker and preproduction regression: the current native owner
  calls `drop(task.await)`, erasing a Tokio panic/cancel join fact. The smallest
  test will launch a task that panics, then assert family shutdown returns its
  exact typed join failure. Before the representation change it must fail to
  compile or observe that fact. A closed-port compile witness will also make a
  scheduler that silently discards its task fail the new trait contract.
- Expected second-stage paths: `entity/{runtime,bombay,family,mod}.rs`,
  the affected Entity family/runtime tests and any public examples/fixtures
  using `EntityShutdown`, plus ledger/queue/current docs. Expected executable
  production delta under +200 net lines after deleting `EntityTaskOwner`;
  public named types `+0/-0` if `EntityShutdown` becomes a generic closed sum,
  with associated task/join types on the existing port. The one group will
  own count, handle custody, and epoch. A native scheduled task has a concrete
  Tokio handle; the test host has a concrete thread handle. An ordinary caller
  sees the exact native join error only when a task fails.
- Shutdown-owner audit before the final second-stage edit: the public generic
  `shutdown(&self)` can be called again or concurrently. After one call closes
  the new affine handle group, a second call currently reaches `take_tasks`
  and panics; more importantly, two callers cannot both receive the same
  non-clone task join failure. Add a focused repeat-call regression before
  production editing. The smallest law is one shutdown claimant and one
  closed `AlreadyClaimed` result for every later caller. Expected files are
  the same runtime and Entity family test paths; production under +15 lines,
  no new named public type and one variant of the existing outcome. Reuse the
  admission mutex as the transaction claim and the existing task group as the
  affine result owner.
- Cancellation audit: a one-shot `AlreadyClaimed` gate prevents duplicate
  ownership but also strands the family if the winning shutdown future is
  dropped while activation is pending. The next preproduction regression
  cancels that future, releases activation, retries shutdown, and requires the
  complete typed family result. A join in progress has the same cancellation
  risk: a consumed handle or already observed failure must return to the one
  task group for the next claimant. The smallest correction is a group-owned
  shutdown claim with a Drop rollback and owned task records that distinguish
  pending join, completed join, and exact failed join. The local port's join
  operation must borrow its task handle so canceling the waiting future cannot
  discard it. This uses no new public named type; expect under +130 net
  executable production lines in `entity/runtime.rs` after replacing the
  current one-shot claim, plus the focused test.
- Drain-count audit before the next edit: the rollback claim restores task
  handles and already joined failures, but a retry after canceling a pending
  join recomputes the directory drain count after the first claimant already
  drained it. That would report zero represented slots for a transaction that
  drained one. The smallest regression pauses one join after the drain, cancels
  its claimant, resumes shutdown, and compares the exact original count and
  retirement trace. The first run must fail on represented count. Keep the
  chosen count in the same group as one closed drain phase; do not repeat the
  affine drain. Expected production edit under 25 lines in `entity/runtime.rs`,
  no public type, plus one test-host join gate and one test.

## ARC-014 closure and TEST-009 feature gate (2026-09-28)

- ARC-014 state: `feature-complete`. One `EntityTaskGroup` owns active tasks,
  idle epochs, join handles, the affine shutdown claim, drain count, and exact
  join failures. The native weak-owner task collection was deleted. Shutdown
  and passivation share the admission gate; a closed family has no public route
  to schedule new lifecycle work. The advanced port explicitly requires an
  owned task handle and a cancellation-safe join. Exact panic and cancellation
  facts return in `EntityShutdown::TaskFailed`. Cancellation restores every
  pending or completed task record and the original family drain count.
- ARC-014 prior-state witnesses failed in pinned-Nix debug and release for
  passivation-after-shutdown, erased panicking task, repeated shutdown,
  canceled shutdown, and canceled join. The focused Entity family and runtime
  suites pass in debug and release (9 and 11 tests). Full locked workspace
  test and build, strict all-target Clippy, rustfmt check, `git diff --check`,
  and optimized production Entity Loom (2 tests) pass. Miri/fuzz/benchmark do
  not exercise the task-group law; TEST-009 owns broader Loom interleavings.
  Task-local stage-baseline delta: production `+299/-110/net +189`, tests
  `+427/-27/net +400`, public named types `+0/-0`; the existing shutdown
  result became a generic closed sum. With test modules classified by their
  `#[cfg(test)]` boundary, the complete dirty tree is 109 changed/untracked
  paths, production `+1128/-2434/net -1306`, tests
  `+4202/-2573/net +1629`, other `+5983/-1788/net +4195`. This includes all
  preserved work already present in the shared tree; no commit was made.
- TEST-009 selected contract: the locked Behavior Core/Actors 0.17.0 and
  Macros 0.12.0 at `435560ce7bea8ad3330ee2d42e5034f837a80602`
  own pure actor transitions, not Entity directory synchronization. Bombay's
  `LocalDirectory`, Entity slot reducer, and effect interpreter own this
  concurrency law. Address 0.2.0, Communication 0.1.2, patched Timers 0.1.0,
  and private Observe are neighboring runtime owners and are unchanged by
  these tests. The exact source, public directory API, lifecycle tests,
  existing Loom tests, and current runtime/Driver boundary docs were checked.
  ARC-003 and ARC-014 are feature-complete, so no dependency blocks this
  test-only item; TEST-009 unblocks ARC-015.
- TEST-009 blocker: five tests in `tests/entity_loom.rs` recreate an unrelated
  `Admission` and `ActivationClaim` model and can pass when production removal,
  fence ordering, cancellation, or command custody changes. The smallest
  first regression exercises `LocalDirectory` under Loom with an installed
  delayed old removal racing a new dispatch and checks the exact activation
  and terminal slot. Further production cases cover pending delivery versus
  drain/fence, canceled waiter versus activation, and admission during drain.
  A mutation of `remove_matching` or an equivalent real owner branch must
  fail for the intended invariant before the toy file is retired.
- TEST-009 change ledger before test edits: expected files are
  `crates/bombay/tests/entity_loom.rs`, `entity_loom_local.rs`, this ledger,
  and `docs/todo.md`; executable production `+0/-0/net 0`, public API
  `+0/-0`, tests approximately `+180/-170` before minimization. Reuse
  `LocalDirectory`, its `EffectInterpreter`, closed lifecycle effects, and
  Loom's selected mutex/atomic substitution. No second admission model,
  runtime wrapper, or new public type is justified. Complete-tree thresholds
  were explicitly authorized by the user for the preserved dirty tree.
- TEST-009 state: `verified`. The five stand-in Loom tests were deleted.
  `entity_loom_local.rs` now runs six production directory/slot scenarios:
  two retained claims/order witnesses and four new pending-resolution/fence,
  old-removal/replacement, waiter-cancel/activation, and dispatch/drain races.
  The new traces assert exact activation and dispatch identities, each
  delivered or rejected command, one fence before graceful retirement, and
  final slot count; cancellation also counts both command drops. Production
  cannot install a replacement before old mapped removal, so the reachable
  race admits either a returned old-slot dispatch or a fresh activation, then
  rejects replayed old termination against the new slot.
- TEST-009 prior-state mutation: temporarily changing the real
  `resolve_draining_delivery` final reservation effect from `EnqueueFence` to
  `Remove` made the focused production Loom case fail (exit 101, expected
  `Fence` absent from the complete trace). The mutation was restored. The toy
  tests did not call the branch. All six production Loom tests pass in debug
  and optimized builds. The complete locked workspace suite, ordinary and
  Loom-configured strict Clippy, rustfmt check, and `git diff --check` pass.
  Executable production `+0/-0/net 0`, test code `+474/-198/net +276`, public
  named types `+0/-0`. Miri/fuzz/benchmark add no evidence for this test-only
  correction. ARC-015 still waits for TEST-021; ARC-008 is selected next.

## ARC-008 feature verification and first change ledger (2026-09-28)

- State: `active`; ARC-007's Behavior Actors/Entity ownership table is
  feature-complete and unlocks this item. `Cargo.lock` selects Behavior
  Core/Actors 0.17.0 and Macros 0.12.0 from
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe.
  The exact Behavior instructions, `ChildOccurrenceShape`/`ChildOccurrences`
  algebra, child-shutdown template and tests, Bombay creation/delivery/input/
  observation/shutdown interpreters, child bindings, selected Address endpoint
  and Communication control capabilities, Observe completion, Timers queue,
  and current boundary/Driver guidance were inspected for this feature.
  Behavior owns the closed child occurrence structure and creation settlement;
  Behavior Actors owns policy; Bombay alone owns concrete endpoint/control,
  actor-space, task, and terminal custody at one occurrence. No owner API is
  missing; this is a Bombay representation defect.
- Exact blocker: `CreationBinding::Established { route, kind }` can be inserted
  without the corresponding endpoint/control pair in a different map. A
  creation can then be treated as a rejected send, corrupt observation, or
  missing control depending on the interpreting lane. `ChildSpace` separately
  walks the same closed occurrence product. The smallest preproduction
  regression constructs an established child binding without capabilities and
  proves the current representation admits that invalid state. Its lawful
  replacement will require establishment to carry endpoint and control
  together, while rejected and absent creation remain distinct.
- First expected paths: `crates/bombay/src/child_bindings.rs`, its focused
  test module, `application_runtime.rs`, any affected child creation and
  terminal tests, this ledger, and `docs/todo.md`. The first architectural
  stage is expected to remove the parallel child-space product and separate
  creation/endpoint maps; executable production net no more than +100 lines
  before deletion, public named types `+0/-0` (all affected constructs are
  crate-private). Reuse the one Behavior-owned occurrence product, the
  existing ActorSpace, ActorRef, ControlSender, ProjectedTask, and exact
  settlement products; delete the repeated cursor and ChildSpace families if
  a focused representation comparison proves one selector sufficient.
- Plain-Rust comparison to perform before the production edit: a single
  `HashMap<CreationId, CreationBinding<Child>>` with an established variant
  owning route/kind/endpoint/control; a route-keyed endpoint map plus
  `CreationId` map; and direct tuple/product composition at the one occurrence.
  Measure caller syntax, static denial of missing capability and duplicate
  creation, error custody, and task/retirement ordering. No macro, generic
  routing framework, or new actor contract is eligible. The test must fail
  against the prior representation before broad migration.
- Complete-tree checkpoint before ARC-008 edits, including all untracked
  files: 110 changed paths, production `+1128/-2434/net -1306`, tests
  `+4626/-2770/net +1856`, other `+6108/-1788/net +4320`; public named types
  remain as recorded per prior item. This uses `difflib.SequenceMatcher`
  against HEAD and classifies `crates/*/src` lines before the first
  `#[cfg(test)]` as production. The user explicitly authorized continuing
  production edits despite the preexisting changed-path threshold.
- ARC-008 prior-representation proof: the new focused
  `established_creation_owns_its_endpoint_and_control` unit regression
  inserted `CreationBinding::Established` without any endpoint/control and
  failed at the required endpoint capability in both pinned-Nix debug and
  optimized runs (exit 101). This is a direct invalid-state witness, not a
  copied model. The final representation must remove the very construction
  the witness uses and replace it with a positive complete-binding and static
  denial witness.
- ARC-008 model comparison and aggregate checkpoint before production:
  route-keyed endpoints plus a creation-ID map leaves the contradictory
  established/absent pair representable. A tuple of two occurrence products
  keeps two traversals and allows one to be updated independently. The direct
  `HashMap<CreationId, CreationBinding<Child, Root>>` has exactly three
  semantic alternatives: map absence while a creation is pending or unknown,
  `Rejected`, and `Established` owning kind, endpoint, control, and
  exact projected task. An occurrence-local creation-order list preserves the
  existing descendant terminal order. The same Behavior-owned occurrence
  product can also own its `ActorSpace`, deleting the parallel `ChildSpace`
  shape and cursor. This is a derived Bombay interpreter representation, not
  a Behavior or Agha transition law. Its concrete use is `EstablishChild`
  followed by child delivery/input/observation/shutdown and parent retirement.
  No new public type, macro, or dynamic lookup is needed. Before: one
  creation status sum, two maps, task and empty retired vectors, two occurrence
  products, and three cursor selectors. Candidate: one closed creation sum,
  one map, one order vector, one actor space per occurrence, and one cursor
  selector. Each surviving value is needed for correlation, exact capability
  use, ordered task retirement, or endpoint allocation. The route is consumed
  by launch and terminal projection and is not needed in settled storage.
  The status/capability
  split and always-empty retired history disappear; no nested actor engine or
  semantic boolean is introduced. The Behavior creation law, Bombay runtime
  capability guide, and Driver action order were cross-checked. Disposition:
  `pass` for a focused representation edit; later callers must prove no
  additional selector is needed or this model reopens.
- ARC-008 result: `feature-complete`. A single Behavior-selected
  `ChildOccurrences` product now owns one `ActorSpace` and one creation map
  per occurrence. `CreationBinding::Established` owns the endpoint, control
  sender, and exact projected task together with creation kind; `Rejected`
  owns none. A single `ChildBindingAt` cursor selects the occurrence for
  creation, exact capability use, and hosting. The route-keyed endpoint map,
  independent child-space product, separate creation/space selectors, and
  always-empty retired vector were deleted. The only parallel order list is
  the explicit original creation order required to join descendant tasks
  deterministically; task handles themselves live only in the map. There is
  no stored route after launch, so a stale route cannot retarget a later
  child. The ordinary application topology example executes same-action
  child deliveries and phased child shutdown on the new representation.
- Remaining-absence law: `None` means no settled creation in that exact
  occurrence, including a creation still pending in the serialized Driver
  commitment. Child delivery/input classify it as `MissingBinding` rejection;
  creation observation, established observation, child observation, and child
  shutdown classify it as `MissingCapability` corruption. A recorded
  `Rejected` creation blocks each dependent item with the same exact
  `CreationCorrelation`. `Established` directly supplies the capability;
  no second endpoint lookup can reclassify it.
- ARC-008 final aggregate checkpoint: before, the child aggregate had
  independent creation and endpoint maps, task and retired vectors, a second
  protocol-space product, and three cursor families. After, its control
  alternatives are map absence, `Rejected`, and a complete `Established`
  product; one map, one order list, one actor space, and one cursor remain.
  The `child_bindings.rs` source shrank from 521 to 325 lines including two
  new internal tests. No module or public spelling was added. The remaining
  values all affect exact correlation, capability use, hosting, or ordered
  terminal custody. There is no arrival-history flag, inferred provenance,
  second lifecycle authority, or semantic boolean. The Behavior creation
  algebra, Behavior Actors child-shutdown plan, runtime capability guide,
  Driver causal law, and official Rust naming rule were rechecked. Disposition:
  `pass`; final project-wide public-surface minimization remains later work.
- ARC-008 inversions and gates: the prior established-without-endpoint
  regression failed in debug and optimized builds (exit 101). The new
  wrong-occurrence and rejected-binding test passes; replacing a duplicate
  creation instead of refusing it made its focused test fail (exit 101).
  A two-creation one-occurrence application test checks both exact
  cancellation terminals and their creation order; reversing the production
  retirement iteration made it fail (exit 101). The existing heterogeneous
  application, exact origin projection, and native Entity child terminal
  tests pass in debug and optimized builds. `cargo run` for the application
  topology example passes. The full locked workspace suite, strict all-target
  Clippy, rustfmt, and diff check passed before the last test-only terminal
  witness; rerun as the final local gate. Task-local stage-baseline delta at
  this checkpoint: production `+122/-425/net -303`, tests
  `+92/-24/net +68`, public named types `+0/-0`; all deleted types and traits
  were crate-private. Complete tree: 111 changed/untracked paths,
  production `+1250/-2859/net -1609`, tests `+4718/-2794/net +1924`, other
  `+6192/-1788/net +4404` by the same full-tree classification. Final docs
  edits will change the other category only.

## TEST-012 feature verification and change ledger (2026-09-28)

- State: `active`, no prerequisites. `Cargo.lock` still selects Behavior
  Core/Actors 0.17.0 and Macros 0.12.0 from
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe.
  The exact Behavior instructions and owner `Actions` fields, equality,
  `NoSends`/`NoBirths`, creations, delivery, and `Step` algebra/tests were
  rechecked. The Behavior Actors wrapper API/test surface, Bombay actor
  facade, four authoring/template test families, and current Driver/runtime
  boundary docs were inspected. Address, Communication, Observe, and Timers
  remain concrete runtime neighbors; no production contract is changed.
- Exact test blocker: `macro_last_authoring` drops increment actions and
  checks only the first read send; `activation_authoring` checks only the
  first send; `outer_authoring` checks step and sends without creations;
  `template_application` checks creation lengths only. A second send,
  omitted creation, or changed stop lane can survive these partial tests.
  The smallest failing regression is full `Actions` equality after mapping
  the named send product into its exact ordered vector, followed by a
  temporary extra-send mutation of one implementation. The mutation must
  fail the new differential but passes the previous selected-field checks.
- Expected paths: the four integration tests named above, this ledger, and
  `docs/todo.md`; executable production `+0/-0/net 0`, public types
  `+0/-0`, test-only delta under +130 net lines if direct equality replaces
  field-by-field assertions. Reuse Behavior's existing `Actions::map_sends`,
  `PartialEq`, typed `Creations`, `Step`, and named send products. No test
  helper trait, erased trace, or alternate effect traversal is justified.
  Macros remain syntax-only and Behavior owns every semantic lane.

- Result: the four authoring/template families now compare complete typed
  `Actions`. The owner/facade/explicit counter paths also compare exact
  initialization, increment, read, and stop products against independent
  expected actions, with overflow results bound before assertion. The builder
  candidate compares both emitted actions. A temporary second owner read send
  failed the full read comparison (exit 101), a temporary second activation
  send failed the active comparison (exit 101), and a duplicated wrapper timer
  request failed the fluent/direct comparison (exit 101); each mutation was
  restored. The outer facade fixture has `NoSends` and `NoBirths`, so an extra
  effect cannot be constructed in that family's type; complete initialization
  and transition equality covers its available lanes. All four focused test
  targets pass in debug and release; locked workspace tests, strict Clippy,
  formatting, and diff check pass. Stage delta: production +0/-0/net 0,
  tests +66/-67/net -1, public named types +0/-0. No production abstraction
  was added.

## TEST-015 feature verification and change ledger (2026-09-28)

- State: `active`, no prerequisites. `Cargo.lock` selects Behavior
  Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, and patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; Observe is Bombay-private.
  The exact selected Behavior instructions and macro/`Actions` owner surface,
  Behavior Actors templates, Bombay's three exported macros, current
  trybuild pass/fail fixtures, public re-exports, and Cargo workspace topology
  were inspected. Address/Communication/Observe/Timers are unchanged
  neighbors. `#[bombay::actor]` delegates to Behavior's owner macro;
  `ActorSpaces` maps named spaces to `Hosts`; `TerminalProjection` maps typed
  origin and retirement to `ProjectTerminal`.
- Exact blocker: `proc_macro_crate` alias resolution has only a local string
  test. The smallest independent regression is a real workspace fixture
  package whose sole Bombay dependency is renamed, which compiles and runs
  all three public macros through that alias. A temporary wrong alias in the
  macro resolver must fail this fixture and be restored. Existing local tests
  do not establish downstream alias hygiene.
- Expected paths: root `Cargo.toml`, `Cargo.lock`, one fixture package manifest
  and source, this ledger, and `docs/todo.md`; production runtime/macro delta
  +0/-0, no new public API types. The fixture will exercise generics, a where
  clause, and caller name collisions while retaining exact owner products.
  No new macro behavior, wrapper, or runtime machinery is justified.

- Result: `tests/renamed-downstream` is a workspace package with only
  `actor_runtime = { package = "bombay-rs", ... }`. Its integration test
  compiles `#[actor_runtime::actor]`, `#[derive(actor_runtime::ActorSpaces)]`,
  and `#[derive(actor_runtime::TerminalProjection)]` across a generic actor
  impl with a where clause, a generic hosted-space product, and local names
  that could capture an unqualified expansion. The actor test compares full
  initialization and transition `Actions`; the hosting assertion checks the
  exact field, and the projection bound checks the exact origin/terminal pair.
  Temporarily forcing the resolver to emit `::bombay` made the renamed fixture
  fail with unresolved path and missing Behavior implementation (exit 101);
  the resolver was restored. Focused debug and release fixture tests, the
  locked workspace suite, strict workspace Clippy, and diff check pass.
  Stage delta: production runtime/macro +0/-0/net 0, fixture tests +92/-0,
  public production types +0/-0; new package manifest and Cargo lock entry
  are workspace/test metadata. Final formatting check follows.

## TEST-016 feature verification and change ledger (2026-09-28)

- State: `active`, no prerequisites. `Cargo.lock` selects Behavior
  Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, and patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; Observe is Bombay-private.
  The exact Behavior instructions, actor macro owner and Behavior traits,
  Bombay child API, both identical compile-fail source files, their distinct
  expected diagnostics, trybuild feature selection, package feature list,
  and Nix flake check definitions were inspected. Other runtime neighbors are
  unchanged. Behavior owns the child `Behavior` requirement; Bombay's
  `Application::child` imposes it at the caller site.
- Exact blocker: the normal workspace check can select only one trybuild
  fixture after feature unification. The smallest regressions are two
  separate pinned package test commands, each proving its selected diagnostic
  fixture. Add both as Nix checks so each state remains required.
- Expected paths: `flake.nix`, this ledger, and `docs/todo.md`; production
  Rust +0/-0, public types +0/-0, tests +0/-0. Reuse the existing two
  diagnostic fixtures and `application_children` test. No generated source
  or new test abstraction is warranted for two short files.

- Result: added two independent flake checks, each executing the existing
  caller-syntax trybuild target in one package feature state. The pinned
  `--no-default-features` run selects
  `application_child_must_be_behavior.rs`; `--all-features` selects
  `application_child_must_be_behavior_feature_unified.rs`; both pass.
  Temporarily swapping fixture selection made the no-default-feature run
  fail its expected diagnostic (exit 101), then was restored and passed.
  `nix eval --json .#checks.aarch64-darwin --apply builtins.attrNames`
  lists both checks. An additional isolated `nix build path:.#checks...`
  was stopped after more than 14 minutes of fresh dependency compilation;
  it did not produce a pass/fail verdict. The required package-level pinned
  gates are green. Stage delta: production Rust +0/-0/net 0, tests
  +0/-0/net 0, flake check definitions +12/-0, public types +0/-0.

## ARC-017 feature verification and change ledger (2026-09-28)

- State: `active` after ARC-007. `Cargo.lock` selects Behavior Core/Actors
  0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, and patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; Observe is Bombay-private.
  The exact Behavior instructions, selected Behavior/Behavior Actors APIs,
  Entity `DispatchWait` and cancellation tests, complete Observe slot/
  publisher/observation unsafe sites and invariant comments, Miri/Loom Nix
  lanes, and runtime boundary documents were inspected. Behavior owns pure
  transitions; Entity owns this cancellation wait; Observe owns the cell and
  its unique publication/take authority. Other dependencies remain typed
  external neighbors and need no contract edit.
- Exact blocker: `DispatchWait::poll` uses `get_unchecked_mut` and
  `Pin::new_unchecked` despite `AffineObservation: Unpin` and no self-reference.
  The smallest regression is the existing cancelled-admission test plus a
  compile proof that the safe `get_mut`/`Pin::new` form typechecks; the former
  tests unchanged cancellation authority, the latter establishes the unsafe
  was unnecessary. Observe's own unsafe requires a per-operation invariant
  inventory and bounded Miri/Loom evidence before further edits.
- Expected paths before first production edit: `entity/runtime.rs`, this
  ledger, `docs/todo.md`, and focused cancellation tests only if a missing
  observable law is found. Expected production -4 or more net lines, public
  types +0/-0. Reuse `AffineObservation`'s existing `Unpin`, standard `Pin`,
  exact directory cancellation, and current Observe slot tests. No new
  wrapper, trait, or ownership abstraction is justified.

### Observe unsafe inventory

`Slot::state` owns the `COMPLETED`, `HAS_WAITER`, and `OUTCOME_VALID` bits;
`Slot::outcome` is the only uninitialized cell. The cell and waiter registry
remain private to the Observe module. The non-Loom and Loom implementations
of each cell operation below have the same contract; Loom's `UnsafeCell`
accessors model conflicts between schedules.

| Unsafe site | Protected state and invariant | Ordering and safe callers |
| --- | --- | --- |
| `unsafe impl Sync for Slot<O>` | A single publisher writes `outcome`; shared readers borrow an immutable completed `O`; `O: Send + Sync` permits shared clones. `Arc` owns reclamation. | `Slot::complete` writes first, then Release `fetch_or(COMPLETED | OUTCOME_VALID)`; `Observation::try_get`, `wait`, `wait_timeout`, and `ObservationFuture::poll` read only after Acquire or SeqCst observes `COMPLETED`. |
| `Slot::set_outcome` and its cell write | The sole `Subject` or consuming `Publisher` writes the initially uninitialized `outcome` exactly once. | Called only from `Slot::complete`, before its Release publication; a second `Subject::complete` fails the completed-state assertion. |
| `Slot::outcome_ref` and its cell read | `outcome` is initialized and remains live while a shared observation holds an `Arc`; no unique take or pooling can coexist. | The four shared observation methods above first acquire `COMPLETED`; `OUTCOME_VALID` remains set until the last reader is gone. |
| `Slot::drop_outcome` and its cell drop | Only a valid, still-owned outcome is dropped once. | `Slot::reset` runs under the entries write lock after the subject is retired and the pooled slot has no other observer; `Slot::drop` runs on final `Arc` destruction. Both check `OUTCOME_VALID`; `reset` clears it with a Release store. |
| `Slot::try_take_outcome` and its cell read/move | Only the sole affine observation or a successful `Arc::try_unwrap` may move `O`; a second poll is rejected. | Acquire load observes `COMPLETED`, Relaxed `fetch_and(!OUTCOME_VALID)` transfers drop ownership, then the initialized cell is moved. `AffineObservation::poll` and `Observation::into_outcome` are the only callers; the latter first proves exclusive `Arc` ownership. |
| `unsafe impl Send for Publisher<O>` | The noncloneable publisher moves unique write authority and `O: Send` across threads; it never reads the cell. | Safe `Publisher::complete(self, O)` consumes the authority and calls `Slot::complete`. |
| `unsafe impl Send for AffineObservation<O>` | The noncloneable affine handle moves sole read/take authority with `O: Send`; the outcome stays in the heap slot while pending. | Safe `affine_pair`, `AffineObservation::poll`, and its registration drop preserve uniqueness; `FutureRegistration` updates/deregisters wakers under the waiter mutex. |

The waiter mutex protects inline/vector registrations. Registration sets
`HAS_WAITER` with SeqCst, updates under the mutex, then rechecks `COMPLETED`
with SeqCst; the publisher's Release RMW and mutex drain prevent a lost wake.
The entries read/write lock protects key-to-generation mapping and pooled
slots. `Subject::drop` removes only its exact generation, then checks strong
count before pooling; no new observer can enter while the write lock is held.
The private fixed retained-key hasher is acceptable only for Bombay's trusted
internal keys. `ObservationSpace` is private in `bombay-rs` and exported only
from the unpublished `observe-tests` verification package; a production
public export would require a collision-hardened key policy review.

- Result: `DispatchWait` now projects with `Pin::get_mut` and `Pin::new`.
  Its private unconditional `Unpin` impl is sound because no field is
  structurally pinned or self-referential; the observation's pending state is
  heap-stable. Removing that impl while retaining the safe projection fails
  `cargo check` at `Pin::get_mut` for a possible `!Unpin` `EntityId<I>`
  (exit 101); the impl was restored. The cancelled-admission regression passes
  debug and release. Miri 0.1.0 / nightly 2026-06-15, seeds 0–3, passed
  `completion_racing_cancellation_drops_outcome_exactly_once`,
  `recycled_slot_never_leaks_previous_generation`,
  `cancelled_migrated_future_fires_neither_waker`, and
  `state_after_panicking_drain_stays_consistent`. The optimized Observe Loom
  suite with `LOOM_MAX_PREEMPTIONS=3` passed all 28 tests, including
  publication, waiter registration, cancellation, pooling, and reuse.
  `panicking_waker_must_not_strand_other_waiters` failed under Miri seed 2 at
  its sleep-based `is_finished` readiness assertion; its registration and
  completion ordering rests on 50/100 ms sleeps and is tracked under
  TEST-011. It is not accepted as safety evidence for this item. The
  deterministic panic/drop probe above passed all four seeds. Locked
  workspace tests, strict Clippy, rustfmt check, and diff check pass.
  Stage delta: production +10/-6/net +4, tests +0/-0/net 0, public named
  types +0/-0. This is a net-positive safety change, not code reduction.

## TEST-006 feature verification and change ledger (2026-09-28)

- State: `verified` after TEST-001. `Cargo.lock` selects Behavior Core/Actors
  0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, and patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; Observe is Bombay-private.
  The exact Behavior instructions, `Actions`, `Interpretation`,
  `SettlementStatus`, `SourceCustody`, child creation products, Behavior Actors
  public template boundary, Engine Driver/Environment source, property/fuzz
  targets, deterministic source/creation/terminal tests, normative Driver law,
  test strategy, and fuzz package manifest were inspected. Engine owns only
  causal order and custody; Behavior owns the typed products; Bombay's
  Address/Communication/Observe/Timers remain external and unchanged.
- Exact blocker: property and fuzz targets both replay an accepted byte stream
  with one sentinel stop. They omit failures, source custody, terminal
  precedence, and typed effect diversity. The smallest regression is a typed
  generated scenario with a rejected or corrupt action followed by a stop or
  source admission; the old model cannot represent it. The new property
  oracle must derive expected complete traces independently from the Driver,
  while the fuzz target must use malformed/long operation streams and check
  structural invariants rather than copy the property oracle.
- Expected paths before any production edit: `driver_property.rs`,
  `fuzz/fuzz_targets/causal_turns.rs`, a shared decode-only grammar file if
  needed, fuzz corpus/verification gate, this ledger, and `docs/todo.md`.
  Production Engine +0/-0/net 0, public types +0/-0; test/fuzz growth may be
  substantial but should replace the redundant byte models rather than
  retain both. Reuse the existing Driver/Environment port and Behavior typed
  products; no Engine effect language or runtime machinery is justified.
- Final evidence: the typed recursive property model compares the complete
  ordered two-lane/creation interpretation, source and ordinary fold sequence,
  typed disposition, publication, and retirement custody. The fixed phase
  matrix and 128 generated cases pass in debug and optimized profiles. A
  temporary real Driver mutation mapping rejected initialization to corrupt
  failed the fixed matrix with the exact typed mismatch (exit 101); it was
  restored. The older byte model and its entailed deterministic replay were
  replaced. The fuzz target separately decodes malformed and long sequences,
  exercises pending source cancellation, and checks structural/drop invariants
  without duplicating the property oracle. Eight named seeds and a pinned
  2,048-run CI campaign retain the resulting corpus and any crash artifacts;
  the local campaign finished with 82 corpus entries and no crashes. Existing
  `driver_law` panic and cancellation-stage regressions own those separate
  non-returning execution boundaries. Complete workspace tests, strict Clippy,
  rustfmt, and whitespace checks pass.
- Checkpoint: production `+0/-0/net 0`; public API `+0/-0` types; the property
  file is `+579/-83` against HEAD and the fuzz target is `+292/-84` against
  HEAD (the latter includes its earlier uncommitted Behavior 0.17 migration).
  The bounded script, eight binary seeds, and 12-line CI addition are new.
  Complete tracked/untracked worktree: 119 changed paths, including earlier
  task stages and user-owned changes; the user authorized this expanded surface.

## TEST-007 feature verification and change ledger (2026-09-28)

- State: `verified`, the lowest-sequence ready P1 item after TEST-006. The exact
  selected dependency versions remain Behavior Core/Actors 0.17.0 and Macros
  0.12.0 at `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0, and Bombay-private Observe. The
  locked Behavior creation source and tests confirm `CreationSequence` owns
  opaque occurrence-local IDs and `CreateChild::into_parts` exposes the exact
  interpreted ID and replacement provenance. The Engine Driver, test adapter,
  full Driver-law test file, allocation/source-order/custody tests, normative
  law, strategy, and current module/capability docs were inspected. Behavior
  owns creation identity and action products; Engine owns causal execution;
  this item changes only Engine test evidence, not any neighbor contract.
- Exact blocker and smallest failing regression: the full causal transcript
  already entails two projections over the identical `[1, 2, 9, 100]` input.
  The initialization loop discards every disposition; a driver that reports a
  wrong terminal class while still initializing once passes it. The creation
  test predicts numeric IDs using a second `CreationSequence` and discards one
  issued value; an interpreter that changes result order can evade its weak
  relation to the actual interpreted IDs. Replace the loop with complete
  disposition and trace cases, and assert creation IDs and replacement relation
  from the real interpretation transcript and final Behavior state. A targeted
  mutation to the tested Driver classification must fail the new regression.
- Planned stage: remove the two entailed projection tests; edit only
  `crates/bombay-engine/tests/driver_law.rs`, this ledger, and `docs/todo.md`.
  Expected production `+0/-0/net 0`, public API `+0/-0` types; test delta
  should be small and preferably negative. Reuse `Probe`, `Env`, the existing
  test adapter, `CreationScopeBehavior`, `CreationScopeEnv`, and the three
  distinct allocation/source-order/custody test targets. No wrapper, new
  product, or Driver code is justified.
- Result: the full transcript remains and the two identical-input count/fold
  projections are gone. Five initialization terminal cases now compare exact
  typed dispositions, residual, and entire ordered transcripts. The creation
  result test derives both IDs from actual interpretation and checks the
  replacement link, source result order, final Behavior observation, and
  complete trace shape. Changing the initialization send `0` to `42` made the
  focused new test fail (exit 101) while its old count assertion remained
  satisfied; reversing interpreted creation result order made the new relation
  fail (exit 101). Both mutations were restored. All 28 Driver-law tests pass
  in debug and optimized builds; strict Engine all-target Clippy, workspace
  rustfmt, and diff whitespace pass. Production `+0/-0/net 0`; public API
  `+0/-0` types. `driver_law.rs` is `+157/-129` against HEAD, including earlier
  uncommitted work. Complete tracked/untracked worktree remains 119 paths.
- Follow-up gate correction: the subsequent full workspace run found stale
  TEST-007 function names in `driver-law-manifest.json` and the manifest's
  stale-reference test. Both now reference the retained exact initialization
  witness. The focused 9-test manifest suite passes, and the D-INIT-1 evidence
  runner kills its duplicate production initialization mutation using that
  witness. No Driver implementation changed.

## TEST-024 feature verification and change ledger (2026-09-28)

- State: `verified`, the lowest-sequence ready P1 item after TEST-007. The exact
  locked Behavior Core/Actors 0.17.0, Macros 0.12.0 revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0, and private Observe remain the
  surrounding build contract. This item owns only the `mutants-gate` tooling
  boundary. The complete verdict source/tests, current baseline, actual
  `mutants.out` record shapes, Nix mutation derivation, Driver test strategy,
  and adjacent mutation queue items were inspected. `cargo-mutants` owns report
  generation; `mutants-gate` owns fail-closed interpretation; no Behavior,
  Engine, or Bombay runtime product changes.
- Exact blocker: `usable` accepts a missing baseline, `tallies` puts mutant
  `Success` and `Failure` into `total` without classifying them, and `check`
  does not reject stale `known_zero_viable` entries or zero-viability claims
  that gained viable candidates. The smallest end-to-end regression is a
  complete candidate file with one mutant `Success`, no baseline, and a
  zero-viable baseline entry: the old gate can return success and can emit a
  poisoned zero-viable baseline. Add table-driven cases first and establish
  these fail against the old parser.
- Expected files before production edit: `crates/mutants-gate/src/main.rs`,
  this ledger, and `docs/todo.md`, with optional current mutation strategy
  wording. The actual cargo-mutants candidate and outcome JSON both supply a
  stable `name` for each candidate; exact name matching is needed because
  per-function counts can conceal a duplicate result and a missing candidate.
  Expected production `+80/-25/net +55` lines, tests up to
  `+200/-35/net +165`, public API `+0/-0` types. Reuse the current `Report`,
  `Outcome`, `Summary`, `Scenario`, `Tally`, and `Baseline`; no new wrapper,
  parser, framework, or runtime abstraction is justified.
- Result: the gate requires exactly one successful baseline and exact one-to-one
  outcome matching by cargo-mutants candidate name, rejects `Success`/`Failure`
  mutant statuses and unknown JSON statuses, and checks stale, duplicate,
  conflicting, and newly viable known-zero entries. `emit-baseline` now reads
  the candidate inventory too and refuses incomplete, surviving, or timed-out
  reports; only actual unviable outcomes can seed zero viability. The new
  malformed-report tests failed against the prior parser because missing
  baseline and stale known-zero data were accepted (exit 101). Five tests pass
  in debug and optimized builds. The preserved local `mutants.out` report is
  correctly rejected for its missing baseline (exit 1). Strict Clippy, rustfmt,
  and whitespace checks pass. Production `+98/-45/net +53` tool lines, tests
  `+146/-14/net +132`, public API `+0/-0` types. Complete tracked/untracked
  working tree: 120 paths, including preserved prior changes.

## TEST-021 feature verification and change ledger (2026-09-28)

- State: `verified`, the lowest-sequence ready P2 row after TEST-024. The exact
  selected Behavior Core/Actors 0.17.0 and Macros 0.12.0 revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0, and private Observe remain in
  force. Behavior Actors owns the distinct actor `Machine` template and its
  receive/become/defer/stop tests. ARC-004 owns the Entity-specific slot
  transition, ordered effects, poisoning, and Loom proof. The deleted Machine
  crate, Cargo workspace/lock references, current Entity source/tests,
  compile-fail absence fixture, module boundaries, capability guide, Driver
  law, and upstream Actor Machine source/tests were inspected.
- Exact blocker and smallest regression: unowned generic `Decision` mapping
  and executor evidence once survived without any second production consumer.
  ARC-004 removed the entire package. The existing downstream
  `entity_cannot_import_machine` compile-fail fixture passed before removal
  only in the wrong direction (the import resolved, so trybuild rejected it)
  and now proves absence. This item must audit all nonhistorical source,
  manifests, examples, tests, benchmarks, and current docs; any retained
  `bombay_machine` import or workspace package would fail the absence law.
- Expected stage: no production edit, no new type, no new test unless the audit
  reveals a genuine consumer. The audit found the obsolete future-tense
  Transition removal section in `docs/driver-law.md`; replace it with the
  present owner boundary. Only that document, this ledger, and the queue
  should change. Reuse
  the existing compile-fail fixture and ARC-004 production Loom/inversion
  results. Expected production `+0/-0/net 0`, tests `+0/-0/net 0`, public API
  `+0/-0` types in this stage.
- Result: pinned Cargo metadata reports 14 workspace packages and no
  `bombay-machine`; source/manifests/examples/benchmarks/current-doc scans find
  only the intentional unresolved-import fixture and Engine's absence guard.
  The downstream compile-fail test passes in debug and optimized builds.
  `docs/driver-law.md` now states the present Engine/Entity/Behavior Actors
  boundary instead of instructing a future removal. No production or test
  line changed in this stage, and no public type was added. Complete
  tracked/untracked worktree remains 120 paths. ARC-015 is newly ready.

## ARC-015 feature verification and change ledger (2026-09-28)

- State: `active` after ARC-004, ARC-014, TEST-009, TEST-010, and TEST-021 all
  reached satisfying states. `Cargo.lock` selects Behavior Core/Actors 0.17.0,
  Macros 0.12.0 at `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address
  0.2.0, Communication 0.1.2, patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and private Observe.
  Selected Behavior owns actor folds, closed `Actions`, creation, and
  settlements; Behavior Actors owns its separate `Machine`, supervisor,
  discovery, shutdown, and timing templates. Bombay Entity owns stable-key
  hydration/admission, the concrete slot transition, directory interpretation,
  and family task join. The exact owner source/tests, current Entity facade,
  pure slot, directory, runtime port, family API, application interface,
  examples, benches, module boundaries, capability guide, and upstream owner
  APIs were inspected. The Entity/Actors overlap table (ARC-007), independent
  transition oracle (ARC-003/TEST-010), package removal (ARC-004/TEST-021),
  and task-group law (ARC-014) settle the prerequisite contracts.
- Inventory finding: `EntityDefinition`, `Entities`, `EntityRef`, `EntityId`,
  capacity, admission and terminal products are ordinary caller-facing API.
  `EntityRuntime` and `LocalEntityRuntime` are deliberate advanced host ports.
  `LocalDirectory`/`EffectInterpreter` and their exact installed output types
  currently have only internal integration tests and benchmarks as external
  callers; they require a conscious advanced-port decision. Slot phase structs,
  `SlotDecision`, `SlotEffectBatch`, and `LifecyclePhase` have no external named
  caller. `EntitySlot`, `SlotEvent`, and edge/evidence values occur only in an
  internal benchmark/Loom fixture. Broad `private_bounds` in `family.rs` and
  `dead_code` on the private native adapter remain. No surviving reducer,
  topology, or executor public type belongs to Bombay.
- Exact blocker and smallest regression: the facade still reexports private
  lifecycle representation. A downstream compile-fail fixture importing
  `entity::ActivatingSlot` and other mechanically owned phase/decision names
  currently compiles, so it must fail against the prior representation and
  pass after those names become inaccessible. A matching compile-pass fixture
  must use the ordinary family API and deliberately supported advanced port.
  Observable directory/Entity transition tests must remain unchanged.
- First independently reviewable stage before production edits: add the
  compile boundary fixture, remove only the mechanically private zero-caller
  facade reexports, and narrow the native `dead_code`/family `private_bounds`
  allowances only where diagnostics identify their exact need. Expected paths:
  `crates/bombay/src/entity/{mod,family}.rs`,
  `crates/bombay/tests/{entity_machine_boundary.rs,compile/{pass,fail}/*}`,
  this ledger, queue, and current user/API docs (at most 9 paths for the first
  stage). Expected production `+0/-15/net -15` lines, tests
  `+30/-0/net +30`, public API `+0/-9` named types. Reuse the existing
  Entity definitions, runtime port, directory and pure transition; add no
  wrapper, builder, actor contract, or macro. A later stage may internalize
  the directory and pure slot after their tests/benchmarks move to the owning
  crate; measure the complete working tree before any broader production edit.
- First-stage proof: the downstream compile-fail import of `ActivatingSlot`,
  `ActiveSlot`, `DrainingSlot`, `SlotDecision`, `SlotEffect`, and
  `SlotEffectBatch` compiled before the edit, making trybuild fail for the
  intended public-boundary law (exit 101). All six names are now inaccessible
  through `bombay::entity`; a positive downstream fixture still typechecks the
  ordinary `Entities`/`EntityRef`/`EntityCapacity` and advanced
  `LocalEntityRuntime`/`EntityRuntime` spellings. Both fixtures pass in debug
  and optimized profiles, the complete Bombay package passes, and strict
  workspace Clippy, rustfmt, and whitespace pass. Removing the module-wide
  `private_bounds` and native-adapter `dead_code` allowances exposed only one
  private bound on `EntityAdmission<D>`; that exact impl now carries a scoped
  `expect` with its host-proof rationale. No new type or wrapper exists.
- First-stage checkpoint: task-local production reexport/lint change is net
  negative, with six fewer externally named lifecycle types; tests add four
  compile-fixture files totaling 77 lines. The complete tree has 124 changed
  or untracked paths; against HEAD the touched production files are
  `entity/mod.rs +3/-10` and `entity/family.rs +19/-14` (the latter includes
  preexisting ARC-014 edits). Public API `+0/-6` types this stage. The broader
  directory/pure-slot exposure decision remains active under this same ID.
- Second stage decision before editing: retain `LocalDirectory` and its
  `EffectInterpreter` as an explicit advanced integration/test-host kernel.
  The kernel uniquely owns exact-key slot installation, dispatch correlation,
  and an affine interpretation barrier; its production consumer is
  `EntityRuntime`, and its external tests/benchmarks exercise this same owner.
  Its installed decision products therefore remain nameable, but
  `DirectoryOutput` and `DispatchOutput` conceal their installed custody stage.
  Rename them to `InstalledSlotDecision` and `InstalledDispatch`, and rename
  the latter's public `output` field to `decision`. The existing concrete
  composition cannot express the installed barrier without those products;
  this rename adds no wrapper or policy and deletes the vague spellings.
  Expand the compile-pass fixture to name this deliberate advanced API.
  Expected paths: `entity/{directory,mod,runtime}.rs`, existing directory
  integration/Loom/bench callers, compile-pass fixture, current docs, and this
  ledger (up to 12 paths). Expected production `+0/-0/net 0` semantic lines,
  tests mechanical substitutions only, public API `+2/-2` renamed types and
  one renamed field, with no additional type cardinality.
- Complete facade inventory (each group lists every externally nameable Entity
  type/trait after the two stages):

  | Classification | Exact names | Concrete caller or required public contract |
  | --- | --- | --- |
  | Ordinary family API | `EntityId`, `EntityDefinition`, `Entities`, `EntityRef`, `EntityCapacity`, `EntityMetrics`, `EntityActivationError`, `EntityAdmission`, `AdmissionFailure`, `EntityShutdown`, `Passivation` | Entity example, `App`/external actor interface, and native family integration tests |
  | Ordinary input/fact products | `DirectoryConfig`, `DirectoryError`, `ActivationId`, `DrainFailure`, `DrainStage`, `Refusal`, `FenceFailure` | `App::entity_family` and `EntityDefinition` callbacks; exact admission/drain errors |
  | Sealed application proof | `EntityApplicationFamilies`, `EntityFamilyAt` | public `App` associated bounds; their private `Sealed` supertraits deny external implementations |
  | Advanced runtime port | `EntityRuntime`, `LocalEntityRuntime`, `Activated`, `RetirementMode` | custom runtime integration tests and the native adapter; `LocalEntityRuntime` is deliberately implementable |
  | Advanced directory/test-host kernel | `LocalDirectory`, `EffectInterpreter`, `InstalledSlotDecision`, `InstalledDispatch`, `DispatchId`, `LifecyclePhase`, `TransitionEvidence` | `EntityRuntime`, directory integration/Loom tests, and the directory benchmark; concrete installed decision/dispatch custody |
  | Advanced pure-fold model | `EntitySlot`, `SlotEvent`, `LifecycleEdge` | independent transition oracle and separate Criterion pure-fold benchmark; no second behavior contract |
  | Private mechanism removed from facade | `ActivatingSlot`, `ActiveSlot`, `DrainingSlot`, `SlotDecision`, `SlotEffect`, `SlotEffectBatch` | only the owning Entity transition/directory implementation uses them; downstream import is statically denied |

  The directory and pure-fold groups are deliberate advanced integration and
  test-host surfaces, not ordinary application prelude items. No public
  reducer, topology, executor, or installation-proof type remains. The native
  host proof stays `pub(crate)`; broad native dead-code suppression is gone;
  the one private-bound exception is scoped to the exact `EntityAdmission<D>`
  impl. The only open traits are the intentional `EntityDefinition`,
  `LocalEntityRuntime`, and `EffectInterpreter` extension seams.
- Feature result: `feature-complete`. Six lifecycle mechanism names are no
  longer exported, two installed directory products and their dispatch field
  have custody-revealing names, and broad Entity lint allowances are gone.
  The compile-fail fixture first failed against the prior public surface
  (exit 101); it and a positive ordinary/advanced fixture now pass in debug
  and optimized builds. Focused directory/runtime/allocations/stress tests,
  optimized production Entity Loom (6/6), both benchmark builds, full locked
  workspace tests/build, strict workspace Clippy, rustfmt, and whitespace
  pass. `LocalDirectory` and pure `EntitySlot` remain advanced test-host
  surfaces with concrete runtime, oracle, and benchmark consumers. The
  complete tracked/untracked checkpoint is 136 files against HEAD:
  production source files `+3195/-3745/net -550`, test/bench/example/tool
  files `+4255/-2324/net +1931`, other files `+7098/-1871/net +5227`.
  These file-class totals include earlier work and count tests embedded in
  `src` files as source-file lines. Public named API in this stage is two
  renames plus six removals (`+2/-8`, net minus six); no new type cardinality.
  Final project-wide minimization remains under ARC-020.

## TEST-018 feature verification and change ledger (2026-09-28)

- State: `active`, the lowest-sequence ready P2 item after ARC-015. The exact
  `Cargo.lock` selects Behavior Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0, and Bombay-private Observe.
  Behavior/Actors own actor folds/templates; Address, Communication, and Timers
  retain their adapter contracts; Observe owns its own atomic state, tests,
  Loom model, and private probe. The selected owner source/tests, both Observe
  crate manifests, path-import harness, fuzz/perf dependents, Nix isolated
  checks, current module/capability docs, and the ARC-017 unsafe inventory
  were inspected. No upstream contract blocks test ownership selection.
- Exact blocker and smallest regression: `observe-tests` path-imports
  `bombay/src/observe/mod.rs` while building its ordinary lib test, so the
  same module-local `tests` and `external_tests` run once in Bombay and once in
  the isolated crate. A test-list count and a duplicate name from both
  binaries must demonstrate this prior representation. Keep Bombay as the
  single ordinary test owner; compile the isolated implementation for Loom
  lib tests and as a normal dependency for fuzz/performance, but omit the path
  import from the isolated crate's ordinary lib-test configuration.
- Expected paths before production edit: `crates/observe-tests/src/lib.rs`,
  this ledger, and `docs/todo.md`; possibly Nix check wording if the existing
  isolated ordinary check becomes empty. Production Observe `+0/-0/net 0`,
  test-harness source under `+12/-8/net +4`, public production API
  `+0/-0` types. Reuse the one owning Observe module and its existing
  configuration-specific Loom/fuzz/perf harnesses; no new cell, wrapper,
  public product, or actor/runtime abstraction.
- Resolution and inversion: before the harness change, both pinned-Nix lib
  test listings contained the same 121 ordinary Observe cases, including the
  affine-handle move regression. Gating the isolated module import to
  `not(test)` or `loom` leaves zero ordinary isolated cases and retains all
  121 in Bombay. The isolated Loom build still lists and passes 28 optimized
  cases. Normal dependency builds for `observe-perf` and all `observe-fuzz`
  binaries compile the implementation and the shared probe. This is the
  complete retained source-reuse rationale pending ARC-019: configuration
  isolation for Loom plus independent fuzz/performance dependencies.
- Verification: full locked workspace tests, strict all-target Clippy,
  rustfmt, and whitespace passed. The Nix `bombay-observe-loom` check retains
  its meaningful 28-test workload; no Nix check previously ran isolated
  ordinary tests. `observe-tests/src/lib.rs` is `+3/-5/net -2` harness lines;
  production `+0/-0/net 0`, public types `+0/-0`. The complete tree contains
  137 changed or untracked file paths at this checkpoint; cumulative source
  file delta `+3198/-3750/net -552` against HEAD includes the harness.

## ARC-019 feature verification and change ledger (2026-09-28)

- State: `feature-complete`, after TEST-018 and ARC-017. `Cargo.lock` selects Behavior
  Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0, and Bombay-private Observe.
  Behavior/Actors own actor semantics and templates; Address, Communication,
  and Timers keep their own capability contracts. Observe alone owns the
  publication/waiting cell, ordinary and Loom tests, and test probes. The
  Bombay source import, isolated harness, fuzz/perf dependencies, Nix Loom
  check, Miri shell, current module/capability docs, and ARC-017 unsafe
  inventory were inspected. No dependency-contract blocker remains.
- Ownership alternatives: a separate unpublished `bombay-observe` crate would
  move every source and test path, manifests, and the Bombay dependency edge,
  plus alter the external test's `crate::observe` paths. It would make the
  private implementation's Rust exports cross a crate boundary and add a
  permanent package despite no distinct owner or behavior. The existing
  cfg/harness structure is the queue's allowed alternative: Bombay owns the
  ordinary tests, while `observe-tests` imports the same source only in its
  Loom lib-test build or as a normal library dependency for fuzz/performance.
  Its ordinary lib-test build imports neither implementation nor probe. This
  preserves the private Bombay boundary and minimizes total surface. The
  remaining path compilation in different configurations is purposeful;
  no ordinary test is recompiled or executed in the isolated crate.
- Smallest regression: TEST-018 established 121 duplicate ordinary test names
  before the cfg change. A listing inversion against Bombay and the isolated
  crate, a 28-case Loom listing, and downstream fuzz/performance builds prove
  this owner selection without new production machinery. `bombay` has no
  dependency edge to `observe-tests`; the private test package depends only on
  primitive libraries, and fuzz/performance depend on that test package.
- Expected paths: this ledger, `docs/todo.md`, and current module-boundary
  guidance; no further production edits or public types. Production
  `+0/-0/net 0`; tests `+0/-0/net 0` beyond TEST-018. Retain the broad
  `dead_code` allowance on Bombay's private Observe module until caller
  minimization in ARC-020 or a failing law independently requires removal;
  the duplicate-module allowance was removed by TEST-018.
- Verification: `cargo tree` through pinned Nix confirms the dependency edge
  from `observe-perf` to `observe-tests` and no edge from Bombay. The isolated
  28-case optimized Loom run, fuzz/performance builds, and ordinary listings
  passed under TEST-018. The Miri shell targets Bombay's ordinary owner; the
  flake Loom check targets the isolated cfg. A focused instrumented run of
  `cargo llvm-cov --locked -p bombay-rs -p observe-tests --lib --summary-only`
  passed with 171 active Bombay library tests, four ignored tests, zero
  isolated ordinary tests, and one `observe/mod.rs` coverage row. Its report
  collection initially found a generated trybuild manifest for removed
  `bombay-machine` under `target/tests/trybuild`; moving that ignored build
  artifact out of `target` allowed the same command to pass. This focused
  report is an accounting check, not a new workspace coverage floor. Complete
  workspace tests, strict Clippy, rustfmt, and whitespace passed in TEST-018.
  Cumulative production `+0/-0/net 0` for ARC-019, tests `+0/-0/net 0`,
  public types `+0/-0`; the complete tree remains 137 changed or untracked
  paths with source-file delta `+3198/-3750/net -552` against HEAD.

## TEST-011 feature verification and change ledger (2026-09-28)

- State: `verified`, the lowest-sequence ready P1 after ARC-019. `Cargo.lock`
  selects Behavior Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0, and Bombay-private Observe.
  Behavior/Actors own folds and templates; Address, Communication, Timers,
  and Observe retain their separate capability contracts. Entity's test
  runtime owns task events; Observe's private registry owns registration
  facts. The current Entity runtime tests, Observe ordinary/panic/stress
  corpus, ARC-017 Miri finding, Loom isolation, and runtime/module/Driver
  documents were inspected. No upstream contract blocks test ordering repair.
- Exact blocker and smallest failing regressions: four Entity tests infer
  activation, delivery, or retirement completion from repeated 1 ms sleeps;
  the Observe panic regression infers parked registration and completion from
  50/100 ms sleeps and failed Miri seed 2 at `is_finished`; the zero-timeout
  test asserts a 100 ms wall-clock ceiling. Removing these sleeps/ceiling
  before synchronization exposes a missing causal signal. Test-only event
  condition variables and exact Observe waiter-registry probes will establish
  the needed order; bounded waits are watchdogs only. Other 1/5/10/20 ms
  sleeps in Observe tests are inventoried for the same treatment, including
  duplicate registration and timeout-boundary cases.
- Expected files: `crates/bombay/tests/entity_runtime.rs`, private Observe
  `tests.rs`, `external_tests/panic_safety.rs`, `external_tests/stress.rs`,
  possibly test-only `test_support/mod.rs`, this ledger, and `docs/todo.md`.
  Production `+0/-0/net 0`, public types `+0/-0`; test-line delta expected
  under +150 net. Reuse the current `TestRuntimeState`, standard mutex and
  condvar, private Observe waiter registry, and existing typed outcomes. Add
  no runtime synchronization service or public probe.
- Resolution: the Entity test runtime now signals activation completion,
  delivery, and retirement under one mutex/condvar; six polling loops wait for
  the exact fact and use a five-second watchdog only for hangs. Observe's
  private test-only registration probe reads the actual thread entries and
  their order. The panic-waker test uses a completion channel, and the timed
  waiter test uses a deliberate independent unpark. The duplicate-registration
  stress test now carries the same waiter thread across two generations, so
  its stale unpark claim is exercised. Other Observe races use a registration
  channel or barrier; the zero-timeout test asserts result and empty registry
  without a wall-clock ceiling. No `sleep` call remains in those targeted
  Entity/Observe files. Production `+0/-0/net 0`, public types `+0/-0`.
- Verification: focused Entity tests passed 11/11 in debug and release; all
  121 ordinary Observe tests passed in debug and release; the isolated Loom
  suite passed 28/28 optimized. The previously failing panic-waker Miri case
  and the timed-waiter recovery case each passed seeds 0–3 under the pinned
  `.#miri` shell. The first Miri attempt stopped before test execution on a
  temporarily missing Nix `cargo-miri` runner; a shell binary inspection and
  retry of the same pinned command succeeded. Full locked workspace tests,
  strict all-target Clippy, rustfmt, and whitespace passed. The complete tree
  contains 138 changed or untracked paths at this checkpoint; including
  untracked files, production `+2909/-3662/net -753`, tests/tools/examples
  `+4737/-2538/net +2199`, other `+7281/-1867/net +5414` against HEAD.

## TEST-019 feature verification and change ledger (2026-09-28)

- State: `verified`, the lowest-sequence ready P2 after TEST-011. `Cargo.lock`
  selects Behavior Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0, and Bombay-private Observe.
  Communication owns mailbox delivery and backpressure; Bombay `ActorRef`
  owns the local ingress facade; Observe, Timers, Behavior, and Actors retain
  their unchanged contracts. The local test module, current Entity/Engine
  benches, Observe performance package, ignored-test inventory, Nix test
  gates, module and capability docs, and selected primitive APIs were
  inspected. No dependency contract blocks deleting measurement-only tests.
- Blocker and smallest regression: `local::tests::compare_direct_and_entity_capable_ingress`
  is ignored, performs seven million-operation rounds per variant, prints a
  median, and has no threshold. `cargo test -- --list` exposes it as an
  ignored ordinary unit test rather than a benchmark. The four timer helpers
  and `DirectActorRef` fixture exist only for that comparison. A true
  benchmark would require exposing private mailbox/ActorRef construction to
  a separate bench crate or duplicating private production composition. The
  queue explicitly permits removal; retain the separate Entity and Observe
  performance harnesses that already own their distinct workloads.
- Expected paths: `crates/bombay/src/local.rs`, this ledger, and
  `docs/todo.md`. Production `+0/-0/net 0`; test-only source roughly
  `+0/-160/net -160`; public types `+0/-0`. Remove the ignored case, four
  timing functions, one fixture, and now-unused timing imports. The three
  remaining ignored local tests have the explicit external Behavior
  post-commit/rejection prerequisite under ARC-006 and one named manual
  replay owner; do not silently count them as passing.
- Resolution: removed the ignored median printer, four timing functions,
  `DirectActorRef` fixture, and test-only time import. This deletes 163 lines
  from `local.rs`'s `cfg(test)` module and changes no executable production
  behavior or public type. The prior lib-test listing named the ignored
  comparison; the new ignored listing names only the three ARC-006
  commit-before-claim cases. Those have an external Behavior prerequisite
  documented under ARC-006/TEST-023 and one manual replay owner: the ARC-006
  implementer runs `nix develop -c cargo test --locked -p bombay-rs --lib
  local::tests:: -- --ignored` after the dependency is available. No current
  green result is claimed for them. Observe's five ignored rustdocs belong to
  TEST-017, not the ordinary unit-test inventory.
- Verification: 10 active local tests passed in debug and release, with the
  three blocked ignores intact; full locked workspace tests, strict all-target
  Clippy, rustfmt, and whitespace passed. The complete tree contains 138
  changed or untracked paths; physical production source-file delta
  `+2905/-3819/net -914` against HEAD includes the removed `cfg(test)` tail,
  separate test/tool/example files `+4737/-2538/net +2199`, other
  `+7349/-1867/net +5482`. Public API `+0/-0` types for this item.

## ARC-005 feature verification and change ledger (2026-09-28)

- State: `feature-complete`, the lowest-sequence ready P2 after TEST-019. `Cargo.lock`
  selects Behavior Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, and patched Timers 0.1.0. The exact Behavior and
  Behavior Actors source/tests/docs, Address/Communication/Observe/Timers
  contracts, Bombay macro source and fixtures, workspace/feature manifests,
  Nix gates, and current module/capability docs were inspected. Behavior
  owns actor expansion; Bombay macros own only the outer syntax facade and
  two selected projections. No dependency contract blocks manifest cleanup.
- Blocker and regression: `crates/bombay/Cargo.toml` retains a `tokio` dev
  dependency with only `macros`, already present in the normal `tokio`
  dependency; the stale root exclusion names a nonexistent `crates/bombay/fuzz`
  package. `bombay-machine` was already removed by ARC-004. Cargo tree shows
  Bombay macros on Syn 2 and locked Behavior macros on Syn 3, but Syn 2 also
  remains through `zerocopy-derive` independently. Prototype changing the
  workspace Syn requirement to 3 and compile Bombay macros/fixtures before
  deciding whether the split is necessary; compiler diagnostics may reject
  the migration but do not justify new macro architecture.
- Expected paths before production edit: root `Cargo.toml`, Bombay manifest,
  perhaps Bombay macro source only if a small compatible Syn migration is
  demonstrated, `Cargo.lock` only if resolution changes, this ledger, and
  `docs/todo.md`. Expected production source `+0/-0/net 0`; manifests at most
  `+1/-4/net -3`; public types `+0/-0`. Reuse Cargo's existing feature
  unification and owning macro expansion; add no wrappers, new macros,
  test-only manifest dependencies, or public surface.
- Resolution: removed the nonexistent `crates/bombay/fuzz` exclusion and
  redundant Tokio dev dependency; ARC-004 had already removed both
  `bombay-machine` dependencies. A Syn 3 prototype compiled Bombay macros
  unchanged after offline lock resolution, so the root workspace now selects
  Syn 3 for the first-party macro crate. Syn 2 remains independently required
  by `zerocopy-derive`. The locked initial prototype failed only because the
  lockfile still named Syn 2; its subsequent pinned offline resolution and
  locked build passed. Macro compile fixtures, renamed downstream crate, and
  debug/optimized macro/terminal tests all passed. Complete locked workspace
  build and tests, strict all-target Clippy, rustfmt, and whitespace passed.
  No public type or Rust source changed. This item's manifests are
  `+1/-4/net -3`, lockfile macro dependency `+1/-1/net 0`.
- Complete-tree checkpoint: 138 changed or untracked paths, production source
  `+2905/-3819/net -914`, test/tool/example files `+4737/-2538/net +2199`,
  other `+7416/-1871/net +5545` against HEAD before this ledger record.

## TEST-022 feature verification and change ledger (2026-09-28)

- State: `verified`, the lowest-sequence ready P2 after ARC-005. `Cargo.lock`
  selects Behavior Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, patched Timers 0.1.0, and Bombay-private Observe.
  Behavior owns ActionItem and the actor expansion; Behavior Actors owns the
  request contracts; Bombay's macro facade derives syntax only. Selected
  Behavior/Actors source/tests/docs, Bombay macro implementation and current
  differential/renamed-downstream fixtures, the capability/module/Driver
  documents, and the exact two dead references were inspected. No dependency
  contract blocks correcting documentation provenance.
- Smallest regression: `rg -n '\.research|MACRO-LAST-COMPARISON' docs README.md
  crates examples` finds one removed 0.15-era caller-probe path in the ledger,
  one missing macro comparison file in current user-facing guidance, and
  historical audit text in `docs/todo.md`. Replace the current guidance with
  links to retained executable macro evidence. Preserve the historical
  ActionItem coherence conclusion in prose without implying that a removed
  probe remains selected under the locked 0.17 contract.
- Expected paths: `docs/open-design-ledger.md`, `docs/user-facing-api.md`,
  `docs/todo.md`; production `+0/-0/net 0`, tests `+0/-0/net 0`, public types
  `+0/-0`. No new documentation file or implementation abstraction.
- Resolution: the 0.15 ActionItem coherence conclusion remains explicitly
  historical prose without a dead probe path or obsolete selected-gate claim.
  Current `user-facing-api.md` links directly to the plain-builder/differential
  macro test, compile fixtures, renamed downstream fixture, and facade source.
  The required `rg` scan finds only intentional queue/historical mentions;
  a scan of relative Markdown links in `docs/` and README found zero missing
  local targets. No executable or public API changed. Full locked workspace
  build/tests, optimized macro fixtures, strict Clippy, and rustfmt already
  passed after the last Rust/manifest edit under ARC-005; whitespace passed
  after this documentation change. The complete tree had 138 changed or
  untracked paths at this checkpoint, production `+2905/-3819/net -914`,
  tests/tools/examples `+4737/-2538/net +2199`, other
  `+7486/-1882/net +5604` against HEAD before this record.

## Ownership map

- Behavior owns the deterministic `Behavior -> Actions` algebra, closed typed
  products, named capability requests, initialization, activation, atomic
  action settlement, source-result custody, and the owning behavior macro.
- Behavior Actors owns reusable actor templates and their topology,
  supervision, shutdown, timing, terminal, pool, and customer-route policies.
- Engine owns only the universal causal `Driver` and affine `Environment`
  protocol.
- Address owns endpoint allocation, claim, resolution, and exact lease
  retirement.
- Communication owns two-lane mailboxes, admission, backpressure, closure, and
  exact rejected-payload recovery.
- Observe owns private retained and affine completion mechanics.
- Timers owns branded generation-safe scheduling state.
- Bombay owns concrete local composition, actor and Entity tasks, capability
  interpretation, application activation and retirement order, external actor
  boundaries, and exact terminal custody.
- Mnesis-Bombay owns aggregate hydration, durable command execution, append and
  conflict policy, and durable outcomes.

## DX53 — atomic Behavior migration

- State: `distilled`.
- Requirement: consume the selected Behavior atomic settlement algebra without
  preserving a second compatibility runtime or recreating owner semantics.
- Result: the Driver returns the final behavior and environment residual;
  Bombay interprets complete action settlements, preserves exact source and
  child custody, and returns role-indexed terminal trees through one retirement
  barrier.
- Application result: `Application::new(root)` is the ordinary single-root
  path. Named child composition uses `.child(Role, actor)`. Advanced concrete
  hosting uses `App` and `ActorSpaces` deliberately rather than through an
  erased or inferred registry.
- Authoring result: `#[bombay::actor]` supplies Bombay's fixed local boilerplate
  and delegates to the Behavior-owned macro. It defines no second actor
  contract. `ActorExt` returns the existing Behavior Actors template types.
- Public-surface result: the migration adds `DriverRetirement`, removes eight
  obsolete topology types and one forwarding function, and removes unsupported
  foundational names from the ordinary prelude.
- Invariants: every accepted, rejected, blocked, corrupt, and unattempted item
  retains exact typed custody; creation precedes dependent delivery; no fold is
  re-entered while commitment is pending; and every terminal edge retires once.
- Inversions: compile fixtures reject missing capabilities, wrong protocols,
  foreign roles, duplicate roles, reinitialization, lifecycle authority through
  actor interfaces, and structural machinery through the ordinary prelude.
- Blocked by: none.
- Unblocks: DX49.

## DX49 — application-native Entity

- State: `distilled`.
- Requirement: install nominal asynchronous Entity definitions in the
  application, preserve truthful origin and typed lifecycle facts, and settle
  every admitted operation and lifecycle task during application shutdown.
- Result: `EntityDefinition` owns hydration and the typed failure, refusal,
  forced-retirement, and final-retirement policies. `Entities<D>` owns the
  family receptionist; `EntityRef<D>` binds one stable domain identity.
- Capacity result: waiter, hydration, and resident bounds remain distinct.
  Hydration finishes before address allocation and routability. Capacity
  refusal, hydration failure, and actor launch failure are separate facts.
- Runtime result: external and actor-originated admissions retain their actual
  caller address. Actor-originated admission crosses Behavior's exact
  `InterpreterRequests` lane. A private receptionist prevents native host proof
  from recurring through actor messages.
- Lifecycle result: passivation fences the exact incarnation; the same stable
  reference can activate a replacement; stale facts cannot retire that
  replacement; forced drain retains the complete `DrainFailure`; and child
  terminal custody reaches the definition policy.
- Shutdown result: Bombay settles the root first, closes every family, drains
  every represented slot, joins every Entity task, and returns typed family
  shutdown summaries plus bounded metrics with zero residents.
- Application result: advanced `App::entity_family`,
  `ApplicationHandle::entities`, `ApplicationHandle::passivate_entity`, and
  `App::run_with_entities` form the one native path. Standard tuples and
  Behavior positions carry the static family product.
- Public-surface result: seven contracts were added—`EntityDefinition`,
  `EntityCapacity`, `EntityActivationError`, `EntityMetrics`,
  `EntityAdmission`, sealed `EntityApplicationFamilies`, and sealed
  `EntityFamilyAt`. The obsolete `EntityFamily` and
  `EntityFamilyInstallation` contracts were removed.
- Change containment: the native stage touched 45 logical files, changed
  production by net `+373` lines against its recorded pre-stage baseline, and
  stayed below its authorized `+400` ceiling with public API fixed at `+7 / -2`
  types.
- Inversions: no `dyn`, `Any`, `TypeId`, boxed future, dynamic host registry,
  callback registry, global allocator, target-as-origin delivery, public
  Observe primitive, polling shutdown, or abort-only normal owner satisfies
  the contract.
- Blocked by: none.
- Unblocks: downstream Mnesis-Bombay adoption.

## DX54 — Triomphe feature minimization

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: Behavior Core 0.14.0, Behavior Actors 0.14.0, and
  Behavior Macros 0.11.4 at
  `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`; Address 0.2.0;
  Communication 0.1.2; Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; and Bombay-private Observe.
- Requirement: retain Triomphe's weak-free `Arc` ownership in Observe without
  selecting unrelated optional integration contracts.
- Ownership: Observe owns completion synchronization and uses only
  `triomphe::Arc`. Triomphe owns that allocation and reference-counting
  primitive. No Bombay production or test consumer uses Triomphe's Serde or
  `StableDeref` integrations.
- Exact blocker and regression: Triomphe 0.1.16 enables `serde` and
  `stable_deref_trait` by default, so the locked normal dependency graph
  contains unused edges. The smallest end-to-end regression is the normal
  `cargo tree` graph: before the change both edges originate at Triomphe;
  afterward neither may do so, and Observe's public behavior must remain
  unchanged.
- Dependency edges: independent of DX53 and DX49. No downstream contract
  depends on either optional integration.
- Blocked by: none.
- Unblocks: none.
- Change ledger: expected tracked files are the root `Cargo.toml`,
  `Cargo.lock`, and this ledger. Expected production source delta is
  `+0 / -0 / net 0`; expected test delta is `+0 / -0 / net 0`; expected public
  API is `+0 / -0` types. The experiment reuses the existing
  `triomphe::Arc`, adds no owner or interpreter, and deletes only unused
  dependency feature edges. Any required source change or verification failure
  falsifies the experiment.
- Result: Triomphe remains at the exact locked 0.1.16 release with only its
  `std` feature. Its unused `serde` and `stable_deref_trait` edges are absent
  from Bombay's normal graph, and `stable_deref_trait` is absent from the
  lockfile. No Rust source or public contract changed.
- Verification: the dependency-tree regression, 122 focused Observe tests,
  the complete locked workspace test suite, formatting, and strict all-target
  workspace Clippy pass through the pinned Nix shell.
- Actual checkpoint: tracked files `3`; production source
  `+0 / -0 / net 0`; tests `+0 / -0 / net 0`; public API
  `+0 / -0` types; manifests `+1 / -11 / net -10`; documentation
  `+45 / -0 / net +45`.

## DX55 — module-scoped test imports

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the complete owner graph recorded above, including the
  exact locked Behavior and Timers revisions, was rechecked before this
  repository-wide structural audit.
- Requirement: imports belong to their owning module scope. Test functions and
  generated bodies receive no exception.
- Ownership: each affected test module owns the names needed by its tests; no
  runtime owner, Behavior lane, or production interface changes.
- Exact blocker and regression: an AST query finds 12 functions containing 19
  block-local `use` declarations across Machine executor tests, Observe tests,
  and Entity error tests. The same query must find zero after the change while
  the complete affected behavior remains unchanged.
- Dependency edges: independent of DX53, DX49, and DX54.
- Blocked by: none.
- Unblocks: none.
- Change ledger: expected tracked files are this ledger and six test-bearing
  Rust files. Expected production source delta is `+0 / -0 / net 0`; test code
  is expected to be net-negative by moving and merging imports; expected public
  API is `+0 / -0` types. Existing modules and names are reused; no wrapper,
  trait, owner, interpreter, or dependency is added. Any cfg-scope change,
  ambiguity, production/API edit, or verification failure falsifies the
  experiment.
- Result: all 19 imports moved into their six owning test modules and repeated
  declarations were merged. The repository-wide AST query now finds zero
  function-local imports in crates or examples.
- Verification: affected Machine, Observe, and Entity error tests, the complete
  locked workspace suite, formatting, and strict all-target workspace Clippy
  pass through the pinned Nix shell.
- Actual checkpoint: tracked files `7`; production source
  `+0 / -0 / net 0`; tests `+14 / -41 / net -27`; public API
  `+0 / -0` types; documentation `+37 / -0 / net +37`; complete tracked delta
  `+51 / -41 / net +10`.

## DX56 — Observe retained-key naming

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph remains unchanged.
- Requirement: private names must state the responsibility they own and must
  not claim false provenance. Observe's measured retained-key hashing policy is
  distinct from current rustc-hash and belongs only to its non-adversarial
  internal key table.
- Ownership: Observe owns its retained-key table and the measured hashing
  policy selected for that table; no public caller selects or observes the
  implementation.
- Governing invariant and regression: only private identifiers, formatting,
  and adjacent prose may change. The arithmetic operations and multiplier
  value remain identical; distribution, dependencies, behavior, and
  performance may not change. Existing promotion, model, allocation, and Loom
  tests remain the caller-visible regression.
- Dependency comparison: a separate `rustc-hash` 2.1.3 experiment passed all
  semantic gates but regressed five-run 8- and 16-thread medians by roughly
  9.5% and 9.9%, so it was reverted rather than retained as a nominal cleanup.
- Dependency edges: independent of DX53, DX49, DX54, and DX55.
- Blocked by: none.
- Unblocks: none.
- Change ledger: expected tracked files are `observe/mod.rs` and this ledger.
  Expected production source is approximately line-neutral; tests remain
  unchanged; expected public API is `+0 / -0` types. No wrapper, dependency,
  owner, branch, or algorithm may be added.
- Result: `RetainedKeyHasher`, `RETAINED_KEY_HASH_MULTIPLIER`, and
  `BuildRetainedKeyHasher` now name the private responsibility and the
  constant's actual role. The obsolete Fx/rustc-hash provenance claim is gone;
  arithmetic operations and the multiplier value are unchanged.
- Verification: both normal Observe suites (122 and 121 tests), formatting,
  and strict all-target workspace Clippy pass through pinned Nix. The preceding
  replacement comparison also passed all 28 isolated Loom models before the
  exact algorithm was restored.
- Actual checkpoint: tracked files `2`; production source
  `+16 / -13 / net +3`; tests `+0 / -0 / net 0`; public API
  `+0 / -0` types; documentation `+41 / -0 / net +41`; complete tracked delta
  `+57 / -13 / net +44`.

## DX57 — Machine output consumer

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph remains unchanged. Machine
  is actor-independent and this change does not enter the Behavior fold or
  capability stack.
- Requirement: serialized and linearized execution need one synchronous
  `Fn(Output)` consumer, not a Bombay trait that merely forwards to `Fn`.
- Ownership: the standard `Fn` contract owns invocation syntax; each caller
  owns its concrete output consumption. Machine owns only ordering, poisoning,
  receipts, and dispatch custody.
- Exact blocker and regression: public `OutputHandler` adds a nominal extension
  port with no production implementation other than its blanket closure
  adapter. All real repository consumers are closures; two private test-only
  implementations model reentrancy. A compile-pass fixture preserves closure
  syntax, while a compile-fail fixture must reject the obsolete trait port.
  Runtime reentrancy, panic, ordering, and exact-drop tests remain independent
  behavior regressions.
- Dependency edges: independent of DX53, DX49, DX54, DX55, and DX56.
- Blocked by: none.
- Unblocks: none.
- Change ledger: expected tracked files are Machine's manifest and lock entry,
  `executor.rs`, four compile-fixture files, and this ledger (`8` files).
  Expected production source is net-negative; tests are net-positive for the
  public syntax fixtures; expected public API is `+0 / -1` types. Existing
  closures, executors, receipts, and output evidence are reused. No replacement
  trait, wrapper, callback registry, owner, or policy may be added. Any caller
  syntax regression, weaker static denial, runtime failure, or performance
  regression falsifies the experiment.
- Result: `OutputHandler` and its blanket implementation are gone. Serialized
  submission and linearized dispatch now accept the exact borrowed
  `Fn(Output)` contract already used by every production caller. Reentrancy and
  poisoning probes retain exact owned-output consumption through closures; no
  replacement trait, wrapper, bound, state, or branch was added.
- Verification: the closure compile-pass fixture and obsolete-port compile-fail
  fixture pass; all 17 normal Machine tests and both optimized Loom models pass;
  the complete locked workspace suite, 502-test Nextest suite, formatting, and
  strict all-target Clippy pass through pinned Nix. Against the retained parent
  revision, serialized output consumption measured 111.300 ns versus 109.817 ns
  per turn and linearized dispatch measured 25.053 ns versus 25.051 ns; the
  removed forwarding trait has no measured performance value.
- Aggregate-drift checkpoint: executor control sums remain
  `Idle | Running | Poisoned` and the existing dispatch/turn outcomes; no
  subordinate state or result alternative changed. Production control-flow
  branches remain `26`, modules remain `1`, and public executor spellings fall
  from `11` to `10`. The production portion of `executor.rs` falls from 532 to
  508 lines (`+18 / -42 / net -24`). Every surviving state still owns the same
  current execution, queue, poison, receipt, or dispatch value. The residue
  scan found no arrival history, repeated cause, false cardinality, nested
  transition authority, semantic boolean, or structural caller syntax added.
  Cross-check against Driver law, Machine runtime tests, and the
  actor-independent ownership boundary: `pass`.
- Actual checkpoint: tracked files `8`; production source
  `+18 / -42 / net -24`; tests `+119 / -26 / net +93`; public API
  `+0 / -1` types; manifests and lockfile `+2 / -0 / net +2`;
  documentation `+60 / -0 / net +60`; complete tracked delta
  `+199 / -68 / net +131`.

## BEH1 — creation-settlement disposition

- State: `feature-complete`; immutable Behavior Core 0.17.0 and Macros 0.12.0
  provide the missing generated-actor creation-result custody contract.
- Exact blocker: `SourceSettlementCustody` for every `Births<C>` settlement
  requires the root event to implement
  `EventIngress<Births<C>, CreationsSettled<A, C>>`. A
  `#[behavior(..., births = { ... })]` declaration generates the closed birth
  product but does not generate that event lane, request an owning receiver,
  or expose a typed disposition that proves the result is intentionally
  retained without return. Bombay's `#[actor]` delegates to that exact macro
  and therefore cannot supply the missing owner contract.
- Regression: the caller-visible `application_terminal_custody` fixture has a
  generated actor that creates one declared child and stops. Under 0.16.0 its
  application fails to compile because its generated
  `EventLayer<ShutdownRequested, User<MailAddr, Never>>` cannot admit the
  required `CreationsSettled` input. A second inversion with application-owned
  children fails on the same law for the appended closed birth product.
- Required owner decision: Behavior must expose one explicit typed policy for
  creation settlement—either generate/name the matching ingress and receiver,
  or represent an intentional no-return disposition in the birth algebra.
  Bombay must not silently discard the authoritative settlement or invent a
  parallel event/effect contract.
- Neighbor finding: `BehaviorSettlements::Settlements` also has no declared
  equality or classification contract tying it to `Actions<B>` in a generic
  interpreter. Bombay can localize the concrete equality at its single
  `ActionInterpreter` seam, so this is friction rather than a separate blocker.
- Result: `creation_settlements = return_to_creator` generates a
  `CreationEvent` ingress and requires the authored `creations_settled`
  transition. `creation_settlements = retain_for_retirement` selects
  `RetirementBirths`, whose exact nonempty settlement yields
  `SourceCustody::Retained` after later live-source lanes have been offered.
  Omitted and explicit `NoBirths` declarations remain policy-free.
- Blocked by: none.
- Unblocks: DX58.

## BEH2 — pool-worker facade resolution

- State: `feature-complete`; `bombay-behavior-macros` 0.12.0 resolves its
  Actors path through the `bombay-rs` facade root while continuing to resolve
  foundational algebra through the Behavior owner.
- Regression: the worker-pool example imports the re-exported
  `bombay::atomic::pool_worker` with no direct Actors dependency and fails at
  the attribute expansion because `atomic` cannot be found in Behavior. The
  same source compiles when the owner crate is an explicit dependency and the
  macro resolves `behavior_actors::atomic`.
- Required owner correction: Macros must make `actors_crate()` resolve the
  Bombay facade's Actors path (`bombay::atomic` for this expansion) rather than
  reuse its foundational Behavior path. Bombay must not introduce a second
  procedural macro or fake an `atomic` module under Core.
- Current downstream disposition: the public worker-pool example declares the
  exact Actors owner dependency for `pool_worker`; all other runtime and
  foundational imports continue through Bombay. This is an explicit owner
  import, not a compatibility layer.
- Result: upstream facade-only and renamed-facade fixtures prove the corrected
  path. Bombay may remove the temporary direct owner dependency only after its
  existing example proves the published facade path end to end.
- Blocked by: none.
- Unblocks: DX58 and facade-only use of the owner `pool_worker` macro.

## DX58 — Behavior crates.io adoption

- State: `feature-complete`; both retained locks select the published 0.17
  graph, BEH1's exact missing custody policy is integrated end to end, and all
  feature gates pass. Final project-wide fixed-point minimization remains
  pending before `distilled`.
- Prior target releases: `bombay-behavior` 0.16.0 and
  `bombay-behavior-actors` 0.16.0 resolve to source commit
  `b9642e84e5719c4e2018f752822a392d3c23e164`; the
  `bombay-behavior-macros` 0.11.6 tag resolves to the same commit. The release
  tags and Cargo registry search were independently resolved before dependency
  selection. That revision's full
  760-line `AGENTS.md` has blob
  `4996c149dc7600c75972e2c041e1cee4a79352a0` and was re-read before this
  migration.
- Neighbor verification: Address 0.2.0, Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe were
  rechecked at their selected source. Their ownership and protocols do not
  change in this feature; only their Bombay interpreters may adapt to the
  owner's total-settlement interface.
- Requirement: replace the mutable Behavior Git patch with the immutable
  crates.io releases and consume the exact published algebra directly. Bombay
  must not retain the superseded two-leg commit API, re-create catalogue
  ownership in core, or add a compatibility runtime.
- Ownership: Behavior Core owns `ActionItem`, `ItemSettlement`,
  `SettledItem`, `Interpretation`, source-result custody, creation ordering,
  and total `Actions::interpret`; Behavior Actors owns catalogue protocols,
  templates, and the unified `TimedEvent`/`TimedReaction`; Bombay owns only
  concrete local interpretation, application activation and retirement, and
  exact terminal custody. Engine remains actor-independent.
- Governing invariants: creation preparation and settlement precede dependent
  sends; every accepted, rejected, blocked, corrupt, and unattempted action
  retains its exact typed source item; closed control retains the exact source
  action; initialization and activation settle through the same contract; the
  final Behavior and affine Environment residual remain recoverable at every
  terminal edge.
- Exact regression: an isolated worktree changed only dependency selection to
  the published releases and ran `nix develop -c cargo check --workspace`.
  The prior representation failed with 31 compile errors in two coherent
  groups: catalogue names still imported from Core, and Bombay's superseded
  `SendInterpreter`/`CreationResults`/two-leg commit machinery. Those failures
  locate the obsolete representation but do not determine the replacement
  architecture. Existing complete settlement, source-custody, closed-control,
  creation-order, initialization, activation, and Driver-retirement tests are
  the caller-visible behavioral regressions.
- Resolved upstream blocker: `bombay-behavior-actors` 0.15.0 declares
  `ReportTerminalOutcome`, `ObserveEstablished`, `CancelObservation`, and
  `ObserveEstablishedCreation` as `InterpreterRequest` values but supplies no
  `ActionItem` implementation for any of them. `InterpreterRequests<Request>`
  implements `SendSettlements` and `InterpretSends` only when
  `Request: ActionItem`, so ordinary termination propagation and exact
  observation cannot participate in the published total-settlement algebra.
  A removed isolated caller probe failed on all four bounds under 0.15.0.
  Both the trait and request types are upstream-owned, so Rust's coherence
  rules prohibit a Bombay implementation. Actors 0.15.1 added the four exact
  `ActionItem` contracts; its historical `interpreter_request_settlement`
  regression covered accepted, unattempted, and creation-prerequisite
  settlement. This probe and the 0.15.1 eligibility step are historical;
  the locked 0.17.0 source and its tests are the selected contract now.
- Falsified blocker candidate: a stopping initialization is not an
  unrepresented pre-publication child result. The Driver records the stop
  verdict, but the local Environment commits initialization and publishes the
  endpoint synchronously before the Driver retires the active incarnation.
  `spawn_owned_with` therefore returns the established capability, after which
  ordinary termination custody applies. The remaining pre-publication
  `Panicked`/`Cancelled` cases and interpreter corruption are outside ordinary
  returned settlement under Bombay's existing unwind/cancellation law; no
  Behavior change is required for this candidate.
- Resolved terminal-contract blocker: Core 0.16.0 adds the blanket
  `BehaviorSettlements` projection. A lifecycle owner can retain the exact
  settlement as `<B as BehaviorSettlements>::Settlements` without repeating
  `SendSettlements` or creation-product bounds through actor tasks, Entity
  leases, terminal projections, or application types. The value remains
  concrete; Bombay must neither erase it nor reconstruct its product shape.
- Resolved source-order blocker: Core 0.16.0 replaces bulk source admission
  with `SourceSettlementCustody::offer_next_to_source`. `SourceCustody`
  distinguishes `Exhausted`, exactly one `Admitted` input, and `Closed` while
  retaining the exact residual. Creation precedes sends, generated named
  products and `SendLayer` preserve their declared order, and each admitted
  result returns control to the Driver so its complete transitive chain can
  become quiescent before the next older result is offered.
- Resolved Actors inconsistency: Actors 0.16.0 adds direct `settle` operations
  for `ObserveEstablished` and `CancelObservation`, matching
  `ShutdownEstablished`. Bombay's one-line `InterpretItem` implementations are
  required static-dispatch leaves and now delegate the settlement law to each
  request owner instead of repeating `ItemSettlement::Accepted(())`.
- Dependency edges: depends on the distilled DX53 ownership model, BEH1, and
  the published upstream contracts; independent of DX54 through DX57.
- Blocked by: none.
- Unblocks: immutable downstream graph alignment, including the version
  prerequisite of MNE1.
- Change ledger: expected tracked files are the root manifest and lockfile,
  Engine's fuzz manifest and lockfile, Bombay's manifest, `lib.rs`,
  `actor_interface.rs`, `actors/actor_ext.rs`, `interpret.rs`,
  `application_runtime.rs`, `child_bindings.rs`, `local.rs`, `launch.rs`,
  `reports.rs`, `observation.rs`, `time.rs`, `termination.rs`,
  `entity/bombay.rs`, `entity/family.rs`, affected caller-visible regression
  fixtures, and this ledger. Up to 24 tracked files are expected. Production
  must remain at or below net `+500` lines and public API at or below `+3`
  types; crossing either limit requires another explicit checkpoint. Existing
  Behavior settlements, actor catalogue products, Driver, Environment,
  capability owners, actor tasks, and terminal trees must be reused; obsolete
  Bombay interpreter and raw creation-result machinery must be deleted rather
  than wrapped.
- Authorized scope: adopting the coherent Orders 1–6 custody contract
  necessarily crosses the 15-file threshold. After that checkpoint was
  reported, the user explicitly supplied the published versions and directed
  Bombay to use the crates.io versions directly. That authorizes the expanded
  file count for this release migration only; it does not authorize a broader
  redesign or relax the production-line and public-type limits.
- 0.16 checkpoint: both retained locks now select the immutable registry
  releases and checksums, and the independent action-item and terminal-custody
  probes pass in debug and optimized builds. The Engine fuzz target also
  compiles against Core 0.16.0 after adopting the owner-provided `Creations`
  product. The workspace check now fails in Bombay's obsolete commit wrapper:
  `CreationCustody` and bulk source offering no longer exist, while the current
  Engine `apply -> Result<(), E>` boundary cannot return a complete settlement
  to the Driver. Adding bounds at the 38 diagnostics would merely leak product
  structure through Entity and application APIs; the 0.16
  `BehaviorSettlements` projection is the selected correction.
- Scope checkpoint: the complete working tree currently spans 22 tracked and
  untracked files. Production is `+859 / -658 / net +201`; tests are
  `+124 / -2 / net +122`;
  manifests and locks are `+24 / -32 / net -8`; documentation is
  `+63 / -32 / net +31`; no new public type has been added. The smallest
  coherent Driver settlement stage must additionally revise Engine's
  environment and Driver ports, their shared law fixtures, allocation,
  property, terminal-custody, compile-pass, benchmark, and fuzz witnesses,
  Bombay's terminal projection, and the three current Driver contract
  documents. That end-to-end surface is expected to reach at most 40 tracked
  files while retaining the existing net-production `+500` and new-public-type
  `+3` ceilings. Crossing the previously authorized 24-file limit requires a
  new explicit authorization before those production edits begin.
- Expanded scope authorized: the user explicitly authorized the requested
  DX58 expansion to at most 40 tracked files. The cumulative migration retains
  the existing net-production `+500` and new-public-type `+3` ceilings. This
  authorization covers only the coherent Engine settlement, Bombay custody,
  regression, and contract-document surface described above; it does not
  authorize unrelated redesign.
- Selected Driver model: `CommitActions` performs exactly one total
  `Actions::interpret` and returns its `Interpretation<B::Settlements>`; it
  owns no source loop and no public error vocabulary. The active Environment
  supplies three concrete ports only: offer at most one settlement result to
  its source, obtain the next admitted source-control event without opening
  ordinary ingress, and publish the installed endpoint after initialization
  custody is resolved. Its retirement value receives every still-owned
  settlement. The actor-independent Driver owns a `VecDeque` of private
  `Offer | AwaitSource` turns. New transitive settlements enter at the front;
  the older residual remains behind them. `Exhausted` discharges a product
  only after Core proves that it contains no further source input, `Admitted`
  changes the front turn to `AwaitSource`, and `Closed` transfers the current
  input and untouched suffix into retirement. No recursion, callback, second
  mailbox, erased value, or interpreter-specific traversal is introduced.
- Initialization law: after address installation, accepted initialization
  effects and every admitted transitive source-result turn become quiescent
  before publication and ordinary ingress. Pure initialization rejection
  remains the only pre-installation Behavior error. A post-installation
  rejected or corrupt settlement, source closure, or stop transfers the exact
  pending deque, publishes the installed endpoint so the fact cannot be
  misclassified as `CreationRejection`, and then crosses the normal retirement
  barrier. A stopping actor never admits its final settlements back into
  itself. Active expected rejection remains settlement data and is returned to
  its declared source; only corruption or closed source custody is terminal.
- Required regressions: observe initialization settlement before publication;
  prove `new transitive -> older residual -> ordinary input` ordering; close
  source admission with two untouched results and recover both; inject a
  corrupt interpretation and recover the entire product; stop with a source
  result and prove no self-reopening; replay each terminal case in optimized
  mode; and drive a long generated source chain to prove the loop is iterative
  rather than recursive. Each test must assert the complete ordered transcript
  and retirement product, with a corresponding order/retention inversion.
- Prior-representation proof: the focused pinned-Nix
  `source_settlement_order` regression compiles against the old Engine port and
  fails on its complete transcript for the intended causal inversion. Actual
  execution is `Committed(1) -> RequestedOrdinary -> FoldedOrdinary ->
  Retired`; the governing law requires `Committed(1) -> Returned(1) ->
  Committed(2) -> Returned(2) -> Ordinary -> Retired`. The current Driver has
  no settlement value or source-only acquisition operation with which it could
  select the lawful transcript.
- 40-path implementation checkpoint: the working tree has reached the exact
  authorized ceiling. Production is `+1555 / -1463 / net +92`; tests are
  `+525 / -255 / net +270`, including the untracked 169-line source-order
  regression; manifests and locks are `+15 / -11 / net +4`; documentation is
  `+196 / -34 / net +162`; public API is `+3 / -2` types. The retained Driver
  runtime suites pass in debug and optimized builds: 30 causal laws, three
  generated properties, six terminal-custody cases, the allocation witness,
  and the source-order regression. Bombay's 166 library tests pass in debug
  and optimized builds; strict library Clippy passes; and the `run_with`,
  actor-interface, and semantic-send suites pass. The root-only application now runs the exact root
  Behavior; declared members alone use the typed application composition, so
  no forwarding wrapper remains solely to normalize `NoBirths`.
- Containment checkpoint: the all-target gates expose seven known files outside
  the authorized set. Engine's `send_not_sync` and
  `environment_phase_authority` sources still implement the superseded
  two-leg Environment port, and three compile-fail `.stderr` witnesses retain
  pre-0.16 diagnostic spelling. Bombay's `template_application` and
  `external_customer_templates` fixtures still import actor-catalogue types
  from Behavior Core. Updating those seven witnesses requires explicit scope
  expansion; no production abstraction is implicated. A ceiling of 50 paths
  would cover them and three further verification-only discoveries without
  relaxing the production or public-API budgets.
- Second scope expansion authorized: the user explicitly directed work to
  continue after the 40-path checkpoint, authorizing DX58 up to 50 tracked and
  untracked paths. The unchanged ceilings remain net `+500` production lines
  and `+3` new public types. This expansion covers only the seven identified
  verification witnesses and up to three additional all-target discoveries;
  it does not authorize new production architecture or bypass BEH1.
- 47-path checkpoint: Engine's complete compile-pass and compile-fail suite now
  passes under 0.16.0, including the non-`Sync` environment and prepared/active
  authority denials. The two Bombay actor-catalogue fixtures now import their
  owning Actors API directly. The complete delta is production
  `+1555 / -1463 / net +92`; tests and examples are
  `+612 / -305 / net +307`, including the untracked source-order regression;
  manifests and locks are `+15 / -11 / net +4`; documentation is
  `+221 / -34 / net +187`; public API remains `+3 / -2` types.
- Additional all-target discovery: four public examples—`counter`, `entity`,
  `application-topology`, and `supervision`—still name pre-0.16 terminal bounds,
  actor-catalogue imports, creation syntax, or retirement fields. All four must
  move together because examples are current public guidance. Editing them
  would reach 51 paths, beyond the authorized 50-path ceiling. No example was
  partially migrated. A 60-path ceiling would cover these four examples and
  reserve nine verification-only paths for diagnostics exposed after they
  compile, without relaxing either code-growth ceiling.
- Registry recheck: `cargo search` inside the pinned Nix shell still reports
  Core 0.16.0, Actors 0.16.0, and Macros 0.11.6 as the latest immutable
  releases. BEH1 therefore remains an external selected-contract blocker.
- Third scope expansion authorized: the user explicitly authorized continued
  work after the 47-path checkpoint, raising the DX58 ceiling to 60 tracked and
  untracked paths. The expansion covers the four public examples as one current
  guidance stage and up to nine verification-only discoveries. Net production
  remains capped at `+500` and new public types at `+3`.
- Supervision-example ownership correction: Actors 0.14 removed the former
  `Supervise`, `ChildTopology`, `Proxy`, and restart-wrapper family and replaced
  it with the clean-room `atomic::FixedSupervisor` construction. Bombay's
  catalogue facade exported every other Actors domain module but omitted
  `atomic`, leaving an application unable to name the owning replacement
  without adding a second direct dependency. The smallest correction touches
  the already-counted `crates/bombay/src/lib.rs` with one module re-export and
  rewrites the already-counted supervision example against `fixed`; it adds no
  public Bombay type and is expected to reduce example lines. The executable
  example will exercise construction and deterministic initialization only.
  Runtime recovery remains ineligible until a separately verified Bombay
  capability owns `PrepareWorkers`; the example must not fabricate that
  interpreter or preserve the removed compatibility stack.
- 51-path all-target checkpoint: the three migrated unblocked examples pass
  debug and optimized tests, strict all-target Clippy, and formatting. The
  topology example now fails only on BEH1. The next workspace check exposed
  five stale guidance files: actor templates still import `Machine` vocabulary
  from Core, Axum omits retained settlements, and the worker-pool pair names
  the Actors 0.13 pool family removed by 0.14. The authorized verification
  reserve covers those exact five files, reaching at most 56 paths. The pool
  example will use the owning `atomic::fifo` and `pool_worker` APIs and stop at
  deterministic initialization for the same unimplemented `PrepareWorkers`
  runtime-capability reason as fixed supervision; no compatibility pool,
  callback, or discarded settlement will be added.
- 57-path containment checkpoint: every stale public example outside BEH1 now
  compiles against the owning 0.16 API. Counter, Entity, fixed supervision,
  actor templates, Axum, and FIFO pool pass their focused debug and optimized
  tests; both example batches pass strict all-target Clippy. Bombay's complete
  all-feature library suite passes in debug and optimized builds (`166 passed`,
  `1 ignored`), and strict all-feature library Clippy passes. Formatting and
  whitespace checks pass. The complete all-target workspace check now reaches
  only the preserved BEH1 failures in `application_terminal_custody` and the
  application-topology example; the independently checked Entity application
  fails on the same generated-birth contract. No obsolete import, terminal
  shape, pool, or supervision diagnostic remains ahead of that blocker.
  Production is `+1560 / -1461 / net +99`; tests and examples, including the
  untracked source-order regression, are `+849 / -803 / net +46`; manifests
  and locks are `+26 / -34 / net -8`; documentation is
  `+290 / -34 / net +256`; public API remains `+3 / -2` types. The additional
  catalogue-module and `Machine` vocabulary re-exports expose existing Actors
  owners and add no Bombay type.
- Remaining external failure: after the in-scope library-test migration, the
  only semantic Bombay test failures are the two
  `application_terminal_custody` inversions and Entity's Profile child, all
  failing on BEH1's missing generated creation-settlement disposition. These
  are retained as the caller-visible upstream regression rather than bypassed
  by discarded settlements.
- 0.17 reactivation: registry search and immutable tags select Core 0.17.0,
  Actors 0.17.0, and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`. The complete 760-line upstream
  `AGENTS.md` was re-read from that exact revision. The Core, Actors, and
  Macros changelogs, generated-custody regression, macro expansion, source
  products, and relevant actor request products were inspected against 0.16.
  Address 0.2.0, Communication 0.1.2, Timers at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe were
  rechecked at their selected source, public ownership interfaces, retirement
  regressions, and current capability documents; none acquires a new contract.
- 0.17 governing law: a birth-owning generated actor explicitly selects either
  exact `CreationsSettled` re-entry through an authored transition or exact
  terminal retention. Bombay's existing generated actors have no lawful live
  decision to make from these results, so they select
  `retain_for_retirement`; no no-op transition or discarded settlement is
  introduced. The actor-independent Driver preserves every `Retained`
  settlement until its one retirement barrier while continuing ordinary
  ingress after all later source lanes have been offered.
- Prior-representation regression: under the selected 0.16 lock,
  `nix develop -c cargo test -p bombay-rs --test application_terminal_custody`
  fails at both root forms because the generated event cannot admit exact
  `CreationsSettled`. That is the intended BEH1 failure. After dependency
  selection and before the Driver edit, a focused Engine regression must prove
  that `SourceCustody::Retained` reaches retirement without another offer,
  source read, fold, or loss; the same regression runs optimized before the
  change broadens.
- 0.17 ownership and reuse: Core owns `CreationEvent`, `RetirementBirths`,
  `RetirementCreationSettlement`, `SourceCustody::Retained`, and source-product
  traversal. Macros owns policy syntax and generated transitions. Actors owns
  retention through its named request products. Engine owns only the universal
  pending-versus-retained settlement queues. Bombay's actor facade forwards the
  owner syntax and the local environment transfers the final exact products
  through its existing retirement value. Address, Communication, Observe, and
  Timers remain unchanged.
- 0.17 change ledger: the expected surface is up to 24 tracked and untracked
  paths: both manifests and locks, the actor facade macro, the Driver and one
  focused Driver regression, every current birth-owning source/fixture and
  affected diagnostic, the current Driver contract documents, the worker-pool
  facade dependency correction if its focused proof passes, and this ledger.
  Production is expected below `+40` net lines; tests and examples are expected
  below `+100` net lines; public API is `+0 / -0` types. Existing Behavior
  policies, settlement products, Driver queue, Environment retirement port,
  application terminal tree, and actor catalogue facade are reused. No wrapper,
  callback, compatibility actor, event variant, dynamic registry, or second
  custody owner is eligible. The expected path count crosses the automatic
  15-file threshold, so production edits remain blocked pending explicit user
  authorization for this 24-path 0.17 migration ceiling; the `+500` production
  and `+3` public-type ceilings remain unchanged.
- 0.17 scope authorized: the user explicitly authorized the recorded 24-path
  DX58 migration ceiling. The unchanged `+500` net production-line and `+3`
  new-public-type ceilings still apply. This authorization covers only the
  immutable dependency selection, generated creation-settlement policy,
  Driver retirement custody, affected callers and diagnostics, pool-worker
  facade proof, and current contract documents named above.
- Application-composition checkpoint: the first 0.17 all-target compile passes
  every direct generated actor, then fails only the declared-member root in
  `application_terminal_custody`. `ApplicationBehavior` appends application
  children after the root's birth node, but its pre-0.17 implementation always
  reconstructed `Births<Combined>` and therefore erased the root birth mode's
  newly explicit settlement disposition. The composition law is: `NoBirths`
  plus declared application children becomes retirement-owned births; existing
  `Births<RootChildren>` remains live-returning after append; existing
  `RetirementBirths<RootChildren>` remains retirement-owned after append. One
  private associated-type mapping owned by application composition expresses
  those three exhaustive policies and deletes the unconditional reconstructed
  mode. The existing heterogeneous application regression must then retain its
  exact combined creation settlement at retirement. A generated live-return
  root still fails statically unless its event algebra can admit the exact
  appended settlement; Bombay must not split, infer, or discard that product.
  This uses the already-budgeted `application_runtime.rs`, adds no public type,
  and does not expand the 24-path ceiling.
- 24-path containment checkpoint: the immutable locks, all-target workspace
  check, workspace build, strict all-target Clippy, formatting, every affected
  focused debug/optimized regression, and every runtime integration test pass.
  The full workspace suite reaches only stale compile-fixture ownership or
  diagnostic text, and the independent fuzz lock reaches only its superseded
  pre-settlement Environment witness. Nine additional verification paths are
  exact: `active_cannot_initialize_again.stderr`,
  `application_child_must_be_behavior.stderr`, the two Entity negative-source
  fixtures, `advanced_behavior_imports.rs`, the three template-authoring
  source fixtures, and `fuzz_targets/causal_turns.rs`. The source fixtures must
  import Actors-owned protocols from `bombay::composition`, `bombay::timing`,
  or the owner root while continuing to import foundational algebra through
  `bombay::behavior`; the diagnostics must retain the same static denials; and
  the fuzz Environment must use the existing total settlement and exhaust it
  without fabricating a source event. No production source, public item, law,
  runtime trace, or dependency changes in this expansion. The current complete
  delta is exactly 24 paths: production `+69 / -12 / net +57`; tests
  `+93 / -42 / net +51`; examples `+2 / -2 / net 0`; manifests and locks
  `+13 / -15 / net -2`; documentation `+187 / -60 / net +127`; public API
  `+0 / -0` reachable types. Completing those exact witnesses requires a
  ceiling of 33 paths; the authorized `+500` net-production and `+3`
  new-public-type ceilings remain unchanged.
- Verification expansion authorized: the user explicitly authorized the exact
  33-path ceiling recorded above. The expansion covers only those nine named
  compile, diagnostic, and fuzz witnesses. It adds no production path, public
  API, compatibility export, runtime policy, or dependency change, and does
  not relax either code-growth ceiling.
- 33-path verification checkpoint: all nine authorized witnesses now pass.
  `activation_authoring`, `application_children`, `entity_authoring`,
  `prelude_levels`, and `template_authoring` retain their caller-visible
  compile-pass and compile-fail contracts, and the independent Engine fuzz
  crate passes `nix develop -c cargo check`. The complete working tree remains
  exactly 33 paths: production `+69 / -12 / net +57`; tests
  `+156 / -88 / net +68`; examples `+2 / -2 / net 0`; manifests and locks
  `+13 / -15 / net -2`; documentation `+229 / -63 / net +166`; public API
  `+0 / -0` types. The positive advanced-import witness now names the
  published `InterpretItem<Delivery<P>, RootEvent, Path>` contract, exposing
  one stale negative witness that still succeeds only by naming removed
  `InterpretDelivery`. The full workspace suite also exposes the Axum-feature
  form of the application-child diagnostic, whose expected output still names
  the former re-export path. Correcting those two sources and diagnostics
  requires three paths beyond the authorized ceiling; none has been edited
  pending a 36-path authorization checkpoint. A read-only all-features sweep
  of every other Bombay integration target passes, so no further stale
  fixture is known.
- Final verification expansion authorized: after the complete workspace suite
  exposed the feature-unified diagnostic, the user explicitly authorized the
  exact 36-path ceiling. The added surface is limited to the current
  `InterpretItem<Delivery<P>, RootEvent, Path>` prelude denial, its diagnostic,
  and the feature-unified application-child diagnostic. All three pass under
  their default and all-feature configurations.
- 36-path gate checkpoint: `nix develop -c cargo fmt --all -- --check`,
  `cargo check --workspace --all-targets`, `cargo build --workspace`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo test --workspace` all pass. The explicit ignored Driver law-manifest
  gate passes; the retained-custody, application-terminal, Entity application,
  topology-example, and worker-pool witnesses pass optimized; and the
  independent Engine fuzz crate passes `cargo check`. Production remains
  `+69 / -12 / net +57`; tests are `+168 / -98 / net +70`; examples are
  `+2 / -2 / net 0`; manifests and locks are `+13 / -15 / net -2`;
  documentation is `+252 / -63 / net +189`; public API is
  `+0 / -0` types.
- Final document expansion authorized: the user explicitly authorized the
  exact 37-path ceiling after the fixed-point audit found one remaining current
  contract document. `docs/runtime-capability-interfaces.md` now selects the
  immutable 0.17.0 Core and Actors plus 0.12.0 Macros registry releases and
  records total typed settlements, exact source custody, explicit generated
  creation-settlement disposition, and established-delivery lookup bypass. No
  historical decision was rewritten as current guidance.
- Final change ledger: the complete tracked and untracked delta is exactly 37
  paths. Production is `+69 / -12 / net +57`; tests are
  `+228 / -99 / net +129`; examples are `+2 / -2 / net 0`; manifests and locks
  are `+13 / -15 / net -2`; documentation is
  `+279 / -77 / net +202`; public API is `+0 / -0` types.
  The immutable dependency tree resolves Core and Actors 0.17.0 and Macros
  0.12.0; no superseded interpreter or selected-version spelling remains in
  current guidance or caller fixtures. The full pinned-Nix workspace gates,
  explicit Driver law-manifest gate, debug/optimized retained-head and semantic
  regressions, all-feature compile contracts, independent fuzz check,
  formatting, and whitespace checks pass.
- Falsification: reject the migration if it needs a second effect algebra,
  catalogue copy, erased action or result, dynamic capability map, hidden
  callback, inferred provenance, compatibility layer, or weaker terminal
  custody. Role-first application assembly remains separate from this release
  adoption.

## DX59 — Machine topology identity rendering

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the workspace remains on Behavior Core and Actors
  0.14.0 and Macros 0.11.4 at
  `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`, Address 0.2.0,
  Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe.
  Their source and public contracts were rechecked; none owns or consumes
  Machine's renderer. `bombay-machine` owns `Topology`, `VertexId`, display
  labels, validation, and Mermaid rendering. Bombay lifecycle metadata is the
  only downstream renderer regression.
- Exact blocker and regression: `Topology::write_mermaid` discards each
  `VertexId` and uses its human-readable label as Mermaid identity. Two
  distinct vertices with the same label therefore collapse into one rendered
  state even though execution and validation distinguish them. The smallest
  failing regression declares two reachable vertices with one shared display
  label and requires two state declarations plus start and transition edges
  that reference their distinct generated IDs.
- Governing invariant: `VertexId` remains the identity in every structural
  interpreter. A Mermaid state description presents `Vertex::label` but never
  substitutes for identity. Every declared vertex is emitted exactly once,
  and start and transition edges refer only to the generated ID derived from
  `VertexId`. Existing unknown-reference formatting failure remains unchanged.
- Syntax evidence: Mermaid's official state-diagram grammar explicitly
  separates a state ID from its description and requires later transitions to
  reference the ID. The renderer will use that ordinary grammar directly; no
  escaping abstraction, syntax framework, allocation, or dependency is
  justified by this identity defect.
- Dependency edges: independent of DX53 through DX58; no downstream feature is
  blocked on this correction.
- Blocked by: none.
- Unblocks: truthful diagrams for any structural consumer whose distinct
  vertices intentionally share presentation text.
- Change ledger: expected tracked files are this ledger,
  `crates/bombay-machine/src/machine.rs`, and
  `crates/bombay/src/entity/lifecycle/machine.rs` (`3` files). Expected
  production source is no more than `+14 / -12 / net +2`; tests are no more
  than `+34 / -12 / net +22`; public API is `+0 / -0` types. Existing
  `Topology`, `VertexId`, `Vertex`, and `Transition` remain the sole owners;
  the obsolete label-to-identity lookup is deleted. Any change to validation,
  execution, topology construction, or public Rust syntax falsifies the stage.
- Result: every declared vertex now has one Mermaid declaration whose stable
  `vertex_<id>` identity is derived from `VertexId` and whose description is
  the existing human label. Start and transition edges use only those stable
  identities. Unknown references retain the same checked lookup and formatting
  error; no topology, validation, execution, allocation, or public Rust
  contract changed.
- Verification: the new duplicate-label regression failed against the prior
  renderer by producing `ready --> ready`, then passed in debug and optimized
  builds after the correction. Bombay's exact lifecycle rendering regression,
  all 18 Machine tests and compile fixtures, the complete locked workspace
  suite, 503-test Nextest suite, formatting, and strict all-target workspace
  Clippy pass through pinned Nix.
- Actual checkpoint: tracked files `3`; production source
  `+10 / -10 / net 0`; tests `+34 / -2 / net +32`; public API
  `+0 / -0` types; documentation `+62 / -1 / net +61`; complete tracked delta
  `+106 / -13 / net +93`.

## DX60 — normative runtime contract alignment

- State: `feature-complete`; final fixed-point audit remains pending.
- Selected contracts: the exact owner graph is unchanged from the table above.
  In particular, Timers is selected from Git revision
  `13e884da7ab41781f52337b0038060e375b00ee0`; the obsolete registry checksum
  and checkout `4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2` are historical provenance only.
- Exact blocker and regression: `runtime-capability-interfaces.md` calls itself
  the current normative runtime contract but identifies the obsolete Timers
  artifact as selected and twice describes exact-capability work as pending
  DX37 slices. `Cargo.lock`, the selected-contract table, current source, and
  caller documentation instead show the Git Timers revision, exact established
  delivery/observation, and deliberate logical `ActorSpace` hosting. The
  smallest regression is a repository scan that finds those three stale
  current-contract claims in the normative document.
- Governing invariant: a normative document names the exact selected artifact
  and describes current ownership without presenting historical probes or
  retired ledger IDs as live architecture. Historical provenance in the
  explicitly historical Driver test derivation remains unchanged.
- Dependency edges: independent of DX53 through DX59.
- Blocked by: none.
- Unblocks: reliable version and ownership review for the next fixed-point
  audit and eventual Behavior release adoption.
- Change ledger: expected tracked files are this ledger and
  `docs/runtime-capability-interfaces.md` (`2` files). Production and tests are
  `+0 / -0`; public API is `+0 / -0` types. No code, Cargo selection, owner,
  interface, or future feature is changed. Any statement not directly proved
  by the selected graph and existing implementation falsifies the repair.
- Result: the normative contract now names the exact locked Timers Git
  revision, describes actor spaces as the deliberate host for logical
  recipients and advanced multi-protocol composition, and describes exact
  established delivery and observation as current behavior. The retired DX37
  roadmap wording and obsolete current-version claim are gone; the explicitly
  historical Driver-strategy provenance remains intact.
- Verification: the pre-edit oracle found all three stale claims. The same
  document now contains none of the obsolete revision or DX37 references, and
  its replacement revision matches `Cargo.toml`, `Cargo.lock`, and every
  current selected-contract table. Complete diff and whitespace checks pass.
- Actual checkpoint: tracked files `2`; production and tests
  `+0 / -0`; public API `+0 / -0` types; documentation
  `+56 / -12 / net +44`.

## DX61 — Machine test transition policy

- State: `feature-complete`; final fixed-point audit remains pending.
- Selected contracts: the exact owner graph remains unchanged. This stage
  touches only Machine's private executor tests; no Behavior or runtime
  capability consumes the representation.
- Exact blocker and regression: `ExclusiveTestMachine` and `OwnershipMachine`
  each encode the mutually exclusive “return normally or panic” transition
  policy in a field named `panic: bool`, with successor construction resetting
  that policy through an unlabelled `false`. The pre-edit structural oracle
  finds two semantic boolean fields and six boolean policy literals in the
  owning test module. The existing success, panic poisoning, exact input
  rejection, and drop-accounting tests are the observable regression suite.
- Governing invariant: a test machine's transition disposition is a closed sum
  with named alternatives. The successor explicitly selects ordinary return;
  no boolean may erase whether a transition returns or panics.
- Dependency edges: independent of DX53 through DX60.
- Blocked by: none.
- Unblocks: truthful fixed-point review of Machine's test-only state model.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay-machine/src/executor.rs` (`2` files). Production is
  `+0 / -0`; tests are no more than `+16 / -10 / net +6`; public API is
  `+0 / -0` types. One private enum replaces both boolean fields and all six
  policy literals. Any runtime trace, assertion, production item, public
  syntax, or dependency change falsifies the cleanup.
- Result: both private machines now store one shared
  `TestTransition::{Return, Panic}` value. Each transition exhaustively matches
  the disposition, and every successor explicitly selects `Return`. The two
  semantic boolean fields and all six policy literals are gone; no production
  item, dependency, or public interface changed.
- Verification: the pre-edit scan found two `bool` fields and six policy
  literals; the post-edit oracle finds none. All 18 Machine tests, compile
  fixtures, and documentation tests pass in debug and optimized builds, and
  formatting plus strict all-target Machine Clippy pass through pinned Nix.
- Actual checkpoint: tracked files `2`; production source
  `+0 / -0`; tests `+22 / -10 / net +12`; public API `+0 / -0` types;
  documentation `+43 / -0 / net +43`; complete tracked delta
  `+65 / -10 / net +55`. The test delta exceeds the six-line forecast because
  both owning transitions retain explicit exhaustive matches; no automatic
  containment threshold is approached.

## DX62 — Observe sequential model state

- State: `feature-complete`; final fixed-point audit remains pending.
- Selected contracts: the exact owner graph remains unchanged. This stage
  touches only Observe's independent proptest reference model; the production
  completion representation and public API are unchanged.
- Exact blocker and regression: the model passes `migrate: bool` to select
  whether a future reuses its latest waker or installs a new one, and stores
  completed `(key, epoch)` identities in `HashMap<_, bool>`. The first erases a
  mutually exclusive test operation; the second stores a set-membership fact
  as a value and admits meaningless `false` entries. The pre-edit structural
  oracle finds exactly that boolean parameter and completion map. Existing
  sequential-model and churn drop-accounting properties are the behavioral
  regressions.
- Governing invariants: future polling receives a closed named waker policy;
  completion history contains exactly the identities known completed. A local
  predicate may decide whether the selected policy installs a registration,
  but it may not replace the policy value or persist completion state.
- Dependency edges: independent of DX53 through DX61.
- Blocked by: none.
- Unblocks: an independent audit of the exhaustive Observe model rather than a
  mechanically shared testing framework.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/src/observe/external_tests/model.rs` (`2` files). Production
  is `+0 / -0`; tests are no more than `+14 / -10 / net +4`; public API is
  `+0 / -0` types. Existing `HashMap` state, proptest operations, observations,
  and wake probes remain; standard `HashSet` owns completion membership. Any
  property, generated operation distribution, outcome, wake count, drop count,
  production item, or dependency change falsifies the correction.
- Result: `FuturePoll::{Repoll, Migrate}` now preserves the generated poll
  operation through waker selection, while a local predicate answers only
  whether that selected operation installs a registration. Completed
  generations are a `HashSet`; false-valued membership is no longer
  representable. The generated operation distribution, independent oracle,
  production code, dependencies, and public interface are unchanged.
- Verification: the pre-edit scan found the boolean policy parameter and
  boolean-valued completion map; the post-edit oracle finds neither. All three
  model properties pass through both the private Observe crate and Bombay,
  focused model properties pass optimized, and all 121 private Observe tests
  plus documentation tests, formatting, and strict affected-package Clippy
  pass through pinned Nix.
- Actual checkpoint: tracked files `2`; production source
  `+0 / -0`; tests `+22 / -12 / net +10`; public API `+0 / -0` types;
  documentation `+50 / -0 / net +50`; complete tracked delta
  `+72 / -12 / net +60`. The test delta exceeds the four-line forecast because
  the named sum and exhaustive local selection remain visible rather than
  being hidden behind a boolean conversion.

## DX63 — Observe exhaustive model state

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact owner graph remains unchanged. This stage
  touches only Observe's deterministic exhaustive reference model; it shares no
  state representation with the proptest oracle or production implementation.
- Exact blocker and regression: `exhaustive.rs` stores live generation phase
  in two `Option<bool>` values, stores retired/completed generation membership
  in two `HashMap<_, bool>` values, and enumerates completion ordering and
  future cancellation as boolean policies. Those encodings conflate absence,
  phase, ordering, and terminal choice with truth values. The pre-edit oracle
  finds those representations. The exhaustive single-key, drop-order,
  future-order, and waker-drain history tests are the behavioral regressions.
- Governing invariants: absence remains `None`; a retained generation has one
  named pending/completed phase; historical completion is set membership; and
  completion order plus future disposition remain named exhaustive dimensions.
  Local predicates may compare these values but may not replace them.
- Dependency edges: independent of DX53 through DX62.
- Blocked by: none.
- Unblocks: a truthful semantic-state scan of the remaining Observe stress and
  allocator test controls.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/src/observe/external_tests/exhaustive.rs` (`2` files).
  Production is `+0 / -0`; tests are no more than `+52 / -34 / net +18`;
  public API is `+0 / -0` types. Existing exhaustive alphabets, history depths,
  handle permutations, observations, subjects, and wake probes remain exact.
  Any reduced history space, changed oracle, production item, shared model
  abstraction, dependency, or public interface falsifies the correction.
- Result: `GenerationPhase::{Pending, Completed}` replaces both live-generation
  booleans, and completed retired generations are represented only by
  `HashSet` membership. `CompletionOrder::{BeforePolls, AfterPolls}` and
  `FutureDisposition::{Cancel, Resolve}` preserve the names of the two
  exhaustive policy dimensions. No alphabet, depth, permutation, timing
  combination, oracle, production item, dependency, or public API changed.
- Verification: all seven exhaustive tests pass through both crate embeddings;
  the focused `observe-tests` suite also passes optimized; the complete 121-test
  private Observe suite and its docs pass; formatting and strict all-target
  Clippy pass for `observe-tests` and `bombay-rs` through the pinned Nix shell.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+66 / -48 / net +18`; public API `+0 / -0` types; documentation
  `+44 / -0 / net +44`.

## DX64 — Driver allocation fixture input ownership

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: Behavior Core 0.14.0, Behavior Actors 0.14.0, and
  Behavior Macros 0.11.4 remain selected at
  `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`; Address 0.2.0,
  Communication 0.1.2, Bombay-private Observe, and Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0` remain unchanged. Their source,
  public contracts, relevant tests, and current documentation were rechecked;
  none owns this actor-independent fixture state.
- Ownership and law: Engine owns the universal Driver and affine Environment
  port. Under the current D-TERM-1, an active Environment yields one exact `B::Event` or
  `None` for permanent exhaustion. The allocation regression's private
  Environment therefore owns one queued event or its absence.
- Exact blocker and regression: `ImmediateEnvironment(bool)` stores the
  mutually exclusive available/exhausted input state as an unnamed truth value,
  then uses `mem::replace` and negation to recover the transition. The pre-edit
  structural oracle finds that tuple boolean. The existing zero-allocation
  end-to-end Driver execution is the behavioral and cost regression.
- Proposed representation: store `Option<User<MailAddr, u8>>` and consume it
  with `Option::take`. This is the exact one-value-or-absence algebra; it adds no
  phase type, wrapper, policy, branch, allocation, or public surface.
- Dependency edges: independent of DX53 through DX63.
- Blocked by: none.
- Unblocks: the remaining Engine test-policy scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay-engine/tests/driver_allocation.rs` (`2` files). Production is
  `+0 / -0`; tests are no more than `+6 / -3 / net +3`; public API is
  `+0 / -0` types. The existing `ImmediateEnvironment`, Driver, Behavior,
  allocation counter, one-event execution, and zero-allocation assertion are
  reused. Any production, dependency, public API, allocation count, event,
  terminal disposition, or additional state type falsifies the correction.
- Result: `ImmediateEnvironment` now owns the exact optional `User` event and
  yields it with `Option::take`. The boolean phase, negation, and
  `mem::replace` protocol are gone. The same event produces the same stopped
  Driver retirement without allocation; production, dependencies, public API,
  and test count are unchanged.
- Verification: the allocation regression passes with zero allocations in
  debug and optimized profiles; the complete Engine suite, compile fixtures,
  Driver laws, inversions, properties, custody tests, and docs pass; formatting
  and strict all-target Engine Clippy pass through the pinned Nix shell.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+6 / -3 / net +3`; public API `+0 / -0` types; documentation
  `+47 / -0 / net +47`.

## DX65 — Driver panic injection policy

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact owner graph recorded in DX64 was rechecked at
  its locked revisions. This stage touches only Engine's current D-RETIRE-1 test fixture;
  no Behavior, Behavior Actors, Address, Communication, Observe, Timers,
  runtime, interpreter, or public contract changes.
- Ownership and law: Engine's Driver owns panic terminality under D-RETIRE-1.
  Its private fixture selects one of two mutually exclusive injection stages:
  Behavior initialization or an ordinary turn. The existing combined test is
  D-RETIRE-1's positive manifest witness; the turn-only test is deliberate
  adversarial evidence reused by the complete manifest and is not redundant.
- Exact blocker and regression: `PanicBehavior::panic_in_init` and
  `panic_case(bool)` erase the selected injection stage behind a truth value;
  three call sites use unexplained literals. The pre-edit structural oracle
  finds the boolean field, parameter, and literals. The two manifest-accounted
  panic tests are the behavioral regressions.
- Proposed representation: a private `PanicStage::{Initialization, Turn}` sum
  owns the policy. Initialization selects its behavior exhaustively; ordinary
  turn injection remains the fixture's transition law. No production or public
  type is added.
- Dependency edges: independent of DX53 through DX64.
- Blocked by: none.
- Unblocks: the remaining Driver fixture-state scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay-engine/tests/driver_law.rs` (`2` files). Production is
  `+0 / -0`; tests are no more than `+15 / -8 / net +7`; public API is
  `+0 / -0` types. Existing Driver, Behavior, Environment, panic capture,
  ownership probes, test names, and manifest references remain exact. Any
  changed panic point, event-source poll count, drop fact, evidence name,
  production item, dependency, or public API falsifies the correction.
- Result: `PanicStage::{Initialization, Turn}` replaces the boolean field,
  parameter, and three literal selections. Initialization selection is
  exhaustive; the established turn injection remains unchanged. Both test
  names and their distinct positive/adversarial manifest roles remain exact.
- Verification: both panic witnesses pass in debug and optimized profiles with
  their exact source-poll and ownership-drop facts; the complete Engine suite,
  compile fixtures, laws, inversions, properties, custody tests, docs, and the
  explicit ignored manifest-closure gate pass; formatting and strict all-target
  Engine Clippy pass through the pinned Nix shell.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+15 / -8 / net +7`; public API `+0 / -0` types; documentation
  `+46 / -0 / net +46`.

## DX66 — Driver custody failure ownership

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph and complete Driver law
  evidence remain as rechecked for DX64 and DX65. This stage touches only the
  terminal-custody fixture for current D-TERM-1 and D-RETIRE-1; no owning dependency,
  runtime, interpreter, production, or public contract changes.
- Ownership and law: the fixture Behavior owns its optional initialization
  failure; the prepared Environment owns its optional activation failure; the
  active Environment owns its optional action-application failure. Each error
  is an exact fact that the Driver must retain in its distinct `DriverError`
  variant beside final custody.
- Exact blocker and regression: five fields/parameters encode failure presence
  as booleans while the transition bodies reconstruct three hard-coded errors;
  six scenarios use thirteen unexplained truth literals. The pre-edit
  structural oracle finds those boolean policies. The complete six-test custody
  matrix is the behavioral regression, including exact state, residual phase,
  committed prefix, completion, and failure assertions.
- Proposed representation: each owner stores `Option<&'static str>`, exactly one
  error or absence, and returns the stored error without reconstruction. This
  adds no wrapper, enum, public type, or new behavior.
- Dependency edges: independent of DX53 through DX65.
- Blocked by: none.
- Unblocks: the remaining Engine semantic-boolean scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay-engine/tests/terminal_custody.rs` (`2` files). Production is
  `+0 / -0`; tests are expected to be line-neutral and no more than
  `+30 / -30 / net +2`; public API is `+0 / -0` types. Existing Behavior,
  Environment phases, events, committed prefixes, error strings, retirement
  assertions, and all six scenario names remain exact. Any changed trace,
  failure provenance, production item, dependency, public API, or aggregate
  failure policy falsifies the correction.
- Result: initialization, activation, and application failure owners now retain
  their exact optional error and return that same value. All five boolean
  fields/parameters, thirteen truth literals, and three internal error
  reconstructions are gone. The complete custody traces and test names remain
  unchanged; no aggregate policy or new type was introduced.
- Verification: all six custody tests pass in debug and optimized profiles with
  complete state/residual/disposition assertions; the complete Engine suite,
  compile fixtures, laws, inversions, properties, docs, and explicit manifest
  closure pass; formatting and strict all-target Engine Clippy pass through the
  pinned Nix shell.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+33 / -33 / net 0`; public API `+0 / -0` types; documentation
  `+50 / -0 / net +50`. The gross test replacement exceeded the `30`-line
  estimate by three because each failure site now carries its exact error at
  setup; the stage remains line-neutral with no added machinery.

## DX67 — Entity Loom admission algebra

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact owner graph remains unchanged and was
  rechecked against the locked Behavior/Actors/Macros, Address, Communication,
  Observe, and Timers sources. Entity's current lifecycle algebra,
  runtime-capability contract, DX49 ownership record, unit tests, runtime tests,
  and Loom fixture were inspected for this stage.
- Ownership and law: Entity lifecycle owns admission closure, outstanding
  delivery reservations, and ordered-fence progress. The independent Loom model
  must represent exactly open admission with a count, draining with a non-zero
  count, or the fence-enqueued terminal state. A successful reservation grants
  the sole capability to submit one resolution.
- Exact blocker and regression: the Loom `Admission` product independently
  stores `closed`, `reservations`, and `fence_enqueued`, admitting closed-zero
  without a fence, open-with-fence, and fenced-with-outstanding-reservations.
  Delivery admission is also reduced to a boolean. The pre-edit structural
  oracle finds those fields and return type. The two exhaustive Loom tests over
  delivery/drain races are the behavioral regressions.
- Proposed representation: replace the coordinated product with
  `Admission::{Open(usize), Draining(NonZeroUsize), Fenced}` and return an opaque
  private `Reservation` only from successful admission. Closing and resolving
  commit one complete valid state; no production type is shared with the
  independent oracle.
- Dependency edges: independent of DX53 through DX66.
- Blocked by: none.
- Unblocks: the remaining Entity concurrency-fixture scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/tests/entity_loom.rs` (`2` files). Production is `+0 / -0`;
  tests are no more than `+65 / -50 / net +15`; public API is `+0 / -0` types.
  The existing mutex linearization, Loom schedules, two delivery contenders,
  drain thread, test names, joins, and terminal assertions remain. The private
  reservation capability deletes the admitted boolean and proves which path may
  resolve. Any reduced schedule space, copied production transition, changed
  terminal law, dependency, production item, or public API falsifies the model.
- Result: the independent model now has only `Open(count)`,
  `Draining(nonzero)`, and `Fenced` states. Successful admission yields one
  private `Reservation`, and only that capability can resolve the count. The
  coordinated booleans and admitted-return boolean are gone; the test model is
  one line smaller and shares no production transition implementation.
- Verification: both admission/fence Loom regressions pass in debug and
  optimized profiles; the complete five-test Entity Loom target and complete
  `bombay-rs` unit, integration, compile-contract, and documentation suites
  pass; formatting and strict all-target `bombay-rs` Clippy pass through the
  pinned Nix shell. The initial malformed two-filter Cargo invocation was
  rejected before compilation and was rerun with the exact shared filter.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+62 / -63 / net -1`; public API `+0 / -0` types; documentation
  `+52 / -0 / net +52`.

## DX68 — Entity Loom activation claim

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph and Entity lifecycle/runtime
  sources rechecked for DX67 remain unchanged. This stage touches only the
  adjacent independent Loom activation-claim oracle; no production owner,
  dependency, or public contract changes.
- Ownership and law: an inactive logical entity admits exactly one activation
  claim. The model's mutex owns one `ActivationClaim` or its absence, while the
  atomic counter independently observes how many activation starts occurred.
- Exact blocker and regression: `Mutex<bool>` stores the claim phase and the two
  contenders interpret `false`/`true` as unclaimed/claimed. The pre-edit
  structural oracle finds that representation. The exhaustive
  `concurrent_claims_start_exactly_one_activation` Loom test is the behavioral
  regression and must still observe a claimed phase plus exactly one start.
- Proposed representation: `Option<ActivationClaim>` is exactly one private
  claim capability or absence. No enum, shared production model, or additional
  synchronization is introduced.
- Dependency edges: independent of DX53 through DX67.
- Blocked by: none.
- Unblocks: the remaining Entity runtime-fixture scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/tests/entity_loom.rs` (`2` files). Production is `+0 / -0`;
  tests are no more than `+10 / -7 / net +3`; public API is `+0 / -0` types.
  The same mutex, two contenders, start counter, Loom schedules, joins, and
  assertions remain. Any changed schedule, start count, synchronization,
  production item, dependency, or public API falsifies the correction.
- Result: the mutex now contains `Option<ActivationClaim>`, so availability is
  absence and the winning contender installs the one claim capability. The
  boolean phase and truth-value assertion are gone; mutex and counter operations
  remain at the same linearization points.
- Verification: the focused claim race passes in debug and optimized profiles
  with exactly one activation start; the complete five-test Entity Loom target,
  formatting, and strict all-target `bombay-rs` Clippy pass through the pinned
  Nix shell. The complete `bombay-rs` suite passed immediately before this
  adjacent isolated stage in DX67.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+8 / -6 / net +2`; public API `+0 / -0` types; documentation
  `+42 / -0 / net +42`.

## DX69 — Entity hash-gate phase

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph and Entity runtime/lifecycle
  contracts rechecked for DX67 and DX68 remain unchanged. This stage touches
  only the real-thread superseded-passivation fixture; no production owner,
  dependency, or public contract changes.
- Ownership and law: `HashGate` owns a deterministic test synchronization
  protocol. It counts selected hash calls, publishes that the third call is
  blocked, and later releases that exact call. Its phase is one of counting,
  blocked, or released; the selected thread identity and call count coexist
  independently with that phase.
- Exact blocker and regression: `HashGateState` coordinates `blocked` and
  `released` booleans, representing blocked-and-released, neither after release,
  and release-before-block combinations. The pre-edit structural oracle finds
  both fields and their wait loops. The caller-visible
  `passivation_reports_superseded_after_incarnation_replacement` race is the
  behavioral regression.
- Proposed representation: one private
  `HashGatePhase::{Counting, Blocked, Released}` sum, selected exhaustively by
  the gate methods and hash implementation. The mutex and condition-variable
  synchronization remain at the same boundaries.
- Dependency edges: independent of DX53 through DX68.
- Blocked by: none.
- Unblocks: the remaining Entity runtime-fixture scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/tests/entity_runtime.rs` (`2` files). Production is `+0 / -0`;
  tests are no more than `+16 / -8 / net +8`; public API is `+0 / -0` types.
  Existing selected thread, third-hash threshold, condition variable,
  passivation/replacement race, test name, and complete outcome assertions
  remain exact. Any changed blocking point, schedule, result, production item,
  dependency, or public API falsifies the correction.
- Result: `HashGateState` now stores exactly one
  `HashGatePhase::{Counting, Blocked, Released}` alongside the independently
  coexisting selected thread and call count. Both coordinated boolean fields
  are gone. The same selected thread blocks on its third hash call and resumes
  only after the test releases that call; no synchronization primitive,
  threshold, or observable assertion changed.
- Verification: the superseded-passivation race passes in debug and optimized
  profiles; all eleven Entity runtime tests pass; formatting and strict
  all-target `bombay-rs` Clippy pass through the pinned Nix shell. The complete
  `bombay-rs` suite passed immediately before this adjacent fixture-only stage.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+16 / -8 / net +8`; public API `+0 / -0` types; documentation
  `+48 / -0 / net +48`.

## DX70 — Observe fuzz state ownership

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the locked Behavior 0.14.0, Behavior Actors 0.14.0, and
  Macros 0.11.4 revision remains
  `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`; Address 0.2.0,
  Communication 0.1.2, and Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0` remain unchanged. Their public
  algebras have no edge to this private Observe oracle. Observe's subject,
  completion, observation, future, waker, and exact-outcome APIs and their
  owning tests were rechecked with all four fuzz targets and the pinned fuzz
  shell declaration.
- Ownership and law: one `(key, epoch)` generation either belongs to the set of
  generations that completed or it does not. Completion membership governs
  readiness, exact waker firing, and whether the last retired observation may
  take its outcome. Active and retired generations obey the identical law.
- Exact blocker and regression: `waker_ops` and `promotion_ops` store completion
  in `HashMap<(u8, u64), bool>` values that are only ever inserted as `true`;
  `future_ops` splits the same fact between a current boolean array and a
  retired-generation boolean map. Its `Fut.cancelled` flag is also initialized
  false for every live vector member, written true only after `pop`, and then
  destroyed without another read; vector membership already owns liveness.
  The pre-edit structural oracle finds all three boolean maps, the flag array,
  the active/retired branch, and the redundant cancellation field. The four
  existing coverage-guided operation alphabets and their exact readiness,
  wake-count, generation-tag, cancellation, move, and drop-accounting
  assertions are the behavioral regressions.
- Proposed representation: use `HashSet<(u8, u64)>` membership directly in
  each independent fuzz target. `future_ops` retains one set across active and
  retired generations because retirement does not change the historical fact;
  presence in its live-future vector represents liveness, while the popped
  owned future supplies the cancellation probes before destruction. No shared
  model, production transition, wrapper, or new fuzz operation is introduced.
- Dependency edges: independent of DX53 through DX69.
- Blocked by: none.
- Unblocks: the remaining Observe fuzz-model scan.
- Change ledger: expected tracked files are this ledger plus
  `future_ops.rs`, `promotion_ops.rs`, and `waker_ops.rs` (`4` files).
  Production is `+0 / -0`; tests must be net-negative and remain within
  `+15 / -35`; public API is `+0 / -0` types. The exact key and epoch domains,
  operation alphabets, iteration bounds, subject/observation/future ownership,
  waker migration and cancellation probes, outcome tags, drop accounting, and
  all oracle messages remain. Any changed fuzz input interpretation, weakened
  assertion, retained semantic boolean, production item, dependency, public
  API, or net-positive test delta falsifies the correction.
- Result: all three independent models now store completed generations as set
  membership. `future_ops` uses the same set before and after retirement, and
  live-vector membership plus ownership of the popped future replaces the
  unread cancellation flag. The boolean maps, current flag array, retired map,
  active/retired lookup branch, field, writes, and guards are gone. Test code
  is twenty lines smaller with no shared model or production machinery.
- Verification: all four fuzz binaries build in the pinned nightly fuzz shell;
  `ops`, `future_ops`, `promotion_ops`, and `waker_ops` each completed 10,000
  bounded runs without an artifact, with the final three affected targets
  replayed after their last edits. All 121 private Observe tests and docs pass;
  strict Clippy passes for the complete Observe library and the three affected
  fuzz bins; root formatting and exact formatting of the affected fuzz files
  pass. The independent workspace's whole-format and whole-Clippy probes also
  exposed pre-existing drift and two `ops.rs` lints outside this stage; no
  unrelated source was absorbed.
- Actual checkpoint: tracked files `4`; production `+0 / -0 / net 0`; tests
  `+59 / -79 / net -20`; public API `+0 / -0` types; documentation
  `+69 / -0 / net +69`. Gross test churn exceeded the forecast because the
  strict lint correction flattened the three completion branches and removed
  the independently proven cancellation guards; the required net-negative
  boundary and all stop thresholds remain satisfied.

## DX71 — Entity activation-gate phase

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: both retained locks select Behavior Core 0.17.0 and
  Behavior Actors 0.17.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, plus Behavior Macros 0.12.0
  at that same revision. Address 0.2.0, Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe
  remain selected. Their exact manifests, public source, relevant tests, and
  current runtime documentation were rechecked with Entity's runtime,
  directory, lifecycle, and real-thread regressions. None owns or consumes
  this private fixture gate.
- Ownership and law: `ActivationGate` owns one synchronization phase. A closed
  gate may retain one current waiter; an open gate retains no waiter. Polling
  and opening must linearize through the same owner so opening either observes
  and wakes the registered waiter or makes every later poll ready. This is a
  test-fixture policy, not a Behavior, actor-template, runtime, or Entity
  production law.
- Exact blocker and regression: `ActivationGate` stores its mutually exclusive
  closed/open phase in `AtomicBool` while a separate `Mutex<Option<Waker>>`
  stores the capability valid only while closed. The representation admits an
  open gate with a retained waker and requires a second atomic phase read after
  registration to repair the split ownership. The pre-edit structural oracle
  finds the `open: AtomicBool`, separate waker mutex, release/acquire stores and
  loads, and duplicate phase check. The existing cancellation, owned-delivery,
  and repeated-passivation tests are the observable regression suite and pass
  before the representation changes.
- Proposed representation: one private
  `ActivationGatePhase::{Closed(Option<Waker>), Open}` sum under the existing
  mutex. `open` consumes the closed waiter's exact capability, commits `Open`,
  releases the lock, and then wakes it. `poll` either updates the one closed
  waiter or returns ready from `Open`. No production state, public type,
  runtime behavior, synchronization dependency, or test schedule changes.
- Aggregate-drift checkpoint: before and after, the fixture has one gate owner
  and two control states. Subordinate phase alternatives change from a boolean
  plus independently optional waker to the two alternatives above; the poll
  branch count drops from three phase checks to one exhaustive match; modules
  and public spellings remain unchanged. The surviving optional value is
  exactly the one current waker valid only in `Closed`. The residue scan must
  find no gate phase boolean, repeated cause, false cardinality, nested
  transition authority, or structural user syntax. The relevant Entity and
  runtime contracts require no normalization change. Disposition: `pass`.
- Dependency edges: independent of DX53 through DX70; selected from the
  remaining Entity runtime-fixture scan exposed by DX69.
- Blocked by: none.
- Unblocks: the remaining Entity runtime-fixture semantic-state scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/tests/entity_runtime.rs` (`2` feature-local files; one new path
  in the cumulative working tree). Production is `+0 / -0`; tests are expected
  within `+18 / -18 / net +8`; public API is `+0 / -0` types. One private enum
  replaces the phase boolean and separate waker field. The existing mutex,
  future, three callers, polling results, wake timing, admission and
  passivation traces, test names, and assertions remain exact. Any production
  edit, public API change, additional synchronization owner, changed schedule,
  or retained gate phase boolean falsifies the stage.
- Result: `ActivationGate` now owns exactly one
  `ActivationGatePhase::{Closed(Option<Waker>), Open}` value under its mutex.
  Opening consumes the exact closed waiter, commits `Open`, releases the lock,
  and only then wakes it. Polling performs one exhaustive phase match. The
  atomic gate flag, separate waker mutex, duplicate phase read, and impossible
  open-with-waker combination are gone; production and public API are
  unchanged.
- Verification: the pre-edit structural oracle found the atomic phase field,
  separate waker mutex, release/acquire operations, and duplicate poll check;
  the post-edit oracle finds none. All eleven Entity runtime tests pass in
  debug and optimized profiles, including the three gate users. The complete
  `bombay-rs` package suite, formatting, strict all-target Clippy, and
  whitespace checks pass through the pinned Nix shell.
- Actual checkpoint: the user explicitly authorized the cumulative tree from
  37 to 38 paths. Feature-local production is `+0 / -0 / net 0`; tests are
  `+18 / -13 / net +5`; documentation is `+76 / -0 / net +76`; public API is
  `+0 / -0` types. One private enum is added with no module or public spelling
  change. The complete cumulative tree is 38 paths at `+685 / -218 / net +467`.

## DX72 — Entity runtime failure selection

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: both retained locks still select Behavior Core 0.17.0
  and Behavior Actors 0.17.0 with their exact registry checksums, plus Behavior
  Macros 0.12.0. Address 0.2.0, Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe remain
  selected. Their public source and tests, Entity's `LocalEntityRuntime`,
  directory rejection, lifecycle refusal, and the complete real-thread
  fixture were rechecked after DX71. None owns these private failure choices.
- Ownership and law: `TestRuntime` independently selects whether activation
  returns its exact `ActivationError` and whether delivery returns the exact
  admitted command. Those choices coexist, but each is exactly one selected
  failure or its absence. The runtime methods must observe the stored choice
  without inferring or reconstructing policy from an unrelated counter.
- Exact blocker and regression: `fail_activation` and `fail_delivery` encode
  those two semantic choices as `AtomicBool`; construction and three test
  mutations use unexplained truth literals, and both runtime methods branch on
  a truth value before constructing their failure. The pre-edit structural
  oracle finds both fields, both initializers, both loads, and all three
  stores. `failed_delivery_returns_the_original_command` and
  `failed_activation_returns_the_command_and_eventually_allows_retry` are the
  caller-visible regressions; the complete Entity runtime binary confirms the
  independent success paths. All pass in debug and optimized profiles before
  the representation changes.
- Proposed representation: `activation_failure: Mutex<Option<()>>` owns the
  exact unit activation error or absence; `delivery_rejection:
  Mutex<Option<()>>` owns the independent rejection selection or absence.
  Existing methods match those sums exhaustively, and tests select `Some(())`
  or `None` directly. No new named type, production state, public API, runtime
  synchronization requirement, retry schedule, error, command, or trace is
  introduced.
- Aggregate-drift checkpoint: `TestRuntimeState` remains one product because
  activation and delivery failure selections may coexist independently. Each
  subordinate policy remains two alternatives, but the stored value changes
  from an erased truth flag to the direct optional failure selection. Branch
  counts, modules, public spellings, and transition authority remain
  unchanged. The exact future decision required from each option is whether
  that operation returns its owned failure. The residue scan must find no
  `fail_activation`, `fail_delivery`, `AtomicBool`, truth-literal policy,
  repeated cause, false cardinality, or structural user syntax in the fixture.
  The Entity and runtime contract cross-check requires no normalization
  change. Disposition: `pass`.
- Dependency edges: independent of DX53 through DX71; selected from the
  remaining Entity runtime-fixture scan exposed by DX71.
- Blocked by: none.
- Unblocks: the remaining Entity runtime-fixture semantic-state scan.
- Change ledger: expected tracked files are this ledger and the already changed
  `crates/bombay/tests/entity_runtime.rs` (`2` feature-local files, `38`
  cumulative paths). Production is `+0 / -0`; tests are expected within
  `+18 / -24 / net +4`; public API is `+0 / -0` types. Existing
  `TestRuntimeState`, runtime methods, unit activation error, exact rejected
  command, counters, retry loop, test names, and complete assertions are
  reused. Any production edit, new named policy abstraction, changed
  synchronization point, failure, payload, trace, or public syntax falsifies
  the stage.
- Result: `TestRuntimeState` now stores `activation_failure` and
  `delivery_rejection` as independent `Mutex<Option<()>>` values. Activation
  returns the selected unit error; delivery returns the exact admitted command
  when rejection is selected. The fixture tests choose `Some(())` or `None`
  directly. Both atomic flags, their initializers, loads, stores, and five
  truth literals are gone without adding a named abstraction or changing any
  production contract.
- Verification: the pre-edit oracle found both boolean fields, both
  initializers, both loads, and all three stores; the post-edit residue scan
  finds none. Both focused failure/retry regressions and all eleven Entity
  runtime tests pass in debug and optimized profiles. Formatting, strict
  all-target `bombay-rs` Clippy, and whitespace checks pass through pinned
  Nix. The complete `bombay-rs` suite passed at the immediately preceding
  DX71 checkpoint before this fixture-only stage.
- Actual checkpoint: cumulative paths remain `38`. Feature-local production is
  `+0 / -0 / net 0`; tests are `+17 / -24 / net -7`; documentation is
  `+77 / -0 / net +77`; public API is `+0 / -0` types. No type, module, or
  public spelling was added. The cumulative tree is `+779 / -242 / net +537`.

## DX73 — Entity replacement admission completion

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact immutable Behavior Core and Actors 0.17.0 and
  Macros 0.12.0 selections remain unchanged in both locks. Address 0.2.0,
  Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe remain
  selected. Their public contracts and tests, Entity admission, passivation,
  activation identity, exact rejected-command return, and the complete
  real-thread fixture were rechecked after DX72. Entity production has no
  working-tree delta and no owner consumes this private completion breadcrumb.
- Ownership and law: the bounded replacement-admission loop owns the command
  until one attempt succeeds or returns the same rejected command for another
  attempt. Success is the loop's control-flow exit; no later decision needs a
  stored history bit. Exhausting the exact 1,000-attempt range is the fixture
  failure.
- Exact blocker and regression: the superseded-passivation test initializes
  `replacement_activated = false`, sets it to `true` only immediately before
  leaving the loop, and then asserts it. The field stores only the history that
  the success branch ran. The pre-edit structural oracle finds the false
  initialization, true write, and assertion. The caller-visible
  `passivation_reports_superseded_after_incarnation_replacement` regression
  preserves exact replacement activation, superseded passivation, command
  custody, and activation count, and passes in debug and optimized profiles
  before the representation changes.
- Proposed representation: a domain-named labeled block encloses the existing
  `0..1_000` range. Successful admission exits that block immediately; every
  rejection retains and retries the returned command; range exhaustion reaches
  the existing failure message directly. No result wrapper, state type,
  helper, unbounded loop, production edit, public API, retry count, command,
  schedule, or assertion changes.
- Aggregate-drift checkpoint: the test has the same command and bounded retry
  range before and after. Subordinate semantic state drops from one two-valued
  history flag to none; the success branch loses one mutation and the later
  assertion becomes the range-exhaustion terminal. Modules, public spellings,
  and transition authority remain unchanged. No future decision needs the
  deleted value. The residue scan must find no replacement-completion boolean,
  arrival history, repeated cause, false cardinality, nested transition
  authority, or structural user syntax. The Entity and runtime contract
  cross-check requires no normalization change. Disposition: `pass`.
- Dependency edges: independent of DX53 through DX72; selected from the
  remaining Entity runtime-fixture scan exposed by DX72.
- Blocked by: none.
- Unblocks: the next repository semantic-state scan.
- Change ledger: expected tracked files are this ledger and the already changed
  `crates/bombay/tests/entity_runtime.rs` (`2` feature-local files, `38`
  cumulative paths). Production is `+0 / -0`; tests must be net-negative and
  remain within `+4 / -10`; public API is `+0 / -0` types. The existing
  bounded range, command, admission result, exact rejection, thread yield,
  failure message, passivation result, activation count, and test name are
  reused. Any new state type, changed retry bound, lost command, altered
  schedule, production edit, or public syntax falsifies the stage.
- Result: the replacement admission now runs inside the labeled
  `replacement_admission` block. Success exits that block immediately,
  unavailable admission returns the exact command to the next bounded attempt,
  unexpected failures retain their existing panic, and exhausting the exact
  range reaches the existing completion failure directly. The history boolean,
  false initialization, true write, and later assertion are gone without a
  replacement state or result wrapper.
- Verification: the pre-edit oracle found all three boolean-history sites; the
  post-edit residue scan finds none. The superseded-passivation regression and
  all eleven Entity runtime tests pass in debug and optimized profiles.
  Formatting, strict all-target `bombay-rs` Clippy, and whitespace checks pass
  through pinned Nix.
- Actual checkpoint: cumulative paths remain `38`. Feature-local production is
  `+0 / -0 / net 0`; tests are `+12 / -17 / net -5`; documentation is
  `+74 / -0 / net +74`; public API is `+0 / -0` types. Gross test churn
  exceeded the `+4 / -10` forecast because retaining the explicit `0..1_000`
  range inside the labeled success boundary indents its ownership-preserving
  match; the stage remains net-negative and adds no type, module, helper, or
  public spelling. The cumulative tree is `+865 / -259 / net +606`.

## DX74 — Observe waker-selection ownership

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: both locks still select immutable Behavior Core and
  Actors 0.17.0 and Macros 0.12.0 with their exact registry checksums. Address
  0.2.0, Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe remain
  selected. Their source, public contracts, relevant tests, and current
  runtime documentation were rechecked with Observe's slot, waiter storage,
  shared/affine futures, direct registrations, cancellation tests, independent
  model, exhaustive histories, stress corpus, fuzz targets, and Loom harness.
  No external owner consumes these private traversal values.
- Ownership and law: one future registration either joins the exact existing
  physical waker entry or installs a new entry. Deregistration retains one
  exact target waker until its one logical owner is removed, after which no
  later waiter may consume that target. Shared physical entries preserve every
  sibling future owner; migration installs the successor before removing the
  predecessor under the same waiter lock.
- Exact blocker and regression: `update_future_waker` records a successful
  join in `joined_existing: bool`, then reconstructs absence by negating it;
  `remove_future_waker_owner` records target consumption in `removed: bool`
  while the exact target remains separately borrowed. The pre-edit structural
  oracle finds both mutable truth flags and their writes. Shared-waker sibling
  cancellation, repeated cancellation, many-waker migration, and the affine
  migration/cancellation Loom witnesses are the observable regressions and
  pass in debug, optimized, and modeled execution before the edit.
- Proposed representation: joining directly selects
  `Option<&mut WakerOwnership>` from the waiter traversal and exhaustively
  increments the selected ownership or installs a new physical entry.
  Deregistration retains `Option<&Waker>` while that exact target still awaits
  removal, consumes it at the first matching physical entry, and leaves it
  absent for later entries. These standard sums preserve the values that drive
  the next decision; no wrapper, helper, callback, allocation, waiter shape,
  public API, or synchronization boundary is added.
- Aggregate-drift checkpoint: Observe retains the same slot states,
  `Waiters::{Empty, One, Many}` storage, and
  `WakerOwnership::{Futures, Persistent}` alternatives. Two subordinate
  boolean breadcrumbs become direct optional references; ownership branches
  and one-lock linearization remain unchanged. Modules and public spellings do
  not change. The surviving values are exactly the existing ownership entry
  selected for increment and the target still awaiting removal. The residue
  scan must find no join/removal history boolean, repeated cause, false
  cardinality, nested transition authority, or structural user syntax. The
  Observe laws and test models require no normalization change. Disposition:
  `pass`.
- Dependency edges: independent of DX53 through DX73; selected by the
  repository-wide production semantic-state scan after DX73.
- Blocked by: none. The user explicitly authorized path 39 and pre-authorized
  later containment checkpoints for the continuing research loop.
- Unblocks: the remaining Observe production semantic-state scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/src/observe/mod.rs` (`2` feature-local files; one new path in
  the cumulative working tree). Production must be net-negative and remain
  within `+14 / -26`; tests are `+0 / -0`; public API is `+0 / -0` types.
  Existing `Slot`, `Waiters`, `WakerOwnership`, iterator traversal, completion
  checks, lock, registration order, exact wakers, and every current model and
  regression are reused. Any added type or helper, changed public contract,
  allocation, ordering, physical-entry count, wake trace, or synchronization
  boundary falsifies the stage.
- Result: `update_future_waker` now obtains
  `Option<&mut WakerOwnership>` directly from the waiter traversal and matches
  the selected ownership or the need for a new physical entry. Waker removal
  carries `Option<&Waker>` until the first matching physical entry consumes
  that exact target. The join and removal history booleans, their writes, and
  the reconstructed absence checks are gone. Waiter storage, ownership counts,
  registration order, synchronization, wake behavior, and public contracts are
  unchanged; no type, helper, allocation, or public spelling was added.
- Verification: the pre-edit oracle found both mutable history booleans and the
  post-edit residue scan finds neither. All twelve focused cancellation and
  migration regressions pass in debug and optimized profiles. All 121 private
  Observe tests and docs pass in both profiles. Four affine Loom interleavings,
  including migration and cancellation, pass against the final source. The
  `ops`, `future_ops`, `promotion_ops`, and `waker_ops` fuzz targets each
  completed 10,000 bounded runs without an artifact. Formatting, strict
  all-target Clippy for both the production package and independent Observe
  harness, and whitespace checks pass through pinned Nix.
- Actual checkpoint: cumulative paths are `39`. Feature-local production is
  `+24 / -26 / net -2`; tests are `+0 / -0 / net 0`; public API is
  `+0 / -0` types. Gross production churn exceeded the `+14 / -26` forecast
  because selecting the ownership directly shifts both exhaustive ownership
  arms beneath the option match and spelling target consumption adds an
  explicit absence arm. The stage remains net-negative and adds no type,
  helper, branch alternative, module, or public spelling. The cumulative tree
  is `+976 / -285 / net +691`.

## DX75 — Observe deadline parking value

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: both locks still select crates.io Behavior Core and
  Actors 0.17.0 and Macros 0.12.0 with checksums
  `9cbbddbb53a81e4cba528a42ff4c9c84b3a0afb568c8af8590c566c3e394f48e`,
  `81acddfd6d22b6ca993553a0707716f136d8df11bd654ce86c5c9ebd24a124f0`,
  and `54320a7ad83f594a9f6875773a6eee28d3b2de8aee609d32016401e71923b33b`.
  Address 0.2.0, Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe
  remain selected. Their source, public contracts, tests, and current runtime
  documents were rechecked. Observe alone owns the private thread-parking
  boundary; no dependency or runtime interpreter consumes its intermediate
  deadline decision.
- Ownership and law: after registration and a completion recheck, a timed wait
  either owns one strictly positive duration for which the current thread may
  park or has reached its deadline and must remove its exact thread waiter
  before returning `None`. Completion is checked before deadline expiry on
  every loop iteration. The Loom branch has no clock and parks once before the
  modeled completion recheck.
- Exact blocker and regression: both cfg variants of `park_until` return a
  boolean that erases the distinction between the positive remaining duration
  and deadline absence; the non-Loom caller negates that truth value to
  reconstruct the timeout branch, while the Loom caller discards an invariant
  `true`. The pre-edit structural oracle finds both boolean return signatures,
  the `false`/`true` literals, the negated call, and the discarded Loom call.
  Nine timeout contract, boundary, panic-safety, and stress regressions pass in
  debug and optimized profiles before the edit; the modeled completed path
  also passes.
- Proposed representation: derive
  `Option<Duration>` directly with `Instant::checked_duration_since`, filtering
  out the zero boundary. `Some(remaining)` owns the exact capacity passed to
  `park_timeout`; `None` enters the existing exact waiter-deregistration path.
  The Loom branch calls `park` directly. The two forwarding helpers and all
  semantic truth literals disappear; no type, helper, public API, allocation,
  synchronization point, timeout, waiter shape, or branch alternative is
  added.
- Aggregate-drift checkpoint: `Observation::wait_timeout` remains the sole
  owner of the deadline, registration, loop, completion-before-deadline order,
  and exact deregistration. One subordinate boolean choice becomes the value
  that actually governs parking. Equality remains an elapsed deadline because
  zero duration is filtered out. The residue scan must find no `park_until`,
  semantic deadline boolean, repeated cause, false cardinality, nested
  transition authority, or structural user syntax. Observe's public contract,
  independent model, stress corpus, and Loom normalization do not change.
  Disposition: `pass`.
- Dependency edges: independent of DX53 through DX74; selected by the
  remaining Observe production semantic-state scan after DX74.
- Blocked by: none.
- Unblocks: the remaining Observe production semantic-state and public result
  scan.
- Change ledger: expected tracked files are this ledger and the already changed
  `crates/bombay/src/observe/mod.rs` (`2` feature-local files, `39` cumulative
  paths). Production must be net-negative and remain within `+10 / -20`; tests
  are `+0 / -0`; public API is `+0 / -0` types. Existing `Instant`, `Duration`,
  `park_timeout`, `park`, waiter lock, thread identity, registration,
  deregistration, loop ordering, return values, and all current regressions are
  reused. Any new type or helper, changed public contract, timing order,
  waiter retention, allocation, synchronization, or branch alternative
  falsifies the stage.
- Result: the non-Loom timeout branch now derives an optional remaining
  duration from the deadline, rejects the zero boundary, and passes the exact
  positive duration directly to `park_timeout`. Absence enters the existing
  exact thread-waiter deregistration path. The Loom branch calls `park`
  directly. Both `park_until` helpers, both boolean return signatures, their
  truth literals, the negated call, and the discarded Loom result are gone.
  No type, helper, public API, allocation, or synchronization point was added.
- Verification: the pre-edit structural oracle found both boolean helpers and
  all four history/decision sites; the post-edit residue scan finds none. The
  nine focused timeout contract, boundary, panic-safety, and stress tests pass
  in debug and optimized profiles. The modeled completed-timeout path passes
  under Loom. All 121 private Observe tests and docs pass in both debug and
  optimized profiles. Formatting, strict all-target Clippy for both the
  production package and independent Observe harness, and whitespace checks
  pass through pinned Nix.
- Actual checkpoint: cumulative paths remain `39`. Feature-local production is
  `+7 / -23 / net -16`; tests are `+0 / -0 / net 0`; public API is
  `+0 / -0` types. Deletions exceed the `-20` forecast because both cfg helper
  definitions and their documentation disappear completely. The stage is
  net-negative and adds no type, module, helper, or public spelling. The
  cumulative tree is `+1067 / -308 / net +759`.

## DX76 — Observe future-registration custody

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: both locks still select the exact crates.io Behavior
  Core and Actors 0.17.0 and Macros 0.12.0 checksums recorded in DX75. Address
  0.2.0, Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe
  remain unchanged. Their source, APIs, relevant tests, and runtime documents
  were rechecked. Observe alone owns the private future-registration protocol;
  no dependency, interpreter, or public caller observes its intermediate
  return value.
- Ownership and law: `FutureRegistration::update` clones the task's candidate
  waker before registry mutation so a panicking clone cannot disturb the prior
  registration. The slot consumes that candidate while attempting an atomic
  install or migration. It returns the exact owned candidate only when the
  registry commit succeeded and local bookkeeping must replace the previous
  waker; completion returns no candidate and leaves local bookkeeping intact.
- Exact blocker and regression: `Slot::update_future_waker` borrows the owned
  candidate and returns `true` for completion or `false` for installation.
  Its caller then reconstructs custody from the negated truth value, retaining
  the separately owned candidate only on `false`. The pre-edit structural
  oracle finds the boolean signature, two completion `true` returns, two
  installed `false` returns, and caller branch. All twelve future cancellation
  and migration regressions and the complete 121-test Observe suite pass in
  debug and optimized profiles before the edit. Four affine Loom witnesses,
  including migration and cancellation, also pass.
- Proposed representation: the slot consumes `next: Waker` and returns
  `Option<Waker>`. `Some(next)` means installation committed and supplies the
  exact capability for local bookkeeping; `None` means completion won and no
  registration exists to remember. Existing waiter entries still receive a
  clone because registry and future each lawfully own one waker. No named type,
  helper, public API, clone, allocation, synchronization point, ownership
  count, or branch alternative is added.
- Aggregate-drift checkpoint: slot registration remains the sole owner of the
  completion checks, waiter lock, join/install choice, migration ordering, and
  previous-owner removal. `FutureRegistration` remains the sole owner of its
  current local waker. One subordinate boolean result becomes the exact
  optional capability crossing that ownership boundary. The residue scan must
  find no private registration-result boolean, truth literal, reconstructed
  custody, repeated cause, false cardinality, nested transition authority, or
  structural user syntax. Observe's public API, models, fuzz alphabets, and
  Loom normalization do not change. Disposition: `pass`.
- Dependency edges: independent of DX53 through DX75; selected by the
  remaining Observe production semantic-state scan after DX75.
- Blocked by: none.
- Unblocks: the remaining Observe production semantic-state and public result
  scan.
- Change ledger: expected tracked files are this ledger and the already changed
  `crates/bombay/src/observe/mod.rs` (`2` feature-local files, `39` cumulative
  paths). Production must remain within `+12 / -10 / net +2`; tests are
  `+0 / -0`; public API is `+0 / -0` types. Existing `FutureRegistration`,
  `Option<Waker>`, candidate clone-before-mutation, slot completion checks,
  waiter lock, physical waker clone, ownership counts, migration order, exact
  previous waker, and all current regressions are reused. Any new type or
  helper, extra clone, changed panic boundary, lost waker, altered waiter
  ownership, public contract, synchronization, or trace falsifies the stage.
- Result: `Slot::update_future_waker` now consumes the candidate waker and
  returns `Some(exact_waker)` only after the waiter registry has committed its
  installation or migration. Completion returns `None`. The caller
  exhaustively destructures that optional capability and updates local
  bookkeeping only when it owns the returned waker. The private boolean result,
  four truth returns, borrowed-candidate custody reconstruction, and negated
  caller branch are gone. Clone-before-mutation and the one registry clone are
  unchanged; no type, helper, clone, allocation, or public spelling was added.
- Verification: the pre-edit structural oracle found the boolean signature,
  four truth returns, and caller branch; the post-edit residue scan finds none.
  All twelve future cancellation and migration regressions and all 121 private
  Observe tests and docs pass in debug and optimized profiles. Four affine Loom
  interleavings pass, including migration and cancellation. The `ops`,
  `future_ops`, `promotion_ops`, and `waker_ops` fuzz targets each completed
  10,000 bounded runs without an artifact. Formatting, strict all-target
  Clippy for both the production package and independent Observe harness, and
  whitespace checks pass through pinned Nix.
- Actual checkpoint: cumulative paths remain `39`. Feature-local production is
  `+8 / -8 / net 0`; tests are `+0 / -0 / net 0`; public API is
  `+0 / -0` types. The stage exactly preserves production size and adds no
  type, module, helper, clone, or public spelling. The cumulative tree is
  `+1156 / -316 / net +840`.

## DX77 — Observe direct-registration readiness

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` remains the sole selection authority and
  resolves the exact published Behavior Core and Actors 0.17.0 plus Macros
  0.12.0 registry graph recorded above. The exact 760-line owner instructions,
  registry source/APIs/tests, Address 0.2.0, Communication 0.1.2, Timers at its
  selected Git revision, Bombay-private Observe source, direct callers, models,
  exhaustive/stress/panic/pool/pair/cancellation tests, fuzz targets, and
  current runtime capability contract were rechecked. No dependency owns or
  consumes this direct Observe return value.
- Ownership and law: one direct registration returns one of two exhaustive
  readiness facts. `Poll::Pending` means the generation is not published and
  this call establishes the existing wake obligation; `Poll::Ready(())` means
  publication won and the caller can read directly. A completion race may
  still leave a harmless registration that produces a documented spurious
  wake, but it does not change the caller's readiness fact. The standard
  `Poll<()>` sum already owns this exact pending/ready protocol in Observe's
  future implementations.
- Exact blocker and regression: public `Observation::register_waker` encodes
  these alternatives as `bool`, documents `true`/`false`, and forces every
  caller, model, stress test, and fuzz target to negate or branch on an unnamed
  truth value. Before production changes, the independent contract regression
  must require `Poll::Pending` before completion and `Poll::Ready(())` after
  completion; the prior signature must fail to compile with an exact
  `expected bool, found Poll` mismatch.
- Proposed representation: change only the return type and its two branch
  values to `Poll<()>`, then migrate each direct caller to exhaustive
  `Poll::{Pending, Ready(())}` comparison or matching. Reuse the existing
  module-scope `Poll` import, state word, waiter lock, exact waker storage,
  registration order, wake rules, tests, models, and fuzz alphabets. Add no
  wrapper, conversion trait, compatibility method, default, allocation, or
  second readiness owner.
- Prior-representation proof: with production unchanged, the independent
  contract now requires `Poll::Ready(())` for both retired-completed and
  move-out cases. Pinned-Nix compilation fails at both calls with E0308,
  `expected bool, found Poll<()>`; this is the intended public syntax failure.
- Dependency edges: independent of DX53 through DX76; selected by the public
  result scan after DX76.
- Blocked by: none.
- Unblocks: DX78.
- Change ledger: expected tracked paths are this ledger,
  `crates/bombay/src/observe/mod.rs`, its unit tests and eight direct external
  test modules, plus `crates/observe-fuzz/fuzz_targets/{ops,waker_ops}.rs`—at
  most `13` feature-local paths, all already within the authorized continuing
  research tree. Production must remain within `+6 / -6 / net 0`; tests must
  remain within `+55 / -55 / net +10`; public API is `+0 / -0` types. The one
  existing public method changes its result spelling but not its behavior.
  Any new abstraction, changed registration/wake trace, lost exact outcome,
  altered operation alphabet, production growth above the bound, or additional
  public item falsifies the stage.
- Planned verification: capture the prior compile failure; run the focused
  contract in debug and optimized profiles; all direct-registration unit,
  model, exhaustive, stress, panic, pool, pair, and cancellation tests; the
  complete 121-test Observe suite and docs in both profiles; applicable affine
  Loom; all four fuzz builds and 10,000-run campaigns; formatting, strict
  all-target Clippy for production and the independent harness, workspace
  tests, diff/whitespace review, and exact delta accounting.
- Result: `Observation::register_waker` now returns the existing
  `Poll<()>` algebra. Both completion checks yield `Ready(())`; an installed or
  deduplicated wake obligation yields `Pending`. Every direct caller, model,
  stress path, and fuzz oracle now carries or exhaustively matches that result.
  The redundant method-level `#[must_use]` disappeared because `Poll` owns that
  contract. No new type, wrapper, allocation, state, synchronization point, or
  runtime path was added.
- Verification: the unchanged implementation failed the independent contract
  at both new readiness assertions with E0308 (`expected bool, found Poll`).
  The final source passes all 121 private Observe tests and docs in debug and
  optimized profiles, all 28 bounded Observe Loom models, the full workspace
  suite, formatting, strict workspace/production/independent-harness Clippy,
  and strict Clippy for all four fuzz bins. `ops`, `future_ops`,
  `promotion_ops`, and `waker_ops` each completed a fresh 10,000-run campaign
  without an artifact. The two pre-existing `ops.rs` nested-map lints recorded
  by DX70 were resolved with `HashMap::Entry` because H26 brought that target
  into the strict final-source gate; its operation alphabet and outcomes are
  unchanged. Residue and whitespace scans are clean.
- Actual checkpoint: `13` feature-local paths, increasing the cumulative tree
  from `39` to `50` paths because the production module and ledger were already
  present. Production is `+9 / -10 / net -1`; tests and fuzz targets are
  `+164 / -103 / net +61`; documentation is `+88 / -0 / net +88`; public API
  is `+0 / -0` types. Gross test churn exceeded the forecast because all 42
  public-call sites adopted the exhaustive result, mutating registrations were
  separated from assertions, and the already-recorded `ops.rs` lint residue
  was removed. No abstraction or runtime machinery was added. The complete
  cumulative tracked tree is `+1417 / -429 / net +988`.

## DX78 — local admission-closure disposition

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` remains the sole selection authority and
  still resolves the exact published Behavior Core and Actors 0.17.0 plus
  Macros 0.12.0 registry graph recorded above; the crates.io records confirm
  those releases remain current and not yanked. The exact 760-line Behavior
  instructions at the recorded owner revision, its Behavior/Actions,
  settlement, and typed-shutdown contracts, Communication 0.1.2's affine
  `MailboxOwner::close_admission`, Address 0.2.0's exact lease retirement,
  Timers at its selected Git revision, Bombay-private Observe publication,
  every local admission-closure caller, public shutdown/external-actor tests,
  and the current runtime, module, Driver-law, and Driver-test documents were
  rechecked. No selected dependency changed after DX77.
- Ownership and law: Communication owns the one affine mailbox-admission
  capability and consumes it to linearize closure. Bombay's private
  `Admission` owns the shared arbitration around that capability. Exactly one
  close attempt produces `Closed`; every later attempt, or an endpoint whose
  admission owner has already retired, produces `AlreadyClosed`. The public
  shutdown boundary maps the first disposition to one typed shutdown request
  and the latter to the existing exact `AlreadyStopping` or `AlreadyStopped`
  rejection. Retirement and external-boundary cleanup explicitly discharge
  either disposition because both satisfy their idempotent close policy.
- Exact blocker and regression: `Admission::close` and
  `EndpointMailbox::close_admission` return `Option<()>`. `Some(())` is an
  unnamed unit sentinel for winning the lifecycle transition; `None` merges a
  repeated close with a retired weak owner. Shutdown reconstructs the semantic
  disposition with `is_none`, while four cleanup sites enumerate
  `Some(()) | None` without naming the facts. Before production changes, a
  focused caller regression must require `AdmissionClosure::Closed` followed
  by `AdmissionClosure::AlreadyClosed`; the prior representation must fail to
  compile because it returns `Option<()>` and has no named alternatives.
- Proposed representation: add one private, must-use
  `AdmissionClosure::{Closed, AlreadyClosed}` sum beside `Admission`; return it
  exhaustively from both close boundaries; let shutdown match the alternatives
  directly; and let retirement and external cleanup explicitly discharge both.
  Reuse the existing owner mutex, weak upgrade, Communication close operation,
  shutdown rejection sum, retirement order, and public methods. Add no public
  item, wrapper around a stored value, allocation, lock, clone, callback, or
  runtime branch.
- Prior-representation proof: with production unchanged, the focused caller
  regression required `AdmissionClosure::Closed` followed by
  `AdmissionClosure::AlreadyClosed`. Pinned-Nix compilation failed at both
  assertions with E0433 because the unit-sentinel representation had no named
  closure disposition. This is the intended model failure.
- Abstraction budget: the private sum owns one unique transition disposition,
  replaces the unit sentinel and absence inference, and is consumed by five
  concrete shutdown/retirement/external-boundary sites. Existing concrete
  values cannot name both alternatives after the affine owner is consumed;
  `bool` would erase them and `Option<()>` is the rejected sentinel protocol.
  The type stores no state and creates no second lifecycle owner.
- Aggregate-drift checkpoint: `Admission` remains the sole Bombay owner of
  shared closure arbitration; Communication remains the sole owner of mailbox
  closure; `ActorRef` remains the sole mapper to public shutdown rejection.
  Public shutdown syntax and outcomes, accepted-prefix draining, stale exact
  endpoint rejection, lease retirement, task order, and terminal publication
  do not change. The residue scan must find no production `Option<()>`,
  `Some(()) | None`, or closure `is_none` inference in this protocol.
  Disposition: `pass`.
- Dependency edges: sequenced after DX77's completed public-result scan and
  independent of DX53 through DX76.
- Blocked by: DX77 (resolved).
- Unblocks: DX79.
- Change ledger: expected tracked paths are this ledger,
  `crates/bombay/src/local.rs`, and `crates/bombay/src/actor_interface.rs`
  (`3` feature-local paths, at most `52` cumulative paths). Production must
  remain within `+30 / -20 / net +15`; tests within `+20 / -0 / net +20`;
  public API is `+0 / -0` types. The stage adds one private sum and removes the
  two unit-sentinel return protocols plus all five inference/discharge sites.
  Any public spelling change, extra state owner, new branch alternative,
  changed shutdown count/classification, lost prefix, stale delivery
  acceptance, retirement-order change, or bound overrun falsifies the stage.
- Planned verification: capture the prior compile failure; run the focused
  admission-disposition and exact-shutdown tests in debug and optimized
  profiles; external-actor prefix/stale-recipient and application lifecycle
  regressions; complete `bombay-rs` tests; formatting; strict all-target
  Clippy; workspace tests; residue, diff, whitespace, and exact-delta review.
- Result: `Admission::close` and `EndpointMailbox::close_admission` now return
  the private must-use `AdmissionClosure::{Closed, AlreadyClosed}` sum.
  `Admission` also owns classification through a weak endpoint, while the
  standard and Entity endpoint arms remain distinct because they carry
  different ingress types. The public shutdown boundary exhaustively maps the
  winning disposition to its one control request and an already-closed
  disposition to the existing stopping/stopped rejection. Retirement and
  external cleanup explicitly discharge both alternatives. The two
  `Option<()>` signatures, unit sentinel, absence inference, and all four
  `Some(()) | None` sites are gone.
- Verification: the prior source failed the focused named-disposition contract
  as intended. Both the disposition and exact shutdown-count regressions pass
  in debug and optimized profiles. The external actor still drains its
  accepted prefix and rejects a stale exact recipient; the application
  lifecycle still separates shutdown acceptance, repeated rejection, and
  termination. All 167 active `bombay-rs` unit tests plus its integration,
  compile, and documentation suites pass; the full workspace passes. Formatting,
  strict package and workspace all-target Clippy, the closure-protocol residue
  scan, and whitespace checks pass through pinned Nix. An attempted combined
  endpoint match arm failed E0308 because standard and Entity admissions have
  different payload types; the final exhaustive arms preserve that static
  distinction instead of adding erasure or generic plumbing.
- Actual checkpoint: `3` feature-local tracked paths and `52` cumulative paths.
  Production is `+30 / -20 / net +10`; tests are
  `+20 / -9 / net +11`; public API is `+0 / -0` types. One private sum was
  added; no stored wrapper, allocation, synchronization point, runtime branch,
  or public spelling was added. Documentation is `+107 / -0 / net +107`; the
  complete cumulative tracked tree is `+1574 / -458 / net +1116`.

## DX79 — shutdown-transition assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` remains the sole dependency-selection
  authority and still resolves the exact published Behavior Core and Actors
  0.17.0 plus Macros 0.12.0 registry graph, Address 0.2.0, Communication
  0.1.2, and the recorded Timers Git revision. The selected Behavior revision
  and 760-line owner instructions, lifecycle/shutdown API, Communication
  closure contract, Address retirement, Observe completion, Timers scheduling,
  all direct `request_shutdown` callers, and current runtime and testing
  documents were rechecked. No owner or selected revision changed after DX78.
- Ownership and law: the lifecycle handle owns each shutdown request and its
  exact typed acceptance or rejection. An assertion may observe a completed
  result; it must not perform the state transition whose result it checks.
  Each test or example therefore invokes `request_shutdown` in lexical order,
  stores its exact `Result<(), ShutdownRejection>`, and only then asserts the
  expected disposition. Production lifecycle ownership and semantics remain
  unchanged.
- Exact blocker and prior-state oracle: fifteen accepted shutdown requests and
  one repeated-request rejection are executed inside `assert_eq!` across eight
  test and public-example files. A whole-repository source oracle captured all
  `16` witnesses before editing. This violates the repository rule that a
  required transition or ownership transfer never occurs inside an assertion,
  where assertion syntax must not own execution.
- Proposed representation: introduce no representation. Bind each existing
  shutdown result immediately before its existing assertion using local
  lifecycle language, preserving exact call count, lexical order, expected
  result, wait, and terminal trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. This is a mechanical caller correction across
  the complete direct test/example witness set; the final source oracle must
  find zero shutdown transitions inside assertions.
- Aggregate-drift checkpoint: public example behavior, request counts, first
  acceptance, repeated rejection, accepted-prefix draining, termination waits,
  and terminal outcomes remain exact. No application fold, effect lane,
  lifecycle implementation, or dependency surface changes. Disposition:
  `pass` before edits.
- Dependency edges: sequenced after DX78 because that stage exposed and fixed
  the same assertion hazard in its focused local regression; independent of
  DX53 through DX77.
- Blocked by: DX78 (resolved).
- Unblocks: DX80.
- Change ledger: expected tracked paths are this ledger plus the eight Rust
  test/example files containing the `16` witnesses (`9` feature-local paths,
  at most `58` cumulative paths because two Rust paths and this ledger are
  already tracked). Production is exactly `+0 / -0 / net 0`; tests and examples
  remain within `+32 / -19 / net +13`; public API is `+0 / -0` types. Any
  production edit, added abstraction, changed call order/count, assertion
  expectation, wait, terminal trace, or bound overrun falsifies the stage.
- Planned verification: prove the prior source oracle reports all `16`
  witnesses; require its final count to be zero; run the affected local and
  integration tests in debug and optimized profiles; execute all four affected
  public examples; run complete package and workspace tests, formatting,
  strict all-target Clippy, whitespace, and exact-delta review through pinned
  Nix.
- Result: all fifteen accepted shutdown requests and the repeated rejection
  now execute as explicit statements before their observational assertions.
  Request counts, order, exact typed expectations, termination waits, and
  terminal traces are unchanged. The final whole-repository oracle finds zero
  shutdown transitions inside assertions. No representation, production code,
  public API, or dependency changed.
- Verification: affected local, `run_with`, actor-interface, and exact-customer
  suites pass in debug and optimized profiles. Counter, actor-templates,
  application-topology, and Entity public examples all run successfully. The
  complete `bombay-rs` package and workspace suites, formatting, strict
  workspace all-target Clippy, whitespace, and structural residue checks pass
  through pinned Nix.
- Actual checkpoint: `9` feature-local tracked paths and `58` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests and examples are
  `+32 / -19 / net +13`; public API is `+0 / -0` types. Documentation is
  `+74 / -0 / net +74`; the complete cumulative tracked tree is
  `+1680 / -477 / net +1203`.

## DX80 — future-poll assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` remains the sole dependency-selection
  authority and still selects Behavior Core and Actors 0.17.0, Macros 0.12.0,
  Address 0.2.0, Communication 0.1.2, and the recorded Timers revision. The
  exact Behavior owner revision `435560ce7bea8ad3330ee2d42e5034f837a80602`,
  its complete 760-line instructions with SHA-256
  `2b7a9195b27f073fec18426da43e9840f8ef668f333b9ad55934f520a37ae226`,
  Driver run/commit laws, Observe shared and affine future polling,
  application termination, Entity admission polling, the five direct caller
  files, and current runtime and test-strategy documents were rechecked. No
  selected dependency or owner contract changed after DX79.
- Ownership and law: `Future::poll` is an execution boundary, not an
  observation. Depending on the exact future, one call may advance Driver
  initialization or commitment, register or replace an Observe waker, transfer
  an affine outcome, register application termination, or advance Entity
  admission custody. Each poll must execute before assertion syntax; the
  assertion may inspect only its returned `Poll` value and later traces.
- Exact blocker and prior-state oracle: thirteen direct poll operations execute
  inside assertions across Driver laws, Observe unit tests and allocation
  measurement, application lifecycle, and Entity admission tests. A
  whole-repository source oracle captured all `13` exact call sites before
  editing. This violates the shared Behavior and Bombay rule that assertions
  are observational and never own a transition, mutation, ownership transfer,
  or required operation.
- Proposed representation: introduce no representation. Bind each exact poll
  result immediately before its current assertion, preserving pinning, context
  and waker identity, call count, order, expected pending/ready disposition,
  outcome custody, subsequent cancellation, and every trace assertion.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final source oracle must find zero direct
  poll calls inside assertions across the complete repository.
- Aggregate-drift checkpoint: Driver facts and poll counts, Observe waker
  migration/cancellation and affine allocation counts, application shutdown
  ordering, Entity activation/delivery custody, and optimized behavior remain
  exact. No fold, future implementation, runtime path, effect lane, or
  dependency surface changes. Disposition: `pass` before edits.
- Dependency edges: sequenced after DX79's first complete lifecycle assertion
  correction; independent of DX53 through DX78.
- Blocked by: DX79 (resolved).
- Unblocks: DX81.
- Change ledger: expected tracked paths are this ledger plus the five direct
  test/performance callers (`6` feature-local paths, at most `60` cumulative
  paths because three callers and this ledger are already tracked). Production
  is exactly `+0 / -0 / net 0`; tests and performance probes remain within
  `+26 / -22 / net +4`; public API is `+0 / -0` types. Any production edit,
  abstraction, changed poll count/order, waker/context identity, expected
  disposition, ownership outcome, trace, cancellation point, or bound overrun
  falsifies the stage.
- Planned verification: require the prior source oracle's `13` witnesses to
  fall to zero; run the affected Driver, Observe, application-lifecycle, and
  Entity admission regressions in debug and optimized profiles; run the Observe
  allocation probe; then run complete package/workspace tests, formatting,
  strict all-target Clippy, whitespace, and exact-delta review through pinned
  Nix.
- Result: all thirteen future polls now execute as explicit statements before
  their observational assertions. Pinning, context and waker identity, call
  count and order, pending/ready values, owned outcomes, and cancellation
  points are unchanged. The final whole-repository oracle finds zero direct
  poll operations inside assertions. No representation, production code,
  public API, or dependency changed.
- Verification: all affected Driver, Observe, application lifecycle, and Entity
  admission suites pass in debug and optimized profiles. The Observe allocation
  harness preserves zero shared-observation allocation and exactly `64` bytes
  in one block for each affine pair operation. The complete `bombay-rs` package
  and workspace suites, formatting, strict workspace all-target Clippy,
  whitespace, and structural residue checks pass through pinned Nix.
- Actual checkpoint: `6` feature-local tracked paths and `60` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests and performance probes
  are `+26 / -22 / net +4`; public API is `+0 / -0` types. Documentation is
  `+75 / -0 / net +75`; the complete cumulative tracked tree is
  `+1781 / -499 / net +1282`.

## DX81 — receive-custody assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors 0.17.0, Macros 0.12.0, Address 0.2.0, Communication 0.1.2, and the
  recorded Timers revision. The exact Behavior owner revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, complete 760-line instructions
  and recorded SHA-256, Behavior fold reception, Communication-backed external
  actor reception, all four direct caller files, and current runtime and
  test-strategy documents were rechecked. No selected dependency or owner
  contract changed after DX80.
- Ownership and law: asynchronous external reception removes one exact message
  from the affine receiver, and Behavior reception consumes one event to
  compute the next transition or typed error. Both operations transfer custody
  or advance semantic execution. They must occur before assertion syntax; an
  assertion may inspect only the resulting message option or transition result.
- Exact blocker and prior-state oracle: eleven direct `receive` operations run
  inside assertions across exact-customer templates, external actor closure,
  application finalization, and macro-versus-explicit Behavior parity. A
  whole-repository source oracle captured all `11` sites before editing. This
  violates the shared rule that assertions never own required execution,
  mutation, or ownership transfer.
- Proposed representation: introduce no representation. Bind each exact
  receive result immediately before its current assertion, preserving receiver
  identity, message and event custody, call count and FIFO order, mapping,
  expected result, later shutdown, and terminal trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final source oracle must find zero direct
  receive calls inside assertions across the complete repository.
- Aggregate-drift checkpoint: external reply identity, accepted-prefix drain,
  FIFO order, closed-lane exhaustion, Behavior transition parity and overflow,
  application finalization order, and terminal outcomes remain exact. No fold,
  mailbox, runtime path, effect lane, or dependency changes. Disposition:
  `pass` before edits.
- Dependency edges: sequenced after DX80's future-execution assertion
  correction; independent of DX53 through DX79.
- Blocked by: DX80 (resolved).
- Unblocks: DX82.
- Change ledger: expected tracked paths are this ledger plus the four direct
  test callers (`5` feature-local paths, at most `61` cumulative paths because
  three callers and this ledger are already tracked). Production is exactly
  `+0 / -0 / net 0`; the pre-format test forecast was
  `+22 / -11 / net +11`; public API is `+0 / -0` types. The first format check
  correctly failed because four simplified assertions collapse under rustfmt.
  Complete-diff review corrected the test bound to
  `+22 / -26 / net -4`, with no path, semantic, or production expansion. Any
  production edit, abstraction, changed receiver identity, call count/order,
  mapping, expected message/result, later shutdown, terminal trace, or corrected
  bound overrun falsifies the stage.
- Planned verification: require the prior source oracle's `11` witnesses to
  fall to zero; run the affected exact-customer, actor-interface, application,
  and macro parity regressions in debug and optimized profiles; then run
  complete package/workspace tests, formatting, strict all-target Clippy,
  whitespace, and exact-delta review through pinned Nix.
- Result: all eleven receive operations now execute as explicit statements
  before their observational assertions. Receiver identity, exact message and
  event custody, FIFO order, mappings, typed results, subsequent shutdown, and
  terminal traces are unchanged. The final whole-repository oracle finds zero
  direct receive operations inside assertions. No representation, production
  code, public API, or dependency changed.
- Verification: affected exact-customer, actor-interface, application, and
  macro parity suites pass in debug and optimized profiles. After applying the
  formatter's required assertion collapse, the complete `bombay-rs` package
  and workspace suites, formatting, strict workspace all-target Clippy,
  whitespace, and structural residue checks pass through pinned Nix.
- Actual checkpoint: `5` feature-local tracked paths and `61` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+22 / -26 / net -4`; public API is `+0 / -0` types. Documentation is
  `+72 / -0 / net +72`; the complete cumulative tracked tree is
  `+1875 / -525 / net +1350`.

## DX82 — Observe outcome-take assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors 0.17.0, Macros 0.12.0, Address 0.2.0, Communication 0.1.2, and the
  recorded Timers revision. The exact Behavior owner revision and complete
  instructions, Bombay-private Observe shared-slot and `into_outcome`
  ownership laws, all four direct caller files, and current capability and test
  documents were rechecked. No selected dependency or owner contract changed
  after DX81.
- Ownership and law: `Observation::into_outcome(self)` consumes the exact
  observation. It returns the sole move-only outcome only when this handle has
  exclusive retired custody; pending or shared custody returns absence while
  still destroying the supplied handle and its registration. This ownership
  transfer must execute before assertion syntax, which may inspect only the
  returned optional outcome.
- Exact blocker and prior-state oracle: eleven consuming `into_outcome` calls
  run inside assertions across Observe unit, contract, pair, and pool tests. A
  whole-repository source oracle captured all `11` sites before editing. This
  violates the shared rule that assertions never own a required ownership
  transfer or state mutation.
- Proposed representation: introduce no representation. Bind every exact
  optional outcome immediately before its current assertion, preserving which
  observation is consumed, pending/shared/exclusive classification, move-only
  value identity, waker disposition, loop round, drop order, and exact message.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final source oracle must find zero direct
  `into_outcome` calls inside assertions across the complete repository.
- Aggregate-drift checkpoint: exclusive takes, shared and pending refusals,
  exact drop ownership, registration cleanup, no fabricated outcomes, and
  no repeated wake remain exact. No Observe implementation, slot protocol,
  runtime path, or dependency changes. Disposition: `pass` before edits.
- Dependency edges: sequenced after DX81's receive-custody correction;
  independent of DX53 through DX80.
- Blocked by: DX81 (resolved).
- Unblocks: DX83.
- Change ledger: expected tracked paths are this ledger plus the four direct
  Observe test callers (`5` feature-local paths and `61` cumulative paths;
  every path is already tracked). Production is exactly `+0 / -0 / net 0`;
  the pre-format test forecast was `+22 / -14 / net +8`; public API is
  `+0 / -0` types. The first format check identified canonical collapsing in
  two simplified assertions; complete-diff review corrected the test bound to
  `+22 / -19 / net +3`, with no path, semantic, or production expansion. Any
  production edit, abstraction, changed consumed handle, custody classification,
  outcome identity, waker/drop order, assertion message, or corrected bound
  overrun falsifies the stage.
- Planned verification: require the prior source oracle's `11` witnesses to
  fall to zero; run affected Observe unit, contract, pair, and pool suites in
  debug and optimized profiles plus applicable affine Loom cases; then run
  complete package/workspace tests, formatting, strict all-target Clippy,
  whitespace, and exact-delta review through pinned Nix.
- Result: all eleven consuming outcome takes now execute as explicit statements
  before their observational assertions. Consumed-handle identity,
  pending/shared/exclusive classification, move-only outcomes, registration and
  waker disposition, loop rounds, drop order, and diagnostics are unchanged.
  The final whole-repository oracle finds zero direct `into_outcome` calls
  inside assertions. No representation, production code, public API, or
  dependency changed.
- Verification: all 121 Observe tests pass in debug and optimized profiles;
  all 28 isolated Loom models pass, including outcome-take/retirement races and
  the 254.66-second promotion-boundary schedule. The complete `bombay-rs`
  package and workspace suites, formatting, strict workspace all-target Clippy,
  whitespace, and structural residue checks pass through pinned Nix.
- Actual checkpoint: `5` feature-local tracked paths and `61` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+22 / -19 / net +3`; public API is `+0 / -0` types. Documentation is
  `+70 / -0 / net +70`; the complete cumulative tracked tree is
  `+1967 / -544 / net +1423`.

## DX83 — Observe blocking-wait assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects the exact owner graph
  recorded above. The Behavior revision and complete instructions,
  Bombay-private Observe `wait`/`wait_timeout` registration, parking,
  cancellation, completion, and cloning laws, all six direct Observe caller
  files, and current capability and test documents were rechecked. Machine's
  independently owned `TurnReceipt::wait` is not part of this feature. No
  selected dependency or owner contract changed after DX82.
- Ownership and law: Observe blocking waits may register the current thread,
  park and recheck through spurious wakes, cancel a timed registration, or
  clone and return an exact outcome. They are required synchronization
  operations, not observations, and must execute before assertion syntax. The
  assertion may inspect only the returned outcome or timeout disposition.
- Exact blocker and prior-state oracle: fifteen direct Observe `wait` or
  `wait_timeout` operations execute inside assertions across unit, contract,
  pair, pool, stress, and Loom tests. A scoped whole-repository oracle captured
  all `15` sites before editing while excluding Machine's distinct receipt
  owner. This violates the shared rule that assertions never own required
  execution, synchronization, or state mutation.
- Proposed representation: introduce no representation. Bind every blocking
  result immediately before its current assertion, preserving observation,
  duration, thread, generation, call count/order, repeated-wait behavior,
  expected outcome/timeout, diagnostic, and subsequent trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final scoped oracle must find zero Observe
  waits inside assertions while leaving Machine receipt waits untouched.
- Aggregate-drift checkpoint: completion-before-wait, timeout cancellation,
  repeated completed reads, generation isolation, no fabricated outcomes,
  wake/park behavior, elapsed-time checks, and stress traces remain exact. No
  Observe implementation, slot protocol, runtime path, or dependency changes.
  Disposition: `pass` before edits.
- Dependency edges: sequenced after DX82's consuming outcome-take correction;
  independent of DX53 through DX81.
- Blocked by: DX82 (resolved).
- Unblocks: DX84.
- Change ledger: expected tracked paths are this ledger plus six Observe test
  callers (`7` feature-local paths, at most `62` cumulative paths because one
  caller is newly tracked). Production is exactly `+0 / -0 / net 0`; formatted
  tests remain within `+30 / -40 / net -10..+30`; public API is `+0 / -0`
  types. Any production edit, abstraction, changed observation/duration/thread,
  call count/order, outcome, timeout, diagnostic, trace, or bound overrun
  falsifies the stage.
- Planned verification: require the scoped prior oracle's `15` witnesses to
  fall to zero and prove Machine receipt waits remain out of scope; run all
  Observe tests in debug and optimized profiles plus all isolated Loom models;
  then run complete package/workspace tests, formatting, strict all-target
  Clippy, whitespace, and exact-delta review through pinned Nix.
- Result: all fifteen Observe blocking waits now execute as explicit statements
  before their observational assertions. Observation, duration, thread and
  generation identity, call count/order, repeated completed reads, exact
  outcome/timeout, diagnostics, timing checks, and stress traces are unchanged.
  The final scoped oracle finds zero Observe waits inside assertions while all
  five Machine receipt-wait witnesses remain untouched. No representation,
  production code, public API, or dependency changed.
- Verification: all 121 Observe tests pass in debug and optimized profiles;
  all 28 isolated Loom models pass, including blocking-wait races and the
  255.59-second promotion-boundary schedule. The complete `bombay-rs` package
  and workspace suites, formatting, strict workspace all-target Clippy,
  whitespace, scoped ownership, and structural residue checks pass through
  pinned Nix.
- Actual checkpoint: `7` feature-local tracked paths and `62` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+30 / -26 / net +4`; public API is `+0 / -0` types. Documentation is
  `+69 / -0 / net +69`; the complete cumulative tracked tree is
  `+2066 / -570 / net +1496`.

## DX84 — Machine receipt-wait assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects the exact owner graph
  recorded above. The Behavior revision and complete instructions, Machine's
  `SerializedExecutor`, consuming `TurnReceipt::wait`, `TurnOutcome`, poison
  and reentrant/concurrent submission laws, both direct caller files, and the
  current module and Driver boundary documents were rechecked. Observe's
  independently owned waits are feature-complete in DX83 and remain out of
  scope. No selected dependency or owner contract changed after DX83.
- Ownership and law: `TurnReceipt::wait(self)` consumes the one completion
  receipt, may block on its condition variable, and returns that exact turn's
  completed-or-poisoned disposition. Submission may also synchronously execute
  a turn before yielding the receipt. Both submit and wait are execution and
  custody boundaries, so they must finish before assertion syntax observes the
  `TurnOutcome`.
- Exact blocker and prior-state oracle: five Machine receipt waits execute
  inside assertions across four executor tests and one benchmark loop. A
  Machine-scoped source oracle captured all `5` sites before editing while
  proving DX83's Observe waits remain absent. This violates the shared rule
  that assertions never own required execution, synchronization, or ownership
  transfer.
- Proposed representation: introduce no representation. Bind each exact
  `TurnOutcome` before its current assertion, preserving executor, receipt,
  input, consumer, thread, call count/order, reentrant or poison disposition,
  trace, and benchmark work.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final scoped oracle must find zero Machine
  receipt waits inside assertions.
- Aggregate-drift checkpoint: serialized turn completion, effect-before-next-
  transition order, reentrant submission, poison propagation to queued
  receipts, per-thread completion, and benchmark iteration count remain exact.
  No Machine implementation, executor state, receipt protocol, or dependency
  changes. Disposition: `pass` before edits.
- Dependency edges: sequenced after DX83's Observe-owner separation;
  independent of DX53 through DX82.
- Blocked by: DX83 (resolved).
- Unblocks: DX85.
- Change ledger: expected tracked paths are this ledger, the Machine executor
  test region, and the Machine benchmark (`3` feature-local paths, at most `64`
  cumulative paths). Production is exactly `+0 / -0 / net 0`; tests and
  benchmark remain within `+14 / -20 / net -6..+14`; public API is
  `+0 / -0` types. Any production-region edit, abstraction, changed
  executor/receipt/input/consumer/thread, call count/order, disposition, trace,
  benchmark work, or bound overrun falsifies the stage.
- Planned verification: require the scoped prior oracle's `5` witnesses to fall
  to zero; run all Machine executor tests in debug and optimized profiles; run
  the Machine executor benchmark harness; then run complete package/workspace
  tests, formatting, strict all-target Clippy, whitespace, and exact-delta
  review through pinned Nix.
- Result: all five consuming Machine receipt waits now execute as explicit
  statements before their observational assertions. Executor, receipt, input,
  consumer, thread, call count/order, completion/poison disposition, trace, and
  benchmark work are unchanged. The final Machine-scoped oracle finds zero
  receipt waits inside assertions. No representation, production code, public
  API, or dependency changed.
- Verification: all 18 Machine unit tests, its compile fixtures, and doctests
  pass in debug and optimized profiles. The benchmark completed all
  `1024 * 2000 * 9` configured turns per workload; serialized payload measured
  `74.444` minimum and `75.108` median nanoseconds per turn. The complete
  `bombay-rs` package and workspace suites, formatting, strict workspace
  all-target Clippy, whitespace, scoped ownership, and structural residue
  checks pass through pinned Nix.
- Actual checkpoint: `3` feature-local tracked paths and `64` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests and benchmark are
  `+13 / -11 / net +2`; public API is `+0 / -0` types. Documentation is
  `+70 / -0 / net +70`; the complete cumulative tracked tree is
  `+2149 / -581 / net +1568`.

## DX85 — Observe thread-join assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors `0.17.0`, Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`,
  and Timers revision `13e884da7ab41781f52337b0038060e375b00ee0`.
  Behavior's complete 760-line instructions at owner revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, checksum
  `2b7a9195b27f073fec18426da43e9840f8ef668f333b9ad55934f520a37ae226`,
  the selected owners' current source, public algebra, tests, and
  documentation, Bombay's Observe implementation and verification corpus,
  all six direct caller files, and current module/capability/test documents
  were rechecked. No selected dependency or owner contract changed after
  DX84.
- Ownership and law: each standard or Loom `JoinHandle::join` consumes the one
  thread handle, synchronizes with that thread's completion, and returns its
  exact result or panic disposition. Join is required lifecycle execution and
  custody transfer, not observation, so it must finish before assertion syntax
  inspects the returned Observe outcome. Entity passivation, Machine executor,
  and path-construction joins have different owners and remain out of scope.
- Exact blocker and prior-state oracle: thirty-four Observe-owned thread joins
  execute inside assertions across unit, pair, panic-safety, stress, internal
  Loom, and external Loom tests. The owner-scoped source oracle captured all
  `34` sites before editing: `3 + 2 + 2 + 12 + 8 + 7`. This violates the
  shared rule that assertions never own required execution, synchronization,
  or ownership transfer.
- Proposed representation: introduce no representation. Bind each exact join
  result immediately before its current assertion, preserving handle, thread,
  generation, join count/order, panic diagnostic, exact outcome, loop scope,
  schedule, and subsequent trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final owner-scoped oracle must find zero
  Observe thread joins inside assertions while leaving other join owners
  untouched.
- Aggregate-drift checkpoint: retained publication, blocking wait, timeout,
  waker, cancellation, panic-safety, slot recycling, generation isolation,
  and standard/Loom scheduling laws remain exact. No Observe implementation,
  slot protocol, runtime path, dependency, or control-state sum changes.
  Disposition: `pass` before edits.
- Dependency edges: sequenced after DX84's Machine receipt-wait separation;
  independent of DX53 through DX83.
- Blocked by: DX84 (resolved).
- Unblocks: DX86.
- Change ledger: expected tracked paths are this ledger plus six Observe test
  callers (`7` feature-local paths, at most `65` cumulative paths because the
  external Loom caller is newly tracked). Production is exactly
  `+0 / -0 / net 0`; formatted tests remain within
  `+70 / -60 / net +10..+70`; public API is `+0 / -0` types. Any production
  edit, abstraction, changed handle/thread/generation, join count/order, panic
  diagnostic, outcome, schedule, trace, foreign-owner edit, or bound overrun
  falsifies the stage.
- Planned verification: require the exact prior oracle's `34` witnesses to
  fall to zero while proving the Entity and other join owners remain out of
  scope; run all Observe tests in debug and optimized profiles plus all
  isolated Loom models; then run complete package/workspace tests, formatting,
  strict all-target Clippy, whitespace, and exact-delta review through pinned
  Nix.
- Result: all thirty-four Observe-owned standard/Loom joins now execute as
  explicit statements before their observational assertions. Handle, thread,
  generation, join count/order, panic diagnostic, exact outcome, loop scope,
  schedule, and subsequent trace are unchanged. The final owner-scoped oracle
  finds zero Observe joins inside assertions while the Entity passivation join
  remains untouched. No representation, production code, public API, or
  dependency changed.
- Verification: all 121 Observe tests pass in debug and optimized profiles;
  all 28 isolated Loom models pass under the repository's release preemption-3
  configuration. The complete `bombay-rs` package and workspace suites,
  formatting, strict workspace all-target Clippy, whitespace, scoped
  ownership, and structural residue checks pass through pinned Nix.
- Actual checkpoint: `7` feature-local tracked paths and `65` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+68 / -40 / net +28`; public API is `+0 / -0` types. Documentation is
  `+76 / -0 / net +76`; the complete cumulative tracked tree is
  `+2293 / -621 / net +1672`.

## DX86 — Entity passivation-join assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects the exact owner graph
  recorded above. The unchanged Behavior owner revision and complete
  instructions, every selected primitive owner surface, Entity's passivation
  and lifecycle contracts, the one direct caller, and current module and
  capability documents were rechecked. Observe's 34 independently owned joins
  are feature-complete in DX85 and remain out of scope. No selected dependency
  or owner contract changed after DX85.
- Ownership and law: the passivation thread owns one racing
  `EntityRuntime::passivate` call and its `JoinHandle`. Joining consumes that
  handle, synchronizes with the exact lifecycle classification, and returns
  `Passivation::Superseded`. The join is required lifecycle execution and
  custody transfer, so it must finish before assertion syntax inspects the
  returned disposition.
- Exact blocker and prior-state oracle: one Entity passivation join executes
  inside an assertion in the incarnation-replacement race regression. An
  Entity-scoped source oracle captured the sole site before editing while
  proving DX85's Observe joins remain absent. This violates the shared rule
  that assertions never own required execution, synchronization, or ownership
  transfer.
- Proposed representation: introduce no representation. Bind the exact
  passivation disposition immediately before its current assertion, preserving
  runtime, entity ID, gate, thread, join count/order, exact disposition,
  activation count, and trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final Entity-scoped oracle must find zero
  passivation joins inside assertions.
- Aggregate-drift checkpoint: active/draining/retiring lifecycle phases,
  replacement activation, `Begun` and `Superseded` classification, retirement,
  and activation-count evidence remain exact. No Entity implementation,
  lifecycle state, runtime path, dependency, or control-state sum changes.
  Disposition: `pass` before edits.
- Dependency edges: sequenced after DX85's Observe thread-join separation;
  independent of DX53 through DX84.
- Blocked by: DX85 (resolved).
- Unblocks: DX87.
- Change ledger: expected tracked paths are this ledger and the existing Entity
  runtime test caller (`2` feature-local paths and `65` cumulative paths).
  Production is exactly `+0 / -0 / net 0`; formatted tests are exactly
  `+2 / -1 / net +1`; public API is `+0 / -0` types. Any production edit,
  abstraction, changed runtime/entity/gate/thread, join count/order,
  disposition, activation count, trace, or bound overrun falsifies the stage.
- Planned verification: require the sole prior witness to fall to zero; run the
  Entity runtime test target in debug and optimized profiles; then run complete
  package/workspace tests, formatting, strict all-target Clippy, whitespace,
  and exact-delta review through pinned Nix.
- Result: the sole Entity passivation join now executes as an explicit
  statement before its observational assertion. Runtime, entity ID, gate,
  thread, join count/order, exact `Superseded` disposition, activation count,
  and trace are unchanged. The final Entity-scoped oracle finds zero
  passivation joins inside assertions. No representation, production code,
  public API, or dependency changed.
- Verification: all 11 Entity runtime tests pass in debug and optimized
  profiles. The complete `bombay-rs` package and workspace suites, formatting,
  strict workspace all-target Clippy, whitespace, scoped ownership, and
  structural residue checks pass through pinned Nix.
- Actual checkpoint: `2` feature-local tracked paths and `65` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+2 / -1 / net +1`; public API is `+0 / -0` types. Documentation is
  `+65 / -0 / net +65`; the complete cumulative tracked tree is
  `+2360 / -622 / net +1738`.

## DX87 — Exclusive-turn assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects the exact owner graph
  recorded above. The unchanged Behavior owner revision and complete
  instructions, all selected primitive owner surfaces, Machine's `Machine`
  fold, `ExclusiveExecutor`, `ExclusiveState`, exact input rejection, panic
  poisoning and payload-drop tests, the four direct caller sites, and current
  module and Driver boundary documents were rechecked. Serialized and
  linearized executor operations remain separate owners. No selected dependency
  or owner contract changed after DX86.
- Ownership and law: `ExclusiveExecutor::turn` mutably owns one immediate
  `Machine::step`, consumes the accepted input and current machine, installs
  either the successor or a permanent poisoned state, and returns the exact
  output or rejected later input. It is a transition and custody boundary, so
  it must finish before assertion syntax observes its output or unwind
  disposition.
- Exact blocker and prior-state oracle: four exclusive turns execute inside
  assertions across the success and panic regressions. The owner-scoped source
  oracle captured all `4` sites before editing: two direct successful results
  and two turns inside `catch_unwind`. This violates the shared rule that
  assertions never own required execution, mutation, or ownership transfer.
- Proposed representation: introduce no representation. Bind each successful
  output or `catch_unwind` disposition immediately before its existing
  assertion, preserving executor, machine, input, turn count/order, panic
  boundary, output/rejection identity, drop counts, and subsequent state trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final owner-scoped oracle must find zero
  exclusive turns inside assertions while leaving serialized and linearized
  operations untouched.
- Aggregate-drift checkpoint: ready/poisoned state, successor installation,
  exact rejected input, panic propagation, affine payload drops, and later
  recovery evidence remain exact. No Machine implementation, executor state,
  runtime path, dependency, or control-state sum changes. Disposition: `pass`
  before edits.
- Dependency edges: sequenced after DX86's Entity join separation; independent
  of DX53 through DX85.
- Blocked by: DX86 (resolved).
- Unblocks: DX88.
- Change ledger: expected tracked paths are this ledger and the existing
  Machine executor test region (`2` feature-local and `65` cumulative paths).
  Production is exactly `+0 / -0 / net 0`. The pre-edit formatted-test forecast
  of `+8 / -10` was falsified before verification because rustfmt and diff
  alignment rewrite both multiline unwind macro shells; the corrected exact
  containment is `+17 / -17 / net 0`. Public API is `+0 / -0` types. Any
  production edit,
  abstraction, changed executor/machine/input, turn count/order, panic
  boundary, output/rejection identity, drop/state trace, foreign-owner edit, or
  corrected bound overrun falsifies the stage.
- Planned verification: require the exact prior oracle's `4` witnesses to fall
  to zero while proving serialized and linearized operations remain separate;
  run all Machine executor tests in debug and optimized profiles; then run
  complete package/workspace tests, formatting, strict all-target Clippy,
  whitespace, and exact-delta review through pinned Nix.
- Result: all four Exclusive executor turns now execute as explicit statements
  before their observational assertions. Executor, machine, input, turn
  count/order, panic boundary, output/rejection identity, drop counts, and
  subsequent state trace are unchanged. The final owner-scoped oracle finds
  zero Exclusive turns inside assertions while nine serialized/linearized
  execution witnesses remain separate. No representation, production code,
  public API, or dependency changed.
- Verification: all 18 Machine unit tests, its compile fixtures, and doctests
  pass in debug and optimized profiles. The complete `bombay-rs` package and
  workspace suites, formatting, strict workspace all-target Clippy,
  whitespace, scoped ownership, and structural residue checks pass through
  pinned Nix.
- Actual checkpoint: `2` feature-local tracked paths and `65` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+17 / -17 / net 0`; public API is `+0 / -0` types. Documentation is
  `+73 / -0 / net +73`; the complete cumulative tracked tree is
  `+2450 / -639 / net +1811`.

## DX88 — Linearized-execution assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects the exact owner graph
  recorded above. The unchanged Behavior owner revision and complete
  instructions, all selected primitive owner surfaces, Machine's `Machine`
  fold, `OutputEvidence`, `LinearizedExecutor`, `DispatchOutcome`, transition
  poisoning, output-queue and dispatch-resumption tests, the six direct caller
  sites, and current module and Driver boundary documents were rechecked.
  Serialized submission remains a separate owner. No selected dependency or
  owner contract changed after DX87.
- Ownership and law: `LinearizedExecutor::submit` mutably executes one affine
  transition while atomically installing its evidence and queued output;
  `dispatch_pending` acquires dispatch ownership and consumes the queued output
  prefix until drained or a consumer panics. Both are execution and custody
  boundaries, so they must finish before assertion syntax observes evidence,
  dispatch disposition, or unwind classification.
- Exact blocker and prior-state oracle: six Linearized executor operations
  execute inside assertions: three successful submissions, two panic-boundary
  calls, and one successful dispatch. The owner-scoped source oracle captured
  all `6` sites before editing while the three Serialized submission witnesses
  remain separate. This violates the shared rule that assertions never own
  required execution, mutation, or ownership transfer.
- Proposed representation: introduce no representation. Bind each evidence,
  dispatch outcome, or local unwind predicate immediately before its existing
  assertion, preserving executor, machine, input, consumer, call count/order,
  panic boundary, output queue, evidence, dispatch disposition, and trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final owner-scoped oracle must find zero
  Linearized operations inside assertions while leaving Serialized submission
  untouched.
- Aggregate-drift checkpoint: transition linearization, latest evidence,
  output order, dispatch ownership, panic recovery, exact-once output drops,
  poison propagation, and traces remain exact. No Machine implementation,
  executor state, runtime path, dependency, or control-state sum changes.
  Disposition: `pass` before edits.
- Dependency edges: sequenced after DX87's Exclusive-turn separation;
  independent of DX53 through DX86.
- Blocked by: DX87 (resolved).
- Unblocks: DX89.
- Change ledger: expected tracked paths are this ledger and the existing
  Machine executor test region (`2` feature-local and `65` cumulative paths).
  Production is exactly `+0 / -0 / net 0`; formatted tests remain within
  `+30 / -30 / net -10..+30`; public API is `+0 / -0` types. Any production
  edit, abstraction, changed executor/machine/input/consumer, call count/order,
  panic boundary, queue/evidence/disposition/trace, foreign-owner edit, or
  bound overrun falsifies the stage.
- Planned verification: require the exact prior oracle's `6` witnesses to fall
  to zero while proving three Serialized submission witnesses remain separate;
  run all Machine executor tests in debug and optimized profiles; then run
  complete package/workspace tests, formatting, strict all-target Clippy,
  whitespace, and exact-delta review through pinned Nix.
- Result: all six Linearized executor operations now execute as explicit
  statements before their observational assertions. Executor, machine, input,
  consumer, call count/order, panic boundary, output queue, evidence, dispatch
  disposition, and trace are unchanged. The final owner-scoped oracle finds
  zero Linearized operations inside assertions while exactly three Serialized
  submission witnesses remain separate. No representation, production code,
  public API, or dependency changed.
- Verification: all 18 Machine unit tests, its compile fixtures, and doctests
  pass in debug and optimized profiles. The complete `bombay-rs` package and
  workspace suites, formatting, strict workspace all-target Clippy,
  whitespace, scoped ownership, and structural residue checks pass through
  pinned Nix.
- Actual checkpoint: `2` feature-local tracked paths and `65` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+19 / -19 / net 0`; public API is `+0 / -0` types. Documentation is
  `+71 / -0 / net +71`; the complete cumulative tracked tree is
  `+2540 / -658 / net +1882`.

## DX89 — Serialized-submission assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors `0.17.0`, Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`,
  and Timers `0.1.0` at `13e884da7ab41781f52337b0038060e375b00ee0`.
  Behavior's owner checkout remains
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; its complete 760-line
  instructions, selected source/public roots, tests, and documentation were
  reread. The primitive owner roots and test/document inventories, Bombay's
  Observe owner, Machine's complete `SerializedExecutor` implementation and
  caller region, and current module and Driver boundary documents were
  rechecked. No selected dependency or owner contract changed after DX88.
- Ownership and law: `SerializedExecutor::submit` accepts or returns one exact
  input, queues accepted custody with its receipt, and lets at most one caller
  drain transitions and output consumption in order. A transition or consumer
  panic poisons the executor and settles every outstanding receipt; a later
  submission returns its exact input. Submission and unwind classification
  must finish before assertion syntax observes panic or rejection.
- Exact blocker and prior-state oracle: exactly three Serialized submissions
  execute inside assertions: two initial submissions inside asserted
  `catch_unwind` expressions and one later poisoned submission inside
  `matches!`. The owner-scoped source oracle captured all `3` sites before
  editing, after the Linearized oracle reached zero. This violates the shared
  rule that assertions never own required execution, mutation, or ownership
  transfer.
- Proposed representation: introduce no representation. Bind each local
  unwind predicate or submission result immediately before its existing
  assertion, preserving executor, machine, input, consumer, call count/order,
  panic boundary, queue/receipt settlement, exact rejection, poison state,
  and trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final owner-scoped oracle must find zero
  Serialized submissions inside assertions without touching reentrant
  submissions or the already separated Exclusive and Linearized owners.
- Aggregate-drift checkpoint: `TurnState::{Idle, Running, Poisoned}`, the input
  queue, machine custody, active receipt, transition branches, module layout,
  and public spellings remain unchanged. No arrival history, repeated cause,
  false cardinality, nested transition authority, semantic boolean state, or
  structural caller syntax is added. Machine and Driver law documents remain
  aligned. Disposition: `pass` before edits.
- Dependency edges: sequenced after DX88's Linearized-operation separation;
  independent of DX53 through DX87.
- Blocked by: DX88 (resolved).
- Unblocks: DX90.
- Change ledger: expected tracked paths are this ledger and the existing
  Machine executor test region (`2` feature-local and `65` cumulative paths).
  Production is exactly `+0 / -0 / net 0`; formatted tests remain within
  `+20 / -20 / net -10..+20`; public API is `+0 / -0` types. Existing
  `SerializedExecutor`, `TurnState`, input queue, receipt settlement, and
  consumer composition are reused unchanged; nothing is added or deleted.
  Any production edit, abstraction, changed executor/machine/input/consumer,
  call count/order, panic boundary, receipt/rejection/poison/trace, reentrant or
  foreign-owner edit, or bound overrun falsifies the stage.
- Planned verification: require the exact prior oracle's `3` witnesses to fall
  to zero while proving reentrant submissions and the already separated
  Exclusive/Linearized owners remain unchanged; run all Machine executor tests
  in debug and optimized profiles; then run complete package/workspace tests,
  formatting, strict all-target Clippy, whitespace, and exact-delta review
  through pinned Nix.
- Result: all three Serialized submissions now execute as explicit statements
  before their observational assertions. Executor, machine, input, consumer,
  call count/order, panic boundary, queue and receipt settlement, exact
  rejection, poison state, and trace are unchanged. The final owner-scoped
  oracle finds zero asserted `catch_unwind` submissions and zero submissions
  inside asserted `matches!`; reentrant submissions and the separated
  Exclusive and Linearized owners are unchanged. No representation,
  production code, public API, or dependency changed.
- Verification: all 18 Machine unit tests, its compile fixtures, and doctests
  pass in debug and optimized profiles. The complete `bombay-rs` package and
  workspace suites, formatting, strict workspace all-target Clippy,
  whitespace, scoped ownership, and structural residue checks pass through
  pinned Nix.
- Actual checkpoint: `2` feature-local tracked paths and `65` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+12 / -16 / net -4`; public API is `+0 / -0` types. Documentation is
  `+80 / -0 / net +80`; the complete cumulative tracked tree is
  `+2632 / -674 / net +1958`.

## DX90 — Driver-run assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors `0.17.0`, Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`,
  and Timers `0.1.0` at `13e884da7ab41781f52337b0038060e375b00ee0`.
  Behavior's owner checkout remains
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; its complete 760-line
  instructions, selected source/public roots, tests, and documentation were
  reread. The primitive owner roots and test/document inventories, Bombay's
  Observe owner, Engine's complete `Driver` implementation, all direct run
  callers in `driver_law.rs`, and current module, Driver law, and Driver test
  strategy documents were rechecked. No selected dependency or owner contract
  changed after DX89.
- Ownership and law: `Driver::run` consumes one affine Driver and executes its
  complete initialization, event/fold/action sequence, terminal selection, and
  retirement barrier before returning `DriverRetirement`. It is the universal
  Engine execution and custody boundary, so it must finish before assertion
  syntax observes the returned disposition.
- Exact blocker and prior-state oracle: exactly `28` `Driver::run().await`
  executions occur inside assertions in `driver_law.rs`; the other two run
  calls there are already explicit statements. The first contiguous-call scan
  found `23`; pre-edit context review found two more fluent chains split across
  lines. After the first mechanical pass, the zero-target full-file oracle
  exposed three additional split chains in initialization-failure and
  complete-actions sections, correcting the authoritative prior total to `28`
  before the second pass. This audit history is retained rather than
  retroactively claiming the initial oracle was complete. The sites violate
  the shared rule that
  assertions never own required execution, mutation, or ownership transfer.
- Proposed representation: introduce no representation. Bind each complete
  `DriverRetirement` or its disposition immediately before the existing
  assertion, preserving concrete Driver, Behavior, Environment, input,
  execution count/order, action transcript, exact disposition, final custody,
  and retirement trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch, or
  production path is permitted. The final owner-scoped oracle must find zero
  Driver runs inside assertions while leaving the two already explicit runs
  and all non-Driver owners untouched.
- Aggregate-drift checkpoint: `ExecutionPhase`, `ActionDecision`,
  `SettlementTurn`, `DriverRetirement`, all Driver control states and error
  alternatives, transition branches, modules, and public spellings remain
  unchanged. No arrival history, repeated cause, false cardinality, nested
  transition authority, semantic boolean state, or structural caller syntax is
  added. The complete Driver law and test strategy remain aligned.
  Disposition: `pass` before edits.
- Dependency edges: sequenced after DX89's Serialized-submission separation;
  independent of DX53 through DX89's Machine-owned changes.
- Blocked by: DX89 (resolved).
- Unblocks: DX91.
- Change ledger: expected tracked paths are this ledger and the existing Engine
  Driver law test (`2` feature-local and `65` cumulative paths). Production is
  exactly `+0 / -0 / net 0`. The pre-edit formatted-test forecast of
  `+70 / -50` was falsified before verification because several multiline
  assertions collapse while the 28 complete executions gain explicit custody
  bindings; the pre-verification containment was `+66 / -55 / net +11`.
  Focused verification then proved that two drop observations had relied on
  the asserted temporary's statement lifetime. Making returned final Behavior
  custody explicit before deliberately discharging the retirement corrected
  the final test containment to `+70 / -55 / net +15`.
  Public API is `+0 / -0` types. Existing
  `Driver`, `DriverRetirement`, `Completion`, `DriverError`, environments, and
  transcripts are reused unchanged; nothing is added or deleted. Any
  production edit, abstraction, changed Driver/Behavior/Environment/input,
  execution count/order, transcript/disposition/custody/retirement trace,
  already-explicit or foreign-owner edit, or bound overrun falsifies the stage.
- Planned verification: require the corrected prior oracle's `28` witnesses to fall
  to zero while proving the two already explicit runs remain separate; run all
  Engine Driver law tests in debug and optimized profiles; then run complete
  package/workspace tests, formatting, strict all-target Clippy, whitespace,
  and exact-delta review through pinned Nix.
- Result: all corrected `28` complete Driver executions now occur as explicit
  statements before their observational assertions. The final owner-scoped
  oracle finds zero Driver runs inside assertions and counts all `30` Driver
  runs in the file, including the two pre-existing explicit statements. The
  first focused debug run exposed two tests whose later drop observations had
  depended on the assertion temporary being destroyed at statement end. Those
  regressions now assert the exact returned final Behavior state and explicitly
  discharge `DriverRetirement` before observing destruction. Driver, Behavior,
  Environment, inputs, execution count and order, action transcripts,
  dispositions, final custody, and retirement traces remain unchanged. No
  production code, public API, dependency, or representation changed.
- Verification: all `30` Driver law tests pass in debug and optimized profiles.
  The complete `bombay-engine` package and workspace suites, formatting,
  strict workspace all-target Clippy, whitespace, scoped ownership, and
  structural residue checks pass through pinned Nix.
- Actual checkpoint: `2` feature-local tracked paths and `65` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+70 / -55 / net +15`; public API is `+0 / -0` types. Documentation is
  `+93 / -0 / net +93`; the complete cumulative tracked tree is
  `+2795 / -729 / net +2066`.

## DX91 — Driver allocation-run assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors `0.17.0`, Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`,
  and Timers `0.1.0` at `13e884da7ab41781f52337b0038060e375b00ee0`.
  Behavior's owner checkout remains
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; its complete 760-line
  instructions and the selected registry Core/Actors roots, source and test
  inventories, and current documentation were reread. Address,
  Communication, Bombay Observe, and Timers public roots and relevant
  source/test/document inventories were rechecked. Engine's complete Driver
  and Environment ports, the allocation fixture, and the Driver law and test
  strategy were reread. No selected dependency or owner contract changed
  after DX90.
- Ownership and law: the allocation harness owns one immediate Environment,
  one affine Driver execution, and the allocation counter around that complete
  execution. `Driver::run` consumes the Driver and returns exact terminal
  custody; the complete run must finish before assertion syntax observes its
  disposition, while the counter must retain its existing before/after scope.
- Exact blocker and prior-state oracle: the allocation fixture contains exactly
  one `block_on(driver.run())` inside `assert_eq!`, and exactly one Driver run
  in total. The repository scan found it after DX90's separate
  `driver_law.rs` owner reached zero. The assertion still owns required
  execution and affine custody transfer.
- Proposed representation: introduce no representation. Bind the returned
  `DriverRetirement` immediately before the existing disposition assertion,
  preserving Driver, Behavior, Environment, event, execution count/order,
  terminal disposition, allocation measurement window, and exact count.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch,
  production path, or allocation is permitted. The final fixture-scoped oracle
  must find zero Driver runs inside assertions and exactly one total run.
- Aggregate-drift checkpoint: the Driver control sums, Environment phases,
  fixture's exact optional event, allocator, transition branches, modules, and
  public spellings remain unchanged. No arrival history, repeated cause, false
  cardinality, nested transition authority, semantic boolean state, or
  structural caller syntax is added. Driver law and test strategy remain
  aligned. Disposition: `pass` before edits.
- Dependency edges: sequenced after DX90's Driver-law separation; independent
  of DX53 through DX89 and does not reopen DX64's exact fixture input model.
- Blocked by: DX90 (resolved).
- Unblocks: DX92.
- Change ledger: expected tracked paths are this ledger and
  `crates/bombay-engine/tests/driver_allocation.rs` (`2` feature-local and `66`
  cumulative paths). Production is exactly `+0 / -0 / net 0`; formatted tests
  remain within `+4 / -2 / net 0..+3`; public API is `+0 / -0` types. Existing
  `Driver`, `DriverRetirement`, `ImmediateEnvironment`, `StopOnOne`, and the
  allocation counter are reused unchanged; nothing is added or deleted. Any
  production edit, abstraction, changed event/disposition/allocation window or
  count, additional execution, foreign-owner edit, or bound overrun falsifies
  the stage.
- Planned verification: require the prior fixture oracle's one asserted run to
  fall to zero and retain exactly one total run; execute the allocation
  regression in debug and optimized profiles; then run the complete Engine
  package/workspace suites, formatting, strict all-target Clippy, whitespace,
  and exact-delta review through pinned Nix.
- Result: the allocation fixture's sole complete Driver execution now binds its
  exact `DriverRetirement` before the disposition assertion. The final scoped
  oracle finds zero assertion-owned runs and exactly one total run. Driver,
  Behavior, Environment, event, execution order, terminal disposition,
  allocation measurement window, and the exact one-allocation result are
  unchanged. No representation, production code, public API, or dependency
  changed.
- Verification: the allocation regression passes in debug and optimized
  profiles with exactly one settlement-queue allocation. The complete Engine
  package and workspace suites, formatting, strict workspace all-target
  Clippy, whitespace, scoped ownership, and structural residue checks pass
  through pinned Nix.
- Actual checkpoint: `2` feature-local tracked paths and `66` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+2 / -1 / net +1`; public API is `+0 / -0` types. Documentation is
  `+75 / -0 / net +75`; the complete cumulative tracked tree is
  `+2872 / -730 / net +2142`.

## DX92 — tagged-ingress send assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors `0.17.0`, Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`,
  and Timers `0.1.0` at `13e884da7ab41781f52337b0038060e375b00ee0`.
  Behavior's owner checkout remains
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; its complete 760-line
  instructions and the selected Core/Actors public roots, source/test
  inventories, and current documentation were reread. Address,
  Communication, Bombay Observe, and Timers public roots and relevant
  source/test/document inventories were rechecked. Communication's complete
  `MailboxRef::send` contract, Bombay's local ingress interpreter and timing
  probes, and current module and capability-boundary documents were inspected.
  No selected dependency or owner contract changed after DX91.
- Ownership and law: Communication owns bounded asynchronous delivery and
  returns `Ok(())` only after accepting the exact payload; closure returns the
  original `UserClosed<U>`. Bombay's tagged-ingress timing probe repeatedly
  sends exact `LocalIngress::Message` values before awaiting the receiver. Each
  send must finish before assertion syntax observes its acceptance result.
- Exact blocker and prior-state oracle: exactly one source site in
  `tagged_ingress_time` executes `MailboxRef::send(...).await` inside `assert!`;
  the site runs once per measured message. The direct-ingress and ActorRef
  timing paths already execute their sends before any assertion and remain
  separate. This assertion owns required asynchronous delivery and payload
  transfer.
- Proposed representation: introduce no representation. Bind each send result
  immediately before the existing `is_ok` assertion, preserving mailbox,
  ingress value, message order/count, backpressure, receiver, timing window,
  acceptance observation, and diagnostic behavior.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch,
  production path, or allocation is permitted. The final owner-scoped oracle
  must find zero tagged mailbox sends inside assertions while the direct and
  ActorRef timing paths remain unchanged.
- Aggregate-drift checkpoint: Communication's open/closing mailbox states,
  Bombay's `LocalIngress::{Message, Fence}`, admission owner, receiver loop,
  transition branches, modules, and public spellings remain unchanged. No
  arrival history, repeated cause, false cardinality, nested transition
  authority, semantic boolean state, or structural caller syntax is added.
  Module and capability-boundary documents remain aligned. Disposition: `pass`
  before edits.
- Dependency edges: sequenced after DX91's allocation-scoped Driver execution;
  independent of DX53 through DX90 and of the direct/ActorRef timing paths.
- Blocked by: DX91 (resolved).
- Unblocks: DX93.
- Change ledger: expected tracked paths are this ledger and
  `crates/bombay/src/local.rs` (`2` feature-local and `66` cumulative paths).
  Production is exactly `+0 / -0 / net 0`; formatted tests remain within
  `+4 / -8 / net -6..+3`; public API is `+0 / -0` types. Existing
  `MailboxRef`, `LocalIngress`, mailbox owner, consumer task, and timing probe
  are reused unchanged; nothing is added or deleted. Any production edit,
  abstraction, changed mailbox/ingress/message/order/count/backpressure,
  receiver, timing window, observation, diagnostic, foreign-owner edit, or
  bound overrun falsifies the stage.
- Planned verification: require the one asserted tagged-mailbox send site to
  fall to zero while direct and ActorRef timing paths remain separate; run the
  ignored ordinary-ingress comparison in debug and optimized profiles; then
  run the complete `bombay-rs` package/workspace suites, formatting, strict
  all-target Clippy, whitespace, and exact-delta review through pinned Nix.
- Result: every tagged-ingress mailbox send now completes as an explicit
  statement before its acceptance assertion. The final owner-scoped oracle
  finds zero tagged mailbox sends inside assertions; direct-ingress and
  ActorRef timing paths remain separate and unchanged. Mailbox, ingress value,
  message order/count, backpressure, receiver, timing window, acceptance
  observation, and diagnostics are unchanged. No representation, production
  code, public API, allocation, or dependency changed.
- Verification: the ignored ordinary-ingress comparison passes in debug and
  optimized profiles. The complete `bombay-rs` package and workspace suites,
  formatting, strict workspace all-target Clippy, whitespace, scoped
  ownership, and structural residue checks pass through pinned Nix.
- Actual checkpoint: `2` feature-local tracked paths and `66` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+4 / -6 / net -2`; public API is `+0 / -0` types. Documentation is
  `+77 / -0 / net +77`; the complete cumulative tracked tree is
  `+2953 / -736 / net +2217`.

## DX93 — Entity family-shutdown assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors `0.17.0`, Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`,
  and Timers `0.1.0` at `13e884da7ab41781f52337b0038060e375b00ee0`.
  Behavior's owner checkout remains
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; its complete 760-line
  instructions and the selected Core/Actors public roots, source/test
  inventories, and current documentation were reread. Address,
  Communication, Bombay Observe, and Timers public roots and relevant
  source/test/document inventories were rechecked. Entity's complete runtime
  shutdown contract, family tests, source ownership, and current module and
  capability-boundary documents were inspected. No selected dependency or
  owner contract changed after DX92.
- Ownership and law: `EntityRuntime::shutdown` closes family admission, settles
  directory-owned lifecycle work, drains every represented incarnation, waits
  for idle, proves the directory empty, closes the task group, and returns the
  exact represented-slot count. This lifecycle transition must finish before
  assertion syntax observes `EntityShutdown`.
- Exact blocker and prior-state oracle: exactly one direct
  `entities.shutdown().await` executes inside `assert_eq!` in
  `runtime_admission_returns_the_exact_move_only_command`. The file's other
  direct shutdown and its separately spawned shutdown task already execute
  outside assertions. The asserted site owns a required family lifecycle
  transition.
- Proposed representation: introduce no representation. Bind the exact
  `EntityShutdown` immediately before the existing represented-count
  assertion, preserving runtime, rejected command/drop evidence, shutdown
  order/count, represented count, and task settlement.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch,
  production path, or allocation is permitted. The final owner-scoped oracle
  must find zero direct Entity shutdown calls inside assertions while the other
  two shutdown paths remain separate.
- Aggregate-drift checkpoint: `EntityAdmission::{Open, Closed}`, lifecycle slot
  phases, `EntityTaskState::{Open, Closed}`, `EntityShutdown`, transition
  branches, modules, and public spellings remain unchanged. No arrival history,
  repeated cause, false cardinality, nested transition authority, semantic
  boolean state, or structural caller syntax is added. Entity module and
  capability-boundary documents remain aligned. Disposition: `pass` before
  edits.
- Dependency edges: sequenced after DX92's tagged-ingress separation;
  independent of DX53 through DX92's other owner scopes.
- Blocked by: DX92 (resolved).
- Unblocks: DX94.
- Change ledger: expected tracked paths are this ledger and
  `crates/bombay/tests/entity_family.rs` (`2` feature-local and `67` cumulative
  paths). Production is exactly `+0 / -0 / net 0`; formatted tests remain
  within `+4 / -2 / net 0..+3`; public API is `+0 / -0` types. Existing
  `EntityRuntime`, `EntityShutdown`, rejecting runtime, command/drop witness,
  and task group are reused unchanged; nothing is added or deleted. Any
  production edit, abstraction, changed command/drop evidence, shutdown
  order/count, represented count, task settlement, other-shutdown edit, or
  bound overrun falsifies the stage.
- Planned verification: require the one direct asserted Entity shutdown to fall
  to zero while retaining all three file-local shutdown paths; run the complete
  Entity family test in debug and optimized profiles; then run the complete
  `bombay-rs` package/workspace suites, formatting, strict all-target Clippy,
  whitespace, and exact-delta review through pinned Nix.
- Result: the remaining direct Entity family shutdown now completes as an
  explicit statement before its represented-count assertion. The final scoped
  oracle finds zero direct Entity shutdowns inside assertions and retains all
  three file-local shutdown paths. Runtime, rejected command/drop evidence,
  shutdown order/count, represented count, and task settlement are unchanged.
  No representation, production code, public API, allocation, or dependency
  changed.
- Verification: all three Entity family tests pass in debug and optimized
  profiles. The complete `bombay-rs` package and workspace suites, formatting,
  strict workspace all-target Clippy, whitespace, scoped ownership, and
  structural residue checks pass through pinned Nix.
- Actual checkpoint: `2` feature-local tracked paths and `67` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+2 / -1 / net +1`; public API is `+0 / -0` types. Documentation is
  `+77 / -0 / net +77`; the complete cumulative tracked tree is
  `+3032 / -737 / net +2295`.

## DX94 — Entity admission assertion separation

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: `Cargo.lock` still solely selects Behavior Core and
  Actors `0.17.0`, Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`,
  and Timers `0.1.0` at `13e884da7ab41781f52337b0038060e375b00ee0`.
  Behavior's owner checkout remains
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; its complete 760-line
  instructions, selected Core/Actors public roots, source/test inventories,
  and current documentation were reread. Address, Communication, Bombay
  Observe, and Timers public roots and relevant source/test/document
  inventories were rechecked. Entity's admission implementation, public
  ownership surface, family/runtime tests, and current module and
  capability-boundary documents were inspected. No selected dependency or
  owner contract changed after DX93.
- Ownership and law: `EntityRuntime::admit` consumes one exact command, either
  returns it in a typed `AdmissionFailure` or commits it to directory-owned
  dispatch and awaits the matching settlement. Closed family admission returns
  `Refusal::Shutdown`; failed activation returns `Refusal::Unavailable` and
  permits a later retry. The complete admission and custody transfer must
  finish before assertion syntax observes its returned alternative.
- Exact blocker and prior-state oracle: exactly three admissions execute inside
  `matches!` assertions: two asynchronous shutdown-refusal paths in
  `entity_family.rs` and one synchronous `block_on` activation-refusal path in
  `entity_runtime.rs`. The initial await-only scan found two; a broader
  `block_on` scan found the third before any source edit, so three is the
  authoritative prior total. All other Entity admission paths already execute
  explicitly or own distinct pending/retry structure. These three sites make
  assertions own required lifecycle work and exact command custody transfer.
- Proposed representation: introduce no representation. Bind each complete
  admission result immediately before its existing assertion, preserving
  runtime, entity identity, origin, exact command, execution order, refusal
  reason, shutdown timing, later retry, activation count, and delivery trace.
- Abstraction budget: no type, helper, wrapper, trait, owner, branch,
  production path, or allocation is permitted. The final owner-scoped oracle
  must find zero Entity admissions inside assertions and retain every existing
  admission path.
- Aggregate-drift checkpoint: `EntityAdmission::{Open, Closed}`, lifecycle
  slot phases, `PendingCommand`, `AdmissionFailure`, `Refusal`, directory and
  task states, transition branches, modules, and public spellings remain
  unchanged. No arrival history, repeated cause, false cardinality, nested
  transition authority, semantic boolean state, or structural caller syntax is
  added. Entity module and capability-boundary documents remain aligned.
  Disposition: `pass` before edits.
- Dependency edges: sequenced after DX93's family-shutdown separation;
  independent of DX53 through DX92's other owner scopes.
- Blocked by: DX93 (resolved).
- Unblocks: the next research scan.
- Change ledger: expected tracked paths are this ledger and the two existing
  Entity test files (`3` feature-local and `67` cumulative paths). Production
  is exactly `+0 / -0 / net 0`; formatted tests remain within
  `+8 / -4 / net +3..+6`; public API is `+0 / -0` types. Existing
  `EntityRuntime`, `AdmissionFailure`, `Refusal`, directory dispatch,
  settlement observation, shutdown, activation, and retry composition are
  reused unchanged; nothing is added or deleted. Any production edit,
  abstraction, changed runtime/entity/origin/command, execution order, refusal
  reason, shutdown timing, retry, activation count, delivery trace,
  already-explicit or foreign-owner edit, or bound overrun falsifies the stage.
- Planned verification: require the corrected prior oracle's three asserted
  admissions to fall to zero while retaining every file-local admission path;
  run the complete Entity family and runtime tests in debug and optimized
  profiles; then run the complete `bombay-rs` package and workspace suites,
  formatting, strict all-target Clippy, whitespace, exact-delta, and research
  record checks through pinned Nix.
- Result: all three Entity admissions now complete as explicit statements
  before their typed-refusal assertions. The final scoped oracle finds zero
  asserted admissions and retains every file-local admission path. Runtime,
  entity identity, origin, exact command, execution order, refusal reason,
  shutdown timing, retry, activation count, and delivery trace are unchanged.
  The first focused debug build caught a local `admission` binding shadowing
  the earlier spawned admission task; the retained domain-specific
  `shutdown_refusal` and `activation_refusal` names preserve the task's later
  join and make each observed alternative explicit. No representation,
  production code, public API, allocation, or dependency changed.
- Verification: all three Entity family tests and all eleven Entity runtime
  tests pass in debug and optimized profiles. The complete `bombay-rs` package
  and workspace suites, formatting, strict workspace all-target Clippy,
  whitespace, scoped ownership, and structural residue checks pass through
  pinned Nix.
- Actual checkpoint: `3` feature-local tracked paths and `67` cumulative
  tracked paths. Production is `+0 / -0 / net 0`; tests are
  `+6 / -3 / net +3`; public API is `+0 / -0` types. Documentation is
  `+86 / -0 / net +86`; the complete cumulative tracked tree is
  `+3124 / -740 / net +2384`.

## DX95 — generic downstream-consumer audit

- State: `feature-complete`; final fixed-point minimization remains pending.
- Evidence selected: the local Mnesis checkout at
  `5d8096912fc3c41769a26c3a32073be2e321e851`, the local Mnesis-Bombay
  checkout at `9d567f6d4f113bf3ac5ffeb569c219efb0ecf160`, its ADR, roadmap,
  production-readiness research, current source/tests, and the live private
  Bombay-Nexus project cards were inspected. Mnesis issues `#358` and `#359`,
  Mnesis-Bombay epic `#2`, cards `#5` through `#12` and `#20` through `#23`,
  and Bombay-Nexus reference/runtime cards `#1` through `#10` were reconciled.
  The accepted long-term Agency harness architecture dated 2026-09-22 was read
  completely at SHA-256
  `9727e78760219ca21fa05677fb0034961744fd6f0b983f960066bf612f02ec0f`.
- Version finding: Mnesis-Bombay's current lock selects Mnesis `0.3.1` but
  still selects released Bombay `0.1.0`, Behavior `0.9.5`, Entity `0.1.0`,
  Communication `0.1.1`, and Address `0.1.1`. Several live card bodies still
  state the earlier Mnesis `0.2.2` baseline. These versions and downstream
  worktrees are evidence only; Bombay's current `Cargo.lock` remains the sole
  selected implementation contract.
- Generic runtime laws evidenced by multiple consumers: preserve stable
  logical identity separately from exact incarnation; initialize completely
  before routability; provide bounded admission and exact rejected-payload
  recovery; distinguish enqueue, completed actor turn, terminal fact, and any
  application-owned durable acknowledgement; keep timers and task ownership
  actor-scoped; support staged readiness, exact retirement, old-incarnation
  non-retargeting, ordered drain, and complete terminal classification; expose
  bounded-cardinality lifecycle observations at their actual linearization
  points; and keep all queues, activation, concurrency, and retry surfaces
  explicitly bounded.
- Consumer-owned laws excluded from Bombay: Mnesis command identity, append,
  conflict, checkpoint, inbox/outbox, projection, saga, and durability policy;
  Agency harness revisions, promotion, task truth, roles, and mandates; Selo
  identity and authorization; Zenoh remote placement and transport; HLC and
  metadata enrichment; persistent-store recovery; and application-specific
  retry, poison, quarantine, or exactly-once claims. No consumer vocabulary may
  enter Bombay merely because a current card requests it.
- DX, performance, and Rust selection law: every future candidate must improve
  a generic actor-runtime invariant or remove measured friction while
  preserving the sole Behavior/Driver/Environment path. Recheck ownership,
  caller syntax and diagnostics, redundant surface, hot-path allocations and
  latency, bounded memory, cancellation/drop behavior, and idiomatic ownership
  and algebraic-state expression together. A wrapper, trait, alias, helper,
  callback, registry, or duplicate test oracle without a distinct invariant is
  rejected. Performance claims require equivalent-boundary measurements; DX
  claims require caller-visible evidence; Rust changes must make illegal states
  or ownership mistakes harder, not merely satisfy the compiler.
- Test selection law: semantic changes require a failing caller-visible
  regression plus applicable inversion, complete state/output, sequence,
  lifecycle, defensive-boundary, concurrency/linearizability, property,
  Loom/fuzz, compile-denial, allocation, and benchmark evidence. Add every
  materially distinct oracle, but reject repeated assertions, discarded
  outputs, implementation-copied models, and tests that cannot fail for the
  intended invariant.
- Aggregate-drift checkpoint: Bombay remains a generic actor runtime. No
  production state, subordinate sum, branch, module, public spelling, consumer
  policy, arrival history, repeated cause, false cardinality, nested runtime,
  semantic boolean, or structural caller syntax changes. Disposition: `pass`.
- Dependency edges: sequenced after DX94 so the resumed loop incorporates the
  requested consumer audit before selecting another implementation candidate.
- Blocked by: DX94 (resolved).
- Unblocks: DX96.
- Change ledger: expected outputs are this one tracked ledger path plus the
  ignored research contract and mirrors (`1` feature-local tracked and `67`
  cumulative tracked paths); production and tests are exactly
  `+0 / -0 / net 0`; public API is `+0 / -0` types;
  tracked documentation remains within `+130 / -5 / net +100..+130`. No source,
  test, manifest, external repository, or live card is edited. Any production
  change, downstream-specific Bombay API, second source of truth, stale-version
  promotion, or bound overrun falsifies the stage.
- Planned verification: cross-check every distilled law against Bombay's
  ownership documents and current lock, require the consumer-specific residue
  scan to remain zero in production, validate all durable research records,
  and run whitespace and exact-delta checks.
- Result: the current consumer sources now constrain future selection without
  becoming Bombay architecture or dependency authority. Generic lifecycle,
  custody, boundedness, readiness, retirement, observation, DX, performance,
  Rust-modeling, redundancy, and invariant-test laws are retained; durability,
  identity, transport, workflow, and application policies remain excluded.
- Verification: the production consumer-vocabulary residue scan is zero;
  Bombay's ownership documents and selected lock remain authoritative; all
  JSONL research records parse; whitespace and exact-delta checks pass.
- Actual checkpoint: `1` feature-local tracked path and `67` cumulative tracked
  paths. Production and tests are exactly `+0 / -0 / net 0`; public API is
  `+0 / -0` types. Tracked documentation is `+87 / -0 / net +87`; the
  complete cumulative tracked tree is `+3211 / -740 / net +2471`.

## DX96 — local fence assertion separation

- State: `feature-complete`; final minimization remains open.
- Selected contracts: Bombay's lock is unchanged; Behavior remains at owner
  revision `435560ce7bea8ad3330ee2d42e5034f837a80602`. Its complete 760-line
  instructions, selected owner roots, Bombay local fence implementation, all
  four fence callers, and capability/module documents were freshly rechecked.
- Law and blocker: `LocalActor::fence` either rejects before control enqueue as
  `FenceFailure::Enqueue`, or transfers one acknowledgement publisher and
  resolves accepted loss as `FenceFailure::Acknowledgement`. Exactly three of
  four local fence calls execute inside assertions; the successful ordering
  witness is already explicit. Required delivery/custody must finish before
  observation.
- Representation and budget: bind each of the three exact fence results before
  its existing assertion. Add no type, helper, branch, allocation, production
  path, test case, or public API; preserve actor, call order, failure stage,
  shutdown/crash, and the explicit successful path. Expected paths are this
  ledger and `local.rs` (`2` feature-local, `67` cumulative); production is
  `+0/-0`, tests were forecast within `+6/-3`, and public types are `+0/-0`.
- Aggregate-drift checkpoint: actor endpoint states, control ingress, publisher
  custody, fence alternatives, transitions, modules, and public spellings are
  unchanged. No consumer policy, redundant oracle, semantic boolean, arrival
  history, or nested runtime is added. Disposition: `pass` before edits.
- Blocked by: DX95 (resolved).
- Unblocks: the next generic runtime scan.
- Planned verification: reduce the three-site scoped oracle to zero while
  retaining all four calls; run focused local tests in debug and optimized
  profiles, then package/workspace, formatting, strict Clippy, residue,
  whitespace, exact-delta, and research-record gates through pinned Nix.
- Forecast correction: rustfmt collapsed each prior multiline assertion, so
  the exact test delta is `+6/-12`, not the forecast `+6/-3`. The additional
  deletions remove only assertion-owned formatting; they add no scope or
  behavior and improve the requested redundancy reduction.
- Result: all three fence operations now complete before their exact typed
  failure is observed. The scoped assertion-owned oracle is zero, all four
  local fence calls remain, and the already explicit successful ordering path
  is unchanged.
- Verification: focused local tests pass in debug and optimized profiles; the
  complete package and workspace suites, formatting, and strict Clippy pass.
  The scoped oracle, whitespace, exact-delta, and research-record gates pass;
  production and public API remain unchanged.
- Actual checkpoint: `2` feature-local tracked paths and `67` cumulative
  tracked paths. Production is exactly `+0/-0/net 0`; tests are
  `+6/-12/net -6`; public API is `+0/-0` types. Tracked documentation is
  `+48/-0/net +48`; the complete cumulative tracked tree is
  `+3265/-752/net +2513`.

## DX97 — local termination assertion separation

- State: `feature-complete`; final minimization remains open.
- Selected contracts: `Cargo.lock` still selects Behavior and Behavior Actors
  `0.17.0`, Behavior Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`,
  and patched Timers `0.1.0` at
  `13e884da7ab41781f52337b0038060e375b00ee0`. Behavior's owner remains
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; its complete 760-line
  instructions were freshly reread. The exact registry sources, owner roots,
  public algebra, relevant lifecycle tests, Observe future, local publication,
  retirement path, and current capability/module documents were rechecked.
- Law and blocker: one exact actor incarnation publishes one retained
  `Result<Exit<A>, Crash>` after retirement; cloned references observe that
  same authoritative typed fact, and an address replacement owns another
  observation. All five local termination waits currently execute inside
  assertions: four normal exits and one panic. Required waiting and typed-fact
  acquisition must complete before observation.
- Representation and budget: bind each of the five exact termination outcomes
  before its existing assertion. Add no test, type, helper, branch, allocation,
  production path, public API, or duplicated oracle; preserve actor identity,
  shutdown/fence order, normal and panic alternatives, and exact expectations.
  Expected paths are this ledger and `local.rs` (`2` feature-local, `67`
  cumulative); production is `+0/-0`, tests stay within `+10/-5`, and public
  types are `+0/-0`.
- Aggregate-drift checkpoint: ActorRef state, Observe ownership, terminal sum,
  publication selection, retirement, transitions, modules, and public syntax
  are unchanged. No consumer policy, semantic boolean, arrival history,
  repeated cause, false cardinality, nested runtime, or structural user syntax
  is added. Disposition: `pass` before edits.
- Blocked by: DX96 (resolved).
- Unblocks: the next generic runtime scan.
- Planned verification: reduce the five-site scoped oracle to zero while
  retaining all five waits and exact alternatives; run focused local tests in
  debug and optimized profiles, then package/workspace, formatting, strict
  Clippy, residue, whitespace, exact-delta, and research-record gates through
  pinned Nix.
- Result: all five exact terminal facts now resolve before observation. The
  scoped assertion-owned oracle is zero, all five waits remain, and the four
  normal exits and one panic preserve their original actor and lifecycle order.
- Verification: focused local tests pass in debug and optimized profiles; the
  complete package and workspace suites, formatting, and strict Clippy pass.
  The scoped oracle, whitespace, exact-delta, and research-record gates pass;
  production and public API remain unchanged.
- Actual checkpoint: `2` feature-local tracked paths and `67` cumulative
  tracked paths. Production is exactly `+0/-0/net 0`; tests are
  `+10/-5/net +5`; public API is `+0/-0` types. Tracked documentation is
  `+50/-0/net +50`; the complete cumulative tracked tree is
  `+3325/-757/net +2568`.

## DX98 — repository-complete source audit

- State: `feature-complete`; the repository-complete audit produced the
  canonical execution queue in `docs/todo.md`; final fixed-point minimization
  remains open.
- Selected contracts: the locked graph and Behavior owner are unchanged from
  DX97; Behavior's complete 760-line instructions were freshly reread. Exact
  registry roots for Behavior, Behavior Actors, Address, and Communication,
  the patched Timers source, Bombay capability/module documents, the research
  contract, and the downstream-evidence boundary were rechecked before scope
  selection.
- Law and blocker: prior stages used repository-wide compiler/test gates but
  feature-local source inspection. That cannot justify a claim that every
  Bombay code surface was scanned. The audit must first enumerate files, then
  account for every source surface regardless of whether it matches a favored
  pattern.
- Complete inventory: `172` non-generated Rust files and `36,079` Rust lines:
  `55` production, `93` tests/fixtures, `4` benchmarks, `5` fuzz targets, `13`
  examples, and `2` historical research probes. The non-generated build graph
  has `27` files and `3,670` lines. Current and historical documentation has
  `21` files and `8,777` lines. Generated `target/` trees are excluded because
  they are dependency/compiler output, not Bombay source.
- Representation and budget: this stage records one file-complete audit matrix
  covering ownership and boundaries, unsafe/dynamic escape hatches, semantic
  booleans, imports and naming, required calls inside assertions, panic and
  partial-control paths, redundant public/private machinery, dependency and
  feature topology, allocation/performance evidence, and test-layer coverage.
  It may identify later features but edits no Rust, manifest, example,
  benchmark, fuzz target, external repository, or live card. Expected path is
  this ledger only (`1` feature-local, `67` cumulative); production/tests/API
  remain `+0/-0`, and tracked documentation stays within `+120/-5`.
- Aggregate-drift checkpoint: no runtime state, transition, product, wrapper,
  branch, module, public spelling, consumer policy, or nested authority changes
  in this audit. Every finding must name its existing semantic owner and remain
  separate until a failing law makes an edit eligible. Disposition: `pass`
  before audit execution.
- Blocked by: DX97 (resolved).
- Unblocks: the first verified repository-wide finding.
- Planned verification: prove every enumerated file is present in the audit
  matrix; inspect complete source plus structural/residue scans; run package
  metadata, all workspace targets, nextest, full tests, formatting, strict
  Clippy, and authoritative `nix flake check`; record exact findings and deltas
  without claiming that green tools prove the architecture.
- Result: the complete audit is recorded in `docs/todo.md` as 25 test/evidence
  findings, 20 architecture findings, their reciprocal canonical queue, crate
  and suite matrices, exact baseline gates, and the terminal audit. The first
  selected finding is TEST-001; the stale Driver benchmark keeps the flake gate
  red and is separately owned by TEST-002.
- Actual checkpoint: the audit changed no production code, test code, public
  API, manifest, example, benchmark, or fuzz target. Its only new path is the
  untracked `docs/todo.md` control plane; the 67 pre-existing modified tracked
  paths remain user work and the audit findings preserve them as the baseline.

## TEST-001 — Driver final-settlement precedence

- State: `verified`; reopened once after the full flake gate exposed and then
  disproved a second publication on accepted initialization continuation.
- Selected contracts: `Cargo.lock` selects Behavior Core and Actors 0.17.0 and
  Macros 0.12.0 from owner revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, and patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`. The complete locked Behavior
  instructions, Core `Interpretation`/`ClassifySettlement`/
  `SettlementStatus`/`SourceCustody` algebra and status/custody tests, Actors
  total-settlement tests, Engine Driver and Environment ports, Address claim
  semantics, Communication retirement semantics, private Observe publication,
  Timers queue contract, and current Driver/runtime/module documents were
  freshly inspected. Address, Communication, Observe, and Timers do not own
  Driver disposition precedence and require no change for this item.
- Governing law and exact defect: Behavior owns the lossless status projection:
  expected rejection is a complete typed settlement fact, while corruption is
  an interpreter-contract failure. Engine alone owns terminal precedence. The
  active loop currently enqueues a stopping turn's settlement and returns
  `Completion::Stopped` before observing corruption. The initialization path
  likewise lets `Step::Stop` hide both rejected and corrupt installed
  initialization settlements. This violates current D-INIT-1, D-TERM-1,
  D-RETIRE-1, and the
  authoritative-fact conservation law. An expected rejection from an active
  stopping turn remains an ordinary stopped completion with exact retirement
  custody because the stopping Behavior cannot admit its final result back to
  itself; a corrupt active settlement is terminal failure. A rejected or
  corrupt installed-initialization settlement prevents successful
  initialization even when initialization also selected stop.
- Smallest prior-representation regression: extend the public Engine
  `terminal_custody` boundary with
  `stopping_turn_corruption_overrides_stop_and_preserves_settlement`,
  `stopping_turn_rejection_preserves_stop_and_settlement`,
  `stopping_initialization_rejection_overrides_stop_and_preserves_settlement`,
  and `stopping_initialization_corruption_overrides_stop_and_preserves_settlement`.
  Each asserts final Behavior state, exact settlement custody, publication,
  one retirement, untouched later ingress, and exact disposition. With
  production unchanged, the corrupt active case and both stopping-
  initialization cases must fail for precedence; the active rejection case is
  the required non-error inversion.
- Behavior-first ownership decision: reuse Core's existing
  `Interpretation::settlement_status` and exact owned settlement unchanged.
  Add no Bombay settlement, wrapper, callback, trait, phase, policy, or public
  type. The only production change is exhaustive Engine ordering between the
  already-owned status and `ActionDecision`; retirement continues to receive
  the same exact product.
- Dependency edges: blocked by DX98 (resolved). Unblocks TEST-002, TEST-006,
  and TEST-007 after verification. No dependency edge is delegated to Address,
  Communication, Observe, Timers, or Behavior Actors because none classifies
  Driver completion.
- Change ledger: expected task-local paths are this ledger,
  `docs/todo.md`, `crates/bombay-engine/tests/terminal_custody.rs`,
  `crates/bombay-engine/src/driver.rs`, `docs/driver-law.md`, and
  `docs/driver-test-strategy.md` (`6` paths beyond the preserved baseline).
  Forecast production is `+9 / -18 / net -9`; tests are at most
  `+190 / -35 / net +155`; public API is `+0 / -0` types. Existing Core
  statuses and settlements, Engine `ActionDecision`, retirement products, and
  the terminal-custody fixture are reused; no production owner is deleted.
  Crossing 15 task-local paths, net `+500` production lines, or three new
  public types requires explicit authorization.
- Planned verification: capture the focused debug failure with production
  unchanged, apply only the exhaustive precedence correction, run the focused
  regression in debug and optimized profiles, run all Engine tests and strict
  Clippy, audit the complete tracked/untracked diff and status/status-custody
  residue, update the Driver law and strategy, and record exact results and
  deltas in both ledgers.
- Prior-representation proof: with `driver.rs` unchanged, pinned-Nix command
  `nix develop -c cargo test --locked -p bombay-engine --test terminal_custody -- --nocapture`
  ran 12 tests and failed exactly the three intended precedence regressions.
  Each retained the expected final Behavior, complete settlement, publication,
  one retirement, and untouched event `99`, but observed `Ok(Stopped)` instead
  of `Err(Settlement(Corrupt))`, `Err(Settlement(Rejected))`, and
  `Err(Settlement(Corrupt))`, respectively. The active expected-rejection
  inversion and all eight prior custody tests passed (`9 passed; 3 failed`).
- Document-scope boundary: the fresh cross-check also confirmed that
  `driver-law.md`'s transactional-initialization section describes the desired
  commit-before-claim transaction rather than the current split
  `activate -> classify -> publish` implementation, and that the test
  strategy's manifest/catalogue claims are not yet executable evidence. Those
  pre-existing gaps remain owned by TEST-023/ARC-006 and TEST-003/TEST-008,
  respectively. TEST-001 updates only the final-settlement precedence clauses
  proved here and does not falsely normalize those later queue items.
- Result: Engine now classifies each installed initialization settlement before
  honoring its stop decision and each active settlement before honoring its
  stop decision. Initialization rejection or corruption therefore prevents a
  false stopped completion; active corruption overrides stop, while active
  expected rejection preserves stopped completion and transfers the exact
  settlement to retirement. No settlement, status, policy, wrapper, trait, or
  public type was added.
- Verification: the prior representation failed exactly the three intended
  regressions (`9 passed; 3 failed`) and preserved the active-rejection
  inversion. The corrected representation passes all 12 custody tests in debug
  and release, the complete Engine suite, workspace formatting, and strict
  all-target Engine Clippy. The six-path task-local diff passes whitespace and
  residue checks. Loom, Miri, fuzz, and benchmark are inapplicable to this
  deterministic precedence edit and remain assigned to TEST-006/TEST-002.
- Actual checkpoint: production is `+29/-35/net -6`; tests are
  `+364/-66/net +298`; public API is `+0/-0` types. The test delta is measured
  against the recorded pre-item `+112/-3` terminal-custody diff and includes
  complete product assertions for every old and new custody case. The forecast
  underestimated this fixture completion; the production delta remained
  smaller than forecast and no containment threshold was approached.
- Reopened defect and prior-representation evidence: the first correction
  called `environment.publish()` before the initialization-status match for
  every successful activation. Accepted continuation then entered
  `drive_active`, whose `ExecutionPhase::Initializing` branch published again
  after the initialization settlement exhausted. `nix flake check path:.`
  compiled the complete release workspace, then failed
  `bombay-example-axum`'s `live_http_flow_reaches_the_root_and_shutdowns_it`
  at Local's exact once-publication guard. The smallest regression extends the
  Engine custody fixture's algebraic publication state to distinguish pending,
  published-once, and repeated publication; existing complete retirement
  assertions must reject the repeated state against this representation.
- Reopened ownership and change ledger: Engine still owns publication
  sequencing, and the existing `ExecutionPhase::Initializing` transition owns
  accepted-continuation publication after initialization settlement custody.
  Terminal initialization alternatives publish directly before retirement.
  Reuse those branches; add no state, owner, wrapper, trait, or public type.
  The reopened delta stays within the same six TEST-001 paths: production is
  expected to remove the misplaced call and restore at most three branch-local
  calls (`+3/-1`); tests are within `+15/-5`; public API remains `+0/-0`.
  The focused fixture must fail before production changes, then pass in debug
  and release along with the Axum live flow and full flake gate.
- Reverified result: the custody fixture's publication sum now records
  `Withheld`, `Published`, or `Repeated`; the first correction failed its
  existing complete retirement assertion solely as `Repeated`. Engine leaves
  accepted continuation publication in the existing post-settlement phase and
  publishes directly only for terminal initialization alternatives. The exact
  Axum live flow passes, and `nix flake check path:.` passes all 17 native
  checks, including workspace tests, build, docs/doctests, strict Clippy,
  formatting, all three Loom checks, panic modes, Axum, and every public
  example.
- Final TEST-001 checkpoint: relative to the recorded pre-item baseline,
  production is `+34/-34/net 0`; tests are `+368/-66/net +302`; public API is
  `+0/-0` types. The same six task-local paths remain in scope. A first flake
  attempt exhausted disk while archiving a successful build; pinned
  `cargo clean` removed only 43.6 GiB of generated, rebuildable `target/`
  artifacts, and the unchanged gate then reached the integration defect before
  the final corrected run passed.

## TEST-002 — Driver benchmark terminal custody

- State: `verified`; resumed with its benchmark patch and prior/focused evidence
  retained after TEST-001 passed the full integration gate.
- Selected contracts and owners: `Cargo.lock` remains on Behavior Core and
  Actors 0.17.0 and Macros 0.12.0 at owner revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`. Core owns the exact total
  `Interpretation` settlement and `SourceCustody`; Engine owns its causal
  settlement queue and transfers every still-owned settlement into active
  retirement. Criterion owns measurement. Address, Communication, Observe,
  Timers, and Behavior Actors add no benchmark-specific law.
- Exact defect and dependency edges: the Driver benchmark's active retirement
  asserts that the settlement vector is empty. A stopping turn instead leaves
  its final complete settlement in Driver custody, so current D-RETIRE-1 transfers one
  exact settlement to retirement. The stale assertion fails the focused
  release benchmark and `nix flake check path:.`. TEST-001 is resolved and
  supplies the focused correctness regression; TEST-002 unblocks ARC-020.
- Smallest prior-representation proof: run
  `nix develop -c cargo test --locked -p bombay-engine --bench driver --release`
  unchanged and retain its exact assertion failure. The benchmark fixture will
  then return retirement custody as its typed residual, compare one complete
  preflight `DriverRetirement` outside Criterion's measured loop, and pass each
  measured retirement through `black_box`. This keeps correctness evidence
  exact without charging assertions to the measurement.
- Expected files and delta: task-local paths are
  `crates/bombay-engine/benches/driver.rs`, this ledger, and `docs/todo.md`.
  Production remains `+0/-0/net 0`; benchmark/test code is expected within
  `+30/-15/net +15`; public API is `+0/-0` types. The private benchmark's
  semantic source-exhaustion boolean is replaced by a closed private enum while
  the file is in scope. No runtime type, branch, or owner is added or deleted.
- Planned verification: preserve the prior release failure, run the corrected
  benchmark in debug and release, run the focused terminal-custody regression,
  restore `nix flake check path:.`, run formatting and strict Engine Clippy,
  and inspect the complete tracked/untracked diff. Loom, Miri, and fuzz are not
  relevant to this deterministic benchmark-fixture correction.
- Result: active retirement returns its settlement vector as the benchmark
  environment's typed residual. One preflight execution compares the complete
  `DriverRetirement`, including the one empty total-settlement product, outside
  Criterion's measured loop; measured executions pass their complete result to
  `black_box`. The semantic source-exhaustion boolean is replaced by a closed
  private `Ingress` sum. The focused correctness oracle remains in
  `terminal_custody`; no runtime or public API changed.
- Verification: the unchanged optimized benchmark failed exactly at the stale
  empty-settlement assertion. The corrected benchmark reports `Success` in
  debug and optimized profiles. All 12 terminal-custody tests pass, and
  `nix flake check path:.` passes all 17 native checks. The task-local diff and
  whitespace checks pass; complete status confirms every unrelated worktree
  change remains present.
- Actual checkpoint: three task-local paths; production `+0/-0/net 0`;
  benchmark/test code `+35/-11/net +24`; public API `+0/-0` types. The private
  enum and complete preflight exceeded the line forecast by nine net lines but
  eliminated the semantic boolean and in-measurement assertions; no repository
  containment threshold was approached.

## TEST-004 — production-bound Driver inversions

- State: `verified`; TEST-003 is now unblocked to replace the honest blocked
  manifest rows with per-law executed evidence.
- Selected contracts and owners: the lock remains on Behavior Core/Actors
  0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`. Behavior owns the deterministic
  fold and total-settlement algebra; Engine owns only the causal Driver;
  `cargo-mutants` owns source mutation, and the existing Nix
  `mutants-sweep` derivation already selects `bombay-engine`. The complete
  Driver, its public-interface law/property/custody suites, mutation config,
  mutation gate, baseline, flake derivations, law manifest, and current
  law/strategy documents were freshly inspected.
- Exact defect: `driver_inversions.rs` is 490 lines containing 38 tests over a
  disconnected local `Fact` vector and scalar predicates, including semantic
  booleans forbidden by the repository model. No test invokes `Driver` or
  mutates production. Forty-eight manifest rows nevertheless cite those tests
  as passing falsification evidence. Duplicate fake mutations include the same
  prefetch vector for three laws and the same lane permutation for two laws.
- Smallest prior-representation regression: add one source-accounting test to
  `law_manifest.rs` that requires the disconnected suite and every
  `driver_inversions::` manifest reference to be absent, the required mutation
  derivation to select `bombay-engine`, and the reviewed baseline to contain
  production Driver candidates. It must fail before replacement for those
  exact missing conditions. Independently run the supported mutation tool over
  `bombay-engine`; its candidate/outcome report, not a copied toy model, owns
  the deliberate mutations.
- Behavior-first ownership and dependency decision: add no production hook,
  cfg branch, duplicate Driver, mutation facade, or new public type. Delete the
  disconnected suite. Reuse the existing public-interface regressions as
  mutation killers and the existing `cargo-mutants`/Nix/mutants-gate path as
  the production mutator. Rows whose fake inversion evidence is removed become
  honestly `blocked`; TEST-003 remains responsible for executable per-law
  evidence and cannot inherit a blanket passing claim.
- Expected paths and delta: task-local paths are this ledger, `docs/todo.md`,
  `driver_inversions.rs`, `law_manifest.rs`, `driver-law-manifest.json`,
  `mutants-baseline.json`, `flake.nix`, and the Driver law/strategy if their
  current proof wording requires correction (at most 9 paths). Production is
  `+0/-0/net 0`; tests/configuration are expected below `+90/-500/net -410`;
  public API is `+0/-0` types. No containment threshold is approached.
- Planned verification: capture the source-accounting failure unchanged; list
  and execute every production Driver candidate through pinned Nix; record
  each caught, unviable, missed, or timed-out outcome; delete fake and duplicate
  inversions; run law-manifest and complete Engine suites in debug/release,
  strict Clippy, formatting, the mutation gate, residue/whitespace checks, and
  the complete tracked/untracked checkpoint. Fuzz/Miri/Loom are N/A for this
  evidence-repair item and remain assigned to their owning queue rows.
- Prior-representation result: the new source-accounting regression failed
  with all four intended defects: the disconnected suite remained, the
  manifest still named it, the required mutation derivation omitted Engine,
  and the reviewed baseline omitted Driver candidates. The first real mutation
  invocation also rejected the old trailing `--profile mutants` spelling as a
  nonexistent Cargo profile, so both mutation derivations now use their
  supported runner form.
- Result: delete all 490 lines and 38 tests in `driver_inversions.rs`. The 48
  rows that had cited those fake mutations now use the production Driver
  mutation source and remain honestly `blocked` for TEST-003; the other 20
  manifest rows remain `passing`. The required Nix source now includes its own
  `flake.nix`, and the required mutation package set includes both `bombay-rs`
  and `bombay-engine`. No production hook, duplicate Driver, or public type was
  added.
- Production mutation evidence: the pinned runner found 14 candidates, all in
  `crates/bombay-engine/src/driver.rs`. Twelve generated replacements were
  compile-time unviable. Both viable mutations—returning `None` from
  `next_progressing_settlement` and deleting its negation—were caught; neither
  was missed or timed out. Their mutation logs name
  `driver_allocation::one_complete_driver_execution_allocates_one_settlement_queue`
  as the killing regression and report the intended failure, two settlement
  queue allocations instead of one. The reviewed baseline therefore requires
  exactly the two viable `next_progressing_settlement` catches and records the
  four generic functions with no viable generated replacement.
- Verification: complete Engine tests pass in debug and release, including
  compile fixtures and documentation tests; strict Engine Clippy, workspace
  formatting, the full manifest suite, whitespace/residue checks, and the
  Engine-only Nextest mutation run pass. The combined Nix mutation derivation
  first proved the filtered-source fix, then stopped in its unmutated Bombay
  baseline because
  `entity_runtime::fence_failures_preserve_the_forced_retirement_stage` observed
  no retirement under concurrent load. That unrelated fixed-yield test passed
  20/20 sequential pinned-Nix reruns. Its causal-wait repair remains with the
  queued Entity test work; the terminal mutation gate must rerun after that
  owner is resolved.
- Actual checkpoint: eight task-local paths; production `+0/-0/net 0`;
  test code `+41/-567/net -526`; public API `+0/-0` types. Configuration and
  evidence registries are `+153/-147/net +6`. The complete tracked and
  untracked status still contains every unrelated baseline path.

## TEST-003 — executable revision-bound Driver-law evidence

- State: `verified`; TEST-004 supplied the production-mutant prerequisite and
  the schema-2 evidence gate is now executable in ordinary Nix checks.
- Exact defect: the schema-1 manifest stores evidence names and local status
  strings, not executed results. Its ordinary test source-scans positive names,
  accepts unresolved inversion labels, and applies the same negative,
  boundary, adversarial, template, and unpinned host-Cargo command to all 68
  laws. The only all-row assertion is ignored and merely requires every status
  string to equal `passing`. TEST-004 has now exposed 48 of those rows as
  honestly blocked because the deleted toy inversions never touched Driver.
- Ownership and revision gate: the lock remains on Behavior Core/Actors 0.17.0
  and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`. Behavior owns the fold,
  `Actions`, source settlement custody, typed child products, and structural
  interpretation; Behavior Actors owns template policy. Engine may catalogue
  only universal causal Driver laws, while Bombay and primitive crates own
  their integration/concurrency facts. Before retaining any of the 68 rows,
  compare its exact claim with those selected owners and current production;
  do not build an execution framework around an ownership-stale law.
- Dependency edges: TEST-004 supplies the production-mutant inventory and
  unlocks this item. TEST-003 in turn remains a prerequisite of ARC-020. The
  template catalogue is independently owned by TEST-008 and must not be
  fabricated here.
- Smallest prior-representation regression: add one ordinary manifest
  contract test that requires the selected Behavior revision on every evidence
  record, structured executable positive/inversion outcomes or an exact owned
  blocker, pinned-Nix commands, no blanket copied evidence fields, and no
  ignored completion gate. It must fail schema 1 for those intended reasons
  before any manifest or law rewrite.
- Expected paths and delta: this ledger, `docs/todo.md`,
  `docs/driver-law.md`, `docs/driver-test-strategy.md`,
  `docs/driver-law-manifest.json`, `law_manifest.rs`, one mutation-gate script,
  and `flake.nix` (at most 8 paths). Production is
  `+0/-0/net 0`; test/gate code is expected below `+250/-250/net 0`; public API
  is `+0/-0` types. Deleting ownership-stale laws and validation tables is
  preferred to adding a manifest framework. No containment threshold is
  approached.
- Prior-representation result: after adding only the schema contract regression,
  `nix develop -c cargo test --locked -p bombay-engine --test law_manifest
  manifest_evidence_is_revision_bound_and_executable -- --exact --nocapture`
  failed as intended. It reported schema 1, no locked Behavior revision, no
  result artifact, scalar rather than structured positive/boundary/inversion
  records for all 68 rows, unpinned commands, and the ignored status-only
  completion gate. No production or manifest representation had changed.
- Planned verification: capture the new contract regression failing against
  schema 1; run each retained positive and inversion/killer through its exact
  pinned-Nix command or record a typed owner/blocker that the gate validates;
  run the manifest suite ordinarily with no ignored completion test, complete
  Engine debug/release, strict Clippy, formatting, any new Nix gate, residue
  and whitespace checks, and the complete tracked/untracked checkpoint.
  Loom/Miri/fuzz/benchmark are N/A unless a retained row specifically names
  their owning command; their broad campaigns remain with their queue items.
- Ownership audit result: the concern that the Driver law had drifted after
  Behavior's redesign was correct at the catalogue boundary. Sixty of the 68
  identifiers restated Behavior's fold/interpretation algebra, Behavior Actors
  template policy, Bombay adapter duties, primitive scheduling policy, absence
  claims, or future campaigns. They are no longer presented as mandatory
  Engine laws. The eight retained laws are initialization/activation, causal
  turns, ordered settlement custody, terminal disposition, affine retirement,
  the phased typed Environment port, the one opaque production surface, and
  executed evidence. This changes documentation and evidence only; Driver and
  Environment production code remain untouched.
- Executable representation: schema 2 binds Core/Actors 0.17.0 and Macros
  0.12.0 to owner revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`. Every law has unique structured
  positive, boundary, and inversion records. The new runner copies the current
  filtered tree, applies each mutation to the real `driver.rs`,
  `environment.rs`, or manifest source, rejects compile-only/unrelated
  failures, restores its disposable copy, and emits an actual JSON result
  artifact. The former ignored status test and the appended-string pseudo
  mutations were deleted; current source scanners are supplemental structural
  policies only.
- Executed result: `nix build path:.#driver-law-evidence --print-out-paths
  --no-link` passed and produced
  `/nix/store/9jcxna3i3fy3hbwyv83qwir9ars2jjzh-bombay-workspace-0.1.0/driver-law-evidence.json`
  with SHA-256
  `7fc2f959df70492f3c64287d3ad65cc9997b76b718189cd5ee99ce9875c230e6`.
  Its eight rows contain only `passed` positive/boundary results and `killed`
  inversion results. Ordinary and optimized manifest suites each passed 9
  tests with none ignored; complete Engine debug/release, strict Engine
  Clippy, Cargo/rustfmt, Nix formatting, shell syntax, JSON parsing, flake
  evaluation, residue, and whitespace gates passed.
- Actual checkpoint: eight task paths; production `+0/-0/net 0`; test/gate
  code `+450/-326/net +124`; public API `+0/-0` types. Nix configuration is
  `+20/-0/net +20`. The test addition is the 253-line disposable mutation
  runner plus schema validation; deletion of 129 net manifest-test lines and
  439 net strategy lines removes the former status framework and pseudo
  inversions. Loom, Miri, fuzz, benchmarks, examples, and template inventory
  are N/A for this evidence-only item and retain their canonical queue owners.

## TEST-005 — production-bound Incarnation inversions

- State: `verified`; this independent P0 evidence item supplies its required
  lifecycle evidence to ARC-020, which retains other unresolved prerequisites.
- Selected contracts and owners: the lock selects Behavior Core/Actors 0.17.0
  and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, and Timers at
  `13e884da7ab41781f52337b0038060e375b00ee0`. Behavior owns folds and typed
  settlements; Engine owns one affine Driver execution; Tokio owns task
  cancellation and unwind mechanics. Bombay's private Incarnation uniquely
  owns the terminal guard that classifies panic versus cancellation only after
  Driver-owned state is destroyed and consumes one retirement capability.
  Current Incarnation, outcome conversion, retirement, launch, termination,
  module-boundary and runtime-capability sources were freshly inspected.
- Exact defect: the two purported inversion tests at the end of
  `incarnation.rs` never invoke or mutate Incarnation. One counts six constant
  labels; the other compares vectors produced by a disconnected toy enum.
  They cannot prove the real panic, cancellation, destruction order, exact
  Driver-result classification, or one-retirement laws. The real panic and
  cancellation tests also await task ownership transfers inside assertions.
- Dependency edges: no prerequisite. The item supplies ARC-020's required
  lifecycle evidence. No Behavior Actors template or primitive capability
  contract is changed or duplicated.
- Smallest prior-representation regression: add one unit contract that
  requires the two toy test definitions to be absent and a pinned-Nix evidence
  gate to mutate the actual `incarnation.rs`/`outcome.rs` sources with named
  real lifecycle killers. Run it unchanged before adding the gate or deleting
  the toys; it must fail because the old representation has only disconnected
  constants.
- Prior-representation result: with only that contract added,
  `nix develop -c cargo test --locked -p bombay-rs --lib
  incarnation::tests::incarnation_mutation_evidence_owns_production_source --
  --exact --nocapture` failed at the first disconnected test definition. The
  evidence script and Nix check were also absent. This is the intended defect,
  before either toy was deleted or any evidence gate was introduced.
- Expected paths and delta: this ledger, `docs/todo.md`, the test-only portion
  of `crates/bombay/src/incarnation.rs`, one executable evidence script under
  `crates/bombay/tests`, `flake.nix`, and the reviewed mutation baseline if its
  stale signatures require correction (at most 6 paths). Production is
  `+0/-0/net 0`; test/gate code is expected below `+300/-100/net +200`; public
  API is `+0/-0` types. No containment threshold is approached.
- Production inversions: arm the terminal guard after Driver polling, reverse
  cancellation destruction order, discard abnormal retirement, classify panic
  as cancellation, classify cancellation as panic, and erase either exact
  returned Driver failure at its private outcome conversion. Each viable
  mutation must make its named real lifecycle regression fail; attempted
  duplicate Driver or retirement consumption must receive its exact affine
  moved-value diagnostic. Any other compile failure, unrelated failure, or
  survivor rejects the evidence run.
- Planned verification: capture the prior contract failure; remove both toys;
  move task awaits before assertions; execute every mutation through the
  pinned shell and an immutable Nix receipt; run the focused lifecycle suite in
  debug and optimized modes, including repeated abnormal completion with exact
  count/outcome assertions; then run complete Bombay debug/release tests,
  strict Clippy, formatting, Nix evaluation/build, residue, whitespace, and the
  complete tracked/untracked checkpoint. Loom/Miri/fuzz/benchmarks are N/A for
  this single-task terminal guard and retain their owning queue rows.
- Result: delete both disconnected tests and their local `Fact`/`Mutation`
  models. The retained lifecycle tests now await task ownership transfers
  before assertions. A repeated optimized regression executes two panics and
  two cancellations against shared observation counters and proves four
  Driver destructions, four retirements, and exactly the ordered typed outcomes
  `[Panicked, Panicked, Cancelled, Cancelled]` with no double acceptance.
- Executable evidence: the pinned runner copies the repository, mutates the
  actual private Incarnation and outcome-conversion sources, and rejects
  survivors, unrelated failures, and accidental compile failures. Seven viable
  inversions are killed by the exact panic, cancellation, destruction-order,
  and typed-failure tests. Duplicate Driver execution and duplicate retirement
  are each denied with `use of moved value` for their consumed capability. The
  immutable Nix result is
  `/nix/store/50vqdfrd89mby6qdn04m60ajdjxzz2ya-bombay-workspace-0.1.0/incarnation-law-evidence.json`
  with SHA-256
  `c3e5fac683b01ea39a7f14719317d31301656d890ce7fdad5047fe35377d0c95`.
- Generated-mutant audit: the pinned cargo-mutants 27.0.0 inventory contains
  five current candidates. The two generic-output replacements and outcome
  default are unviable, retirement-callback deletion is caught, and deleting
  the terminal guard times out only under the indiscriminate sequential unit
  suite because terminal-dependent tests wait after publication disappears.
  The focused gate kills that same guard deletion immediately. The shared
  baseline now records the measured current generic signatures and viability;
  it does not misclassify the timeout as successful evidence.
- Verification: all nine focused tests pass in debug and optimized profiles.
  Complete Bombay tests pass in debug and release, including 167 unit tests,
  every integration and compile fixture, and documentation tests (only the
  pre-existing explicit performance comparison and five compile-only doc
  examples remain ignored). Strict all-target Clippy, Cargo/rustfmt, Nix
  formatting, shell syntax, JSON parsing, flake evaluation, the immutable Nix
  evidence build, whitespace, and full tracked/untracked inspection pass.
  The temporary broad cargo-mutants attempts changed no repository source.
- Actual checkpoint: six task paths; production `+0/-0/net 0`; test/gate code
  `+391/-85/net +306`; public API `+0/-0` types; Nix and mutation configuration
  `+23/-5/net +18`. The gate exceeded its test-code forecast because nine real
  source inversions require exact replacement and result validation, but no
  production, file-count, or public-surface containment threshold was reached.
  Module boundaries, runtime capability guidance, launch/termination adapters,
  and examples were audited; no caller contract or example changed.

## TEST-023 — activation visibility evidence

- State: `active`; the ordinary desired-law regressions are deliberately
  ignored only until ARC-006 changes production. They must be run explicitly
  with `--ignored` now and fail for the recorded claim-before-commit reason.
- Selected contracts: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; Address 0.2.0;
  Communication 0.1.2; Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; and Bombay-private Observe.
  Their relevant initialization, total-settlement, claim/resolve/lease,
  mailbox-admission, timer-retirement, and one-publication contracts were
  freshly inspected against the selected sources.
- Ownership: Behavior owns pure initialization actions, complete typed
  interpretations, exact settlements, and accepted/rejected/corrupt
  classification. Behavior Actors owns reusable fold/template activation, not
  runtime address registration. Address owns immediate visibility after
  `try_claim`, opaque snapshots, and generation-safe release through `Lease`.
  Communication owns prepared mailbox state and exact drain/rejection.
  Observe owns the retained terminal fact and Timers owns only actor-local
  scheduling. Engine owns the affine prepared/active port and calls
  publication only after initialization settlement custody. Bombay's
  `LocalEnvironment` alone owns the concrete commit/claim/publication
  transaction.
- Exact defect: `LocalEnvironment::activate` calls
  `AddressSpace::try_claim` before awaiting `CommitActions::commit`. Because a
  cloned Address space resolves a claim immediately, another task can obtain
  the endpoint while initialization is held pending. After rejected or
  corrupt initialization, Driver currently calls `publish` before retiring
  the active environment, so simply moving the claim below `commit` would
  still make a failed initialization transiently resolvable.
- Dependency edges: TEST-023 supplies the causal regression required by
  ARC-006. ARC-006 must make that same regression ordinary and green before
  ARC-001 may change lifecycle authority. No Behavior Actors template or
  primitive capability contract changes here.
- Smallest prior-representation regression: a private test interpreter enters
  initialization commitment, signals that exact point, and waits on a gate.
  A concurrent `AddressSpace::resolve` must observe absence while the gate is
  closed. Accepted completion must yield exactly one live claim; rejected and
  corrupt completion must never yield a resolvable endpoint and must preserve
  complete settlement and retirement custody. These are ordinary desired-law
  tests with `#[ignore]`, not `should_panic`, inverted assertions, a feature
  switch, or a meta-test. Explicit `--ignored` execution must fail the current
  representation at the first visibility assertion.
- Expected files and delta: task-local paths are
  `crates/bombay/src/local.rs`, this ledger, and `docs/todo.md`. Production is
  `+0/-0/net 0`; tests are expected below `+240/-0/net +240`; public API is
  `+0/-0` types. The existing `LocalEnvironment`, `CommitActions`,
  `AddressSpace`, `ActorRef`, `LocalResidual`, and complete Behavior settlement
  products are reused. No wrapper, runtime service, effect algebra, public
  trait, or duplicate activation state is eligible.
- Planned verification: run each ignored regression explicitly in debug and
  optimized modes to capture the same intended failure, prove the ordinary
  suite skips only these named architecture-blocked cases, inspect the exact
  typed values without mutation inside assertions, and record the complete
  tracked/untracked checkpoint. ARC-006 will remove every ignore and must make
  these unchanged desired assertions green before its production change can
  close. Loom, Miri, fuzz, benchmarks, examples, and public documentation are
  N/A for this deterministic evidence-only row; the terminal audit retains
  their independently assigned obligations.
- Prior-representation result: all three ordinary desired-law tests compiled
  without a production change. Explicit debug and optimized execution of the
  accepted test observed `AddressVisibility::Present` while the initialization
  commit gate was closed, rather than `Absent`. The rejected and corrupt tests
  each carried their complete interpretation through exact active retirement,
  then observed `AddressVisibility::Present` both while commit was gated and
  after the failed commit, rather than `Absent`. These are the intended
  claim-before-commit defects; no expected panic or inverted assertion
  accepted them.
- Evidence result: the test interpreter owns one initialization request, one
  commit-entry notification, one affine release gate, and one exact retirement
  token. The accepted case asserts pending absence, post-commit presence,
  accepted settlement, publication, lease release, empty ingress/tasks, and
  exact retirement custody. The rejection and corruption cases assert absence
  at every observation and the complete typed settlement status, continuation,
  ingress, activation tasks, descendants, and owner-cancellation custody.
  Their three `#[ignore]` attributes name ARC-006 and are the only temporary
  staging mechanism authorized for this row.
- Verification result: the complete local test module passes in debug and
  optimized profiles with 10 tests passed and four ignored: the three named
  TEST-023 regressions plus the pre-existing manual performance comparison.
  Strict Bombay library/test Clippy, workspace formatting, and diff whitespace
  checks pass. Each TEST-023 regression fails explicitly under `--ignored` in
  both profiles for the same recorded reason.
- Actual checkpoint: the test module adds 305 staged lines; production is
  `+0/-0/net 0` and public API is `+0/-0` types. The ledger and queue are the
  only documentation paths. ARC-006 is unblocked and must remove all three
  ignores while making their unchanged assertions green.

## ARC-006 — transactional activation ownership blocker

- State: `feature-complete` under the locked Address 0.3.0 reservation and Behavior
  0.19.0 child commitment contracts. The 0.17.0/0.2.0 contradiction and
  alternatives below record the earlier blocked selection, not current API.
- Current law and smallest failing regression: reserve exclusively before
  initialization action interpretation while remaining absent from logical
  resolution; commit the child privately before those effects; publish only
  after accepted continuing initialization and source settlement. The three
  existing TEST-023 ignored tests fail under the prior claim-before-commit
  order in both debug and optimized profiles. Their assertions remain the
  first end-to-end regression. Engine terminal-initialization and child-startup
  tests must then assert complete unpublished custody and exact commitment.
- Stage A expected files: `crates/bombay/src/local.rs`,
  `crates/bombay-engine/src/driver.rs`, their focused tests, this ledger and
  `docs/todo.md`. Expected production `+30/-20/net +10` lines; public types
  `+0/-0`. The existing AddressSpace becomes a Reservation then a Lease;
  the existing ActiveEnvironment publication port remains the only publish
  operation. No local address table or visibility flag is added.
- Stage B expected files: `crates/bombay/src/launch.rs`,
  `application_runtime.rs`, `child_bindings.rs`, `local.rs`, `terminal.rs`,
  local startup tests and current public guidance. Expected production
  `+185/-95/net +90` lines; public types `+0/-0` for a private closed local
  activation rejection distinguishing address refusal from abandoned private
  child binding. The existing task owner and creator-local binding table
  commit the exact child; a private two-way acknowledgement is separate from
  the existing public activation observation. Its cancellation returns
  untouched initialization actions through the existing driver residual;
  exact post-commit retirement stays in the existing `LocalOutcome` product.
  The returned child result reuses
  `CommittedChild`, `EstablishedActor`, and `InstalledActor` rather than adding
  a Bombay child contract.
- Stage A observation: the three TEST-023 causal visibility regressions now
  execute and pass in optimized builds; Address reserves before interpretation
  and only `ActiveEnvironment::publish` makes a continuing accepted endpoint
  resolvable. Engine's independent causal model and terminal-custody tests
  pass in optimized builds with early rejection/corruption/stop unpublished.
  The owner-visible test assertion was updated from post-commit visibility to
  post-publication visibility because Address 0.3.0 expressly separates those
  phases. Debug child startup now fails at its old public-readiness wait with
  `Ended`; this is the Stage B prior-representation regression.
- Stage B checkpoint (complete working tree, including inherited edits and
  untracked paths): 171 changed paths, 45 untracked. Source under crate
  `src/` directories is `+4405/-4363/net +42` lines, including embedded unit
  tests; tests, examples, and tools outside those directories are
  `+4552/-2355/net +2197`; documents are `+11836/-1821/net +10015`;
  other files are `+90/-44/net +46`. The selected child-commitment and pure
  initialization-panic changes add no new public type, but extend existing
  closed terminal and error sums. This aggregate includes the previously
  authorized dirty tree, so it is not a stage-only reduction claim. Pinned-Nix
  strict workspace Clippy and `git diff --check` pass after the exact
  reservation-rejection regression was added. Debug and optimized execution
  of that newest regression are still pending while macOS holds the freshly
  linked test binary before its test entry point.
- Final nested-creation evidence stage, before test edit: the remaining
  ARC-006 progress witness is one root that creates a child whose own
  initialization creates a grandchild, then receives its typed child report
  before root retirement and retains exact three-generation custody. The
  prior claim-before-commit representation already fails the three TEST-023
  visibility regressions; this witness checks that private child binding does
  not stall later creation. Expected file:
  `crates/bombay/tests/application_terminal_custody.rs`; production
  `+0/-0/net 0`; public types `+0/-0`. It reuses the existing App, actor
  spaces, Behavior creation products, and terminal projection. No new runtime
  abstraction or policy is introduced.
- An initial test probe polled extra App actor spaces for child visibility.
  That was an invalid observation: creator-local `NestedBindings` owns those
  child protocol spaces, and App's additional spaces are independent. The
  probe saw empty unrelated spaces while exact child and grandchild tasks
  existed. It was replaced with a typed `ReportToParent` emitted after the
  grandchild creation lane was interpreted; the root receives that report and
  stops. The resulting end-to-end witness passes in debug and optimized
  builds. Its terminal trace retains the root's two accepted settlements,
  child's accepted creation/report settlement, exact descendant roles, and
  grandchild cancellation. Grandchild initialization may race with parent
  retirement, so its terminal retains either no settlement or one accepted
  initialization settlement; neither alternative invents a publication.
- `LocalActivationRejection<A>` candidate answers the abstraction budget:
  it owns the one exact reason a prepared host could not commit; transforms
  Address refusal and a dropped private binding acknowledgement into distinct
  Driver activation failures; `ClaimError<A>` cannot truthfully encode binding
  abandonment; it removes the old ClaimError-only assumption and unreachable
  prepublication classification; a child whose parent drops the binding
  acknowledgement is the concrete use. It remains private; the existing
  `ActorRetirement` terminal sum receives an exact `BindingAbandoned` variant
  for that path. This is a closed error sum, not a wrapper around another
  architecture.
- Desired law: initialization interpretation must settle before an endpoint is
  claim-visible; rejected or corrupt interpretation must never claim; accepted
  interpretation must claim before activation publication; and every failure
  must retain its exact factual custody.
- Locked-owner contradiction: Behavior 0.17.0 defines
  `ChildCreationOutcome::HostRejected` as the current routed child plus complete
  initialization `Actions` that were never interpreted. Its documentation
  explicitly forbids reconstructing a pre-initialization value or discarding
  an affine action. Address 0.2.0 exposes only immediate `try_claim`, whose
  success is immediately resolvable, and an affine `Lease`; it has no hidden
  reservation or atomic promotion contract.
- Why the obvious reorder is unlawful: moving `CommitActions::commit` before
  `AddressSpace::try_claim` consumes the initialization `Actions`. If the later
  claim returns `AddressInUse` or `RegistrationIdsExhausted`, Bombay has the
  current child and complete `ActionSettlement`, but Behavior's only
  host-rejection product requires the now-consumed uncommitted `Actions`.
  Returning, cloning, rebuilding, or relabeling those actions would falsify
  authoritative custody. Dropping the settlement would erase a factual
  committed prefix.
- Alternatives rejected: claim-in-`publish` has the same post-commit custody
  gap; `resolve` preflight is racy; treating claim failure as unreachable
  weakens Address's typed error; a Bombay pending-address enum, second table,
  gated `ActorRef`, or admission flag duplicates Address/Communication state;
  reporting the child creation corrupt says an already-interpreted item was
  untouched and loses the settlement.
- Required upstream ownership: Behavior may add one post-commit child
  establishment rejection that owns the current child, exact
  `ActionSettlement`, and claim reason; alternatively Address may add an affine
  non-resolvable reservation whose rejection occurs before interpretation and
  whose promotion atomically installs the endpoint afterward. The locked
  releases own neither contract, so Bombay cannot locally implement ARC-006
  without violating the Behavior-first and authoritative-fact gates.
- Checkpoint: production `+0/-0/net 0`; architecture tests
  `+0/-0/net 0` beyond completed TEST-023; public API `+0/-0` types. The item
  remains nonterminal and must be resumed when one exact upstream contract is
  selected and locked.

## ARC-007 — Entity and Behavior Actors ownership differential

- State: `active`; selected after ARC-006 reached its exact locked-owner
  blocker. This audit uses Behavior Core and Behavior Actors 0.17.0 and
  Behavior Macros 0.12.0 from owner revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`, Address 0.2.0,
  Communication 0.1.2, Bombay-private Observe, and Timers revision
  `13e884da7ab41781f52337b0038060e375b00ee0`.
- Exact defect: the current documents say both that Behavior Actors owns
  reusable actor lifecycle policy and that Bombay Entity owns lifecycle,
  without separating actor-child lifecycle from stable logical routing. That
  ambiguity can cause `StableProxy`, `DynamicSupervisor`, `Registry`,
  `Resolver`, or Behavior Actors `Machine` to be copied into Entity, or cause
  Entity's asynchronous admission transaction to be deleted as if it were
  generic supervision.
- Dependency edges: this item is independent of ARC-006. It blocks ARC-003's
  single transition-classification owner, ARC-004's `bombay-machine`
  decision, ARC-014's task-owner consolidation, ARC-008's child-binding
  state, ARC-017's pinning audit, and every retained Entity redesign. Those
  items remain blocked until this ownership map is terminal.
- Governing differential: a Behavior Actors template is itself one
  deterministic `Behavior -> Actions` fold. Entity is a Bombay runtime
  transaction outside that fold: a stable domain key accepts and retains
  caller-owned commands while domain hydration and actor launch are pending,
  then reserves exact-incarnation delivery and fences accepted delivery before
  exercising an affine runtime lease. No selected Behavior Actors protocol
  accepts a domain command for an absent key and returns it through one shared
  on-demand activation attempt.
- Locked-owner table before implementation:

| Law | `EntitySlot` / `EntityRuntime` | `StableProxy` | `DynamicSupervisor` | `Registry` / `Resolver` / `Machine` | Owner conclusion |
|---|---|---|---|---|---|
| Stable identity and routing | `EntityId<I>` indexes a Bombay directory even while no actor exists; `EntityRef<D>` admits the domain command | one already-running proxy actor is the service recipient | manages retained keyed proxy children; its public protocol is start/replace/stop/query/cancel, not keyed service delivery | discovery maps keys to typed recipients; `Machine` has no name or routing law | Bombay Entity owns absent-to-live stable routing; discovery remains upstream and is not copied |
| First activation | first command is retained and emits `StartActivation` for a fresh `ActivationId` | owner `Start` emits a worker child creation and later initialization/activation requests | `Start` creates and controls a `StableProxy` entry | registry bind/lookup and machine receive/become do not launch runtime incarnations | Entity owns runtime activation; Behavior Actors owns all actor-child creation and readiness after hydration |
| Concurrent first admissions | later commands join one bounded waiter sequence; overflow returns the exact command as `Busy` | a second initial worker submission is returned as `InitialWorkerOutcome::Overlap` | duplicate start is rejected as `AlreadyExists`; no caller domain commands are coalesced | no shared activation attempt | traces differ; Entity waiter admission is retained |
| Hydration failure | one typed runtime failure rejects every waiter in order as `Unavailable`, removes the slot, and reports the authoritative hydration error once | worker initialization failure occurs only after child creation and retains a `ProxyDrain` | reports worker/proxy creation, initialization, or activation failure with exact worker custody | none reconstruct domain behavior state | Bombay `EntityDefinition::hydrate` and its one-to-many admission settlement are residual Entity law |
| Replacement | after passivation and exact removal, a later domain command claims a fresh activation; there is no in-place replacement command | explicit replace drains the predecessor and stages one successor, retaining both worker attempts and shutdown facts | explicit keyed replace owns operation/cancellation authority | registry rebind is naming only; machine phase change is local state only | Behavior Actors owns actor replacement; Entity owns only command-triggered reactivation after retirement |
| Stale asynchronous facts | `ActivationId` prevents an old activation, delivery, fence, or termination fact from changing the current slot; a stale successful activation still retires its affine lease | worker attempts and initialization/activation correlations classify foreign child facts and emit typed diagnostics | key generation and operation IDs classify stale management facts | stale registry unbind protects recipient binding; machine has no correlation law | each owner retains its own identity domain; Entity must not translate actor correlations into `ActivationId` |
| Passivation | atomically closes stable-key admission, waits for reserved deliveries, enqueues a user-lane fence, then transfers the exact lease for retirement | owner shutdown/replace requests child shutdown and waits for child stop plus shutdown settlement; no mailbox fence | stop/global shutdown drains proxy entries under selected actor-drain policy | none owns runtime delivery reservations or leases | Entity owns the delivery fence; Behavior Actors owns child shutdown policy |
| Shutdown | family admission closes, installed lifecycle tasks settle, every represented slot drains, then the family task group closes | one proxy closes owner/service admission and settles one current worker | global supervisor shutdown drains its retained keyed entries | registry/resolver never stop by policy; machine stops only when its domain function returns `Move::Stop` | Bombay owns application/family ordering; templates retain their nested actor shutdown laws |
| Terminal custody | `Retire` transfers one affine incarnation lease and mode; only matching `Terminated` emits `Remove`; definition policy receives the complete actor retirement | retains worker submission, initialization, activation, shutdown resolution, and `ChildStopped` in exact outcome/drain products | emits exact lifecycle/diagnostic products and `EntryRetired` cause | discovery retains recipients only; machine error retains the exact rejected user event and cause | do not duplicate any upstream child terminal product; Entity retains only directory lease/removal and application retirement delivery |

- Upstream reuse/deletion gate: Entity's native actor launch already composes
  the exact authored Behavior, including any selected Behavior Actors wrappers;
  it does not reproduce worker initialization, replacement, keyed supervision,
  discovery, or the reusable Behavior Actors machine. No current `EntitySlot`
  event or effect is trace-identical to those upstream protocols, so ARC-007
  expects no production deletion. The separate Bombay lifecycle topology and
  generic executor layers are not excused by this conclusion: ARC-003 and
  ARC-004 must decide whether their classification machinery is redundant with
  the one retained Entity reducer.
- Regression and prior representation: add an executable pure-fold
  differential to the existing Entity lifecycle test module. It must drive
  complete Entity activation/failure/reactivation/stale/passivation/termination
  traces and a real locked `StableProxy` concurrent-start trace. The
  pre-change documentation has no executable proof tying these distinct
  outputs to their owners, so the new test must first fail against a source
  mutation that substitutes proxy overlap for Entity waiter retention and
  against a mutation that skips Entity's fence before retirement.
- Expected files and delta: `crates/bombay/src/entity/lifecycle/mod.rs`,
  `docs/module-boundaries.md`, `docs/runtime-capability-interfaces.md`,
  `examples/entity/src/main.rs`, this ledger, and `docs/todo.md`. Production is
  `+0/-0/net 0`; tests are expected below `+240/-0/net +240`; public API is
  `+0/-0` types. The test reuses `EntitySlot`, its typed events/effects, and
  the locked `StableProxy` public fold without adapters, translation enums, or
  copied transition tables.
- Public API delta: none. No new wrapper, actor, lifecycle trait, state type,
  registry, effect algebra, or public spelling is eligible.
- Planned verification: capture both intended mutation failures, run the
  focused differential in debug and optimized profiles, then run the Bombay
  library/integration tests, strict Clippy, formatting, and diff checks through
  pinned Nix. Loom, Miri, fuzz, and benchmarks are N/A for this ownership-only
  item; their dedicated queue rows and terminal gates remain authoritative.
- Result: `EntitySlot` retains only the Bombay-specific stable-routing
  transaction. The locked `StableProxy` fold was executed beside it without an
  adapter: a second Entity command remained in the shared activation waiter
  set and was later returned in ordered hydration-failure custody, while a
  second proxy worker start immediately returned
  `InitialWorkerOutcome::Overlap`. The complete Entity trace additionally
  proves fresh reactivation after removal, stale successful-activation lease
  retirement, stale fence/termination rejection, the ordered fence, graceful
  lease transfer, and exact removal.
- Prior-representation mutation evidence: changing the production activating
  fold to reject the second command as `Busy` failed
  `entity_first_demand_is_not_stable_proxy_worker_start` at the required empty
  effect/retained-waiter assertion. Changing the production drained path to
  retire immediately without `EnqueueFence` failed
  `entity_trace_owns_hydration_waiters_fence_and_runtime_generation` at the
  required `Draining` state. Neither failure used an expected panic, inverted
  assertion, duplicate model branch, or ignored test; both mutations were
  restored before verification.
- Deletion result: no Entity production event/effect was trace-identical to an
  upstream actor-template contract, so deleting production here would erase
  Bombay-only law. Three superseded narrow unit tests for hydration-failure
  order, stale activation, and stale termination were deleted after the new
  complete trace subsumed their assertions. The real duplicate local
  transition-classification and executor wrappers remain explicitly assigned
  to newly unblocked ARC-003 and ARC-004; this item is feature-complete, not
  distilled.
- Documentation/example result: `module-boundaries.md` and
  `runtime-capability-interfaces.md` now state the exact split, and the Entity
  example distinguishes command-triggered reactivation from explicit
  `StableProxy` replacement.
- Verification result: the two focused folds pass in debug and optimized
  profiles; all 13 retained Entity library tests passed in both profiles;
  the 15 selected Entity runtime/family/application integrations passed in
  both profiles; the complete Bombay package passed with 166 library tests,
  four deliberate TEST-023/manual ignores, all integration/compile fixtures,
  five Entity Loom tests, and doctests; the Entity example ran; strict Bombay
  library/test Clippy, workspace formatting, and diff whitespace checks pass.
- Actual checkpoint: production `+0/-0/net 0`; tests `+225/-43/net +182`;
  public API `+0/-0` types. Six task-local files changed: the lifecycle test
  module, three current documents, the Entity example comment, and the queue.
  No wrapper, trait, alias, actor, state product, or effect language was added.

## TEST-008 — locked actor-template boundary inventory

- State: `active`; selected as the lowest-sequence ready P1 row after
  ARC-007. It unblocks ARC-010 and does not broaden any Engine law.
- Exact defect: `driver-template-manifest.json` schema 2 contains only owner
  metadata and an empty `mirrored_templates` array. The corresponding Engine
  test accepts that empty representation and checks only two obsolete test
  paths. It therefore cannot detect an omitted locked template, an omitted
  typed capability/source-action lane, or a newly copied actor-template test
  in Engine.
- Locked dependency evidence: Cargo.lock selects `bombay-behavior` and
  `bombay-behavior-actors` 0.17.0 plus `bombay-behavior-macros` 0.12.0 from
  revision `435560ce7bea8ad3330ee2d42e5034f837a80602`. The complete selected
  Behavior `AGENTS.md` remains the repository-wide design contract. The
  Actors source exports 45 concrete Behavior compositions: five atomic
  aggregates, three fundamental compositions, five discovery templates, nine
  lifecycle compositions, three operations templates, one persistence
  template, eleven routing templates, five time templates, and three workflow
  templates. `ChildShutdownPlan` is included because the public
  `shutdown_after_children` builder returns it even though its concrete type is
  not re-exported at the crate root.
- Ownership and dependency edges: Behavior owns `Actions`, structural send
  products, `InterpreterRequest`, `SourceAction`, births, settlements, and
  source custody. Behavior Actors owns all 45 template folds, event sums,
  named request products, wrapper order, and the request/source-action types
  they emit. Engine owns only universal causal execution and must not depend
  on or reproduce Behavior Actors. Bombay owns the concrete interpreters. The
  inventory must expose ARC-010's interpreter gap without implementing it in
  this test item.
- Exact capability surface to catalogue: actor-owned request types are
  `ScheduleAt`, `ScheduleAfter`, `ObservePeer`, `ObserveChild`,
  `ObserveCreation`, `ObserveEstablishedCreation`, `ObserveEstablished`,
  `CancelObservation`, `ShutdownChild`, `ShutdownEstablished`,
  `ReportTerminalOutcome`, `ReportShutdownPlan`, `DiagnosticAction`,
  `CustomerDelivery`, `InitializeWorker`, and `BeginActivation`. Actor-owned
  source actions are `AssignWorker`, `PrepareWorkers`, and `ProxyOperation`;
  `ScheduleAt` and `ScheduleAfter` also implement the Behavior-owned
  `SourceAction` contract. Template lanes additionally use Behavior-owned
  delivery, established delivery, child creation, and `ReportToParent`
  primitives. Bombay currently interprets the public lifecycle/time requests
  and generic delivery/birth products; the locked atomic path exposes the
  unresolved `PrepareWorkers` application capability assigned to ARC-010.
- Smallest prior-representation regression: replace the permissive emptiness
  assertion with a schema-3 contract containing an independent exact set of
  all 45 template IDs and all actor-owned capability IDs. Require one source,
  event boundary, ordered lane description, composition edge, and owning test
  reference per template; require source, contract kind, emitters, and Bombay
  interpretation status per capability. Compare exact sets so deleting an
  entry fails, reject a non-empty `mirrored_templates` list, and scan all
  Engine Rust sources/tests except the manifest gate itself plus its Cargo
  manifest for an Actors dependency/import. Added alone, this regression must
  fail the current schema-2 representation because both inventories are
  absent—not through `should_panic`, inverted assertions, or an ignored test.
- Prior-representation result: with only the new schema contract test present,
  `nix develop -c cargo test --locked -p bombay-engine --test law_manifest
  engine_does_not_mirror_actor_template_laws -- --exact --nocapture` exited
  101. The test itself compiled and ran, then reported schema 2 and the exact
  missing sets of 45 template IDs and 19 capability IDs. No manifest,
  production code, expected panic, inverted assertion, or ignored test was
  used to manufacture the failure.
- Expected files and delta: this ledger, `docs/todo.md`,
  `docs/driver-template-manifest.json`,
  `crates/bombay-engine/tests/law_manifest.rs`, and
  `docs/driver-test-strategy.md` (at most five files). Production
  `+0/-0/net 0`; test/gate code below `+180/-30/net +150`; public API
  `+0/-0` types. No wrapper, trait, alias, Behavior, effect algebra, or
  interpreter is permitted in TEST-008.
- Planned proof and gates: capture the focused test failing against schema 2;
  populate only the audited schema-3 boundary data; mutate one template away
  and add one mirrored-template entry to prove both failures; then run the
  focused manifest test in debug and optimized profiles, the complete Engine
  manifest suite, complete Engine tests in debug and release, strict Engine
  Clippy, workspace formatting, diff whitespace checks, and the full tracked
  and untracked checkpoint.

## BEH3 — exact atomic source-action rejection return

- State: `feature-complete` upstream. The selected immutable Behavior Actors
  0.19.0 release supplies `AssignWorker::settle` and
  `ProxyOperation::settle`, each returning the exact original source action on
  lower-capability rejection. Bombay's consumers remain ARC-010 work.
- Owner: Behavior Actors alone owns the representation and reconstruction of
  `AssignWorker` and `ProxyOperation`. Bombay owns the send and child-input
  interpretation, including exact rejected-payload recovery.
- Exact blocker: `AssignWorker::into_parts` transfers the affine assignment
  and receipt to Bombay for a real delivery, but `AssignWorker::returned` is
  `pub(in crate::atomic)`. If Communication rejects the assignment payload,
  Bombay receives that exact payload back but cannot reconstruct the required
  `ItemSettlement::Rejected { item: AssignWorker, reason }`. The job is not
  generally `Clone`, and rejecting before attempted delivery would falsify
  the runtime fact. `ProxyOperation::into_parts` likewise transfers its affine
  control and operation ID, with no public reconstruction path after a child
  input rejection. No cast, guessed field, new Bombay action, or erased route
  can restore those owner-private values lawfully.
- Required upstream result: satisfied in the selected 0.19.0 release by the
  two exact settling methods. Its focused owner tests cover accepted and
  rejected paths; Bombay differential evidence is assigned to ARC-010.
- Blocked by: none.
- Unblocks: ARC-010.

## TEST-013 — assertion-independent required operations

### 2026-10-01 selected-contract audit and first change ledger

- State: `active`. ARC-011, ARC-015, and ARC-019 are
  `feature-complete`; TEST-013 unblocks ARC-020. `Cargo.lock` selects
  Behavior Core/Actors 0.20.0 from `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`,
  Macros 0.13.0, Address 0.3.0, Communication 0.1.2, Bombay-private Observe,
  and only Timers Git patch `13e884d`. The selected Behavior `AGENTS.md` was
  read. Current Behavior `Behavior`/`Actions` and Actors activation source and
  tests, Address allocation/reservation, Communication mailbox ownership,
  Observe future publication, Timers `TimerQueue::pop_due`, Bombay Entity
  passivation, local/Driver tests and examples, and current Driver law,
  strategy, capability, and module documents were inspected. These calls
  already have owning typed results; there is no upstream dependency blocker
  or new protocol consumer.
- Exact first blocker: the current repository executes required Address
  allocations, Timer pops, Observe waits, Entity passivations, termination
  awaits, an Engine timeout, and a law-manifest insertion inside `assert!` or
  `assert_eq!`. Required state changes and awaits must occur before any
  assertion observes their returned values. The smallest structural
  regression is a macro-body scan over current Rust sources for those exact
  owner operations; it must fail on the prior representation in both debug
  and optimized-independent source form, then pass after the edits. Focused
  owner tests and executable examples preserve each returned typed result.
- Ownership: Address owns allocation sequences, Timers owns generation-safe
  removal, Observe owns event/wait completion, Entity owns passivation phases,
  Engine owns Driver progress, and Bombay test/example callers own the
  sequence of these operations and their observations. No Behavior fold,
  effect interpreter, or runtime contract changes in TEST-013. A local
  `let` binding before the assertion expresses every required operation;
  no helper, wrapper, trait, macro, or new public type is eligible.
- First-stage change ledger before edit: expected paths are
  `crates/bombay/src/address.rs`, `time.rs`, `observation.rs`, `local.rs`,
  `observe/tests.rs`, `crates/bombay-engine/tests/driver_law.rs` and
  `law_manifest.rs`, `crates/bombay/tests/actor_interface.rs`,
  `entity_runtime.rs`, `entity_application.rs`, `run_with.rs`,
  `application_terminal_custody.rs`, and the `counter`, `actor-templates`,
  `application-topology`, and `entity` public examples, plus this ledger and
  `docs/todo.md`. Forecast production `+0/-0/net 0`, test/example source
  `+70/-35/net +35`, public API `+0/-0` types. The in-file tests physically
  change crate `src/` but add no production behavior. Reuse existing typed
  result values and test expectations; delete assertion-owned execution.
  Keep pure transformations and conversion ownership for a second explicit
  residue stage after this first gate. Run focused owner suites in debug and
  optimized builds, executable examples, strict Clippy, formatting, and the
  complete tracked/untracked checkpoint.

### 2026-10-01 first-stage checkpoint and second-stage change ledger

- The prior representation failed the balanced assertion-body scan with 31
  required operations; the first-stage representation passes with zero. The
  four changed public examples run in debug and release. Bombay's 190 library
  tests and five owning integration suites, plus Engine's Driver and manifest
  suites, pass in both profiles. Strict workspace Clippy, format check, and
  `git diff --check` pass.
- Complete working-tree checkpoint including untracked files: 139 tracked plus
  60 untracked paths, 199 total. Physical crate source including inline tests
  `+5751/-4798/net +953`; standalone tests/examples/scripts
  `+6847/-2441/net +4406`; docs `+15690/-1860/net +13830`; other
  `+255/-146/net +109`. This is cumulative inherited task scope, not the
  TEST-013 increment. TEST-013 adds zero public types.
- Residue blocker: pure `Actions::map_sends` and consuming `into_*` calls still
  occur inside assertions. These operations transfer ownership and must be
  evaluated before the assertion. Some custom assertion helpers also receive
  awaited delivery calls; inspect whether they consume authoritative facts.
  The exact second-stage regression is a balanced macro-body scan for
  `map_sends` and consuming `into_*` in assertions, plus a call-site audit of
  custom assertion helpers that receive `.await`. It fails on the present
  representation and must pass after edits.
- Second-stage change ledger before edit: expect
  `crates/bombay/tests/{macro_last_authoring,activation_authoring,actor_interface,entity_application}.rs`,
  `crates/bombay/src/{launch,local,entity/mod}.rs`, and possibly additional
  test callers revealed by the complete scan, plus this ledger and queue.
  Forecast production `+0/-0/net 0`; test source `+30/-15/net +15`; public
  API `+0/-0` types. Reuse the existing concrete action and result types;
  introduce no wrappers, helpers, traits, or macros.

### 2026-10-01 resolution checkpoint

- TEST-013 is `verified`. The second-stage residue scan previously found 12
  `map_sends` transfers and four `into_*` transfers inside assertions; it now
  finds zero. The balanced multiline scan finds no awaited, mutating, or
  consuming operation in an assertion. Its remaining `.join` hits are pure
  path/string construction, and its `.await` hit is a quoted source string.
  The requested `rg` residue command finds only identifiers or quoted strings.
  An ignored Observe snippet now binds the awaited observation, and the Entity
  application assertion helpers receive already settled send results. Existing
  lock/Observe lookups in comparisons are observational reads.
- Debug and optimized Bombay library (190 tests), both authoring suites,
  actor interface, Entity application, and application terminal custody all
  pass. First-stage owning tests and four executable examples passed in both
  profiles. Strict workspace all-target Clippy, formatting check, and
  `git diff --check` pass. No public API or production behavior changed.
- Final complete working-tree checkpoint including untracked files: 139
  tracked plus 60 untracked paths, 199 total. Physical crate source including
  inline tests `+5761/-4803/net +958`; standalone tests/examples/scripts
  `+6873/-2455/net +4418`; docs `+15720/-1860/net +13860`; other
  `+255/-146/net +109`. TEST-013 adds zero public types. The next queue item
  is TEST-014; ARC-020 retains its listed prerequisites.

## ARC-018 — typed root and child origin provenance

### 2026-10-01 selected-contract audit and change ledger

- State: `active`. ARC-011 is `feature-complete` and unblocks this item;
  ARC-018 unblocks ARC-016. `Cargo.lock` selects Behavior Core/Actors 0.20.0
  at `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0,
  Address 0.3.0, Communication 0.1.2, Bombay-private Observe, and only the
  Timers patch `13e884d`. The selected Behavior `AGENTS.md`, Behavior
  `ChildRole`/`ChildPosition` and Actors child-topology source/tests,
  Address/Communication/Observe/Timers contracts, Bombay terminal and launch
  paths, macro expansion and compile fixtures, application tests/examples,
  capability/module guidance, and Driver law/strategy were inspected. No
  upstream version, protocol consumer, or ownership edge is unverified.
- Blocker: `ActorOrigin<Owner,Role>` contains `Option<u64>` and both `root`
  and `child` constructors exist for every role. `ActorOrigin<Owner,Here>`
  can therefore hold a child nonce and `into_declared_root` copies it; the
  inverse mismatch is likewise constructible. The smallest prior-regression
  is a unit test that constructs a child origin at `Here`, projects it as a
  root, and requires no child nonce; it fails on the prior representation.
  Compile-fail fixtures must deny both conversion directions after the fix.
  Runtime projection tests must still preserve address, child nonce, and
  exact retirement payload.
- Model the two genuine domain products as `RootOrigin<Owner>` with address
  and `ChildOrigin<Owner,Role>` with address and required nonce. The root
  has no nonce accessor; the child's nonce returns `u64`. `ProjectTerminal`
  remains the owning static lift and `ChildRole<Owner>` remains the static
  structural-to-declared role proof. `#[derive(TerminalProjection)]` remains
  syntax only; ARC-016 separately audits its type-name parsing.
- Alternatives: a closed enum inside the existing generic
  `ActorOrigin<Owner,Role>` still permits a root variant under a child role;
  a third defaulted generic provenance parameter cannot select root versus
  child from `Role` on stable Rust without an overlapping default/role law;
  a runtime check weakens static denial. Two nominal concrete products express
  exactly the two states and delete optional nonce handling. Each owns one
  distinct provenance and no extra runtime state or policy.
- Change ledger before production edit: expected terminal model,
  application/launch projection, macro derive, public reexports, compile
  fixtures and affected tests/examples, plus current docs/ledger. Forecast
  more than 15 changed paths across this already approved task surface,
  production `+80/-90/net -10`, tests/examples `+120/-120/net 0`, public API
  `+2/-1` types. The user previously explicitly authorized the expanded
  surface checkpoint in this tree. Reuse `ProjectTerminal`, `ChildRole`,
  `OwnedTask`, and the exact existing retirement sum; delete optional nonce
  and wrong-provenance constructors. No extra actor, wrapper, policy, or
  effect interpreter is eligible. Run compile-pass/fail and exact projection
  tests in debug/release before broader gates.

### 2026-10-01 resolution checkpoint

- ARC-018 is `feature-complete`; final project-wide distillation remains.
  The prior-representation command
  `nix develop -c cargo test --locked -p bombay-rs --lib a_child_origin_cannot_be_projected_as_the_root`
  failed its intended assertion: the projected root retained `Some(13)`.
  The corrected model has separate `RootOrigin<Owner>` and
  `ChildOrigin<Owner, Role>` public products, with no optional nonce or
  root-to-child/child-to-root conversion. The two new compile-fail fixtures
  reject both conversions for missing methods, and the existing wrong-role
  fixture still rejects an incorrect `ChildRole::Position` equality. The
  private projection path and macro derive preserve the exact address, child
  nonce, retirement payload, and structural role proof; the renamed
  downstream crate checks expansion through an aliased Bombay dependency.
- Bombay's 192 library tests and eight affected integration suites pass in
  both debug and optimized builds. Full locked debug workspace tests, six
  executable public examples, full locked workspace build, strict all-target
  Clippy, rustfmt check, and `git diff --check` pass. The current capability
  and module documents describe the split types. Historical 0.17 research
  probes are marked frozen; current tests and examples use the new contract.
- Complete working-tree checkpoint including untracked paths: 144 tracked
  plus 64 untracked, 208 total. Physical crate source including inline tests
  `+6019/-5051/net +968`; standalone tests/examples/scripts
  `+6938/-2501/net +4437`; docs `+16001/-1865/net +14136`; other
  `+273/-154/net +119`. Relative to the pre-ARC-018 checkpoint, physical
  crate source net grew 41 lines; this is new provenance capability code,
  not code reduction. Public API `+2/-1` types. ARC-016 is newly ready and
  selected next; TEST-017 still awaits ARC-016.

## ARC-013 — actor-local timer and fact custody

### 2026-10-01 selected-contract audit and first change ledger

- State: `active`. ARC-009 is retained and unblocks this item; ARC-013
  unblocks ARC-020. `Cargo.lock` selects Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0,
  Address 0.3.0, Communication 0.1.2, Bombay-private Observe, and only the
  Timers patch `13e884d`. The selected Behavior `AGENTS.md`, current Behavior
  action/settlement and Actors timer/observation request APIs and tests,
  Address/Communication/Observe/Timers source and documentation, Bombay
  `LocalEnvironment`, `ActionInterpreter`, application capability source,
  current capability/module contracts, and Driver law/strategy were inspected.
  No upstream contract or consumer blocker remains.
- Current schedule: the Driver serializes `activate`, `next`, `apply`, and
  `offer_next` on one `ActiveLocalEnvironment`. `next` polls `FactQueue` and
  reads `LocalTimers`; the interpreter schedules timers and registers facts
  only while a complete action is committed. The queue's Observation future
  can wake from another task/thread, but that wake does not mutate the
  `FactQueue` vector. `LocalTimers` and `FactQueue` are cloned only to connect
  Environment and interpreter capability views. No concurrent queue mutator
  has been found. Retaining their `Arc<Mutex<_>>` needs separate proof; the
  ownership rewrite follows the ordering law in a separate stage.
- First blocker: `FactQueue::next` scans registrations in order but
  `swap_remove`s the first ready fact. With three simultaneously ready facts,
  the observed order is first, third, second. Select registration order among
  facts ready at the same poll as Bombay's deliberate policy; it is not a
  foundational actor-model law. Cancellation of an awaiting `next` future
  must not consume registered facts. A test will first reproduce this exact
  prior failure with three independently published facts, a cancelled waiter,
  and exact typed event order, then pass after the smallest queue edit.
- First-stage change ledger before edit: `crates/bombay/src/observation.rs`,
  this ledger, and `docs/todo.md`. Forecast production `+1/-1/net 0`,
  in-file tests `+35/-0/net +35`, public API `+0/-0` types. Reuse existing
  `PendingFact`, `Observation`, and `Vec`; replace unordered removal with
  order-preserving removal. No new abstraction, actor policy, or runtime
  capability. Run focused debug/release regression and inverse mutation
  before the separate synchronization custody analysis.

### 2026-10-01 ordering checkpoint and second change ledger

- The new exact registration-order and cancelled-wait regression failed on
  the prior representation: observed peer addresses 1, 3, 2 against 1, 2, 3.
  Replacing `swap_remove` with `remove` passes all five observation tests in
  debug and release. Formatting and diff checks pass. Complete working-tree
  checkpoint including untracked files: 139 tracked plus 60 untracked paths,
  199 total; physical crate source including inline tests
  `+5795/-4805/net +990`, standalone tests/examples/scripts
  `+6873/-2455/net +4418`, docs `+15855/-1860/net +13995`, other
  `+255/-146/net +109`. No public type added.
- Second blocker: both `LocalTimers` and `FactQueue` hold `Arc<Mutex<_>>`
  solely because `LocalEnvironment` and its `ActionInterpreter` retain
  duplicate views. The causal Driver calls action interpretation only between
  `next` calls. Wakes from other tasks touch the Observe slot's own
  synchronization, not queue state. The interpreter's fact and timer state can
  therefore be the sole owner; Environment should ask that owner for the
  next local fact/deadline rather than retaining a clone. Existing tests will
  compile against the refactored owner and preserve traces. A source
  invariant must find no `Arc<Mutex<TimerQueue...>>`, no
  `Arc<Mutex<FactState...>>`, and no duplicate queue construction in
  `LocalEnvironment::prepare`.
- Alternatives: `Rc<RefCell<_>>` merely replaces a lock with runtime borrow
  checks and retains split ownership; a shared channel invents a timer/fact
  service; passing borrowed queues into every Behavior `InterpretItem` would
  forward all static capability implementations through a new structural
  product. Give the existing application capability product sole ownership
  and have the already private `CommitActions` port expose local acquisition.
  This deletes duplicate Environment fields and clone paths, without adding
  a public type or external runtime policy.
- Second-stage change ledger before production edit: expected
  `crates/bombay/src/{time,observation,local,interpret,application_runtime}.rs`,
  potentially owning tests in `launch.rs`, and current capability/module
  documentation. Forecast production `+90/-115/net -25`, tests
  `+25/-10/net +15`, public API `+0/-0` types. Reuse `TimerQueue`,
  `FactQueue`, `ApplicationCapabilities`, `ActionInterpreter`, and the Driver's
  existing one-event port. No wrapper/builder/macro/new public product.
  Run focused debug/release timer, observation, local/application traces,
  then strict workspace gates.

### 2026-10-01 resolution checkpoint

- ARC-013 is `feature-complete`; final project-wide minimization remains.
  `FactQueue` now preserves registration order among simultaneously ready
  observations and survives cancellation of a polled waiter. The prior
  representation failed that exact test with 1, 3, 2; current debug and
  optimized observation suites pass. Existing timer replacement and peer
  multiplicity tests also pass.
- `ApplicationCapabilities` now owns the sole `LocalTimers` and `FactQueue`
  values. `LocalEnvironment` holds neither. Its private `CommitActions` port
  queries the same interpreter for deadline and local events; the nested
  selection preserves the original priority of facts before activation-task
  events. The peer-observation forwarding wrapper was deleted; the existing
  address space and queue are borrowed directly. A source scan finds no
  `Arc<Mutex<TimerQueue...>>`, `Arc<Mutex<FactState...>>`, queue clone, or
  remaining `LocalPeerObservations`. No public type was added.
- Full locked debug workspace tests passed. The Bombay 191 library tests and
  five application/template suites passed in optimized build; the same
  focused suites passed debug. Strict all-target workspace Clippy, rustfmt
  check, and `git diff --check` passed. Current capability and module guidance
  describe the single owner. Complete working-tree checkpoint including
  untracked files: 139 tracked plus 60 untracked paths, 199 total; physical
  crate source including inline tests `+5905/-4978/net +927`, standalone
  tests/examples/scripts `+6873/-2455/net +4418`, docs
  `+15903/-1865/net +14038`, other `+255/-146/net +109`. Relative to the
  pre-ARC-013 checkpoint, physical crate source net decreased 31 lines
  despite the new regression. Public API `+0/-0` types. ARC-020 remains the
  final distillation owner.

## TEST-014 — exact fact and generated-identity custody

### 2026-10-01 selected-contract audit and change ledger

- State: `active`. ARC-011 and ARC-015 are `feature-complete`; TEST-014
  unblocks ARC-020. `Cargo.lock` selects Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0,
  Address 0.3.0, Communication 0.1.2, Bombay-private Observe, and only the
  Timers Git patch `13e884d`. The selected Behavior `AGENTS.md`, current
  Behavior action/source/creation products and Actors policy source/tests,
  Bombay run-with, authoring, Observe, Address/Communication/Timers capability
  documentation, Driver law/strategy, and module ownership were inspected.
  No upstream contract or consumer is blocked.
- The original audit's three discarded `run_with` termination results are
  already bound and compared with exact `Exit` variants; the Driver creation
  test now captures both generated IDs from `CreationSequence::issue`, and
  compares the resulting `CreateChild` IDs, replacement predecessor, exact
  result event, and behavior state without predicted numeric identities.
  The authoring suites compare complete typed `Actions` against expected
  products. Current live-source `let _ =` scan finds only an intentional
  Observe reentrant-wake drop and a Loom-only unused timeout; compile-fail
  fixtures intentionally exercise statically denied expressions. No
  authoritative `Actions`, termination, or generated identity is discarded.
- Smallest remaining blocker: the Observe reentrant-wake test discards the
  taken observation through `let _ =`, hiding that its drop is the actual
  test stimulus. The prior representation fails an exact source residue scan
  for discarded observation values. Express the disposal with `drop` and
  preserve the reentrant wake trace. No production behavior changes.
- Change ledger before edit: expected path
  `crates/bombay/src/observe/external_tests/stress.rs`, this ledger, and
  `docs/todo.md`. Forecast production `+0/-0/net 0`, in-file test source
  `+1/-1/net 0`, public API `+0/-0` types. Reuse the existing Observation
  custody and wake test; add no wrapper, helper, policy, or macro. Focused
  debug/release stress witness, source residue scan, strict Clippy,
  formatting, and complete tracked/untracked checkpoint are required.

### 2026-10-01 resolution checkpoint

- TEST-014 is `verified`. The discarded-observation residue scan failed on the
  prior `let _ = ...take()` and passes after replacing it with an explicit
  `drop`. The reentrant wake test passes in debug and release. Review of
  current `run_with` termination callers, Driver `CreationSequence::issue`
  and resulting complete trace, and all authored Behavior action tests
  confirms their exact facts are bound and asserted. Remaining live-source
  `let _ = timeout` is a Loom-only unused `Duration`, not an authoritative
  fact; compile-fail fixture expressions deliberately test static rejection.
  Strict all-target workspace Clippy, format check, and `git diff --check`
  pass. No production behavior or public API changed.
- Complete working-tree checkpoint including untracked files: 139 tracked
  plus 60 untracked paths, 199 total. Physical crate source including inline
  tests `+5762/-4804/net +958`; standalone tests/examples/scripts
  `+6873/-2455/net +4418`; docs `+15787/-1860/net +13927`; other
  `+255/-146/net +109`. Public types added by TEST-014: zero. ARC-013 is
  the lowest-sequence remaining ready item; ARC-020 retains TEST-014's
  now-satisfied edge and its other listed prerequisites.

## ARC-011 — launch and task cleanup ownership

### 2026-10-01 startup/finish waiter blocker and change ledger

- State: `active`. ARC-009 is `retained` and ARC-012 is `feature-complete`;
  ARC-011 unblocks ARC-018, ARC-016, TEST-013, and TEST-014. It is
  `feature-complete`, with final repository minimization pending. The exact lock
  selects Behavior Core/Actors 0.20.0 at
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0,
  Address 0.3.0, Communication 0.1.2, Bombay-private Observe, and the sole
  Timers patch at `13e884d`. The selected Behavior instructions and
  initialization/settlement contracts, selected Actors shutdown tests,
  Address reservation, Communication control-lane closure, Observe terminal
  publication, Timers actor queue, Bombay launch/Environment/Incarnation
  source and tests, and the current capability, module, Driver law, and
  strategy documents were inspected.
- Exact first blocker: dropping a pending `spawn_root_with` waiter detaches the
  started actor task and drops a bare `oneshot::Sender<OwnerCancellation>`;
  Communication's receiver sees lane closure rather than a cancellation
  request. The unpublished Address reservation survives after the
  initialization gate is released. Dropping a pending `OwnedTask::finish`
  waiter similarly detaches the task and closes, but does not request,
  cancellation, so the actor's termination observation never completes.
  Two existing ignored `local::tests` are the smallest end-to-end regressions.
- Ownership: Behavior owns pure folds and typed effects, Engine the causal
  Driver, Address the invisible reservation and lease, Communication the
  actor mailbox and control lane, Tokio the private owner-cancellation
  oneshot, Observe terminal publication, Timers
  actor-local scheduling, and Bombay the spawned incarnation task and the
  affine right to request owner cancellation. This is Bombay task custody,
  not new Behavior or supervisor policy.
- First production stage, before broader cleanup: an actor-task cancellation
  authority must own the sender from the instant a task is spawned until the
  join has completed. Its `Drop` requests `OwnerCancellation` if a startup or
  finish waiter disappears; an orderly join disarms it only after the task
  returns. The existing raw sender and duplicated manual send/drop branches
  are replaced. Expected touched files: `crates/bombay/src/launch.rs`, the
  two existing ignored tests in `crates/bombay/src/local.rs`, this ledger,
  and `docs/todo.md`. Forecast production `+35/-12/net +23`, tests
  `+0/-2/net -2` annotation lines, public types `+0/-0`; one private
  cancellation authority is eligible. It uniquely owns the sender's armed
  state and turns owner loss into a cancellation request; a bare sender drop
  cannot express that law. The two focused regressions are its concrete use.
- Keep the later launch duplication, `ProjectedTask` mapping task, and public
  application spellings out of this first production edit. After the waiter
  blocker passes in debug and optimized builds, separately compare plain
  functions/inherent methods/associated products with `LaunchSystem`, one
  root/owned spawn transaction, and typed origin projection. Any additional
  production stage needs its own change-ledger forecast and differential
  trace; a wrapper that only relocates bounds or tasks is ineligible.
- Prior-representation falsification: in debug, each focused ignored
  `local::tests::dropped_*` regression exited 101 at its intended cleanup
  assertion. The optimized filtered command ran both and exited 101 with the
  same two failures. The root probe released its interpretation gate, yet a
  fresh Address reservation was still denied. The finish probe timed out
  waiting for the exact actor termination after dropping its join waiter.
- First-stage result: `OwnerCancellationAuthority` now owns the cancellation
  sender from actor spawn through startup and finish. Dropping either waiter
  requests cancellation; a completed join disarms that request. Both focused
  regressions pass in debug and optimized builds, as do all 15 `local::tests`
  in both profiles. Strict Bombay all-target Clippy, workspace formatting,
  and `git diff --check` pass. The physical source delta for this stage is
  `+65/-19/net +46` including the two in-file test annotations removed from
  `local.rs`; no public type was added. This is net-positive capability code.
  Complete working-tree checkpoint: 198 changed paths, including 60 untracked;
  crate `src/` (including in-file tests) `+5448/-4674/net +774`, standalone
  tests/examples/scripts `+7528/-2401/net +5127`, documentation
  `+13987/-1858/net +12129`, other `+904/-146/net +758`. The inherited
  worktree remains in place. The remaining ARC-011 launch and task
  consolidation needs a separate failing law and change ledger before another
  production edit.

### 2026-10-01 root-launch composition stage

- Exact blocker: `LaunchSystem::launch_with` and `launch_axum` each construct
  the same Address root space, hosted space product, root interpreter,
  `ApplicationCapabilities`, and root actor task. The only semantic difference
  begins after startup: an arbitrary external boundary versus an Axum listener
  with graceful shutdown. A future edit to one construction path can diverge
  from the other. The existing public `run_with` root/child custody traces and
  Axum live-root/bind-denial traces are the smallest differential witness;
  deliberately omitting an owned root field or reordering Axum bind and spawn
  must fail those traces. This is a structural reduction with no new behavior
  law, so there is no honest pre-fix runtime failure from duplication alone.
- Plain Rust comparison: one private generic async function can take concrete
  `Spaces`, `Actor`, and `ApplicationAddresses`, and return the existing
  `RootActor<Actor, Vec<Terminal>>`. Inherent methods would attach policy to
  `HostedActorSpaces` or Address, which do not own application launch;
  associated types or an extension trait would add an interface without
  eliminating bounds. `LaunchSystem` remains the existing private application
  host proof; no new launcher trait, facade, public product, or macro is
  justified. `Application` constructs a staged declared-child root; `App`
  launches an already composed Behavior with explicit host spaces and optional
  installed families. Both caller spellings therefore retain distinct
  construction policies and use the same private launch operation.
- Stage change ledger before production edit: touch
  `crates/bombay/src/application_runtime.rs`, `crates/bombay/tests/run_with.rs`,
  this ledger, and `docs/todo.md`;
  forecast production `+50/-95/net -45`, tests `+1/-0/net +1`, public types
  `+0/-0`. Reuse `ActorSpace`, `HostedActorSpaces`, `ApplicationAddresses`,
  `ApplicationCapabilityInputs`, `RootInterpreter`, `spawn_root_with`, and
  `RootProjection`; delete the duplicated root-construction branch from
  `launch_with` and `launch_axum`. The private function owns no new state; it
  computes the same root launch transaction and returns the existing concrete
  owner. It is justified solely by deleting duplicated executable setup.
  Run focused `run_with`, `application_terminal_custody`, and feature-gated
  `axum` tests in debug and optimized builds, strict Clippy, formatting, and
  a complete tracked/untracked delta checkpoint before another stage.
- Result: the two entry paths now call `launch_application_root` after their
  respective boundary preparation. The exact root setup exists once. Physical
  task-local source delta was `+61/-66/net -5` rather than the forecast
  `net -45`: long static bounds and the two call sites consume most of the
  removed duplication. One assertion was added to the existing ordinary
  boundary trace, and no public type changed. This is a small source reduction,
  not a larger cleanup claim. `run_with` 12/12, application terminal custody
  4/4, and feature-gated Axum 3/3 pass in debug and release; strict all-feature
  Bombay Clippy, formatting, and whitespace checks pass. Inverting the one
  shared root address to `MailAddr(1)` made both the ordinary boundary and Axum
  live-root tests fail at their actual endpoint assertion; the source was
  restored and the strengthened ordinary witness passes in debug and release.
  The existing descendant and panic tests also pass in both profiles. The
  complete tracked/untracked checkpoint is 138 tracked changed paths plus 60
  untracked files = 198: physical crate source including in-file tests
  `+5509/-4740/net +769`, standalone tests/examples/scripts
  `+6816/-2401/net +4415`, documentation `+15406/-1858/net +13548`, other
  `+255/-146/net +109`.

### 2026-10-01 shared local spawn transaction stage

- Exact blocker: `spawn_root_with` and `spawn_owned_with_mode` repeat the same
  termination/report/cancellation pair creation, `LocalEnvironment::prepare`,
  `Incarnation` task spawn, startup await, and startup failure join. The root
  path genuinely publishes a public endpoint and retains weak shutdown
  control; the owned path transfers an exact private binding acknowledgement
  and retains direct control. The smallest end-to-end evidence is the existing
  root publication/address reservation tests and owned-child private-binding
  abandonment test in `launch::tests`, plus the dropped-startup and
  dropped-finish regressions in `local::tests`. A shared transaction must pass
  all of them in debug and optimized builds. The duplicated code itself has
  no honest pre-fix semantic failure; this stage removes a divergence point.
- Plain Rust alternatives: a private generic function can take a closure
  converting the prepared `LocalEnvironment` into its exact typed publication
  variant, startup receiver, and control projection. This uses `FnOnce` and
  inference to encode only the genuine root/owned differences, with no new
  trait, wrapper, public type, runtime lookup, erased result, or macro.
  Inherent methods on Address or `LocalEnvironment` would move Bombay launch
  policy into the wrong owner. A new launcher trait would duplicate the
  existing `LaunchSystem` host proof. The existing `RootActor`, `OwnedActor`,
  `OwnedTask`, `SpawnError`, and `LocalEnvironment` products remain.
- Change ledger before production edit: touch `crates/bombay/src/launch.rs`,
  this ledger, and `docs/todo.md`; forecast production
  `+95/-110/net -15`, tests `+0/-0/net 0`, public types `+0/-0`. Delete the
  duplicated runtime setup and startup failure branches. Verify the five
  launch, 17 local, and application terminal-custody traces in debug and
  release, strict Clippy and formatting, and the complete working-tree delta.
- Result: `spawn_local_with` owns the one termination/report/cancellation
  setup, prepared `LocalEnvironment`, task spawn, startup await, and failure
  join. Its `FnOnce` publication choice returns either exact root publication
  and weak shutdown control or private binding and direct control. The five
  launch, 17 local, and four application terminal-custody tests pass in debug
  and release; strict all-feature Clippy, formatting, and whitespace checks
  pass. Physical task-local source delta is `+41/-17/net +24`, above the
  forecast reduction because the static bounds and typed call sites cost more
  than expected. This is net-positive composition code; it centralizes the
  cancellation/settlement transaction and removes two divergent executable
  branches. No public type was added. Complete checkpoint: 138 tracked
  changed paths plus 60 untracked files = 198; crate source including in-file
  tests `+5732/-4790/net +942`, standalone tests/examples/scripts
  `+6816/-2401/net +4415`, docs `+15514/-1858/net +13656`, other
  `+255/-146/net +109`.

### 2026-10-01 join authority and projection comparison

- Exact blocker: `OwnedTask::retire`/`finish` and
  `ProjectedTask::retire`/test `finish` each manually request or disarm the
  same affine owner-cancellation sender around a Tokio join. A future edit can
  disarm before join or omit cancellation on one path. Existing dropped-finish
  and projected-child retirement regressions are the smallest end-to-end
  checks; moving disarm before join must fail the gated dropped-finish test.
- Plain Rust comparison: methods on the existing private
  `OwnerCancellationAuthority` can consume it with the existing typed
  `JoinHandle<T>` and return Tokio's exact `JoinError`. Callers retain their
  distinct interpretation of actor outcomes versus projected root terminals.
  A generic task wrapper or trait would add a type/interface for the same
  state and is rejected. Forecast source `+20/-25/net -5`, in-file tests
  `+0/-0/net 0`, public types `+0/-0`. Touch `crates/bombay/src/launch.rs`,
  this ledger, and `docs/todo.md`. Reuse `OwnedTask`, `ProjectedTask`,
  `OwnerCancellationAuthority`, and Tokio `JoinHandle`; delete the four
  duplicated manual request/disarm branches. Run root/child/cancellation,
  panic, descendants, and Axum traces in debug and release, strict Clippy,
  formatting, and the complete checkpoint.
- Projection-task comparison: the existing `ProjectedTask<B, Root>` must fit
  the `ChildOccurrenceShape::Member<Position, Child, Tail>` binding product.
  The selected `ProjectTerminal<ChildOrigin<Owner, Role>,
  ActorRetirement<B, Root>>` proof is different for structural children,
  appended application children, and declared-role children. Storing the
  original `OwnedTask` and typed `ChildOrigin<Owner, Role>` in `ProjectedTask`
  would require `Owner` and `Role` parameters through `CreationBinding`,
  `ChildBinding`, `RuntimeChildBindings`, nested bindings, and every
  `ProjectChildTerminal` result. A function pointer or boxed closure would
  hide those role parameters in runtime projection dispatch. The current one
  mapping task closes that concrete proof once, owns and settles the original
  task, and returns exactly `Root`; dropping the parent waiter still requests
  cancellation without dropping the mapping task. Removing that one task by
  adding a new type graph or erased projector does not meet the source-size
  and static-provenance constraints, so the conditional task-removal proposal
  is retained as an intentional boundary. The existing projected child test
  proves complete nested terminal custody; root/declared-role traces prove
  both projection forms. No production change is authorized for this
  conditional subitem.
- Join-authority result: `OwnerCancellationAuthority::retire` requests
  cancellation before awaiting a typed `JoinHandle<T>`;
  `OwnerCancellationAuthority::finish` disarms only after that join returns.
  `OwnedTask`, `ProjectedTask`, and failed startup use those two consuming
  operations and retain their distinct actor-outcome or projected-terminal
  interpretation. A deliberate inversion moving disarm before await made the
  gated dropped-finish test fail at its exact cleanup assertion; restoration
  passes. All 190 Bombay library tests, 12 ordinary run/boundary tests, four
  application terminal-custody tests, and three feature-gated Axum tests pass
  in debug and optimized builds. Strict all-feature Bombay Clippy, workspace
  formatting, and whitespace checks pass. This task-local change adds no
  public types. Because the new edits coalesce with prior `launch.rs` hunks,
  the complete physical source diff moved from `+5732/-4790/net +942` to
  `+5732/-4789/net +943`; the movement is not a standalone line count for the
  method extraction. Complete tree: 138 changed tracked files and 60
  untracked files = 198, with crate source including in-file tests
  `+5732/-4789/net +943`, standalone tests/examples/scripts
  `+6816/-2401/net +4415`, documentation (before final current-guidance
  notes) `+15569/-1858/net +13711`, and other `+255/-146/net +109`.
  ARC-011 is `feature-complete`; global type and interface distillation is
  still required by the terminal audit.
- Post-stage workspace gate: `nix develop -c cargo test --locked --workspace`
  passed across the full debug workspace, integration suites, compile fixtures,
  and documentation tests. The earlier focused optimized runs cover all
  changed launch/task paths; global optimized and flake gates remain final
  audit obligations after later queue items. Final ARC-011 tracked/untracked
  checkpoint after current-document revisions: 138 tracked changed paths plus
  60 untracked files = 198; crate source including in-file tests
  `+5732/-4789/net +943`, standalone tests/examples/scripts
  `+6816/-2401/net +4415`, docs `+15632/-1860/net +13772`, other
  `+255/-146/net +109`. Public types added by ARC-011: zero.

### 2026-10-01 activation-task settlement custody stage

- Exact blocker: `OwnedTask::finish` and startup failure settle the returned
  `LocalResidual::activation_tasks` in the joining waiter. If that waiter is
  dropped after the actor publishes termination, the detached actor task can
  return its residual into an unobserved `JoinHandle`; Tokio then drops the
  residual and aborts its actor-owned `JoinSet`. Sending owner cancellation
  alone does not give the activation task a cleanup owner. The smallest new
  regression will gate one activation task after interpreter retirement, drop
  the root finish waiter, wait for termination, release the task, and require
  its exact completion signal. The prior representation must fail that test
  in debug and optimized builds before production changes.
- Ownership: `ActivationTasks` and its `JoinSet` are Bombay actor-task custody;
  Observe publishes the separate actor-termination fact. The spawned
  incarnation task must await activation settlement before its output may be
  dropped by a disappeared caller. A plain async block around the existing
  `Incarnation::run` and `settle_local_outcome` can express the law; no wrapper,
  trait, new public type, erased task, or extra Tokio task is needed. The
  existing `OwnedTask` and `OwnerCancellationAuthority` remain the join and
  cancellation owners.
- Change ledger before production edit: touch `crates/bombay/src/launch.rs`,
  the owning `crates/bombay/src/local.rs` test, this ledger, and `docs/todo.md`.
  Forecast production `+55/-20/net +35`, in-file tests `+85/-0/net +85`, public
  types `+0/-0`. Reuse the existing `settle_local_outcome`, `Incarnation`,
  `LocalResidual`, and `ActivationTasks`; delete settlement from the waiter
  and startup failure paths. Preserve the exact panic distinction: an
  incarnation-task panic becomes `IncarnationOutcome::Panicked`, while a panic
  from a nested activation task still resumes unwinding at an awaiting owner.
  The existing `JoinError` from Tokio's actor-owned `JoinSet` must travel
  through private settlement results; no erased new panic container is added.
  Run the focused regression in debug and release
  before and after, then the local and launch tests, Clippy, formatting, and a
  complete tracked/untracked checkpoint.
- Prior-representation falsification: the gated regression exits 101 in debug
  and optimized builds at `retirement did not abort task startup`. It first
  observed exact `Crash::Cancelled` termination, then found that the
  `ActivationTasks` `JoinSet` had been dropped and its task-start sender was
  closed. The original task panic behavior is now an explicit preservation
  requirement of this stage.
- Result: each spawned incarnation task now settles its own returned
  `ActivationTasks` before handing the exact local outcome to an optional
  joiner. `OwnedTask` and startup failure only resolve that one task's result;
  a nested activation `JoinError` remains separate from an incarnation task
  panic, and a joining owner resumes the nested panic. The previously failing
  cancellation/settlement regression passes in debug and optimized builds.
  A separate activation-task panic test and all 17 local plus five launch
  tests pass in both profiles. Strict all-feature Clippy, formatting, and
  whitespace checks pass. This stage's physical crate-source delta including
  its in-file tests is `+182/-33/net +149`; the test fixtures and two
  regressions account for most of the added lines. Public API `+0/-0` types.
  Complete tracked/untracked checkpoint: 138 changed tracked paths plus 60
  untracked files = 198; crate source including in-file tests
  `+5691/-4773/net +918`, standalone tests/examples/scripts
  `+6816/-2401/net +4415`, documentation `+15465/-1858/net +13607`, other
  `+255/-146/net +109`.

## ARC-009 — semantic ownership of application capabilities

### 2026-10-01 selected-contract field and interpreter audit

- State: `retained` after direct-composition evidence. ARC-008 and ARC-010 are `feature-complete`; ARC-009 unblocks
  ARC-013 and ARC-011. The exact lock selects Behavior Core/Actors 0.20.0
  from `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0,
  Address 0.3.0, Communication 0.1.2, Bombay-private Observe, and the sole
  Timers patch at `13e884d`. The complete selected Behavior `AGENTS.md`, its
  `InterpretItem`/`InterpretSends`/source contracts, selected Actors requests
  and tests, Bombay source and focused terminal tests, and the current
  capability, module, Driver law, and Driver strategy documents were read.
- Exact concentration and design blocker: `ApplicationCapabilities` stores
  multiple owners' state and implements all named interpreter leaves. Behavior
  requires one concrete `Interpreter` implementing each leaf of a composed
  send product; it does not automatically route those leaves to Rust fields.
  A mechanical split would introduce one forwarding implementation per leaf
  and increase surface without changing a law. Before production edits,
  attempt a compile-only direct composition and a complete retirement trace
  using existing concrete owners, then identify the smallest state relocation
  that removes a real invariant or duplicate implementation.
- Current field ownership: `actor_spaces`, `allocations`, and `address` belong
  to local addressing/delivery and creation; `control` belongs to factual
  actor ingress; `timers` to actor-local Timers; `facts`, `peers`, and
  `exact_observations` to Observe registration/cancellation;
  `next_child_route` and `child_bindings` to one child occurrence namespace;
  `activation_tasks` to actor-owned asynchronous capability work;
  `terminal_reports` and `parent_reports` to distinct report custody;
  `origins` to static terminal projection. `control` and `activation_tasks`
  are genuinely shared between source, activation, and observation work.
- Interpreter map from current source: creation routing and child
  establishment use the child namespace plus Address; delivery, customer,
  assignment, and diagnostics use Address/Communication; child input,
  proxy control, child observation, and child shutdown use the established
  child occurrence; worker initialize/activation/preparation use actor task
  custody and typed ingress; `ScheduleAt`/`ScheduleAfter` use Timers;
  peer/established observations and cancellation use Observe; parent,
  terminal, and shutdown-plan reports use their typed report/control owners.
  `EntityAdmission` is an Address lookup, not a new Entity interpreter.
- Baseline law and inversion: the existing live two-child terminal test and
  held-source supervisor/pool tests observe complete action results and
  retirement custody. A proposed split must retain exact child terminals,
  reports, activation task results, and unattempted source actions. Removing
  or forwarding any one owned path must fail a named existing or new
  inversion before broader production edits. Existing `ActionInterpreter`
  remains the direct Behavior `Actions` adapter to Engine.
- Research-stage change ledger before any production edit: expected touched
  files are this ledger, `docs/todo.md`, and at most a compile-only or
  differential test under `crates/bombay/tests/`; expected production
  `+0/-0/net 0`, public types `+0/-0`. Reuse `ApplicationCapabilities`,
  `LocalTimers`, `FactQueue`, `ChildBindings`, `ActivationTasks`,
  `LocalTerminalReports`, `ApplicationAddresses`, `ActionInterpreter`, and
  the selected Behavior product traversal. Any later production stage needs
  its own exact failing witness, candidate owner, touched-file forecast,
  production delta, and public-type forecast before its first edit.
- Trait disposition to test: `TerminalReportTransaction` owns begin/finish
  selection around one complete action; `RetireCapabilities` carries the
  affine task/descendant boundary into generic `ActionInterpreter`. Their one
  implementation is not by itself proof of redundancy. `LaunchSystem`
  constrains every ordinary/advanced application launch and overlaps
  ARC-011's spawn consolidation; compare an inherent or free-function
  spelling and avoid deleting it here merely to duplicate long bounds.
- Plain-Rust composition probe: the exact selected `InterpretSends` contract
  was compiled in `/tmp/bombay-arc009-direct-owners`. Two independent lane
  owners, one implementing `InterpretItem<ParentReport, (), Here>` and the
  other `InterpretItem<TimerSchedule, (), Inside<Here>>`, fail as an ordinary
  tuple at `require_complete_interpreter::<(ParentReportLane, TimerLane)>()`.
  Pinned `cargo check --locked` reports E0277 for both missing tuple leaf
  implementations. A named `ActorCapabilities` product with two forwarding
  `InterpretItem` implementations compiles. It adds no invariant or
  transformation; multiplying that pattern across all 28 current request
  leaves would relocate the same interpretation behind 28 forwarding bodies.
  The existing `ApplicationCapabilities` directly implements each selected
  leaf against its typed owner and is the smaller static product.
- The complete current leaf set is `Creations<CreateChild>`; `Delivery`,
  `EstablishedDelivery`, `CustomerDelivery`, three `DiagnosticAction` forms,
  `ChildDelivery`, `ChildInput`, `ProxyOperation`, and `AssignWorker`;
  `InitializeWorker`, `BeginActivation`, and `PrepareWorkers`; `ScheduleAt`
  and `ScheduleAfter`; `EntityAdmission`; `ObserveCreation`,
  `ObserveEstablishedCreation`, `ObservePeer`, `ObserveChild`,
  `ObserveEstablished`, and `CancelObservation`; `ShutdownChild`,
  `ShutdownEstablished`, `ReportToParent`, `ReportTerminalOutcome`, and
  `ReportShutdownPlan`. The field/owner map above assigns each leaf.
  `next_child_route` spans every structural child occurrence;
  `exact_observations` binds Behavior observation IDs to cancellation of
  actor-owned tasks. Placing either in an Address, Observe, or one-occurrence
  primitive would give that primitive Bombay interpretation policy. A new
  wrapper around either field would only rename the existing state.
- Retention result: `ApplicationCapabilities` is the required one concrete
  interpreter product, while `ActionInterpreter` is the one Behavior-to-Engine
  adapter. `RetireCapabilities` transfers exact event/task/descendant custody
  across that generic adapter, `TerminalReportTransaction` brackets complete
  action interpretation, and `LaunchSystem` supplies the application-host
  launch operation across concrete `Spaces` substitutions. Each trait has
  one generic implementation but owns a distinct boundary; none solely
  compresses bounds. ARC-011 may minimize launch orchestration after its own
  cancellation regression. The prior 0.19.0 awaited source failed the
  held-source shutdown trace; selected runtime tests now prove task-event
  acquisition, child terminal order, terminal report selection, unattempted
  preparation custody, and complete retirement. The full locked workspace
  tests and 21-check Nix flake gate passed. This audit added no production
  code, test code, or public type; the proposed forwarding product was
  rejected as net-positive machinery. ARC-009 is `retained`, unblocking
  ARC-013 and ARC-011.

## TEST-025 — executable supervisor and pool policy evidence

### 2026-10-01 selected 0.20.0 feature verification and change ledger

- State: `verified` after ARC-010 reached `feature-complete`; TEST-025 unblocks
  ARC-020 in the audit queue and DIST1 in the wider feature graph. The exact
  lock selects Behavior Core/Actors 0.20.0 from
  `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, Macros 0.13.0,
  Address 0.3.0, Communication 0.1.2, Bombay-private Observe, and the sole
  Timers Git patch at `13e884d`. The selected Behavior instructions, Actors
  supervisor/proxy/FIFO source and owner tests, Bombay current source,
  focused runtime tests, examples, and normative capability, module, Driver,
  and strategy documents were inspected for this item.
- Ownership: Behavior owns pure `Actions` and complete settlements; Behavior
  Actors owns `FixedSupervisor`, `StableProxy`, `FifoPool`, recovery,
  interruption, diagnostic, and shutdown policy; Bombay owns only concrete
  interpretation, task custody, activation, and retirement. Address owns
  leases, Communication the mailbox, Observe completion publication, Timers
  actor scheduling, and Engine the causal Driver. The 0.20.0 owner tests prove
  diagnostic ingress and preparation/shutdown interleaving; the Bombay
  examples and runtime regressions prove those policies through a real `App`.
- Exact prior defect and falsifier: the public supervision binary previously
  inspected only initialization actions. Under selected 0.19.0, an isolated
  live fixed-supervisor caller failed E0277 at absent proxy diagnostic ingress;
  the held-source FIFO live caller timed out before its required
  `ShuttingDown` rejection. A valid witness must observe replacement after a
  real worker stop, exact typed diagnostic custody under a rejected later
  role, shutdown, and every terminal; the FIFO witness must observe actual
  job completion, interruption policy, and source custody.
- Smallest evidence stage: run the existing current supervisor and FIFO
  binaries in debug and optimized builds; inspect their independent runtime
  tests for full typed or observable traces and retain the pure initialization
  test only for its independent creation/observation correlation. Add a
  focused test only if a required observation is absent. Forecast touched
  files: this ledger, `docs/todo.md`, current example/evidence guidance, and
  possibly one existing supervisor test. Expected Bombay production
  `+0/-0/net 0`; public types `+0/-0`; reuse all selected owner templates,
  runtime interpreters, and existing test fixtures.
- Dependency edges: blocked by ARC-010, now satisfied; TEST-025 unblocks
  ARC-020 and DIST1, while the latter remains blocked by its other owners.
- Result: both public binaries ran in debug and optimized builds under pinned
  Nix. The fixed-supervisor runtime and coordinated-recovery tests passed in
  both modes; their traces show real activation, first-worker stop,
  declaration-order replacements, terminal diagnostic custody under a later
  role's rejection, shutdown, and exact root/proxy/worker retirement. The
  FIFO runtime tests passed in both modes and cover accepted work, completion,
  backlog rejection, both interruption dispositions, held-source shutdown,
  source panic, and terminal custody. The remaining supervisor pure test
  asserts initial creation/observation correlation, which the live trace
  cannot observe, so it is an independent construction law. This iteration
  added no production code, tests, or public type; complete worktree physical
  source `+5383/-4655/net +728`, standalone tests/examples/tooling
  `+7528/-2401/net +5127`, at the 198-path/60-untracked checkpoint. The
  final Nix flake gate passed all 21 checks after generated build outputs were
  cleaned.

## ARC-010 — complete Behavior Actors capability interpretation

### 2026-10-01 published 0.20.0 adoption stage

- State: `feature-complete` after the focused debug, optimized, and full
  workspace gates. `Cargo.lock` selects registry Behavior Core 0.20.0
  (`10ebe68552dcb07f48c5af0af4ef0cafbbf4f9743a4a58845879768d712f9846`)
  and Actors 0.20.0
  (`84d5b21b694fd9c86a9e0cdba72e15e478181ba0977e3b10dc34f476f16e5c2b`),
  both from `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`; Macros remains
  0.13.0. The sole root patch remains Timers at `13e884d`. Address 0.3.0,
  Communication 0.1.2, Bombay-private Observe, and the exact Timers source
  retain their established primitive ownership. The selected revision's
  complete `AGENTS.md`, public worker-preparation and proxy-report algebra,
  owner tests, current owner docs, and Bombay's capability, module, Driver,
  and test-strategy documents were inspected before this stage.
- Exact prior blocker and falsifier: under 0.19.0, StableProxy emits a typed
  `ProxyDiagnostic` that FixedSupervisor cannot ingest (the isolated
  supervisor caller fails E0277). A permanent FIFO pool with a held affine
  replacement source cannot fold its queued shutdown: the distinct job sent
  after shutdown times out instead of returning its exact `ShuttingDown`
  rejection before source release. Both failures were established against the
  prior selected representation and preserved as research probes.
- Selected owner contract: Actors 0.20.0 gives fixed and dynamic supervisors
  exact typed proxy-diagnostic ingress. `PrepareWorkers::start` consumes one
  source action into a ticket-bearing start receipt and the affine source
  attempt; the complete `WorkerPreparation` returns later through a typed
  event. FIFO, keyed, and fixed owner tests cover shutdown, late results,
  foreign tickets, and exact source custody. Behavior owns generic action
  settlement; Actors owns preparation and supervisor policy; Bombay owns the
  concrete source interpreter, actor task custody, event acquisition, and
  retirement. Communication remains the two-lane mailbox, Address the lease,
  Observe the publication owner, Timers the actor-local queue, and Engine the
  causal Driver.
- Smallest end-to-end regression: run the held-source FIFO `App` test so
  shutdown is observed through a distinct job's exact `ShuttingDown`
  rejection while the source is still held, then release the source and prove
  no replacement starts and terminal custody is complete. Compile and run
  the fixed-supervisor diagnostic/shutdown probe through the selected crate.
- Change ledger before Bombay production edits: expected task-touched files
  are `Cargo.toml`, `Cargo.lock`, `application_runtime.rs`, `interpret.rs`,
  `local.rs`, `worker_preparation.rs`, focused supervisor tests,
  `fifo_pool_recovery.rs`, this ledger, `docs/todo.md`, the revision-bound
  Driver/template manifests and their assertions, the Engine fuzz lockfile,
  and current capability/Driver/example guidance. Forecast production
  source `+91/-74/net +17` from the cleanly applicable owner adoption patch;
  tests `+170/-6/net +164`; public Bombay types `+0/-0`. Reuse the existing
  `ApplicationCapabilities`, `ActorTaskGroup`, Communication control sender,
  `InterpretItem`, `ItemSettlement`, `PrepareWorkers`, and exact source and
  event products. Delete the synchronous source-settlement path. No Bombay
  actor contract, preparation request, registry, or policy type is eligible.
  The already recorded authorization covers the cumulative Bombay worktree
  above the repository's path/line thresholds; this is a separate focused
  stage from later application cleanup.
- Dependency edges: BEH3 and TEST-008 are satisfied; ARC-010 unblocks
  TEST-025 and ARC-009. Both rows are ready after the complete interpreter
  and live-policy gates pass.
- Result: Bombay now returns the exact accepted preparation-start receipt and
  later delivers the complete source result through the actor-owned task event
  path. The old synchronous interpreter fails the 0.20.0 type contract; the
  prior 0.19.0 live probe times out on a shutdown rejection while the source
  is held. The selected held-source regression receives the exact
  `ShuttingDown` rejection before source release and creates no replacement.
  The selected fixed-supervisor tests exercise typed proxy diagnostics,
  ordered two-role preparation, later-role rejection, replacement activation,
  and complete terminal custody. Pure source tests cover accepted, worker
  rejected, source rejected, unattempted, and late shutdown results. All
  focused tests passed in debug and optimized builds; the complete locked
  workspace test and build, strict Clippy, rustfmt check, Driver law evidence,
  and `git diff --check` passed. The separate Nix flake check passed all 21
  checks after generated build artifacts were cleaned.
- Complete tracked and untracked checkpoint against HEAD after the selected
  adoption: 198 changed paths including 60 untracked. Physical crate `src/`
  (including embedded tests) `+5383/-4655/net +728`; standalone
  tests/examples/tooling `+7528/-2401/net +5127`; docs/README
  `+13681/-1858/net +11823`; other files `+904/-146/net +758`. Public
  Bombay types in this adoption stage `+0/-0`. The focused initial
  source-adoption patch was `+91/-74/net +17` before test-only adjustments.

### 2026-10-01 coordinated supervisor preparation witness

- Diagnostic-custody continuation: the isolated two-role probe uses
  `StopOnShutdown` around its root supervisor while sending the supervisor's
  own `FixedCommand::shutdown`. A direct-root compile attempt fails E0277:
  `FixedSupervisorEvent` has no `InjectEvent<ShutdownRequested, Here>`, which
  `App::run_with` requires for external lifecycle authority. The wrapper is
  therefore a real runtime capability, and the attempted removal was reverted.
  The smallest remaining witness is to inspect the existing terminal
  settlement's `DiagnosticAccepted::Terminal` in its named supervisor
  diagnostic lane and assert the exact second-role rejection and reason.
  Expected path: the research fixture plus current evidence/ledger/TODO;
  Bombay production `+0/-0`, public types `+0/-0`. No runtime adapter or
  second diagnostic policy is eligible.
- The exact locked Core/Actors 0.19.0 contract remains selected. Its fixed
  supervisor alone emits a `PrepareWorkers` request with an ordered, nonempty
  group after coordinated recovery; FIFO and keyed pools request one role.
  Behavior owns the source-action settlement, Actors owns roster selection and
  recovery policy, and Bombay owns only the affine source interpreter. Address,
  Communication, Observe, and Timers retain their existing roles.
- Smallest remaining local witness: drive a fixed supervisor with two live
  roles through its own failure/recovery path, record the source calls, and
  compare accepted role order and later-role rejection with the owner's pure
  `PrepareWorkers` result. The selected release cannot execute its proxy
  diagnostic ingress in Bombay, so this runs only in the isolated owner
  candidate copy and remains a research probe until that contract is selected.
  Expected paths: one research fixture, this ledger, and the TODO. Forecast
  Bombay production `+0/-0`, public types `+0/-0`; reuse the existing
  `WorkerPreparationSource`, `FixedSupervisor`, `StableProxy`, and `App`.
- The isolated fixture at
  `docs/research-probes/fixed-supervisor-coordinated.rs` now runs the actual
  two-role `App` composition against the unsubmitted owner candidate. A
  primary worker stop selects `OneForAll` recovery; Bombay's source records
  Primary then Secondary. When both submit, two replacements activate, the
  supervisor accepts shutdown, and the root, both stable proxies, and all
  four worker incarnations retire with completed stopped terminals. When the
  second role rejects, the source still records both role attempts, no
  replacement activates, the terminal diagnostic stops the root, both proxy
  tasks return `OwnerCancelled`, and their original workers retain one
  completed and one owner-cancelled terminal respectively. This is an exact
  consequence of the chosen `DiagnosticDisposition::terminate` policy; it is
  not claimed as an orderly drain.
- Both focused tests pass in debug and optimized builds in the isolated
  candidate copy; strict test-target Clippy passes. Inversions that submit a
  second Primary instead of Secondary and accept a role configured to reject
  fail at the distinct activation and no-replacement assertions. The copied
  fixture was restored to the research source byte-for-byte afterward. The
  main lock remains on registry Actors 0.19.0, so this does not clear the
  ARC-010 or TEST-025 blocker. This stage adds no Bombay production line or
  public type.
- A temporary copy of the same fixture in the exact locked main build fails
  E0277: `FixedSupervisorEvent` lacks `EventIngress` for the stable proxy's
  `ChildReport<ProxyDiagnostic<...>>`. The temporary test was removed after
  this negative control; at that checkpoint the research source and isolated
  runnable copy shared SHA-256
  `7e470fd4c27da35427711a120d4be1870450426f3800952353b55adf9ab4c658`.
- The diagnostic-custody extension is now proven against the isolated owner
  candidate. The existing root `StopOnShutdown` remains because `App::run_with`
  requires its typed `ShutdownRequested` ingress; direct supervisor launch
  fails E0277 and was reverted. The rejected two-role trace reads the retained
  terminal settlement's named diagnostic lane and asserts exactly one
  `DiagnosticAccepted::Terminal(FixedDiagnostic::WorkerPreparationFailed)`.
  Its trigger is Primary, its prepared prefix is Primary, its rejected role is
  Secondary with the exact `WorkshopRejection::WorkerUnavailable`, and its
  remaining suffix is empty. Debug, optimized, and strict test-target Clippy
  pass. Temporarily classifying the owner reason as `Unattempted` fails the
  exact diagnostic assertion; the owner source was restored to a clean tree.
  No Bombay production line or public type changed. The research source and
  isolated runnable copy now share SHA-256
  `7f8edc5443557b3e3ee0a57f8d645370201ed470ef082b639907a8175c7b0e11`.
- The final refined fixture was also copied temporarily into the exact locked
  main build. Its compile-only negative control still fails E0277 at the
  missing `EventIngress<Births<StableProxy<...>>,
  ChildReport<ProxyDiagnostic<...>>>`; the temporary test was removed. The
  candidate owner checkout is clean at `e55c2cb6c3a3c10bc5ee96c7d817a39a4a8e239a`.
  Main-tree `git diff --check` and pinned `cargo fmt --all -- --check` pass.
- Complete tracked/untracked checkpoint after diagnostic-custody and
  held-source contract research: 194 changed paths, 58 untracked;
  production `+5381/-4653/net +728`, tests/examples/fuzz/benches
  `+5820/-2435/net +3385`, docs and README `+14877/-1858/net +13019`,
  other `+100/-49/net +51`. Public types in this stage `+0/-0`. This is the
  complete inherited worktree, not the delta of the diagnostic probe. The
  recovery checkpoint records authorization above cumulative thresholds.
- Complete tracked/untracked checkpoint against `HEAD`: 194 changed paths,
  58 untracked; crate `src/` including in-file tests `+5381/-4653/net +728`;
  tests/examples/fuzz/benches outside `src/` `+5846/-2452/net +3394`;
  docs and README `+14696/-1841/net +12855`; other repository files
  `+100/-49/net +51`. Public types in this stage `+0/-0`. The inherited
  recovery checkpoint records authorization above cumulative limits.

### 2026-10-01 live shutdown-during-preparation blocker

- An isolated owner worktree at `/tmp/bombay-behavior-late-preparation`, based
  on selected revision `e5c703e966eba4d2a15fe2c129594f57ae2270fc`,
  now holds the test-first two-stage preparation experiment. Its complete
  contract and aggregate-drift checkpoints are in
  `docs/engineering/worker-preparation-interleaving.md` in that worktree.
  The FIFO and fixed-supervisor caller tests fail against an untouched
  selected owner checkout at the absent consuming `start()` and start-event
  APIs in both debug and optimized builds. The source candidate supplies an
  affine start receipt, first-attempt source custody, and typed late return;
  direct-worker expectation moves from `Issued` to `Started` before accepting
  that return. This is still an incomplete research branch. Pinned owner
  `cargo check --locked -p bombay-behavior-actors --lib` fails at the remaining
  FIFO, keyed, and fixed old-consumer cluster. Its current complete worktree
  has eleven changed paths; production `+664/-176/net +488`, tests
  `+88/-11/net +77`, public types proposed `+2/-0`. The proposed separate
  public late-result wrapper was removed; the existing `WorkerPreparation`
  now owns the private source-rejected outcome. Bombay production remains
  unchanged for this experiment. FIFO's concrete event and correlation scan
  have begun. The early-failure result now carries only exact interpreter
  fault or skipped facts, with no impossible ready-worker alternative. The
  next coherent source edit crosses the owner +500 net-line checkpoint; the expanded
  surface question is pending. Neither ARC-010 nor TEST-025 is unblocked.
- Complete Bombay worktree checkpoint before this line, including untracked
  files: 194 changed paths, 58 untracked; crate `src/` production
  `+5381/-4653/net +728`, tests/examples/fuzz/benches outside `src/`
  `+5797/-2393/net +3404`, docs/README `+14980/-1858/net +13122`, other
  `+131/-91/net +40`. The worker-preparation experiment in this continuation
  changed Bombay documentation only: Bombay production `+0/-0`, public API
  `+0/-0`. The inherited Bombay cumulative threshold has its prior recorded
  authorization; the separate owner worktree threshold does not.

- Rechecked the registry through pinned `nix develop -c cargo search
  bombay-behavior-actors --limit 1`; 0.19.0 is still the latest listed owner
  release. `Cargo.lock` still selects registry Core/Actors 0.19.0 and Macros
  0.13.0, with only Timers patched. The selected owner source fixes
  `PrepareWorkers::Accepted` to complete `WorkerPreparation`; its source
  action sends that exact result back through typed admission. No selected
  release offers a start receipt and later preparation result.
- Probe refinement: a queued shutdown command alone does not prove that the
  pool folded it before a prepared worker returned. The refined negative control
  keeps the affine source held, sends shutdown, then submits one more exact
  job and requires its `ShuttingDown` rejection before releasing preparation.
  That rejection is the independent observable witness that shutdown entered
  the Behavior fold. Expected paths: the research patch, its gap note, this
  ledger, and the TODO; production `+0/-0`, public types `+0/-0`. Reuse the
  selected `FifoCommand`, `FifoOutcome`, and existing live recovery fixture.
- The refined patch fails in both the exact locked build and the isolated
  proxy-diagnostic candidate at `shutdown must fold while preparation is held:
  Elapsed(())`. The test captures the timeout before releasing the source, so
  the failure proves that shutdown did not fold while preparation remained
  outstanding; it no longer infers folding from mailbox admission. The
  second submitted job carries a distinct submission ID and payload and, on a
  conforming path, must be returned with `AdmissionRejection::ShuttingDown`.
  `patch --dry-run -p1` succeeds. The ordinary test source was restored to
  SHA-256 `8fbd4a5cf845176b18660d61a366c62dd3c402838526383c17a86c306db5a3b6`.
- The exact locked Core/Actors 0.19.0 source at `e5c703e` was selected for
  the runtime probe. The owner pure-fold test admits shutdown before the
  in-flight `PrepareWorkers` result and creates no replacement when that
  result arrives. The live Bombay adapter instead awaits the complete source
  action inside `InterpretItem`, and Engine D-TURN-1 awaits that action's
  settlement before reading the next mailbox event. Those are distinct
  timing contracts; neither Address, Communication, Observe, nor Timers owns
  a substitute policy for the missing interleaving.
- The initial failing extension to the real permanent FIFO `App` regression holds the
  affine source's first preparation on a oneshot after it announces startup,
  admits the pool's own shutdown command, releases preparation, and inspects
  the returned terminal tree. The desired owner-policy trace has only the
  stopped original worker. The exact locked build instead returns two worker
  descendants: a replacement starts before the queued shutdown is folded.
  The focused pinned-Nix command fails at `shutdown must suppress replacement`
  with `left: 2, right: 1`; the isolated proxy-diagnostic candidate produces
  the same result. The probe patch and reproduction are preserved in
  `docs/research-probes/fifo-shutdown-during-preparation.patch` and
  `docs/research-probes/fifo-preparation-shutdown-gap.md`. The ordinary test
  source was restored byte-for-byte afterward.
- This adds an independent selected-contract blocker to ARC-010. The current
  `PrepareWorkers` source-action settlement cannot return its complete
  `WorkerPreparation` until the affine source finishes; spawning the work
  behind the existing request would require a fabricated accepted receipt or
  an untyped side channel. The Behavior/Actors owner must decide a typed
  start/late-resolution contract before Bombay can interpret this policy
  without violating D-TURN-1 or source custody. No production edit, public
  type, or second runtime policy was added. The previously identified
  supervisor diagnostic-ingress blocker remains separate.
- Pinned rustfmt and `git diff --check` pass after restoring the ordinary
  recovery test. The live probe remains a research patch because its expected
  assertion fails against the selected 0.19.0 contract. This stage adds no
  production line or public type; the complete-tree measurement follows the
  source/test/document classification used by this checkpoint.
- The complete locked Bombay workspace test suite and strict workspace
  all-target Clippy pass after the new FIFO interruption tests and probe
  restoration. Owner fixed-supervisor tests show that coordinated recovery
  emits a multi-role `PrepareWorkers` request; that is the owning path for the
  remaining local later-role and ordering evidence.
- The refined research patch passes strict test-target Clippy in the isolated
  copy. After restoring the main source, all three ordinary locked FIFO
  recovery tests pass. The refined probe changes no production line or public
  type; it strengthens the exact previously recorded timing blocker.
- Owner-contract audit after the live failure: FIFO and keyed pools each
  request one role, while fixed supervision requests an ordered group. The
  selected keyed-pool lifecycle test folds shutdown before preparation returns;
  the fixed supervisor enumerates 5,040 preparation/proxy arrival orders. The
  current `PrepareWorkers` action has no truthful early settlement variant:
  `Accepted` requires completed preparation, `Rejected` requires an actual
  source rejection, `Blocked` has `Never` prerequisite, and `Corrupt` and
  `Unattempted` have distinct adverse meanings. The active Bombay Environment
  does not monitor delayed activation-task failures before retirement, so
  reusing its task set alone would risk an indefinitely waiting actor. The
  acceptance conditions and task-custody consequence are recorded in the
  research gap note; no owner or Bombay production edit followed this audit.
- Complete tracked/untracked checkpoint for the owner-contract audit, measured
  before appending these bullets: 194 changed paths, 58 untracked;
  production `+5381/-4653/net +728`, tests/examples/fuzz/benches
  `+5820/-2435/net +3385`, docs and README `+14918/-1858/net +13060`,
  other `+100/-49/net +51`, public types in this stage `+0/-0`. The earlier
  recovery checkpoint records authorization above cumulative thresholds.
- Complete tracked/untracked checkpoint for this refinement: 194 changed
  paths, 58 untracked; crate `src/` including in-file tests
  `+5381/-4653/net +728`; tests/examples/fuzz/benches outside `src/`
  `+5846/-2452/net +3394`; docs and README `+14765/-1841/net +12924`; other repository
  files `+100/-49/net +51`; public types in this stage `+0/-0`.
- Complete tracked/untracked checkpoint against `HEAD` after this research
  stage: 193 changed paths, 57 untracked. Crate `src/` including in-file
  tests: `+5381/-4653/net +728`; tests/examples/fuzz/benches outside `src/`:
  `+5846/-2452/net +3394`; docs and README: `+14174/-1841/net +12333`; other repository
  files: `+100/-49/net +51`. Public types in this stage: `+0/-0`. The
  recovery checkpoint records user authorization above cumulative limits.

### 2026-10-01 FIFO interrupted-assignment runtime stage

- The current lock still selects Behavior Core/Actors 0.19.0 from `e5c703e`,
  Macros 0.13.0 from `3f08364`, Address 0.3.0, Communication 0.1.2,
  Bombay-private Observe, and Timers `13e884d`; the sole root patch is
  Timers. The exact selected Behavior instructions match the owner checkout's
  `AGENTS.md` by SHA-256. The owner remote HEAD remains `e5c703e`.
- Selected Actors `FifoPool::interrupt_assignment` returns an assigned job's
  complete payload under `Interruption::Fail` or reinserts the exact customer
  job under `Interruption::Retry` before invoking the owner's recovery policy.
  Behavior owns the pure action and settlement algebra; Actors owns the
  interruption/recovery choice; Bombay owns only concrete interpretation,
  delivery, worker task retirement, and typed terminal projection. Address,
  Communication, Observe, and Timers retain their locked primitive ownership.
- Smallest independent runtime regression: an initially ready worker receives
  one job and stops without a completion report. Under `Interruption::Retry`,
  the same accepted job must complete on one source-prepared replacement with
  no early customer return. Exact job correlation, replacement source count,
  normal root termination, and both stopped worker terminals must survive.
  The inversion selects `Interruption::Fail` and must return the original
  assigned payload instead of completing it.
- Expected files: existing `fifo_pool_recovery.rs`, this ledger,
  `docs/todo.md`, and current capability evidence. Forecast production
  `+0/-0`, tests `+90/-0`, public types `+0/-0`; reuse the selected pool,
  source action, `WorkerPreparationSource`, `App`, and terminal product. No
  actor policy, runtime wrapper, or new effect lane is eligible. ARC-010 still
  blocks TEST-025 and ARC-009 on selected supervisor diagnostic ingress.
- Result: the existing recovery fixture now has a worker that stops after
  accepting assignment without reporting completion. With
  `Interruption::Retry`, the original accepted job completes on one
  source-prepared replacement with its original job ID and typed result; no
  customer return precedes it. With `Interruption::Fail`, the customer gets
  the same job ID and exact payload with
  `AssignedReturnReason::WorkerStopped`; one replacement then completes a
  later correlated job. Both traces return normal root termination and two
  distinct stopped worker terminals with no descendants. The source prepares
  exactly once per trace. These are selected owner policies exercised through
  Bombay `App`, with no policy copied into the test Behavior.
- All three focused recovery tests pass in locked debug and optimized builds;
  strict test-target Clippy passes. In isolated copies, selecting `Fail` for
  the retry witness fails its completed-outcome assertion, and selecting
  `Retry` for the return witness fails its returned-assignment assertion.
  Both mutations were restored. The earlier estimate of `+90` test lines was
  exceeded by the two distinct owner-policy traces; no production line or
  public type was added. The TEST-025 pool checklist is now satisfied, while
  its supervisor and final policy integration remain blocked by ARC-010.
- Complete tracked/untracked checkpoint against `HEAD`: 191 changed paths,
  55 untracked; crate source under `src/` including in-file tests
  `+5381/-4653/net +728`, tests/examples/tooling outside `src/`
  `+5929/-2484/net +3445`, docs and README `+13898/-1858/net +12040`;
  public types in this stage `+0/-0`. The earlier recovery checkpoint
  authorizes the cumulative task surface above repository thresholds.

### 2026-10-01 FIFO full-backlog payload custody

- Selected Actors 0.19.0 owns `BacklogCapacity`, `AdmissionRejection`, and the
  opaque `FifoOutcome::into_rejected` projection, which returns the exact
  submission ID, original job payload, and reason. Bombay owns only the
  concrete application delivery and shutdown. The selected owner contract
  and existing activation-held runtime fixture were inspected before this
  evidence-only extension; no production code or public type changed.
- The second test in `crates/bombay/tests/fifo_pool_runtime.rs` holds the sole
  worker's activation, admits one job into a capacity-one backlog, then
  observes the second submission rejected with its exact `SearchJob` and
  `AdmissionRejection::BacklogFull`. It releases activation, observes the
  admitted job complete with the same correlation and typed result, and
  returns a stopped root and worker after the pool's own shutdown command.
- The focused locked test passes in debug and optimized builds and strict
  test-target Clippy. In an isolated copy, increasing backlog capacity from
  one to two makes the rejected-outcome projection fail, and the fixture was
  restored. This covers one saturation branch, not interruption, cancellation,
  or all capacity-accounting schedules; TEST-025 and ARC-010 stay blocked.
- Complete tracked/untracked checkpoint against `HEAD`: 191 changed paths,
  55 untracked; crate source under `src/` including in-file tests
  `+5381/-4653/net +728`, tests/examples/tooling outside `src/`
  `+5744/-2484/net +3260`, docs and README `+13829/-1858/net +11971`;
  public types in this stage `+0/-0`.

### 2026-10-01 FIFO permanent-recovery runtime evidence

- The selected Core/Actors 0.19.0 `fifo` construction owns permanent recovery,
  its `PrepareWorkers` source action, job correlation, and `WaitForActorGraph`
  shutdown. Bombay owns only the concrete source operation and typed runtime
  interpretation; Communication, Address, Observe, and Timers retain their
  locked primitive roles. This test stage changes no selected contract,
  production adapter, public type, or owner template.
- `crates/bombay/tests/fifo_pool_recovery.rs` now runs an actual one-role
  permanent pool in `App`. The initial worker completes one submitted job and
  stops; the exact source prepares one replacement, which completes another
  correlated job. The pool receives its own shutdown command, terminates
  normally, and returns root and two distinct stopped worker terminals with
  no hidden descendants. The source count remains exactly one. Both jobs'
  accepted and completed customer outcomes retain their submission/job
  correlation and typed result.
- The focused locked integration test passes in debug and optimized builds;
  strict test-target Clippy passes. In an isolated copy, changing the first
  worker to remain available prevents source preparation and fails the
  replacement-wait assertion after its bounded test timeout; the original
  worker was restored. The worker state is an explicit `WorkerAvailability`
  sum, and no action occurs inside its pure fold beyond the returned
  `Actions`. After this stage, complete locked Bombay workspace tests,
  strict workspace all-target Clippy, workspace rustfmt, and diff whitespace
  checks all pass.
- This adds real permanent-recovery evidence to ARC-010 and TEST-025. It does
  not settle multi-role ordered preparation, per-worker rejection at runtime,
  interruption/saturation, keyed-pool behavior, or the selected supervisor
  diagnostic-ingress blocker. Both queue rows remain blocked.
- Complete tracked/untracked checkpoint against `HEAD`: 191 changed paths,
  55 untracked; crate source under `src/` including in-file tests
  `+5381/-4653/net +728`, tests/examples/tooling outside `src/`
  `+5639/-2484/net +3155`, docs and README `+13790/-1858/net +11932`;
  public types in this stage `+0/-0`. The prior recovery checkpoint records
  authorization above the cumulative task thresholds.

### 2026-10-01 local worker-preparation custody evidence

- The root lock still selects Behavior Core/Actors 0.19.0 from `e5c703e` and
  Macros 0.13.0 from `3f08364`, with Address 0.3.0, Communication 0.1.2,
  Bombay-private Observe, and Timers at `13e884d`. The selected Behavior
  instructions, `PrepareWorkers`/`WorkerPreparation` source contract, pool
  shutdown transition and tests, existing Bombay adapter, and the primitive
  ownership map were rechecked. The owner remote HEAD is still `e5c703e`;
  `cargo search` still lists Actors 0.19.0. No newly selected owner contract
  resolves the separate supervisor diagnostic-ingress blocker.
- This evidence-only stage adds two tests to the existing Bombay preparation
  module. One returns the exact source action as `Unattempted` during pool
  shutdown, checks its source/role identity before return, observes terminal
  stop, and proves the source operation was never called. The other holds the
  asynchronous source's first preparation after it starts, requests shutdown,
  then releases and returns the accepted preparation; the pool stops without
  creating a replacement. The existing source-rejection test now also returns
  the exact rejected action to its pool and observes one diagnostic with no
  replacement or repeat preparation. No production adapter, public type,
  policy, or owner template changed.
- Focused locked debug and optimized Bombay tests each pass all five
  preparation cases. Strict library Clippy passes. In an isolated copy, making
  the supposedly unattempted source action actually run fails the no-call
  assertion; omitting shutdown before the late prepared return fails the
  terminal-stop assertion. Both inversions were restored. These prove local
  custody and causal order only; multi-role preparation order and later-role
  rejection, runtime retirement of an in-flight source, and full supervisor
  diagnostics remain open.
- Complete tracked/untracked checkpoint against `HEAD`: 190 changed paths,
  54 untracked; crate source under `src/` including in-file tests
  `+5373/-4653/net +720`, tests/examples/tooling outside `src/`
  `+5367/-2484/net +2883`, docs and README
  `+13733/-1858/net +11875`. Stage-local production code `+0/-0`,
  in-file tests approximately `+90/-0`, public types `+0/-0`. The prior
  recovery checkpoint records authorization above cumulative thresholds.

### 2026-10-01 temporary-pool source stage

- The selected Core/Actors 0.19.0 source at `e5c703e` declares
  `WorkerSource<Role, Worker, Plan> for Never` with both rejection types
  `Never`; its `PoolRecovery::temporary` contains no replacement source.
  The root lock and sole Timers patch remain as recorded below. The exact
  selected Behavior instructions and the relevant pool source, tests,
  Bombay `WorkerPreparationSource`, local interpreter, Communication,
  Address, Observe, and Timers contracts were rechecked for this stage.
  Behavior Actors owns the temporary recovery policy and source action;
  Bombay owns only the concrete preparation port. No worker preparation can
  occur from the uninhabited source, but the generic runtime interpreter
  still requires its trait implementation to type-check the complete closed
  effect product.
- Exact independent blocker: a concrete `App` with one temporary FIFO pool
  and one worker fails E0277 because `Never` lacks
  `WorkerPreparationSource<WorkerRole, SearchWorker, ActivationNotice>`. The
  smallest regression is the same end-to-end pool startup/shutdown test with
  an activation handshake and terminal projection. It must fail against
  the current selected build before any production edit; after the fix it
  must pass debug and optimized with exact activation/retirement traces.
- Expected files: `crates/bombay/src/worker_preparation.rs`, one pool runtime
  integration test, this ledger, and `docs/todo.md`. Forecast production
  `+20/-0/net +20`, tests `+140/-0/net +140`, public types `+0/-0`.
  Reuse Behavior's existing uninhabited `Never`, the selected Actors
  `WorkerSource` implementation and temporary policy, and Bombay's existing
  `WorkerPreparationSource` port and interpreter. The direct ordinary-Rust
  implementation matches on impossible `Never`; no new request, wrapper,
  macro, or effect lane is eligible. This local stage does not settle the
  independent fixed/dynamic proxy diagnostic blocker.

### 2026-10-01 exact worker-shutdown resolution stage

- The selected Actors 0.19.0 `ShutdownEstablished<Child, Here>` owns an exact
  `ShutdownId`, returns `ItemSettlement`, and declares
  `ReturnsToEmitter<EstablishedShutdownResolved<Child::Protocol>, Here>`.
  Its FIFO pool waits for both the child stop and that resolution under
  `ActorDrainPolicy::WaitForActorGraph`. Bombay's existing
  `InterpretEstablishedShutdown` sends the exact control request, but its
  `InterpretItem<ShutdownEstablished<...>>` discards the typed return event.
  Behavior owns generic action settlement/custody; Behavior Actors owns the
  shutdown request, response, pool state, and drain policy; Communication
  owns control admission; Observe owns the exact child stop. Bombay owns only
  the concrete result injection into the parent control lane.
- Smallest regression: the temporary FIFO pool accepts and completes a real
  search job, receives its exact child-stop fact after an orderly shutdown,
  yet root termination times out because the shutdown-resolution event never
  reaches its fold. The same test with a bounded drain reaches a forced
  terminal, proving the shutdown command and worker stop path. An isolated
  trace recorded `ShutdownEstablished` as accepted and `ChildStopped` as
  delivered, with no `WorkerShutdownSettled` transition. Run the unbounded
  test against the current selected Bombay source before this edit, then
  require debug and optimized exact terminal custody after the correction.
- Expected files: `crates/bombay/src/application_runtime.rs`, the same pool
  integration test, this ledger, and `docs/todo.md`. Forecast production
  `+14/-1/net +13`, tests `+0/-0`, public types `+0/-0`. Reuse
  `EstablishedShutdownResolved::accepted/rejected`, the existing
  `ApplicationCapabilities::return_fact`, exact `ShutdownId`, and selected
  `ItemSettlement`. No new event, effect lane, wrapper, or runtime service is
  eligible. This is a separate local ARC-010 capability adapter law; the
  upstream proxy diagnostic blocker remains independent.

- Prior selected-build proof: the pool runtime fixture failed E0277 for
  missing `Never: WorkerPreparationSource` before the first local edit. The
  vacuous implementation uses two exhaustive matches on the uninhabited
  source and adds no state or fallible path. After it compiled, the same
  fixture completed a real job but timed out after five seconds with
  `WaitForActorGraph`. A bounded-drain inversion returned a terminal. In the
  isolated trace, the `ShutdownEstablished` adapter accepted the request
  and the exact `ChildStopped` fact entered the pool, which remained waiting
  for `EstablishedShutdownResolved`.
- The adapter now emits that selected owner result through the existing
  parent event ingress for both accepted and rejected shutdown attempts,
  preserving the original `ItemSettlement` independently. The unbounded
  test now passes in debug and optimized builds. It observes worker
  activation, accepted submission correlation, exact completed search
  result and role, orderly shutdown, a stopped root, and one stopped worker
  terminal with no descendants. No timer or forced retirement is used.

### 2026-10-01 public FIFO example stage

- Exact blocker: the selected `worker-pool` example still stops after its
  pure `initialize` call, although Bombay now executes a temporary FIFO pool
  and returns every emitted capability fact. The new integration regression
  is the smallest end-to-end control: one actual submitted search is
  accepted, completed with a correlated typed result, and followed by
  `WaitForActorGraph` shutdown and exact child terminal custody. The old
  example contains no submission or terminal path, so it fails this
  required policy trace by source inspection; the new regression failed
  against the prior `Never` and shutdown-resolution representations above.
- Expected files: `examples/worker-pool/src/main.rs`, its worker source,
  `docs/todo.md`, this ledger, and current example guidance. Forecast Bombay
  production `+0/-0/net 0`, public types `+0/-0`, example code
  `+125/-55/net +70`. Reuse the selected `fifo` template, Bombay `App`,
  concrete `ActorSpaces`, typed external caller, and the exact pool terminal
  projection already proven in the integration regression. No pool policy is
  copied into an application Behavior and no new runtime abstraction or
  macro is eligible. The supervisor example stays blocked on selected owner
  diagnostic ingress.
- The executable example now uses the selected `fifo` constructor and a
  concrete three-space `App`. It admits one search, observes accepted and
  completed customer outcomes with the same job ID, then sends the pool's
  own `FifoCommand::Shutdown` and retains stopped root/worker terminals.
  The public binary passes through pinned Nix; its example test passes in
  debug and optimized builds. The independent `fifo_pool_runtime` test also
  passes in both profiles. After this stage, complete locked Bombay
  workspace tests, strict all-target Clippy, rustfmt check, and diff
  whitespace check pass. An inversion omitting the exact shutdown return
  event timed out under `WaitForActorGraph`; the corrected adapter passed.
- All six selected Actors requests declaring `ReturnsToEmitter` were audited:
  established creation, established observation and cancellation, worker
  initialization, worker activation, and exact established shutdown now
  inject their selected typed results. This adapter returns the selected
  rejection as a distinct event while preserving the exact rejected
  `ItemSettlement`. No product-wide result traversal was added.
- The independent FIFO runtime regression now holds its activation plan
  behind an explicit oneshot release. It admits the job while the only
  worker cannot yet run, then releases activation and observes the same job
  complete before unbounded orderly drain. A temporary zero-capacity backlog
  inversion failed at the exact accepted-customer-outcome assertion, and the
  positive capacity was restored. This proves queued admission and activation
  ordering without a sleep or a model copied from the pool implementation.
- Complete tracked/untracked checkpoint against `HEAD`, measured by
  `git diff --numstat` plus untracked file line counts: 190 changed paths,
  54 untracked; crate source under `src/` including in-file tests
  `+5283/-4653/net +630`, tests/examples/tooling outside `src/`
  `+5385/-2501/net +2884`, docs and README
  `+13674/-1841/net +11833`. Since the preceding research checkpoint,
  source moved `+38/-4/net +34`, tests/examples/tooling
  `+366/-37/net +329`, docs `+228/-3/net +225`; public types
  `+0/-0`. The recovery checkpoint records authorization above cumulative
  thresholds. ARC-010 remains `blocked` on the selected owner proxy report;
  this local stage does not satisfy TEST-025's adverse and keyed-pool cases.

### 2026-10-01 selected-release audit and AssignWorker stage

- State: `active` after TEST-008; selected `Cargo.lock` and the sole Timers
  `[patch]` still choose Behavior Core/Actors 0.19.0 at
  `e5c703e966eba4d2a15fe2c129594f57ae2270fc`, Macros 0.13.0 at
  `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, Timers `13e884da7ab41781f52337b0038060e375b00ee0`,
  and Bombay-private Observe. The exact selected Behavior instructions were
  read. The 0.19.0 `BeginActivation`, `InitializeWorker`, `DiagnosticAction`,
  `CustomerDelivery`, `AssignWorker`, and `ProxyOperation` source contracts,
  upstream examples/tests, existing Bombay application interpreters, and the
  schema-3 capability inventory were inspected. The earlier 0.17.0 blocker
  below is a dated stage record; 0.19.0 supplies `AssignWorker::settle` and
  `ProxyOperation::settle` with exact rejected source reconstruction.
- Ownership: Behavior owns ordered `InterpretItem`/`ItemSettlement` and typed
  source-action custody. Behavior Actors owns every atomic request, admission
  result, correlation, and pool/supervisor transition. Communication owns exact
  accepted/rejected worker mailbox delivery; Address owns any logical route;
  Observe retains terminal facts; Timers owns scheduled actions. Bombay only
  composes the existing lower typed interpreters at its application capability
  boundary. ARC-010 unblocks TEST-025 and ARC-009 (and DIST1 in the wider
  reciprocal graph).
- Exact next blocker: `ApplicationCapabilities` has an exact established
  delivery interpreter but no `InterpretItem<AssignWorker<P, Job>, E, Path>`.
  The upstream `settle` method now delegates exactly that lower delivery and
  reconstructs the original request on rejection or corruption. No new
  router, worker request, or error vocabulary is needed.
- Smallest prior-representation regression: a compile-only static requirement
  for the concrete Bombay application capability to interpret the exact
  `AssignWorker` item. It must fail with the missing `InterpretItem` bound
  before the implementation and pass afterwards; later end-to-end pool tests
  will verify accepted/rejected/unattempted source custody, lifecycle, and
  terminal traces. The source action is affine and cannot be hand-constructed
  by an application test.
- Expected task-local paths: `application_runtime.rs`, one compile-only test,
  this ledger, and `docs/todo.md`. Forecast production `+25/-0/net +25`,
  tests `+30/-0/net +30`, public API `+0/-0` types. Reuse upstream
  `AssignWorker::settle`, existing `EstablishedDelivery<P>` interpretation,
  `ApplicationCapabilities`, and the exact `ItemSettlement` product. No
  wrapper, builder, trait, facade, or public product is planned. This stage
  cannot claim ARC-010 feature completion until the other five interpreters
  and TEST-025 laws pass.
- AssignWorker result: the static requirement failed against the previous
  `ApplicationCapabilities` with `E0277` naming the exact missing request.
  Its new interpretation calls the upstream `AssignWorker::settle` against
  Bombay's existing `EstablishedDelivery` interpreter. The accepted receipt,
  exact rejected request/payload, and corruption reconstruction remain
  upstream-owned; Bombay adds no error mapping or source-action state.
  Focused debug and optimized tests pass. This is a compile-contract stage;
  the cross-template observable trace remains required by TEST-025.
  Complete tracked/untracked checkpoint: 184 changed paths, 49 untracked;
  production `+4628/-4543/net +85` (net `+35` since ARC-002, including the
  embedded compile test), tests/examples/fuzz/benches outside `src/`
  `+4869/-2373/net +2496`, docs `+12194/-1834/net +10360`, whole tree
  `+21788/-8793/net +12995`. Public types `+0/-0` in this stage. The
  production estimate was exceeded by embedded static evidence and import
  formatting; no new production abstraction was added.
- Next ARC-010 stage, `CustomerDelivery<P>`: the locked Actors 0.19.0 enum
  owns four named logical/exact outcome variants, including an original
  `ReplyRoute<P>` that must remain with a rejected submission. Bombay already
  interprets both lower `Delivery<P>` and `EstablishedDelivery<P>` leaves.
  The exact blocker is their missing named product interpretation. A concrete
  static requirement must first fail for the missing `InterpretItem`, then the
  new interpreter must preserve all four variants and return each original
  delivery/customer route on rejection. Expected files are
  `application_runtime.rs`, this ledger, `docs/todo.md`, and focused source
  tests; forecast production `+75/-0/net +75`, tests `+30/-0/net +30`, public
  types `+0/-0`. Reuse the two existing lower delivery interpreters, upstream
  `CustomerDelivery` and `ReplyDelivery`, and Behavior's exact
  `ItemSettlement`; no new wrapper, error, route, macro, or policy is eligible.
- CustomerDelivery result: its concrete static requirement failed against the
  previous Bombay capability with `E0277`. The new interpreter delegates each
  named logical or exact leaf to the existing lower delivery interpreter and
  reunites the complete upstream variant, including the original customer
  route on rejected submissions. The small private reunion function transforms
  one actual lower settlement; it owns no new state or policy. Focused debug
  and optimized compilation passes, as does strict all-target Clippy. Its
  observable four-variant trace remains required with TEST-025, so ARC-010
  stays active. Complete tracked/untracked checkpoint: 184 changed paths,
  49 untracked; production `+4729/-4552/net +177` (net `+92` since the
  previous checkpoint, including embedded compile evidence), tests/examples/
  fuzz/benches outside `src/` `+4869/-2373/net +2496`, docs
  `+12222/-1834/net +10388`, whole tree `+21917/-8802/net +13115`.
  Public types `+0/-0` in this stage. The production forecast was exceeded
  because all four exhaustive variants and their exact corrupt/rejected
  reconstruction required an explicit typed reunion; no new public surface
  resulted.
- Next ARC-010 stage, `DiagnosticAction<Route, Diagnostic>`: selected Actors
  0.19.0 statically seals three route families (`Recipient<P>`,
  `EstablishedRecipient<P>`, and `Infallible`) and retains route-free terminal
  diagnostic custody as an accepted action. Bombay already interprets the
  two routed delivery leaves. The exact blocker is missing action-level
  interpretation for all three route types. First add a concrete static
  requirement that fails on this omission, then compose the existing leaf
  interpreters and return terminal diagnostic data unchanged. Forecast paths:
  `application_runtime.rs`, this ledger, `docs/todo.md`, focused tests;
  production `+90/-0/net +90`, tests `+35/-0/net +35`, public types `+0/-0`.
  Reuse upstream `DiagnosticAction`/`DiagnosticAccepted` and existing
  `Delivery`/`EstablishedDelivery` interpretation. No diagnostic store,
  router, error, policy, wrapper, or macro is eligible.
- DiagnosticAction result: the concrete static requirement failed for all
  three sealed route families with `E0277` against the previous capability.
  The two routed interpretations delegate to the existing delivery leaves and
  reconstruct the original diagnostic action on rejection or corruption; the
  terminal route returns the unchanged diagnostic. Focused debug and optimized
  tests and strict all-target Clippy pass. End-to-end diagnostic traces remain
  required with TEST-025. Complete tracked/untracked checkpoint, measured
  against `HEAD` with line-sequence comparison including untracked files:
  184 changed paths, 49 untracked; production `+4825/-4508/net +317`,
  tests/examples/fuzz/benches outside `src/` `+5003/-2454/net +2549`, docs
  `+12247/-1816/net +10431`, whole tree `+22075/-8778/net +13297`.
  Public types `+0/-0`. This count uses a more exact line comparison than
  earlier stage checkpoints, so its difference is not the stage delta.
- Next ARC-010 stage, `InitializeWorker<W, P>`: selected Actors 0.19.0
  retains the affine plan, worker attempt, initialization attempt, and exact
  established target, and `resolve` returns one of ReadyForActivation,
  EffectsRejected, or Stopped without losing those facts. Bombay child
  creation already waits for the complete initialization settlement before it
  publishes the established binding. Thus an established live target is ready;
  an already published exact termination is stopped. Child creation rejection
  cannot yield an `InitializeWorker` request because the proxy never acquires
  an established worker. The exact blocker is the absent interpreter for this
  request and its typed return event. A concrete static requirement using the
  selected `StableProxy` event must fail against the prior capability before
  the edit. Expected paths: `application_runtime.rs`, this ledger, focused
  compile and observable tests, and eventually `docs/todo.md`; forecast
  production `+45/-0/net +45`, test `+20/-0/net +20`, public types `+0/-0`.
  Reuse `InitializeWorker::resolve`, the exact installed endpoint and Observe
  termination, existing `return_fact`, and upstream `ChildStopped`; no
  initialization state, service, or duplicated outcome type is eligible.
- InitializeWorker result: the concrete selected `StableProxy` requirement
  failed with `E0277` against the prior capability and now passes in debug
  and optimized builds. The interpreter uses the exact established endpoint's
  Observe fact to choose Stopped or ReadyForActivation, then calls upstream
  `resolve` and returns its typed report to the proxy. Creation's settled
  installation law excludes an effect-rejected request at this boundary.
  An end-to-end proxy trace remains required before this law is considered
  verified, and ARC-010 remains active. No new public type was added.
- Next ARC-010 stage, `BeginActivation<W, P>`: selected Actors 0.19.0 owns
  the single-use permit and plan. Its `started` report must enter the actor
  control lane before the plan is polled; the completed ready/rejected report
  must return on that same typed lane, or enter existing actor-owned
  `ActivationTasks` retirement custody if the lane closes. The exact blocker
  is the missing interpreter. A concrete `StableProxy` compile requirement
  must fail first. Expected paths are `application_runtime.rs`, this ledger,
  focused tests, and eventually `docs/todo.md`; forecast production
  `+45/-0/net +45`, test `+20/-0/net +20`, public types `+0/-0`.
  Reuse upstream `BeginActivation::started/activate`, existing `return_fact`,
  control sender, and `ActivationTasks`; no second activation service or
  intermediate event product is eligible. An end-to-end trace must prove
  started-before-ready, rejection custody, and retirement ordering.
- BeginActivation compile result: the selected `StableProxy` requirement failed
  with `E0277` against the prior capability and now passes in debug. Bombay
  enqueues upstream's Started report before it spawns the plan future; the
  actor-owned `ActivationTasks` retains an exact completed event if its typed
  control lane closes. Optimized and observable tests remain pending. This is
  not ARC-010 feature completion.
- Next ARC-010 stage, `ProxyOperation<Here, Worker, Plan>`: both selected
  supervisors emit this source action for their first `StableProxy` child
  occurrence. Actors 0.19.0 owns `settle`, the operation witness, and exact
  accepted/rejected reconstruction through `ProxyControlAdmission`. Bombay
  has occurrence-local child bindings and a typed control lane, but no
  `ProxyControlAdmission` or action interpreter. The smallest static
  requirement on a concrete parent with a `StableProxy` birth must fail before
  implementation. Accepted admission must return the exact
  `EstablishedActor`; missing/rejected binding must return MissingBinding and
  the full control; closed control must recover the original `ProxyControl`
  through the selected `RecoverEvent` implementation. Expected paths:
  `application_runtime.rs`, this ledger, focused tests, and `docs/todo.md`;
  forecast production `+75/-0/net +75`, tests `+35/-0/net +35`, public types
  `+0/-0`. Reuse `ChildBindingAt<ChildHead>`, existing control send,
  `InstalledActor`, and upstream `ProxyOperation::settle`; no new proxy
  service, binding map, error, or receipt type is eligible.
- ProxyOperation compile result: the concrete parent-with-proxy-birth
  requirement failed with `E0277` before the interpreter and now passes. The
  new `ProxyControlAdmission` on the existing capability reads only its
  `ChildHead` binding, returns the exact installed actor after accepted
  control send, returns MissingBinding for absent/rejected binding, and
  recovers the whole upstream control when the typed lane closes. Upstream
  `ProxyOperation::settle` owns the operation witness and source-action
  reconstruction. All six missing atomic compile contracts now pass in debug;
  the locked workspace suite and strict all-target Clippy passed after the
  manifest was updated to `implemented`. They are not yet runtime verified.
- End-to-end blocker probe: `docs/research-probes/fixed-supervisor-runtime.rs`
  is a concrete application with a fixed supervisor, exact stable proxy,
  worker activation notice, and terminal tree assertion. Against the current
  Bombay runtime it fails compilation with two independent mismatches:
  `StableProxy::Event` has no `InjectEvent<ShutdownRequested, Here>`, although
  Bombay's generic owned-child launch requires it; and
  `FixedSupervisorEvent` admits `ChildReport<ProxyOutcome/ProxyDiagnostic>`
  from `Births<StableProxy<...>>`, while Bombay's parent-report sink selects
  `ChildHead`. The raw root also needs an ordinary `StopOnShutdown` wrapper,
  and a local host product must explicitly host the supervisor's reply
  protocols. These are concrete lower-boundary needs, not a reason to invent
  another supervisor or actor contract. The probe remains out of the Cargo
  test target until all selected contracts compile.
- Change ledger for the runtime alignment: the smallest failing regression is
  that probe's launch bound; restore its old bound/source to demonstrate the
  original compile denial. Expected files are `local.rs`, `launch.rs`,
  `application_runtime.rs`, `reports.rs`, one existing explicit parent-report
  test, the focused runtime probe, this ledger, and `docs/todo.md`. Forecast
  production `+8/-12/net -4`, test/probe `+0/-0/net 0` before the host product,
  public types `+0/-0`. The child task's owner-cancellation channel already
  handles forced retirement without a `ShutdownRequested` event. Reuse the
  selected Behavior `Births` report source and existing child task ownership.
  No new shutdown service, report adapter, or wrapper is eligible.
- Runtime-alignment result: removing the unused generic
  `ShutdownRequested` bound from owned child launch lets the selected
  `StableProxy` enter Bombay's existing owner-cancelled task hierarchy. The
  parent report sink now uses the selected parent `Birth` source; the existing
  explicit parent-report integration test was updated and passes. Wrapping the
  root fixed supervisor with existing `StopOnShutdown` resolves its caller
  shutdown authority. The executable probe then isolates one upstream
  contract gap: `StableProxy` always exposes
  `ReportToParent<ProxyDiagnostic<Worker, Plan>>`, but selected Actors 0.19.0
  `FixedSupervisorEvent` implements no `EventIngress` for that report. It only
  accepts `ProxyOutcome`, and no selected atomic supervisor event accepts
  `ProxyDiagnostic`. Bombay cannot discharge or deliver this authoritative
  typed fact without a second policy or a change by its Behavior Actors owner.
  A complete runtime test therefore remains a deliberately noncompiled
  research probe at `docs/research-probes/fixed-supervisor-runtime.rs`.
  crates.io search still lists 0.19.0 as the latest selected Actors release.
  ARC-010 is `blocked` on a Behavior Actors release defining the diagnostic
  consumer; TEST-025 and ARC-009 remain blocked by ARC-010. ARC-012 has no
  such dependency and is the next active queue item.
- Complete tracked/untracked checkpoint after this stage, using line-sequence
  comparison against `HEAD`: 185 changed paths, 50 untracked; production
  `+5033/-4516/net +517`, tests/examples/fuzz/benches outside `src/`
  `+5008/-2454/net +2554`, docs `+12522/-1816/net +10706`, whole tree
  `+22563/-8786/net +13777`. No new public type in the six-interpreter or
  alignment stages. The production net exceeds 500; the repository recovery
  checkpoint records the user's explicit authorization to continue production
  edits above the 15-path/500-line threshold in this inherited tree. These
  figures include all earlier inherited work, not solely ARC-010.

### 2026-10-01 selected-archive provenance and blocker recheck

- `Cargo.lock` pins registry checksums, not an owner Git commit. The selected
  Core and Actors 0.19.0 archives' `.cargo_vcs_info.json` names
  `e5c703e966eba4d2a15fe2c129594f57ae2270fc`; the Macros 0.13.0
  archive names `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`.
  Earlier entries had called release commit `379f845` the selected revision.
  The exact selected `AGENTS.md` content was read and SHA-256 compared across
  all three archived revisions and the release commit: all equal
  `2b7a9195b27f073fec18426da43e9840f8ef668f333b9ad55934f520a37ae226`.
  The selected Core/Actors source differs from the release commit only in
  README content; Macros differs only in a fixture lock. The previously
  verified semantics and tests therefore apply to the actual locked source.
- Reinspection of the selected source confirms both `FixedSupervisorEvent`
  and `DynamicSupervisorEvent` admit
  `ChildReport<ProxyOutcome<Worker, Plan>>` from
  `Births<StableProxy<Worker, Plan>>`; neither admits
  `ChildReport<ProxyDiagnostic<Worker, Plan>>`. `StableProxy` declares both
  `ReportToParent` lanes and emits the diagnostic lane for unexpected worker
  start, stop, initialization, activation, and shutdown results. Both
  supervisors own an explicit diagnostic disposition, and their existing
  `UnexpectedInput` diagnostic can preserve a complete typed input, but the
  owning library has not defined how these proxy diagnostic reports enter
  and affect its roster. Bombay cannot implement the external trait for the
  external event type or choose the supervisor's policy. The executable
  probe remains blocked on a Behavior Actors contract change.
- A compile-only witness using the selected upstream
  `tests/proxy_command_recovery.rs` concrete `Worker`/`Hydrate` fixture was
  applied in an isolated checkout at `e5c703e`. The pinned Nix test command
  failed with the intended E0277 twice: neither fixed nor dynamic event type
  implements `EventIngress<Births<StableProxy<Worker, Hydrate>>,
  ChildReport<ProxyDiagnostic<Worker, Hydrate>>>`. The witness patch and
  proposed acceptance criteria are preserved under `docs/research-probes/`;
  no upstream production contract has been invented or published.
- Isolated owner-side experiment, before any production edit: the exact
  blocker is those two E0277 denials. The smallest end-to-end witness is the
  same `fixed-supervisor-runtime.rs` probe after the selected child report
  ingress compiles. The user-level law is that a child proxy's authoritative
  diagnostic travels through its parent's declared birth occurrence and the
  supervisor's existing `DiagnosticDisposition`, retaining the full
  diagnostic and child correlation. This is a derived Bombay composition
  policy, not a general actor-model guarantee; the owner research audit
  already records that fixed supervision owns proxy reports. Expected owner
  files in an isolated checkout: fixed/dynamic `event.rs`, fixed/dynamic
  `mod.rs`, dynamic `diagnostic.rs`, and a focused pure-fold test. Forecast
  owner production `+60/-0/net +60`, tests `+45/-0/net +45`, public types
  `+0/-0` (variants on existing sums). Reuse Behavior `EventIngress`,
  `ChildReport`, `Births`, and both existing supervisor diagnostic
  dispositions. This experiment does not change Bombay's locked dependency
  or assert an upstream policy has been accepted.
- The isolated owner-side candidate is preserved as
  `docs/research-probes/behavior-actors-proxy-diagnostic-owner.patch` (eight
  owner paths, production `+110/-0/net +110`, test `+128/-17/net +111`,
  public types `+0/-0`). Fixed supervision retains the complete proxy report
  as an `UnexpectedInput` diagnostic; dynamic supervision routes it through
  its existing diagnostic disposition with one new variant on the existing
  `DynamicDiagnostic` sum. The source and pure-fold tests pass across all
  three affected owner test binaries in debug and optimized builds (30, 99,
  54 tests); the complete owner crate and workspace test suites and strict
  workspace all-target Clippy pass. Temporarily dropping the report in each
  supervisor made its focused
  pure-fold test fail for the intended law, and both changes were restored.
  `git apply --reverse --check` confirms the patch artifact matches the
  isolated checkout. The same source is on the isolated, unpushed local
  branch `codex/proxy-diagnostic-parent-ingress` at
  `e55c2cb6c3a3c10bc5ee96c7d817a39a4a8e239a`. Bombay's selected
  immutable dependency is unchanged; owner review, publication, and adoption
  remain outside this candidate.
- The same clean local owner branch also passes its authoritative
  `nix flake check` on aarch64-darwin: build, nextest, Clippy, docs/doctests,
  formatting, TOML formatting, package, audit, and deny checks are green.
  This verifies the candidate for review; it does not select it in Bombay.
- An isolated Bombay copy selected that candidate through temporary Cargo
  path patches. Its concrete four-space fixed-supervisor application probe
  compiled and passed in debug and optimized builds through the pinned Nix
  shell after the local exact-shutdown resolution stage. The first probe
  requested shutdown through the outer `StopOnShutdown` wrapper and returned
  `OwnerCancelled` descendants; it did not exercise the supervisor's drain
  policy. The corrected probe sends `FixedCommand::Shutdown` to the
  supervisor itself. It now observes activation, accepted command, normal
  root termination, completed proxy and worker descendants, and root/child
  origin forms. The corrected source is preserved at
  `docs/research-probes/fixed-supervisor-runtime.rs`; the main Cargo manifest
  and lock still select the immutable 0.19.0 archives. Strict Clippy passes
  for the isolated Bombay integration test. A pinned `cargo search` on
  2026-10-01 still lists 0.19.0 as latest published Actors version. The
  probe does not exercise diagnostic delivery or restart exhaustion; the
  selected Bombay build separately proves FIFO job completion and orderly
  drain. After copying the current local FIFO adapter and public example into
  the isolated candidate selection, complete Bombay workspace tests and
  strict workspace all-target Clippy pass there too. TEST-025 stays blocked
  on the owner contract and its remaining adverse policy traces.
- Complete tracked/untracked checkpoint before the local pool stages, measured
  with `git diff --numstat` plus untracked file line counts: 189 changed paths,
  53 untracked; crate source under `src/` including in-file tests
  `+5245/-4649/net +596`, tests/examples/tooling outside `src/`
  `+5019/-2464/net +2555`, docs and README
  `+13446/-1838/net +11608`. This research stage adds no Bombay production
  line or public type. The earlier recovery checkpoint records authorization
  above cumulative thresholds.

- State: `ready` after the independently verified worker-preparation stage;
  BEH3's exact settlement methods are selected in Actors 0.19.0. TEST-008
  verified the earlier locked capability inventory; the 0.19.0 inventory
  must be reverified feature-locally before implementation.
- Selected contracts: Behavior Core and Actors 0.17.0 and Macros 0.12.0 from
  `435560ce7bea8ad3330ee2d42e5034f837a80602`; Address 0.2.0;
  Communication 0.1.2; Bombay-private Observe; and Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`. The only root patch is
  that Timers revision. The exact Behavior `AGENTS.md` was reread.
- Ownership: Behavior owns `InterpretItem`, `ItemSettlement`, source-action
  custody, and ordered products. Behavior Actors owns `PrepareWorkers`, its
  nonempty role sequence, `WorkerSource` type declaration, worker submission,
  and the supervisor/pool transitions. Bombay owns only the concrete source
  capability and typed interpretation outside the pure Behavior transition.
  Address, Communication, Observe, and Timers remain their existing primitive
  owners; worker preparation does not add a substitute for any of them.
- Exact blocker: the selected `PrepareWorkers` request is emitted by fixed
  supervisors and both pools, but `ApplicationCapabilities` has no matching
  `InterpretItem`. The supervisor and pool examples consequently stop at pure
  initialization. The schema-3 template manifest also identifies six other
  missing atomic interpreter boundaries. They remain within ARC-010 after
  this first, independently reviewed stage.
- Smallest prior-representation regression: construct one real
  `PrepareWorkers` emission through the locked template, require the local
  application's concrete interpreter to accept its exact action item, and
  observe that the current program does not compile because this capability
  is absent. A later end-to-end test must cover ordered accepted submissions,
  first-role source rejection, later per-worker rejection, unattempted custody,
  retirement, and shutdown requested during preparation. Both debug and
  optimized runs must witness the same law before broadening the change.
- Change ledger for the worker-preparation stage: expected paths are this
  ledger, `docs/todo.md`, one focused Bombay test, the application capability
  interpreter and its public import, and the relevant example and capability
  manifest (at most eight task-touched paths). Expected production source is
  `+80/-0/net +80` to `+180/-0/net +180`; tests are expected below
  `+300/-0/net +300`; public API is provisionally `+2/-0` types: one concrete
  worker-source interpreter port and one first-attempt outcome sum. These are
  ceilings for the candidate design, not an allowance to add forwarding
  wrappers. Reuse `PrepareWorkers`, `PendingWorkerPreparation`,
  `WorkerPreparation`, `WorkerSubmission`, `InterpretItem`, `ItemSettlement`,
  `ApplicationCapabilities`, and the existing actor task hierarchy. Delete the
  examples' pure-only limitation when actual policy execution is demonstrated.
  The application-facing port must perform real preparation on its first and
  later calls; a mandatory no-op admission hook is ineligible.
- Dependency edges: blocked by BEH3 after TEST-008; unblocks TEST-025 once all
  seven atomic boundaries and executable example policies are verified.
- Containment checkpoint: the inherited working tree already has 79 changed
  or untracked paths and net `+642` production lines. The user explicitly
  authorized expanded-surface production work in this existing tree on
  2026-09-28. Preserve those prior edits and report task-local as well as
  complete-tree deltas at each stage.
- Worker-preparation stage result: `WorkerPreparationSource` gives the source
  one real first-role operation with source/worker rejection distinguished,
  plus one later-role operation that cannot retroactively reject the already
  admitted source. `ApplicationCapabilities` interprets the exact upstream
  `PrepareWorkers` without another request type, positional traversal, or
  runtime capability map. The matching manifest status is now `implemented`.
  A compile-only Bombay regression failed against the prior interpreter with
  E0277 for the exact missing `InterpretItem` and passes after this stage. A
  real FIFO recovery request exercises accepted submission, worker rejection,
  and source rejection; the accepted preparation returns to the upstream pool
  and creates one replacement. Source rejection returns the request with its
  original source and first role. The later-role and shutdown-during-preparation
  inversions remain open under this blocked item.
- Stage verification: focused worker-preparation tests pass four cases in
  debug and optimized builds; the template manifest gate, complete locked
  workspace tests, strict Bombay all-target Clippy, workspace formatting, and
  diff whitespace checks pass through pinned Nix. No claim of ARC-010 feature
  completion or project-wide distillation follows from those focused gates.
- Stage checkpoint: nine task-touched paths after updating the queue and
  correcting both public example descriptions; the original eight-path
  estimate was exceeded by this required documentation audit;
  production source `+124 / -0 / net +124`; tests `+357 / -0 / net +357`;
  public API `+2 / -0` types. The complete working tree contains 81 changed
  or untracked paths and retains the user's preexisting changes. The original
  test estimate below `+300` was exceeded by the real upstream FIFO recovery
  fixture, and that expansion is recorded rather than relabeled as cleanup.

## ARC-012 — owned activation and termination phases

### 2026-10-01 selected-contract audit and owner-cancellation stage

- State: `active` after ARC-001 reached feature-complete. ARC-012 unblocks
  ARC-011; ARC-011 also awaits ARC-009. The dependency edge is reciprocal in
  `docs/todo.md`. ARC-010's external diagnostic-contract blocker is independent.
- Exact selected contracts: `Cargo.lock` resolves Behavior Core/Actors 0.19.0
  from `e5c703e966eba4d2a15fe2c129594f57ae2270fc` and Macros 0.13.0
  from `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`, Address 0.3.0,
  Communication 0.1.2, patched Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe.
  The selected Behavior `AGENTS.md` was read; its affine product and naming
  rules apply. I rechecked the selected `Behavior -> Actions` and
  `ReportTerminalOutcome`/`ShutdownRequested` contracts, the Engine
  `Environment`/`ActiveEnvironment` retirement port and `Completion` law,
  Address reservation/lease, Communication control closure, Observe affine
  pair, Timers' owned queue, Bombay launch/local/termination/terminal source,
  current integration tests, and the four normative runtime/Driver documents.
- Ownership: Behavior owns the terminal request and typed fold decision;
  Engine owns the fused completion/retirement law; Address and Communication
  own reservation/mailbox resources; Observe owns affine publication; Timers
  owns scheduling state. Bombay alone owns which local phase holds the
  publisher, owner-cancellation token, and selected terminal report. No new
  lifecycle policy, observation cell, or public runtime type is eligible.
- First exact blocker: `LocalResidual::Retired` stores
  `owner_cancellation: Option<OwnerCancellation>`, so terminal projection and
  termination publication separately reinterpret the same optional field and
  check `Completion::Exhausted` by convention. A distinct residual alternative
  can transport this authoritative cause directly. The smallest prior-shape
  regression is a compile-only match requiring `LocalResidual::OwnerCancelled`
  on the actual residual type; it fails with the missing variant before the
  change. Existing nested terminal-custody and cancellation traces must still
  prove exact selected `Crash::Cancelled` and retained descendants in debug
  and optimized builds. An inversion returning ordinary Retired for owner
  cancellation must fail those observable traces.
- Change ledger before the first production edit: expected files are
  `local.rs`, `launch.rs`, `terminal.rs`, one local compile-shape test, focused
  terminal-custody tests, this ledger, and `docs/todo.md`. Forecast production
  `+40/-25/net +15`, tests `+20/-0/net +20`, public types `+0/-0`.
  Reuse the existing `OwnerCancellation` token, `LocalResidual` product,
  `ActorRetirement::OwnerCancelled`, and `TerminationPublication`. No wrapper,
  trait, facade, or builder is added. This stage addresses cancellation
  provenance only; activation publisher custody and termination selection
  remain separate ARC-012 stages after its focused proof.

### 2026-10-01 cancellation stage proof and activation-custody stage

- The prior residual-shape witness failed with E0599 for the missing
  `OwnerCancelled` variant. The new variant carries the original
  `OwnerCancellation` token; normal retirement has no cancellation field.
  `application_terminal_custody` passed all four tests in debug and release.
  An inversion projecting cancellation as ordinary completion failed
  `root_returns_only_after_owning_ordered_direct_child_terminals` at its
  exact child-cancellation assertion; the correct projection was restored.
- Complete tracked/untracked checkpoint: 185 changed paths, 50 untracked;
  production `+5104/-4544/net +560`, tests/examples/fuzz/benches
  `+5057/-2481/net +2576`, docs `+12567/-1796/net +10771`, public types
  `+0/-0` in this stage. The earlier recovery checkpoint records the user's
  authorization to continue past the cumulative source/path thresholds.
- Activation phase inventory: before Driver publication, the prepared
  Environment owns the endpoint and reservation; the caller owns the single
  activation observation. On accepted publication, the active Environment
  sends the exact endpoint once. On pre-publication completion, activation
  failure, panic, or task abort, dropping that single sender wakes the caller,
  which joins the terminal task for the exact rejection. After publication,
  retirement owns only the termination publisher. Owned children already
  receive their private binding through `commitment` and require no second
  activation publication authority. `Arc<Mutex<Option<Publisher<_>>>>` adds
  shared custody across Environment and retirement despite this affine order.
- Next exact blocker and smallest law: root activation must settle once on
  publication and wake on every pre-publication end, including panic and
  abort, while privately bound children keep their own acknowledgement.
  Existing `spawn_root_with` and `spawn_owned_with_mode` terminal tests
  exercise those observable paths. Before a production change, run their
  focused prior-representation traces and identify the exact test for each
  terminal path. Expected files: `local.rs`, `launch.rs`, focused tests, this
  ledger and TODO. Forecast production `+18/-35/net -17`, tests `+0/-0`,
  public types `+0/-0`. Reuse Tokio's affine oneshot sender, existing
  `LocalEnvironment::Publication`, `commitment`, `startup_failure`, and
  Observe termination publisher. Delete the optional activation publisher
  and no-op publication callback; no new public abstraction is eligible.

### 2026-10-01 activation proof and terminal-selection stage

- Activation now uses one Tokio oneshot sender owned by the prepared
  Environment; successful publication consumes it and pre-publication end
  drops it. The private child binding continues through its existing
  acknowledgement. `Arc<Mutex<Option<Publisher<_>>>>`, its retirement
  duplicate, and the no-op publication callback are deleted. `launch::tests`
  (five tests) and `run_with` (12 tests, including public activation failure)
  passed debug and optimized. The `PublicationNotice` sum distinguishes an
  environment with no caller notification from one with a consuming sender.
- Complete tracked/untracked checkpoint: 185 changed paths, 50 untracked;
  production `+5120/-4606/net +514`, tests/examples/fuzz/benches
  `+5057/-2481/net +2576`, docs `+12604/-1796/net +10808`, public types
  `+0/-0` in this stage. The recovery checkpoint records authorization above
  the cumulative thresholds.
- Remaining exact blocker: selected terminal report is held in
  `Arc<Mutex<Option<Termination<_>>>>` shared between action interpretation
  and termination publication. The Driver completes interpreter retirement
  before its final `Retirement::retire` call, so the selected report can move
  through a single owned channel at that boundary. The interpreter owns a
  closed `Unselected | Selected(report)` state across action transactions;
  retirement owns the receiver. A continuing action discards its selected
  report, a stopping action retains the first selected report, and any panic,
  abort, or non-stopped completion falls back to the Driver outcome. No
  concurrent reader of the mutable selection was found.
- Prior-representation observable controls: `run_with` tests
  `terminal_outcome_report_selects_the_exact_publication_before_stop`,
  `supervision_report_selects_the_typed_failure_publication_before_stop`,
  and `supervision_report_from_a_continuing_action_cannot_override_later_shutdown`
  all pass under the shared selection. Their independent actor outcomes and
  published facts will be retained as the differential proof. A temporary
  inversion dropping the selected report must fail its exact publication
  assertion. Expected files: `termination.rs`, `reports.rs`, `launch.rs`,
  `interpret.rs`, `application_runtime.rs`, focused tests, ledger, TODO.
  Forecast production `+35/-35/net 0`, tests `+0/-0`, public types `+0/-0`.
  Reuse Tokio oneshot, existing `TerminationSelection`,
  `TerminalReportDisposition`, and `TerminationPublication`; delete the
  shared mutex and Arc without adding a second observation mechanism.

### 2026-10-01 ARC-012 feature gate

- State: `feature-complete`; final repository-wide minimization remains.
  `ARC-011` still awaits `ARC-009`, which in turn awaits blocked `ARC-010`.
  No dependency edge changed and no queue row became ready.
- Phase proof: `PublicationNotice::Unobserved | Notify` owns the optional
  caller notification by variant; the root notification is a single consumed
  oneshot sender. `LocalResidual::Retired | OwnerCancelled` distinguishes
  normal retirement from the exact owner token. `LocalTerminalReports` owns
  `TerminationSelection::Unselected | Selected`, retains only a stopping
  action's first report, and hands that report through one consumed sender to
  `TerminationPublication`. The receiver is closed when no report was
  selected; an `Empty` result at retirement violates Driver order and is
  rejected. Panic and cancellation never apply an earlier selected report.
- Observable controls: debug and optimized `launch::tests` (five), `run_with`
  (12), `application_terminal_custody` (four), and the two new focused
  termination cases passed. `run_with` covers normal stop, typed terminal
  selection, continuing-action discard, and activation rejection;
  `launch::tests` covers pre-publication end and reservation rejection;
  terminal custody covers owner cancellation and nested descendants. The
  focused terminal cases cover stopped/exhausted completion, activation
  failure, panic, task abort classification, and owner-cancellation report
  discard. The full locked workspace test gate and strict all-target Clippy
  passed, as did pinned rustfmt check after formatting.
- Adverse controls: mapping owner cancellation to ordinary completion fails
  the exact child-custody assertion; replacing selected terminal publication
  with the fallback fails the public `LinkDied` assertion (`Normal` observed).
  Both mutations were restored. The old shared publisher and mutex were
  removed by source inspection, and the application and child traces stayed
  equal across the representation change.
- Complete tracked/untracked checkpoint: 186 changed paths, 50 untracked;
  source under `src/` including in-file tests `+5244/-4657/net +587`,
  tests/examples/fuzz/benches outside `src/` `+5057/-2481/net +2576`, docs
  `+12675/-1796/net +10879`, public types `+0/-0` in ARC-012. The recovery
  checkpoint records the user's authorization above cumulative thresholds.

## Public examples

Seven public packages exercise caller-visible behavior:

- `counter`: state, request/reply, domain error, and lifecycle shutdown;
- `actor-templates`: timeout and shutdown composition through `ActorExt`;
- `application-topology`: heterogeneous children, same-action delivery, and
  ordered shutdown;
- `supervision`: live fixed-supervisor worker replacement, shutdown, and exact
  root/proxy/two-worker retirement under the selected owner diagnostic ingress;
- `worker-pool`: live FIFO admission, assignment, completion, customer result,
  ordered drain, backlog rejection, permanent replacement, and both assigned
  job interruption choices;
- `axum`: HTTP translation through a truthful external actor boundary;
- `entity`: hydration, stable identity, passivation, reactivation, exact
  retirement, and joined family shutdown.

Most examples use `Application`. Entity deliberately uses advanced `App`
because its definition statically names the concrete logical protocols it
hosts. Hiding that product would require erasure or dishonest automatic host
materialization.

## Verification baseline

The final tree passed the following gates through pinned Nix on 2026-09-15:

```text
nix develop -c cargo fmt --all -- --check
nix develop -c cargo build --locked --workspace
nix develop -c cargo test --locked --workspace
nix develop -c cargo nextest run --workspace
nix develop -c cargo clippy --locked --workspace --all-targets -- -D warnings
nix develop -c env RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps
nix develop -c cargo test --locked --release -p bombay-rs --test entity_application
nix develop -c cargo run --locked -p bombay-example-entity
nix develop -c cargo run --locked --release -p bombay-example-entity
nix flake check path:.
git diff --check
```

Nextest passed 503 tests across 49 binaries. All 17 declared flake checks
passed. The four private-Observe fuzz targets each passed 10,000 bounded runs
through the pinned fuzz shell. Entity, Machine, and Observe concurrency laws
also pass their optimized Loom gates.

## Downstream boundary

### MNE1 — durable Mnesis execution

- State: `blocked` by an external release and dependency-graph alignment, not
  by a missing Bombay runtime contract.
- Requirement: Mnesis-Bombay must select current Bombay and the same exact
  Behavior generation, hydrate aggregate state before Entity routability,
  execute its existing durable handler contract, and preserve admission,
  decision, append/conflict, uncertain commit, and durable completion as
  distinct typed facts.
- Bombay support: native Entity installation, stable identity, truthful
  provenance, passivation, retirement, capacity, family shutdown, and task
  joining are available.
- Rejected shortcut: an admitted mailbox command is not evidence of a durable
  commit. Bombay will not add a second persistence abstraction, erased command
  registry, or compatibility actor to conceal version skew.
- Unblocks: a future Mnesis-Bombay release using the current Bombay contract.
