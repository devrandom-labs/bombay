# Bombay audit TODO and session handoff

Status: complete. All 45 canonical rows are terminal after the Linux fuzz
runner recheck. The [PRD inventory](prd-backlog/README.md) and
[evidence report](prd-backlog/evidence.md) supply additional research; the
canonical queue below controls this goal. Historical claims in detailed
records require re-verification against the current lock and source.

This is the durable handoff for the repository-wide architecture and test
audit performed on 2026-09-22 and extended crate-by-crate on 2026-09-23. It
records confirmed defects, weak or redundant evidence, missing tests,
dependency ownership, architectural duplication, and the proof required to
close each item. A later session should be able to resume from this file
without relying on chat history.

This file does not replace the live feature graph in
`docs/open-design-ledger.md`. Before changing production code, add or update the
applicable ledger entry and its change ledger there. Never mark an item complete
only because the current implementation passes its existing tests.

## Audit snapshot

- The worktree already contained 67 modified tracked paths before this file was
  created. Treat those changes as user work. Inspect the complete diff before
  touching an overlapping file.
- `.research/` was removed from the repository and moved to the recoverable
  location `/Users/joel/.Trash/bombay-research-20260922`.
- The source inventory found 419 Rust test attributes in 69 source files and
  102 dedicated test, benchmark, fuzz-target, or example Rust surfaces. Counts
  include configuration-specific tests that do not run in one invocation.
- Locked contracts inspected for this audit:
  `bombay-behavior 0.17.0`, `bombay-behavior-actors 0.17.0`,
  `bombay-behavior-macros 0.12.0`, `bombay-address 0.2.0`,
  `bombay-communication 0.1.2`, and `bombay-timers` at
  `13e884da7ab41781f52337b0038060e375b00ee0`.
- The complete Behavior 0.17 `AGENTS.md` was read from upstream revision
  `435560ce7bea8ad3330ee2d42e5034f837a80602`. Re-resolve and reread the selected
  revision if `Cargo.lock` changes.
- The architecture audit inspected every workspace package, both excluded fuzz
  packages, the public examples, all Bombay production modules, and the locked
  Behavior/Behavior Actors source relevant to child products, source actions,
  stable proxies, dynamic supervision, worker pools, shutdown, and discovery.
- `nix develop -c cargo tree --locked --duplicates` found no evidence of a
  duplicated Bombay domain implementation. Its notable version splits were
  Syn 2/Syn 3, `rustc-hash` 1/2, and `getrandom` 0.3/0.4. ARC-005 migrated
  Bombay's macro crate to Syn 3; Syn 2 remains through transitive
  `zerocopy-derive`. The other splits are transitive tooling or dependency
  choices.
- The original first-party coverage baseline was 88.28% regions, 88.24%
  functions, and 86.81% lines before TEST-018 removed duplicate ordinary
  Observe execution. There is no enforced threshold, and several low-coverage
  public/runtime areas remain. TEST-020 must establish a fresh baseline.

## Persistent goal contract

The execution goal is:

> Resolve every item in this document through the smallest Behavior-first
> change, prove each result against the prior representation, distill the
> architecture, and finish with every required repository gate green.

Once started, this goal remains active across sessions. A session ending, a
green focused test, a blocked item, or a feature-complete implementation is not
completion. Use this file as the goal's durable control plane:

- **Authoritative queue:** only the state and dependency columns in the
  canonical queue below control selection. Prose `Status` lines in the detailed
  findings record audit confidence and historical context; they are not queue
  state.
- **One active item:** at most one row may be `active`. Resume it before
  selecting anything else.
- **Deterministic selection:** when no row is active, choose the `ready` row
  with the lowest priority number (`P0` before `P1` before `P2`), then the
  lowest sequence number. Do not choose a convenient later item.
- **No silent scope growth:** one iteration owns one ID. A failing regression
  may be added before that ID's production fix, but a second ID begins only
  after the first has a resolution record and state transition.
- **No false completion:** call the goal complete only after the terminal audit
  below succeeds and no row is `ready`, `active`, `blocked`, or
  `feature-complete`.
- **No premature blocking:** if one item cannot proceed, record the exact
  external blocker and select another ready item. Mark the overall goal blocked
  only when no useful item remains and the same external blocker has prevented
  progress for three consecutive goal turns.

### Queue state machine

| State | Meaning | Satisfies downstream dependencies? | Terminal? |
| --- | --- | --- | --- |
| `blocked` | One or more named queue dependencies are unresolved, or a recorded external authority is required | No | No |
| `ready` | Every named dependency is satisfied and the item has not started | No | No |
| `active` | The only item currently being researched, tested, or changed | No | No |
| `feature-complete` | An architecture change passes its focused gates but awaits repository-wide minimization and distillation | Yes | No |
| `verified` | A test/evidence item passes, failed against the prior defect, and has complete recorded evidence | Yes | Yes for `TEST-*` only |
| `retained` | Audit proves the current design/evidence has a unique owner and must remain; the proof and inversion are recorded | Yes | Yes |
| `removed` | The redundant/invalid surface and every current reference were deleted and verified | Yes | Yes |
| `distilled` | Project-wide audit proves the architecture item minimal after all downstream work | Yes | Yes for `ARC-*` only |

An architecture row normally moves `active -> feature-complete`. The terminal
audit revisits every such row and either promotes it to `distilled`, changes it
to `retained` with proof, or reopens it. A test row normally moves
`active -> verified`. Never use `done`.

### Canonical execution queue

Queue schema: `1`. Update a row immediately when its state changes. Whenever a
dependency becomes satisfied, rescan the whole table and change every newly
unblocked row to `ready`. A dependency is satisfied only by
`feature-complete`, `verified`, `retained`, `removed`, or `distilled`.

| Seq | ID | Priority | State | Depends on | First action when selected |
| ---: | --- | :---: | --- | --- | --- |
| 001 | TEST-001 | P0 | verified | — | Preserve exactly one publication while correcting final-settlement precedence |
| 002 | TEST-002 | P0 | verified | TEST-001 | Correct the stale benchmark custody assertion and restore the flake gate |
| 003 | TEST-004 | P0 | verified | — | Replace disconnected inversion mutations with production-bound inversions |
| 004 | TEST-003 | P0 | verified | TEST-004 | Make the law manifest execute positive and inversion evidence |
| 005 | TEST-005 | P0 | verified | — | Replace constant mutation checks with production incarnation mutations |
| 006 | TEST-023 | P0 | verified | — | Add the gated commit-before-claim visibility regression |
| 007 | ARC-006 | P0 | distilled | TEST-023 | Implement one commit-before-claim activation transaction |
| 008 | ARC-007 | P0 | distilled | — | Produce the Entity versus Behavior Actors ownership/differential table |
| 009 | ARC-001 | P0 | distilled | ARC-006 | Prove caller syntax, then remove erased shutdown authority |
| 010 | ARC-002 | P1 | distilled | ARC-001 | Decide whether the external address claim has an observable owner |
| 011 | TEST-008 | P1 | verified | — | Inventory every locked Behavior Actors template and capability request |
| 012 | ARC-010 | P1 | distilled | TEST-008 | Interpret the missing upstream source-action capability directly |
| 013 | TEST-025 | P1 | verified | ARC-010 | Execute supervisor and pool policies end to end |
| 014 | ARC-003 | P1 | distilled | ARC-007 | Give the retained Entity transition one classification owner |
| 015 | ARC-004 | P1 | distilled | ARC-003 | Remove or reduce `bombay-machine` to a proven shared law |
| 016 | TEST-010 | P1 | verified | ARC-003 | Compare complete Entity transitions with an independent oracle |
| 017 | TEST-021 | P2 | verified | ARC-004 | Remove unowned Machine tests/API or prove a concrete consumer |
| 018 | ARC-014 | P1 | distilled | ARC-003 | Unify Entity task ownership and make spawn rejection explicit |
| 019 | TEST-009 | P1 | verified | ARC-003, ARC-014 | Replace toy Entity Loom models with production interleavings |
| 020 | ARC-015 | P1 | distilled | ARC-004, ARC-014, TEST-009, TEST-010, TEST-021 | Shrink the Entity public surface after its owners stabilize |
| 021 | ARC-008 | P1 | distilled | ARC-007 | Give each Behavior child occurrence one cohesive runtime state |
| 022 | ARC-009 | P1 | retained | ARC-008, ARC-010 | Decompose the complete known capability set by semantic owner |
| 023 | ARC-013 | P2 | distilled | ARC-009 | Prove or remove actor-local shared synchronization and define fact order |
| 024 | ARC-012 | P1 | distilled | ARC-001 | Replace coordinated optional activation/termination authority with owned phases |
| 025 | ARC-011 | P1 | distilled | ARC-009, ARC-012 | Consolidate launch/spawn orchestration and projection tasks |
| 026 | TEST-012 | P1 | verified | — | Compare complete typed `Actions` for every authoring path |
| 027 | TEST-015 | P1 | verified | — | Add a real renamed-downstream macro crate |
| 028 | TEST-016 | P1 | verified | — | Make the compile-fixture feature matrix explicit |
| 029 | ARC-018 | P2 | distilled | ARC-011 | Make root/child origin provenance algebraic |
| 030 | ARC-016 | P1 | distilled | ARC-011, ARC-015, ARC-018, TEST-012, TEST-015, TEST-016 | Remove type-name string inference from retained macros |
| 031 | TEST-018 | P2 | verified | — | Select one owner for ordinary Observe tests |
| 032 | ARC-017 | P1 | distilled | ARC-007 | Remove unnecessary pinning unsafe and inventory Observe unsafe |
| 033 | ARC-019 | P2 | distilled | TEST-018, ARC-017 | Give Observe one physical compilation owner |
| 034 | TEST-011 | P1 | verified | ARC-003, ARC-019 | Replace Entity/Observe sleep ordering with causal handshakes |
| 035 | TEST-019 | P2 | verified | — | Move the pseudo-benchmark to an owned performance gate or remove it |
| 036 | ARC-005 | P2 | distilled | — | Recheck overlapping manifest edits, then remove workspace residue |
| 037 | TEST-022 | P2 | verified | — | Preserve useful research conclusions and delete stale references |
| 038 | TEST-006 | P1 | verified | TEST-001 | Expand Driver property/fuzz coverage to failure and terminal branches |
| 039 | TEST-007 | P1 | verified | TEST-001 | Remove redundant Driver cases and complete partial assertions |
| 040 | TEST-013 | P1 | verified | ARC-011, ARC-015, ARC-019 | Move required operations out of assertions after source layout settles |
| 041 | TEST-014 | P1 | verified | ARC-011, ARC-015 | Eliminate discarded authoritative facts and predicted identities |
| 042 | TEST-017 | P1 | verified | ARC-016, ARC-019 | Convert current ignored docs into executable/compile-fail evidence |
| 043 | TEST-024 | P1 | verified | — | Make the mutation verdict parser fail closed for every outcome |
| 044 | ARC-020 | P2 | distilled | TEST-002, TEST-003, TEST-005, TEST-006, TEST-007, TEST-011, TEST-013, TEST-014, TEST-017, TEST-019, TEST-022, TEST-024, TEST-025, ARC-002, ARC-005, ARC-013, ARC-015, ARC-016, ARC-019 | Audit and minimize every remaining caller-facing API |
| 045 | TEST-020 | P2 | verified | ARC-020 | Run and enforce the final mutation, coverage, fuzz, Miri, and sanitizer obligations |

### Iteration loop

Repeat this loop until the terminal audit succeeds:

1. **Resume.** Read the active goal. If this execution goal has not been
   started, create it with the exact objective quoted above; never overwrite a
   different unfinished goal. Then read this queue, the selected item's complete
   detailed record, root `AGENTS.md`, the exact locked Behavior `AGENTS.md`, and
   the relevant current design documents. Run `git status --short`; preserve
   unrelated changes. If one row is `active`, resume it and skip selection.
2. **Reconcile.** Resolve `Cargo.lock`, patches, and the current upstream API.
   Validate every dependency named by the selected row and repair any
   inconsistent `Blocked by`/`Unblocks` edge in `docs/open-design-ledger.md`
   before changing code.
3. **Select.** If no item is active, apply the deterministic selection rule,
   change that row to `active`, and update the loop cursor at the end of this
   file. If no row is ready, audit the graph for a cycle or missing reciprocal
   edge before treating the situation as an external blocker.
4. **Bound the stage.** Add the required change ledger to
   `docs/open-design-ledger.md`: exact defect, smallest end-to-end regression,
   expected files, expected production delta, public types added/removed, and
   upstream owners reused/deleted. Stop for authorization before exceeding 15
   changed files, 500 net new production lines, or three new public types.
5. **Apply the Behavior-first gate.** Locate the exact upstream law and tests.
   Prototype direct composition first. For an architecture item, record why the
   upstream owner is reused or the precise residual Bombay-only gap.
6. **Prove the defect.** Add the caller-visible regression, compile fixture,
   differential trace, mutation, or benchmark threshold first. Restore or
   simulate the old representation and record that it fails for the intended
   reason. A test that only passes on current code is insufficient.
7. **Make the smallest change.** Change only the selected owner. Delete replaced
   wrappers, branches, traits, aliases, callbacks, tests, and documents in the
   same item; do not leave two current spellings.
8. **Verify locally.** Run the focused debug and optimized regressions through
   pinned Nix. Assert complete typed state, effects, errors, custody, and
   terminal output. Run compile-pass/fail, Loom, Miri, fuzz, or benchmark checks
   named by the item.
9. **Minimize.** Inspect the complete tracked and untracked diff. Remove residue
   and record exact production/test/public-API deltas. A net-positive production
   change cannot be described as consolidation or code reduction.
10. **Close the iteration.** Complete every checkbox in the detailed item or
    mark it `N/A` with evidence. Append the resolution record below. Move a test
    to `verified`, `retained`, or `removed`; move an architecture item to
    `feature-complete`, `retained`, or `removed`. Rescan and unlock rows, update
    the loop cursor, then select the next item on the next iteration.

### Required resolution record

Append this under the selected item's detailed record before changing its queue
state out of `active`:

```text
Resolution record (YYYY-MM-DD)
queue state: verified | feature-complete | retained | removed | distilled
upstream owner and locked revision:
prior-representation failure command and result:
focused debug command and result:
focused release command and result:
additional Loom/Miri/fuzz/compile/benchmark evidence:
production: +A / -B / net C
tests:      +A / -B / net C
public API: +N types / -M types
documents/examples audited:
commit or working-tree reference:
newly unlocked IDs:
remaining risk or N/A rationale:
```

## Behavior-first ownership gate

Bombay must depend on the locked Behavior algebra and Behavior Actors templates
as much as their exact contracts permit. “Use Behavior” means reuse its state,
requests, settlements, child products, custody, and policies; merely wrapping
or translating the same law into Bombay-owned types does not satisfy this gate.
Apply this sequence before every architecture item below:

1. Resolve the exact locked revision and identify the upstream owner and tests.
   In 0.17, important owners include `Actions`, `InterpretSends`,
   `InterpretCreations`, `SourceSettlementCustody`, `ChildOccurrenceShape`,
   `ChildOccurrences`, `StableProxy`, `DynamicSupervisor`, the pool/supervisor
   templates, and their named source actions such as `PrepareWorkers`.
2. Write a compile-only or pure-fold prototype using those concrete upstream
   types. Compare the complete observable trace against the current Bombay
   spelling. Do not start by adding a Bombay trait, wrapper, alias, callback, or
   effect enum.
3. If the law is identical, delete the Bombay copy and its duplicate tests.
   Keep only concrete runtime interpretation, Tokio task ownership, local
   address/mailbox installation, and other responsibilities that upstream does
   not own.
4. If the law differs, record the smallest exact gap: inputs, invariants,
   outputs, errors, ordering, and terminal custody. Retain only the state and
   transition needed for that gap, with a differential inversion proving why
   the upstream type alone is insufficient.
5. Interpret upstream typed `Actions` lanes directly through
   `InterpretItem`/`InterpretSends`/`InterpretCreations`. Never introduce a
   Bombay effect language, positional traversal, dynamic capability map, or
   second actor/lifecycle contract.
6. Record the ownership decision and dependency edge in
   `docs/open-design-ledger.md` before a production edit. A current local
   implementation is evidence to audit, not authority to preserve.

## Rust review standards

Use these primary references when resolving an item, while treating the
repository and selected Behavior instructions as stricter project law:

- the official [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
  and their [checklist](https://rust-lang.github.io/api-guidelines/checklist.html)
  for public naming, traits, conversions, documentation, and common behavior;
- the guidelines on
  [static validity](https://rust-lang.github.io/api-guidelines/dependability.html)
  and [type safety/newtypes](https://rust-lang.github.io/api-guidelines/type-safety.html)
  when deleting invalid states or transparent wrappers;
- the standard-library [`Pin`](https://doc.rust-lang.org/std/pin/struct.Pin.html)
  contract and the
  [Rustonomicon safe/unsafe boundary](https://doc.rust-lang.org/stable/nomicon/safe-unsafe-meaning.html)
  for every unsafe operation;
- the standard-library [`Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html)
  contract and the Rust Book's
  [shared-state guidance](https://doc.rust-lang.org/book/ch16-03-shared-state.html)
  before retaining shared ownership or mutexes in logically serialized code;
- the Rust Reference definition of
  [trait objects](https://doc.rust-lang.org/beta/reference/types/trait-object.html).
  Trait objects are valid Rust, but the selected Behavior contract explicitly
  requires static dispatch here, so `dyn` needs a repository-specific removal
  even where Rust itself would permit it.

## Required proof for every test repair

- Exercise the production owner or its public caller-visible interface. A toy
  model that is not compared with production is not mutation evidence.
- Establish that the test fails on the original defect or a deliberate mutant
  for the intended law.
- Keep state transitions, mutation, ownership transfer, required calls, and
  `.await` operations outside assertion arguments. Bind the observation, then
  assert it.
- Assert the complete typed result, successor state, effect lanes, custody, and
  terminal disposition, or compare an independent complete observable trace.
- Do not predict generated identifiers from implementation details, discard
  `Actions`, or erase an authoritative termination/failure fact.
- For lifecycle and generation laws, replay the same fact in optimized tests
  and prove it cannot be accepted twice.
- Run every Rust/Cargo command through pinned Nix.

## Detailed finding records

Each finding description below preserves the original defect or proposed
repair as dated audit evidence. Its `Status` line, completed checklist, and
resolution record state the current result; the canonical queue controls
current execution state.

The canonical queue above owns execution state and order. The records below own
the evidence, checklist, and exit criteria for each ID. Read the complete record
for the selected row; never infer completion from the table's short first-action
summary.

## Test defects and gaps

### TEST-001 — Final Driver settlement can be misclassified

**Status:** verified. **Priority:** P0. **Owner:** `bombay-engine`.

`crates/bombay-engine/src/driver.rs` queues the settlement from a stopping
active turn and returns `Completion::Stopped` before checking whether that
settlement is corrupt. Initialization has the same precedence shape: a
stopping initialization is reported as stopped without classifying a rejected
or corrupt settlement. The existing initialization-count test includes a
failure-shaped case but discards the disposition, so it passes over this bug.

- [x] Add caller-visible active-stop regressions for corrupt and rejected final
  interpretation, asserting exact disposition, final settlement custody,
  retirement count, no later ingress, and complete residual state.
- [x] Add the equivalent stopping-initialization cases or document, from the
  locked contract, why initialization deliberately has a different precedence.
- [x] Run the regressions in debug and release and prove the prior ordering
  fails for the intended reason before changing production.
- [x] Make the smallest ownership-preserving production correction and update
  the Driver law/strategy and ledger evidence.
- [x] Preserve exactly one publication on accepted continuation and prove the
  regression fails against the first TEST-001 correction.

Exit evidence: exact regression names and commands, prior-failure output,
debug/release results, and a complete disposition/custody trace.

Resolution record (2026-09-23)

```text
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Engine owns only precedence
prior-representation failure command and result: nix develop -c cargo test --locked -p bombay-engine --test terminal_custody -- --nocapture; 9 passed, 3 intended precedence regressions failed after observing Ok(Stopped) instead of the exact corrupt/rejected settlement error
focused debug command and result: nix develop -c cargo test --locked -p bombay-engine --test terminal_custody; 12 passed
focused release command and result: nix develop -c cargo test --locked --release -p bombay-engine --test terminal_custody; 12 passed
additional Loom/Miri/fuzz/compile/benchmark evidence: full bombay-engine suite passed, including compile fixtures, 38 inversions, 30 law tests, 3 properties, 10 manifest tests plus 1 ignored, source-order tests, custody tests, and rustdoc; strict all-target Engine Clippy and workspace formatting passed. Loom, Miri, fuzz, and benchmark are N/A because this item changes deterministic sequential precedence without unsafe, shared scheduling, parsing, or performance behavior; TEST-002 and TEST-006 own benchmark and fuzz evidence.
production: +29 / -35 / net -6
tests:      +364 / -66 / net +298
public API: +0 types / -0 types
documents/examples audited: driver-law.md, driver-test-strategy.md, open-design-ledger.md, and the complete public-example inventory; no example uses or exposes Driver settlement precedence directly
commit or working-tree reference: uncommitted TEST-001 changes in six task-local paths atop the preserved 67-path worktree baseline
newly unlocked IDs: TEST-002, TEST-006, TEST-007
remaining risk or N/A rationale: transactional activation and executable law-manifest gaps are deliberately retained under TEST-023/ARC-006 and TEST-003/TEST-008; no TEST-001 residue remains
```

Reopened evidence (2026-09-23): the first `nix flake check path:.` rerun after
TEST-002 compiled the workspace but failed the Axum live-flow test because
Local observed a second `ActiveEnvironment::publish` call. The first TEST-001
correction moved `environment.publish()` before the initialization-status
match and then entered `drive_active`, whose initialization phase publishes
after settlement custody. TEST-001 is active again; its earlier resolution
record is retained as the exact evidence that exposed this integration defect.

Reverification record (2026-09-23)

```text
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Engine owns publication sequencing and final-settlement precedence
prior-representation failure command and result: nix develop -c cargo test --locked -p bombay-engine --test terminal_custody stop_returns_final_behavior_and_active_residual -- --exact --nocapture; the complete retirement matched except publication was Repeated rather than Published. The independent flake witness failed Axum at Local's exact once-publication guard.
focused debug command and result: nix develop -c cargo test --locked -p bombay-engine --test terminal_custody; 12 passed
focused release command and result: nix develop -c cargo test --locked --release -p bombay-engine --test terminal_custody; 12 passed
additional Loom/Miri/fuzz/compile/benchmark evidence: exact Axum live flow passed; complete Engine suite, strict all-target Engine Clippy, and workspace formatting passed; nix flake check path:. passed all 17 aarch64-darwin checks including build, workspace tests, docs/doctests, Clippy, formatting, Machine/Entity/Observe Loom, panic modes, Axum, and every public example. Miri/fuzz remain N/A for deterministic precedence and are assigned to TEST-006/TEST-020; the retained TEST-002 benchmark patch participated in the green flake run.
production: +34 / -34 / net 0
tests:      +368 / -66 / net +302
public API: +0 types / -0 types
documents/examples audited: driver-law.md, driver-test-strategy.md, open-design-ledger.md, all public examples through the flake gate, and the exact Axum live integration
commit or working-tree reference: uncommitted TEST-001 changes in the same six task-local paths atop the preserved 67-path baseline; generated target artifacts were removed with pinned cargo clean after a no-space packaging failure and are rebuildable
newly unlocked IDs: TEST-002; TEST-006 and TEST-007 remain ready
remaining risk or N/A rationale: transactional commit-before-claim and executable law-manifest gaps remain explicitly owned by TEST-023/ARC-006 and TEST-003/TEST-008; exact once-publication and final-settlement precedence now have independent Engine and Local integration evidence
```

### TEST-002 — The Driver benchmark fails the repository gate

**Status:** verified; benchmark custody law and final gate passed. **Priority:** P0. **Owner:** `bombay-engine`.

`crates/bombay-engine/benches/driver.rs` expects retirement to receive no
settlements, while a stopping turn transfers its final settlement to
retirement. Both `nix flake check path:.` and the focused release benchmark test
fail at that stale expectation.

- [x] Replace the stale expectation with the exact terminal-custody law.
- [x] Keep benchmark measurement separate from correctness assertions where
  possible; retain one focused correctness regression in the test suite.
- [x] Re-run the focused benchmark test and `nix flake check path:.`.

Resolution record (2026-09-23)

```text
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Core owns exact total settlements, Engine owns terminal transfer, and Criterion owns measurement
prior-representation failure command and result: nix develop -c cargo test --locked -p bombay-engine --bench driver --release; failed at benches/driver.rs because active retirement received the stopping turn's one settlement while the stale fixture asserted an empty settlement vector
focused debug command and result: nix develop -c cargo test --locked -p bombay-engine --bench driver; Success
focused release command and result: nix develop -c cargo test --locked -p bombay-engine --bench driver --release; Success
additional Loom/Miri/fuzz/compile/benchmark evidence: terminal_custody passed all 12 complete-custody regressions; nix flake check path:. passed all 17 native checks, including the benchmark through workspace all-target gates, build, workspace tests, docs/doctests, strict Clippy, formatting, three Loom suites, panic modes, Axum, and every public example. Additional Miri/fuzz evidence is N/A for this deterministic benchmark fixture and remains assigned to TEST-020/TEST-006.
production: +0 / -0 / net 0
tests:      +35 / -11 / net +24 (benchmark code)
public API: +0 types / -0 types
documents/examples audited: Driver law and test strategy already state exact terminal retirement custody; terminal_custody retains the focused correctness oracle; no current document or example taught the stale empty-settlement expectation
commit or working-tree reference: uncommitted TEST-002 changes in three task-local paths atop the preserved worktree and verified TEST-001 changes
newly unlocked IDs: none; ARC-020 still has unresolved dependencies besides TEST-002
remaining risk or N/A rationale: the benchmark now returns complete custody as its typed residual, compares the complete DriverRetirement once before measurement, and black-boxes measured results; its semantic source-state boolean was replaced by a closed private enum. No TEST-002 residue remains.
```

### TEST-003 — The 68-law manifest does not execute its claimed evidence

**Status:** verified. **Priority:** P0. **Owner:** `bombay-engine`.

`crates/bombay-engine/tests/law_manifest.rs` source-scans for positive test
function names but never proves those tests ran. It does not resolve inversion
references. Its ignored completion gate only checks that JSON status strings
say `passing`. Across the 68 rows, all laws share the same negative reference,
boundary reference, three adversarial references, empty template boundary, and
generic `cargo test -p bombay-engine` command; 17 rows reuse one structural
positive and 18 reuse one structural inversion. The documented command also
omits pinned Nix.

- [x] Change every unsupported `passing` row to an honest blocked state.
- [x] Define executable, revision-bound evidence records with unique and
  applicable positive, boundary, and inversion evidence per law.
- [x] Resolve and validate inversion references, not just positive test names.
- [x] Make the completion gate consume actual command/result evidence rather
  than local arrays or status strings.
- [x] Retain source scanners only as supplemental structural policy gates; do
  not count appended forbidden strings as semantic mutation evidence.
- [x] Remove the ignored gate once the ordinary CI path performs the real check.

Exit evidence: one traceable executed record for every law, with no blanket
status claims and no unqualified host `cargo` command.

Resolution record (2026-09-23)

```text
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602 own folds, Actions, total settlements, source custody, child products, and templates; Engine owns only the retained affine causal protocol
prior-representation failure command and result: `nix develop -c cargo test --locked -p bombay-engine --test law_manifest manifest_evidence_is_revision_bound_and_executable -- --exact --nocapture` failed schema 1 for the intended reasons: no selected revision or result artifact, scalar evidence for all 68 rows, unpinned commands, and the ignored status-only gate
focused debug command and result: `nix develop -c cargo test --locked -p bombay-engine --test law_manifest` passed 9/9 ordinary tests with none ignored; `nix develop -c cargo test --locked -p bombay-engine` passed the complete Engine unit, integration, compile, property, custody, allocation, and documentation surface
focused release command and result: `nix develop -c cargo test --locked --release -p bombay-engine --test law_manifest` passed 9/9; `nix develop -c cargo test --locked --release -p bombay-engine` passed the same complete optimized Engine surface
additional Loom/Miri/fuzz/compile/benchmark evidence: `nix build path:.#driver-law-evidence --print-out-paths --no-link` produced immutable receipt `/nix/store/9jcxna3i3fy3hbwyv83qwir9ars2jjzh-bombay-workspace-0.1.0/driver-law-evidence.json` (SHA-256 7fc2f959df70492f3c64287d3ad65cc9997b76b718189cd5ee99ce9875c230e6) with 8 positive passes, 8 boundary passes, and 8 real-source mutations killed by their named tests; strict Engine Clippy, Cargo/rustfmt, Nix formatting, shell syntax, JSON parsing, flake evaluation, residue, and whitespace checks passed; Loom/Miri/fuzz/benchmark are N/A because their campaigns remain assigned to TEST-020/TEST-006 and this item changes no concurrency or performance implementation
production: +0 / -0 / net 0
tests:      +450 / -326 / net +124 (schema gate plus disposable real-source mutation runner; configuration is +20/-0/net +20)
public API: +0 types / -0 types
documents/examples audited: the stale 68-law cross-owner catalogue was distilled to 8 Engine-owned laws; Driver law, strategy, manifest, current ledger references, mutation gate, and flake checks were updated; examples are N/A because no caller or production contract changed
commit or working-tree reference: current preserved dirty worktree; eight TEST-003 paths atop verified TEST-001/002/004 changes
newly unlocked IDs: none; ARC-020 still has unresolved dependencies besides TEST-003
remaining risk or N/A rationale: actor-template inventory, broader property/fuzz campaigns, redundant Engine-test cleanup, Miri/Loom/coverage/sanitizers, and final repository gates retain their canonical TEST-008/006/007/020 owners; no unsupported Driver-law status remains
```

### TEST-004 — Driver inversion tests mutate a disconnected toy model

**Status:** verified. **Priority:** P0. **Owner:** `bombay-engine`.

All 38 tests in `crates/bombay-engine/tests/driver_inversions.rs` operate on
local booleans, counts, vectors, `Fact`, and `Inversion`; none invokes `Driver`
or production code. Several are internally redundant: initialization order,
commit order, and no-prefetch use the same prefetch mutation; source order
repeats lane-order cases; self-send repeats the causal inversion.

- [x] Replace toy mutations with deliberate production mutants, a supported
  mutation runner, or public-interface fixtures that actually invert the named
  Driver branch.
- [x] Require each retained inversion to be killed by its named regression for
  the named reason.
- [x] Collapse duplicate mutations only after recording which distinct law is
  still proved elsewhere.

Depends on TEST-003 for manifest claims; it may be implemented independently.

Resolution record (2026-09-23)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at `435560ce7bea8ad3330ee2d42e5034f837a80602` own the fold and settlement algebra; Engine owns the causal Driver; cargo-mutants owns source mutation
prior-representation failure command and result: `nix develop -c cargo test --locked -p bombay-engine --test law_manifest production_driver_mutation_evidence_has_one_owner -- --exact --nocapture` failed because the disconnected suite and its manifest references remained, the required derivation omitted Engine, and the baseline omitted production Driver candidates
focused debug command and result: `nix develop -c cargo test --locked -p bombay-engine` passed every Engine unit, integration, compile, and documentation test (one explicit TEST-003 completion gate remains ignored)
focused release command and result: `nix develop -c cargo test --locked --release -p bombay-engine` passed the same complete Engine surface
additional Loom/Miri/fuzz/compile/benchmark evidence: `PROPTEST_CASES=64 nix develop -c cargo mutants --package bombay-engine --test-tool nextest --no-shuffle --colors never --minimum-test-timeout 180 --output /tmp/bombay-test004-nextest-mutants.2dMCqU` classified all 14 production Driver candidates: 2 caught by `driver_allocation::one_complete_driver_execution_allocates_one_settlement_queue`, 12 unviable, 0 missed, 0 timeout; strict Engine Clippy, workspace fmt, manifest, residue, and whitespace gates passed; Loom/Miri/fuzz/benchmark are N/A because this item changes evidence/configuration only
production: +0 / -0 / net 0
tests:      +41 / -567 / net -526
public API: +0 types / -0 types
documents/examples audited: all 68 manifest rows, Driver law, Driver test strategy, mutation configuration/baseline, and Nix derivations audited; 48 unsupported rows are blocked for TEST-003 and 20 remain passing; examples are N/A because no caller contract changed
commit or working-tree reference: current preserved dirty worktree; eight TEST-004 paths, including deletion of the 490-line toy suite
newly unlocked IDs: TEST-003
remaining risk or N/A rationale: `nix build .#mutants --no-link` reaches the combined unmutated baseline but one load-sensitive Entity fixed-yield test failed once and passed 20/20 sequential reruns; its causal-handshake repair belongs to queued Entity test work, and the combined mutation derivation remains a terminal-audit obligation

### TEST-005 — Incarnation mutation tests are disconnected constants

**Status:** verified. **Priority:** P0. **Owner:** Bombay incarnation.

`crates/bombay/src/incarnation.rs` has one test that only counts six mutation
name strings and another that compares hard-coded toy transcripts. Neither
executes `Incarnation`, so neither proves that the real panic, cancellation,
drop-before-publish, classification, or exactly-once retirement regressions
kill those mutations.

- [x] Delete the string-count test.
- [x] Replace toy transcripts with deliberate production mutants or prove the
  real lifecycle regressions kill each inversion.
- [x] Preserve the existing real panic/cancellation tests, but move task awaits
  outside assertions and add optimized replay/double-acceptance evidence.

Resolution record (2026-09-23)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at `435560ce7bea8ad3330ee2d42e5034f837a80602` own folds and typed settlements; Engine owns one affine Driver execution; Tokio owns task cancellation/unwind mechanics; Bombay Incarnation owns post-destruction panic/cancellation classification and one retirement capability
prior-representation failure command and result: `nix develop -c cargo test --locked -p bombay-rs --lib incarnation::tests::incarnation_mutation_evidence_owns_production_source -- --exact --nocapture` failed at the first disconnected toy-test definition before either toy or representation was changed; the executable gate and Nix check were absent
focused debug command and result: `nix develop -c cargo test --locked -p bombay-rs --lib 'incarnation::tests::' -- --nocapture` passed all 9 focused tests, including exact complete outcomes, environment destruction, retirement count, ordering, and replay
focused release command and result: `nix develop -c cargo test --locked --release -p bombay-rs --lib 'incarnation::tests::' -- --nocapture` passed the same 9 tests; two replayed panics and two replayed cancellations produced exactly four destructions, four retirements, and `[Panicked, Panicked, Cancelled, Cancelled]`
additional Loom/Miri/fuzz/compile/benchmark evidence: `nix build path:.#incarnation-law-evidence --print-out-paths --no-link` produced `/nix/store/50vqdfrd89mby6qdn04m60ajdjxzz2ya-bombay-workspace-0.1.0/incarnation-law-evidence.json` (SHA-256 `c3e5fac683b01ea39a7f14719317d31301656d890ce7fdad5047fe35377d0c95`) with 7 production-source mutations killed and 2 duplicate-consumption mutations denied by exact moved-value diagnostics; complete Bombay debug/release suites, every compile fixture, strict all-target Clippy, rustfmt, nixfmt, shell syntax, JSON, flake evaluation, whitespace, and tracked/untracked inspection passed; Loom/Miri/fuzz/benchmarks are N/A because this item changes only a single-task terminal guard's evidence and their broader campaigns retain separate queue owners
production: +0 / -0 / net 0
tests:      +391 / -85 / net +306
public API: +0 types / -0 types
configuration: +23 / -5 / net +18 across the Nix evidence check and measured cargo-mutants signatures
documents/examples audited: module boundaries, runtime capability interfaces, Driver lifecycle boundary, launch, termination, and every current Incarnation reference were audited; no caller contract or example changed
commit or working-tree reference: current preserved dirty worktree; six TEST-005 paths
newly unlocked IDs: none; ARC-020 retains other unresolved prerequisites
remaining risk or N/A rationale: the indiscriminate sequential cargo-mutants unit runner times out after deleting terminal publication because unrelated tests wait for a fact the mutant removed; the focused production-mutation gate kills that inversion immediately and the required all-suite nextest mutation derivation remains a terminal-audit obligation

### TEST-006 — Driver property and fuzz models cover only the happy byte path

**Status:** verified after TEST-001. **Priority:** P1.

`driver_property.rs` and `fuzz/fuzz_targets/causal_turns.rs` generate nearly the
same `Vec<u8>` model: every settlement succeeds and `u8::MAX` stops. They do not
generate activation failures, behavior failures, rejected/corrupt settlements,
pending source turns, cancellation, panic, heterogeneous lanes, creation
results, or terminal re-admission. The property oracle also repeats the same
stop branch as production. `deterministic_replay_is_exact` is entailed by the
stronger model comparison and adds no distinct adversary in its current form.

- [x] Define a typed operation grammar that covers every Driver phase,
  admission source, decision, failure class, and terminal edge.
- [x] Keep the oracle independent from Driver branching; share only operation
  decoding, never expected semantics.
- [x] Give property and fuzz tests distinct jobs: bounded model comparison vs.
  coverage-guided malformed/long operation sequences.
- [x] Add a bounded, reproducible fuzz campaign with seed/corpus/crash retention
  to verification instead of leaving the target workspace-excluded/on demand.
- [x] Remove or repurpose deterministic replay to test a genuinely independent
  determinism boundary.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602 own the typed Actions, creation, settlement, and source-custody algebra; Engine owns the affine Driver
prior-representation failure command and result: with only the selected Driver initialization rejection classification temporarily changed to Corrupt, nix develop -c cargo test --locked -p bombay-engine --test driver_property bounded_phase_matrix_covers_failure_source_and_terminal_edges -- --exact failed exit 101 on the exact Rejected versus Corrupt disposition while the complete typed effect/retirement trace agreed; production source was restored
focused debug command and result: nix develop -c cargo test --locked -p bombay-engine --test driver_property — 2/2 passed
focused release command and result: nix develop -c cargo test --locked --release -p bombay-engine --test driver_property — 2/2 passed
additional Loom/Miri/fuzz/compile/benchmark evidence: nix develop .#fuzz -c bash crates/bombay-engine/fuzz/verify-causal-turns.sh — 2,048 seeded runs, 82 retained corpus entries, no crash; corpus and crash artifacts retained under the ignored run directory and uploaded by CI; full workspace tests, strict all-target Clippy, rustfmt, and diff whitespace passed. Existing Driver cancellation-at-every-await and panic-stage tests retain those separate ownership laws; the fuzz grammar covers pending source cancellation, while the property grammar covers typed initialization, activation, ordinary and source turns, action decisions, three settlement statuses, source custody, two send lanes, child creation, and terminal re-admission
production: +0 / -0 / net 0
tests:      driver_property.rs +579 / -83 against HEAD; fuzz target +292 / -84 against HEAD including its preexisting Behavior 0.17 API migration; one new bounded campaign script and eight binary seed files
public API: +0 types / -0 types
configuration: CI +12 / -0; fuzz package and lock update the selected Behavior 0.17 contract
documents/examples audited: Driver law and strategy, existing cancellation/panic/terminal tests, fuzz package, CI and Nix shell; no public example contract changed
commit or working-tree reference: preserved dirty working tree; 119 tracked/untracked paths at this checkpoint, including prior unrelated work
newly unlocked IDs: none; TEST-007 is the next lowest-sequence ready P1 row
remaining risk or N/A rationale: the final flake check and whole-goal distillation remain terminal audit obligations; this TEST changes no production law
```

### TEST-007 — Driver unit evidence contains redundant and incomplete cases

**Status:** verified after TEST-001. **Priority:** P1.

In `driver_law.rs`, the complete causal transcript test already entails the two
count/projection tests using the same `[1, 2, 9, 100]` scenario. The
initialization-once test discards dispositions. The creation test predicts
nonces `1` and `2` by issuing IDs in a second local sequence and discards the
second issued value.

- [x] Keep the full transcript test; give the two projection tests distinct
  boundary/inversion inputs or remove them.
- [x] Assert every initialization case's exact disposition and residual.
- [x] Capture creation identifiers from observable interpretation output rather
  than predicting the allocator's next values.
- [x] Retain `driver_allocation`, `source_settlement_order`, and
  `terminal_custody`; they provide distinct allocation, order, and custody
  evidence.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Behavior owns opaque creation IDs and ordered actions, Engine owns Driver order and custody
prior-representation failure command and result: changing Probe initialization's committed send from 0 to 42 left the old count-only initialization assertion true but made the new exact transcript test fail exit 101 on Commit([42]) versus Commit([0]); reversing interpreted creation result order made the new creation relation fail exit 101 on [2,1] versus [1,2]; both changes were restored
focused debug command and result: nix develop -c cargo test --locked -p bombay-engine --test driver_law — 28/28 passed after removal of two entailed tests
focused release command and result: nix develop -c cargo test --locked --release -p bombay-engine --test driver_law — 28/28 passed
additional Loom/Miri/fuzz/compile/benchmark evidence: strict Engine all-target Clippy, workspace rustfmt check, and diff whitespace passed; the three distinct Driver allocation, source-order, and terminal-custody targets remain in the already green locked workspace suite. Concurrency and performance campaigns belong to their separate queue IDs
production: +0 / -0 / net 0
tests:      driver_law.rs +157 / -129 / net +28 against HEAD, including earlier uncommitted Driver-law work; TEST-007 removes two redundant test functions
public API: +0 types / -0 types
documents/examples audited: Driver law/strategy and creation owner source/tests; no public contract or example changed
commit or working-tree reference: preserved dirty worktree; 119 tracked/untracked changed paths at this checkpoint
newly unlocked IDs: none; TEST-024 is the next lowest-sequence ready P1 row
remaining risk or N/A rationale: final whole-goal distillation remains; this evidence item changes no production law
follow-up gate correction: the later full workspace test exposed two stale TEST-007 function references in driver-law-manifest.json and law_manifest.rs. Both now name the retained exact initialization witness. The 9/9 focused manifest tests pass, and nix develop -c bash crates/bombay-engine/tests/driver-law-evidence.sh --law D-INIT-1 reports the duplicate-initialization production mutation KILL. The complete workspace gate is rerun after this correction
```

### TEST-008 — The actor-template boundary manifest is empty

**Status:** verified.
**Priority:** P1.

`docs/driver-template-manifest.json` contains only metadata and an empty
`mirrored_templates` array. `engine_does_not_mirror_actor_template_laws` merely
checks that emptiness and absence of two filenames. This does not meet
`docs/driver-test-strategy.md`, which calls for an audited inventory of Behavior
Actors exports, variants, lanes, composition edges, owners, and evidence.

- [x] Inventory the exact locked Behavior Actors API and tests.
- [x] Record every template boundary and its owning upstream evidence without
  copying template semantics into Engine.
- [x] Make the boundary gate fail for an omitted or accidentally mirrored
  template.

Resolution record (2026-09-23)

```text
queue state: verified
upstream owner and locked revision: bombay-behavior-actors 0.17.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602 owns all 45 Behavior compositions, their event/effect products, and 19 actor-owned request/source-action contracts; Engine owns only the no-mirroring boundary and Bombay owns concrete interpretation
prior-representation failure command and result: nix develop -c cargo test --locked -p bombay-engine --test law_manifest engine_does_not_mirror_actor_template_laws -- --exact --nocapture; exited 101 after the new test compiled and ran, reporting schema 2 plus the exact missing sets of 45 template IDs and 19 capability IDs
focused debug command and result: nix develop -c cargo test --locked -p bombay-engine --test law_manifest engine_does_not_mirror_actor_template_laws -- --exact; 1 passed
focused release command and result: nix develop -c cargo test --locked --release -p bombay-engine --test law_manifest engine_does_not_mirror_actor_template_laws -- --exact; 1 passed
inversion evidence: renaming workflow.workflow to workflow.workflow-omitted-by-mutation made the focused test exit 101 with that exact missing/extra pair; adding atomic.stable-proxy to mirrored_templates made it exit 101 with Engine mirrors actor-template policy; both manifest mutations were restored
additional Loom/Miri/fuzz/compile/benchmark evidence: nix develop -c cargo test --locked -p bombay-engine and its --release form both passed the complete Engine package: compile fixtures, 30 Driver-law tests, 3 properties, all 9 manifest tests, allocation, source-order, 12 custody tests, and rustdoc. nix develop -c cargo clippy --locked -p bombay-engine --all-targets -- -D warnings passed after the exact catalogue test received a scoped too-many-lines rationale; nix develop -c cargo fmt --all -- --check passed after pinned rustfmt. Loom, Miri, fuzz, and benchmark are N/A because TEST-008 adds deterministic manifest evidence and no production, unsafe, concurrency, parser, or performance behavior.
production: +0 / -0 / net 0
tests:      +214 / -28 / net +186
public API: +0 types / -0 types
documents/examples audited: driver-template-manifest.json schema 3 now records every public template boundary and capability status; driver-test-strategy.md documents the executable gate and seven missing atomic interpreters; open-design-ledger.md records ownership, edges, expected delta, failure, mutations, and gates. Public examples are unchanged because no caller or runtime behavior changed.
commit or working-tree reference: uncommitted TEST-008 changes in five task-local paths atop the preserved tracked/untracked baseline
newly unlocked IDs: ARC-010, selected immediately as the lowest-sequence ready P1 row
remaining risk or N/A rationale: BeginActivation, CustomerDelivery, DiagnosticAction, InitializeWorker, AssignWorker, PrepareWorkers, and ProxyOperation are honestly recorded as missing Bombay interpreter boundaries owned by ARC-010; no template law was copied into Engine
```

### TEST-009 — Five Entity Loom tests prove only a toy model

**Status:** verified. **Priority:** P1. **Owner:** Entity directory.

`crates/bombay/tests/entity_loom.rs` implements local `Admission` and
`ActivationClaim` models and never calls `LocalDirectory` or another production
owner. The separate `entity_loom_local.rs` is real production Loom evidence but
only covers concurrent claims.

- [x] Remove or demote the five disconnected tests unless they are compared
  step-for-step with production.
- [x] Add production Loom scenarios for fence/reservation ordering, delayed
  removal versus replacement, canceled waiter versus activation completion,
  and reservation/drain races.
- [x] Assert exact command custody, activation identity, fence placement, and
  terminal slot state for every interleaving.

The five stand-in tests were deleted. `entity_loom_local.rs` now has six tests
over the production directory and slot, including four new interleavings for
pending reservation/fence, old removal/replacement dispatch, waiter
cancellation/activation, and dispatch/drain. The replacement witness covers
the reachable ordering: a new slot can be claimed only after the old removal
has interpreted, while a dispatch before that removal is returned exactly.
The cancellation witness counts both command drops and checks the complete
action trace selected by the production cancellation disposition.

Resolution record (2026-09-28)
```text
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602 own actor policy; Bombay LocalDirectory and EntitySlot own the tested synchronization
prior-representation failure command and result: with the real resolve_draining_delivery fence effect temporarily changed to Remove, the pinned-Nix debug command RUSTFLAGS='--cfg bombay_entity_loom' cargo test --locked -p bombay-rs --test entity_loom_local pending_delivery_resolution_precedes_the_fence_in_every_interleaving -- --exact exited 101 with the missing Fence in the typed trace; the source was restored. The five deleted stand-in tests never invoked this production branch
focused debug command and result: RUSTFLAGS='--cfg bombay_entity_loom' through pinned Nix, cargo test --locked -p bombay-rs --test entity_loom_local; 6 passed
focused release command and result: the same command with --release; 6 passed
additional Loom/Miri/fuzz/compile/benchmark evidence: strict Loom configured Clippy for this target passed; full locked workspace test, ordinary strict all-target Clippy, rustfmt check, and git diff --check passed. Miri, fuzz, and benchmark are N/A because this is a concurrency-model evidence replacement with no production change
production: +0 / -0 / net 0
tests:      +474 / -198 / net +276
public API: +0 types / -0 types
documents/examples audited: open-design-ledger.md, todo.md, existing Entity directory/runtime tests and boundary docs; no example contract changed
commit or working-tree reference: uncommitted TEST-009 changes in the preserved working tree
newly unlocked IDs: none; ARC-015 still requires TEST-021
remaining risk or N/A rationale: no Loom test claims a physically impossible replacement before the old mapped slot is removed; the reachable race and old fact replay are covered
```

### TEST-010 — Entity exhaustive testing uses production as its oracle

**Status:** verified; ARC-003's independent oracle and inversions passed.
**Priority:** P1.

The original bounded 137,561-trace test was self-referential. ARC-003 replaced
it with an independent oracle in
`crates/bombay/src/entity/lifecycle/transition.rs` that compares complete
state, typed effects, and classification for every enumerated trace. TEST-010
must audit edge coverage and stale-fact inversions before closing this item.

- [x] Build an independent transition table/model from the documented lifecycle
  law, or a complete expected-output table for every state/input pair.
- [x] Compare complete next state, evidence, effects, returned payloads, and
  identities; then retain bounded sequence enumeration for composition.
- [x] Add inversion cases for each classification edge and replay stale facts.

The explicit seven-edge witness table checks each reachable phase against all
19 input forms, checks stale facts before applicable edges, and checks replay
afterward. A deliberate stale-force classification mutation failed the new
test at `ForceStale`; the restored code passes debug and optimized focused
tests. TEST-010 adds no executable production code or public API.

### TEST-011 — Runtime ordering tests depend on scheduler sleeps

**Status:** verified. **Priority:** P1.

The prior Entity runtime tests used 1 ms polling sleeps. Observe panic/stress
tests used 10–100 ms sleeps to assume waiter registration, and the zero-timeout
test imposed a 100 ms wall-clock ceiling. The selected tests now establish
their order through exact observable facts.

- [x] Replace sleep-as-readiness with barriers, notifications, condition
  variables, registration probes, or another observable causal handshake.
- [x] Keep timeouts only as bounded hang watchdogs, not as the mechanism that
  establishes order.
- [x] Replace the strict zero-time elapsed ceiling with a nonblocking semantic
  observation and registry cleanup assertion.

ARC-017 evidence: Miri 0.1.0 with seeds 0–3 failed
`panicking_waker_must_not_strand_other_waiters` at seed 2's `is_finished`
check. That prior test used 50/100 ms sleeps to infer registration/completion;
a deterministic panic/drop probe passed the same Miri seeds. The causal
registration test now passes the same seeds.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Bombay-private Observe and Entity runtime tests, with Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602
prior-representation failure command and result: ARC-017's pinned Miri 0.1.0 nightly 2026-06-15 run with seeds 0–3 failed panicking_waker_must_not_strand_other_waiters at seed 2's sleep-based is_finished assertion; the old test did not observe registration or completion causally
focused debug command and result: pinned-Nix cargo test --locked -p bombay-rs --test entity_runtime passed 11/11; pinned-Nix cargo test --locked -p bombay-rs --lib observe:: passed 121/121
focused release command and result: the same Entity and Observe focused suites with --release passed 11/11 and 121/121
additional Loom/Miri/fuzz/compile/benchmark evidence: optimized isolated Observe Loom passed 28/28. Pinned #miri shell replay with MIRIFLAGS=-Zmiri-many-seeds=0..4 passed both panicking_waker_must_not_strand_other_waiters and wait_timeout_waiter_recovers_after_panicking_drain on seeds 0–3; the first Miri attempt stopped before execution because the Nix toolchain's cargo-miri path was temporarily absent, and the same pinned shell succeeded on retry. Full locked workspace tests, strict workspace all-target Clippy, rustfmt check, and git diff --check passed. Fuzz and benchmarks are unchanged and remain TEST-020/019 obligations
production: +0 / -0 / net 0; only owning test modules and the Entity test runtime changed
tests:      complete tree test/tool/example delta +4737 / -2538 / net +2199 against HEAD, including prior stages; this stage adds test-only causal signals and removes polling sleeps
public API: +0 types / -0 types
documents/examples audited: four Entity 1 ms loops plus two yield loops now wait on the test runtime's exact fact signal; Observe panic, stress, and timeout tests use exact private registrations, a channel or barrier, and post-race registry assertions. The stale-token stress test now reuses the same waiter thread across generations. No sleep calls remain in the targeted Entity/Observe corpus; no example changed
commit or working-tree reference: preserved dirty tree with 138 changed or untracked paths; complete tree production delta +2909 / -3662 / net -753 against HEAD, tests/tool/examples +4737 / -2538 / net +2199, other +7281 / -1867 / net +5414, measured after TEST-011 edits and before this record
newly unlocked IDs: none; TEST-019 is the next lowest-sequence ready P2 item
remaining risk or N/A rationale: duration-based timeout behavior is still exercised as domain behavior, while no duration is used to infer readiness; bounded waits serve only as hang watchdogs. Project-wide distillation remains ARC-020
```

### TEST-012 — Differential authoring tests discard or partially compare Actions

**Status:** verified. **Priority:** P1.

`macro_last_authoring.rs` discards several increment `Actions` and compares only
selected send/step fields for read and stop. `activation_authoring.rs` and
`outer_authoring.rs` omit lanes or cardinality. `template_application.rs`
compares creation length rather than creation contents. Spurious or reordered
effects can pass these tests.

- [x] Compare complete typed `Actions` for initialization, transition, stop,
  error, and every facade/explicit implementation pair.
- [x] Assert exact send and creation contents/order and exact successor step,
  not selected fields or lengths.
- [x] Add an extra-send mutant to the macro and activation families and a
  duplicate timer-request mutant to the wrapper family; all fail the new
  complete comparison and were restored. The outer facade uses `NoSends` and
  `NoBirths`, so an extra effect is unconstructible in its typed actions.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0, 435560ce7bea8ad3330ee2d42e5034f837a80602
prior-representation failure command and result: temporary second owner read send, second active send, and duplicate wrapper schedule each failed its complete Actions comparison under focused nix develop -c cargo test --locked -p bombay-rs (exit 101); each old partial assertion missed the respective added item
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --test macro_last_authoring --test activation_authoring --test outer_authoring --test template_application — passed
focused release command and result: nix develop -c cargo test --locked -p bombay-rs --release --test macro_last_authoring --test activation_authoring --test outer_authoring --test template_application — passed
additional Loom/Miri/fuzz/compile/benchmark evidence: compile-pass/fail fixtures in focused targets and full locked workspace test passed; strict workspace Clippy, rustfmt check, and diff check passed; concurrency and performance tools are N/A to this test-only differential change
production: +0 / -0 / net 0
tests:      +66 / -67 / net -1
public API: +0 types / -0 types
documents/examples audited: four authoring/template test families and open-design-ledger.md; no example or production contract changed
commit or working-tree reference: uncommitted TEST-012 test changes in preserved working tree
newly unlocked IDs: none; ARC-016 still has other prerequisites
remaining risk or N/A rationale: outer facade's NoSends/NoBirths makes an extra-lane mutant structurally impossible; full Actions equality is retained. Project-wide distillation remains the terminal audit
```

### TEST-013 — Assertions perform required operations and ownership transfers

**Status:** verified. **Priority:** P1.

The audit found required operations inside assertions in Address allocation,
Timer popping, Machine transition, Incarnation task joins, Observation polling,
Driver cancellation, Entity passivation, lifecycle termination, and examples.
This violates the repository rule that assertions are observational only and
can erase behavior in disabled/debug variants or obscure custody.

- [x] Bind every allocation, transition, pop, passivation, `.next().await`, task
  join, and termination await before asserting its value.
- [x] Re-run this residue search and manually inspect multiline assertions:

  ```text
  rg -n 'assert(_eq|_ne)?!\([^\n]*(allocate|transition|pop_due|\.await|passivate|next\(\))' crates examples
  ```

Known files include `address.rs`, `time.rs`, `incarnation.rs`,
`observation.rs`, `driver_law.rs`, `actor_interface.rs`, `entity_family.rs`,
`entity_runtime.rs`, `run_with.rs`, and the counter, actor-template, and Entity
examples.

Resolution (2026-10-01): the initial balanced scan failed on 31 assertion-owned
operations; the final scan finds zero. The requested line search now matches
only identifiers and strings. Consuming `map_sends`/`into_*` conversions and
assertion-helper send arguments were also bound. Focused owner tests and four
executable examples passed in debug and release; strict workspace Clippy,
formatting, and diff checks passed. The complete checkpoint and exact stage
commands are in `docs/open-design-ledger.md`.

### TEST-014 — Authoritative facts and generated values are discarded

**Status:** verified. **Priority:** P1.

`run_with.rs` discards three exact termination results. The Driver creation test
discards a generated ID and predicts allocator values. Several authoring tests
discard returned `Actions`. These patterns can hide failure classification,
extra effects, or identity errors.

- [x] Bind and assert each exact termination result, or document and test a
  deliberately typed discharge policy.
- [x] Remove every `let _ =` that discards Actions, generated identity, or an
  authoritative lifecycle fact.
- [x] Keep intentional panic-triggering `let _` uses in Machine tests only when
  the caught panic is itself the asserted observation.

Resolution (2026-10-01): the exact termination, creation ID, and complete
`Actions` checks are present in the selected 0.20 source. The remaining
Observe reentrant test now uses explicit `drop` for its stimulus; its focused
test passes in debug and release. The only remaining live-source `let _ =`
discards a Loom-only `Duration`; compile-fail fixtures retain their deliberate
statically denied expressions. Strict Clippy, formatting, and diff checks pass.
No Machine panic-triggering `let _` remains; that exception is inapplicable.
The complete checkpoint is in `docs/open-design-ledger.md`.

CI recheck (2026-10-02): the projected-child retirement test now obtains
creator-local IDs from Behavior 0.20.0 `CreationSequence::issue` instead of
literal nonces. The original PR CodeQL gate flagged two fixed nonce values;
the corrected `c4400b5` result has no such alerts. Its focused test passed
through pinned Nix in debug and release (1/1 each), and the corrected source
passed the repeated 21-check local flake gate. Production behavior and public
types are unchanged; the embedded test source changed `+15/-4` lines.

### TEST-015 — Public macro coverage lacks a renamed-downstream crate

**Status:** verified. **Priority:** P1. **Owner:** `bombay-macros`.

Trybuild covers ordinary facade syntax and several diagnostics, but
`proc_macro_crate` path selection is only tested as a local string. There is no
real downstream package that aliases `bombay-rs` and compiles all three macros.

- [x] Add a temporary/fixture downstream crate with a renamed Bombay dependency
  and compile every exported macro from caller syntax.
- [x] Include hygiene, generics, where clauses, and collision cases.
- [x] Pair compile evidence with TEST-012's complete expansion-equivalence
  runtime evidence.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0, 435560ce7bea8ad3330ee2d42e5034f837a80602; Bombay macro facade delegates to that owner
prior-representation failure command and result: temporarily forcing the Bombay macro resolver to emit ::bombay, then nix develop -c cargo test --locked -p bombay-renamed-downstream, failed compilation (exit 101) with unresolved paths and missing Behavior implementation; mutation restored
focused debug command and result: nix develop -c cargo test --locked -p bombay-renamed-downstream — passed
focused release command and result: nix develop -c cargo test --locked -p bombay-renamed-downstream --release — passed
additional Loom/Miri/fuzz/compile/benchmark evidence: three exported macros compile from a real alias-only downstream package; full locked workspace test and strict Clippy pass. Concurrency/performance tools are N/A to alias expansion; TEST-012 supplies complete action comparison
production: +0 / -0 / net 0 runtime/macro lines
tests:      +92 / -0 / net +92 integration fixture lines; one doc-only package lib line
public API: +0 production types / -0 production types
documents/examples audited: root workspace manifest, Cargo.lock, existing macro trybuild fixtures, open-design-ledger.md, and TEST-012 authoring runtime comparisons
commit or working-tree reference: uncommitted TEST-015 package in preserved working tree
newly unlocked IDs: none; ARC-016 still has other prerequisites
remaining risk or N/A rationale: final project-wide macro architecture audit is ARC-016/ARC-020; fixture is retained as a workspace gate
```

### TEST-016 — Compile fixtures need an explicit feature matrix

**Status:** verified. **Priority:** P1.

`application_child_must_be_behavior.rs` and its `_feature_unified` counterpart
have identical source and distinct expected diagnostics. This duplication is
intentional only if both dependency-feature states run. Workspace feature
unification can otherwise select just one branch.

- [x] Retain both fixtures, or generate their shared source, while keeping the
  two diagnostic expectations.
- [x] Add explicit pinned gates for `bombay-rs --no-default-features` and
  `bombay-rs --all-features`; record which fixture each executes.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0, 435560ce7bea8ad3330ee2d42e5034f837a80602; Behavior owns the static child requirement
prior-representation failure command and result: temporary swap of the two cfg-selected trybuild fixtures, then nix develop -c cargo test --locked -p bombay-rs --no-default-features --test application_children, failed its expected diagnostic (exit 101); selection restored
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --no-default-features --test application_children -- --nocapture — passed, ordinary fixture selected; nix develop -c cargo test --locked -p bombay-rs --all-features --test application_children -- --nocapture — passed, feature-unified fixture selected
focused release command and result: N/A, no runtime law changed; compile diagnostics are checked in both package feature states
additional Loom/Miri/fuzz/compile/benchmark evidence: both Nix check names appear in nix eval; direct pinned feature-matrix tests pass. An optional isolated nix build of the first check was interrupted after more than 14 minutes of dependency compilation without a verdict; final flake gate remains pending
production: +0 / -0 / net 0 Rust lines
tests:      +0 / -0 / net 0
public API: +0 types / -0 types
documents/examples audited: flake.nix, both trybuild sources and expected diagnostics, application_children.rs, Cargo features, open-design-ledger.md
commit or working-tree reference: uncommitted TEST-016 flake check definitions in preserved working tree
newly unlocked IDs: none; ARC-016 still has other prerequisites
remaining risk or N/A rationale: full isolated flake build was not completed; final repository gate must run it after remaining queue work
```

### TEST-017 — Ignored and uncompiled documentation is not API evidence

**Status:** verified. **Priority:** P1.

Five Observe rustdoc snippets are ignored, including affine/no-clone and
double-completion examples that should be compile-fail evidence. README and
current API documents contain many `rust,ignore` blocks that Cargo never checks.
The ignored `law_manifest` gate is covered by TEST-003.

- [x] Convert the Observe no-clone and double-complete snippets to real
  compile-fail fixtures; make positive affine usage compile/run or `no_run`.
- [x] Move current public README/API syntax into caller-facing compile-pass,
  compile-fail, or executable example fixtures. Mark genuinely illustrative
  pseudocode explicitly.
- [x] Do not treat historical design snippets as current API obligations.

Resolution (2026-10-01): the exact private Observe source is compiled through
the existing unpublished `observe-tests` package. Three downstream compile-fail
fixtures reject cloning publication authority, completing it twice, and
cloning affine observation; a positive integration test moves a non-`Clone`
`String` through completion. Before the change, the workspace reported five
ignored Observe doctests and executed none. Afterward, the Bombay and Observe
doctest runs report zero ignored tests. Current README syntax points to
executable application examples/tests, and the schematic fragments in the
current API, capability, and Driver documents are marked `text` and linked to
their compiled owners. The historical decision and archived diagnostic probe
retain their historical snippets; neither claims current compilation.
`nix develop -c cargo test --locked -p observe-tests --test observe_contract`
and its `--release` form passed, as did the locked workspace tests/build,
strict all-target Clippy, rustfmt check, and `git diff --check`. No public type
was added. TEST-017 unblocks ARC-020.

### TEST-018 — Observe's 121 ordinary tests execute twice

**Status:** verified. **Priority:** P2.

`observe-tests` path-imports the same private module that Bombay's library tests
run. The normal workspace suite therefore executes the same 121 ordinary tests
in both crates, plus duplicated ignored rustdocs. The isolated crate remains
valuable for Loom, fuzz, and performance boundaries.

- [x] Give ordinary Observe tests one owner.
- [x] Keep the isolated harness only for configuration-specific Loom/fuzz/perf
  jobs that cannot safely share the Bombay crate build.
- [x] Verify test listing after the change and document each retained duplicate
  technique.

```text
Resolution record (2026-09-28)
queue state: verified; ARC-019 is ready
upstream owner and locked revision: Bombay-private Observe; Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602
prior-representation failure command and result: pinned-Nix cargo test --locked -p observe-tests --lib -- --list and -p bombay-rs --lib -- --list each listed 121 ordinary Observe tests, including observe::external_tests::affine::affine_handles_retain_no_outcome_after_move
focused debug command and result: pinned-Nix cargo test --locked -p observe-tests --lib passed with zero ordinary tests; Bombay's lib listing retains all 121
focused release command and result: RUSTFLAGS='--cfg loom' LOOM_MAX_PREEMPTIONS=3 pinned-Nix cargo test --locked -p observe-tests --lib --release passed all 28 Loom tests
additional Loom/Miri/fuzz/compile/benchmark evidence: isolated Loom listing retained 28 cases; pinned-Nix cargo check --locked -p observe-perf and cargo check --locked --manifest-path crates/observe-fuzz/Cargo.toml --bins passed; full locked workspace test, strict all-target Clippy, rustfmt check, and git diff --check passed. Miri and fuzz execution remain separately owned by TEST-020
production: +0 / -0 / net 0; the isolated test harness changed +3 / -5 / net -2 source lines
tests:      +3 / -5 / net -2 harness lines
public API: +0 types / -0 types
documents/examples audited: ordinary tests live only in Bombay; the isolated path import remains for its 28 Loom tests and as a normal dependency for fuzz/performance, and the shared probe exists only for those normal dependencies. The broad duplicate-module lint allowance was removed
commit or working-tree reference: preserved dirty tree, 137 changed or untracked paths including this stage; cumulative source-file delta +3198 / -3750 / net -552 against HEAD includes the test harness
newly unlocked IDs: ARC-019 is ready; TEST-011 and TEST-013 still await ARC-019 plus their other listed dependencies
remaining risk or N/A rationale: ARC-019 owns physical source compilation simplification; TEST-018 changes no Observe semantics
```

### TEST-019 — The ignored performance comparison is not a test or benchmark

**Status:** verified. **Priority:** P2.

The former ignored local-runtime comparison performed seven
one-million-operation runs, printed medians, and asserted no acceptance
threshold. Its test-only timing fixtures have been removed; the existing
Entity and Observe performance harnesses retain their distinct workloads.

- [x] Move it to a benchmark/performance harness with recorded environment,
  measurement method, comparable baseline, and an explicit review threshold,
  or remove it.
- [x] Leave ordinary ignored-test inventory empty unless a test has a documented
  external prerequisite and a named CI/manual owner.
- [x] Correct the Observe performance harness descriptions for distinct-key
  retirement and same-key pooled reuse; retain both measured workloads.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Communication 0.1.2 owns mailbox delivery; Bombay local.rs owns ActorRef ingress. Behavior Core/Actors 0.17.0 and Macros 0.12.0 are locked to 435560ce7bea8ad3330ee2d42e5034f837a80602
prior-representation failure command and result: the pinned-Nix lib-test listing included compare_direct_and_entity_capable_ingress as an ignored ordinary test; its seven-run median had no assertion or review threshold, so a performance regression could not fail it
focused debug command and result: pinned-Nix cargo test --locked -p bombay-rs --lib local::tests:: passed 10 active tests with three documented ARC-006 ignores; --ignored --list names exactly those three and no performance test
focused release command and result: the same focused --release command passed 10 active tests with three ARC-006 ignores
additional Loom/Miri/fuzz/compile/benchmark evidence: no timing/benchmark behavior was retained or changed; full locked workspace tests, strict all-target Clippy, rustfmt check, and git diff --check passed. Separate Entity and Observe performance harnesses retain their own workload owners; TEST-020 owns final benchmark/coverage cadence
production: +0 / -0 / net 0 executable production behavior; the removed cfg(test) tail in local.rs is -163 test-only lines (physical production-source file shrank)
tests:      +0 / -163 / net -163 test-only lines in local.rs
public API: +0 types / -0 types
documents/examples audited: the only remaining ordinary ignored tests are address_is_absent_until_accepted_initialization_commit_completes, rejected_initialization_never_becomes_addressable, and corrupt_initialization_never_becomes_addressable. Each has the external post-commit/rejection contract prerequisite recorded under ARC-006 and TEST-023; manual replay owner is the ARC-006 implementer, using nix develop -c cargo test --locked -p bombay-rs --lib local::tests:: -- --ignored after that dependency is available. They are not counted as passing today. Five ignored Observe doctest snippets have a separate TEST-017 owner
commit or working-tree reference: preserved dirty tree, 138 changed or untracked paths; complete physical source-file classification against HEAD: production +2905 / -3819 / net -914, test/tool/example files +4737 / -2538 / net +2199, other +7349 / -1867 / net +5482. Production classification includes cfg(test) code embedded in local.rs
newly unlocked IDs: none; ARC-005 is the next lowest-sequence ready P2 item
remaining risk or N/A rationale: removing an unthresholded measurement leaves ordinary ingress without a performance floor; a new benchmark should be proposed only with a concrete regression threshold and supported caller surface. The three ARC-006 ignores cannot be made green before their upstream prerequisite
```

### TEST-020 — Mutation, coverage, fuzz, and Miri are mostly on-demand

**Status:** verified; TEST-003/004/005/017/018/024 prerequisites are terminal.
**Priority:** P2.

Coverage produces HTML/summary output but enforces no floor. The default mutant
gate covers only `bombay-rs`, has four function floors and one known-zero area,
and does not establish Driver branch coverage. The sweep emits a baseline rather
than enforcing it. Fuzz and Miri shells provide commands but no bounded CI
campaign, retained corpus, or required result.

- [x] Define per-owner mutation obligations and eliminate surviving semantic
  mutants, especially Driver terminal/failure branches and actor execution outcome
  classification.
- [x] Adopt coverage floors only after removing duplicate execution and mapping
  uncovered code to meaningful laws; never substitute a percentage for
  semantic assertions.
- [x] Add bounded reproducible fuzz and Miri jobs, with seed/corpus/result
  retention and an explicit cadence.
- [x] Record sanitizer applicability and a command/result if used.

The selected Behavior 0.20.0 owner map and change ledger are in
`open-design-ledger.md` under TEST-020. The new required Nix coverage check
passed Driver 128/136, actor execution 504/523, actor outcome 46/51, and
primary Observe 495/528 owner lines; the pre-change report failed the actor
outcome floor at 25/30. Its missing `SettlementFailed` law now has a pure
typed-custody test, debug/release passes, and a killed real-source mutation.
Pinned ASan fuzz passed 2,048 Driver runs and 1,024 runs for each of four
Observe targets with retained seeds, corpus, and results. Pinned Miri passed
12 Observe affine tests for each seed 0–3. CI runs both fuzz campaigns on
push/PR and Miri weekly or on manual dispatch. The final 870-candidate
cargo-mutants sweep passed its unmutated baseline and classified 342 caught,
524 unviable, four exact reviewed equivalences, and zero timeouts. The
generated 206-floor/246-zero-viable baseline passed the fail-closed check;
the earlier partial and disk-exhausted attempts provide no mutation verdict.

Linux CI recheck (2026-10-02): run `36982123753` passed its 21-check flake but
the Driver fuzz executable exited 127 before testing because the loader could
not find `libstdc++.so.6`. Its skipped Observe campaign also exposed an
unconditional artifact upload. The pinned fuzz shell now supplies
`stdenv.cc.cc.lib` on Linux, and each artifact upload follows its campaign's
attempted outcome. Run `36985076274` passed the Linux flake, Driver and
Observe bounded fuzz campaigns, and both artifact uploads. The two retained
artifacts are `driver-fuzz-36985076274` and
`observe-fuzz-36985076274`; local `nix flake check path:. --max-jobs 1
--cores 2` passed all 21 checks on the repair. Production behavior and
public types are unchanged.

Resolution record (2026-10-02)

```text
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.20.0 at 804b2bf25325a523884ec49d8a4ae6d2d2b6e9da; Macros 0.13.0 at 3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8
prior-representation failure command and result: nix develop -c cargo run --locked --release -p mutants-gate -- check <complete-sweep>/mutants.out mutants-baseline.json failed on unaccounted functions with the stale seven-entry zero-viable/three-floor baseline
focused debug command and result: pinned debug owner regressions and six mutants-gate tests passed; source mutations caught the previously surviving laws
focused release command and result: matching pinned optimized owner regressions and six mutants-gate tests passed
additional Loom/Miri/fuzz/compile/benchmark evidence: complete 870-candidate all-feature Nix sweep passed; coverage owner floors, five ASan fuzz campaigns, and four Miri seeds passed; conditional Loom is a terminal-audit gate
production: +7625 / -5961 / net +1664, cumulative physical working tree before this resolution record
tests: +7720 / -2584 / net +5136, cumulative physical working tree before this resolution record
public API: +0 types / -0 types in TEST-020
documents/examples audited: TEST-020 ledger, Driver test strategy, CI, flake, mutation baseline, coverage/fuzz/Miri owners and seed corpora
commit or working-tree reference: dirty working tree; complete sweep /nix/store/db5kynqpqyvkmjd3br1jfgpdjs3mn6sf-bombay-workspace-mutants-sweep-0.1.0
newly unlocked IDs: terminal audit of 19 feature-complete ARC rows
remaining risk or N/A rationale: terminal cross-repository minimization and final gate reruns remain open; no TEST-020 semantic survivor remains
```

Terminal performance addendum (2026-10-02): the first standalone Nix
performance build ran both Entity benches and failed only when packaging a
Criterion directory those custom benches do not create. The corrected
`flake.nix` runs the existing Driver Criterion benchmark and both Entity
benches, retains all three output reports plus Criterion data, and propagates
bench failures. `nix build path:.#packages.aarch64-darwin.performance
--print-out-paths --no-link --max-jobs 1 --cores 2` now passes; the complete
receipt is in the terminal checkpoint of `open-design-ledger.md`. This gate
repair adds no production or benchmark source and no public type.

### TEST-021 — `bombay-machine` exposes behavior without demonstrated consumers

**Status:** verified after ARC-004 removed the package. **Priority:** P2.

`Decision::map_effects`, `map_state`, `map`, and
`LinearizedExecutor::evidence()` had no concrete repository consumer or direct
public contract evidence. ARC-004 removed their package and retained its
Entity-specific concurrency laws in the owning directory with production
Loom evidence. Audit the complete repository for residue before verification.

- [x] Prefer removing unowned public surface as part of Machine minimization.
- [x] Add tests only if a concrete consumer first proves the shared semantic
  responsibility belongs in Machine.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Behavior Actors owns the distinct actor Machine, while Bombay Entity owns its concrete slot transition and Engine owns only Driver
prior-representation failure command and result: ARC-004's downstream bombay_machine::Decision compile-fail fixture resolved before dependency removal, causing trybuild to fail for the intended absence law; after removal it fails to resolve with E0432 and the boundary test passes. ARC-004's reversed output queue mutation also failed the production Loom ordering test and was restored
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --test entity_machine_boundary — 1/1 compile-fail boundary passed
focused release command and result: nix develop -c cargo test --locked --release -p bombay-rs --test entity_machine_boundary — 1/1 passed
additional Loom/Miri/fuzz/compile/benchmark evidence: nix develop -c sh -c 'cargo metadata --locked --no-deps --format-version 1 | python3 ...' reported 14 workspace packages and no bombay-machine package; nonhistorical repository scan found only the intentional compile-fail fixture and Engine absence assertion. ARC-004 already passed production Entity Loom, allocation, error, benchmark-build, full workspace, Clippy, and fmt gates. New concurrency/performance runs are N/A to this absence audit; git diff --check passes
production: +0 / -0 / net 0 this stage
tests:      +0 / -0 / net 0 this stage
public API: +0 types / -0 types this stage; ARC-004 removed the unowned public package
documents/examples audited: Cargo manifests and lock, every current source/example/benchmark, module boundaries, capability guide, Driver law, and Behavior Actors Machine owner; docs/driver-law.md now states the current boundary instead of a pending removal
commit or working-tree reference: preserved dirty worktree; 120 tracked/untracked changed paths at this checkpoint
newly unlocked IDs: ARC-015 becomes ready P1 and is selected next
remaining risk or N/A rationale: no concrete second consumer exists; no generic Machine test or API is retained. ARC-015 owns Entity public-surface distillation
```

### TEST-022 — Removed research paths left stale documentation references

**Status:** verified. **Priority:** P2.

The ledger formerly named a removed 0.15-era research probe, while the current
API guide referred to a missing macro comparison document. The historical
coherence finding and current macro evidence now have tracked owners.

- [x] Preserve any still-current conclusion in a tracked current or historical
  document, then remove or replace each dead link.
- [x] Run `rg -n '\.research|MACRO-LAST-COMPARISON' docs README.md crates examples`
  and require no unexplained current-guidance reference.
- [x] Label the two Behavior 0.19 research probes as dated snapshots; ensure
  their reproduction instructions do not claim the current 0.20 lock.

Terminal audit addendum (2026-10-02): the two retained 0.19 probes now state
their historical scope and point to the selected 0.20 ARC-010/TEST-025 record.
The proxy `Cargo.lock` claim is explicitly at capture, and the FIFO patch
instructions identify the historical snapshot. `rg` found no current-lock
claim in those notes; `git diff --check` passed. Documentation only:
production `+0/-0/net 0`, tests `+0/-0/net 0`, public types `+0/-0`.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Behavior Actors owns ActionItem requests and Behavior owns actor expansion
prior-representation failure command and result: rg -n '\.research|MACRO-LAST-COMPARISON' docs README.md crates examples found the dead 0.15 probe path in open-design-ledger.md and nonexistent macro comparison file in current user-facing-api.md
focused debug command and result: the same rg command now finds only explicit historical/queue-audit mentions; a relative Markdown link scan of docs and README found zero missing local targets. The four new macro evidence links resolve to tracked source/tests
focused release command and result: N/A for documentation-only provenance; the locked optimized macro tests passed under ARC-005 without a Rust edit here
additional Loom/Miri/fuzz/compile/benchmark evidence: N/A because no executable contract changed; git diff --check passed, and prior full workspace build/test/Clippy/fmt gates remain unchanged
production: +0 / -0 / net 0
tests:      +0 / -0 / net 0
public API: +0 types / -0 types
documents/examples audited: current user-facing-api.md now links directly to the plain-builder/differential macro test, compile fixtures, renamed downstream fixture, and facade source; open-design-ledger.md preserves the 0.15 ActionItem coherence conclusion in explicitly historical prose without selecting its deleted probe. All local Markdown links in docs and README resolve
commit or working-tree reference: preserved dirty tree, 138 changed or untracked paths; complete tree before this record: production +2905 / -3819 / net -914, test/tool/example files +4737 / -2538 / net +2199, other +7486 / -1882 / net +5604 against HEAD
newly unlocked IDs: none; no ready queue item remains
remaining risk or N/A rationale: historical evidence remains provenance only; current contracts are derived from the locked 0.17 owners and executable fixtures
```

### TEST-023 — Activation can become addressable before initialization commits

**Status:** verified; ARC-006 made the desired-law regressions executable and passing. **Priority:** P0.
**Owner:** local Environment. **Unblocks:** ARC-006.

`LocalEnvironment::activate` currently calls `AddressSpace::try_claim` before
`CommitActions::commit` interprets the initialization `Actions`. This contradicts
the documented activation transaction and permits resolution of an endpoint
whose initialization effects are still pending. Existing launch tests wait for
publication and therefore do not exercise direct address-space visibility.

- [x] Build a deterministic gated interpreter: enter initialization commit,
  hold it pending, and attempt to resolve the address concurrently.
- [x] Prove the address is absent before successful initialization settlement
  and present only after the complete accepted commit.
- [x] Add rejected and corrupt initialization cases proving the endpoint is
  never resolvable and the exact initialization/custody residual is returned.
- [x] Prove the regression fails on the current claim-before-commit order, then
  run the focused cases in debug and release.

Exit evidence: a causal trace of `initialize -> commit -> claim -> publish`, an
inversion trace for failed commit, and the exact Address lease/residual custody.

Resolution record (2026-09-23)

```text
queue state: verified; the three desired-law tests remain explicitly ignored until ARC-006 changes production, as authorized by the user
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Address 0.2.0 owns immediate claim visibility and Lease release; Communication 0.1.2 owns mailbox custody; Timers 13e884da7ab41781f52337b0038060e375b00ee0 and Bombay Observe are unchanged; LocalEnvironment owns transaction order
prior-representation debug commands and results: the accepted command `nix develop -c cargo test --locked -p bombay-rs --lib local::tests::address_is_absent_until_accepted_initialization_commit_completes -- --exact --ignored --nocapture` observed Present while commit was gated instead of Absent at local.rs:1330. `nix develop -c cargo test --locked -p bombay-rs --lib initialization_never_becomes_addressable -- --ignored --nocapture` ran both rejection cases through exact settlement retirement, then each observed Present instead of Absent at local.rs:1372.
prior-representation optimized commands and results: the same accepted and two-case commands with `--release` failed at the same visibility assertions and with the same typed observations
focused ordinary debug command and result: nix develop -c cargo test --locked -p bombay-rs --lib local::tests::; 10 passed, the three named TEST-023 cases and one pre-existing manual performance comparison ignored
focused ordinary release command and result: nix develop -c cargo test --locked --release -p bombay-rs --lib local::tests::; 10 passed with the same four explicit ignores
additional verification: nix develop -c cargo clippy --locked -p bombay-rs --lib --tests -- -D warnings passed; nix develop -c cargo fmt --all -- --check passed after applying the reported rustfmt layout; git diff --check passed
production: +0 / -0 / net 0
tests:      +305 / -0 / net +305 staged test-module lines
public API: +0 types / -0 types
documents/examples audited: driver-law.md, driver-test-strategy.md, runtime-capability-interfaces.md, module-boundaries.md, Local Environment/launch tests, and all public examples; none currently exposes direct AddressSpace activation visibility
commit or working-tree reference: uncommitted TEST-023 changes in local.rs, open-design-ledger.md, and this queue atop the preserved worktree
newly unlocked IDs: ARC-006
remaining risk or N/A rationale: the ignored tests intentionally keep the desired assertions unchanged while ARC-006 owns the production correction and removal of all three ignore attributes; Loom, Miri, fuzz, benchmarks, and examples are N/A for this deterministic evidence-only row and retain their assigned terminal-audit owners
```

### TEST-024 — The mutation verdict gate can accept incomplete outcomes

**Status:** verified. **Priority:** P1. **Owner:** `mutants-gate`.

`usable` rejects a failed baseline only when a baseline record exists; a report
with no baseline can pass. `tallies` counts mutant `Success` and `Failure` in
`total` but classifies neither as viable nor erroneous, and `emit_baseline` can
therefore turn infrastructure/test failures into `known_zero_viable` entries.

- [x] Require exactly one successful unmutated baseline, rejecting missing,
  duplicate, or non-successful baselines.
- [x] Exhaustively classify every mutant summary. Only a genuine `Unviable`
  outcome may seed `known_zero_viable`; `Success`, `Failure`, and future unknown
  statuses must fail closed.
- [x] Add table-driven fixtures for every `Summary`, missing/duplicate baseline,
  mixed failures, incomplete candidate results, and stale baseline entries.
- [x] Prove the current gate accepts at least one deliberate malformed report
  before correcting it.

```text
Resolution record (2026-09-28)
queue state: verified
upstream owner and locked revision: Cargo.lock selects Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; cargo-mutants owns report generation and mutants-gate owns complete fail-closed verdict interpretation; no runtime dependency contract changed
prior-representation failure command and result: with only the new malformed-report tests added, nix develop -c cargo test --locked -p mutants-gate tests::baseline_cardinality_and_every_mutant_summary_fail_closed -- --exact failed exit 101 because the old check accepted a report with no baseline; the companion incomplete/stale test failed exit 101 because the old check did not identify a stale known-zero entry. No inverted assertion or expected-panic device was used
focused debug command and result: nix develop -c cargo test --locked -p mutants-gate — 5/5 passed
focused release command and result: nix develop -c cargo test --locked --release -p mutants-gate — 5/5 passed
additional Loom/Miri/fuzz/compile/benchmark evidence: strict mutants-gate all-target Clippy, workspace rustfmt check, and diff whitespace passed. The preserved local mutants.out report has no unmutated baseline; the pinned nix develop -c cargo run --locked -q -p mutants-gate -- check mutants.out mutants-baseline.json now rejects it with exit 1 and the exact missing-baseline diagnostic. Separate concurrency/fuzz/performance tools are N/A to this JSON verdict tool
production: +98 / -45 / net +53 tool lines
tests:      +146 / -14 / net +132 tool lines
public API: +0 types / -0 types
documents/examples audited: actual cargo-mutants outcomes/candidate JSON, mutation baseline, Nix mutation derivation, Driver test strategy, crate architecture matrix; no application example changed
commit or working-tree reference: preserved dirty worktree; 120 tracked/untracked changed paths at this checkpoint
newly unlocked IDs: none; selector next chooses the lowest-sequence ready P2 row
remaining risk or N/A rationale: the existing local mutants.out is an incomplete historical report, not a green gate; the final full Nix mutation sweep remains a separate TEST-020/terminal obligation
```

### TEST-025 — Supervisor and pool examples do not execute their policies

**Status:** verified against selected Behavior Core/Actors 0.20.0. **Priority:** P1.
**Owner:** Behavior Actors capability interpretation and public examples.

The earlier `supervision` public example called only `initialize` and inspected
initial `Actions`. Its current binary executes worker replacement and
retirement. The `worker-pool` example and an independent FIFO runtime
regression now admit and complete a job, return the exact customer result,
and perform orderly worker shutdown.
An additional FIFO capacity-one regression holds activation, admits one queued
job, and returns the next submission's exact payload and `BacklogFull` reason
before releasing the worker and completing orderly drain. Raising capacity to
two fails the rejected-outcome assertion. Another independent FIFO `App`
regression executes permanent recovery: one
worker completes and stops, the source prepares exactly one replacement, the
replacement completes a second correlated job, and root plus both workers
return completed stopped terminals. An inversion that keeps the first worker
alive fails the source-preparation witness.
The same fixture exercises both interruption policies when an assigned worker
stops without completing: `Retry` completes the original job on a replacement;
`Fail` returns the exact assigned payload and worker-stop reason before that
replacement serves a later job. Both paths preserve customer correlation,
source count, orderly shutdown, and two-worker terminal custody.
The public supervision binary now performs a worker stop, replacement,
supervisor shutdown, and exact root/proxy/two-worker retirement using selected
Actors 0.20.0. Independent selected-release tests run coordinated two-role
replacement and later-role rejection with diagnostic and terminal custody.
The held-source FIFO test rejects a later job as `ShuttingDown` before the
source returns, then retires without creating a replacement.

- [x] After ARC-010, run both templates through `Application`/`App` without
  recreating their state machines in Bombay.
- [x] Supervision: force one worker failure/recovery and assert complete typed
  lifecycle, diagnostic, shutdown, and retirement traces.
- [x] Pool: admit work, complete it, exercise backlog/interruption policy, and
  assert exact customer result, worker return, shutdown, and terminal custody.
- [x] Retain focused pure-fold initialization tests only where they prove an
  independent construction law rather than duplicating the end-to-end trace.

```text
Resolution record (2026-10-01)
queue state: verified
upstream owner and locked revision: Behavior Core/Actors 0.20.0 at 804b2bf25325a523884ec49d8a4ae6d2d2b6e9da, with Macros 0.13.0; FixedSupervisor, StableProxy, FifoPool, and their policy inputs remain upstream-owned
prior-representation failure command and result: the old public supervision binary inspected only pure initialization, so it had no worker stop/replacement or terminal witness; the isolated 0.19.0 live supervisor probe failed E0277 on absent typed proxy diagnostic ingress and the held-source FIFO probe timed out awaiting a pre-release ShuttingDown rejection
focused debug command and result: nix develop -c cargo run --locked -p bombay-example-supervision and -p bombay-example-worker-pool both passed; selected runtime tests fixed_supervisor_runtime (1/1), fixed_supervisor_recovery (2/2), and fifo_pool_recovery (5/5) passed
focused release command and result: both public example binaries passed under cargo run --locked --release; all three selected runtime test targets passed 1/1, 2/2, and 5/5 under --release
additional Loom/Miri/fuzz/compile/benchmark evidence: selected supervisor test preserves the exact terminal WorkerPreparationFailed diagnostic and source role order; FIFO tests preserve exact customer correlation, both interruption choices, held-source shutdown, and terminal custody; full locked workspace test/build, strict Clippy, rustfmt, git diff --check, and all 21 Nix flake checks passed. No extra concurrency model is required for this example audit
production: +0 / -0 / net 0 in this evidence-only TEST-025 iteration; complete worktree physical crate src/ remains +5383 / -4655 / net +728 against HEAD
tests: +0 / -0 / net 0 in this evidence-only TEST-025 iteration; complete standalone test/example/tooling worktree remains +7528 / -2401 / net +5127
public API: +0 types / -0 types
documents/examples audited: public supervision and worker-pool binaries, examples README, current PRD evidence, all selected supervisor/FIFO runtime tests, and the supervision pure initialization test, whose creation/observation correlation is independent of the live trace
commit or working-tree reference: preserved dirty tree; 198 changed paths including 60 untracked at the pre-resolution checkpoint
newly unlocked IDs: none in the audit queue, because ARC-020 retains other blockers; DIST1 in the wider feature graph also retains other blockers
remaining risk or N/A rationale: no new production type or policy was needed; ARC-020 owns final repository-wide API minimization
```

## Architecture findings

These findings came from the same crate audit. Keep each production redesign in
a separate ledger-backed stage from the test repairs above. “Confirmed” means
the structural fact exists in current source; it does not mean the replacement
design may skip its required regression or upstream comparison.

### ARC-001 — Replace erased shutdown authority

**Status:** distilled; terminal repository audit confirmed minimal remaining surface. **Priority:** P0.
**Owner:** local Communication/lifecycle boundary.

The prior `ActorRef` stored `Weak<dyn ShutdownControl>`, erasing the concrete
Behavior event type. Its `Option` also encoded the semantic distinction between
a runtime actor with shutdown authority and an external endpoint without it.
The public reference combined delivery, termination observation, and lifecycle
control even though the architecture calls them separate capabilities.

- [x] Write compile-pass/fail syntax for delivery-only, observation, and typed
  shutdown custody before changing representation.
- [x] Remove `dyn ShutdownControl` and the single production implementation;
  use the concrete Behavior event/control capability or a type-level projection
  without creating a second mailbox or actor contract.
- [x] Make external-versus-runtime authority a type distinction, not
  `Option<Weak<_>>`. Return the exact original authority/input on rejection.
- [x] Prove ordinary sending remains protocol-indexed and cloneable while
  shutdown ownership cannot be forged, attached to an external endpoint, or
  used after termination.

Exit evidence: no production `dyn`/erased lifecycle authority, public syntax
fixtures, and unchanged complete shutdown/termination traces.

Resolution (2026-10-01): the pre-change desired compile-pass fixture failed
solely because `ApplicationLifecycle<P>` could not carry a concrete event
index. `ApplicationHandle<P, E, Families>` now retains the root's typed weak
control projection and hands `ApplicationLifecycle<P, E>` to the boundary;
`ActorRef<P>` carries delivery and exact termination observation without an
optional shutdown field. Direct child and Entity retirement borrow their
existing exact `ControlSender<Child::Event>` from the installed binding/lease.
`ShutdownChild` rejection retains its complete original typed request;
application shutdown borrows its lifecycle authority, and the synthesized
zero-sized ingress has no caller-owned payload to lose. Compile-pass delivery,
observation, and lifecycle syntax passes; wrong-event and delivery-reference
shutdown inversions fail for the intended type/method reasons. First, repeated,
and post-termination shutdown results pass in debug and optimized runs. Full
locked workspace tests pass in both profiles; strict all-target Clippy passes.
The two ignored ARC-011 cancellation witnesses remain separate. The complete
working tree checkpoint is in the open design ledger.

### ARC-002 — Remove or make external address claims observable

**Status:** distilled; terminal repository audit confirmed minimal remaining surface. **Priority:** P1.
**Owner:** external interface/address composition.

`ExternalActor::establish` previously created a private `AddressSpace`, claimed
into it, and discarded the only resolver. The unobservable claim, lease, and
unreachable address-conflict error were removed. A lease that guards a table
no other value can query does not own an observable invariant.

- [x] Add an observable test for the intended address claim, or prove that only
  the allocated origin identity and exact recipient matter.
- [x] If resolution is required, use the application-owned address space and
  prove retirement removes the exact claim. Otherwise delete the private space,
  lease, and `ExternalActorError::Address` path.
- [x] Preserve affine receive authority and exact rejected-message recovery.

Resolution (2026-10-01): no resolver of the private space escaped
`ExternalActor::establish`; the existing two-origin and exact-reply tests prove
allocated `MailAddr` plus `EstablishedRecipient<P>` is the complete external
contract. The new exhaustive-error compile-pass fixture failed on the prior
unreachable `ExternalActorError::Address` variant and passes after removing
that variant, the private space, and its lease. Affine receiver denial and
exact rejected-message recovery pass in debug and optimized focused tests.
The full locked debug workspace suite, formatting, strict all-target Clippy,
and diff check pass. ARC-002 contributes production net `-13` lines and no new
public type; the complete tree checkpoint is in the open design ledger.

### ARC-003 — Give Entity transition classification one owner

**Status:** distilled; terminal repository audit confirmed minimal remaining surface. **Priority:** P1.
**Owner:** Entity lifecycle. **Unblocks:** TEST-010 and TEST-021.

`SlotReducer::reduce` owns the executable transition, while
`EntitySlot::handles`, `SlotEvent::trigger`, `LifecycleEdge::endpoints`,
`LIFECYCLE_TOPOLOGY`, and the post-reduction topology lookup independently
restate its classification. `LifecycleMachine` wraps the reducer largely to
recover `TransitionEvidence`, which callers then decode back into domain
results. Coordinated drift can pass the current exhaustive test.

- [x] After ARC-007 identifies the law Bombay still owns, choose one total
  transition whose returned domain disposition contains the exact successor,
  effects, returned payloads, and classification.
- [x] Generate documentation/topology from that owner or validate a declarative
  table directly against it; never consult a second table after execution to
  decide what happened.
- [x] Delete `handles`, edge/trigger conversions, wrapper machinery, and
  evidence decoding that no longer owns a responsibility.
- [x] Use TEST-010's independent state/input oracle and exhaustive sequence
  enumeration to prove the simplification.

ARC-003 retained the `LifecycleMachine` `Base` adapter solely to satisfy the
current ordered executor port; ARC-004 owns its deletion/minimization. The
post-reduction `handles`/`trigger`/edge lookup is gone. A pure state/input
oracle now compares complete effects and slot authority over the existing
137,561 bounded traces and direct edge/replay sequences. Its early-fence
inversion failed under the prior classification in debug and optimized builds,
and the complete oracle failed when that old branch was temporarily restored.
The task-local change ledger and gates are recorded in
`docs/open-design-ledger.md`; ARC-003 is feature-complete rather than distilled.

### ARC-004 — Minimize `bombay-machine`

**Status:** distilled; TEST-021 and terminal audit confirmed package absence.
**Priority:** P1. **Owner:** Entity only if a residual law remains.

The crate exposes two transition algebras—`Reducer<S, E> -> Decision<S, F>` and
`Machine::step(self, input) -> (Output, Self)`—plus topology composition and
three executors. Production uses only Entity's adapter and
`LinearizedExecutor`; `Compose`, `Then`, `Product`, `Routed`, `Either`,
`Structure`, `ExclusiveExecutor`, and `SerializedExecutor` have no production
consumer. `OutputEvidence` has only Entity as a production implementation, and
the executor retains cloned “latest evidence” as a historical breadcrumb.

- [x] Compare the remaining Entity law first with locked Behavior transitions,
  `StableProxy`, and `DynamicSupervisor`; do not expand this crate to make the
  comparison compile.
- [x] If Entity still needs one concurrent slot primitive, prototype it as a
  concrete private Entity owner. Retain a generic/public crate only if at least
  two production consumers prove identical ordering, poisoning, evidence, and
  terminal laws.
- [x] Delete unused combinators, executors, `Decision::map*`, stored evidence,
  re-exports, and tests that only test the deleted framework.
- [x] Keep Loom evidence for any retained concurrency primitive and measure the
  public-type and production-line reduction.

Target outcome: remove `bombay-machine` from Bombay's production dependency if
no independently shared law survives.

ARC-004 removed the package and the Entity adapter, retained the private
Entity-owned slot and production Loom ordering evidence, and recorded the
complete-tree line/type delta and gates in the design ledger. The historical
generic Machine test edits were removed with that package.

### ARC-005 — Remove manifest and workspace residue

**Status:** distilled; ARC-020 and terminal audit confirmed no manifest residue.
**Priority:** P2.

- [x] Remove redundant `bombay-rs` dev-dependencies for `tokio` and
  `bombay-machine` if the normal dependencies already supply every required
  feature.
- [x] Remove the stale `crates/bombay/fuzz` workspace exclusion; no manifest or
  source exists there.
- [x] Evaluate aligning `bombay-macros` with the selected Syn major version.
  Retain Syn 2 and Syn 3 together only if migration evidence proves the split is
  necessary.

```text
Resolution record (2026-09-28)
queue state: feature-complete; ARC-020 owns final workspace minimization
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Bombay macros own only syntax projection
prior-representation failure command and result: root manifest excluded nonexistent crates/bombay/fuzz; Bombay duplicated Tokio's macros feature in dev-dependencies; cargo tree showed first-party bombay-macros on Syn 2 while the locked Behavior macro owner uses Syn 3. The initial locked Syn 3 prototype correctly rejected a stale lock (exit 101); an offline pinned-Nix resolution updated Cargo.lock, then bombay-macros compiled without source changes
focused debug command and result: pinned-Nix cargo check --locked -p bombay-rs --all-targets passed after removing the redundant Tokio dev dependency and stale exclusion; the six focused Bombay macro/compile-fixture targets and renamed downstream crate passed with Syn 3
focused release command and result: pinned-Nix cargo test --locked --release -p bombay-rs --test macro_last_authoring --test terminal_projection passed 5/5 and 3/3, including compile-fail fixtures; full locked workspace build and test passed after the manifest change
additional Loom/Miri/fuzz/compile/benchmark evidence: strict all-target workspace Clippy, rustfmt check, and git diff --check passed; Loom/Miri/fuzz/benchmark semantics are unchanged. Syn 2 remains in Cargo.lock through transitive zerocopy-derive, so removing it would require an unrelated upstream migration
production: +0 / -0 / net 0 Rust source; manifests +1 / -4 / net -3, Cargo.lock macro dependency +1 / -1 / net 0
tests:      +0 / -0 / net 0
public API: +0 types / -0 types
documents/examples audited: root and Bombay manifests, Cargo.lock, Nix gates, selected macro source/compile fixtures, renamed downstream crate, and current dependency inventory; no public example behavior changed
commit or working-tree reference: preserved dirty tree, 138 changed or untracked paths; complete tree against HEAD production +2905 / -3819 / net -914, test/tool/example files +4737 / -2538 / net +2199, other +7416 / -1871 / net +5545 before this record
newly unlocked IDs: none; TEST-022 is the next lowest-sequence ready P2 item
remaining risk or N/A rationale: the remaining Syn 2 dependency is transitive, and the exact locked Behavior Syn 3 owner now matches Bombay's first-party macros. Final project-wide API/manifest audit remains ARC-020
```

### ARC-006 — Make activation one commit-before-claim transaction

**Status:** distilled; Address 0.3.0 reservation and Behavior 0.20.0 child commitment passed terminal audit. **Priority:** P0.
**Owner:** `LocalEnvironment` implementation of Engine `Environment`.

The prior order was address claim, initialization action commit, then endpoint
publication. Address 0.3.0 now reserves an address invisibly before commitment,
and Bombay publishes the reservation only after accepted continuing settlement
and source admission. Complete debug and optimized evidence now covers every rejection and terminal custody path.

- [x] Add TEST-023 without modifying production and capture the failing causal
  trace.
- [x] Reserve the exact address invisibly before interpreting initialization,
  then publish that reservation only after accepted continuing settlement.
  Preserve untouched initialization `Actions` on reservation rejection.
- [x] Return exact unpublished terminal custody after any committed early
  stop, rejection, or corruption; child creation reports private host
  commitment before public publication.
- [x] Prove address reservation rejection, abandoned private binding, nested
  creation progress, and final termination custody end to end in debug and
  optimized builds.
- [x] Cross-check Driver initialization precedence with TEST-001 so activation
  failure, settlement failure, stop, and host rejection cannot mask one another.
- [x] Correct the planning inventory's current activation status note while
  retaining its dated ACT requirements.

Terminal documentation addendum (2026-10-02): the current planning notes now
record ARC-006's invisible Address reservation and ordinary visibility
regressions; the original ACT marks remain dated requirements. The original
claim-before-commit implementation failed those tests, and the locked
workspace suite passed them. `git diff --check` passed. Documentation only:
production/tests `+0/-0/net 0`, public types `+0/-0`.

Resolution record (2026-10-01)
queue state: feature-complete; final repository distillation remains open
upstream owner and locked revision: Behavior Core/Actors 0.19.0 at e5c703e966eba4d2a15fe2c129594f57ae2270fc and Macros 0.13.0 at 3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8; Address 0.3.0; Communication 0.1.2; Timers 13e884da7ab41781f52337b0038060e375b00ee0; Bombay-private Observe
prior-representation failure command and result: the three TEST-023 causal visibility regressions failed in both debug and optimized builds under the original claim-before-commit order, as recorded under TEST-023 and ARC-006 in the live ledger
focused debug command and result: `nix develop -c cargo test --locked -p bombay-rs --lib launch::tests::address_reservation_rejection_returns_untouched_initialization -- --exact` passed; `nix develop -c cargo test --locked -p bombay-rs --test application_terminal_custody privately_bound_child_reports_its_nested_creation_before_parent_retirement -- --exact` passed
focused release command and result: both exact focused commands above with `--release` passed; the full locked release workspace suite passed before the final test-only nested witness was added
additional compile/precedence evidence: `nix develop -c cargo test --locked --workspace --quiet` passed with the nested witness; the Engine stopping-initialization rejection/corruption, activation failure, pure-init panic and Local unpublished-custody tests ran in both workspace profiles; strict workspace Clippy, workspace build, rustfmt check, and `git diff --check` passed at the feature checkpoint
complete working-tree checkpoint, including inherited edits and 45 untracked paths: 171 changed paths; all crate `src/` files `+4405/-4363/net +42` (includes embedded tests and tools); tests/examples/tools outside `src/` `+5036/-2389/net +2647`; documents `+11943/-1837/net +10106`; other files `+100/-79/net +21`; raw `pub` type/trait declarations `+12/-40` across the cumulative tree. The final nested witness adds production `+0/-0/net 0` and public types `+0/-0`.
newly unlocked IDs: ARC-001 is ready; PLACE1 remains blocked by NET1, AUTH1, and authoritative placement/fencing
remaining risk or N/A rationale: grandchild initialization may be canceled before or after its own accepted initialization settlement once its typed creation has been acknowledged; the witness preserves either exact terminal custody. The two ignored cancellation tests belong to ARC-011; its corrected startup-waiter witness fails for the exact reservation task-ownership law. ARC-006 remains subject to terminal repository-wide distillation; it is not `distilled`.

Historical blocker record (2026-09-23; superseded by selected Address 0.3.0)

```text
selection: ARC-006 was selected after TEST-023, verified against every locked owner, and returned to blocked without a production edit
exact external blocker: Behavior 0.17.0 ChildCreationOutcome::HostRejected requires the current child plus complete initialization Actions that were never interpreted (actor/creation.rs:621-652). Commit-before-claim necessarily consumes those Actions; a later AddressSpace::try_claim failure can retain only the factual interpretation settlement. The locked result algebra has no post-commit host-rejection alternative capable of carrying that settlement.
rejected Bombay workarounds: cloning or reconstructing Actions; erasing the settlement; reporting committed work as an untouched/corrupt creation; a second address table; a Bombay-only pending endpoint; a boolean/gated ActorRef; TOCTOU preflight resolve; or treating ClaimError as unreachable. Each loses custody, duplicates an owner, exposes another invalid state, or contradicts Address 0.2.0.
external contract required to resume: either Behavior must own a post-commit child establishment rejection carrying exact child state and ActionSettlement, or Address must own a non-resolvable affine reservation that can be atomically promoted after interpretation while preserving uncommitted Actions on reservation rejection. The selected releases provide neither contract.
production/test/public-API delta for ARC-006: +0/-0/net 0; +0/-0/net 0; +0/-0 types
dependency effect: ARC-001 remains blocked; useful ready work remains, so the overall goal and queue continue with ARC-007
```

### ARC-007 — Differentially separate Entity from Behavior Actors

**Status:** distilled; retained stable routing is separated from Behavior Actors by terminal differential evidence. **Priority:** P0.
**Owner:** Behavior Actors unless a Bombay-only gap is proven.
**Blocks:** ARC-003, ARC-004, ARC-014, and Entity expansion.

Bombay Entity owns stable logical identity, hydration, admission, fences,
passivation, and replacement. Locked Behavior Actors already owns zero/one
worker lifecycle in `StableProxy`, bounded keyed supervision in
`DynamicSupervisor`, worker initialization/replacement, discovery
`Registry`/`Resolver`, its actor `Machine` template, shutdown, and terminal
policy. The current local
directory/machine must not silently own those same laws twice.

- [x] Produce a state/input/output/error/terminal table comparing
  `EntitySlot`/`EntityRuntime` with `StableProxy`, `DynamicSupervisor`, and
  `Registry`/`Resolver` and the Behavior Actors `Machine` at the exact locked
  revision.
- [x] Build a pure-fold differential probe for activation, concurrent first
  admissions, hydration failure, replacement, stale facts, passivation,
  shutdown, and exact terminal custody.
- [x] Assign upstream ownership wherever traces and invariants are identical.
  Compose the concrete upstream Behavior; do not translate it into Entity
  events/effects or copy its transition table.
- [x] Record each residual Bombay-only gap and why upstream composition cannot
  express it. Expected candidates are stable runtime identity, asynchronous
  hydration/admission, and the ordered fence—not generic lifecycle or
  supervision.
- [x] Update module boundaries, the open ledger, and public examples before any
  retained Entity redesign.

Exit evidence: an explicit ownership table, differential traces, deleted local
machinery for every upstream-owned law, and no new parallel actor abstraction.

Resolution record (2026-09-23)

```text
selection and versions: selected after ARC-006 reached its exact external blocker; verified Behavior Core/Actors 0.17.0 and Macros 0.12.0 from 435560ce7bea8ad3330ee2d42e5034f837a80602, Address 0.2.0, Communication 0.1.2, Bombay-private Observe, and Timers 13e884da7ab41781f52337b0038060e375b00ee0
ownership result: StableProxy owns actor-child creation, initialization, readiness, explicit replacement, shutdown, and exact child custody; DynamicSupervisor owns keyed proxy management and operation authority; Registry/Resolver own typed recipient discovery; Behavior Actors Machine owns receive/become/defer/stop. Bombay Entity alone owns absent-key command retention through asynchronous hydration/launch, exact-incarnation delivery reservations, the ordered fence, affine runtime lease retirement, and matching removal.
differential result: the executable locked StableProxy fold returns a second worker start as InitialWorkerOutcome::Overlap while Entity retains a second domain command in the one bounded activation attempt and later returns both waiters in order on hydration failure. The complete Entity trace proves reactivation after removal, stale activation lease retirement, stale fence and termination rejection, fence-before-retire, graceful lease transfer, and exact removal.
prior-representation proof: temporarily rejecting the second production Entity waiter as Busy failed entity_first_demand_is_not_stable_proxy_worker_start at the empty-effect retention assertion; temporarily bypassing EnqueueFence and retiring immediately failed entity_trace_owns_hydration_waiters_fence_and_runtime_generation at the required Draining state. Both mutations were restored. No ignore, expected panic, inverted assertion, or copied implementation model accepted either defect.
deletion result: no Entity production event/effect duplicates an upstream actor-template trace, so production deletion here would erase Bombay-only law. The complete trace superseded and deleted three narrower tests for hydration-failure ordering, stale activation, and stale termination. ARC-003/004 now own deletion of the genuinely duplicate local LifecycleMachine/TransitionEvidence/bombay-machine classification layers; ARC-007 remains feature-complete rather than distilled until that audit finishes.
commands and results: nix develop -c cargo test --locked -p bombay-rs --lib entity::lifecycle::tests::entity_ -- --nocapture (2 passed); the same command with --release (2 passed); each of the two temporary production mutations failed its exact focused test with exit 101; nix develop -c cargo test --locked -q -p bombay-rs --lib entity:: and the --release equivalent (13 passed each); nix develop -c cargo test --locked -p bombay-rs --test entity_runtime --test entity_family --test entity_application and the --release equivalent (15 passed each); nix develop -c cargo test --locked -q -p bombay-rs (complete package passed: 166 library tests, four deliberate TEST-023/manual ignores, all integrations/compile fixtures, five Entity Loom tests, and doctests); nix develop -c cargo run --locked -p bombay-example-entity (passed); nix develop -c cargo clippy --locked -q -p bombay-rs --lib --tests -- -D warnings (passed); nix develop -c cargo fmt --all -- --check (passed); git diff --check (passed)
production/test/public-API delta: +0/-0/net 0; +225/-43/net +182; +0/-0 types. No wrapper, actor, state product, trait, alias, or effect language was added.
documentation/example result: docs/open-design-ledger.md contains the exact owner table; docs/module-boundaries.md and docs/runtime-capability-interfaces.md state the retained boundary; the Entity example distinguishes command-triggered reactivation from StableProxy replacement.
dependency effect: ARC-003, ARC-008, and ARC-017 are now ready. ARC-004 remains blocked on ARC-003. The selector chooses TEST-008 next because it is the lowest-sequence ready P1 row.
N/A: Miri, fuzz, and benchmarks add no ownership evidence for this pure-fold/documentation item and remain mandatory under their assigned queue rows and the terminal audit.
```

### ARC-008 — Give each child occurrence one cohesive binding state

**Status:** distilled; terminal repository audit confirmed minimal remaining surface.
**Priority:** P1. **Owner:** Behavior child product plus Bombay runtime binding.

`ChildBinding` splits one occurrence across a creation map, endpoint/control
map, task vector, retired vector, and a parallel `ChildSpace` product. Separate
`CreationBindingAt*`, `ChildBindingAt*`, and `ChildSpaceAt*` cursor-trait families
walk the same Behavior-owned HList. `CreationBinding::Established` stores only a
route, so the representation can claim establishment while the endpoint/control
entry is absent; different interpretation paths classify that state as either
corrupt or rejected.

- [x] Use locked `ChildOccurrenceShape`/`ChildOccurrences` as the single
  structural product. Define one occurrence-owned closed state for creation,
  endpoint/control, task, and retirement custody, including any lawful pending
  phase.
- [x] Collapse parallel cursor traversals to one static occurrence selector;
  do not reimplement Behavior creation routing or positional effect traversal.
- [x] Make “established without its capabilities” unrepresentable and specify
  one exact rejection/corruption law for every remaining absence.
- [x] Add inversion tests for wrong occurrence, duplicate binding, rejected
  creation, stale route, retirement, and complete descendant custody.

One `ChildOccurrences` product now owns each occurrence's address space and a
closed creation entry. `Established` carries endpoint, control sender, kind,
and projected task; `Rejected` carries none. Map absence covers pending or
unknown IDs. One selector replaces the three cursor families; a creation-order
list preserves task retirement order. A route is consumed by launch and
terminal projection, so no stale route remains in settled storage. Internal
tests distinguish rejected versus wrong-occurrence IDs and prevent duplicate
overwrite. The two-child application trace proves exact, ordered terminal
custody. The application-topology example exercises same-action child delivery
and phased child shutdown.

Resolution record (2026-09-28)
```text
queue state: feature-complete
upstream owner and locked revision: Behavior Core/Actors 0.17.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602 own creation and child policy; Bombay owns concrete occurrence custody
prior-representation failure command and result: nix develop -c cargo test --locked -p bombay-rs --lib child_bindings::tests::established_creation_owns_its_endpoint_and_control -- --exact and its --release form both exited 101 because Established could be recorded without an endpoint; the invalid construction is no longer expressible
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --lib child_bindings::tests:: (2 passed); focused actor_interface, application_terminal_custody, entity_application, and terminal_projection integrations passed
focused release command and result: the same unit and four integration targets with --release passed; the two-child ordered terminal case passed separately
inversion evidence: changing duplicate recording to overwrite made duplicate_creation_keeps_the_first_exact_settlement fail (exit 101); reversing creation-order retirement made root_returns_only_after_owning_ordered_direct_child_terminals fail (exit 101); both production mutations were restored. Wrong occurrence has a distinct typed selector; rejected is not established; stale route is N/A because settled storage contains no route
additional Loom/Miri/fuzz/compile/benchmark evidence: application-topology example ran and checked same-action delivery plus child shutdown. Existing compile fixtures and the full locked workspace test passed before the last two-child test edit; final suite rerun follows. Loom, Miri, fuzz, and benchmark are N/A because the change is static occurrence representation and preserves the same externally synchronized primitives
production: +122 / -425 / net -303 since the pre-ARC-008 checkpoint
tests:      +92 / -24 / net +68 since the pre-ARC-008 checkpoint
public API: +0 named types / -0 named types; removed constructs were crate-private
documents/examples audited: open-design-ledger.md, runtime-capability-interfaces.md, module-boundaries.md, application-topology example, child/Entity tests, and selected Behavior owner sources
commit or working-tree reference: uncommitted ARC-008 changes in the preserved working tree
newly unlocked IDs: none; ARC-009 still waits for upstream-blocked ARC-010
remaining risk or N/A rationale: project-wide distillation is deferred to the terminal audit; TEST-012 is the next ready P1 item
```

### ARC-009 — Decompose application capabilities by semantic owner

**Status:** retained after selected-contract composition proof. **Priority:** P1.
**Owner:** typed effect-lane interpreters.

`ApplicationCapabilities` stores actor spaces, allocation, control, timers,
facts, peer observations, child routes/bindings, activation tasks, exact
observations, terminal reports, parent reports, and origin projection, while
directly implementing many unrelated `InterpretItem` contracts. This is the
workspace's clearest god-object risk. `LaunchSystem`, `RetireCapabilities`, and
`TerminalReportTransaction` also have one production implementation and partly
serve to hide generic bounds rather than model substitutable ports.

- [x] Map every field and `InterpretItem` impl to one semantic owner and its
  locked Behavior request. Mark genuinely shared transaction state explicitly.
- [x] Do not mechanically split the struct into forwarding wrappers. Give state
  to concrete lane owners, then retain at most a thin static product/composition
  boundary required by Behavior interpretation.
- [x] Remove single-implementation traits used only for bound compression;
  prefer private concrete functions, inherent methods, or named associated
  products at the narrowest law.
- [x] Preserve `ActionInterpreter` as the direct adapter from complete Behavior
  `Actions` to Engine `CommitActions`; it is not a second effect algebra.
- [x] Prove retirement returns every activation task, child terminal, report,
  and unattempted source action after decomposition.

The exact Behavior product traversal requires one concrete interpreter type for
all leaves. A tuple of independently implemented timer and parent-report lane
owners fails E0277 for both leaves; a named product compiles only after adding
forwarding `InterpretItem` implementations. Bombay's current product already
owns the static composition and directly interprets each leaf through typed
Address, Communication, Observe, Timers, child-binding, report, and task owners.
The actor-local child-route counter and exact-observation cancellation map
cross those primitive boundaries and have no narrower existing owner. The
three single-implementation traits each own a distinct transaction or affine
launch/retirement port; none solely compresses a bound. Adding wrappers would
grow code while preserving the same owner and observable trace. Retention is
the smallest Behavior-first disposition; ARC-011 may still reduce launch code
under its separate failing cancellation law.

```text
Resolution record (2026-10-01)
queue state: retained
upstream owner and locked revision: Behavior Core/Actors 0.20.0 at 804b2bf25325a523884ec49d8a4ae6d2d2b6e9da; Behavior InterpretSends requires one Interpreter for both SendLayer leaves and owns every typed settlement
prior-representation failure command and result: nix develop -c cargo check --locked --manifest-path /tmp/bombay-arc009-direct-owners/Cargo.toml failed E0277 twice when the composed interpreter was only (ParentReportLane, TimerLane); a new ActorCapabilities product with two forwarding impls passed the same command but added no semantic law
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --test application_terminal_custody passed 4/4; the selected supervisor, FIFO, source-preparation, and terminal-report tests passed in the complete workspace suite
focused release command and result: the same application_terminal_custody target with --release passed 4/4; selected supervisor and FIFO focused targets passed in --release; Nix flake's release workspace target passed
additional Loom/Miri/fuzz/compile/benchmark evidence: all 21 Nix flake checks passed, including Driver and incarnation law evidence, Loom, documentation, strict Clippy, and public examples. New Loom/Miri/fuzz evidence is N/A because this iteration changes no runtime state or synchronization
production: +0 / -0 / net 0 in this retention audit; complete physical crate src/ worktree against HEAD is +5383 / -4655 / net +728 including embedded tests
tests: +0 / -0 / net 0 in this retention audit; complete standalone test/example/tooling worktree is +7528 / -2401 / net +5127
public API: +0 types / -0 types
documents/examples audited: all 28 selected InterpretItem leaves, the owning Behavior traversal, ApplicationCapabilities fields, ChildBindings, LocalTimers, FactQueue, exact observation registration, terminal reporting, root/child retirement, and all public application launch forms
commit or working-tree reference: preserved dirty tree; 198 changed paths including 60 untracked; complete docs +13900/-1858 and other +904/-146 at the pre-resolution checkpoint
newly unlocked IDs: ARC-011 and ARC-013
remaining risk or N/A rationale: ARC-011 owns launch/task consolidation and its independently failing cancellation regressions; ARC-020 owns final public-surface minimization. This retained static product adds no public type and no dynamic capability lookup
```

### ARC-010 — Interpret the complete selected Behavior Actors capability set

**Status:** distilled on the published Behavior Core/Actors 0.20.0 contract.
The 0.19.0 fixed-supervisor diagnostic ingress and live shutdown-during-source
gaps are prior-representation failures. The selected 0.20.0 contract supplies
typed diagnostic ingress, an exact preparation-start receipt, and a later
typed preparation result. Bombay interprets the split source action and its
later event, and the live pool and supervisor probes pass.
**Priority:** P1. **Owner:** Bombay typed effect-lane interpreters.
**Unblocks:** TEST-025 and ARC-009.

The locked supervisor and pool templates emit `PrepareWorkers`, whose affine
source Bombay now interprets, including the uninhabited `Never` source of a
temporary pool. The other six atomic interpreter compile contracts also exist.
Bombay now returns the exact accepted or rejected `ShutdownEstablished`
resolution to the owning pool, proven by a real FIFO job and unbounded drain.
The selected 0.20.0 fixed supervisor now runs through Bombay with typed
diagnostic custody. The public supervision binary performs live replacement
and shutdown; the public FIFO binary executes a job and orderly drain. The
independent FIFO runtime tests cover capacity-one rejection, permanent
recovery, both interruption choices, and shutdown folded before an affine
replacement source returns. Multi-role supervisor tests cover ordered
preparation, later-role rejection, and exact terminal custody.

- [x] Extend TEST-008's locked template/capability manifest to enumerate every
  Behavior Actors request emitted by the public templates and whether Bombay
  interprets it.
- [x] Design the smallest statically typed application capability for worker
  preparation. It must consume/return the exact upstream source action and
  produce `WorkerSubmission`; no callback may run inside a Behavior fold.
- [x] Implement the upstream `InterpretItem` contract directly. Do not add a
  Bombay worker request, supervisor, pool, registry, or recovery state machine.
- [x] Test accepted, per-worker rejected, source rejected, unattempted,
  retirement, shutdown-during-preparation, ordering, and exact source custody.
- [x] Convert supervision and worker-pool examples to actual end-to-end runtime
  policies as specified by TEST-025.

```text
Resolution record (2026-10-01)
queue state: feature-complete
upstream owner and locked revision: Behavior Core/Actors 0.20.0 at 804b2bf25325a523884ec49d8a4ae6d2d2b6e9da; Macros 0.13.0; the sole root patch is Timers 13e884da7ab41781f52337b0038060e375b00ee0
prior-representation failure command and result: the isolated 0.19.0 fixed-supervisor App probe failed E0277 at absent typed ProxyDiagnostic ingress; the held-source FIFO App probe failed its required pre-release ShuttingDown rejection with Elapsed(())
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --test fifo_pool_recovery (5/5); fixed_supervisor_runtime (1/1); fixed_supervisor_recovery (2/2); the source-panic terminal assertion passed after the full suite
focused release command and result: the same three test targets with --release passed 5/5, 1/1, and 2/2; the source-panic terminal assertion passed after the full suite
additional Loom/Miri/fuzz/compile/benchmark evidence: selected-contract migration failed compilation at the old synchronous interpreter before the fix; the Driver law evidence script passed all eight positive, boundary, and production mutation laws at the selected revision; cargo test --locked --workspace, cargo build --locked --workspace, strict all-target Clippy, rustfmt check, and git diff --check passed
production: +5383 / -4655 / net +728 across the complete physical crate src/ worktree against HEAD, including embedded tests; the focused owner-adoption patch itself was +91 / -74 / net +17 before final test-only adjustments
tests: +7528 / -2401 / net +5127 across complete standalone Rust, shell, and example worktree against HEAD
public API: +0 types / -0 types in this 0.20 adoption stage
documents/examples audited: current README, public examples, Driver law and strategy, capability and template manifests, PRD evidence, and upstream owner probes; the supervision binary runs failure/replacement/shutdown and the FIFO binary runs job completion and drain
commit or working-tree reference: preserved dirty worktree; 198 changed paths including 60 untracked at this checkpoint; docs +13681/-1858 and other files +904/-146
newly unlocked IDs: TEST-025 and ARC-009
remaining risk or N/A rationale: ARC-020 owns final global minimization; no new public Bombay type or duplicate policy was added; all 21 Nix flake checks passed after generated build outputs were cleaned
```

Local progress (2026-10-01)

- The selected 0.19.0 `PoolRecovery::temporary` source is `Never`; Bombay now
  implements its vacuous preparation port. The prior executable FIFO fixture
  failed E0277 for that exact missing bound.
- Bombay now returns both accepted and rejected
  `EstablishedShutdownResolved` facts to the owning pool while retaining the
  exact `ItemSettlement`. Before the correction, a real FIFO job completed,
  but `WaitForActorGraph` timed out after the worker stop was observed.
- The public `worker-pool` binary and test now execute one correlated search
  and orderly drain. The independent FIFO regression holds worker activation,
  proves accepted backlog admission, releases activation, observes the exact
  result, and retains stopped root and worker terminals. A zero-capacity
  inversion fails its accepted-outcome assertion. Focused debug/release,
  complete locked workspace tests, strict Clippy, and formatting pass.
- The local preparation tests now cover an exact unattempted source returned
  during shutdown and an accepted preparation returned after shutdown began.
  A rejected source also returns its exact action to the pool, which emits
  one diagnostic without starting another worker or preparation.
  Both pass in debug and optimized builds. In isolated inversions, actually
  calling the unattempted source fails the no-call assertion, and returning
  the prepared worker before shutdown fails the terminal-stop assertion.
  Multi-role ordering, later-role rejection, and in-flight runtime retirement
  remain open, so the combined preparation checklist stays unchecked.
- A locked `App` integration regression now proves one permanent FIFO recovery
  end to end: exact source invocation, two correlated completed jobs, and
  orderly root/two-worker retirement. Focused debug/release and strict Clippy
  pass. Keyed-pool policy and supervisor failure traces remain open.
- A capacity-one FIFO runtime regression now returns the exact second search
  payload and `BacklogFull` rejection while activation holds the sole worker.
  The admitted search subsequently completes and the worker drains. A
  capacity-two inversion fails the rejection assertion in an isolated copy.
  Keyed-pool policy and supervisor failure traces remain open.
- Both selected FIFO interruption policies now run through `App` after an
  assigned worker stops. `Retry` completes the original job on one replacement;
  `Fail` returns the exact assigned payload and worker-stop reason before that
  replacement completes another job. Both pass in debug and optimized builds,
  and swapping their policy input fails the expected customer outcome. The
  TEST-025 pool subcheck is satisfied; its supervisor subcheck remains blocked.
- A locked, live shutdown-during-preparation probe now gives an independent
  fold witness: after shutdown is admitted while the source waits, a later
  submitted job must receive its exact `ShuttingDown` rejection before source
  release. Both the locked build and the isolated owner candidate time out at
  that reply, proving the Driver has not folded shutdown while preparation is
  outstanding. The first version also returned two worker terminals after
  release. The refined failing extension is preserved in
  `docs/research-probes/fifo-shutdown-during-preparation.patch`, with exact
  reproduction and ownership analysis beside it. The ordinary test source
  was restored. This leaves the combined preparation checklist open and adds
  a second selected owner-contract blocker alongside proxy diagnostic ingress.
- An isolated owner candidate at local branch
  `codex/proxy-diagnostic-parent-ingress` (`e55c2cb`) passes its full
  workspace gates and a Bombay fixed-supervisor shutdown probe in debug and
  release. It is unpushed. The selected immutable Actors release still lacks
  that parent diagnostic ingress, so ARC-010 and TEST-025 remain blocked.
- A second isolated supervisor `App` probe now exercises coordinated
  two-role recovery through Bombay's source interpreter. It proves ordered
  Primary/Secondary preparation, two replacement activations and completed
  terminal custody on success, plus later-role rejection with no
  replacement activation and typed owner-cancelled proxy custody under the
  selected terminal diagnostic policy. Both traces pass in debug and release;
  strict Clippy and two policy inversions pass. The runnable fixture is
  `docs/research-probes/fixed-supervisor-coordinated.rs`. It still requires
  the unsubmitted owner diagnostic-ingress candidate and therefore does not
  close the selected-release checklist.
- The coordinated rejection probe now also inspects the exact retained
  terminal diagnostic: Primary triggered recovery and was prepared, Secondary
  alone was rejected with `WorkerUnavailable`, and no role remained. A direct
  root-supervisor launch failed E0277 for missing external shutdown ingress,
  so its `StopOnShutdown` wrapper remains a required capability. Debug,
  release, and strict Clippy pass; an owner diagnostic-reason inversion fails
  the intended assertion. This still runs only with the unsubmitted owner
  candidate.
- A separate isolated owner worktree now has test-first FIFO and fixed-supervisor
  callers for a consuming preparation start and late typed return. Both caller
  tests fail against the untouched selected 0.19.0 source in debug and release
  at the missing start API and event. The partial source experiment separates
  issued and started expectations and retains the affine source for a late
  return; it has not passed its full owner compile gate or Bombay's live timing
  witness. Its current production delta is `+664/-176/net +488` across eleven
  changed paths with two proposed public types. The owner checkpoint for
  expanded surface is pending. This changes no queue state.

Blocker and stage record (2026-09-28)

- The selected Behavior Actors 0.17.0 exposes
  `AssignWorker::into_parts` but restricts its exact reconstruction method to
  `pub(in crate::atomic)`. After an actual rejected Communication send,
  Bombay cannot return the original `AssignWorker` and payload in the required
  typed `ItemSettlement::Rejected`. `ProxyOperation::into_parts` has the same
  private reconstruction problem for rejected proxy input. BEH3 records the
  owner change needed in a selected immutable upstream release.
- `PrepareWorkers` now has the Bombay `WorkerPreparationSource` operation and
  direct `InterpretItem` implementation. The prior compile probe failed with
  E0277 for that exact missing capability. Four focused cases pass in debug
  and release; the schema-3 manifest gate, complete locked workspace tests,
  strict Bombay Clippy, formatting, and diff whitespace checks pass.
- Remaining ARC-010 work includes executable supervisor and pool policies, later
  role and shutdown-during-preparation evidence, executable supervisor/pool
  policies, and complete terminal custody. This row is blocked rather than
  feature-complete. Stage-local delta: production `+124/-0/net +124`;
  tests `+357/-0/net +357`; public API `+2/-0` types. Nine task paths were
  touched, with the user's 79-path inherited worktree preserved.

### ARC-011 — Consolidate launch and application orchestration

**Status:** distilled; terminal repository audit confirmed minimal remaining surface.
**Priority:** P1. **Owner:** Bombay local composition.

`App`, `Application`, and the single-implementation `LaunchSystem` repeat
`run`/`run_with`/`run_axum` routing. `launch_with` and `launch_axum` duplicate
root space discovery, allocation, interpreter construction, actor spawn,
application-handle creation, and terminal projection. `spawn_root_with` and
`spawn_owned_with_mode` duplicate activation/termination pairs, cancellation,
environment preparation, retirement, Tokio spawning, and startup waiting.
`ProjectedTask::project` spawns an extra Tokio task solely to await an existing
actor task and map its terminal value.

The ARC-006 audit corrected the ignored startup-waiter witness to assert that
Address reservation is invisible during initialization and to attempt a fresh
reservation after the startup waiter is dropped. It fails under
`nix develop -c cargo test --locked -p bombay-rs --lib local::tests::dropped_root_activation_waiter_releases_its_reservation -- --exact --ignored`:
the reserved actor has no surviving cleanup owner. The independent ignored
finish-waiter witness retains its join-owner failure. These are ARC-011 task
custody regressions, not ARC-006 publication failures.

The startup and finish waiters now own a cancellation authority until join.
Both formerly ignored regressions and all 15 local tests pass in debug and
optimized builds. The live ledger records the prior-representation failures,
first-stage delta, and complete working-tree checkpoint.
The actor task also settles its own activation tasks before returning a
residual: a new dropped-finish regression failed in both profiles before that
change and now passes. A separate panic regression preserves unwind at the
joining owner. All 17 local and five launch tests pass in debug and release.

- [x] Keep `Application` and advanced `App` only if caller syntax proves two
  distinct semantic construction policies; share one private launch transaction.
- [x] Factor one concrete spawn transaction parameterized only by genuine root
  versus owned-actor differences; do not introduce a new public launcher trait.
- [x] Evaluate storing the original actor task plus typed projection data and
  projecting when joined. Retain the one mapping task: eliminating it would
  propagate owner/role generics through the entire child-binding product or
  erase the statically checked origin projection. The live ledger records
  the concrete type comparison and cancellation/panic ownership.
- [x] Unify `OwnedTask`/`ProjectedTask` finish and retire paths around exact
  ownership transfer, then test root, child, Axum, cancellation, panic, and
  descendants as complete traces.

### ARC-012 — Replace coordinated optional authority with owned phases

**Status:** distilled; terminal repository audit confirmed minimal remaining surface.
**Priority:** P1. **Owner:** activation and termination transactions.

`ActivationPublisher = Arc<Mutex<Option<Publisher<_>>>>` coordinates one affine
publication authority between activation and retirement. `LocalEnvironment`
first stores an `ignore_publication` callback and later replaces it with
`publish_with`. `TerminationSelection` and `LocalResidual::Retired` similarly
encode terminal/owner-cancellation provenance through shared or optional side
state. `Publication::{Pending, Published}` already demonstrates the clearer
sum-type direction.

- [x] Enumerate every lawful activation/termination phase and which value owns
  the publisher, cancellation authority, and terminal report in that phase.
- [x] Replace mandatory no-op callbacks and `Arc<Mutex<Option<_>>>` custody with
  consuming typestate or a closed owned sum; no hidden callback should be
  required to construct a valid environment.
- [x] Represent owner cancellation as a named terminal/residual alternative,
  not an `Option` whose meaning depends on `Completion::Exhausted` elsewhere.
- [x] Prove publication and terminal selection are exactly once across normal
  stop, pre-publication end, activation failure, panic, abort, and owner cancel.

ARC-012's phase inventory and proof are in the live ledger. The root
publication sender is consumed on publication or dropped on any earlier exit;
private children retain their separate affine binding acknowledgement. The
interpreter owns terminal selection until its one retirement handoff. Public
normal/early/failure traces, terminal-custody tests, and focused panic, abort,
and owner-cancellation selection tests pass in debug and optimized builds.
Deliberately misclassifying owner cancellation or discarding the selected
terminal report fails the corresponding public trace.

### ARC-013 — Remove synchronization that only compensates for ownership splits

**Status:** distilled; terminal repository audit confirmed minimal remaining surface.
**Priority:** P2. **Owner:** actor-local timers and observations.

`LocalTimers` and `FactQueue` use `Arc<Mutex<_>>` so the Environment and action
interpreter can hold separate views, even though their own docs say the Driver
serializes access. Shared ownership and mutexes are valid when real concurrent
custody exists; here they may be compensating for a composition boundary.
`FactQueue::next` linearly scans a vector and `swap_remove`s the first ready
entry without a documented order for simultaneously ready facts.

- [x] Draw the actual access schedule and prove whether any operation can run
  concurrently. If not, give one owner direct mutable access and pass bounded
  borrows/commands across the commit boundary.
- [x] Define the fact-delivery order—registration, readiness, or another domain
  law—before selecting a queue/map/heap. Test simultaneous readiness and
  cancellation without predicting implementation indices.
- [x] Retain `Arc`/`Mutex` only with a named concurrent consumer and Loom or
  causal evidence; do not optimize merely to remove locks.

Resolution (2026-10-01): the Driver serializes local acquisition and effect
interpretation; Observe slots own cross-task wake synchronization. The
application capability product now owns both queues directly. A newly polled
and cancelled waiter regression first failed under `swap_remove` with ready
facts 1, 3, 2 and now proves registration order 1, 2, 3. Focused debug and
release suites, full locked debug workspace tests, strict Clippy, rustfmt,
and diff checks pass. The complete delta and selected-contract proof are in
`docs/open-design-ledger.md`.

### ARC-014 — Unify Entity task ownership and make spawn failure explicit

**Status:** distilled; terminal repository audit confirmed minimal remaining surface. **Priority:** P1.
**Owner:** Bombay task hierarchy.

Entity runtime maintains `EntityTaskGroup` for active/idle/shutdown state while
the Bombay adapter separately tracks `JoinHandle`s in `EntityTaskOwner`.
`LocalEntityRuntime::spawn` promises that every task is driven to completion,
but `BombayEntityRuntime::spawn` silently returns when its weak task owner can no
longer be upgraded. The type-level contract therefore permits the exact stranded
lifecycle tasks its documentation forbids.

- [x] Assign one owner for registration, cancellation, join, idle epochs, and
  family shutdown; delete parallel counts/handle collections.
- [x] Make scheduling after shutdown unrepresentable or return a typed rejection
  carrying the task/required fact. Never silently drop a required future.
- [x] Test spawn-versus-shutdown, task panic/cancel, activation task completion,
  drain task completion, and exact family terminal ordering under Loom where
  practical.

Native Entity scheduling now returns an owned Tokio task handle directly to
`EntityTaskGroup`. The admission gate excludes new public activation and
passivation work once shutdown begins; internal lifecycle tasks remain in the
group until the family is drained. The former weak-owner silent return is gone.
Shutdown owns one affine claim, preserves handles and the original drain count
across cancellation, and returns exact panic/cancel join failures. Causal
spawn/shutdown, activation, drain, and terminal-order regressions pass in debug
and optimized builds. The production Entity Loom claim and slot-order tests
pass optimized; broader production Loom interleavings belong to TEST-009.

Resolution record (2026-09-28)
```text
queue state: feature-complete
upstream owner and locked revision: Behavior Core/Actors 0.17.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602 own actor policy; Bombay owns the Entity task group and native Tokio composition
prior-representation failure command and result: pinned-Nix debug and release focused runs of shutdown_claims_passivation_before_pending_delivery_settles failed with Begun; family_shutdown_preserves_retirement_task_failure failed with a panic; family_shutdown_has_one_result_owner panicked on repeat; cancelled_shutdown_returns_task_custody_to_the_family returned AlreadyClaimed; cancelled_join_preserves_the_original_family_drain_count returned represented 0
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --test entity_family; 9 passed. nix develop -c cargo test --locked -p bombay-rs --test entity_runtime; 11 passed
focused release command and result: the same two commands with --release; 9 and 11 passed
additional Loom/Miri/fuzz/compile/benchmark evidence: RUSTFLAGS='--cfg bombay_entity_loom' through pinned Nix, cargo test --locked --release -p bombay-rs --test entity_loom_local; 2 passed. Miri, fuzz, and benchmark are N/A to the task-group ownership law. Full locked workspace test, build, strict all-target Clippy, rustfmt check, and git diff --check pass
production: +299 / -110 / net +189 since the pre-ARC-014 checkpoint
tests:      +427 / -27 / net +400 since the pre-ARC-014 checkpoint
public API: +0 named types / -0 named types; EntityShutdown changed from a struct to a generic closed result and gained variants; LocalEntityRuntime gained Task, TaskFailure, and join
documents/examples audited: runtime-capability-interfaces.md, module-boundaries.md, open-design-ledger.md, Entity example, Entity application tests, public Entity paths
commit or working-tree reference: uncommitted ARC-014 changes in the preserved working tree
newly unlocked IDs: TEST-009, selected next
remaining risk or N/A rationale: final public-surface minimization belongs to ARC-015; production Loom interleaving expansion belongs to TEST-009. Advanced LocalEntityRuntime hosts must obey the documented cancellation-safe join and infallible scheduling contract
```

### ARC-015 — Shrink Entity's public mechanism surface

**Status:** distilled; terminal repository audit confirmed minimal remaining surface.
**Priority:** P1. **Owner:** public `entity` facade.

The public module exports directory internals, reducer types, slot phase structs,
events/effects, topology machinery, executor evidence, and runtime ports in
addition to the ordinary `EntityDefinition`/`Entities`/`EntityRef`/capacity API.
Most have no demonstrated external production consumer. Names such as
`DirectoryOutput`, `DispatchOutput`, and `LifecycleOutput` also obscure custody
stages, and broad `private_bounds`/`dead_code` allowances hide transitional
composition debt.

- [x] Inventory every exported Entity item with at least one real caller and
  classify it as ordinary API, explicitly supported advanced port, or internal
  mechanism.
- [x] Make lifecycle representation, topology validation, reducer/executor
  types, and installation proofs private unless a caller-visible use proves
  otherwise. Seal advanced extension traits by default.
- [x] After ownership simplification, rename remaining values by domain custody
  and stage; do not perform isolated cosmetic churn over duplicate machinery.
- [x] Remove module-wide/broad lint allowances and retain only narrow,
  documented exceptions. Add compile fixtures for the deliberately supported
  public surface.

```text
Resolution record (2026-09-28)
queue state: feature-complete; ARC-020 owns final project-wide minimization
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602; Behavior Actors owns actor templates, Bombay Entity owns stable-key lifecycle and the advanced directory/test-host kernel
prior-representation failure command and result: with only the downstream compile-fail fixture added, nix develop -c cargo test --locked -p bombay-rs --test entity_public_surface lifecycle_representation_is_private_to_the_entity_owner -- --exact exited 101 because all six private mechanism imports still compiled; the fixture now fails at those imports and the test passes. ARC-003/004 production transition, output-order, and stale-fact inversions continue to own the semantic law
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --test entity_public_surface — 2/2 passed; focused directory, allocations, stress, runtime, and advanced-kernel tests passed
focused release command and result: nix develop -c cargo test --locked --release -p bombay-rs --test entity_public_surface and the optimized directory/allocations/stress/runtime group passed
additional Loom/Miri/fuzz/compile/benchmark evidence: optimized real-directory Entity Loom passed 6/6; both Entity benchmarks built; full locked workspace tests and build, strict all-target workspace Clippy, rustfmt, and whitespace passed. Miri/fuzz are unchanged and retain separate queue owners
production: complete tree source-file delta +3195 / -3745 / net -550 against HEAD (includes preexisting user and earlier stages); ARC-015's source edits remove six public names and broad lint allowances, and rename two installed products/one field without adding policy
tests:      complete tree test/bench/example/tool-file delta +4255 / -2324 / net +1931 against HEAD; ARC-015 adds a 2-test downstream compile boundary and positive/negative fixtures
public API: +2 renamed type names / -8 old names, net -6 externally nameable types; one public field renamed; no new type cardinality
documents/examples audited: complete Entity export inventory and caller classification in open-design-ledger.md; module boundaries and user-facing API now name the installed decision custody; both Entity benchmark call sites compile; no ordinary example behavior changed
commit or working-tree reference: preserved dirty working tree, 136 changed or untracked paths at this checkpoint (source/test/other totals include untracked files)
newly unlocked IDs: TEST-013 and TEST-014 still require ARC-011; ARC-016 still requires ARC-011 and ARC-018. TEST-018 is the next lowest-sequence ready P2 row
remaining risk or N/A rationale: `LocalDirectory` and the pure `EntitySlot` fold remain deliberate advanced integration/test-host APIs with concrete runtime, oracle, Loom, and benchmark consumers; ARC-020 must audit their final minimality after remaining changes
```

### ARC-016 — Remove syntax-spelling inference from macros

**Status:** distilled; terminal repository audit confirmed minimal remaining surface.
**Priority:** P1. **Owner:** `bombay-macros`.

`ActorSpaces` recognizes fields by the last path segment string `ActorSpace`
and detects duplicate protocols by token-string equality. Type aliases and
syntactically different equivalent paths can therefore evade or change the
derive. `TerminalProjection` similarly parses final segment names
`RootOrigin`/`ChildOrigin`/`ActorRetirement` and special-cases `ChildHead`/`ChildTail`
spellings. `#[actor]` is the accepted Behavior-delegating facade, but it injects
broad lint allowances into user methods.

- [x] Prefer deleting transitional `ActorSpaces`/`Hosts` machinery when
  application topology internalization makes it unnecessary. Current `App`
  and Entity paths still require one static multi-protocol host product, so
  retain the derive and its exact `Hosts<P>` proofs.
- [x] Where a derive remains necessary, generate trait/type obligations and let
  Rust resolve identity; do not claim semantic type checking from token
  spelling. Add alias, qualification, renamed-crate, generic, and collision
  fixtures.
- [x] Audit each lint injected by `#[actor]`. The three signature-only Clippy
  exceptions are required by the Behavior fold signature; an authored-body
  warning remains visible under strict Clippy.
- [x] Preserve the accepted actor facade's exact delegation to the locked
  Behavior macro; macros may derive syntax, never own another transition law.

Resolution (2026-10-01): the old derive failed an aliased-host pass fixture
with missing `Hosts<First>` and rejected an aliased root-origin declaration.
Explicit `#[actor_space(P)]` fields and child-source markers now generate
concrete Rust obligations; duplicate/mismatched hosts and wrong child roles
or retired actors fail at the compiler. The renamed downstream, generic,
alias, qualification, and collision fixtures pass. Removing each `#[actor]`
signature lint allowance experimentally failed strict Clippy on legitimate
Behavior folds; the allowances remain scoped to `receive` and a scratch
Clippy inversion proved body warnings still fire. Focused debug and optimized
suites, full locked workspace tests, workspace build, strict Clippy,
formatting, and diff checks pass. Complete surface delta and owner evidence
are in `docs/open-design-ledger.md`.

### ARC-017 — Minimize and prove every unsafe boundary

**Status:** distilled; terminal repository audit confirmed minimal remaining surface.
**Priority:** P1. **Owner:** Entity future and private Observe implementation.

`DispatchWait::poll` uses `Pin::get_unchecked_mut` and `Pin::new_unchecked` even
though its nested `AffineObservation` explicitly implements `Unpin` and the
future has no self-reference. The standard `Pin` API therefore appears able to
express this projection safely. Observe contains justified low-level unsafe
state and manual `Send` implementations with substantial tests; those require a
formal inventory, not blanket deletion.

- [x] Replace the Entity projection with `get_mut`/`Pin::new` or another safe
  projection and prove cancellation behavior unchanged.
- [x] Inventory every Observe unsafe impl/function/block with its invariant,
  protected fields, synchronization order, and safe callers. Keep unsafe in the
  smallest private modules.
- [x] Run bounded Miri and Loom jobs covering publication, registration,
  cancellation, pooling, panic/drop, and reuse; retain exact seeds/results.
- [x] If `ObservationSpace` ever becomes public outside the trusted internal
  address-key use, revisit its non-collision-hardened hasher and document or
  remove the non-adversarial-key assumption. It remains private to Bombay's
  production crate and public only in the unpublished Observe test package.
- [x] Make the two raw-waker borrowed-`Arc` safety comments describe the
  actual reconstruct/forget ownership step instead of a nonexistent
  `ManuallyDrop` borrow; retain the already selected Miri proof.

```text
Resolution record (2026-09-28)
queue state: feature-complete
upstream owner and locked revision: Behavior Core/Actors 0.17.0 and Macros 0.12.0, 435560ce7bea8ad3330ee2d42e5034f837a80602; Bombay-private Observe owns its cell and affine authority
prior-representation failure command and result: temporarily removing DispatchWait's new Unpin impl while retaining safe projection, then nix develop -c cargo check --locked -p bombay-rs, failed with E0277 at Pin::get_mut (exit 101); impl restored. The prior unchecked projection had no behavior failure; this compile inversion proves the safe model supports generic !Unpin IDs
focused debug command and result: nix develop -c cargo test --locked -p bombay-rs --test entity_runtime canceling_admission_does_not_cancel_shared_activation_or_deliver_command -- --exact — passed
focused release command and result: nix develop -c cargo test --locked -p bombay-rs --release --test entity_runtime canceling_admission_does_not_cancel_shared_activation_or_deliver_command -- --exact — passed
additional Loom/Miri/fuzz/compile/benchmark evidence: Miri 0.1.0 nightly 2026-06-15, MIRIFLAGS=-Zmiri-many-seeds=0..4, four targeted Observe tests passed all seeds; optimized Observe Loom with RUSTFLAGS='--cfg loom' LOOM_MAX_PREEMPTIONS=3 passed 28/28. One sleep-based Miri panic-waker test failed at seed 2 and is tracked under TEST-011, not accepted as evidence. Full locked workspace tests, strict Clippy, rustfmt check, and diff check passed
production: +10 / -6 / net +4
tests:      +0 / -0 / net 0
public API: +0 types / -0 types
documents/examples audited: Observe slot/unsafe inventory in open-design-ledger.md, Entity runtime cancellation, Observe test corpus, Nix Miri/Loom lanes, and module-boundary guidance; no public contract changed
commit or working-tree reference: uncommitted ARC-017 safe projection and inventory in preserved working tree
newly unlocked IDs: none; ARC-019 still waits for TEST-018
remaining risk or N/A rationale: sleep-based Observe probe belongs to TEST-011; project-wide distillation remains terminal audit
```

### ARC-018 — Make actor origin provenance unrepresentable as an invalid state

**Status:** distilled; terminal repository audit confirmed minimal remaining surface. **Priority:** P2.
**Owner:** terminal projection.

`ActorOrigin` stores root-versus-child provenance in `Option<u64>` even though
the alternatives have different semantic meaning. Its crate-private `root` and
`child` constructors are available for any `Role`, and conversion methods copy
the optional nonce unchanged, so type parameters do not fully prevent a root
role with a child nonce or vice versa.

- [x] Model root and child occurrence as a closed sum or distinct
  role-indexed representations, with a child nonce required only for a child.
- [x] Keep the application role and Behavior structural position statically
  checked through the existing `ChildRole` law.
- [x] Add compile/runtime inversions proving invalid role/provenance pairs cannot
  be constructed or projected, then update macro fixtures.

Resolution (2026-10-01): the prior unit regression projected a child with
nonce 13 as a root and failed because the root retained `Some(13)`. Separate
`RootOrigin` and `ChildOrigin` products now make that conversion unavailable;
two compile-fail fixtures reject both directions and the existing wrong-role
fixture retains the `ChildRole` structural proof. Focused debug and release
terminal suites, full locked debug workspace tests, six public examples,
workspace build, strict Clippy, formatting, and diff checks pass. The complete
surface checkpoint is in `docs/open-design-ledger.md`.

### ARC-019 — Give Observe one physical code owner

**Status:** distilled; the cfg harness and private-module allowance passed terminal audit.
**Priority:** P2. **Owner:** private Observe implementation.

`observe-tests` retains a conditional `#[path]` import of Bombay's private
source for Loom and normal fuzz/performance dependencies. Bombay alone owns
ordinary Observe tests. The former duplicate-module allowance is gone;
Bombay's private-module `dead_code` allowance remains explicit after the
final caller audit measured 26 diagnostics for its separately verified keyed API.

- [x] Select one physical compilation owner: either a private non-publishable
  workspace crate used by Bombay and the harnesses, or a cfg/harness structure
  that does not recompile ordinary tests. Do not expose Observe as supported
  public Bombay API merely to simplify the build.
- [x] Keep Loom, model, fuzz, drop-count, and performance techniques distinct;
  remove only identical compilation/execution.
- [x] Verify dependency direction, test listing, docs, Miri/Loom cfgs, and
  coverage accounting after the move.

```text
Resolution record (2026-09-28)
queue state: feature-complete; ARC-020 owns final minimization
upstream owner and locked revision: Bombay-private Observe, with Behavior Core/Actors 0.17.0 and Macros 0.12.0 at 435560ce7bea8ad3330ee2d42e5034f837a80602
prior-representation failure command and result: TEST-018's pinned-Nix listings showed the same 121 ordinary Observe cases in both crates before cfg isolation; the selected harness removes all 121 from observe-tests' ordinary lib-test build while preserving Bombay's 121
focused debug command and result: pinned-Nix cargo test --locked -p observe-tests --lib passed zero ordinary tests; Bombay's 121 remained, and all 171 active Bombay lib tests passed under coverage instrumentation
focused release command and result: RUSTFLAGS='--cfg loom' LOOM_MAX_PREEMPTIONS=3 pinned-Nix cargo test --locked -p observe-tests --lib --release passed all 28 Loom cases
additional Loom/Miri/fuzz/compile/benchmark evidence: pinned-Nix cargo check for observe-perf and all observe-fuzz bins passed; cargo tree confirms observe-perf depends on observe-tests while bombay-rs does not; the Miri shell targets Bombay's ordinary Observe tests and the Loom flake check targets the isolated cfg. Pinned-Nix cargo llvm-cov --locked -p bombay-rs -p observe-tests --lib --summary-only passed after moving a stale generated trybuild/bombay-machine manifest out of target; Bombay ran 171 active tests plus four ignored, observe-tests ran zero, and the report contained one observe/mod.rs coverage row. This focused coverage scope is not a new whole-workspace floor. Full locked workspace tests, strict Clippy, rustfmt, and whitespace passed under TEST-018
production: +0 / -0 / net 0 beyond TEST-018
tests:      +0 / -0 / net 0 beyond TEST-018
public API: +0 types / -0 types
documents/examples audited: module-boundaries.md now names Bombay as the ordinary owner and the isolated Loom/fuzz/performance roles; no example contract changed
commit or working-tree reference: preserved dirty tree, 137 changed or untracked file paths; cumulative source-file delta +3198 / -3750 / net -552 against HEAD includes the TEST-018 harness change
newly unlocked IDs: TEST-011 is ready; TEST-013 and TEST-017 still await their remaining listed dependencies
remaining risk or N/A rationale: the allowed cfg structure still compiles one source under different Loom and normal dependency configurations, but never duplicates ordinary tests. The private Observe dead_code allowance and final API minimization remain under ARC-020
```

### ARC-020 — Complete a caller-facing Rust API audit

**Status:** distilled; terminal API audit confirmed minimal remaining surface. **Priority:** P2.
**Owner:** each remaining public crate.

The API audit must follow deletion, not rationalize current surface. Public
types need deliberate `Debug`/comparison/conversion/iterator/error behavior,
examples, `must_use`, and naming according to the official Rust API Guidelines,
with explicit exceptions for affine or sensitive capabilities. The selected
Behavior instructions also prohibit vague architectural names such as
`Output`, `Result`, `Problem`, `fact`, and `Incarnation`, while current Bombay
documents and code use some of those terms; that instruction/document conflict
must be resolved centrally rather than by piecemeal renaming.

- [x] For each retained public type/function, record its semantic owner, caller,
  construction invariant, standard traits, error/custody behavior, and example.
- [x] Remove public aliases, wrappers, and traits that only rename storage,
  forward calls, or hide structural paths. Keep justified newtypes such as
  `EntityId`, whose construction separates stable domain identity from actor
  addresses.
- [x] Reconcile prohibited vocabulary with current project documents and the
  locked Behavior revision, then rename only after ownership is settled.
- [x] Run rustdoc with warnings denied, compile every current public example,
  and add explicit tests for intentional omissions such as non-`Clone` affine
  authority.
- [x] Test whether the receive-timeout example can omit `stop_on_shutdown`.
  It cannot: the exact locked `Application::run_with` contract requires the
  root event to accept `ShutdownRequested`, and the unwrapped root fails E0277.
  The wrapper is retained as a static composition requirement; the separate
  shutdown run exercises its transition policy.
- [x] Rename the two current actor-space test fixtures called `LocalActors` so
  their names identify the owned protocol spaces, and rerun their focused
  positive/negative compile evidence.
- [x] Correct the current capability guide's Macros 0.13.0 source revision
  attribution against the exact lock.
- [x] Reconcile the current Driver strategy's Macros revision and the planning
  guide's 13/19 capability claim with the selected 0.20 template inventory.
- [x] Reconcile current Driver law, template evidence matrix, completion
  navigation, and EXEC external-work note with the selected 0.20 contract.

Terminal evidence addendum (2026-10-02): those four guides now distinguish
the selected Core/Actors and Macros revisions, live versus still unverified
template policies, and historical EXEC research hashes. The selected
manifest, owner archives, and Bombay live tests falsified the old missing
interpreter claims; the current guidance no longer makes them.
`git diff --check` passed. Documentation only: production/tests
`+0/-0/net 0`, public types `+0/-0`.

Terminal guide addendum (2026-10-02): the Driver strategy names both exact
owner commits. The planning guide reports 19/19 typed interpreters for the
selected 0.20 contract and marks its six-gap rows as historical requirements;
its evidence page scopes the former `todo.md` disposition to the 2026-09-29
snapshot. The pinned-Nix template inventory test passed, all selected archive
source/evidence paths exist, and `git diff --check` passed. Production and
tests `+0/-0/net 0`; public types `+0/-0`.

Terminal audit addendum (2026-10-02): the guide now names distinct locked
Core/Actors and Macros source revisions. The original shared-revision claim
fails against the Macros archive; `git diff --check` passes after the
documentation-only fix. Production and tests `+0/-0/net 0`, public types
`+0/-0`.

Resolution (2026-10-01): [the public API audit](public-api-audit.md) records
every Bombay-owned reachable export and operation, its owner/caller,
construction invariant, standard traits, exact custody, and compiled example
or fixture. No unowned public alias or wrapper remained. Two exact private
synonyms, `LocalAddresses` and `OccurrenceBindings`, were deleted. Private
architectural identifiers `Incarnation`/`IncarnationOutcome`, `FactQueue` and
its family, `InstalledSlotFacts`, and `OutputTarget` were renamed for their
owned actor execution, termination observation, Entity evidence, and installed
effect source. Historical research retains old names only as dated snapshots.
The direct compiler probe established 26 keyed Observe dead-code diagnostics
if Bombay drops its module-scoped allowance, so the same physical Observe
source and a precise allowance remain for its independently verified keyed
API. The original downstream handle `Debug` test failed with six E0277
diagnostics, plus one for `InstalledActor`; redacted implementations now pass.
The original `#![deny(unused_must_use)]` fixture compiled while discarding a
composed policy and exact terminal; it now fails for both intended warnings.
Focused debug/release handle, actor execution, observation, host, child,
Entity, and must-use tests passed. The actor execution receipt at
`/nix/store/7ni5h8dcj2jzyqq29w7r6casmfw482sp-bombay-workspace-0.1.0`
records seven killed real-source mutations and two denied affine ownership
inversions at Behavior 0.20.0. Locked workspace tests/build, strict all-target
Clippy, warning-denied rustdoc, rustfmt, and diff check passed. This row is
`feature-complete`; final project-wide distillation follows TEST-020.

## Current retention hypotheses (not terminal evidence)

These are scope guards against deleting known-good ownership boundaries while
working nearby. They do not put any queue row in `retained`; the selected item
must still supply its required differential or inversion evidence.

- `bombay-engine` is a cohesive universal Driver boundary. Its
  `Environment`/`ActiveEnvironment` typestate expresses a real affine protocol,
  and the executor-neutral trait is a genuine port rather than speculative
  polymorphism. Fix TEST-001/002 and ARC-006 without moving actor-template or
  local runtime policy into Engine.
- `ActionInterpreter` directly interprets the locked Behavior `Actions` and is
  the correct semantic bridge to Engine. Decompose its capabilities as needed,
  but do not replace it with a Bombay effect enum.
- `#[bombay::actor]` is the already selected macro-last facade and delegates to
  the owning Behavior macro. Keep it unless DX42 evidence is superseded.
- `ActorExt` only returns existing Behavior Actors wrapper types. Its forwarding
  is acceptable caller-discovery policy under the current design record; keep
  it synchronized with upstream and attach no local semantics.
- `IngressMode::{Standard, Entity}` is a closed representation choice with two
  actual mailbox layouts; it is not the semantic-boolean problem found in
  shutdown authority.
- `EntityId<T>` is a justified newtype because it separates stable logical
  domain identity from actor addresses and prevents otherwise swappable values.
- Observe's unit, independent-model, exhaustive, Loom, fuzz, panic/drop, and
  performance techniques test different properties. Unsafe code is not itself
  a defect; retain each operation whose invariant and verification survive
  ARC-017.
- `mutants-gate`, `observe-perf`, and the fuzz packages are cohesive tooling
  boundaries. Repair their correctness/ownership gaps without moving their
  policies into production crates.

## Initial crate-by-crate architecture matrix (2026-09-22)

This is the opening assessment, preserved to show why each row entered the
queue. The canonical queue, resolution records, and terminal audit checkpoint
supersede its earlier defect descriptions.

| Package/surface | Opening assessment | Required work or retained boundary |
| --- | --- | --- |
| `bombay-engine` | Cohesive Driver and affine Environment port; one settlement-precedence defect and one activation-order integration defect | Retain boundary; TEST-001/002/003/004/006/008/023, ARC-006 |
| former `bombay-machine` | Removed by ARC-004; Entity owns the surviving concrete slot law | TEST-021 absence audit |
| `bombay-macros` | Accepted actor facade is Behavior-owned; topology/terminal derives infer types from syntax strings | TEST-012/015/016, ARC-016 |
| `bombay-rs` local runtime | Static Behavior interpretation is sound in direction; shutdown erasure, activation ordering, shared optional authority, and launch duplication weaken ownership | ARC-001/006/009/011/012/013/018 |
| `bombay-rs` child/application composition | Correctly starts from Behavior child products, but parallel binding stores/traversals and one capability aggregate duplicate ownership | ARC-008/009/010 |
| `bombay-rs` Entity | Rich integration, but transition truth is duplicated, task ownership is split, public mechanics are excessive, and upstream template overlap is unresolved | ARC-003/004/007/014/015; TEST-009/010/011 |
| private Observe module | Strong low-level verification and deliberate affine API; physical dual compilation and unsafe inventory remain | TEST-011/017/018/020, ARC-017/019 |
| `observe-tests` | Conditional Loom/fuzz/performance boundary with no ordinary test duplication; private Observe `dead_code` allowance remains for final audit | ARC-020 |
| `observe-perf` | Dedicated deterministic performance harness; distinct-key retirement and same-key pooled reuse are described accurately | Keep the separate workload measurements and their TEST-019/020 verification owners |
| `observe-fuzz` | Distinct fuzz techniques but on-demand and without a retained CI cadence | TEST-020 |
| `bombay-engine-fuzz` | Typed malformed/long operation grammar, retained seeds, and bounded CI campaign verified by TEST-006 | TEST-020 broader campaign audit |
| `mutants-gate` | Complete baseline, candidate identity, outcome, and stale-entry checks verified by TEST-024 | Retain separate tooling boundary; TEST-020 full sweep |
| actor/counter/application/Axum examples | Generally teach direct Behavior/Behavior Actors composition; some tests assert required async operations inline | TEST-013/014/017 |
| Entity example | Demonstrates a real Bombay-only stable-identity use, but currently depends on the unresolved custom Entity architecture and wide public mechanics | ARC-007/015 and complete terminal assertions |
| supervision/worker-pool examples | Supervision now executes one replacement and exact retirement under selected Actors 0.20.0; FIFO executes admission, recovery, interruption, and shutdown during held preparation. Keyed-pool and final adverse-policy coverage remain open. | ARC-010, TEST-025 |

## Architecture evidence index

Use these as starting points, then inspect the complete owning module and its
current diff rather than editing from a single quoted line:

| Item | Bombay evidence | Locked upstream evidence |
| --- | --- | --- |
| ARC-001/006 | `crates/bombay/src/local.rs` | Behavior `Actions`/settlement contracts and `crates/bombay-engine/src/environment.rs` |
| ARC-002 | `crates/bombay/src/actor_interface.rs` | Address claim/lease API selected by `Cargo.lock` |
| ARC-003/007 | `crates/bombay/src/entity/lifecycle/`, `directory.rs`, `runtime.rs` | Behavior Actors `atomic/stable_proxy`, `atomic/dynamic_supervisor`, `discovery/registry.rs`, `discovery/resolver.rs`, and `machine.rs` |
| ARC-004 | Removed package; `entity/directory.rs` and `entity/lifecycle/transition.rs` | Behavior transition contract and Behavior Actors `Machine` |
| ARC-008 | `crates/bombay/src/child_bindings.rs` and child interpretation impls in `application_runtime.rs` | Behavior `actor/creation.rs` (`ChildOccurrenceShape`, `ChildOccurrences`) |
| ARC-009/010 | `crates/bombay/src/application_runtime.rs`, `interpret.rs`, `reports.rs` | Behavior effect interpretation and Behavior Actors `atomic/worker/preparation.rs` |
| ARC-011/012 | `crates/bombay/src/application.rs`, `application_runtime.rs`, `launch.rs`, `local.rs` | Engine activation/retirement traits and Behavior settlement custody |
| ARC-013 | `crates/bombay/src/time.rs`, `observation.rs` | locked Timers API and Behavior Actors timer/observation requests |
| ARC-014/015 | `crates/bombay/src/entity/bombay.rs`, `runtime.rs`, `family.rs`, `mod.rs` | upstream owners selected by ARC-007 |
| ARC-016 | `crates/bombay-macros/src/lib.rs`, trybuild fixtures | locked Behavior macros and child-role traits |
| ARC-017/019 | `crates/bombay/src/observe/`, `entity/runtime.rs`, `crates/observe-tests`, `observe-perf`, `observe-fuzz` | standard `Pin`/unsafe contracts linked above |
| ARC-018 | `crates/bombay/src/terminal.rs` and terminal projection derive/tests | Behavior `ChildRole`/structural position contract |
| ARC-020 | crate roots, public re-exports, rustdoc, examples, and compile fixtures | locked Behavior instructions and official Rust API Guidelines |

## Current evidence-separation hypotheses

Do not remove these merely because they exercise the same broad feature. Recheck
the distinction when the owning queue item runs:

- Observe unit, independent model, bounded exhaustive, Loom, fuzz, drop-count,
  and performance tests use different verification techniques. Only the exact
  duplicate ordinary execution in TEST-018 is redundant.
- Entity pure transition, runtime integration, and production Loom tests should
  remain distinct once TEST-009 and TEST-010 make their oracles real.
- Entity's retained production Loom tests prove concurrent output order after
  the generic Machine package removal.
- Driver allocation, causal trace, settlement order, and terminal custody tests
  have distinct observable laws.
- The two application-child compile fixtures intentionally cover different
  feature-unification diagnostics, subject to TEST-016's explicit matrix.

## Initial suite audit matrix (2026-09-22)

The resolution records and queue states above supersede this initial inventory.

| Surface | Assessment | Resume item |
| --- | --- | --- |
| Engine law/runtime tests | Strong custody/order fixtures mixed with a real precedence gap, discarded dispositions, redundant projections, and no commit-before-claim visibility regression | TEST-001, 007, 023 |
| Engine law manifest/inversions | Claims are self-referential or disconnected from production | TEST-003, 004, 008 |
| Engine property/fuzz | Same narrow accepted-byte state space; major failure/terminal branches absent | TEST-006, 020 |
| Engine benchmark | Stale terminal-custody assertion breaks flake check | TEST-002 |
| former Machine unit/Loom | Package removed; Entity-owned production Loom tests retain the relevant ordering law | TEST-021 |
| Bombay incarnation/local runtime | Real lifecycle tests exist, but fake mutation evidence, assertion-owned awaits, and an ignored pseudo-benchmark remain | TEST-005, 013, 019 |
| Entity lifecycle/directory/runtime | Integration coverage is substantial; independent 137,561-trace oracle needs edge audit and some readiness is sleep-based | TEST-010, 011 |
| Entity Loom | Six production directory interleavings; disconnected model deleted | TEST-009 verified |
| Observe | Broad and generally strong techniques; exact duplicate ordinary execution, sleep-based readiness, and ignored docs remain | TEST-011, 017, 018 |
| Actor facade/templates/macros | Useful compile coverage, but incomplete Actions comparisons, no renamed downstream crate, and no runtime source-action evidence | TEST-012, 015, 016, 025 |
| Examples/current docs | Several executable examples are sound, but supervisor/pool stop at initialization, assertion-owned awaits remain, and many `rust,ignore` snippets are unchecked | TEST-013, 017, 022, 025 |
| Mutation/coverage/Miri/fuzz gates | Available mostly on demand; claims and thresholds are incomplete, and the mutation verdict parser can fail open | TEST-020, 024 |

## Verification commands and current results

All commands below must run through pinned Nix.

Current audit results:

- `nix develop -c cargo fmt --all -- --check` — passed.
- `nix develop -c cargo clippy --locked --workspace --all-targets -- -D warnings`
  — passed.
- `nix develop -c cargo test --locked --workspace` — passed.
- `nix develop -c cargo nextest run --locked --workspace` — 507 passed,
  2 skipped.
- `nix develop -c cargo test --locked -p bombay-rs --all-features --no-fail-fast`
  — passed; 167 library tests passed and 1 was ignored, all integration and
  trybuild tests passed, and 5 rustdocs were ignored.
- `nix develop -c cargo test --locked -p bombay-rs --no-default-features --no-fail-fast`
  — passed; it selected the ordinary
  `application_child_must_be_behavior.rs` diagnostic fixture. The all-features
  run selected the `_feature_unified` fixture, confirming both variants work
  when invoked explicitly.
- `nix develop -c cargo llvm-cov --locked --workspace --summary-only` — passed;
  88.28% regions, 88.24% functions, 86.81% lines.
- Warning-free workspace documentation — passed.
- `nix flake check path:.` — failed at the stale Driver benchmark in TEST-002.
- `nix develop -c cargo test --locked -p bombay-engine --bench driver --release`
  — failed at the same stale terminal-custody assertion.

Minimum final gate after focused repair:

```text
nix develop -c cargo fmt --all -- --check
nix develop -c cargo clippy --locked --workspace --all-targets -- -D warnings
nix develop -c cargo test --locked --workspace
nix develop -c cargo test --locked -p bombay-rs --no-default-features
nix develop -c cargo test --locked -p bombay-rs --all-features
nix develop -c cargo nextest run --locked --workspace
nix flake check path:.
```

Add focused debug and release commands for the selected law. The three ARC-006
visibility regressions now run ordinarily. Two ignored Local cancellation
tests remain under ARC-011; the startup-waiter test now probes the invisible
reservation directly and fails for its task-ownership law under `--ignored`.

## Final terminal audit result (2026-10-02)

All 45 canonical rows are terminal: 25 `TEST-*` rows are `verified`, 19
`ARC-*` rows are `distilled`, and ARC-009 is `retained` by its direct
composition evidence. No checkbox remains open. A reciprocal scan of the 67
dependency edges found no missing ID, cycle, or unsatisfied prerequisite. The
source/export/caller/example/fixture/benchmark/fuzz/document inventories and
each architecture row's minimal retained owner are recorded in the
[terminal ledger](open-design-ledger.md#final-audit-closure-2026-10-02) and
[public API audit](public-api-audit.md).

The merged-tree `nix flake check path:. --max-jobs 1 --cores 2` passed all 21
`aarch64-darwin` checks again after the TEST-014 nonce-fixture correction.
The corrected PR commit passed CodeQL with no nonce alerts, Rust analysis,
and cargo-deny. The later `00ab6e8` PR run passed its Linux flake, both bounded
fuzz campaigns, and both artifact uploads after the pinned C++ runtime path
was supplied to the fuzz shell; its CodeQL, Rust analysis, and cargo-deny
checks passed too. `nix develop -c cargo test --locked --workspace`
also passed after merging `main`. The independent source-equivalent
`nix build path:.#packages.aarch64-darwin.mutants --print-out-paths --no-link
-L` passed its fail-closed baseline after 870 candidates: 342 caught, 524
unviable, four classified equivalent misses, and zero timeouts. Its complete
outcomes and per-candidate logs are preserved under ignored
`target/mutation-evidence/mutants.out/`. Earlier final-source gates also
passed four-seed Observe Miri, bounded ASan fuzzing of Driver and four Observe
targets, the Observe performance harness, and the Nix performance package.
The `main` merge and later packaging correction changed no runtime, Engine,
or macro source after those gates.

The release check is separate from this audit queue. The published
`bombay-engine` 0.2.0 and `bombay-macros` 0.1.0 contain older code, while
`bombay-rs` 0.1.1 is not yet published. The selected Timers git revision has
a typed scheduling rejection absent from the published Timers 0.1.0 archive.
The manifest now gives the local macro dependency an explicit version, and
the publish workflow waits for a merged release PR. The exact registry and
version dependencies are recorded in the
[publication ledger](open-design-ledger.md#bombay-publication-manifest-blocker-2026-10-02).

## Terminal audit

Run this only after TEST-020 is `verified`. Failure of any step reopens the
smallest owning item and resumes the iteration loop; it does not create an
untracked cleanup task.

1. **Preliminary queue closure:** every `TEST-*` row is `verified`, `retained`,
   or `removed`; every `ARC-*` row is `feature-complete`, `distilled`,
   `retained`, or `removed`; no row is `ready`, `active`, or `blocked`; no
   detailed checkbox remains open unless marked `N/A` with evidence; no
   dependency edge is missing, cyclic, or inconsistent with
   `docs/open-design-ledger.md`.
2. **Behavior ownership:** re-resolve `Cargo.lock` and patches, reread the exact
   selected Behavior instructions if the revision moved, and rerun the template
   capability inventory. Prove there is no Bombay-owned duplicate actor trait,
   effect language, child product, lifecycle/supervision law, dynamic capability
   map, or source-action request where the locked upstream owns the contract.
3. **Repository audit:** inspect every production module, public re-export,
   test, compile fixture, example, benchmark, fuzz target, current document, and
   manifest against the final ownership map. Search for every deleted name and
   obsolete spelling. Re-run the crate-by-crate and suite matrices in this file.
4. **Full verification:** run the minimum final gate below plus TEST-020's
   recorded mutation, coverage, bounded fuzz, Miri, Loom, sanitizer, rustdoc,
   benchmark, no-default-feature, and all-feature obligations through pinned
   Nix. Record exact commands, tool versions, seeds/corpora, results, and any
   explicitly inapplicable check.
5. **Final minimization:** inspect the complete tracked and untracked diff.
   Remove types, traits, wrappers, aliases, branches, lint allowances, tests,
   documents, and dependencies that lost their owner. Record final production,
   test, documentation, and public-API deltas without calling a net-positive
   change a reduction.
6. **Distillation:** revisit every `feature-complete` architecture row against
   the complete repository. Promote it to `distilled` only if its remaining
   surface is minimal; otherwise reopen it as `ready` with a new exact
   sub-checklist under the same ID.
7. **Final queue closure and goal completion:** prove that every `TEST-*` row is
   terminal and every `ARC-*` row is now `distilled`, `retained`, or `removed`,
   with no `feature-complete` row left. Update current and historical documents,
   set the cursor below to `complete`, and only then mark the persistent goal
   complete. Report the final gates, deltas, retained risks, and recovery status
   of any removed material.

## Loop cursor

Update this block at the end of every iteration. It is a cache of the canonical
queue, not a substitute for rescanning that queue.

```text
goal: complete
queue schema: 1
active item: none
next item by selector: none
last terminal item: TEST-020 verified
last architecture resolution: ARC-020 distilled
external blockers: none for the canonical queue; crate publication has the separate Timers and coordinated version dependencies above
locked Behavior Core/Actors revision: 804b2bf25325a523884ec49d8a4ae6d2d2b6e9da; Macros revision: 3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8
worktree checkpoint: complete source delta against origin/main, including tracked and untracked files, is recorded in the final change ledger below
last updated: 2026-10-02
```

## Change ledger for this audit

```text
baseline:   origin/main after reconciling the release merge; 269 changed paths, 0 untracked, including 16 binary fuzz seeds
production: +8855 / -7060 / net +1795 (runtime, Engine, macros, and removed Machine source; embedded unit tests included)
tests:      +8583 / -3195 / net +5388 (tests, examples, benches, fuzz, and tools outside production source)
public API: +7 types / -41 types (reachable named owners; see docs/public-api-audit.md)
docs:       +19092 / -1959 / net +17133 (reconciled architecture, ownership, verification, release, and recovery records)
other:      +938 / -230 / net +708 (manifests, lock, Nix, CI, baselines, and configuration)
removed:    obsolete bombay-machine source/package removed; .research/ remains recoverable at the Trash path recorded above
```
