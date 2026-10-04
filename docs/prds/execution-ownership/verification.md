# EXEC verification evidence

Status: staged EXEC evidence. DG-SHUTDOWN is independently accepted; the
remaining decision gates are open. No full EXEC acceptance, Bombay delivery PR
or passing delivery CI is claimed. The historical baseline is
`2fccedf6eb636ac22143e7e01de7e784f96e2b4e`, with its original lock and manifest
hashes in PRD section 16. Later selected contracts and retained evidence are
recorded below and in the PRD.

## Fresh preservation commands (2026-10-02)

Every Rust command below executed through the pinned Nix shell. These results
preserve completed local work; they neither reproduce the remaining defects
nor accept their proposed representations.

| Command | Result |
| --- | --- |
| `nix develop -c cargo test --locked -p bombay-rs --features axum --test run_with --test axum --test application_terminal_custody -- --skip compile_checked` | 20 passed; two compile checks deliberately filtered for this runtime baseline. |
| `nix develop -c cargo test --locked -p bombay-rs --test actor_interface --test entity_application --test terminal_projection` | Seven passed, including external-authority and terminal-shape compile harnesses. |
| `nix develop -c cargo test --locked -p bombay-rs --lib dropped_ -- --nocapture` | Five passed. |
| `nix develop -c cargo test --locked --release -p bombay-rs --lib dropped_ -- --nocapture` | Five passed. |
| `nix develop -c cargo test --locked -p bombay-rs --lib launch::tests::projected_child_retirement_preserves_origin_state_and_descendants -- --exact` | One passed. |
| `nix develop -c cargo test --locked -p bombay-rs --lib observation::tests` | Five passed. |
| `nix develop -c cargo test --locked -p bombay-rs --release --lib observation::tests` | Five passed. |
| `nix develop -c cargo test --locked -p bombay-rs --lib exact_shutdown_classifies_repeated_and_stopped_requests_without_resending` | One passed. |
| `nix develop -c cargo test --locked -p bombay-rs --release --lib exact_shutdown_classifies_repeated_and_stopped_requests_without_resending` | One passed. |

The two initially filtered compile harnesses were subsequently executed:

| Command | Result |
| --- | --- |
| `nix develop -c cargo test --locked -p bombay-rs --test run_with run_with_protocol_is_compile_checked` | One harness passed: shutdown-authority compile pass and five static denials. |
| `nix develop -c cargo test --locked -p bombay-rs --features axum --test axum axum_protocol_inversion_is_compile_checked` | One harness passed: wrong root protocol statically denied. |
| `nix develop -c cargo test --locked -p bombay-rs --test actor_ref_authoring` | One harness passed: three authoring static denials. |

These focused results do not establish full workspace or EXEC acceptance.

The baseline at `46f7ee8` subsequently passed
`nix develop -c cargo test --locked --workspace` (exit 0), including workspace
unit, integration, compile-contract and documentation tests. Production and
the selected lock were unchanged. This is preservation evidence; it does not
cover the isolated defect witnesses, repaired candidates, feature-specific
optimized verification, Loom configuration, benchmarks or remaining EXEC gates.

At the unchanged production baseline `8d06f67`, both
`nix develop -c cargo fmt --all -- --check` and
`nix develop -c cargo clippy --workspace --all-targets -- -D warnings`
passed (exit 0). These checks will be required again for the retained changes.
Historical inversion receipts for completed ARC fixes remain historical.
[Compiled observation race failures](observation.md#compiled-original-race-failures-2026-10-02)
now establish two original-defect failures in debug and optimized builds; no
repaired representation or gate acceptance is claimed.

The direct ready-source witness also passed in debug and optimized builds:

```sh
nix develop -c cargo test --locked -p bombay-rs --lib source_admission_places_the_exact_input_on_the_actor_control_lane
nix develop -c cargo test --locked --release -p bombay-rs --lib source_admission_places_the_exact_input_on_the_actor_control_lane
```

Each executed one test. Early exact-filter attempts named the wrong module and
executed zero tests; those are excluded from evidence. The verified symbol is
`application_runtime::atomic_interpretation_contract::source_admission_places_the_exact_input_on_the_actor_control_lane`.

## Baseline inspected runtime source anchors

| Path | SHA-256 |
| --- | --- |
| `Cargo.lock` | `df9acbe4e947538ce4e8ec979243210c665a4dd7710bc018d28c269af2ada81e` |
| `application_runtime.rs` | `0b8e2c1c475ab46c50c451082b8d67dadb0a138e1d3ee8ccca9129ee7b7f57c4` |
| `local.rs` | `ae5ddbbece5e36a37d1587e8cf7df23f9077344b04c17d8d2a7bc82b77a5557f` |
| `launch.rs` | `de139eff7b65504bd2ee9d6d629b53984e9e92c7f505d7a2695adc3b951258dc` |
| `observation.rs` | `e1dd70410bf4aecccf6531c330167fc7dfda12f0bd0e881e108a1afc4d4465ff` |
| `child_bindings.rs` | `92368ea79112b752d722c5e9565a401599b40bd9fd115fd268d4cd3d7e01aaa5` |
| `entity/bombay.rs` | `0c67e5b9fee571595a90bd4d1ce88dae37a6d5131d518d8bae0480f49c86492f` |
| `worker_preparation.rs` | `13bacbcc1ece47ad8407e30a98bef48aa24fa26590893ca9fe17c1fd5f5e16cf` |

Paths in this table are relative to `crates/bombay/src/` except `Cargo.lock`.

## Research custody and next gates

Research agents inspected task/projection, observation/shutdown and repository
consumers without repository-write authority. Their reports do not sign gates.
Remaining original-defect witnesses, ordinary-Rust prototypes, all XO/EV mappings,
independent review, full repository checks, benchmark, minimization and delivery
remain mandatory. User-requested comparative research is underway for source
cancellation, caller disappearance, capability failure and observation identity.
The user selected exact relationship cancellation authority and capability-failure conservation; representation gates remain open. Later selected policies and the user’s authorization to choose subsequent
recommendations are recorded in the owning decision records.

## Independent research review

Reviewer `/root/contract_inventory` inspected the original observation and
capability-custody failures, then the exact-authority feasibility and ordinary
product custody comparison against their recorded source/patch hashes and
profile logs. The reviewer confirms those bounded evidence claims and their
limitations; it approves no gate, retained API or integrated implementation.
It excluded its own application-signature comparison from independent review.
The review corrected the second capability-task spawn-site description to
worker preparation and clarified two new opaque types versus two replacement
protocol types. All decision gates remain open.

## Independent Communication failure evidence

[EXEC section 19](../execution-ownership.md#19-communication-admission-prerequisite-2026-10-02)
records the frozen actual ActorRef defect, complete trace and independently
reproduced one-test failures in both profiles. Shared-target zero-test runs
are excluded. This verifies the original prerequisite defect. The current released correction
and registry selection are recorded in EXEC section 20; full gates remain open.

[Single-owner retirement-cause review](task-custody.md#single-owner-retirement-cause-review-2026-10-02)
records conditional independent review of the 18-path isolated candidate,
final source-only profile/inversion evidence and its standalone generic inference
cost. The final unchanged source freeze also passes full workspace/all-feature
tests in both profiles. Capability/receiver/Communication integration,
capability-failure production and full DG-TASK remain open.
That source freeze contains new test names using prohibited architectural
vocabulary (`AcquisitionBoundary`, `SourceFact` and related identifiers).
Its logs remain historical evidence; naming correction, minimization, a new
source freeze and independent review are required before retention.

## Complete change measurement

The canonical tree now selects Rust 1.99.0, published Communication 0.1.3,
Behavior Core/Actors 0.21.2 and Macros 0.13.1. EXEC section 46 records fresh
published-source verification and the pending selected-build checks. Section 32
retains its original independently reviewed compiler and allocation evidence.
Actor ownership repairs remain separate, unretained candidates.
The separate fuzz graph selects the same Core and actual local Engine version. Baseline has no inherited tracked or untracked delta.
The complete canonical Bombay delta follows; owning repository delivery has
its separate complete ten-path record in the shutdown decision record.

## Fresh canonical nextest verification (2026-10-03)

At clean canonical 56683b34d6c14b99591986fa503c7cb66076b890, the required
ordinary workspace nextest run passes all 421 binary tests, with zero skipped:

```text
nix --option eval-cache false develop -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/bombay-exec-canonical-nextest-target cargo nextest run --locked --workspace -j 2
```

The target is fresh. All 345 tracked inputs remain byte-identical through the
run; the working tree has no tracked or untracked changes. Input manifest
1a067a8ee2e9c02421f22de82f400625be493731eeb5220b4d6493003a7013f4
and receipt 9593376725ae619cd75caa8564b1d38cd7de8217500a8ab7789fe648ec30a519
bind the exact command, unchanged sources, terminal exit zero and complete log
at /tmp/bombay-exec-canonical-nextest.log. This supplements the earlier Cargo
workspace/doctest verification; nextest does not run doctests or the hook-only
measurement witnesses. It does not verify unretained semantic comparisons,
final EXEC acceptance or remote CI.

## Full native Nix checks (2026-10-03)

Clean canonical bd4bcce3c2c6a7be80cd84bb73ae95871db0c14a passes all 21
declared aarch64-darwin checks, with terminal exit zero:

```text
nix --option eval-cache false flake check --max-jobs 1 --cores 2 --print-build-logs
```

All 345 tracked inputs remain unchanged through completion. Receipt
e0c0c1c5efd9abfb37af4522c4e1a6553dfb3255dd80f45b4d2b97cd78b72360
at /tmp/bombay-exec-canonical-flake-verification.json binds the input manifest,
complete log, all 21 output paths and their registered Nix metadata. Cargo
1.99.0 (5f94df478) is observed in the build log. The earlier no-build evaluation
is recorded separately and is not counted as verification execution.

The checks cover release workspace build/tests, documentation and doctests,
strict lint/formatting, both child feature configurations, Entity/Observe Loom,
Driver/actor law evidence, unwind/abort boundaries, coverage and executable
example checks. Axum's deterministic package tests run; its long-running server
is not started by that gate. Owner coverage reports 128/136 Driver lines,
547/563 actor-execution lines, 46/51 actor-outcome lines and 502/525 Observe
lines, exceeding their configured floors. These are the tool's reported line
counts, not a new claim of complete production-branch coverage.

The retained canonical tree passes these repository checks. Unretained semantic
repairs, the normal-family lifetime correction, full EXEC decision gates,
other platforms and required remote PR review/CI remain outstanding. Passing
this run does not authorize retaining an unaccepted interface or merging EXEC.

Current isolated native consumer c39d5606 passes full workspace/all-target/
all-feature strict and formatting. Subsequent full debug workspace execution
in native-workspace-regression-9ogp6ocv (logbd307415) stops in Engine's existing
law_manifest guard: its unconditional `dyn Any` ban contradicts EXEC section74's
approved intrinsic Rust panic-payload exception. Engine compile1, allocation3,
Driver29 and property2 pass; law_manifest passes8/fails1. Later workspace suites
are unexecuted. Correct the narrow guard with a forbidden-erasure inversion;
neither quality receipt is full CI, runtime acceptance or Bombay delivery.
All345 source hashes remain exact; canonical selection is unchanged.

The narrow guard successor subsequently passes both-profile controls, strict
and formatting. Full native debug reruns preserve two later nonpasses:
logb05d9661 at the stale repeated pure-panic oracle, then logbeec3b76 at the
Entity integration test's stale Completed-only retirement oracle after all259
Bombay unit tests pass. The replay control passes in both profiles. Selected
source-bound corrections and their independent reviews are in EXEC section86;
complete current workspace, family test and strict results remain pending.
The subsequent family focused-stagea1029c6c passes its complete one-test
controller in both profiles with all345 inputs exact; full workspace and
combined strict/final format remain pending at that checkpoint.
These isolated outcomes do not replace canonical CI or full EXEC acceptance.

The complete native census0ceb2bfd then ran both profiles with no-fail-fast:
each returned101 with exactly three failed targets and451 Cargo-reported passes.
The source-bound reconciliation in EXEC sections87–89 corrects only existing
test oracles and expected errors; original failures remain recorded. Successor
ea6ef627 runs public-surface2, terminal-projection3, two-family1 and run_with15
controls in each profile, then complete workspace tests in each profile,
all-target/all-feature strict lint and formatting, all exit0. The broad logs
report454 passes/61 Cargo summaries per profile, zero failures/ignored; nested
trybuild summaries are not additional semantic-law counts. All345 inputs remain
exact,340 unchanged from the census foundation. Authenticationee12ac43 binds
the logs and prior nonauthor source reviews. Canonical dependencies/source are
unchanged; this is isolated native verification, not remote CI or EXEC completion.

Actual completed receiving comparison1638aca4 has four original runtime
failures, four ordinary controls, four allocation-substitution failures and four
restores across both profiles. Each selector executes exactly one test after
the actual root/family owners join. Authentication894dd36f binds all345 restored
inputs,344 unchanged, and the original0vs1 oracles. The Python mutation-selector
stop occurred before any family source mutation or Cargo invocation; it is
orchestration evidence, not a semantic failure. Strict covers only the library
with all features; cfg tests still need owning lint execution. Formatting passes.
No public receiving repair or live-family/cleanup failure gate follows.

Final grouped quality403f21fc passes actual owning --lib --tests strict and
formatting. Independent88e282a2 authenticates all345 inputs/344 unchanged and
precisely inherits the unchanged runtime bodies: separate original/control,
finite prepared-worker allocation replacement and exact restoration in both
profiles, plus five existing controls per profile. Its four-line fulfilled
length expectation changes no executable body. The initial broad-selector log
was overwritten and remains excluded; retained old strict101 is not relabeled.
This proves the bounded allocation comparison, not live installation or a gate.

Completed receiving cfg-quality0e2d8d9e subsequently runs owning --lib --tests
all-feature strict101 and formatting0 on current ea6's other344 exact inputs.
Three test-only diagnostics remain explicit; earlier library-only strict
does not establish cfg quality. Historical section91 public compilation attempts
remain NONPASS and execute no runtime tests. Later inferred-actor receiving and
exact-reason comparisons in sections92–93 pass their bounded runtime/strict
checks; full task/API and other decision gates remain open.

<!-- exec-research-counts -->

```text
production: +167 / -34 / net 133
tests: +3295 / -557 / net 2738
documentation: +11173 / -97 / net 11076
manifest/lock: +38 / -33 / net 5
public API: +0 types / -0 types
changed tracked paths: 69
untracked paths: 0
```

## Pure Driver test repair (2026-10-03)

Pre-retention record: the existing approved driver_law.rs contains four invalid
fixtures that publish observations from inside Behavior folds or carry runtime
observers in actor state. Replace them with pure state and actual host traces;
keep the 29 existing test names and all eight Driver law families. Expected
change: that one existing test path, tests +621 / -483 / net 138; production
+0 / -0 / net 0; public API +0 types / -0 types. Reuse the actual Driver,
TestEnvironment, complete Actions and typed creation identities. No runtime
contract, initialization-panic payload repair or full EXEC gate is accepted.

Author freeze: source `3e5a2a3527d7ad9b19022a18dd7ec42467b3daeae9dc3aff94e1c7d100c7badf`,
receipt `8754468ef11d6537a5e24bd65b2b4f26bed5d894dbe948b8677cc86a20313e20`,
patch `2481b039140ea14e866e37f127c41fdce9d670462135064d2c26b9a311483322`.
Independent reviewer /root/observation_research read the complete source, patch,
mutations and logs, authenticated 345 sources and 118 artifacts, and verified
that all other 344 baseline files remain unchanged. Signed review
`f532e3cb23c2586234b34714da266e3e9421631df504f0bd4ddf7463d8b1b31c`
accepts bounded test retention only; it includes no reviewer Rust execution.
The author executed all 29 tests in both profiles, strict focused Clippy,
formatting and all eight Driver evidence families successfully. Runtime
inversions fail for the intended law in both profiles. The distinct E0499
comparison proves static exclusion of overlapping mutable borrows, not a
runtime mutation. Creation traces preserve actual issued IDs/kinds and original
box allocations; Box is Clone, so this is original-allocation evidence, not a
compile-time no-Clone proof or proof of actual child establishment.

Integrator read the repaired folds, host observations and typed-creation
comparison, transferred the exact signed source, and independently ran:

```sh
nix develop -c cargo test --locked -p bombay-engine --test driver_law
nix develop -c cargo test --locked -p bombay-engine --release --test driver_law
nix develop -c cargo clippy --locked -p bombay-engine --test driver_law -- -D warnings
nix develop -c cargo fmt --all -- --check
nix develop -c bash crates/bombay-engine/tests/driver-law-evidence.sh --output /tmp/bombay-driver-purity-retained-laws
```

All exit 0 on the retained exact source. Both profiles pass 29 tests; all eight
law families pass positives/boundaries and kill their intended inversions.
Actual execution adds `--option eval-cache false` to Nix and uses a dedicated
Cargo target with incremental compilation disabled. Transfer receipt
`26b774b6525083beab4e9726cbe592d8f11db4a68dd31f3361052770784e3df7` binds the actual commands' logs and final source.
This retains the test repair only. Complete EXEC gates remain open.

## Current module inventory reconciliation (2026-10-03)

Read-only nonauthor source review
`5e3d12baa770f7a77a7b9c117b3ec7971d911af623fc01e59043568f5606c0f5`
binds canonical 458440a and the selected published 0.21.1/0.13.1 release.
Its lexical index `3c5cef1bf246c7414b29f993179bf11d73a96f0a94a3900f5281ef3f9b4204e7`
covers 24 source files, 706 production declarations/methods, 209 impl groups
and 262 product-member rows; generated token templates are tagged separately.
Exports, macros, consumer references and tests have separate inventories.
These counts describe an inspection aid, not a compiler-resolved visibility
graph or accepted destination map. The complete source remains authoritative.

Coordinator authenticated all 43 bound artifacts and confirmed the 24 Rust
files remain byte-identical. Factual corrections to the approved current module
and capability guides are retained: installed-capability exports, absent
interpretation error, current exact-observation map/tasks versus peer/child
queue, and independent registrations rather than mandatory tasks per consumer.
No execution semantics changed. Replacement of the historical module decision
record stays external because that path is outside the approved 114 allowance.
Semantic gates, exact destinations, compiler visibility, differential extraction
and independent gate acceptance remain open. No Rust command, file move or
new public type is claimed by this inventory work.


## Retained static shutdown correction (2026-10-03)

EXEC section 35 and the shutdown decision record contain the signed
DG-SHUTDOWN acceptance. Only the two reviewed source files were transferred,
byte-identical to their accepted hashes. Canonical transfer receipt:
`a8d9e995ece32689220794d62f9e921e4911c70684099641b51388b89df859d3`.
Existing ARC regressions and the retained pure Driver repair remain unchanged.

Canonical focused verification uses the pinned prefix
`nix --option eval-cache false develop -c env CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/bombay-shutdown-root-fresh-target` with:

```sh
cargo test --locked -p bombay-rs --lib shutdown -- --nocapture
cargo test --locked -p bombay-rs --release --lib shutdown -- --nocapture
```

Both exit zero with seven tests (five new, two inherited preservation cases).
The identical source's independent fresh Clippy/formatting and author full
all-feature workspace verification are recorded in the signed shutdown record;
no duplicate canonical full-workspace run is claimed here. Final combined
EXEC verification remains required. Complete baseline-relative counts above
include these source/test changes and all tracked/untracked documentation.
Current test-module starts were rechecked after the production-line change;
classification uses local.rs line 1164 and application_runtime.rs line 3069.

## Fresh independent actor scheduling evidence (2026-10-03)

The reviewed EV-25 stage adds 769 test lines in launch.rs and changes two
configuration lines (+2/-2) in Cargo.toml; production Rust and public types
remain unchanged. Tests are exempt from production condensation. The workspace
Tokio feature enables multi-thread support for this evidence; current public
application runners still select the current-thread scheduler.

Author receipt:
`557366e26d6ff739cec05804c2a1965667198f1def73de7f017dffba9f0fbf66`.
Independent non-author review:
`3fe673a5e4fb4112bd4c00aafc641798b9197383fd9101df4361e8c0e8d88536`.
Coordinator fresh verification receipt:
`6a16748e0c962ad33d5da7ce5c7e4f96affb60111e667b3551a9c5e63f3b227f`.
The fresh worktree starts at c70d0a3 and preserves the accepted shutdown sources.
Only the candidate configuration and launch.rs were copied into that review
worktree. Canonical source remained unchanged until the transfer below.

The coordinator read the full candidate and selected Tokio poll-hook source.
Actual actor task IDs, distinct worker IDs and complete typed retirement traces
prove overlapping runtime work. Exclusive task polling, together with the one
consumed Driver and exclusive mutable Behavior, proves exclusive actor folds.
The overlap includes test-host permission waits; it does not measure simultaneous
CPU instructions. Both author serial-only counterfactuals compile and fail the
same intended overlap assertion (one versus two), after cleanup; the earlier
mutex type error is excluded.

Fresh pinned-Nix commands use
`nix --option eval-cache false develop -c env CARGO_INCREMENTAL=0 RUSTFLAGS='--cfg tokio_unstable' CARGO_TARGET_DIR=/tmp/bombay-actor-overlap-root-target`:

```sh
cargo test --locked -p bombay-rs --lib independent_actor_execution -- --nocapture
cargo test --locked -p bombay-rs --lib --release independent_actor_execution -- --nocapture
cargo clippy --locked -p bombay-rs --lib --tests -- -D warnings
```

Each test command passes two tests; Clippy exits zero. Workspace formatting
also passes through the pinned Nix shell. Logs and exact commands are bound by
the coordinator receipt. This is bounded scheduling evidence, not selection of
a public runner API or acceptance of EV-30. Throughput and before/after task
and allocation measurements, including the actual projection graph, remain
required. Canonical production remains net +133 lines with zero new public types.

### Canonical scheduling transfer

The two reviewed files are now retained at their exact accepted hashes;
transfer receipt
`b5886a88223f89ecd52559543ef2811d717f6001cd4d25bb4c942499e70bb27a`
preserves the complete original launch.rs prefix and accepted shutdown sources.
Both canonical scheduling commands above pass two tests, exit zero, in debug
and optimized builds. Logs are `/tmp/bombay-actor-overlap-canonical-debug.log`
and `/tmp/bombay-actor-overlap-canonical-release.log`. These are the same reviewed
source bytes; fresh source Clippy and formatting above already passed.
The final combined workspace/Nix gates remain required.

## Reviewed execution measurement tests (2026-10-03)

PRD section 37 records the pre-edit scope. The retained addition is 402 net
test lines across two already approved source paths: zero production lines,
new public types or new unsafe operations. The original production prefixes
and all accepted EV-25 bodies are unchanged. Author receipt
e7ecabfec28cf5a7e38056ae5c4a67fe07aa9f7d60fed6a5062d18df87cf12bf,
independent non-author review
7c89c9e31befabafe28261993ec1e8bce45bebaebce8a5763c6b29923d21724c,
and coordinator fresh review
c9305571684e6e70307c54b6bd0d1fafe5dae9e6273bc78265addbc10cd32520
bind the exact sources, selected dependencies, commands and logs.

The coordinator independently passes four scheduling/measurement tests in
each profile, eleven owning execution tests in each profile, the explicitly
ignored release measurement, ordinary-build strict Clippy and workspace
formatting. All Rust commands run through the pinned Nix shell. Hook commands
use this prefix:

```sh
nix --option eval-cache false develop -c env CARGO_INCREMENTAL=0 RUSTFLAGS='--cfg tokio_unstable' CARGO_TARGET_DIR=/tmp/bombay-measurement-root-target
```

Exact Cargo arguments:

```sh
cargo test --locked -p bombay-rs --lib independent_actor_execution -- --nocapture
cargo test --locked -p bombay-rs --lib --release independent_actor_execution -- --nocapture
cargo test --locked -p bombay-rs --lib actor_execution::tests -- --nocapture
cargo test --locked -p bombay-rs --lib --release actor_execution::tests -- --nocapture
cargo test --locked -p bombay-rs --lib --release measure_independent_actor_throughput_and_scoped_allocations -- --ignored --nocapture
```

Ordinary-build Clippy explicitly removes RUSTFLAGS and uses the same target:

```sh
nix --option eval-cache false develop -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/bombay-measurement-root-target cargo clippy --locked -p bombay-rs --lib --tests -- -D warnings
nix --option eval-cache false develop -c cargo fmt --all -- --check
```

The final async gate starts work after all original requests are admitted.
Snapshots after the await remain within one task poll. The selected Barrier
implementation releases its short synchronous lock before awaiting; its owning
tests cover rendezvous and Send. Because cancellation is unsafe for reuse, an
interrupted measurement abandons the fresh gate rather than retrying it.
Disabled, counting and overflow are distinct; unwinding clears ownership.
Original allocation, panic-scope release and overflow inversions each compile
and fail the intended assertion in both profiles before exact restoration.

The coordinator's release sample processes 256 requests on two actor tasks in
3.40175 ms; construction is 40 allocations, joined cleanup zero, seven actor
polls contain 550 allocations, and interpreted work adds zero. These are scoped
counts and one timing sample, not a performance floor or whole-runtime total.
Timing includes typed delivery, shared start and settlement completion.
Off-poll worker allocation and reporting are excluded. Two roots with no
children cannot satisfy the required actual projection-graph comparison.

The earlier native-payload inspection was removed, ordinary-build dead code
was independently reproduced as Clippy exit 101 and corrected, and the missing
shared start was corrected before retention. Their frozen predecessor receipts
aa4d7a97d2c19b1393d427d8d1dbdc5e7aaa094175100f42f35c6f0f2f4b148a
and 57508049d11ad79b33dfdb9e74ef5d9d25f5bf9dba13a01b56b869babf22dd5b
remain rejected evidence. The retained panic test keeps the actual opaque
JoinError and task identity; native payload conservation remains undecided.

Canonical transfer receipt
04d3279ea40d03cd3eb90cdb97ffe4e0bc508a48d165a46792b5aa97accf3e32
binds launch.rs 84364e2378ec8515e4f7076706880b4bae962714223c471c46463b0e165fccca
and actor_execution.rs 380c0c077a47c6a43c61eb57006d5f26b4f7ef9789078846cbf5911f2d1c5e08.
Final combined verification and full EV-30 remain required.

The exact transferred canonical sources pass four tests (one measurement
ignored) in each profile using the two independent_actor_execution commands
above. Logs are /tmp/bombay-measurement-canonical-debug.log and
/tmp/bombay-measurement-canonical-release.log; both exit zero. The same exact
source bytes pass the coordinator's owning execution tests, Clippy and
formatting recorded above. The complete tracked/untracked measurement at this
checkpoint remains in the change record, including documentation and config.

### Canonical workspace verification after measurement retention

At 77da777, all four ordinary-build commands exit zero through pinned Nix:

```sh
nix --option eval-cache false develop -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/bombay-measurement-root-target cargo build --locked --workspace
nix --option eval-cache false develop -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/bombay-measurement-root-target cargo test --locked --workspace
nix --option eval-cache false develop -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/bombay-measurement-root-target cargo clippy --locked --workspace --all-targets -- -D warnings
nix --option eval-cache false develop -c cargo fmt --all -- --check
```

The test log contains 61 successful summaries and 422 passing tests, including
current examples, renamed dependency, macros, compile fixtures and retained
Driver/runtime regressions. Hook-only measurement tests have the separate
verification above. The source manifest and exact command/log hashes are in
/tmp/bombay-measurement-canonical-workspace-verification.json. This verifies the
retained branch (receipt SHA-256
94d640b9f4b292e5cd9c62468d0f6d5a3707e2faad7bb61d439e8e8d43f1f95a),
not unretained prototypes or full EXEC acceptance. Subsequent
semantic implementation and final PR still require their complete gates.
The completed private target cache was removed to free disk space; original
source, frozen receipts and logs were preserved.

### Isolated publication repair checkpoint

Receipt 8e8ae06474c178b036a21baee928eca124f531fff2b01fa7a178011682662aae
indexes all 53 command records, source hashes and logs in
/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-publication-port-execution-2wsmhhm3.
The final source is the historical typed foundation plus the reviewed publication
increment; current canonical preservation and full EXEC acceptance are separate.
Commands run from that stage's workspace through the pinned canonical Nix shell:

```sh
nix develop /Users/joel/Code/devrandom/bombay -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/bombay-api-private-composition-target cargo test --locked --workspace --all-features
nix develop /Users/joel/Code/devrandom/bombay -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/bombay-api-private-composition-target cargo test --locked --workspace --all-features --release
nix develop /Users/joel/Code/devrandom/bombay -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/bombay-api-private-composition-target cargo clippy --locked -p bombay-rs -p bombay-engine --all-targets --all-features -- -D warnings
nix develop /Users/joel/Code/devrandom/bombay -c env -u RUSTFLAGS CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/bombay-api-private-composition-target cargo fmt --all -- --check
```

All four final commands exit zero. Workspace tests pass 432/432 across 61
summaries in each profile; focused Local and Engine custody suites pass 31 and
14 respectively. Exact compile-fixture commands pass in both profiles. Ten
compiled intended mutation failures, followed by restored positives, are bound
by the same receipt. The earlier interrupted broad run is explicitly excluded.
The old impure Driver fixture's execution supplies no semantic acceptance.

Remote research commit 5df5b8ede3b001db4649f524ae7154c6c3b28d0f matches all
345 final source hashes. Its first focused foundation commit is 8399e7f.
`git ls-remote origin refs/heads/research/exec-publication-retirement` confirms
the final commit. Canonical commit 181b88c is also pushed. Both worktrees contain
zero untracked paths at this checkpoint; canonical production remains net133
and public types +0/-0. Research generated corpus and verification artifacts
stay outside the canonical source tree and remain authenticated in the receipt.
Neither remote branch has full EXEC review, CI or merge acceptance.

### Current-source repair verification plan

EXEC section 40 is the pre-edit checkpoint. All commands in
proposed-command-argv.json, SHA-256
6059632777c06e6136db95a70d97db907c6aeb24cf30ea936019cf9644e71b1e,
are unexecuted forecasts. The exact file is in the proposal directory linked by
task-custody.md. It specifies focused debug/release regressions before broadening,
12 foundation and five publication mutation cohorts with both-profile intended
failures and restored positives, static denials, current preservation tests,
ordinary/hook builds, all-feature workspace/strict/fmt, stable acquisition tests,
fresh 10,000-run selected-nightly fuzzing and full Nix gates on actual final source.
The acquisition campaign does not cover publication Break or unbounded fairness.
Neither historical passing totals nor the remote research checkpoint certify
this new combined source. Actual archive, command/log/source hashes, final full
tracked/untracked measurement and independent review remain required.

## Published 0.21.2 selection verification (2026-10-04)

EXEC section 46 selects published Core/Actors 0.21.2 at edc2d466; Macros
0.13.1 remains at 5ca. Source snapshot 289f19d81a90b4bc02c7e6a5d226b66fded01684959c47cfeaa7dac409416841
binds all 345 tracked inputs through twenty successful commands and all 21
native Nix checks. Index ae9359e60a8c7a4b625e01c39df8abc2a29dbc0f92c414791ce2f88b6c392d20
records exact argv, terminal status, complete logs and source hashes under
/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-published-0212-selected-svcvi7k2.

Five focused suites pass in both profiles: manifest 9, shutdown 7, FIFO
recovery 5, child terminal custody 4 and application execution 15. Full
all-feature workspace tests pass 422/422 across 61 summaries per profile;
nextest passes 421 with zero skipped. Build, ordinary strict all-target lint,
formatting and warning-denied documentation pass. Actual `tokio_unstable`
strict lint and four concurrency/allocation tests pass in both profiles; the
ignored benchmark is not run. An earlier command using a nonexistent cfg is
preserved and excluded as hook evidence.

`nix --option eval-cache false flake check --max-jobs 1 --cores 2 --print-build-logs`
exits zero with all 21 aarch64-darwin outputs registered. Receipt
458710363ec177a58005d01e0dc7da793266099cdbeec7981da23e9bff3082c9
and filtered-source binding f61d2f48ff04e6b2759d1ee5b9f14c3e4352f9a300b38cf30a0f24852f7461cf
authenticate all 319 actual Nix input files against that snapshot. Other
platforms and remote CI are not verified by this native run.

All eight Driver laws pass their positives, boundaries and mutations in that
debug Nix run. Optimized receipt 1325497f66ec070b1a3a02175cd1c3b0633581f74d654220ef7a1fe952250856
binds the separate eight-law run, two command-only `--release` additions and
restoration of all 345 inputs. Actual optimized log and overlay establish the
profile; generated receipt command fields remain canonical references. Their
equal artifact bytes alone do not prove both executions. The existing actor
runner also exits zero, but its receipt retains stale hardcoded revision 804b;
it is excluded as fresh selected actor metadata evidence. Section 45's pending
runner correction and full EXEC gates remain required.

Independent selection review 50599b880e2e7a2f80b3a00f52257e20faa82951f4547c029646232d071d74e6
and coordinator authentication 41586baf6fe0824e5f3b6e122041d20d2f5e600822e5fea334b0d651bf1e41d5
accept the bounded selection and twenty completed commands. Independent
optimized-law review f9f3f005b14272fd42671d5662c1a10fee21dc3c2a86b38858a12e3db0395d5d
and Nix supplement 53a3cd5eee1d57fe62cd417bb07241a30e36adeb313faf7258ade5a3447e7662
authenticate those final results with the same qualifications. Subsequent
evidence-only record updates do not alter tested Rust, manifests or locks.
This is preservation and selection evidence, not acceptance of ownership repairs.

## Observation static and pure-law experiment (2026-10-04)

The isolated owning source is the exact 806-file archive of published EDC plus
the section-48 reviewed 13-path proposal and reviewed test corrections. Final
input manifest: 67984d169c06b85dde4468ede5494a8d9eebe388a123c791d334445cffda5783.
Bombay canonical runtime, its working tree and the physical Behavior checkout
remain preserved. All commands execute inside Bombay's pinned Rust 1.99 Nix
shell; the full actual argv, logs, exits, source hashes and profile switches
are retained in the following receipts. These results accept bounded evidence
only; they do not accept DG-OBSERVATION or full EXEC.

The command boundary is `nix --option eval-cache false develop -c env
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
CARGO_TARGET_DIR=/tmp/bombay-api-private-composition-target python3`; the
coordinator alone executes each receipt's subprocess commands. Pure tests
unset RUSTFLAGS; static consumers set RUSTFLAGS=-Dwarnings.

Pure controller: `cargo test --manifest-path <owning>/Cargo.toml --locked
-p bombay-behavior-actors --test established_capabilities`, with and without
`--release`, passes all 18 tests each. Receipt
5326cbc56ffcae3044303a6d2ed5cbdd2710dbb9d564224aabd1542c5ca9489c.
The exact selected test for each mutation is
`exact_monitor_retains_cancel_rejection_and_reacts_once_in_both_arrival_orders`
with `-- --exact`. Four unique source omissions remove Started correlation,
make rejected cancellation terminal, lose terminal phase after completion,
or accept terminal replay. Each compiles and fails its intended runtime oracle
in both profiles (eight exit101); restoring the original file passes the
selector in both profiles (eight exit0). Every restoration authenticates all
806 source inputs, not only the mutated file. Receipt
950281c43941b15c69335e211ec178d69259f1c6282778ff75d832f261eb01f2; independent
review c6bd6866cd37e0d4fe7083adde76008d68ed7cf3472ca8669036c1ccc895c28e.

External static consumer: `cargo check --manifest-path <consumer>/Cargo.toml
--locked --offline --message-format=json`, with and without `--release`.
Each positive precedes its corresponding denial, preserving exact source
bytes. All 18 positives exit0 with no compiler diagnostics under -Dwarnings;
all 18 denials exit101 with only the expected error-code family:

| Constraint | Expected compiler denial |
| --- | --- |
| Stopped report protocol | E0308 |
| Cancellation protocol | E0308 |
| Read-only relationship used as cancellation permission | E0308 |
| Cancellation authority used twice | E0382 |
| Accepted observation request issued twice | E0382 |
| Observation sequence cloned | E0599 |
| Observation sequence transferred twice | E0382 |
| Cancellation reply lane missing | E0277 |
| Inner observation reply lane missing | E0277 |

The last denial has two instances of the same intended missing-lane bound.
An actual initial `cargo check --offline --message-format=json` resolves the
consumer lock from the copied owner lock; all 17 shared registry tuples retain
their versions and checksums. Core and Actors are coherent path dependencies
on the same owning archive; Macro 0.13.1 is that archive's unchanged source.
Actual receipt 83985498c79b2a8ab7d49725c00cc0a2afd40a0f7af7538a4af17c02b2b00614
and nonauthor review c1c28943d6ba3b1de94de4d174c5f417962de14407cc142c97838814f6403c27
authenticate all 36 checks. Evidence originally executed under
/tmp/nix-shell.JcaCRb/bombay-observation-static-actual-6mc69knc; byte-identical
preservation under the observation-static-preserved-thsk75xa artifact retains
those actual paths (receipt 6a0dda2414c88af0e77a22ae7c18cd7eb29ed7d071f784953ad24c108fa8afa8).
No rerun at the preserved path is claimed.

The actual owning change record is 9e34a7017f64823295fa4a5753a9be11d602aa2e9a95b3b963fed57745224cba:
production +622/-187/net435; tests +1180/-307/net873; documentation
+96/-12/net84; 13 existing changed paths; public nominal types +3/-0.
This isolated library measurement is separate from the complete canonical
Bombay working-tree footer above. Full owning checks and consumer audit are
in progress. Runtime/controller integration, wrapper recovery, EV20, fuzz,
minimization and independent full decision-gate review remain open.

The full owning workspace subsequently passes under the same frozen 806 inputs:
`cargo test --manifest-path <owning>/Cargo.toml --workspace --locked` in debug
and with `--release`, followed by `cargo clippy --manifest-path
<owning>/Cargo.toml --workspace --all-targets --locked -- -D warnings` and
`cargo fmt --manifest-path <owning>/Cargo.toml --all -- --check`. All four exit0; each profile passes 958 tests including documentation tests.
Receipt adb78e2ce162fd56f94a50ea66093dd03cd764e8484142b041e279ccd4284caa.
This is Bombay's pinned 1.99 toolchain evidence. The owner's pinned-toolchain
Nix gates, published-doc checks, consumer audit, fuzz and full review are still
required before release or dependent acceptance.

The first native owning-pin run is nonpassing: ten actual flake checks, nine
pass and the documentation gate fails only its nine-import rustdoc rule.
Nextest passes all 865 tests with zero skips; all 93 doctests pass. This is a
documentation-gate failure, not a doctest failure. Receipt
d549a8c5d9f41b0e45f253e7b48d3d8634d8102e2073716f86397bec8b3a3cba
and its original log are preserved separately.

Before edits, bounded corrective assignment is the exact source-bound
nine-import removal in four compile-fail snippets: proposal
9b95bac8d1b77d734ec64a41b18b9675bea62e807f674851ef4af7138c3d99fc,
patch ef8c979fe28bd0a7e236b3f8ca96b893db5af6038b5cc910e5fa0da5c14a887e,
independent eligibility c19813c48bc1fd07e2e8cb99b506e8e7ba473945c68484f81aec7228bc6f58b5.
Only the already approved owning established-protocol path changes; semantic
implementation and public-type delta are zero. Fully qualified external names
replace the imports; the four invalid operations, bounds and E0308/E0382 laws
remain unchanged. Source-comment line delta is -9 net, classified with
production-source lines. No checker exemption or full acceptance is granted.
The external static consumers above are complete library modules with visible
module-scope imports, rather than embedded rustdoc/fixture snippets.

Corrected native owning-pin verification now passes all ten checks, including
the actual documentation import/error-code scripts: receipt
54dea9c78d6eddb95c12c9e1822a8f3931b58b9f777a4d236856878763b32bfe,
806-input manifest fdc4407efabbc4283ba0b1019913b18689c960549fba0dc88c81fb7b12432ac0.
Independent review 59402109995d5eed31a844413f2cbad3331403cda776d299fc8f067e88f7164d
authenticates all ten actual outputs and 397 filtered-store source files;
evaluated versus actually built main derivations are explicitly distinguished.
Focused import-rule and 93-doctest runs in both profiles also pass (e41d75168a6481d2a40e4d52125fcc770ce698dac704d45250523b5a4111abc2).
The original nonpass remains preserved. The candidate is remotely backed up,
without merge or acceptance, on Behavior's research/exec-observation-ownership
at d873151ddb80c7c8f4c0558a0cdac5f8c82c0915; backup receipt
4078ddfbc7540cfb3621dd63b79426fa6c4af8cdf0b565db83b94e3d8dec0985
binds all 806 worktree bytes to the verified candidate.

The changed catalogue fuzz target's first actual build fails E0277 at its
Debug-dependent outcome assertion; bounded receipt
f5d951432c737fc305fe3991748f488bc2b9e383f798c7580fd3236b03734241.
An actual original strict-fuzz run under 1.99 fails the same constraint
(61c000c557fb1872556ed6140f3e2cbfeecf2a8193de064bba1ba7cbf319a9ae);
the fake RuntimeAddr lacks Debug, not an owning Exit contract. This compiler
veto is not a semantic mutant kill. Lexical disposal of its returned fake
recipient is a separately source-identified lint risk, not yet an observed
strict failure.

Before edits, the next bounded test-only assignment is the exact fuzz
correction c6955d3e9cb4f3efa44090113027edd78c9ee120d911ecf1c6a181c05bc618c3
and permanent static-documentation coverage
8602f70a54ca507f0a34ababd87de31409809de959dea87ba29f282ad6f8abf4,
with nonauthor eligibility 256acc8db8af105bbf0d732cc0ba96a07681b546567daa426a59918ac1756179.
Only two already approved paths change; executable implementation and public
types do not grow. Fuzz tests change +2/-3/net-1, preserving full outcome
equality and the lexical returned owner. Rustdoc source changes +99/-2/net97:
retain four observation denials, strengthen two to shared-address protocols,
and add the five missing denials plus nine positive controls. There are ten
negative snippets in the whole file because its unrelated shutdown denial
remains intact. These comments count as production-source lines in the
measurement. Both pinned-toolchain doctests/checkers, strict fuzz, bounded
fuzz, source restoration and independent outcome review remain required.

### Current-source observation prerequisite transfer and bounded fuzz verification

Section50's actual345-file isolated baseline and38 source rows received
nonauthor transfer review 1fced6cbf47124108f68cafe7c951f932fa0267d72d8bcf4e7137d79d903333f.
Root applied only those38 approved sources and the section49 approved actor
runner correction. Receipt d95e7eb8c62d962e417985245e2a9839c00c5c1af9458ef399c280f0fe1b2381
authenticates all345 resulting inputs, exactly39 changed paths, and the exact
6ca901 application-runtime prerequisite. Current manifests, locks and guidance
remain unchanged in that isolated archive. Canonical runtime files remain
preserved. This is bounded experiment transfer, not design-gate acceptance.

The owning observation source epoch 5b0311726b9e95d623717bb688d3adc4de88bb92e443f0f19fcab65302b32e39
passes both107-doctest suites, the import checker, strict changed-fuzzer lint
and workspace formatting; receipt f6dd0adcadbca6f10c5de36a7b73fde1ad5b6e4816ccb91f938621bd55acd50d.
Actual pinned-nightly catalogue fuzz execution rebuilds the instrumented target
and completes10,000 runs, exit0; receipt47ff317769a7827046d4d758815e16ccb42e654c848c781a9262e99c615f1181.
All806 source inputs are unchanged during execution. Nonauthor review
83e5f9431b153a999622c75077cd0da3d1ea7bf401fb55b66dd1a55e66dd7d39
authenticates the actual logs, two reviewed changed sources, unchanged other804
files and fuzz lock. Original E0277 remains preserved; an unobserved
drop_non_drop lint is not claimed. These are bounded five-report alphabet
checks, not exhaustive ten-state sequence verification. Fresh native owning
checks and full downstream observation acceptance remain required.

### Actual actor profiles and first downstream observation compiler boundary

Corrected actor-law runner debug receipt
972b6276339a0e8ac0f4dbe4fb5f31fba3b8e89f9255e5c66cb85220c033b885
and optimized receipt
1dc76a2a7dc9892b3dd94b4f8692649d1131bd3240cdb8b48878f091e0559dfa
retain all15 actual Cargo commands per profile: five positive references
exit0, eight compiled runtime inversions exit101 at their exact named
oracle, and two affine E0382 denials exit101. Optimized assignment
8136244e592f0d9481741e0ebe6dae51f83bd1ac20790c9812573b57bd98b088
adds --release at all three actual Cargo command sites in a temporary
345-input copy; every optimized command/log proves its actual profile.
Nonauthor provenance reviews f54c5cd4feeb42ad2b8a3a98ae330a14b7229d03e8f0892b05bf57c5f92a7599
and df748813e3a634e00fc16167f10e96a81bc375bc7f792f2aae15dd28296a7aa3
authenticate these outcomes without approving the reviewer's authored core.
All345 source-root inputs remain unchanged during each runner; its private
mutation copies are restored by the recorded runner before disposal. No
full execution gate is accepted.

Actual offline Cargo metadata resolution, receipt
47393f2fa58871902a8503a9b2afea574c20f0fc3fc67837ef34a12319a52cea,
selects one local Core0.21.2, Actors0.21.2 and Macro0.13.1 in the
workspace; standalone Engine fuzz selects its actual Core/Macro dependency
pair. Both real locks preserve every other complete package record, including
root Timer13e. Source-path experiments are explicit; no registry release is
asserted. Root source and all806 owner inputs remain bound separately.

First downstream compile/list receipt
e416234b4c2ee5aadaee5b6912fc55fe4e390b9ad9c338aec0746927f1fe8cce
is nonpass, exit101 with eight cfg-test diagnostics. Two assume a nonexistent
ActivationTasks::len, two assume ActorRef-based recipient equality, and four
retain the hidden pin! future borrow beyond drop of its Pin reference.
No runtime witness executed. Keep the complete original diagnostics. Correct
tests using existing task settlement, a real endpoint delivery oracle and
lexical borrow scopes; do not originate new production methods, equality
implementations, generic bounds or weaken custody assertions from these
compiler errors. Dependent downstream acceptance remains blocked.

The final owning native source epoch
d5c25e8d74113427aff6da0098abfb49b27f28c8435aed973cd9cf34e230aaeb
passes all ten actual aarch64-darwin native checks, receipt
a787c2db4725ab88815a2f15e760dd4e8270d9261faa65f5c3bb0a5eac4f2ab7:
865 nextest tests, zero skipped, and107 doctests. The owning native pin is
Rust1.95; Bombay's verification pin remains Rust1.99. This is fresh owner
verification after permanent static documentation and single-target fuzz
formatting, not full downstream observation or EXEC acceptance.

Final formatted-source dedicated catalogue checks, receipt
87fb37a72d23d8462e0ee3cd9b3ef286f5649cf2f4da0c8e37c1766a1729115a,
pass explicit rustfmt, strict changed-fuzzer Clippy and10,000 actual
pinned-nightly fuzz runs. All806 primary source hashes remain d5c25-bound.
This qualifies the earlier workspace-only formatting check without silently
formatting other fuzz targets. Continuous one-monitor sequence coverage
remains a separate required observation-model obligation.

Nonauthor final native/resolver review
d3eb82c82412dad08f2352f09c2c4e4b1c829efc5760f4a456f0db09bcf7ed4e
and final targeted fuzz review
7025ac2b03d8366a1ae815d9d32619986c18b28d914557a7825c09570964c6aa
authenticate those results and the two real locks. Native filtered source
contains397 primary-manifest files plus one generated cache Rust file:
crates/behavior-testkit/fuzz/target/aarch64-apple-darwin/release/build/thiserror/ce207f63c17d7219/out/private.rs,
SHA c2ecb9a0205121ce129c369a8ba8abdab85c8658153dabff7b6b2b0d5a57c065.
This exact thiserror build output is outside the actor/module target closure;
it is not one of the806 primary inputs or a new reviewed owner source. Tooling may
scan it; no never-read claim is made. Preserve this source-closure qualification.

Before the requested research backup update, complete owning change record
f10bd32ebd3bbf18440efa5cb3c8b980f5f0cbcc24dbfb07f9653a8dc9ef1919
measures13 existing paths against EDC: production source +710/-187/net523,
tests +1240/-305/net935, documentation +96/-12/net84, manifests0,
three observation-only public nominal types added and none removed. Production
source counts include doctest/documentation comments in production files;
this is new capability source, not code reduction. Source-owned backup
preedit7dacb00feb5ab8f56ae64d0406310268a7c082fe2fc8fdb11209db84a7aa6b77
changes only the two already approved protocol/fuzzer files from d873.
All other804 primary bytes remain preserved. Root may update the reviewed
research backup after authenticating this final measurement; no upstream
PR, release, canonical selection or full observation gate is accepted.

### Current complete observation-retirement and continuous model witnesses

The reviewed owner research backup was actually pushed to
research/exec-observation-ownership at0f0d9384ff711f850d332d50dca7b42abcf26373.
Receipt19d495f0596eef230e62c813a0f56ce82be90699df5e22d789eef2b9093de94f
and independent5a9391a4f1a5ae52bc44a1cf32b1fe5efddd092f5106c97aca40c9e47a1f76ea
authenticate the complete806-source d5c epoch,13-path measurement and actual
remote head. This is a research backup; no owning observation PR or release
is claimed. Later continuous tests are a separate source epoch.

The user delegates recommended decisions without further questions (PRD52).
Root applies independently reviewed strict cfg correction and full EV20
suffix sequentially, receiptb3439c77d5128e7640615533e39af601f789658d76e1b7323d1212fa7428076b.
Actual pinned formatting changes only assigned source paths. The resulting
345-file runtime manifest is
c51cb99c986cff12ba568a5f5e6edd03d3d8f354d8771be575290a03fe1ddd3e.
Full15-test group passes without warnings in both actual profiles:
debug371497517e38b3a5b9a8934e96d4b12c7e52dbf05f3dc09d1a8e0e98342bd313;
optimized787ecd666bc931189f2911af672109f85d11c831f5d047ccc255722f36eab4ec.
The deterministic controller now exercises real advanced ControlClosed(E)
and a real StopOnShutdown/Monitor Driver retirement. Original acquired
Stopped survives LocalResidual task settlement and Completed.control, with
whole relationship, outcome, timestamp and complete retained action lanes.
These are actual runtime results; synchronous Started/rejection closure
remains a distinct open custody question.

Actual two semantic omissions discard the failed-send event or erase
Completed.control. All four omitted runs compile, then fail the intended
finite0-versus1 custody oracle in debug and optimized builds. All four
restored runs pass. Receipt
f721833a4ad522fa25fe8f735e88c014123fb82a12a7a59f33816f7a8cc72422
binds all actual commands/logs and full345-source restoration;806 owner
inputs remain unchanged. Every actor/gate/activation task is joined before
these final assertions. An initial orchestration TypeError executed zero
Cargo calls and is preserved separately as excluded-script-type-error-
verification (68bf91c9caa37b777003c72c415e4a12822e16dd14fc2f2a695bbfde187d72f6);
it is no semantic nonpass or acceptance evidence. Independent outcome
review is pending; no full observation or EXEC gate is accepted.

The qualified continuous proposal changes exactly the two approved owner
test paths, receipt68a0b395e46d55272875b222fd157361cab5386c503426265b561d34738291db.
After pinned formatting, all806 source inputs bind manifest
b542bd82e4ed1fd7eb8af1c84d2ed31a6ce8018c2bd207608457634117868787.
Both actual model tests pass in each profile:
debug1b8d5a72ca4c7ce5c2332d8c532571c6ab47c33394a0d8a20083afa1243ec61d;
optimizedd27360578ae3c68693f974c1c759c39124ddf856e4e03985f66fb49b59301250.
The oracle follows five complete normative lifetimes, checks every original
fact after each step, and explicitly reconstructs only a never-accepted
request for serial retry. It does not assert arbitrary ten-state exhaustion.
Fresh source-bound omissions, strict checks, continuous fuzz and affected
full-owner checks remain required before accepting the extended model.

Native family original controller assignment11dde98f750db6481b46ba1a879b1296c3ea4f847c4db1e6e809ad12d5e3d0bb
freezes all345 canonical50df inputs and adds only the independently reviewed
313-line cfg probe (PRD54). Its source reachability review7070e8c authenticates
the actual sealed family product, represented-ID clone fault and final custody
oracle; complete callback provenance is explicitly excluded. Actual execution
results follow separately, and no repair or joined family success is implied.

### Reviewed observation and native-family execution results

EV20 outcomes and both-profile continuous model positives receive independent
review841ee7a34015c9ca71e61acb80a91bf4894ee4f6cbf3e58b0f9162b1315c0865.
The four compiled event/projection omissions fail their intended finite
oracles, and all four restored runs pass after joined cleanup. The earlier
orchestration TypeError is excluded, not a semantic failure.

Current continuous model inversions0e201754c55f3ed4d217fe5557aeac265e4f5b29ae0f84f90d30f8e8fd36b1d2
contain eight compiled intended failures and eight restored passes across
both profiles, with all806 primary inputs restored. Independent review
3745dbf8b3d8c8c92d27a13a6a573d24905dd7a676f150348344815284f9cd8f
authenticates these results and actual10,000-run continuous fuzz campaign
a88c3eab714aded950c6f485f490de3be148467d1d7a8ef53fe7b0dd2e2a5790.
The campaign uses a copied seed corpus; no crash artifact is produced.
This is bounded execution, not arbitrary alphabet or sequence exhaustion.

Affected full-owner native checks9b73950e05a6447b57eec823e71d4ab9f39c4ca9dac4b87bc41e322f0ad41fc0
pass all ten checks:865 executable tests with zero skips and107 rustdoc
tests. Root-pinned owner/fuzz strict and corresponding formatting pass;
continuous-strict receiptd897b4f32449c67c33d928cb0e74af81d8fa0da45c31a2f1fbe5b79b1cd6e5f8
retains the distinct46-diagnostic downstream strict failure. Six equivalent
test-length expectations and one field-local complexity annotation address
that failure without new runtime semantics. Updated source manifest
2ed4be1bc45af3a90698385d2eebad1acabf0326999a27ff512f1fd64deabcde
then passes15 focused tests in each profile, workspace all-target strict
Clippy and formatting, receipt100e20786c06d1a5bc6559fcd33df1127653f5296b76a71d0b1ca11f983f3868.
Independent81c8203f3e329cfcbe204c159cde6a511bc24e7812f8fcb28955652bb48eb733
authenticates actual commands, equivalent successor, and unchanged other
344 inputs. Earlier inversions retain their actual source-epoch binding.

The stronger native-family original controllera7f43ec19404db80a9f16c20d87570d7eb0eae6a63a359c725a8e2e31afdf5df
compiles and fails the final joint root/head-result custody oracle in both
profiles, without warnings, after available prefix/callback observations
and fixture cleanup. Original controllerc9201176e9b6d9c7ad64b72d338a06490240c6d199473fb138bd1dc3d24b9865
and its independent76054474451a0dc266e8797ef69c8ae9da14ac480d164194f75e38097b2d6aa0
remain preserved. Independent e9da06b76767db7ee090c219d22de2086ef48949d6ece98412e6673183b7c87a
authenticates stronger native results and full-owner native checks.
The logs prove joint custody failure; they do not identify which individual
Weak is absent. Runtime destruction is not successful joined family shutdown.
The native defect remains uncorrected; no complete observation/EXEC gate,
canonical runtime integration, owning observation release or final PR is claimed.

The paired external public/private consumers execute in both profiles against
the current345/806-source epochs, receipt
a8e1e8b4090d4dfb6ab298c876bc8ffbad30ae99b34b204134a7045cca298eba.
Distinct package names force actual positive compilation; both negatives
produce only E0603. Independent142f4eb7b1d434275d6999da6b39f385f40a3438402c649ad3a9eb5529e6e4a6
and source supplementa37d57e703ffa3dd53398722d3b29f8be72ca2446d9e00ee5835c21afb5b8846
authenticate source/metadata/locks and the identical selected patch graph.
An earlier same-name/version fixture reused a fresh positive cache artifact
for the negative candidate; excluded receipte3c4382216ce684a81dfb826717172517113847ef2ebca8627f9582a6cf985e0
preserves that non-witness. It is not a successful privacy denial.

The complete continuous owner source is now backed up remotely at
95b3b4c4d9ff46b02a399553d6d5a595be16ab53 on research/exec-observation-ownership,
receipt51d03f23ac1e45855045672d77893212c7db83f1504fae1f30b86d01d1f13770. All806 primary bytes
match the tested b542 epoch. The actual commit changes two existing test
paths (+742/-242/net500), including pinned formatting; earlier +486/-240
was the pre-format proposal, not the final commit delta. The reviewed complete
EDC-relative13-path recordf9fb07d0539945bbc0fb468f02a7891eddee3d572ff06054840065cc82af3d09
remains production-source +710/-187/net523, tests +1752/-317/net1435,
documentation +96/-12/net84, public nominal types +3/-0. No upstream
observation PR/release or final gate acceptance is implied by this backup.

### Smaller observation and direct application-work results

Two-owner focused receiptf57e96521b4db7587ab0a5531cb6a2de5f106eeff76faad1b7c4459dfa686432
passes capabilities, settlements, continuous model and15 actual retirement
tests in both profiles. Staticd6ba75600e31e1e10b2c86c5ff243864a305b3c111fa355681eba76678a18d4a
contains36 real checks with distinct package names:18 freshly compiled
positives and18 intended denials. Model808438ed025556833f4e7613a29a564a3d116cccb24ac27d69f10d2d46534873
contains eight compiled intended failures and eight restored passes.
Independent5fa948d003825603b70e83eda227a9276d8c44ee21e3f1f74e7d07509eaa751e
authenticates those exact source and diagnostic results.

Full workspace receipt e763f220a236e3729004e2e1b3a575eca76fd72d6045c7eefae71b33d0e337d8
passes owner972 and runtime444 tests in each profile and both strict/format
suites. Native/fuzz55c9bf52ad4f3586fae86d5f5d94d6e9fa2ef989754b2a03a1575123b255661a
passes ten owning native checks, changed-target strict lint and10,000 runs;
308 copied seeds remain and the corpus grows to359, with zero crash artifacts.
Independent557611d8dcc6514c18846f53a1cd60299185f972b8e8dd46ee27f7cc9465d2de
binds all logs, native filtered source and restored806/345 inputs. Registry
unused-patch notices in narrow downstream test graphs are preserved; they
are distinct from Rust lint failures. The excluded first broad lock resolution
never reaches semantic tests and does not update selected dependencies.

The actual two-owner code backup2800cca0c89619aeae07594b659318709c128672
matches all806 tested bytes, remote receipt330bd037192fed6c36349b7f914d68b1731a6b71e0df1aa563e65d1b7ca8fcc1.
Its exact ten-file commit is +102/-254/net-152. Complete thirteen-path
measuremente09e7eee70de5b1a1fdd732e2aadf22bba52f33a1818911541fa10c9f5f4f938
remains net437 production-source,1375 tests and78 documentation lines,
with two new public nominal types. Independent81572424a9953672057cf6418c67e55e24c2b702fb5bb387aefc73ada3b745c8
reviews the copy and complete measurement. Previous95b3 remains its parent.

The direct-F private unit comparison passes eight controllers in both profiles,
strict all-target lint and formatting under696e81dacec63b0a26dd7057fecadb6ddea91b9d03c68b5ad3298105560a9fdc.
The original seven warnings and thirteen strict diagnostics remain recorded.
Four mutation cohorts95984e27bb75ebcd8bf408ac10e3a648cbcabf6f0fffde3557a57c054cb0094f
produce eight intended runtime failures and eight complete eight-test
restorations. Fifth-mutation E0282 is excluded as semantic evidence;
explicit existing channel typing then yields two intended post-join failures
and two complete eight-test restorations under333344ffa7359943cf5dd2ccb9520642efcfa2251aaf08ed369a1fc6001944c6.
Independent679b3ca73d19c5830e2ab17885c99cda3232abbed94dd246122c0125209c8f4f
authenticates the first four cohorts and corrected positives. Final independent
review67d76a403b50864c940f3d61a4dda08f54b7c32b1d081b0bf1782f17dd2314b7
accepts the corrected fifth outcome and authenticates its retained single
Copy-origin-drop warning. All345 actual unit inputs are restored;
no public API or non-unit family acceptance is claimed.

Fresh two-owner runtime custody omissions1a742ac74fc72a392aedfad98821d07354e6ad06961b1253eaf40e8a697f0d15
produce four intended failures and four passes after actual joined cleanup
across both profiles. All345 runtime and806 owner inputs are restored.
The generated patch backup is archived outside source; complete source
extras are accounted separately. The same independent final outcome review
accepts these bounded results and source restoration. These fresh results do
not relabel earlier three-owner receipts
or accept a full observation/task/EXEC gate.

The two additional application-work panic controllers pass with the original
native payload retained opaquely by the caller, receipt
2ddd02ffea4b5ce2d1f0de0501942d2312db68fb41acebe4a817eac616926c4b.
Ten warning-free positives/restorations pass in both profiles, strict lint and
formatting pass, and four compiled classification failures occur after real
joined cleanup. Original external Weak observers prove payload identity and
release without native payload inspection. Independent
c687c07c717f601262df0bb9099f3e4a61040bf49345173c9aece43b3b35ac0f
binds all345 restored source inputs and preserves the first strict nonpass and
mutation warning. The rejected downcasting text remains unexecuted. These
private tests add no public API and do not accept a full execution gate.

### Public completed-result receiving quality

EXEC section92's actual public controls/inversions/restores are source-bound
to the clean346-input quality successor1eaca071, independently reviewed364980a6.
Four public controls per profile, four intended finite allocation failures and
four exact restores execute atjn3ly450; no fresh runtime run is claimed for
the declaration-only successor. Fresh named lib/test default/Axum strict and
format pass. The first compile/privacy/equality/lint nonpasses remain immutable.
Production +185/-51/net134 and tests +464/-0/net464 versus nativeea6 are
isolated research deltas, not the canonical change record. Full cleanup/caller
work receiving, reason export, HTTP/families, migration and decision gates remain
open. Actual receipt and review:

- /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-public-fixture-quality-execution-l5x1x4gm/verification.json
- /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-public-receiving-clean-actual-independent-15h_k5db/review.json

### Public exact startup reason

EXEC section93 actualc29d1c42 and independenta523ca87 authenticate four
public controls/profile, sole pre-export E0432/profile, actual compiled
misclassification failure and exact restored pass/profile, default/Axum
named-target strict and fmt. Complete row/value discharge precedes the final
negative reason oracle. All346 inputs are restored, other344 unchanged.
This publishes one existing closed error name with zero new nominal types.
Full API/cleanup/family/HTTP/migration gates remain open. Exact commands and
source/log manifests are retained in:

- /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-public-composed-error-execution-x1c8lmu2/verification.json
- /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-composed-error-actual-independent-nez1yw1q/review.json

### Observation prerequisite merged

[EXEC section63](../execution-ownership.md#63-reviewed-observation-prerequisite-delivery-2026-10-04)
records actual Behavior PR84 delivery: independent exact-head approval,
fourteen latest passing checks, authenticated main rules and resolved threads,
and merge58895153640bb8bdc037c480dcec48a2ba57afa7. Superseded cancelled CI
is excluded. At that merge-stage checkpoint, verified-main publication and
selected dependency checks remained pending. Section67 below records their
subsequent outcomes. Upstream delivery alone accepts no Bombay decision gate.


[EXEC section65](../execution-ownership.md#65-actual-observation-release-preflight-checkpoint-2026-10-04)
records actual corrected release preflight, ten local Nix checks, independent
exact-head approval and all14 passing checks. Behavior PR85 merged at
1fc8fb55ae9c84ac77fbda293c7da63aea2c853b; publication was pending at that checkpoint.
[Section66](../execution-ownership.md#66-current-typed-observation-polling-comparison-assignment)
records the compile-only polling comparison: intended static denials and
restorations pass in both profiles, with the original harness false rejection
and actual strict-lint nonpass preserved separately. No full gate is accepted.


[Section67](../execution-ownership.md#67-published-observation-source-and-isolated-selection-checkpoint)
records passing verified-main CI and automatic publication, authenticated
Core/Actors0.22.0 archives, and actual isolated root/fuzz registry resolution.
Canonical selection remains0.21.2; compiled integration and full gate acceptance
remain open. [Section68](../execution-ownership.md#68-current-parent-conversion-original-defect-assignment)
records fifteen existing controls passing in each profile and the first
original parent run: active-turn ownership loss reaches its intended final
oracle; initialization fails an earlier incorrect mailbox-barrier assumption
and is excluded as defect proof. No optimized original or repaired pass follows.


[Sections69–70](../execution-ownership.md#69-registry-verification-and-exact-metadata-follow-up)
record fresh registry focused checks and exact declaration-only metadata updates
with passing manifest/law-runner/strict/format outcomes. The corrected original
parent witnesses retain their intended failures in both profiles with strict
lint and formatting passing. A newly source-confirmed earlier-receipt loss
keeps full conservation acceptance open; outside-owner cleanup alone is
insufficient. Historical failures and EDC outcomes remain separately preserved.


[Sections71–72](../execution-ownership.md#72-retained-row-isolation-and-ordinary-ownership-comparison)
record full registry workspace checks in both profiles, the original Core
receipt-loss witness, corrected outside-owner positives and semantic inversions,
and the ordinary lexical ownership comparison. The
[task custody record](task-custody.md#executed-preservation-outside-a-panicking-interpretation)
keeps their exact scopes and ownership map. The earlier failed assertion6874
concerned the explicitly discharged delivery unit, not the retained prior row.
Original failed command results remain preserved. Current-turn Core receipt
custody and complete task/API/projection acceptance remain open.

[Section73](../execution-ownership.md#73-source-tail-inversion-checkpoint) and
[the source-tail record](task-custody.md#executed-source-tail-ownership-comparison)
record the original generic-source loss, ordinary controls, actual Core closure
ordering inversion, both-profile failures/restorations and exact source binding.
This evidence selects no public interruption contract or full gate.


### Source and paired work pre-edit records (EXEC sections94–95)

Factual nonauthorb8be2a7f authenticates corrected PRD878cfe72 and scopea5704873:
156→161 paths adds exactly five existing Core/Actors owning files; conditional
public nominal allowance5→7 reserves SourceProgress and ApplicationOutcome.
Canonical pre-edit4ed515d3 retains69 tracked/zero untracked paths, production
net133, tests net2738 and public nominal0/0. This scope delegation does not
approve source, retain a public type or accept a semantic gate. Source reviews
420cf72e and a281f18c require correction before execution. Their immutable
proposals and future controller plans supply no compiler or runtime pass.


## Reviewed source library, paired result and current receipt checkpoints

These are independently reviewed isolated results for EXEC sections94–96.
All Rust commands ran under Bombay's pinned Nix shell with Rust1.99 and one
cache writer; the bounded named-package checks do not cover the whole workspace.
Canonical selection remains Core/Actors0.21.2; only these experiments use the
verified0.22 graph. None accepts an additional decision gate or delivery.

Source-library successor7c155 has actual receipt
`452184fe10d84d176f5250b62d7dd3b6f43f1c57b08856f5759ea1ba6a9be8cc`
at /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-source-library-bounds-execution-jj2xu8ig/verification.json;
independent review
`e1915ec12b43482687551d40b2b55d4cf99e50aaf959421d55e76ade9e96c817`.
All1152 inputs match the predecessor plus sixteen reviewed texts. Locked offline
metadata identifies the three actual local owning manifests by resolved filesystem
identity, retains Timer13e and every unrelated package dictionary. This corrects
the earlier literal /var versus /private/var path test without relabelling its
NONPASS. Core, Actors, and combined Bombay/Engine `cargo check --offline --locked
-p <owning package> --lib` each exit0. The full proposal remains production
+605/-306/net299, cfg tests +73/-66/net7; adapter migration, direct source-custody
controllers, static boundaries, all-target checks and semantic gates are open.

Paired UNIT quality successor has actual receipt
`dd5d7e2d25f07b92b5f80fab5432af7030f1aa82ed4375fdf750780cd47eac8e`
at /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-unit-test-quality-execution-ikpfe137/verification.json;
independent review
`7af145c7f74d3afd10b0afc468c09426efbf1cc5acb15887775245e14c41d764`.
The prior actuala082 receipt and da792 review authenticate fourteen controllers,
four intended finite failures and four exact restores in each profile. The fresh
successor reruns exactly the changed borrowed-output controller in debug/release:
`cargo test --locked -p bombay-rs --test completed_application_receiving
borrowed_local_work_returns_non_send_output_after_root_join [--release] -- --exact`.
Each runs one and passes. Other thirteen bodies and finite mutations/restores are
source-identical and explicitly inherited, not rerun. Default and Axum-feature
`cargo clippy --locked -p bombay-rs [--features axum] --lib --test
completed_application_receiving -- -D warnings` and `cargo fmt --all -- --check`
all exit0. All346 inputs and five logs authenticate. Production bodies matcha082;
six public panic rustdoc lines and the exact two test lint corrections are the
only quality changes. Full formatted c29-relative delta: production +292/-4/net288
(including six rustdoc lines), tests +678/-4/net674, three paths, public nominal
+1/-0; private production nominals2 and private test nominal1. Declared application,
family prefix/fault cleanup, HTTP, raw descendants, full migration and minimization
remain outside this bounded UNIT evidence and are still required.

Corrected current ActionItem receipt witness has actual receipt
`81e9dc45679c991ce3f1fe29b24ba6bae79f671f99b1d7113cd22582212aa4d0`
at /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-current-ready-oracle-execution-_9t41yd4/verification.json;
independent review
`2b1dc982cdc5d9b6693dacf158e0c9205e679b82188870967c23ae68227e53f3`.
All806 inputs authenticate; only the existing total_interpretation test changes.
The sixteen actual command logs establish three healthy controls per profile,
original failure per profile at the full outside-entry/allocation/content oracle,
finite acquired-receipt-surrender failure per profile, and exact restore per
profile; owning strict and final fmt exit0. Every qualified test runs exactly one.
Actual original traces say outside=None and original allocation count1 both before
and after known-owner/cause discharge in debug and optimized builds. This is an
unavailable/stranded receipt, not automatic destruction; earlierc9 remains an
unexpected-oracle NONPASS and is never substituted for intended original proof.
Ordinary outside borrowed acquisition retains the complete entry/allocation/content
and releases it once; intentional Ready surrender instead reaches count0 versus1.
Original opaque panic custody is independently discharged before final oracles.
No production fix is made: normal current receipt, interpreted prefix and untouched
suffix must still compose through vectors, layers, Actions and Driver retirement.
A slot inside the unwinding settle_item frame is insufficient; borrowing the later
source-offer loop cannot repair the earlier receiving boundary.
