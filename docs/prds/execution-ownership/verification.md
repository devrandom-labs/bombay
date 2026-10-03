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
Behavior Core/Actors 0.21.1 and Macros 0.13.1. EXEC section 32 records the
independently reviewed compiler compatibility and allocation-test correction.
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

<!-- exec-research-counts -->

```text
production: +167 / -34 / net 133
tests: +3295 / -557 / net 2738
documentation: +6106 / -87 / net 6019
manifest/lock: +38 / -33 / net 5
public API: +0 types / -0 types
changed tracked paths: 67
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
