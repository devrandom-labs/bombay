# EXEC verification evidence

Status: baseline research; all decision gates remain open. No integrated EXEC
acceptance, independent gate approval, Bombay delivery PR or passing delivery CI is
claimed. The selected baseline is `2fccedf6eb636ac22143e7e01de7e784f96e2b4e`,
with the exact lock and manifest hashes in PRD section 16.

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

<!-- exec-research-counts -->

```text
production: +150 / -18 / net 132
tests: +1181 / -536 / net 645
documentation: +4963 / -81 / net 4882
manifest/lock: +36 / -31 / net 5
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
