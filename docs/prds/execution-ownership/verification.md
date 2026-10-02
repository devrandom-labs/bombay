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
The user selected exact relationship cancellation authority and capability-failure conservation; representation gates remain open. No other proposed policy is selected from a recommendation alone.

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

This stage updates project records and selects published Communication 0.1.3.
Production Rust and tests remain unchanged; manifest/lock changes measure
+3 / -3 / net 0. Baseline has no inherited tracked or untracked delta.
The complete canonical Bombay delta follows; owning repository delivery has
its separate complete ten-path record in the shutdown decision record.

<!-- exec-research-counts -->

```text
production: +0 / -0 / net 0
tests:      +0 / -0 / net 0
public API: +0 types / -0 types
documentation: +2217 / -11 / net 2206
manifest/lock: +3 / -3 / net 0
changed tracked and untracked paths: 13
```
