# Backlog status and dependencies

Updated: 2026-10-06. This is the implementation index for the
[requirements inventory](README.md). Each selected feature has its specification,
locked-contract verification, decisions, change record and completion evidence
in `docs/prds/`. Recheck the current lock and affected owners when selecting work.
Historical inventory marks and research snapshots are not current eligibility.

The previous local-runtime audit is closed: 25 TEST items were verified,
ARC-009 was retained, and the other 19 ARC items were distilled. Its chronological
record remains in Git history. Do not restart that audit as a feature backlog.

## Remaining product work

`candidate` means available for PRD preparation and fresh verification, not
permission to implement an unverified interface. `blocked` names unresolved
prerequisites. A feature becomes `active` only when its PRD records the required
verification and accepted decisions. No product row below claims completion.

| ID | Status | Blocked by | Unblocks | Required outcome and evidence |
| --- | --- | --- | --- | --- |
| EXEC1 | blocked; independent execution research continues | External: reviewed lossless interpretation contract; verified selection of the published observation contract | DIST1 | [Execution ownership PRD](../prds/execution-ownership.md): caller-owned hosting, multicore concurrency and exact execution/result ownership. Canonical Core/Actors0.21.2 and Communication0.1.3 remain selected; conditional research uses the verified unreleased receiving revision81ba2c0. The combined implementation is in [draft PR #326](https://github.com/devrandom-labs/bombay/pull/326). At ba3d3c3, Deny and CodeQL pass; Nix fails on the outdated actor verification script. EXEC §225 records the independently reviewed two-file test repair: both complete scripts and restored owning cohorts pass debug and optimized builds; a pushed replacement still needs full CI. current observation checks have independent bounded review under §224. Ten existing consumer corrections are independently qualified for conditional backup: five complete test suites pass both builds, old report assertions fail as intended, and three finite mains plus real HTTP checks pass both. The counter correction is independently verified on the current startup source in both builds. The prepared-input repair passes30 tests and intended original-bug inversions in both builds with independent review; remaining consumer coverage and full CI remain open. Completed runtime fixes and DG-SHUTDOWN are preserved. Engine74 and application30 pass both profiles at their recorded source epochs; optimized Driver inversions and the intended startup work/root-loss regressions are independently qualified. Final public API, prepared actor-input custody, Entity/HTTP and external-work integration, current observation acceptance, full verification, seven gates, module extraction, minimization, reviewed CI and main merge remain open. Detailed authoritative evidence and dated snapshots are in EXEC and its supporting records; all nine controller dispositions and the canonical source-retention HOLD remain open. |
| NET1 | blocked | External: immutable Zenoh/codec selection and verified typed extension contract | PLACE1, DIST1, SELO1 | [Networking](networking.md): two-process request/reply, exact versus ambiguous delivery, bounded queues and static routing; exercise the same law with Zenoh and a deterministic faulting test host. |
| AUTH1 | candidate | — | PLACE1, DIST1, SELO1 | [Identity](identity-and-placement.md): deterministic accepted, denied, stale and unavailable admission; provider substitution through a typed contract before downstream Selo integration. |
| PLACE1 | blocked | NET1, AUTH1; external: authoritative placement/fencing contract | DIST1 | [Placement](identity-and-placement.md): competing activation and partition recovery deny a stale owner's durable effects. |
| MNE1 | blocked | External: Mnesis-Bombay release and dependency-graph alignment | DIST1, SELO1 | [Durability](durability-and-operations.md): select current Bombay and compatible Behavior; hydrate before routability and retain exact admission, decision, conflict, uncertain commit and durable completion outcomes. |
| DIST1 | blocked | EXEC1, NET1, AUTH1, PLACE1, MNE1 | OPS1 | [Distributed composition](durability-and-operations.md): supervised, durable, authenticated actors recover across host failure. |
| OPS1 | blocked | DIST1 | — | [Operations](durability-and-operations.md): equivalent semantics on self-hosted and Kubernetes deployments, bounded telemetry, drain and compatible upgrades. |
| SELO1 | blocked | AUTH1, NET1, MNE1; external: Selo KERI runtime | — | [Downstream identity](identity-and-placement.md): run the admission contract suite against Selo, then prove rotation, delegation, revocation and deployment scenarios. |

Every internal `Blocked by` edge has its reciprocal `Unblocks` edge here.
ARC-006 activation, ARC-010 capability interpretation and TEST-025 executable
supervisor/pool evidence are completed local prerequisites, so they do not
remain unresolved edges into PLACE1 or DIST1. Broader template-policy coverage
still belongs to the requirement inventory and must be selected explicitly.
Selo is a separate downstream milestone; DIST1 and OPS1 may use explicit test
identity for evidence, while production security claims require a verified
configured provider. Bombay depends on neither Selo nor Mnesis. Mnesis owns
persistence and `mnesis-bombay` owns its Bombay integration; mailbox admission
never proves a durable commit. Production failover requires resource-enforced
fencing in addition to durability. No provider registry, second actor algebra,
Bombay cryptographic identity implementation or new consensus algorithm is
selected by this index.

Automatic topology-owned actor-space materialization also remains an explicit
inventory requirement; it is not supplied by named topology declarations.
Select its owning contracts and acceptance witness in a separate PRD when that
requirement is chosen. The product table groups the remaining programme work;
it does not remove the inventory's finer requirements or optional decisions.

## Retained local evidence

These pointers preserve the completed prerequisites used by the backlog and
EXEC research. They are evidence locations, not a fresh test certification.

| Former audit item | Selected outcome | Current evidence |
| --- | --- | --- |
| ARC-001, ARC-002 | Typed weak application lifecycle authority; exact child/Entity control; external interfaces own admission and origin without an unobservable private Address lease. | [Capability contract](../runtime-capability-interfaces.md), [public API audit](../public-api-audit.md), [local shutdown tests](../../crates/bombay/src/local.rs). |
| ARC-006, TEST-023 | Invisible Address reservation through initialization commitment; failed commitment never publishes an endpoint. | `address_is_absent_until_accepted_initialization_commit_completes`, `rejected_initialization_never_becomes_addressable`, `corrupt_initialization_never_becomes_addressable` in [local tests](../../crates/bombay/src/local.rs). |
| ARC-011 | Dropped startup/finish waiters retain cleanup authority; actor task settles activation work; root and owned launches share setup; typed origin projection remains. | `dropped_root_activation_waiter_releases_its_reservation`, `dropped_finish_waiter_retains_actor_cleanup`, `dropped_finish_waiter_settles_actor_owned_activation_task`, `activation_task_panic_still_unwinds_the_joining_owner` in [local tests](../../crates/bombay/src/local.rs). |
| ARC-012 | Affine activation publication and terminal-report handoff; owner cancellation has a distinct typed residual. | [Actor execution](../../crates/bombay/src/actor_execution.rs), [local composition](../../crates/bombay/src/local.rs), [terminal publication tests](../../crates/bombay/tests/run_with.rs). |
| ARC-010, TEST-025 | All 19 inventoried actor-owned capabilities interpreted against Behavior Actors 0.20.0; executable supervisor and FIFO recovery/shutdown witnesses. | [Template manifest](../driver-template-manifest.json), [supervisor recovery](../../crates/bombay/tests/fixed_supervisor_recovery.rs), [FIFO recovery](../../crates/bombay/tests/fifo_pool_recovery.rs), [template applications](../../crates/bombay/tests/template_application.rs). |
| ARC-020, TEST-008 | Minimal retained module ownership and separate revision-bound actor-template inventory. | [Module map](../module-boundaries.md), [public API audit](../public-api-audit.md), [template manifest](../driver-template-manifest.json). |
| TEST-020 | Corrected Linux fuzz shell and unconditional artifact-upload failures; CI run 36985076274 passed both bounded campaigns and uploads. | [CI workflow](../../.github/workflows/checks.yml), [Driver campaign](../../crates/bombay-engine/fuzz/verify-causal-turns.sh); exact historical receipt remains in Git. |

Behavior Core/Actors 0.21.2 release revision is
`edc2d466a50df7cd396f891e3da31fc9e3747bbd`; Macros 0.13.1 revision is
`5ca96444f0a66e9a013b6989e3e53d345cbabf65`. The Driver and template
manifests retain revision-bound evidence. Earlier EXEC notes selected 0.17.0:
their hashes, source inventories and open representation experiments are
historical until their PRD explicitly reconciles them with the current owners.

## Retirement change record

Scope: preserve unresolved programme edges and relevant local evidence here;
retire the closed audit chronology; redirect repository guidance and PRDs to
this index and feature-local records. Preserve the user's existing document
and research-probe deletions. No runtime contract, dependency selection or
public Rust type changes. Remove the artifact audit's exemptions for the two
retired historical documents. Verification and complete working-tree counts follow.

Verification:

- `nix develop -c cargo test -p bombay-engine --test law_manifest`: nine passed.
- `nix develop -c cargo fmt --all -- --check`: passed.
- `nix develop -c cargo test --locked -p bombay-rs --features axum --test run_with --test axum --test application_terminal_custody -- --skip compile_checked`: 20 passed; two compile checks filtered out (EXEC preservation baseline).
- Local Markdown file links resolve; remaining programme dependency edges are
  reciprocal; retired-document references and whitespace checks pass.

The user's initial deletions cover nine tracked documents/research artifacts.
The current complete-tree counts include those deletions and the new status
index. Documentation counts include retired research fixtures stored under
`docs/`; no production Rust or public type changed. The test delta is solely
removal of two retired-document exceptions from the existing artifact audit.
No full workspace or remote CI completion is claimed by these focused checks.

<!-- complete-tree-counts -->

```text
production: +0 / -0 / net 0
tests: +1 / -3 / net -2
documentation: +320 / -14139 / net -13819
other: +0 / -0 / net 0
public API: +0 types / -0 types
working tree: 30 changed paths (1 untracked)
```
