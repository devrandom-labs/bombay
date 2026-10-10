# Backlog status and dependencies

Updated: 2026-10-10. This is the implementation index for the
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
verification and accepted decisions. `merged` records actual reviewed delivery;
other product rows remain incomplete.

The audited static-remote-actor specification and post-EXEC audit merged through
[PR329](https://github.com/devrandom-labs/bombay/pull/329), commit
`a9c5b7d4a1cf2b15504acef43c20c0da9b5e320e`, after final-head review and all four
observed CI checks passed. [Preparation delivery](../prds/static-remote-actors.md#change-record-and-current-checkpoint)
records exact evidence and skipped optional Miri work. This is documentation
delivery; AUTH1 and NET1 keep their implementation statuses below.

| ID | Status | Blocked by | Unblocks | Required outcome and evidence |
| --- | --- | --- | --- | --- |
| EXEC1 | merged | — | — | [PR326](https://github.com/devrandom-labs/bombay/pull/326) merged at 2026-10-08T10:13:33Z, commit `81256c8e00b580f37f4a1036ac28ec53b1ea948b`, after all four required checks passed on6cf7736. [Current delivery](../prds/execution-ownership.md#277-actual-reviewed-delivery) retains all eight gates, scoped code/PRD/minimization, all15 local commands/21 Darwin checks, clean Linux checks/fuzz, unchanged coverage/mutation floors and all17 accepted agent contributions. Scope277/global public API+13/−1 remain. Earlier Darwin classifier/resource failures and inherited proof/style limits remain explicit. |
| NET1 | blocked | AUTH1; external: verified typed composition, protocol/security and resource contracts | PLACE1, DIST1, SELO1 | [Static remote actor PRD](../prds/static-remote-actors.md) retains the selected profile and open remote gates. Its nine controlled dependencies are published, reviewed and merged, with passing registry-only consumer proof. Joined-retirement prerequisite [PR330](https://github.com/devrandom-labs/bombay/pull/330) merged at2026-10-10T08:11:03Z, commit017ec7603e0193e26129e0af6d10b8c04fdad78e, after independent source review and all four observed CI checks passed on690e628. Local debug/optimized controls, intended source inversions, actual report-construction denials, standard cleanup-panic custody, full workspace/docs/all-target tests and strict Clippy pass; exact evidence and limitations remain in the PRD. The isolated exact-recipient capacity-nonwaiting service admission prerequisite passes public/owning debug and optimized controls, intended inversions, full local checks and independent minimization review; [PR331](https://github.com/devrandom-labs/bombay/pull/331) merged at2026-10-10T09:45:37Z, commit9fc225ee0e96e697f0b48b619abe6ea8c6721e7c, after independent review and all four observed checks passed (required Nix38039822580). [Research PR332](https://github.com/devrandom-labs/bombay/pull/332) merged at2026-10-10T11:47:54Z, commita3a018790b4a0edb61d67e190d8978b1ce38464f, after independent review and all six observed checks passed, including both actual Linux TLS campaigns (16 cases/profile); its local scope/transport prerequisites do not close full remote witnesses. The owning closure-diagnostic correction is reviewed, merged and actually published as Communication0.1.4; the candidate selects only that corrected registry node, preserving all other178 locked records. Protected remote admission, two-process/fault-host correctness, exact versus ambiguous delivery and bounded remote ownership remain unimplemented; all full R01–R25 witnesses remain unexecuted. Choices use delegated recommended selections. |
| AUTH1 | candidate | — | NET1, PLACE1, DIST1, SELO1 | [Admission scope and gates](../prds/static-remote-actors.md): deterministic accepted, denied, stale and unavailable admission; provider substitution through a typed contract before downstream Selo integration. The user selected one coherent permission/time check and capacity-nonwaiting mailbox attempt, with permission updates excluded and fresh revalidation on retries. Local owner inspection is recorded; clock, coordination mechanism, resource and executable composition gates remain. |
| PLACE1 | blocked | NET1, AUTH1, MNE1; external: authoritative placement/fencing contract | DIST1 | [Placement](identity-and-placement.md): competing activation and partition recovery deny a stale owner's durable effects. Static routing in NET1 does not wait for this automatic-placement milestone. |
| MNE1 | blocked | External: Mnesis-Bombay release and dependency-graph alignment | PLACE1, DIST1, SELO1 | [Durability](durability-and-operations.md): select current Bombay and compatible Behavior; hydrate before routability and retain exact admission, decision, conflict, uncertain commit and durable completion outcomes. Local migration can proceed independently of NET1; complete distributed writer fencing with PLACE1. |
| DIST1 | blocked | NET1, AUTH1, PLACE1, MNE1 | OPS1 | [Distributed composition](durability-and-operations.md): supervised, durable, authenticated actors recover across host failure. |
| OPS1 | blocked | DIST1 | — | [Operations](durability-and-operations.md): equivalent semantics on self-hosted and Kubernetes deployments, bounded telemetry, drain and compatible upgrades. |
| SELO1 | blocked | AUTH1, NET1, MNE1; external: Selo KERI runtime | — | [Downstream identity](identity-and-placement.md): run the admission contract suite against Selo, then prove rotation, delegation, revocation and deployment scenarios. |

Every internal `Blocked by` edge has its reciprocal `Unblocks` edge here.
The [post-EXEC audit](../prds/execution-ownership/post-delivery-audit.md) records
fresh delivery/source checks and the corrected AUTH1 → NET1 and MNE1 → PLACE1
edges. The [next PRD's parallel plan](../prds/static-remote-actors.md#parallel-execution-and-critical-path)
separates concurrent contract research, independent implementation and shared
integration gates. Neither research eligibility nor this plan completes a row.
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
| ARC-001, ARC-002 | Typed weak application lifecycle authority; exact child/Entity control; external interfaces own admission and origin without an unobservable private Address lease. | [Capability contract](../runtime-capability-interfaces.md), [public API audit](../public-api-audit.md), [application lifecycle](../../crates/bombay/src/application/interface.rs), [local shutdown admission](../../crates/bombay/src/local/environment.rs). |
| ARC-006, TEST-023 | Invisible Address reservation through initialization commitment; failed commitment never publishes an endpoint. | `address_is_absent_until_accepted_initialization_commit_completes`, `rejected_initialization_never_becomes_addressable`, `corrupt_initialization_never_becomes_addressable` in [local Environment tests](../../crates/bombay/src/local/environment.rs). |
| ARC-011 | Dropped startup/finish waiters retain cleanup authority; actor task settles activation work; root and owned launches share setup; typed origin projection remains. | `dropped_root_activation_waiter_releases_its_reservation`, `dropped_finish_waiter_retains_actor_cleanup`, `dropped_finish_waiter_settles_actor_owned_activation_task`, and `owner_retirement_preserves_later_activation_task_panic` in [local Environment tests](../../crates/bombay/src/local/environment.rs); `owned_outcome_preserves_activation_panic_and_cancellation` in [native launch tests](../../crates/bombay/src/launch.rs). The former `activation_task_panic_still_unwinds_the_joining_owner` locator is historical; current tests retain the complete typed failure. |
| ARC-012 | Affine activation publication and terminal-report handoff; owner cancellation has a distinct typed residual. | [Actor execution](../../crates/bombay/src/actor_execution.rs), [local Environment](../../crates/bombay/src/local/environment.rs), [local action settlement](../../crates/bombay/src/local/effects/mod.rs), [native launch](../../crates/bombay/src/launch.rs), [terminal publication tests](../../crates/bombay/tests/run_with.rs). |
| ARC-010, TEST-025 | All 19 inventoried actor-owned capabilities interpreted against Behavior Actors 0.20.0; executable supervisor and FIFO recovery/shutdown witnesses. | [Template manifest](../driver-template-manifest.json), [supervisor recovery](../../crates/bombay/tests/fixed_supervisor_recovery.rs), [FIFO recovery](../../crates/bombay/tests/fifo_pool_recovery.rs), [template applications](../../crates/bombay/tests/template_application.rs). |
| ARC-020, TEST-008 | Minimal retained module ownership and separate revision-bound actor-template inventory. | [Module map](../module-boundaries.md), [public API audit](../public-api-audit.md), [template manifest](../driver-template-manifest.json). |
| TEST-020 | Corrected Linux fuzz shell and unconditional artifact-upload failures; CI run 36985076274 passed both bounded campaigns and uploads. | [CI workflow](../../.github/workflows/checks.yml), [Driver campaign](../../crates/bombay-engine/fuzz/verify-causal-turns.sh); exact historical receipt remains in Git. |

Historical local-audit selection: Behavior Core/Actors 0.21.2 release revision is
`edc2d466a50df7cd396f891e3da31fc9e3747bbd`; Macros 0.13.1 revision is
`5ca96444f0a66e9a013b6989e3e53d345cbabf65`. The Driver and template
manifests retain revision-bound evidence. Earlier EXEC notes selected 0.17.0:
their hashes, source inventories and open representation experiments are
historical; EXEC's published selection and current manifests now use
Core/Actors 0.23.0 and Macros 0.14.0 at
`d69f992b371c12ab34e73b18e45b8112c90a1508`.

## Retirement change record

Historical ledger for the closed audit retirement; these are not current worktree counts.

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
