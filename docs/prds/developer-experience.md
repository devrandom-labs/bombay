# Developer experience coordination

Status: active. Lossless result consumption and borrowed terminal inspection
are implemented; inferred topology and recursive execution remain open. The paired work lives on `codex/developer-experience-docs`
in Bombay and Behavior, based on Bombay main
`c3afb3011090ac4c9112fd39e4a76980db856df0` and Behavior main
`d69f992b371c12ab34e73b18e45b8112c90a1508`.

The owning specification is Behavior's
`docs/engineering/developer-experience-prd.md`, with its source coverage appendix.
Read that file from the paired branch while this work is unmerged. This local
record deliberately does not copy its 28 requirements or invent a second
acceptance authority.

## Selected direction

Use the existing actor facade for domain actors, `ActorExt` for genuine
same-actor decoration, owning constructors for standalone policies, named
`Actions` lanes for effects, and one `Application` spine for local execution,
HTTP and Entity. Infer topology and complete terminal custody from domain
roles. Keep real policy and exact failure handling explicit.

Bombay owns application assembly, hosting, external clients and terminal
consumption. Behavior owns the static transition and effect algebra; Actors
owns reusable compositions. Simplify at the owner that can derive the repeated
information. Preserve affine custody, creation provenance, service lanes,
initialization order and static capability distinctions.

## Scope and next work

The owning PRD now contains the single DX01–DX28 implementation checklist and
the pre-edit application-result conservation law. Fresh pinned-shell child
compile-pass/denial and result-consumption debug/optimized witnesses pass.
This is a bounded stage-1 substep; it does not close the counter/heterogeneous
inference gate or authorize broad migration. All requirement dispositions,
change measurements and remaining acceptance live in the owning PRD.

The Timers delivery prerequisite now uses published 0.2.0 rather than a
workspace-only patch. Package verification and an unpatched extracted consumer
pass; inference remains a separate open gate. The user authorized the expanded
selected programme surface on 2026-10-09; its per-stage law, provenance,
composition and measurement gates still apply.

The inferred-custody caller fixtures are pre-implementation acceptance tests.
They cover the real counter, heterogeneous and repeated children, a plain state
base, recursive creation/refusal, nominal born-role selection, and timer
readiness from actual reports. Earlier candidates passed these fixtures but rejected valid pure birth aliases.
The current isolated source copy uses retained ActorCustody and passes 16 finite
journey tests in debug and optimization; recursive execution still fails six
E0275 diagnostics. The retained library lacks the inferred API, so the feature
worktree suite remains red. Independent source-port and factory reviews have
not proved the joint alias/recursive law; these candidates remain unretained.

The original creation interpreter now has a focused custody falsifier: dropping
one pending startup attempt leaves an empty request/reply row, no joined child,
and destroys the original definition. Debug and optimized private original-
contract executions fail the intended law under pinned Rust 1.99. Before the
fixture was retained here: one owning creation test module, tests +205/-5,
production/API/state/module delta zero. It checks exactly one original allocation,
content and available correlation/provenance through the complete returned lanes.
This deliberately red regression requires startup authority and join to enter
parent-owned custody before awaiting; cleanup cancellation alone does not return
ownership. Actual locked registry debug and optimized executions both fail; strict package
Clippy and formatting pass. The paired owning PRD contains its law and aggregate
checkpoint.

The owning PRD maps all 270 backlog requirement rows and all 100 failure
records, seven executable examples, 45 catalogue entries and 19 inventoried
actor-owned capabilities. Presence in that map does not certify an execution
or commit to implementing all networking and durability work in the DevX effort.

The retirement consumption substep groups the identical 14 owned fields of
20 alternatives into `ActorCustody`; all 24 outcomes remain. The real counter
and startup-refusal journeys pass in debug and optimized builds, and a broken
borrowed accessor fails both. Borrowed behavior and normal completion retain
the original retirement for complete owned decisions; completion is not a claim
that independent cleanup has no failures. Other callers are migrated only to that proven
field grouping. Complete gates and measurements remain in the owning PRD;
this does not close inferred application construction.

The proven Behavior send-routing and facade rustdoc fixes are in owner
[PR #89](https://github.com/devrandom-labs/bombay-behavior/pull/89), ready for
review with native ten-gate/927-test verification, extracted package preflight
and all exact-head CI including strict mutation passing. Publication and Bombay
adoption remain open; current dependencies are unchanged.

Begin with a compiling counter and heterogeneous-child journey that infers
runner and terminal types while returning complete startup/retirement failures.
Then prove supervisor and pool use before migrating the catalogue. A shorter
happy path that drops owned failure values fails acceptance. The existing
[execution contract](execution-ownership.md) remains a preservation obligation.

The detailed owner assignments, stage dependencies, Rust crate choices,
compiler-denial cases, resource limits, migration gates and stop conditions
live only in the owning PRD. Each production stage requires its own measured
law and failing regression under AGENTS; this candidate is not a blanket
production change-budget approval.

## Documentation change record

Replaced the 13,280-line execution campaign with the compact preservation
contract; deleted nine chronological companion files. Historical links point
to immutable Git source. Retained every backlog requirement and failure record,
corrected the requirement count from 260 to 270, and added this coordination
entry. Status tables identify historical evidence instead of implying current
source certification. Runtime code, dependencies, public API and tests are
unchanged. Local Markdown targets, changed-document headings and whitespace checks pass;
all 270 requirement rows and 100 failure records are byte-identical to main.
`nix flake check` passed formatting but could not complete the dependency build:
`No space left on device`. No workspace runtime-test pass is claimed.
The complete documentation diff is 17 paths, including nine deleted documents,
with 214 added and 22647 removed lines (net -22433).
Production, tests, dependencies and public API deltas are zero. Cross-repository
checks and scope are recorded once in the paired Behavior evidence appendix.
