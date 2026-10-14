# Execution ownership contract

This is the preservation contract and source map for Bombay's current execution
owners. The historical EXEC campaign and its nine companion records remain
pinned below. The [post-delivery audit](execution-ownership/post-delivery-audit.md)
retains its dated findings and proof limits. Later joined-retirement requirements
and current integration evidence belong to the
[remote-actors PRD](static-remote-actors.md); historical signatures and pass marks
do not certify the combined source or close its remaining requirements.

The original requirements, decisions, rejected experiments, measurements and
delivery receipts remain in the
[pinned EXEC record](https://github.com/devrandom-labs/bombay/blob/c3afb3011090ac4c9112fd39e4a76980db856df0/docs/prds/execution-ownership.md)
and its linked companion files. Consult the exact historical section for an
old acceptance decision; do not treat its signatures or pass marks as current.
The source and executable witnesses determine implemented behavior.

## Execution and ownership

Bombay runs the existing causal Driver on a caller-owned async host. The
configured blocking adapter accepts a Tokio runtime builder and rejects use
inside an entered runtime. It is another host for the same execution contract.
Application work and actor execution have distinct ownership: caller-local
work must not acquire actor-task `Send` requirements merely through a wrapper.

The execution and result-receiving futures can be retained independently.
Dropping a waiter is not proof of actor retirement or absence of admitted work.
Startup, supplied but uninvoked work, active work, completed work and cleanup
can leave different owned values. A work result can coexist with cleanup
failure. Keep every acquired value in the owning result path across cancellation
and panic cuts; do not replace missing evidence with success.

The advanced receiving interface retains application-work outcome, original native
root result and both notification results independently; Entity execution also
retains its family results. Joining the exact root and publishing its retirement
report may finish while application work is running. Final native handoff and
application-family shutdown retain their work barrier. A later cleanup-task failure
cannot erase an acquired native result or either original notification cause.

Children retain the original native result with their parent. Shared read-only
retirement reports are derived by runtime owners from actual joined tasks and
resource retirement before application projection. Actor failure, incomplete
retirement and notification failure remain separate facts. Unpublished startup
retains its actual termination-notification result without inventing a second
publication. Actor establishment and complete resource retirement are separate
assessments; failure to establish an actor does not itself prove incomplete cleanup.

Behavior remains the pure typed transition algebra. Driver causality, staged
creation, initialization commitment, typed action settlements, source-return
ordering and complete terminal custody remain separate responsibilities.
Application syntax must not bypass their owners.

| Concern | Source owner | Preservation witness locations |
| --- | --- | --- |
| Declaration and staging | [Application](../../crates/bombay/src/application/mod.rs), [composition](../../crates/bombay/src/application/composition.rs) | [Topology example](../../examples/application-topology/src/main.rs), [application children](../../crates/bombay/tests/application_children.rs) |
| Caller-host and configured blocking execution | [Application execution](../../crates/bombay/src/application/execution.rs) | [Runner tests](../../crates/bombay/tests/run_with.rs) and tests in the owning source |
| Acquired work, result and cleanup | [Application execution](../../crates/bombay/src/application/execution.rs) | [Completed application receiving](../../crates/bombay/tests/completed_application_receiving.rs), [terminal custody](../../crates/bombay/tests/application_terminal_custody.rs) |
| Actor task ownership | [Actor execution](../../crates/bombay/src/actor_execution.rs), [native launch](../../crates/bombay/src/launch.rs) | Tests in those owners and [local Environment](../../crates/bombay/src/local/environment.rs) |
| Causal interpretation | [Driver](../../crates/bombay-engine/src/driver.rs), [Environment port](../../crates/bombay-engine/src/environment.rs) | [Driver law](../driver-law.md), [law manifest](../../crates/bombay-engine/tests/law_manifest.rs) |
| Origin and complete retirement | [Terminal](../../crates/bombay/src/terminal.rs), [child retirement](../../crates/bombay/src/local/children.rs) | [Terminal projection](../../crates/bombay/tests/terminal_projection.rs), [static fixtures](../../crates/bombay/tests/compile) |
| Exact lifecycle authority | [Application interface](../../crates/bombay/src/application/interface.rs), [observation effects](../../crates/bombay/src/local/effects/observation.rs) | Static fixtures and local Environment tests |
| Relationship observation | [Observation](../../crates/bombay/src/observe/mod.rs) | [Observation tests](../../crates/bombay/src/observe/tests.rs), [external contract tests](../../crates/bombay/src/observe/external_tests/contract.rs) |
| Async worker preparation | [Worker preparation](../../crates/bombay/src/worker_preparation.rs), [creation effects](../../crates/bombay/src/local/effects/creation.rs) | [Supervisor recovery](../../crates/bombay/tests/fixed_supervisor_recovery.rs), [FIFO recovery](../../crates/bombay/tests/fifo_pool_recovery.rs) |

These are source/test locators, not claims that all listed witnesses were run
by this cleanup. Use the current lock when repeating them.

## Preservation gates

The former eight gate identifiers remain useful for tracing the accepted
contract. A future change must name the gates it affects and supply fresh
source-bound evidence. Their historical acceptance does not pre-approve a
new abstraction or confer a fresh passing status.

| Gate | Contract to preserve when simplifying |
| --- | --- |
| DG-API | One caller-host execution law across ordinary application, advanced composition, Entity and HTTP. Return original declaration/work inputs on pre-start rejection. Preserve the configured blocking-host rejection and actual work/output plus cleanup outcomes. |
| DG-TASK | Retain ownership across dropped startup, work, finish, source-wait and retirement futures; distinguish failed spawn, task panic, cancellation and host destruction. Join where the host remains able to make progress. |
| DG-SHUTDOWN | Message addressing does not grant behavior-indexed lifecycle authority. Wrong behavior, role, old address occurrence and external-client authority remain statically denied or truthfully rejected by the owning primitive. |
| DG-OBSERVATION | One owner and defined ordering for relationship establishment, completion and cancellation; retain already acquired completion. Immediate completion, cancellation races and reused IDs must not leak, duplicate or misattribute outcomes. |
| DG-PROJECTION | Return exact origin, terminal values and failed creation custody. Preserve cleanup timing for a child that finishes before its parent and classify projection panic without losing other owned evidence. |
| DG-WRAPPERS | Keep a wrapper only for a distinct invariant, authority or meaningful consumer. Compare ordinary values and methods; measure diagnostics and bounds without erasure or lost custody. |
| DG-MODULES | Give each semantic concern one owner, narrow visibility and a compositional module path. Moving code does not justify a second lifecycle authority. |
| DG-WORK | Typed external-work admission is bounded and returns rejected input. Admitted work has a completion/shutdown owner. Do not promise preemption of blocking or noncooperative work. |

## Limits and failure evidence

Native panic payloads at the host edge are retained opaquely as the original
Rust payload. This existing runtime contract does not authorize erased actor
messages, dynamic capability lookup or an untyped effect bus. Simplifying
terminal types must preserve original panic evidence. Any instruction change
to explain that boundary must be explicit and narrowly scoped.

An async destructor cannot promise joining. Destroying the host, aborting the
process or running noncooperative work can prevent further progress. A deadline
or cancelled wait does not establish that an external effect never happened.
Describe these limits without inventing a successful terminal value.

Retain adverse witnesses for construction, polling, ready-result acquisition,
producer destruction and cleanup failure. Preserve exact move-only inputs,
accepted effect prefixes, unattempted suffixes, acquired ingress, source custody,
child results and multiple independent retirement failures. Control progress
and fairness require explicit policies and bounded-work witnesses.

The archived delivery record includes earlier Darwin resource/classifier
failures and scoped evidence limitations alongside later successful runs.
Do not copy a later green summary as proof that every earlier limitation was
resolved, or as evidence for the current documentation branch.

## Developer-experience follow-up

The [DevX coordination record](developer-experience.md) selects a single
`Application` composition spine and one explicit topology declaration with derived
hosting and complete terminal custody. The current code uses one `Application`
owner for roots, cold actors, explicit hosting and Entity families. The actual topology supplies
native ActorCustody and its typed RetirementSchema; presentation does not replace
the runtime's joined-retirement evidence.

The backlog's [EXEC requirements](../prd-backlog/local-runtime.md) and
[failure contracts](../prd-backlog/failure-contracts.md) remain acceptance inputs.
New work must reconcile a changed law with them, the actual owner source and
its executable regressions. Keep one bounded feature record; Git owns the
superseded chronology.
