# Evidence and limits

Snapshot: 2026-09-29, current working tree. This inventory is source-based
planning, not a fresh certification of the workspace. Existing uncommitted
changes are included in the observed baseline. Before implementation, repeat
feature-local verification against the selected lockfile and patches.
The documentation-reconciliation table below records that snapshot's edits;
The local audit is now closed; [current status](status.md) retains its relevant
evidence and the remaining product dependencies.

Follow-up: [core integration research](core-integration.md) adds focused
execution evidence for supervisor/pool prerequisites. Worker preparation tests
pass; publication ordering and diagnostic retention probes reproduce defects.
The earlier documentation-only verification record below remains the record
of that earlier task, not a claim that these follow-up probes were never run.
As of 2026-10-01, the public FIFO example executes one correlated job and
orderly worker drain. Independent runtime tests also prove queued admission
before activation, exact `BacklogFull` payload return at capacity one, and
one permanent replacement after a worker stops, with two correlated
completed jobs and exact two-worker terminal custody. The runtime also
exercises both selected interruption dispositions after an assigned worker
stops: `Retry` completes the original job, while `Fail` returns its exact
payload and reason before the replacement serves another job. The
supervisor example now executes worker replacement and terminal retirement.
Selected Actors 0.20.0 supplies typed diagnostic ingress and a late source
completion event. Runtime tests prove pool shutdown folds while preparation
is held, without starting a replacement, and preserves exact source custody.

## Selected contracts

| Owner | Selected contract inspected | Consequence |
| --- | --- | --- |
| Behavior | bombay-behavior 0.20.0, registry VCS revision `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da` | Owns Actions and the typed capability algebra. Exact revision's AGENTS.md applies. |
| Behavior Actors | bombay-behavior-actors 0.20.0, same revision | Owns existing supervision, pools and template policies. |
| Behavior macros | bombay-behavior-macros 0.13.0, registry VCS revision `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8` | Owns syntax generation; Bombay must not replace its semantics. |
| Address | bombay-address 0.3.0 | Owns local claims/leases and opaque resolution; its process-local representation is not a wire address. |
| Communication | bombay-communication 0.1.2 | Owns bounded user delivery, separate control delivery, closure and payload recovery. |
| Observe | Private Bombay implementation | Owns completion publication and waiting; is not a missing external dependency. |
| Timers | 0.1.0 patched to `13e884da7ab41781f52337b0038060e375b00ee0` | Owns volatile actor scheduling/generations, not persistent reminders. |
| Tokio | 1.53.1 in this lockfile | Ordinary runners select current-thread execution; multithread execution needs deliberate integration. |

Sources: [Cargo.lock](../../Cargo.lock), [workspace manifest](../../Cargo.toml),
[Bombay manifest](../../crates/bombay/Cargo.toml). Behavior source was read from
the exact registry packages and the matching VCS revision, not inferred from
the sibling checkout's current branch. The root patch selects Timers; a nearby
checkout at another revision is not the build contract.

Current architectural references remain
[capability ownership](../runtime-capability-interfaces.md),
[module boundaries](../module-boundaries.md),
[Driver law](../driver-law.md) and
[Driver test strategy](../driver-test-strategy.md).
This inventory does not replace those normative contracts.

## Concrete findings

| Finding | Source | What may truthfully be concluded |
| --- | --- | --- |
| All inventoried atomic interpreter requests and `PrepareWorkers` have typed Bombay interpretation | [Application runtime](../../crates/bombay/src/application_runtime.rs), [capability manifest](../driver-template-manifest.json) | Selected 0.20.0 adds typed proxy diagnostic ingress and separates accepted source start from its late result; live policy evidence remains necessary. |
| Queued pool shutdown folds while `PrepareWorkers` holds an affine source | the prior 0.19.0 failure recorded in Git history, [selected runtime regression](../../crates/bombay/tests/fifo_pool_recovery.rs) | The 0.19.0 trace failed; the selected runtime rejects a later job as `ShuttingDown` before source release and does not start a replacement. |
| Assignment/proxy exact returned-request reconstruction remains selected in Actors 0.20.0 | Behavior Actors `src/atomic/pool/assignment.rs`, `src/atomic/stable_proxy/operation.rs` | Bombay's rejected delivery interpreters can return the original typed request; the earlier 0.17.0 owner gap is resolved. |
| Activation publication regressions | [local activation](../../crates/bombay/src/local.rs), [ARC-006 retained evidence](status.md#retained-local-evidence) | The selected invisible-reservation order passes ordinary gated visibility regressions; the original claim-before-commit order failed them. |
| Ordinary public runners construct current-thread Tokio runtimes | [Application runtime](../../crates/bombay/src/application_runtime.rs) | Ordinary multicore actor execution and caller-owned async embedding are not supplied by merely having Tokio as a dependency. |
| User mailbox capacity is fixed at DEFAULT_USER_CAPACITY in ordinary construction | Same runtime | Application-facing capacity configuration remains a specific gap. |
| The public supervision example executes worker replacement and retirement; the FIFO example executes a job and orderly drain | [supervision](../../examples/supervision/src/main.rs), [worker pool](../../examples/worker-pool/src/main.rs), [supervisor recovery](../../crates/bombay/tests/fixed_supervisor_recovery.rs), [pool recovery](../../crates/bombay/tests/fifo_pool_recovery.rs) | Selected 0.20.0 tests prove coordinated replacement, later-role rejection, exact terminal diagnostic custody, child retirement, both pool interruption choices, and shutdown during held preparation. |
| Timed/Machine public application witnesses exist | [template application tests](../../crates/bombay/tests/template_application.rs) | These are existing integration evidence, not missing template implementations. |
| Cache, Barrier and Latch customer witnesses exist | [external customer tests](../../crates/bombay/tests/external_customer_templates.rs) | Do not infer absent delivery support from lack of a separate adapter per template. |
| Public lifecycle/terminal policy tests exist | [run_with tests](../../crates/bombay/tests/run_with.rs) | A supervision-report test is not, by itself, a live supervisor restart test. |
| Entity hydration, admission, capacity, passivation and metrics exist | [entity family](../../crates/bombay/src/entity/family.rs) | Local virtual-entity mechanisms should be reused; they do not by themselves prove storage recovery or cluster placement. |
| Typed local external interfaces exist | [actor interface](../../crates/bombay/src/actor_interface.rs) | Remote protocol export should integrate with established boundaries, not invent a second ordinary actor contract. |
| No integrated Zenoh or Mnesis dependency in the standard runtime | [manifest](../../crates/bombay/Cargo.toml), [exports](../../crates/bombay/src/lib.rs) | Network/durable paths are new integration work; re-exported persistence templates are not evidence of durable execution. |
| Existing CI and Driver failure campaigns exist | [checks workflow](../../.github/workflows/checks.yml) | Extend existing gates for distribution; do not claim the repository lacks verification infrastructure. |

## Neighboring repositories

### mnesis-bombay

The local sibling repository's inspected lockfile selects Behavior 0.9.5,
published Bombay 0.1.0, the former bombay-entity package and Mnesis/store 0.3.1.
That is not this checkout's current dependency graph.

Its execution crate already contains runtime-independent execute_command,
DirectCommandOutcome, CommitFailure, Once/Replay execution and phase tracking,
including conflict and uncertain-commit handling. Its Bombay integration
exports ExecuteRequest and entity-delivery adaptation. The remaining work is
version migration and a verified current-runtime interpretation/hydration path,
not invention of all command execution semantics.

Relevant source locations in that sibling checkout: `Cargo.lock`,
`crates/execution/src/lib.rs`, `crates/bombay/src/lib.rs` and execution integration tests.
These are neighboring-source observations, not dependencies silently added to
Bombay by this inventory.

### Selo

Inspected GitHub main revision:
`c7f394bf46c2e150dcdd38f36b7b0989d85ea68e` in `devrandom-labs/selo`.
The source contains `crates/selo-naming`; the inspected manifest does not
compose Bombay, Mnesis or Zenoh services. Names include agent mailboxes,
queries, presence and KERI event/state paths. Identifier shape validation in
the naming crate is not cryptographic KERI verification.

Sources: [pinned repository](https://github.com/devrandom-labs/selo/tree/c7f394bf46c2e150dcdd38f36b7b0989d85ea68e),
[naming contract](https://github.com/devrandom-labs/selo/blob/c7f394bf46c2e150dcdd38f36b7b0989d85ea68e/docs/naming.md).
Repository access may require authentication. Selo's future implementation is
a downstream workstream; it is not a prerequisite for simulated Bombay identity.

## Evidence limits

The broader stack research and failure properties are in
[failure contracts](failure-contracts.md). The Mnesis findings below refine the
initial integration inventory; they are not a new claim of completed Bombay
durability.

### Mnesis 0.3.1: confirmed contracts and remaining limits

The exact registry source records VCS revision
`c71d9ec6edc05d19fd2194f9d7cee292117b0505`. Its pure aggregate kernel, store
contracts and selected sibling integration were inspected. The Fjall and
Postgres adapter sources were additionally read at that revision; neither is
thereby selected as Bombay's production backend.

| Surface | Source finding | Integration consequence |
| --- | --- | --- |
| RawEventStore::append | Requires atomic expected-version comparison and event insertion with sequential event versions. | Existing optimistic concurrency is useful, but does not name a node or hosting grant. An obsolete owner that reloads the latest version can still be a valid version writer unless authority is separately enforced. |
| CommandRepository::execute | Decides and saves events; Ignored performs no append and returns no durable position. | Accepted no-op is not a durably recorded command outcome; the product must select whether no-op/rejection deduplication is required. |
| mnesis-bombay execute_command | command_id is carried into AmbiguousCompletion; the generic execution path does not itself write a command-ID/result table. | Do not claim duplicate-command recognition solely because the function accepts a command ID. |
| Conflict replay | Reloads a fresh aggregate for confirmed conflicts only; ambiguous append is returned separately. | Retain this existing behavior and add integration evidence; do not make uncertain commits transparently retryable. |
| PhaseTracker | Retains in-process observed command phase across future cancellation/panic. | It is not a process-crash-persistent execution journal. |
| Snapshotting | Snapshot saves are best effort; a successful event append remains successful if snapshot save fails. | Event history remains the recovery authority; failed snapshot creation must not roll back a successful command in the runtime's report. |
| Projection state | Existing projection/state contracts pair state with its checkpoint. | Reuse this atomic checkpoint contract; do not invent an independent offset update that can get ahead of state. |
| Subscription wake | Wake signals lead consumers back to stored events. | A missed wake is not loss of committed history; recovery must still catch up from the persisted position. |
| Fjall adapter | Contains transactional event append, projection state persistence and AtomicAppend for multiple stream runs. | Cross-stream atomic primitives already exist here; any higher-level command deduplication or hosting fencing needs a concrete composition and proof. |
| Postgres adapter | Event append uses a SQL transaction; notifications happen after commit and are best effort. | Notification failure cannot retroactively make a committed append uncommitted. Database durability and failover configuration still require verification. |
| In-memory adapter | Useful test storage, selected by existing integration tests. | No process-crash durability claim follows from these tests. |

Pinned primary sources:
[store append contract](https://github.com/devrandom-labs/mnesis/blob/c71d9ec6edc05d19fd2194f9d7cee292117b0505/crates/store/src/store.rs),
[command execution](https://github.com/devrandom-labs/mnesis/blob/c71d9ec6edc05d19fd2194f9d7cee292117b0505/crates/store/src/execute.rs),
[snapshot policy](https://github.com/devrandom-labs/mnesis/blob/c71d9ec6edc05d19fd2194f9d7cee292117b0505/crates/store/src/snapshot.rs),
[projection persistence](https://github.com/devrandom-labs/mnesis/blob/c71d9ec6edc05d19fd2194f9d7cee292117b0505/crates/store/src/projection.rs),
[Fjall](https://github.com/devrandom-labs/mnesis/blob/c71d9ec6edc05d19fd2194f9d7cee292117b0505/adapters/fjall/src/store.rs),
[Postgres](https://github.com/devrandom-labs/mnesis/blob/c71d9ec6edc05d19fd2194f9d7cee292117b0505/adapters/postgres/src/store.rs).

Backend fsync, replication and disaster-recovery guarantees have not been
certified by inspecting an append function. Those remain DUR-09/FAIL-09 gates,
requiring the selected backend configuration and failure experiments.

- No completion percentage is warranted by counting templates, requests,
  source files or unchecked backlog rows.
- V rows identify an evidence obligation, not a proven defect.
- M rows refer to the inspected standard runtime path, not a claim that no
  research probe or external library has ever implemented the idea.
- X rows require verification in their owning repository before assigning an
  implementation status there.
- D and O rows are proposed product choices, not broken promises in the
  existing API.
- No Rust source changed during this inventory task, and no fresh full
  workspace gate was run for these Markdown additions.

## Documentation reconciliation

This task corrected current-facing claims while retaining historical evidence:

| Document | Disposition / correction |
| --- | --- |
| Root README | Removed the completed-framework claim; linked current evidence, missing integration and proposed failure guarantees. |
| examples/README.md | Removed claims that supervisor/pool binaries demonstrate live replacement and job completion; identified pure initialization scope. |
| user-facing-api.md | Marked target versus current implementation, corrected selected template names, distinguished logical/exact recipients, removed unsupported generic atomic-interpretation claim and assertion-owned operations in the shutdown illustration. |
| module-boundaries.md | Corrected obsolete supervisor/pool names and replaced alternate-production-transport scope with the Zenoh-only direction. |
| runtime-capability-interfaces.md | Updated planned networking scope and explicitly marked activation order as an unresolved target law, not completed behavior. |
| runtime-completion-design.md | Pruned the duplicate long-form proposal into navigation; removed competing proposed fluent APIs and obsolete transport interchangeability requirement. |
| driver-law.md | Retained normative semantics; a required law is not proof of current implementation. ACT documents the known activation gap. |
| driver-test-strategy.md | Retained verification requirements and clarified historical audit references; the existing executable template inventory is not described as still awaiting inventory. |

This is a source-backed correction of identified contradictions, not a claim
that every legacy audit checkbox or ignored documentation snippet has been
revalidated. Future documentation changes must label implementation, target
contract, proposal and historical record separately. Advertised support needs
a selected-version executable witness; completion counts must not be inferred
from catalogue size.

## Template inventory

The following appendix is generated from the current 45-entry
[manifest](../driver-template-manifest.json). Upstream paths are relative to the
exact selected Behavior Actors crate. They locate policy evidence; they are
not assertions that every public composition already executes through Bombay.

The five atomic rows below reflect the selected 0.20.0 interpretation update:
`Live witness` names an executed Bombay integration, while `Verify` means an
owner policy or broader runtime composition still needs evidence. The
historical missing-interpreter classification no longer applies.

| Template | Current evidence level | Selected owner source / policy tests |
| --- | --- | --- |
| `DynamicSupervisor`, `dynamic` | Verify: typed interpretation exists; full live policy witness remains open | `src/atomic/dynamic_supervisor/mod.rs`; `tests/dynamic.rs`, `tests/atomic_request_product.rs` |
| `FifoPool`, `fifo` | Live witness: submission, recovery, interruption, and shutdown exercised | `src/atomic/fifo_pool/mod.rs`; `tests/fifo_pool.rs`, `tests/fifo_pool/retirement.rs` |
| `FixedSupervisor`, `fixed` | Live witness: worker replacement and exact retirement exercised | `src/atomic/fixed_supervisor/mod.rs`; `tests/fixed_supervisor_initialization.rs`, `tests/fixed_supervisor_protocol.rs` |
| `KeyedPool`, `keyed` | Verify: typed interpretation exists; full live policy witness remains open | `src/atomic/keyed_pool/mod.rs`; `tests/keyed_pool.rs`, `tests/keyed_pool/lifecycle.rs` |
| `StableProxy` | Live witness: fixed-supervisor replacement exercises its child proxy | `src/atomic/stable_proxy/mod.rs`; `tests/proxy.rs`, `tests/stable_proxy_shutdown_model.rs` |
| `Machine` | Live witness located; additional compositions still require verification | `src/machine.rs`; `src/machine.rs#[cfg(test)]` |
| `MessageAdapter`, `MessageAdapterWithRoute` | Verify: owner policy exists; no universal runtime-completion claim | `src/composition/message_adapter.rs`; `src/composition/message_adapter.rs#[cfg(test)]` |
| `Stash` | Verify: owner policy exists; no universal runtime-completion claim | `src/stash.rs`; `src/stash.rs#[cfg(test)]` |
| `Presence` | Verify: owner policy exists; no universal runtime-completion claim | `src/discovery/presence.rs`; `src/discovery/presence.rs#[cfg(test)]` |
| `PubSub` | Verify: owner policy exists; no universal runtime-completion claim | `src/discovery/pub_sub.rs`; `src/discovery/pub_sub.rs#[cfg(test)]` |
| `Registry` | Verify: owner policy exists; no universal runtime-completion claim | `src/discovery/registry.rs`; `src/discovery/registry.rs#[cfg(test)]` |
| `Resolver` | Verify: owner policy exists; no universal runtime-completion claim | `src/discovery/resolver.rs`; `src/discovery/resolver.rs#[cfg(test)]` |
| `Topic` | Verify: owner policy exists; no universal runtime-completion claim | `src/discovery/topic.rs`; `src/discovery/topic.rs#[cfg(test)]` |
| `shutdown_after_children`, `ChildShutdownPhases::finish`, `ChildShutdownPlan` | Verify: owner policy exists; no universal runtime-completion claim | `src/lifecycle/child_shutdown.rs`; `src/lifecycle/child_shutdown.rs#[cfg(test)]`, `tests/child_shutdown_interpretation.rs` |
| `FinalizeOnShutdown` | Verify: owner policy exists; no universal runtime-completion claim | `src/shutdown.rs`; `src/shutdown.rs#[cfg(test)]` |
| `HeterogeneousShutdownCoordinator` | Verify: owner policy exists; no universal runtime-completion claim | `src/lifecycle/shutdown_coordinator.rs`; `src/lifecycle/shutdown_coordinator.rs#[cfg(test)]`, `tests/exact_shutdown_action.rs` |
| `PropagateTermination` | Verify: owner policy exists; no universal runtime-completion claim | `src/lifecycle/termination_propagation.rs`; `src/lifecycle/termination_propagation.rs#[cfg(test)]` |
| `ShutdownCoordinator` | Verify: owner policy exists; no universal runtime-completion claim | `src/lifecycle/shutdown_coordinator.rs`; `src/lifecycle/shutdown_coordinator.rs#[cfg(test)]`, `tests/exact_shutdown_action.rs` |
| `StopOnShutdown` | Verify: owner policy exists; no universal runtime-completion claim | `src/shutdown.rs`; `src/shutdown.rs#[cfg(test)]` |
| `Task` | Verify: owner policy exists; no universal runtime-completion claim | `src/lifecycle/task.rs`; `src/lifecycle/task.rs#[cfg(test)]` |
| `TerminationMonitor`, `TerminationMonitorWith` | Verify: owner policy exists; no universal runtime-completion claim | `src/lifecycle/termination_monitor.rs`; `src/lifecycle/termination_monitor.rs#[cfg(test)]`, `tests/established_capabilities.rs` |
| `Watch` | Verify: owner policy exists; no universal runtime-completion claim | `src/watch.rs`; `src/watch.rs#[cfg(test)]` |
| `Configuration` | Verify: owner policy exists; no universal runtime-completion claim | `src/operations/configuration.rs`; `src/operations/configuration.rs#[cfg(test)]` |
| `Health` | Verify: owner policy exists; no universal runtime-completion claim | `src/operations/health.rs`; `src/operations/health.rs#[cfg(test)]` |
| `Readiness` | Verify: owner policy exists; no universal runtime-completion claim | `src/operations/readiness.rs`; `src/operations/readiness.rs#[cfg(test)]` |
| `Cache` | Live witness located; additional compositions still require verification | `src/persistence/cache.rs`; `src/persistence/cache.rs#[cfg(test)]` |
| `Acknowledgements` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/acknowledgements.rs`; `src/routing/acknowledgements.rs#[cfg(test)]` |
| `Buffer` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/buffer.rs`; `src/routing/buffer.rs#[cfg(test)]` |
| `CircuitBreaker` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/circuit_breaker.rs`; `src/routing/circuit_breaker.rs#[cfg(test)]` |
| `Correlator` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/correlator.rs`; `src/routing/correlator.rs#[cfg(test)]` |
| `Deduplicator` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/deduplicator.rs`; `src/routing/deduplicator.rs#[cfg(test)]` |
| `OrderGate` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/order_gate.rs`; `src/routing/order_gate.rs#[cfg(test)]` |
| `PriorityQueue` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/priority_queue.rs`; `src/routing/priority_queue.rs#[cfg(test)]` |
| `RateLimiter` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/rate_limiter.rs`; `src/routing/rate_limiter.rs#[cfg(test)]` |
| `Router` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/router.rs`; `src/routing/router.rs#[cfg(test)]` |
| `Sequencer` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/sequencer.rs`; `src/routing/sequencer.rs#[cfg(test)]` |
| `WorkQueue` | Verify: owner policy exists; no universal runtime-completion claim | `src/routing/work_queue.rs`; `src/routing/work_queue.rs#[cfg(test)]` |
| `Deadline` | Live witness located; additional compositions still require verification | `src/time/deadline.rs`; `src/time/deadline.rs#[cfg(test)]`, `tests/timer_action_settlement.rs` |
| `Lease` | Verify: owner policy exists; no universal runtime-completion claim | `src/time/lease.rs`; `src/time/lease.rs#[cfg(test)]` |
| `OneShot` | Live witness located; additional compositions still require verification | `src/time/one_shot.rs`; `src/time/one_shot.rs#[cfg(test)]`, `tests/timer_action_settlement.rs` |
| `Periodic` | Live witness located; additional compositions still require verification | `src/time/periodic.rs`; `src/time/periodic.rs#[cfg(test)]`, `tests/timer_action_settlement.rs` |
| `ReceiveTimeout` | Live witness located; additional compositions still require verification | `src/time/receive_timeout.rs`; `src/time/receive_timeout.rs#[cfg(test)]`, `tests/timer_action_settlement.rs` |
| `Barrier` | Live witness located; additional compositions still require verification | `src/workflow/barrier.rs`; `src/workflow/barrier.rs#[cfg(test)]` |
| `Latch` | Live witness located; additional compositions still require verification | `src/workflow/latch.rs`; `src/workflow/latch.rs#[cfg(test)]` |
| `Workflow` | Verify: owner policy exists; no universal runtime-completion claim | `src/workflow/coordinator.rs`; `src/workflow/coordinator.rs#[cfg(test)]` |

## Documentation task change ledger

Task scope: eight new PRD/research documents and corrections in nine existing
Markdown documents. Production implementation and executable verification were
not part of this documentation change. Existing unrelated edits are retained.

Task-owned change:

```text
production: +0 / -0 / net 0
tests:      +0 / -0 / net 0
public API: +0 types / -0 types
```

Complete working-tree snapshot, including pre-existing tracked and untracked
changes (file-based source/test categories; embedded tests remain in source):

```text
production:   +3307 / -4079 / net -772
tests:        +3763 / -2273 / net +1490
documentation: +8671 / -937 / net 7734
other:        +1062 / -985 / net +77
```

The inherited public API was not recounted or certified by this docs task.
Validation: `git diff --check`; local Markdown link-target checks; uniqueness
and counts of requirement, property and scenario IDs; manifest template count;
and hashes confirming no inherited non-documentation file changed. All passed.
No Cargo, Rust, backend-crash or distributed test campaign was run for this
Markdown-only change. Existing historical test reports remain historical.
