# EXEC: application execution and local actor ownership

Date: 2026-09-29. Owner: Bombay. Backlog ID: `EXEC1`.

**Revision note (2026-10-01):** The ARC-001 shutdown authority change has
replaced the erased `ActorRef` field with a typed weak application lifecycle
projection and exact child/Entity control senders. The source inventory below
records the earlier representation; current API and verification are in the
backlog status index and runtime capability document. ARC-002 also removed the
unobservable private external Address claim and its impossible error branch;
the dated source inventory below predates that change.
ARC-012 later replaced the shared optional activation publisher and terminal
report selection with affine handoffs, and gave owner cancellation its own
residual variant. Its focused proof is in the backlog status index; the dated source
inventory below also predates that change.
ARC-011 subsequently added cancellation authority for dropped startup/join
waiters, moved activation-task settlement into the spawned actor task, and
shared root/application and root/owned launch setup. The dated task-custody
and projection descriptions below are research snapshots, not current source
claims; the ARC-011 retained-evidence entry and current runtime capability document
record the selected representation.
ARC-020 later renamed the private actor execution and outcome symbols; the
snapshot names below remain historical, and `docs/module-boundaries.md` records
the current source paths.

**Status: selected for execution (2026-10-02); current contract verification
and decision gates are the first work stage. Production implementation is gated
by the decision records below.** This is
not a claim that the proposed Rust API compiles or that the runtime satisfies
the required cancellation laws today.

This PRD replaces the earlier conversational criterion that EXEC must produce
a net reduction in production lines. Model quality is the acceptance criterion:
one semantic owner, truthful names, coherent hierarchy, narrow interfaces,
explicit ownership transfer, and no duplicate implementation of the same law.
Line, file, type, allocation, and task counts are diagnostics and review limits.
They cannot justify combining distinct responsibilities or keeping a wrapper
with no independent purpose.

## 1. Authority and how to execute this document

Read Bombay's [AGENTS.md](../../AGENTS.md), resolve the current lock and patches,
and read the complete `AGENTS.md` at the selected Behavior release revision.
Then read the [backlog status](../prd-backlog/status.md),
[capability contract](../runtime-capability-interfaces.md),
[module map](../module-boundaries.md), [Driver law](../driver-law.md), and
[Driver verification contract](../driver-test-strategy.md).

This PRD specifies required outcomes and the boundaries of permitted work. It
does not authorize an agent to replace an unavailable owning contract with an
invented abstraction. Current implementation is evidence, not architectural
authority. A sibling repository's unselected HEAD is not the build contract.

Terms used here:

- **Required**: an acceptance condition. An implementation cannot omit it.
- **Preserved**: existing observable behavior that must remain unless a named
  decision explicitly changes it and updates its callers and evidence.
- **Decision gate**: a question with bounded experiments and required evidence.
  Dependent production work is blocked until the coordinating design reviewer
  accepts and records one concrete answer.
- **Candidate**: a hypothesis to test, not an instruction to add production code.
- **Deferred**: excluded from this PRD; no completion credit is claimed for it.

Never interpret missing detail as permission. Record the exact unresolved
ownership equation, affected requirement, and smallest falsifying example;
return that issue to the coordinator. Independent work may continue. Do not
resolve the uncertainty through a default, erased callback, additional generic
parameter, broad error, silent discard, or compatibility implementation.

All `XO-*`, `DG-*`, `EV-*`, and `WP-*` labels are documentation identifiers.
They must not appear in production/test type names, module names, filenames,
feature names, or test function names. Source names use domain language.

## 2. User problem and release outcomes

An application author must be able to execute one Bombay application inside
an existing Tokio runtime without nesting a runtime or blocking its worker.
An author who wants Bombay to own execution must have a small convenience
surface that drives the same application future. Current-thread and multithread
execution must preserve the same actor, effect, shutdown, and terminal laws.

Maintainers must be able to locate application orchestration, actor task
ownership, endpoint authority, ingress, observation, child custody, and effect
interpretation without tracing one concern across unrelated top-level files.
An Entity host must execute the same local actor composition without depending
on application-runner internals.

The following are required executable user stories:

1. An async Tokio application starts Bombay, obtains its typed application
   interface, exchanges a command/reply, requests root shutdown, and awaits the
   exact application terminal result without owning or shutting down Tokio.
2. A synchronous caller executes the equivalent application through the owned
   executor convenience and receives the same typed result.
3. Two independent actors run on multiple Tokio workers; each actor remains
   serialized. No new actor trait, scheduler, or parallel Behavior fold exists.
4. An HTTP application binds first, starts Bombay, exposes the typed interface
   to its router, and coordinates HTTP/root shutdown through one application
   execution path.
5. An application with Entity families retires the root and completes the
   required family shutdown/join path, including ordinary root-startup failure.
6. Cancellation at every asynchronous ownership transfer has a specified
   outcome. A surviving runtime cannot contain silently abandoned child or
   capability tasks as a consequence of dropping an application future.

These are desired contracts, not new guarantees attributed to Agha or Tokio.
They are deliberate Bombay execution policies over the existing Behavior law.

## 3. Scope and dependency boundary

| Inventory | Coverage in this PRD |
| --- | --- |
| EXEC-01, EXEC-02, EXEC-03 | Executor ownership, mode selection, canonical async application execution. |
| EXEC-04, EXEC-06, EXEC-08 | Serialization, cancellation, panic, retirement, joining, and exact terminal ownership. |
| EXEC-05 | Explicit cooperative limits and a bounded external-work witness; no new general work-pool service. |
| EXEC-07 | Standard execution remains `Send`; a `!Send` host is explicitly deferred. |
| EXEC-09 | Public examples and an independent-actor concurrency benchmark. |
| ACT-08 | Static shutdown authority; ownership redesign is gated by DG-SHUTDOWN. |
| APP-04, APP-05 | Preserve runner startup/error visibility and ingress closure laws. |
| APP-06 | Distinguish requested shutdown, owner cancellation, and executor destruction. A configurable universal grace deadline remains deferred. |
| APP-07, APP-08 | Update affected authoring/diagnostic/documentation surfaces; no unrelated template support claim. |

Not included: integrating the now implemented six atomic interpreters into
executable supervisor and pool policies, repairing
Behavior's source-action rejection/diagnostic contracts, choosing distributed
identity or transport, Mnesis integration, a new mailbox-capacity API, ordinary
Entity API expansion, supervision/restart policy, or replacing the Driver.
See [core integration](../prd-backlog/core-integration.md) for those dependencies.

ARC-006 has resolved the activation publication defects under the selected
Address 0.3.0 contract. This PRD must preserve its executable visibility and
failed-commit regressions; moving code supplies no additional activation proof.
If a selected EXEC solution requires a new Address reservation or Behavior
settlement contract, record that specific dependent work as blocked. Do not
turn all independent EXEC research into an upstream wait.

### 3.1 Audited build contract

| Owner | Original selected source (historical baseline) | Required inspection |
| --- | --- | --- |
| Behavior | `bombay-behavior 0.17.0`, `435560ce7bea8ad3330ee2d42e5034f837a80602` | Actions, ordered interpretation, source custody, births, established capability ports; complete revision AGENTS. |
| Behavior Actors | `0.17.0`, same revision | Observation, shutdown, terminal reporting, preparation protocols and their tests; preserve template policy. |
| Behavior Macros | `0.12.0`, same revision | Existing generated protocol and concrete effect types; no new EXEC macro. |
| Address | `0.2.0`, root lock checksum | Claim visibility, resolve, exact lease retirement, concurrent tests. |
| Communication | `0.1.2`, root lock checksum | Control/user lanes, admission owner, close/send race, exact rejected payloads, drain and receiver drop. |
| Observe | Bombay-private source | Shared/affine publication, waiting, cancellation and drop tests. |
| Timers | `0.1.0`, patch `13e884da7ab41781f52337b0038060e375b00ee0` | Queue ownership, generation safety, deadlines, sequence and overflow tests. |
| Tokio | `1.53.1` in `Cargo.lock` | Runtime ownership, spawning, handle drop/abort, joining, scheduler assumptions and blocking-work limits. |

Recheck this table at every integration checkpoint. A lock change invalidates
affected evidence until the relevant source, tests, instructions and contracts
are reverified. Do not automatically upgrade a dependency while implementing
this PRD. The complete imported Observe primitive and the primitive libraries
are not targets for cosmetic reorganization.

## 4. Original source inventory and its limits

The table below is the dated 0.17.0 inventory. Section 15 records execution
against the current selected contracts. The PRD and its required outcomes
remain current; resolved observations below are preservation obligations.

Line numbers drift; the named symbols, owning paths and lock identify the
evidence. The pre-edit manifest required in section 12 adds content hashes.

| Finding | Source anchor | Classification and required response |
| --- | --- | --- |
| Seven owned current-thread runtime constructions | `application_runtime.rs`: `App::{run, run_with, run_axum, run_with_entities}` and `Application::{run, run_with, run_axum}` | Confirmed duplication. One owned-executor construction authority and one async application execution implementation. |
| Internal `launch` already calls `launch_with` with unit work | `LaunchSystem::launch` | Confirmed existing reuse. Do not claim there are separate actor loops here or add another forwarding object. |
| HTTP repeats root capability construction, activation and joining | `LaunchSystem::launch_axum` | Confirmed duplication. Preserve HTTP-specific coordination while consuming common application execution. |
| Root and owned-child spawn bodies repeat activation/termination/environment setup | `launch.rs`: `spawn_root_with`, `spawn_owned_with_mode` | Confirmed duplication. Preserve the distinct returned control authority while sharing actor startup. |
| Application runner file implements every actor's runtime capabilities | `ApplicationCapabilities`, `EstablishChild`, `InterpretItem`, `RetireCapabilities` | Confirmed responsibility mismatch. Move actor execution ownership below application orchestration. |
| Entity imports application-runner composition | `entity/bombay.rs`: imports and native host implementation | Confirmed wrong placement of a shared responsibility. Entity and applications must depend on the same local actor owner. |
| Actor reference erases its shutdown sender | `local.rs`: `ShutdownControl`, `TypedShutdownControl`, `ActorRef.shutdown`, `shutdown_liveness` | Confirmed static-dispatch violation in this representation. DG-SHUTDOWN is mandatory. |
| Dropped owner can detach execution | `OwnedTask::finish`, spawn activation waits, `ActiveLocalEnvironment::next` closed-cancellation branch | Source-derived cancellation gap, not a newly executed regression. DG-TASK must reproduce and repair the exact ownership failure. |
| Cancellation is polled differently by ordinary and source acquisition | `ActiveLocalEnvironment::{next,next_source}` | Confirmed implementation distinction. Test both; do not assume fixing `next` covers source settlement or effect waits. |
| Projection spawns another task | `ProjectedTask::project` | Confirmed task, not proof it is unnecessary. DG-PROJECTION must establish cleanup timing and panic semantics before deleting it. |
| Observation uses two mechanisms | `observation.rs::FactQueue`; `EstablishedObservationInterpreter` and `exact_observations` in `application_runtime.rs` | Confirmed split scheduling/cancellation ownership. DG-OBSERVATION selects one actor-owned relationship authority. |
| Old exact observer may remove a reused ID | exact observation task removes `id`; cancellation removes and permits reuse; selection can race | Source-derived race candidate. Build a deterministic regression before calling it a reproduced defect. |
| Exact observer task can run before `started` is admitted | observation starts a task before calling `return_fact(started)` | Source-derived ordering risk on multiple workers. Test immediate completion against the required start/terminal order. |
| Runtime vocabulary has synonymous aliases and forwarding products | `OccurrenceBindings`, `LocalAddresses`, `LocalTerminalReports`, `FactState`, `HostedActorSpaces` | Review candidates, not automatic deletion authority. Apply the disposition table in section 7. |
| Documentation diverges from observation implementation | capability document's exact-observation description | Confirmed discrepancy. Final guidance must name the implemented authority and scheduling path. |
| Panic documentation says retirement completes during unwind | `lib.rs` panic strategy | Wording requires correction: synchronous drop/publication does not prove asynchronous joining. |

The preceding audit ran:

```sh
nix develop -c cargo test --locked -p bombay-rs --features axum \
  --test run_with --test axum --test application_terminal_custody \
  -- --skip compile_checked
```

Result: 15 runtime tests passed; two compile-check tests were filtered out.
This establishes a preservation baseline only. It does not prove the proposed
API, cancellation fixes, observation race, multithread behavior, or full gates.
No production code was changed by that audit.

## 5. Required ownership and dependency direction

| Responsibility | Sole semantic owner | Required boundary |
| --- | --- | --- |
| Pure actor policy and state transitions | Behavior / Behavior Actors | Runtime interprets their exact typed requests; no template-specific Driver branch. |
| Causal fold/interpret/source-custody sequence | Engine | No Tokio, mailbox, HTTP, Entity, observation registry, or child policy. |
| Application declaration and semantic child roles | Application composition | Pure values; no runtime handles or live endpoint allocation while authoring. |
| Application lifetime and executor ownership | Application execution | Installs application resources, executes typed application work, awaits required retirement, then returns. |
| One running actor task and cancellation/join custody | Local actor execution | Exactly one accountable owner at every handoff; cannot silently detach on caller cancellation. |
| Prepared/active resource composition | Local Environment | Uses Address, Communication, Observe, Timers; supplies Engine's existing port. |
| User ingress and exact endpoint capabilities | Local endpoint/ingress | Preserve protocol identity and weak admission; an endpoint cannot keep an actor alive. |
| Child creation status, endpoint/control/task ownership | Existing closed child bindings | One authoritative product; no second map claiming a child is established. |
| Concrete action interpretation | Local actor effects | One static capability composition; existing owning leaf traits, no service registry. |
| Observation relationship state | Local observation interpretation | One authority for registration, completion, cancellation and ID reuse. Observe still owns publication/waiting primitives. |
| Timer scheduling state | Existing TimerQueue | Exactly one queue per actor; no new timer task, channel, or generation counter. |
| Exact terminal custody and external publication | Local termination | Preserve typed retirement separately from its documented coarse external observation. |
| Stable Entity admission/hydration/passivation | Existing Entity owner | Reuse local actor execution; retain its separate family task/group semantics. |
| HTTP serve/bind outcomes | Application HTTP integration | No direct Environment construction, actor spawn, or duplicate terminal projection. |

Required conceptual dependencies:

```text
Application declaration -> Application execution -> Local actor execution
Entity hosting ----------------------------------> Local actor execution
HTTP coordination ------> Application execution
Local actor execution --> Local Environment + concrete effect interpretation
Local Environment ------> Engine port + Address/Communication/Observe/Timers
Engine -----------------> Behavior
```

This describes responsibility direction, not a demand for a trait per arrow.
Local actor implementation must not import application runners or Axum. Shared
address allocation may remain in the address domain and be passed concretely.
Terminal projections may be supplied through the existing static contract;
they must not require the local owner to inspect an application topology.

## 6. Source hierarchy and migration map

The following is the target ownership layout to validate at DG-MODULES. It is
not permission to pre-create empty modules or a type for each filename.

```text
crates/bombay/src/
  application/
    mod.rs                declarations and curated application exports
    composition.rs        declared child product and role projections
    execution.rs          application lifetime and owned/caller executor entry
    interface.rs          application handle and external capability surface
    http.rs               optional Axum coordination and errors
  local/
    mod.rs                local composition exports; no second implementation
    endpoint.rs           ActorRef and exact endpoint capabilities
    ingress.rs            admission, user/control acquisition, Entity fences
    environment.rs        prepared/active Environment and resource residuals
    execution.rs          actor startup, task custody, cancellation and joining
    children.rs           closed creation bindings and descendant custody
    termination.rs        outcomes, terminal projection contract and publication
    effects/
      mod.rs              concrete actor capabilities and complete interpretation
      creation.rs         existing typed birth interpretation
      delivery.rs         logical/exact/child/source delivery interpretation
      observation.rs      observation relationships and their interpretation
      timers.rs           existing queue adaptation and timer interpretation
      reports.rs          parent and terminal report interpretation
  address.rs              local address domain and application allocation
  entity/                 existing Entity aggregate
  actors/                 existing Behavior Actors authoring composition
  observe/                existing private primitive, unchanged by rearrangement
```

| Existing source | Destination responsibility |
| --- | --- |
| `application.rs` | `application/mod.rs` declaration ownership. |
| `application_runtime.rs`: App, runners, RunError | Application declaration/execution; error split decided by DG-API. |
| `application_runtime.rs`: staging, ApplicationBehavior, origin products | `application/composition.rs`; preserve role and initialization laws. |
| `application_runtime.rs`: handle/lifecycle; `actor_interface.rs` | `application/interface.rs`; retain separately justified capability types. |
| `application_runtime.rs`: Axum | `application/http.rs`; call common execution. |
| `application_runtime.rs`: capabilities and interpreter implementations | `local/effects/`; rename scope to actor ownership after DG-MODULES. |
| `local.rs`: ActorRef, endpoint representation, SendError | `local/endpoint.rs`; ingress construction details stay private to their owner. |
| `local.rs`: admission, ingress modes, inbox | `local/ingress.rs`; preserve affine drain and fence laws. |
| `local.rs`: Environment states and residual | `local/environment.rs`; keep consuming phase relation together. |
| `local.rs`: capability tasks; `launch.rs`: task owners | DG-TASK chooses one coherent placement under local execution/effects; no mechanical split by current file. |
| `launch.rs`, `incarnation.rs`, `retirement.rs` | Local execution and termination according to exact owned authority; do not copy the Driver. |
| `outcome.rs`, `terminal.rs`, `termination.rs` | `local/termination.rs`; preserve distinct exact outcome and coarse observation contracts. |
| `child_bindings.rs` | `local/children.rs`; retain one closed product. |
| `observation.rs`, `time.rs`, `reports.rs` | Matching `local/effects` concern; retain owning primitive semantics. |
| `topology.rs` | Concrete host/resolution proof belongs with local composition; DG-WRAPPERS decides forwarding products. |
| `worker_preparation.rs` | Keep the public source contract stable; private interpretation belongs with its proven local effect owner. No new pool policy. |

Rules for accepting the layout:

1. Use `mod.rs` at aggregate roots, with narrow child visibility. Do not retain
   both `application.rs` and `application/mod.rs`, or both forms for `local`.
2. Root modules curate ownership and exports. They must not become replacement
   2,500-line implementation files or re-export every private symbol.
3. Public paths such as `bombay::ActorRef` remain intentionally curated. A
   directory move alone does not authorize a public API break.
4. Use `pub(super)` or a specific ancestor visibility where sufficient. Record
   every necessary `pub(crate)` crossing and its real consumer.
5. Parent paths supply qualification. `local/effects/observation.rs` is enough;
   do not repeat the entire path in a filename or create one file per verb.
6. A child module must hide a meaningful design decision. Combine a proposed
   child with its parent if it contains only forwarding or an alias. Conversely,
   do not combine distinct authorities to meet a file/line target.
7. This tree is a bounded proposal. DG-MODULES must attach a symbol-to-owner map
   before migration. A worker may not independently invent a different tree.
8. Known terminology in retained public contracts is not renamed incidentally.
   In particular, do not rename every existing incarnation/retirement symbol
   because upstream and Bombay instructions use different domain qualifiers;
   resolve any actual instruction conflict in the recorded naming review.

## 7. Existing abstraction disposition

| Construct | Required treatment |
| --- | --- |
| Driver and Environment/ActiveEnvironment | Retain the owning Engine contract. Changes require a separately demonstrated Engine law defect. |
| ApplicationBehavior | Retain its real initialization/birth composition semantics. It is not a pass-through wrapper. Test declared and dynamically created children together. |
| ApplicationLifecycle | Retain restricted lifecycle authority unless a replacement proves the same static denial. Small field count is not grounds for deletion. |
| ActorInterface / ExternalActor | Preserve external origin, admission, affine receive ownership and separation from lifecycle authority. |
| Root terminal projections and ActorOrigin | Preserve semantic role, concrete Behavior/error, and descendant custody. Do not replace with strings or one erased outcome. |
| ActionInterpreter | Its ordered complete interpretation and action-scoped report disposition are real responsibilities. DG-WRAPPERS may relocate or integrate it only with differential evidence. |
| ChildBindings / OccurrenceBindings | Choose one canonical spelling for the identical product; remove the synonym and all stale references. Preserve occurrence distinctions in the actual type. |
| ActorSpace / LocalAddresses | Choose the existing canonical ActorSpace spelling for this identical address-space alias unless the scope review proves a distinct public contract. Do not add a third name. |
| LocalTerminalReports | Try direct use of the existing report-selection owner. Retain a separate value only if its restricted authority is real and tested, not merely forwarding methods. |
| HostedActorSpaces / ResolveLogical | Compare direct existing Hosts composition with the current adapter. Delete the adapter only if logical resolution's selected policy and static denials are preserved. |
| FactState / FactQueue | Rename to observation domain language. Remove storage-only wrapping if it owns no independent invariant; DG-OBSERVATION first resolves relationship ownership. |
| LocalTimers | Keep one TimerQueue. Compare direct borrowing from its actor owner against current serialized shared views. Do not replace its mutex with unsafe or a dynamic context merely to reduce allocation. |
| OwnedTask / ProjectedTask / ActivationTasks | DG-TASK and DG-PROJECTION must specify authority, cleanup timing and failure custody before deciding representation or task count. |
| Entity task group | Preserve its different family admission/shutdown-claim law. Similar JoinHandle storage does not justify a universal task-group trait. |

Every retained/new abstraction needs five answers in its decision record:
unique state/authority, unique transformation, why the existing composition
cannot express it, machinery removed or subsumed, and a concrete consumer.
An alias may abbreviate a truthful unwieldy type; it may not create competing
names for the same domain value without a documented distinction.

## 8. Normative execution requirements

### 8.1 Application and executor

| ID | Required contract |
| --- | --- |
| XO-01 | There is one implementation of application execution. Simple execution, application work, HTTP integration, and Entity-family execution compose it without duplicating actor startup. |
| XO-02 | Caller-owned execution creates no Tokio runtime and never calls block_on. It does not stop or reconfigure the caller's runtime. |
| XO-03 | Owned execution constructs its runtime in one semantic location and drives the same application future. Construction failure occurs before actor activation. |
| XO-04 | Current-thread and multithread modes are explicit. Thread-count validation and defaults are documented at DG-API; no global executor or implicit fallback. |
| XO-05 | The ordinary host requires Send actors/events/effects as justified by current owning interfaces. No LocalSet, spawn_local, unsafe Send, or hidden !Send mode in this change. |
| XO-06 | Application work is invoked exactly once after successful root activation; ordinary startup failure invokes it zero times. Preserve the typed handle and callback return value. |
| XO-07 | Preserve current ordinary callback policy: it is awaited, then root completion is joined. Callback completion does not request root shutdown. Root completion does not automatically cancel arbitrary application work. Document that a pending callback can keep this API pending. |
| XO-08 | A callback returning Result carries that Result as its own return value; an Err is not silently reclassified as actor failure. Panic and future drop are different and handled by the task contract. |
| XO-09 | Application declaration/materialization stays pure until execution. Preserve semantic child roles, exact terminal projections and one initialization. |
| XO-10 | Entity families are shut down after root execution returns, including ordinary root-startup Err. Preserve family shutdown order/results. Cancellation or panic cannot simply skip ownership; DG-TASK specifies that path. |
| XO-11 | Family shutdown results remain typed. The current runner discards them when returning a root-startup error; do not promise preservation of independent shutdown failures without specifying the complete error equation at DG-API. |
| XO-12 | Existing defaults such as mailbox capacity remain unchanged here. Do not bundle APP-01 configuration into an executor refactor. |

### 8.2 Actor execution and task custody

The following table specifies protocol distinctions, not a requirement to add
an execution-state enum. Reuse ownership and existing sum/product types first.

| Situation | Required behavior and authority |
| --- | --- |
| Before task spawn | No actor task exists; caller owns all prepared inputs. Dropping them cannot publish a successful activation. |
| Spawned, awaiting activation | Startup owner retains cancellation and task custody. Dropping the activation waiter cannot abandon the spawned task. |
| Active, acquiring input | Owner cancellation can request retirement; ordinary shutdown is still a Behavior policy request. A sender closing is not automatically equivalent to either. |
| Awaiting source input/custody or effect completion | Cancellation law must be explicit at each await; do not assume the ordinary inbox select handles it. Partial accepted effects remain factual. |
| Retirement requested | No new Behavior turn after the owning Driver contract ends execution. Close admission and retain queued payloads under existing Communication laws. |
| Retirement/join underway | Cancelling the waiter cannot discard the only remaining owner of children or cleanup tasks. Do not publish successful joined completion early. |
| Joined and returned | One affine terminal result transfers to its caller. No still-owned child/capability work is hidden behind a successful application return. |
| Executor is destroyed or process aborts | Cannot promise asynchronous cleanup ran. Classify the observable local outcome where execution permits it; do not claim durable effects rolled back or a graceful drain completed. |

| ID | Required contract |
| --- | --- |
| XO-13 | One owning actor execution contains one consuming Driver run. No second actor loop or per-template scheduler. |
| XO-14 | Root, child, and Entity startup use the same local construction law; differences in ingress and returned authority stay static and explicit. |
| XO-15 | At each await, record the owner of the actor JoinHandle, cancellation sender, child tasks, capability tasks, publication authority, and affine terminal payload. |
| XO-16 | Successful awaited application return is a join barrier for the hierarchy/resources it owns. Completion observation and complete joined retirement are distinct events where the current contract distinguishes them. |
| XO-17 | Dropping the caller's future must initiate or transfer cancellation/cleanup through a specifically identified owner while Tokio remains alive. No silent detachment, and no claim that synchronous Drop awaited cleanup. DG-TASK determines the concrete mechanism and the exact disposition of terminal/application values when their original receiver no longer exists; no fictitious return or accidental double drop. |
| XO-18 | Dropping the runtime, a non-yielding Behavior, or permanently pending uncancellable external work limits liveness. State these limits explicitly; do not claim a universal deadline or preemption guarantee. |
| XO-19 | Preserve normal waiting versus owner-forced retirement. Normal finish awaits the existing owner without requesting retirement; abandoned-wait cancellation is a distinct ownership transfer. Preserve that distinction and prove every sender's ownership contract. |
| XO-20 | Panic, controlled Behavior failure, activation failure, settlement failure, owner cancellation, exhaustion, and normal stop remain distinct where their owning typed contracts distinguish them. |
| XO-21 | Child retirement remains in the existing observable order, including occurrence/role distinctions and descendant results. Do not switch to unordered joins solely for speed. |
| XO-22 | Capability-task errors and returned events retain their actual typed source/custody. No log-and-continue, default success, blanket panic, or discarded join result may replace a selected contract. |
| XO-23 | A terminal projection is an ownership-preserving conversion of the exact result and origin. Any additional task must own independently required progress, not merely shorten a generic signature. |

### 8.3 Shutdown authority

| ID | Required contract |
| --- | --- |
| XO-24 | User delivery, shutdown request, and forced retirement are distinct authorities. A clone of a messaging interface must not acquire new lifecycle authority. |
| XO-25 | Replace trait-object shutdown storage with a verified static concrete contract. No dyn, Any, TypeId, unsafe cast, serialized control envelope, or callback that hides the same erased target. |
| XO-26 | A shutdown request targets the captured actor generation. Address reuse must never retarget an old capability. |
| XO-27 | Preserve request acceptance versus eventual shutdown. Existing AlreadyStopping/AlreadyStopped outcomes remain truthful; a request does not promise the Behavior stopped. |
| XO-28 | Preserve admission closure before accepted shutdown can admit further ordinary user work. Test delivery racing closure and the exact rejected payload. |
| XO-29 | A live endpoint cannot keep admission open or keep an actor executing after its owner retires it. An ExternalActor without Behavior shutdown policy must not acquire fictitious actor-control authority. |
| XO-30 | Shutdown established through Behavior's typed ingress stays realized through that owner contract. If the selected contract cannot express a static solution, record a dependency blocker; do not implement a parallel lifecycle channel speculatively. |

### 8.4 Observation relationships

Preserve the difference between uncancellable peer/child observations and the
ID-addressed established observation protocol. Sharing ownership does not
authorize changing their public request/event types or delivery cardinality.

| Condition | Required ID-addressed observation outcome |
| --- | --- |
| Start with free ID | Register the exact target generation; admit Started before any Stopped event for this relationship. |
| Start with live ID | Return the existing typed IdAlreadyBound rejection. Leave the old registration unchanged. |
| Target already terminated | Started followed by one exact Stopped notification, preserving timestamp meaning; no lost observation. |
| Cancellation wins before completion commits | Remove that relationship, emit Cancelled once, and prevent a later Stopped for it. |
| Completion wins before cancellation commits | Commit its one Stopped notification; later cancellation returns NotObserved. Already admitted events are not retracted. |
| Start after the old relationship is removed | Establish a new relationship. Any old pending work must be unable to remove, cancel or notify on behalf of the new registration. |
| Two independent consumers observe one target | Both retain their independently requested terminal notifications. Neither registration cancels the other. |
| Actor retires | Release all registrations, cancel/settle owned waits, and preserve already admitted or returned control values under the retirement contract. |

The completion commit point is the owning relationship authority's irrevocable
decision to admit the completion notification. DG-OBSERVATION must identify its
exact source operation and the failed-admission custody path. Removing a map
entry alone is not proof of a delivered notification.

| ID | Required contract |
| --- | --- |
| XO-31 | One actor-owned authority linearizes ID registration, cancellation, completion and reuse. A second task/map must not independently decide the same relationship's state. |
| XO-32 | Peer/child observation preserves independent consumers, exact generation and existing typed injection. No allocation counter may infer lifecycle provenance. |
| XO-33 | ID reuse safety is proven by adversarial replay. Add an internal generation only if an existing single-owner representation cannot express the required authority; it is not a default implementation requirement. |
| XO-34 | Do not add an observation cell, global registry, task per request, or new channel as an organizational convenience. Reuse Observe and compare the existing actor polling path first. |
| XO-35 | No notification disappears because the control lane closes. Preserve the exact returned event in the selected terminal custody path, or record the upstream contract gap. |
| XO-36 | Preserve documented acquisition order and source priority. State the concrete fairness limits of the retained polling order, including a continuously ready mailbox; do not claim bounded observation/timer progress without a proven bound. A different fairness policy requires an explicit decision amendment, not an incidental select reordering. |

### 8.5 HTTP, timers, external work and public errors

| ID | Required contract |
| --- | --- |
| XO-37 | HTTP bind failure precedes root activation and router construction. Router construction occurs once after activation. |
| XO-38 | Root termination initiates graceful HTTP shutdown. Serving failure requests root shutdown and preserves both the serving error and eventual root terminal. It does not guarantee a Behavior-independent shutdown deadline. |
| XO-39 | HTTP delegates actor startup/joining to application execution; it owns only HTTP resources, coordination and error mapping. |
| XO-40 | One actor-owned TimerQueue retains generation, replacement, due-order and overflow laws. Refactoring sharing must not introduce a second queue or task. |
| XO-41 | Blocking/CPU-heavy external work must not execute inside Behavior init/receive/transition. A bounded typed interpreter witness exercises admission and shutdown; no EXEC-specific generic worker-pool framework. |
| XO-42 | If blocking work cannot be stopped after it starts, the contract must say so. Do not equate cancelling its awaiting future with cancellation of the underlying work. Awaited completion and executor destruction must remain distinguishable. |
| XO-43 | Async errors cannot fabricate a Runtime construction failure. DG-API decides whether to retain an existing broader error type or separate ownership-bearing errors; no erased aggregate error. |
| XO-44 | Root failure and Entity shutdown failure can coexist. DG-API must specify their complete returned product/sum before claiming both are preserved. No use of ? may skip independently required cleanup. |
| XO-45 | Ordinary public API includes no Driver phase controls, untyped capability bag, manual prepare/init/run-loop sequence, or public task owner introduced just to hide internal types. |

## 9. Decision gates: exact experiments and stop conditions

All gates below are **open** in this PRD. The coordinator must record evidence
before changing a gate to accepted. A gate is not accepted because a worker
produced a compiling patch or because another agent assumed its answer.

Every decision record must contain: the law and requirement IDs; competing
representations using current owners; exact candidate application syntax;
compile-pass and compile-fail diagnostics; complete observable traces; ownership
at every await/drop; expected files and production/public-surface delta;
rejected alternatives and reasons; dependency revisions; acceptance command
results; and reviewer disposition. Record resulting control-state alternatives
and which current value each retained alternative owns. No new policy can be
smuggled into a naming or compiler-fix patch.

Create decision evidence only when that work starts, under
`docs/prds/execution-ownership/`: `application-api.md`, `task-custody.md`,
`shutdown-authority.md`, `observation.md`, `terminal-projection.md`,
`abstraction-disposition.md`, `module-ownership.md`, and `external-work.md`,
respectively for the gates below. Record integrated witness commands/results in
`verification.md`. Do not create empty files with a passing status. The PRD is
the requirement authority; these records select implementations within it.

Acceptance is fail-closed: the coordinator and a reviewer who did not author
the candidate both sign the record with the inspected source hash, then update
the gate's status and link here. An unavailable independent reviewer leaves
the gate open. A gate that changes a required outcome needs a visible PRD
amendment explaining the changed law; it cannot silently overrule this document.
Approval of a design record does not waive repository surface checkpoints.

| Gate | Experiments required | Accepted artifact / implementation stop condition |
| --- | --- | --- |
| DG-API | Compare ordinary async inherent methods plus a blocking convenience against an ordinary free function driving the same future. Compare existing Tokio builder input versus a closed Bombay mode value only if a real semantic distinction requires one. Exercise Application, advanced App, Entity families and HTTP with inferred types. | Exact signatures, errors, defaults, nested-runtime behavior, runtime feature selection, migration table and valid public examples. No runtime wrapper/trait chosen in advance. No public runner implementation until accepted. |
| DG-TASK | Reproduce dropped startup, dropped application work, dropped finish, source-wait cancellation and dropped retirement. Compare improving existing task ownership with transferring cleanup to a specifically owned execution task. Enumerate panic, failed spawn, closed cancellation sender and runtime destruction. | Ownership graph and transfer table with no unowned task at any await/drop. Define who can still join and observe cleanup after the application future is gone, without promising synchronous async cleanup. No async public release before acceptance. |
| DG-SHUTDOWN | Compile-only witnesses for ordinary root shutdown, established child shutdown, external actors, reused addresses and two behavior implementations of one protocol. Compare existing concrete capabilities before changing an owning primitive. | Static target/authority representation, invalid-use denials and admission-close trace. If impossible under locked contracts, exact upstream requirement and affected work blocked. No erased fallback. |
| DG-OBSERVATION | Deterministically exercise immediate completion, cancel/completion races and reused IDs. Compare existing actor-owned polling with independent-task design, including returned-event custody. | One owner and linearization point for each relationship operation; prescribed outcome table passes. Any retained task/map has an independent responsibility. No assumed generation token or extra observation framework. |
| DG-PROJECTION | Compare current eager projection task with projecting in the existing actor completion/join path. Use a child that terminates while the parent continues and a capability completion requiring later settlement. Inject projection panic. | Exact cleanup timing, terminal conversion/custody and panic classification; task-count change measured. Do not remove a task if this delays required cleanup or changes failure semantics. |
| DG-WRAPPERS | For every section-7 candidate, try direct existing values/methods with the same two meaningful consumers where available. Inspect locality, authority, diagnostics and type bounds. | Retain/delete/reshape table with individual reasons and regressions. No blanket removal of wrappers, no blanket retention of aliases, no universal capability trait. |
| DG-MODULES | Map every current production symbol in the affected files to one owner. Trace imports from application, Entity, macros, tests and public exports. Apply the selected Behavior vocabulary and Rust API Guidelines. | Frozen file/symbol ownership map, narrow visibility map and exact migration paths. No production file move before this record. |
| DG-WORK | Exercise one existing typed external-work port with a bounded admission mechanism, rejection carrying its input, operation completion and shutdown. Inspect the exact Tokio blocking-work contract if used. | Concrete work limit, ownership of admitted work and shutdown outcome; no promise of preemption. If no existing port can express it, report CAP dependency rather than inventing an EXEC service. |

### 9.1 Public API constraints for DG-API

Preferred syntax to compare, **not a compiled API promise**:

```text
Application::new(root).run().await
Application::new(root).run_with(application_work).await
```

Do not automatically rename existing methods or add seven async twins. The
accepted record must choose a single ordinary spelling and explicitly document
source compatibility. Async-first naming is the preferred candidate, not a
waiver of compilation, diagnostics, error or ownership evidence.

The comparison must answer all of these, with exact signatures rather than
phrases such as "add options later":

1. Which methods are async, which blocking, and which public conveniences are
   retained or removed? Which values determine terminal type inference?
2. How does a synchronous caller choose current-thread versus multithread and
   a valid worker count? Who owns the Tokio builder/runtime? What is the default?
3. What happens when a blocking entry is invoked from a Tokio task? Prefer a
   documented, typed rejection before side effects if the selected current
   error model can express it; do not silently panic or spawn another runtime.
4. What happens when the async future is polled outside an enabled Tokio host?
   Establish and document a truthful precondition or checked failure; never
   fabricate a runtime as fallback. Specify required time/network drivers.
5. What exact error owns runtime construction failure, application staging
   failure, activation failure, HTTP failure and family shutdown failure?
6. Which ordinary input/output remains the caller's value? Avoid replacing a
   callback's Result with another framework error or losing its Err payload.
7. Are Entity-family reports preserved when the root fails? Specify the
   complete error product/sum and its public-surface cost before changing it.
8. Which public re-exports must remain for macro-generated/associated types?
   A private module cannot conceal a required public type; public visibility
   also does not justify putting implementation traits in the prelude.

Accepted syntax must cover no children, heterogeneous declared children,
dynamic births, advanced protocol hosts, ordinary application work, Entity
families and Axum. No caller may spell structural paths or provide irrelevant
dummy callbacks merely to satisfy a proposed abstraction.

## 10. Verification matrix

Each witness uses natural domain names. Tests must assert full typed payloads
or independently observable traces; implementation branches copied into a
model, source-string scans, and repeated assertions of one field are not
semantic evidence. Use move-only payloads for custody laws.

Behavior bodies remain pure. No channel, clock, callback, runtime operation,
task or observation publisher may be called from init/receive/transition to
make a test pass. Observe runtime scheduling outside the Behavior fold through
the existing Environment/interpreter seam. Use deterministic barriers or
explicit test-host control, never timing-sensitive sleeps as the race oracle.

| Witness | Required evidence | Requirements / gate |
| --- | --- | --- |
| EV-01 | Caller-owned Tokio executes the complete application; another caller task continues; no nested runtime; host remains usable after Bombay returns. Its error surface matches the selected async ownership equation. | XO-01–03, XO-43, DG-API |
| EV-02 | Owned current-thread and multithread modes produce equivalent typed lifecycle traces. Runtime construction failure activates nothing. | XO-03–05 |
| EV-03 | Application work called once after activation and zero times on startup error; its success and Err values survive unchanged. | XO-06–09 |
| EV-04 | Pending application work remains pending after root termination; completed work does not itself stop root. These are bounded deterministic tests, not hangs. | XO-07 |
| EV-05 | Root-startup failure still closes and joins installed Entity families; simultaneous family failure retains the selected complete outcome. | XO-10–11, XO-44 |
| EV-06 | Drop before spawn and during activation: exact input/drop counts, no success publication, no surviving abandoned task or lease after designated cleanup. | XO-15–18, DG-TASK |
| EV-07 | Drop during application work and terminal join: owner requests/transfers cleanup; independent retained observer sees the selected outcome; children/tasks reach the selected barrier. | XO-15–20 |
| EV-08 | Cancel while next_source, interpretation, source settlement, and retirement are pending. Accepted effects are retained; interrupted async cleanup is not called complete. | XO-15–22 |
| EV-09 | Normal finish does not accidentally cancel the actor when its sender closes; explicit owner cancellation does not look like normal exhaustion. | XO-19–20 |
| EV-10 | Parent panic/cancellation and child panic/cancellation preserve their separate provenance; no silently detached child or lost join failure. Include projection panic. | XO-20–23, DG-PROJECTION |
| EV-11 | Root, child and Entity ingress share startup law; each preserves its actual control authority and exact terminal type. | XO-13–14 |
| EV-12 | Child that finishes while parent continues does not defer required cleanup until parent retirement; final child terminals remain in the existing order. | XO-21–23 |
| EV-13 | Messaging-only interface cannot request shutdown or own retirement; two behaviors with the same protocol do not require type erasure. | XO-24–25, DG-SHUTDOWN |
| EV-14 | Stale shutdown capability cannot stop a new actor at a reused address; repeat shutdown cannot close/publish twice. | XO-26–29 |
| EV-15 | Delivery races shutdown admission closure; accepted prefix and exact move-only rejected messages are preserved. External actor has no invented Behavior control. | XO-28–30 |
| EV-16 | Already-stopped target gives Started then Stopped exactly once; duplicate start preserves original observer and yields exact rejection. | XO-31–32, DG-OBSERVATION |
| EV-17 | Both orderings of cancel versus completion satisfy the outcome table. Cancel twice; replay completion twice; no contradictory Cancelled and later Stopped. | XO-31–33 |
| EV-18 | Cancel old ID, reuse it for a different target, release old completion. Old work cannot remove/cancel/complete the new relationship. | XO-33 |
| EV-19 | Two independently requested observations of one target both complete; cancelling one leaves the other intact; test nested template consumers. | XO-32 |
| EV-20 | Control closure during notification returns the exact event into terminal custody; retirement leaves no live observation task/registration. Task/resource inspection confirms reuse of Observe and the accepted single-owner representation. | XO-34–35 |
| EV-21 | Simultaneously ready observation, mailbox input, source input and timer deadlines exercise the preserved acquisition policy. Continuous-mailbox evidence establishes its actual fairness limits rather than asserting an unsupported bound. | XO-36, XO-40 |
| EV-22 | HTTP bind failure starts no actor/builds no router; router built once; root termination shuts down server; serving failure retains error plus terminal. | XO-37–39 |
| EV-23 | Timer replacement/stale expiry/overflow traces unchanged; no second timer queue. | XO-40 |
| EV-24 | External work saturation rejects with original input; accepted work's completion and cancellation/shutdown are truthful, including work that cannot be stopped. | XO-41–42, DG-WORK |
| EV-25 | Two actors perform overlapping runtime work on distinct workers; independent instrumentation sees no concurrent fold of one actor. A serial-only mutation fails. | EXEC-02, EXEC-04 |
| EV-26 | Compile denials for !Send actor/effect, wrong protocol, wrong child role, forged shutdown, duplicated affine ownership, and forbidden ordinary Driver controls. | XO-05, XO-24–30, XO-45 |
| EV-27 | Public examples, renamed dependency fixture, macros and external consumer compile with selected API; no private structural path leaks. | DG-API, DG-MODULES |
| EV-28 | Before/after differential traces prove wrapper/module consolidation preserves initialization, actions, return custody, admission, configured defaults and terminal order. | XO-12, DG-WRAPPERS, DG-MODULES |
| EV-29 | Scope checks: Engine has no Tokio/HTTP/template policy; local owner imports no application runner; Entity executes through local composition. | Section 5 |
| EV-30 | Benchmark reports independent-actor overlap/throughput and scheduler configuration, plus task/allocation counts before/after. No performance threshold invented after observing results. | EXEC-09, DG-PROJECTION |

Race tests must control the contested point in production ownership, using the
narrowest test-only seam if required. A fake implementation that omits the
original competing task is not evidence that the production race is fixed.
Changes adding a test seam must explain why existing concrete composition
cannot expose the race, and must not add a new production protocol.

For every repaired defect: run the new witness on the original representation
or a precise semantic inversion and establish failure for the intended law.
Compilation failure is not a killed semantic mutant. Run the focused law in
debug and optimized builds before broadening changes. Lifecycle/generation
witnesses replay the same fact in optimized builds and prove no second
acceptance. Required operations must occur outside assertions.

### 10.1 Required command plan

All Rust commands run through pinned Nix. The following baseline commands are
exact; new test target names are recorded when DG artifacts select them, not
guessed by an implementation worker.

```sh
nix develop -c cargo test --locked -p bombay-rs --test run_with
nix develop -c cargo test --locked -p bombay-rs --features axum --test axum
nix develop -c cargo test --locked -p bombay-rs --test application_terminal_custody
nix develop -c cargo test --locked -p bombay-rs --test actor_interface
nix develop -c cargo test --locked -p bombay-rs --test entity_application
nix develop -c cargo test --locked -p bombay-rs --test terminal_projection
nix develop -c cargo build --locked --workspace
nix develop -c cargo test --locked --workspace
nix develop -c cargo test --locked -p bombay-rs --no-default-features
nix develop -c cargo test --locked -p bombay-rs --features axum
nix develop -c cargo fmt --all -- --check
nix develop -c cargo clippy --locked --workspace --all-targets -- -D warnings
nix develop -c cargo clippy --locked -p bombay-rs --all-targets --features axum -- -D warnings
nix build path:.#driver-law-evidence --no-link
nix flake check
```

Also run focused debug/release regressions, relevant existing concurrency gates
if their synchronization changed, and the selected execution benchmark. Record
exact commands, feature flags, lock/source snapshot and results. Never report
an axum-disabled build as HTTP verification. Do not run primitive-wide tests
as a substitute for the concrete Bombay race witness. A pre-existing failure
must be attributed, preserved and reported; it cannot silently be excluded
from a claimed green full gate.

## 11. Multi-agent work breakdown and synchronization

This section is an execution contract for a future coordinated run. It does
not itself start agents or authorize production edits now. Agents may research
in parallel; production parallelism begins only after shared contracts and
file ownership are frozen. One coordinator remains responsible for the whole
ownership model and integration.

| Package | Owner role and deliverable | Prerequisites | Allowed changes |
| --- | --- | --- | --- |
| WP-BASELINE | Coordinator: selected revisions, baseline manifest, requirement/evidence matrix, cumulative change budget and reciprocal backlog edges. | None | Documentation/evidence only. |
| WP-TASK-DESIGN | Execution researcher: DG-TASK and DG-PROJECTION, original-defect witnesses and await/drop ownership table. | WP-BASELINE | Isolated experiments and law tests; no retained production API. |
| WP-OBSERVATION-DESIGN | Observation researcher: DG-OBSERVATION, deterministic race/order evidence and one authority model. | WP-BASELINE | Isolated experiments and observation law tests. |
| WP-SHUTDOWN-DESIGN | Capability researcher: DG-SHUTDOWN and static target/authority witnesses. | WP-BASELINE | Compile experiments and narrowly scoped owning-contract research. |
| WP-API-DESIGN | Application researcher: DG-API, DG-WORK, consumer syntax/errors, compatibility table. | WP-BASELINE; final selection waits for task/shutdown answers | Isolated public-consumer experiments; no invented capability APIs. |
| WP-CONTRACT | Coordinator with independent review: accepts/rejects gate evidence, DG-WRAPPERS and DG-MODULES, freezes symbols/files, identifies remaining blockers. | All affected design packages | Decision records and PRD status only. |
| WP-LAYOUT | Coordinator/integrator: establish frozen module ownership by differential-tested mechanical extraction before independent writers begin. | WP-CONTRACT, original behavior baseline, and required expanded-surface authorization | Frozen file moves/import/export changes only; preserve current semantics and known failures. No duplicate retained implementation. |
| WP-TASK | Actor execution implementer: accepted task/cancellation/projection model and focused witnesses. | WP-LAYOUT; task/projection gates accepted | Only assigned local execution/termination files and associated tests. |
| WP-OBSERVATION | Observation implementer: accepted relationship authority, race and retirement witnesses. | WP-LAYOUT; observation gate accepted | Only assigned local observation files/tests; shared structures remain coordinator-owned. |
| WP-SHUTDOWN | Endpoint implementer: accepted static shutdown representation and denials. | WP-LAYOUT; shutdown gate accepted | Only assigned endpoint/ingress files/tests; no upstream mutation without separately selected contract. |
| WP-APPLICATION | Application implementer: accepted async/owned execution and HTTP composition. | Task/shutdown interfaces accepted; semantic dependencies integrated | Assigned application files/tests; no duplicate actor construction. |
| WP-ENTITY | Entity integration implementer: consume local execution owner and preserve family shutdown/errors. | Accepted local interface and DG-API error equation | Native hosting integration and focused family witnesses; no directory/state-machine redesign. |
| WP-MIGRATE | Coordinator: remaining callers, examples, exports and docs using proven contracts. | Relevant semantic packages pass focused debug/release laws | Frozen API/symbol/path map only; may invent no interface or policy. |
| WP-VERIFY | Independent reviewer: adversarial evidence, boundary audit, stale-pattern scan and final gates. | Integrated implementation | Tests/evidence/report; production issues returned to their assigned owner. |

The present monolithic files prevent safe concurrent production editing. Until
the accepted migration creates independent files, **one designated integrator
is the sole writer of `application_runtime.rs`, `local.rs`, `launch.rs`,
`lib.rs`, manifests/locks, the backlog status index, and shared test fixtures**. Workers send
bounded patches or experiment evidence; they do not all edit those files.

Do not move unproven implementations to separate files merely to create work
for more agents. WP-LAYOUT extracts the current implementation only after
DG-MODULES and its differential baseline are accepted; semantic changes then
land through the same frozen ownership contract. If this extraction itself
requires a new semantic interface, stop WP-LAYOUT and reopen the responsible
gate instead of broadening a supposedly mechanical patch. Alternative models
stay in isolated experiments and never coexist as production paths.

### 11.1 Mandatory agent assignment format

Each assignment must include every field below. An incomplete assignment is
research-only and cannot authorize production edits.

```text
Objective and exact requirement IDs:
Baseline commit plus working-tree snapshot hash:
Selected dependency versions/revisions:
Accepted decision records and exact interface signatures:
Single semantic owner and preserved behaviors:
Files exclusively writable by this agent:
Shared files writable only by the integrator:
Files/contracts explicitly forbidden to change:
Smallest failing witness and intended failure:
Complete expected trace and payload custody:
Permitted production/public-type delta and cumulative remaining budget:
Required debug/release, negative and inversion commands:
Dependencies to wait for; precise stop conditions:
Handoff artifacts and independent reviewer:
```

Every handoff returns: changed-file list and diff; law-to-symbol map; actual
production/tests/public API delta; test commands/results; original-defect
evidence; remaining uncertainty; and dependency/interface changes (normally
none). The receiving agent rechecks the snapshot before applying the patch.

Agent disagreement is resolved by the coordinator against the law and consumer
witness, not by majority vote, whichever code compiles first, or merging both
representations. An independently found upstream contract gap reopens the
affected gate. Other agents must not code around it.

### 11.2 Working-tree and merge discipline

The original research baseline was extensively dirty. The 2026-10-02
execution baseline at `2fccedf6eb636ac22143e7e01de7e784f96e2b4e` is clean.
Preserve any tracked and untracked work present at subsequent checkpoints.
Never reset, stash, clean, revert, or overwrite unrelated changes to obtain an
easier baseline. A worktree from the selected clean commit contains the current
source baseline. Experiments based on a later dirty tree must explicitly receive
that selected overlay and record its hashes. The original dirty-overlay
requirement applied to the historical research snapshot.

The coordinator serializes integration, reruns dependent witnesses after a
shared-interface change, and prevents two agents from defining the same task
owner, error product or capability. No worker may weaken a bound, add Clone,
widen visibility, change a lock, or introduce an alias merely to resolve a
merge/compiler conflict. Return such a conflict as a design issue.

## 12. Change containment and completion

Before the first production edit, write the feature-local record in the
selected PRD: exact locked owners; verified reciprocal dependencies; selected
gates; one smallest failing regression; expected files and production delta;
public types added/removed; existing owners/products reused or deleted.
Keep feature detail here; the backlog index records status and dependencies.

Create a complete baseline manifest of tracked and untracked paths, content
hashes, current diff and line counts. Record task-local deltas separately from
the inherited working tree. Production-file counts that include embedded unit
tests must be labelled as such; do not invent a public-API count for inherited
work that has not been audited.

At each checkpoint report:

```text
production: +A / -B / net C
tests:      +A / -B / net C
public API: +N types / -M types
changed tracked and untracked paths:
actor tasks / projection tasks / observation tasks per exercised application:
retained abstractions and the law each owns:
```

Repository stop thresholds apply to the **cumulative task across every agent**:
more than 15 changed files, more than 500 net new production lines, or more than
three new public types requires explicit expanded-surface authorization before
further production edits. This proposed hierarchy is likely to exceed the
file threshold. PRD approval, an instruction to finish, splitting packages,
separate agents or separate commits does not waive it. Prepare the concrete
file/surface plan and evidence before requesting that authorization.

Forbidden implementation shortcuts:

- Another actor trait, effect algebra, supervisor policy, scheduler, registry,
  mailbox, timer service, observation cell, or generic runtime object.
- Trait objects, erased futures, Any/TypeId/downcasts, unsafe lifetime/type
  escapes, serialized local control, or untyped callbacks hiding target types.
  Standard library Error::source's required trait-object return is not a new
  runtime dispatch abstraction; do not "fix" it by breaking the Error contract.
- Boolean phase/authority/provenance state, structural role strings, inferred
  lifecycle provenance, or a second copy of authoritative creation state.
- Generic parameters/traits with no meaningful substitution, a capability bag,
  or blanket public re-exports that make internal bounds application API.
- New EXEC macros, no-op caller policies, dummy wrappers, compatibility actor
  implementations, or parallel old/new production execution paths.
- Silent terminal/event discard, blanket panic for a newly modeled failure,
  manual rollback claims for accepted effects, or a success result before joins.
- File shuffling presented as an ownership repair without a dependency change;
  dense code presented as improvement while authority becomes harder to follow.

### 12.1 Feature-complete acceptance

All of these are required:

1. Every applicable decision gate is accepted with recorded evidence; no worker
   invented an unreviewed contract. A deferred gate means its dependent feature
   is not feature-complete, even if other packages landed.
2. Every XO requirement has a named witness and concrete owning symbol/module.
   Every EV witness has an executable command or an explicit external blocker;
   a blocked required witness prevents this PRD's full completion claim.
3. Async, owned current-thread, owned multithread, HTTP and Entity execution
   compose the same local actor owner; preserved return/error laws pass.
4. Cancellation, observation reuse, static shutdown authority and complete
   joined-return behavior have positive and deliberate inversion evidence.
5. The actor/public API names, module hierarchy and visibility conform to the
   selected rules and accepted symbol map. No forwarding-only replacement
   layer or duplicate runtime path remains.
6. Applicable public consumers, examples, macros, compile diagnostics, tests,
   benchmarks, research probes, docs and re-exports use the selected contract.
   Useful superseded reasoning moves to the historical record; contradictory
   current guidance is removed.
7. Required gates pass on the integrated source and selected lock. Remaining
   unrelated failures are explicitly reported and cannot be called a green gate.
8. Final complete-tree and task-local change ledgers are recorded. Report added
   capability separately from deletion; a net-positive change is not described
   as code reduction.

Use the repository states accurately: `active` for eligible verified work,
`blocked` for an unresolved prerequisite, `feature-complete` after feature gates,
and `distilled` only after the required project-wide minimization audit. Never
use `done`. This PRD and passing focused tests do not establish distillation.

## 13. References

- [Completion inventory](../prd-backlog/README.md) and
  [local execution requirements](../prd-backlog/local-runtime.md#exec--parallel-execution-and-embedding).
- [Current application execution](../../crates/bombay/src/application_runtime.rs),
  [local Environment](../../crates/bombay/src/local.rs),
  [actor launch/task ownership](../../crates/bombay/src/launch.rs),
  [observation queue](../../crates/bombay/src/observation.rs),
  [child custody](../../crates/bombay/src/child_bindings.rs), and
  [Entity host](../../crates/bombay/src/entity/bombay.rs).
- [Rust API naming](https://rust-lang.github.io/api-guidelines/naming.html) and
  [future-proofing](https://rust-lang.github.io/api-guidelines/future-proofing.html)
  inform public naming/visibility review; they do not select Bombay policy.
- [Tokio 1.53.1 Runtime](https://docs.rs/tokio/1.53.1/tokio/runtime/struct.Runtime.html)
  and the selected local Tokio task sources govern executor behavior. Verify
  exact version semantics before retaining cancellation or blocking-work claims.

## 14. Original PRD-authoring validation and change ledger

This record covers creation of the PRD, not implementation of EXEC. Four
documentation paths changed: this file, the inventory index, the local-runtime
inventory's EXEC introduction, and the programme status backlink. No decision
gate was accepted, feature state changed, or dependency edge altered.

Task-local delta: documentation `+811 / -1 / net 810`;
production `+0 / -0 / net 0`; tests `+0 / -0 / net 0`;
public API `+0 types / -0 types`.

Complete working-tree checkpoint, including inherited tracked/untracked work:
production files `+3578 / -4158 / net -580`; tests/examples/verification
`+4158 / -2226 / net +1932`; documentation `+10118 / -1814 / net +8304`;
other files `+97 / -76 / net +21`; 116 changed tracked paths and 35 untracked
files. Production-file counts include embedded unit tests. These are file-based
measurement categories, not certification of inherited changes or their API.

Validation: all 45 XO requirements have explicit EV mappings; 30 witness IDs,
eight decision gates and 14 work-package IDs are unique and resolve; local
document/source links resolve; whitespace checks pass, including this new
untracked PRD. Snapshot comparison found only the four intended documentation
changes and no production/test edits. No Rust commands were rerun for this
documentation-only task; section 4 labels the preceding audit's test evidence.

## 15. EXEC execution checkpoint (2026-10-02)

EXEC is selected next on `exec-prd-backlog`. Its PRD is current and will be
implemented through the backlog → PRD → verification → PR → passing CI →
merge workflow. The older source inventories and candidate experiments are
inputs to reconciliation; they do not replace the required outcomes.

### Selected contracts and first source inspection

The current `Cargo.lock` and sole patch select:

| Owner | Current selection |
| --- | --- |
| Behavior Core / Actors | Registry 0.20.0; both archive VCS records identify `804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`. |
| Behavior Macros | Registry 0.13.0; archive VCS record identifies `3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`. |
| Address | Registry 0.3.0, checksum `8dfc2197b4156cc87c4021a2fa0e8767a5efb009c98d4238c4147714840fc1dc`. |
| Communication | Registry 0.1.2, checksum `fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2`. |
| Observe | Bombay-private owning source in `crates/bombay/src/observe/`. |
| Timers | 0.1.0; sole crates.io patch selects Git revision `13e884da7ab41781f52337b0038060e375b00ee0`. |
| Tokio | Registry 1.53.1; standard Bombay enables macros, rt, sync and time; Axum adds net. Multithread runtime support is not selected by Bombay's manifest. |

Read the complete Behavior instructions at the exact Core/Actors revision.
Inspected application runner signatures and `LaunchSystem::{launch,launch_with,
launch_axum}`, the Bombay dependency features, current launch ownership and
runtime preservation tests. Seven current-thread builder constructions remain
in the seven public runner methods. `launch` already delegates to `launch_with`;
HTTP still repeats root launch, handle construction and joining. Entity execution
awaits family shutdown after ordinary root return but discards that shutdown
product on root-startup error. These are current DG-API comparison inputs.

Fresh primitive API/test inspection and the remaining decision experiments are
still required before production eligibility; this checkpoint does not certify
those inspections or accept a gate. The first work stage is DG-API's ordinary
Rust signature/inference comparison, with DG-TASK cancellation custody checked
before exposing caller-owned asynchronous execution. Reuse the existing
LaunchSystem futures, typed application projections and owning runtime primitives.
No new public type or runtime wrapper has been selected.

### Preserved evidence and verification

ARC-001/002 static lifecycle authority, ARC-006 activation, ARC-011 abandoned
wait cleanup, ARC-012 affine terminal custody, and ARC-010/TEST-025 interpreter
and template evidence are linked in the [backlog status index](../prd-backlog/status.md#retained-local-evidence).
Their original defects are not reopened by this PRD. All eight decision gates
still need their feature-specific comparison and review; the completed audit
does not establish caller-owned async execution or multicore acceptance.

Initial preservation command:

```sh
nix develop -c cargo test --locked -p bombay-rs --features axum \
  --test run_with --test axum --test application_terminal_custody \
  -- --skip compile_checked
```

Result: 20 passed, zero failures; two compile-check tests filtered out.
This proves the current application boundary, typed terminal custody and HTTP
baseline. It does not pass EV-01 caller hosting or any unexecuted EXEC witness.

### First stage change boundary

This stage reconciles the current PRD and records source/baseline evidence;
production `+0 / -0 / net 0`, tests `+0 / -0 / net 0`, public API
`+0 types / -0 types`. The separate ledger-retirement task changes one existing
artifact test by removing obsolete exemptions. No EXEC runtime edit is retained.
Before production work, the accepted decision record must provide the exact
failing witness, signatures, expected files/line delta and public surface.
That preceding documentation task exceeded 15 changed paths before its commit.
The current execution baseline is clean; its cumulative EXEC change budget starts
at zero. The proposed module migration still requires a measured expansion plan
and explicit authorization before crossing a repository checkpoint.
The documentation migration and its complete-tree measurement are recorded in
[backlog status](../prd-backlog/status.md#retirement-change-record).


## 16. Fresh execution baseline and sequencing amendment (2026-10-02)

The user started the EXEC delivery goal on `exec-prd-backlog` at
`2fccedf6eb636ac22143e7e01de7e784f96e2b4e`. Initial tracked and untracked
working-tree delta was empty. All existing local-runtime fixes remain in the
baseline. The exact lock SHA-256 is
`df9acbe4e947538ce4e8ec979243210c665a4dd7710bc018d28c269af2ada81e`;
selection remains the section-15 dependency set, with the sole Timers patch.
The complete Behavior instructions were read from the exact release object
`804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`, fetched from its owning repository.
No dependency was changed.

Baseline manifest: 344 tracked and untracked files, individually SHA-256 hashed,
with an empty binary diff, captured at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-exec-baseline-1h6higml/manifest.json`.
Manifest SHA-256:
`1457dc847f5e4f9ac8883a92c06a68dda8b36ff17d4b24821a22578d2fd51958`.
This local evidence path is a research receipt, not a portable acceptance gate.

### User-selected implementation order

The user resolved the conflict between section 11's layout-first sequence and
AGENTS.md's blocker-first containment rule: **fix and verify semantic blockers
before module extraction**. This explicitly supersedes the layout-first
prerequisites in section 11 for WP-TASK, WP-OBSERVATION, WP-SHUTDOWN and
WP-APPLICATION. Each semantic edit still requires its accepted, independently
reviewed decision record, original-defect witness, change record and budget.
WP-LAYOUT follows focused debug/release semantic verification and requires the
accepted DG-MODULES map, differential baseline and expanded-surface approval.
No acceptance condition is removed. Until extraction, one integrator owns all
shared production files; research agents have no repository-write authority.

### Current-source reconciliation

- Root, child and Entity already share local startup construction. Active
  startup/finish abandonment regressions preserve the ARC-011 fixes; dated
  ignored-test descriptions do not identify current defects.
- The actor task already settles its activation tasks. A projection task's
  independent cleanup-progress premise in the dated record is obsolete;
  removing it still requires origin, panic, custody and timing evidence.
- `OccurrenceBindings`, `LocalAddresses` and `FactState` are already absent.
  `TerminationObservations` owns its vector directly and uses ordered removal.
  No new deletion credit can be claimed for these historical candidates.
- Capability tasks have three current production spawn sites: worker activation,
  worker preparation and established observation. Migrating observation alone
  cannot justify deleting capability-task authority.
- Static shutdown is already realized by typed root lifecycle authority and
  Behavior 0.20's behavior-indexed installed actor product. The dated upstream
  shutdown-impossibility finding does not apply to the current selected API.
- Established observation still uses a separate ID map and task.
  [Compiled original-race witnesses](execution-ownership/observation.md#compiled-original-race-failures-2026-10-02)
  now reproduce Started-order and stale-ID removal failures in debug and
  optimized builds. The old copied-algorithm replay is not their evidence.
- Timers already uses one directly owned queue, without the historical shared
  mutex view. Preserve its current borrowing and generation laws.
- Seven owned current-thread runner constructions and separate HTTP startup
  remain. Entity shutdown results are still dropped on ordinary root failure.

All eight gates remain open. No source audit, preservation test, recommendation
or user sequencing answer constitutes gate acceptance.

### Open research and user decisions

The user requires correctness and robustness at every core boundary: preserve
as much exact information as possible, with explicit policy only at a higher
owning layer. No compatibility argument can justify accidental typed-fact
loss or a result that says something different from the proven event.

DG-TASK: the first source-cancellation question was premature. The standard
`SourceAdmission` synchronously enqueues the exact control event before returning
acceptance; its existing direct-poll witness proves that event is ready. An
admitted source is not thereby an indefinitely pending external operation.
Research must establish the reachable boundary first: backpressured delivery
interpretation, transitive source progression, or post-Driver capability
settlement. Current `next_source -> None` still means source closure and cannot
silently mean owner cancellation. No Engine port change is selected or presumed
necessary. The user asked for further research rather than selecting that
initially framed alternative. Subsequent actual standard-chain research shows
owner cancellation is not polled even when each finite source receipt yields.
The user then selected a **typed retirement boundary preserving exact actor
state, admitted receipts and unoffered settlement remainder**. Cancellation
need not wait for an endless transitive chain. An already running
noncooperative operation may still require completion; its receipt must remain
distinct from cancellation. This selects no particular Engine/Behavior port
or representation. Existing Retained cannot silently relabel live source
inputs. Owning contract comparisons and independent review remain required.

Caller disappearance also needs a researched ownership equation: arbitrary work
future disposition, receiverless exact values, and who progresses root/child and
Entity cleanup. Capability-task panic currently drops typed residual custody. The user
selected the core law: preserve the complete actor outcome and all available
task failures in a typed result, joining remaining capability work before
returning. Noncooperative work may keep the join pending indefinitely; values
destroyed inside a panicking task cannot be recovered. The ordinary-Rust
comparison and original loss are recorded in task-custody.md. Representation,
live acquisition, projection and public interfaces remain independently gated.
The later receipt discussion selected separate wait and result custody:
cancelling a wait preserves the retained receipt; surrendering the last
receipt relinquishes custody, with explicit exactly-once release after
cleanup joins. No implicit global store is selected. Application callback
and Entity integration still need their complete ownership witnesses.

DG-OBSERVATION product-law amendment: after the comparative explanation, the
user selected **exact observation-relationship cancellation authority, with its
target protocol statically checked and acknowledgement routing separate from
authority**. A cancellation from an old relationship cannot consume a newer
relationship that reuses the numeric ID, even when the protocol is the same.
Protocol matching alone is insufficient. This strengthens the explicit
cancellation/static-denial witnesses; it selects no implementation or new type.

The locked request contains only a public numeric ObservationId and a protocol
marker. It cannot distinguish those same-protocol old/new cancellation requests.
DG-OBSERVATION therefore has an owning-contract prerequisite: a verified
Behavior Actors relationship-authority contract, including its producers,
receipts, consumers and version relationship. The ID-only actor-polling
candidate remains comparison evidence and cannot be retained for the selected
law. Prepare the smallest ordinary-Rust comparison and exact upstream scope;
no token, generation, registry or new observation primitive is presumed.
Dependent production work remains blocked; independent EXEC research continues.

Fresh command results and this stage's complete delta are recorded in
[verification](execution-ownership/verification.md). This stage changes
only documentation, adds no public type and authorizes no production edit.


## 17. Source-retirement surface checkpoint (2026-10-02)

Status: **user-authorized bounded expansion (2026-10-02)**. The user's
“yes authorize everything” and “just do these changes” authorize the concrete
36-path, 150-net-production-line, zero-new-public-type source-retirement stage
below. All design gates remain open; this is surface authorization, not approval
of a representation or waiver of independent review. No retained production
code changed. The user-selected typed source-retirement law requires a factual
Engine disposition and exact Bombay cancellation/residual custody; it cannot
be represented by source closure or terminal-only Retained.

Before this checkpoint record, the complete retained delta against
2fccedf6eb636ac22143e7e01de7e784f96e2b4e is seven documentation paths,
documentation +828 / -8 / net +820; production +0 / -0 / net 0; tests
+0 / -0 / net 0; public API +0 types / -0 types. The current complete count,
including this record, is maintained in verification.md.

The isolated partial owning comparison changes nine production paths and one
test-only source path. Production +82 / -60 / net +22; tests +533 / -6 / net
+527, including earlier capability/receipt comparisons; public types +0 / -0.
Public signatures change: Environment/ActiveEnvironment gain an exact request
type, next_source uses standard ControlFlow, and existing Completion and
DriverRetirement retain that type. No new wrapper, trait or runtime is added.
This is a comparison, not accepted API. Partial patch SHA-256:
`5254b3c6611365f5257407a0d4f959abd893001765f0310f5a94124f09b31f04`;
receipt SHA-256:
`7fb30c866f67146b105c530756d9a2a147602543231ba0a1f6a12e9cf68bd60d`,
in the capability-retention-ftl9frah directory recorded above. Its one debug
source-retirement witness passes with exact cancellation request, state,
admitted receipt and unoffered accepted suffix. A private_interfaces warning
exposes private OwnerCancellation through public ActorRetirement and remains
unresolved. Optimized, inversion and full consumer verification are pending.
This is not a green gate, a completed fix or a retained production delta.

The proposed nine production paths plus seven existing documentation paths
reach 16, crossing AGENTS.md's 15-file checkpoint before required consumers.
Expanded source edits stopped before authorization. The authorized expansion
is **up to 36 cumulative paths for this source-retirement stage**, under the
following inventory, **at most 150 net new production lines and no new public
types**. Existing public interface changes remain independently reviewed.
Any additional path, public type or larger production allowance requires a
new checkpoint; this does not authorize later observation-owning contracts,
application redesign or module extraction. The inventory includes potential
diagnostic/manifest adjustments; unused allowances are not editing targets.

Production (nine paths):

- `crates/bombay-engine/src/environment.rs`
- `crates/bombay-engine/src/driver.rs`
- `crates/bombay/src/local.rs`
- `crates/bombay/src/actor_outcome.rs`
- `crates/bombay/src/actor_execution.rs`
- `crates/bombay/src/retirement.rs`
- `crates/bombay/src/terminal.rs`
- `crates/bombay/src/termination.rs`
- `crates/bombay/src/launch.rs`

Owning tests, advanced hosts, benchmark/fuzz and diagnostics (13 paths):

- `crates/bombay/src/application_runtime.rs`
- `crates/bombay-engine/tests/support/mod.rs`
- `crates/bombay-engine/tests/driver_property.rs`
- `crates/bombay-engine/tests/terminal_custody.rs`
- `crates/bombay-engine/tests/source_settlement_order.rs`
- `crates/bombay-engine/tests/compile/pass/send_not_sync.rs`
- `crates/bombay-engine/tests/compile/fail/environment_phase_authority.rs`
- `crates/bombay-engine/tests/compile/fail/environment_phase_authority.stderr`
- `crates/bombay-engine/benches/driver.rs`
- `crates/bombay-engine/fuzz/fuzz_targets/causal_turns.rs`
- `crates/bombay-engine/tests/law_manifest.rs`
- `crates/bombay-engine/tests/driver_law.rs`
- `crates/bombay-engine/tests/driver-law-evidence.sh`

Current and affected normative documentation (13 paths):

- `docs/prd-backlog/status.md`
- `docs/prds/execution-ownership.md`
- `docs/prds/execution-ownership/application-api.md`
- `docs/prds/execution-ownership/external-work.md`
- `docs/prds/execution-ownership/observation.md`
- `docs/prds/execution-ownership/task-custody.md`
- `docs/prds/execution-ownership/verification.md`
- `docs/driver-law.md`
- `docs/runtime-capability-interfaces.md`
- `docs/public-api-audit.md`
- `docs/module-boundaries.md`
- `docs/driver-law-manifest.json`
- `docs/driver-test-strategy.md`

Affected mutation baseline (one path):

- `mutants-baseline.json`

Independent scope reviewer `/root/contract_inventory` corrected the initial
34-path proposal to 36: the Driver evidence script must execute the new law's
inversion, and the mutation baseline records the deleted private adapter.
The corrected request supersedes the earlier 34-path question. Authorization
permits bounded isolated comparison; retention still requires the gates below.
The same reviewer confirms the partial patch's exact
hashes, real private-interface warning and missing optimized/inversion checks.
The tested request is a Copy unit cancellation fact; move-only request custody
and necessity over a unit disposition plus residual still need comparison.
Generic defaults are not accepted merely because they compile.

The smallest blocker is the actual recurring typed ScheduleAfter source chain:
owner cancellation remains pending despite guaranteed yields; the selected
retirement must retain its exact admitted receipt and unoffered accepted suffix.
Existing Driver, LocalResidual, owner request/oneshot authority, SourceCustody,
ActorRetirement and affine Observe primitives are reused. The unused private
generic TerminationPublication Retirement adapter is a deletion candidate only
if the accepted local classification proves its replacement. A public
Completed result should not expose private cancellation machinery; compare
extraction of non-retirement Completion<Never> with the existing OwnerCancelled
projection while conserving every payload. No generic coarse policy may be
invented to make the match compile.

Required before retention: independently reviewed DG-TASK/affected projection
contract, corrected public interface, original failure and repaired positive
oracles in debug/optimized builds, all relevant typed causal/source invariants
and inversions, advanced-host/diagnostic migration and current-document audit.
All full EXEC gates remain required afterward.

## 18. Nested shutdown checkpoint (2026-10-02)

The user explicitly authorized the bounded **38-path** expansion. It extends
section 17's canonical inventory with exactly these two paths:

- `docs/prds/execution-ownership/shutdown-authority.md`
- `docs/driver-template-manifest.json`

The cumulative allowance remains at most 150 net new production lines and zero
new public types. This also covers the independently demonstrated nested
shutdown blocker in the existing `local.rs` and `application_runtime.rs` paths;
it does not approve the full shutdown gate or later observation/application
contracts. Before this record, retained changes were seven documentation paths,
+1026 / -8 / net +1018; production/tests/public types zero. Verification.md
maintains the complete current tracked and untracked count.

Exact selected Actors 0.20.0 permits `ShutdownEstablished<B, TargetPath>` when
the target event admits that path. Bombay hardcodes `Here` in both interpretation
and its installed control ingress. The isolated nominal target admits
`ShutdownRequested` at `Inside<Here>`; the owning ActionItem compiles, but the
original Bombay interpreter fails E0277 in debug and optimized builds.

The isolated correction forwards the existing generic TargetPath unchanged
through InstalledActor and both existing interpretation implementations.
Production +17 / -16 / net +1; internal tests +184 / -0; public types +0 / -0.
It adds no owner, runtime, macro or policy. Artifact directory:
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-nested-shutdown-4arnlt3o`.
Patch SHA-256 `e328ce7e1ee10fe73213baced1862565fb319ac2b32aabfb66f1bb5f6f2b066c`;
receipt SHA-256 `287c0846a29d34c921389858ae99462159d785fc47b403b4a664eace4d91bb4f`.

Research author `/root/contract_inventory`; independent reviewer
`/root/observation_research` verified artifact/source/log hashes and reran both
profile witnesses through pinned Nix: two passed per profile. The full trace is
one nested shutdown event and Accepted(1), AlreadyStopping(2), AlreadyStopped(3),
AlreadyStopped(4) acknowledgments, with exact rejected IDs. The existing Here
regression also passes both profiles; formatting and strict library Clippy pass.
The coordinator independently inspected the patch and verified artifact hashes.

This supports the narrow generic-path correction, not full DG-SHUTDOWN
acceptance. Actual committed application/child installation, same-protocol
distinct behaviors, stale-address exact authority, admitted delivery/rejected
payload custody, and the remaining static denials must have their required
evidence before production retention. No retained production edit is made by
this record.

## 19. Communication admission prerequisite (2026-10-02)

Fresh shutdown implementation witnesses exposed a selected dependency
contradiction. Communication 0.1.2 promises that consuming or dropping
MailboxOwner prevents every stale MailboxRef from obtaining a new delivery
permit, while allowing pre-close permits to finish. The actual close only
drops its counting UserSender; UserAnchor::upgrade tests whether the remaining
count is nonzero. An earlier in-flight operation keeps that count nonzero,
allowing a new operation to obtain a permit after shutdown has closed admission.
ActorRef::send_from delegates to that MailboxRef without another admission gate.

An actual ActorRef witness fails the intended law in both debug and optimized
builds: shutdown is accepted before the post-close operation is constructed;
freeing capacity with the consumer still alive then admits that new payload.
The test observes the complete delivered trace and original Vec allocations
rather than treating a timeout as closure. The frozen evidence and independent
reproduction are recorded below; no dependency edit is accepted.

The coordinator independently inspected the selected source and its contract.
Archive VCS revision is `6067df1cb12b4e87086f120fb3e879fd5afdbd92`, source path
`crates/communication/src/lib.rs`. The exact release tree contains no AGENTS.md.
Remote main at `e1017dc4da7e8d3ca014757d2e7308fa4426eb2b` differs only in agent
configuration/security files; no source correction or newer release exists in
the inspected comparison. Earlier primitive tests allow an operation admitted
before closure to finish but do not cover a new admission while that permit
remains live. These are distinct laws; pre-close work must retain its custody.

Communication owns the required atomic admission-close/acquire law, distinct
from the number of existing permits. Bombay must not introduce a second mailbox
or silently weaken shutdown closure. Dependent local acceptance and production
retention remain blocked until the owning correction is verified and selected.
Independent Engine, ownership and observation implementation comparisons
continue. Any owning source expansion requires its concrete change record,
scope checkpoint and independent review; the 38-path authorization does not
cover unseen Communication files. The backlog records this external prerequisite.

### Frozen defect evidence and independent reproduction

Artifact directory:
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-communication-close-f03vzd78`.
Patch SHA-256 `68c5be85b38bbc8dfedea693c5f9480aba166b194ce1bad729be626571219ef2`;
receipt SHA-256 `d7228ee6cd3cfccecbf1fe55096b7c8a366f645e828e35b8103d19bb5a8d254c`.
The coordinator verified both hashes and the recorded debug/release logs.
The isolated delta is one existing test-bearing source path, tests +79 / -2 /
net +77; production and public types zero. No retained source delta results.

Author `/root/contract_inventory`; independent reviewer
`/root/observation_research` verified source, selected lock and artifact hashes,
then reproduced the intended failure with a private per-copy build directory:

```sh
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked --target-dir <artifact>/independent-target -p bombay-rs --lib shutdown_close_denies_a_new_sender -- --nocapture
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked --release --target-dir <artifact>/independent-target -p bombay-rs --lib shutdown_close_denies_a_new_sender -- --nocapture
```

Each executes exactly one test and exits 101 for the intended assertion.
The complete delivered trace is `17:[11,13], 43:[37,41], 61:[47,53,59],
31:[19,23,29]`: 61 is the forbidden new post-close admission; 31 is the valid
pre-close pending operation. Both payloads retain their original allocations.
Pre-close admission completion must continue to work after the correction.

An intervening rerun using a shared research build directory selected another
copy's artifact and executed zero matching tests. It is excluded from evidence;
the independent private-directory runs reproduce the actual defect. The owning
fix, concurrency proof, performance comparison and released dependency selection
remain pending. No Bombay-side duplicate admission mechanism is authorized.

## 20. Proposed Communication correction checkpoint (2026-10-02)

Status: **user authorized** by the explicit response "approved" after the
52-path checkpoint request. This authorization expands the bounded
38-path source/shutdown stage to **52 cumulative canonical paths**, counting
both repositories. It authorizes no particular admission representation and
does not waive independent design review, owning verification or delivery.

Measured retained baseline-to-`d7e2f04` delta: eight documentation paths,
+1257 / -8 / net +1249; production +0 / -0 / net 0; tests +0 / -0 / net 0;
public types +0 / -0; untracked paths zero. The revised isolated source candidate
currently measures production +247 / -180 / net +67 and zero new public types;
its test minimization/checks are still in progress. The isolated generic
shutdown correction adds one net production line. Neither is retained.

The exact blocker and failing end-to-end regression are section 19's forbidden
post-close admission while a legitimate pre-close operation remains live.
Communication owns the correction. Compare ordinary mutex-serialized admission
with an explicit admission phase in the existing atomic sender-count state;
retain only a proven acquire/close linearization with exact rejected payloads.
Reuse the existing user ring, control lane, UserSender/UserAnchor and
MailboxOwner/MailboxRef. Add no second mailbox, permit registry, runtime policy
or public type. Raw channel semantics, overflow, already admitted work and the
owning performance/allocation contracts require independent verification.

The proposed owning allowance is at most **80 net new production lines** and
**500 net test/benchmark lines**, with no new public types. These are bounds,
not measurements or permission to omit required laws. The existing cumulative
150-net-production-line ceiling remains: current source 67 + shutdown 1 +
owning allowance 80 = 148. Report exact additions/deletions after implementation;
stop for another concrete checkpoint if the required correction exceeds a bound.

New owning Communication paths (10; repository-relative):

- `crates/communication/src/lib.rs`
- `crates/communication/tests/mailbox_retirement.rs`
- `crates/communication/tests/loom.rs`
- `crates/communication/tests/mailbox_allocation.rs` (new)
- `crates/communication/benches/twolane.rs`
- `README.md`
- `docs/mailbox-admission.md` (new)
- `crates/communication/CHANGELOG.md`
- `Cargo.toml`
- `Cargo.lock`

New Bombay paths beyond section 18's inventory (four):

- `Cargo.toml`
- `Cargo.lock`
- `README.md`
- `docs/prd-backlog/evidence.md`

Communication's package manifest inherits the owning workspace version; inspect
it during release but no change is forecast there. The owning manifest/lock
allowance includes publication rather than pretending source verification alone
selects a released contract. Bombay's manifest/lock selects the verified release;
README and the backlog evidence table update current guidance. Existing authorized
PRD, shutdown, verification, status, capability and API records retain exact
dependency hashes, independent review and eventual PR/CI/merge evidence.
Historical dated snapshots remain historical. No release version, fix, PR or
merge is claimed by this proposal.

### Owning pre-edit record

The four test/benchmark entries above correct package-relative spellings in the
initial proposal to actual repository-relative paths. They name the same
owning files and add no path beyond the approved 52-path surface.

The smallest owning regression is now executed against the exact selected
Communication source, independently of Bombay: one pre-close send remains
pending while owner closure precedes the first poll of a new send. Both debug
and optimized builds fail the intended law, with complete queue `[1,2,4,3]`:
4 is the forbidden new admission; 3 is the legitimate pre-close operation.
Original Box identities, exactly one user-lane-closed marker and final None
are observed; no timeout substitutes for closure. The isolated test adds 73
lines in the existing owning mailbox_retirement test, with no production edit.

Artifact directory:
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/communication-admission-owner-3d10vk15`.
Original regression patch SHA-256
`679b5a599c08359ca127fbda7e5a73f6146df7011de94be85a8b14fd8b2ffe2d`.
Receipt SHA-256 `23cba15eee846abf46cf870cab3e60cc28d24d18ca05e41747e78064ceaa7764`;
selected owning source SHA-256
`8598863bfa0bb6a5d8454b4cf0af3fcb35a4b57379b89474da3d6b25626ebf17`;
selected owning lock SHA-256
`482b67e02adad36bcba4eacf6950f2d35dfa255ed93313984357c1ce7bda5bd8`.
The pre-edit ordinary comparison is `admission-comparison.md` in that directory,
SHA-256 `baaac97257c27014d6f745e1d1cf6b561297391e47d8740928a4a99b58ec50c9`.
Independent reviewer `/root/observation_research` verifies the selected source,
lock, patch and genuine one-test failures in both profiles. This accepts the
defect evidence, not an owning fix or full shutdown gate.

The first bounded implementation experiment is the ordinary private closed sum
`Open(UserSender<U>) | Closed`, shared by existing MailboxOwner and nonowning
MailboxRef. It owns the admission phase independently of live in-flight senders.
Acquisition clones the existing sender only while Open and releases the guard
and temporarily promoted admission Arc before awaiting. Owner Drop explicitly
closes under the same lock even when a reference holds that temporary Arc;
the displaced sender drops outside the lock. Reuse the raw UserSender/UserAnchor,
ring, backpressure, control lane and consumer marker unchanged. This deletes the
incorrect inference that a surviving sender implies new mailbox admission.
The concrete consumers are existing mailbox send and try_send, including stale
references racing owner retirement. No public type is added or removed.

Forecast: one owning production path, net +45 to +65 lines, subject to the
approved +80 owning and +150 cumulative ceilings. This adds one mailbox
construction allocation and per-operation locking; it is not code reduction
or accepted performance. Compare the phase/count atomic alternative and verify
owning allocation/throughput contracts before retention. The independent design
critique identifies explicit Drop closure, lock-free await boundaries and
outside-lock sender destruction as required conditions. At that pre-edit checkpoint, repaired traces,
Loom/concurrency, raw API compatibility and performance remained unproved.
The [current owning correction record](execution-ownership/shutdown-authority.md#communication-owning-correction-and-delivery-boundary-2026-10-02)
now records the verified candidate, complete measured delta and remaining
delivery prerequisites; the accepted cost and upstream merge do not yet
prove a published or selected dependency.
