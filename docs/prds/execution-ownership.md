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
Initial selection was the section-15 dependency set, with the sole Timers patch.
The released Communication selection in section 20 supersedes that package only.
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

Caller disappearance also needs a researched ownership equation for
receiverless exact values and who progresses root/child and Entity cleanup.
[Application work disposition](execution-ownership/task-custody.md#application-work-disposition-2026-10-02)
and its caller-owned discharge timing are user-selected.
Capability-task panic currently drops typed residual custody. The user
selected the core law: preserve the complete actor outcome and all available
task failures in a typed result, joining remaining capability work before
returning. Noncooperative work may keep the join pending indefinitely; values
destroyed inside a panicking task cannot be recovered. The ordinary-Rust
comparison and original loss are recorded in task-custody.md. Representation,
live acquisition, projection and public interfaces remain independently gated.
The later receipt discussion selected separate wait and result custody:
cancelling a wait preserves the retained receipt; surrendering the last
receipt relinquishes custody under the selected
[caller-owned discharge rule](execution-ownership/task-custody.md#selected-caller-owned-discharge-and-acquisition-policy-2026-10-02).
No implicit global store is selected. Application callback
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
now records the reviewed correction, complete measured delta, passing CI,
both merges and verified publication. The released selection below resolves
this dependency prerequisite; full EXEC gates remain open.

### Released dependency selection pre-edit record

Communication's correction and generated 0.1.3 release are now delivered through
reviewed PRs #7 and #8 with passing CI. Publication is verified independently;
[the owning evidence](execution-ownership/shutdown-authority.md#communication-owning-correction-and-delivery-boundary-2026-10-02)
records the source correction and its scope. Released registry archive checksum
is `eb0dc8a057efce6e387c9bc24955ffb020b2c138c5ce6b10c1f6c211d32ad268`,
VCS commit `272a2343187b40615ab26c2d0d2e136010a16e77`, source SHA-256
`c6e601bbc4deb6c17d0ab5b2a3ca9af53482fbc689c603e41a1ceca51edaea5c`.
All fourteen packaged Rust source/test/benchmark files match that release tree.
No standalone archive tests are claimed: owning workspace CI supplies the test
provenance where unpublished testkit dependencies are needed.

The fresh registry-selected Bombay regression passes one actual ActorRef test
in both debug and optimized builds, preserving the accepted three-message trace,
pre-close allocation and exact rejected post-close Vec allocation. The same
oracle fails for original 0.1.2 in both profiles. Frozen published-selection
receipt `3c22c9a87628402af5284c7026237a03c3c4bcee97abf4b626b2a96f4708c760`
and patch `2bbe1ca85efebc5a2bea31f62b759f8792f6c6f6947882f4e4d70dc8b3e6ecd0`
are in `bombay-published-communication-zgzyf3vj` under the recorded scratch root.
Independent reviewer `/root/observation_research` authenticated all artifacts,
read the patch and verified only one registry package changes in the lock.
This accepts the narrow dependency selection, not full DG-SHUTDOWN or EXEC.

Recorded before the canonical edit: root Cargo.toml will require 0.1.3 and Cargo.lock will select
exactly that registry version/checksum. Expected paths: those two already
authorized paths. Expected production +0 / -0, tests +0 / -0, public types
+0 / -0; manifest/lock +3 / -3 / net 0. Reuse the released owning admission
state; add no local phase, patch or duplicated gate. All other selected package
records, including Core/Actors/Macros and Timers' patch, must remain unchanged.
Fresh pinned-Nix locked workspace verification follows the selection.

The canonical selection now matches the frozen manifest and lock exactly:
Cargo.toml SHA-256
`2c5f9bfebcb5cf6db87874995debbbcceac14dbce6886485978b4665acc56c76`;
Cargo.lock
`1ca7df546ffd2db7406810a32d745c1fe70b891189c42b8a020daa242c6710b4`.
A parsed before/after comparison proves that Communication's version/checksum
is the only changed package record. The actual selected registry source and VCS
metadata match the reviewed release. Core/Actors remain 0.20.0 at 804b2bf,
Macros 0.13.0 at 3f08364, Address 0.3.0, Tokio 1.53.1 and the exact Timers patch
remain unchanged. The selected complete Behavior instructions still apply.

Canonical `nix develop -c cargo test --locked --workspace` passes 414 tests
in 61 result summaries, exit zero. Log `/tmp/bombay-exec-communication-013-workspace.log`,
SHA-256 `9b07b81d9a25d3c291d9e014a21e4cea839c97bf960e9516372d5096aa3efa44`.
`nix develop -c cargo fmt --all -- --check` and
`nix develop -c cargo clippy --workspace --all-targets -- -D warnings` also pass.
Their `/tmp/bombay-exec-communication-013-fmt.log` and `-clippy.log` SHA-256
values are respectively
`f30664ab871acc90ca42079b62a213e06ace0acb49e91b12eb5b86403afb7257` and
`26f34b25512d8130c5fe14c71da45ac8ea0aae8d09745860cf0b3b829612f45c`.
These verify released dependency selection and preserve existing local fixes;
all EXEC design gates and final delivery remain required.

## 21. Distinct capability-failure vocabulary checkpoint (2026-10-02)

Status: **user authorized** by the explicit response "Authorize the bounded
expansion". This permits the scope below, not self-approval of its design gates.
The user's authorization to adopt recommendations resolves policy decisions,
not this repository's explicit change-budget checkpoint. Section 20 permits
52 cumulative paths, 150 net production lines and zero new public types for
the named source/shutdown/Communication stage. The task's live-failure policy
needs an additional owning Behavior Actors vocabulary change.

Measured delivered source across repositories remains Communication
production +62 / -13 / net 49, tests/benchmarks +502 / -5 / net 497, new public
types zero. Canonical Bombay production/tests/public types remain zero;
manifest/lock +3 / -3 / net 0, thirteen tracked paths and no untracked files.
Its current complete documentation measurement is in verification.md.
Source-retirement net 64 and nested-shutdown net 1 remain isolated, not retained.

The original actual capability failure resumes unwinding and loses recoverable
actor/sibling results, as authenticated in task-custody.md and
terminal-projection.md. Selected Crash has no distinct operation-failure cause;
using EnvironmentFailed, Panicked or Cancelled would erase the selected
distinction. Reuse the existing Crash sum, TerminalOutcome, ReportTerminalOutcome
and owning monitor/propagation templates; add no new actor law, result wrapper
or public type. The proposed variant is CapabilityFailed, describing a
capability task failure acquired while its actor is live. Actual typed failures
and actor state remain Bombay's responsibility. The variant cannot stand in
for the unimplemented task-custody correction or rewrite completed outcomes.

Proposed expanded allowance: **64 cumulative paths**, adding these twelve
Behavior repository paths to the approved 52-path inventory:

- crates/actors/src/termination.rs
- crates/actors/src/lifecycle/termination_propagation.rs
- crates/actors/src/lifecycle/termination_monitor.rs
- docs/established-capabilities.md
- Cargo.toml
- Cargo.lock
- README.md
- crates/actors/CHANGELOG.md
- crates/behavior/CHANGELOG.md
- tests/interpreter-contract/Cargo.lock
- crates/behavior-macros/tests/fixtures/Cargo.lock
- crates/behavior-testkit/fuzz/Cargo.lock

Allow at most **6 net owning production lines**, **120 net owning test lines**,
one additional existing-enum variant and zero new public types. The cumulative
150-net-production-line ceiling remains: source 64 + shutdown 1 + delivered
Communication 49 + proposed allowance 6 = 120. These are pre-edit bounds, not
measurements or design-gate approval. Complete action/replay conservation tests,
compiled distinguishing inversions, exact-profile verification, independent
review and full owning consumer/document audit remain required.

The two package manifests inherit the owning workspace version; no independent
manifest edit is forecast. Adding an exhaustive public enum variant is a
breaking API change. The release stage must inspect generated coupled versions,
update all four workspace locks and README, pass the invoked Behavior release
skill's exact-head preflight, CI and review, and verify both actual published
archives/tags. Bombay's already-authorized manifest/lock paths then select the
verified release and revalidate its exact instructions/sources and consumers.
No release version, PR, merge or publication is asserted by this proposal.
Observation authority, consuming diagnostics, runtime interpreter inputs and
full application/task implementation require their separate measured scope;
this narrow allowance does not silently authorize those expansions.

### Complete consumer audit and three-path correction

Independent reviewer `/root/contract_inventory` audited all current owning
source, documentation, benchmark, macro, fixture and fuzz consumers. Three
existing coverage suites also need the new cause in their exhaustive lists:

- crates/actors/tests/fifo_pool.rs
- crates/behavior-testkit/tests/compositions.rs
- crates/behavior-testkit/tests/terminal_outcome_sequences.rs

The last is the independent terminal-outcome generator/model. Generic Err(_)
policy consumers already accept the full cause; specific Failed fixtures and
CreationRejection::EnvironmentFailed are not exhaustive Crash inventories.
No other required changed path was found. The original twelve-path forecast
missed these current coverage consumers; they cannot be omitted to fit it.

Proposed correction: **67 cumulative paths**, retaining the approved 6-net
production/120-net-test owning caps, 150 cumulative production ceiling,
zero new public types and exactly one new public enum variant. Before this
correction, the owning uncommitted candidate measures production +3 / -0,
tests +51 / -2 / net 49, documentation +10 / -0 across four tracked paths,
untracked zero. Focused debug/release each pass nine tests, fmt and strict
all-target owning Clippy pass. Review requires cause wording to describe live
acquisition as primary cause, without claiming physical failure chronology;
inversion and full owner CI are still required.

The user explicitly authorized the three test paths. The corrected allowance
is 67 cumulative paths with the same limits; remaining gate/review/verification
requirements still apply.

### Frozen owning cause contract and verification

The isolated owning branch `exec-capability-failure` starts at selected
`804b2bf25325a523884ec49d8a4ae6d2d2b6e9da`. Seven existing paths change:
production +3 / -0 / net 3; tests +56 / -4 / net 52; documentation +12 / -0;
public API +0 / -0 types and one breaking exhaustive enum variant; untracked
zero. The approved 67-path forecast includes the later generated release
metadata, not 67 currently edited files. Cumulative source/shutdown/delivered
Communication/actual cause forecast is 64 + 1 + 49 + 3 = 117 net production.

Patch SHA-256:
`3d17ddb24d53853c6c5bbe7e0e44ea607bee67bb1c9e7030390af0a1cbaebbfe`;
receipt `146baefa350ba42f14ca6dfd68e6d260634315b796d4c5a184df4a7b44d08030`
authenticates the seven sources and 15 verification artifacts. The existing
Crash sum gains CapabilityFailed; no new state, interpreter, routing, authority,
wrapper, generic bound or policy input is added. Templates conserve that same
cause through their existing explicit policy paths. Public syntax is the
existing TerminalOutcome with Err(Crash::CapabilityFailed), not a second result
or repeated cause label. Aggregate control states, subordinate products and
transition branches remain unchanged. Normalized lifecycle/capability and EXEC
cause/disposition laws were cross-checked; no arrival history, coordinated
flags, inferred provenance or structural caller syntax is introduced. Drift
checkpoint disposition: pass for this bounded shared cause contract only.

Before production, the standalone consumer of selected Actors 0.20.0 cannot
name the required variant: E0599 in debug and optimized checks. This is static
expressivity evidence, not an original runtime regression. The actual original
capability panic/custody loss remains in the task decision record. For semantic
inversion, changing only existing production propagation emission to reclassify
CapabilityFailed as EnvironmentFailed causes the complete-outcome regression
to fail with intended compiled assertion exit 101 in both profiles. The exact
source is restored and all nine focused lifecycle tests pass in both profiles,
including duplicate-fact rejection in optimized execution.

Pinned-Nix verification against this unchanged candidate:

- cargo test --locked --workspace: 946 tests pass, including documentation.
- cargo nextest run --locked --workspace: 857 pass, zero skipped.
- focused actors lifecycle debug/optimized: nine each pass.
- optimized FIFO recovery, crash-policy composition and independent terminal
  sequence model: one each passes.
- cargo fmt --all -- --check and strict workspace/all-target Clippy: exit 0.
- interpreter dependency graph check: one local version per owning crate.
- external interpreter debug/optimized: 18 each pass using the owner's pinned
  1.95 compiler inside Bombay's pinned Nix invocation.
- nix flake check -L: all ten compatible Darwin checks pass, including build,
  tests, lint, documentation, formatting, dependency audit/policy and package
  inspection. Other architectures are verified by their required CI lanes.

The initial external interpreter run with Bombay's Rust 1.96 failed only an
existing E0599 text snapshot ("associated item" versus "constant"). It is
excluded from semantic evidence. No fixture or acceptance criterion was
weakened; the owning pinned toolchain used by its CI passes unchanged fixtures.
The unchanged candidate also passes the owning fuzz build through pinned Nix
(exit 0). Final independent reviewer `/root/contract_inventory` authenticated
the complete patch, all seven sources and all 15 frozen receipt artifacts,
including its own source/consumer review, and signed this bounded contract.
The source commit is `09a4cdc365dce63c5a2fa3b3d7d29c1dbf58fc62`;
[owning PR 78](https://github.com/devrandom-labs/bombay-behavior/pull/78) targets
main with an explicit semver-breaking label. Its
[independent review](https://github.com/devrandom-labs/bombay-behavior/pull/78#pullrequestreview-5398261097)
is posted as COMMENTED through the shared GitHub account, transparently naming
the non-author agent; it is not a separate GitHub principal approval. The
current branch rules require a PR, Nix Flake Check and resolved review threads,
with zero separate approving reviews. All required and repository CI gates
remain mandatory before merge. All 14 current exact-head checks pass, including required Nix Flake Check
and aggregate Mutation Gate. Earlier label-triggered CI was cancelled by the
replacement run and is excluded. Review threads are empty; merge state was
CLEAN. PR 78 merged at `705d03754b0640f5565b10aa37bc2b22462045bf`
on 2026-10-03, and its main CI run 37085358333 passes.
[Release PR 79](https://github.com/devrandom-labs/bombay-behavior/pull/79)
selects Core/Actors 0.21.0. Its corrected exact head
`81b70a40b5a1aad289c7e18df225c3ef7fc0dec2` reconciles all four workspace
locks, README installation versions and both coupled changelogs. The bundled
release-skill preflight passes (log SHA-256
`6a45b6a36472a7ca9aecec3f99f65ca800360cb65c771d68bd00724d511ec754`);
all ten local Darwin Nix checks and all 14 exact-head remote checks pass.
The eight-path metadata patch is frozen at SHA-256
`477668fe7a39f3ab5da821a77e38034bdd4ae3c7ec2ae5262a98bc6787f427ac`,
with receipt `39fd48f41f2296e7d05bb914f7f9340e82cb15a067f535b10eecd5270240c5f6`.
Its independent non-author
[review](https://github.com/devrandom-labs/bombay-behavior/pull/79#pullrequestreview-5398414526)
uses the disclosed shared account, not a separate GitHub principal approval.
With no unresolved threads and CLEAN merge state, the authorized release PR
merged at `5f9185c9a66bdb80216b63f89a5c42fa02becaa0` on 2026-10-03.
Main CI run 37087541543 passes. Verified-commit Release run 37087999908
passes. Both registry archives and tags identify the same reviewed merge.
Core 0.21.0 checksum is
`5b03af3448d25805c27bd37517479f632160fd81932ce60d6a73be22cac5d0a1`;
Actors 0.21.0 checksum is
`16c7a7d39ab3c10bb074f3e23330df39247d2284c89de2c0d2e107d2a59e7287`.
Published source comparison and fresh owning verification precede Bombay
selection; that selection is not yet claimed.
This does not implement
Bombay's failure acquisition, actor retirement or full result custody and does
not close DG-TASK, feature acceptance or delivery.

## 22. Fuzz prerequisites and early actor ownership checkpoint (2026-10-03)

Status: user explicitly authorized the bounded expansion. The allowance is
69 cumulative paths, 165 net production lines and zero new public types.
The prior 67-path/150-line and initial 69-path/161-line limits are superseded
only for this bounded stage. Other full EXEC
gates and later scope remain required; this stage does not defer or accept them.

Complete retained/candidate delivery delta across canonical Bombay and the
Communication/Behavior owning branches: production +65 / -13 / net 52;
tests/benchmarks +558 / -9 / net 549; public types +0 / -0, one existing
public enum variant added; 38 tracked repository-qualified paths, zero
untracked files. Canonical Bombay's complete documentation/manifest delta is
in the verification footer. Communication delivery is merged/published;
Behavior's source and eight-path release metadata PRs are merged; registry
publication is verified, with Bombay selection pending. Isolated research artifacts have separate recorded measurements
and are not claimed as retained production.

The already-approved source correction forecasts 64 net production lines,
and nested shutdown 1; with retained 52 this is 117. Current source comparison
passes all-feature workspace 418 tests/61 summaries in both profiles, strict
Clippy and formatting. Independent review found the separate fuzz header
invalid and redundant nested async expressions in advanced hosts. The corrected
header, direct ordinary expressions and excluded-fixture formatting are now
verified; the diagnostic snapshot changes only the two shifted source line
numbers, preserving both E0599 phase denials. That existing snapshot path is
already in section 17's approved manifest. Its standalone Completion inference
cost is explicitly accepted: unannotated Completion::Stopped requires a type
annotation; Driver-derived values infer the exact request. Do not claim a
default preserves the former unannotated spelling.

The separate fuzz check now reaches an additional prerequisite: its tracked
Cargo.lock still records local Engine 0.1.0 while the actual owning manifest
is 0.2.1. After the independently verified coupled Behavior release, its
manifest must also select that same Core version. Required new paths:

- crates/bombay-engine/fuzz/Cargo.toml
- crates/bombay-engine/fuzz/Cargo.lock

These add no production line or public type; update only the verified owning
versions/necessary locked graph, inspect the actual delta and run the separate
pinned-Nix fuzz build/check. Proposed corrected allowance: **69 cumulative
paths**, including both previously omitted configuration consumers.

The three original application/startup custody failures in the task record
prove an independent owning handoff gap. A read-only ordinary-Rust proposal
extracts the existing actor construction into a plain function returning its
coexisting original cancellation authority, startup receiver, control and
raw actor join handle before startup.await. Existing child/Entity startup
wrappers reconstruct their existing OwnedTask; a static join owner conserves
actual startup and full actor results while caller values stay caller-owned.
Reuse the current actor task, Environment/Driver, startup notice, cancellation
request and Tokio JoinHandle; introduce no grant clone, service, result cell,
public type or alternate actor loop.

Bounded first comparison: launch.rs measured direct-argument proposal +55 / -7 / net 48;
application_runtime.rs test-only, at most 280 additional comparison test lines.
The user explicitly authorized this expansion from 180 after the formatted
complete draft measured 236 lines and the remaining truthful custody checks
forecast at most 280; production/files/public type limits are unchanged.
This proves the owning handoff and actual static join composition, rather than
claiming complete LaunchSystem/public-runner/Entity implementation. All required
scope stays pending until implemented and accepted. The full internal
LaunchSystem integration separately forecasts +124 / -34 / net 90 and requires
its own measured stage before implementation. No 80-line full-fix claim is made.

Proposed cumulative ceiling for the bounded handoff stage: **165 net production
lines** (117 approved forecast + 48), still zero new public types and at most
69 paths. Preserve ARC-011/012 regressions, establish exact original-fail /
repair-pass and distinguishing inversions in both profiles, verify all owning
consumers, and obtain independent review before retaining the representation.
Any excess or new semantic/public surface requires another concrete checkpoint.

The initial 44-line handoff forecast did not survive pinned-rustfmt measurement:
the final direct existing-argument form measures +55 / -7 / net 48. A tuple
form measures +50 / -7 / net 43 but makes input names less explicit without
proving another ownership benefit. Retain direct named inputs. The user
explicitly authorized the four-line expansion to 165 net production lines,
with 69 cumulative paths and zero new public types unchanged. The isolated
bounded handoff comparison may proceed within that allowance; full application
integration remains unaccepted.

### Published Behavior dependency selection pre-edit record

Verified Release run 37087999908 succeeds at the reviewed release merge.
Both registry archives pass checksum comparison and all 200 published Rust
files (Core 28, Actors 172) exactly match that commit. Both annotated tags
resolve to it. Its complete AGENTS.md was reread; SHA-256 remains
`2b7a9195b27f073fec18426da43e9840f8ef668f333b9ad55934f520a37ae226`.
The source/owning tests and complete seven-path cause change are verified in
section 21; release changes add metadata only. Existing primitive selections
and patches remain unchanged. Actors owns the distinct public stop reason;
Bombay still owns detection, retirement and typed task failures.

Before the canonical edit: root Cargo.toml will require Core/Actors 0.21.0;
Cargo.lock will select only those verified package versions/checksums. Expected
manifest/lock +6 / -6 / net 0, production/test/public types zero. Reuse all
existing owners; no runtime or API change is included. Update the already
authorized capability contract record to the actual selected artifacts,
including its stale Communication 0.1.2 entry. The separate source-candidate
fuzz graph selects the same verified Core in its separately authorized paths.
Current root manifest/lock hashes before this edit are recorded above.
Audit every current version consumer; retain explicitly dated earlier evidence
as history. Verify the resulting actual locked graphs and all-feature workspace
tests, formatting and strict lint through pinned Nix. An unexpected dependency
or new consumer contract blocks retention rather than being inferred.

The selected-version consumer audit also finds current Driver law/template
manifests and their four test assertions still pinned to 0.20.0. Their already
authorized files require only revision/version binding replacement: no new
Driver law or template policy. The production delta remains zero; test binding
changes are +4 / -4 / net 0. Fresh current-revision Driver positive, boundary
and mutation evidence must be run before retaining the updated evidence IDs.
The initial 0.21 workspace build failed for disk exhaustion and is excluded;
only a completed rerun can count. Earlier dated acceptance remains historical.

### User revision of line-count checkpoints (2026-10-03)

The user explicitly instructed: "just go on! lines are lines" after the
additional test-line checkpoint. This supersedes the requirement to request
further permission solely for production or test line-count increases.
The previous 165/280 bounds remain stage estimates; measure actual additions,
deletions and scope at every checkpoint and keep stages independently
reviewable. File/public-type limits, verified ownership, pre-edit law/model
provenance, independent design review and every acceptance/delivery gate remain
required. This changes permission handling, not semantic scope or correctness.

Canonical published selection verification passes all-feature workspace
414 tests/61 result summaries in each profile, strict all-target Clippy and
formatting. Fresh Driver evidence executes every canonical law's positive,
terminal/phase cases and mutations against the actual selected revision;
receipt SHA-256 `adebd3c77a3214fc42bccbf302d6e0bc3ff77e8f45569ac99d3bbbbeb8da370b`.
The initial disk-exhausted build is excluded. This accepts dependency binding,
not the unimplemented EXEC runtime laws. Before updating the separately
authorized canonical fuzz manifest/lock, preserve their hashes below; change
only Core 0.20.0 to 0.21.0 and local Engine 0.1.0 to actual 0.2.1. These
configuration changes add no production code, test code or public types.
- crates/bombay-engine/fuzz/Cargo.toml: `9b5246f90fc0123463fadeba70d8dcb052ffba8617dcc4264c8fcb278761d7cb`
- crates/bombay-engine/fuzz/Cargo.lock: `50735c06931635877c64ccd69622eef8f1bb9191bd3bd4833c7d0430aa9d130e`

The non-author contract reviewer signs the published dependency selection's
13-path patch `6d07d585c7c88a1f7e3840ba6c2e61bc139e0e269eaeb5844678252edc882f75`
and receipt `98faab71b286398b2b804a9b6ea9e3706224b66299f03263792f1977c960a883`.
It authenticates all files/logs, independently compares all 200 published
Rust files and peels both GitHub tags. Root lock changes only Core/Actors;
fuzz lock changes only Core and local Engine. The fresh Driver run proves
16 positive/terminal cases and eight killed mutations; full run SHA-256
`3cd7634f2930d3a80d56e93b01a388ec17aee71acca96f5842d2eed3b1ca1469`.
Appending that already-reviewed run hash produces receipt
`3c4a034ce5da9c159ba48598d9c1e74059d141ded435bdfe78174adeb065e9de`.
The corrected capability document explicitly distinguishes historical 0.1.2
admission failure from selected 0.1.3 repair. The backlog no longer lists
the released stop-reason vocabulary as an unresolved external prerequisite.
Full EXEC runtime and observation ownership remain blocked on their gates.

Current selected artifact hashes after verification:
- Cargo.toml: `a570253873f7b3ba12838719ebdcbf3ad7e217d6cecfe75bb16dc8a9f2664cb4`
- Cargo.lock: `747f92160972f773c68d624517c456765f6c793bd21fb4faf4f4ec131420e8b1`
- crates/bombay-engine/fuzz/Cargo.toml: `1b63701694923a4c6410e7b11d34d932c2a3de9c08077dbfa89486273286bf54`
- crates/bombay-engine/fuzz/Cargo.lock: `8f0fad594c8bd6837adf143c012735fe45ebf4d1ca6b185135fab59f70ea42dc`

## 23. Complete live-capability repair surface checkpoint

Status: user-authorized complete 89-path expansion. The user explicitly
approved both the initial request and its corrected complete inventory;
the corrected 89-path allowance governs this stage. The user removed
line-count permission checkpoints in section
22; line estimates below require no separate approval. The original request
still expressly requires a concrete checkpoint before enlarging file/public
surface. This stage repairs the independently reproduced current-version loss
of surviving actor state when a background capability operation panics.

Current canonical Bombay measurement against the original clean baseline:
production +0 / -0 / net 0; tests +4 / -4 / net 0; public types +0 / -0;
23 changed tracked paths, zero untracked paths. The delivered owning fixes add
production +65 / -13 / net 52 and test/benchmark net 549 across Communication's
ten and Behavior's fifteen distinct paths. Together with canonical Bombay this
is 48 changed repository-qualified paths; section 22's earlier 38-path count
predates the complete published dependency selection. Isolated source-retirement
and handoff comparisons are measured separately, not claimed as retained code.

The scope reviewer reconstructs the named 69-path inventory, then finds two
already updated research records missing from it: abstraction-disposition.md
and terminal-projection.md. No explicit added-file checkpoint was located for
them. The complete accounted union is 71; those two records must be included
in this authorization rather than deleted or omitted from measurement. The complete repair stage needs 23 source/consumer
paths: six production owners, with five already authorized, and seventeen
existing examples/test consumers. Eighteen additional repair paths make the proposed
authorized union **89**, including the two earlier omitted records. Pre-edit scope receipt SHA-256:
`92cf6fc7e7422b3c505503bc1e29268e2e7d2b952f58bdf1d01f38909e050aab`.
The initial scope receipt
incorrectly claimed 69/87 as the complete union; reconciliation receipt
`65cfc16d09f062cfc7eeb51ddf0db3365aa039efa631e65c0eac70432bd70df1`
corrects it without changing that frozen evidence. No production source edits
precede this checkpoint.

Additional existing paths:

- `crates/bombay/src/interpret.rs`
- `crates/bombay/tests/application_support/mod.rs`
- `crates/bombay/tests/application_terminal_custody.rs`
- `crates/bombay/tests/axum.rs`
- `crates/bombay/tests/entity_application.rs`
- `crates/bombay/tests/fifo_pool_recovery.rs`
- `crates/bombay/tests/fifo_pool_runtime.rs`
- `crates/bombay/tests/fixed_supervisor_recovery.rs`
- `crates/bombay/tests/fixed_supervisor_runtime.rs`
- `crates/bombay/tests/run_with.rs`
- `crates/bombay/tests/terminal_projection.rs`
- `examples/actor-templates/src/main.rs`
- `examples/application-topology/src/main.rs`
- `examples/axum/src/main.rs`
- `examples/counter/src/main.rs`
- `examples/entity/src/main.rs`
- `examples/supervision/src/main.rs`
- `examples/worker-pool/src/main.rs`

The six owners are existing local, interpretation, application-runtime, launch,
terminal and termination modules. The private ordinary Result/Driver-request
model and smallest failing regression are in task-custody.md. Reuse the existing
ActivationTasks join set, LocalResidual, generic Driver completion, total terminal
projection and concrete actor state. Delete first-error settlement return and
the subsequent inner-result/unwind path. Estimate net 180–300 production lines
and 350–650 test/example lines, measured after formatting; these are estimates.
Add/remove zero public types. The existing ActorRetirement gains CapabilityFailed
with its original first JoinError and available state; owned alternatives retain
later failures and one accepted unread cancellation occurrence without duplicate
causes. AllocationRejected remains untouched.

Every consumer must explicitly check or discharge these coexisting values.
In particular, the pool recovery regression must stop calling a background
operation failure an actor panic. Existing source/ARC regressions, complete
startup projections, both profiles, original-fail/repair-pass and compiled
inversions, full workspace checks, all producer/consumer contracts and independent
review remain mandatory. This bounded repair does not accept raw executor-error
erasure, projection-panic sibling loss, publication/join timing, full family
cleanup or DG-TASK. Those required seams remain in scope.


## 24. Native Entity retirement witness checkpoint

Status: user-authorized one-path test-only expansion to 90. The user explicitly
approved adding the existing native Entity test owner. Section 23 authorizes
the preceding named 89-path union. The actual native
lease owner `crates/bombay/src/entity/bombay.rs` is outside that inventory;
its existing owning tests need private access to the original actor task and
cancellation authority. Expanding the union to **90** permits the original
failure witness and ordinary same-lease comparison there. It authorizes no
production change, new public type, visibility widening or Entity lifecycle
redesign. Expected test increment: 200–400 lines, measured after formatting;
line-count checkpoints remain waived.

Current retained complete change record: canonical Bombay production +0 / -0 /
net 0; tests +4 / -4 / net 0; public types +0 / -0; 23 changed tracked paths,
zero untracked. Delivered owning corrections add production +65 / -13 / net 52,
test/benchmark net 549; the retained cross-repository union remains 48 paths.
The separately frozen live-capability candidate measures production +313 /
-59 / net 254 and tests +749 / -73 / net 676 across 23 incremental Rust paths;
no public types. It is not retained and its source-port review successor is
still being verified. Other isolated stages retain their separate receipts.

Source-derived blocker: native Entity retire requests Behavior shutdown, awaits
`actor.termination()`, then invokes its already-owned `task.retire()`.
An actor can accept shutdown without choosing Stop; acceptance is not completion.
Entity `RetirementMode::Graceful` means successful fence acknowledgement proved
command processing, not that the actor obeyed a shutdown request. `Forced`
retains failed-drain provenance. Neither mode makes that pre-cancellation wait
safe. No unexecuted failure or universal deadlock is claimed.

Witness law: preserve the actual selected lease retirement mode and exact queued
shutdown request while exercising an actor that continues after shutdown.
Compare the original pending retirement with invoking the same existing owner
cancellation/join authority, inspecting the complete actual retirement and lease
release. Use barriers and observed Pending, no timeout as a semantic deadline,
no effect inside Behavior folds. Reuse the native definition, existing task,
ActorRetirement and Entity lifecycle types. Do not expose private authority to
make an integration fixture reach it. Original-fail/comparison-pass in both
profiles and independent review remain required before any production proposal.

## 25. Actual child-binding result comparison checkpoint

Status: user-authorized one additional existing test owner. The explicit
four-file approval for sections 25–26 raises the cumulative allowance to 94.
Add only cfg(test) evidence in `crates/bombay/src/child_bindings.rs`; the already
approved `launch.rs` supplies a test-only ordinary function consuming its actual
private projection task and returning the original standard Result. No
production visibility, public type, traversal trait or production edit is
authorized by this proposal. Estimated tests +260 / -20 / net 240.

The previously reviewed actual projection fault preserves neither the original
JoinError nor later sibling results. This comparison puts actual installed tasks
into the existing established binding product, then consumes creation order,
original creation ID/kind/endpoint/control and every original join result in a
closed per-occurrence product. It compares complete available custody against
the existing formatter/early-panic path. Original failure, positive, compiled
inversion, exact restoration and strict checks are required in both profiles.

Current bindings do not store the original ChildOrigin/route nonce; CreationId
cannot substitute for it. Original typed origins retained independently by the
test caller demonstrate feasibility only. Standard birth integration, member
origin ownership and recursive descendant output remain required separately;
this test expansion does not authorize or accept that production amendment.
Preedit receipt:
`72706eaa394acc8f48913a42ef1c5361cc625ad0a1cd8877b72a98b59ed9d719`.
The signed projection baseline adds 387 test lines; the separate native Entity
comparison adds 214. Neither is silently composed into this experiment.

## 26. User-requested stable Rust pin checkpoint

Status: user-authorized three additional existing configuration paths.
Together with section 25 the authorized union is 94. Retention still requires
passing toolchain verification; initial compatibility failures are below.
The user requested updating Nix for the latest Rust. Official release and actual
distribution manifest identify stable 1.99.0, released 2026-10-01, compiler
`b940084d7` dated 2026-09-28. The isolated candidate updates `rust-toolchain.toml`,
the matching manifest hash in `flake.nix`, and only Fenix/its rust-analyzer source
in `flake.lock`. Nixpkgs, Crane, other inputs, selected Cargo dependencies and
the separately pinned Miri/fuzz nightly are unchanged. No new language feature
or architecture is selected merely by upgrading the compiler.

Measured initial pin-only three-path candidate: configuration +9 / -11 / net -2;
production/tests/public types zero. Stable manifest hash:
`sha256-zm3dyIY2T414ZRR3EhLOvptzG6gta4WZUcawzMUWtqI=`.
Fenix revision: `c8ed30fa2e75f7191a0fb8398a4a84dd009d12fc`.
Canonical remains on 1.96.0 while existing research freezes finish. Toolchain
installation, new-shell compiler identity and workspace build/test/fmt/strict
Clippy remain required before retaining the pin. Earlier frozen evidence keeps
its actual compiler version; integrated EXEC verification must use the final pin.
No existing diagnostic or gate is waived if the new compiler exposes a failure.

Measured current canonical delta before these four proposed paths:
production +0 / -0 / net 0; tests +4 / -4 / net 0;
documentation +3618 / -63 / net 3555; manifest/lock +13 / -13 / net 0;
public types +0 / -0; 23 tracked paths, zero untracked. The retained owning
corrections remain production net 52, test/benchmark net 549, 48 paths across
repositories. Isolated candidates keep their separate complete receipts.

## 27. Rust 1.99 compatibility checkpoint

Status: user-authorized eight additional existing paths, 94 to 102. The user
explicitly approved the compatibility files after inspecting this proposal.
The initial shell
identifies Rust/Cargo 1.99.0 and builds the workspace. Rustfmt changes one brace
indentation in the already approved application_runtime.rs, production +1 / -1 /
net 0. Seven compile-failure snapshots differ while their intended rejection
remains: type excerpt abbreviation, shorter qualified signatures, removal of
misleading private-import suggestions, publisher qualification and one additional
missing-Behavior-bound diagnostic. Preserve every complete actual diagnostic;
do not weaken the rejected programs or erase their static checks.

The seven additional snapshot paths are:

- crates/bombay/tests/compile/fail/actor_spaces_wrong_field.stderr
- crates/bombay/tests/compile/fail/application_child_must_be_behavior_feature_unified.stderr
- crates/bombay/tests/compile/fail/axum_wrong_root_protocol.stderr
- crates/bombay/tests/compile/fail/entity_lifecycle_representation_is_private.stderr
- crates/bombay/tests/compile/fail/run_with_wrong_root_protocol.stderr
- crates/bombay/tests/compile/fail/application_actor_projection_requires_attribute.stderr
- crates/observe-tests/tests/compile/fail/publisher_cannot_complete_twice.stderr

The eighth path is crates/bombay/src/observe/mod.rs. Clippy 1.99 rejects the
existing constant chunks_exact(8) spelling. The proposed ordinary as_chunks::<8>()
expresses the same ordered eight-byte hashing and exact remainder, removing the
unnecessary slice-to-array conversion/expect. It changes no hash algorithm,
observation state, public interface or Behavior contract. Measured proposal:
production +4 / -7 / net -3; snapshots +30 / -16 / net 14; public types zero.
Hasher patch SHA-256:
`31840dac2ae9dcfa43de4e28a409d432d8623a15eff8a17a345db3deb43ff115`.
Diagnostic proposal SHA-256:
`b1700c006059a1191af58d11c409c634617e671b329a3bd16654f00f55aff12c`.
The authorized source and snapshots are applied only in the isolated candidate;
canonical source remains unchanged. All-feature workspace tests pass in debug
and optimized builds. Strict Clippy finds additional compatibility sites below;
the compiler upgrade is not accepted yet.

The initial Clippy attempt aborts before analysis because Fenix's macOS
combination cannot find install_name_tool, silently ignoring its failed compiler
library-path adjustment. The actual selected Fenix source and Nix build log
establish this packaging defect. In already approved flake.nix, adding the
existing Darwin cctools package to the toolchain derivation's native build inputs
lets the same path-adjustment script run; actual cargo clippy --version then
succeeds as 1.99. This is a packaging correction, not an actor runtime abstraction.
No global library-path override or gate suppression is selected. The original
abort and later compiled strict-lint failure remain separate evidence.

## 28. Remaining Rust 1.99 lint compatibility checkpoint

Status: user-authorized expansion to 106 paths and the narrow child-binding
production annotation, following explicit approval of this exact proposal.
The patch remains isolated until verified and independently reviewed. The strict
all-target/all-feature Clippy discovery identifies four additional existing
paths, raising the approved cumulative union from 102 to 106:

- crates/bombay-engine/tests/driver_allocation.rs
- crates/bombay/src/address.rs
- crates/bombay/src/entity/directory.rs
- crates/bombay/src/worker_preparation.rs

The existing test-only child_bindings.rs permission also needs a narrow
amendment for one method-local lint expectation in its unchanged production
NoChildBindings implementation. This authorizes no descendant ownership change.
The other twelve paths are already approved; their complete before-source hashes
and proposed patch are frozen in rust-199-lint-proposal.json and
rust-199-lint-proposal.patch in the isolated toolchain candidate. Patch SHA-256:
`abcb8b2d455916c5933f640062eeab4d9c57d017b3274d412355327eaa90a5ab`.

Measured proposed patch, before application or rustfmt:
production +18 / -2 / net 16; tests/benchmarks +343 / -11 / net 332;
public types +0 / -0; sixteen existing paths, no new source files.
These are annotations and equivalent spellings, not new runtime capabilities.
The production additions are four method/function-local lint expectations;
the two replacements rename atomic fetch_update to try_update with identical
closures, orderings and owned results. Actual Rust 1.99 standard-library source
marks try_update stable since 1.95 and delegates fetch_update directly to it:
<https://github.com/rust-lang/rust/blob/b940084d7eb6a299eb4bfeb8e34901bc051e7ac4/library/core/src/sync/atomic.rs>.
The declared 1.96 minimum and separately pinned nightly therefore need not move.

Ordinary-Rust comparison: eagerly evaluating a trait-port body and wrapping its
result in ready changes mutation, panic and input-release timing. Returning an
async move body preserves timing but repeats signatures and syntax without a
new domain capability. Preserve the existing cold async method and use a
method-local unused_async_trait_impl expectation with an explicit reason; stale
expectations remain denied. For the existing large SpawnError, retain exact
by-value rejection rather than introducing allocation or changing the public
error to satisfy a size heuristic. Empty-lane assertions report length zero
without requiring new PartialEq/Debug bounds. No blanket lint allowance is
proposed. Reviewer /root/contract_inventory independently recommended this
bounded comparison; full upgrade review and passing verification remain open.

Original evidence: rust-199-final-verification.json records compiler, Nix
formatting, Rust formatting, workspace build and all-feature tests in both
profiles passing; strict Clippy exits 101. rust-199-clippy-discovery.log records
the additional sites without lint suppression. After authorization, require
strict Clippy, formatting and affected checks, unchanged deferred-execution
semantics, a 1.96 compatibility build for the renamed atomic calls, complete
tracked/untracked measurement and independent review of the final source.

## 29. Complete compiler consumer and owning macro checkpoint

Status: user-authorized seven-path expansion to 113 following explicit approval
of this exact compiler consumer and owning macro/release proposal. Independent
review and all retention/delivery gates remain required. After section 28,
all-feature workspace tests pass again in debug and optimized builds and strict
optimized library Clippy passes. Full all-target lint discovery now reaches
application consumers and finds additional sites. Preserve this distinction:
passing library checks does not mean the compiler upgrade is accepted.

Four additional existing Bombay paths require the same cold-operation lint
expectations or observational assertion spelling:

- crates/bombay/tests/entity_runtime.rs
- crates/bombay/tests/entity_family.rs
- crates/bombay/tests/template_application.rs
- examples/axum/src/http.rs

Three additional existing owning Behavior paths are required for the generator
correction and reviewed published release:

- crates/behavior-macros/src/lib.rs
- crates/behavior-macros/Cargo.toml
- crates/behavior-macros/CHANGELOG.md

Proposed union: 106 to 113; zero added/removed public types and no new source
files. All other measured consumers and release locks/manifests are already in
the approved union. Release-plz selects the actual package/version changes;
do not invent the final release diff or treat an unpublished patch as the
selected contract. No file extraction is included in this compatibility stage.

Measured concrete Bombay proposal: production +24 / -4 / net 20;
tests +125 / -26 / net 99 across fifteen existing paths, including the four new
paths above. The production additions are five method-local expectations in
existing examples; replacements preserve empty-lane assertions. A redundant
test import is removed. Complete typed action equality assertions retain their
existing PartialEq contract with a local expectation rather than adding Debug
requirements to owning types. Consumer patch SHA-256:
`9469e1e43127990dd6e8b3bef4748cca408cf5c81d7d191808412324f21300b1`.

Selected Macros 0.13.0 VCS revision is
`3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`; actual registry lib.rs SHA-256 is
`a4ba22289ed01515dcace9b34af5464ee88ea5f04ccd800c2a5e854134787e6a`,
also byte-identical at selected Core/Actors 0.21.0 revision. The exact generator
has eight field/value repetitions. Standard Rust field shorthand uses the same
Ident and owned expression: change only fields: fields, prior_fields:
prior_fields and field: field to their shorthand. Preserve later-field
expressions, interpretation order, concrete types, static bounds and every
Complete/Corrupt/Admitted/Closed alternative. Measured generator proposal:
production +8 / -8 / net 0, no algebra, interface or type changes. Patch SHA-256:
`b88511fb608bb6603092c7010ec40bb45e72daec761dbe34c617cf9c7128d8ee`.

The same generated warning appears in multiple unrelated applications on Rust
1.99. Fix the generator once rather than adding caller exceptions. Independent
reviewer /root/contract_inventory verified all eight exact sites and recommended
this owning correction. Existing macro crate-resolution, renaming/hygiene,
compile-pass/fail and complete multi-lane interpretation/custody regressions
must pass. Actual 1.99 downstream strict lint must fail with original selected
generation and pass with the correction. Full owning CI, independent review,
reviewed merge, registry publication and actual downstream lock selection remain
required; a local patch is experimental evidence only.

Current isolated compiler candidate before this proposal: 27 tracked source,
snapshot and configuration paths; production +23 / -10 / net 13;
tests/benchmarks/snapshots +373 / -27 / net 346;
configuration +19 / -14 / net 5; public types zero. Its complete source hashes,
patch and tracked-path measurement are frozen in rust-199-current-measurement.json;
scratch scripts/logs/proposed patches remain separate untracked evidence and
must enter the final complete receipt. Canonical production is unchanged.

The approved consumer patch is applied only in the isolated candidate. One
initial expectation used a nonexistent lint name; correct it to
clippy::manual_assert_eq and preserve the original complete transition equality.
That compiler rejection is excluded from semantic and original-defect evidence.
The corrected comparison passes formatting, all-target/all-feature strict Clippy
and all-feature workspace tests in both profiles on actual Rust 1.99. The
experimental Cargo patch selects only the changed macro source; all other
dependency versions remain unchanged. This is not a published dependency or
canonical toolchain acceptance. The unchanged minimum 1.96 also builds the
annotated Bombay library and equivalent atomic renames successfully.

Owning macro commit `41f9d425aa245064a2b64eca81bbc4484d948f26` changes only the
eight generator lines. Original published expansion fails the targeted strict
1.99 lint in both profiles; corrected expansion passes both. Owning workspace
nextest reports all 857 tests passed; formatting and all-target/all-feature
strict Clippy pass through the pinned 1.96 shell. The authoritative owning Nix
flake check passes all ten aarch64-darwin checks; other architectures require
the PR's CI. Complete one-path source receipt:
`6d0f4095a7387f09e3c9c4c5e380678c187bf89128b089a1459ac27e8ec73fdb`.
Production +8 / -8 / net 0; tests +0 / -0; public types +0 / -0;
zero untracked owner paths. Evidence is stored separately from the owner tree.

Focused owning [PR 80](https://github.com/devrandom-labs/bombay-behavior/pull/80)
was independently approved by /root/contract_inventory at that exact head
([review 5399361168](https://github.com/devrandom-labs/bombay-behavior/pull/80#pullrequestreview-5399361168)). All fourteen checks passed, including owning
Nix, eight mutation shards and their aggregate, CodeQL, API audit and dependency
policy. It merged as `92ed7c9b59fc008f851e7bff0c8c0733b1005b65`.
Independent receipt SHA-256:
`3ec6523927d23258e78a1d3b7b9c8c80621410c7a4e0d26e40afe76bcaa66404`.
The original receipt mistakenly counted seven local Darwin checks; its log
contains ten. Original evidence is unchanged; immutable correction/merge receipt
SHA-256 `bf4bd81817ce682d2985081bb57c935418a2eb753e0419125b23f4a831767427`
authenticates that correction. Passing main CI run 37103572686 triggered normal
release run 37103992018, which opened
[release PR 81](https://github.com/devrandom-labs/bombay-behavior/pull/81).
Registry publication is pending. The generated head
`dadc1becde47d0787b770fb19a30e21bf34b61fa` announces Macros 0.13.1 and
Core/Actors 0.21.1 but leaves the shared package/dependency versions at 0.21.0;
three nested locks also fail `--locked` preflight. Repair these existing approved
manifest/lock paths to match the generated candidate versions, then rerun the
complete preflight, owning Nix gate, CI and independent release review. The approved compiler candidate still
requires registry release selection, final complete source/delta receipt and
independent review before canonical retention. Full EXEC semantic gates,
correctness-first module extraction and Bombay delivery remain open.


Release PR 81 correction is pushed at
`0e2756a9b0eda4002854140190ce2dbf306ba7fa`. The complete release diff has
nine existing paths: configuration +21 / -21 / net 0; changelogs +18 / -0;
production/test/public types zero; untracked zero. All four locked graphs now
resolve. The release skill preflight passes packaged-consumer verification for
Core/Actors 0.21.1 and Macros 0.13.1; the authoritative owning Nix check passes
all ten local Darwin checks. Source/patch/log receipt SHA-256:
`28b8c1b34b0dcc94fdaaae4d3755632ae398aac747e2c4c6721c4929b84f160d`.
Independent release reviewer /root/contract_inventory approved exact head
([review 5399435924](https://github.com/devrandom-labs/bombay-behavior/pull/81#pullrequestreview-5399435924)); final review receipt SHA-256
`0f2a6c19a55a7ccf069539980625d7655c2490c6f1c7704dfe96d5df46c6c1fa`.
Root lock changes four local versions, including unpublished inherited Testkit;
each nested graph changes exactly the three published local packages.
All fourteen exact-head checks passed in CI run 37104305190. Reviewed release
PR 81 merged as `5ca96444f0a66e9a013b6989e3e53d345cbabf65`; normal main
CI run 37104928777 must pass before publication runs. Actual registry/tags and
downstream selection remain pending.
Both README installation constraints remain 0.21 and correctly include 0.21.1.
No release source semantics change.


Main CI 37104928777 and normal release workflow 37105423987 both succeeded
at merge `5ca96444f0a66e9a013b6989e3e53d345cbabf65`. Actual registry
archives and annotated release tags are verified:

| Package | Published version | Registry checksum |
| --- | --- | --- |
| bombay-behavior-macros | 0.13.1 | fdbea4c696f3bed02965835fd253087d696a25bf206e229e174bd2882fc2638c |
| bombay-behavior | 0.21.1 | b82e4373287b71f2f90a2a16c62da9f6bebb8dd282df5a0b4b6e4aaa9a5d411c |
| bombay-behavior-actors | 0.21.1 | 444670302a1b8e34b9f721ed0f071383d0f27f100e26e8365bf7c14d39195639 |

All package VCS metadata and annotated tags resolve to that merge. Each packaged
Rust file is byte-equivalent to its actual owning source: two macro, twenty-eight
Core and 172 Actors files. The exact selected revision’s full AGENTS was reread
and is unchanged from the previous verified release. Core/Actors Rust semantics
are unchanged; the only owning syntax change is the eight macro initializers.
Canonical Bombay still selects 0.21.0 and Rust 1.96 while final verification is
pending. The isolated 1.99 candidate restores the original registry manifests/
lock before selecting the published versions; the experimental macro path patch
is not retained. Current contract pointers and each independent lock graph must
match the final selection before canonical compiler acceptance.

The first published-candidate verification attempt is excluded: concurrent
in-source evidence creation caused a Nix file-set evaluation failure, and the
parallel flake check exhausted disk during compilation. Neither is a Rust
semantic failure. Preserve both original logs, place new evidence outside the
source tree, and rerun Cargo checks sequentially before the full Nix gate.
Compiler acceptance remains blocked until the final frozen checks and independent
review pass.

Independent final compiler classification found an error in the intermediate
production aggregates in sections 28–29: cold-trait lint annotations outside
`cfg(test)` were counted as tests. Preserve the earlier frozen measurements as
historical, explicitly superseded counts. Current source has 125 net production
lines, including these annotations; the final complete per-line measurement
and receipt must use actual conditional ownership. No new type or semantic
operation is introduced by those annotations. The user waived line-count
approvals; the approved 113-path/zero-added-public-type scope still applies.

Final published-dependency Cargo checks now pass on the isolated Rust 1.99
candidate: compiler identity, Cargo formatting, Nix formatting, locked workspace
build, all-target/all-feature strict Clippy in both profiles, and all-feature
workspace tests in both profiles. Evidence is outside the source tree at
`/tmp/bombay-rust-199-published-final`. Source inventory SHA-256
`f53da2bf09885317ee6e416905960689a8efaee18bee063553556c14acdecf53`
records all 345 archived tracked paths, 53 changed paths inside the approved
113-path set, and all 75 isolated untracked evidence paths. The 21-check Nix
gate and final independent source/delta review remain pending. Canonical
selection is unchanged; no compiler or semantic gate is approved by these
intermediate results.

## 30. Complete minimal-feature compiler diagnostic scope (2026-10-03)

The final full Nix gate fails the existing no-default-features child-authoring
check. Rust 1.99 still rejects the unchanged invalid program with E0277, but
reports an additional complete E0277 diagnostic that its distinct non-axum
snapshot lacks. Existing axum-enabled fixture/snapshot is already approved
and passed; do not conflate their diagnostics or weaken either check.

Propose adding exactly one existing path to the approved allowance, 113 to 114:
`bombay/crates/bombay/tests/compile/fail/application_child_must_be_behavior.stderr`.
Concrete expected patch: tests +22 / -0 / net 22; production +0 / -0;
public types +0 / -0. The invalid `.rs` program, test selection and compiler
bound remain unchanged. Exact proposed snapshot SHA-256:
`e31df5eb30192c9268cd12f27e4cc445e4646798c8cd87640a0972a9dae20f18`.
Original failed gate and complete actual compiler output are preserved outside
the tree. Current isolated 53-path measurement: production +138 / -13 / net 125;
tests +411 / -58 / net 353; documentation +44 / -44 / net 0;
configuration +32 / -27 / net 5; no new public types. Independent conditional
measurement SHA-256 `0a25d2d200367071f6a894062561f8a87e5d6a68c09867f556b8070632224a16`.
The source inventory includes all 75 untracked isolated evidence paths. The
proposed snapshot is external evidence until explicit file-scope authorization.
After approval, rerun both feature configurations and all 21 Nix checks, freeze
the new complete source/delta, and obtain independent final compiler review.
No toolchain or full EXEC acceptance is claimed from the twelve successful
checks before that gate failure; cancelled checks remain unpassed.

User explicitly approved section 30’s one-file expansion on 2026-10-03.
The allowance is now 114 cumulative paths, with zero added public types;
line-count approvals remain waived. Apply only the recorded 22-line expected
diagnostic, then obtain fresh complete verification and independent review.

## 31. Preserve selected Loom atomic compatibility (2026-10-03)

The remaining Entity Loom gate reveals that the standard-library try_update
rename cannot be used on the selected Loom 0.7.2 AtomicU64. Its actual owning
source and atomic_int tests provide fetch_update only. Preserve the exact
ordering, checked increment and returned previous-value law. Within the already
approved directory.rs path, compare conditional selection of the existing
function item: Loom fetch_update under bombay_entity_loom and std try_update
otherwise, with one unchanged closure/call. No duplicated algorithm, wrapper,
public type, dependency update or deprecated-warning suppression is required.
Expected production increment approximately four net lines; no new test paths.
The failed compiled Loom invocation is preserved as compiler-compatibility
evidence, not an original semantic runtime-defect witness. Actual pinned-Nix
Loom checks and normal strict lint must both pass before retaining this change.
Final complete source hashes, conditional counts and independent review must
be refreshed; earlier source inventory f53da2bf remains historical.

The first conditional function-item comparison requires the closure argument’s
already-owned u64 type to be explicit (E0282); record that inference cost, not
a new architectural requirement. Preserve its failed build/full-gate logs and
refresh the source freeze after this ordinary annotation. No semantic runtime
regression is claimed for that compiler veto.

Final typed function-item source freezes all 345 tracked paths, 54 changed
paths and 75 untracked isolated evidence files. Inventory SHA-256:
`af836ddc4fade1bbfbd87ee0cc95b485512df9504a352103f2eb93a772844199`.
Independent recomputation SHA-256:
`3d7eab9aedb75babcc57c25ae34358aa9d7fe8502b5dfedec57a7708286d6998`.
Measured candidate delta: production +150 / -18 / net 132; tests +433 / -58 /
net 375; documentation +44 / -44 / net 0; configuration +32 / -27 / net 5;
zero new public types. All 54 source paths are within the approved 114-path
allowance. The function-item comparison changes directory.rs by +13 / -6 /
net 7 from the archived original; its single typed closure preserves the exact
existing order/result/checked-increment law. Final checks and independent
compiler acceptance remain pending; no canonical source is changed.

## 32. Attribute coverage allocation counts before changing the oracle (2026-10-03)

The typed compiler candidate passes all eight Cargo checks. Its full Nix gate
passes fifteen named checks, including both child feature modes and actual
Entity Loom, but fails owner coverage: the unchanged one-settlement-queue
allocation test measures six allocations against its required one. Five checks
are cancelled and must still run. Do not subtract five, skip the test, or relax
the allocation law based on an instrumentation hypothesis. Preserve the actual
complete coverage log at typed-owner-coverage-failure.log outside the tree.

The original global counter covers all threads and includes construction, poll
and the successful retirement assertion. Authorize only diagnostic research
in the already approved engine/tests/driver_allocation.rs test path: fixed
allocation slots, const nonallocating thread-local closed measurement phases
and before/after snapshots may establish source attribution. No new unsafe,
production code, public types or test threshold changes. Expected approximately
100 test lines; root-approved preedit receipt SHA-256:
`e98e9afb230405576a4988f84b607e3d0c2670c8c71455158dbe41dea8c1ce10`.
Original/diagnostic and normal/actual coverage comparisons must run through the
pinned 1.99 shell and keep the one-allocation assertion. Other-thread/Outside
classification alone cannot claim an exact thread identity. Final compiler
acceptance remains blocked until the attribution, any justified smallest repair,
complete source/delta freeze, all gates and independent review pass.

The five cancelled independent Nix checks subsequently pass. Forty-eight
focused original/diagnostic comparisons pass, as do the exact diagnostic owner
coverage and one exact unedited original owner-coverage rerun. The historical
six-allocation provenance remains unknown; lack of reproduction is not a
diagnosis. Provenance receipt SHA-256:
`dad223d2d0088a93e0ac06a6f8a93421c037f4cc6add6800ff9571f574c852c1`.

Before retaining the test, compare a deterministic attribution witness in the
same approved test path: five real retained allocations on another scoped
thread during the measured interval make the global oracle misattribute work
to Driver. A const thread-local closed Idle/Active counter can keep Driver’s
one-allocation requirement without subtraction. Include future construction
and polling, finish before assertion/spawn/join overhead, release/join the worker
before assertions, and require an extra same-thread allocation inversion to
fail with two. No new unsafe, production or public types; expected approximately
90 test lines. Root-approved preedit receipt SHA-256:
`f6582dd44b1a2ed2d20c6a634bf824883ffc7b3c226a45d3d80d3276f8da9988`.
This proves the measurement flaw if the comparisons pass; it does not identify
the historical six allocations. Independent review, complete final source/delta
and actual all-gate rerun remain required before canonical compiler acceptance.

The user clarified that production code must be condensed while tests are
exempt from that condensation requirement. Keep meaningful regressions and
inversions; this does not change correctness gates or authorize additional
files or public types.

The final isolated allocation correction changes one approved test path by
+135 / -3 / net 132, with no production, public-type or unsafe additions.
Its checked thread-local counter has an absorbing Overflowed state: overflow
cannot wrap into a plausible count, and rejection occurs outside allocation.
The deterministic five-other-thread witness, same-thread extra-allocation
inversion and numeric overflow inversion compile and fail for their intended
laws in both profiles; all three restored positives pass normal and LLVM
coverage in both profiles. Strict lint, formatting and exact owner coverage
pass. Receipt SHA-256:
`88eb0a57a5ac1732fd88a9dc5e0e963713aad968c875e0c6f17f913f07a019f0`.
The numeric boundary test does not claim physically performing usize::MAX
allocations. Historical six-allocation provenance remains unknown.

Combine only this frozen test correction with the frozen compiler candidate
for independent review and final gates. Combined inventory SHA-256:
`93a57d98d376a049e53a942e68164777c8ee42ac9f669d47608df890542f27fe`.
All 345 selected source hashes are checked; only the allocation-test path
differs from the typed predecessor. The 54 changed source paths remain within
the approved 114-path allowance. Final combined verification and independent
acceptance are pending; canonical production remains unchanged.

Final independent compiler acceptance authorizes retaining this bounded stage,
not any EXEC semantic/design gate. Review SHA-256:
`f4da28e858c9d8d51cc835830a6c5f423bc236c209590ca0b3e1f378f69297b0`.
It binds the separate nonauthor allocation review
`94b9b905e0f5ab6a5833c7f9fb1d3bc6fdf09d4dd287a1d2462f99bea4ac1527`,
all eight completed Cargo commands (receipt
`22863b8cd555c61015841a44fdad95afe597d31066d5c88e532a20ce6c9e08e0`)
and all 21 native Nix checks (receipt
`569843befb092fe2e44c1744ae71b6cc07784880e8b042ee72d2de13b902d42d`).
The complete source and evidence freeze remains unchanged after verification
(postcheck receipt
`3d4b623c3d02863cd63724f2fa434a7ce713f344f3c91c8aa2e674f65bdf6849`).
Other declared platforms require remote CI; this is not a claim that those
platforms or full EXEC acceptance passed.

Canonical retention authenticates all 54 destination files before copying,
preserves unrelated existing records, and verifies every transferred hash.
Transfer receipt SHA-256:
`fa87ba5b7ea7b6a7a6abf5e017bb23ec406f04829763d502e9c7d50231e44aca`.
The retained selection is stable Rust 1.99.0, Core/Actors 0.21.1 and Macros
0.13.1 at release revision `5ca96444f0a66e9a013b6989e3e53d345cbabf65`.
Registry checksums are Core
`b82e4373287b71f2f90a2a16c62da9f6bebb8dd282df5a0b4b6e4aaa9a5d411c`,
Actors `444670302a1b8e34b9f721ed0f071383d0f27f100e26e8365bf7c14d39195639`,
and Macros `fdbea4c696f3bed02965835fd253087d696a25bf206e229e174bd2882fc2638c`.
All 202 packaged Rust files authenticate the release source; Core/Actors
bytes match 0.21.0, and Macros changes only the reviewed eight syntax lines.
The complete selected 760-line AGENTS.md remains unchanged, SHA-256
`2b7a9195b27f073fec18426da43e9840f8ef668f333b9ad55934f520a37ae226`.
Timers remains the only patch; no experimental macro path patch is retained.
Current dependency pointers and executable manifests move together; dated
research, source hashes and earlier compiler failures remain historical.

The retained candidate measures production +150 / -18 / net 132; tests
+568 / -61 / net 507; configuration +32 / -27 / net 5; zero new public
types. Measurement SHA-256:
`5c4bf46d6d57bf1107472a1dc2a2083e675a057935dcfb68fb94eabaab80d50f`.
Cold trait futures retain execution timing and exact rejected inputs; no
actor law is reconstructed for compiler convenience. Actor ownership repairs,
full decision gates, module extraction, final minimization and reviewed Bombay
PR delivery remain required.

## 33. Exact observation ownership scope proposal (2026-10-03)

Current approved cumulative scope is 114 paths and zero new public types.
The retained compiler changes are committed as `f950fdf` (compiler and selected
release) and `5576d7b` (allocation-test correction). Canonical production is
+150 / -18 / net 132; tests +568 / -61 / net 507; public types +0 / -0.
The complete tracked/untracked delta is in verification.md; source-condensation
preference exempts tests, and line-count approvals remain waived.

The observation repair proposes 11 additional existing Behavior files, giving
125 cumulative paths, and at most three direct public owners:
ObservationSequence owns deterministic request identity derivation;
ObservationRelationship<P> owns the accepted relationship identity and its
protocol; ObservationAuthority<P> owns one cancellation permission. This
replaces reusable numeric cancellation IDs with exact, protocol-matched
authority and preserves original rejected requests. Bombay interprets these
requests; Behavior Actors owns their reusable protocol and template policy.

Additional existing files (relative to the Behavior repository):

- crates/actors/src/lib.rs
- crates/actors/src/protocol/established.rs
- crates/actors/src/protocol/mod.rs
- crates/actors/src/shutdown.rs
- crates/actors/tests/established_capabilities.rs
- crates/actors/tests/interpreter_request_settlement.rs
- crates/behavior-testkit/fuzz/fuzz_targets/catalogue_sequences.rs
- crates/behavior-testkit/tests/exact_termination_model.rs
- docs/adapter-contract.md
- docs/engineering/public-surface-inventory.md
- docs/engineering/template-law-audit.md

The 32-path observation stage reuses 21 already approved files; these eleven
are its complete additional set. Current manifest SHA-256:
`ac72a749eab838d7535f04f3a624fffc3d48f04b070747ce81ee76ae46d44303`.
The prior independently reconciled model checkpoint
`4cac787e009a6cd5623294be7528e76e406197fea474a297ec62f6ef7e53704d`
contains all five abstraction answers and selected source hashes; its dated
113-to-124 arithmetic is superseded only by this 114-to-125 reconciliation.
The private ordinary-Rust reservation comparison and independent review in
observation.md establish bounded feasibility, not three-owner minimality or
actual producer/Monitor correctness. Selected 0.21.1 owning Rust sources are
byte-identical to that 0.21.0 research baseline.

No new owning source or public type is authorized by this proposal. Budget
permission would allow the exact owner repair and its consumers, tests and
release; complete ordinary-Rust comparisons, actual rejection/publication
races, protocol denials, independent gates and minimization remain mandatory.
No fourth public type, registry or duplicate actor contract is proposed.
