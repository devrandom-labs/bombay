# Bombay architecture backlog

This is the canonical executable backlog. It contains only current selection
rules, dependency state, unfinished-item contracts, and runtime invariants.
Completed campaign narratives are intentionally omitted from this public tree.

## Working rules

For every feature:

1. Inspect the exact locked source, public API, tests, and documentation for
   Bombay Behavior, Observe, Timers, Communication, and Address; also inspect
   Transition and Machine Executor when the execution path is affected.
2. Record the feature-specific verification and ownership map here before
   moving the item from `blocked` to `active` or changing implementation.
3. State the observable invariant and add an inversion test that proves it.
4. Compose existing typed primitives. Do not add `dyn`, `Any`, downcasts,
   parallel spawn paths, runtime policy, or a duplicate Behavior protocol.
5. Before completion, try to remove every added type, trait, alias, field,
   task, channel, adapter, and public export that lacks an independent
   invariant.
6. Reconcile this ledger, run workspace tests, rustfmt, Clippy, and
   rustdoc, then use `feature-complete`. Use `distilled` only after the final
   project-wide ownership and interface audit. Never use `done`.

Priority is `P0` through `P3`; points are relative size. States are
`unblocked`, `active`, `blocked`, `feature-complete`, and `distilled`.
`Blocked by` and `Unblocks` are inverse edges until the prerequisite is
complete. Repair any disagreement before selecting work. Select from the
dependency graph, not table order. An item blocked only by its own fresh
cross-crate verification is eligible for verification, not implementation.

## Current dependency graph

All runtime-foundation and engine-path items are distilled: A1-A4, A6,
B1-B8, C1-C3, C6-C8, D1-D5, F1, V1, X1-X7. Their evidence is historical and
must not be treated as a waiver of per-feature verification.

| ID | Work | Priority | Points | State | Blocked by | Unblocks |
|---|---|---:|---:|---|---|---|
| A5 | Executor conformance beyond Tokio | P2 | 3 | blocked | C5; a second executor | M2 |
| C4 | Hung-fold detection capability | P3 | 8 | blocked | fresh verification; concrete policy owner and oracle | operations policy |
| C5 | Additional router/clock implementations | P3 | 8 | blocked | fresh verification; concrete second consumers | A5, P1 |
| F2 | General control-protocol decision | P2 | 5 | blocked | concrete second priority protocol | M2 |
| F3 | Typed reply-to and timeout composition | P1 | 5 | feature-complete | — | M2 |
| F4 | Backpressured stream-ingestion adapter | P1 | 8 | blocked | fresh verification; `mnesis-bombay` Phases 0-4 | M2 |
| F5 | Typed heterogeneous fan-in decision | P2 | 5 | blocked | concrete fan-in invariant | M2 |
| F6 | Replacement-incarnation observation protocol | P1 | 5 | feature-complete | — | M2 |
| F7 | Transactional root activation | P0 | 5 | feature-complete | — | M2 |
| R1 | Supersede legacy Bombay with this runtime and publish renamed crates | P0 | 5 | feature-complete | — | M2 |
| L1 | Worker-pool and key-persistent routing library | P2 | 8 | blocked | concrete library consumer | M3 |
| L2 | Typed local pub/sub boundary | P2 | 8 | blocked | concrete group consumer; Zenoh boundary | M3 |
| L3 | Intake stashing and typed admission refusal | P1 | 5 | feature-complete | — | M3 |
| L4 | Graceful in-flight worker draining | P1 | 8 | feature-complete | — | M3 |
| L5 | Retry scheduling and backoff composition | P1 | 5 | feature-complete | — | M3 |
| L6 | Queryable application reporting | P1 | 5 | feature-complete | — | M3 |
| L7 | Reusable scheduler-library decision | P2 | 5 | blocked | invariant beyond Behavior timers | M3 |
| AF0 | Comparative actor-system feature research and canonical feature table | P0 | 8 | blocked | fresh Akka, Erlang/OTP, Kameo, and complete local cross-crate verification | S1, E1, E2, E3, E4, E5, E6, E7, E8, E9 |
| E1 | Facade builders and authoring macros | P1 | 8 | blocked | AF0 | E9 |
| E2 | Static type-inference diagnostics | P1 | 5 | blocked | AF0 | E9 |
| E3 | Cookbook and public usage guidance | P1 | 8 | blocked | AF0 | E9 |
| E4 | First-class actor definition and one-value spawn | P0 | 3 | blocked | AF0 | E9 |
| E5 | Bombay-owned application routing | P0 | 8 | blocked | AF0 | E9 |
| E6 | Concise nominal behavior authoring | P1 | 5 | blocked | AF0 | E9 |
| E7 | Live-actor capability and terminal-result ergonomics | P1 | 5 | blocked | AF0 | E9 |
| E8 | Facade, prelude, and system-construction coherence | P1 | 3 | blocked | AF0 | E9 |
| E9 | Expressive reference-application distillation | P0 | 8 | blocked | AF0, E1, E2, E3, E4, E5, E6, E7, E8 | M4 |
| P1 | Single-threaded/no-std portability decision | P3 | 8 | blocked | C5; concrete embedded consumer | M2 |
| Q1 | Reproducible CI and build matrix | P0 | 5 | feature-complete | — | M5 |
| Q2 | Coverage and integration breadth | P1 | 8 | feature-complete | — | M5 |
| Q3 | Concurrency, property, fuzz, and Miri gates | P1 | 13 | feature-complete | — | M5 |
| Q4 | Mutation-testing gates | P1 | 8 | feature-complete | — | M5 |
| Q5 | Allocation and competitive benchmark gates | P0 | 13 | feature-complete | — | M5 |
| Q6 | Doctest and panic-mode gates | P1 | 5 | feature-complete | — | M5 |
| Q7 | Lifecycle observation ordering contract | P0 | 3 | feature-complete | — | M5 |
| S1 | Comparative actor-model and composition synthesis | P0 | 13 | blocked | AF0 | M5 |
| M2 | Runtime-operations milestone distillation | P1 | 8 | blocked | A5, F2, F4, F5, P1 | FV1 |
| M3 | Optional-library milestone distillation | P2 | 8 | blocked | L1, L2, L7 | optional release |
| M4 | Developer-experience milestone distillation | P1 | 8 | blocked | E9 | FV1 |
| M5 | Competitive-verification milestone distillation | P1 | 13 | blocked | S1 | FV1 |
| FV1 | Competitive local-framework release audit | P1 | 13 | blocked | M2, M4, M5 | framework release |
| K1 | KERI identity/authority handoff | P2 | 8 | blocked | bombay/KERI integration repository and ledger | K2 |
| K2 | Zenoh transport/discovery handoff | P2 | 13 | blocked | K1; owning integration repository and ledger | K3 |
| K3 | Authenticated remote-actor handoff | P2 | 13 | blocked | K2; owning integration repository and ledger | remote framework |

The previous E1-E9 authoring campaign is not an adequate feature inventory for
the public actor system. Its artifacts remain historical evidence, but all of
those items are reopened and blocked by AF0. AF0 is the only next item and is
eligible for research and verification only. It must study established actor
systems and the complete local dependency/runtime implementation, then produce
the canonical feature table, actor-role inventory, composition matrix,
ownership boundary, and evidence-backed dependency graph. Implementation item
IDs and ordering may be created only from that research result. No engine,
runtime, macro, facade, example, or public-API implementation may start first.

### AF0 required research deliverables

AF0 is one architecture-research feature with three inseparable outputs:

1. **Canonical actor-system and ecosystem feature table.** Compare Akka,
   Erlang/OTP, Kameo, and the current Bombay repositories. Inventory every
   feature exposed by those systems—not only Bombay's current local-runtime
   scope—and list every actor role and reusable capability,
   including ordinary actors, supervisors, proxies, routers, pools, hierarchy,
   watching/linking, timers, shutdown/finalization, restart policy, stashing,
   state machines, actor references, spawning, and terminal observation. For
   each row record user intent, system-owned machinery, observable guarantees,
   failure semantics, authoritative evidence, and eventual classification as
   core runtime, ready actor template, optional library, external integration,
   or explicitly rejected capability. Classification happens after inventory;
   absence from today's Bombay code is not grounds for omission.
2. **Behavior Engine black-box contract.** Audit the current Engine, Behavior,
   Transition, Machine Executor, `Incarnation`, and `ActorEnvironment`. Record
   exactly what one engine run consumes, returns, and owns; where the event
   loop and executor live; where the Tokio task is launched; how cancellation,
   panic, initialization, effect interpretation, and retirement are ordered;
   and which currently public engine types or lifecycle phases are accidental
   implementation surface.
3. **Layer and ownership map.** Draw the complete dependency direction from
   application behavior and actor templates through Behavior composition,
   Engine execution, Bombay environment interpretation, incarnation ownership,
   shared system services, and the final public API. Every mechanism must have
   exactly one owner. The application layer may express domain behavior,
   topology, and policy only; it may not implement interpreter, routing,
   registration, executor, lifecycle-turn, or error-product machinery.

AF0 must also produce a typed composition matrix showing every supported and
rejected permutation of actor roles and capabilities. Only after these tables
are reviewed may the ledger create implementation features for Engine repair,
runtime layering, reusable actor templates, interpreter encapsulation, or the
public authoring API. The public API is necessarily the final implementation
layer.

### AF0 research record — actor-system feature inventory

Research is in progress; this table is the candidate inventory, not an
implementation plan or completion claim. Authoritative inputs currently
include Akka Typed's actor index, lifecycle, supervision, interaction,
router, timer, dispatcher, mailbox, stash, FSM, and coordinated-shutdown
guidance; Erlang/OTP's process, `gen_server`, supervision-tree, supervisor,
link, and monitor contracts; Kameo's current actor, spawn, supervision, link,
pool, pub/sub, stream, mailbox, and lifecycle documentation; and the complete
local Bombay source and dependency graph. Every row must be verified against
all applicable owners before AF0 becomes active.

| Candidate actor-system feature | User supplies | Actor system supplies | Current Bombay evidence/status |
|---|---|---|---|
| Ordinary typed actor | State, message protocol, domain reaction | Mailbox loop, serialization, task, reference, stop | Behavior and Engine exist; application boundary is not yet distilled |
| Actor definition/template | Constructor arguments and policy choices | Reusable recipe for every new incarnation | `Actor<B>` is only an inert value; no complete reusable template inventory |
| Spawn and typed reference | Address and actor template | Allocation, initialization, registration, task launch, live reference | Exists, but public generic bounds expose interpreter wiring |
| Mailbox and backpressure | Capacity/policy choice where relevant | Priority/control lane, bounded delivery, payload recovery | Communication-owned primitive exists and is composed by Bombay |
| Parent/child hierarchy | Child templates and topology | Ownership, start/stop ordering, recursive retirement | Typed births and child leases exist; authoring model needs research |
| Supervisor | Strategy, restart policy, child templates | Failure monitoring, restart decisions, budget, escalation | Behavior supplies a supervisor algebra; application composition is verbose |
| Worker | Domain behavior and construction arguments | Placement under supervisor/pool and incarnation replacement | Worker is a contextual role, not a separate engine type |
| Stable proxy | Worker protocol and replacement policy | Stable endpoint across replaceable worker incarnations | Behavior `Proxy` exists; topology and endpoint wiring leak into applications |
| Router | Routee template/group and selection policy | Forwarding, routee lifecycle, routing strategy | No distilled Bombay actor template; endpoint routing traits are lower-level machinery |
| Worker pool | Worker template, size/keying, policy | Routees/proxies, scheduling, supervision, replacement | Behavior pools exist; Bombay integration and public template remain unproven |
| Nested supervision | Child supervisor templates | Arbitrary typed supervision trees | Algebra appears composable; full permutation matrix is not recorded |
| Watch/monitor | Target reference and domain reaction | Exact-generation terminal notification and cancellation | Behavior/Observe/Bombay primitives exist |
| Link/failure propagation | Linked actors and reaction policy | Bidirectional relationship and failure propagation | No general public link contract; must not be inferred from watch |
| Timers/deadlines | Timer identity, schedule, reaction | Generation replacement, stale rejection, lifecycle cancellation | Behavior and Timers primitives exist; Bombay interprets them |
| Receive timeout | Duration and reaction | Rearming rules tied to actor traffic and incarnation | Behavior wrapper and Bombay oracle exist |
| Request/reply | Request protocol, reply destination, correlation policy | Typed destination and optional convenience pattern | Typed `Recipient<B>` composition exists; no runtime request registry is allowed |
| Stash | Capacity/order and release policy | Buffered protocol handling with deterministic replay | Behavior owns stash algebra; Bombay should add no duplicate stash |
| Finite-state behavior | States and transitions | Standard behavior/state composition | Behavior owns machine/state primitives |
| Graceful shutdown | Domain finalization/drain policy | Priority shutdown delivery, child ordering, terminal retirement | Current wrappers/runtime exist; application ergonomics remain unresolved |
| Coordinated system shutdown | Global phases and participants | System-wide ordered termination | Not established as a local Bombay feature; requires a concrete invariant |
| Lifecycle hooks/signals | Domain reactions only where semantically needed | Start, restart, stop, terminal ordering | Bombay lifecycle reporting exists but is overly public |
| Restarted incarnation | Fresh template plus restart policy | New engine, behavior, mailbox generation, timers, and observations | Supervisor/proxy path exists; exact template ownership needs audit |
| Delivery failure | Domain decision when recoverable | Preserve payload and distinguish unknown/closed/rejected | Typed delivery errors exist; composed runtime errors leak mechanically |
| Registration/discovery | Stable logical address where needed | Exact-generation claim, resolution, and release | Address/Bombay adapters exist; user-facing topology is unresolved |
| Dispatcher/executor selection | Explicit policy only for a proven use case | Scheduling and serialized turns | Engine uses Machine Executor; Tokio task launch is in `Incarnation` |
| Actor testing | Behavior or complete template under test | Synchronous behavior test and asynchronous runtime test facilities | Engine/runtime tests exist; one black-box engine API does not |
| Stream attachment | Stream and backpressure policy | Lifecycle-bound ingestion into actor mailbox | Existing F4 remains blocked by a concrete consumer |
| Stream processing | Sources, transforms, sinks, materialization policy | Backpressure, cancellation, supervision, fan-in/fan-out, lifecycle | Full Akka Streams/Kameo attachment comparison still required |
| Pub/sub | Topics, membership, publication protocol | Typed subscription, fan-out, membership lifecycle, failure handling | Optional L2 candidate exists; complete actor-template contract is unresearched |
| Event bus | Event classification and subscription policy | Local typed/untyped broadcast and lifecycle cleanup | Not inventoried in current Bombay scope |
| Broadcast router | Routees and broadcast policy | One-to-many delivery and aggregate failure semantics | Not implemented as a ready template |
| Round-robin/random/consistent-hash router | Routees and key/selection policy | Standard reusable selection algorithms | Behavior pools cover part of this space; comparative inventory required |
| Scatter-gather / first-response | Request and completion policy | Fan-out, timeout, winner selection, cancellation semantics | No core contract; must be researched |
| Tail chopping / hedged request | Request, delay, retry targets | Staggered requests and first-success selection | No core contract; must be researched |
| Work pulling / balancing pool | Work protocol and capacity | Demand-aware worker scheduling and replacement | Pool scheduling semantics require comparison |
| Throttling / rate limiting | Rate and overflow policy | Admission timing, buffering, refusal | No canonical Bombay template |
| Batching / aggregation | Batch limits and flush policy | Timers, buffering, downstream delivery | Expressible in domain behavior; reusable-template decision unresearched |
| Async result adaptation / pipe-to-self | Future and result mapping | Lifecycle-safe conversion into actor messages | No distilled public facility |
| Receptionist / service discovery | Service key and registration intent | Dynamic typed discovery and listings | Address routing is not a receptionist; capability absent |
| Actor selection / path lookup | Logical path and resolution policy | Hierarchical lookup and failure semantics | Bombay addresses are exact local designations, not paths |
| Cluster membership | Node participation and failure detector policy | Membership, reachability, convergence events | External/distributed scope; inventory still mandatory |
| Cluster-aware routing | Routee discovery and placement policy | Routing across live cluster members | Absent; transport/discovery handoff must be mapped |
| Cluster sharding | Entity identity and allocation/passivation policy | Single logical entity placement, rebalance, buffering | Bombay Entity/Nexus boundary must be researched, not assumed |
| Singleton | Singleton identity and failover policy | Cluster-wide unique active instance | Absent; distributed integration candidate |
| Distributed data / CRDT | Replication and consistency choice | Convergent replicated state | Outside local runtime but mandatory ecosystem inventory |
| Remote deployment | Placement and protocol contract | Remote spawning, serialization, lifecycle/failure mapping | Explicit external integration candidate |
| Remote actors | Remote protocol and authority integration | Transport, discovery, serialization, failure semantics | Explicitly outside local core; K1-K3 handoff |
| Serialization and protocol evolution | Wire schema and compatibility policy | Encoding, manifests, version negotiation, rejection | External boundary; no local-core duplication |
| Persistence | Durable event/state policy | Recovery and durable storage integration | Outside current local runtime; Nexus boundary remains separate |
| Event sourcing | Commands, events, state evolution | Journal, replay, snapshots, recovery lifecycle | Nexus/CQRS boundary requires explicit mapping |
| Durable state | State model and update policy | Durable store, recovery, concurrency semantics | External persistence integration candidate |
| Durable mailbox | Delivery durability and acknowledgment policy | Persistent queue and recovery | Communication currently owns in-memory two-lane mailbox only |
| Delivery guarantees | Idempotency/correlation policy | At-most/at-least/exactly-once mechanisms and failure visibility | Current local delivery is not a distributed guarantee |
| Passivation / idle entity lifecycle | Idle policy and recovery identity | Stop/reactivate buffering and ownership | Entity/sharding integration candidate |
| Scheduled jobs | Job and schedule policy | Durable or ephemeral scheduling, cancellation, ownership | Timers cover incarnation-local scheduling only |
| Coordinated shutdown phases | Named phases, dependencies, tasks | Ordered system termination and timeout handling | Not yet a Bombay system feature |
| Hot code upgrade | State conversion and version policy | Upgrade callback/system-message coordination | Erlang/OTP capability; unsupported unless deliberately designed |
| Runtime introspection | Metadata exposure policy | Actor tree, mailbox depth, status, lifecycle facts | Partial lifecycle facts exist; no coherent introspection API |
| Metrics | Instrument names and cardinality policy | Mailbox, throughput, latency, restart, failure metrics | Kameo comparison and Bombay ownership unresearched |
| Tracing/logging | Context and privacy policy | Actor/message/lifecycle spans and structured events | No canonical runtime integration |
| Dead letters / unhandled messages | Domain policy for observation | Collection, diagnostics, suppression, lifecycle | Bombay exposes typed delivery failures but no dead-letter facility |
| Dispatcher isolation | Workload class and blocking policy | Executor selection, fairness, throughput controls | Tokio is fixed today; portability item A5 remains blocked |
| Mailbox variants | Capacity, priority, fairness, overflow policy | Standard mailbox implementations and selection | Communication owns one two-lane design; full comparison required |
| Test probes and deterministic testkit | Protocol assertions and test policy | Spawnless behavior tests, async probes, virtual time, fishing/event filters | Existing tests are internal; no public testkit inventory |
| Fault injection / chaos testing | Failure scenarios | Controlled crash, delay, partition, and restart observation | Verification capability not inventoried |
| Deployment/configuration | Topology and policy configuration | Validation, defaults, runtime loading | No canonical Bombay configuration layer |

Primary comparative evidence:

- [Akka Typed actors and feature index](https://doc.akka.io/libraries/akka-core/current/typed/index.html)
- [Akka Typed lifecycle and supervision](https://doc.akka.io/libraries/akka-core/current/typed/guide/tutorial_1.html)
- [Akka Typed routers](https://doc.akka.io/libraries/akka-core/current/typed/routers.html)
- [Akka Typed interaction patterns and lifecycle-bound timers](https://doc.akka.io/libraries/akka-core/current/typed/interaction-patterns.html)
- [Akka Streams backpressure contract](https://doc.akka.io/libraries/akka-core/current/stream/stream-flows-and-basics.html)
- [Akka Cluster higher-level tools](https://doc.akka.io/libraries/akka-core/current/typed/cluster.html)
- [Akka Cluster Sharding and passivation](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)
- [Akka Typed persistence and event sourcing](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html)
- [Erlang/OTP design principles](https://www.erlang.org/docs/27/system/design_principles.html)
- [Erlang/OTP supervisor contract](https://www.erlang.org/doc/system/sup_princ.html)
- [Erlang process links and monitors](https://www.erlang.org/doc/system/ref_man_processes.html)
- [Kameo repository and feature inventory](https://github.com/tqwewe/kameo)
- [Kameo spawn and supervised-child API](https://docs.rs/kameo/latest/kameo/actor/trait.Spawn.html)

### AF0 research record — current execution and layering

The current execution path is factual evidence, not the desired API:

| Layer today | Takes | Does | Gives | Problem to research |
|---|---|---|---|---|
| `System::spawn` | `Actor<B>` plus shared router/lifecycle configuration | Prepares mailbox, observations, environment, registration, and incarnation | `Handle` or registration failure | Public bounds expose effect-routing and creation-observation machinery |
| `PreparedIncarnation::launch` | Committed incarnation resources and launch mode | Calls `tokio::spawn`, emits lifecycle facts, retains abort/completion edges | `Handle` | Task ownership is Bombay-owned but split from Engine lifecycle ownership |
| `Incarnation::run` | `Driver`, registration lease, observations, lifecycle reporter | Manually calls engine init, loop, and retire phases; publishes terminal result | Completion/peer observations | Bombay knows and orchestrates Engine internal phases |
| `Driver` | `Compose<B>` and `Environment` | Initializes, stores `Active<B>`, owns serialized event loop, passes effects to environment | `RunExit` or `RunError` | `Driver`, `RuntimeEffects`, phase methods, and retirement protocol are public |
| `ActorEnvironment` | Mailbox source and incarnation effect services | Supplies events, creates children, routes sends, retires children | Environment errors | Requires application send types to implement Bombay interpreter contracts |
| Application send type | Domain effects | Currently implements algebra, traversal, creation probing, routing, and error composition in complex examples | Routable runtime effect product | Application is incorrectly performing Bombay's job |

AF0 must answer and record, with inversion tests proposed for every answer:

| Required engine/layering decision | Required evidence |
|---|---|
| Exact black-box Engine input and output | Current Engine tests plus Behavior/Transition/Machine Executor contracts |
| Whether Engine owns retirement on return, error, panic, and cancellation | Current `Driver`, `Incarnation`, task-drop, and terminal-publication ordering |
| Whether Engine exposes any stateful driver or phase methods | Independent test requirements and the sole production call site |
| Exact boundary between the Engine loop and Bombay environment | Fake-environment tests and `ActorEnvironment` responsibilities |
| Exact owner of `tokio::spawn` and cancellation authority | Incarnation generation, address lease, task handle, and executor portability requirements |
| Representation passed from Behavior through Engine to Bombay | Behavior purity plus complete Bombay interpretation without application trait implementations |
| How each actor template obtains one engine and environment | Normal, child, supervisor, proxy, router, and pool permutations |
| Which traits must remain technically public for cross-crate compilation | Macro hygiene and Rust coherence, with ordinary docs/preludes kept clean |
| Final public application layer | Comparative actor APIs and the reference application after all lower layers exist |

The target dependency direction to validate is strictly one-way:

```text
application domain behavior and topology
    → ready actor templates and Behavior composition
    → one black-box Engine run per incarnation
    → Bombay-owned environment interpretation
    → Bombay-owned incarnation/task/registration/retirement
    → shared mailbox, address, observation, timer, and executor primitives
```

No lower layer may require an application to implement its interpreter,
executor, routing, registration, lifecycle-turn, wrapper-product, or composed
error contracts. AF0 must correct this diagram if source evidence disproves
any edge before implementation features are created.

## Current dependency snapshot

The workspace selects the clean local Behavior 0.10.0 checkout at
`63045b4c60e0c652bbd024c60f5f49069683d54c` and local Address 0.2.0 checkout
at `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` through Cargo patches. It locks
Communication 0.1.1, Observe 0.1.0, Timers 0.1.0, Transition 0.1.0, and Machine
Executor 0.1.0 from crates.io. The exact manifest and lock are authoritative. Earlier
feature records naming Behavior commit
`31f33897dbcdf8fd92da39affe125c90f59d32a2` are historical evidence from the
temporary pre-release pin. The Bombay
Communication owner checkout's source matches the locked 0.1.1 package, while
its workspace manifest still declares 0.1.0; the registry version remains
authoritative.

The local Behavior revision is two commits after the 0.10.0 release. The local
Address revision supplies the 0.2.0 opaque resolved-endpoint contract. AF0 must
reverify these active sources feature by feature; the earlier migration audit
does not waive that requirement.

Ownership remains:

- Behavior owns pure folds, event/protocol algebra, births, wrappers, and typed
  send products.
- Communication owns the sole two-lane mailbox, per-lane FIFO, backpressure,
  closure, and rejected-payload recovery.
- Address owns typed identity, registration, and generation fencing.
- Observe owns exact-generation terminal publication.
- Timers owns keyed, generation-safe monotonic scheduling.
- Transition owns the affine machine step; Machine Executor owns ordered
  execution turns; `bombay-engine` owns their Behavior orchestration.
- bombay owns addresses, `System`, `Handle`, spawning, effect
  interpretation, endpoint selection through `DeliveryRouter`, and exact
  incarnation retirement.

## Unfinished contracts

### Runtime operations: A5, C4, C5, F2-F7, P1, R1, M2

#### R1 verification — 2026-08-13

The repository identity decision is explicit: the legacy implementation in
`devrandom-labs/bombay` is removed, and the runtime formerly developed as
Actorpass supersedes it wholesale. This is not a merge of the two runtimes.
The exact imported source is Actorpass
`0f42fc27411944b93c8e30c68c3b44b3101274fc`, including F7 transactional
activation. The public core package is renamed to `bombay-rs` with Rust crate
name `bombay`; `bombay-behavior-engine` is renamed to `bombay-engine`; and
`actorpass-framework` is renamed to `bombay-framework`.

The locked neighboring contracts remain Behavior
`31f33897dbcdf8fd92da39affe125c90f59d32a2`, Communication 0.1.1, Address
0.1.1, Observe 0.1.0, Timers 0.1.0, Transition 0.1.0, and Machine Executor
0.1.0. Their source, public APIs, tests, and relevant documentation were
already rechecked for F7 at this exact runtime revision; R1 changes no algebra,
protocol, lifecycle ordering, or ownership boundary. Behavior remains the pure
algebra owner; Communication the mailbox owner; Address registration owner;
Observe terminal-publication owner; Timers schedule owner; Transition/Machine
Executor ordered-machine owners; the renamed Bombay core remains runtime
composition owner; and `bombay-engine` remains Behavior orchestration owner.

R1 must update every manifest, crate import, public re-export, test, example,
benchmark, fuzz target, research workspace, diagnostic probe, CI/build input,
and current document. Historical evidence may retain old commit/repository
provenance only when explicitly labeled historical. Completion requires one
package graph with no live `actorpass`, `actorpass-framework`, or
`bombay-behavior-engine` package/import and all repository gates passing.

The repository-wide rename audit found no live legacy package or import, and
the renamed manifests, public re-exports, tests, examples, benchmarks, fuzz
target, documentation, and CI/build inputs are synchronized. The repository
gates pass. R1 is `feature-complete` pending M2 and the project-wide release
audit.

- A5 requires the same temporal conformance suite for a real second executor;
  trait conformance alone is insufficient.
- C4 remains consumer-gated. Hung-fold detection needs an explicit policy
  owner and an oracle; it may not smuggle cancellation policy into the kernel.
- C5 and P1 require concrete second router, clock, or embedded consumers before
  generalizing the current Tokio runtime.
- F2 requires a second priority protocol. Typed shutdown alone does not justify
  a general control framework.
- F3 is ordinary application protocol: a request carries a typed `Recipient`,
  timeout uses existing timers, and late reply and peer retirement are explicit.
  No ask object, correlation registry, reply channel, or runtime handle exists.
- F4 belongs in a focused adapter owning one bounded pump, source cancellation,
  actor backpressure, and accounting for rejected payloads.
- F5 defaults to an explicit typed event sum. It may reopen only when a real
  consumer cannot preserve types and payload ownership that way.
- F6 is Behavior-owned replacement-resolution algebra first; bombay only
  interprets a runtime fact if that algebra requires one.

#### F7 verification — 2026-08-13

F7 is the focused operation requested by
`devrandom-labs/bombay#307`, with `bombay-entity` 0.1.0 and
`mnesis-bombay` as concrete consumers. The exact bombay source is
`6466232b00d36b021098499498f59f6f594023ed`; the locked dependencies are
Behavior `31f33897dbcdf8fd92da39affe125c90f59d32a2`, Communication 0.1.1,
Address 0.1.1, Observe 0.1.0, Timers 0.1.0, Transition 0.1.0, and Machine
Executor 0.1.0. Entity 0.1.0 was inspected at
`68d0f503205a569ddda88124f5add8e8a652e18f`. For each owner, current source,
public API, tests, and relevant documentation were rechecked for this feature.

The ownership map is exact: Behavior owns the pure initialization fold and
typed effects; Machine Executor and the bombay Behavior Engine own ordered
execution and effect interpretation; Communication owns the sole two-lane
mailbox and rejected-payload recovery; Address owns exact registration
generations; Observe owns exact terminal publication; Timers owns only keyed,
generation-safe schedules and is not on this activation path. Bombay owns
provisional resource construction, initialize-before-register ordering,
registration, launch, cancellation, and terminal retirement. Entity owns
stable entity identity, activation coordination, admission, fences, draining,
passivation, and the policy choice between graceful and forced retirement.

The public-contract gap is demonstrated by
`mnesis-bombay/tests/bombay_entity_contract.rs`: `System::spawn` commits
registration synchronously before the launched task polls `Driver::run_init`,
whereas Entity may commit its stable endpoint only after preparation and
transactional activation succeed. Bombay's child-birth path already proves
the required sequence: `prepare_provisional` → `Driver::run_init` → `commit`
→ `run_initialized`. The implementation must make that sequence canonical
for roots, preserve typed initialization/effect/registration errors, and
return a cloneable delivery-only capability separately from one affine
graceful/forced retirement capability without exposing private typestates,
mailbox senders, registration leases, observation subjects, executor handles,
`dyn`, `Any`, or downstream vocabulary.

The observable invariant is: successful return happens only after the entire
initialization fold and all initialization effects complete, then exact
registration commits and the initialized incarnation launches; failure leaves
no routable endpoint, and provisional resources retire exactly once. The
endpoint is cloneable, retirement authority is not, graceful and forced
retirement both await the exact terminal outcome, and no returned endpoint can
survive failed initialization. Inversion tests must fail if registration moves
before initialization, launch moves before initialization effects, a failed
provisional registration leaks, retirement authority becomes cloneable, or a
failed initialization yields a live endpoint. A downstream compile/conformance
probe must implement Entity 0.1.0's `LocalEntityRuntime` without private types
or erasure.

The implementation reuses the private provisional/committed typestates and
the single `PreparedIncarnation::launch` operation shared by ordinary root
spawn, transactional root activation, and child birth. That operation owns
the sole task, abort-control, completion, and lifecycle-start ceremony;
`System::activate` reaches it through the
`Driver::run_init`/`Incarnation::run_initialized` path. `System::activate`
returns `RootEndpoint<B>` separately from the affine
`BehaviorRetirement<R, B, L>` alias; the latter preserves typed terminal
outcomes and graceful/forced authority without exposing any underlying seat.
The final interface audit removed the initial behavior/system-specific wrapper
bounds from the retirement object: `RootRetirement<R, T>` now has only the two
independently required generic facts, while the public alias keeps downstream
Entity associated types concise and static.

Five activation inversion oracles prove init/effect/registration ordering,
typed stage failures, rollback, separate capability shape, exact cancellation
completion, exact-once provisional destruction after collision, registration
release, and immediate same-address reuse. The transition-driver Entity probe now
implements the released 0.1.0 `LocalEntityRuntime` directly with
`RootEndpoint<EntityActor>` and `BehaviorRetirement<EntityRouters,
EntityActor>`; its former retire-command channel and owner task were removed.
All ten Entity conformance tests pass. Workspace tests, rustfmt, Clippy, rustdoc,
the downstream probe, documentation/re-export synchronization, and the
repository-wide positional/duplicate-protocol audit pass. F7 is
`feature-complete` pending M2 and the project-wide release audit.

#### F6 verification — 2026-08-13

F6 uses Bombay Behavior commit
`31f33897dbcdf8fd92da39affe125c90f59d32a2` temporarily while its 0.9.5
release runs through CI. Bombay previously locked 0.9.4; the live crates.io
index still ended at 0.9.4 during this verification. The selected commit is a
clean repository object; unrelated uncommitted pool/test changes in the sibling
checkout are not part of the contract.

The feature-specific source, public API, tests, and documentation audit found:

- Behavior owns `CreationKind::ReplacementIncarnation`, creation installation
  and rejection events, worker-stop events, and the new interpreter-neutral
  `ReplacementResolution` projection. `WorkerCreationResolved::into_replacement`
  distinguishes an installed successor from a rejected attempt without nonce
  arithmetic; `WorkerStopped` remains the separate dead-incarnation fact.
  The nominal `#[behavior::behavior(...)]` macro is authoring syntax only and
  creates no observation or runtime path.
- Observe 0.1.0 owns retained publication for one exact outcome generation. It
  neither knows replacement provenance nor combines terminal and creation
  facts.
- Timers 0.1.0 owns keyed monotonic schedule generations and has no role in
  replacement resolution.
- Communication 0.1.1 owns the bounded two-lane mailbox, FIFO, closure, and
  rejected-payload recovery. It transports typed events but assigns no
  incarnation meaning.
- Address 0.1.1 owns exact registration identities, generation fencing, and
  address reuse. Its opaque registration generation must not be exposed or
  aliased with Behavior child nonces.

Therefore bombay owns only the existing interpretation edges: publish the
exact stopped-child fact and report the already-recorded creation result to the
stable proxy. It must add no replacement registry, observation request, lease
authority, correlation identity, or duplicate protocol. The F6 observable
invariant is that one replacement attempt yields a Behavior-owned installed or
rejected resolution naming its explicit predecessor, while predecessor death
remains independently observable. An inversion test must fail if birth is
misclassified as replacement, replacement provenance is inferred, or death and
creation resolution are collapsed.

The implementation audit found no missing bombay protocol: the existing
typed report edge already preserves all fields required by the Behavior
projection. Its oracle now covers both rejected and installed replacements and
was inverted by substituting the fresh worker nonce for the stable proxy; that
inversion failed. The existing child-stop oracle independently proves the dead
incarnation fact. The reference application also uses the new nominal macro
for an eligible plain behavior. Workspace tests, rustfmt, Clippy, and rustdoc
pass. F6 is `feature-complete` pending M2 and the project-wide final audit.
- M2 distills these operations together and must remove duplicate adapters and
  executor seats before it can unblock FV1.

### Optional libraries: L1-L7, M3

- L1 worker routing is Behavior policy. Stable key affinity must survive
  replacement or change only through explicit rebalance; extraction requires a
  second consumer and adds no runtime pool or erased recipient.
- L2 requires a concrete typed-group consumer and a decision separating local
  membership from Zenoh transport/discovery ownership.
- L3 keeps accepted intake in application state, preserves replay order, and
  makes overflow and draining refusal typed and observable.
- L4 keeps drain state, in-flight work, deadline, and completion accounting in
  the behavior; it creates no mailbox mode or runtime drain controller.
- L5 composes Behavior send products and typed `SendAlgebra::send`; it keeps
  retry generation and backoff in application state and introduces no timer
  adapter or handwritten product algebra.
- L6 exposes reporting through ordinary typed messages and reply recipients,
  without a registry or query subsystem.
- L7 remains blocked until an invariant exists beyond Behavior timers.
- M3 audits optional artifacts independently and does not block FV1.

### Developer experience: E1-E9, M4

- E1 may provide syntax only when it expands to the canonical public owners.
  `local_system!` must expand to `System::new`; Behavior's nominal attribute is
  preferred for eligible plain user-message behaviors. Semantic
  wrappers and service-event behaviors remain explicit.
- E2 keeps compile failures focused on actionable ownership and inference
  boundaries; diagnostic aliases may not become alternate construction paths.
- E3 keeps cookbook and public usage guidance synchronized with the locked APIs,
  typed send paths, sole spawn path, and kernel exclusions.

#### E4-E9 authoring-UX audit — 2026-08-14

Historical snapshot: the API counts and Behavior 0.9.5/0.10.0 descriptions in
this audit record the pre-E5 baseline. The later E5 local lifecycle migration
section and the current composition rules are normative.

The audit inspected the complete tracked repository, including the core and
framework public exports, `hello`, `local_runtime`, the 1,241-line `job_queue`,
all tests, benchmarks, fuzz inputs, README, cookbook, and current architecture
documents. It also rechecked the source, public API, tests, and relevant
documentation of the exact locked crates.io packages Behavior 0.9.5, Observe
0.1.0, Timers 0.1.0, Communication 0.1.1, Address 0.1.1, Transition 0.1.0,
and Machine Executor 0.1.0. The repository currently contains 96 spawn call
sites, 60 handwritten `Behavior` implementations, 19 `Handler`
implementations, 12 application `EndpointRegistry` implementations, and 24
application `DeliveryRouter` implementations. Counts are evidence for this
audit, not permanent API targets.

The minimal authoring target is not raw line-count reduction at the expense of
types. A small application should declare its domain behavior, declare each
real message route once, construct one system, spawn one actor value, and use
the returned capabilities. The job queue must keep domain policy visible while
removing duplicated endpoint type algebra, generated routing bodies, test-only
probe actors, repeated empty initialization folds, and lifecycle pattern
matching that adds no domain decision. Each item must measure the ceremonial
constructs it removes and must not replace them with hidden runtime policy.

##### E4 verification and contract

Behavior 0.9.5 already makes one behavior value the owner of application state
and statically maps its address, message, complete event, send algebra, birth
mode, phase, and error types; it does not contain a concrete address value.
Address 0.1.1 owns only exact registration generations, Communication 0.1.1
owns only typed mailbox endpoints, Observe 0.1.0 owns completion publication,
and Timers 0.1.0 owns schedules. Transition and Machine Executor accept the
behavior machine but deliberately know no actor identity. Therefore bombay is
the exact owner of the missing authoring concept: an inert `Actor<B>` pairing
`B::Addr` with `B` before runtime resources exist.

E4 adds one minimal public value with construction, borrowing, and consuming
parts operations. `System::spawn` and `System::activate` consume that value;
child `Create` remains Behavior-owned and distinct because it carries a
creator-local nonce and creation provenance. `Actor` implements no behavior,
contains no mailbox, task, handle, router, lifecycle state, or policy, and does
not survive as a second live runtime object. The observable invariant is that
the address value and behavior can enter preparation only as one statically
matched `Actor<B>`. An inversion compile test must reject `Actor<B>` built with
an address other than `B::Addr`, and runtime oracles must prove spawn and
activation ordering and outcomes are unchanged.

The implementation adds only the two-field inert `Actor<B>` and routes root
spawn, transactional activation, ordinary preparation, provisional
preparation, and the child-runtime handoff through it. Behavior's `Create`
remains unchanged. All 89 root spawn call sites and all six direct
transactional activation call sites now pass one actor value; the framework
prelude, public exports, examples, tests, benchmarks, README, cookbook, and
module-boundary documentation use the same concept. A compile-fail doctest
rejects an address outside `B::Addr`; a focused unit test round-trips exactly
the address and behavior; the process allocation oracle and every existing
spawn, activation, creation, lifecycle, panic, cancellation, routing, and
retirement oracle pass unchanged. Workspace tests, rustfmt, Clippy, rustdoc,
and the repository-wide old-call-shape audit pass. E4 is `feature-complete`
pending E9, M4, and the project-wide release audit.

##### E5 verification and contract

Historical contract record, superseded by “E5 local Behavior lifecycle
migration — 2026-08-14” below. Older public API names remain here only as
evidence for the migration decision.

Fresh verification on 2026-08-14 inspected the exact locked registry source,
public API, tests, and documentation for Behavior 0.9.5, Communication 0.1.1,
Address 0.1.1, Observe 0.1.0, Timers 0.1.0, Transition 0.1.0, and Machine
Executor 0.1.0. It also inspected the Behaviorpass repository instructions,
source, testkit, current documentation, every local branch, remote main
`fcc3aedeb76cf69a034920ec84556cd886bb141b`, and the complete Actorpass routing
implementation and guidance at `0f42fc27411944b93c8e30c68c3b44b3101274fc`.

The prior E5 contract was wrong to prescribe an application routing macro.
Behavior's clean recursive routing work solves effect-lane selection:
`SendProduct`, semantic `Own`/`Inner<Path>`, `SendInput`, and Bombay's generic
`RouteSends` interpreter mean applications never implement or traverse effect
routing. That does not solve endpoint selection. `Delivery<A, M>` and
`Recipient<A, M>` contain only an address/route and message; they carry no
destination behavior, event protocol, or endpoint type. `Behavior` associates
its own input `Addr`, `Msg`, and `Event`, but an emitting behavior's output
delivery does not identify which receiving behavior owns `M`. Multiple real
behaviors in both framework examples accept the same address/message protocol,
so associated-type projection cannot select or even prove distinct endpoint
implementations.

Communication owns typed mailbox lanes and rejected payload recovery but no
heterogeneous endpoint space. Address owns homogeneous `AddressSpace<A, E>`
tables, exact claims, and leases; it deliberately has no `Any`, `TypeId`,
downcast, or runtime typemap. Observe and Timers add no delivery fact.
Transition and Machine Executor do not participate in routing. Bombay owns
endpoint registration, lookup, route resolution, and effect interpretation.
Consequently ordinary actor applications must not be made to own `Routes`,
`AddressRouter`, `EndpointRegistry`, `DeliveryRouter`, `IncarnationEndpoint`,
or endpoint aliases; those are runtime implementation concepts.

An attempted behavior-only declarative expansion was deliberately compiled and
rejected: Rust coherence cannot distinguish implementations whose address,
message, and event are only unrelated associated-type projections, and shared
message protocols are genuinely ambiguous under the current `Delivery<A, M>`
contract. Adding field names, modes, repeated message annotations, generated
route structs, or a system type-list merely moves the same missing destination
fact into user syntax. It is not accepted E5 work. Dynamic registries, erased
endpoints, `Any`, `TypeId`, downcasts, global envelopes, and runtime protocol
lookup remain prohibited.

E5 was blocked on a truthful static destination/endpoint contract.
That contract must make ordinary registration and delivery fully Bombay-owned,
preserve pure behaviors and rejected payload ownership, support repeated
message types without user routing declarations, and remain recursively
composable through births and wrappers. It may require a Behavior-owned typed
destination fact or a narrower Bombay mailbox/address composition, but must be
designed and verified in the owning repository before Bombay consumes it.
Until that contract lands, the existing low-level traits remain for the runtime
and current compatibility examples; they must not be promoted as the intended
application UX. Once it lands, E5 must remove routing vocabulary from the framework prelude, README,
cookbook, both framework examples, and all ordinary public signatures while
retaining focused internal adapter tests and explicit advanced extension
documentation only where a concrete non-Bombay interpreter requires it.

Behavior 0.10.0, released at
`4c756f5a6e8bf6aacfe6435250b95fb9fdcab985` on 2026-08-14, satisfies that
external prerequisite. Its exact release source, public API, changelog,
README, algebra tests, protocol-destination tests, compile-fail tests, model
tests, properties, fuzz consumers, wrapper compositions, and pool/supervision
consumers were inspected afresh. `Recipient<B>` and `Delivery<B>` are indexed
by the concrete destination `Behavior`; `B::Addr` and `B::Msg` determine the
address and payload, identical address/payload pairs remain distinct across
destination behaviors, `Recipient::resolve(parent)` is the sole public route
resolution operation, and the value carries no endpoint or runtime handle.
`Handler` and `Pure` now carry the complete send algebra, and an actor with no
ordinary sends uses `Vec<Never>`. Behavior deliberately exposes no route enum,
endpoint table, registration mechanism, or runtime topology object.

The Behavior 0.10 migration is complete across the locked dependency graph,
runtime implementation, unit and integration tests, doctests, examples,
benchmarks, mutation metadata, public documentation, and framework facade.
Bombay now interprets `Delivery<B>` and `Recipient<B>` exclusively, keys
registration and delivery compatibility adapters by `B`, uses `Vec<Never>`
for behaviors with no ordinary sends, and contains no consumer of Behavior's
private route representation. Identical address/message protocols are covered
by distinct destination behavior types in the request/reply and job-queue
oracles. Workspace build/tests, rustfmt, strict Clippy, all three executable
examples, and the repository-wide obsolete-pattern audit pass. This completes
the dependency migration but not E5: application-owned route structs and the
framework routing re-exports remain the next active seam and prevent a
`feature-complete` state.

The unchanged exact neighboring contracts were rechecked for this migration:
Communication 0.1.1 still owns only typed two-lane mailbox transport and
rejected payload recovery; Address 0.1.1 still owns homogeneous exact-generation
tables and leases; Observe 0.1.0 and Timers 0.1.0 add no delivery topology;
Transition 0.1.0 and Machine Executor 0.1.0 remain outside routing. Therefore
Bombay must key delivery and registration by destination behavior, resolve
only through `Recipient<B>::resolve`, and own every endpoint/topology object.
No compatibility copy of Behavior's private `Route`, address/message-indexed
delivery protocol, dynamic registry, or erased endpoint is permitted. E5 is
now `active`: upgrade the exact lock to Behavior 0.10.0, migrate the complete
runtime and repository, then remove ordinary application routing declarations
and routing vocabulary from the framework surface before feature completion.

##### E5 local Behavior lifecycle migration — 2026-08-14

Fresh verification for this follow-up inspected the exact locked registry
source, public API, tests, and packaged documentation for Behavior 0.10.0,
Communication 0.1.1, Address 0.1.1, Observe 0.1.0, and Timers 0.1.0. It also
inspected the source, public exports, algebra tests, compile-fail tests,
property/model/fuzz consumers, examples, README, domain-boundary guidance, and
wart audit in the clean local Behavior checkout at
`63045b4c60e0c652bbd024c60f5f49069683d54c`. The local Address checkout is at
`7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`; the local Observe checkout is at
`43016e5f781e006e072e77af996c4da64466dee8`; the local Timers checkout is at
`4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2`; and the Communication owner
workspace is at `e2e8f3995154dcc420a85b04e5f658e8af0f862f`. Untracked local
research/editor files in those neighboring checkouts are not dependency
contracts and were not modified. The registry packages remain authoritative
for every neighbor other than the explicitly requested local Behavior target.

The target Behavior revision removes `Handler`, `Pure`, `BehaviorFn`,
`SendProduct`, `Inner`, `Transcript`, `run`, `Compose::from_fns`,
`Compose::from_behavior`, `Compose::build`, and direct public initialization or
transition on a definition. `Compose<B>` now owns an uninitialized definition;
its consuming `initialize` returns initialization actions and `Active<B>`.
Only `Active<B>` exposes `transition`, `receive`, and typed `on`; direct
`Behavior` implementations receive crate-minted `InitializationTurn` and
`ActiveTurn` capabilities. `BehaviorBase` and `Compose::base`/`Active::base`
replace positional wrapper inspection, and `StashStatus` exposes only the
semantic stashed-message count. Heterogeneous send products are now named
semantic wrapper/application structs that implement `SendAlgebra` and
`SendInput`; there is no public positional product or path language to copy.
The nominal `#[behavior::behavior]` attribute is the ordinary user-message
authoring path, while explicit `Behavior` remains for service-event protocols
and semantic wrappers.

The exact ownership map remains unchanged: Behavior owns pure definitions,
the consuming initialization/active typestate boundary, event and effect
algebra, semantic wrappers, births, supervision, and typed destinations;
Communication owns only the two-lane mailbox and rejected-payload recovery;
Address owns exact-generation registration and endpoint snapshots; Observe
owns exact-generation terminal publication; Timers owns keyed generation-safe
scheduling. Bombay Engine must consume `Compose<B>`, interpret the returned
initial actions, and store only `Active<B>` in its transition machine. Bombay
owns runtime preparation, task execution, effect interpretation, endpoint
selection, and retirement, but must not recreate Behavior initialization state
or expose a second way to mint lifecycle turns. This migration changes no
Communication, Address, Observe, or Timers protocol and introduces no registry,
request/reply mechanism, lifecycle framework, or supervision policy.

The local Address 0.2.0 follow-up was reverified from its complete public
source, semantics/reclamation/Loom tests, benchmark, guide, changelog, and
adversarial research suite. Its only contract change from 0.1.1 is that
`AddressSpace::resolve` returns `Resolved<E>`, an opaque read-only endpoint
snapshot. `Resolved<E>` dereferences and implements `AsRef<E>` but cannot be
constructed, destructured, mutated through the wrapper, moved out, or used as
registration authority. Resolution clones the endpoint outside the table lock;
the snapshot remains valid after release or address reuse. `Lease` remains the
sole exact-generation release authority and `RegistrationId` remains only an
opaque process-local correlation identity. Bombay must consume the resolved
capability through shared access, must not expose Address's internal `Arc`, and
must not add a second snapshot, reclamation, or registration mechanism.

The local lifecycle/address migration is `feature-complete`: the workspace now
selects the audited local Behavior revision and Address 0.2.0; Engine consumes
`Compose<B>` exactly once and stores only `Active<B>`; Bombay interprets named
semantic sends and typed optional event routing; obsolete Behavior exports and
call shapes are absent from current code, examples, benchmarks, mutation
metadata, and normative documentation. `cargo check --workspace --all-targets`,
`cargo test --workspace`, formatting, and workspace Clippy pass. Historical
records retain old names only in sections explicitly marked historical.

E5 itself remains `active` because its older, separate application-routing
objective still has advanced `EndpointRegistry`/`DeliveryRouter` adapters in
the framework examples. Those traits were removed from the ordinary framework
prelude in this migration, but eliminating the concrete endpoint-selection
adapters requires the remaining E5 design work; lifecycle migration does not
silently claim that separate outcome.

An address-integration audit on 2026-08-14 rechecked the locked Address 0.1.1
source, tests, benchmark, and public contract alongside Behavior 0.10.0,
Communication 0.1.1, Observe 0.1.0, and Timers 0.1.0, then traced every Bombay
registration, delivery, peer-observation, lifecycle, example, benchmark, and
test adapter. `EndpointRegistry<B, D>` must remain destination-behavior indexed:
Behavior intentionally distinguishes destinations with identical address and
message protocols, and Bombay routers use `B` as the static topology selector.
`AddressRouter` must also remain Bombay-owned rather than aliasing the foreign
`AddressSpace`; it hides table maintenance operations and names the typed
runtime topology boundary. Address registration generations remain distinct
from Behavior nonces and Observe completion generations.

Address's internal `Arc<E>` must remain hidden. A prototype opaque handle to
the shared registered endpoint benchmarked faster for Bombay-shaped endpoints,
but the Address adversarial cascade suite proved that it changes ownership:
snapshots then retain affine fields of the registered endpoint and delay nested
lease retirement. Endpoint-defined `Clone` is therefore a semantic boundary,
not redundant work. Address instead owns one opinionated lookup result:
`resolve` returns an opaque, read-only endpoint-defined snapshot. Bombay can
dereference that capability but cannot obtain the storage handle, mutate the
snapshot, choose a reclamation path, erase `B` or `D`, or reconstruct lease or
lifecycle authority. No local path override or duplicate Bombay lookup table
is accepted as a permanent integration.

##### E6 verification and contract

Historical baseline: its Behavior 0.9.5 authoring inventory is superseded by
the local lifecycle migration contract above.

Behavior 0.9.5 already supplies `Handler`/`Pure`, `BehaviorFn`, the nominal
`#[behavior::behavior]` macro, `Compose`, semantic wrappers, `Stash`, and both
`WorkerPool` and `KeyedWorkerPool`; its algebra and tests make those the only
authoritative behavior construction paths. Observe, Timers, Communication,
Address, Transition, and Machine Executor provide no behavior-authoring
syntax. Bombay may select and re-export Behavior facilities but may not clone
their protocols or generate a competing `Behavior` implementation scheme.

E6 rewrites every eligible plain application behavior to the existing nominal
macro or `Handler` path, uses existing pool/stash composition where its exact
contract matches, and records any irreducible macro or naming limitation as a
Behavior-owned upstream prerequisite. Semantic wrappers such as the job
queue's drain/retry coordinator remain explicit. Completion requires removing
repeated associated-type and empty-init ceremony without function-pointer type
aliases, positional send traversal, duplicate protocols, or a Bombay-owned
behavior macro. Compile diagnostics must still name the user's nominal type
and exact unsupported protocol.

##### E7 verification and contract

Behavior 0.9.5 owns `Exit` and `Crash`; Communication returns rejected payloads;
Address owns registration failures; Observe retains the exact published value;
Timers has no terminal-result role; Transition and Machine Executor expose
poisoning without classifying actor policy. Bombay therefore remains the owner
of `ActorRef`, affine `Handle`, `RootEndpoint`, `RootRetirement`, `RunError`,
`RunExit`, and `TaskOutcome`. The nested result retains materially different
return, behavior failure, environment failure, poison, panic, cancellation,
collection, and explicit-stop facts and cannot be flattened globally.

E7 audits repeated `handle.actor_ref()` access and terminal matching, then adds
only zero-policy conveniences that preserve ownership and every exact variant.
It must not make `Handle` cloneable, hide whether waiting consumes lifecycle
authority, turn shutdown publication into retirement, discard rejected
payloads, or normalize detailed root outcomes to peer outcomes. Each retained
method or helper needs an application call-site reduction and an inversion
test proving the original exact value remains recoverable.

##### E8 verification and contract

Communication 0.1.1 requires an explicit user-lane capacity and fixes the
priority default; Address and Bombay routing require a concrete router value.
Behavior, Observe, Timers, Transition, and Machine Executor own no system
configuration defaults. The current `local_system!` is a named-argument spelling
of `System::new`, while the framework prelude omits several Behavior 0.9.5
application facilities used by the reference domain.

E8 makes the facade, prelude, README, and cookbook one coherent authoring
surface. It must decide whether `local_system!` earns its existence, export the
selected current Behavior vocabulary, and remove stale or duplicate setup
spelling. Mailbox capacity and the concrete router remain explicit; no default
may silently choose backpressure, priority, topology, lifecycle, clock, or
executor policy. Compile examples must prove the canonical setup has one path
and retains its concrete inferred system type.

##### E9 verification and contract

The same dependency audit found that no neighboring crate owns application
presentation. Behavior's pool and stash types may replace matching handwritten
policy, but Observe probes, Timers probes, Communication stress shapes, Address
reuse oracles, and executor classification tests belong in focused tests rather
than the narrative application. Bombay-framework owns the reference program
that demonstrates how those exact primitives compose.

E9 rewrites `hello`, `local_runtime`, `job_queue`, README, and cookbook after
E4-E8 settle. The executable job queue must read primarily as address/domain
types, worker and queue policy, route declarations, actor construction, and the
scenario. Test-only reporter/collector actors and exhaustive lifecycle
assertions move to focused tests. The completion gate records before/after
counts for total lines and, separately, spawn arguments, endpoint/incarnation
aliases, manual registry/router bodies, repeated behavior associated-type
blocks, and nested terminal-result matches. At the canonical call site, each
actor is one value passed to one spawn operation and each real route is named
once. Repository-wide tests and inversion oracles must retain every existing
behavioral invariant.

Some explicit inputs are not UX defects: the sender address carries Behavior
event provenance and child-relative routing; mailbox capacity selects
Communication backpressure; a concrete router selects real endpoints; behavior
associated types preserve the complete static protocol; and detailed terminal
results preserve distinct ownership failures. E4-E9 may improve spelling and
local inference around those facts but may not erase them.

M4 is reopened and blocked by E9. After E9, M4 must compare the complete
authoring surface against the minimal target, remove helpers that merely move
ceremony, and only then return to `feature-complete`; FV1 remains blocked by
both M2 and M4.

### Verification and release: Q1-Q7, S1, M5, FV1

The local CI, integration, concurrency, property, fuzz, Miri, mutation,
allocation, benchmark, doctest, panic-mode, and lifecycle-ordering gates are
feature-complete. M5's 2026-08-13 cross-repository audit verified each locked
dependency against its owner checkout and audited Bombay Entity, Nexus,
`mnesis-bombay`, and CESR/KERI. M5 is feature-complete, not distilled, pending
FV1. FV1 remains blocked by M2; optional M3 artifacts publish independently.

### External handoffs: K1-K3

KERI authority, Zenoh transport/discovery, and authenticated remote actors are
ordered integration-repository handoffs, not bombay features. Local address
identity, KERI authority, Zenoh naming, and remote protocol identity may never
substitute for one another.

## Behavior composition rules

Derive effects from the exact locked Behavior API. Compose heterogeneous lanes
as named semantic send structs, implement `SendAlgebra` plus one `SendInput`
per semantic lane, and emit with typed `SendAlgebra::send`. Positional product
paths no longer exist. Keep `RouteSends`, `ObservesCreations`, and composed
routing errors at the runtime-adapter boundary rather than in domain behavior.
Behavior 0.10's `Delivery<B>` selects the destination behavior. The current
`EndpointRegistry<B, D>` and `DeliveryRouter<B>` implementations are temporary
Bombay runtime compatibility adapters under E5; they are not the intended
application authoring model and must disappear from ordinary examples and the
framework prelude before E5 is feature-complete.

Use Behavior's nominal attribute for plain `User`/`Never` behaviors. Keep
semantic wrappers explicit, and construct wrapper stacks through `Compose` so
only the runtime can cross from definitions into active behaviors.

Audit the entire repository—including examples, benchmarks, tests, research,
diagnostic probes, documents, and public re-exports—for obsolete positional
composition or duplicate protocol implementations before completion. Mark
retained old API descriptions historical.

## Distilled runtime invariants

- One canonical provisional/prepared typestate and `Incarnation` execution
  machinery, exposed as ordinary `System::spawn` or the focused
  initialize-before-register `System::activate` operation.
- Child liveness is retained by an affine typed `ChildLease`.
- Behavior birth mode infers child runtime without manual capabilities.
- Spawn, child creation, and routing contain no trait-object or `Any` erasure.
- Bombay Communication is the sole mailbox implementation.
- Incarnation-local timers distinguish timer identity and generation.
- Actor resources retire before executor terminal classification.
- The exact address lease releases before completion publication, so a public
  terminal outcome denotes a fully retired, reusable address.
- Tokio return, panic, cancellation, and terminal-detachment laws are directly
  tested.

Future work must reopen a concrete item before widening any invariant.
