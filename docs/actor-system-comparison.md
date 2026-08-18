# Historical actor-system comparison

Date: 2026-08-16

Status: historical research input. Current ownership decisions are normative
only in [`runtime-capability-interfaces.md`](runtime-capability-interfaces.md).

This was AF0's normalized decision record. Sources were the then-selected Bombay crates and primary project
documentation for [Akka Typed 2.10.20](https://doc.akka.io/libraries/akka-core/current/typed/index.html),
[Erlang/OTP 29](https://www.erlang.org/doc/system/design_principles.html),
[Kameo 0.22](https://github.com/tqwewe/kameo), and
[Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/overview).

The comparison is about guarantees, not spelling. Bombay should copy neither
an object-oriented actor context nor OTP callback conventions. Its advantage is
that actor roles are Behavior values and all effects remain typed algebra.

## Canonical capability decisions

| Capability family | User intent and supplied data | System machinery and guarantee | Failure boundary | Bombay owner/decision |
|---|---|---|---|---|
| ordinary actor, state machine, stash | Define protocol, state, transition, capacity/release policy | Serialized initialization and turns; complete typed effects | Controlled behavior failure or explicit stop | Behavior/Actors policy; Engine executes; Bombay supplies environment |
| root/guardian/application | Supply one root behavior | Exactly one application lifecycle root; stopping it ends the local system | Root terminal result reaches process boundary | Actors `Guardian`; Bombay private root runner; `#[bombay::main]` generates entry |
| spawn, reference, hierarchy | Supply template, child identity and initial topology | Initialize before publication; child cannot outlive owner; exact typed endpoint | Typed creation rejection; no fabricated live reference | Behavior birth algebra plus Bombay Address/Communication composition |
| static and dynamic supervision | Select child template, strategy, restart class, budget and backoff | Observe exact incarnation; decide replacement; preserve logical policy state | Exhausted budget/escalation remains a typed terminal fact | Actors `Supervisor`, `DynamicSupervisor`, `BackoffSupervisor`; no Bombay policy |
| proxy, router, pool, work queue | Select routees/workers, selection, capacity, overflow and affinity | Deterministic selection/scheduling and typed forwarding | Owned rejection, unavailable routee, or supervised worker failure | Actors templates; Bombay interprets ordinary creation/delivery/timer leaves |
| watch, monitor, link | Select exact peer and reaction policy | Exact-generation terminal observation; cancellation is idempotent | Unknown generation is an error; links do not imply distributed reachability | Observe mechanism + Actors policy + Bombay task/control composition |
| timers, deadlines, receive timeout | Select timer identity, deadline and reaction | Keyed generation replacement; stale timers never fire; affine cancellation | Clock/queue failure is runtime failure; expiry remains a typed fact | Timers mechanism; Actors policy; Bombay owns Tokio clock driver |
| graceful/tree/coordinated shutdown | Supply drain/finalization/acknowledgement policy | Priority admission, child-first ownership, terminal barrier | Rejection differs from later completion; timeout is policy | Actors shutdown templates; Bombay owns admission and affine traversal mechanism |
| request/reply and correlation | Carry a typed `Recipient`, correlation and timeout policy | Ordinary typed delivery; optional policy templates correlate results | Unknown/closed endpoint preserves payload; timeout is not delivery success | Behavior delivery algebra; no request registry or implicit future table |
| registration, discovery, topics, pub/sub | Supply typed key/membership/publication policy | Deterministic typed mutation and fan-out | Missing/conflicting membership is owned typed rejection | Ready Actors templates; lifecycle integration in Bombay; distributed discovery external |
| local mailbox and delivery | Select only a proven capacity/overflow option | Two lanes, per-lane FIFO, aging, cancel-safe receive, unchanged rejection | Closed/rejected delivery returns the original payload | Communication owns mailbox; Bombay binds it to exact Address generation |
| terminal observation | Ask for exact incarnation completion | One retained result observable before/during/after publication | Unknown exact generation cannot be reconstructed from address absence | Observe owns cell; Bombay normalizes and publishes after release |
| async result adaptation | Supply task/future result mapping | Lifecycle-bound result becomes a later actor fact | Cancellation/failure is typed and cannot re-enter synchronously | Actors `Task`; Tokio execution adapter belongs in Bombay only when required |
| streams and backpressure | Supply stream graph, demand and materialization policy | Demand propagation, cancellation, supervision, fan-in/fan-out | Stream failure is not ordinary mailbox delivery failure | Optional external subsystem; never inflate the core mailbox/Engine |
| persistence/event sourcing/durable state | Supply event/state/recovery policy and store | Journal/store recovery, snapshots and durable concurrency contract | Storage/recovery failures are external integration facts | Nexus/Mnesis integration; not Bombay local core |
| virtual identity, passivation, sharding | Supply logical entity key and placement/passivation policy | Stable logical identity across activations and nodes | Split brain, duplicate activation and rebalance are distributed concerns | Bombay Entity/Nexus/Zenoh integration; local `ActorRef` makes no such promise |
| cluster, remote deployment, singleton, CRDT | Supply membership, authority, placement and consistency choices | Reachability, convergence, placement and serialization | Partition and protocol-version failure are first-class | Future authenticated Zenoh layer; explicitly absent locally |
| serialization/protocol evolution/delivery guarantee | Supply schema, version and idempotency policy | Wire compatibility and selected at-most/at-least-once semantics | Decode, duplicate, loss and retry are transport facts | External transport boundary; local delivery claims only one admission attempt |
| operations: health, readiness, config, metrics, tracing | Supply domain reports and cardinality/privacy policy | Typed operational state; runtime instruments its own mechanisms | Observation must not alter actor semantics | Actors policies plus optional instrumentation integration; no universal service locator |
| testing, probes, virtual time, fault injection | Supply scenario and expected protocol facts | Pure synchronous policy tests and full runtime tests | Test facility must not change production semantics | Behavior test helpers plus `#[bombay::test]`; virtual clock only with a real second clock owner |
| hot upgrade, release handling, deployment config | Supply version conversion and deployment policy | OTP/host-specific coordinated replacement | Upgrade incompatibility is operational failure | Explicitly outside current local runtime; do not imply support |

## Comparative conclusions and negative evidence

- Akka Typed creates one user guardian with the `ActorSystem`; children are
  recursively owned, and supervision is selected on behavior subtrees. Its
  streams, clustering, sharding, and persistence are separate facilities.
  Therefore Bombay's guardian must not become a global policy/service object.
- Erlang/OTP supervisors are explicit processes whose child specifications
  contain restart class, shutdown, worker/supervisor type, and ordered startup;
  termination reverses child order. Therefore supervision belongs in explicit
  actor templates, not an implicit runtime switch.
- Kameo's core supplies actor execution while pools, pub/sub, brokers, queues,
  and scheduling are reusable actors in `kameo_actors`. Therefore a broad actor
  catalogue is evidence for reusable Behavior templates, not more Engine types.
- Orleans supplies stable virtual identities, a grain directory, activation
  placement/collection, silo lifecycle, persistence, and cluster services.
  Bombay's exact local incarnation references intentionally promise none of
  those distributed properties.

Absence is deliberate: Bombay local core has no path lookup, receptionist,
dead-letter bus, request table, durable mailbox, cluster membership, placement,
remote spawning, persistence, stream materializer, or hot-upgrade machinery.
Those features cannot be inferred from `Address`, which owns only local
exact-generation claim/resolution/release.

## Typed composition matrix

`yes` means the composition is expressible by current Behavior/Actors algebra
and Bombay has the required runtime leaf. `generated` means RI4 must create the
closed static product. `external` means an owning integration is required.

| Outer role | Ordinary | Supervisor | Dynamic supervisor | Proxy | Router/pool | Watch/timer/stash/shutdown wrapper | Remote/persistent entity |
|---|---:|---:|---:|---:|---:|---:|---:|
| Guardian | generated | generated | generated | generated | generated | yes | external |
| Static creator | generated | generated | generated | generated | generated | yes | external |
| Supervisor | yes | yes | yes | yes | yes | yes | external |
| Dynamic supervisor | yes | yes | yes | yes | yes | yes | external |
| Proxy | yes | yes | yes | rejected unless protocol-compatible | yes | yes | external |
| Router/pool | yes | yes | yes | yes | yes | yes | external |
| Pure wrapper | yes | yes | yes | yes | yes | yes | external |

Rejections are structural:

- runtime-selected heterogeneous Rust types require a closed enum/template or
  are rejected;
- a child may not outlive its owning incarnation;
- wrappers cannot invent capabilities absent from their named send product;
- request/reply is delivery composition, not synchronous re-entry;
- remote/persistent identities never masquerade as local `ActorRef` values.

## Engine black-box decisions and inversion oracles

| Decision | Contract | Existing proof/oracle |
|---|---|---|
| one closed input | `Driver::new` consumes one final composed Behavior and one environment | compile failure for missing/mismatched environment; universality tests |
| initialize once | Behavior initialization precedes environment activation | initialization count/order inversions |
| activation barrier | no event source exists before complete initialization actions commit | environment phase-authority compile test; activation ordering tests |
| one serialized fold | request one event, fold once, await complete action application, then request another | exclusivity, no-prefetch, non-reentrancy and interpretation-count oracles |
| exact complete actions | the whole typed action product crosses once; Driver never projects lanes | complete-output and lane-order oracles |
| creation precedence | environment applies births before sends and scopes creation results to that action | creation-precedence/result-scope oracles |
| no rollback/retry | an application failure reports factual prefix and is terminal | committed-prefix, no-rollback and retry oracles |
| exact completion | explicit stop differs from exhausted source; behavior, activation and environment errors remain distinct | completion and exact-error oracles |
| affine retirement | every ordinary return awaits one retirement; panic/cancellation only drop ownership | retirement, panic and cancellation oracles |
| executor neutrality | Engine owns no Tokio task, mailbox, address, observation, topology or `Send` requirement | environment substitutability, non-Send compile fixture, structural manifest |

The Engine API is intentionally only `Driver`, `Environment`,
`ActiveEnvironment`, `Completion`, `DriverError`, and the action alias. Bombay
must not expose Engine phase methods or recreate the loop.

## Historical replacement graph

```text
AF0 decisions
  -> RI4a explicit closed topology metadata
      -> RI4b generated ApplicationCapabilities product and typed address storage
          -> RI4c private run_root(Guardian<Root>)
              -> E4/E5 one-value actor creation and generated routing
              -> E1/E2/E6 explicit Behavior authoring and diagnostics
              -> E8 #[bombay::main], #[bombay::test], prelude
                  -> E3 public examples and guidance
                  -> E7 boundary ActorRef/run-result ergonomics
                      -> E9 Akka-IoT-sized acceptance application
```

This graph is preserved only to explain earlier sequencing. It is superseded by
the live graph in `open-design-ledger.md`; in particular, it predates the UC1
Communication and UO1 Observe blockers and must not be used to select work.
