# Bombay Driver test strategy

This is the accepted verification contract for the Driver law. It defines how
each law is falsified and does not itself constitute a completion claim.

The strategy supplements [`driver-law.md`](driver-law.md). Actor-template unit
tests prove each deterministic fold; these tests prove that the universal
Driver can execute every closed template/domain composition without learning
template semantics or violating runtime ordering.

## Fresh dependency and ownership record

This strategy was derived after rechecking the current local Behavior/Actors
source at `5d8c8e0b0294f92bd7ce90beb18646acd46393af`, Address at
`7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`, Observe at
`43016e5f781e006e072e77af996c4da64466dee8`, Timers at
`4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2`, and the locked Communication
0.1.1, Observe 0.1.0, Timers 0.1.0, Address 0.2.0, Transition 0.1.0, and Machine
Executor 0.1.0 APIs and tests. Transition and Machine Executor were inspected
only to verify their removal from Engine; they are not Driver dependencies.

The Behavior mismatch is deliberately resolved. The workspace and lock now
select `bombay-behavior-actors` 0.12.0 as the complete reusable actor layer and
`bombay-behavior` 0.12.0 as its core algebra. Engine accepts any closed
Actors/custom Behavior and does not recreate composition or routing algebra.

Ownership remains:

- Actors tests own each template's pure state-transition law.
- Engine tests own universal Driver sequencing and template opacity.
- Bombay tests own concrete capability interpretation and incarnation order.
- Communication, Address, Observe, and Timers retain their primitive
  concurrency/model tests; Bombay tests their composition rather than copying
  their implementations.
- Integration repositories own persistence, entity, transport, and other
  external capability conformance.

## Meaning of complete coverage

“All templates in all possible ways” cannot mean enumerating infinitely many
domain values or arbitrarily deep generic nesting. It means closing every
finite, declared dimension below and failing the test gate when an exported
template or supported composition is unaccounted for.

Completion requires:

1. every exported concrete actor template and strategy variant appears in a
   machine-checked manifest;
2. every public event constructor and every continue/stop/error outcome path
   for that template crosses the real Driver at least once;
3. every named action lane emitted by the template reaches its real or
   conformance interpreter;
4. every documented supported template-to-template composition edge compiles
   and runs in both meaningful orders;
5. every documented rejected edge fails to compile for the intended reason;
6. generated deeper compositions cover all compatibility-graph paths through
   an agreed depth and all semantically important full stacks beyond it; and
7. every Driver law has a positive oracle and a deliberate inversion that
   makes that oracle fail.

No aggregate test count substitutes for this closure accounting.

## Law-test manifest contract

The repository must contain one machine-readable manifest generated from the
canonical `D-*` headings in `driver-law.md`. Every law has exactly one manifest
row and no manifest row may name an unknown law. The CI consistency test fails
on missing, duplicate, renamed, reordered-without-review, or orphaned IDs.

Each row contains these mandatory fields:

| Field | Required meaning |
|---|---|
| `law` | Exact stable `D-*` identifier |
| `owner` | Crate and layer that can observe the property |
| `positive` | Executable oracle proving the promised behavior |
| `inversion` | Deliberate violating implementation or mutation |
| `killer` | Test that fails against that inversion |
| `negative` | Invalid input/composition/failure case |
| `boundaries` | Zero, one, limit, rollover, closure, or cancellation edges that apply |
| `adversarial` | Schedule/fault/fuzz campaign that applies |
| `templates` | Template and composition cells exercising the law |
| `command` | Exact reproducible command that runs the evidence |
| `status` | `planned`, `blocked`, or `passing` with evidence revision |

`not-applicable` is not a blanket escape. It is permitted only for a specific
field, with a written ownership/type argument and a review test that would fail
if the field later became applicable. `externally-owned` requires both the
owner's test identifier and a Bombay integration/conformance oracle; linking a
dependency test alone does not close a Driver law.

A row becomes `passing` only when its positive oracle passes, its inversion is
killed, all applicable negative/boundary/adversarial evidence passes, and the
named command is run in CI. Compilation, code review, coverage percentage, or a
test name without an assertion cannot substitute for executable evidence.

Law tests are divided by observable owner:

- Engine: Behavior opacity, exactly-once folds, turn sequencing, no prefetch,
  commit boundaries, terminal fusion, and black-box API constraints.
- Bombay integration: action-lane meaning, partial commits, transactional
  publication, incarnation/drop order, lifecycle classification, self-send,
  and concrete environment coherence.
- Primitive crates: their own mailbox, address, observation, timer, and typed
  capability algorithms, plus Bombay conformance tests at each adapter edge.
- Repository gates: dependency removal, single production path, visibility,
  forbidden-bound/static-bound checks, template accounting, and documentation
  synchronization.

The first test added during implementation is the manifest-consistency test.
No production Driver replacement may merge while any law row is `planned`,
`blocked`, missing its killer, or absent from the CI command set.

## Template manifest

The manifest is generated from the exact selected `bombay-behavior-actors`
public exports and checked against a reviewed semantic classification. The gate
fails when a public template is added, removed, or renamed without updating its
Driver evidence.

The current families to account for are:

| Family | Current templates and strategy variants |
|---|---|
| Composition/fundamental | `Machine`, `Stash`; `Activate` and `Compose` are lifecycle/composition traits, not templates |
| Lifecycle/shutdown/watch | `Task`, `Watch`, `FinalizeOnShutdown`, `StopOnShutdown` |
| Supervision | `Proxy`, `Supervisor`, one-for-one, one-for-all, rest-for-one, stop/retire reactions |
| Pools | `WorkerPool`, `KeyedWorkerPool`, affinity and interruption variants |
| Routing/admission | `Router<RoundRobin>`, `Router<Broadcast>`, `Router<LeastLoaded<_>>`, `Router<ConsistentHash<_>>`, `Router<RendezvousHash<_>>`, `WorkQueue`, `PriorityQueue`, `Buffer`, `CircuitBreaker`, `RateLimiter`, `Correlator`, `Acknowledgements`, `Sequencer`, `OrderGate`, `Deduplicator` |
| Discovery/pub-sub | `Registry`, `Resolver`, `Presence`, `Topic`, `PubSub` |
| Time | `Deadline`, `ReceiveTimeout`, `OneShot`, `Periodic`, `Lease` |
| Workflow | `Workflow`, `Barrier`, `Latch` |
| Persistence policy | `Cache` |
| Operations | `Health`, `Readiness`, `Configuration`, `Features` |

Protocol, message, state, result, error, and evidence types are not counted as
separate templates, but every variant they expose must be represented in the
owning template's event/outcome coverage.

Each manifest row records:

- exact type constructor and source revision;
- family and semantic owner;
- minimal custom domain used to close its generic slots;
- accepted event constructors;
- emitted named action lanes;
- initialization, continue, stop, and controlled-error reachability;
- required environment interpreters;
- supported inner/outer composition edges;
- deliberately rejected compositions;
- deterministic, Driver, Bombay-runtime, property, fuzz, mutation, and
  performance evidence identifiers; and
- unresolved runtime capability or external-integration blocker.

## User-defined behavior matrix

The Driver must first be tested independently of reusable templates using
user-defined Behaviors that vary every relevant shape.

### State shapes

- zero-sized and stateless;
- scalar counter and checked arithmetic boundary;
- enum typestate with every variant transition;
- owned collection with empty, one, capacity-boundary, and large states;
- move-only state with drop accounting;
- large and highly aligned state;
- nested domain state containing independently updated subdomains;
- state whose `Drop` panics in an isolated subprocess oracle; and
- `Send` but intentionally not `Sync` state, proving exclusive ownership is
  sufficient where the production contract permits it.

### Event shapes

- ordinary user event;
- composed user/service event sum;
- move-only payload;
- empty and maximum-size payloads admitted by the test configuration;
- repeated equal values and unique sequence identities;
- control-priority and user-lane events after real mailbox selection;
- timer, observation, creation-result, and shutdown facts; and
- events whose clone, comparison, hash, or drop behavior exposes accidental
  duplication or hidden constraints.

### Action shapes

- empty continue;
- exactly one send;
- multiple sends in one lane;
- heterogeneous named send lanes;
- creation only;
- multiple ordered creations;
- creations plus dependent sends;
- continue with actions;
- stop with final actions;
- controlled error with no actions;
- rejected action with exact payload recovery; and
- maximal configured batches and zero-capacity admission.

### Lifecycle shapes

- empty initialization, initialization actions, initialization stop,
  initialization error, and initialization panic;
- first-event stop/error/panic;
- stop/error/panic after a long successful prefix;
- permanent environment closure before any event and after arbitrary turns;
- cancellation while waiting for input, committing initialization, committing
  a turn, and retiring; and
- attempted reuse after every terminal route.

## User-defined behavior plus templates

Every template is closed over at least three custom domains:

1. a minimal inert domain that isolates template behavior;
2. a stateful domain emitting ordinary deliveries; and
3. an adversarial domain emitting heterogeneous actions, stopping, failing,
   and panicking at controlled points.

For wrappers, the matrix proves both owned and forwarded event lanes. An outer
template must consume its protocol exactly once and must preserve every inner
event and action it does not own. Reordering wrappers must either preserve the
documented semantics or be a compile-time rejection with a recorded reason.

The matrix includes semantic full-stack compositions, not only pairs, such as:

```text
shutdown + watch + task + supervisor + proxy + stateful domain
receive-timeout + deadline + stash + stateful domain
worker pool + supervisor + proxy + adversarial worker domain
registry/resolver/presence + stateful discovery domain
buffer/rate-limit/circuit-breaker + delivery domain
workflow + timer-backed domain
operations templates + versioned reply domain
```

Exact syntax and supported ordering come from the aligned Actors API; these
examples do not pre-approve combinations that the type algebra rejects.

## Composition generation

Maintain a reviewed directed compatibility graph whose nodes are templates and
whose edge `A -> B` means `A<B<Domain>>` is supported.

Generate:

- all nodes individually;
- every declared edge;
- every valid path of depth two and three;
- every repeated-template path the API intentionally supports;
- one maximal stack per distinct capability set;
- permutations where order is semantically meaningful;
- duplicated request types in different named lanes;
- multiple address/message/reply types; and
- compile-fail cases for every absent or explicitly forbidden edge.

Pairwise generation catches interaction defects economically; covering arrays
extend this to three-way capability interactions. Property-generated runtime
traces then vary events and outcomes within each compiled composition.

The generator emits stable case identifiers so shrinking or compiler errors can
be reproduced without regenerating the entire matrix.

## Oracle layers

### 1. Compile-time conformance

Use compile-pass and compile-fail fixtures to prove:

- every supported composition closes `B::Event` and its complete
  `ActionsOf<B>` projection;
- the selected environment interprets every lane;
- a missing capability interpreter fails compilation;
- an invalid event route fails compilation;
- no application implementation of Driver/interpreter plumbing is required;
- no `dyn`, `Any`, downcast, erased envelope, ambient context, or positional
  product traversal enters the path; and
- nominal user-defined Behaviors remain nameable in actors, births, endpoints,
  and routers.

Compiler diagnostics are snapshot-tested only for stable, intentionally
authored messages; otherwise fixtures assert pass/fail and the relevant error
category without freezing compiler prose.

### 2. Deterministic trace model

A synchronous model consumes a generated sequence of events and interpreter
outcomes and produces a canonical transcript:

```text
Initialized(actions)
EventAccepted(id)
Folded(id, actions)
ActionsCommitted(id)
Stopped | Closed | BehaviorFailed | EnvironmentFailed
Retired
```

The real Driver runs against a deterministic environment and must refine the
model exactly. Every accepted event appears once; no commit or terminal fact is
invented, lost, duplicated, or reordered.

The model also represents `CommitAccepted(lane, item)` prefixes. A later failure
or cancellation preserves that factual prefix, performs no implicit retry, and
never claims rollback. Creation publication is modeled separately as the narrow
transaction defined by D-INC-6.

### 3. Differential migration oracle

During Transition removal only, run identical generated traces through the
historical Driver and the candidate direct-Behavior Driver. Compare observable
actions, errors, stops, input ownership, and retirement. This is test-only
migration evidence, never a second production path, and is deleted after the
new law suite independently proves the replacement.

Historical behavior is not automatically correct: divergences are classified
against the Driver law, and a known historical bug becomes a negative fixture
rather than the expected result.

### 4. Metamorphic tests

Assert relationships that hold without a full expected transcript:

- appending events after a guaranteed stop changes nothing;
- splitting a source into ready/pending polls preserves the accepted trace;
- inserting scheduler yields changes no semantic output;
- replacing payloads with unique identities preserves their order and count;
- adding an empty action lane changes no other lane;
- wrapping with a semantic identity template preserves the inner trace;
- changing unrelated capability timing cannot change pure fold decisions; and
- deterministic replay from the same initial definition and facts produces the
  same transcript.

### 5. Model-based and property testing

Generate stateful command sequences covering initialization, events, action
commit success/failure, closure, stop, cancellation requests, and injected
panics. Compare every prefix against a small ownership/state model. Shrink by
removing operations, simplifying payloads, reducing composition depth, and
reducing pending-poll counts while retaining the failure.

Properties include exactly-once processing, prefix closure after termination,
payload conservation, action-lane conservation, creation-before-send, and
retirement idempotence at the observable boundary.

Generated histories also assert: at most one prefetched event, no recursive
self-send turn, no interpreter callback into Behavior, no polling after a
terminal edge, no busy polling while dependencies are pending, and equivalence
under arbitrary scheduler-yield insertion.

## Negative and boundary strategy

Every boundary is tested at `0`, `1`, configured maximum minus one, maximum,
and maximum plus one where representable:

- mailbox capacity and queued producers;
- send/action batch length;
- creation count and duplicate nonces;
- wrapper depth and event-sum width;
- timer deadline equality, past deadlines, and generation rollover;
- restart, retry, rate, queue, buffer, workflow, barrier, latch, cache, and
  membership limits;
- sequence/version/identity counters near rollover;
- empty and fully occupied child scopes;
- environment closure with empty and nonempty queues; and
- cancellation before poll, after readiness, during commit, and immediately
  before terminal publication.

Integer boundaries use checked construction and test the documented rejection
rather than relying on debug/release overflow differences.

Negative tests also include wrong reply behavior, wrong address family,
missing event injection, missing send interpreter, unsupported birth mode,
misordered wrapper products, stale timer/observation/creation facts, duplicate
completion, and attempts to use internal Driver phase controls from application
code.

## Adversarial strategy

### Poll and cancellation adversary

A manually controlled future harness chooses `Pending` or `Ready` at every
Driver await boundary. It drops and resumes owning futures at every legal poll
point, uses wake-before-register and wake-after-register schedules, and verifies
that no event or action is duplicated or lost.

### Fault injection

Inject failure and panic independently at:

- Behavior initialization;
- initialization-action commit;
- event acquisition;
- Behavior turn;
- each creation reservation/install/result stage;
- each named action interpreter;
- action commit after a successful prefix;
- environment retirement;
- lifecycle reporting and terminal observation; and
- payload/state destructors in isolated subprocess tests.

Every injection has an ownership oracle for Behavior state, event payload,
actions, child reservations, mailbox handles, timer tokens, address lease, and
terminal publication.

### Concurrency adversary

Race:

- many producers with Driver polling;
- control and user lanes under configurable aging;
- delivery with last-sender and consumer retirement;
- stop, abort, panic, and environment closure;
- timer replacement/cancellation with expiry;
- observation install/cancel with peer completion;
- child creation with parent retirement;
- address release with resolution and replacement generation; and
- external capability completion with incarnation cancellation.

Loom remains in the primitive owner for shared-memory algorithms. Bombay uses
real adapter compositions, deterministic schedule control where possible, and
repeated multithreaded races without claiming that repetition proves all
interleavings.

### Systematic schedule exploration

For bounded actor topologies, enumerate delivery, readiness, cancellation, and
failure schedules against a small sequential/reference model. Use partial-order
reduction only where operations are proven independent. Preserve and replay the
smallest counterexample schedule. Include protocol deadlock, behavioral
deadlock, livelock, orphaned-message, unexpected-order, and stale-generation
classes identified by actor-system testing research.

Linearizability is asserted only for a template or capability whose own
contract declares a linearization point. It is never projected onto the Driver
or onto asynchronous delivery generally.

## Extreme-adversarial campaigns

Run separate bounded campaigns unsuitable for ordinary unit tests:

- millions of turns with unique sequence identities;
- maximum supported composition depth and type width;
- sustained full mailbox with cancelled and blocked producers;
- control floods with waiting users and the exact configured fairness law;
- mass child creation/retirement and rapid address-generation reuse;
- equal-deadline timer storms, replacement storms, and stale expirations;
- simultaneous observer registration/completion/cancellation storms;
- panic and abort storms during every Driver phase;
- allocator-failure injection where the platform harness supports it;
- memory-pressure and file-descriptor/resource exhaustion for external
  interpreters in their owning integration suites;
- long-running soak with stable live-resource counts; and
- deterministic replay of every discovered failure seed.

Campaigns have explicit operation/time bounds, progress reporting, seed
capture, and hang detection. A timeout is a failure with the last reproducible
trace, not a discarded run.

## Specialized verification

- **Structured fuzzing:** generate typed event/interpreter operations rather
  than arbitrary bytes only; retain a byte decoder for libFuzzer integration.
- **Mutation testing:** require mutations of every Driver branch and ordering
  edge to be killed; surviving mutations create named test obligations.
- **Miri:** exercise move-only payload, drop order, aliasing, and cancellation
  smoke paths supported by Miri.
- **Sanitizers/platform tools:** run thread/address/leak tooling where Rust and
  the platform support the relevant configuration.
- **Allocation tests:** prove the steady-state Driver turn adds no allocation
  beyond the Behavior/environment operations being measured.
- **Performance tests:** measure empty, delivery-heavy, heterogeneous-action,
  deep-composition, closure, and retirement paths; compare distributions, not
  one sample.
- **Compile-time tests:** track representative deep-composition compile time,
  type-size diagnostics, and binary-size impact so static composition remains
  usable by developers.
- **Documentation tests:** every public authoring example runs through the same
  Driver path; no example implements hidden interpreter plumbing.
- **Repository audit:** scan examples, benchmarks, fuzz targets, research
  probes, documentation, and re-exports—not only `crates/`—for bypasses,
  obsolete Transition paths, positional products, and unmanifested templates.

## Coverage accounting

The test report is a matrix, not a percentage:

```text
template
  x domain shape
  x event/outcome path
  x action lane
  x composition edge/path
  x environment result
  x lifecycle terminal route
  x verification technique
```

Each cell is `covered`, `compile-rejected`, `externally-owned`, or `blocked`
with an evidence identifier. No blank cells are allowed. Line/branch coverage
locates unexamined implementation but does not close a semantic cell.

The report contains one row for every `D-*` identifier in
[`driver-law.md`](driver-law.md), mapping that explicit law to its positive
oracle, inversion and killer, applicable template/composition cells, and owning
test gate under the manifest contract above. Adding or renaming a law without
updating this mapping fails the documentation and test-manifest check.

## Completion gate

The ordinary `cargo test -p bombay-engine` command executes every currently
implemented standalone oracle and validates every manifest reference. The two
all-evidence assertions are explicit completion gates because they must remain
red while any integration row is honestly unexecuted:

```console
cargo test -p bombay-engine --test law_manifest -- --ignored
```

That command must pass before Driver-law completion can be claimed. Ignoring
the assertions during the ordinary development suite does not waive them; it
keeps already-passing per-law commands executable while preserving a distinct
red completion signal.

The Driver test strategy is ready for implementation only after:

1. the Driver law is explicitly accepted;
2. the Behavior dependency is aligned and the exact template manifest is
   regenerated;
3. every supported/rejected composition edge is reviewed;
4. owners agree which runtime-backed templates can run end to end and which
   remain blocked by absent interpreters;
5. every Driver law maps to at least one positive, negative, boundary,
   adversarial, mutation, and ownership oracle as applicable; and
6. ordinary, generated, fuzz, mutation, concurrency, Miri, allocation,
   performance, documentation, and repository-audit gates have explicit
   commands, budgets, and failure-retention rules.

Passing template unit tests, Driver unit tests, or workspace tests alone is
never sufficient.
