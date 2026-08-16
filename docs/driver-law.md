# Bombay Driver law

This is the accepted normative contract for `bombay-engine` and its Bombay
runtime integration. Every `D-*` identifier is mandatory.

The corresponding verification design is
[`driver-test-strategy.md`](driver-test-strategy.md).

## Explicit law set

These identifiers are the canonical index for this discussion draft. A law is
not accepted merely because it appears here. Acceptance, rejection, or revision
must be recorded explicitly. Explanatory sections below must cite and must not
contradict these laws.

### Behavior-boundary laws

- **D-BEH-1 — Universality.** One Driver algorithm runs every closed custom
  Behavior and every supported actor-template composition. A template cannot
  introduce a second Driver or event loop.
- **D-BEH-2 — Closed input.** The Driver receives only the final composed
  `B::Event`; it does not inspect or select the event's originating capability
  or mailbox lane.
- **D-BEH-3 — Closed output.** One successful fold yields one complete
  `ActionsOf<B>`. The Driver neither invents, erases, duplicates, nor
  template-specifically traverses action lanes.
- **D-BEH-4 — Behavior opacity.** The Driver knows only the Behavior contract.
  It contains no supervisor, proxy, pool, timer, persistence, workflow,
  routing, discovery, or application-domain branches.
- **D-BEH-5 — Exactly-once initialization.** The owned Behavior is initialized
  exactly once before any active event is accepted.
- **D-BEH-6 — Exactly-once turn.** Every event accepted from the environment is
  folded through the current Behavior exactly once.
- **D-BEH-7 — Decision integrity.** The successor Behavior state, complete
  actions, and continue/stop verdict from one fold are one indivisible
  decision. No part may be combined with another turn.
- **D-BEH-8 — Controlled failure.** A controlled initialization or turn error
  ends Driver execution. No nonexistent actions are interpreted and no later
  event is accepted.

### Turn-order laws

- **D-TURN-1 — Single active turn.** At most one Behavior turn is active for
  one Driver execution.
- **D-TURN-2 — Non-reentrancy.** Turn `N + 1` cannot begin until turn `N` and
  local commitment of its complete actions have finished.
- **D-TURN-3 — Initialization ordering.** Complete initialization actions are
  committed before the first active event is obtained.
- **D-TURN-4 — Commit-before-next-input.** Complete actions from an accepted
  event are committed before the Driver asks for another event.
- **D-TURN-5 — Commit is local.** Commitment ends at each capability's owned
  acceptance/rejection boundary. It does not wait for recipient processing,
  timer expiry, storage completion, transport acknowledgement, or a business
  reply.
- **D-TURN-6 — External completion by event.** Facts produced after local
  commitment can influence the Behavior only by returning through a declared
  typed event lane.
- **D-TURN-7 — No hidden interleaving.** The universal Driver exposes no
  reentrancy flag or callback interleaving mode. Any future interleaving model
  requires a separate typed protocol and cannot weaken this Driver.
- **D-TURN-8 — No prefetch.** The Driver requests at most one event and owns at
  most one uncommitted event. It cannot read ahead, batch, scan, skip, or retain
  a private event backlog.
- **D-TURN-9 — Self-send is asynchronous.** A send to the current incarnation
  is committed through the ordinary delivery capability and can become only a
  later event. It cannot recursively invoke the Behavior.
- **D-TURN-10 — No capability callback turn.** An interpreter cannot enter the
  Behavior or Driver synchronously. A capability result that affects Behavior
  is enqueued as an event under D-TURN-6.
- **D-TURN-11 — Synchronous fold.** Initialization and event folding contain no
  Driver await point. The Behavior contract requires deterministic computation
  without I/O; termination remains a condition on user code. The Driver supplies
  no preemption, watchdog, or blocking-operation escape hatch.

### Action-interpretation laws

- **D-ACT-1 — Exactly-once interpretation.** Every successful decision's
  complete actions are handed to the environment exactly once.
- **D-ACT-2 — Creation precedence.** Creations are resolved in vector order
  before sends from the same action value, as required by Behavior's Actions
  contract.
- **D-ACT-3 — Lane preservation.** Order within each named action lane is
  preserved. The Driver invents no order between independent lanes beyond the
  ordering required by their owning contracts.
- **D-ACT-4 — Payload ownership.** Acceptance, rejection, failure, panic, and
  cancellation obey affine Rust ownership: no payload is duplicated or used
  after move. An unaccepted payload is returned only at a boundary whose
  contract promises recovery; cancellation or unwind may instead drop the value
  currently owning it and must not falsely report recovery.
- **D-ACT-5 — Honest completion.** Committing a delivery proves only the
  documented admission boundary, never recipient processing or business
  success. The same rule applies to every other capability.
- **D-ACT-6 — Stop actions survive.** Final actions emitted with a stop verdict
  are committed before ordinary terminal return.
- **D-ACT-7 — Interpretation failure is terminal.** If commitment fails, the
  Driver accepts no later event and reports the environment failure to its
  incarnation owner.
- **D-ACT-8 — No fictitious transaction.** General action commitment is not an
  all-or-nothing transaction. If an interpreter fails or is cancelled after a
  successful prefix, the already accepted prefix remains factual and is never
  reported as rolled back.
- **D-ACT-9 — No implicit retry.** The Driver never retries an action or an
  uncertain commit. Retry, deduplication, idempotency, acknowledgement, and
  delivery guarantees belong to explicitly composed capability protocols.
- **D-ACT-10 — State is not rolled back.** A successful fold installs its
  successor Behavior before commitment. If commitment subsequently fails, the
  incarnation terminates; the Driver does not reconstruct the predecessor
  state or reuse the successor in another incarnation.
- **D-ACT-11 — Creation-result scope.** Same-action creation observations refer
  only to creations in that exact action value. They report the exact committed
  installation or rejection once, cannot observe an earlier/later turn, and
  cannot convert an unobserved creation failure into recovery.

### Capability-boundary laws

- **D-CAP-1 — Static sufficiency.** A concrete environment must statically
  supply `B::Event` and interpret every lane of `ActionsOf<B>`. A missing
  capability interpreter is a compile-time failure.
- **D-CAP-2 — Heterogeneous ownership.** Each capability retains its own typed
  requests, facts, errors, ordering, cancellation, and resource laws. The
  Driver does not normalize them into one dynamic service protocol.
- **D-CAP-3 — No ambient authority.** A Behavior receives no environment,
  runtime context, actor handle, channel, clock, registry, router, or service
  locator.
- **D-CAP-4 — No dynamic capability registry.** The Driver/environment boundary
  contains no `dyn` capability map, `Any`, downcast, erased action envelope, or
  runtime string/key lookup.
- **D-CAP-5 — Coherent environment lifetime.** One Driver execution cannot
  combine an event source, interpreter, or retirement scope from different
  incarnations.
- **D-CAP-6 — Runtime substitutability.** Live Bombay, deterministic tests,
  simulations, and future runtime adapters may implement the same environment
  law without changing Behavior or Driver semantics.
- **D-CAP-7 — Exact errors.** Capability errors retain their semantic lane and
  owned payload. The environment may compose them into a closed error sum but
  the Driver cannot erase, merge, stringify, or reinterpret them.
- **D-CAP-8 — No hidden guarantees.** The Driver adds no durability, remote
  transparency, global ordering, exactly-once delivery, timeout, retry, or
  acknowledgement guarantee beyond the selected capability contracts.

### Identity and scheduling laws

- **D-ID-1 — No address value.** The Driver stores and requires no actor
  address value. `B::Addr` remains only a type-level part of events, actions,
  creations, and exits.
- **D-ID-2 — No incarnation authority.** The Driver owns no registration lease,
  mailbox generation, observation subject, task handle, abort authority, or
  terminal publisher.
- **D-SCHED-1 — Scheduler separation.** The Driver selects no thread,
  dispatcher, priority, throughput quota, reduction budget, or preemption
  policy.
- **D-SCHED-2 — Mailbox-policy separation.** Mailbox admission, priority,
  control/user fairness, queue capacity, and producer backpressure remain owned
  by Communication and the concrete event source.
- **D-SCHED-3 — Cooperative resumability.** The incarnation/executor may yield
  and resume Driver polling according to its scheduling budget without
  changing the accepted event/action transcript.
- **D-SCHED-4 — Source order is authoritative.** The Driver preserves the exact
  order yielded by its event source and promises no global, cross-sender, or
  cross-lane order that the source does not promise.
- **D-SCHED-5 — Progress is conditional.** Driver progress requires a ready
  source, a terminating Behavior fold, a completing interpreter, and continued
  executor polling. The Driver promises neither fairness nor liveness when one
  of those conditions is absent.
- **D-SCHED-6 — No busy wait.** When its source or interpreter is pending, the
  Driver remains pending and relies on the future's wake contract. It neither
  spins nor manufactures work.

### Termination laws

- **D-TERM-1 — Explicit stop.** A stop verdict ends Behavior ingress after its
  final actions are committed.
- **D-TERM-2 — Source closure.** Permanent environment closure ends execution
  without synthesizing a Behavior event or turn.
- **D-TERM-3 — Ordinary retirement.** Every ordinary Driver return attempts
  environment retirement exactly once before returning its result.
- **D-TERM-4 — Panic terminality.** A panic during initialization or a turn
  makes the execution terminal; no successor Behavior or later event is used.
- **D-TERM-5 — Cancellation honesty.** Cancelling the Driver future drops its
  owned Behavior/environment but does not claim that asynchronous retirement
  completed.
- **D-TERM-6 — No recovery surface.** The black-box Driver exposes no poison
  recovery, reset, restart, or reuse operation. Replacement is a new
  incarnation constructed by its owner.
- **D-TERM-7 — Terminal results are honest.** Explicit stop returns
  `Completion::Stopped`; permanent input exhaustion returns
  `Completion::Exhausted`. Both remain successful `Ok` values. Behavior and
  environment failures retain their exact, disjoint `DriverError` variants.
  Panic and cancellation remain distinguishable at the incarnation layer that
  owns their classification.
- **D-TERM-8 — Terminal means fused.** Once any terminal path begins, the Driver
  never polls the event source, folds Behavior, or starts another action commit.

### Incarnation-integration laws

- **D-INC-1 — Transactional initialization.** Root activation and child birth
  complete Driver initialization and initialization-action commitment before
  publishing the new address generation.
- **D-INC-2 — One execution per generation.** One incarnation owns exactly one
  Driver execution; a replacement receives fresh Driver and environment
  values.
- **D-INC-3 — Drop before publication.** On return, failure, panic, or
  cancellation, Driver-owned resources are retired or dropped before address
  release and terminal outcome publication.
- **D-INC-4 — Terminal classification ownership.** Driver reports exact
  Behavior/environment failures and the factual successful `Completion` cause.
  The incarnation classifies task panic/cancellation and publishes actor death.
- **D-INC-5 — No split Driver lifecycle.** Transactional activation is
  coordinated by a later construction/publication owner and the concrete
  environment around the first local action commitment. The Driver exposes
  only consuming `run`; no caller can invoke initialization, looping, or
  retirement independently.
- **D-INC-6 — Publication is the narrow transaction.** Address-generation
  publication is transactional: failed preparation releases reservations and
  publishes no live generation. This does not turn ordinary action commitment
  into a transaction or erase a factual committed prefix.

### Surface and dependency laws

- **D-API-1 — Black-box application surface.** Ordinary application authors
  use actor definitions and whatever typed capabilities later composition
  layers provide—not Driver lifecycle controls.
- **D-API-2 — Technical visibility is not authority.** Items made public only
  for cross-crate integration remain hidden from the facade and documented as
  internal integration surface.
- **D-API-3 — No template-specific API.** The Driver exposes no capability- or
  template-specific methods or result types.
- **D-API-4 — Minimum bounds.** Driver types add no `Clone`, `Sync`, `'static`,
  allocation, serialization, or thread-affinity bound unless the exact owning
  executor or capability contract requires it.
- **D-DEP-1 — No Transition dependency.** `bombay-engine` does not depend on
  `bombay-transition`; actor templates may use Transition independently when
  they genuinely own representable-machine laws.
- **D-DEP-2 — No Machine Executor dependency.** `bombay-engine` does not depend
  on `bombay-machine-executor`; executor policies remain available to concrete
  consumers that independently require them.
- **D-DEP-3 — Single production path.** No direct fold bypass, legacy Driver,
  compatibility executor, test hook, or template-specific loop forms a second
  production execution path.

### Verification laws

- **D-VER-1 — Observable transcript.** Tests can observe a deterministic
  transcript of initialization, accepted events, decisions, commitments,
  terminal classification, and retirement without gaining control over the
  production lifecycle.
- **D-VER-2 — Inversion sensitivity.** Every law above has an oracle that fails
  when the corresponding ordering, ownership, or boundary is deliberately
  inverted.
- **D-VER-3 — Complete template accounting.** Every exported actor template,
  strategy variant, public event/outcome path, supported composition edge, and
  rejected composition is accounted for by the Driver test matrix.
- **D-VER-4 — Repository-wide closure.** Tests, examples, benchmarks, fuzz
  targets, research probes, diagnostics, documentation, and re-exports contain
  no unaccounted Driver bypass or obsolete normative contract.
- **D-VER-5 — Observation is non-semantic.** Test transcripts, tracing,
  metrics, and lifecycle diagnostics may observe facts but cannot select work,
  mutate Behavior, alter ordering, keep an incarnation alive, or prevent
  retirement when an observer fails.
- **D-VER-6 — No untested law.** No `D-*` law may be accepted, implemented,
  declared complete, or retained without executable evidence that directly
  observes the promised property.
- **D-VER-7 — Falsification required.** Each law's evidence must include a
  deliberate violating implementation or mutation and must fail for that
  violation. A test that also passes when its law is inverted is not evidence.

## Purpose

The Driver is the universal, actor-independent execution black box for every
closed Bombay `Behavior`, including custom domain behaviors and compositions of
reusable actor templates.

## Research basis and limits

The law separates the minimal actor turn from policies supplied by a concrete
runtime or a typed protocol. Research is evidence for that boundary; no paper
is treated as an API specification.

- Agha's actor semantics permits an actor, in response to a communication, to
  compute a replacement Behavior, create actors, and send messages. It does not
  require Bombay to await the external completion of those messages before the
  replacement can receive another communication. Bombay's D-TURN-2 is therefore
  a deliberately stronger, non-reentrant local-commit policy, not a claim about
  the maximum concurrency allowed by the actor model.
- Current Akka describes one-message-at-a-time actor execution while placing
  dispatcher throughput and thread selection in a hidden execution environment.
  This supports D-TURN-1 and D-SCHED-1; it does not make Akka's mailbox or
  dispatcher choices universal laws.
- Current Orleans defaults to non-reentrant request completion but permits
  explicitly selected interleaving. This supports making Bombay's default
  explicit. It also confirms that interleaving is a separate protocol/policy,
  not an accidental consequence of using `async`.
- Erlang/OTP separates process signal ordering, selective receive, reductions,
  and scheduling. Bombay likewise leaves source selection and scheduling below
  the Driver; it does not adopt selective receive in the Driver.
- *Actor Capabilities for Message Ordering* (2025) obtains stronger ordering by
  constraining actor references with typed capabilities and effects. That
  supports D-SCHED-4 and D-CAP-8: stronger order belongs in an explicit typed
  protocol, not in a universal loop.
- Plyukhin and Agha's distributed actor termination work (2020/2021) distinguishes
  actor termination from ordinary reachability and states safety/liveness under
  explicit assumptions. Bombay therefore does not make the Driver infer global
  quiescence; incarnation and higher runtime layers own lifecycle facts.
- Paul, Agha, Patterson, and Varela's failure-aware actor model (2021) proves
  eventual progress only under stated failure and scheduling assumptions. This
  supports D-SCHED-5: the Driver must not advertise unconditional liveness.
- Actor concurrency-bug studies and DS2 schedule exploration show that actor
  isolation does not remove protocol deadlocks, livelocks, ordering bugs, or
  failure races. Those are addressed by the verification strategy, not by
  making the Driver a policy engine.

Primary references:

- [Agha, *Actors: A Model of Concurrent Computation in Distributed
  Systems*](https://www.ics.uci.edu/~jajones/INF102-S18/readings/28_Actors-AghaThesis.pdf)
- [Akka typed actor introduction](https://doc.akka.io/libraries/akka-core/current/typed/guide/actors-intro.html)
- [Akka dispatchers](https://doc.akka.io/libraries/akka-core/current/typed/dispatchers.html)
- [Orleans request scheduling](https://learn.microsoft.com/en-us/dotnet/orleans/grains/request-scheduling)
- [Plyukhin and Agha, *Scalable Termination Detection for Distributed Actor
  Systems*](https://arxiv.org/abs/2007.10553)
- [Paul et al., *Verification of Eventual Consensus in Synod Using a
  Failure-Aware Actor Model*](https://arxiv.org/abs/2103.14576)
- [Gordon, *Actor Capabilities for Message Ordering*](https://arxiv.org/abs/2502.07958)
- [Al-Mahfoudh et al., *Efficient Linearizability Checking for Actor-based
  Systems*](https://arxiv.org/abs/2110.06407)
- [Torres Lopez et al., *A Study of Concurrency Bugs and Advanced Development
  Support for Actor-based Programs*](https://arxiv.org/abs/1706.07372)

Exactly one Driver algorithm exists. A supervisor, proxy, pool, state machine,
timer policy, persistence adapter, or other template changes the concrete
event and action algebra; it does not add another Driver or event loop.

## Thin-layer property

The Driver is important because it owns the causal boundary immediately above
Behavior, not because it contains runtime machinery.

Its complete recurring responsibility is:

```text
ask the environment for B::Event
    -> fold the Behavior exactly once
    -> give the complete ActionsOf<B> to the environment
    -> wait until those actions are committed
    -> repeat or stop
```

The Driver owns this ordering and nothing capability-specific. It does not
route a delivery, schedule a timer, create a child, write storage, publish an
observation, select a mailbox policy, or know an actor address. Those operations
belong to the environment and its capability-specific interpreters.

The Driver is therefore the next thin layer over Behavior:

```text
Behavior: Event -> Actions
Driver:   acquire Event -> invoke Behavior -> commit Actions
Runtime:  give each Event and Action its concrete meaning
```

This separation must remain visible in the implementation. Convenience is not
grounds for moving an interpreter, registry, scheduler, address, or service
handle into the Driver.

## Layering

```text
application domain and topology
    -> Behavior templates and static composition
    -> bombay-engine Driver
    -> bombay::core::Incarnation
    -> future concrete composition layers
```

The dependency and authority direction is one-way. A lower layer must not ask
application code to implement execution, routing, lifecycle, registration, or
product-traversal machinery.

## Transition removal gate

The first implementation change is to remove `bombay-transition` and
`bombay-machine-executor` from `bombay-engine`.

The actor Driver uses none of Transition's meaningful capabilities:

- no topology is derived from a composed Behavior;
- no `then`, `product`, or `routed` machine composition is used;
- the production adapter reports a fabricated one-state topology; and
- Machine Executor exists in this path to wrap the Behavior turn and make a
  publicly reusable mutable Driver poisonable.

Transition remains valid infrastructure for Bombay Entity and any future actor
template that explicitly owns a representable state-machine invariant. It is
not part of the universal actor execution path merely because another Bombay
subsystem uses it.

Removal is accepted only when the replacement Driver passes all laws and
inversion oracles in this document. Tests must prove behavioral equivalence;
the old adapter, topology fixture, executor compatibility tests, and
poison-reentry API must then be deleted rather than preserved as a parallel
path.

## Inputs

One Driver execution consumes:

1. one inert, fully composed concrete `Behavior<Ph = Never>` definition; and
2. one concrete actor environment statically sufficient for that Behavior's
   complete event and action types.

`B::Addr` remains part of the Behavior algebra. The Driver does not receive,
store, allocate, register, resolve, or release an address value. The concrete
Bombay environment and incarnation may own the address value required to give
deliveries, creations, observations, and exits their runtime meaning.

The Behavior never receives an environment, runtime context, actor handle,
channel, clock, registry, router, or ambient capability interface.

## Outputs

A successful Behavior fold produces its complete typed `Actions` value:

- named send lanes;
- ordered fresh child creations; and
- continue or terminal verdict.

Actions are commands across the Driver/environment boundary, not a public
stream of application results. The environment consumes every action exactly
once. Domain-visible results travel through typed deliveries, service
observations, external endpoints, or later events.

One complete Driver execution returns one terminal classification to its
incarnation owner. It does not publish that result, release an address, or
classify task cancellation by itself.

## Environment boundary

The Driver depends on two affine environment phases. `Environment<B>` is
prepared and has one operation: consume itself and the complete initialization
actions to produce `ActiveEnvironment<B>`. Only the active phase can obtain the
next `B::Event`, commit later actions, or consume itself in ordinary
retirement. A prepared environment cannot expose ingress, and an active
environment cannot activate again.

This is a Driver-facing internal port, not an application capability API.
Bombay may compose it internally from independently tested event-source,
effect-interpreter, and retirement components. The Driver must see one coherent
lifetime so it cannot combine a source from one incarnation with an interpreter
or retirement scope from another.

The environment owns live or simulated meaning. A Tokio-backed Bombay actor,
deterministic test, simulation, or future runtime adapter may provide different
implementations without changing Driver semantics.

### Commit is not external completion

The Driver waits for the environment to commit the current action value before
accepting another event. Commit means that the relevant runtime operation has
crossed its owned acceptance boundary, for example:

- a delivery was admitted or rejected with its payload preserved;
- a fresh child was installed or its creation was rejected;
- a timer or observation was registered;
- an asynchronous storage or transport operation was started; or
- a typed service request was otherwise accepted by its owning interpreter.

Commit does not mean that another actor processed a delivery, a timer expired,
storage completed, or a remote peer replied. Such external completion returns
later through a typed event. The Driver serializes Behavior decisions; it does
not hold an actor turn open for external work.

### Non-reentrant default

The Driver never begins a second Behavior turn while the first turn or
commitment of that turn's actions is incomplete.

```text
turn N decides
    -> commit turn N's runtime intents
    -> only then begin turn N + 1
```

This serializes decisions and protects actor-local state without serializing
the external world. Deliveries may wait in other mailboxes, timers may expire
later, and storage or transport work may finish later. Their factual results
re-enter through typed events.

The universal Driver has no reentrancy flag, callback, or hidden interleaving
mode. Any future interleaving capability requires a separately designed typed
template/runtime protocol with explicit state-isolation, ordering, liveness,
failure, and cancellation laws. It must not weaken the default Driver contract.

### Scheduling is outside the Driver

The Driver processes only one event at a time, but it does not choose threads,
dispatcher policy, priority, throughput quota, reduction budget, or preemption.
The incarnation and executor layer own cooperative scheduling and fairness.
They may poll or resume the Driver according to a work budget without changing
the Driver's event/action ordering law.

Mailbox lane selection and fairness also remain source-owned. The Driver sees
only the next already selected `B::Event` and must not reinterpret its origin.

## Capability composition

Runtime capabilities are heterogeneous and must remain owned by their distinct
interpreters. The Driver does not receive, enumerate, select, store, or look up
individual capabilities.

The final composed Behavior closes the Driver boundary with two concrete type
facts:

```text
B::Event       complete input algebra
ActionsOf<B>   complete output algebra derived from B's associated types
```

The runtime constructs one concrete environment specialized for that exact
Behavior. The Engine port is:

```rust,ignore
trait Environment<B: Behavior<Ph = Never>> {
    type Active: ActiveEnvironment<B>;
    type Error;

    async fn activate(self, actions: ActionsOf<B>)
        -> Result<Self::Active, Self::Error>;
}

trait ActiveEnvironment<B: Behavior<Ph = Never>> {
    type Error;

    async fn next(&mut self) -> Option<B::Event>;
    async fn apply(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error>;
    async fn retire(self);
}
```

The environment is a static composition of capability-specific interpreters:

```text
complete Actions
    -> ordered child creations  -> child-runtime interpreter
    -> delivery lanes           -> routing interpreters
    -> timer lanes              -> timer interpreter
    -> observation lanes        -> observation interpreter
    -> persistence lanes        -> persistence interpreter
    -> other typed service lanes -> their owning interpreters
```

This is not a service registry, dynamic capability map, erased envelope, or
ambient context. Each capability retains its own request, response, error,
ordering, and ownership laws. Static composition proves that the selected
environment can interpret every action lane and inject every runtime result
required by `B::Event`. A missing interpreter makes that Behavior/environment
pair fail to compile.

Adding a capability changes the composed Behavior's event/action types and the
concrete environment composition. It does not add a Driver branch, Driver
method, template-specific loop, or runtime handle to the Behavior.

The universal Driver remains limited to:

```text
obtain B::Event
    -> fold the Behavior once
    -> pass the complete ActionsOf<B> to its active environment
    -> await complete interpretation
    -> repeat
```

## Execution law

The Driver performs this sequence and no other:

```text
initialize the owned Behavior exactly once
    -> consume Environment::activate with the complete initialization actions
    -> receive the only ActiveEnvironment
    -> if terminal: retire and return
    -> otherwise:
        obtain exactly one event
        -> fold it exactly once
        -> interpret the complete successful actions
        -> if terminal: retire and return
        -> otherwise repeat
```

The Driver never obtains the next event until interpretation of the previous
complete action value has finished successfully.

The environment interprets creations in vector order before sends from the
same action value. It preserves the documented order within every named send
lane. The Driver does not know or traverse the structure of a send product.

A controlled Behavior error ends execution without interpreting nonexistent
actions. An environment interpretation error ends execution without obtaining
another event. Permanent environment closure ends execution without invoking a
Behavior turn.

## Transactional initialization boundary

Bombay root activation and child birth must commit initialization before
publishing a new address generation. The prepared environment owns this
transaction, and successful publication is required before it returns the
active environment. The Driver still exposes only one consuming operation:

```text
consume Driver::run
    -> initialize exactly once
    -> Environment::activate(initialization actions)
    -> terminal completion, or request the first event
```

The concrete environment may acknowledge its first successful local commitment
to a transaction owner without giving that owner access to Behavior state or a
Driver continuation. Publication may occur only after that acknowledgement.
Ordinary users and integration code receive no `prepare`, `run_init`,
`run_loop`, or `retire` phase controls. The consuming Driver future cannot be
cloned, restarted, initialized again, or used after completion.

## Ownership boundaries

The Driver owns:

- the concrete Behavior value during execution;
- exactly-once initialization;
- serialized one-event-at-a-time folds;
- ordering between event acquisition and complete action interpretation;
- ordinary execution classification; and
- ordinary environment retirement before returning.

The Driver does not own:

- an address value or registration lease;
- mailbox, timer queue, observation subject, router, or child scope semantics;
- task spawning or executor selection;
- abort authority or task-cancellation classification;
- terminal outcome publication;
- supervision, restart, routing, persistence, or stream policy; or
- Transition topology or machine-executor policy.

The incarnation owns one exact actor generation: address value, registration,
mailbox generation, environment resources, task, cancellation, and terminal
publication around one Driver execution.

A later layer must own construction: selecting concrete adapters, preparing
generation resources transactionally, establishing registration at the
correct boundary, launching exactly one incarnation task, and returning typed
external capabilities. This law does not prescribe a `System` object.

## Panic and cancellation

A panic during a Behavior fold terminates the incarnation. No subsequent event
may be obtained and no successor Behavior may be reused. The incarnation's
drop-owned terminal guard classifies the panic and preserves resource-drop,
address-release, and publication ordering.

Cancellation drops the Driver future and its owned Behavior/environment. The
Driver must not claim that asynchronous retirement completed after its future
was cancelled. The incarnation guard classifies cancellation only after
Driver-owned resources have been dropped.

The final black-box interface must not expose poison recovery or Driver reuse.
Poison is an internal consequence of a reusable executor seat; it is not an
actor-system lifecycle state.

## Public surface law

Application authors interact with actor definitions and the typed capabilities
of later composition layers. They do not receive Driver lifecycle controls.

Engine items may need technical public visibility because Bombay is a separate
crate. Such items must be hidden from the facade and documented as integration
surface. Public visibility does not authorize third-party lifecycle
orchestration.

The target Engine surface contains no independently public:

- Transition adapter or topology;
- machine executor;
- runtime-effect product traversal API;
- phase-state mutation API;
- manual retirement operation; or
- template-specific Driver.

## Required proof suite

Implementation is incomplete until each law has a positive oracle and a
deliberate inversion that fails. The machine-checked proof manifest contains
exactly one row for every `D-*` identifier above. Each row names:

1. the positive oracle;
2. the deliberate semantic inversion and the oracle that kills it;
3. negative and boundary cases;
4. cancellation, panic, and partial-commit ownership where applicable;
5. affected templates and composition edges;
6. the owning crate for any primitive-level proof; and
7. the command and gate that run the evidence.

The manifest must fail when a law is added, removed, duplicated, or renamed
without corresponding evidence. The exhaustive strategies and coverage-cell
rules are defined in [`driver-test-strategy.md`](driver-test-strategy.md).

## Implementation order

After this law is accepted and the Behavior dependency version is aligned:

1. add law-focused tests around the existing observable Driver behavior;
2. remove Transition and Machine Executor from `bombay-engine` dependencies;
3. replace `BehaviorMachine`/`ExclusiveExecutor` with one private direct
   Behavior turn seat;
4. make consuming `run` the only Driver lifecycle operation;
5. let incarnation and transactional activation coordinate publication around
   the concrete environment's first local commitment;
6. delete obsolete adapter, topology, executor-compatibility, poison-reentry,
   and public phase-state code;
7. update all examples, benchmarks, probes, tests, re-exports, and documents;
8. run workspace tests, Clippy, rustfmt, rustdoc, allocation, performance,
   mutation, concurrency, and adversarial gates; and
9. perform the final decomposition/public-interface audit before assigning
   `feature-complete`.

No step may temporarily introduce a second production transition path.
