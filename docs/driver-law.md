# Bombay Driver law

This is the accepted normative contract for `bombay-engine` and its Bombay
runtime integration. Every `D-*` identifier is mandatory.

The selected contract is Behavior Core and Actors 0.20.0 at
`804b2bf25325a523884ec49d8a4ae6d2d2b6e9da` and Macros 0.13.0 at
`3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`.
The owner supplies one direct `Behavior -> Actions` fold, ordered creations,
named send interpretation, total action settlements, and typed source custody.
Every ordinary Driver terminal path transfers all still-owned settlements
through the one retirement barrier and returns the final concrete Behavior
together with the prepared or active environment residual.

The corresponding verification design is
[`driver-test-strategy.md`](driver-test-strategy.md).

## Ownership boundary

The former 68-row catalogue mixed the universal Engine loop with laws already
owned by Behavior, Behavior Actors, Bombay's concrete environment, primitive
crates, and future test campaigns. That catalogue was removed rather than
manufacturing Engine evidence for contracts Engine cannot observe.

At the selected revision, Behavior owns initialization and transition,
complete `Actions`, creation-before-send interpretation, total settlement
classification, source custody, and closed child products. Behavior Actors
owns template topology and lifecycle policy. Bombay owns the local mailbox,
address lease, timers, observation, capability interpreters, incarnation task,
and publication/retirement integration. Engine owns only the affine causal
protocol below.

## Explicit law set

These eight identifiers are the complete normative Engine index. Explanatory
text can refine them but cannot create another mandatory law implicitly.

- **D-INIT-1 — One initialization and activation boundary.** The Driver invokes
  Behavior initialization exactly once. Rejection retires the prepared
  environment without activation. A panic in the synchronous pure initialization
  fold also retires the prepared environment and returns the surviving behavior
  with `InitializationPanicked`; it does not invent an error or Actions. Success
  transfers the complete initialization
  `Actions` once to `Environment::activate`; activation failure returns its exact
  prepared residual. The resulting settlement is classified before ordinary
  ingress, and the active environment is published exactly once only after no
  initialization settlement can make further source progress.
- **D-TURN-1 — One causal turn at a time.** The Driver obtains at most one event,
  folds the current Behavior synchronously exactly once, installs that
  successor state, and applies the complete returned `Actions` exactly once
  before asking for another event. It does not prefetch, recurse, retry, roll
  back, or wait for facts beyond the Environment's declared local commitment.
  Pending sources and commits remain pending without busy-waiting or self-wake.
- **D-SETTLE-1 — Ordered total-settlement custody.** After every successful
  decision, the Driver preserves Behavior's exact settlement and offers at most
  one result to its typed source. An admitted result and its transitive action
  chain progress before the older product; a retained product is never
  reoffered and cannot hide an older progressing product. Source closure or a
  corrupt settlement is terminal, with every still-owned product transferred
  to retirement in causal queue order.
- **D-TERM-1 — Exact fused terminal disposition.** Accepted or lawfully rejected
  final actions accompany active stop; corruption overrides stop. Rejected or
  corrupt installed initialization is failure even when initialization chose
  stop. Permanent ordinary-source closure is exhaustion and creates no
  synthetic turn. Behavior, activation, and settlement failures remain
  disjoint. Once a terminal edge is selected, no later event, fold, apply, or
  source offer begins.
- **D-RETIRE-1 — Honest affine retirement.** Every ordinary return crosses the
  applicable prepared or active retirement barrier exactly once and returns
  the final concrete Behavior, exact residual, complete settlement custody,
  and factual disposition together. A pure initialization panic is caught
  before commitment and returns through prepared retirement. Panic after that
  boundary or cancellation drops the currently owned execution and never
  falsely claims that asynchronous retirement or completion occurred.
- **D-PORT-1 — One typed phased Environment port.** `Environment<B>` alone owns
  preparation and activation; `ActiveEnvironment<B>` alone owns ingress,
  action application, source settlement, publication, and active retirement.
  The port preserves `B::Event`, `ActionsOf<B>`, settlement, errors, and
  residuals without type erasure or cross-incarnation mixing. The Driver adds
  no `Clone`, `Sync`, `'static`, runtime, or capability-specific bound that this
  protocol does not require.
- **D-SURFACE-1 — One opaque production path.** `Driver::run` is the sole
  direct-Behavior execution path. Engine contains no template, application,
  mailbox, address, timer, observation, identity, scheduler, dynamic registry,
  recovery, restart, or reusable lifecycle authority. Cross-crate technical
  visibility does not make Driver an application-facing lifecycle API.
- **D-EVIDENCE-1 — Executed revision-bound falsification.** Every retained law
  has one exact positive witness, one applicable boundary witness, and one
  unique mutation of its real owning source, all bound to the selected Behavior
  revision. The required Nix gate executes those witnesses, requires the named
  killer to fail against the mutation for a test-failure reason, and emits one
  immutable result record per law. Source-text policy checks are supplemental;
  appended forbidden strings and local status fields are not semantic evidence.

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
  replacement can receive another communication. Bombay's D-TURN-1 is therefore
  a deliberately stronger, non-reentrant local-commit policy, not a claim about
  the maximum concurrency allowed by the actor model.
- Current Akka describes one-message-at-a-time actor execution while placing
  dispatcher throughput and thread selection in a hidden execution environment.
  This supports D-TURN-1 and D-SURFACE-1; it does not make Akka's mailbox or
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
  supports D-TURN-1 and D-PORT-1: stronger order belongs in an explicit typed
  protocol, not in a universal loop.
- Plyukhin and Agha's distributed actor termination work (2020/2021) distinguishes
  actor termination from ordinary reachability and states safety/liveness under
  explicit assumptions. Bombay therefore does not make the Driver infer global
  quiescence; incarnation and higher runtime layers own lifecycle facts.
- Paul, Agha, Patterson, and Varela's failure-aware actor model (2021) proves
  eventual progress only under stated failure and scheduling assumptions. This
  supports D-TURN-1: the Driver must not advertise unconditional liveness.
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
    -> bombay private ActorExecution
    -> future concrete composition layers
```

The dependency and authority direction is one-way. A lower layer must not ask
application code to implement execution, routing, lifecycle, registration, or
product-traversal machinery.

## Transition ownership

`bombay-engine` owns only the universal affine Driver and Environment port.
It has no Transition adapter, Machine executor, topology fixture, or mutable
poison-reentry API. Bombay Entity owns its concrete slot transition and ordered
effect interpretation. Behavior Actors owns the separate actor `Machine`
template's receive/become/defer/stop policy. Neither law is copied into the
Driver.

## Inputs

One Driver execution consumes:

1. one inert, fully composed concrete `Behavior<Ph = Never>` definition; and
2. one concrete actor environment statically sufficient for that Behavior's
   complete event and action types.

`BehaviorAddr<B>` is projected through `B::Protocol`, separate from the
executable Behavior algebra. The Driver does not receive, store, allocate,
register, resolve, or release an address value. The concrete Bombay environment
and incarnation may own the address value required to give deliveries,
creations, observations, and exits their runtime meaning.

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

One complete Driver execution returns one `DriverRetirement`: the final
monomorphized Behavior, the exact prepared or active environment residual, and
the factual completion/failure disposition. It does not publish that result,
release an address, inspect either retained product, or classify task
cancellation by itself.

## Environment boundary

The Driver depends on two affine environment phases. `Environment<B>` is
prepared: it can consume itself and the complete initialization actions to
produce `ActiveEnvironment<B>`, or consume itself into its typed residual when
Behavior initialization fails. Activation rejection returns the error and
prepared residual together. Only the active phase can obtain the next
`B::Event`, commit later actions, or consume itself into the same residual
type. A prepared environment cannot expose ingress, and an active environment
cannot activate again.

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
Behavior. The following port sketch is schematic; the compiled contract lives
in [Engine](../crates/bombay-engine/src/driver.rs) and its
[law tests](../crates/bombay-engine/tests/driver_law.rs):

```text
trait Environment<B: Behavior<Ph = Never>> {
    type Active: ActiveEnvironment<
        B,
        Residual = Self::Residual,
        Settlement = Self::Settlement,
    >;
    type Settlement: ClassifySettlement;
    type Error;
    type Residual;

    async fn activate(self, actions: ActionsOf<B>)
        -> Result<
            (Self::Active, Interpretation<Self::Settlement>),
            (Self::Error, Self::Residual),
        >;
    async fn retire(self) -> Self::Residual;
}

trait ActiveEnvironment<B: Behavior<Ph = Never>> {
    type Settlement: ClassifySettlement;
    type Residual;

    async fn next(&mut self) -> Option<B::Event>;
    async fn next_source(&mut self) -> Option<B::Event>;
    async fn apply(
        &mut self,
        actions: ActionsOf<B>,
    ) -> Interpretation<Self::Settlement>;
    async fn offer_next(
        &mut self,
        settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement>;
    fn publish(&mut self);
    async fn retire(
        self,
        settlements: Vec<Self::Settlement>,
    ) -> Self::Residual;
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
    -> retain the complete Interpretation settlement
    -> offer ordered results back to their typed sources
    -> repeat
```

## Execution law

The Driver performs this sequence and no other:

```text
initialize the owned Behavior exactly once
    -> consume Environment::activate with the complete initialization actions
    -> receive the only ActiveEnvironment and exact initialization settlement
    -> resolve live-return custody or retain the product for retirement
    -> publish the installed environment
    -> if terminal: retire and return Behavior + residual + disposition
    -> otherwise:
        obtain exactly one event
        -> fold it exactly once
        -> interpret the complete successful actions into one total settlement
        -> resolve live-return custody or retain the product for retirement
        -> if terminal: retire and return Behavior + residual + disposition
        -> otherwise repeat
```

The Driver never obtains the next ordinary event until interpretation and all
progressing source custody from the previous complete action value have become
quiescent. A source-admitted control event is the only intervening input; its
new settlement progresses before the older residual resumes. A terminally
retained product remains at its original queue position until retirement and
is never offered again.

The environment interprets creations in vector order before sends from the
same action value. It preserves the documented order within every named send
lane. The Driver does not know or traverse the structure of a send product.

A controlled Behavior error ends execution without interpreting nonexistent
actions. An environment interpretation error ends execution without obtaining
another event. Permanent environment closure ends execution without invoking a
Behavior turn.

Disposition classification observes the complete settlement before honoring a
stop verdict. Corruption therefore overrides an active stop. Lawful rejection
from an active stopping turn remains stopped with exact retirement custody,
while rejected or corrupt installed initialization overrides an initialization
stop because successful initialization was never established.

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
- ordinary execution classification;
- causal ordering of pending and terminally retained total settlements;
- ordinary environment retirement; and
- transfer of the final concrete Behavior and typed environment residual
  without inspecting or erasing either one.

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
9. perform the final decomposition/public-interface audit before claiming
   project-wide distillation.

No step may temporarily introduce a second production transition path.
