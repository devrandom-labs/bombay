# Runtime capability interfaces

Status: current normative runtime contract. This document supersedes older Bombay
descriptions of `LocalActors`, `LocalProtocol`, protocol namespaces, timer
driver channels, activation channels, and application-owned routing products.
Those descriptions remain useful history, but they are not the target
architecture.

The 2026-09-14 dependency restart selects the lean Behavior commit
`a272adf8d2cbb6a2784d565f47c74adff3e7d01b`. It owns the direct
`Behavior -> Actions` fold, ordered `Vec<Create<_>>`, named send interpretation,
and static birth installation. The earlier atomic settlement and role-first
application designs are superseded evidence only. Bombay's current contract is
root-first application assembly, exact interpretation errors, and terminal
behavior/environment custody through the one Driver.

## One runtime, two halves

An executable Bombay actor is the composition of exactly two kinds of value:

1. a deterministic foundational `Behavior`, wrapped where required by
   Behavior Actors' reusable policy templates; and
2. concrete runtime capabilities supplied and owned by the primitive crates.

The Behavior half decides. The capability half performs. A capability result
returns to the Behavior as a later typed event. No Behavior contains I/O,
channels, clocks, address tables, runtime handles, or executor tasks.

### Authoritative-fact conservation

Once Bombay obtains an authoritative typed fact, no reusable layer may
accidentally erase or misclassify it. Any preservation, transformation, or
discharge must be an explicit typed policy. This is a conservation law at the
Behavior/runtime boundary, not a rule that all failures bubble upward.

For an exact child terminal fact, a statically composed policy may restart the
child, retire its slot, discharge the fact as coordinated-shutdown completion,
or propagate the complete outcome. Bombay supplies the fact to every
independent typed consumer and interprets the selected actions; it must not
cancel one consumer because another observes the same incarnation, collapse
the outcome to an ordinary stop, or select propagation globally.

Bombay is the concrete application composition boundary. It does not introduce a second
mailbox, address namespace, observation cell, timer service, or Behavior
algebra. Its irreducible work is to own an incarnation, serialize
turns through the universal Driver, interpret the Behavior's typed effects,
and order activation and retirement across the primitive capabilities.

## Exact audited dependency set

The selected artifacts were refreshed on 2026-09-14 against this workspace's
lockfile, not against adapters or remembered APIs.

| Capability | Exact source used by Bombay | Current owner |
|---|---|---|
| Behavior | Git `bombay-behavior 0.14.0` at exact revision `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`, plus macros 0.11.4 from the same revision | pure `Behavior -> Actions` algebra, named `SendInterpreter` requests, ordered creations, static `DispatchBirth`/`InstallBirth`, closed products, and stable protocol/birth/event composition |
| Behavior Actors | Git `bombay-behavior-actors 0.14.0` at exact revision `a272adf8d2cbb6a2784d565f47c74adff3e7d01b` | reusable supervision, proxy, worker-pool, lifecycle, timing, routing, discovery, persistence, workflow, and operations actors over the same foundational algebra |
| Address | crates.io `bombay-address 0.2.0`, checksum `3e1516fea69ddb6d885c69e1e2f41d889f0a9b176cc1927fb6348b0646cb2cb1` | typed address claim, exact registration lease, opaque resolution |
| Communication | crates.io checksum `fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2`, package `bombay-communication 0.1.2` | two-lane mailbox, delivery, backpressure, affine user-admission retirement |
| Observe | Bombay-private import of semantic commit `b3b5f36a3b514713012086dfc72f5327d15fe2b2` plus exact Loom-bound fix `ef2ea13e65889aa3bf713822041e032020e98d73` from `feat/affine-observation` | keyed exact-generation facts plus shared and affine unkeyed publication pairs; no separately published actor API |
| Timers | Git `bombay-timers 0.1.0` at exact revision `13e884da7ab41781f52337b0038060e375b00ee0` | single-owner generation-safe timer queue |

Observe's complete selected implementation and verification corpus are now
owned privately by this workspace. Timers remains selected from the locked Git
revision; that exact source, its tests, and its documentation were inspected.

## Ownership table

| Question | Sole owner | Bombay's use | Bombay must not add |
|---|---|---|---|
| What does pure behavior decide? | Behavior | run the Driver and interpret typed actions | another behavior trait or effect algebra |
| Which reusable actor policy applies? | Behavior Actors | Bombay composes and runs the selected concrete template | a second template implementation or hidden policy |
| Where does a logical typed endpoint resolve? | Address | retain exact claims and leases for namespace-relative lookup while exact established capabilities, including external actors, carry their concrete endpoint directly | a named namespace object, second registry, erased endpoint map, or capability wrapper |
| How does an event reach one actor? | Communication | construct one mailbox per incarnation; retain its cloneable `MailboxRef` plus weak admission authority in the Address endpoint | Tokio delivery channels or a Bombay mailbox |
| How is a terminal fact published? | Observe | give observers the captured observation and retirement the publisher | Tokio activation/termination channels or a mutable Bombay observation cell |
| How do multiple policies consume one authoritative fact? | Behavior owns each typed disposition; Bombay owns independent exact delivery | retain independent observation tasks for every statically requested consumer and preserve the complete fact supplied to each | one per-incarnation consumer slot, implicit cancellation, outcome collapsing, or global propagation policy |
| How are due timers ordered and invalidated? | Timers | keep a `TimerQueue` in the actor-owned environment and drive its next deadline with the executor clock | a timer task, timer-command channel, second queue, or generation counter |
| Who owns child/supervisor/router/shutdown policy? | Behavior Actors | interpret named creation, delivery, observation, timer, report, and shutdown lanes | a second actor framework or dynamically selected policy |
| Who owns Entity lifecycle and its Bombay integration? | Bombay | compose Entity identity, activation, delivery, and retirement with the local runtime | a second entity runtime or erased entity registry |
| Who owns task execution and ordering? | Bombay | own executor tasks, capability instances, Driver turns, child task ownership, activation and retirement order | policy disguised as task plumbing |

The current runtime still retains one `ActorSpace<P>` per locally hosted
protocol as a transitional resolver. Behavior 0.14's exact established
capabilities are intended to remove that lookup from established internal
delivery. Logical recipients at discovery, external ingress, stable-name, and
transport boundaries continue to use Address; roles and routes never become
storage identities.

## External actor interface

`ActorInterface<Api>` is Bombay's transport-neutral application boundary.
`Api` is an application-defined static product of receptionist capabilities;
the runtime neither discovers exports from topology nor grants lifecycle
authority through the product. `ApplicationLifecycle<P>` is the separate
projection for root shutdown and termination.

`external::<P>()` composes existing owners rather than adding another actor
runtime: Bombay's application allocator supplies one fresh `MailAddr`, Address
claims the exact endpoint lease, Communication supplies one admission owner
and affine consumer, and the existing protocol-indexed `ActorRef<P>` supplies
the established endpoint carried by `EstablishedRecipient<P>`. The external
actor owns no Behavior, Driver, timer queue, child topology, or runtime task.
Its send method admits a `User` with the external actor's own address as
origin. Its receiver returns the complete `User` so reply provenance is not
discarded. Closure precedes lease retirement, and stale exact recipients reject
without retargeting.

The interface's direct send accepts only `EstablishedRecipient<Target>`. This
is not an arbitrary restriction: a logical `Recipient<Target>` resolves in one
selected Address namespace, while the transport-neutral interface deliberately
does not expose Bombay's private topology product or an erased protocol map.
An application that intends stable logical names must select a typed local
host or a transport-specific namespace interpreter explicitly. Receiving a
stable supervisor proxy therefore preserves a useful logical capability but
does not make it globally exact. A mixed `ReplyRoute<P>` also retains both
lanes in its static sends product and continues to require `Hosts<P>` for its
logical branch.

## Behavior foundation, Behavior Actors templates, and Bombay runtime

### Current contract retained

- `Protocol` is the stable address/message signature used by recipients,
  deliveries, address spaces, and live references. `Behavior::Protocol` names
  that signature; `Behavior` separately owns the deterministic executable
  transition with its event, phase, send product, birth product, and typed
  error.
- `Actions` and the Driver expose intent; they do not execute it.
- heterogeneous children are authored through `Children::new().child(...)`
  and `.create(...)`; `ChildChoice` supplies closed static dispatch.
- named semantic send products and `SendEffects::send` route effects without
  positional traversal or handwritten application product routers.
- Guardian, DynamicSupervisor, Supervisor, WorkerPool, Router, Watch,
  ShutdownCoordinator, timer-backed templates, and the rest of the reusable
  actor catalogue are Behavior Actors-owned policy expressed as ordinary
  Behaviors. Entity and concrete runtime integration remain Bombay-owned.

### Interface conclusion

Bombay is the ordinary dependency and application surface. It re-exports the
one Behavior authoring attribute and the selected Behavior Actors vocabulary;
direct foundational Behavior values remain the power-user escape hatch. Every
path terminates in the same concrete `Behavior -> Actions` algebra, and
Bombay's generic `App` owns execution.

The selected Behavior algebra now carries exact live endpoints through
`EstablishedRecipient<P>`, exact delivery, occurrence-preserving creation
facts, structural child delivery, observation, and shutdown. Sends still do
not imply local hosting. `ActorSpaces` and `Hosts<P>` remain the deliberate
advanced Bombay implementation for logical recipients and multi-protocol
application composition; they must not leak into ordinary authoring or be
replaced by dynamic normalization. Exact established delivery bypasses that
logical resolution path.

Bombay's current exact-capability implementation has `MailAddr` select
`ActorRef<P>` as its established endpoint, every activated reference can issue
an opaque `EstablishedRecipient<P>`, and exact delivery uses that reference
directly without destination `Hosts<P>` evidence or Address lookup. Logical
delivery remains unchanged. Exact observation and cancellation also use the
endpoint's retained Bombay-private termination observation through the existing
actor-local fact queue; observer IDs, immediate resolution, cancellation, and
terminal consumption share one authoritative queue state. Same-action creation
results, child shutdown, and Entity lifecycle integration remain owned by their
existing concrete Bombay interpreters and bindings.

## Address

### Current contract retained

`AddressSpace<A, E>` already supplies:

- exclusive `claim`/`try_claim`;
- an opaque, cloneable `Resolved<E>` snapshot;
- an exact-generation `Lease<A, E>` whose release or drop retires only its
  own registration;
- registration scope and registration identity;
- safe reclamation and shrinking.

Its tests cover conflict, stale lease isolation, release/drop, reentrant
endpoint destruction, panics, concurrent resolve/retire, reclamation, model
properties, Loom schedules, and fuzzing.

### Interface conclusion

Retain Address unchanged. Bombay stores concrete `AddressSpace` values
directly. The endpoint should contain only the delivery capability required by
an `ActorRef`—normally Communication's `MailboxRef` plus immutable observation
handles where the public reference contract requires them. Address must not
learn about actors, mailboxes, task ownership, observation, or topology.

Address resolution is not activation notification and should not acquire an
async wait API merely to compensate for missing Observe composition.

## Communication

### Current contract retained

`mailbox_channel(Config)` returns `ControlSender<C>`, affine
`MailboxOwner<U>`, cloneable `MailboxRef<U>`, and the single
`Consumer<C, U>`.

- the control lane is unbounded and non-blocking;
- the user lane is bounded and backpressured;
- control is preferred with a configured aging cap;
- `MailboxOwner<U>` is the sole strong user-admission authority;
- `MailboxRef<U>` does not keep admission alive or reopen it after closure;
- closure and teardown return every non-linearized payload exactly once;
- `Consumer::drain` recovers already queued values;
- receive and send cancellation are safe;
- the last sender closes its lane and consumer teardown wakes blocked senders.

The implementation has extensive unit, edge, teardown-oracle, allocation,
leak, stress, property, and Loom coverage. Bombay must reuse these semantics,
not wrap them in Tokio channels.

### Affine user-admission retirement

`MailboxOwner::close_admission(self)` (and owner drop) linearizes user-lane
closure. Already accepted payloads remain drainable. Racing, blocked, and
later user sends that did not linearize recover their exact payload. Stale
`MailboxRef` values cannot reopen admission. Control admission is intentionally
separate and remains live until the final `ControlSender` drops, allowing
shutdown and terminal facts to reach an actor after public ingress closes.

The concrete use is:

```rust,ignore
let (control, owner, mailbox, inbox) = communication::mailbox_channel(config);
owner.close_admission();
while let Some(event) = inbox.recv().await {
    // drain values accepted before close
}
```

Bombay adds no competing channel or cancellation policy. Its private weak
admission handle exists only so a typed `ActorRef` can atomically retire public
ingress before sending `ShutdownRequested` through the control lane.

### Bombay composition

Communication is the only in-process event transport. Runtime control facts
that are actor events use the control lane; user messages use the user lane.
Activation and termination are facts, not communications, and therefore use
Observe. Timer state is not communication and remains in Timers.

## Observe

### Current contract retained

`ObservationSpace<K, O>` creates exact subject generations. `Subject<K, O>`
is the single publisher and retention owner. `Observation<O>` captures one
generation and supports synchronous waiting, timeout, waker registration, and
async `IntoFuture`; completed outcomes remain visible to captured observers.
`Observation<O>` is cloneable without an `O` bound. Outcome retrieval by
`try_get`, `wait`, or `IntoFuture` still requires `O: Clone`; `into_outcome`
supports a move-only outcome only when the observation owns the final slot
reference and is therefore not a fan-out mechanism.
Dropping the subject retires only its exact generation and permits safe slot
reuse. Tests cover stale-generation isolation, cancellation, waiter races,
waker behavior, panics, reclamation, exhaustive/model schedules, and Loom.

### Unkeyed one-publication pair

Bombay's private Observe module exposes the shared unkeyed pair over the same
proven slot protocol:

```rust,ignore
let (publisher, observation) = observe::pair::<Outcome>();
publisher.complete(outcome);
let outcome = observation.await;
```

Required laws:

1. `pair<O>()` returns one non-cloneable `Publisher<O>` and one cloneable
   `Observation<O>`;
2. `Publisher::complete(self, outcome)` consumes the only publication
   authority, making double completion impossible through safe code;
3. cloning `Observation<O>` requires no `O` bound, while fan-out retrieval
   requires `O: Clone` and gives each observer a clone of the retained result;
4. completion remains visible after publisher consumption;
5. dropped or cancelled waiters do not consume the outcome or strand others;
6. publication wakes all registered waiters even when one user waker panics;
   every waiter is attempted before the first panic resumes;
7. the first version documents incomplete `Publisher` drop as leaving captured
   observations unresolved, matching the existing Observe publication model;
8. the pair allocates one fresh slot and does not allocate a key table, assign
   a generation, pool the slot, or pretend that `()` is identity;
9. dropping all publisher/observation handles destroys the exact slot, and no
   future pair can refer to it.

Observe deliberately does not publish abandonment from `Publisher::drop`.
Bombay therefore owns the stronger integration invariant that every launch and
retirement branch consumes its publisher with a semantic outcome; an accidental
drop remains a failed runtime invariant rather than acquiring new Observe
semantics.

For one-owner result transfer, including an exact rejected Entity command, the
same slot protocol also exposes an affine pair:

```rust,ignore
let (publisher, observation) = crate::observe::affine_pair::<MoveOnlyOutcome>();
publisher.complete(outcome);
let outcome = observation.await;
```

`AffineObservation<O>` is not cloneable and moves the exact `O` without an
`O: Clone` or `O: Sync` bound. Cancellation removes its current waker and task
migration replaces stale registration. This is private runtime machinery;
ordinary actor APIs expose the semantic activation, termination, fence, or
dispatch result rather than Observe types.

The existing keyed API remains correct for discoverable and replaceable keyed
subjects. A direct pair represents one already-identified, non-replaceable
fact; its fresh allocation is its identity and needs no generation number.
Bombay now uses this directly. Retirement owns the termination publisher;
`ActorRef` carries the observation. Activation uses a separate pair whose
outcome is the exact live reference or typed activation rejection. The keyed
`TerminationCell`, activation MPSC, and test oneshot have been deleted.

## Entity

Bombay owns local Entity installation and lifecycle composition.
`EntityDefinition` names one stable ID, the complete authored Behavior stack,
its concrete application host product, asynchronous hydration, and exact fact
consumers. `App::entity_family` installs that definition under a semantic role;
`ApplicationHandle::entities` selects its cloneable receptionist, while the
application retains shutdown and task-join authority.

`EntityCapacity` keeps concurrent hydration and active-or-activating resident
bounds independent. Hydration finishes before address allocation and
routability. All families use the application's one `ApplicationAddresses`
source, and native execution reuses the existing child bindings, typed effect
interpreters, terminal projection, and owned task hierarchy. It does not add a
second actor contract, registry, lifecycle service, or implicit
`StopOnShutdown` policy.

External admission uses `ExternalActor::send`, preserving the external actor's
claimed `MailAddr`. Inside a Behavior fold, `EntityRef::request` produces an
`EntityAdmission<D>` in the exact `InterpreterRequests` lane; the interpreter
supplies the emitting actor's address. A refusal returns the exact command to
the definition's typed `admission_refused` consumer. Admission remains
distinct from processing and durable completion.

`EntityActivationError` distinguishes resident-capacity refusal, hydration
failure, and exact actor launch retirement. Forced drains retain their complete
`DrainFailure`; normal retirement retains final state, residual ingress,
descendant terminals, and completion. Shutdown closes family admission only
after the root settles, drains every represented incarnation, joins all
family-owned tasks, and returns `(EntityShutdown, EntityMetrics)` for each
semantic role. Metrics remain finite-cardinality counters rather than an
entity-ID event stream.

Two definitions remain non-interchangeable even when their ID and command
types coincide, and one `EntityRef<D>` retains its logical identity across
passivation and reactivation. The generic `EntityRuntime` and
`LocalEntityRuntime` remain the advanced integration port; ordinary native
applications do not construct them.

## Timers

### Current contract retained

`TimerQueue<I, K, V>` already supplies single-owner schedule, cancel,
`next_deadline`, and `pop_due`. `Token<K>` carries generation identity;
replacement and stale cancellation are safe; equal-deadline ordering is
deterministic. Its test suite covers exhaustive traces, differential models,
adversarial generation behavior, memory bounds, and fuzzing.

### Interface conclusion

Retain Timers unchanged. A queue belongs inside each actor environment that
interprets timer effects. Bombay drives it in the same actor task:

1. schedule/cancel actions mutate the owned queue;
2. the event loop reads `next_deadline`;
3. the executor clock waits until either mailbox input or that deadline;
4. `pop_due(now)` turns due values into the exact typed runtime event;
5. that event enters a later Driver turn.

Bombay stores the queue in `LocalTimers`, a minimal shared typed view between
the Environment deadline poll and the effect interpreter. It has no task,
channel, command enum, or retirement protocol. Tokio supplies only the clock
wait and task scheduler; it does not own timer policy, identities, or a second
timer service. If a reusable clock abstraction is later required for virtual
time, it belongs at Bombay's executor adapter boundary and must not alter the
TimerQueue algebra.

## Minimal incarnation composition

One local incarnation needs only:

```text
Behavior value + universal Driver
Communication mailbox + affine retirement owner
Address lease for the mailbox's weak delivery anchor
Observe activation publisher/observation
Observe termination publisher/observation
TimerQueue owned by the actor environment
Pending Observe facts polled by the actor environment
Bombay-owned task and owned child-task collection
```

The distilled runtime has no `LocalActors`, `LocalProtocol`, public namespace,
activation channel, termination cell, timer task, timer command channel, or
runtime-stop oneshot.

## The Behavior spine

Bombay does need one spine that holds these capabilities together. That spine
already exists in `bombay-engine`: the affine `Environment<B>` /
`ActiveEnvironment<B>` typestate pair. It should remain the only actor-loop
port.

```rust,ignore
pub trait Environment<B: Behavior<Ph = Never>> {
    type Active: ActiveEnvironment<B, Residual = Self::Residual>;
    type Error;
    type Residual;

    fn activate(
        self,
        initialization: ActionsOf<B>,
    ) -> impl Future<Output = Result<Self::Active, (Self::Error, Self::Residual)>>;

    fn retire(self) -> impl Future<Output = Self::Residual>;
}

pub trait ActiveEnvironment<B: Behavior<Ph = Never>> {
    type Error;
    type Residual;

    fn next(&mut self) -> impl Future<Output = Option<B::Event>>;

    fn apply(
        &mut self,
        actions: ActionsOf<B>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn retire(self) -> impl Future<Output = Self::Residual>;
}
```

Its laws are:

1. `Environment` is prepared but cannot yield ingress.
2. `activate(self, initialization)` consumes the prepared value, commits the
   complete initialization actions, acquires the Address lease, and publishes
   activation. Only success returns an `ActiveEnvironment`.
3. `next` is the only event-acquisition point. It merges Communication input
   with due values from the owned TimerQueue; it does not decide policy.
4. `apply` receives one complete successful Behavior decision and dispatches
   its named effect lanes. It never calls the Behavior recursively.
5. `retire(self)` is affine and is the completion barrier for every capability
   owned by that incarnation.
6. The Driver owns the causal sequence `initialize -> activate ->
   (next -> transition -> apply)* -> retire`.

This trait is generic infrastructure and should stay hidden from ordinary
application users. It is public only at the framework-extension boundary if a
third party needs to provide an entirely different incarnation host.

### How adapters plug in

The spine itself must not grow one method per runtime feature. Effects are
open-ended and typed. Each capability plugs into the action interpreter at the
smallest semantic lane it owns:

```rust,ignore
pub trait InterpretSends<A: Address, Sends> {
    type Error;

    fn interpret_sends(
        &mut self,
        from: A,
        sends: Sends,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait SpawnChild<A: Address, C: Behavior<Protocol: Protocol<Addr = A>>> {
    type Error;

    fn spawn_child(
        &mut self,
        address: A,
        creation: Create<A, C>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

The exact creation dispatch remains Behavior's existing `InstallBirth` /
`DispatchBirth` contract. Observation, timer, report, shutdown, persistence,
and transport requests should use the same pattern only when their named
effect products are genuinely distinct. Bombay must not introduce one erased
`Capability` trait whose methods accept `dyn Any`, a runtime enum, or an
untyped request.

The concrete local environment is therefore ordinary static composition:

```rust,ignore
struct LocalEnvironment<B, Capabilities>
where
    B: Behavior,
{
    inbox: communication::Consumer<B::Event, User<BehaviorAddr<B>, BehaviorMessage<B>>>,
    mailbox_owner: communication::MailboxOwner,
    timers: timers::TimerQueue<Instant, TimerKey, B::Event>,
    address_lease: bombay_address::Lease<BehaviorAddr<B>, ActorRef<B::Protocol>>,
    termination: observe::Publisher<Termination<BehaviorAddr<B>>>,
    capabilities: Capabilities,
}
```

This shape is illustrative: Communication and Observe own the final names and
generic parameters. The important property is that the fields are the real
primitive values, not Bombay wrappers around them.

`Capabilities` is a closed static product assembled for the concrete
Behavior. Leaves implement only the lane traits they understand. Product
implementations delegate structurally to those leaves. The compiler therefore
proves that every emitted effect has an interpreter, while unused capabilities
need not exist.

### What is and is not pluggable

The distinction is semantic:

| Component | Pluggable? | Reason |
|---|---|---|
| Behavior value and Actors templates | yes, by ordinary generic composition | this is application policy |
| Named effect-lane interpreter | yes, statically | local/remote delivery, durable storage, OS tasks, and future adapters may perform the same typed request differently |
| Whole `Environment<B>` | yes, extension-only | simulators, deterministic test hosts, embedded hosts, or a non-Tokio executor may own a complete incarnation differently |
| Address, Communication, Observe, Timers in the standard local runtime | no runtime choice exposed to users | they are Bombay's selected lawful local primitives; making every app choose them adds configuration without meaning |
| Driver causal order | no | changing it changes Behavior execution semantics |
| Actor lifecycle/supervision/routing policy | not a runtime adapter | these are Behaviors and are composed as values |

So Bombay should “have them all” in its standard local environment, but hold
them as separable concrete capabilities behind the one Environment spine.
Pluggability exists where an application or integration can truthfully supply
a different mechanism, not merely because Rust permits a trait.

### Activation order

1. Construct the Communication mailbox and affine retirement owner.
2. Construct activation and termination Observe pairs.
3. Construct the public `ActorRef` from its typed address, weak delivery
   anchor, and termination observation.
4. Construct the Driver and actor-local interpreter capabilities.
5. Run initialization and transactionally interpret its complete action set.
6. Claim the Address only after initialization has committed successfully.
7. Publish activation success with the exact live `ActorRef`, or publish the
   typed rejection and retire every partially acquired capability.
8. Enter the mailbox/deadline event loop.

Publishing the address before initialization commits exposes a broken actor;
publishing activation before the address claim makes the returned reference
temporarily unresolved. Both are forbidden.

### One turn

1. Acquire one control, user, or due-timer event.
2. Convert it to the Behavior's exact typed event.
3. Run one Driver turn with no capability mutation during the fold.
4. If the fold rejects, retain the input according to its typed error and
   perform no partial effects.
5. If it succeeds, interpret named effect lanes in their defined commit order.
6. Capability success/rejection becomes a later event; it never re-enters the
   current Behavior call.

### Retirement order

1. Close new mailbox admission through Communication's affine owner.
2. Drain all values accepted before closure according to the selected normal
   shutdown protocol; forced retirement uses the separately typed forced path.
3. Let Bombay shutdown templates request and observe graceful child
   retirement in their declared order. When the owner itself terminates,
   cancel and join every remaining owned child as the non-negotiable ownership
   fallback.
4. Drop/cancel actor-local timers.
5. Drop the exact Address lease so new resolutions fail.
6. Drop all strong delivery senders; weak anchors cannot resurrect the lane.
7. Publish the exact terminal outcome through Observe.
8. Release the Bombay task owner.

The Address lease must retire before terminal publication: an observer that
sees termination must never subsequently resolve that same incarnation as
live. Child ownership must settle before the parent's terminal publication so
the published subtree outcome is truthful.

## Public API consequence

The functional entry remains intentionally small and value-oriented:

```rust,ignore
let terminal: ApplicationTerminal<_> = Application::new(root()).run()?;
inspect_terminal(terminal);
```

`Application::new(root).child(Role, actor)` is the ordinary root-first
declaration and execution value. The application-owned terminal projection
retains every root and child outcome. Advanced multi-protocol applications may
temporarily use `App::new()` with a typed local actor-space product where
intentional logical deliveries require additional canonical hosts.
Users configure supervision, routing, worker capacity, restart policy, and
shutdown policy on those template values, where the policy belongs. They never
construct a runtime, guardian, mailbox, observation space, timer queue, Driver,
or interpreter. Bombay re-exports Behavior's one owning macro at the root; it
does not add another actor trait, parser, expansion, or effect language.
`ActorExt` layers and owning template constructors produce ordinary concrete
Behavior values. Every construction terminates at this same boundary and
creates no second runtime, effect algebra, policy owner, or dynamic capability
mechanism.

## Acceptance gate

This capability layer is not complete merely when it compiles. Completion
requires all of the following:

1. no Tokio channel in Bombay duplicates Communication, Observe, or Timers;
2. no Bombay type duplicates an Address space or Observe subject;
3. no Behavior receives a runtime handle or performs I/O;
4. every interpreter-originated fact has a statically checked event consumer;
5. every typed effect lane has exactly one owning interpreter;
6. one local endpoint type has exactly one shared Address space per
   application instance;
7. activation cannot expose an uninitialized or unresolved actor;
8. terminal observation cannot coexist with resolution of the retired
   incarnation;
9. parent terminal publication implies all owned child tasks have settled;
10. user payloads are recovered exactly across full, closed, rejected,
    cancelled, initialization-failed, and shutdown races;
11. timers require no auxiliary task and reject stale generations;
12. the root functional API runs a heterogeneous created child, routes a
    message, observes a terminal fact, handles a timer, and shuts down the tree;
13. workspace tests, compile tests, documentation tests, formatting, strict
    Clippy, Loom/model suites in owning repositories, and repository-wide
    obsolete-pattern scans are green.
