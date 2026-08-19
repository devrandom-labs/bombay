# Runtime capability interfaces

Status: normative design input for E5. This document supersedes older Bombay
descriptions of `LocalActors`, `LocalProtocol`, protocol namespaces, timer
driver channels, activation channels, and application-owned routing products.
Those descriptions remain useful history, but they are not the target
architecture.

## One runtime, two halves

An executable Bombay actor is the composition of exactly two kinds of value:

1. a deterministic `Behavior`, including reusable policy templates from
   Behavior Actors; and
2. concrete runtime capabilities supplied and owned by the primitive crates.

The Behavior half decides. The capability half performs. A capability result
returns to the Behavior as a later typed event. No Behavior contains I/O,
channels, clocks, address tables, runtime handles, or executor tasks.

Bombay is the composition boundary. It does not introduce a second mailbox,
address namespace, observation cell, timer service, lifecycle algebra, or
supervision policy. Its irreducible work is to own an incarnation, serialize
turns through the universal Driver, interpret the Behavior's typed effects,
and order activation and retirement across the primitive capabilities.

## Exact audited dependency set

The audit was repeated for E5 on 2026-08-17 against the versions selected by
this workspace, not against adapters or remembered APIs.

| Capability | Exact source used by Bombay | Current owner |
|---|---|---|
| Behavior and Behavior Actors | local patch checkout `79b66f0041df956345aee8624504f82df4114f9b`, package `bombay-behavior-actors 0.12.0` | pure algebra, Driver contract, actor policy templates, explicit proxy-subtree shutdown |
| Address | local patch checkout `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`, package `bombay-address 0.2.0` | typed address claim, exact registration lease, opaque resolution |
| Communication | crates.io checksum `fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2`, package `bombay-communication 0.1.2` | two-lane mailbox, delivery, backpressure, affine user-admission retirement |
| Observe | crates.io checksum `7017c773ae142628b6a244cddad0db987cfba22df4c93730844a7d43cc657304`, package `bombay-observe 0.1.1` | keyed exact-generation facts plus affine unkeyed publication pairs |
| Timers | crates.io checksum `5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d`; matching checkout `4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2` | single-owner generation-safe timer queue |
| Entity | canonical checkout `68d0f503205a569ddda88124f5add8e8a652e18f` | valuable entity lifecycle laws, but currently encoded over a legacy parallel runtime |

The local Observe and Timers checkouts are not patched into this workspace;
the locked registry artifacts remain the build authority. The matching
checkouts were inspected for source, tests, and documentation. Entity is not a
current Bombay dependency and was audited as a neighboring future capability.

## Ownership table

| Question | Sole owner | Bombay's use | Bombay must not add |
|---|---|---|---|
| What does the actor decide? | Behavior / Behavior Actors | run the Driver and interpret typed actions | another actor trait, effect algebra, or policy layer |
| Where does a typed live endpoint resolve? | Address | retain one shared `AddressSpace<A, E>` for each locally hosted endpoint type and hold its `Lease` for one incarnation | a named namespace object, registry, resolver protocol, or erased endpoint map |
| How does an event reach one actor? | Communication | construct one mailbox per incarnation; retain its cloneable `MailboxRef` plus weak admission authority in the Address endpoint | Tokio delivery channels or a Bombay mailbox |
| How is a terminal fact published? | Observe | give observers the captured observation and retirement the publisher | Tokio activation/termination channels or a mutable Bombay observation cell |
| How are due timers ordered and invalidated? | Timers | keep a `TimerQueue` in the actor-owned environment and drive its next deadline with the executor clock | a timer task, timer-command channel, second queue, or generation counter |
| Who owns child/supervisor/router/shutdown policy? | Behavior Actors | interpret its named creation, delivery, observation, timer, report, and shutdown lanes | runtime supervision policy or lifecycle framework |
| Who owns task execution and ordering? | Bombay | own executor tasks, capability instances, Driver turns, child task ownership, activation and retirement order | policy disguised as task plumbing |

“One shared `AddressSpace` per locally hosted endpoint type” is an internal
static product requirement, not a public namespace abstraction. Address owns
the space and its laws. Bombay merely retains the concrete spaces required by
the closed application topology.

## Behavior and Behavior Actors

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
  ShutdownCoordinator, timer-backed templates, and the rest of the Actors
  catalogue own their policy as ordinary Behaviors.

### Interface conclusion

No Bombay actor abstraction is required. Users compose concrete Behavior
values directly and package the root with a named product of typed local actor
spaces. Bombay's generic `App` owns that separation and execution.

The remaining topology evidence cannot truthfully be inferred merely from all
types appearing in a Behavior. A send may target a remote or externally
provided endpoint, recursive behavior graphs are open, and mentioning a
protocol does not prove local hosting. The actor system therefore supplies a
named, statically checked product of locally hosted `ActorSpace<P>` values.
Concrete `Hosts<P>` implementations map each local protocol to its field
without erasure or positional traversal.

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

Observe 0.1.1 exposes the unkeyed pair over the same proven slot protocol:

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

The minimum owning API is:

```rust,ignore
pub fn pair<O>() -> (Publisher<O>, Observation<O>);

pub struct Publisher<O> {
    slot: Arc<Slot<O>>,
}

impl<O> Publisher<O> {
    pub fn complete(self, outcome: O);
}

impl<O> Clone for Observation<O> {
    fn clone(&self) -> Self;
}
```

`Subject<K, O>` remains the keyed publisher; renaming it would create unrelated
compatibility churn. Observe should extract the existing unsafe publication
and panic-safe waiter drain from `Subject::complete` into one internal
`Slot::complete`. Both `Subject` and `Publisher` delegate to that operation so
the pair cannot fork the concurrency law.

Abandonment is explicitly deferred. Publishing abandonment from
`Publisher::drop` would invoke user wakers from a destructor; resuming a user
waker panic during an existing unwind could abort the process. Observe should
not bake in `Result<O, Abandoned>` or a second observation wrapper until that
destructor policy has its own law and adversarial proof. Bombay instead owns
the invariant that every launch and retirement branch explicitly consumes its
publisher with a cloneable semantic outcome. An accidental dropped publisher
remains observable as a violated Bombay invariant or hang in tests, rather
than silently acquiring new Observe semantics.

Direct pair tests must cover observation before and after completion, cloned
fan-out, cancellation and shared/migrated wakers, synchronous wait races,
panicking-waker drain, exactly-once outcome destruction, documented incomplete
publisher drop, auto-trait behavior, Loom races, and compile failure for
publisher cloning or reuse after consuming completion.

The existing keyed API remains correct for discoverable and replaceable keyed
subjects. A direct pair represents one already-identified, non-replaceable
fact; its fresh allocation is its identity and needs no generation number.
Bombay now uses this directly. Retirement owns the termination publisher;
`ActorRef` carries the observation. Activation uses a separate pair whose
outcome is the exact live reference or typed activation rejection. The keyed
`TerminationCell`, activation MPSC, and test oneshot have been deleted.

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

## Entity

### Laws that must survive

The canonical Entity implementation proves important behavior:

- one single-flight activation per stable entity identity;
- bounded concurrent activation waiters and ownership-preserving refusal;
- generation-safe activation commitment and retirement;
- one committed live incarnation;
- admission closes before accepted work drains;
- passivation fences accepted delivery before retirement;
- stale activation, delivery, fence, passivation, and termination facts cannot
  mutate a newer generation;
- cancellation removes only the matching waiter;
- forced drain has a typed and testable outcome.

These are Entity laws, not Bombay plumbing.

### Legacy shape rejected

The current `LocalDirectory`, `EffectInterpreter`, `LocalEntityRuntime`,
`EntityRuntime`, separate transition machine, and serialized/linearized
executors were built as a runtime layer over the former actorpass stack. They
duplicate the new Behavior Driver and Bombay composition boundary. Bombay must
not integrate them as a second runtime, directory thread, condvar protocol, or
effect interpreter.

### Required re-foundation

Entity should become one or more ordinary Behavior Actors templates. Stable
identity, activation slots, drain phases, fences, and refusal policy remain
pure state and typed events. The template emits the existing named Behavior
capabilities:

- creation to install a fresh entity behavior or stable proxy;
- delivery to route accepted commands;
- observation to receive exact incarnation termination;
- timer requests only for explicit passivation/deadline policy;
- shutdown requests through the standard shutdown protocol.

Address supplies endpoint identity, Communication supplies delivery and
backpressure, Observe supplies exact activation/termination facts, Timers
supplies time, and Bombay interprets those effects. `EntityId<T>` may remain a
pure domain key. No `LocalEntityRuntime` or Entity-owned executor survives.

The migration is not complete until every existing lifecycle law is ported to
Driver-level unit/model/property/adversarial tests and an end-to-end Bombay
trace proves concurrent same-key commands create exactly one incarnation,
then drain and retire it without accepting a command across the fence.

## Minimal incarnation composition

With the two small primitive gaps filled, one local incarnation needs only:

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

The distilled target has no `LocalActors`, `LocalProtocol`, public namespace,
activation channel, termination cell, timer task, timer command channel, or
runtime-stop oneshot. The current worktree has already removed the first two
and all Observe/timer compensations listed above.

## The Behavior spine

Bombay does need one spine that holds these capabilities together. That spine
already exists in `bombay-engine`: the affine `Environment<B>` /
`ActiveEnvironment<B>` typestate pair. It should remain the only actor-loop
port.

```rust,ignore
pub trait Environment<B: Behavior<Ph = Never>> {
    type Active: ActiveEnvironment<B>;
    type Error;

    fn activate(
        self,
        initialization: ActionsOf<B>,
    ) -> impl Future<Output = Result<Self::Active, Self::Error>>;
}

pub trait ActiveEnvironment<B: Behavior<Ph = Never>> {
    type Error;

    fn next(&mut self) -> impl Future<Output = Option<B::Event>>;

    fn apply(
        &mut self,
        actions: ActionsOf<B>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn retire(self) -> impl Future<Output = ()>;
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
3. Let Behavior Actors shutdown templates request and observe graceful child
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
use bombay::prelude::*;

fn main() -> Result<(), RunError> {
    App::new(root(), AppActors::default()).run()
}
```

`App::new()` returns one ordinary actor-system value containing a concrete
composition of Behavior Actors templates and its typed local actor spaces.
Users configure supervision, routing, worker capacity, restart policy, and
shutdown policy on those template values, where the policy belongs. They never
construct a runtime, guardian, mailbox, observation space, timer queue, Driver,
or interpreter. Bombay provides no entry or topology macros.

## Required changes by repository

### Bombay Communication

- 0.1.2 supplies the affine `MailboxOwner`/`MailboxRef` contract;
- its owning tests cover closure, blocked/racing sends, stale references,
  draining, control-lane independence, and consumer teardown;
- Bombay consumes that contract directly and adds no mailbox abstraction.

### Bombay Observe

- 0.1.1 supplies the unkeyed pair and documented incomplete-publisher drop;
- its pair-specific unit, compile-fail, cancellation, allocation, panic-waker,
  and Loom evidence is present in the exact published artifact;
- Bombay consumes the pair directly for activation and termination facts.

### Bombay Entity

- preserve the lifecycle state machine and its laws;
- replace the legacy runtime/directory/executor façade with Behavior Actors
  templates and named capability effects;
- prove parity before deleting the old implementation.

### Bombay

- remove `LocalActors`/`LocalProtocol` and store Address spaces directly in the
  closed internal topology product;
- activation MPSC/oneshot, timer command MPSC/task, and runtime-stop oneshot are
  removed;
- use Communication for all actor event transport, Observe for facts, and an
  actor-owned TimerQueue for timer state;
- keep application `DeliveryRouter<A, M>` implementations only where they
  select real external endpoints; do not handwrite product routing;
- keep topology evidence private/transitional and delete unused
  `outbound`/`provided` declarations;
- prove the ordinary `App::new(root, actors).run()` path directly.

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
12. Entity parity preserves every generation, fence, admission, and drain law;
13. the root functional API runs a heterogeneous created child, routes a
    message, observes a terminal fact, handles a timer, and shuts down the tree;
14. workspace tests, compile tests, documentation tests, formatting, strict
    Clippy, Loom/model suites in owning repositories, and repository-wide
    obsolete-pattern scans are green.
