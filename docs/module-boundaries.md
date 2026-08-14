# Bombay module boundaries

This document defines the ports, adapters, composition boundary, and ownership
seats of the local runtime. The active queue remains in
`docs/open-design-ledger.md`.

## Hexagonal runtime

```text
Bombay Behavior (domain algebra)
             |
             v
       Driver<B, E> (application core)
             |
       Environment port
             |
             v
 ActorEnvironment (Tokio/Bombay adapter)
   | mailbox  | routing  | timers  | children/observation

System (composition root)
  -> PreparedIncarnation (unpublished launch transaction)
  -> Incarnation { Driver, TerminalRetirement } (one live generation)
  -> Completion (detachable Tokio termination + published actor outcome)
  -> Handle (external capability)
```

The `Driver` is process-like execution without actor identity. It owns a pure
behavior and an `Environment`, initializes the behavior, folds events in
sequence, interprets every successful transition before receiving another
event, and retires the environment on every ordinary return. It owns no
address, Tokio task, lifecycle reporter, or terminal publication.

An `Incarnation` is the identity-bearing runtime process. It adds the exact
address-generation lease and terminal publication order around a `Driver`.
This distinction prevents behavior execution from depending on runtime
identity while giving one concrete object responsibility for a live actor
generation.

## Roles and laws

| Role | Concrete items | Law |
|---|---|---|
| Domain algebra | Bombay `Behavior`, typed events/actions | Pure initialization and folds; no I/O, clock, channel, or runtime handle |
| Inert actor definition | `Actor<B>` | Pair exactly one `B::Addr` value with `B`; own no runtime resource or policy |
| Engine core | `bombay_engine::{Driver, RunExit, RunError, RuntimeEffects}` | One fold at a time; interpret its complete effect before the next event; retire the port before returning |
| Primary/secondary ports | `bombay_engine::Environment`, bombay `EventSource`, `EventSender`, routing and lifecycle traits | Describe capabilities bombay needs; contain no Tokio or Bombay implementation policy |
| Runtime adapter | `ActorEnvironment`, Bombay Communication mailbox wrappers, `AddressRouter`, `IncarnationEffects` | Give the ports their one local Tokio/Bombay meaning without changing Behavior policy |
| Composition root | `System` | Select concrete adapters, prepare all resources transactionally, then launch exactly once |
| Pre-launch transaction | `PreparedIncarnation` | Can exist only after exact registration; dropping it starts no behavior and rolls back prospective resources |
| Canonical launch | `PreparedIncarnation::launch` | Construct the sole Tokio task/control/completion ceremony for ordinary roots, initialized roots, and children |
| Running aggregate | `Incarnation` | Own exactly one `Driver` and one `TerminalRetirement` for one address generation |
| Terminal guard | `TerminalRetirement` | Drop actor resources, release exact registration, emit retired, publish outcomes once, emit completed |
| Outward completion seat | `Completion` | Tokio join proves task-owned values were dropped; Bombay Observe supplies the actor outcome |
| External edge | `ActorRef`, `Handle`, `RootEndpoint`, `RootRetirement`, `ChildLease` | Expose only delivery/shutdown, abort/wait, or child capability—not internal consumers, subjects, or registries |

`RuntimeEffects` is the DTO across the application-core/output-port joint. It
contains only sends and child creations because the `Driver` consumes the
Behavior verdict itself. `ActorEnvironment` interprets that DTO structurally;
it does not reinterpret the verdict or behavior policy.

`Environment` includes `retire` because input, effect interpretation, and the
resources supporting both form one adapter lifetime. A separate retirement
trait allowed invalid combinations and split one lifecycle obligation across
two ports.

## Source ownership

```text
crates/
  bombay-engine/src/
    driver.rs        actor-independent Driver and RuntimeEffects
    environment.rs   Environment port
    run.rs           terminal execution vocabulary
    behavior_machine.rs Behavior-to-Machine adaptation

  bombay/src/mailbox/
    protocol.rs      EventSource/EventSender ports
    communication.rs Bombay Communication adapter

  bombay/src/routing/
    actor_ref.rs     external typed delivery edge
    delivery.rs      registration, resolution, routing, and send-algebra ports/adapters

  runtime/
    system.rs        composition root
    environment.rs   concrete actor environment adapter
    incarnation_effects.rs       generation-owned capability state
    timer_interpretation.rs      absolute/relative timer request interpretation
    observation_interpretation.rs child/peer monitor installation and unwatch
    child_publication.rs         creation and worker-terminal event publication
    incarnation.rs   prepared/live ownership and terminal publication
    completion.rs    Tokio termination + Observe outcome seat
    handle.rs        external control/completion capability
    outcome.rs       task outcome normalization
    children.rs      Behavior birth-mode to runtime-child adapter selection
    child_lease.rs   affine child capability
    child_scope.rs   parent-owned live child set
    lifecycle.rs     optional fact-reporting port and adapter
```

The folder boundary follows dependency direction: `core` knows only Behavior
and its own port; concrete adapters depend inward on `core`; `System` composes
the adapters. There is no generic `integration` layer because the concrete
environment is a runtime adapter, and no executor abstraction because every
local adapter currently requires Tokio. Embassy work may extract a runtime
adapter boundary once a second implementation provides concrete laws.

## Adapter seams that remain intentional

`EventSource` is narrower than `Environment`: it isolates mailbox ingress and
allows the concrete environment algorithm to be tested with an in-memory
source. `Environment` additionally owns output interpretation and retirement.

The routing traits describe different authority directions and therefore do
not collapse into a registry object: `EndpointRegistry` claims/releases exact
generations, `DeliveryEndpoint` accepts resolved delivery, `DeliveryRouter`
routes an outbound message, `PeerObserver` captures a generation-specific
completion, and `RouteSends` interprets Behavior's statically composed send
algebra. A rejected endpoint returns `RejectedDelivery<M, E>` so neither an
unknown lookup nor a closed resolved mailbox can erase the owned message.
Application send products use Behavior's recursive `SendInput`
selection through semantic `Own`/`Inner<Path>` aliases. They do not implement
`RouteSends`, `ObservesCreations`, or routing-error products; bombay provides
those generic structural interpretations once. Behavior's `Delivery<B>`
statically identifies the destination behavior. The remaining public
`EndpointRegistry<B, D>` and `DeliveryRouter<B>` adapters are temporary E5
runtime compatibility seams, not intended application concepts.

`IncarnationEffects` is the compiler-nameable product of capabilities owned by
one generation, not a service registry. It stores the timer queue, child scope,
observation monitor tasks, parent reporter, response edge, and router required
by the concrete environment. Interpretation authority is separated:
`timer_interpretation` handles only typed schedules,
`observation_interpretation` handles monitor installation/cancellation, and
`child_publication` handles same-action creation results and worker terminal
facts. None are dynamically looked up or exposed to a behavior.

`Handle::abort` and the exact `Incarnation` share one private cancellation
request fact. It is not a second lifecycle protocol: Tokio still owns task
cancellation, while the fact supplies the bombay runtime checkpoint between
synchronous initialization and first mailbox ingress. Healthy actors do not
yield at that boundary.

`ChildRuntime` and `RuntimeBirthMode` bridge Behavior's type-level birth mode
to either `NoChildren` or `SystemChildren`. The sealed bridge prevents an
erased child registry while keeping `System::spawn` the only construction
path. `ChildLease` retains liveness; consuming its completion seat for
observation does not remove it from the parent's retirement scope.

Lifecycle traits form an optional outbound reporting port. Reporters publish
completed facts and cannot influence preparation, execution, or retirement.
They add no lifecycle state machine or policy.

## Preparation and terminal order

```text
System::prepare(Actor)
  -> allocate completion generations and mailbox
  -> construct ActorEnvironment
  -> claim the exact address generation
  -> derive reporter and external edge
  -> construct Driver
  -> return PreparedIncarnation

System::spawn(Actor)
  -> split prepared ownership into Incarnation, ActorRef, and observation
  -> spawn Incarnation once
  -> return Handle { ActorRef, Completion }

System::activate(Actor)
  -> construct the private provisional ownership state
  -> run initialization and interpret all initialization effects
  -> claim the exact address generation
  -> launch the initialized Incarnation once
  -> return RootActivation { RootEndpoint, RootRetirement }

return, error, panic, or cancellation
  -> Driver/Incarnation drops behavior and retires environment resources
  -> TerminalRetirement releases the exact address lease
  -> publish detailed and peer-normalized outcomes exactly once
  -> Tokio task termination makes Completion::wait resolvable
```

Tokio's `JoinHandle<()>` is only a temporal proof that the future and its
owned resources have dropped. It is not a second actor-outcome channel;
`TaskOutcome` comes solely from the Observe generation completed by
`TerminalRetirement`. Dropping `Completion` detaches rather than cancels;
abort authority remains an explicit operation on `Handle`.

## Deliberate scope

Bombay owns local runtime composition: addresses, spawning, delivery,
timers, child capability retention, observation wiring, and exact incarnation
retirement. Bombay Behavior owns actor policy, Communication owns the two-lane
mailbox, Timers owns deadline ordering/replacement, Observe owns outcome
generations, and Address owns registration identity and reclamation.

This architecture does not add supervision strategy, restart budgets,
request/reply, remote discovery, persistence, lifecycle hooks, a dynamic
service registry, or an erased timer/child/message envelope.
