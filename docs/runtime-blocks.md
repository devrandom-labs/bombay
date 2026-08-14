# Bombay runtime blocks

Bombay is the live composition kernel. It does not reimplement the generic
correctness mechanisms on which actor execution relies.

This is a kernel boundary, not a claim that every useful framework capability
belongs here. Facade, stream, and pool capabilities remain outside Bombay when
another focused crate owns their invariant. Bombay emits only facts at the
runtime edges it owns;
instrumentation storage, formatting, filtering, and export stay outside.
Request/reply remains an ordinary typed protocol unless an independent
invariant proves a helper is necessary.

| Block | Owns | Does not own |
|---|---|---|
| behavior | Pure event-to-effect policy | I/O, tasks, clocks |
| bombay-communication | Local queued input and backpressure | Address resolution |
| bombay-address | Local live address ownership and resolution | Remote discovery |
| bombay-observe | Generation-safe completion publication and observation | Supervision policy |
| bombay-timers | Keyed generation-safe scheduling | Deadline policy |
| bombay | Task ownership, effect interpretation, and typed composition | The algorithms above |
| Nexus | CQRS durability and aggregate reconstruction | Live actor presence |
| Zenoh | Distributed routing, query, and liveliness | Aggregate ordering |
| KERI | Controller identity, authority, and provenance | Local actor designation |

Behavior effect composition crosses this boundary in one direction: pure
named send algebras are populated through semantic `SendInput` implementations,
then bombay passes their fields to runtime services or `DeliveryRouter` in fold
order. Positional product mutation is outside the supported composition.

## Names at the distributed boundary

- An bombay address designates one current local actor endpoint.
- A Nexus aggregate identifier designates a durable event stream.
- A KERI AID identifies a controller.
- A KERI SAID identifies immutable content.
- A Zenoh key expression names the distributed resource used to route or query
  aggregate commands, events, and state.

Zenoh arrival order is not aggregate order; Nexus revisions establish durable
ordering. A successful local enqueue or Zenoh publication is not durable
acceptance. A SAID establishes content identity, not authority; KERI validation
establishes authority before a command reaches the pure behavior.

## Extraction gate

A new pass requires an actor-independent invariant, more than one plausible
implementation, an independent correctness oracle, useful benchmarks, and a
concrete bombay integration point. Actor-specific ordering remains here:

```text
birth:       claim address -> start task -> permit sends
termination: release address -> publish completion
deadline:    schedule -> expire -> enqueue event -> fold next turn
```

Integration uses narrow typed capability seats rather than a universal dynamic
plugin registry. Standard constructors may hide the composed concrete types,
but bombay must not use `Any` or trait-object message routing to do so.
