# Minimal lifecycle ownership

This document describes the implemented layers only. General observation and
external lifecycle-handle layers are deliberately not designed here.

## Current stack

```text
Behavior
  -> bombay-engine::Driver
  -> crate-private LocalEnvironment
  -> crate-private ActiveLocalEnvironment
  -> bombay::core::Incarnation
  -> LocalActors::spawn on Tokio -> ActorRef
```

The Driver consumes one Behavior and one coherent typed environment. Its only
lifecycle operation is consuming `Driver::run`: initialize once, consume the
prepared environment to activate with the complete initialization actions,
then acquire one event, fold once, and commit once per turn through the active
environment.

The Incarnation consumes one already-constructed Driver and one affine
`Retirement` capability. Its only operation is consuming `Incarnation::run`.
It preserves the Driver result, classifies panic and cancellation, drops all
Driver-owned values, and then invokes retirement exactly once with the exact
`IncarnationOutcome`.

LocalEnvironment is prepared. It commits initialization, claims the exact
Address endpoint, and returns the only active value that permits ingress.

```text
owned Driver + Retirement
          |
          v
   Incarnation::run
          |
          +-- Completed(Stopped | Exhausted)
          +-- BehaviorFailed(error)
          +-- ActivationFailed(error)
          +-- EnvironmentFailed(error)
          +-- Panicked
          `-- Cancelled
          |
          v
 Driver values dropped -> Retirement::retire(outcome)
```

Cancellation means dropping the running Incarnation future. A private affine
terminal guard distinguishes that path from panic unwind. There is no public
prepare, initialize, loop, finish, reset, poison-recovery, or retirement phase.

## Ownership table

| Resource | Owner |
|---|---|
| pure state transition and typed actions | Behavior |
| one causal execution and ordinary completion/error classification | Driver |
| initialize-before-address-claim ordering | LocalEnvironment activation |
| panic/cancellation classification and post-drop retirement notification | Incarnation |
| concrete capability interpretation and resources | the Driver's environment |
| one selected local address, typed user endpoint, and two-lane mailbox | LocalEnvironment -> ActiveLocalEnvironment |
| one Tokio task and post-activation typed reference handoff | LocalActors::spawn |
| outward lifecycle handles, general routing, timers, and observation input | no implemented layer yet |

Incarnation does not own an address, generation value, mailbox, scheduler,
task handle, abort authority, observation subject, timer queue, child policy,
or capability registry. A later layer may close the remaining generation and
transaction laws by supplying a concrete `Retirement` implementation and by
constructing an Incarnation, but it must not split or duplicate Driver or
Incarnation lifecycle control.

## Current invariants

- one Incarnation can execute only its one consumed Driver;
- successful stop and source exhaustion remain distinct;
- Behavior and environment failures remain distinct and preserve their values;
- panic and cancellation remain distinct;
- Driver-owned values drop before retirement is called;
- retirement is invoked exactly once on every terminal path;
- no current public API exposes a second execution or lifecycle path; and
- a complete Incarnation run adds no allocation of its own.
- activation hands launch the exact published reference without resolving the
  address a second time; and
- the local environment forwards every Behavior-owned birth product unchanged.

Replacement, terminal observation, cancellation control, and heterogeneous
construction remain later-layer obligations. The launch slice proves one
selected generation's mailbox admission, address publication, Tokio launch,
and typed-reference handoff; it is not evidence for handle, hierarchy, or
System laws.
