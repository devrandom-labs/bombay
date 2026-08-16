# Bombay module boundaries

Bombay is rebuilt in `crates/bombay/src/core/`, one ownership layer at a time.
Only implemented layers are normative here. Historical runtime structures in
the design ledger are not current APIs.

## Current stack

```text
Bombay Behavior
    pure Behavior and typed Actions
            |
            v
Bombay Engine
    Driver<B, E> + Environment<B> -> ActiveEnvironment<B>
            |
            v
Bombay core
    LocalEnvironment -> ActiveLocalEnvironment
    Incarnation<B, E, R>
    LocalActors<B>::spawn -> ActorRef<B>
```

## Driver

`bombay_engine::Driver` owns one closed Behavior and one coherent typed
prepared environment. Its consuming `run` initializes once, activates that
environment with the complete initialization actions, obtains one event at a
time from the resulting active environment, folds once, commits once, and
consumes active retirement on ordinary completion.

## Incarnation

`bombay::core::Incarnation` is exactly one layer above Driver. It owns:

- one already-constructed Driver;
- one affine `Retirement` capability; and
- terminal classification for that one execution.

Its complete algorithm is:

```text
own Driver and Retirement
    -> run Driver once
    -> destroy Driver-owned values
    -> classify Completion or exact Driver failure
    -> invoke Retirement once

panic or cancellation
    -> destroy the active Driver future
    -> classify Panicked or Cancelled
    -> invoke Retirement once
```

`IncarnationOutcome<BehaviorError, ActivationError, EnvironmentError>` contains only factual
terminal information:

- `Completed(Completion)`;
- `BehaviorFailed(BehaviorError)`;
- `ActivationFailed(ActivationError)`;
- `EnvironmentFailed(EnvironmentError)`;
- `Panicked`; or
- `Cancelled`.

It deliberately owns no address or generation value. A later layer may place
an exact generation lease and outcome publisher inside its `Retirement`
implementation without changing Incarnation.

## Local environment

The crate-private `LocalEnvironment` implements `Environment<B>` directly:

```text
commit initialization actions
    -> claim the exact Address endpoint
    -> only then return ActiveLocalEnvironment
```

Commitment or claim failure exposes no active value. The active value alone
owns mailbox ingress and the exact Address lease; consuming retirement drops
both. There is no second core preparation or publication abstraction.

## Local launch

`LocalActors<B>` owns one protocol-indexed Address space and Communication
configuration. Its `spawn` method constructs the existing local Environment,
Driver, and Incarnation, launches that one future on Tokio, and waits for the
Environment activation boundary. Only then does it return `ActorRef<B>`.

`ActorRef<B>` is a non-owning user-lane capability. It can inject only
`(B::Addr, B::Msg)` through `B::Event::user`; it cannot receive, control,
cancel, observe, register, or keep an incarnation live. The inferred commit
closure consumes each complete `ActionsOf<B>` without adding a public
interpreter trait.

The local environment's publication hook is an inferred one-use closure. It
hands launch the exact already-claimed `ActorRef`; it contains no Tokio type and
launch performs no second address lookup. The environment accepts every closed
Behavior birth mode because it forwards the complete `ActionsOf<B>` unchanged;
child interpretation remains outside this layer.

## Source boundary

```text
crates/bombay/src/
  lib.rs
  core/
    mod.rs          current core surface
    incarnation.rs  one consuming Driver execution
    launch.rs       one Tokio launch and live-reference handshake
    local.rs        private typed mailbox/address environment
    outcome.rs      exact terminal classification
    retirement.rs   affine terminal handoff port
```

The former `runtime`, `mailbox`, and `routing` production trees were removed.
There is no System, prepared incarnation, lifecycle handle, alternate Driver
path, or compatibility fallback.

## Bounds and dispatch

Core adds only the bounds required by each static layer:

```text
B: Behavior<Ph = Never>
E: Environment<B>
R: Retirement<B::Error, E::Error, E::Active::Error>
```

Driver and Incarnation add no executor bounds. The concrete launch layer alone
adds the `Send + 'static` bounds required by `tokio::spawn`; it adds no dynamic
dispatch or generic executor port.

## Deferred layers

General heterogeneous routing, timers, observations, children, cancellation,
and outward lifecycle handles remain deferred. The implemented launch slice
only constructs and runs one local incarnation and returns its typed send
capability.
