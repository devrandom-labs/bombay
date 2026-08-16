# Driver integration guide

This guide describes the accepted direct-Behavior boundary. Earlier Bombay
facade recipes using `Compose::new`, `RunExit`, `RunError`, `RuntimeEffects`, or
split Driver lifecycle phases are superseded and have been removed.

## Behavior authors

Construct a Behavior normally and pass the inferred final value to a future
typed construction layer:

```rust,ignore
let behavior = machine
    .stash(route)
    .deadline(timer_id, deadline, on_elapsed);

spawn(behavior)?;
```

Users do not name nested wrapper types, construct a Driver environment, inspect
action-product nesting, or implement routing algebra. Supervision, pools,
timers, observation, shutdown, and other templates remain Behavior-owned typed
composition.

## Adapter authors

One generic adapter constructs a concrete `Environment<B>` and passes the
inferred Behavior directly to `Driver::new`. The Driver then:

1. consumes `B` through `Activate::initialize()` exactly once;
2. applies the complete initialization `ActionsOf<B>` before acquiring input;
3. acquires one `B::Event`, folds `Active<B>` once, and applies the complete
   successful `ActionsOf<B>` once;
4. waits only for that local application before acquiring another event;
5. stops after final actions or reports permanent input exhaustion; and
6. waits for the environment retirement barrier before ordinary return.

The environment statically owns event conversion and action interpretation.
Unsupported capability combinations fail through trait bounds. It may preserve
factual partial commits in its typed error, but the Driver performs no retry or
rollback.

## Outcome vocabulary

- `Ok(Completion::Stopped)` means the Behavior explicitly stopped and its final
  actions were successfully applied.
- `Ok(Completion::Exhausted)` means the environment permanently exhausted its
  event source without synthesizing another Behavior event.
- `Err(DriverError::Behavior(error))` preserves the controlled Behavior error.
- `Err(DriverError::Environment(error))` preserves the action-application
  error, including any environment-defined committed-prefix facts.
- Panic and cancellation are not converted into Driver results. The future
  incarnation owner classifies them after Driver-owned values are dropped.

## Ownership boundary

The Driver owns only the universal causal turn boundary. A concrete environment
owns heterogeneous capability interpretation. A future Bombay incarnation owns
address generation, task lifetime, cancellation classification, and terminal
publication. A later layer will own transactional construction and
publication; the current core deliberately does not prescribe its object
model.

See [`driver-law.md`](driver-law.md) for the normative laws and
[`driver-test-strategy.md`](driver-test-strategy.md) for executable evidence
requirements.
