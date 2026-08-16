# bombay

Bombay is being rebuilt around a universal direct-Behavior Driver, one runtime
ownership layer at a time. The old facade, System, mailbox/routing adapters,
examples, benchmarks, fuzz target, and integration tests have been removed.

The Driver accepts an inferred, closed Bombay Behavior value and one concrete
typed environment:

```text
Behavior definition
  -> Activate::initialize exactly once
  -> Environment::activate with complete initialization ActionsOf<B>
  -> receive one affine ActiveEnvironment
  -> acquire one B::Event
  -> Active<B>::transition exactly once
  -> ActiveEnvironment::apply complete ActionsOf<B> exactly once
  -> repeat, stop, or exhaust input
  -> consume ActiveEnvironment::retire before ordinary return
```

Its technical integration surface is deliberately limited to
`Driver<B, E>`, `Environment<B>`, `ActiveEnvironment<B>`, `ActionsOf<B>`,
`Completion`, and `DriverError`. It owns no address, mailbox policy, routing,
capability registry, template semantics, incarnation identity, or terminal
publication.

The first Bombay-owned layer is one incarnation:

```rust,ignore
let driver = Driver::new(behavior, environment);
let incarnation = Incarnation::new(driver, retirement);
incarnation.run().await;
```

`Incarnation` owns exactly one Driver and one `Retirement` capability. It
classifies successful completion, exact Behavior/activation/active-environment
failure, panic, and cancellation, and invokes retirement only after
Driver-owned values have been dropped. It owns no identity, construction,
executor, handle, routing, timer, observation, or child policy.

The crate-private local environment is the one concrete preparation path over
Bombay Communication and Address. It commits initialization actions and claims
the exact typed endpoint before yielding `ActiveLocalEnvironment`, the only
value with ingress and the Address lease. It injects user messages through the
behavior's complete `UserEvent` and treats user-lane closure separately from
complete mailbox exhaustion. There is no generic activation/publication
decorator.

The next concrete layer launches that incarnation on Tokio and returns a typed
reference only after activation succeeds:

```rust,ignore
let actors = LocalActors::<Tasks>::new(32);
let tasks = actors
    .spawn(address, Tasks::new(), |actions| interpret(actions))
    .await?;
tasks.send(origin, TaskMessage::Add(task)).await?;
```

`LocalActors<B>` is only one behavior-indexed address namespace and mailbox
configuration. `ActorRef<B>` can send the destination behavior's exact message
protocol but owns no task, receive authority, cancellation, or observation.
There is still no `System`, public executor abstraction, or lifecycle handle.
Activation transfers that exact already-published reference directly; it does
not resolve the address again. The local environment accepts every Behavior
birth mode and passes the complete action product to the inferred interpreter.

The normative contracts are:

- [`docs/driver-law.md`](docs/driver-law.md)
- [`docs/driver-test-strategy.md`](docs/driver-test-strategy.md)
- [`docs/module-boundaries.md`](docs/module-boundaries.md)
- [`docs/open-design-ledger.md`](docs/open-design-ledger.md)

Focused standalone verification currently runs with:

```console
nix develop --command cargo test -p bombay-engine
nix develop --command cargo test -p bombay-rs --all-targets
nix develop --command cargo clippy -p bombay-engine --all-targets -- -D warnings
nix develop --command cargo clippy -p bombay-rs --all-targets -- -D warnings
nix develop --command cargo fmt --all -- --check
```

The explicit complete law-manifest gate intentionally remains red until the
later construction/publication and template-runtime layers supply real evidence
for the deferred rows:

```console
nix develop --command cargo test -p bombay-engine --test law_manifest -- --ignored
```

Those rows are not simulated inside Engine or the incarnation layer.
