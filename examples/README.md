# Bombay examples

Every example is a downstream Cargo package that depends on the public
`bombay-rs` façade. There is one examples tree and each folder is named for the
API concept it demonstrates. Most use the ordinary `Application` boundary;
the Entity example deliberately uses advanced `App` composition because its
nominal definition must statically name the concrete protocols it hosts.

Examples use `bombay::actors::ActorExt` (imported by the prelude) whenever an
existing actor is decorated with reusable stash, timer, deadline,
receive-timeout, or shutdown policy. Standalone roles such as `Machine`,
`Task`, supervisors, and worker pools retain their owning constructors because
their inputs are the policy being demonstrated. Examples never reproduce a
template law in domain receive code or add a wrapper that their scenario does
not need.

## `actor-templates`

For learning the primary template-selection path. It constructs the standalone
`Machine` role, then composes receive-timeout and shutdown policy through
`ActorExt`. Work is deferred while the machine is closed, replayed after its
phase change, checked by the idle reaction, and terminated by the real Bombay
timer interpreter. The example contains no custom `Behavior` implementation.

```text
src/processor.rs  state, messages, machine transition, and timeout reaction
src/main.rs       ActorExt composition and executable application boundary
```

## `counter`

Direct level-1 actor authoring: state, a public message enum, typed
request/reply, a named send lane, initialization, continuation, controlled
overflow, and application-owned shutdown. The example uses `#[bombay::actor]`,
which lowers directly to the owning Behavior attribute while removing only
fixed address, ignored sender, and framework lint boilerplate. The reply
protocol remains an ordinary explicit nominal `Protocol`; Bombay has no
protocol macro. Because the actor owns an inhabited domain error, its
successful tests assert that result explicitly rather than pretending the fold
is infallible.
The unit tests consume the definition through the owning `.initialize()` API
before any pure receive, so initialization cannot be silently skipped or
repeated. The binary runs the same actor through `Application`, establishes a
typed external customer, increments, reads the exact reply, and requests
shutdown through the application's separate lifecycle capability. The binary
also consumes the exact projected terminal and checks that root state,
descendant custody, ingress residuals, and completion are preserved. There is
no second artificial domain stop command.

```text
src/main.rs       executable Application request/reply and lifecycle shutdown
src/counter.rs    state, protocol, messages, receive logic, and invariant
```

## `entity`

Application-owned native Entity installation. A nominal asynchronous
`EntityDefinition` is declared under a semantic role with independent
hydration and resident bounds. The executable admits a command, passivates the
incarnation, uses the same stable `EntityRef` to activate a replacement, then
settles the root before Bombay closes, drains, and joins the family. It inspects
both exact retirement payloads and the bounded final metrics; no actor fold
performs hydration, admission, passivation, or observation directly.

```text
src/main.rs  native definition, stable reference journey, and retirement custody
```

## `application-topology`

A parent-owned topology with two genuinely different named child protocols.
The parent stages both births and both same-action deliveries through the
generated nominal routes. Behavior Actors derives a creation-dependent phased
shutdown plan from the committed children; Bombay interprets it and returns the
complete role-indexed terminal tree. The pure test independently asserts both
births, deliveries, payloads, and the continuation verdict.

```text
src/main.rs  actors, named topology, phased shutdown, and exact terminal custody
```

## `supervision`

A timed domain worker under the selected fixed `Supervise` template. Restart
strategy, permanence, budget, window, delayed backoff, topology, and terminal
failure reaction are explicit inputs. The executable proves one replacement,
restart-budget denial, complete lifecycle retention, typed terminal reporting,
and custody of the stable proxy plus both worker incarnations. Bombay adds no
supervisor wrapper or alternate lifecycle contract.

```text
src/main.rs  worker policy, supervisor construction, lifecycle trace, and terminal tree
```

## `worker-pool`

A one-worker bounded pool with explicit backlog capacity, retry interruption,
permanent restart, restart budget/window/timing, assignment ownership, and a
typed external reply protocol. The executable observes ordered acceptance and
completion before graceful application shutdown. The worker test asserts the
complete selected `ReportToParent<PoolCompletion<_>>` action lane directly.

```text
src/main.rs  pool policy, external customer, ordered replies, and terminal custody
src/worker.rs domain job/result, assignment fold, and pure action oracle
```

## `axum`

The optional HTTP boundary: Axum JSON extraction maps to a typed Bombay root,
closed-mailbox rejection returns the exact payload as `503`, and root
termination gracefully stops the server. Only this example enables the
`axum` feature; the other examples have no Axum dependency.

The live test drives the complete path—HTTP request, typed application handle,
root Behavior transition, HTTP response, lifecycle shutdown request, and
graceful server termination—while the unit tests retain the smaller boundary
oracles.

The transport path creates one real external actor per admitted request. Its
fresh claimed address becomes the foundational `User::from`; the target is no
longer fabricated as the sender. The router receives `ActorInterface<OrderApi>`
and a separate `ApplicationLifecycle<OrderBook>`, so ordinary order admission
cannot acquire shutdown or topology authority.

```text
src/domain.rs       HTTP/domain order value
src/order_book.rs   pure actor state and transition
src/http.rs         Router, static gateway, handlers, and HTTP tests
src/main.rs         single-root Bombay application, bind address, and `run_axum`
```

Run them through the pinned shell:

```text
nix develop -c cargo run --locked -p bombay-example-counter
nix develop -c cargo run --locked -p bombay-example-entity
nix develop -c cargo run --locked -p bombay-example-actor-templates
nix develop -c cargo run --locked -p bombay-example-application-topology
nix develop -c cargo run --locked -p bombay-example-supervision
nix develop -c cargo run --locked -p bombay-example-worker-pool
nix develop -c cargo test --locked -p bombay-example-axum
```
