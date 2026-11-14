# Bombay examples

Every example is a downstream Cargo package that depends on the public
`bombay-rs` façade. There is one examples tree and each folder is named for the
API concept it demonstrates. Every example uses the same `Application` owner.
Child graphs use closed `topology!` declarations for their static hosting and
native descendant custody; actor templates retain their owning constructors.

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
request/reply, named send lanes, initialization, continuation, controlled
overflow, and application-owned shutdown. The example uses `#[bombay::actor]`,
which lowers directly to the owning Behavior attribute while removing only
fixed address, ignored sender, and framework lint boilerplate. The snapshot
protocol remains an ordinary explicit nominal `Protocol`; the increment
processing reply uses the existing `MessageProtocol` alias. Bombay has no
protocol macro. Because the actor owns an inhabited domain error, its
successful tests assert that result explicitly rather than pretending the fold
is infallible.
The unit tests consume the definition through the owning `.initialize()` API
before any pure receive, so initialization cannot be silently skipped or
repeated. The local binary runs the same actor through `Application`, establishes
a root-bound typed client, sends one Increment and one Read, and requests
shutdown through the application's separate lifecycle capability. It acquires
Work, cleanup, native retirement and both notification results, then prints the
snapshot and retirement completion. There is no artificial domain stop command.

`IncrementAndReply` returns its correlated receipt through Actions only after
the checked increment succeeds. The owning tests exercise this processing
receipt and complete native custody. A Read snapshot alone does not acknowledge
an increment.

The owning Read test creates a separate typed reply port for each request.
Cancelling a borrowed receive preserves that request's reply; closing a settled
port refuses late replies with their original payloads. The Counter continues
through a fresh port, while its native result retains the rejected send and its
retirement report records the failure. This is local ownership evidence.

```text
src/main.rs       executable Application request/reply and lifecycle shutdown
src/counter.rs    state, protocol, messages, receive logic, and invariant
```

## `remote-counter`

The [remote Counter](remote-counter/README.md) runs the Counter and its customer
as separate programs through Bombay's public `remote` capabilities. The pure
customer requests a correlated increment, reads the value and requests an
authorized stop; Node owns networking and cleanup. Only the customer selects
a receive timeout, which reports an uncertain operation without retrying it.

This application uses an explicitly selected finite test identity supplied by
a trusted assignment operator. `production-required` refuses that provider
before Node startup. It is separate from the local `counter`; its package tests
own subprocess control and assertions. Full remote and production identity
acceptance remain in the remote PRD.

Protected Counter fault and transport regressions live under
`crates/bombay/tests/protected_counter`, built by the isolated transport-custody
test package. They use explicit finite test providers and are verification
fixtures. Their commands and evidence belong in the
[remote PRD](../docs/prds/static-remote-actors.md), rather than application setup.

## `entity`

Application-owned native Entity installation. A nominal asynchronous
`EntityDefinition` is declared under a semantic role with independent
hydration and resident bounds. The executable admits a command, awaits retirement through
`application.retire_entity(role, &id)`, uses the same stable `EntityRef` to start a fresh incarnation, then
settles the root before Bombay closes, drains, and joins the family. It inspects
both exact retirement payloads and the bounded final metrics; no actor fold
performs hydration, admission, passivation, or observation directly.

```text
src/main.rs      application assembly, stable reference journey and reporting
src/account.rs   account commands and actor state
src/accounts.rs  hydration and complete family callback custody
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

An executable `FixedSupervisor` example. It constructs the owner-defined
recovery policy, stops the first worker, observes its replacement, requests
supervisor shutdown, and checks exact root, proxy, and both worker terminals.
Its focused pure initialization test separately checks creation/observation
correlation before the runtime interprets either action.
See [supervision requirements](../docs/prd-backlog/local-runtime.md#sup--live-supervisors-and-stable-proxies).

```text
src/main.rs  worker policy, live replacement, shutdown, and terminal custody
```

## `worker-pool`

An executable FIFO pool example. The worker computes a typed search result;
the binary admits one job, checks its correlated customer outcomes, sends the
pool's own shutdown command, and verifies exact root and worker retirement.
Queue saturation, interruption, failure, and keyed-pool obligations remain in the
[pool requirements](../docs/prd-backlog/local-runtime.md#pool--live-worker-pools).

```text
src/main.rs  FIFO policy, live submission, customer outcome, and ordered shutdown
src/worker.rs domain job/result and pure assignment fold
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
fresh allocated address becomes the foundational `User::from`; the target is no
longer fabricated as the sender. The router receives `ActorInterface<OrderApi>`
and a separate `ApplicationLifecycle<OrderBook, OrderBookEvent>`, so ordinary order admission
cannot acquire shutdown or topology authority.

```text
src/domain.rs       HTTP/domain order value
src/order_book.rs   pure actor state and transition
src/http.rs         Router, static gateway, handlers, and HTTP tests
src/main.rs         single-root Bombay application, bind address, and `run_axum`
```

Run them through the pinned shell. Supervision executes worker replacement and
retirement; the worker-pool command executes the FIFO path described above:

```text
nix develop -c cargo run --locked -p bombay-example-counter
nix develop -c cargo run --locked -p bombay-example-entity
nix develop -c cargo run --locked -p bombay-example-actor-templates
nix develop -c cargo run --locked -p bombay-example-application-topology
nix develop -c cargo run --locked -p bombay-example-supervision
nix develop -c cargo run --locked -p bombay-example-worker-pool
nix develop -c cargo test --locked -p bombay-example-axum
```
