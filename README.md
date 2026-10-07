# Bombay

Bombay runs pure, statically typed Behavior actors with runtime-owned local
mailboxes, address spaces, observation, timers, and task hierarchy. Ordinary
downstream examples live under [`examples/`](examples/README.md).

The optional `axum` feature consumes the same typed external boundary used by
CLI, tests, and embedded clients. The [Axum example](examples/axum/src/main.rs)
and its [HTTP integration tests](examples/axum/src/http.rs) execute that
boundary.

`ActorInterface<Api>` contains only the application-defined static product of
exported exact actor capabilities plus external-customer creation. Lifecycle
authority is projected separately. No actor is wrapped in an HTTP state mutex,
no target address is fabricated as the sender, and no dynamic registry is
introduced.

Bombay is an application and local runtime layer over closed,
deterministic Behaviors and the reusable templates owned by Behavior Actors.
Applications describe actors, topology, routing, supervision, timers, and
shutdown through Bombay. The foundational Behavior API remains available for
power users; Behavior Actors owns reusable policy and Bombay supplies local
execution. Full catalogue policy verification, durable execution and
distributed hosting remain separate requirements. EXEC acceptance and delivery
status are recorded in the selected PRD; source formation alone is not acceptance.

The [application topology example](examples/application-topology/src/main.rs)
executes the ordinary async root boundary and inspects its complete raw root
retirement through an application-owned terminal projection.

`ApplicationTerminal` is an application-owned `#[derive(TerminalProjection)]`
sum. It retains the exact final state, runtime origin, descendant terminals,
and completion or failure for every declared actor.

`run`, `run_with`, and feature-gated `run_axum` are inherent async methods.
They use an entered, caller-owned Tokio host. `Application::run_blocking`
accepts the caller's configured Tokio `Builder`, constructs an owned host,
and drives the same paired execution and receiving path. It rejects an entered
runtime before effects and returns the original application, Builder, and
`RunError` on that refusal or a native build failure. The Builder selects
current-thread or multithread execution; Bombay adds no scheduler enum.

`execute` and `execute_with` return separate execution and result futures.
Keep the result future when execution may be dropped or may unwind. The
[run_with tests](crates/bombay/tests/run_with.rs) exercise supplied Work, exact
output, shutdown, and raw root custody. Work futures and outputs may borrow
caller values and need not be `Send`; independently spawned actor tasks retain
their actual `Send + 'static` requirements.

Returning from the boundary does not stop the actor. The handle names shutdown
authority separately from root delivery: `application.lifecycle()` is the one
ordinary root-lifecycle spelling, and its `termination()` retains the exact
incarnation fact. A boundary-owned `Result<T, E>` remains the exact bare supplied Work output.
A user-returned `Option<T>` also remains unchanged, including `None`. The output
coexists with the independent complete cleanup result. When no Work is supplied, both callable and output are `None` in
`Option<Never>`; no unit callable is fabricated. The raw root result retains `RootOrigin<Owner>` and the actual
`ActorRetirement<Actor, Terminal, ChildFailures>`. Its `ActorTaskFailed` variant
owns the original actor `JoinError`. `ApplicationCleanupError::PublicationClosed`
and `TaskFailed` separately retain cleanup receiving and cleanup-task errors.
`Terminal` selects descendant projection, not automatic root projection.
For an empty descendant destination, `.run::<_, _, Never, _>().await` makes
that choice explicit while the other axes remain inferred. Project the root
outside the runtime only when the application selects that policy.

An interface establishes a real typed external actor rather than inventing a
raw sender address. The [external actor tests](crates/bombay/tests/actor_interface.rs)
execute the exact send, reply, and rejected-payload path.

The external actor owns one fresh allocated `MailAddr`, one cloneable exact reply
capability, and one affine receiver. `send` supplies that address as truthful
`User::from`; rejected admission returns the exact message. Dropping the
external actor closes admission and never retargets a
stale exact recipient.

Activated references can also issue an opaque
`EstablishedRecipient<Protocol>`. Actors send to that exact incarnation with
`EstablishedDelivery`; Bombay delivers directly without requiring the
sender's application topology to host or resolve the destination protocol.
The capability closes with its incarnation and never retargets to a
replacement at the same logical address. Addressed `Recipient` / `Delivery`
remain the distinct stable-name form.

Direct transport-neutral interface sends deliberately accept exact
`EstablishedRecipient<P>` capabilities. A logical `Recipient<P>` names a
namespace-relative stable identity and still requires a statically declared
local host or a separately selected transport namespace. Receiving a logical
proxy therefore does not silently turn it into an exact or globally routable
capability. The mixed `ReplyRoute<P>` form likewise retains both static lanes;
choosing its exact variant at runtime does not erase the logical host
requirement from the application type.

Bombay's `Application` is the ordinary root-first declaration-through-execution
value. `Application::new(root).child(Role, actor)` adds an application-owned
actor under a nominal semantic role while Rust infers its complete concrete
type. Bombay privately stages those values through Behavior's existing
`Children`, `BirthNodeAppend`, `DispatchBirth`, and `InstallBirth` contracts;
it defines no second creation algebra or positional public lookup. Advanced
compositions whose intentional logical-address effects require additional
canonical protocol hosts may use `App::new` with the transitional static
actor-space product.
Ordinary state folds use the thin `#[bombay::actor]` facade. It supplies
Bombay's fixed address and mechanical sender/lint spelling, then delegates to
Behavior's owning attribute; the resulting nominal protocol, sends, births,
errors, and fold are exactly owner-generated. Explicit nominal `Behavior` and
`#[bombay::behavior::behavior]` remain deliberate power-user forms.

`bombay::prelude::*` is intentionally an application prelude: it contains the
Level-1 authoring vocabulary including consuming `Activate`, common Level-2
templates and policies, Entity handles, `ActorRef`, `Application`, and runtime
results. Pure folds therefore run after exactly one visible `.initialize()`;
Bombay does not add a parallel test harness. Foundational
implementation traits and structural effect types are imported deliberately
from `bombay::behavior`; explicit actor spaces, host proofs, topology
representation markers, and advanced `App` are imported from `bombay` only
when deliberately constructing the advanced boundary.
The reusable catalogue is also browsable through its owning semantic modules,
including `bombay::supervision`, `bombay::routing`, and `bombay::timing`.
Behavior Actors supplies the reusable actor templates and their policy laws;
Bombay adds no compatibility catalogue or aggregate abstraction.
The ordinary prelude adds one static composition trait, so wrapper policies are
selected directly from a behavior value. The [actor-template example](examples/actor-templates/src/main.rs)
executes a receive-timeout policy followed by shutdown policy.

The inferred value is exactly the existing
`StopOnShutdown<ReceiveTimeout<Service>>`; method order remains part of the
concrete type and Bombay introduces no wrapper, erased behavior, or default.
Ordinary authors select every policy input explicitly. Supervisors and worker
pools retain the owning Behavior Actors constructors because their topology and
capacity inputs are semantic policy rather than an inner behavior to decorate.

Bombay-owned Entity integration installs nominal asynchronous
`EntityDefinition` values on `App` under semantic roles. Cloneable
`Entities<D>` receptionists produce stable `EntityRef<D>` logical references;
the application retains family shutdown and task-join authority. External and
Behavior-originated admission preserve truthful caller provenance and exact
rejected commands without claiming processing or durable commit. Native
activation hydrates before routability, shares the application address source,
preserves exact failure and descendant-terminal facts, and returns bounded
family metrics after root-first shutdown. `run_with_entities` returns Work
outcome, a separate root receipt, and the recursive family receipts together.
Each acquired root or family report leaves the cleanup producer before it
awaits the next owner. Native cleanup failure and receiving errors remain
separate from those acquired reports. The generic directory/runtime
equation remains an explicit advanced `bombay::entity` integration surface.

## Architecture

```text
Behavior + bombay-engine::Driver
  inside Environment / ActiveEnvironment
    + Bombay Communication mailbox
    + Bombay Address lease
    + Bombay Observe activation and termination facts
    + actor-owned Bombay Timers queue
    + typed capability-lane interpreters
    + Bombay-owned task hierarchy
```

Behavior decides; runtime capabilities perform; capability results return as
later typed events. Ordinary applications select a live Tokio host or supply
a Builder to `run_blocking`; they do not construct a Bombay guardian, Driver,
mailbox, timer queue, observation subject, incarnation, or interpreter.
Dropping a runtime limits task liveness. An entered or captured Handle is not
a liveness guarantee: keep the selected host alive and polling through all
required actor, cleanup, and family joins. Completion observation alone does
not replace those owned results.

The standard local runtime uses Address, Communication, Observe, and Timers
directly. Bombay does not wrap them in a second registry, namespace, mailbox,
or timer service. Extension adapters plug into typed effect lanes. The Engine
`Environment<B>` boundary permits complete alternative hosts for testing,
embedded execution, or another executor.

## Current status

The [source-backed completion inventory](docs/prd-backlog/README.md) separates
implemented local behavior, missing integration and unverified claims. The
[failure contracts](docs/prd-backlog/failure-contracts.md) define proposed
distributed acceptance properties, not guarantees already supplied by this
release. Zenoh is the selected production networking direction; Mnesis-backed
durability and downstream Selo identity integration remain planned work.

The direct Driver and lower lifecycle layers exist. Communication 0.1.3's
affine mailbox admission owner, Bombay's private affine Observe import, and the
actor-owned TimerQueue are integrated. The redundant runtime-stop channel,
activation channels, keyed termination cell, timer task, and timer command
channel are gone.

The exact selected contracts and any live blockers are recorded in
[`docs/prd-backlog/status.md`](docs/prd-backlog/status.md). The local
explicit application path has executable source and controls. Named topology
and same-path HTTP use private closed actor-space composition. Advanced App
still supplies canonical hosts for deliberate logical-delivery protocols.
Feature acceptance, distillation and merge state remain in the selected PRD
and backlog; source formation does not establish those states.

The product exposes Behavior's generated user-message form and statically
typed application composition. Every level is static: generated code reduces to ordinary concrete
`Behavior`, `Actions`, send, birth, and interpreter types, and application
communication remains explicit in `Actions`.

## Documentation

- [Completion requirements and PRD groups](docs/prd-backlog/README.md)
- [Stack evidence and documentation status](docs/prd-backlog/evidence.md)
- [Failure rules and properties](docs/prd-backlog/failure-contracts.md)
- [User-facing API](docs/user-facing-api.md)
- [Runtime capability interfaces](docs/runtime-capability-interfaces.md)
- [Module boundaries](docs/module-boundaries.md)
- [Driver law](docs/driver-law.md)
- [Driver verification strategy](docs/driver-test-strategy.md)
- [Backlog status and dependencies](docs/prd-backlog/status.md)

## Development

```console
nix develop -c cargo build --workspace
nix develop -c cargo test --workspace
nix develop -c cargo fmt --all -- --check
nix develop -c cargo clippy --workspace --all-targets -- -D warnings
```

The toolchain is pinned in `rust-toolchain.toml`; `nix develop` provides the
repository development shell.
