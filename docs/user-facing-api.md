# Bombay user-facing API

This is the target product contract. Bombay owns the stable application and
runtime experience over foundational Behavior and the reusable templates owned
by Behavior Actors.

## Entry boundary

A complete single-protocol application supplies one ordinary root value:

```rust,ignore
let terminal: ApplicationTerminal<_> = Application::new(root()).run()?;
inspect_terminal(terminal);
```

`run` owns asynchronous execution. Users do not write an auxiliary `async fn`,
construct a Tokio runtime, or construct a lifecycle wrapper. The ordinary
function is the execution path. `ApplicationTerminal` is the application's
`#[derive(TerminalProjection)]` sum: `run` does not erase the final root,
descendant custody, or the exact fact that selected retirement.

For a framework-neutral external boundary, `run_with` supplies the live
application's concrete typed handle and keeps the future and output generic:

```rust,ignore
let (boundary_result, terminal): (_, ApplicationTerminal<_>) =
    Application::new(root()).run_with(|application| async move {
    boundary(application.root()).await?;
    let lifecycle = application.lifecycle();
    lifecycle
        .request_shutdown()
        .expect("the live root accepts its first shutdown request");
    })?;
inspect_terminal(terminal);
```

Boundary completion is not a shutdown request. `ApplicationHandle` projects
root delivery through `root()` and lifecycle authority through `lifecycle()`;
it does not duplicate either capability's operations. `run_with` returns
`Result<(Output, Terminal), RunError<_>>`; if `Output` is `Result<T, E>`, that
value remains nested so boundary and runtime failures are not aggregated.

Shutdown policy is part of the root value, not a second execution mode:

```rust,ignore
let ((), terminal): (_, ApplicationTerminal<_>) =
    Application::new(worker_pool.stop_on_shutdown()).run_with(|application| async move {
    let lifecycle = application.lifecycle();
    assert_eq!(lifecycle.request_shutdown(), Ok(()));
    assert_eq!(lifecycle.termination().await, Ok(Exit::Normal));
    })?;
inspect_terminal(terminal);
```

This is the same execution spelling for every root. The concrete root must
explicitly accept or transform `ShutdownRequested`; Bombay does not infer a
shutdown policy from topology and does not secretly insert a root wrapper.

With the opt-in `axum` feature, the same boundary can host an HTTP adapter:

```rust,ignore
Application::new(root()).run_axum(
    "127.0.0.1:3000".parse()?,
    order_http::router,
)?;
```

The router factory receives `ApplicationHandle<Root::Protocol>`. Axum owns
extraction and HTTP responses; `application.root()` is the delivery-only
reference and `application.lifecycle()` is the lifecycle authority.
Bombay owns the shared executor, graceful server stop, exact root termination
classification, and the distinction between HTTP and root failures.

## Application composition

The root is an ordinary Behavior composition. Application-owned user-message
folds use `#[bombay::actor]`. The facade supplies only Bombay's fixed address,
an omitted sender, and scoped framework lint spelling before invoking
Behavior's owning attribute. An explicit nominal `Behavior` or the nested
`#[bombay::behavior::behavior]` path remains available when the complete
foundational algebra is deliberately needed. Bombay maintains no second actor
semantic contract and no protocol macro.

The ordinary prelude is curated by semantic level. It provides `Activate`,
`Actions`, `BehaviorActed`, `Protocol`, typed recipients and deliveries, child
staging, common templates and policy values, Entity values, and the application
runner. Pure tests consume a definition through `.initialize()` and fold its
`Active<B>` value, preserving initialization actions and preventing a second
initialization. Bombay adds no competing test harness. The prelude does not
import `Behavior`, interpreter traits, logical-host requirements, structural
effect paths, explicit actor spaces, host proofs, topology representation
markers, or advanced `App`. Library authors reach the exact Behavior-owned
items through `bombay::behavior`; advanced runtime composition uses explicit
`bombay::{App, ActorSpace, ActorSpaces, Hosts}` imports; reusable template
families are browsable through `bombay::supervision`,
`bombay::routing`, `bombay::lifecycle`, `bombay::timing`, and the other
semantic modules.
Reusable roles are concrete Behavior Actors templates. Ordinary wrappers use
`ActorExt`; standalone actors, supervisors, and worker pools use their owning
constructors. Bombay adds no aggregate aliases or compatibility catalogue.

`Application::new(root)` declares one pure root. Chained
`.child(role, actor)` calls declare application-owned actors under nominal
semantic roles while each actor expression determines its concrete type. The
private running application stages them only after the root's initialization
effects, appends their birth nodes to the root birth product, and installs them
through Behavior's existing closed `Children`, `BirthNodeAppend`,
`DispatchBirth`, and `InstallBirth` contracts. Roles never become protocols,
addresses, structural positions, runtime lookups, or events the domain root
must accept.

Every Behavior fold has one absolute rule: compute state and return the
complete `Actions`. Communication, creation, lifecycle, timer, and observation
work must appear in typed action lanes and must be performed only by Bombay's
interpreter. Calling a sender, channel, callback, task, clock, or observation
publisher inside `init`, `receive`, or `transition` is not an integration
shortcut; it makes the returned `Actions` an incomplete account of the
transition.

Advanced multi-protocol applications may still package the pure root separately
from a named product of runtime-owned local actor spaces:

```rust,ignore
#[derive(Default, ActorSpaces)]
struct AppActors {
    system: ActorSpace<SystemProtocol>,
    groups: ActorSpace<GroupProtocol>,
    devices: ActorSpace<DeviceProtocol>,
    queries: ActorSpace<QueryProtocol>,
}

```

`ActorSpaces` is deliberate level-3 runtime plumbing, not the ordinary
application API. It generates one ordinary `Hosts<P>` implementation for each
`ActorSpace<P>` field. These implementations remain the static protocol-to-
space mapping; they are generated at compile time, never erased or discovered
at runtime. Duplicate protocols are rejected. Bombay's generic
`App<Root, Actors>` retains the spaces while giving only the root to the
Behavior Driver.

A single-protocol root does not declare that product. Bombay supplies its one
concrete protocol space directly:

```rust,ignore
Application::new(OrderBook::default()).run_axum(address, order_http::router)?;
```

This is the ordinary Axum example boundary. Advanced compositions whose
intentional logical-address effects need more local protocols explicitly own
the corresponding concrete actor spaces. Behavior's duplicate-preserving
`LogicalHostRequirements` product validates that product as repeated
`Hosts<P>` evidence; it neither constructs nor normalizes actor spaces.

## Behavior authoring

Applications first select and compose Bombay actor templates. A template-only
single-protocol root can use `Machine` for state transitions and `OneShot` for
timed termination without application code implementing `Behavior`:

```rust,ignore
let root = Machine::new(state, phase, transition)
    .with_one_shot(TimerId(1), Duration::from_secs(1), stop)
    .stop_on_shutdown();

Application::new(root).run()
```

`bombay::actors::ActorExt` is imported by the ordinary prelude, so reusable
wrapper policy is discoverable from every concrete behavior value. Each method
returns the exact existing Behavior Actors wrapper: the example above is inferred as
`StopOnShutdown<OneShot<Machine<...>>>`. Call order remains visible and
semantic; no wrapper, timer, shutdown rule, or default is selected implicitly.
Standalone templates such as `Machine` and `Task` keep constructors because
their inputs are meaningful policy rather than a behavior to decorate.

Choose a template by the application problem it owns:

| Application need | Existing Behavior Actors vocabulary |
| --- | --- |
| State transitions and deferred messages | `Machine` |
| Hold and replay selected messages | `.with_stash(...)` |
| Idle, relative, periodic, or absolute time policy | `.with_receive_timeout(...)`, `.with_one_shot(...)`, `.with_periodic(...)`, `.with_deadline(...)`, `Lease` |
| One result with cancellation | `Task` |
| Graceful lifecycle policy | `.stop_on_shutdown()`, `FinalizeOnShutdown`, `Watch`, `Link`, `TerminationMonitor` |
| Fixed and dynamic supervision | `Supervisor`, `Supervise`, `DynamicSupervisor`, `Proxy` |
| FIFO and keyed assignment | `WorkerPool`, `KeyedWorkerPool` |
| Transform or route protocols | `MessageAdapter`, `Router`, `Broadcast` |
| Admission, ordering, and resilience | `Buffer`, `WorkQueue`, `PriorityQueue`, `RateLimiter`, `CircuitBreaker`, `Sequencer`, `Deduplicator`, `Correlator`, `Acknowledgements`, `OrderGate` |
| Discovery and membership | `Registry`, `Resolver`, `Presence`, `PubSub`, `Topic` |
| Multi-party coordination | `Latch`, `Barrier`, `Workflow` |
| Bounded state retention | `Cache` |
| Operational state | `Health`, `Readiness`, `Features`, `Configuration` |

These names are owning library constructions, not Bombay imitations. They use
the same root-first `Application` and exact typed interpreter lanes as every
other actor; there is no separate atomic module, source-settlement framework,
or aggregate runtime path.

When the template catalogue cannot express an application-specific
user-message fold, `#[bombay::actor]` delegates its nominal algebra to the
owning Behavior attribute while the application authors deterministic state
and exact actions:

```rust,ignore
struct Device {
    temperature: Option<f64>,
}

enum DeviceMessage {
    Record(f64),
    Read(Recipient<Temperature>),
}

#[bombay::actor(
    sends = { temperatures: Vec<Delivery<Temperature>> },
)]
impl Device {
    fn receive(&mut self, message: DeviceMessage) -> BehaviorActed<Self> {
        // Domain transition returning exact visible Actions.
    }
}
```

The message remains an explicit Rust type on the fold. Bombay's local address
is fixed by the application runtime, so the facade supplies it and the ignored
sender mechanically. `MACRO-LAST-COMPARISON.md` records the ordinary-Rust,
diagnostic, differential, hygiene, and expansion evidence that justified this
narrow syntax layer.

Effects are authored with Behavior's named semantic send products and
typed `SendAlgebra::send`. Heterogeneous children use Behavior's closed
`Children` / `ChildChoice` creation algebra. Application code does not
implement product traversal, runtime interpreters, or Bombay capability
protocols.

`Protocol` deliberately contains only the actor's address and message
signature. `Behavior` contains its executable event and effect algebra. This
separation lets `Recipient` and `Delivery` name a destination without
recursively proving the destination's entire actor tree—for example, a root
that sends to a supervisor whose `MessageAdapter` replies to that same root.

`#[bombay::actor(...)]` expands through
`#[bombay::behavior::behavior(...)]`; it does not implement Behavior semantics.
The owner expansion retains the same nominal protocol, named send and closed
birth products, `NoSends`, `NoBirths`, `Never`, and visible `Actions`. Named
destination-only protocols use an ordinary explicit `Protocol` implementation
so nominal identity remains obvious and inspectable.
Template-owned service-event sums, phases, ingress proofs, and wrapper policy
remain explicit Behavior Actors composition rather than macro inference.

## Authoring levels

The intended ordinary experience currently has three escape depths:

1. `#[bombay::actor]` state folds whose declarations enumerate permitted sends
   and births and lower to the exact Behavior-owned expansion;
2. `ActorExt` for policy decorating an existing behavior and owning Behavior
   Actors constructors such as `Machine` and `Task` when their inputs are the
   standalone role's semantic policy;
3. direct foundational `Behavior`, `Actions`, named products, paths, and
   interpreter traits for novel library composition.

All generated and template-produced values are concrete, every operation is
limited by a statically declared capability, and every effect remains explicit
in `Actions`. There is no Akka-style unrestricted context, ambient runtime
lookup, dynamic registry, or erased topology.

Level 3 is intentionally inspectable. A library author implements the existing
`Behavior` contract and returns complete `Actions`; Bombay does not generate a
smaller effect language over it. The selected Actors package owns exhaustive
template laws. Bombay owns concrete activation and interpretation through the
selected Behavior operations.

## Runtime boundary

Actor protocols have two truthful typed destination forms:

- `Recipient<P>` names a logical address that may intentionally resolve the
  current incarnation; and
- `EstablishedRecipient<P>` names one exact already-activated incarnation and
  never retargets.

`ActorRef<P>` remains the external/runtime reference. Its
`established_recipient()` method produces the opaque exact capability that may
then cross typed actor messages. Neither form exposes receive authority, a
mailbox, runtime task ownership, endpoint extraction, or cancellation.

Exact request/reply can therefore state its lifetime policy directly:

```rust,ignore
enum CounterMessage {
    Read(EstablishedRecipient<CounterValue>),
}

Actions::cont().send_values(EstablishedDelivery::new(reply_to, value))
```

This send needs no destination actor-space declaration or address lookup.
Use the logical `Recipient<P>` / `Delivery<P>` pair when stable-name or
replacement routing is intended instead.

External transports, CLIs, tests, and embedded clients use one boundary:

```rust,ignore
struct Api {
    orders: EstablishedRecipient<Orders>,
}

let interface = application.interface(Api {
    orders: application.root().established_recipient(),
});
let lifecycle = application.lifecycle();
let mut caller = interface.external::<OrderReplies>()?;

caller
    .send(
        &interface.api().orders,
        OrderCommand::Get {
            reply_to: caller.recipient(),
        },
    )
    .await?;
let reply = caller.receive().await;
```

`ActorInterface<Api>` does not expose topology, private children,
interpreters, or shutdown. `Api` is the application-owned receptionist
product; Bombay does not infer publicness from behavior types or names.
`ExternalActor<P>` owns a fresh claimed address, supplies that address as the
truthful message origin, clones only its exact reply capability, and keeps the
receiver affine. `ApplicationLifecycle<P>` is the separate shutdown and
termination authority.

The direct interface send is intentionally exact. A logical `Recipient<P>` is
relative to a selected Address namespace and cannot be made transport-neutral
by copying its numeric address. Such capabilities remain useful for stable
proxy, membership, discovery, and replacement policy, but direct external use
requires a separately declared logical host/transport interpreter. Similarly,
`ReplyRoute<P>` retains both logical and exact lanes statically; selecting its
exact variant does not remove the logical host bound. Exact-only external
customer seams should instantiate templates with
`EstablishedRecipient<P>`.

The intended ordinary public surface is:

- `Application::new(root).child(role, actor).run()`, `run_with`, feature-gated
  `run_axum`, exact terminal projections, and typed runtime errors;
- a focused prelude for Level-1/2 Bombay actor vocabulary;
- pure logical `Recipient<P>` and exact `EstablishedRecipient<P>` values in
  actor messages;
- `ApplicationHandle<P>` at running application boundaries, with its
  non-owning root `ActorRef<P>` reached through `root()` and its explicit
  `ApplicationLifecycle<P>` reached through `lifecycle()`;
- `ActorInterface<Api>` for one explicit receptionist product and creation of
  real typed external actors;
- `ExternalActor<P>` with cloneable exact reply capability, truthful origin,
  and affine receive ownership;
- `ApplicationLifecycle<P>` as the projection carrying only shutdown and
  termination authority;
- retained terminal facts through `ActorRef::termination`;
- concrete Behavior Actors templates and their policy types;
- `bombay::testing::InfallibleResultExt` as an explicit pure-test import for
  exactly uninhabited fold errors.

The advanced composition surface remains available through explicit
`bombay::{App, ActorSpace, ActorSpaces, Hosts}` imports. It is not in the
ordinary prelude and is not the target application API.

Users never manually construct or implement:

- a runtime, System, Guardian, Driver, Environment, or Incarnation;
- mailboxes, channels, capacities, address spaces, claims, or leases;
- observation publishers, timer queues, executor tasks, or child-task owners;
- creation, delivery, observation, timer, report, or shutdown interpreters;
- supervision loops, worker scheduling, restart machinery, or shutdown
  traversal;
- `SendAlgebra`, `SendInput`, creation dispatch, or product-routing
  implementations already supplied generically.

## Stable Entity references

An ordinary application names asynchronous reconstruction and exact lifecycle
fact consumers once in a nominal definition:

```rust,ignore
struct Accounts;

impl EntityDefinition for Accounts {
    type Id = AccountId;
    type Behavior = StopOnShutdown<Account>;
    type Hosts = Spaces;
    type HydrationError = LoadAccountError;
    type Terminal = ApplicationTerminal;

    async fn hydrate(
        &self,
        id: EntityId<Self::Id>,
    ) -> Result<Self::Behavior, Self::HydrationError> {
        load_account(id).await.map(|account| account.stop_on_shutdown())
    }

    // Consume activation failure, actor-originated refusal, forced-drain,
    // and exact final-retirement facts here.
    # /* ... */
}

let application = App::new(root, spaces).entity_family(
    AccountsRole,
    Accounts,
    DirectoryConfig::default(),
    EntityCapacity::new(concurrent_hydrations, residents),
)?;

let (output, root_terminal, shutdowns) = application.run_with_entities(
    |application| async move {
        let accounts = application.entities(AccountsRole);
        let account = accounts.entity(account_id);
        let interface = application.interface(account);
        let caller = interface.external::<Replies>()?;
        caller.send(interface.api(), AccountCommand::Deposit(amount)).await
    },
)?;
```

`EntityRef<Accounts>` retains the logical account ID, not an actor address or
incarnation. The same value therefore survives passivation and replacement.
Its definition is nominal: two families remain non-interchangeable even when
they select identical ID and message types. `Entities<D>` is only the
cloneable admission/passivation receptionist; `App` retains family shutdown
and task-join authority. `ApplicationHandle::passivate_entity` starts graceful
passivation by role without exposing an incarnation address.

External actors use the same `send` operation for exact actor recipients and
Entity references, and their claimed address reaches the incarnation as
truthful caller provenance. Inside a Behavior fold, use
`EntityRef::request(command)` in a named
`InterpreterRequests<EntityAdmission<D>>` lane. The runtime supplies the
emitting actor's address and sends exact refusal ownership to
`EntityDefinition::admission_refused`; no fold performs the admission effect
directly.

Successful admission does not claim that the actor processed the command or
that durable state committed. External rejection returns the exact owned
command in `AdmissionFailure<Command>`. Hydration failure, resident-capacity
refusal, host-launch retirement, forced drain, and final actor retirement are
separate typed facts consumed by the definition.

The generic `EntityRuntime<I, C, R>`, `EntityId<I>`, directory kernel, and
`LocalEntityRuntime` remain available under `bombay::entity` for integrations
and deliberate advanced use. The kernel now closes admission, settles
installed activation/delivery work, drains every represented exact
incarnation, and joins its logical task group without polling. Native lowering
hydrates before address allocation, uses the application allocator, preserves
forced-retirement provenance and descendant terminals, and launches the exact
authored lifecycle stack. `run_with_entities` settles the root first, then
closes, drains, and joins every declared family and returns its bounded metrics.

## Local actor spaces

Behavior cannot infer whether every mentioned protocol is locally hosted,
remote, or externally supplied. At the deliberate advanced boundary,
a named actor-space product owns one exact `ActorSpace<P>` for each locally
hosted protocol and `Hosts<P>` selects that field statically. This is retained
for explicit multi-protocol composition and is not presented as ordinary
single-root application assembly. There is no manifest, namespace, runtime
registry, `TypeId`, `Any`, or endpoint enum.

The ordinary application API contains no manually authored HList or positional
witness. Role-first application assembly is withheld until its exact static
ownership projection exists; the single-root path and deliberate advanced
actor-space path remain executable.

The concrete host product is authored independently of Behavior's logical-host
evidence. Equal logical requirements repeat the same lawful `Hosts<P>` bound;
they do not request multiple spaces or compiler-resolved deduplication. Raw
addressed delivery, observation, and child shutdown still require their owning
typed capability boundaries; Bombay neither searches occurrence spaces nor
treats roles as protocol identities.

`InterpretItem<Delivery<P>, RootEvent, Path>` remains the broader routing
capability: an interpreter may route locally, remotely, or through a
composition of both. `Hosts<P>` says only that this actor system can install
and resolve local incarnations of `P`.

Established destinations use absolute `Recipient<P>` values, while
creator-local delivery uses `ChildRecipient<P>` and cannot escape as a stable
identity. Dynamic-supervisor outcomes now return the established managed-child
recipient, and pool assignments carry their completion recipient. Bombay
interprets both generically; it does not conceal address arithmetic behind a
parallel reference abstraction.

## Acceptance application

Bombay is not complete when it can merely run a counter. The acceptance
application must prove all of these while retaining the tiny `main`:

- recursive heterogeneous creation;
- typed local delivery and rejected-payload recovery;
- dynamic supervised membership and explicit restart policy;
- backoff timers and stale-timer rejection;
- worker-pool scheduling;
- exact-incarnation observation and cleanup;
- orderly recursive shutdown;
- exact terminal failure reporting;
- no runtime plumbing in application code.

Graceful shutdown order is Behavior policy expressed by templates such as
`ShutdownCoordinator` and `TreeShutdown`. Independently of that policy,
Bombay cancels and joins any child still live when its owner terminates, so an
incomplete graceful protocol cannot leak a subtree or deadlock retirement.

Current implementation eligibility and upstream blockers are recorded in
[`open-design-ledger.md`](open-design-ledger.md).

The acceptance application originated as a manually constructed foundational
composition. Behavior's generated products and Bombay's actor recipes
now reduce that mechanical spelling while preserving the same action
visibility, lifecycle ownership, shutdown order, typed errors, and compile-time
capability rejection. Owner differential tests retain the manual constructions
as independent equivalence evidence.
