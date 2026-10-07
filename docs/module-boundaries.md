# Bombay module boundaries

This document maps source ownership. Semantic capability laws are in
[`runtime-capability-interfaces.md`](runtime-capability-interfaces.md); current
implementation blockers are in
[`prd-backlog/status.md`](prd-backlog/status.md).
The [public API audit](public-api-audit.md) records each retained caller-facing
type, its methods, construction and custody rules, standard trait decisions,
and compiled caller evidence.

The [completion requirements](prd-backlog/README.md) extend the
planned scope with Bombay-owned Zenoh networking, typed identity integration
and distributed hosting. Zenoh is the only planned production networking
substrate; deterministic test hosts remain supported. Selo's KERI integration
is supplied by Selo, which builds on Bombay, Mnesis and `mnesis-bombay`;
there is no reverse Bombay dependency. These are planned ownership boundaries,
not additional source modules or currently implemented public APIs.

## Workspace boundary

```text
bombay-behavior
    deterministic Behavior algebra
                |
                v
crates/bombay-engine
    Driver<B, E>
    Environment<B> -> ActiveEnvironment<B>
                |
                v
crates/bombay
    actor API, template composition, local capabilities, and actor execution ownership
```

### `bombay-engine`

Engine owns only:

- the universal causal sequence from initialization through retirement;
- the affine prepared/live Environment port;
- complete action handoff;
- exact Driver completion and failure vocabulary.

Engine owns no executor, address, mailbox, observation, timer, task,
incarnation, topology, actor template, or runtime adapter. Its semantics and
verification are defined by `driver-law.md` and `driver-test-strategy.md`.

### `bombay`

Bombay owns:

- the public application façade over Behavior and Behavior Actors;
- Entity lifecycle and application-level composition over reusable Behavior
  Actors templates;
- one concrete standard local Environment composition;
- actor incarnation task ownership and terminal classification;
- Address-space retention and exact lease ordering;
- Communication mailbox construction and event acquisition;
- Observe activation and termination publication;
- actor-local TimerQueue integration with the executor clock;
- static interpretation of named creation, delivery, observation, timer,
  report, and shutdown effect lanes;
- recursive child-task retirement;
- inherent async `Application::run` and advanced `App::run`, supplied Work,
  and paired execution/result receiving over one common execution owner;
- configured `Application::run_blocking`, which drives that same pair;
- the opt-in Axum boundary that gives a router the typed application handle
  and coordinates HTTP/root termination.

Bombay does not own Behavior's pure algebra, a second effect algebra, a
registry, a service locator, a public namespace, or a competing lifecycle
framework.

Bombay does own conservation of authoritative typed facts at the runtime
boundary. Once an exact fact is obtained from Address, Communication, Observe,
Timers, or Driver completion, no reusable Bombay layer may erase or
misclassify it accidentally. Independent typed consumers of the same exact
fact must coexist. Preservation, transformation, and discharge remain explicit
typed Behavior policies; this runtime obligation does not create a global
“failures bubble upward” rule.

## Source disposition

The runtime is one concrete local composition. Removed transitional creation,
delivery, and positional-product modules are not part of the current source
tree:

| Module | Current responsibility | Public disposition |
|---|---|---|
| `lib.rs` | minimal façade and semantic-level re-exports | curated application prelude, template-family modules, `Application`, `App`, `RunError`, typed boundaries, transitional hosting evidence, and deliberate `behavior` power surface |
| `application.rs` | pure ownership of one root and its semantic-role child declarations before execution | `Application` and its root-first `child` method are public; it owns values and roles, never runtime capabilities, addresses, or a second birth algebra |
| `application_runtime.rs` | one paired application execution/result owner and concrete runtime capability product | `ApplicationHandle`, `ApplicationLifecycle`, `App`, `ApplicationOutcome`, and `RunError`; inherent async calls, configured owned blocking, and same-path HTTP/Entity composition |
| `child_bindings.rs` | one closed occurrence product for exact child creation status, endpoint/control, address space, ordered task ownership, and descendant retirement | crate-private; application interpretation consumes its typed creation state |
| `entity/` | native family definition, stable logical identity, bounded hydration/admission, passivation, exact retirement, and family shutdown/join | `EntityDefinition`, `EntityCapacity`, `Entities<D>`, and `EntityRef<D>` form the native application surface; the generic directory and runtime port remain advanced |
| `topology.rs` | static `Hosts<P>` runtime selection | public for deliberate advanced `App` composition; ordinary `Application` privately materializes its closed actor-space product |
| `local.rs` | prepared/live Environment and concrete delivery/installed-actor capabilities | root-curated `ActorRef`, `InstalledActor`, and exact-payload `SendError`; `InstalledActor` has no public constructor or shutdown method |
| `launch.rs` | spawn concrete owned incarnation tasks, request owner cancellation on abandoned waits, and settle actor-owned activation tasks inside the spawned incarnation task | only the `ActorSpace<P>` alias is public; lower-layer RootActor/spawn_root_with/OwnedTask.finish remain cfg(test) native owners, not a second production runner |
| `observe/` | actor-independent exact publication and shared/affine waiting | private to Bombay; its ordinary tests run in Bombay, while the isolated test package compiles the same source for Loom and for fuzz/performance dependencies |
| `interpret.rs` | statically dispatch complete named action lanes through Behavior's owning interpretation and settlement types | traversal and interpreters remain crate-private; root `Completion` and `SettlementFailure` exports belong to Engine |
| `observation.rs` | own the actor's pending peer and child termination facts in registration order and inject their typed Behavior events | private; the application capability product owns its single queue |
| `time.rs` | own the actor's `TimerQueue` and adapt due entries into typed events | private; the application capability product owns its single queue |
| `terminal.rs` | preserve exact root and child origin provenance and typed actor retirement | public `RootOrigin<Owner>`, `ChildOrigin<Owner, Role>`, `ActorRetirement`, and `ProjectTerminal`; root origins have no nonce and child origins require one |
| `reports.rs` | preserve exact typed parent reports | private |
| `termination.rs` | publish the coarse external termination observation | private |
| `actor_execution.rs` | one Driver execution plus terminal classification | private `ActorExecution` owning the Driver and retirement authority |
| `actor_outcome.rs` | exact terminal vocabulary | private `ActorExecutionOutcome` preserving every disposition |
| `retirement.rs` | affine terminal handoff | private |

`crates/bombay-macros` owns three syntax-only projections: the narrow
`#[bombay::actor]` facade delegates to Behavior's owning actor expansion,
`TerminalProjection` lifts exact actor retirements into an application-owned
sum using explicit child-source markers and Rust's `ChildRole` proofs, and
transitional `ActorSpaces` derives static protocol-host proofs from explicit
field annotations. Neither derives type identity from a spelling. None
owns actor semantics, an effect algebra, lifecycle policy, or runtime behavior.
Bombay has no protocol macro.

Communication UC1, Observe UO1, actor-owned timers, exact report preservation,
and recursive task retirement are integrated. Any later simplification must
preserve the same ownership and observable inversion proofs rather than
reintroducing a parallel runtime mechanism.

## Static adapter boundary

The Engine Environment pair is the one behavior spine. The concrete local
Environment contains real primitive values, not Bombay wrappers around them.
Named effect leaves implement focused static traits such as send
interpretation and Behavior's existing birth installation. Products delegate
to leaves at compile time.

Do not introduce:

- `dyn Any`, downcasts, or erased effect envelopes;
- a dynamic capability registry;
- one giant trait with a method for every runtime facility;
- user-authored traversal of Bombay's capability products;
- a second Tokio-specific Driver loop.

## Public boundary

Ordinary users should name only Bombay actors, templates and policy values,
pure logical `Recipient<P>` or exact `EstablishedRecipient<P>` values,
external boundary `ActorRef<P>` values, stable `EntityRef<F>` values, `run`,
and deliberate run errors. Driver, Environment,
incarnation, primitive capability construction, topology storage, task
ownership, and effect interpreters remain internal or advanced extension
surface.

Behavior's application-authoring profile generates nominal effect and birth
products. Bombay re-exports `ActorExt` and owning Behavior Actors constructors
such as `Machine`, `FixedSupervisor`, `DynamicSupervisor`, `FifoPool`, and
`KeyedPool`; Bombay does not wrap them in compatibility recipes. Every
policy and domain capability input remains explicit. Behavior owns the
deterministic algebra, Behavior Actors owns reusable actor policy, and Bombay
owns concrete application composition, activation, and runtime interpretation.
The result remains inspectable concrete composition, never a dynamic context,
erased graph, alternate actor trait, or second runtime path.

## Entity boundary

Entity is not a second actor lifecycle. A stable `EntityId` is a Bombay
runtime-routing key that can accept a caller-owned domain command while no
actor exists. The Entity directory owns the bounded shared activation waiter
set, asynchronous domain hydration, exact-incarnation delivery reservations,
the ordered processing fence, the affine runtime lease, and removal after the
matching termination fact.

Behavior Actors retains every law inside the activated actor graph.
`StableProxy` owns one proxy actor's worker creation, initialization,
readiness, explicit replacement, shutdown, and terminal custody;
`DynamicSupervisor` owns keyed proxy management and its operation authority;
`Registry` and `Resolver` own behavior-level typed recipient discovery; and
Behavior Actors `Machine` owns only receive/become/defer/stop policy. Entity
may launch an authored behavior already composed from those templates, but it
must not translate or reproduce their events, effects, correlations, errors,
or terminal products.

The Entity slot's total `decide` transition returns its successor state,
ordered effects, and disposition together. One private slot mutex linearizes
that turn and queues its effects for ordered interpretation without holding the
lock during callbacks. The independent state/input oracle checks the
transition directly. Bombay has no separate Machine crate or topology
execution layer.

The ordinary Entity facade exposes stable references, nominal definitions,
capacity, admission results, and terminal facts. The deliberate advanced
`LocalEntityRuntime` port and `LocalDirectory` kernel remain nameable. A
directory dispatch returns an `InstalledDispatch` containing its correlation
and one `InstalledSlotDecision` awaiting interpretation. Slot phase structs,
raw effect batches, and private native host proofs do not cross that boundary.

Entity lifecycle tasks are registered and joined by one Entity task group.
The local runtime port returns a concrete join handle for each scheduled task;
family shutdown preserves exact panic or cancellation failures in its typed
shutdown outcome. The group gives one shutdown claimant the result and returns
task custody on cancellation. Admission and passivation share the shutdown
gate, so a passivation cannot schedule a drain after family shutdown claims it.

Consequently Entity "replacement" means a fresh command-triggered activation
only after the previous incarnation's fenced retirement and exact directory
removal. It is not `StableProxy::replace` or `DynamicCommand::Replace`.
Conversely, none of the locked templates accepts a domain command for an absent
key and retains that command through one shared hydration attempt, so composing
them does not replace the Entity directory transaction.
