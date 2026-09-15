# Bombay module boundaries

This document maps source ownership. Semantic capability laws are in
[`runtime-capability-interfaces.md`](runtime-capability-interfaces.md); current
implementation blockers are in
[`open-design-ledger.md`](open-design-ledger.md).

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
    actor API, templates, local capability composition, and incarnation ownership
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
- the ordinary `Application::new(root).run()` single-root boundary and the
  deliberate advanced `App::new(root, actors).run()` boundary;
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
| `application_runtime.rs` | actor-system runner and concrete runtime capability product | `ApplicationHandle`, `ApplicationLifecycle`, `App`, and `RunError`; opt-in `AxumRunError` and Axum execution |
| `entity/` | native family definition, stable logical identity, bounded hydration/admission, passivation, exact retirement, and family shutdown/join | `EntityDefinition`, `EntityCapacity`, `Entities<D>`, and `EntityRef<D>` form the native application surface; the generic directory and runtime port remain advanced |
| `topology.rs` | transitional static `Hosts<P>` runtime selection | public until `Application` materialization internalizes the closed actor-space product |
| `local.rs` | prepared/live Environment and external typed reference | only `ActorRef` and exact-payload `SendError` escape the private module |
| `launch.rs` | construct one incarnation and place it on Tokio | only the `ActorSpace<P>` alias is public |
| `observe/` | actor-independent exact publication and shared/affine waiting | private, non-publishable, and verified through isolated normal/Loom/fuzz/performance harnesses |
| `interpret.rs` | statically dispatch complete named action lanes and classify concrete runtime interpretation failure | only the flat `EffectInterpretationError` sum is public; traversal and interpreters remain private |
| `observation.rs` | multiplex retained peer and child termination facts into typed Behavior events | private; unsupported duplicate observation and cancellation adapters remain removed |
| `time.rs` | adapt the actor-owned `TimerQueue` into typed events | private |
| `reports.rs` | preserve exact typed parent reports | private |
| `termination.rs` | publish the coarse external termination observation | private |
| `incarnation.rs` | one Driver execution plus terminal classification | private |
| `outcome.rs` | exact terminal vocabulary | private |
| `retirement.rs` | affine terminal handoff | private |

`crates/bombay-macros` owns three syntax-only projections: the narrow
`#[bombay::actor]` facade delegates to Behavior's owning actor expansion,
`TerminalProjection` lifts exact actor retirements into an application-owned
sum, and transitional `ActorSpaces` derives static protocol-host proofs. None
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
such as `Machine`, `Supervisor`, `DynamicSupervisor`, `WorkerPool`, and
`KeyedWorkerPool`; Bombay does not wrap them in compatibility recipes. Every
policy and domain capability input remains explicit. Behavior owns the
deterministic algebra, Behavior Actors owns reusable actor policy, and Bombay
owns concrete application composition, activation, and runtime interpretation.
The result remains inspectable concrete composition, never a dynamic context,
erased graph, alternate actor trait, or second runtime path.
