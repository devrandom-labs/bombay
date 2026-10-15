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
With the opt-in `remote` feature, `remote::verify` owns storing the provider's
returned result before its future is disposed. Its private verification module
uses an outside standard result slot and owns no authentication policy.
The merged ingress primitive's private `remote/ingress.rs` owns the nonwaiting
acquire-and-transfer operation exported as `remote::try_retain` and its two native
refusal causes in `IngressError`; reviewed PR363 records delivery. The caller
owns the Tokio channel, count capacity and intact original/permit products. This module
adds no mailbox, actor authority or transport runtime. The new private
`remote/metadata.rs` owns the shared raw fifteen-field `Metadata` declaration
exported with `remote`. Existing optional Serde owns its structural codec;
protocol products own their fields. Metadata is untrusted and format-neutral:
object-shape, resource, profile/version, SDK, replay and permission acceptance
remain distinct receiving obligations. Reviewed PR368 delivers public
`remote/json.rs`: its borrowed `check` uses selected json-event-parser0.2.3
for complete outer/metadata object checks and five explicit acceptance maxima.
It preserves native syntax causes and leaves original protected/proof buffers
with the caller. Concrete Serde protocols still own required/unique fields;
provider, replay and permission policy remain separate. Token materialization
precedes its token check, so no peak-heap bound is claimed. Actual Node consumers
reuse Counter and terminating MailLedger in application_clients/json_receiving.rs;
they are tests rather than a second production receiver. The optional remote graph now names the pinned controlled SDK and configuration
owners. Private application/node.rs owns one finalized Application and one fresh
Session; Application::open_node checks private remote/startup.rs policy first.
Node::execute_with uses the existing Application execution owner and lends the
Session to trusted work. No remote command is authenticated or admitted by this
transport foundation. Typed network exports and multi-node composition remain
pending;
transport fixtures do not establish that public surface. Protected Counter
regressions belong to `crates/bombay/tests/protected_counter`, executed by the
isolated transport-custody package rather than an example binary.

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
- Observe activation, termination and joined-retirement report publication;
- actor-local TimerQueue integration with the executor clock;
- static interpretation of named creation, delivery, observation, timer,
  report, and shutdown effect lanes;
- recursive child-task retirement;
- inherent async `Application::run`, supplied Work,
  and paired execution/result receiving over one common execution owner, with native
  root and two-stage notification receipts independent of Work-phase cleanup;
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
| `lib.rs` | minimal façade and semantic-level re-exports | curated application prelude, template-family modules, `Application`, `RunError`, typed boundaries, transitional hosting evidence, and deliberate `behavior` power surface |
| `application/mod.rs` | root, role-indexed child and Entity-family declarations; curate the application surface | `Application` owns root, selected protocol spaces, role-indexed cold actors and Entity families. Construction starts no task and adds no birth algebra. |
| `application/scope.rs` | establish external endpoints needed before root construction | `ApplicationScope` transfers one root declaration and the existing address allocation source into Application. Prepared and staging rejection retain it for exact-value retry; receivers remain independently owned. |
| `application/composition.rs` | static declaration-to-Behavior composition and exact origin selection | canonical `ApplicationBehavior`, `ApplicationDefinitionError` and `ApplicationStagingError`; implementation traits remain private |
| `application/execution.rs` | one paired execution/result owner, supplied Work, cleanup custody and configured blocking entry | `ApplicationOutcome`, `ApplicationCleanupError` and `RunError`; async, owned blocking and Entity execution share this owner |
| `application/interface.rs` | application capability construction, fixed-target communication and explicit lifecycle projection | ApplicationHandle is the entry; ApplicationClient owns fixed-target replies and ApplicationSender owns one-way sending. Both reuse ExternalActor. The remote implementation branch adds Application-certified actor construction and native nonwaiting client attempts. ApplicationLifecycle owns shutdown/termination/joined-report observation. ActorInterface exports named capabilities without lifecycle authority; no receiving is cloned. |
| `application/node.rs` | opt-in fresh native Session and one finalized Application declaration; reuse the existing execution/receiving owner | `remote::Node`, `NodeExecutionRefusal`; no replacement actor lifecycle or forwarding topology builders |
| `remote/startup.rs` | selected configured TLS profile and exact original-input startup refusal | `remote::NodeStartupRefusal`; native certificate parsing and TLS remain SDK-owned |
| `remote/clients.rs` | remote-only Application-certified client construction refusals | `remote::ExportError`; the existing Application interface owns certification and returns the original target, while native endpoint failures retain their original cause |
| `application/http.rs` | opt-in HTTP preparation, binding and serving over the common execution owner | existing application methods; retains bind, serving and actor failures. The feature-gated outcome mapper remains in `application/execution.rs`. |
| `local/children.rs` | closed occurrence product, exact creation status, endpoint/control and descendant task retirement | crate-private; creation-order native custody and independent descendant retirement assessments stay with the local actor |
| `entity/` | native family definition, stable logical identity, bounded hydration/admission, passivation, exact retirement, and family shutdown/join | `EntityDefinition`, `EntityCapacity`, `Entities<D>`, and `EntityRef<D>` form the native application surface; the generic directory and runtime port remain advanced |
| `topology.rs` | logical protocol hosting, root registration and application-selected descendant schema | `Hosts<P>`, `RootRegistration<P>` and `ApplicationTopology<Root, Members>` preserve distinct logical/physical registration; no-birth roots use their own ActorSpace |
| `local/mod.rs` | curate the concrete local composition and its child modules | private module; canonical public capabilities are re-exported at crate root |
| `local/endpoint.rs` | exact messaging endpoint, installed-actor proof and shutdown authority | root-curated `ActorRef`, `InstalledActor` and exact-payload `SendError`; `InstalledActor` has no public constructor; its typed `request_shutdown` submits to the exact child control lane |
| `local/ingress.rs` | mailbox admission, standard/Entity ingress and exact retirement drain | private; reuses Communication's mailbox |
| `local/environment.rs` | prepared/live Engine port and exact residual custody | private; one standard local Environment for roots, children and Entity actors retains operation failures and descendant retirement assessments before native conversion |
| `local/execution.rs` | actor-owned activation tasks, exact task failures and owner cancellation | private; task settlement remains with the actor owner |
| `launch.rs` | spawn concrete owned incarnation tasks, request owner cancellation on abandoned waits, and settle actor-owned activation tasks inside the spawned incarnation task | only the `ActorSpace<P>` alias is public; lower-layer RootActor/spawn_root_with/OwnedTask.finish remain cfg(test) native owners, not a second production runner |
| `observe/` | actor-independent exact publication and shared/affine waiting | private to Bombay; its ordinary tests run in Bombay, while the isolated test package compiles the same source for Loom and for fuzz/performance dependencies |
| `local/effects/mod.rs` | concrete actor capability product and ordered complete action settlement | private; uses Behavior's owning interpretation and settlement types, never a second effect algebra |
| `local/effects/creation.rs` | establish fresh child requests and retain exact rejected creation inputs | root-curated `NativeChild`, affine `PendingChild` and sealed `EstablishNativeChild` select one existing native kernel; child creation precedes dependent sends |
| `local/effects/delivery.rs` | interpret named logical, established and child sends | private; exact endpoint selection and rejected payload recovery |
| `local/effects/observation.rs` | own the actor's pending peer and child termination facts in registration order and inject their typed Behavior events | private; the application capability product owns its single queue |
| `local/effects/timers.rs` | own the actor's `TimerQueue` and adapt due entries into typed events | private; the application capability product owns its single queue |
| `terminal.rs` | preserve exact root and child origin provenance and typed actor retirement | public `RootOrigin<Owner>`, `ChildOrigin<Owner, Role>`, `ActorRetirement`/`ActorCustody`, `RetirementSchema`/`ProjectTerminal`, runtime-issued read-only `ActorRetirementReport` with its two assessment sums, and affine `ActorNotificationReceipts`/`RetirementNotificationError`; child origins retain nonce, CreationId and CreationKind, while root origins have no child nonce |
| `local/effects/reports.rs` | action-scoped terminal-report custody and exact typed parent reports | private; one curated crate-private `LocalTerminalReports` crossing, not an exposed reports module |
| `termination.rs` | publish the coarse external termination observation | private |
| `actor_execution.rs` | one Driver execution plus terminal classification | private `ActorExecution` owning the Driver and retirement authority |
| `actor_outcome.rs` | exact terminal vocabulary | private `ActorExecutionOutcome` preserving every disposition |
| `retirement.rs` | affine terminal handoff | private |

`crates/bombay-macros` owns syntax-only projections: the narrow
`#[bombay::actor]` facade delegates to Behavior's owning actor expansion,
`TerminalProjection` lifts exact actor retirements into an application-owned
sum through total owned `From` inclusions using explicit child-source markers
and Rust's `ChildRole` proofs, and
transitional `ActorSpaces` derives static protocol-host proofs from explicit
field annotations. Neither derives type identity from a spelling. None
owns actor semantics, an effect algebra, lifecycle policy, or runtime behavior.
The optional inclusion grants no native execution authority. The `topology!`
declaration derives exact native selection, protocol spaces and actor-indexed
recursive custody, reusing the sealed consuming startup operation and existing
Application interpreter. It owns no parallel runtime or lifecycle policy.
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
