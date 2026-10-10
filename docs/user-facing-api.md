# Bombay user-facing API

This is the target product contract. Bombay owns the stable application and
runtime experience over foundational Behavior and the reusable templates owned
by Behavior Actors.

Implementation status is separate from this target contract. Local application,
external interface, timer, Entity, supervisor, and worker-pool paths have
executable coverage. Async methods use a caller-owned Tokio host. The explicitly
owned `run_blocking` entry takes a configured Tokio Builder for current-thread
or multithread execution. Durability and networking remain planned.
See the [source-backed inventory](prd-backlog/evidence.md). The short code
fragments below are schematic, omit surrounding definitions, and are not
compilation evidence. The executable, compiler-checked spellings are:

| Boundary | Executable or compile-checked owner |
| --- | --- |
| Root execution, typed terminal, application children | [application topology example](../examples/application-topology/src/main.rs), [terminal projection tests](../crates/bombay/tests/terminal_projection.rs) |
| `run_with`, lifecycle, shutdown, exact terminal results | [run_with tests](../crates/bombay/tests/run_with.rs) |
| Axum adapter | [Axum example](../examples/axum/src/main.rs), [HTTP tests](../examples/axum/src/http.rs) |
| Advanced actor-space product and protocol hosts | [supervision example](../examples/supervision/src/main.rs), [host compile fixtures](../crates/bombay/tests/actor_spaces.rs) |
| `Machine`, timers, and `ActorExt` wrappers | [actor-template example](../examples/actor-templates/src/main.rs), [template application tests](../crates/bombay/tests/template_application.rs) |
| Actor facade and exact send lane | [counter example](../examples/counter/src/counter.rs), [macro parity tests](../crates/bombay/tests/macro_last_authoring.rs) |
| Exact external request and reply | [actor interface tests](../crates/bombay/tests/actor_interface.rs) |
| Native Entity definition and application | [Entity example](../examples/entity/src/main.rs), [Entity application test](../crates/bombay/tests/entity_application.rs) |

## Entry boundary

`Application` owns a pure root and its declarations. `run`, `run_with`, and
feature-gated `run_axum` are inherent async methods. They acquire the currently
entered Tokio host when polled, and return original inputs with `TryCurrentError`
when no host is entered. Constructing their futures does not stage or start an
actor. Actors execute on that selected host; supplied Work executes where its
execution future is polled. Work and its output may borrow caller values and
need not be `Send`. Actor tasks keep their actual `Send + 'static` requirements.

For a root with an empty descendant projection destination:

```text
let outcome = Application::new(service.stop_on_shutdown())
    .run::<_, _, Never, _>()
    .await;
```

`Never` selects the empty descendant destination. The actual Actor, staging
failure, and child failures remain inferred. A declared child topology selects
its actual closed descendant projection instead. Bare inference is preferred
where its constraints determine that destination; an explicit meaningful
selection is allowed. No default, dummy value, or wrapper supplies missing
information.

The successful outer result owns `(work_outcome, root_receiving,
notification_receiving)`. The work outcome is `ApplicationOutcome`; its presence
does not imply a completed actor or erase an initialization refusal. Its closed alternatives
retain the actual phase and values:

- `StagingRejected` returns the actual cold partial product.
- `Unstarted` returns the untouched application and its actual cold Work. Supplied
  Work is bare; no-work remains `None`; cold HTTP uses unit while its original
  application, Router and address remain in the application-input product.
- `Prepared` returns the actual prepared Actor/Spaces product, original Work,
  and the actual cleanup result, before actor ownership transfer.
- `NotInvoked` returns original Work, the startup receiving error when
  present, and the independent cleanup result.
- `Completed` returns the original supplied output and independent cleanup.
  No-work completion returns `None` without a fabricated callable.
- `Interrupted` retains cleanup when the Work publication does not provide an
  owned result; it does not infer whether an actor existed from channel closure.

No supplied Work uses `Option<Never>` with `None` for both Work and output.
`Completed { output: None }` records an acquired startup grant without supplied
Work. For supplied Work, `output` is the original bare `WorkFuture::Output`. A user
`Err(error)` or `None` remains that exact output. The framework adds no `Some`
layer and cannot fabricate absence when the supplied output type is unit.

Work-phase cleanup is `Result<(), ApplicationCleanupError>`.
`PublicationClosed(RecvError)` retains the original cleanup-publication receiving
error; `TaskFailed(JoinError)` retains the actual cleanup task error. Alongside
the work outcome, `root_receiving` is
`Result<(RootOrigin<Owner>, ActorRetirement<Actor, Terminal, ChildFailures>), RecvError>`
and `notification_receiving` is `Result<ActorNotificationReceipts, RecvError>`.
The two named notification fields, `termination` and `retirement`, each retain
success or their own original `RetirementNotificationError`. Acquired native and
notification receipts survive a later cleanup-task failure. A closed receipt
retains its actual receiving error; it cannot establish an absent actor or
invent a JoinError. The native root's `ActorTaskFailed(JoinError)` separately
owns actor-task failure. `Terminal` is the descendant destination.
The root stays raw. A caller may use the existing `ProjectTerminal::project`
outside the runtime to apply its own root projection policy, using the original
origin and retirement without changing the actual Owner/Actor distinction.
`ApplicationOutcome` is an explicit `bombay` root import, not a prelude export.

Supplied Work receives the activated application's concrete handle:

```text
let outcome = Application::new(service.stop_on_shutdown())
    .run_with::<Never, _, _, _, _, _>(|application| async move {
        let lifecycle = application.lifecycle();
        let reply = boundary(application.root()).await;
        let shutdown = lifecycle.request_shutdown();
        (reply, shutdown)
    })
    .await;
```

Returning from Work does not request shutdown. The root's concrete Behavior
must accept or transform `ShutdownRequested`; topology does not select policy.
`ApplicationHandle::root()` projects delivery, and `lifecycle()` projects
shutdown/termination authority. Its `retirement()` waits for a runtime-issued
`ActorRetirementReport` after the actual actor and owned work have joined, while
application Work can still run. Independent waits and cancellation of one wait
do not consume this shared fact. The report separately assesses whether
retirement was established and whether failures were found, none were found
after complete checking, or checking was incomplete. It preserves known failure
presence even when retirement is unestablished. Later report-notification or
application conversion faults cannot rewrite the issued report. The complete
native result remains with its existing owner; neither ordinary termination
nor the report replaces it. Final native handoff and Entity family shutdown
retain their Work-completion barrier.

For cancellation or unwind recovery, use `execute` or `execute_with` to obtain
separate execution and receiving futures. The paired constructor captures its
entered host immediately and stays cold until execution is polled. Keep and
poll receiving after dropping execution. A one-future `run` or `run_with` owns
both futures; dropping it also surrenders the receiver. Original native setup
or Work panic payloads unwind to the caller. A Ready output is published before
Work-future disposal, so retained receiving can still acquire it if disposal
panics. A recovered Prepared product is actual prepared input, not a fabricated
cold application.

The selected host must remain live and polling through the required joins.
Holding a Handle or observing termination does not guarantee host liveness.
Runtime destruction can cancel remaining tasks; neither entry promises recovery
of values that user code already consumed or destroyed inside a failing call.
Explicitly surrendering the final receiver discharges its original returned
values once. It does not authorize accidental loss in reusable code.

The sole owned synchronous convenience is `Application::run_blocking(builder)`.
Supply an ordinary configured `tokio::runtime::Builder`. An entered-runtime
refusal occurs before construction or actor effects. Refusal returns
`(application, builder, RunError::BlockingInEnteredRuntime)`; a real build error
returns `(application, builder, RunError::Runtime(original_io_error))`. A
successful build drives the same async pair and preserves its full result.
There is no automatic current-thread default, scheduler enum, or blocking
supplied-Work/HTTP twin. A synchronous supplied-Work caller may explicitly own
its Tokio host and call `block_on` on the same async method.

With `axum`, `execute_axum` and async `run_axum` use that same ownership path.
Bind occurs before staging and returns the actual cold application, original
router, address, and native bind error on failure. The router is invoked once
only after activation. Acquired HTTP Work owns the original Router/Listener pair. Work output is the
exact bare `Result<(), io::Error>` from serving; raw root and cleanup facts coexist with it. Graceful serving
completion is not proof that the root result is successful. Axum owns HTTP
extraction/responses; Bombay adds no projected aggregate AxumRunError.

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

```text
#[derive(Default, ActorSpaces)]
struct AppActors {
    #[actor_space(SystemProtocol)]
    system: ActorSpace<SystemProtocol>,
    #[actor_space(GroupProtocol)]
    groups: ActorSpace<GroupProtocol>,
    #[actor_space(DeviceProtocol)]
    devices: ActorSpace<DeviceProtocol>,
    #[actor_space(QueryProtocol)]
    queries: ActorSpace<QueryProtocol>,
}

```

`ActorSpaces` is deliberate level-3 runtime plumbing, not the ordinary
application API. It generates one ordinary `Hosts<P>` implementation for each
field explicitly marked `#[actor_space(P)]`; Rust verifies the field is an
`ActorSpace<P>`, including through a type alias. These implementations remain
the static protocol-to-space mapping; they are generated at compile time,
never erased or discovered at runtime. Rust rejects duplicate protocols.
Bombay's generic
`App<Root, Actors>` retains the spaces while giving only the root to the
Behavior Driver.

A single-protocol root does not declare that product. Bombay supplies its one
concrete protocol space directly:

```text
let outcome = Application::new(OrderBook::default().stop_on_shutdown())
    .run_axum::<Never, _, _, _, _>(address, |application| {
        let interface = application.interface(order_http::OrderApi {
            orders: application.root().established_recipient(),
        });
        order_http::router(interface, application.lifecycle())
    })
    .await;
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

```text
let root = Machine::new(state, phase, transition)
    .with_one_shot(TimerId(1), Duration::from_secs(1), stop)
    .stop_on_shutdown();

Application::new(root).run::<_, _, Never, _>().await
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
| Graceful lifecycle policy | `.stop_on_shutdown()`, `FinalizeOnShutdown`, `Watch`, `TerminationMonitor` |
| Fixed and dynamic supervision (runtime integration incomplete) | `FixedSupervisor`, `DynamicSupervisor`, `StableProxy` |
| FIFO and keyed assignment (runtime integration incomplete) | `FifoPool`, `KeyedPool` |
| Transform or route protocols | `MessageAdapter`, `Router` |
| Admission, ordering, and resilience | `Buffer`, `WorkQueue`, `PriorityQueue`, `RateLimiter`, `CircuitBreaker`, `Sequencer`, `Deduplicator`, `Correlator`, `Acknowledgements`, `OrderGate` |
| Discovery and membership | `Registry`, `Resolver`, `Presence`, `PubSub`, `Topic` |
| Multi-party coordination | `Latch`, `Barrier`, `Workflow` |
| Bounded state retention | `Cache` |
| Operational state | `Health`, `Readiness`, `Configuration`; `FeatureSet` is supporting state |

These names come from the selected owning library. Existing compositions use
the same Behavior algebra and typed interpreter lanes. The owner has an
`atomic` module; Bombay must complete interpretation of its requests before
advertising those templates as executable Application features.

When the template catalogue cannot express an application-specific
user-message fold, `#[bombay::actor]` delegates its nominal algebra to the
owning Behavior attribute while the application authors deterministic state
and exact actions:

```text
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
sender mechanically. The [macro-last comparison](../crates/bombay/tests/macro_last_authoring.rs)
keeps the plain-builder experiment and compares complete observable actions
with the owning Behavior form. [Compile fixtures](../crates/bombay/tests/outer_authoring.rs)
and the [renamed downstream crate](../tests/renamed-downstream/tests/renamed.rs)
exercise diagnostics and generated-path hygiene. The
[facade expansion](../crates/bombay-macros/src/lib.rs) delegates to Behavior's
owning macro.

Effects are authored with Behavior's named semantic send products and
typed `SendEffects::send`. Heterogeneous children use Behavior's closed
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

```text
enum CounterMessage {
    Read(EstablishedRecipient<CounterValue>),
}

Actions::cont().send_values(EstablishedDelivery::new(reply_to, value))
```

This send needs no destination actor-space declaration or address lookup.
Use the logical `Recipient<P>` / `Delivery<P>` pair when stable-name or
replacement routing is intended instead.

External transports, CLIs, tests, and embedded clients use one boundary:

```text
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
`ExternalActor<P>` owns a fresh allocated address, supplies that address as the
truthful message origin, clones only its exact reply capability, and keeps the
receiver affine. `ApplicationLifecycle<P, E>` is the separate shutdown and
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

- inherent async `Application::run`, `run_with`, feature-gated `run_axum`,
  paired `execute`/`execute_with`/`execute_axum`, raw actual root results,
  application-selected descendant projections, and configured `run_blocking`;
- a focused prelude for Level-1/2 Bombay actor vocabulary;
- pure logical `Recipient<P>` and exact `EstablishedRecipient<P>` values in
  actor messages;
- `ApplicationHandle<P, E, Families>` at running application boundaries, with its
  non-owning root `ActorRef<P>` reached through `root()` and its explicit
  `ApplicationLifecycle<P, E>` reached through `lifecycle()`;
- `ActorInterface<Api>` for one explicit receptionist product and creation of
  real typed external actors;
- `ExternalActor<P>` with cloneable exact reply capability, truthful origin,
  and affine receive ownership;
- `ApplicationLifecycle<P, E>` as the projection carrying only shutdown and
  termination authority;
- retained terminal facts through `ActorRef::termination`;
- concrete Behavior Actors templates and their policy types;
- `bombay::testing::InfallibleResultExt` as an explicit pure-test import for
  exactly uninhabited fold errors.

The advanced composition surface remains available through explicit
`bombay::{App, ActorSpace, ActorSpaces, Hosts}` imports. It is not in the
ordinary prelude and is not the target application API.

Users never manually construct or implement:

- a Bombay System, Guardian, Driver, Environment, or actor execution object;
  selecting a caller Tokio host or configuring its ordinary Builder is explicit;
- mailboxes, channels, capacities, address spaces, claims, or leases;
- observation publishers, timer queues, executor tasks, or child-task owners;
- creation, delivery, observation, timer, report, or shutdown interpreters;
- supervision loops, worker scheduling, restart machinery, or shutdown
  traversal;
- `SendEffects`, `SendInput`, creation dispatch, or product-routing
  implementations already supplied generically.

## Stable Entity references

An ordinary application names asynchronous reconstruction and exact lifecycle
fact consumers once in a nominal definition:

```text
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
    // and exact native retirement with its read-only subtree assessment here.
    # /* ... */
}

let application = App::new(root, spaces).entity_family(
    AccountsRole,
    Accounts,
    DirectoryConfig::default(),
    EntityCapacity::new(concurrent_hydrations, residents),
)?;

let application_receiving = application.run_with_entities(
    |application| async move {
        let accounts = application.entities(AccountsRole);
        let account = accounts.entity(account_id);
        let interface = application.interface(account);
        let caller = interface.external::<Replies>()?;
        caller.send(interface.api(), AccountCommand::Deposit(amount)).await
    },
).await;
```

`application_receiving` is a `Result`: success retains
`(work_outcome, root_receiving, notification_receiving, family_receiving)`; host-entry rejection returns
the original application, Work callable, and actual entered-host error.

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
separate typed facts consumed by the definition. `EntityActivationError` keeps
allocation refusal before an incarnation starts distinct from a started native
retirement, its report and ordinary-notification result. `EntityDefinition::retired`
receives the original native result and the independently derived read-only
`ActorRetirementReport`. Family callback/conversion failures stay in family
failure custody and cannot rewrite that actor-subtree assessment.

The generic `EntityRuntime<I, C, R>`, `EntityId<I>`, directory kernel, and
`LocalEntityRuntime` remain available under `bombay::entity` for integrations
and deliberate advanced use. `EntityRuntime` closes admission, drains every
represented exact incarnation, and joins its logical task group without
polling. The kernel installs and interprets activation/delivery decisions.
`LocalDirectory::dispatch` returns
`InstalledDispatch { dispatch_id, decision }` for one admitted command;
the owned `InstalledSlotDecision` crosses `LocalDirectory::interpret` exactly
once. The pure `EntitySlot`/`SlotEvent` fold remains an advanced model and
benchmark boundary. Slot phase structs and raw effect batches stay private to
Entity.

Native lowering hydrates before address allocation, uses the application allocator, preserves
forced-retirement provenance and descendant terminals, and launches the exact
authored lifecycle stack. `run_with_entities` awaits the same paired owner as
`execute_with_entities`. The result keeps Work outcome, root receiving,
notification receiving, and the recursive family receiving product separate.
Work cleanup has a unit normal result or a distinct `ApplicationCleanupError`
for its receiving/task failure. The independent
root receipt owns its exact origin plus `ActorRetirement`, including an original
native actor failure in `ActorTaskFailed`; each family receipt owns
its original role, complete shutdown, metrics, and native disposal cause.
Root is published before family shutdown; each completed family head is
published before awaiting its tail. A cancelled or panicking later producer
cannot authorize loss of an already acquired root/head report. A receiving
error for an unfinished row is not a fabricated family report. This order is
runtime custody, not a new Behavior shutdown policy.

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

Logical destinations use namespace-relative `Recipient<P>` values; exact
established destinations use `EstablishedRecipient<P>`. Meanwhile,
creator-local delivery uses `ChildRecipient<P>` and cannot escape as a stable
identity. Dynamic-supervisor outcomes now return the established managed-child
recipient, and pool assignments carry their completion recipient. Generic
delivery already exists, but interpreting AssignWorker and ProxyOperation
with exact rejection custody is still blocked in the selected contract.

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
- no internal Bombay task, mailbox, or lifecycle plumbing in application code;
  the caller supplies its Tokio host or configured Builder.

Graceful shutdown order is Behavior policy expressed by templates such as
`ShutdownCoordinator` and `HeterogeneousShutdownCoordinator`. Independently of that policy,
Bombay retains cancellation and join ownership for any child still live when
its owner terminates. Completion requires a live polling host and cooperative
operations; noncooperative work can delay retirement indefinitely. An incomplete
graceful protocol does not authorize surrendering that retained ownership.

Current implementation eligibility and upstream blockers are recorded in
[`prd-backlog/status.md`](prd-backlog/status.md).

The acceptance application originated as a manually constructed foundational
composition. Behavior's generated products and Bombay's actor recipes
now reduce that mechanical spelling while preserving the same action
visibility, lifecycle ownership, shutdown order, typed errors, and compile-time
capability rejection. Owner differential tests retain the manual constructions
as independent equivalence evidence.
