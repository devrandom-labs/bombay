# Bombay user-facing API

This is the existing API guide, with source and test pointers. Bombay owns the
application and runtime experience over Behavior and Behavior Actors. The
[developer experience coordination](prds/developer-experience.md) records the implemented
feature-branch inference and the remaining migration and release gates.
When prose and source disagree, verify the source and correct the prose.

Implementation status is separate from this target contract. Local application,
external interface, timer, Entity, supervisor, and worker-pool paths have
executable coverage. Async methods use a caller-owned Tokio host. The explicitly
owned `run_blocking` entry takes a configured Tokio Builder for current-thread
or multithread execution. Durability and networking remain planned.
See the [source-backed inventory](prd-backlog/evidence.md). This guide is included in crate rustdoc. The complete entry example below
is executed as a doctest; fragments labelled text or ignore omit surrounding
definitions and remain schematic. The executable, compiler-checked spellings are:

Start with an actor, its messages and `Application`. Add a policy to that actor
through `ActorExt`, or construct a queue, supervisor or pool through its owning
constructor. Types are inferred from the actual messages, workers and policy
values. Direct `Behavior` implementations and typed `Actions` remain available
when you need more control; the same ownership and failure contracts apply at
each layer.

| Boundary | Executable or compile-checked owner |
| --- | --- |
| Root execution, typed terminal, application children | [application topology example](../examples/application-topology/src/main.rs), [terminal projection tests](../crates/bombay/tests/terminal_projection.rs) |
| `run_with`, lifecycle, shutdown, exact terminal results | [paired execution tests](../crates/bombay/tests/run_with.rs) |
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

A leaf actor uses one Application and returns independent work and cleanup:

```rust
# struct Service;
# #[bombay::actor(message = bombay::behavior::Never)]
# impl Service {}
# let host = tokio::runtime::Builder::new_current_thread()
#     .enable_all().build().expect("caller-owned host");
# host.block_on(async {
let service = bombay::actors::ActorExt::stop_on_shutdown(Service);
let outcome = bombay::Application::new(service)
    .run_with(|application| async move {
        let shutdown = application.request_shutdown();
        assert_eq!(shutdown, Ok(()));
        application.termination().await
    })
    .await
    .unwrap_or_else(|original| {
        drop(original);
        panic!("the application must start");
    });
let (termination, cleanup) = outcome.try_into_completed()
    .unwrap_or_else(|original| {
        drop(original);
        panic!("the work must complete");
    });
assert_eq!(termination, Ok(bombay::prelude::Exit::Normal));
let (_origin, retirement) = cleanup.expect("independent joined cleanup");
assert_eq!(retirement.completion(), Some(&bombay::Completion::Stopped));
# });
```

A no-birth root infers `Never` descendants and `Vec<Never>` child failures.
When root construction needs an exact recipient, establish it through an affine
`ApplicationScope` first. Consuming that scope declares one root and keeps early
endpoints and later clients on the same allocation source. Missing-host,
staging and prepared-setup rejection retain that namespace with the original
inputs. A prepared retry uses the returned scope's `with_topology` method.

```rust
# type Announcements = bombay::behavior::MessageProtocol<bombay::MailAddr, u64>;
# struct Service { ready: bombay::behavior::EstablishedRecipient<Announcements> }
# #[bombay::actor(message = bombay::behavior::Never,
#     sends = { announcements: Vec<bombay::behavior::EstablishedDelivery<Announcements>> })]
# impl Service {
#     fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
#         Ok(bombay::behavior::Actions::cont().send_announcements(
#             bombay::behavior::EstablishedDelivery::new(self.ready.clone(), 42)))
#     }
# }
# let host = tokio::runtime::Builder::new_current_thread()
#     .enable_all().build().expect("caller-owned host");
# host.block_on(async {
let scope = bombay::ApplicationScope::new();
let mut announcements = scope.external::<Announcements>().expect("early endpoint");
let root = bombay::actors::ActorExt::stop_on_shutdown(Service {
    ready: announcements.recipient(),
});
let outcome = scope.with_root(root).run_with(|application| async move {
    let announcement = announcements.receive().await.expect("root ready");
    let shutdown = application.request_shutdown();
    assert_eq!(shutdown, Ok(()));
    announcement.message
}).await.unwrap_or_else(|original| {
    drop(original);
    panic!("original application must start");
});
let (announcement, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
    drop(original);
    panic!("complete original outcome");
});
assert_eq!(announcement, 42);
let (_, retirement) = cleanup.expect("independent joined cleanup");
assert_eq!(retirement.completion(), Some(&bombay::Completion::Stopped));
# });
```

For child graphs, one `topology!` declaration selects the closed actor-indexed
schemas through `Application::with_topology(root, declaration::Topology::new())`.
The actual actor, staging failure and descendant custody are inferred by
`run` and `run_with`. Generated products and exhaustive enums permit borrowed
inspection and consuming matches; unmatched sums remain complete owned values.
The [topology tests](../crates/bombay/tests/application_topology.rs) and
[existing custody journeys](../crates/bombay/tests/application_terminal_custody.rs)
execute these paths. No default policy or dummy capability fills an unused lane.

Finite singleton chains infer their actors from the constructed root and its
births. For example, a fixed supervisor declares only its relationships and
external protocols:

```rust,ignore
bombay::topology! {
    supervisor_application {
        root { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
        hosts { status: Status; capability: Capability; }
    }
}
```

The [executed supervisor](../crates/bombay/tests/application_topology/supervision.rs)
uses the owning constructor directly, without composed return aliases. For
recursive classes with named child roles, `node for Node { ... }` names the
nominal actor owning those roles and derives its actual wrapped type. Cold
members or multiple root roles use `root: Root { ... }`; the constructor still
supplies root decorations. These anchors contain domain types, with no spaces,
structural positions or retirement enums. Rust rejects inconsistent incoming
actors, forged roles and missing report lanes. The
[recursive tests](../crates/bombay/tests/application_topology/recursive.rs)
exercise both wrapper orders and complete failure/retirement custody.

Cold classes also infer their actor types from `.child(role, actor)`:

```rust,ignore
bombay::topology! {
    timed_application {
        root: TimedParent {}
        members {
            Timer(TimerOutside) => timer;
            Shutdown(ShutdownOutside) => shutdown;
        }
        timer {}
        shutdown {}
    }
}
```

The [native timer journey](../crates/bombay/tests/native_timer_retirement.rs)
constructs both actual wrapper orders and consumes every original retirement
with this declaration. [Cold-member tests](../crates/bombay/tests/cold_member_retirement.rs)
cover equal actor types with distinct logical hosting, nested descendants,
recursive creation and original initialization-refusal custody. Recursive cold
classes use the same nominal role-owner rule; their wrapped types stay inferred.

Entity families are independent hydrated roots in that same declaration:

```rust,ignore
bombay::topology! {
    entity_application {
        root: Root {}
        hosts { accounts: Account; profiles: Profile; }
        families { account: Accounts; profile: Profiles; }
        account {}
        profile { worker(ProfileChildrenWorker) => worker; }
        worker {}
    }
}
```

The definitions supply their actual `Behavior`; protocol hosts remain explicit
logical address-space choices. A definition with children selects the generated
`ProfileRetirements` as `Terminal` and `Vec<<Self::Terminal as RetirementSchema>::Failure>`
as `ChildFailures`. Its existing hydration and consuming retirement callbacks
retain those exact values. The [two-family journey](../crates/bombay/tests/entity_application.rs)
executes simultaneous hydration, refusals, reactivation and child retirement.
Declaring a family class starts no task; `.entity_family(role, definition, directory, capacity)`
supplies its real policy and owned definition.

The successful outer result owns `ApplicationOutcome`. It does not imply a
completed actor or erase an initialization refusal. Its closed alternatives
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

Use `work_output()` to borrow completed work and `cleanup()` to borrow the
independent cleanup Result. `try_into_completed()` consumes a completed outcome
into `(output, cleanup)` even when cleanup failed; another phase returns the
entire original outcome in `Err`. These operations require no `Clone`, `Debug`
or `Send` bound on caller values and do not classify actor success. The
[counter](../examples/counter/src/main.rs) exercises owned consumption;
[result tests](../crates/bombay/tests/application_outcome.rs) cover every phase,
move-only values, cleanup failure and actual startup refusal.

No supplied Work uses `Option<Never>` with `None` for both Work and output.
`Completed { output: None }` records an acquired startup grant without supplied
Work. For supplied Work, `output` is the original bare `WorkFuture::Output`. A user
`Err(error)` or `None` remains that exact output. The framework adds no `Some`
layer and cannot fabricate absence when the supplied output type is unit.

For non-family constructors, cleanup is
`Result<(RootOrigin<Owner>, ActorRetirement<Actor, Terminal, ChildFailures>),
ApplicationCleanupError>`. `PublicationClosed(RecvError)` retains the original
cleanup-publication receiving error. `TaskFailed(JoinError)` retains the actual
cleanup task error. The root's `ActorTaskFailed(JoinError)` variant separately
owns the native actor failure. Preserve these distinct owners; do not infer
absent actors or manufacture a `JoinError` from a closed channel. `Terminal` is the descendant destination.
The root stays raw. A derived application sum receives its exact owned pair
with `(origin, retirement).into()`. `ProjectTerminal<Origin, Actor>`
is the native interpreter port: its associated `Descendants` selects the child
retirement tree before launch, independently of the projected report. Explicit
native policies retain their original task context and failure causes.
`ApplicationOutcome` is an explicit `bombay` root import, not a prelude export.

Advanced custom hosting supplies `RootRegistration` for its physical root
namespace and `ApplicationTopology` for complete custody. A custom accessor's
failure remains an actual setup failure: receiving preserves the original
callable and prepared actor for retry in that same namespace. Custom native
child presentation uses the existing `NativeChild` and sealed
`EstablishNativeChild` interpreter ports; it must retain the complete retirement
and actual native task context. The [completion and receiving tests](../crates/bombay/tests/completed_application_receiving.rs)
exercise panicking host access, distinct actor/caller executors, original
allocation return, and partial family-cleanup cancellation. Ordinary application
assembly uses the generated topology above.


Use `ActorRetirement::completion()` to borrow the normal stop or exhaustion
reason. Failure and owner cancellation return `None`; inspection leaves the
complete retirement untouched. A normal actor completion can coexist with
child or late-joined capability failures, which remain in `custody()`.

Use `ActorRetirement::behavior()` to borrow retained actor state before
choosing how to consume the complete retirement. State is available in normal
completion and failure alternatives; its presence does not imply success.
`ActorTaskFailed`, `Panicked` and `Cancelled` have no retained actor and return
`None`. Inspection preserves every cause, effect, unread message and child
value in the original retirement. Existing matches remain appropriate when
your decision needs the exact phase or owned fields.

`ActorRetirement::custody()` borrows the common `ActorCustody` product: actor
state, unread control/user messages, descendants, child failures and acquired
runtime values. Allocation refusal happens before this product exists; its
original actor is still available through `behavior()`. Executor outcomes
without returned actor state also have no custody product. Presence never
certifies successful completion.

Migration: the 20 alternatives with runtime custody now contain `custody`
instead of repeating those 14 fields. Match `Completed { custody, completion,
.. }` to consume the original product, or the exact failure variant to retain
its primary cause. Read or move `custody.behavior`, `custody.user` and
`custody.descendants` directly. Initialization progress, active interpretation,
source progress, settlements and phase-specific errors remain in their original
variants. All 24 alternatives and all owned values are preserved.

Supplied Work receives the activated application's concrete handle:

```text
let outcome = Application::new(service.stop_on_shutdown())
    .run_with(|application| async move {
        let reply = boundary(application.root()).await;
        let shutdown = application.request_shutdown();
        (reply, shutdown)
    })
    .await;
```

Returning from Work does not request shutdown. The root's concrete Behavior
must accept or transform `ShutdownRequested`; topology does not select policy.
`ApplicationHandle::client()` creates a root-bound client with typed reply
receiving; its protocol can be inferred from the domain reply capability.
`sender()` creates a one-way root sender without a reply declaration.
`request_shutdown()` and `termination()` delegate to the existing lifecycle
owner. Request acknowledgement, observed termination and joined cleanup remain
distinct. `lifecycle()` remains the explicit projection when another consumer
needs only lifecycle authority. Clients and senders gain no such authority.

```text
let mut client = application.client()?;
client.send(CounterMessage::Increment).await?;
client.send(CounterMessage::Read(client.recipient())).await?;
let reply = client.receive().await;
```

The [counter](../examples/counter/src/main.rs) and
[FIFO pool](../examples/worker-pool/src/main.rs) execute this root-bound path.
[Client regressions](../crates/bombay/tests/application_clients.rs) preserve
real sender identity, move-only rejections, accepted replies and both wrapper
orders; compile denials prevent cloned receiving, forged construction,
wrong-root messages and lifecycle authority.

The root is the default target. Select another exact actor recipient or stable
Entity reference once with `application.client_to(target)` or
`application.sender_to(target)`. The same client methods apply; a selected
target keeps its own exact-incarnation or Entity admission contract. If endpoint
allocation fails, both constructors return `(original_target, error)` for
recovery or retry. The [Entity example](../examples/entity/src/main.rs) uses:

```text
let account = application.entities(AccountsRole).entity(account_id);
let sender = application.sender_to(account)?;
sender.send(AccountCommand::Deposit(40)).await?;
```

For genuinely shared reply receiving across targets, `application.external()`
establishes the existing affine `ExternalActor` directly; each `caller.send(&target,
command)` makes its target choice explicit. `ActorInterface<Api>` remains the
named product for exported capabilities outside the application callable and
provides the same `client_to`/`sender_to` constructors. Exporting a target never
exports its lifecycle authority. No universal request/reply convention is inferred
from arbitrary domain commands.

For cancellation or unwind recovery, use `execute` or `execute_with` to obtain
separate execution and receiving futures. The paired constructor captures its
entered host immediately and stays cold until execution is polled. Keep and
poll receiving after dropping execution. A one-future `run` or `run_with` owns
both futures; dropping it also surrenders the receiver. Original native setup
or Work panic payloads unwind to the caller. A Ready output is published before
Work-future disposal, so retained receiving can still acquire it if disposal
panics. A recovered Prepared product is actual prepared input, not a fabricated
cold application.

All entry points select descendant custody from the application's topology.
Use `execute_with(work)` and `execute()` without retirement type arguments.
The same selection applies to `execute_axum(address, router)`,
`run_axum(address, router)`, `execute_with_entities(work)` and
`run_with_entities(work)`. The [entry-point regressions](../crates/bombay/tests/application_outcome.rs)
exercise local non-Send work, cold cancellation/retry, initialization refusal
and original-router recovery on HTTP bind refusal.

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
extraction/responses; Bombay adds no projected aggregate `AxumRunError`.

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
markers. Explicit hosting is selected through `Application::with_topology`.
Library authors reach the exact Behavior-owned items through `bombay::behavior`; advanced runtime composition uses explicit
`bombay::{Application, ActorSpace, ActorSpaces, Hosts}` imports; reusable template
families are browsable through `bombay::atomic`,
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
`Application<Root, Spaces, Members, Families>` retains the spaces while giving only the root to the
Behavior Driver.

A single-protocol root does not declare that product. Bombay supplies its one
concrete protocol space directly:

```text
let outcome = Application::new(OrderBook::default().stop_on_shutdown())
    .run_axum(address, |application| {
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

Application::new(root).run().await
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

Use the direct Application clients above inside Work. When a transport must
retain a named product of exported capabilities, construct an `ActorInterface`:

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

Explicit hosting uses `Application::with_topology` with deliberately imported
`bombay::{ActorSpace, ActorSpaces, Hosts}`. `Application` remains in the ordinary
prelude. One declared topology deriving hosting and retirement remains the target
for complex graphs.

Users never manually construct or implement:

- a Bombay System, Guardian, Driver, Environment, or actor execution object;
  selecting a caller Tokio host or configuring its ordinary Builder is explicit;
- mailboxes, channels, address claims or leases; explicit topology currently
  supplies `ActorSpace` values, pending generated hosting;
- observation publishers, timer queues, executor tasks, or child-task owners;
- creation, delivery, observation, timer, report, or shutdown interpreters;
- supervision loops, worker scheduling, restart machinery, or shutdown
  traversal;
- `SendEffects`, `SendInput`, creation dispatch, or product-routing
  implementations already supplied generically.

## Stable Entity references

An ordinary application names asynchronous reconstruction and exact lifecycle
fact consumers once in a nominal definition:

Directory limits are validated before any root, child, family or runtime port
transfers. Use `DirectoryConfig::default()` or
`DirectoryConfig::new(shards, activation_waiters)`; rejection returns both
original limits in `DirectoryConfigError`. The limits are immutable. Declaring
a family and constructing its directory/runtime then have no configuration
error branch.

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
    // and exact final-retirement facts here.
    # /* ... */
}

let application = Application::with_topology(root, spaces).entity_family(
    AccountsRole,
    Accounts,
    DirectoryConfig::default(),
    EntityCapacity::new(concurrent_hydrations, residents),
);

let application_receiving = application.run_with_entities(
    |application| async move {
        let accounts = application.entities(AccountsRole);
        let account = accounts.entity(account_id);
        let caller = application.sender_to(account)?;
        caller.send(AccountCommand::Deposit(amount)).await
    },
).await;
```

`application_receiving` is a `Result`: success retains
`(work_outcome, root_receiving, family_receiving)`; host-entry rejection returns
the original application, Work callable, and actual entered-host error.

`EntityRef<Accounts>` retains the logical account ID, not an actor address or
incarnation. The same value therefore survives passivation and replacement.
Its definition is nominal: two families remain non-interchangeable even when
they select identical ID and message types. `Entities<D>` is only the
cloneable admission/passivation receptionist; `Application` retains family shutdown
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
`execute_with_entities`. The result keeps Work outcome, root receiving, and the
recursive family receiving product separate. Work cleanup has a unit normal
result or a distinct `ApplicationCleanupError` for its receiving/task failure. The independent
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

The ordinary application API contains no manually authored `HList` or positional
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
identity. Dynamic-supervisor outcomes retain the established stable-proxy capability,
and pool assignments carry their completion recipient. The fixed-supervisor
and FIFO/keyed-pool runtime tests exercise ordinary delivery, proxy input,
complete rejection custody and joined retirement. Dynamic application assembly
remains open because construction needs its durable lifecycle capability first.

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
