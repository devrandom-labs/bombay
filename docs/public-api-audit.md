# Retained public API audit

This retains ARC-020's owner inventory and reconciles the EXEC application
surface against published Behavior Core/Actors 0.23.0 and Macros 0.14.0 at
archive revision `d69f992b371c12ab34e73b18e45b8112c90a1508`, Address 0.3.0,
Communication 0.1.3, private Observe, and patched Timers in `Cargo.lock`. It covers Bombay-owned
items reachable from the `bombay`, `bombay::entity`, `bombay::actors`, and
`bombay_engine` roots. Re-exported Behavior and Behavior Actors families retain
their upstream ownership. The [Rust API Guidelines checklist](https://rust-lang.github.io/api-guidelines/checklist.html)
informs names, traits, error behavior, and examples; exact ownership follows
the selected Behavior instructions and [module map](module-boundaries.md).

Each row names its public methods or trait operations, its owner and caller,
the construction invariant, standard trait decision, and the custody or error
behavior. `Debug` on a generic value is conditional on its payload being
`Debug` unless stated otherwise. An omitted `Clone` on an affine value
prevents duplicating its completion, receive, lease, or pending-decision
authority. A value without `Debug` carries an unconstrained domain payload or
opaque runtime port; callers inspect typed variants or consume the value
instead of relying on a lossy rendering. No error or terminal payload is
erased merely to implement a formatting trait.

The root entry paths are compiled by the [application topology](../examples/application-topology/src/main.rs),
[counter](../examples/counter/src/main.rs), [Axum](../examples/axum/src/main.rs),
[supervision](../examples/supervision/src/main.rs), and
[worker-pool](../examples/worker-pool/src/main.rs) examples; the
[public handle trait test](../crates/bombay/tests/public_handle_debug.rs),
[external caller test](../crates/bombay/tests/actor_interface.rs), and
[authoring compile matrix](../crates/bombay/tests/macro_last_authoring.rs) establish narrower
positive and negative boundaries. The Engine surface is exercised by its
[Driver law tests](../crates/bombay-engine/tests/driver_law.rs) and
[terminal custody tests](../crates/bombay-engine/tests/terminal_custody.rs).

## Bombay root and actor composition

| Item and operations | Owner, caller, construction invariant | Traits and custody / failure |
| --- | --- | --- |
| `MailAddr`, `APPLICATION_ROOT`, `From<u64>` | Bombay local address; runtime allocates a non-wrapping address, while advanced hosts/tests may name one explicitly. The number is identity, never endpoint authority. | `Debug`, `Copy`, `Clone`, equality, order, hash; no error payload. |
| `Application::new`, `child`, `root` | Bombay root-first declaration; app author supplies each concrete Behavior and semantic role before launch. | Conditional `Debug`/`Clone`/`Copy`/equality; `child` consumes the old declaration and retains all values. |
| `App::new`, `entity_family`, inherent async `run`, `run_with`, `run_with_entities`, `run_axum` | Advanced Bombay composition; caller supplies a root, closed host product, and explicit Entity policies. One common paired owner drives execution. | Conditional `Debug`; affine application inputs. `entity_family` returns exact `DirectoryError`; async calls return original inputs with `TryCurrentError` or complete phase/Work/cleanup/root/family custody. |
| `Application::execute`, `execute_with`, `execute_axum`; `App` counterparts and `execute_with_entities` | Paired constructor captures a live entered host, retains cold inputs, and separates execution from result receiving. | No erased future or extra wrapper; receiving can outlive dropped execution. Work/output need no `Send` or `'static` bound. Actual actor tasks preserve their owning bounds. |
| `Application::run_blocking` | Caller supplies an ordinary configured Tokio Builder; Bombay rejects entered-runtime use before effects and drives the same async pair after successful build. | Returns original Application and Builder with `BlockingInEnteredRuntime` or exact native `Runtime(io::Error)`. No scheduler enum/default or separate actor loop. |
| `ApplicationOutcome` | Sole public application phase/result sum, explicitly imported from the Bombay root. | `#[must_use]`, no affine Clone; exact cold/partial/prepared/Work/Ready output and actual cleanup errors retained. No-work uses `Option<Never>`; supplied Work and output are bare original values. HTTP cold Work is unit before binding; acquired Work owns Router/Listener. Root remains raw; Terminal selects descendants. |
| `ApplicationBehavior` | Bombay's statically composed root and declared child birth product; only `Application` materializes it. | Implements the exact Behavior algebra; no independent actor contract or exposed constructor. Exact root error is retained by `ApplicationDefinitionError` in its associated Behavior contract. |
| `ApplicationHandle::root`, `interface`, `lifecycle`, `entities`, `passivate_entity` | Live Bombay application; external boundary receives this handle only after activation. Each method projects an existing endpoint, family, or lifecycle authority. | Conditional `Clone`, redacted `Debug`; no equality for live capability identity. Passivation yields closed `Passivation` disposition. |
| `ApplicationLifecycle::request_shutdown`, `termination` | Bombay lifecycle projection; caller holds root control separately from message delivery. | `Clone`, redacted `Debug`; shutdown returns exact `ShutdownRejection`, termination awaits the exact root generation. |
| `ApplicationCleanupError` | Application result receiving owns actual cleanup-publication or cleanup-task failure. | Flat `PublicationClosed(RecvError)` and `TaskFailed(JoinError)` retain the original source; actor native failure remains `ActorRetirement::ActorTaskFailed`. No actor absence is inferred from channel closure. |
| `RunError` | Existing owner of the two actual owned-executor failures. | Flat `BlockingInEnteredRuntime` and `Runtime(io::Error)` variants; Debug/Display/Error and truthful native source. No generic root/staging/terminal aggregate; originals coexist beside it. |
| `ActorRef::address`, `send_from`, `termination`, `established_recipient` | Bombay exact local mailbox reference; runtime issues it only after commit. | `Clone`, redacted `Debug`; no direct shutdown authority. Closed send returns `SendError` with the exact message; termination remains bound to one generation. |
| `InstalledActor` | Bombay's installed endpoint plus typed control authority, used as Behavior's `EndpointAddress::Installed` product. Runtime alone constructs it after binding. | `Clone` for multiple typed interpreters and redacted `Debug`; no public constructor or direct shutdown method. Its generic behavior identity prevents cross-behavior substitution in compile fixtures. |
| `SendError::into_message` | Communication rejection projected through `ActorRef`; caller regains the original payload. | `Debug`, `Display`, `Error`; no `Clone` requirement on the payload. |
| `ActorInterface::api`, `external` | Bombay external-caller product; application explicitly exports an API value and the runner's allocation source. | `Clone` when API is cloneable, redacted `Debug`; `external` returns exact allocation error before issuing any recipient. |
| `ExternalActor::address`, `recipient`, `send`, `receive`, `close_admission` | Bombay's allocated external actor; caller owns the sole receiver and may clone only its recipient. | Redacted `Debug`, intentionally no `Clone` ([denial fixture](../crates/bombay/tests/compile/fail/external_actor_receiver_is_affine.rs)); send returns the destination's exact rejection, receive returns the truthful origin. |
| `ExternalActorError` | Bombay external allocation failure. | `Debug`, `Display`, `Error`; exact `AllocationRejection` source. |
| `ExternalTarget::send_from` | Sealed Bombay delivery port for an exact actor recipient or Entity reference. | No object erasure; associated message and error preserve destination custody. Caller uses `ExternalActor::send`. |
| `ActorSpace<P>` | Bombay's protocol-closed alias for Address's concrete space of exact `ActorRef<P>` endpoints; advanced topology caller supplies it. | Inherits Address's traits and claim/lease rules. The alias fixes protocol/address/endpoint relationships; it adds no wrapper or alternative registry. |
| `Hosts<P>::space` and `ActorSpaces` derive | Bombay static host proof; advanced application supplies explicit `#[actor_space(P)]` fields. | The derive emits only `Hosts<P>` impls. Wrong, missing, and duplicate hosts fail compilation; no dynamic lookup. |
| `RootOrigin::address` | Bombay exact root source identity, created only by runtime. | `Debug`, `Copy`, `Clone`, equality; no nonce or child conversion. |
| `ChildOrigin::address`, `nonce`, `into_declared_child` | Bombay exact child source identity, created by runtime and converted only with Behavior `ChildRole` proof. | `Debug`, `Copy`, `Clone`, equality; role conversion preserves address and nonce. Wrong role/actor origins fail compilation. |
| `ActorRetirement` | Bombay exact actor terminal sum; runtime selects one variant and retains final Behavior, settlements, messages, descendants, and cause when available. | `#[must_use]`, intentionally no `Clone`; no generic `Debug` that would require or discard unconstrained exact payloads. Consumers exhaustively match owned variants. |
| `ProjectTerminal::project` and `TerminalProjection` derive | Bombay application-owned terminal projection; caller defines its closed sum. | Derive delegates to typed origins and exact retirements; no erased terminal or string-inferred type identity. |
| `WorkerPreparationStart` | Bombay's one first-role preparation choice over upstream worker submissions. | `#[must_use]`, intentionally no `Clone`; source and worker rejections remain distinct owned variants. |
| `WorkerPreparationSource::prepare_first`, `prepare_next` | Bombay effect interpreter port for upstream `PrepareWorkers`; application supplies the concrete source. | Affine mutable source; first attempt can return a complete source rejection, later attempts only a role rejection. The Behavior fold never calls it. |
| `ActorExt::with_stash`, `with_one_shot`, `with_periodic`, `with_deadline`, `with_receive_timeout`, `stop_on_shutdown` | Bombay syntax for existing Behavior Actors `Stash`, timing, and shutdown wrappers; app author supplies every policy. | Each `#[must_use]` method consumes its Behavior and returns the exact upstream concrete type; no Bombay wrapper state or new actor law. |
| `#[bombay::actor]` | Bombay's one accepted application syntax facade; author supplies state and fold. | Delegates to the locked Behavior expansion and preserves its complete typed Actions. |
| `Completion`, `SettlementFailure` | Engine's exact Driver disposition and settlement failure sums, re-exported for terminal matching. | `Debug`, `Copy`, `Clone`, equality; `SettlementFailure` implements `Error`. |
| `testing::InfallibleResultExt::infallible` | Deliberate test-only helper for `Result<T, Never>` or `Result<T, Infallible>`; a pure test caller extracts an inhabited success. | No panic branch; adding an inhabited error makes the call fail compilation. Absent from the ordinary prelude. |

The Entity root and advanced directory are compiled by the
[Entity example](../examples/entity/src/main.rs),
[Entity application tests](../crates/bombay/tests/entity_application.rs),
[public surface tests](../crates/bombay/tests/entity_public_surface.rs),
[Entity error tests](../crates/bombay/tests/entity_errors.rs), and
[Entity compile fixtures](../crates/bombay/tests/compile/pass/entity_public_surface.rs).

## Entity and advanced directory

`EntitySlot::decide` exposes transition evidence to an advanced caller, but
its `SlotDecision`, `SlotEffect`, and `SlotEffectBatch` representations remain
inside the private lifecycle module. The caller can infer the returned value
and inspect its evidence; the
[representation denial](../crates/bombay/tests/compile/fail/entity_lifecycle_representation_is_private.rs)
proves those product names and phase internals cannot be imported. Their rows
below record the internal ownership decision, not three additional public
exports. External effect interpretation goes through the named
`LocalDirectory::interpret` interface.

| Item and operations | Owner, caller, construction invariant | Traits and custody / failure |
| --- | --- | --- |
| `EntityId::new`, `get`, `into_inner` | Bombay stable logical identity; caller supplies a domain key independently of any actor address. | `Copy`/`Clone`/equality/order/hash when the key permits; conditional `Debug`/`Display`. It is a justified newtype that prevents address/key exchange. |
| `EntityDefinition::hydrate`, `activation_failed`, `admission_refused`, `forced_retirement`, `retired` | Application owns one nominal family, reconstruction, and each typed disposition callback. | Trait preserves concrete Behavior, hydration error, terminal, and command types; callbacks consume exact failure or retirement once. No runtime trait object is required. |
| `EntityCapacity::new` | Caller supplies independent nonzero hydration and resident bounds. | `Debug`, `Copy`, `Clone`, equality; no hidden default policy. |
| `Entities::entity` | Application-installed family receptionist; caller binds a stable domain key. | `Clone` shares the family endpoint, not an actor; opaque runtime storage has no equality or generic `Debug`. |
| `EntityRef::id`, `request` | Stable typed family reference; caller may request while no actor exists. | `Clone` retains logical identity; `request` returns a concrete `EntityAdmission` action, never sends from a Behavior fold. |
| `EntityAdmission` | Entity action interpreter owns one actor-originated command. Only `EntityRef::request` constructs it. | No `Clone`/`Debug` requirement on the command; action interpretation reports refusal to `EntityDefinition` with exact command. |
| `EntityActivationError` | Entity definition callback receives resident-capacity, hydration, or exact actor-launch failure. | Deliberately an owned domain outcome rather than `Error`; no lossy generic `Debug` or `Clone` bound on Behavior/terminal. |
| `EntityMetrics` | Family reports fixed counter observations, never authority. | `Debug`, `Copy`, `Clone`, equality; snapshot values are public. |
| `EntityApplicationFamilies` | Sealed static family product produced by `App::entity_family`. | No separate runtime or dynamic map; exposes typed live/shutdown projections to the application. |
| `EntityFamilyAt::select_family` | Sealed role/position selection proof for `ApplicationHandle::entities`. | Static role mismatch fails compilation; no address-based or runtime family lookup. |
| `DirectoryConfig::default` | Advanced caller supplies shard and waiter limits; constructor checks power-of-two shards. | `Debug`, `Copy`, `Clone`, equality, `Default`; invalid shards yield `DirectoryError::InvalidShardCount` before a runtime is built. |
| `DirectoryError` | Directory rejects invalid configuration or exhausted non-reusable IDs. | `Debug`, `Display`, `Error`; ID exhaustion returns the exact command. |
| `EffectInterpreter::start_activation`, `deliver`, `reject`, `enqueue_fence`, `retire` | Advanced host interprets one installed Entity slot decision outside the slot lock. | Concrete typed command, endpoint, and affine lease pass through the port; no dynamic capability table or callback inside a Behavior fold. |
| `LocalDirectory::new`, `with_hasher`, `dispatch`, `activation_succeeded`, `activation_failed`, `cancel_waiter`, `delivery_resolved`, `begin_drain`, `fence_acknowledged`, `force_drain`, `terminated`, `len`, `is_empty`, `interpret` | Entity kernel owns concurrent slot storage and exact ordered interpretation; advanced test hosts and Bombay runtime call it. | Deliberately no `Clone` or equality for the authoritative directory. Every mutation returns an installed decision; `interpret` consumes it, preserving exact rejected commands/leases. |
| `InstalledSlotDecision` | Directory returns one installed successor and effect queue with phase/evidence. | `#[must_use]`; no `Clone` because interpretation is single-owner. Effect payloads remain private until `LocalDirectory::interpret`. |
| `InstalledDispatch` | Directory couples one dispatch correlation to its installed decision. | `#[must_use]`; no `Clone`, exact ID and decision move together. |
| `ActivationId::new`, `get` | Directory allocates one nonzero activation generation. | `Debug`, `Copy`, `Clone`, equality/order/hash; stale generations never become current by address reuse. |
| `DispatchId::new`, `get` | Directory allocates one nonzero command correlation. | `Debug`, `Copy`, `Clone`, equality/order/hash; no zero sentinel or inferred identity. |
| `Refusal` | Entity slot classifies one rejected command. | `Debug`, `Copy`, `Clone`, equality; command stays in its typed rejection. |
| `DrainStage` | Entity slot names the point where graceful drain stopped. | `Debug`, `Copy`, `Clone`, equality; no independent policy. |
| `DrainFailure` | Entity slot carries stage and unresolved reservation count. | `Debug`, `Copy`, `Clone`, equality; exact forced-retirement reason remains in `RetirementMode`. |
| `RetirementMode` | Entity interpreter receives graceful or forced retirement authority. | `Debug`, `Copy`, `Clone`, equality; forced variant contains `DrainFailure`. |
| `EntitySlot::phase`, `activation_id`, `decide`, `decide_all` | Pure Entity lifecycle state; advanced simulations and directory use the same total transition. | Conditional `Debug`; no `Clone` on command/lease storage. `decide` consumes state and returns one `SlotDecision` with successor, complete effects, and disposition. |
| `SlotDecision` | Pure slot transition returns successor, ordered effects, and disposition as one product. | Conditional `Debug`, `#[must_use]`; no `Clone` requirement on command or lease. |
| `SlotEffectBatch::as_slice`, `into_vec`, `for_each` | Pure slot transition owns exact ordered effects before the directory interprets them. | Conditional `Debug`, `Default`, `Extend`, `FromIterator`, `Add`; consuming traversal preserves effect order and payloads. |
| `SlotEvent` | Pure lifecycle input with owned command, endpoint, lease, or observed generation. | Conditional `Debug`; no generic `Clone` on affine payloads; stale and accepted inputs are distinct variants. |
| `LifecyclePhase` | Compact observation of Entity slot phase. | `Debug`, `Copy`, `Clone`, equality/order/hash; not a substitute for the owned slot. |
| `LifecycleEdge` | Names one actual phase-changing transition. | `Debug`, `Copy`, `Clone`, equality/order/hash; only `TransitionEvidence::Traversed` carries it. |
| `TransitionEvidence` | Slot's traversed/self-loop/ignored disposition. | `Debug`, `Copy`, `Clone`, equality; never inferred from the next phase alone. |
| `Activated` | Advanced runtime port returns delivery endpoint and exact affine retirement lease together. | No `Clone`; generic endpoint and lease need no `Debug` bound. Directory consumes the lease exactly once. |
| `FenceFailure` | Runtime classifies enqueue versus acknowledgement failure. | `Debug`, `Copy`, `Clone`, equality, `Error`; stage remains explicit. |
| `Passivation` | Native family reports begun, absent, already draining, superseded, or shutdown. | `Debug`, `Copy`, `Clone`, equality; no semantic boolean status. |
| `LocalEntityRuntime::spawn`, `join`, `activate`, `activation_failed`, `deliver`, `fence`, `retire` | Advanced actor host implements exact Entity activation, owned task joins, and fenced retirement. | Associated endpoint/lease/task types preserve static ownership; rejection returns the command and task failure remains typed; no whole-Environment replacement for ordinary users. |
| `AdmissionFailure` | Entity runtime returns exact command with refusal or namespace exhaustion. | `Debug`, `Display`, `Error` conditional on command formatting; no command erasure. |
| `EntityRuntime::new`, `admit`, `shutdown`, `passivate` | Bombay native Entity runtime owns directory and task group; advanced callers may host it. | `Clone` shares the runtime state, not shutdown result. Admission returns exact command failure; shutdown returns `EntityShutdown` after owned tasks join. |
| `EntityShutdown` | Entity task group returns one affine shutdown claim with settled count or exact task failures. | Conditional `Debug`; no `Clone` on owned failures. `AlreadyClaimed` is a distinct closed alternative. |

The universal Driver package retains its actor-independent API. Bombay's
ordinary prelude exposes only `Completion` and `SettlementFailure`; advanced
hosts import the Engine types deliberately. The [Driver law](driver-law.md)
and [test strategy](driver-test-strategy.md) define their exact semantics.

## Engine

| Item and operations | Owner, caller, construction invariant | Traits and custody / failure |
| --- | --- | --- |
| `ActionsOf<B>` | Engine names the locked Behavior `Actions` associated-type product for a concrete closed Behavior. | Structural type alias only; no new effect representation or storage. |
| `Driver::new`, `run`, `receive_run` | Engine owns exactly one Behavior and one prepared Environment. | Intentionally affine, no `Clone`; `run` returns `Result<DriverRetirement, Self>`. `receive_run` borrows outside original-driver/reply slots; incomplete retirement returns the surviving Driver without repeating attempted work. |
| `DriverRetirement` | Engine pairs final Behavior, residual, and disposition without reconstruction. | Conditional `Debug`/equality, `#[must_use]`; exact payloads remain public fields and owned. |
| `DriverError` | Engine preserves Behavior, activation, settlement, interpreter-contract and distinct initialization/transition/host/activation/retirement panic causes. | Conditional `Debug`, `Display`, `Error`; no equality promise. Native panic payloads and typed sources retain their original ownership. |
| `Completion` | Engine records explicit stop, exhausted source, or the exact `RetirementRequested(Request)`. | Conditional `Debug`, `Copy`, `Clone`, equality; a retirement request does not claim stop, exhaustion or completed cleanup. |
| `SettlementFailure` | Engine records rejected, corrupt, or closed source settlement. | `Debug`, `Copy`, `Clone`, equality, `Error`; no accepted prefix is rewritten. |
| `Environment::activate`, `retire` | Advanced host loans prepared original inputs and outside reply slots for activation or rejection retirement. | Unit-returning borrowing futures preserve surviving inputs and acquired replies through producer disposal; only an acquired complete reply with exhausted original inputs establishes completion. No ordinary ingress before publication; initialization source progress precedes publication. |
| `ActiveEnvironment::next`, `next_source`, `apply`, `offer_next`, `publish`, `retire` | Advanced host supplies live event, typed Actions interpretation, publication and retirement. | Ordered source custody and exact retirement requests remain distinct. `apply`, `offer_next` and `retire` loan original inputs/replies; retirement retains surviving input and partial progress outside its disposable future. No second Driver policy or fabricated completion. |

`#[doc(hidden)]` on Engine's re-exports limits ordinary rustdoc visibility;
it does not erase the advanced port. `bombay::prelude` deliberately omits
foundational `Behavior`, Engine, structural effect paths, and topology proof
traits. The [prelude compile fixtures](../crates/bombay/tests/prelude_levels.rs)
prove those import levels. No extra public wrapper, alias, registry, or
actor contract was added by this audit.
