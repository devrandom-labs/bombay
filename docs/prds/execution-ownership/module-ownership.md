# Module ownership research

**Revision note (2026-10-01):** ARC-001 removed `ShutdownControl`,
`TypedShutdownControl`, `ShutdownSignalRejection`, and `ActorRef.shutdown`.
The table below is a dated source inventory, not the current module map. See
`../../module-boundaries.md` and `../../open-design-ledger.md` for current
ownership and selected dependencies. ARC-002 removed the private external
Address space and lease from `actor_interface.rs`.
ARC-012 removed `ignore_publication` and moved terminal-report selection to
an owned action transaction with one retirement handoff. The dated table below
is not a current source-symbol inventory.
ARC-011 also shared root/owned spawn setup and moved activation-task
settlement into the spawned actor task. Its current source ownership is in
`../../module-boundaries.md`.
ARC-020 later renamed the private `Incarnation`/`IncarnationOutcome` source to
`ActorExecution`/`ActorExecutionOutcome` under `actor_execution.rs` and
`actor_outcome.rs`. The snapshot symbols and hashes below remain historical.

Status: **DG-MODULES open.** This is an evidence map for WP-CONTRACT, not a
frozen migration plan or authority to move files. It covers the named
production symbols and implementation groups in the affected Bombay crate
files. A method-level inventory, accepted DG-TASK/DG-SHUTDOWN/DG-OBSERVATION
interfaces, exact visibility proof, differential extraction, and independent
review are still required before WP-LAYOUT.

## Snapshot and source boundary

The current `Cargo.lock` selects Behavior/Behavior Actors 0.17.0 and Behavior
Macros 0.12.0 (selected-release documentation names revision
`435560ce7bea8ad3330ee2d42e5034f837a80602`), Address 0.2.0,
Communication 0.1.2, Tokio 1.53.1, and Timers 0.1.0 patched to
`13e884da7ab41781f52337b0038060e375b00ee0`. Bombay's private Observe
implementation remains where it is. The exact Behavior revision's complete
`AGENTS.md`, Bombay instructions, ledger, Driver law and strategy, capability
contract, module map, public exports, consumers, macros, primitives and
relevant tests were inspected. The [task record](task-custody.md) has the
lock/Driver/launch hashes and dependent cancellation issues.

Affected source hashes at this research snapshot:

| Source | SHA-256 |
| --- | --- |
| `lib.rs` | `d3e7b899a4ce9a5ad9df9d3a22b36fb9f13c7be3c2a06da4e49bf4771952abff` |
| `application.rs` | `288b197287a0faa25de721823c4a499be8633cc4d5028b6af9a2b7a1ddcbee12` |
| `actor_interface.rs` | `0fb33e84dd022917b8e68fdef1ccf672c8c038ba8e5307d7cf4dae7406209227` |
| `interpret.rs` | `eb78bdf24b814c7961cd4f05392d8df9021f2431b23dfb01ba2ea4385b9038b3` |
| `topology.rs` | `929ae4f5d10584a5c0d72b36e2545152e6aa71749a6162128e920d3b5f686c7b` |
| `observation.rs` | `317bbebbbc390e884c5352ece8a5324c8f0042429a230c365f324c19ebaf938e` |
| `reports.rs` | `ff2049f63ccc01e2c61cfb9e533733687395cfb1c3551ee418b4421b879559b6` |
| `time.rs` | `a10ac7e8d6d5396617aaf8c7932e265b0055dfe4992e15aecc98986ffd9ff7b2` |
| `termination.rs` | `7abe041b720dcb48c628617819d9218d33024325d5d0ff90ac5884f62d0e182b` |
| `outcome.rs` | `bbdd26da46e9a199b73d4be044ffceff750ed609279135e186937914050153ea` |
| `retirement.rs` | `735c59a3b0d290419e7db9b9b899d21733c58a0574488ac7e0079d4a50206456` |
| `address.rs` | `77faf184c3f02a346ec5982d9b989b75a50f1c85747560dbf7c6fdf2fce7963d` |
| `entity/bombay.rs` | `fb7b4b0fdb29ac53feebfb256221dcfe3679f445ff21b707ee40c97bd23424d8` |
| Bombay macro source | `c4bc12f44eea9b60e1191c790b1c51d2b0e6d787e154cb608545e318b30b73fb` |

The proposed directories are the parent PRD's bounded hierarchy. A destination
below is an ownership hypothesis until its semantic gate accepts the contract;
it is not a mechanical rename instruction. Associated methods and impls stay
with their owning type except an existing `InterpretItem` implementation whose
effect lane is explicitly assigned below.

## Application and external interface symbols

| Current source and named symbols | Owning responsibility / proposed placement | Evidence and unresolved interface |
| --- | --- | --- |
| `application.rs`: `Application`, `new`, `child`, `root`, `into_parts` | `application/mod.rs` declaration | Pure root and role-indexed child values. No Tokio/Address allocation while authoring. Preserve root public path and compile denials for non-Behavior children. |
| `application_runtime.rs`: `App`, `new`, `entity_family` | `application/mod.rs` advanced declaration | Holds root, static Spaces and family definitions. `entity_family` validates the directory shard count and receives already validated capacity; no live Entity task at declaration. Retain deliberate advanced surface. |
| `application_runtime.rs`: `RunError`, `App::{run,run_with,run_with_entities}`, `Application::{run,run_with}`, `LaunchSystem`, `DirectRoot`, `DeclaredRoot`, `RootProjection` | `application/execution.rs` for lifetime/entry, with static role projection split below | Seven runtime builders are duplicated; `launch` already uses `launch_with`. DG-API chooses signatures/error equations and whether `RootProjection` remains the private static seam. `LaunchSystem` currently also constructs actor capabilities; that construction must move to local ownership without a second launcher. |
| `application_runtime.rs`: `AxumRunError`, `LaunchSystem::launch_axum`, both `run_axum` methods | `application/http.rs` | Bind before activation and router once after it; serving failure retains source plus exact terminal. Consume common application execution. DG-API and DG-TASK gate panic/drop cleanup. |
| `application_runtime.rs`: `ApplicationHandle`, `ApplicationLifecycle` and their methods | `application/interface.rs` | Handle owns root reference, address allocator and family receptionists; lifecycle projects restricted root shutdown/termination authority. Compile denial: messaging-only `ActorRef` has no public shutdown method. |
| `actor_interface.rs`: `ActorInterface`, `ExternalTarget`, sealed target impls, `ExternalActor`, `ExternalActorError`, external receive/lease Drop | `application/interface.rs` external capability owner | Interface holds explicit exported product, not inferred topology. `ExternalActor` owns affine receive and lease; do not collapse into `ActorRef`. It depends on local Admission/ActorRef, Address and Communication. Keep public paths and `ExternalActor` non-Clone denial. |
| `actor_interface.rs`: `ExtractLocalEndpoint` | Local endpoint interpretation, likely `local/endpoint.rs` | Used both by public ExternalTarget realization and local established-delivery interpretation. Moving to application would force local effects to import application. It performs no application lifecycle policy. |
| `application_runtime.rs`: `RootBirthNode`, `ApplicationProduct`, `ApplicationBirthNode`, `RootOriginProduct`, `AppendApplicationBirths`, `StageApplicationChildren`, `DeclaredApplicationChildren`, `stage_application`, `ApplicationBehavior`, `ApplicationDefinitionError`, `ApplicationStagingError`, `ComposeApplication` | `application/composition.rs` | Declared topology becomes one Behavior initialization/birth sequence. `ApplicationBehavior` owns real semantics and stays. `ApplicationDefinitionError` is in its public associated `Behavior::Error`; public path/diagnostic review is mandatory. |
| `application_runtime.rs`: `StructuralOrigins` | Common local origin marker candidate; **placement unresolved** | Entity imports and uses this marker as its own native actor origin, so placing it only in `application/composition.rs` preserves a wrong-way Entity -> application edge. Determine whether its structural origin law belongs below application before moving it. |
| `application_runtime.rs`: `ApplicationOrigins`, `RootTerminalOriginMapper`, `NoRootTerminalOrigins`, `RootTerminalOrigin`, `AppendedOrigins`, `ProjectAppendedChild`, `ProjectDeclaredChild` | Application role mapping; **projection call placement unresolved** | Their `task` methods ultimately call `ProjectedTask::project`, which spawns a Tokio task. Wholesaling them into pure `application/composition.rs` would make that module perform runtime work. Moving them into local effects risks local importing application topology. Keep declarations and executable application-specific impls separate if the contract supports it. |
| `application_runtime.rs`: `ProjectChildTerminal` | Existing generic projection seam may belong under `local/`; **unresolved** | Creation interpretation already requires this trait for both local child and application-declared origin shapes. Retain the existing trait as a candidate, with application-specific impls in application execution and generic structural implementation under local if evidence accepts it; do not invent a new trait solely for a file move. |
| `application_runtime.rs`: `DEFAULT_USER_CAPACITY`, `routed_creation` | Capacity default likely local ingress; rejected creation reconstruction local creation interpretation | The constant is used for root/children. Preserve 1024 and its consumers. `routed_creation` reconstructs a rejected, owned birth request; its exact custody keeps it with birth interpretation, not application entry. |

## Local actor, effect and termination symbols

| Current source and named symbols | Owning responsibility / proposed placement | Evidence and unresolved interface |
| --- | --- | --- |
| `launch.rs`: `SpawnError`, `OwnedActor`, `RootActor`, `OwnedTask`, `ProjectedTask`, `ActivationRejection`, `Activation`, `ActivationPublisher`, `spawn_owned_with`, `spawn_root_with`, `spawn_owned_entity_with`, `spawn_owned_with_mode`, `finish_owned_task`, `settle_local_outcome`, `startup_failure`, `complete_activation` | `local/execution.rs` for startup/task custody; terminal conversion/publication may belong to sibling `local/termination.rs` | Root/child bootstrap duplicates setup; returned control authority differs. DG-TASK/DG-PROJECTION must select Drop/cleanup and task count first. `SpawnError` typed content is consumed by application and Entity; retain its exact variants. |
| `launch.rs`: `LocalRetirement` and its `Retirement` impl | `local/termination.rs` if it remains the one terminal handoff | Owns activation rejection and coarse terminal publication after Driver destruction. Its sync Drop path is not an async join. The accepted task model may change construction timing. |
| `launch.rs`: `ActorSpace`, `LocalAddresses` | `ActorSpace` remains public advanced local host type, likely `local/mod.rs`; delete synonymous `LocalAddresses` only after DG-WRAPPERS proof | Macro-generated `Hosts` impl and external compile fixtures name `bombay::ActorSpace`. `LocalAddresses` currently aliases the identical value and is private. |
| `local.rs`: `Admission`, `AdmissionClosure`, `LocalIngress`, `StandardIngress`, `EntityIngress`, `EndpointMailbox`, `IngressMode`, `LocalInbox`, `collect_retired_ingress` | `local/ingress.rs`; endpoint representation shared with `local/endpoint.rs` | Admission owns affine Communication owner. Entity fence and standard input have different protocol semantics. `LocalInbox::Drop` drains. Do not create a second mailbox or expose admission owner publicly. |
| `local.rs`: `ActorRef`, `SendError`, `ShutdownControl`, `ShutdownSignalRejection`, `TypedShutdownControl` | `local/endpoint.rs` | ActorRef is public messaging reference; shutdown storage is a current `dyn` violation and DG-SHUTDOWN must select a static authority. Its Clone and weak admission must remain. Endpoint has no public shutdown method. |
| `local.rs`: `LocalEnvironment`, `ActiveLocalEnvironment`, `LocalResidual`, `Publication`, `ignore_publication` | `local/environment.rs` | Prepared/active phase relation and one consuming Engine port stay together. Retired residual retains ingress, settlements, capability tasks and descendants. Keep `pub(super)` among local children where possible. |
| `local.rs`: `ActivationTasks`, `CapabilityRetirement`, `OwnerCancellation`, `CommitActions` | DG-TASK decides between `local/execution.rs` and local effect/environment placement | `ActivationTasks` currently owns a Tokio JoinSet; `CapabilityRetirement` is the typed transfer from interpreter to Environment; `OwnerCancellation` is an affine forced-retirement fact. Splitting these by existing line number before custody decision would cross the same authority. |
| `interpret.rs`: `ActionSettlementOf`, `InterpretedActionSettlement`, `RetireCapabilities`, `ActionInterpreter` | `local/effects/mod.rs` | One ordered Behavior-owned action traversal and action-scoped terminal-report transaction. Retain until differential DG-WRAPPERS evidence proves an equivalent direct composition. The runtime does not own a second effect algebra. |
| `application_runtime.rs`: `ApplicationCapabilityInputs`, `ApplicationCapabilities`, `NoParent`, `RootCapabilities`, `RootInterpreter` | `local/effects/mod.rs` concrete actor capability composition | `ApplicationCapabilities` is instantiated for roots, children and Entity actors; the current name and file misstate ownership. `entity/bombay.rs` imports it from the runner today. Any rename is chosen at DG-MODULES, not from compiler pressure. |
| `application_runtime.rs`: `SourceAdmission`, `EstablishChild`, creation request interpretation (lines ~1478–1715) | `local/effects/creation.rs`, using `local/children.rs` | Creation interpretation installs fresh child, records exact binding and returns typed acceptance/rejection. Preserve creation-before-send order, one initialization and rejected request contents. `worker_preparation.rs::settle_worker_preparation` may be colocated as an owning creation leaf only after proving its semantic lane. |
| `application_runtime.rs`: logical, established, child and source delivery implementations (lines ~1717–1894) | `local/effects/delivery.rs` | Select genuine external endpoint or typed child binding; exact rejected payload recovery. No application-specific `SendAlgebra` or duplicate router. |
| `application_runtime.rs`: timer request interpretations (lines ~1895–1925 and 1943–1974); `time.rs`: `LocalTimers`, `TimerError` | `local/effects/timers.rs` | One TimerQueue shared by Environment acquisition and interpreter. Preserve generation, replacement, order and overflow. No timer task. |
| `application_runtime.rs`: Entity admission leaf (lines ~1926–1942) | Entity-owned interpreter impl candidate; exact placement still open | The current `InterpretItem<EntityAdmission<D>>` implementation bounds `D: EntityDefinition` and `D::Hosts: NativeEntityHost`, creating local effects -> Entity while Entity already imports local capabilities. Its body only calls `request.interpret(self.address).await` and returns accepted. Test whether the impl can live in Entity with narrow local-origin address access, preserving coherence and the request's exact semantics; no new adapter trait is justified by the move alone. |
| `application_runtime.rs`: creation/peer/child observations and established observation interpreter (lines ~1975–2186, 2241–2373); `observation.rs`: `FactSource`, `InjectFact`, `PendingFact`, `FactState`, `FactQueue`, `ObservationError`, `LocalPeerObservations` | `local/effects/observation.rs` | Ordinary peer/child facts currently poll in Environment; established ID observations use a task/map. DG-OBSERVATION chooses one relationship authority. `Fact*` vocabulary conflicts with selected Behavior instructions; rename by domain law after gate. |
| `application_runtime.rs`: parent/terminal/shutdown-plan report leaves (lines ~2187–2240); `reports.rs`: `TerminalReportTransaction`, `LocalTerminalReports`, `LocalParentReports`, `ParentReporting` | `local/effects/reports.rs`, with termination selection owned by `local/termination.rs` | Terminal report selection is action-scoped; parent reports use typed control ingress. `LocalTerminalReports` may be forwarding-only; DG-WRAPPERS selects retention/deletion. |
| `application_runtime.rs`: established shutdown interpreter (lines ~2374–2409) | `local/effects/` leaf colocated with endpoint authority after DG-SHUTDOWN | It consumes Behavior Actors' typed shutdown request; must not introduce an alternate lifecycle channel. File choice follows the selected static capability. |
| `application_runtime.rs`: `RetireCapabilities` impl (lines ~2410–2453) | `local/effects/mod.rs` | Consumes observation registrations, child bindings and capability tasks once; cancellation/ordering requires DG-TASK and DG-OBSERVATION. |
| `child_bindings.rs`: `RuntimeChildBindings`, `ChildBindings`, `OccurrenceBindings`, `NoChildBindings`, `ChildBinding`, `CreationBinding`, `ChildBindingAt`, `ChildBindingAtCursor`, `NestedChildBindings`, `NestedBindings`, `RetireChildTasks` | `local/children.rs` | One closed occurrence-local product and creation-order child custody. `OccurrenceBindings` is an identical synonym candidate under DG-WRAPPERS. Keep occurrence distinction and exact terminal order. |
| `incarnation.rs`: `Incarnation`, `Terminal`; `outcome.rs`: `IncarnationOutcome`; `retirement.rs`: `Retirement` | `local/execution.rs` for one Driver run/guard; `local/termination.rs` for exact classification and retirement contract | These are presently public only inside private modules and crate re-exports. Selected Behavior instructions ban `Incarnation` as an architectural identifier; Bombay docs use it normatively. PRD requires a recorded naming conflict resolution, not incidental rename. |
| `terminal.rs`: `LocalOutcome`, `ActorOrigin`, `ProjectTerminal`, `ActorRetirement`; `termination.rs`: `TerminalReportDisposition`, `TerminationSelection`, `TerminationPublication`; `local.rs`: `Termination` alias | `local/termination.rs`, with action-report selection possibly in `local/effects/reports.rs` | Exact typed terminal differs from coarse `Result<Exit, Crash>`. Public `ActorOrigin`, `ProjectTerminal`, `ActorRetirement` are named by macro expansion and application tests. Preserve root re-exports. |
| `topology.rs`: `Hosts`, `HostedActorSpaces`, `ResolveLogical` | `local/mod.rs` or a substantive local host child, pending DG-WRAPPERS | `Hosts` is public advanced static protocol proof generated by `ActorSpaces`; `HostedActorSpaces` adds logical resolve policy. Neither may be deleted merely because it has few fields. |
| `address.rs`: `MailAddr`, `ApplicationAddresses` | root `address.rs` as in PRD | `MailAddr` implements Behavior `EndpointAddress` using `ActorRef`, so this file has a type-level local-endpoint dependency. Do not claim a wholly actor-independent address module; `ApplicationAddresses` owns the one non-wrapping allocator. |
| `worker_preparation.rs`: `WorkerPreparationStart`, `WorkerPreparationSource`, `settle_worker_preparation` | Public source contract remains curated at root; private interpretation belongs in its proven effect lane | This is ARC-010 scope, not an EXEC redesign. A move cannot alter supervisor/pool preparation semantics. |

Internal test modules in these files stay with their owning mechanism. A
mechanical extraction may move focused tests to the new owner, preserving
their exact assertions and ignored activation failures. No test should move
into an application runner merely because it calls a public `run` method.

## Actual consumers and visibility constraints

The current crate root exports `Application`, `App`, `ApplicationBehavior`,
`ApplicationHandle`, `ApplicationLifecycle`, `RunError`, optional
`AxumRunError`, `ActorInterface`, `ExternalActor`, `ExternalActorError`,
`ExternalTarget`, `ActorRef`, `SendError`, `ActorSpace`, `Hosts`, `MailAddr`,
`ActorOrigin`, `ActorRetirement`, `ProjectTerminal`, and worker preparation
types. Macro expansion names `bombay::MailAddr`, `bombay::Hosts`,
`bombay::ActorSpace`, `bombay::ActorOrigin`, `bombay::ActorRetirement`, and
`bombay::ProjectTerminal` through crate-renaming resolution. These root paths
must remain unless a separately accepted public change updates the macro and
all consumer fixtures.

`bombay::prelude` intentionally omits `ActorSpace`, `Hosts`, Driver and
foundational interpreter traits. `prelude_hides_hosts`,
`prelude_hides_actor_space`, `actor_interface_has_no_lifecycle`,
`actor_ref_has_no_shutdown`, wrong-protocol and wrong-role compile fixtures
are part of the visibility contract. Public examples `entity` and `axum`
use the advanced and HTTP paths; application/terminal tests name the concrete
typed sums. Moving modules may alter diagnostics, so compile-fail stderr and
crate-renaming tests require review, not blind regeneration.

The chief wrong-way source edge is
`entity/bombay.rs -> application_runtime::{ApplicationCapabilities,
ApplicationCapabilityInputs, NoParent, StructuralOrigins}`. Entity also
consumes `launch::{OwnedActor, SpawnError, spawn_owned_entity_with}` and
`child_bindings`; this proves concrete actor hosting belongs below the
application runner. `application_runtime.rs` in turn imports local, child,
observation, timer, report, terminal and topology machinery. The target
dependency direction is application and Entity -> local actor execution ->
Environment/effects/primitives -> Engine/Behavior, with HTTP -> application.
There is a second current edge in the opposite direction: the Entity admission
`InterpretItem` implementation is in the application runtime's concrete
capability product and names Entity's `EntityDefinition` and `NativeEntityHost`.
Its one-line interpretation takes only the local actor address; test impl
coherence and a narrow address accessor before choosing the owner.

Narrow-visibility hypothesis: sibling files under `local/` can share
`pub(super)` values through the local parent. Application execution and Entity
should import only the few explicitly required local entry/terminal types via
`pub(crate)`, while public application values remain curated by `lib.rs`.
The exact list cannot be frozen until task, shutdown and observation owners
are selected; widening all new files to `pub(crate)` to satisfy imports would
fail DG-MODULES.

## Stop conditions before any file move

1. Resolve the static origin/projection seam. `ProjectChildTerminal` currently
   spawns a task from a trait implemented with application role mappings and
   called inside actor-local creation interpretation. `StructuralOrigins` is
   consumed by Entity and must not be stranded in application composition.
   State which module owns the operation, which value carries the role, and
   the narrow interface. Test the existing trait under local ownership and
   application-specific impls under application execution before introducing
   anything new. An application declaration module must not start tasks
   during authoring; local effects must not import an application runner.
2. Resolve `ApplicationCapabilities` naming and its exact local/Entity
   consumers after DG-TASK, DG-SHUTDOWN and DG-OBSERVATION. Test whether the
   Entity admission impl can move to Entity with only local-origin address
   access; preserve Rust coherence and its typed acceptance. Do not create a
   second capability product or a façade that only forwards every lane.
3. Finish a method/impl-level symbol inventory and attach every symbol in the
   affected production files to one owner. Enumerate all cross-sibling
   `pub(crate)` needs, macro-generated paths, public associated types and
   compile-denial fixtures. The grouped map above is not yet that proof.
4. Record DG-WRAPPERS dispositions for aliases and forwarding products;
   resolve the selected Behavior vocabulary conflict with established Bombay
   `Incarnation` wording. Preserve existing public names unless an accepted
   migration proves a change.
5. Run a differential baseline before the first extraction: initialization,
   ordered actions, returned exact values, admission, default capacity,
   child order, Entity family shutdown and HTTP. Keep ignored activation
   regressions visible. Move code mechanically with one integrator; if a new
   semantic interface is needed, reopen its owner gate.

Expected changed source paths for the eventual hierarchy exceed the
repository's 15-file stop threshold. No production/public-type delta or
acceptance command result is selected here. This record adds documentation
only and leaves DG-MODULES open for independent review.
