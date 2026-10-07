# Module ownership

The accepted pre-extraction map53edb384 binds22 original owners and713
production declaration/method entries, with238 true impl/trait items and55
signature-bound opaque `impl Future` fragments. The one common execution
kernel retains19 generic axes and7 value parameters. The approved scope
permits17 new module files and removal of nine former owners, with no new
semantic interface or type.

The extraction is installed. Independent source and count reviews accept its
conservation bridge cfb2027f and complete cfg supplement ca894640. The cfg
mapping has20 destination entries for19 distinct original source ranges:
`installed_shutdown_contract` is partitioned between its two actual private
owners. Three later intact regressions are placed with their private mechanisms
in `local/execution.rs` and `local/endpoint.rs`; no field or method is exposed
for their access.

Locator erratum16fcece1 places the whole feature-gated
`ApplicationOutcome::into_http` impl in `application/execution.rs` (current
guard1162/method1174; original application_runtime.rs9622–9679). HTTP entry
methods are separately owned by `application/http.rs`.

Actual module verification and EV-27–29 acceptance remain pending. Source
conservation does not close DG-MODULES. The sole current closure list is in
the [EXEC PRD](../execution-ownership.md#remaining-blockers--current-closure-table).
The inventories and stop conditions below are historical source epochs. Their
versions, names, hashes and earlier eligibility states are preserved as evidence
and do not describe the current selected contract or accepted map.

---

# Historical source inventories

**Revision note (2026-10-01):** ARC-001 removed `ShutdownControl`,
`TypedShutdownControl`, `ShutdownSignalRejection`, and `ActorRef.shutdown`.
The table below is a dated source inventory, not the current module map. See
`../../module-boundaries.md` and `../../prd-backlog/status.md` for current
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

---

# Historical module ownership reconciliation (source 931)

This is an unaccepted source-only DG-MODULES proposal against actual931, not a production move or final gate signature. Semantic verification precedes layout as explicitly selected in EXEC's user-selected implementation order (lines933–940), superseding the earlier layout-first work-package sequence.

The exact retained source epoch is capability-preparation-route-runner-jvm879_s, verification931f3158ea0a8aa2d48c4c856b882d4b9efecbc7335fbed3b74f8b584ef1b84f: all346 Runtime files and180 Actors files individually rehashed. The canonical lock still selects Core/Actors0.21.2, Macros0.13.1, Address0.3.0, Communication0.1.3, Tokio1.53.1 and the sole Timer13e patch. Actual931 uses published registry Core0.22, local research Actors0.22 and the same verified Macros/primitive selections. These are separate build contracts.

The old192-line record is historical: its0.17/0.12 versions, Incarnation/outcome file names, ActorOrigin, old shutdown-control symbols, ignores and projection/task signatures are not current authority. Source hashes in that record are not relabeled. Current concrete task comparison and paired/native research contracts are unretained. In particular, a narrow task comparison's reported81 production lines and canonical cumulative133 production lines describe different baselines/scopes; neither accepts the research symbols nor proves a reduction. Current conditional task storage remains cfg(test), while ordinary production actor/task/raw retirement changes remain their independently version-bound research contracts.

## Complete source inventory

`production-symbol-owners.json` contains755 declaration occurrences across20 production-owning files, including every named type/trait/alias/constant, trait-associated declaration and method found in the exact source, complete headers, impl/trait owner, current visibility, source line/hash, proposed destination and concrete struct/enum members. `module-symbol-owner-map.md` renders every row. `before/` freezes34 full source files, including14 Observe test/support files. Observe's296 fixture declarations are separated in `observe-test-fixture-symbols.json`, not counted as production.

`module-imports-and-root-exports.json` inventories338 import/export statements including the preserved Observe fixture sources. `named-symbol-consumers.json` inventories exact named-token occurrences across all526 Runtime/Actors files and current canonical docs/macros/examples/fixtures. Common names and unresolved method calls are lexical candidates, not static Rust identity proof. `cross-domain-consumer-edges.json` focuses the actual owners and their Entity/macro/application/test/example/benchmark/doc consumers. This inventory does not claim compiler-resolved public API completeness or signature eligibility.

## Proposed existing ownership groups

| Existing owner | Proposed destination | Required preserved fact / current consumers |
| --- | --- | --- |
| Application root/role values and App declarations | application/mod.rs | Cold declarations allocate no runtime. Preserve Application/App root names and explicit root/child values. |
| ApplicationBehavior, StageApplicationChildren, ComposeApplication, exact staging errors and role-origin products | application/composition.rs | Pure initialization and real birth algebra unchanged. Methods that call ProjectedTask projection/spawn belong to execution, not pure declarations. |
| run/run_with/execute_application_with, LaunchSystem, RootProjection, ApplicationOutcome and caller work/publication ownership | application/execution.rs | Preserve current raw original JoinError and staging/cold/work/output equations; conditional paired outcome remains research. No new launcher or outcome wrapper for a move. |
| Axum methods/errors/acquire_axum_retirement | application/http.rs | HTTP composes the same execution; original serve/result/root cleanup owner remains intact. No second runtime builder. |
| ApplicationHandle/ApplicationLifecycle and ActorInterface/ExternalActor/ExternalTarget | application/interface.rs | Keep affine ExternalActor receive ownership, exact rejection and lifecycle restriction distinct from ActorRef. |
| ActorRef, InstalledActor, SendError, request_actor_shutdown and ExtractLocalEndpoint | local/endpoint.rs | Public messaging/installation facts stay root-curated. Shutdown request remains typed private operation; ExtractLocalEndpoint is used by both external interface and lower delivery. |
| Admission/AdmissionClosure, LocalIngress/IngressMode, StandardIngress/EntityIngress, EndpointMailbox, LocalInbox and drain | local/ingress.rs | One Communication owner, FIFO control/user acquisition and Entity fence/drain semantics; no second mailbox. |
| LocalEnvironment/ActiveLocalEnvironment, LocalResidual, LocalActivationRejection, Publication/PublicationNotice | local/environment.rs | Prepared/active phase relation and resource retirement remain together. Do not mechanically split affine native/Ready/drop receiving cuts. |
| ActivationTasks, OwnerCancellation and closure, OwnedActor/OwnedTask/RootActor/ProjectedTask/OwnerCancellationAuthority, shared spawn transaction | local/execution.rs | Preserve sole root/child/capability joins and original raw errors; no detachment, reconstruction or invented join-all acceptance. |
| ActorExecution/Terminal and outcome/retirement/terminal-publication conversions | local/execution.rs plus local/termination.rs by authority | ActorExecution owns universal Driver execution; ActorExecutionOutcome/ActorRetirement/ChildFailure preserve typed causes. Exact retirement is distinct from coarse Observe termination. |
| RuntimeChildBindings and exact occurrence bindings/cursors, structural origin proof | local/children.rs | Creation ordering, original CreationId/kind/route, address/task/endpoint custody and actual typed descendant facts; Entity and application both use this. |
| ActionInterpreter, RetireCapabilities, ApplicationCapabilityInputs/ApplicationCapabilities, CommitActions/CapabilityRetirement, inert interpretation | local/effects/mod.rs | Concrete actor composition is not application-only. Keep exact owning Behavior traversal; renaming ApplicationCapabilities requires a recorded naming decision, not compiler pressure. |
| SourceAdmission/EstablishChild/creation interpretation | local/effects/creation.rs using children/execution | Existing route-reservation/Start–record–ACK law stays distinct from child initialization/host/native task gaps. No new birth algebra. |
| Logical/exact/child/established/source delivery and reunite_customer_delivery | local/effects/delivery.rs | Select genuine endpoint and recover whole rejected payload; public leaf/action input receiving remains its semantic gate. |
| Current observation types and relationship interpretation | local/effects/observation.rs | Preserve actual independent registration/fact queues and termination sharing; current published0.22 two-owner Observe law is not dated three-owner Sequence. |
| LocalTimers/TimerError and concrete timer interpretation | local/effects/timers.rs | Existing generation-safe actor-local TimerQueue and current acquisition order; no timer task/service. |
| ParentReporting/LocalParentReports/LocalTerminalReports/TerminalReportTransaction | local/effects/reports.rs | Preserve exact parent values and action-scoped terminal selection. No forwarding wrapper added just for a file. |
| Structural origins/projection and Entity admission impl | Existing children/effects owners plus entity/bombay.rs candidate | Current bidirectional edge requires source/coherence proof before moving an impl; no address-access trait merely for layout. |
| MailAddr/ApplicationAddresses | address.rs | Keep exact non-wrapping allocator and endpoint type dependency; not actor-independent abstraction. |
| Hosts/HostedActorSpaces/ResolveLogical and ActorSpace | local aggregate existing static proof | Macro and Entity consumers require existing public root paths. Delete nothing solely because it is an alias. |
| WorkerPreparationSource/WorkerPreparationStart and settle_worker_preparation | Existing public source contract; exact private creation/preparation lane | Preserve source actor/template ownership. Later spawned preparation metadata is not normal action settlement custody. |
| observe/ | Unchanged private primitive | Exact keyed/pair/affine publication/waiting; isolated Loom/fuzz/test package compiles the same source. No observation primitive split. |

## Actual public and cross-domain boundaries

The current lib.rs is frozen in before/. It exports Application, App, ApplicationBehavior, ApplicationDefinitionError, ApplicationStagingError, ApplicationHandle, ApplicationLifecycle, ApplicationOutcome, RunError and optional AxumRunError; ActorInterface/ExternalActor/ExternalActorError/ExternalTarget; ActorRef/InstalledActor/SendError; ActorSpace/Hosts; RootOrigin/ChildOrigin/ChildFailure/ActorRetirement/ProjectTerminal; MailAddr and preparation types. ApplicationOutcome and ChildFailure are current research root surfaces, not implicitly canonical approval. Engine owns Completion/SettlementFailure. The actors/behavior and prelude surfaces remain curated.

The actual Bombay macro source still spells root ActorRetirement, RootOrigin, ChildOrigin, ProjectTerminal, Hosts and ActorSpace through renamed-crate resolution. TerminalProjection consumes the actual three-parameter ActorRetirement and exact root/child origin proof. ActorSpaces emits Hosts::space returning ActorSpace; it cannot be migrated by changing public qualification implicitly. The actor facade still delegates to Behavior's owning expansion. Private module paths must not become generated public references.

Entity's current native host imports application_runtime::{ApplicationCapabilities,NoParent,StructuralOrigins}, creates ApplicationCapabilityInputs from that module, and calls launch::{OwnedActor,SpawnError,spawn_owned_entity_with}; it also uses local::{ActorRef,CommitActions,request_actor_shutdown}, topology Hosts and exact ActorRetirement. Moving the concrete actor product under local removes this wrong application edge without inventing another product. Conversely, the EntityAdmission InterpretItem impl still names EntityDefinition/NativeEntityHost and calls the current request interpretation with local actor address. Whether that impl belongs in Entity or remains a concrete actor effect integration must be proved with unchanged coherence and the narrow existing concrete address value; no accessor/adapter selected here.

Visibility proposal:

- Preserve every externally nameable root re-export and current exact public associated type. Root paths are part of macro and fixture law, not file layout.
- Keep endpoint/mailbox/resource fields private. New local child files may need pub(in crate::local) for actual local sibling sharing, not public constructors or exposed raw authority.
- Application execution and Entity require only existing typed local spawn/result/projection/host operations via pub(crate); their real import sites are inventoried. Do not make every moved field pub(crate).
- Application composition's pure role structures may stay private to application; executable origin projection impls depend on existing local execution/children. StructuralOrigins cannot be stranded under application because Entity uses it.
- Pure application/header imports, interpreted child projection and public macro-root paths are distinct: no production forwarding type, bridge trait or new global bound is justified by relocation.

## Minimal extraction proposal after semantic acceptance

1. Freeze the final semantic composition/source graph, including normal action receiving, affine initialization/retirement, capability-task facts and genuine child consumers. Existing focused passes do not accept all those contracts. Update this map by exact changed declarations only; root's pending quality changes are cfg-only and do not change production symbol ownership.
2. Obtain nonauthor/coordinator DG-MODULES map and visibility disposition. Test the existing Entity-origin/projection boundary and exact external root signatures without new ports. Reopen a semantic gate if a move needs a new ownership interface.
3. Mechanically extract one existing owner at a time, starting with endpoint/ingress and existing children/termination responsibilities only after their accepted differential baseline. Application extraction follows the unchanged concrete local entry; keep one implementation and one aggregate module form.
4. Preserve module-level imports, source-bound private mechanism tests, historical original-defect fixtures and precise expected diagnostic content. Module offsets may need reviewed stderr updates; never turn the original negative into a healthy replacement or discard complete effect lanes.
5. Run focused debug/optimized before/after differential cohorts for pure initialization/ordered sends/births, original rejection values, cold work, cancellation/publication, FIFO/control/user drain, child/order/projection, Entity families and HTTP. Then external renamed-crate/macro/static denial, rustdoc/examples/bench/fuzz and required strict checks against the selected graph.
6. Record actual complete tracked/untracked delta and exact new file union before moves. Target layout exceeds15 source files. User delegation permits root recommendations but neither current229 research scope nor seven conditional nominal allowance is blanket layout selection. No public type or method addition is forecast for mechanical extraction; actual production line delta is unmeasured until complete source edits, not assumed reduction.

No software source was edited and no Rust/Cargo/Nix/Git/cache/slot command ran. This text changes no production/test/API contract. It freezes an actionable map and remaining exact cross-owner decisions; no DG-MODULES signature, production source eligibility or full feature approval is granted.
