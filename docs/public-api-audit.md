# Retained public API audit

This retains ARC-020's owner inventory and reconciles the EXEC application
surface against published Behavior Core/Actors 0.26.0 at archive revision
`98983b456c4a8f7cd0911982eaf9488455b6d8bc`, Macros 0.14.2, Address 0.3.0,
Communication 0.1.6, private Observe, and published Timers 0.2.0 in `Cargo.lock`. It covers Bombay-owned
items reachable from the `bombay`, `bombay::entity`, `bombay::actors`,
`bombay::remote`, and `bombay_engine` roots. Re-exported Behavior and Behavior Actors families retain
their upstream ownership. The [Rust API Guidelines checklist](https://rust-lang.github.io/api-guidelines/checklist.html)
informs names, traits, error behavior, and examples; exact ownership follows
the selected Behavior instructions and [module map](module-boundaries.md).

The user requires Zenoh to remain private to Bombay, including advanced APIs.
Current Node/Export rows below inventory provisional transport-facing contracts;
they do not approve retaining those contracts for complete remote delivery.
Native Config, Session, Query, Reply, declaration and transport callback types
must leave the public surface. Networking belongs to Application construction,
execution and retirement, with Bombay errors preserving original causes. The
remote PRD and acceptance matrix record the required ordinary public witness.

Each row names its public methods or trait operations, its owner and caller,
the construction invariant, standard trait decision, and the custody or error
behavior. `Debug` on a generic value is conditional on its payload being
`Debug` unless stated otherwise. An omitted `Clone` on an affine value
prevents duplicating its completion, receive, lease, or pending-decision
authority. A value without `Debug` carries an unconstrained domain payload or
opaque runtime port; callers inspect typed variants or consume the value
instead of relying on a lossy rendering. No error or terminal payload is
erased merely to implement a formatting trait.

The root entry checks are owned by the [application topology](../examples/application-topology/src/main.rs),
[counter](../examples/counter/src/main.rs), [Axum](../examples/axum/src/main.rs),
[supervision](../examples/supervision/src/main.rs), and
[worker-pool](../examples/worker-pool/src/main.rs) examples; the
[public handle trait test](../crates/bombay/tests/public_handle_debug.rs),
[external caller test](../crates/bombay/tests/actor_interface.rs), and
[authoring compile matrix](../crates/bombay/tests/macro_last_authoring.rs) establish narrower
positive and negative boundaries. These references identify the owning checks;
the combined integration still requires its final full verification. No ordinary
multi-node remote API is implemented by this reconciliation. The subsequent
merged opt-in `remote` feature provides `remote::verify`: one ordinary function over
a borrowed request, a caller-owned standard result slot and a concrete provider
future. It adds no public type, trait or bound beyond `FnOnce` and `Future`.
Its owning verification and protected Application tests cover result custody,
borrowed-wait cancellation and occupied-slot suppression; network composition
remains pending. Reviewed [PR363](https://github.com/devrandom-labs/bombay/pull/363)
delivers `remote::try_retain` and `IngressError<Original>`, reusing existing Tokio
ownership products without a new dependency or actor protocol. Reviewed PR364 delivers four types: raw Metadata, one Node and two
original-bearing refusal sums, with passing required CI and scoped minimization
evidence in the [remote PRD](prds/static-remote-actors.md). Full remote acceptance
remains separate. Merged PR368 delivers the JSON check described below with
passing combined and required CI checks. Merged PR370 delivers the certified
client and native nonwaiting attempt after scoped source/execution reviews and
all six checks pass. Main-source delivery is distinct from registry publication.
Native Session acquisition is not endpoint readiness or application authority.
The Engine surface is exercised by its
[Driver law tests](../crates/bombay-engine/tests/driver_law.rs) and
[terminal custody tests](../crates/bombay-engine/tests/terminal_custody.rs).

## Bombay root and actor composition

| Item and operations | Owner, caller, construction invariant | Traits and custody / failure |
| --- | --- | --- |
| `MailAddr`, `APPLICATION_ROOT`, `From<u64>` | Bombay local address; runtime allocates a non-wrapping address, while advanced hosts/tests may name one explicitly. The number is identity, never endpoint authority. | `Debug`, `Copy`, `Clone`, equality, order, hash; no error payload. |
| `remote::try_retain` | Opt-in nonwaiting ingress transfer into a caller-owned Tokio channel using its explicit shared Semaphore. | `Original: 'static`, no original Clone/Debug bound; accepted `(original, OwnedSemaphorePermit)` remains caller-owned across active/unread custody. No authentication, actor admission, numeric default or byte/heap bound. |
| `remote::IngressError` | Exact capacity refusal or native channel refusal. | Payload-free Debug/Display and standard Error source; Capacity retains original + TryAcquireError, Queue retains TrySendError<(Original, OwnedSemaphorePermit)>. No Clone; channel refusal keeps the charge beside the original. |
| `remote::json::check` (merged PR368) | Borrowed checks of complete protected JSON and separate proof bytes before concrete Serde decoding. | Selected event parser0.2.3; outer/decoded metadata objects, five explicit maxima, original native syntax cause. Does not require metadata presence or issue authentication/admission; no peak-heap claim. |
| `remote::json::Limits` (merged PR368) | Five independent required acceptance maxima: input/proof bytes, decoded token bytes, logical nesting and native parser states. | Public semantic NonZeroUsize inputs, Clone/Copy/Debug/equality; no Default, builder or hidden values. Native state count differs from nesting. |
| `remote::json::Error` (merged PR368) | Flat typed syntax/object/size/nesting refusal, preserving the actual native syntax source. | Exhaustive manual Display/Error retain the source without generated inline imports. NoEvent covers the native Option return but is not exercisable with the selected complete-input parser; no witness claimed. No serialization or payload ownership transfer. |
| `remote::Metadata` | Concrete protected protocols place fifteen untrusted declarations in their `bombay` field. Deserialization does not issue authority. | Serde Serialize/Deserialize, Debug, equality; all object fields required and unknown/duplicate fields rejected. Derived decoding also accepts arrays, so the bounded receiver must enforce object shape. Debug shows declared identifiers; original bytes/proof, version/profile/permission/replay validation remain separate. |
| `Application::open_node`, `remote::Node::run_with`, `execute_with`, `close` | Finalized ordinary root/child/Entity-family Application plus one newly opened native Session; opening accepts Bombay Network and constructs the selected TLS profile privately. | Redacted unconditional Debug, no Clone/Default. Startup returns original Application/Network plus typed refusal. One declaration transfer; run_with joins the complete ApplicationOutcome with a Session-independent business result. Both Work callbacks receive only the ordinary Application handle and may retain caller-local borrows and non-Send results; execute_with returns separately owned execution/receiving. Both use the existing family invocation spine, expose its declared receptionists and retain exact family shutdown results. Original Work survives refusal. Node staging includes `Families` and prepared inputs retain `Arc<Spaces>`, including for an empty family product. Borrowed waits preserve outside owners; whole-operation disposal is surrender. SDK close retains its own Result after actor receiving. |
| `remote::Network`, `NetworkLayout`, `ConnectionEndpoints`, `CredentialSource` | Explicit roots/query count, Peer or RouterClient directions, all-of plain addresses and independent File/Base64 credentials; consumed by the actual Node opener. | Debug redacts credential payloads; no Clone, Default, builder or public native conversion. Borrowed mapping preserves originals on refusal; actual startup/Counter controls qualify this input stage only. |
| `remote::NodeStartupRefusal` | Original-input refusal for unsupported directions/settings, path/address/credential construction, excessive quota, missing/wrong host or actual native open failure. | Payload-safe Debug/Display and Error retain original endpoint/open standard-Error Boxes, unboxed JSON conversion cause and TryCurrentError. Fixed identity, explicit NonZero quota and closed credentials remove the old raw Config-only error variants. No readiness, authentication or whole-service quota promise. |
| `remote::NodeExecutionRefusal<Work>` | Already-transferred declaration, closed Session, or actual missing execution host; each variant owns original Work. Only host refusal restores the original declaration for retry. | Unconditional payload-free Debug/Display/Error, no Work Clone/Debug/Send requirement. Actual host source retained. Work receives no SDK Session. Later execution and close still require a suitable live host; opening does not bind them to that host. Returned execution/receiving owners must leave scope before mutable close. |
| `Application::new`, `child`, `root` | Bombay root-first declaration; app author supplies each concrete Behavior and semantic role before launch. | Conditional `Debug`; affine declaration. `child` consumes the old declaration and retains its root, selected scope, existing actors and families. |
| `Application::with_topology`, `entity_family`, inherent async `run`, `run_with`, `run_with_entities`, `run_axum` | The same Application owns caller-selected static hosting and explicit Entity policies. Cold actors and families coexist, staged before root initialization. | Conditional `Debug`; affine application inputs. `entity_family` consumes validated `DirectoryConfig` and has no configuration-error branch; async calls return original inputs with `TryCurrentError` or complete phase/Work/cleanup/root/family custody. |
| `Application::execute`, `execute_with`, `execute_axum`, `execute_with_entities` | Paired constructor captures a live entered host, retains cold inputs, and separates execution from result receiving. | No erased future or extra wrapper; receiving can outlive dropped execution. Work/output need no `Send` or `'static` bound. Actual actor tasks preserve their owning bounds. |
| `Application::run_blocking` | Caller supplies an ordinary configured Tokio Builder; Bombay rejects entered-runtime use before effects and drives the same async pair after successful build. | Returns original Application and Builder with `BlockingInEnteredRuntime` or exact native `Runtime(io::Error)`. No scheduler enum/default or separate actor loop. |
| `ApplicationOutcome` | Complete coexisting work/root/notification/family product, explicitly imported from the Bombay root; WorkOutcome owns the work phase. | `#[must_use]`, no affine Clone; exact cold/partial/prepared/Work/Ready output and actual cleanup errors retained. No-work uses `Option<Never>`; supplied Work and output are bare original values. HTTP cold Work is unit before binding; acquired Work owns Router/Listener. Root remains raw; Terminal selects descendants. |
| `ApplicationScope::new`, `external`, `with_root`, `with_topology` | Affine application address source establishes exact external recipients needed before pure root construction; consumption declares one root. | `Default`, redacted `Debug`, no `Clone`; receiving stays separately owned. Actual staging/prepared scope returns with original inputs for retry. |
| `ApplicationClient::address`, `recipient`, `send`, `receive`, `close_admission` | Root-bound or explicitly selected destination and sole typed reply consumer reuse the existing ExternalActor. | Affine receive ownership; destination-specific original refusal and truthful sender identity, no lifecycle authority. |
| `ApplicationClient::try_send` (`remote`, merged PR370) | Existing client specialized for an exact EstablishedRecipient; attempt the retained target with its existing allocated caller, without waiting for capacity. | Native Full/Closed originals, no message Clone/Debug bound, no new target input or retry. Permission must be rechecked by the owning service before each attempt. Stable Entity resolution retains awaited send. |
| `ApplicationSender::address`, `send`, `try_send` | Root-bound or explicitly selected one-way delivery owns its exact target and fresh address, without a reply endpoint. | No receiving or lifecycle capability; immutable producers can share a sender when its target is Sync. Exact target and async rejection semantics remain those of ExternalTarget. Only exact EstablishedRecipient targets expose try_send, delegating native Full/Closed original-command return without waiting; Entity hydration remains async. |
| `ApplicationBehavior` | Bombay's statically composed root and declared child birth product; only `Application` materializes it. | Implements the exact Behavior algebra; no independent actor contract or exposed constructor. Exact root error is retained by `ApplicationDefinitionError` in its associated Behavior contract. |
| `ApplicationHandle::root`, `interface`, `client`, `sender`, `client_to`, `sender_to`, `external`, `lifecycle`, `entities`, `passivate_entity` | Live Bombay application; external boundary receives this handle only after activation. Each method projects an existing endpoint, family, or lifecycle authority. | Conditional `Clone`, redacted `Debug`; no equality for live capability identity. Passivation yields closed `Passivation` disposition. |
| `ApplicationHandle::export_client` (`remote`, merged PR370) | Certify one actual root or descendant recipient against this Application's existing allocation source before constructing the existing reply client. | Foreign actors and external reply endpoints return their original recipient before reply allocation. Closed owned actors remain associated. No network publication, readiness, authentication, permission or lifecycle authority is issued. |
| `remote::ExportError` | Merged PR370 supplies `UnownedTarget` and `Endpoint { cause: ExternalActorError }`. PR372 adds `NodeAbsent`, `SessionClosed`, `QueryCapacity`, `Storage`, `Declaration`, `Conflict` and `RetentionCapacity`; construction/retention separately return original inputs. | Display omits native declaration text; Debug uses the existing startup-error tuple style over that bounded text. Error::source preserves the original allocation, reservation and declaration causes. Declaration names the existing native box through its identical standard Error type, without reboxing. Seven variants were added by PR372; this diagnostic correction adds none. Private ownership stays in export.rs; public spelling remains unchanged. The scoped diagnostic correction merged through PR384; full remote privacy/service gates remain open. No Clone, new provider trait or target registry. |
| `ApplicationHandle::export` (`remote`, merged PR372) | Node attaches its private Session to its actual live handle; certify this Application's existing root or descendant before declaring an export. Explicit key, limits, stable provider identity comparison and ingress-refusal consumer. | Original target returned on failure; no reply endpoint allocated until a new request is retained. No public constructor can pair a foreign Application with this Session. No readiness, authentication, permission or unique-declaration promise. |
| `remote::ExportLimits` (merged PR372) | Required nonzero query-channel, retained-query and retained-request count maxima. | Clone/Copy/Debug/equality; no Default or hidden limit. Native channel/semaphore range checked before declaration; no whole-service heap bound. |
| `remote::Export` (merged PR372) | Own declaration, query receiver, certified target and append-only retained originals. `retain`, `request`, `attempt`, `retry`, `receive_query`, `receive_native`, `undeclare`. | Unconditional redacted Debug, no Clone. Each retained request owns an independent existing ApplicationClient. Duplicate/conflict/count checks precede lazy endpoint allocation; native Full/Closed commands remain original. Permission loan spans nonwaiting attempt/result storage; retry only resends a retained Full. Native acquisitions survive borrowed-wait cancellation. Undeclare returns all request clients, accepted query prefix and original result products; whole-owner disposal surrenders custody. |
| `remote::Export::receive` (candidate) | Borrow the existing query receiver and all eligible native request waits together; retain actual User/EOF in its row before issuing the existing key. | Either occupied outside slot suppresses acquisition. Pending native registrations stay alive until operation disposal; acquired originals survive. Native-first progress is bounded by once-acquired lifetime rows, assuming continued polling/host budget, not elapsed time. FuturesUnordered adds per-child plus fixed collection allocation/rebuild costs. No new public type, protocol validation, capacity wake, automatic retry or business-completion claim. |
| `remote::RequestKey` (merged PR372; target-key candidate) | Runtime-issued Target protocol, opaque issuing declaration and private append-only row; no public constructor. | Unconditional redacted Debug and equality; no Clone/Copy. No reply endpoint is owned by the key. Weak identity reuses the existing declaration allocation, preserving distinct backing storage without strong admission ownership. Export checks issuer before row lookup or permission/command construction. Another Target is a type error; another declaration of the same Target returns None from request/receive_native and UnknownRequest from attempt/retry. |
| `remote::Admission` (merged PR372) | Unattempted, interrupted-return Uncertain, or complete native Returned success/Full/Closed. | Unconditional state-only Debug; no command formatting/Clone bound. Admission is distinct from business completion, authorization and later native reply. |
| `remote::RequestRefusal` (merged PR372) | Unknown request or original current-permission cause; prior acquired facts stay retained. | Unconditional payload-hiding Debug; Display only requires cause Display; Error/source only requires cause Error + 'static on that trait implementation. Methods continue accepting borrowed non-Error causes. No conversion, cloning or error erasure. |
| `ApplicationLifecycle::request_shutdown`, `termination`, `retirement` | Bombay lifecycle projection; caller holds root control separately from message delivery. | `Clone`, redacted `Debug`; shutdown returns exact `ShutdownRejection`, termination awaits the exact root generation; retirement independently observes the runtime-issued joined-subtree report while Work remains live. |
| `ApplicationCleanupError` | Application result receiving owns actual cleanup-publication or cleanup-task failure. | Flat `PublicationClosed(RecvError)` and `TaskFailed(JoinError)` retain the original source; actor native failure remains `ActorRetirement::ActorTaskFailed`. No actor absence is inferred from channel closure. |
| `RunError` | Existing owner of the two actual owned-executor failures. | Flat `BlockingInEnteredRuntime` and `Runtime(io::Error)` variants; Debug/Display/Error and truthful native source. No generic root/staging/terminal aggregate; originals coexist beside it. |
| `ActorRef::address`, `send_from`, `try_send_from`, `termination`, `established_recipient` | Bombay exact local mailbox reference; runtime issues it only after commit. | `Clone`, redacted `Debug`; no direct shutdown authority. Closed send returns `SendError` with the exact message; termination remains bound to one generation. |
| `InstalledActor` | Bombay's installed endpoint plus typed control authority, used as Behavior's `EndpointAddress::Installed` product. Runtime alone constructs it after binding. | `Clone` for multiple typed interpreters and redacted `Debug`; no public constructor. Its existing typed `request_shutdown` closes user admission and submits to the exact child control lane; success is submission, not joined retirement, and unwind can leave partial effects. Its generic behavior identity prevents cross-behavior substitution in compile fixtures. `retirement()` observes its exact shared runtime-issued report without adding stop authority. |
| `SendError::into_message` | Communication rejection projected through `ActorRef`; caller regains the original payload. | `Debug`, `Display`, `Error`; no `Clone` requirement on the payload. |
| `ActorInterface::api`, `external`, `client_to`, `sender_to` | Bombay external-caller product; application explicitly exports an API value and the runner's allocation source. | `Clone` when API is cloneable, redacted `Debug`; `external` returns exact allocation error before issuing any recipient. |
| `ExternalActor::address`, `recipient`, `send`, `try_send`, `receive`, `close_admission` | Bombay's allocated external actor; caller owns the sole receiver and may clone only its recipient. | Redacted `Debug`, intentionally no `Clone` ([denial fixture](../crates/bombay/tests/compile/fail/external_actor_receiver_is_affine.rs)); send returns the destination's exact rejection, receive returns the truthful origin. Exact-recipient try_send returns native Full/Closed originals without a capacity wait or new message Clone/Debug/Send/static restriction; no stable Entity hydration or authentication is added. |
| `TrySendError` | Deliberate re-export of Communication's existing user-delivery refusal sum; Bombay defines no duplicate. | Full and Closed retain the original message. Display describes pressure/closure; Debug/Error inherit the owner's payload Debug requirement and may expose payloads. Native closure is not actor termination or joined retirement. |
| `ExternalActorError` | Bombay external allocation failure. | `Debug`, `Display`, `Error`; exact `AllocationRejection` source. |
| `ExternalTarget::send_from` | Sealed Bombay delivery port for an exact actor recipient or Entity reference. | No object erasure; associated message and error preserve destination custody. Caller uses `ExternalActor::send`. |
| `ActorSpace<P>` | Bombay's protocol-closed alias for Address's concrete space of exact `ActorRef<P>` endpoints; advanced topology caller supplies it. | Inherits Address's traits and claim/lease rules. The alias fixes protocol/address/endpoint relationships; it adds no wrapper or alternative registry. |
| `Hosts<P>::space` and `ActorSpaces` derive | Bombay static host proof; advanced application supplies explicit `#[actor_space(P)]` fields. | The derive emits only `Hosts<P>` impls. Wrong, missing, and duplicate hosts fail compilation; no dynamic lookup. |
| `RootOrigin::address` | Bombay exact root source identity, created only by runtime. | `Debug`, `Copy`, `Clone`, equality; no nonce or child conversion. |
| `ChildOrigin::address`, `nonce`, `creation_id`, `creation_kind`, `into_declared_child` | Bombay exact child source identity, created by runtime and converted only with Behavior `ChildRole` proof. | `Debug`, `Copy`, `Clone`, equality; role conversion preserves address, nonce and actual creation identity/kind. Wrong role/actor origins fail compilation. |
| `ActorRetirementReport`, `RetirementAssessment`, `ActorFailureAssessment` | Existing runtime owners derive two independent assessments after actual joined retirement, before application conversion. | Read-only report, no public constructor or Default; Copy/Clone/Debug/equality. Known failures survive incomplete checking; notification or conversion faults occurring after publication cannot rewrite the report. |
| `ActorNotificationReceipts`, `RetirementNotificationError` | Existing joining owners retain original termination and joined-report notification results beside native custody. | Named affine termination/retirement fields; exact opaque panic allocation or actual receipt-closure source, redacted Debug. No native cloning or serialization. |
| `ActorRetirement::completion` | Bombay owns the normal stop/exhaustion reason in the Completed alternative. | Borrow only; failure/cancellation return absence without consuming their causes. Independent child and cleanup failures remain in the original retirement. |
| `ActorCustody` and `ActorRetirement::custody` | Bombay owns the 14 coexisting actor/runtime custody values shared by 20 retirement alternatives. | Owned product, borrowed inspection, no new authority or `Clone`; all phase-specific causes remain in the 24-alternative retirement sum. Allocation refusal and executor outcomes without returned state have no product. |
| `ActorRetirement` | Bombay exact actor terminal sum; runtime selects one variant and retains final Behavior, settlements, messages, descendants, and cause when available. | `#[must_use]`, intentionally no `Clone`; no generic `Debug` that would require or discard unconstrained exact payloads. Consumers exhaustively match owned variants. |
| `TerminalProjection` derive and owned `From` inclusion | Exact total `(origin, retirement)` embedding into the caller's declared sum. | Preserves aliases, arbitrary payloads and static role proofs; no runtime authority or erased data. |
| `ProjectTerminal<Origin, Actor>` | Existing native projection port selects raw `Descendants` before child launch and consumes complete retirement in its native task. | Child raw custody and projected report have independent types; original cancellation and native panic causes remain intact. |
| `WorkerPreparationStart` | Bombay's one first-role preparation choice over upstream worker submissions. | `#[must_use]`, intentionally no `Clone`; source and worker rejections remain distinct owned variants. |
| `WorkerPreparationSource::prepare_first`, `prepare_next` | Bombay effect interpreter port for upstream `PrepareWorkers`; application supplies the concrete source. | Affine mutable source; first attempt can return a complete source rejection, later attempts only a role rejection. The Behavior fold never calls it. |
| `ActorExt::with_stash`, `with_one_shot`, `with_periodic`, `with_deadline`, `with_receive_timeout`, `stop_on_shutdown`, `finalize_on_shutdown` | Bombay syntax for existing Behavior Actors `Stash`, timing, and shutdown wrappers; app author supplies every policy. | Each `#[must_use]` method consumes its Behavior and returns the exact upstream concrete type; no Bombay wrapper state or new actor law. |
| `#[bombay::actor]` | Bombay's one accepted application syntax facade; author supplies state and fold. | Delegates to the locked Behavior expansion and preserves its complete typed Actions. |
| `Completion`, `SettlementFailure` | Engine's exact Driver disposition and settlement failure sums, re-exported for terminal matching. | `Debug`, `Copy`, `Clone`, equality; `SettlementFailure` implements `Error`. |
| `testing::InfallibleResultExt::infallible` | Deliberate test-only helper for `Result<T, Never>` or `Result<T, Infallible>`; a pure test caller extracts an inhabited success. | No panic branch; adding an inhabited error makes the call fail compilation. Absent from the ordinary prelude. |

The JSON prerequisite delivered by PR368 has five public shape/limit tests
in remote_json.rs and two actual Node Counter/MailLedger consumers in
application_clients/json_receiving.rs. They reuse the existing actor definitions
and receiving owners. SessionLocal deterministic records establish neither a
network handshake nor production authentication. Full remote export/admission
composition remains pending.

Merged PR370 adds public root/sibling/nested-child and closed-target
checks in fixed_supervisor_recovery.rs, plus a separate unhosted same-address
foreign-Application test in protected_counter/hosting.rs. The original hosted
test remains intact. The owning interface test checks refusal before allocation,
one remaining usable reply endpoint, exact exhaustion and original error custody.
Focused controls and six intended ownership inversions pass in both profiles;
fresh restored controls, local combined checks and required CI pass. Actual
reviewed delivery is merge8d809b3d. These checks do
not establish the full remote flow.

The nonwaiting method's actor_interface test retains exact Box allocations and
sender origins through pressure, one retry and repeated closure. Its certified
Counter consumer uses an independent existing processing receipt as a FIFO fence
before closing/draining replies; false retry success cannot leave it waiting for
an absent reply. Both controls pass in debug and optimized. The source reviews
retain two rejected hanging drafts. Five intended cuts reach their failures in
both profiles, then both consumers pass on byte-exact restored source; independent
execution review8a97b4a6 qualifies these results. Local combined checks and
required CI pass; reviewed PR370 merged8d809b3d. Complete remote operations
retain their separate gates.

The remote foundation's concrete caller checks are
[public Node composition](../crates/bombay/tests/protected_counter/public_node.rs),
[startup refusals](../crates/bombay/tests/remote_node.rs), and
[raw metadata](../crates/bombay/tests/remote_metadata.rs). Native TLS/fault campaigns
remain separately qualified transport evidence, rather than full remote API proof.

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
| `EntityDefinition::hydrate`, `activation_failed`, `admission_refused`, `forced_retirement`, `retired` | Application owns one nominal family, reconstruction, and each typed disposition callback. | Trait preserves concrete Behavior, hydration error, terminal, and command types; callbacks consume exact failure or native retirement once; `retired` also receives the read-only actor-subtree report before any application conversion. No runtime trait object is required. |
| `EntityCapacity::new` | Caller supplies independent nonzero hydration and resident bounds. | `Debug`, `Copy`, `Clone`, equality; no hidden default policy. |
| `Entities::entity` | Application-installed family receptionist; caller binds a stable domain key. | `Clone` shares the family endpoint, not an actor; opaque runtime storage has no equality or generic `Debug`. |
| `EntityRef::id`, `request` | Stable typed family reference; caller may request while no actor exists. | `Clone` retains logical identity; `request` returns a concrete `EntityAdmission` action, never sends from a Behavior fold. |
| `EntityAdmission` | Entity action interpreter owns one actor-originated command. Only `EntityRef::request` constructs it. | No `Clone`/`Debug` requirement on the command; action interpretation reports refusal to `EntityDefinition` with exact command. |
| `EntityActivationError` | Entity definition callback receives resident-capacity, hydration, allocation-before-launch refusal, or started actor-launch native/report/first-notification custody. | Deliberately an owned domain outcome rather than `Error`; no lossy generic `Debug` or `Clone` bound on Behavior/terminal. |
| `EntityMetrics` | Family reports fixed counter observations, never authority. | `Debug`, `Copy`, `Clone`, equality; snapshot values are public. |
| `EntityApplicationFamilies` | Sealed static family product produced by `Application::entity_family`. | No separate runtime or dynamic map; exposes typed live/shutdown projections to the application. |
| `EntityFamilyAt::select_family` | Sealed role/position selection proof for `ApplicationHandle::entities`. | Static role mismatch fails compilation; no address-based or runtime family lookup. |
| `DirectoryConfig::{new,default}` | Owns positive immutable shard and waiter limits; `new` validates the power-of-two hash mask before any affine application or runtime inputs transfer. | `Debug`, `Copy`, `Clone`, equality, `Default`; `DirectoryConfigError::InvalidShardCount` returns both original limits. Read-only accessors expose each limit. |
| `DirectoryError` | Directory rejects exhausted non-reusable activation or dispatch IDs. | `Debug`, `Display`, `Error`; ID exhaustion returns the exact command. |
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
| `EntityRuntime::new`, `admit`, `shutdown`, `passivate` | Bombay native Entity runtime owns directory and task group; construction consumes validated configuration without an error branch. Advanced callers may host it. | `Clone` shares the runtime state, not shutdown result. Admission returns exact command failure; shutdown returns `EntityShutdown` after owned tasks join. |
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
