# Existing abstraction disposition evidence

**Frozen research snapshot:** ARC-011 on 2026-10-01 retained
`ProjectedTask` as the concrete typed origin boundary, moved activation-task
settlement into the spawned actor task, and shared root/owned setup. Its
current disposition and tests are recorded in the backlog status index; the table
below describes the prior 0.17.0 representation.

**Revision note (2026-10-01):** ARC-001 retained the public lifecycle
projection as `ApplicationLifecycle<P, E>` and removed the erased shutdown
path from `ActorRef<P>`. The table below records the earlier 0.17.0 source
audit; the current selected contract and verification are in
`../../prd-backlog/status.md`. ARC-002 removed the unobservable Address lease
from `ExternalActor<P>`; its allocated origin, exact recipient, affine receive,
and admission owner remain.

**Later ARC-012 evidence (2026-10-01):** `LocalTerminalReports` now owns the
mutable `Unselected | Selected` transaction state and the sole sender of the
selected terminal report. `TerminationPublication` owns its receiver. The
dated candidate below to delete this wrapper by sharing an
`Arc<TerminationSelection<_>>` is superseded; the owning transaction now has
an affine handoff. See the ARC-012 proof in the backlog status index.

Date: 2026-09-29. Work package: `WP-CONTRACT` input for `DG-WRAPPERS` in the
[execution PRD](../execution-ownership.md). Status: **source audit complete;
gate open**. The table records direct ordinary-Rust candidates and exact
falsifiers. It is not approval to edit production or migrate callers. No
compile-only or differential experiment was added in this work package.

The current lock selects registry Behavior and Behavior Actors `0.17.0`; both
archives record Git revision `435560ce7bea8ad3330ee2d42e5034f837a80602`
in `.cargo_vcs_info.json`. Their complete revision `AGENTS.md` was read.
Address is `0.2.0`, Communication `0.1.2`, Tokio `1.53.1`, and patched Timers
`0.1.0` is at `13e884da7ab41781f52337b0038060e375b00ee0`. The current
Driver, capability, module and ledger documents and the relevant source/tests
were inspected. The source has not been frozen for a mechanical move; the
coordinator must recheck hashes and two-consumer experiments before accepting
this gate.

## Decision method

For each construct, the record asks: (1) what current value or authority does
it alone own; (2) what event/effect transformation does it alone perform; (3)
whether direct existing composition expresses that law; (4) what machinery a
change would delete or subsume; and (5) which two real consumers or materially
different paths prove the construct is needed. A constructor and its sole
caller are **not** treated as two independent semantic consumers. The
disposition `retain` means the present semantic responsibility is real; its
eventual file and exact representation remain subject to the other gates.

The ordinary-Rust comparison must compile the same root application, declared
child, dynamically created child, and native Entity actor where the construct
participates. It must preserve exact typed terminals, static denials, action
order and rejection payloads. An accepted deletion requires a differential
trace of both relevant consumers; a grep result alone is evidence of
duplication, not proof of semantic equivalence.

## Contracts with distinct responsibility

| Construct and source | State/authority and transformation | Two real consumers or paths | Direct-expression result and disposition |
| --- | --- | --- | --- |
| Engine `Driver` and `Environment`/`ActiveEnvironment` (`bombay-engine/src/driver.rs`, `environment.rs`) | Driver owns causal turn/settlement progression; the affine Environment owns prepared-to-active acquisition, interpretation, and residual return. | Bombay local actor launch and native Entity launch both instantiate the same `Driver` through `LocalEnvironment`; pure Engine tests exercise an independent host. | Retain. Existing composition is this contract; deleting it would require another actor loop. No EXEC cleanup credit. A change requires a separate Engine-law falsifier. |
| `ApplicationBehavior<Root, Product>` (`application_runtime.rs`) | Owns root plus one consumable declared-child product. Initialization appends declared births after root initialization births; transition delegates root and preserves birth shape; error distinguishes root failure from double initialization. | `Application::child` staging and declared-root terminal projection consume it; the differential must compare a root that also dynamically creates children. | Retain semantic composition. A bare `Root` cannot represent declared child initialization and complete terminal custody. No new wrapper should imitate it. Verify initial birth order and typed error through both consumers. |
| `ApplicationLifecycle<P>` (`application_runtime.rs`) | Restricts an exact root `ActorRef` to shutdown request and termination observation; it confers no exported messaging or topology access. | `ApplicationHandle::lifecycle()` for application work and Axum's router/graceful shutdown; `actor_interface_has_no_lifecycle` compile failure protects the converse. | Retain unless a direct capability product passes the same static denial and usability witness. Its only two methods forward, but the restricted authority is distinct; field count does not decide deletion. |
| `ActorInterface<Api>` (`actor_interface.rs`) | Holds explicitly exported `Api` plus application address allocation authority; it grants fresh external reply actors without lifecycle control. | Public application callback/router delivery and Entity or exact-recipient `ExternalTarget` values in the exported API; `ActorInterface::external` is an additional real address consumer. | Retain. Bare `Api` loses external address allocation; `ApplicationHandle` would grant broader authority. Keep compile denial of `request_shutdown`. No extra interface wrapper. |
| `ExternalActor<P>` (`actor_interface.rs`) | Owns claimed address, affine mailbox receiver, admission, lease and termination publisher; cloneable recipient is separate producer authority. | Ordinary command/reply application examples and `tests/actor_interface.rs` receive/closure tests; `external_actor_receiver_is_affine` denies cloned receive authority. | Retain. Direct `ActorRef` is a sending endpoint and cannot own an affine external receive consumer. Preserve exact rejected messages and lease retirement. |
| `ActorOrigin<Owner, Role>` and `ActorRetirement<B, Root>` (`terminal.rs`) | Origin holds exact address, optional creation nonce and static owner/role; retirement holds final concrete Behavior, complete settlements, ingress and ordered descendants or a distinct failure. `ProjectTerminal` performs a typed total lift. | Direct/declared root projection and child role projection; terminal custody tests assert role and descendant structure. | Retain semantic types. Plain `MailAddr` or an erased terminal loses role, provenance, failure and descendants. DG-PROJECTION may move when the lift runs; it cannot erase what is lifted. |
| `ActionInterpreter<Capabilities>` (`interpret.rs`) | Owns one concrete capability product and action-scoped terminal-report selection; it commits ordered creations/sends, offers source settlements and retires capabilities. | Application root/child launch and native Entity launch both instantiate it. | Retain the one ordered interpreter law. A direct call to Behavior's `Actions::interpret` alone omits report begin/finish disposition and retirement. If relocated, compare full action settlements and terminal selection in both paths. |
| `ChildBindings<Owner, Root>` (`child_bindings.rs`) | Existing Behavior `ChildOccurrences` product with runtime `ChildBinding` leaves owns creation status, endpoint/control, creation order and child task custody per structural occurrence. | `ApplicationCapabilities::establish_child` and its delivery/observation interpretation, plus ordered `RetireChildTasks` and native Entity hosting. | Retain one canonical product. No flat map can preserve heterogenous occurrence-specific static selection without proving another equivalent typed product. DG-TASK/DG-PROJECTION may reshape the stored task, but not erase occurrence law. |
| `ActorSpace<P>` (`launch.rs`) | Public alias to Address's exact `AddressSpace<P::Addr, ActorRef<P>>`; no new state or transformation. | Advanced `App`/`Hosts<P>` composition and `ChildBinding`'s per-protocol actor space; `LocalPeerObservations` resolves logical peers through it. | Retain the canonical public spelling while the advanced API exists. It abbreviates a truthful cumbersome type. A new endpoint table is forbidden. |
| `FactQueue<A,E>` (`observation.rs`) | Holds independent captured termination waits and static injection functions; `next()` multiplexes peer/child completion into one actor Environment. | `ObservePeer` and `ObserveChild`/shutdown-child paths, with independent observers of the same target. | Retain the queue's semantic concern; rename in the selected module layout to observation language. DG-OBSERVATION must first prove whether exact ID relationships can join this same authority. Direct `Vec` alone lacks the public operation/typed-injection owner needed by both consumers. |
| `LocalTimers<E>` (`time.rs`) | One shared view of a single Timers `TimerQueue`; translates `ScheduleAt`/`ScheduleAfter` into typed `TimerElapsed`, checks relative-deadline overflow and maps exact schedule errors. | Action interpretation schedules and the active Environment queries deadlines/pops due events; timer tests cover replacement generation. | Retain until an ordinary borrowing experiment shows how the interpreter and Environment can share the one queue through current affine ports. `Arc<Mutex>` is synchronization across their views, not a second timer queue. Removing it must not move timing into Behavior or add unsafe/dynamic context. |
| `EntityTaskGroup` (`entity/runtime.rs`) | Owns family admission state, active count, idle epoch, affine shutdown claim, drain phase and exact task failures. | Entity task `begin`/guard completion and family shutdown/drain/reporting paths; multiple Entity lifecycle tests observe these. | Retain its separate family law. Similar `JoinHandle` storage to actor tasks does not permit a universal task-group trait. DG-TASK and WP-ENTITY still need dropped-caller custody evidence. |

The paired paths above identify distinct responsibilities, not permission to
add more generic parameters. Exact names and visibility after file migration
remain a DG-MODULES decision.

## Synonyms and forwarding candidates

| Construct and source | Present authority/transformation | Consumers and direct existing expression | Machinery that could be deleted; decision still needed |
| --- | --- | --- | --- |
| `OccurrenceBindings<Owner, Root>` (`child_bindings.rs`) | Exactly `type OccurrenceBindings = ChildBindings`; it adds no value, invariant, or transformation. | `ApplicationCapabilities` construction/child interpretation and native Entity host both use it. Substitute `ChildBindings<Owner, Root>` in those two distinct paths and compare diagnostics. | Delete the alias and every import if compiled signatures and typed errors remain unchanged. This is the strongest source-level deletion candidate; its canonical name should be `ChildBindings` as the PRD specifies. |
| `LocalAddresses<P>` (`launch.rs`) | Exactly `type LocalAddresses = ActorSpace`; no separate authority or transformation. | Launch functions and `LocalPeerObservations` use it. Substitute `ActorSpace<P>` in both paths, plus focused tests. | Delete the private alias/imports if public type inference and `Hosts` denials stay intact. Keep the public `ActorSpace` spelling. |
| `LocalTerminalReports` (`reports.rs`) | Stores only `Arc<TerminationSelection<MailAddr>>`; `begin`, `finish` and `report_outcome` forward to existing selection methods, with identity conversion to `Termination`. | `launch.rs` constructs it and `ApplicationCapabilities` is its one operational consumer through `TerminalReportTransaction` and `ReportTerminalOutcome`. No second independent report policy was found. | Candidate delete: store/use the same `Arc<TerminationSelection<MailAddr>>` directly in the capability product, call its `begin`/`finish`/`select`, and preserve publisher sharing. This deletes the wrapper and forwarding methods, not `TerminationSelection` or the action transaction. Differential test retain/discard and first-report-wins through root and Entity. |
| `HostedActorSpaces<N>` with `ResolveLogical<Target>` (`topology.rs`) | Wrapper forwards `Hosts<P>`; its sole extra operation resolves from `Hosts<Target>::space()` and clones the resulting `ActorRef`. It stores no new state. | `LaunchSystem::launch_with`/HTTP and native Entity hosting both wrap their host product. Logical delivery is interpreted through one `ResolveLogical` bound in `ApplicationCapabilities`. | Candidate delete both wrapper and private trait: have the concrete logical-delivery interpreter require `Hosts<Target>` and directly call `.space().resolve(&address)`. Compare both App and Entity compile paths, missing-host compile denial, exact payload return and generation capture. A blanket trait implementation is unnecessary if direct code compiles. |
| `FactState<A,E>` (`observation.rs`) | Wraps only `Vec<PendingFact<A,E>>`; no independent state, phase or transformation. | Only `FactQueue` reads/writes it, so there are **not two consumers** justifying a named state. | Candidate replace `Arc<Mutex<FactState>>` with `Arc<Mutex<Vec<PendingFact>>>`. Keep the queue's owning operations. DG-OBSERVATION may add a genuine relation alternative, so decide after its selected representation. |

For `LocalTerminalReports`, `HostedActorSpaces`, and `FactState`, the source
supports the absence of a unique current state. It does not prove that a
mechanical deletion preserves every trait bound and diagnostic. The gate stays
open until the direct-expression attempts compile and their two real paths
produce equal traces. The two aliases are exact Rust type identities, but
public diagnostics and imports still need the scoped compile check.

## Task owners deferred to their semantic gates

| Construct | Unique current authority and two paths | Exact dependency and deletion condition |
| --- | --- | --- |
| `OwnedTask<B, Descendants>` (`launch.rs`) | Holds actor JoinHandle and owner-cancellation sender. Root finish/retire, child creation, and native Entity retirement use the same owner. | Retain authority; DG-TASK must fix drop and join transfer before changing representation. Delete only machinery whose replacement preserves normal wait versus forced retirement and exact residuals. |
| `ProjectedTask<B, Root>` (`launch.rs`) | Holds projected child JoinHandle, cancellation sender and static Behavior marker. Declared child and dynamic child bindings both use projected terminals; `RetireChildTasks` joins them in creation order. | DG-PROJECTION must measure prompt post-Driver settlement and panic/sibling custody. A projection task is not justified merely by generic type shortening. It may be deleted only after equivalent progress and role-projected terminal traces. |
| `ActivationTasks<E>` (`local.rs`) | `JoinSet<Result<(), E>>` owns spawned external completion and exact rejected-event recovery during residual settlement. Current production has **one spawn site**, exact established observation in `application_runtime.rs`; root and child residual paths both settle it. | If DG-OBSERVATION moves exact relationships into the actor's existing queue, there is no current production producer left. Then remove this type, capability/residual fields and `settle_activation_tasks` if the integrated custody law passes. Keep it only if a separately evidenced external task still needs independent progress and exact return. Future hypothetical work is not a retention reason. |

The actor and Entity task groups have different authorities. Deleting one
cannot be inferred from the other's representation or task count.

## Tests and unresolved decisions

The coordinator should accept only after these bounded experiments:

1. Compile-only substitution of both aliases in application and Entity paths;
   verify public examples and the missing-host compile diagnostic.
2. Differential test direct `TerminationSelection` against the current report
   wrapper: two reports in one action, a following continuing action, a stop
   action, root and Entity terminal publication, and owner cancellation.
3. Differential test direct `Hosts<Target>::space().resolve()` against
   `HostedActorSpaces::resolve_logical()` for present, absent and retired exact
   generations, with rejected payload ownership and an Entity host product.
4. Compare `FactQueue` using a direct pending vector and the selected exact
   relationship representation after DG-OBSERVATION. Assert peer/child
   multiplicity, event order, cancellation/reuse, and no extra allocation
   promise beyond what is measured.
5. Compare direct timer borrowing with the current one-queue shared view. If
   the current affine Environment/interpreter interface cannot express direct
   borrowing, retain `LocalTimers` with that explicit reason; do not change
   Engine merely to delete a mutex.
6. Use DG-TASK and DG-PROJECTION's controlled pending-work/panic witnesses
   before deciding any task-owner representation. Count tasks and check exact
   terminal, ingress and returned-event custody.

The proposed deletions add **zero public types**. Net lines and file count
remain unmeasured because no production edit was made. The current working
tree contains unrelated concurrent changes, so the coordinator must measure
the complete tree and the task-local delta at the next checkpoint. The gate
is open until these witnesses are recorded and the frozen symbol/file map in
DG-MODULES selects exact destinations.

## Current-source reconciliation (2026-10-02)

The tables above are historical hypotheses, not instructions to recreate
removed code. Fresh inspection at canonical Bombay 4e009b9 selects Core/Actors
0.20.0 and Communication 0.1.3. `OccurrenceBindings` and `LocalAddresses` are
already absent. `TerminationObservations` directly owns its pending Vec; the
old FactState/FactQueue and shared observation mutex are absent. `LocalTimers`
directly owns one TimerQueue, with no Arc or mutex. Their old deletion/borrowing
experiments therefore have no current subject. Preserve these completed fixes
and their regressions rather than claiming new EXEC reduction.

Current `LocalTerminalReports` owns the selected report transaction and its
affine sender; its old forwarding-only removal proposal is superseded.
`HostedActorSpaces` and `ResolveLogical` still exist and need the prescribed
direct-Hosts comparison for application and native Entity consumers.
ActivationTasks has three production spawn sites: worker activation, worker
preparation and established observation. Moving observations alone would not
remove its remaining producers. Current task/failure laws must cover all three.
No wrapper gate is accepted
from this source reconciliation.

Current source SHA-256 values: topology.rs
`7e5d9181a53d34995e5cfbc9b44e0562bd154afe0812a23589d898199db90ead`;
time.rs `62251d2c58943d353ca02d075af0d593783226ada219bc734c23a6e3661c72b0`;
reports.rs `9cd827bcb64bfa2ab217ef4da7d4158447b8b752086e8ff3fc1ce1aca44c55bf`.
Other current source anchors are in verification.md. This is read-only evidence;
production, tests and public types remain unchanged.

## Direct Hosts comparison on the selected release (2026-10-03)

`bombay-direct-hosts-0_rmhopj`, based on canonical 1feefcf, selects published
Core/Actors 0.21.1 and Macros 0.13.1 under pinned Rust 1.99. Ordinary
`hosts.space().resolve(&address)` compiles in actual application and native
Entity consumers without another wrapper or support type. Existing wrapper
delivery is compared with that direct expression.

Application delivery captures the exact endpoint before awaiting capacity.
An admitted send stays on its old generation after lease release and a fresh
claim at the same address. Unknown and closed destinations return the whole
original delivery and Vec allocation. Native Entity hydration, activation,
binding acknowledgment, fencing, delivery and graceful retirement return the
complete pure actor ledger, original allocations and terminal residual lanes.
Both resolution routes disappear after retirement; captured endpoints reject
the exact payloads. Native address reuse is not covered by the Entity fixture.

Receipt `dd63bde0c90e0dc8c78913f40e20d3e572b1b41b05bcf5914209010321fae97f`;
patch `fa131bbcb789bd14d055bceab1afe7ba91ee90ce3545708626710402411e4187`.
Two approved test modules: +373 / -6 / net 367; production/public types zero.
Independent nonauthor review
`302f67838c581fdd6c351090d2ac2d7672f87c6f41ef0d4cd7efaa7ba95eac4a`
authenticates all 345 sources and 74 artifacts, reads complete fixtures and
selected Address/Communication contracts, and accepts bounded research only.
Coordinator separately authenticated those sources and artifacts, read the
complete final patch and reran both positives in a fresh isolated target under
pinned Nix: two tests pass in each profile. Receipt
`414badbf688b8079dd79970df82da3b0f9c3c99e0bc0a516e2fc6ca8eba23afd`
binds those commands/logs. No coordinator mutation rerun or reviewer Rust
execution is claimed.

Author positives and restorations pass two tests in each pinned-Nix profile;
formatting and strict package all-target Clippy pass. Re-resolving an admitted
send or copying its payload fails the intended runtime assertions in both
profiles. Missing Hosts produces E0277; wrong protocol/payload produces E0308.
These are counterfactual inversions and static denials, not original defects.
Production bound migration, wrapper deletion, full application/birth consumers,
noncooperative Entity retirement, forced family cleanup and metrics remain
open. topology.rs is unchanged and outside the approved edit scope.
DG-WRAPPERS is not accepted by this bounded comparison.
