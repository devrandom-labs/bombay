# Shutdown authority research for EXEC

**Resolution note (2026-10-01):** ARC-001 now uses
`ApplicationLifecycle<P, E>` with a typed weak `ControlSender<E>` and keeps
`ActorRef<P>` as a protocol-indexed delivery reference. Child and Entity
shutdown borrow their existing exact control senders. The 0.17.0 contract and
erased representation below are the dated pre-change experiment; current
selection and gates are recorded in `../../prd-backlog/status.md`.

Date: 2026-09-29. Gate: `DG-SHUTDOWN` remains **open**. This record is evidence
for `WP-SHUTDOWN-DESIGN`, not an accepted API or permission to edit production.
It covers `XO-24` through `XO-30` and the proposed `EV-13` through `EV-15` and
`EV-26` witnesses in [the EXEC PRD](../execution-ownership.md).

## Selected contracts and ownership

The workspace lock selects registry `bombay-behavior 0.17.0` (checksum
`9cbbddbb53a81e4cba528a42ff4c9c84b3a0afb568c8af8590c566c3e394f48e`),
`bombay-behavior-actors 0.17.0` (`81acddfd6d22b6ca993553a0707716f136d8df11bd654ce86c5c9ebd24a124f0`),
Behavior Macros `0.12.0`, Address `0.2.0`, Communication `0.1.2`, and the
Timers `0.1.0` patch at `13e884da7ab41781f52337b0038060e375b00ee0`.
Both registry Behavior packages' `.cargo_vcs_info.json` identify release
revision `435560ce7bea8ad3330ee2d42e5034f837a80602`. The complete
`AGENTS.md` at that revision was read. The sibling checkout's HEAD is not the
selected contract. The isolated compile experiment below depends directly on
Behavior, Behavior Actors, and Communication at these exact versions; it does
not depend on Timers. Its independent lock selected some different transitive
versions, including `syn 3.0.6` versus the root lock's `3.0.5`. Its evidence is
limited to the named public shutdown types, not a workspace integration gate.

| Owner | Relevant selected fact |
| --- | --- |
| Behavior | `Behavior::Protocol` fixes public message identity; `Behavior::Event` remains an independent associated type. `EndpointAddress::Established<P>` is indexed by `P`, not by implementing Behavior `B`. `EstablishedActor<B>` stores an `EstablishedRecipient<B::Protocol>` and a `PhantomData<B>`; interpretation yields the protocol endpoint. |
| Behavior Actors | `ShutdownRequested` enters `B::Event` through `InjectEvent<ShutdownRequested, Path>`. `ShutdownChild<B, Occurrence>` names the child occurrence. `ShutdownEstablished<B, Path>` retains `B` in its request type, but its `InterpretEstablishedShutdown<B, Path>` method receives the protocol endpoint from `EstablishedActor<B>`. `StopOnShutdown<B>` adds another event layer while preserving `B::Protocol`. |
| Communication | `ControlSender<E>` is concrete over the complete event type `E`; `send` returns the rejected `E` if its consumer is gone. `MailboxOwner<U>::close_admission` consumes the only user-admission owner. A racing send is admitted before closure or returns its original payload. |
| Address | `AddressSpace<A, E>::resolve` returns an exact endpoint snapshot. Releasing a `Lease` and reclaiming the same address never changes an earlier snapshot. Address's registration identity is not a substitute for the actor event type. |
| Observe | Bombay's termination observation is captured for one incarnation; a later address occupant has a different publication. |
| Timers | Shutdown does not require a timer or generation token; adding one here would duplicate the selected queue's responsibility. |

The existing Bombay source realizing these facts is `local.rs::{ActorRef,
LocalEnvironment, ActiveLocalEnvironment}`, `launch.rs::{OwnedActor,
RootActor}`, `child_bindings.rs::CreationBinding`, `application_runtime.rs::{
ApplicationHandle, ApplicationLifecycle, InterpretEstablishedShutdown,
InterpretItem<ShutdownChild>}`, `entity/bombay.rs::NativeEntityLease`, and
`address.rs::EndpointAddress for MailAddr`. Existing `local.rs` shutdown tests
verify repeated/stopped classification and basic actor shutdown. They do not
prove a static replacement. Communication's lane tests and Address's snapshot
tests cover their primitive laws. The [capability guide](../../runtime-capability-interfaces.md),
[module map](../../module-boundaries.md), [Driver law](../../driver-law.md), and
[Driver tests](../../driver-test-strategy.md) were checked for owner boundaries.

## Exact authority equation

For a concrete installed behavior `B`, write `P = B::Protocol` and
`E = B::Event`. The existing graceful shutdown request needs all of:

```text
exact incarnation endpoint:  ActorRef<P> (mailbox, weak admission, termination)
typed event ingress:         ControlSender<E>
static injection proof:      E: InjectEvent<ShutdownRequested, Here>
effect order:                check terminal -> acquire control authority ->
                             close that incarnation's admission -> send E
acceptance result:           Ok(()) or AlreadyStopping/AlreadyStopped
later result:                one exact incarnation terminal observation
```

Request acceptance does not prove a Behavior stopped. Owner-forced retirement
is a different operation. In Bombay's current method, admission closure wins
before the accepted shutdown event is sent. Its weak admission owner prevents
a stale endpoint from reopening a user lane. A `ControlSender<E>` borrowed from
another actor, even one at the same address, is not a valid match: endpoint,
admission, control sender, and termination must all name the **same installed
incarnation**. No address lookup may reconstruct that match after the fact.

| Consumer | Existing ownership product | Static disposition under investigation |
| --- | --- | --- |
| Root application | `ApplicationHandle<P>` exposes `ActorRef<P>` and projects `ApplicationLifecycle<P>`; `spawn_root_with` currently discards its available `ControlSender<E>` after preparing the Environment. | The root execution owner can derive a typed **weak** control capability from the Environment's existing strong `Arc<ControlSender<E>>`, then project it alongside the exact root endpoint. The public lifecycle must not keep the control lane open merely by being cloned. That projection must carry `E` or concrete `B` in its Rust type; `P` alone cannot determine it. Application work and HTTP signatures must propagate that type without exposing Driver ownership. |
| Direct child | `CreationBinding<Child, Root>::Established` already co-owns `ActorRef<Child::Protocol>`, `ControlSender<Child::Event>`, and the task, keyed by one typed occurrence and creation ID. | Borrow those exact fields for `ShutdownChild<Child, Occurrence>`. No new map, wrapper or second control channel is needed for this scoped request. Close admission before sending the injected child event; preserve `CreationBinding::Rejected` and missing-binding settlements. |
| Native Entity | `NativeEntityLease<D>` owns `OwnedActor<D::Behavior, _>`; that actor already contains both the exact endpoint and `ControlSender<D::Behavior::Event>`. | The Entity retirement code can use those co-owned concrete fields. Its family retirement/forced-retirement policy remains the Entity owner's decision. |
| External actor | `ExternalActor<P>` owns a user mailbox and affine receiver with control type `Never`; its `ActorRef<P>` is made by `ActorRef::external` with no Behavior shutdown target. | No Behavior shutdown authority exists. External receipt, user-admission closure and receiver drop remain its own law. A public `ActorInterface<Api>` cannot gain lifecycle authority by projection. |
| Generic established actor | `ShutdownEstablished<B, Path>` supplies `B` statically, but `EstablishedActor<B>::interpret` yields `<MailAddr as EndpointAddress>::Established<B::Protocol> = ActorRef<B::Protocol>`. | This endpoint contains no `ControlSender<B::Event>` once the erased field is removed. No existing passed value or proven closed binding in this generic interpreter supplies a matching concrete sender. Gate remains open. |

The current `ActorRef<P>` stores `Option<Weak<dyn ShutdownControl>>`.
`LocalEnvironment` and `ActiveLocalEnvironment` retain the corresponding
`Arc<dyn ShutdownControl>`. This virtual call is precisely how a reference
indexed only by `P` currently reaches a sender indexed by `E`; moving it to a
callback, another trait object, `Any`, a serialized envelope, or raw pointer
would preserve the same erasure. The existing `ActorRef<P>` public method is
already private, and the compile-fail fixtures deny shutdown on a messaging
reference, direct shutdown on `ApplicationHandle`, and lifecycle on
`ActorInterface`. Preserve those denials.

## Ordinary Rust comparison and falsifying experiment

The first candidate is direct concrete composition: a private function borrows
the exact `ActorRef<P>` and `ControlSender<E>` from the same owning product and
requires `E: InjectEvent<ShutdownRequested, Here>`. Root lifecycle access
would first upgrade a typed weak view of the Environment-owned sender, as the
current erased lifecycle does; closure is never interpreted as a new sender
authority. This uses no new protocol, macro, dynamic dispatch, or special
Behavior API. Root, direct child, and Entity have identifiable construction
sites. A named restricted lifecycle value may be justified at the public root
because it grants a distinct capability; the existing `ApplicationLifecycle`
is already that value. Its generic index, not its existence, is the open
design question.

The second candidate would make `ActorRef<P, E>` carry the concrete sender.
It cannot be substituted for Bombay's current
`EndpointAddress::Established<P> = ActorRef<P>` while preserving two Behavior
implementations of the same `P` with different `E`; the owning associated type
has no `B` or `E` parameter. A default generic argument, associated-type
projection guessed from `P`, or a wrapper around `ActorRef<P>` merely hides
that missing index. A third candidate, a protocol-indexed shutdown channel,
adds independent lifecycle ingress and is prohibited by `XO-30` unless an
explicitly verified owning-contract change selects it. A runtime registry of
`B`-specific controls would duplicate Address's exact endpoint ownership and
requires an unproven heterogenous lookup; it is not an accepted workaround.

The isolated `/tmp/bombay-shutdown-design.Y0VS9a` crate used the selected
public APIs with `WorkerProtocol = MessageProtocol<RuntimeAddr, ()>` and two
concrete behaviors:

```rust
type OuterWorker = StopOnShutdown<Worker>;
type NestedWorker = StopOnShutdown<OuterWorker>;

// Both satisfy Behavior<Protocol = WorkerProtocol>.
// Their Event types are different closed event sums.
let outer_actor = EstablishedActor::<OuterWorker>::issued(endpoint);
let nested_actor = EstablishedActor::<NestedWorker>::issued(endpoint);

// The two actual ControlSender<B::Event> values each accept a
// statically injected ShutdownRequested through their own event lane.
```

Executed with the pinned shell:

```sh
nix develop -c cargo test --locked --offline \
  --manifest-path /tmp/bombay-shutdown-design.Y0VS9a/Cargo.toml --lib
```

Result: one test passed. A deliberate variant returning
`ControlSender<OuterWorker::Event>` where
`ControlSender<NestedWorker::Event>` is required fails with `E0308`:
`EventLayer<ShutdownRequested, User<...>>` is not
`EventLayer<ShutdownRequested, EventLayer<ShutdownRequested, User<...>>>`.
The failing command was the same test command with `--features wrong_event`;
it exited `101` for the intended type mismatch. This proves the two types are
distinct and that the existing `EstablishedActor<B>` can hold the same
protocol endpoint representation for either `B`. Issuing both in this type
experiment is not a claim that both were truthfully installed at once: the
upstream power-user constructor delegates that issuance proof to its caller.
It does **not** prove that the candidate Bombay API compiles or meets the
complete admission/retirement law.

## Unresolved contract and smallest falsifier

The unresolved case is a `ShutdownEstablished<NestedWorker, Here>` effect
created from an exact `EstablishedActor<NestedWorker>` that the current actor
does **not** own as a direct child. Its interpreter receives only
`ActorRef<WorkerProtocol>`. Another installed `OuterWorker` may use the same
protocol. Removing the erased target leaves no lawful way to pick the nested
actor's `ControlSender<NestedWorker::Event>` from that endpoint while retaining
the generic exact-established shutdown contract. Returning
`AlreadyStopping`, routing through a logical address, or calling a control
sender belonging to the other behavior would each falsify `XO-25` or `XO-26`.

The coordinating reviewer must establish one of these outcomes before closing
`DG-SHUTDOWN`:

1. Demonstrate a complete, non-erased in-repository construction for **every**
   `ShutdownEstablished<B, Path>` target permitted by the selected upstream
   contract, including unrelated Behavior implementations of one protocol,
   wrapper nesting, stale endpoints, and targets outside direct-child custody;
   or
2. Record a precise upstream change so exact actor installation/transfer
   carries a `B`-indexed lifecycle authority independently of the `P`-indexed
   delivery endpoint, with no executor object in the pure Behavior fold. The
   selected Behavior and Behavior Actors contracts, their producers, and
   downstream interpreter must agree before Bombay migrates.

The second item is a required **capability equation**, not approval of a
specific new associated type or public constructor. Compare ordinary
associated types, a concrete capability product, and narrowed request scope
upstream; test issuance, static denial, exact rejected-event return, and
source compatibility. If narrowing is chosen, prove every current producer
and ordinary composition remains expressible. Do not silently delete generic
established shutdown merely because no present Bombay example emits it.

Coordinator source review of the selected release narrows the upstream
impact: `Behavior::ChildCreationOutcome<C, Occurrence>::into_actor()` currently
extracts the `EstablishedCreation<C::Protocol, Occurrence>` **protocol
recipient** and calls `EstablishedActor::from_recipient`; the declared-role
`EstablishedCreation::into_actor<Parent>()` does the same. Thus adding a
behavior-indexed authority only to `EstablishedActor<B>` cannot work unless
the committed creation result also transfers the matching incarnation's
authority, or a separately proven static installation path supplies it. The
source hashes for this reviewed release are `4dc0a8bb3806436a935f8e882b643682ccada7cb4fb2959b6731eff59c8dabe0`
for Behavior `actor/addressing.rs` and
`7017a85ef1f222f21f952a1bb8ce1a85b0fad5dc919288d7677c4854c22a224f`
for `actor/creation.rs`. The public `EstablishedActor::issued` constructor is
explicitly a power-user installation assertion, so its truthful issuance
rules and the distinction between message-only and lifecycle authority also
need a deliberate decision. No specific upstream representation is accepted
by this review.

Before acceptance, add the smallest end-to-end failing regressions for an
unrelated exact established target, the two-behaviors-one-protocol case, and
address reuse. Verify an old capability cannot shut down a replacement even
when the logical address is equal; repeat shutdown and termination publication
under debug and optimized builds. Race user delivery against admission closure
and assert the complete accepted prefix or exact move-only rejected payload.
The original erased implementation must fail a deliberate static-denial
witness, and the selected replacement must preserve runtime outcomes. The
existing primitive tests alone do not satisfy these Bombay witnesses.

No production source, shared ledger, or retained tests were edited in this
research package. Dependency edge: generic established shutdown blocks
`DG-SHUTDOWN` and the static shutdown implementation work; root/child/Entity
experiments can continue without claiming the gate accepted.

## Selected 0.20.0 nested-path correction (2026-10-02)

The historical erased-authority blocker above is resolved by the selected
Core's `EndpointAddress::Installed<B>` and Bombay's existing
`InstalledActor<B>`. Fresh inspection found a different, smaller blocker:
Actors permits a generic `ShutdownEstablished<B, TargetPath>`, while Bombay's
installed ingress and interpretation are restricted to `Here`.

The isolated candidate and user-authorized 38-path checkpoint are recorded in
[EXEC section 18](../execution-ownership.md#18-nested-shutdown-checkpoint-2026-10-02).
The candidate changes only the existing concrete ingress and implementations
to forward TargetPath. It adds no lifecycle owner, public type or policy.
Original exact fixtures fail E0277 in debug and optimized builds; the repaired
fixtures pass, with one nested event and the complete ordered accepted,
already-stopping and already-stopped acknowledgment trace. The existing Here
regression passes both profiles. Source, lock, log and patch hashes are in the
frozen receipt linked by section 18.

Author `/root/contract_inventory`; independent reviewer
`/root/observation_research` verified all receipt hashes and independently ran
the two nested witnesses in both profiles through pinned Nix. It found no
material defect in the narrow path correction. This supports an XO-30
subdecision; **DG-SHUTDOWN remains open**. The constructed installed capability
proves actual control ingress, not committed Application/child installation or
the full actor termination law. Exact rejected IDs and complete acknowledgment
reasons do not by themselves prove every rejected target/ingress value's custody.
Those broader witnesses, stale-address authority, same-protocol distinct
behaviors, admission races and static denials remain required before retention.
