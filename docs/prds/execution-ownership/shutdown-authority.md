# Shutdown authority research for EXEC

**Resolution note (2026-10-01):** ARC-001 now uses
`ApplicationLifecycle<P, E>` with a typed weak `ControlSender<E>` and keeps
`ActorRef<P>` as a protocol-indexed delivery reference. Child and Entity
shutdown borrow their existing exact control senders. The 0.17.0 contract and
erased representation below are the dated pre-change experiment; current
selection and gates are recorded in `../../prd-backlog/status.md`.

Historical experiment date: 2026-09-29; the gate was open for that contract.
Current signed acceptance is recorded below. The historical experiment was
evidence for `WP-SHUTDOWN-DESIGN`, not production permission.
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

## Communication owning correction and delivery boundary (2026-10-02)

The authorized owning candidate is now frozen against Communication main
`e1017dc4da7e8d3ca014757d2e7308fa4426eb2b`; its source matches the selected
0.1.2 source before correction. The private admission state is
`Open(UserSender<U>) | Closed` behind a standard Mutex. A pre-close acquisition
keeps its existing sender; owner closure forbids every new acquisition even
while that sender survives. The displaced sender drops outside the lock, and
the lock and promoted admission Arc are released before awaiting. Raw sender,
ring, control, backpressure and consumer closure semantics stay with their
existing owners. No public type or second Bombay admission gate is added.

Eight mailbox-retirement tests cover exact post-close rejection, pre-close
completion, unpolled send rejection, cancelled pending payload release,
destructor reentry, Send/non-Sync payloads and closure races with complete
terminal traces. A separate private law proves owner closure despite a promoted
reference, and an allocation witness measures construction and steady sends. An actual Bombay ActorRef witness also
fails on original source and passes with this exact owning source in both
profiles; the temporary patch does not claim a published selection.

The same-law RwLock comparison passes correctness witnesses but trades a modest
single-producer gain for worse measured contention. Mutex single-producer
median is 13.232 microseconds per 1024 sends, versus 12.086 for RwLock in
the fresh same-law comparison. Its mixed 20,000-user/20,000-control contention
run (including setup, spawn/join, closure and drain) measures 1.3779 milliseconds
for Mutex and 1.6827 for RwLock. The earlier paired original-to-Mutex round
measures 8.9518 to 12.847 microseconds and 1.3417 to 1.4433 milliseconds
respectively. These separate rounds are local measurements,
not latency/fairness guarantees. Mailbox construction adds one allocation;
steady sends and exact rejection remain allocation-free in the owning witness.
The separate-phase atomic comparison rejects a legitimate pre-close grant;
this does not disprove all possible atomic designs. The user accepted this measured cost and authorized proceeding with the
upstream PR on 2026-10-02.

Frozen upstream draft:
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/communication-reviewed-owner-y9mxupb4/owner`.
Whole patch SHA-256:
`09f07a4eb84dc14542e718501d95c64d8d5c794f8a9836667743d2b1f912f564`;
change record:
`69453bfc11f30da521ed59608e6c5d91cd7376cb4d0e1b583b0b0e83137ef7fd`;
final verification receipt:
`37742eca7e3664da7671e6fb8a21600af8b3ec42e0d85fc5f412e567c3035d70`.
Seven owning paths measure production +62 / -13 / net +49, tests/benchmarks
+502 / -5 / net +497, documentation +105 and public types +0 / -0.
The alternative difflib +63 / -14 production measurement has the same net
and source bytes; this complete owning record uses Git's diff. The approved
owning +80 production and +500 tests/benchmarks ceilings remain in force.

Pinned-shell workspace tests, strict Clippy, formatting, all seven owning
aarch64-darwin Nix checks and the complete Loom suite pass. Loom executes one
private law and twelve models with `LOOM_MAX_PREEMPTIONS=3`; it does not promise
exhaustive unbounded scheduling. The receipt records all exact commands/log
hashes. Author `/root/contract_inventory`; independent reviewer
`/root/observation_research` authenticated the final seven paths and five logs,
accepted the narrow correctness/minimization result and preferred Mutex subject
to the user's performance decision. This is not full DG-SHUTDOWN acceptance.

The exact seven-path proposal is committed as
`4e54b33f23d53644681f2241d7afefceb72b4700` and delivered through
[Communication PR #7](https://github.com/devrandom-labs/bombay-communication/pull/7).
[Independent PR-head review](https://github.com/devrandom-labs/bombay-communication/pull/7#pullrequestreview-5397112099)
by `/root/observation_research` verifies the fetched GitHub diff is byte-identical
to the signed whole patch and all seven committed blobs match. It was posted
as COMMENTED through the available author account: this is independent agent
review evidence, not a separate GitHub account approval or external human review.
No owning GitHub approval-count requirement applies. The user explicitly
instructed merging; the PR merged only after all five check entries passed at
that unchanged reviewed head.

| Passing check | Evidence |
| --- | --- |
| Nix PR | [run 37067842494](https://github.com/devrandom-labs/bombay-communication/actions/runs/37067842494) |
| Nix push | [run 37067828040](https://github.com/devrandom-labs/bombay-communication/actions/runs/37067828040) |
| Advisories and Licenses | [run 37067842373](https://github.com/devrandom-labs/bombay-communication/actions/runs/37067842373) |
| Analyze Rust | [run 37067842288](https://github.com/devrandom-labs/bombay-communication/actions/runs/37067842288) |
| CodeQL | [check 111041029382](https://github.com/devrandom-labs/bombay-communication/runs/111041029382) |

Merge commit `c9a39a325bda9ea17a92ad9e6aabc7082e502557`,
merged at `2026-10-02T21:40:46Z`, is independently confirmed by GitHub.
The scratch parent directory retains `owner-pr-merge-receipt.json`, SHA-256
`7fc3c932e14bfeeadc38512039a8ed42a8579e0a9eccb1b5f03736fe7600a8da`,
binding reviewed head, all checks, review URL and merge.
The generated [version PR #8](https://github.com/devrandom-labs/bombay-communication/pull/8)
at `940bfd9fef41a9d8eb49a89d80b5d10bec0ac5e5` changes only the three approved
release paths: workspace version, four matching local package lock versions
and six changelog lines. Source and all other dependency records are unchanged.
[Independent exact-head review](https://github.com/devrandom-labs/bombay-communication/pull/8#pullrequestreview-5397174834)
approves that version-only change with the same transparent agent/account
limits. Both [Nix PR](https://github.com/devrandom-labs/bombay-communication/actions/runs/37068327677)
and [Nix push](https://github.com/devrandom-labs/bombay-communication/actions/runs/37068324825),
[dependency policy](https://github.com/devrandom-labs/bombay-communication/actions/runs/37068327675),
[Rust analysis](https://github.com/devrandom-labs/bombay-communication/actions/runs/37068327669)
and [CodeQL](https://github.com/devrandom-labs/bombay-communication/runs/111042420757)
passed before merge `272a2343187b40615ab26c2d0d2e136010a16e77`
at `2026-10-02T21:48:03Z`.

[Publication workflow](https://github.com/devrandom-labs/bombay-communication/actions/runs/37068966628)
succeeded at that merge. The [0.1.3 release/tag](https://github.com/devrandom-labs/bombay-communication/releases/tag/bombay-communication-v0.1.3)
was published at `2026-10-02T21:48:46Z` and points to that commit. Registry
version 0.1.3 is not yanked; downloaded checksum
`eb0dc8a057efce6e387c9bc24955ffb020b2c138c5ce6b10c1f6c211d32ad268`
matches registry metadata and Cargo's selected checksum. Archive VCS metadata
points to the merge, and all fourteen packaged Rust files byte-match the release
tree. Publication receipt SHA-256
`8880357b5f4a81372643387fa229f2def025c0603059ee6a1bc912ac654055e4`
and source equivalence receipt
`c23a26d0a79e7bcdf52ecb15c83a8f52c41ba30daf0f037db7b4c4a4a0cd1ca0`
are retained in the scratch parent directory. Independent reviewer authenticated
the archive, metadata and all file comparisons; its own direct registry request
failed, so it does not claim an independent fresh registry query. No standalone
archive test execution is claimed where unpublished testkit is required.

Complete delivered owning record: ten authorized paths, production net +49,
tests/benchmarks net +497, documentation +111, manifest/lock net 0, public types
+0 / -0. Record SHA-256
`9174e02044b98c0bf71735a06eb943bb274ad866feaa9945068ce7ff3ce491bc`.
Fresh registry-selected actual Bombay interoperability passes in both profiles;
EXEC section 20 records its independent review, canonical dependency selection
and workspace checks. This owning delivery does not mark EXEC or DG-SHUTDOWN
complete.

## Current selected-contract review (2026-10-03)

The current candidate uses the published Core/Actors 0.21.1 contract at
`5ca96444f0a66e9a013b6989e3e53d345cbabf65`, Communication 0.1.3 and pinned
Rust 1.99.0. The historical protocol-only authority gap above is not the
current contract: `InstalledActor<B>` already carries the concrete actor's
control authority. The remaining correction propagates the existing typed
`TargetPath` through shutdown interpretation, independently of the observer's
acknowledgement path. It adds no authority, channel or public type.

Candidate receipt `285df7c0c82e4faf2fb2b174090babbd45d92051aae2a0c1b3d73b6f5c84eb8b`
binds two already approved files: production +17 / -16 / net 1; tests +831 /
-0; public types +0 / -0. The root coordinator independently authenticated
345 source hashes, 87 artifacts and 343 unchanged baseline files, then read
the complete changes and selected owning shutdown contracts. The actual
factory commitment, transferred capability, root weak control, same-address
replacement and external admission witnesses cover the narrow gate's required
ownership paths. Standard emitted birth, a separate live observer and complete
application execution are not additional DG-SHUTDOWN requirements.

The preliminary review identified four no-op launch callbacks and four
retired-result checks omitting activation tasks. The accepted successor below
corrects both, inspects the repeated original rejection and uses module-scope
imports. The acknowledgement contains
only the shutdown ID, rejection reason where applicable, and protocol marker;
there is no target payload to recover or add. Preliminary coordinator review
`8404b10e39bcbf246773e22375f775e4fc9e60452bbf3124587b4de84e051d74`
records these findings without gate acceptance or canonical source retention.
That preliminary artifact accepts no gate; the corrected source has the
separate signatures and verification below. Tests remain exempt from
production condensation.


## Accepted static shutdown authority (2026-10-03)

DG-SHUTDOWN is accepted for the exact correction in EXEC section 35.
Author /root/contract_inventory's corrected receipt
`ec8e3b3588f367ca62b06d18f9c7d0af5dc56219e63ec920e364ad513e9140d0`
binds the complete patch
`345f8f080d8e3bcaa1f51f93aa4987c9130607013a789df65c2cf2449f886c76`.
Coordinator /root's signature is
`88a267147f9b28a1fd278251b948818655a8f2cf5ddf194e19a657805551c386`;
independent non-author reviewer /root/observation_research's signature is
`6e11212d317fd1918ef295fcac67e3dfd46a9dfc863863e2e680d01ba3dc1e50`.
Both bind the two complete source hashes recorded in section 35, all 345 source
hashes and 71 artifacts; 343 files remain equal to the candidate's baseline.

| Required law | Accepted evidence |
| --- | --- |
| XO-24–25 / EV-13 | Existing ActorRef is messaging-only; concrete InstalledActor<B> and weak ApplicationLifecycle<P,E> retain exact typed control. Two behaviors sharing a protocol have different event types; existing generic Ingress carries the selected shutdown path without erasure. |
| XO-26–27 / EV-14 | Joined old root and installed actor generations cannot close actual replacements at the same address. AlreadyStopping and AlreadyStopped replay does not admit a second shutdown input. |
| XO-28–30 / EV-15 | A pre-close permit completes and its original non-Clone LedgerSubmission drains before closure; first-polled post-close submission returns the original allocation. ExternalActor owns admission/receipt without Behavior shutdown authority. |
| Established capability transfer | The actual factory commits each child and returns its concrete capability; an unrelated interpreter with NoChildBindings shuts down those exact targets. Target and acknowledgement ingress are independently typed. |

Every action lane is observed outside Behavior, all retained activation-task
lanes are checked, and successful child terminals preserve original state and
settlement products. Returned shutdown requests preserve their original ID,
concrete target and ingress. The acknowledgement contains only ID/reason and
its static protocol marker. Original constructor routes are inspected as
inputs; no CreationId or generation is guessed from sequence arithmetic.

All Rust commands use the pinned shell. Author verification runs restored
focused shutdown tests (seven per profile: five new, two inherited preservation
cases), all-feature workspace tests (421 tests / 61 result summaries per
profile), strict workspace/all-target/all-feature Clippy and formatting; all
exit zero. Four static cohorts per profile reject the original Here defect,
wrong event, external shutdown and messaging reference used as installed
capability. Five compiled runtime counterfactuals per profile fail the intended
admission, single-request, original allocation and stale-generation laws. Static
compiler rejection is not counted as a killed semantic mutant.

The coordinator independently reran the exact corrected source in an isolated
worktree with a fresh target. Prefix for the first three commands:
`nix --option eval-cache false develop -c env CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/bombay-shutdown-root-fresh-target`.

```sh
cargo test --locked -p bombay-rs --lib shutdown -- --nocapture
cargo test --locked -p bombay-rs --release --lib shutdown -- --nocapture
cargo clippy --locked -p bombay-rs --lib --tests -- -D warnings
nix --option eval-cache false develop -c cargo fmt --all -- --check
```

Both focused runs pass seven tests; Clippy and formatting exit zero. The
independent reviewer inspected authenticated source and logs and claims no
separate fresh execution. No owning primitive, public method or constructor
changes. Source delta: production +17 / -16 / net 1; tests +922 / -0; public
types +0 / -0. The earlier incomplete receipt and review remain historical.

The admission fixture's manually published observation is separate from the
real joined actor-generation witnesses. Advanced factory commitment does not
claim a whole public Application, standard emitted-birth integration or Entity
family policy. Those are not additional shutdown-gate requirements. Broader
EV-26 denials, other EXEC decisions, combined retained verification, final
minimization and reviewed PR/CI/merge remain open.
