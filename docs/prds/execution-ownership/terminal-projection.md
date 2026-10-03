# Terminal projection decision evidence

**Frozen research snapshot:** ARC-011 on 2026-10-01 retained the one child
projection task after comparing typed origin propagation with erased
projection dispatch. The spawned actor task now settles its own activation
tasks, and abandoned join waiters request owner cancellation. Descriptions
below of `finish_owned_task` and bare sender-drop behavior apply only to the
earlier 0.17.0 snapshot; current evidence is in the ARC-011 retained evidence in the backlog status index.

Status: **open research; the projection task is retained until its independent
timing and panic laws are proved.** This record addresses DG-PROJECTION,
XO-20–23 and EV-10–12, and depends on [task custody](task-custody.md). No
production or test source was edited for this record. The required controlled
capability-completion and projection-panic witnesses have not run.

## Exact source and ownership

The snapshot selects Behavior/Actors 0.17.0 at the documented revision
`435560ce7bea8ad3330ee2d42e5034f837a80602`, Tokio 1.53.1, and the
primitive versions listed in the [parent PRD](../execution-ownership.md).
Relevant source SHA-256 values are `launch.rs`
`5dd7c4b96928d4f3e121b00b3c065485323c4eb7831329cb68c2a80a69e17f9c`,
`terminal.rs` `e60780ea588083f80a6c5c7c49e4c40fb889ebdaa642a5f6e0a98eba3be47111`,
`child_bindings.rs` `294e118bd5676cbb9203f89147309432f32b149be59b05ddbb4f2835516611ed`,
and `application_runtime.rs`
`37d03bff8bc32e38fece076bceacaffdcf204fbeb47163b8831ae4dea4e1687b`.
Any changed hash reopens the affected finding.

[`ProjectedTask::project`](../../../crates/bombay/src/launch.rs) consumes an
`OwnedTask<B, Vec<Root>>` and exact `ActorOrigin<Owner, Role>`. It moves the
actor `JoinHandle` and origin into a new Tokio task, calls
`finish_owned_task(actor_task).await`, converts the resulting
`LocalOutcome` to `ActorRetirement`, and calls the static
`Root::project(origin, terminal)`. The owner-cancellation sender stays in
`ProjectedTask` with the projection task's `JoinHandle<Root>`.

[`ChildBinding`](../../../crates/bombay/src/child_bindings.rs) retains one
`ProjectedTask` with the exact endpoint, control sender and creation kind.
At parent retirement, `RetireChildTasks` removes tasks in creation order,
calls each `retire().await` sequentially, then retires the next structural
child occurrence. Thus the parent's complete `ActorRetirement` contains
ordered, role-projected descendant terminals. A task may not simply be
replaced by a completion-order `JoinSet`.

`ActorRetirement::from_local` retains concrete final Behavior, settlements,
control/user ingress and descendants for ordinary outcomes. It converts an
exhausted completion with a captured `OwnerCancellation` fact to
`OwnerCancelled`; a task panic/cancel lacks final state and remains distinct.
`ProjectTerminal` is a total static lift into the application-owned sum; it
does not own actor execution or cleanup policy. `ActorOrigin` retains exact
address/nonce and static owner/role. These types have real semantic ownership
and are not forwarding wrappers to delete.

The root path is different: `LaunchSystem::launch_with` joins the root
`OwnedTask` and calls its `RootProjection::project` synchronously in the
application future. A root projection panic happens after the root's task
already returned; it unwinds the caller's future and yields no application
terminal. It must not be mislabeled as a Behavior panic. The child task
comparison below must not silently change this root contract.

## Required timing trace

Current successful child path:

```text
child Driver returns exact residual
  -> Incarnation LocalRetirement publishes coarse termination synchronously
  -> child actor task completes LocalOutcome
  -> projection task joins actor task
  -> projection task awaits LocalResidual::settle_activation_tasks
  -> projection task converts exact ActorRetirement + ActorOrigin to Root
  -> parent later requests retirement and joins ProjectedTask in creation order
  -> parent residual contains the ordered Vec<Root>
```

The coarse termination observation can become ready **before** capability
task settlement and projection complete. `ActivationTasks` is a Tokio
`JoinSet<Result<(), Event>>`; its `settle` joins all tasks and appends their
returned events to residual control ingress, rethrows a task panic, and panics
on unexpected cancellation. `JoinSet::drop` aborts remaining tasks. The
projection task therefore has a current independent progress responsibility:
when a child stops while its parent continues, it settles that child's
capability work without waiting for parent retirement. This is a source
fact, not proof that a second task is the only Rust representation.

The current test `launch::tests::projected_child_retirement_preserves_origin_state_and_descendants`
checks one exact final terminal and nested descendant, while
`application_terminal_custody` checks ordered direct children and declared
roles. Neither holds a child capability task pending while the parent stays
live, measures task count, or injects a projection panic.

## Panic, drop and failure custody to preserve or amend explicitly

| Event | Current path and observable limit |
| --- | --- |
| Child Behavior/Driver panic | Actor task unwinds; `Incarnation::Terminal` publishes `Crash::Panicked`. Projection task joins an errored actor handle, converts it to `IncarnationOutcome::Panicked`, then exact `ActorRetirement::Panicked`. |
| Child actor task cancelled | Terminal drop publishes `Crash::Cancelled`; projection task obtains an actor `JoinError` and produces exact `ActorRetirement::Cancelled`. This is distinct from a cooperative `OwnerCancelled` residual. |
| Projection function panics | Projection task panics **after** child actor join and post-Driver capability settlement. Its `JoinHandle` fails. `ProjectedTask::retire`/test `finish` then panic in the parent via `unwrap_or_else`; parent `Incarnation::Terminal` classifies `Panicked`. Child coarse observation may already say normal. There is no projected `Root` value. |
| Parent retires while first projection has panicked | Ordered child loop panics on that first `ProjectedTask::retire`. Unvisited sibling `ProjectedTask`s are dropped: their cancellation senders close and their projection handles detach. A sibling actor awaiting ordinary work can remain live on a surviving Tokio runtime. This is a source-derived failure candidate, not an executed regression. |
| Parent or caller drops while a projection is pending | `ProjectedTask` drops sender and projection `JoinHandle`; Tokio detaches the projection task, which may still join the child and then drop its projected `Root` because no joiner remains. No parent-held exact terminal survives. |
| Capability task panic | `ActivationTasks::settle` resumes unwind inside projection task; remaining `JoinSet` tasks abort on drop. Parent sees projection-task panic later. A candidate must not silently call this a child Behavior panic or accepted effect success. |

An optimization that projects only inside `RetireChildTasks` would delay
`finish_owned_task`, hence capability-task settlement, until parent retirement.
It also moves projection panic from a child-completion task into the parent's
retirement call. An optimization that puts post-Driver settlement inside the
existing actor task may preserve eagerness with one fewer task, but its
publication timing, returned type and panic classification must be measured.
Neither optimization repairs parent/sibling custody automatically.

## Candidate ordinary-Rust comparison

Test each candidate with the same one-parent/two-child Behavior composition,
one independently gated child capability completion, and a deliberate
`ProjectTerminal` panic. Record exact concrete types, inferred caller syntax,
compiler errors and static denials. `ActorRetirement` and `ProjectTerminal`
remain the exact contract; no erased terminal box or dynamic callback is
eligible.

| Candidate | Type and task equation to test | Acceptance/falsifier |
| --- | --- | --- |
| Current eager projection task | One actor task **plus one projection task** per established child; parent owns the projection handle and cancellation sender. | Prove prompt capability settlement while parent is live, exact projection order at parent join, complete sibling custody after one projection panic, and dropped-parent cancellation. Current code appears to fail the latter two. |
| Settle in actor task, project when parent joins | One actor task per child; its return is an already settled exact `LocalOutcome`. Parent holds the actor handle, cancellation authority and origin until ordered retirement. | Capability completion must happen before parent retirement, with the same coarse observation/settled-result distinction. Projection panic may occur later but must not skip sibling cleanup or relabel the failing child. No second task merely to shorten a type signature. |
| Project in actor completion task | One actor task returns the projected `Root`, with origin supplied at spawn after fresh allocation. | Must preserve source type, startup rejection ownership, exact child/role inference, parent-continue cleanup, and projection panic classification. Do not add a generic projection framework or broaden every launcher solely to satisfy this candidate. |
| Keep a separate completion task with a proven independent purpose | One actor task plus one specific completion task, justified by cleanup or publication progress that cannot occur in the actor task. | State and test the unique authority held by the second task and the code it replaces. A task that only invokes total `ProjectTerminal` after a join fails the abstraction test. |

`ProjectedTask` currently owns two different permissions: request forced
retirement and join an exact projected result. Its retained representation
must say why these coexist and how a caller drop transfers both. The
proposed candidate must answer the five abstraction questions from the PRD
with a real consumer and a measurable task count.

## Deterministic witness design and stop condition

1. Start a parent that keeps accepting work, plus a child that stops after
   initialization and starts a typed capability completion blocked by a test
   gate. Await child coarse termination, then release the capability gate
   **before** requesting parent retirement. An independent log must record
   `child coarse terminal -> capability completion -> parent retirement`; the
   projected exact terminal must include the completion event. The selected
   model must define whether coarse publication may precede that completion.
2. Repeat with two child occurrences and order their stop times opposite to
   their creation order. The parent's exact descendant vector must remain in
   creation/role order, with each original terminal and nonce. If a child
   begins a nested descendant, its result must stay nested under that child.
3. Implement a test-only `ProjectTerminal` lift that deliberately
   panics for one child role. Keep a later sibling pending. Retire the parent
   and observe its panic classification, the earlier child's coarse terminal,
   and whether the later sibling is actually requested to retire and joined.
   Original code is expected to fail the sibling-custody assertion; first
   verify this for the intended reason. Repeat in debug and optimized builds.
4. Drop the parent execution during pending child settlement while Tokio
   survives. Count actor tasks, projection tasks, capability tasks, lease
   releases and exact payload drops. The result may be discharged under an
   explicit policy when no caller remains; it cannot be described as an
   exact terminal delivered to that caller.
5. Invert each selected law: defer capability settlement to parent join,
   reorder child joins by completion, duplicate one projection, or suppress
   projection panic. Its distinct witness must fail observably, not merely
   through a source-string scan.

Existing focused baseline commands, once a test is added, use Bombay's pinned
Nix shell, for example `nix develop -c cargo test --locked -p bombay-rs
--lib launch::tests::projected_child_retirement_preserves_origin_state_and_descendants
-- --exact` and `nix develop -c cargo test --locked -p bombay-rs
--test application_terminal_custody`. The new witness names and exact commands
remain to be recorded after test files exist. No new command was run for this
research record; the existing audit's 15 passing runner tests are only a
preservation baseline.

DG-PROJECTION stays open until a completed comparison records the measured
task count, exact cleanup timing, panic and sibling custody, compile
diagnostics, expected production/public-surface delta, focused debug/release
results, original-defect failure, and independent reviewer signature. The
coordinator must choose one concrete representation before WP-TASK edits.

## Current sibling-custody defect and ordinary comparison (2026-10-02)

Fresh evidence uses the current 0.20.0 owning contracts and registry-selected
Communication 0.1.3, with the canonical manifest/lock hashes recorded in EXEC
section 20. The earlier sender-drop and deferred-capability descriptions above
remain historical: current cancellation authority requests retirement on Drop,
and the actor task settles capability work before returning. Preserve those ARC
fixes. Current `owned_outcome` still resumes an activation-task panic inside the
projection task; `ProjectedTask::retire` then panics on its actual JoinError.
The real ordered `ChildBinding` retirement stops there and discards an
independently completed sibling result.

The actual-source witness independently observes that discarded sibling's whole
retirement: exact ChildOrigin, state 19, accepted Stop settlement, empty ingress,
Stopped completion and the original descendant Vec allocation. Its intended
no-unwind assertion fails after compilation in both debug and optimized builds,
one test each. The first child's already-destroyed values are not recoverable.
The application projector remains total; this is a runtime failure, not a
deliberately panicking caller projection.

An ordinary comparison destructures the two existing ProjectedTasks in their
owning test module, awaits both actual cancellation/join operations without an
early return, and retains standard Result values beside existing creation ID,
kind, endpoint, control and ChildOrigin in tuples. It keeps the original owned
JoinError and complete sibling retirement. One test passes in each profile;
formatting and strict Clippy pass. No production repair, public role/product
selection or task-count reduction is claimed.

Frozen directories under the recorded scratch root are
`bombay-projected-child-custody-yrdixc4k` and
`bombay-projected-result-custody-38n60dk4`. Original receipt SHA-256:
`538c106b8c41fa64f2ee1665e628b504b489421d05fa70839d4ac2db32eb4165`;
original patch:
`24e1d2196cb1332a5a32252dd08880bc970143a11f0449513a18a08fbe764dfc`.
Comparison receipt:
`abd4f32a48e517a3a1f6f9d0a53379de9ee8608709474f779eddc333ea1a16fa`;
comparison patch:
`083c977d0e635d16c2f1a398379abda82114b36e166d5a1a0452f4df45f9b993`.
Receipts record source hashes, exact pinned-Nix commands and profile logs.
Both stages touch only launch.rs tests: original +243 / -1, comparison
+267 / -1; production and public types zero. These are separate research
snapshots, not cumulative retained test growth.

Independent reviewer `/root/observation_research` read both complete patches,
current source and authenticated all six original and eight comparison
artifacts. It signs these bounded defect/feasibility claims only. The historical
defect witness awaits discarded-result publication before checking failure;
a repaired implementation that retains the sibling would keep that publisher
alive. Adapt the final regression to observe successful result custody directly
rather than waiting for discard. Public births/roles, all sibling cleanup,
projection timing and failure provenance remain required. DG-PROJECTION is open.

## Projector contract-fault conservation comparison (2026-10-02)

Separate frozen `bombay-total-projection-custody-qi3sl6z4` injects a panic into
ProjectTerminal after it receives a complete stopped child retirement. This
violates the documented total static-lift contract; it is not a lawful
projector or the preceding lawful-caller runtime defect. After that fault,
ordinary tuples retain each child's existing origin, ID, kind, endpoint and
control outside its joining task, while a standard Result pair joins both
actual handles. The acquired JoinError preserves the original panic String's
bytes and allocation; the sibling's whole stopped retirement is retained and
both endpoints retire. Original ProjectedTask::finish instead replaces that
panic with formatted text; the compiled exact-message oracle fails in both
profiles with exit 101. No recovery of values destroyed inside the projector
panic is claimed.

One positive test passes in each profile, including restored positives;
formatting and strict Clippy pass. Isolated launch.rs tests +219 / -2 / net 217;
production/public types zero. This uses archival Communication 0.1.2 and the
source-retirement research freeze, not current integration acceptance.
Receipt `ed58988e62c4be9ef31b716bcaa548159ea11c5c302b5a4667b932239fd30735`;
patch `01e99eb1b4aca2bf7d0147f9a8a1d1b12709266d7aa1fc00de4563739d55928f`.
Its 24 artifacts were authenticated. Independent reviewer
`/root/contract_inventory` signs bounded after-fault conservation only, together
with immutable scope corrections
`479d8e8b23efbf44935ca00f94dc8b975c651c060ab3d6eb05acd9c0829da647`.
The two role markers are nominal ChildOrigin parameters without owning
ChildRole<RootProbe> topology proof; actual heterogeneous RuntimeChildBindings
registration/traversal remains unproved. Fixture names RejectedProjectionActor,
RejectedRole and Rejected incorrectly imply typed rejection when the child
stopped normally and projection panicked. Correct naming and minimization
before retention. No public representation, arbitration or gate is approved.


## Selected-version heterogeneous custody review (2026-10-03)

The frozen `bombay-heterogeneous-projection-k6c6l431` comparison uses selected
Core/Actors 0.21.0 and Communication 0.1.3, layered on the independently reviewed
startup handoff. Its generated Catalog declares two distinct child types and
real ChildRole positions. Actual ChildBindings records hold both established
children; the first caller projection deliberately panics after receiving its
completed actor, while the second actor's interpreter cleanup remains gated.

The original traversal returns a parent panic before the sibling finishes,
replaces the original panic payload with formatted text, and loses the later
full retirement. The original regression compiles and fails for that exact
payload oracle in debug and optimized builds. The ordinary comparison joins
both actual handles into a closed product of standard Results beside each
role's existing origin, creation identity, kind, endpoint and control. It
preserves the original JoinError, task identity and String allocation, plus
the sibling's original state allocation and complete retirement. One positive
passes in each profile. Restoring the original formatted-panic discharge fails
in both profiles; restored positives, formatting and strict Clippy pass.

Receipt: `0b6356646f4785f4bd09a0f5de2ecab73b86368b347e119323c05fd538806fee`.
Test-only patch: `93ca12f8ac01486d78298903424a973506d677fdc32cc4384aebed88e71c5d15`.
Increment: one existing launch.rs path, tests +387 / -0 / net 387;
production and new public types zero. Initial declaration-order blocking runs
are excluded: the corrected generated role order places the faulting child
before the gated sibling without changing any owning arbitration contract.

Independent reviewer `/root` read the complete patch, verifier scripts and
profile logs, authenticated all 43 artifacts and 26 source hashes, and checked
25 unchanged source paths plus the complete launch.rs baseline prefix against
the signed selected-version handoff. This signs only the bounded defect and
ordinary ownership comparison. There is no fresh reviewer execution, live
Catalog Driver, standard establish_child integration, recursive descendant
model, retained public result representation or full DG-PROJECTION acceptance.
Values destroyed inside the faulty caller projection cannot be recovered.
Joining leaf results alone does not satisfy the required recursive scope.

## Actual established-binding extraction review (2026-10-03)

The authorized section 25 successor, bombay-established-binding-results-yaujv51z,
puts both original ProjectedTasks inside the actual ChildBindings before either
comparison. A test-only ordinary function in that owner consumes creation order
and each original ID, kind, endpoint, control and task. The launch owner returns
each original Result of projected terminal or JoinError; the caller joins both
occurrences into its closed typed product. No production prefix changes.

Receipt `ceda20e758abb0610260f225bc3b0a6a49ddd2e15dae53aa09217c32ac58b378`;
incremental patch
`658e82e0efc3c623ca09b760b1b735df1fdbbf98add23b928ede8421fec3cbf9`.
Two existing test paths: +111 / -38 / net 73; production/public types zero.
The original formatter/early-return regression fails in both profiles. The
ordinary comparison and restored positives pass; all 230 preservation tests,
formatting and strict Clippy pass in both applicable profiles on pinned 1.96.
Three compiled inversions fail in both profiles: original first-error discharge,
creation-kind substitution and original declared-origin substitution.

Independent reviewer /root authenticated 27 source hashes and 37 artifacts,
read the full fixture, incremental patch, owner, scripts and failure logs, and
checked both production prefixes. Fresh reviewer executions independently
reproduce original exit 101 and positive exit 0 in debug and optimized builds.
Review receipt
`1a11bd3c215cd128e580986c288ae0679c0a7f83815bcabbea13563204c36b45`.
This accepts bounded evidence only; DG-PROJECTION and retained production remain
open. Original origins are still separately caller-owned constructor inputs
(109/113), distinct from creation identities (1/2). They are not yet stored by
production bindings. Full control/endpoint identity retention relies on original
owned-field movement; only endpoint addresses have an independent observable
comparison. Results are post-projection and cannot recover values destroyed
inside a faulty projector. Actual bindings may also hold Rejected replay markers;
this two-established-child fixture does not authorize discarding those markers
without their owning policy or duplicating their independently owned payloads.
Standard birth integration, live parent execution, caller disappearance and
recursive raw actor results remain required.

## Standard-birth recursive comparison (2026-10-03)

The current selected-version comparison uses actual standard establish_child
bindings: a live parent owns two genuine FIFO branches, each with an actual
worker grandchild. One branch finishes while the parent remains live; the other
branch's cleanup is gated. Actual parent interpreter retirement, rather than
its coarse termination observation, determines when to release the sibling.

The original traversal panics on the first failed terminal projection, erases
its original failure and loses the parent result. The ordinary comparison
consumes existing bindings and joins their original handles into standard
Results. It keeps the original JoinError and panic String allocation and
contents, waits for the sibling, and returns normal recursive terminals. Exactly
one actually acquired child-completion report remains accounted for in returned
parent state or retirement ingress. Accepted unit shutdown receipts follow
the owning explicit discharge policy; they are not fabricated as retained
payloads. Behavior state and folds remain pure; runtime gates belong to the host.

Corrected successor receipt
`42e3c77949a7f4cd87648f71f6a2bf1c100709c3ee44feac6727d79701cd1a5e`;
full patch `08e730a59778789b64ae565d1adcc43faee4a9cbc0838bfb5447331cd9e9cfb6`.
Three approved existing paths, tests +990 / -0 / net 990; production and public
types zero. The successor fixes two weak oracles: allocation/length alone did
not prove original panic contents, and report comparisons did not require a
report to exist. Compiled content substitution and report omission each fail
for the intended assertion in both profiles; restored positives pass.

Independent reviewer /root read the full parent fixture and consuming owner
methods, authenticated 345 sources, 30 successor artifacts and 57 predecessor
artifacts, and independently reran two positives and the original defect in
both profiles using a fresh dedicated cache and pinned Nix Rust 1.99. Positives
exit 0; originals compile and exit 101 at the parent-result conservation law.
Review receipt `1e50466d60163cc5b0d338103f814d0dbfec375886097d6461cf72e79cee9a78`
accepts bounded evidence only. No public or retained repair is approved.

DG-PROJECTION remains open: failed-branch origin is separately host-owned rather
than part of its returned Result; nested branch FIFO settlement checks establish
acceptance but not full opaque pool state and all nested lanes. Current nested
retirement cannot express a grandchild's projector failure. Heterogeneous role
products, dropped-parent ownership, recursive raw actor failures and projection
minimization remain required. Values destroyed inside a consuming user projector
cannot be recovered. Preliminary compile failures, interrupted runs, a wrong
coarse-parent timing assumption and a completed-future repoll failure are
excluded; none is counted as original-defect evidence.

## Constructor-input origin custody comparison (2026-10-03)

An additional ordinary-Rust comparison uses two actual actors with distinct
generated child roles. Its closed per-role product retains creation metadata
and `Result<Terminal, (original typed ChildOrigin, original JoinError)>`.
The success terminal owns its origin once; only failure needs the separate
origin. Both eager projection results are joined before caller code directly
constructs its concrete failure variant. Original error identity/panic kind
and the complete successful sibling, including its original Vec allocation,
remain available. No native payload inspection or downcast is introduced.

Receipt `783cae988f6031aa769639ffd1f1d75bb33bbc85e05679734ba61d0b7acdcd09`;
incremental patch
`b352f96bbc6c9e491357a66249ae909100a86c67fcaf6bcd4a1bbe4fbb2ad875`.
Independent non-author review
`9840d13575d7c1a74f23e3ec40f1e5e5b2098d6b331b71bac1d4e85f677b774d`
authenticates all 345 sources and 49 artifacts. Coordinator read the complete
330-line append, source contracts and intended diagnostics and independently
authenticated those hashes; no coordinator Rust rerun is claimed. Production
and public-type delta is zero. The candidate remains isolated and unretained.

Positive and restored origin comparison and inherited normal recursive case
pass in both profiles. Substituting the original origin compiles and fails its
typed equality in both profiles. Originless output, wrong declared role and
recursive closed product are three E0308 static denials; these are interface
limits, not executable repairs. Formatting and strict lint pass.

This reserves explicitly supplied constructor routes beside the tasks. It does
not fix actual standard creation's origin storage; the Catalog topology is not
a running parent. Primary projection remains eager and a panicking consuming
projector can destroy its inputs. A single Pending poll of a newly spawned
joining task does not establish that it reached a particular await; the real
unreleased sibling gate and consuming join-all source support the bounded
cleanup claim. Many occurrences, recursive failure shape, dropped-parent
custody and the deferred-primary alternative remain open under DG-PROJECTION.

## Exact settlement-bound comparison (2026-10-03)

The existing generic ActorExecutionOutcome and LocalResidual can store an
explicit settlement type with only B:Behavior. Current LocalOutcome selects
B::Settlements, so actual launched actors still need BehaviorSettlements.
Three uninvoked generic functions prove expanded storage, both-way identity
when Settlements=S, and the existing OwnedTask requirement. They do not run
actors or establish cleanup. Restoring only B:Behavior on the OwnedTask probe
produces the intended Sends/Birth E0277 diagnostics in both profiles; restoring
the genuine bound compiles. Formatting and strict package Clippy pass.

Author receipt
72fcf52b16cddb2b1ba64c49cff831f370e580ed03cc0732bde6c60ce272ea9f,
patch 225ee1ed3f1b5090fce9340540ca6af216bc48b56822d8d5edb9616a0a5f2f7e
and source c91ae7d6bcbbdc1f2629e13e54728fbdaaf6adb623c1b93c4c67806907737b5c
bind 57 cfg(test) lines in launch.rs; production/public delta zero, 345 sources,
344 unchanged and no untracked source. Nonauthor coordinator review
a898d5987d5499cd5b23a0790ccea18014aae556895e1c59aabfe2492ed7b147
reads the entire patch and actual source contracts, authenticates all sources
and 38 artifacts and accepts only this bounded compiler evidence. No fresh
reviewer Rust execution or canonical transfer is claimed. Initial warning-only
receipt 736cc39974af824332297b5f2d5209d603eabc6a9a02c5b9531bfb81587052ce
and the subsequent lint failure remain excluded predecessors. Narrow fulfilled
lint expectations identify uninvoked probes and the deliberately expanded type;
no new alias hides that type.

This removes an incidental type-expression obstacle, not the recursive output
contract. ChildOccurrenceShape::Member is universal over Child:Behavior and
has no per-child settlement selector. One supplied S cannot represent arbitrary
heterogeneous children. Current RetireChildTasks returns Vec<Root>; it has no
existing associated raw recursive output. Actual parent/pool/grandchild types
are inaccessible in the current launch fixture, so proposed concrete recursive
and Vec-mismatch rows were not attempted. Nested shutdown decorators were
explicitly rejected as recursive evidence. Producer integration, provenance,
public shape, cleanup timing and full DG-PROJECTION remain open.

## Selected recursive producer equation (2026-10-03)

Read-only model ee20ac913bb86e16ccd6c4fad072c82ac18c5e5ad251fb07bb758e78571e0aa0,
receipt fc1e075a2823cd4d1673bd5ef174d54a74c0f7600b6d5b13989673da13224d6b
and independent non-author review
fcea34f9133b7e4018cd77b1f000ddb7257bdd117751128a35ac52650f3919cc
reconcile the selected producer and universal child shape. Actual EstablishChild
knows its concrete BehaviorSettlements proof before launch, but returns Core's
creation receipt. The universal Member requires only Behavior; no raw retirement
selector or settlement proof passes through that contract. Current child bindings
store already-projected Root tasks and retire them in per-occurrence creation
order followed by the tail, without establishing global chronology.

Existing concrete recursive terminal enums can inject each whole
already-constructed ActorRetirement and typed ChildOrigin through the existing
total ProjectTerminal constructor. Each concrete arm selects its own settlement
product. This preserves that constructor's inputs, not the original raw task
Result or JoinError already classified by owned_outcome. It neither deletes the
eager projection task nor proves that failed branches retain their origins and
all siblings join. Actual heterogeneous standard births with different settlement
products and a grandchild must be compared before selecting this representation.

Reviewer and coordinator authenticate fifteen selected source hashes; the prior
72fcf52 evidence supplies the actual two missing settlement-bound E0277 diagnostics.
A universal Member E0276 denial is still uncompiled. No new trait, macro, blanket
Behavior bound, producer contract, source edit or public type is selected. Native
failure ownership, deferred primary projection, task minimization and full
DG-PROJECTION remain open; no fresh Rust execution is claimed.
