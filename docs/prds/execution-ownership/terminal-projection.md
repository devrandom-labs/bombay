# Terminal projection decision evidence

**Current closure:** DG-PROJECTION, XO-23 and EV-10/12/30 representation
comparison are independently accepted. The existing eager actor-plus-projector
composition is retained. Final integrated checks remain; historical sections
below do not reopen the accepted decision.

## Current representation decision

Independent decisions53d440a4 and3cb96357 select the existing composition. The
actor already owns capability settlement; the separate projector owns the
exact native failure cause and task identity of terminal conversion. This is
not retained as a second actor cleanup owner or as a performance promise.

The actual ordinary-Rust completion candidate passes all20 native control,
classification-fault and restoration rows in both profiles. Its eight required
cost/order rows also pass: the whole child-custody target covers failed startup,
ordered/heterogeneous/nested children and cancellation; both computation
omissions fail at the original complete post-join 0-versus-128 oracle and both
restorations pass. Actual receipts0d9ce136/2dba6c21,66d6e5a0/b3d9ff6b/39267209
preserve every stream, source graph and restoration. Collector-only false
flags remain intact and are independently qualified; no valid row was repeated.

| Selected81ba2c0 / Rust1.99 optimized observations | Retained eager | Unretained completion |
| --- | --- | --- |
| Actual native roles | Parent, child, projector, root join | Parent, combined child/projection, root join |
| Cold formation allocations | 2 | 2 |
| Controller execution/join allocations | 34 | 34 |
| Observed task-poll allocations | 559 | 560–561 |
| Elapsed healthy/restored | 5.059–5.435 ms | 10.519–12.609 ms |

The same workload, regions and exclusions apply as the matching comparison
below. Runs occur at different times; these observations establish neither
causal speed nor a whole-runtime allocation total. The alternative removes
one native task but adds three channels, two private state sums and192 net
production lines to reconstruct the existing native-source and startup-custody
distinctions. It is rejected on total ownership/model simplicity, not because
one-task Rust is impossible or because counts alone outweigh correctness.
Neither its source nor its candidate-only quality warnings are retained.

No additional representation law or experiment remains unresolved. The
join-time static carrier probe remains unexecuted evidence only; it receives
no fabricated runtime-counterexample credit.

## Current matching-workload comparison

Both versions execute the same 128 parent and 128 child computations on two
Tokio workers, return the original vectors and sums, and join the exact typed
retirements. Healthy, omitted-child-computation and restored controls ran in
debug and optimized builds. Both omissions fail at the intended 0-versus-128
oracle after joining. The original source and all unchanged inputs are restored.

| Optimized observations | Original 2fccedf6 | Current integration |
| --- | --- | --- |
| Elapsed, healthy and restored | 9.068–10.736 ms | 5.059–5.435 ms |
| Actual task roles | Parent, child, projector | Parent, child, projector, root join |
| Cold formation allocations | 0 | 2 |
| Controller execution/join allocations | 27 | 34 |
| Observed task-poll allocations | 302–305 | 559 |

These are separate measured regions, not a whole-runtime heap total. Payload
setup, runtime construction, off-poll worker allocations and reporting are
excluded. Compiler and owning-library revisions also differ (Rust 1.96/Behavior
Actors 0.20 versus Rust 1.99/0.22); the timing difference cannot be attributed
to one task or promised as a general speedup. Allocation growth is explicit.
The authoritative actual receipts are original 6d84a8f0 and current 226f083f;
the unchanged-body style bridge is 632559bb. Independent review 32b27a08 binds
the complete streams, source graphs, original selected dependency graph and
restorations. This closes the before/after measurement requirement, not the
representation gate or final integrated benchmark.

**Frozen research snapshot:** ARC-011 on 2026-10-01 retained the one child
projection task after comparing typed origin propagation with erased
projection dispatch. The spawned actor task now settles its own activation
tasks, and abandoned join waiters request owner cancellation. Descriptions
below of `finish_owned_task` and bare sender-drop behavior apply only to the
earlier 0.17.0 snapshot; current evidence is in the ARC-011 retained evidence in the backlog status index.

Historical status: **open research; the projection task was retained pending
its independent timing and panic laws.** This record addresses DG-PROJECTION,
XO-20–23 and EV-10–12, and depends on [task custody](task-custody.md). No
production or test source was edited for that initial record. Its controlled
capability-completion and projection-panic witnesses had not yet run.

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

## Concrete recursive comparison pre-edit (2026-10-03)

Pre-edit 203b3a60afd30e765ba2587f155bc6aca682e328ded252441ae3de018c9a6f2e,
independent review
9a997f8bd7cc8c732bf8ca16a1547f9da5427a02d10a3acfb0b41a96630f82d7
and coordinator review
9aa7ff767da2da79f295a2a852185fab6752c613c4d3bd49bcd141acbf7596ad
permit one external launch.rs cfg-only ordinary-Rust comparison: four pure
actors, a concrete recursive sum and the existing total ProjectTerminal
constructors. Leaf and Branch have different settlement products; Branch creates
a real grandchild through standard installation. Caller acceptance/rejection
owns the whole already-constructed tree only after all results return.

The selected source explicitly discharges accepted report units and exhausts
empty creation settlements. Nonempty creation receipts remain in the final
typed lanes. Private child binding acknowledgement precedes initialization
effects and publication; a parent report cannot prove early publication.
Receipt/origin address correlation does not prove endpoint generation identity.
The opaque issuer and original nonce remain owned without invented getters.

Expected unformatted delta is +368/-0 tests, production/public types zero,
seven private domain definitions and no new trait. The required iterator advance
was moved outside its assertion before eligibility. Positive/inverted runtime
traces, static denials, exact formatted freeze and final independent review
remain required. No raw task-error recovery, deferred primary projection,
task deletion, canonical retention or full DG-PROJECTION is selected.

## Reviewed constructed recursive retirement comparison (2026-10-03)

Receipt 64df30ef2e8df9061faf5636bcb249ae577b5378cc21cae314bcf4f26b1a25d3
freezes final source
6f084686c37f12eda0d4f7993eacead89eb7ac961e72a7c95c84e9f3aa1d7343.
Nonauthor review ce5ec7a8cacf3198cf8c186bea284a59fa3625fe02ffd90292e42a6017f068dc
is qualified by immutable correction
a6d5016b4ecc378554ab947ce438e74f3ff60fd9350a457fb900c24b74bf942d:
its original source field named a historical snapshot. The supplement rebinds
the actual final source and all corrected verification artifacts; the historical
snapshot's logs do not certify the final experiment. Coordinator inspection is
8699b55aee4968849ad31304927d0ed6cc65450c4eea4599ab8bafb471b3746d.

The actual standard application returns the root, two children with different
settlement products and a real grandchild. Four existing total constructors
move their complete typed origins and already-constructed ActorRetirement values
into one recursive closed sum. Subsequent caller acceptance and rejection both
retain that tree. Full state, original allocations, structural child order,
creation receipt correlations and every remaining typed lane are checked after
the application joins. Accepted report units are explicitly discharged by their
owning contract. Endpoint interpretation and iterator advancement occur before
observational assertions; opaque IDs support no invented identity oracle.

Both-profile positives/restorations pass. Four static-denial cohorts produce
eight expected failures; three runtime counterfactuals produce six intended
failures after complete application return. These clone the Leaf allocation in
its pure initialization, corrupt retained creation kind, or erase the joined
grandchild. Strict package all-targets Clippy and workspace formatting pass
through pinned Nix. Earlier compiler failures and pre-correction assertions are
excluded. Both reviewers authenticate 345 sources, 91 artifacts, 14 selected
owners and the unchanged 179-package lock, including 165 external packages;
neither reran Rust. Formatted cfg tests are +564/-0; production/public changes
are zero. Eager projection tasks and upstream raw JoinError loss remain. This
accepts bounded feasibility, not deferred projection, task deletion or the full
gate.

## Root and child projection ownership reconciliation (2026-10-03)

Read-only model receipt
f04e40b210741c5ecf97ee72bf78a8520d2dc3e4e8895b80cc20e194c4fc9ab6
and independent review
f3f58790fd8b0b994e00efc55547ac8534b5e43ca37f456cc43dd6bb317eb4c0
separate two requirements that must not be conflated. Root primary conversion
can retain the concrete raw result at its existing factory proof until the caller
requests conversion. A child task can return a homogeneous application terminal
without requiring settlement proofs on every shared occurrence member.
Removing its separate projection task therefore is not blocked merely by the
universal member's weaker bound.

The child factory must still recover the complete original startup rejection:
the same actor join currently supplies that state and initialization custody
when its startup receiver closes. Mapping that join to the application terminal
would remove this recovery unless an explicit typed transport preserves it.
Available original task errors and role origins must survive, and all siblings
must join before arbitrary root primary conversion. Full unprojected child
storage is a stronger alternative with a separate missing output selector;
it is not implied by root primary deferral alone. No transport, task deletion,
new interface, native-panic exception or full DG-PROJECTION is accepted here.
Coordinator inspection
3ef65f07ea81a53ec92fa29ad63cd98bb51e974c77f09d26d0a64446ffa7e29b
and the independent review bind 33 selected, historical canonical and typed-source
inputs; no Rust command or source edit ran for this model. The next comparison
must provide the concrete factory amendment and consumer delta rather than
repeat already-established compile-only syntax.

## Factory transport comparison and remaining fault proof

Read-only component receipt
7c676c4c1e2346410fa370330cb77a026230a4c534a3aced46d4044773ce448f,
independent review
9c5418e9289314f9e54da1ebf4856fbfb16ededd818735e0efabc3f082c94497 and
coordinator inspection
f1faa1aed7003ffca27f072aef35b9a9fc29909d1e3dcc95fa8a20a10e8b966f
bind five artifacts and 15 source inputs. Immutable supplement
941a9f0f1245473d0d6c80affd6d7c8ea7c2a4987fbc5fdaff84448d5d1e2c33
corrects the earlier model's inherited 33-input count and qualifies reachability.
Historical canonical document hashes authenticate against Git181; later docs
are not silently rebound to those bytes. No source or Rust command changed.

An ordinary consuming function can convert settled child retirement inside the
existing actor task; the paper component alone adds net5 production lines and
removes zero tasks. Changing its raw result also requires explicit custody for
whole startup rejection. A branch-specific rejection channel and Option terminal
are hypotheses: original Start/ACK must prove that an established child cannot
return the delivered-rejection alternative, and waiter drop must retain a join
owner. No new transport, bound or public result is accepted by this comparison.

Ordinary initialization panic retains the actual partially changed child under
the selected Driver and cannot prove an unavailable-child receipt gap. The
separate payload-destructor characterization proposal tests whether a second
panic can escape before Start; it remains unexecuted. Any owning creation-receipt
change first requires genuine reachability and exact available provenance.
Recursive caller conversion can destroy values it consumes while panicking;
outer task error and origin cannot reconstruct those values. Deferred primary
conversion, eager child cleanup, exact remaining custody, all-sibling joining
and actual projection-task counts must be proven together. The full candidate
remains ineligible; DG-PROJECTION stays open without invented receipt vocabulary.


### Required live projection-panic witness

Source successor3b8cc77b/e28bcbbc is independently qualified by
7f0b75d91fc0: append only its owning cfg(test) module to current App, preserving
current production/imports. Expected test delta +415/−0, no production/public
types or additional path. Two real declared children use the existing local
spawn/child binding/projector path. The first projector transfers its complete
child retirement then panics with its original opaque payload; parent remains
live until explicit shutdown and joins the later sibling. All root/child lanes,
ordered settlements, origins, native task ID/cause and once-only releases are
checked after join and host disposal. Earlier one-row/empty settlement oracles
were rejected and corrected; no runtime defect is inferred from those oracles.

Run pinned formatting, the exact healthy witness in debug and release, then
misclassify only the actual projector JoinError as ActorTaskFailed in each
profile; it must fail the provenance oracle and pass after immediate restoration.
No compiler error, timeout or unjoined task counts as the intended inversion.
This closes the missing live evidence only after actual independent review;
DG-PROJECTION also requires its recorded task-count/representation disposition.

The first current formation rejects a test pattern (E0308): this one declared
birth kind yields direct ChildCreationOutcome, not a ChildChoice head sum.
Remove only the surplus Head pattern/import; the two exact committed rows,
origins and full receipt oracles remain. This compiler failure is no semantic
inversion credit and causes no production change.

A second formation rejects alias constructor inference (E0284). Use standard
Default::default() at the already concretely typed ApplicationCapabilities
constructor: the complete binding product is specified there. No new bound,
alias, annotation, wrapper or semantic symbol is introduced.

The corrected current witness9717d228 is now independently qualified:
healthy debug/release and immediate restored runs each pass one case; both
sole ProjectionTaskFailed→ActorTaskFailed inversions fail the first provenance
oracle after all native joins. Classifier514b792f and all348 snapshot paths
are restored/unchanged. Evidence: `/tmp/bombay-live-projection-current-path.txt`
points to checks50277823, full streams and restoration; initial debug stream
is `/tmp/bombay-live-projection-inferred-debug.stdout`. The actual source
prefix before the added module is byte-exact; this is real projector failure,
not a constructed JoinError or historical defect claim. Four old unused
methods and one new unused child-space field still prevent strict credit.

Remove only that unused child-space field/construction: the private child
binding owns its actual space and this NoSends witness performs no logical
child resolution. Recheck the changed witness in both profiles. Retain eager
projection pending required whole-parent/child task/allocation/throughput
measurement and final representation acceptance. Source counts are two
Tokio tasks/Task Cells per established child, not total heap-allocation counts;
ProjectedTask itself adds no Box/Arc/channel. Do not justify the second task
as actor capability settlement, which the actor task already performs.
