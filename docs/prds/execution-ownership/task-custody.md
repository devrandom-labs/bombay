# Task custody decision evidence

**Frozen research snapshot:** The ARC-011 change on 2026-10-01 resolved the
startup/finish waiter and activation-task settlement failures described below.
Current 0.20.0 source and tests, with the retained typed projection task, are
recorded in `docs/prd-backlog/status.md` under ARC-011. Failure candidates and
source descriptions below apply to the earlier 0.17.0 snapshot only.

Status: **open research; no representation accepted and no production edit
authorized by this record.** This is the WP-TASK-DESIGN input for DG-TASK,
XO-10 and XO-13–23, and EV-06–11 in the [execution PRD](../execution-ownership.md).
It records source facts and falsifiable experiments. Two original-defect
runtime regressions have now run for dropped root activation and dropped
normal-finish waiters; the other drop edges and ordinary-Rust candidate
comparison have not.

## Selected contract and scope

At this snapshot `Cargo.lock` selects Behavior and Behavior Actors 0.17.0,
Behavior Macros 0.12.0, Address 0.2.0, Communication 0.1.2, Tokio 1.53.1,
and patched Timers 0.1.0 at `13e884da7ab41781f52337b0038060e375b00ee0`.
Bombay's selected-release documentation identifies Behavior revision
`435560ce7bea8ad3330ee2d42e5034f837a80602`. The complete
[revision instructions](https://raw.githubusercontent.com/devrandom-labs/bombay-behavior/435560ce7bea8ad3330ee2d42e5034f837a80602/AGENTS.md)
were read, alongside the registry release API, the root instructions, Driver
law and strategy, capability contract, module map, ledger, and the relevant
primitive source and tests. The registry archive itself contains no `AGENTS.md`.
The lock's Behavior entries are registry checksums, not git sources; the
identified release revision must be reverified if the package changes.

Evidence anchors at the frozen research snapshot:

| Source | SHA-256 |
| --- | --- |
| `Cargo.lock` | `dfd3b2d7640281ed8cfbda6b022a281c4369a0cbf4b5ddbdd0623fa43537db07` |
| `crates/bombay/src/launch.rs` | `5dd7c4b96928d4f3e121b00b3c065485323c4eb7831329cb68c2a80a69e17f9c` |
| `crates/bombay/src/local.rs` | `8df01c9c5b47724f4aac255e647ebe6fc3f681c60d38e8dfe73a804144b80975` |
| `crates/bombay/src/application_runtime.rs` | `37d03bff8bc32e38fece076bceacaffdcf204fbeb47163b8831ae4dea4e1687b` |
| `crates/bombay/src/child_bindings.rs` | `294e118bd5676cbb9203f89147309432f32b149be59b05ddbb4f2835516611ed` |
| `crates/bombay/src/incarnation.rs` | `114e7c048fa6d153ce9532e50c6d4e18d29fc932d96e173b448003fe16104f7e` |
| `crates/bombay/src/entity/bombay.rs` | `fb7b4b0fdb29ac53feebfb256221dcfe3679f445ff21b707ee40c97bd23424d8` |
| `crates/bombay-engine/src/driver.rs` | `318063893c3bfc2b3065ec920013fa1e91057e88905a61fd72a659e104798ecb` |
| selected Behavior `AGENTS.md` fetched at the exact revision | `2b7a9195b27f073fec18426da43e9840f8ef668f333b9ad55934f520a37ae226` |

The owning primitives constrain this decision. Address `Lease::drop` removes
only its generation; Communication `MailboxOwner::close_admission` closes
admission and retains the exact accepted/rejected message split; Observe owns
publication and independent waits; Timers owns the actor-local queue. Behavior
`SourceCustody` distinguishes `Exhausted`, `Retained`, `Admitted`, and `Closed`.
Engine's Driver returns exact residuals only on ordinary awaited retirement;
its documented cancellation path drops current custody and makes no claim
that asynchronous retirement completed. Bombay must not invent a second
Driver or change a source failure into an owner request.

## Current authority and transfer graph

```text
Application future
  -> spawn_root_with activation waiter
       -> actor JoinHandle + owner-cancellation Sender (local variables)
       -> actor task: Incarnation(Driver(LocalEnvironment), LocalRetirement)
            -> activation/termination publishers; Address lease; ingress;
               child bindings; capability task JoinSet
  -> root OwnedTask + application work future
  -> root OwnedTask::finish -> actor JoinHandle -> LocalOutcome
       -> settle_activation_tasks -> projected application terminal

Parent actor capability product
  -> spawn_owned_with_mode activation waiter
  -> ChildBinding: endpoint + control sender + ProjectedTask
       -> projection task: child actor JoinHandle, exact origin and terminal
       -> ProjectedTask retains owner-cancellation Sender and projection JoinHandle
  -> ordered ChildBindings::retire_child_tasks -> Vec<exact child terminals>
```

The actor task is the only running Driver. The child projection task is another
Tokio task that joins the actor and settles capability tasks; its independent
timing is examined in [terminal-projection.md](terminal-projection.md).
Tokio 1.53.1 explicitly documents that dropping a `JoinHandle` detaches its
task and loses its return value. Dropping a `JoinSet` aborts its tasks.

| Await or drop edge | Authority held immediately before it | Current drop/cancel consequence and fact still available |
| --- | --- | --- |
| Before `tokio::spawn` in `spawn_root_with` / `spawn_owned_with_mode` | The preparing frame owns Behavior, Environment, publishers, cancellation pair; no actor task exists. | Ordinary value drops release resources. No successful activation can be published by a task that was never spawned. A panic here has no `SpawnError` return; witness needed. |
| `activation.await` after spawn in both functions | Local frame owns actor `JoinHandle` and cancellation sender. Actor task owns Driver, Environment, publisher and potential lease. | Dropping the waiter drops sender and handle. Handle detaches; closed sender is later read as **continue** by `ActiveLocalEnvironment::next`. No remaining join authority is identified. An unobserved successful activation may leave actor and lease live. This is source-derived; execute EV-06. |
| `startup_failure(task).await` | Startup frame owns cancellation sender; nested future owns actor handle and later the returned `LocalOutcome`/capability tasks. | Dropping either frame loses join authority. If outcome settlement is pending, its `JoinSet` drops and aborts activation tasks. Exact `SpawnError` may never be returned. |
| `establish_child` awaits `spawn_owned_with` during action interpretation | Parent Driver task owns its capability product, birth request and pending child result; child startup frame owns its actor handle/sender. | Aborting parent while the child awaits activation drops the child startup owner. Child may continue while no `CreationBinding` was recorded. `Incarnation` publishes a coarse cancellation for the parent but cannot reconstruct the child's accepted birth effects. |
| `launch_with` awaits root startup | Application frame owns spaces, allocations, family handles where installed, and the startup future. | Dropping application execution cancels the startup waiter above; later family shutdown is skipped because its future is gone. No exact application terminal is returned. |
| `launch_with` awaits application work | Application frame owns `RootActor` including `OwnedTask`; arbitrary work future owns its own values. | Dropping application frame drops root task owner, closing cancellation without requesting it and detaching join. Work return value is dropped. Root may remain live on caller runtime. |
| `launch_axum` awaits HTTP serving | Application frame owns listener, root `OwnedTask`, lifecycle handle and serving future. | Dropping this frame closes the serving future and drops the root owner as above. A serving error requests Behavior shutdown before the ordinary root join; neither event alone proves forced retirement. |
| `root.task.finish().await` | `finish` destructures and drops sender before awaiting actor handle; actor task owns environment and children until it exits. | Dropping this await detaches the handle. Normal sender closure is deliberately not owner cancellation. Actor may continue; an eventual exact `LocalOutcome` is dropped by Tokio if no joiner remains. |
| `OwnedTask::retire().await` | `retire` sends `OwnerCancellation`, then moves handle into `finish_owned_task`. | Dropping waiter detaches handle even though request was sent. Actor only observes request at `next`; if stuck in another await, join/cleanup is not observed. Eventual exact outcome is lost. |
| `ActiveLocalEnvironment::next().await` | Actor task owns active Environment, cancellation receiver, mailbox, lease, interpreter, children. | `Some(OwnerCancellation)` stores the fact and returns `None`, producing Driver `Completion::Exhausted`; `LocalRetirement` publishes `Crash::Cancelled`, and later `ActorRetirement` converts to `OwnerCancelled` if exact residual survives. `None` from dropped sender removes receiver and continues. |
| `next_source().await` | Same actor task, plus Driver pending source settlement. | It only awaits `inbox.recv_source()`. Owner request is not polled. Returning `None` would make Driver classify `SettlementFailure::SourceClosed`, so treating it as cancellation would falsify provenance. |
| `apply().await` and `offer_next().await` | Driver holds Behavior and ordered settlement queue; interpreter future holds the current complete action or settlement and possibly accepted partial effects. | No cancellation poll is present. Aborting this future drops its owned values; current API has no automatic return of partially accepted facts. A cooperative cancellation design must decide when it is safe to stop and how exact custody survives. |
| `LocalEnvironment::activate().await` | Prepared Environment owns endpoint, mailbox, timer/facts, interpreter and owner receiver; after `try_claim`, activation future also holds lease. | A task abort drops them synchronously; no ordinary `LocalResidual` is returned. `Terminal` classifies `Cancelled`/`Panicked`, but that classification cannot assert async child/capability cleanup completed. |
| prepared/active `retire().await` | Environment or future owns admission, lease, interpreter, child bindings and accepted ingress; active path closes admission, then awaits interpreter retirement before draining and dropping lease. | Abort while interpreter retirement is pending drops those values; lease releases on drop and `LocalInbox` drains through Drop, but exact residual/descendant terminal custody is not returned. Coarse termination can publish after the Driver future drops. |
| `ApplicationCapabilities::retire().await` / `ChildBindings::retire_child_tasks().await` | Parent interpreter owns observation cancellations, capability `JoinSet`, and ordered child `ProjectedTask`s. | It sends observation cancellations, then joins children sequentially. Dropping its future can drop unvisited task owners; a projection panic can unwind the loop. Neither condition proves all children were requested to retire. |
| `settle_activation_tasks().await` | JoinSet and the exact residual are in `finish_owned_task` or projection task. | Cancellation drops JoinSet, aborting outstanding capability work; no settled residual is returned. Panic in one activation task resumes unwind and can drop the rest. Current `TerminationPublication` may already have published the actor's coarse terminal before this await. |
| Entity family `shutdown().await` | Installed family owns its runtime shutdown/join product. | Application future drop during family shutdown loses that caller's completion; Entity has a separate family task law. Its exact transfer/drop equation must be included in DG-API and WP-ENTITY before async public release. |
| Native Entity `retire_lease` waits for `actor.termination().await` | Entity owner still holds the actor's `OwnedTask` and resident lease. | It requests Behavior shutdown first, but sends `OwnerCancellation` through `task.retire()` only **after** termination is observed. A Behavior that never completes shutdown can keep this path pending; this is a source-derived liveness dependency requiring an Entity witness, not a proven deadlock under every valid Behavior. |
| Owned Tokio runtime drops | Runtime owns remaining scheduled tasks. | Tokio 1.53.1 does not guarantee they finish; async retirement and exact terminal return cannot be promised after destruction. |

The `Incarnation::Terminal` drop guard publishes one `Panicked` or `Cancelled`
classification after the Driver future is dropped. It does **not** join the
children or capability tasks and cannot provide a `LocalResidual` that the
Driver never returned. `ActorRef::termination` is a retained coarse
`Result<Exit, Crash>` observation; it cannot reconstruct `ActorRetirement`.

## Smallest falsifiers, still to execute

Use a caller-owned current-thread Tokio test so the runtime survives dropping
the Bombay future. All gates must use barriers, oneshots and independent
event/drop logs, not sleeps or a model copied from implementation branches.
The test Behavior folds remain pure. Tests are proposed under the owning
Bombay crate, with domain names rather than PRD identifiers.

1. **Dropped activation waiter.** Pause the interpreter's initialization
   commitment after actor spawn. Poll `spawn_root_with` once until pending,
   drop that future, release the commitment gate and yield. Assert that a
   separate retained observation and Address lookup see the selected eventual
   cleanup: no abandoned actor, admission or lease remains. The original
   implementation is expected to fail because the detached actor can wait
   for work indefinitely. Run both orderings at a defined activation commit
   point. If owner cancellation wins before commitment, require no successful
   activation publication. If initialization commitment and publication win
   before waiter cancellation, activation success may be factual; still
   require cleanup and exact terminal classification. Do not infer a rollback
   from merely dropping the waiter. Repeat for `spawn_owned_with_mode`,
   including one parent creation awaiting child activation.
2. **Dropped application work.** Start a root that waits for messages; hold
   `launch_with` inside a pending application-work future. Drop the execution
   future while keeping Tokio alive. An independent captured exact actor
   observation and Address resolution must show the selected cleanup outcome;
   original code is expected to leave root live. Record the dropped work
   value exactly once. Repeat with Entity families once their owner equation
   is selected.
3. **Dropped finish and retire.** Separately poll `OwnedTask::finish` and
   `OwnedTask::retire` to pending, drop the waiter, and inspect retained
   observation, lease, child task count and exact payload drops. `finish` must
   preserve normal wait until cancellation is explicitly requested by the
   application owner; `retire` must retain cleanup/join authority after its
   request. Do not treat a closed sender as proof of requested retirement.
4. **Source and effect waits.** Construct a complete typed action settlement
   whose source is admitted but awaits control; then request owner cancellation.
   Observe both progress and classification. Separately hold a typed effect
   interpreter after it accepts one item but before the complete action
   settles, and drop application execution. Preserve the admitted fact and
   exact unaccepted remainder. The selected model must state whether the
   current effect finishes before forced retirement; it cannot silently abort
   it and claim full retirement.
5. **Retirement and panic.** Block child retirement, drop its caller, then
   release it. Repeat with a deliberate panic in child execution and with a
   projection panic followed by another live sibling. Require separately
   observable child and parent provenance and a complete join barrier where
   one is promised. Replay in optimized build; a terminal fact cannot be
   accepted twice.
6. **Runtime destruction limit.** Drop a caller-owned runtime with a pending
   actor and record only allowed synchronous drops/coarse publication. This
   witness must not assert asynchronous settlement or exact terminal return.

The pre-existing `launch.rs` tests prove normal `finish` and one projected child
terminal, and `incarnation.rs` tests prove its single panic/cancellation drop
classification. They do not exercise dropped startup/application/finish
waiters, source-wait cancellation, capability settlement cancellation, or
the surviving-runtime ownership law.

The first retained Bombay witness is
`local::tests::dropped_root_activation_waiter_releases_its_actor_lease`. It
reuses the existing gated initialization interpreter, awaits its entry after
Address has claimed the lease, drops the only `spawn_root_with` waiter, then
releases the gate while Tokio remains alive. On the original implementation,
`nix develop -c cargo test --locked -p bombay-rs --lib
local::tests::dropped_root_activation_waiter_releases_its_actor_lease --
--exact --ignored --nocapture` exits 101 after the bounded lease-release wait: `dropping
the startup waiter left a live actor without a cleanup owner; gate release:
Ok(())`. This is a compiled Bombay regression for that one edge, unlike the
isolated observation replay. It does not yet prove the selected cleanup model
or any other drop edge. The identical
`nix develop -c cargo test --locked --release -p bombay-rs --lib
local::tests::dropped_root_activation_waiter_releases_its_actor_lease --
--exact --ignored --nocapture` also exits 101 for the same orphaned-lease assertion;
neither failure depends on debug assertions.
The regression is retained with an explicit ignore reason while the gate is
open, so unrelated default tests remain runnable; accepting DG-TASK requires
removing that ignore and passing the witness on the original custody path's
replacement.

A second retained witness,
`local::tests::dropped_finish_waiter_retains_actor_cleanup`, activates the
same pure probe through the actual root launcher, polls `OwnedTask::finish()`
to pending, drops its only waiter and keeps Tokio alive while an independent
`ActorRef::termination()` waits. The original code fails its bounded cleanup
assertion in both profiles: `nix develop -c cargo test --locked -p bombay-rs
--lib local::tests::dropped_finish_waiter_retains_actor_cleanup -- --exact
--ignored --nocapture` and the same command with `--release` both exit 101
with `dropping the finish waiter left the active actor without a cleanup
owner`. This witness is also explicitly ignored by default until DG-TASK
selects and proves the complete transfer of cancellation and join authority.

## Ordinary Rust representations to compare

These are experiments, not implementation decisions. For each, compile the
same root, one created child, one Entity actor, and a pending work callback;
record actual types, inference, diagnostics, task counts, source ownership
trace and exact drop outcomes.

| Candidate | Existing concrete mechanism to try | Falsifier / unresolved cost |
| --- | --- | --- |
| Keep authority in `OwnedTask` through waits | Borrow the existing handle during joins, retain sender as a distinct normal-wait/forced-retire authority, and test a focused `Drop` path that transfers cleanup to a Tokio task if the caller disappears. Use existing `JoinHandle` and oneshot; no new public owner. | A synchronous `Drop` cannot await; a detached cleanup task needs a concrete retained join owner and defined behavior outside a live runtime. It may need narrower internal storage. Cannot claim success from merely sending cancellation. |
| One actor execution task owns complete post-Driver settlement | Let the existing actor task itself settle its returned activation tasks before publishing a joined result, while a caller owner retains cancellation/join authority. | This may change coarse publication ordering or panic classification and does not by itself solve dropped startup/app work or source-wait liveness. Projection evidence is separate. |
| Specifically owned application execution task | A spawned application task retains root, work and family shutdown values; a caller cancellation signal requests the selected cleanup policy, and a retained owner observes the task result. | A bare `tokio::spawn(...).await` still detaches on caller drop. Arbitrary work future cancellation and return-value disposition need an exact law; adding a generic runtime object solely to hold a handle is rejected. |
| Abort the actor task on owner disappearance | Tokio `JoinHandle::abort` plus existing `Incarnation::Terminal` drop classification. | Cancels in-flight effect/source/retirement futures and can lose accepted effects and descendant custody. It may be valid only for an explicitly separate forced-abort failure contract, never as a quiet substitute for cooperative retirement. |

Compare plain inherent methods and consuming functions first. A private sum is
eligible only if it makes mutually exclusive real authority states
unrepresentable otherwise; no semantic bool, trait object, dynamic task
registry, callback erasure, or new actor scheduler. The current owner value
already has unique state: one join permission and one cancellation request.
Any replacement must identify what machinery it deletes and which concrete
consumer needs its new invariant.

## Gate blockers and handoff

- **Source-wait equation unresolved.** `next_source -> None` means
  `SourceClosed` at the Driver boundary. The selected cooperative owner
  cancellation cannot use that same value while promising `OwnerCancelled`.
  Establish a witness and determine whether an existing typed control input,
  a bounded Bombay-local sequencing rule, or a separately justified Engine
  port change is necessary. Do not infer an Engine feature from compiler
  friction.
- **In-flight effect custody unresolved.** The Behavior/Engine ordinary return
  preserves complete action settlements; dropping interpretation does not.
  Specify which accepted effects may finish and where the remainder resides
  before choosing a cancellation mechanism.
- **Application-work and family ownership unresolved.** A cancelled arbitrary
  caller callback has no return receiver, and family shutdown currently runs
  after root execution in the same future. State whether the callback is
  cancelled or awaited and who completes family shutdown; no implicit
  graceful-work promise. Native Entity retirement currently awaits the
  actor's termination before it can send its owner-retirement request; verify
  that forced retirement cannot wait forever on a Behavior that ignores an
  accepted shutdown request.
- **Projection and sibling panic unresolved.** See the linked record. A parent
  panic currently can interrupt its sequential child retirement before all
  siblings are requested to retire.

Expected production files after a candidate is accepted: `launch.rs`,
`local.rs`, `application_runtime.rs`, `child_bindings.rs` and possibly Entity
integration, later assigned to the frozen ownership hierarchy. Public types
expected: zero unless a compile-only witness proves a distinct user authority.
Production line delta, exact signatures, compiler diagnostics, and test
commands/results are **undetermined**; this keeps DG-TASK open. The focused
regressions above must first fail on the original design for the intended law
in debug and optimized builds, then pass after the selected change. The
coordinator must record its pre-edit ledger and obtain the PRD's independent
reviewer disposition before WP-TASK implementation.


## Current cancellation and failure-custody research (2026-10-02)

Baseline and exact selected owners are in PRD section 16. Current startup and
normal-finish abandonment regressions pass in both profiles; they must remain.
The actor task already settles capability work before returning its exact
outcome. Projection no longer owns that settlement progress.

Standard source admission synchronously sends the exact control event before
returning acceptance. Its actual ready-control witness passed in debug and
optimized builds (commands in `verification.md`). Missing cancellation polling
in `next_source` alone is not a reproduced external-operation defect. Reachable
pending boundaries include bounded delivery pressure, child startup, Entity
admission, ordered child retirement and post-Driver capability settlement.
No Engine cancellation extension is selected by this research.

The user asked for further research and requires maximal truthful information
and correctness in core execution. Distinguish cancellation intent, actor
termination, completed joins and disposition of receiverless values. A token
proves none of the other three events. No source closure, ordinary stop or
successful join may substitute for its actual provenance.

Primary comparisons:

- [Tokio 1.53.1 task cancellation](https://docs.rs/tokio/1.53.1/tokio/task/index.html#cancellation)
  destroys a yielding task future; joining establishes completion. Running
  blocking work generally cannot be aborted. It does not complete async cleanup.
- [Tokio JoinHandle](https://docs.rs/tokio/1.53.1/tokio/task/struct.JoinHandle.html#cancel-safety)
  supports cancellation-safe borrowed waiting, while dropping the handle
  detaches execution and loses its result. Result custody needs another owner.
- [Akka graceful stop](https://doc.akka.io/libraries/akka-core/current/actors.html#graceful-stop)
  coordinates subtree stopping and postStop; long-running work can stall it.
  [Coordinated shutdown](https://doc.akka.io/libraries/akka-core/current/coordinated-shutdown.html)
  has explicit phase timeouts and failure policy, which cannot substitute for
  Bombay's complete joined-return law.
- [Erlang processes](https://www.erlang.org/doc/system/ref_man_processes.html)
  distinguishes exit signaling, trapped exit, resource release and reply-alias
  deactivation. Native work may outlive directly visible process resources.
- [Orleans cancellation](https://learn.microsoft.com/en-us/dotnet/orleans/grains/cancellation-tokens)
  requires cooperative observation by running calls; acknowledgements and
  partial streaming results have distinct semantics.

These comparisons do not select Bombay policy or prove rollback. Completing
current interpretation preserves its exact settlement but can delay retirement
indefinitely. A typed safe cancellation boundary must instead retain its
current action, untouched suffix, admitted events and outstanding work.
Capability-specific cooperative cancellation requires its owning typed outcome;
arbitrary future abort cannot invent exact recovery.

### Compiled recoverable-custody loss

The actual source was copied from baseline into
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-activation-custody-mpqp4jpv`.
Only a 110-line test module was appended. Existing production functions and
the selected lock remain unchanged. Tests call actual
`LocalResidual::settle_activation_tasks`, not a copied model.

The first task returns a move-only event [11]; a later task waits on a gate.
Direct settlement polling is pending with zero witnessed drops. The later task
then panics without owning any witnessed value. Before the actual JoinError
returns, the complete sorted destruction inventory is: returned event [11], retained settlement
[22], accepted control [33], accepted user [44], descendant terminal [55].
Every payload is destroyed exactly once. `ActivationTasks::settle` first drops
its accumulated events; the residual's `?` then drops the other owned values.
All five were recoverable outside the panicking task.

The characterization test
`later_activation_panic_destroys_returned_event_and_actor_residual` passes;
the intended conservation oracle
`later_activation_panic_preserves_recoverable_actor_custody` fails.
Both commands ran from the isolated directory:

```sh
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked -p bombay-rs --lib activation_custody_research -- --nocapture --test-threads=1
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked --release -p bombay-rs --lib activation_custody_research -- --nocapture --test-threads=1
```

Each compiled and exited 101: one characterization passed and one conservation
assertion failed. Patch SHA-256:
`9a84c0c153c208dc648ae7c0ffbe32c970914241fe2d7c154cb42cf9f4762d85`.
Experiment delta: tests +110 / -0; production/public API zero. Repository
delta from this experiment is zero. This proves original recoverable-custody
loss, not a fixed representation or recovery of unavailable values destroyed
inside the panicked task. A replacement must preserve ambient actor outcome,
exact returned events and actual failure provenance, then prove the remaining
work's disposition and join barrier. DG-TASK remains open.


Independent original-evidence review: `/root/contract_inventory`, which authored
neither observation nor custody experiment, inspected both patches, all
positive/failed debug/release logs and the unchanged lock. It confirms the
custody claim against patch
`9a84c0c153c208dc648ae7c0ffbe32c970914241fe2d7c154cb42cf9f4762d85`
and instrumented local source
`f1c5ad61fc6c4706d3f675927c34c3539cd49c8ebca2b62200f47d6ea80b3018`.
Sorting proves the complete inventory and exactly-once destruction, not
chronological drop order. This review certifies research evidence only;
DG-TASK remains open with no replacement candidate accepted.


### Ordinary-Rust custody comparison and selected failure law

The isolated comparison at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-capability-retention-ftl9frah`
adds only a 204-line cfg(test) module. Candidate routines consume the actual
JoinSet and exhaustively reconstruct the current LocalResidual and
ActorExecutionOutcome alternatives, returning ordinary products of exact
outcome and `Vec<JoinError>`. No named wrapper or public type is introduced.
The positive witness includes two panics, one cancellation, first and later
returned events, final Behavior, accepted control/user messages and descendant
terminal custody. It replays twice; no witnessed value drops before the
consumer releases custody, and every move-only value then drops exactly once.
The concrete retained settlement collection is empty in this comparison, so
this test does not prove a nonempty settlement payload.

From the isolated directory, both commands exit zero with one passing test:

```sh
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked -p bombay-rs --lib capability_retention_comparison -- --nocapture --test-threads=1
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked --release -p bombay-rs --lib capability_retention_comparison -- --nocapture --test-threads=1
```

Patch SHA-256:
`e6ebb575e509aabcecab274d380ac4733275c90cb4de2a31d15233c30c97fc35`;
candidate local.rs SHA-256:
`e00f4fe747793787083396f191ed1fd3006fa8fd06d33c378095afc9dcebcaef`.
Scratch delta: tests +204 / -0; production/public types zero. Retained
repository delta from the experiment: zero. These test-local alternatives
prove feasibility, not integrated runtime or public-consumer behavior.

The user selected the core capability-failure law on 2026-10-02: **preserve
the complete actor outcome and all available task failures in a typed result,
and join remaining capability work before returning**. Noncooperative work
may keep that join pending indefinitely. Values destroyed inside a panicking
task cannot be recovered. This selects the law, not the product spelling,
public interface or gate representation. Live acquisition failure, child
projection, caller disappearance, Entity ownership and receiverless-value
disposition still need their own proofs or decisions. DG-TASK remains open.


Caller-disappearance discussion: the user requested further explanation rather
than selecting receiverless release. After separating arbitrary application
work, retained cleanup ownership and complete-result custody, the user
authorized proceeding with the **separate completion receiver comparison**.
Cancelling execution need not surrender receipt of complete cleanup results.
This is a preferred research direction, not acceptance of an API or final
receiver-drop policy. Keeping results permanently would be an unbounded
retention policy; silently releasing them would be an unselected discharge.
Both must remain explicit. Application-work disposition and the exact
root/children/Entity transfer equation remain open until proved and selected.


Source-cancellation comparison rejected as currently justified: returning
`SourceCustody::Retained` from offer_next solely because an owner request is
pending would let the Driver reach ordinary cancellation acquisition. However,
the locked variant means that no live source input remains and exact terminal
custody is required; Driver retained products are not offered again. An
unoffered live source suffix cannot silently be relabelled terminal-only
custody. The variant's storage capacity is not authority for that semantic
change. No such implementation or new Engine port is selected. A typed owning
cancellation disposition would need independent contract evidence.
