# Task custody decision evidence

**Frozen research snapshot:** The ARC-011 change on 2026-10-01 resolved the
startup/finish waiter and activation-task settlement failures described below.
The retained typed projection task and its regressions are recorded under
ARC-011 in `docs/prd-backlog/status.md`. Current EXEC selects Core/Actors
0.21.2; PRD section 46 records the reviewed release and fresh source verification. Failure candidates and
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


### Compiled transitive-source cancellation characterization

At the same isolated capability-retention directory, a separate 182-line test
module appended to baseline application_runtime.rs uses the actual Driver,
LocalEnvironment, ActionInterpreter, ApplicationCapabilities and ScheduleAfter
source action. Patch SHA-256:
`7190ab385846c8c816aab889c628a5db6556c736e38fcb5bf2f2216a3e0e3ca1`.
A test-only wrapper inserts barriers and yields after every admission; it does
not replace interpretation or the Driver. The first exact receipt is admitted,
then the owner sends cancellation before source acquisition. Sixteen finite
receipt folds continue to admit replacement schedules despite explicit yields.
Initial activation is not published and the address is never resolvable.
An independently enqueued typed ShutdownRequested then ends the recurrence.
The actual result is Stopped with Retired residual, not OwnerCancelled or
SourceClosed: one exact accepted timer receipt remains in control ingress,
The observed 18 complete Accepted settlements have empty source/creation lanes;
that count is reported in both logs rather than asserted as a law. User
ingress, descendants and capability tasks are empty.

From that directory, both commands pass one characterization test:

```sh
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked -p bombay-rs --lib transitive_source_cancellation -- --nocapture --test-threads=1
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked --release -p bombay-rs --lib transitive_source_cancellation -- --nocapture --test-threads=1
```

Logs: source-debug.log and source-release.log. This demonstrates cancellation
not being polled during a standard transitive chain even when executor fairness
is supplied. It is not a failing regression against an accepted bounded
cancellation law: no arbitrary 16-fold deadline is selected. A proper original
noncompletion oracle and repaired witness remain required once the typed
retirement boundary is selected. Retained repository delta: zero.


After this characterization, the user selected the source-cancellation law:
**require typed retirement with exact remaining custody**. Preserve actor
state, admitted receipts and unoffered settlement remainder rather than
waiting for an endless transitive chain. An already running noncooperative
operation may still need completion; its receipt must remain distinct from
owner cancellation. This is law selection, not acceptance of an owning
contract change, port, public type or gate. The original noncompletion
regression and concrete ordinary-Rust ownership comparison remain required.


### Selected receipt law and layer ownership

After further ownership discussion, the user selected explicit receipt custody:
waiting and receipt ownership are separate; cancelling a wait preserves the
retained exact-result receipt. Surrendering the final receipt relinquishes
result custody; retained cleanup still joins, then explicitly releases exact
undeliverable results once. There is no implicit global result store. This
selects no public wrapper or interface; the callback and Entity integration
proofs remain required.

The actual-launch comparison was strengthened to three passing tests in both
profiles, including final-receiver surrender while execution still joins before
result release. selected-complete-receipt-law.patch SHA-256:
`3b38c53c503350dcad206b93828c39c6d3318bbcffa2c348f6c670027ab01111`;
launch source SHA-256:
`36ef89c0c72b9b975c63064b80565f8a723d96db467c6fde9fa2737dafc7d170`.
Commands use the same pinned-Nix root, package and complete_result_receiver_comparison
filter recorded in the artifact directory. This appends 118 test lines to
baseline launch.rs; earlier two-test receipt evidence is superseded. Current
owning_outcome panic can still prevent result publication until the selected
capability-failure conservation law is integrated. Universal receipt delivery,
arbitrary callback work, descendants and Entity families are not proved here.

Original source noncompletion now compiles and fails its intended assertion
in debug and release; source-original-fail-{debug,release}.log and patch
`cc7e8cfc085fd31f318ab65efe8b66f82f99f0936c2702487a1634e413345bb6`
record a 250ms watchdog observing noncompletion, not a domain deadline.
Test cleanup aborts the task; no production timeout or abort policy is selected.
The original-only setup waits for continued progress. A repaired regression
must instead observe retirement versus progress, preserving exact custody, so
early correct retirement is accepted rather than rejected by that setup.
Independent reviewer `/root/contract_inventory` confirms the compiled
original failure and this positive-oracle limitation.

A separate actual two-request characterization retains the exact unoffered
second accepted schedule in the oldest settlement suffix, in addition to
actor state and the admitted first receipt. Both profiles pass one test. Patch
SHA-256: `fda950ce82c58b4f34a7bef1cbd6a85e8933a787c15fccc10974125955e93b5d`.
It appends 193 test lines to baseline application_runtime.rs; it is not a
selected-retirement fix. Current isolated overlay is local.rs +204 test lines,
application_runtime.rs +193 and launch.rs +118: 515 test lines, production/new
public types zero. Manifest selected-custody-research-receipt.json SHA-256:
`2b3463d1a46864c4be06a247bf25f3674827b8a14df2cb8ee8b262ad57e5f7eb`.
Retained repository production/test/public-type delta remains zero.

Ownership audit: source acquisition and its exact Driver disposition belong
to Engine; source normalization/admission remain Behavior/Communication laws.
Concrete cancellation facts, actor task joins and application receipts belong
to Bombay. Observation protocol and reusable monitor policy belong to Behavior
Actors, while Bombay owns queue execution. ActorExecution, its outcome,
Retirement and TerminationPublication are crate-private despite pub declarations
inside private modules. The generic TerminationPublication Retirement adapter
has one direct unit-test consumer and no production consumer. Removing that
adapter cannot by itself supply a lawful source-neutral Engine retirement
classification. Existing LocalRetirement publishes Crash::Cancelled only from
actual typed OwnerCancelled custody; all representable new completion/residual
combinations need a truthful disposition. No generic cancellation policy or
new port is accepted from this audit.

## Affine source-retirement comparison (2026-10-02)

Within the authorized source-stage surface, the researcher froze an ordinary
unit-disposition alternative in the same capability-retention-ftl9frah directory:
`source-unit-comparison.patch` SHA-256
`c496281d05de85f4f9085da34b13e1fbef01a788c3736366bbfbb53befb20ec4`;
receipt SHA-256
`5b6965fda5ca512e604775f549ae6325467d904d23d2ceef4f1300cb33eb5622`.
The coordinator independently verified both hashes and inspected the owning
acquisition, Driver and publication changes. Production +47 / -30 / net +17;
tests +534 / -7 / net +527, including earlier isolated comparisons; zero new
public types. Five production paths and four additional test-only paths change.
These are isolated deltas, not retained changes or gate acceptance.

The alternative uses `ControlFlow<(), Option<Event>>` for source acquisition
and a distinct `Completion::RetirementRequested`; the exact non-Clone owner
request remains owned once in existing `LocalResidual::OwnerCancelled`.
Conservation alone does not justify copying that affine fact into a generic
completion. The debug source witness passes, as reported by its author;
optimized and independent behavioral review remain required. The private
ordinary publication path currently panics on the new disposition, relying on
the producer's exact residual correlation. That precondition and coarse
publication are not accepted from the direct-Driver witness.

Independent source review also found that ordinary `next`, unchanged in this
candidate, stores the owner request but returns `None`; Driver then labels it
`Exhausted`. Existing exact local cancellation projection does not make that
Engine claim of permanent source exhaustion true. A direct ordinary-acquisition
falsifier and a comparison covering both acquisition ports are required before
retention. Any additional consumer paths must be measured against the checkpoint.

## Single-owner retirement-cause review (2026-10-02)

The revised isolated candidate transfers the exact non-Clone, non-Copy owner
request once into `Completion<Request>`. It deletes the duplicate cause from
LocalResidual. Both ordinary and source acquisition return
`ControlFlow<Request, Option<Event>>`; a retirement request is distinct from
permanent source exhaustion. Driver restores the unoffered settlement suffix
to its causal position before retirement. Public ActorRetirement continues to
separate exact OwnerCancelled from ordinary Completed with `Completion<Never>`.
No new actor policy, capability map or coarse generic policy is introduced.

The final source-only patch `source-candidate.patch` has SHA-256
`770d29797287485243e6b169480d7b03c6d80103772e5396b80117fa70fe8f08`;
receipt SHA-256
`676dfc1e686c85fb0e0d0614216bbf3556962362683b9d9750580aafd371f854`.
Both reside in the previously recorded capability-retention-ftl9frah directory;
`source-candidate/` contains the frozen source. It covers 18 canonical paths
within the authorized stage: production +239 / -175 / net +64;
tests +891 / -63 / net +828; public types +0 / -0.
These are isolated measurements; no production correction is retained.

Author `/root/task_custody_research`; independent reviewer
`/root/observation_research` verified every snapshot and clean-stage source hash,
all seven verification logs and the four inversion cohorts. The final fixture
adds an observational `Step::Continue` assertion to the already checked complete
nonempty source settlement. Its focused source-retirement test passes once in
each profile, and strict workspace/all-target Clippy passes after that addition.
The preceding candidate passed all 228 Bombay library tests in both profiles
and formatting. The coordinator then verified the final unchanged 18-source
freeze in the clean stage with `cargo test --locked --workspace --all-features`
and its `--release` counterpart, each through the pinned Nix shell with separate
external targets. Both exit 0: 61 result summaries, 418 passed tests per profile.
Log hashes: debug `e77e04d7146e9057eb42e097bc1af0f9644bdf4fd3cd2fdcb6bb9c0c05d8c170`;
optimized `31fbfeb884cdcb375aa0d10389e694f72181202854948f5ad61829e8649e482d`.
Logs reside beside the source-retirement-clean-30jnh_oa directory, suffixed
`-final-debug.log` and `-final-release.log`. This validates the isolated source
correction, not later capability, receiver or Communication integration.

The witnesses exercise ordinary acquisition, a real ScheduleAfter source with
an admitted receipt and unoffered remainder, jointly ready mailbox/local facts,
and cancellation while retaining the same previously pending acquisition future.
They preserve exact move-only state and events, every settlement lane, lease
retirement, exact OwnerCancelled projection and coarse Cancelled publication.
The original unbiased mailbox/local arbitration is preserved inside an outer
owner-request boundary. A prior three-way biased comparison incorrectly favored
the mailbox; that candidate was rejected rather than changing XO-36.

Inversion receipt SHA-256
`afe47abe795c6ba25f89928ef913866cab6fc7b94c533ceb30c5c020f110df8a`
authenticates compiled debug and optimized failures for omitted owner acquisition,
the original recurring source chain, false Exhausted classification and false
public cancellation projection. Each fails its intended law. Those runs precede
only the final additional observational assertion; no mutant remains in the
candidate. Truthful Driver/runtime drafts and the public-interface audit were
independently reviewed, but the changed acquisition contract is not yet current
repository guidance.

The independent review accepts the narrow acquired-request conservation and
source-arbitration evidence conditionally. It approves no full DG-TASK gate.
Required retention conditions include the recorded compatibility cost, cumulative
surface checks, integrated verification and the remaining design-gate conditions.
Capability-failure production, caller cleanup, Entity cleanup and the selected
unread-request law remain unimplemented. Earlier test-only alternate settlers
cannot establish those production laws.

Source compatibility cost: the generic Never default does not infer an
unannotated standalone `let completion = Completion::Stopped`; an actual
compiled probe reports E0282. `let completion: Completion = Completion::Stopped`
or a type inferred from Driver output provides the request type. This cost
must be judged explicitly before interface retention, rather than hidden by
the default or treated as architectural necessity from compiler output.

## Unread owner request conservation (2026-10-02)

The user selected the recommendation to retain an accepted unread cancellation
request with the exact result when an independent result receiver has no caller
request context. For example, actor stop can win before the cancellation is
read: the actual outcome remains Stopped, while the unread request records what
was asked. Stopped alone establishes that cancellation did not cause the finish;
it cannot establish whether anyone requested it. Preserve each fact once, with
no second marker repeating the actual completion reason.

This is Bombay's concrete owner-request/result contract. Engine continues to
own generic acquisition and the exact request it actually acquired; it must not
invent an actor-specific unread-request policy. Distinguish accepted queued
requests from sends rejected after the receiver closes. Review the exact
close/drain race, surviving receiver ownership, complete typed result equation
and final-receiver discharge before retention. No public type, field or wrapper
is selected by this law decision. The current zero-new-public-type source and
mailbox checkpoint does not authorize an unseen result-interface expansion.

## Distinct live capability failure observation (2026-10-02)

The user selected a distinct background-operation failure cause for ordinary
termination observers when a capability task fails while its actor is live.
Do not label this owner cancellation, a Behavior-requested stop or Driver panic.
The complete result must also preserve the exact actor state and all available
actual task failures while joining remaining work, under the already selected
unbounded cooperative limit. A task failure discovered after an actual completed
actor outcome is a coexisting result fact; it must not retroactively rewrite that
outcome or an already published termination.

Bombay owns concrete task acquisition, retirement and result custody. Behavior
Actors owns the shared terminal vocabulary and template consumers. Selected
0.20.0 `termination.rs` (SHA-256 `bb74a3f6b9344008a1f3d4dbc0f494249e494454994daa7be33592c1eddbc1b7`) provides only
Failed, EnvironmentFailed, Panicked and Cancelled; its documented terminal law
preserves provenance rather than reconstructing it from an adjacent diagnostic.
The new distinct cause therefore requires owning-contract verification and an
independently reviewed Behavior Actors change before dependent integration.
Foundational Behavior's deterministic fold does not own this executor policy.

The user additionally selected immediate retirement of only the owning actor
when its capability task fails while it is live. Preserve the first failure
at the typed retirement boundary, then join remaining tasks and keep any later
failures. Do not wait for the actor to stop naturally after the operation that
could supply its next event has failed. Parent and peer reactions retain their
existing explicit policies; this is not automatic global propagation.

This is a selected law, not a selected representation or an accepted gate.
The current 52-path, zero-new-public-type source/mailbox allowance does not
include this upstream contract expansion. Compare live acquisition and completed
retirement separately, audit owning consumers and record a measured expansion
before any additional production edit. Keep the backlog prerequisite current.

## Pre-start completion custody (2026-10-02)

The user selected returning untouched application inputs to the independent
result receiver when execution is dropped before it starts. No actor exists
at that boundary: do not fabricate an ActorCancelled terminal or actor origin.
Preserve the unstarted declaration (root, declared actors and family definitions)
and the uninvoked work input once. The final-receiver surrender law still applies
if no receiver remains. Original inputs and a started complete result are
alternatives, not duplicated fields beside an actor outcome.

Compare ordinary `ControlFlow<OriginalInputs, FinishedResult>` with a named
closed completion sum; `(application, work)` can group existing exact inputs
without a new input wrapper. An actual application seam and pre-first-poll Drop
witness are still required. Existing work callback/future/output bounds allow
borrowed, non-'static values; spawning the entire application or its combined
publisher cannot silently strengthen those bounds. Separate static actor cleanup
from caller-owned input/work/result custody using existing affine handoffs.
No public execution product, extra runner or final representation is selected.

## Actual capability-source custody comparison (2026-10-02)

The isolated comparison at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-capability-source-custody-i613dlhm`
uses actual interpreted observation tasks and the actual LocalEnvironment/Driver,
on the final source-retirement candidate. Two of four capability tasks panic
during terminal-event conversion outside the Behavior fold. After the first
failure is joined, the other targets are released; two exact termination events
remain recoverable despite Communication control closure. An ordinary join-all
comparison retains both actual JoinErrors and the complete already-stopped actor
outcome: state, accepted user input, nonempty descendant custody and the admitted
timer receipt plus exact rejected timer remainder. Required operations occur
outside assertions.

The original live acquisition path instead unwinds the incarnation task and
destroys its still-recoverable state. Its preservation oracle fails for that
specific law in both profiles. A separate inversion of the join-all comparison
returns early after the first failure and loses later events; it also fails in
both profiles. These are distinct failures, not a claim that the live path has
been repaired. The positive fixture characterizes the original live failure and
proves only the stopped-outcome joining comparison.

A separate actual cleanup witness borrows the existing affine result future,
polls Pending and drops that borrowed wait. Dropping the owning execution wait
then requests cleanup. Reawaiting the same retained receiver yields the exact
actor result after cleanup; the actor value drops once when that result drops.
An opaque `Future + Unpin + Send` therefore suffices for this narrow receiving
seam; a new public receiver wrapper has not been justified.

Patch SHA-256:
`ad2f5e14da403169ead3a1362fab10b6d8d06a0a21eccd9c3310374f1f21eba1`;
final receipt:
`88acb61f76d91b6bcdb9f1ace02b19ed683bdd8daf0ac91e14eb73184565ec5e`;
final two-law inversion patch:
`34bc70329b2a028dfc3d51b6979b52e1b317ded75253506b90baba93964fa421`.
The receipt records exact commands, eleven logs, source hashes and limitations.
All commands use the pinned Bombay Nix shell and `cargo test --locked`,
`-p bombay-rs --lib`, with debug and `--release` variants. Filters are
`capability_failure_custody`, `borrowed_completion_wait`,
`actual_registered_capabilities` and `original_live_capability`; each run executes
the intended one or two tests. Both final positive profile runs pass two tests;
the borrowed receiver passes one each. Both combined inversions fail two each;
the restored final fixture passes again. No zero-test run is evidence.

Independent reviewer `/root` read the complete patch and authenticated all three
source hashes, both final patches and eleven logs. This accepts the bounded
source-failure, join-all and borrowed-receiving evidence, not DG-TASK. The
descendant fixture is exact opaque custody, not actual child registration; only
the Completed outcome is compared. The actor failure/error alternatives, real
child projections, all sibling joins, pre-start inputs, application lifetime
bounds, family cleanup and implementation of the selected live failure policy remain required. Available
incarnation, capability and projection failures must retain their distinct
provenance. The first failure's task cannot yield values it already destroyed.

Isolated test-only delta: three already authorized paths, production +0 / -0;
tests +637 / -0; public types +0 / -0. These lines are research evidence and
are not a retained test design or a grant to expand the production surface.

## Read-only task and application consumer forecast (2026-10-02)

`full-task-consumer-inventory.json` in the same scratch directory (SHA-256
`e47d47d50ef71b06a05001d4cdf2427da82fe69a665155d1fde4ef8942e5030a`)
records the exact 52 authorized paths and a prospective task/application
migration. Their union is 123 paths: 71 additional paths, including the owning
Behavior Actors terminal vocabulary/consumers, Bombay result consumers and macro
expansion, paired diagnostic fixtures, current guidance and executable examples.
This is a forecast, not authorization to edit those paths.

The recorded estimate is Bombay production +230 / -100 / net +130 and Behavior
Actors +5 / -0. With independently measured source retirement +64, shutdown +1
and Communication +49, the cumulative forecast is +249, exceeding the approved
+150 ceiling. Currently private OwnerCancellation may need to become publicly
nameable with private construction; a named application execution or failure
product remains unproved. Do not select additional public products to fill an
allowance. Complete the actual ordinary-Rust lifetime/result comparison, then
show a concrete bounded expansion and obtain user approval before production
edits. Observation authority and later module extraction are separate scopes.

## Application work disposition (2026-10-02)

The user selected immediate release of an unfinished application work future
when execution is dropped. Retained actor/child/family cleanup still joins and
the independent receiver keeps its exact results. Work that already completed
keeps its exact returned value; unfinished work has no returned value. Do not
add a redundant completion flag beside that sum. This is an explicit discharge
of the pending future, not a claim that external effects were rolled back.

Before the callback is invoked, it remains an untouched owned input. Root
startup rejection therefore must return that callback together with the exact
root rejection and actual installed-family shutdown facts. It cannot claim an
untouched whole application after staging, initialization or family installation.
The pre-first-poll original-input law remains the distinct earlier boundary.
Actual pending-work Drop, completed affine output, cleanup/family races and
public syntax proofs remain required; this law selects no extra result wrapper.

Fresh current-source check: application_runtime.rs LaunchSystem::launch_with
owns the callback across startup, whose `?` discards it uninvoked on rejection.
It awaits application work before joining root; dropping that future releases
unfinished work and preserves the existing OwnedTask cleanup authority. A
completed output instead remains in the same waiting frame and is lost on its
drop. There is no independent full-result receiver at this seam. Public
App/Application run_with still synchronously calls block_on, so an unpolled
public execution-drop witness requires the accepted async API first. Private
LaunchSystem Drop evidence cannot substitute for that public contract.
Reviewer `/root/task_custody_research` and `/root` independently read these
current paths; this is source evidence, not a newly executed regression or
acceptance of the missing result-custody implementation.

The same read-only audit identifies these missing actual-application witnesses;
ordinary tuple comparisons do not substitute for them:

| Boundary | Required observable custody |
| --- | --- |
| Public execution never polled | Original root, declarations, family definitions, Spaces identity and uninvoked callback return untouched. No public execution future exists yet. |
| Startup waiter dropped after spawn | Uninvoked callback and exact cleanup results survive; existing reservation cleanup still completes. |
| Pure initialization rejection | Callback invocation count zero, original callback capture and exact recoverable rejection returned together. Existing tests prove only no invocation. |
| Invoked work Pending, execution dropped | Affine work future released exactly once; root, child and native Entity cleanup and result receipts survive. Also test an already-stopped root while work stays Pending, preserving XO-07. |
| Work completed, execution dropped during root/family join | Original move-only output, including an application Result::Err payload, survives without inventing a runtime failure. |
| Root rejection after family installation | Uninvoked callback, actual root rejection and actual family shutdown/metrics products coexist; do not claim an untouched whole application. |

Current run_with_entities awaits installed.shutdown but discards its product
through outcome.map on root rejection. Completed output remains in the same
future across that shutdown. Current callback/future/output bounds are Send
without `'static`; lifetime decisions may not be inferred from spawn bounds.
ARC-011 cleanup requests and ARC-012 restored Entity task records remain valid
and must be preserved. These witnesses depend on accepted application/task
interfaces and the selected borrowed-value discharge rule; none was executed
or accepted by this source audit. Source SHA-256:
`0b8e2c1c475ab46c50c451082b8d67dadb0a138e1d3ee8ccca9129ee7b7f57c4`.

## Borrowed output and receiverless cleanup comparison (2026-10-02)

The frozen `bombay-receiverless-cleanup-custody-c7app_dl` comparison uses actual
root retirement and existing affine Observe receipts. Completed application
output contains an original move-only Vec and a caller-borrowed slice; static
cleanup owns the actual root task. Receiving after cleanup preserves the exact
output and root result. Surrendering both receivers before cleanup still lets
the same retained cleanup task join and release the undeliverable root once.
An omitted-join inversion fails its intended root-release assertion in both
profiles; it does not prove a permanently aborted cleanup task.

This last disposition releases completed work output before cleanup joins.
It therefore did **not** satisfy the original complete-result discharge timing;
the later selected caller-owned discharge rule below explicitly amends it.
Retaining a caller-borrowed output beyond disappearance of
all caller-owned futures/receivers needs a lifetime proof; do not silently add
`'static`, permit earlier discharge or call this full DG-TASK acceptance.

Receipt SHA-256:
`61e42e39952d27fe8855611fba25a450245ac262681b1f34947640dbb27a938c`;
complete comparison patch:
`f73ea303c4fe4dae54b6f0255fa6401fed3ef28205550e35e86ca8e31297b8dd`.
The receipt records fifteen authenticated artifacts and exact pinned-Nix
commands. Debug/release each pass two tests covering three output dispositions;
formatting and strict Clippy pass; the compiled join omission fails one test
in each profile and restored positives pass again. Independent reviewer
`/root/contract_inventory` authenticated the artifacts and independently reran
both positive profiles. Its signature covers the bounded research trace only.

One existing application_runtime.rs test module, production/public types zero,
complete tests +426 / -0; extension from the preceding frozen comparison
+28 / -1. This uses the historical Communication 0.1.2 lock and isolated
source-retirement patch 770d2979, not current 0.1.3 integration acceptance.
Native family cleanup, pending-work Drop and actual public execution remain
unproved. A named public execution or receiver wrapper is not justified here.

## Joint-ready cancellation and capability failure (2026-10-02)

The isolated actual Environment witness in
`bombay-joint-retirement-custody-pv1_15m9` completes a real capability panic
before accepting owner cancellation. Both are ready at the next source poll;
the existing owner-first port acquires cancellation first. Retirement retains
the actual JoinError, joined exactly once, beside that affine request and the
complete initialization settlement. Acquisition order is not physical failure
chronology. No actor completion or observer cause is asserted by this witness.

Receipt SHA-256:
`d9c4c65034f8ce650e5d560598c58f1e17531ad88d519343c3ff18fe4320cf5d`;
patch `bff9ba3d57fe0417ef38f86712f957bda725f71c0660f13702143a45e048bac7`.
Debug/release each pass three tests; formatting and strict Clippy pass.
Omitting owner acquisition compiles and fails one intended test in each profile;
restoring it passes again. `/root/contract_inventory` authenticated fourteen
artifacts and the complete trace, signing this bounded characterization only.
One existing local.rs test module, +109 / -31, production/public types zero.
Its old Communication 0.1.2/source770d baseline is not fresh integration proof.

The primary observer cause when both requests are ready is cancellation-first,
as selected in the later policy amendment below.
Keeping existing cancellation priority needs no readiness probe; the full result
must still retain the failure. Preferring ready failures requires consuming
completed task results and retaining any ordinary events encountered, since
JoinSet has no nonconsuming failure peek. Such a probe cannot claim a globally
earliest failure or atomic readiness snapshot. This frozen witness predates
selection and does not itself prove integrated observer-cause behavior.

## Exact final cancellation admission (2026-10-02)

The actual Tokio 1.53.1 oneshot comparison closes the receiver before its final
drain. An accepted OwnerCancellation survives repeated close and is received
once; another receive is Closed. A later send returns the original rejected
request. Concurrent send/close permits only accepted-and-retained or
rejected-and-returned, without prescribing the winner. A snapshot without
close demonstrably loses a subsequently accepted request when the reader drops.
That compiled inversion fails in both profiles; it models the unsafe proposed
snapshot, not an original public App defect.

Frozen directory: `bombay-owner-admission-custody-p0sdutst`; receipt SHA-256
`7e0a0cb399fdeda70812ba76d748103712af62c66df69b6b71e3f2736e3c02f2`;
patch `42cbf3fc12b6acfe9de53638c3465bfc6764c33280f78b2288bf641fca00253c`.
The receipt records exact pinned-Nix commands: two close/replay tests and one
32-race test pass per profile; formatting and strict Clippy pass. Independent
reviewer `/root` read the complete patch and selected oneshot source/tests,
authenticated all twenty-five artifacts and both dependency source hashes.
This accepts only the primitive comparison. Every actor outcome still needs
integration retaining unread accepted requests; the later policy amendment
selects arbitration without claiming that this primitive witness proves it.
One local.rs test module, +66 / -2; production/public types zero. The historical
source770d/Communication 0.1.2 baseline does not prove current-lock integration.

## Completed output retention and compiler constraint (2026-10-02)

The successor `bombay-completed-output-retention-8nndu4__` keeps the original
completed output in a caller-owned tuple while both receipts drop, joins the
same actual static root cleanup, then releases the output once. Its original
move-only allocation and caller-borrowed slice remain intact. That remaining
caller owner is essential: this is not genuine last-owner surrender.
Moving the same output into independent Tokio cleanup fails E0597 at the
original borrowed slice because spawn requires a `'static` future. This is an
architecture constraint, not a runtime regression. The public API currently
permits non-`'static` work and output; do not silently strengthen those bounds.

Receipt SHA-256:
`25c204648449fe6698255837cbd3635f02a2a6fa646f4cb9eb3b71e1e28d65d9`;
extension patch:
`cfa548c0c2f20dbcf287c5f5a28fc6719a4b000a0aea12b839093888873edeef`.
Two tests pass per profile; formatting and strict Clippy pass; the static
denial compiles to E0597 and restored positives pass. Independent reviewer
`/root/contract_inventory` authenticated all artifacts and inspected the exact
ownership/denial, accepting only those bounded claims. Extension +27 / -8
test lines in application_runtime.rs; production/public types zero. The
historical 426/source770d/Communication 0.1.2 baseline remains explicit.
The later caller-owned discharge amendment resolves that timing. This frozen
comparison remains bounded ownership/compiler evidence, not public API acceptance.

## Selected caller-owned discharge and acquisition policy (2026-10-02)

The user authorized the recommendations and instructed Bombay to choose
evidence-backed recommendations for subsequent policy decisions. This does
not waive explicit change-budget checkpoints or independent design review.

Completed application output remains exact while its receiver exists. Dropping
its last receiver explicitly releases that caller-owned output immediately;
static actor/child/family cleanup continues, joins, then releases its own
undeliverable results exactly once. The same rule applies to an uninvoked
callback and other borrowed application inputs retained by caller-owned
custody. Do not move them into longer-lived cleanup or strengthen existing
Send bounds to `'static`. A surviving receiver must preserve its original
values, including the untouched-input and startup-rejection cases.
This amends the earlier all-values-after-cleanup timing; there is no new
global owner or retention policy. Actual integrated witnesses remain required.

When owner cancellation and a capability failure are both ready at acquisition,
keep cancellation-first primary cause, preserving the acquired failure in the
full typed result. This reports the source actually selected; it asserts no
physical event chronology and requires no consuming failure-readiness probe.
Available failures still retire only their owning live actor when selected;
already completed actor outcomes are not rewritten. Existing source priority
and fairness limits remain subject to XO-36 and independent verification.

## Actual application work comparison (2026-10-02)

The isolated `bombay-application-work-custody-95xkrvff` comparison exercises
actual Application fields and private LaunchSystem::launch_with against the
current Communication 0.1.3 lock. Dropping pending caller work releases its
borrowed future immediately; existing root cancellation and address retirement
still occur. A completed move-only output, including its borrowed slice,
survives loss of the execution waiter through a separate affine receiver.
Dropping the last receiver releases that output immediately while static root
cleanup continues. Normal completion retains the complete root retirement;
shutdown settlement asserts both send lanes, creations and remaining custody.

Receipt SHA-256:
`f87e3043e9f32a8cbc503ea24b3a60d5ed1e82b8c61226bc1a9daf1dd8cd278c`;
patch `66112e9dd43e7cfdf1b0f1f7a191ca58796cba4b545a8c1f5a2b50db1e1cfc78`;
source `b1ce728a305091ae57cb68cc8eaa5f8cafaa1df49f830dd80e2f6f9da219dc15`.
Two tests pass and restored positives pass in each pinned-Nix profile; fmt and
strict Clippy pass. Retaining unfinished work fails the intended drop-count
assertion in both profiles. Returning the completed output directly through
the original execution frame reproduces its loss on execution drop in both.
Reviewer `/root` read the complete patch and authenticated all 31 recorded
artifacts independently of author `/root/task_custody_research`. This approves
these bounded observations, not DG-TASK or the public application API.

The callback explicitly publishes output to the separate receiver before
returning unit: that workaround is not the proposed public API. Actual root
join-result custody after execution drop, child and native Entity-family
cleanup, callback rejection/Err payloads and an already-stopped root with
unfinished application work remain unproved. Existing tests +254 / -10 /
net 244; production, public types and canonical source delta zero.

## Actual original application/startup custody failures (2026-10-03)

`bombay-application-result-loss-1n_9k0i1` adds three original-only witnesses
against current Communication 0.1.3 and unchanged production. After actual
LaunchSystem work starts, execution drop still cancels the actor and retires
its address under ARC-011, but the separate full-root-result receiver closes
without the result. Actual pure initialization rejection retains reason 61
and never calls the borrowed callback, but releases its original captured
allocation instead of returning the uninvoked callback. During gated actual
startup, dropping its waiter retains existing capability retirement and address
reservation cleanup, yet loses the complete unpublished startup result.

The last witness deliberately uses existing RootProbe's initialization Stop.
Its outcome remains Stopped even if an owner request was queued but unread.
It proves no universal ban on temporary publication during continuing
initialization. Full state/settlement/ingress/descendant recovery assertions
after receive are intentionally unreachable on the original defect; they are
not passing conservation evidence. No callback-output publication workaround,
native-family integration, heterogeneous children, unpolled public application
or partial-stage-input recovery is claimed.

Receipt `dcbbc2cca4566aa54465a8d0f1da69408330e2648071612a735ca68fbd2bf8df`;
patch `01debdf71f4402d2f7a50a37eb4a4526ec28b253b2ed1b53f406076933962f8f`.
Both pinned-Nix profiles compile and fail all three intended runtime assertions
(exit 101); formatting and strict Clippy pass. The earlier generic-owner E0284
compiler failure is excluded. Independent reviewer `/root` read the complete
patch and receipt, authenticated all 20 artifacts, unchanged production
prefixes and intended failure logs, and signs original-defect evidence only.
Tests +291 / -2 / net 289, two existing paths; production/public/canonical zero.

The smallest ordinary ownership split must happen before startup.await: the
caller retains the original affine cancellation authority, while static
cleanup owns the existing startup receiver and raw actor join handle. A
borrowed wait on an owning Tokio JoinHandle preserves its output when that
wait is dropped; final handle surrender releases an undeliverable result
only after its task finishes. Caller callback/work/output retain their
existing nonstatic bounds and separate immediate-discharge law. No extra
result cell, cloned cancellation authority or global retention owner is
justified. This is a proposed composition, not a verified repair.

### Current-lock source correction successor (2026-10-03)

The isolated `bombay-source-retirement-current-8wipthay` successor starts from
`3f51c53536d6d4825526e35e0448caed75d824a5`, preserving canonical work.
Selected root manifest/lock hashes remain
`2c5f9bfebcb5cf6db87874995debbbcceac14dbce6886485978b4665acc56c76` /
`1ca7df546ffd2db7406810a32d745c1fe70b891189c42b8a020daa242c6710b4`.
The current frozen patch is
`296d2b10fe55d3ce319aeeb94ad7863279ffa0201a31d0721d08bbb0c43e1afe`;
receipt `94d55e011b5df52123690d890ad127e177e3c57b0d7ee5c0f5f3183af1b48058`.
It removes prohibited test vocabulary and redundant nested async expressions
from advanced consumers, fixes the fuzz crate attribute order, formats excluded
fixtures and updates only the two diagnostic line numbers. The separate fuzz
lock changes local Engine 0.1.0 to its actual 0.2.1 owning version. Nineteen
actual changed paths are frozen; all-path patch +1200 / -292 / net 908 includes
tests and that one-line lock replacement. Independent hunk classification
measures production +241 / -177 / net 64; tests/benchmarks/fixtures/fuzz
+958 / -114 / net 844; lock +1 / -1 / net 0; public types +0 / -0.
Complete archive enumeration finds 19 changed tracked paths, no missing paths,
and 45 untracked evidence files (logs, patches and records), with no untracked
Rust or manifests. Disposable build outputs are excluded explicitly. This
complete isolated delta is distinct from canonical Bombay's working-tree delta.

All-feature workspace verification passes 418 tests and 61 result summaries
in each profile. Strict workspace/all-target Clippy, workspace formatting and
explicit excluded-fixture/fuzz formatting pass. Current-lock inversions restore
actual omission of owner acquisition, recurring-source cancellation starvation,
Driver request-to-exhaustion collapse and public projection misclassification.
All four cohorts compile and fail the intended law in both profiles (eight
exit-101 runs); exact source hashes are restored afterward. The inversion
receipt is `a9cb6ef7964d47ea622a977ec23585d85b527f1ff6f60df4efa1e3555527fedd`.
All three focused source filters pass after restoration in both profiles.
The restored separate locked fuzz check passes; an earlier check overlapped
inversion editing and is excluded. Actual nightly fuzz build and a 10,000-run causal_turns campaign pass through
the pinned fuzz shell. They are bounded generated-sequence evidence, not
an exhaustive proof. The independent reviewer signs this precise source-only
successor after authenticating the three saved mutation patches and matching
each prior-source hunk exactly once. The recurring-source witness observes
16 progress steps with a timed escape; it does not prove an unbounded fairness
or joining deadline. The review does
not accept live capability failure, application result ownership or full DG-TASK.

### Published-version source comparison (2026-10-03)

The isolated `bombay-source-retirement-021-q5lj9xhe` successor preserves the
previous freeze and changes only verified dependency/configuration and Driver
version bindings. Frozen patch
`dbc42f8bff3eec65663f960a46ca83950d3cbd15dfc6090e837602da5664d4dc`,
receipt `bfc2b67ad1dc8f936ac0fe06219f051060df35c7837313d3ce2e39047fe13d78`.
Twenty-five tracked paths change against the same 3f51 baseline, total
+1245 / -337 / net 908; owning production remains the signed 64-net-line
correction. Fresh workspace/all-feature verification passes 418 tests and
61 summaries in each profile, strict Clippy and formatting. All four saved
source/projection mutations compile and fail in both profiles; a saved
executing script records exact restoration, followed by all focused positive
filters passing. The actual selected-version nightly fuzz build and 10,000-run
campaign pass. The independent non-author reviewer authenticates all 25 source hashes and
22 artifacts, reads the saved mutation script and signs selected-version
equivalence and the bounded acquired-request law. Complete classification:
production +241 / -177 / net 64; tests/benchmarks/fuzz/fixtures
+962 / -118 / net 844; documentation +29 / -29 / net 0; manifests/locks
+13 / -13 / net 0; public types +0 / -0. Complete archive enumeration also
finds 145 non-build untracked files: 46 preserved historical evidence files,
25 current evidence files and 74 generated fuzz inputs. No additional
untracked Rust or manifests exist. Build outputs are explicitly excluded.
The current fuzzer's RetirementRequest=Never exercises earlier causal laws;
it does not generate the new acquired-retirement Break case. An actual
typed-request generator and distinguishing fuzz inversion remain required.
These results do not claim full application/task ownership
or canonical source retention; earlier copied probe artifacts stay historical.


### Acquired-request fuzz evidence and minimization review (2026-10-03)

The isolated successor `bombay-acquired-retirement-fuzz-mx4me6fo` extends the
existing causal-turn target with actual move-only requests on both ordinary
and source acquisition. The host retains the original boxed request or
transfers it once into `Completion::RetirementRequested`; complete actor
state is compared with delivered inputs, and original pending settlement
allocations, payloads, order and unoffered remainder are checked against an
independent commit/discharge ledger. The Behavior owns only pure state.

Independent reviewer `/root` read the complete target, incremental patch and
executing inversion script, authenticated all 25 changed-source hashes and
29 artifact hashes, and compared the other 24 sources with the prior freeze.
Three deterministic tests pass in each profile. Mutating both actual Driver
acquisition arms to destroy the request and return Exhausted produces two
intended compiled failures in each profile; exact source restoration precedes
three passing tests in each profile. Strict lint/format checks and the pinned
nightly 10,000-run campaign pass. Receipt SHA-256
`c689a3314b56e73a70ce116dc3e0d48143ccd98d73b5b124ccedc43607c0cf43`,
incremental patch `d2d4738f5fba91a7c09d0f39ec7076b61e43e7cd472c37ce85e04884bd408e8d`,
source `482f1c35c90cdd022549c753c01a6564c468fa1e9aa3800711537fe9d097e06e`.
One existing test/fuzz path changes +392 / -169 / net 223; production and
public types remain zero. The aggregate isolated delta is production
+241 / -177 / net 64; tests/benchmarks/fuzz/fixtures +1305 / -238 / net 1067;
documentation +29 / -29; manifests/locks +13 / -13; 25 changed tracked paths
and 204 non-build untracked paths (71 prior records, 30 current records,
103 corpus inputs). No additional untracked Rust or manifests exist.

This signs bounded acquisition evidence, not canonical retention. Arming-only
cases exclude a false acquired-request result; they do not prove the exact
alternate terminal cause. Pending-future drop proves release, not asynchronous
recovery. Replaying both ports on the same host after explicit queue discharge
proves affine request nonreplay, not a restarted Driver. Chronological
retirement-last ordering is not asserted by the replacement ledger.
The aggregate-drift review reopens minimization: research-mechanic actor/host
names and a plan sum duplicating `Option<(port, occurrence)>` must be corrected
in a separately frozen successor. Full task/application gates remain open.


### Live-failure ownership model for next experiment (2026-10-03)

This is a pre-edit model, pending independent review and fresh selected-version
regression; it accepts no new public interface. The existing actual
capability-source witness proves that joining failure as unwind destroys
recoverable actor custody, while joining all results preserves it.

Use the existing Engine request parameter with one Bombay-private closed sum:
owner cancellation owns its original request; capability failure owns the first
acquired `JoinError`. `CommitActions` and `RetireCapabilities` return ordinary
`Result<Event, JoinError>` at their existing local-input seam. A local failure
becomes the exact Driver Break request. Engine gains no Tokio dependency or
actor policy. Preserve owner-first acquisition and the existing inner source
arbitration, including their recorded fairness limits.

`ActivationTasks` joins all remaining work and returns the ordinary product of
exact returned events and later join failures. No first-error return, resumed
unwind or second joining framework is needed. Each available failure transfers
once: the acquired failure stays in the primary retirement request; later
failures coexist with the actual actor outcome. An accepted unread cancellation
request remains distinct, after closing and draining its original receiver.
Neither a late failure nor an unread request rewrites the actual terminal cause.

Expected isolated owning paths are existing `local.rs`, `interpret.rs`,
`application_runtime.rs`, `launch.rs`, `terminal.rs`, and `termination.rs`.
Re-use `ActivationTasks`, `LocalResidual`, `ActorExecutionOutcome`, the generic
Driver request and existing total terminal projection. Private new request
alternatives own genuinely distinct original values; no actor state machine,
public task owner, generic failure wrapper or new event algebra is proposed.
Estimate production net 90–160 lines before full consumer migration; line
counts are diagnostics under the user's amended checkpoint. Public field and
variant changes, any necessary public owner-request value, complete caller
syntax and cumulative file/type surface must be measured before retention.

Compare live acquisition, late failure after each existing actor outcome,
joint-ready cancellation/failure, closed sender, returned control-lane events,
remaining-task join barriers and total projection. Original-defect and precise
first-error/misclassification inversions must compile and fail in both profiles;
then restore and pass. Existing completed local-runtime regressions remain.
Aggregate-drift review must justify every surviving result field, explicitly
exclude duplicate failure causes or request markers, and trace every current
consumer before this model can become production. Full DG-TASK remains open.

### Current live-failure regression and bounded repair stage

Independent reviewer `/root/contract_inventory` signs the fresh selected-0.21
original-defect witness in `bombay-live-capability-selected-a1qq4i4w`:
receipt `9c9855184464be0d066ad1be2ec7bd3e693e8eeab011aa7d3ee26436ec114db1`,
patch `3218d05f1956fafb938fc2e7c26ed19748605c3f482ed76f926a450acbd4064e`.
All 25 selected source paths remain identical to the prior source freeze;
134 test lines in the existing application-runtime module introduce the fault.
The actual observation task panics in nominal event injection outside every
Behavior fold. Acquisition resumes that panic through the actual local Driver;
the separately owned actor's original vector is lost instead of returned.
Both profiles compile and fail at the intended actor-custody expectation;
strict lint and formatting pass. The destroyed event inside the panicking task
is expressly outside recoverable custody. This accepts the original witness,
not a repaired runtime or full task gate.

The bounded repair will reuse the six existing owning paths listed above.
Independent review confirms the ordinary result/request model is eligible for
the experiment. The accepted-but-unread `OwnerCancellation` has no fields,
allocation identity or issuing authority; its sender owns authority. An explicit
exhaustive transformation `OwnerCancellation => ()` into a named
`unread_owner_cancellation: Option<()>` preserves that occurrence without a new
public wrapper. Closing and draining the receiver must precede that transfer;
an acquired primary cancellation must never populate the same field.
All existing startup and public projections must preserve first and later
failures. Publication before asynchronous joining and projection-task sibling
loss remain separately required seams; this bounded repair cannot accept them.

### Independently reviewed comparison successors

Root signs the corrected handoff comparison's receipt
`e7e7f12c10f202390a91ad02f21b7c663d184945ef688f6a4bdcf0e218826b5a`
and patch `4bee2b266e8f5d13b58ddbbfbeb87f0e02ee6d487419763017a2744ca8662e8a`.
The outer verifier now acknowledges only after its startup/outcome assertions;
the caller awaits that acknowledgement in both retention dispositions. A
deliberate verifier panic fails both profiles with the acknowledgement, whereas
the same fault without it passes, proving the prior false-positive mechanism.
Production remains +60 / -7 / net 53; tests add 307 lines. Full 225 library tests
pass in both profiles, as do strict lint/format checks. Three compiled ownership
mutations fail in both profiles and exact restoration passes. This historical
0.20 comparison checks retained payload allocation and observed finish traces;
the retained state's finish field and actual 0.21 source-retirement composition
require the separately frozen successor. Full application integration is open.

Root also signs the distilled acquisition fuzz successor receipt
`33c13ec97a470b3b4cd259bb9035018cdc32dced3b8a0994718695eeda25d76f`
and incremental patch
`1309c143ca9bc7d61b7f4e7adaf8f05ad1c3b93abb737a237a5eb126e853a6ca`.
Domain names replace research-mechanic names; ordinary
`Option<(AcquisitionPort, usize)>` removes the redundant acquisition-plan sum.
The existing fuzz path changes +432 / -217 / net 215 versus the original host.
All other selected source paths are unchanged. Three deterministic tests and
both compiled Driver-arm inversions execute in each profile, exact restoration
passes, strict lint/format passes and the pinned campaign completes 10,000 runs.
The aggregate source freeze retains production net 64, test net 1059, 25 changed
tracked paths and 267 non-build untracked evidence/corpus paths. All bounded
limitations in the preceding acquisition review still apply; this does not
prove async recovery, chronological retirement ordering or full DG-TASK.

### Independently reviewed selected-version handoff successor

Root signs the bounded 0.21 handoff successor in
`bombay-startup-handoff-021-oq10i1vf`, receipt
`030a8ad54694d4e6b855c4d70b231d2f07801a95300742cef434a328cb73eceb`,
incremental patch
`4d714744a81421b3e5295d6c72645a5e1fc12186b3c8c9552edfddb01dafccda`.
The complete patch is
`026d70b50bda0891aaa33ceeb19bd66a97289db0c7445b4efd9654e9bf1aeffa`.
Root reads the full handoff and test patch, three executing verification scripts
and profile logs, authenticates all 64 artifacts and 26 source hashes, and
compares every other source with the independently signed selected-version
source freeze. No fresh reviewer execution is claimed.

The production extraction remains exactly the signed ordinary four-value
handoff: +60 / -7 / net 53. The 299 added test lines now assert the retained
RootFinish field as well as original payload allocation, every residual field,
startup closure, and both explicit receipt dispositions. Cancellation is the
actual `Completion::RetirementRequested(OwnerCancellation)`; no duplicate
residual cause or false exhaustion remains. Both profiles pass 229 Bombay
library tests and thirteen Engine source-custody tests; strict lint in both
profiles and formatting pass. Three compiled custody inversions fail in each
profile, then exact restoration passes. The acknowledged verifier fault fails
with its intended closed receipt; omitting the acknowledgement wait makes that
same fault falsely pass in each profile. Constructor absence is only E0432
static evidence, not a runtime regression.

Whole isolated delta: production +301 / -184 / net 117; tests/fuzz/fixtures
+1261 / -118 / net 1143; documentation +29 / -29; manifests +13 / -13;
26 changed tracked paths and no new public types. External evidence files are
separately inventoried by the receipt. The newer acquisition fuzz successor
remains separate. The three actual application-loss regressions remain unfixed;
this accepts the lower ownership seam and its current composition, not full
Application, family, task-failure, projection or DG-TASK acceptance.


## Independently reviewed selected-version failure repair (2026-10-03)

The isolated `bombay-live-capability-source-port-yw_7gpby` repair uses selected
Core/Actors 0.21.0 and Communication 0.1.3. It composes the signed source-retirement
baseline with the actual observation-conversion failure witness; startup handoff
and the later fuzz successor are not integrated into this freeze.

One acquired primary cause owns either the original owner request or the actual
first JoinError through Driver completion. Later capability events and failures
are joined into existing residual fields without rewriting that cause. The
standard `(Vec<Event>, Vec<JoinError>)` settlement product replaces first-error
return; the inner Result/unwind pipeline is deleted. Accepted unread owner
cancellation is closed/drained before asynchronous cleanup and exhaustively
transformed from its private zero-field value into one `Option<()>` report.
Primary owner cancellation has no duplicate unread occurrence. Existing public
retirement variants preserve these coexisting fields; CapabilityFailed retains
its original failure and available actor state. AllocationRejected is unchanged.

The original actual Driver observation failure now retains the actor's original
Vec allocation and first task failure. Both actual local acquisition ports
return that failure once; a second poll cannot reacquire it. The direct source
port test does not fabricate an admitted Driver source operation. Joint-ready
owner priority, late failures after owner retirement, all-task join barriers,
original move-only events/panic values, all three residual phases and distinct
public causes are separately exercised. Coarse publication remains before late
capability joins; late failures do not rewrite factual completion.

The six production owners and seventeen test/example consumers remain inside
section 23's authorized paths. Increment over the source/witness baseline:
production +313 / -59 / net 254; tests +859 / -73 / net 786; new public types zero.
The additional source-port/naming successor changes tests only, net 110.
Whole workspace/all-feature tests pass 423 tests across 61 summaries in each
profile. Five production inversions compile and fail with intended exit 101 in
both profiles: original unwind, early join return, unread omission, coarse cause
collapse and source failure misclassified as closure. Exact source restoration,
233 owning library tests and one pool regression pass in both profiles;
formatting and strict workspace/all-target/all-feature Clippy pass.

Receipt: `44d70fa26b14f70a7baa1b5567dc2efc50f28212e91ee3ecdc8e127a3f525846`.
Complete patch: `b0d26d8b2d3f74e2336f864eaa211d934f4a11c8673c1a43319abfcddc36d15d`.
Successor patch: `76c35cfee2b5e628bf1d5f4152c9b6efc434134f5f40b696d18fd566783cdf7a`.
The prior receipt's narrative library count 231 was a typo; its authenticated
logs report 232. The successor reports 233 and leaves the prior freeze untouched.

Independent reviewer `/root` inspected the complete patches, six owners,
seventeen consumers, both scripts, mutation/profile logs and ownership proof,
and authenticated all 23 source hashes and 34 artifacts. Fresh independent
pinned-Nix executions of `cargo test --locked -p bombay-rs --lib capability_failure`
and the same command with `--release` pass five focused tests each. Review
receipt: `1eeb575027266895d6648229ff6811948005fe76e9f60eeaa3b82e270b467836`;
fresh log hashes: `5d646fab06e5c93393d6e4ea1ba02c086c9e9987d17385f6eb6b4aeee8d282ac`
and `78a48b04e8fce0b8e575e7665a5c1a91c5f26876dc26393409d8d6023453c1f3`.
This accepts the bounded model and evidence, not full DG-TASK or production
retention. Outer actor executor-error custody, projection sibling faults,
application receivers, notification admission and family cleanup remain required.

Standard precommit rejection branches may assert empty late-failure/unread fields
because the actual standard factory supplies fresh default bindings and empty
activation tasks, and rejection precedes action commitment. `new_with_bindings`
itself accepts caller-supplied bindings; it does not impose that absence on
advanced interpreters. The sole unexposed cancellation sender is not requested
on the completed rejection-return path and remains owned through joining.
Dropping that startup waiter is a separate handoff seam. Generic SpawnError and
ActorRetirement preserve nonempty fields rather than adopting a discard default.

## Independently reviewed native Entity lease comparison (2026-10-03)

The section 24 authorized test-only comparison uses the exact original native
Entity lease and signed 0.21.0 startup-handoff baseline. The actual native fence
acknowledges before Graceful retirement; that disposition does not assert actor
shutdown. Original retirement admits ShutdownRequested, then polls Pending
before acquiring its available cancellation authority. Dropping that waiter
allows existing task cleanup to cancel the actor and return its resident permit,
but loses the exact result intended for the surviving definition callback.
The compiled original oracle fails with exit 101 in both profiles.

The ordinary comparison consumes the same activation's original affine lease,
omitting only the pre-owner termination wait. It retains the original state Vec
allocation, OwnerCancelled cause, one queued ShutdownRequested, all empty
settlement/user/descendant lanes, entity and activation IDs, and resident release.
The retained state is released once after explicit receiver discharge. It passes
both profiles; dispatching the comparison back to the original native method
compiles and fails the same oracle in both. Exact restoration passes, as do 230
preservation tests per profile, formatting and strict owning-library/test Clippy.
The deliberately failing original witness is selected separately and excluded
only by name from preservation runs.

Increment: one approved existing path, tests +214 / -0 / net 214;
production and public types zero. Receipt:
`29a85dba1088af18677c429a7627582e8219b9a7d10b862185f24a7a585dc495`;
incremental patch:
`86ec62c9751b6797eaa9bd2f0be8439e88b2ff4baa5b85260d6cb4056297344b`.
Independent reviewer `/root` read the complete test patch, native owner, both
scripts and all profile/mutation/restoration/preservation logs, authenticated
27 source hashes and 43 artifacts, and verified the unchanged original source
prefix. Review receipt:
`c547591913232c5d7134844fb1f49288ec802429beb6db8d7b97a80596ddfe23`.
This is source/artifact review without a fresh independent Rust execution.

This accepts bounded failure/comparison evidence, not production retention or
full DG-TASK. Removing the wait alone does not prove callback custody when the
native retirement future is dropped during its task join. Actual installed
family/Directory cleanup, that surviving callback owner, forced disposition and
failure/projection integration remain required. The actual metrics.retired call
is preserved, but the family-owned private metric snapshot is not asserted here.

## Initialization panic payload custody (2026-10-03)

Current Engine `Driver::run` acquires the original panic value from
`catch_unwind`, then its `Err(_)` arm discards that value and returns only
`DriverError::InitializationPanicked`. Bombay's initialization result keeps
the actor and retirement fields but cannot recover the discarded payload.
Engine owns this capture boundary; Bombay owns preservation through its result
projection. This is distinct from Bombay's outer Tokio `JoinError` erasure.

The root-authored isolated witness uses a pure actor with an original immutable
Arc ledger and an initialization count. Initialization transfers that ledger
into `panic_any`. Actual Driver retirement returns the partially mutated actor,
the correct panic classification and exactly one prepared-environment retirement,
but loses the acquired ledger. The original ownership assertion compiles and
fails in debug and release. An ordinary Rust `catch_unwind` Result over the same
owning initialization and environment retirement retains the exact concrete
payload, Arc/Vec allocation, contents and actor state; both profiles pass.
Replacing that payload with a new empty ledger compiles and fails the ownership
oracle in both; exact restoration passes. Strict owning-test lint and formatting
pass through pinned Nix. External Weak observations prove liveness/release, not
destructor chronology or a destructor count.

Frozen comparison receipt SHA-256:
`6096fe45c45827e9c765a02ac6b2b0324da211c88ec050e8d3272d1b2fb07fdc`;
patch SHA-256:
`8101286c1a8fdf49d11fd53b204cc152cd03ad2b2763b854e42cd7d68c73de44`.
One already authorized existing test path, tests +115 / -0 / net 115,
production/public types zero. Bounded independent review is recorded below.
No public error representation, production repair or full gate is selected. The older impure
fixtures in that same file are excluded from this evidence and have a separate
correction stage; passing their existing assertions is not purity proof.

Nonauthor `/root/task_custody_research` authenticated all 345 copied source
files and 18 artifacts, read the actual Driver and prepared-environment owner,
and independently reproduced the original failure and ordinary positive in
both profiles with a fresh pinned-Nix cache. Bounded review receipt SHA-256:
`c14d186a52da73024b49f6b1a2373a65ddc0a0b563fec1032e091a2346c6bd12`.
This establishes loss and preservation feasibility only. Arbitrary original
panic payloads cannot inherit the current error's `PartialEq`/`Eq` promises
without a separate interface comparison; no equality or payload-erasure policy
is silently selected.

## Current combined custody repair review (2026-10-03)

The isolated composition combines the typed Driver retirement request,
synchronous transfer of the four original startup owners and join-all capability
failure custody against current published dependencies. Final patch SHA-256:
`28f99d7ed39cd4c44777e1454333eda3586e77d4ba1f14e882ae596642be5475`;
author receipt SHA-256:
`4760d9ae2ca3555094abd6d6dac07367f4095d08511fad04e27b27f2a6ed6d23`;
scope supplement SHA-256:
`cf799f388ea56bd1bace3b60f5505d539691ab153fa1b6eec03375c0aa4f0305`.

Nonauthor `/root/task_custody_research` authenticated all 345 source files,
91 generated untracked corpus inputs, 890 external artifacts and 202 selected
published Rust sources. Its fresh dedicated pinned-Nix cache passes 254 owning
library/integration/Engine tests in each profile. Review receipt SHA-256:
`fa122fceb6c3957d6855ab4b677bbc7ab99716ab92d0a5105db842599f45d702`.
A preceding reused-cache compiler failure is excluded; unchanged source rebuilt
cleanly. The reviewer inspected the original regressions, exact restoration and
compiled inversions, including move-only source retirement, transitive source
chains, startup waiter drop, all available capability failures and returned
events, and accepted unread cancellation custody.

The author ran the production composition's full workspace in both profiles:
426 tests across 61 summaries pass. Subsequent test-only source-fuzz and pure
trace/property successors pass their focused checks in both profiles; production
and the other 342 source files match the whole-workspace freeze. The final
composition has not been relabeled as a new whole-workspace run. The actual
acquired-request fuzzer passes a fresh 10,000-run campaign. Twelve core inversion
cohorts plus its two-source-arm Break inversion and the pure source-priority
inversion fail at intended compiled assertions in both profiles and restore.

Scope: 37 already authorized existing paths; production +617 / -221 / net 396,
tests +2721 / -408 / net 2313, new public types zero. The corpus and external
receipts are inventoried research evidence, not newly proposed retained paths.
This accepts bounded composition evidence, not full DG-TASK or canonical
semantic retention. Outer actor JoinError and initialization panic payload
custody, recursive projection, complete application/Entity family delivery,
observation ownership, current documentation and final consumer migrations
remain required before their affected gates close.

## Native panic ownership boundary (2026-10-03)

Fresh read-only audit of current Engine/Bombay and selected Core/Actors traces
the caught initialization payload through both root and child paths. Existing
root SpawnError, ActorRetirement and RunError alternatives can forward that
original value directly. Completion observers can still publish a classification
while the full result retains custody. Current rejected-child bindings are
different: they store no task or full result, and child creation transfers the
surviving actor into Core's classification report. An independent runtime owner
for the original child panic is not present and must be proved, not assumed.

Audit receipt `2a7787a2aecd8a090464fa1c5bd826a2c8cfdd8000540c2409b1c6c2a9d7e587`;
immutable scope correction
`2d1354b6fc3aeb65ffcbab660498e78d86056ab34832697f8eff8c6deffee507`
adds the omitted repository qualifier to its original path comparison: all six
minimal owner paths and eight direct consumer paths are already authorized.
Integrator authenticated 22 current files, eight selected sources and the
selected complete AGENTS hash. Production/tests/public changes are zero.
This is ownership analysis, not interface or gate approval.

Exact Rust 1.99 source shows catch_unwind returns the native thread Result,
whose failure owns Box<dyn Any + Send>. Merely spelling thread::Result does not
remove that underlying representation. Tokio's existing JoinError is an already
accepted opaque runtime failure owner, but its panic-payload constructor is
private; it cannot directly receive the captured initialization payload.
Resuming unwinding without a separate owner loses available actor/residual
values. Restricting arbitrary panic_any values to a known actor error changes
the required recovery law. Whole-error Clone/PartialEq/Eq promises also need
review before any original arbitrary payload is added.

Selected Behavior instructions prohibit introducing Any/trait objects. The user
explicitly approves in EXEC section 41 passive ownership and transfer
of Rust's native unwind payload in Engine/Bombay, outside Behavior state and
algebra.
No dynamic domain dispatch, downcast routing, erased actor result or catch-all
protocol is proposed. Implementation still requires an exact reviewed pre-edit
model and complete
custody verification; the representation-policy blocker is resolved.
This does not claim recovery of values destroyed inside user code, support for
panic=abort/foreign exceptions, or safe discharge when the payload's destructor
itself panics. Cleanup-before-discharge needs an observable ownership proof.

## Startup publication scope requiring clarification (2026-10-03)

EV-06 requires "no success publication" for drop before spawn and during
activation. Section 8.2 explicitly prohibits successful activation before spawn;
its spawned-startup row requires retained cancellation and task custody. The
reviewed startup-input comparison proves recovery and joined retirement, not
absence of publication during an already admitted activation.

Current Driver::drive_active publishes after initialization settlements finish,
before acquiring its first ordinary event. LocalEnvironment's private commitment
acknowledgement precedes action commitment; its publish method publishes the
address lease and calls the endpoint notice without checking queued cancellation.
The separate typed-source retirement comparison retains this publication path.
Thus a queued request and a failed send to the departed startup receiver must
not be described as preventing the actor's endpoint publication. Cancellation
acquisition, endpoint publication and caller receipt are distinct operations.

The user selected **prevent publication when cancellation wins**. Section 8.2
now requires the actor to remain invisible when owner cancellation wins before
publication, while preserving already accepted effects and exact remaining
values through joined cleanup. This is deliberate Bombay policy, not a new
Behavior-fold law. It applies to the shared local root, child and Entity
construction boundary.

An actual original-publication trace using the existing runtime notice is being
prepared. That trace must preserve the complete Address-owned resolved endpoint
snapshot; resolution does not return a bare ActorRef. The first compiler veto
and failed source are frozen separately. No policy fix or acceptance is claimed
from the original positive trace. The precise cancellation/publication winning
boundary, typed disposition, ordinary-Rust comparison and independent review
remain required before implementation or EV-06 acceptance. A queued request,
request acquisition, publication and startup delivery are not interchangeable
evidence. Independent input-custody and family-lifetime comparisons continue.

## Executed original startup-publication trace (2026-10-03)

Receipt cb56782405b7be914d65ecb2cfd70e2beccf7bdb25a4773b633783884441edc2,
nonauthor review
85936370b0b96f277c6d237580be3126dd5af9dc2979d2926bece015ef3ad21a
and coordinator inspection
290c787c010492c7faca5cd5f2f2d42d0c69260fe1c255cae82d34df72901fe3
accept diagnostic evidence only. The same execution is dropped before releasing
its original private commitment acknowledgement. Actual publication subsequently
captures a visible complete Address-owned Resolved snapshot, and the original
startup sender returns its exact ActorRef through Err. The retained result then
joins the original cancelled Wait root, preserving its original allocation and
complete terminal lanes. Final Address resolution is absent.

All 20 tests pass in both profiles, with pinned-Nix strict default package
all-targets Clippy and workspace formatting passing. Earlier compiler and lint
vetoes are excluded. The cfg-only increment is +94/-6/net88; new production,
public types, private types and variants are zero. The inherited conditional
launch net53 remains separate. Both inspections authenticate 345 sources,
30 artifacts and the unchanged 179-package lock with 165 external packages;
neither reran Rust. The queued request is not an acquired cancellation or an
accepted winning boundary. This proves current visibility, not a repair or
EV-06 acceptance; the typed publication decision and its regression remain open.

## Publication decision and original-regression pre-edit (2026-10-03)

Ordinary-Rust comparison receipt
2d903c93486c96638c829c37fac1547eaea2f5f423de92c8e39024f65940731c
and independent review
a9d0bb540fc7aab41f1e2343d94565c4ca3b021a59c3d8243d2582e4aee9c993
identify a candidate existing-port amendment:
`publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()>`.
The Driver would enter its active phase only on Continue; Break would preserve
the original request through its existing retirement disposition. Bombay would
check its original cancellation receiver before publishing the reservation.
An absent sender permits publication; an empty receiver remains available for
later cancellation. A hidden publication skip through the unit-returning port
would misrepresent the Driver phase. A separate check-and-publish port adds an
avoidable split decision. No additional type, field or runtime owner is proposed.

The proposed winning boundary is the receiver's acquire snapshot within the
synchronous publication call. A request accepted before that call must win.
An overlapping send after an empty snapshot may lose this decision and remains
an exact later cancellation request. This does not claim atomic ordering between
the oneshot sender and physical Address publication. The paper three-owner patch
is +20/-4/net16 production lines, not an executed or complete consumer migration.
Generic Engine custody, shared root/child/Entity consumers and the overlapping
boundary still need witnesses and review before selecting this amendment.

Corrected original-only test draft
993e98f46d4a3f328a8de1d3b8bd2464f44d0585a5013dfcae217dcb961d63f5
has independent eligibility review
4062edff6782f2267cf8a08f2b4711bb3025a2d2e0d5e6f367babab62982eb7a
and coordinator inspection
a148d3116e5a2322b1b12540766278bc047f2426eece442c92a02229e17b6124.
Its one existing local test module changes +207/-2/net205 lines; production,
public/private types, fields, variants and wrappers are unchanged. All 345
immutable typed-source 4760 inputs and six draft artifacts authenticate. The
earlier import-style objection and its original draft remain preserved.
The actual Driver test accepts the original request before releasing commitment,
joins its complete retirement, then checks the notice's complete ActorRef and
Resolved snapshot for forbidden visibility. Two direct-port contrasts preserve
the complete accepted initialization settlement and check sender absence and
later cancellation. The full Driver's Exhausted source explicitly discharges
its initialization receipt; this row claims no retained receipt suffix.

Run the unchanged original in debug and optimized pinned-Nix builds: the
visibility regression must fail at its final oracle, and both contrasts must
pass. Compiler vetoes require separate frozen source and diagnostic corrections;
they are not semantic failure evidence. This pre-edit approves only that bounded
original experiment, not a production repair, current-source integration,
EV-06 or DG-TASK acceptance. Canonical production remains net133 with zero new
public types and 67 tracked/zero untracked paths under the approved 114-path scope.

### Executed original visibility regression

Final receipt 85e533d7cce96834072666ac5da2a6711fa4b9d6737437ec8f8d31160d9a2f20,
nonauthor review
ca05f5c7a0d258fad725e8f42380804757164a7faa4ec013482cd8b0159cae3d
and coordinator inspection
332958824e113cc59bc4fa9bd30068a06914547a624c57311bb4eeb9058687b7
accept the bounded original defect evidence. Both debug and optimized builds
compile and fail at the final forbidden-visibility oracle after the same Driver
has joined its complete retirement. The original ActorRef and complete resolved
snapshot prove publication occurred; final lease removal does not excuse it.
Both direct sender-absence and postpublication-cancellation contrasts pass in
both profiles. Pinned-Nix strict package library/tests Clippy and workspace
formatting pass. The receipt retains all eight verification command arguments,
source hashes and logs.

The formatted cfg delta is +246/-2/net244 in one existing file, with no
production, public/private type, field, variant, wrapper or unsafe additions.
All 345 sources and 23 artifacts authenticate; 344 foundation sources and the
179-package/165-external lock remain exact. The earlier Debug-convenience and
assertion-style vetoes remain separately frozen and excluded from runtime proof.
This is immutable typed-source research, not current canonical integration.
The candidate publication port, generic retained-suffix proof, overlapping cut,
launched cancellation authority and root/child/Entity consumers remain required;
EV-06 and DG-TASK stay open.

### Complete publication experiment pre-edit

The original visibility failure above justifies an isolated comparison of the
existing publication port returning `ControlFlow<Self::RetirementRequest, ()>`.
Corrected complete text receipt
ec8830e93639ce5dcee59e4484e6112e56f371181f8a822f660624590ab1a6d3
freezes patch
968881fcb2d1a004cf6feb316c7128a278e96a2adf29ddacc7726c07554ba903.
Independent eligibility reviews
0b6a8599488da4a217611eb604aeb29edc5d090b61e035a33c17dfcc41035f5a
and d884e02f6d9571ba3ba8004957eae800df47269259fb0e919ca421ee26230275,
and coordinator inspection
309701e4a10f071e53d3233e98c862715d4dd24bdda5bc10795ff9542cf32506
authenticate all 345 baseline/proposed sources and 25 artifacts. The predecessor
documentation and command objections are resolved; its 13 Rust/diagnostic texts
are unchanged. This accepts experiment eligibility only, not the final boundary,
public API, retained production, EV-06 or DG-TASK.

All 15 proposed paths are within the authorized 114-path allowance: Engine's
driver/environment, driver benchmark, causal-turn fuzz target, phase-authority
Rust/diagnostic fixtures, send-not-sync fixture, property/source-order/support/
terminal-custody tests; Bombay's local and actor-execution files; and the existing
Driver-law and capability-interface documents. Against immutable typed foundation
4760, the forecast is production +25/-4/net21, tests +448/-21/net427,
documentation +73/-32/net41, public types +0/-0. There are no new production
types, fields, variants, traits, owners or runtime services. The three test-only
request slots represent successive prepared, active and residual custody.
Inherited foundation production net396 is separate from this increment; this
is not a measured current-canonical overlay.

Engine reuses its associated retirement request, Completion, retained settlement
queue and affine retirement barrier. Bombay reuses its original cancellation
receiver and pending Address reservation/publication. An acquired request moves
directly into retirement; Continue alone enables ordinary input. Generic Engine
tests must preserve the original request allocation and complete retained suffix.
Local tests must prevent actual visibility and preserve complete joined cleanup;
sender absence and later cancellation remain valid contrasts. Both profiles must
pass, while restoring the original Local defect, ignoring generic Break,
publishing twice after Break, and erasing the retained suffix must each fail after
joined retirement. Actual selected-compiler diagnostics, strict Clippy and
formatting must pass. All Rust commands use pinned Nix with unset RUSTFLAGS,
one build job, disabled incremental compilation and an exclusive target cache.

Only the designated experiment author writes this isolated composition.
Equivalent import, formatting and assertion corrections retain their diagnostic
and exact correction; semantic, ownership, type, bound, policy or scope changes
stop for review. Final evidence requires a nonauthor review. No historical whole
file may replace current production: independently reviewed preservation map
0d9140a356714a7716e86a831c1f9fc12092fc86fddbc9222ceaa2f2b8d27785
and review aa2c27d13dfb8c82ee160667200db08ba3374a7ad0c033f13848dfb9e888e1a1
identify current shutdown, compiler, EV-25/EV-30 and pure Driver witnesses that a
future incremental overlay must preserve. Canonical production remains net133,
zero new public types and 67 tracked/zero untracked paths. Shared root, child,
Entity and overlapping-publication witnesses remain required.

### Publication consumer correction and overlap pre-edit

The first 15-path experiment passes focused Local and Engine custody tests,
four compiled inversions with restored positives in both profiles, actual
compile fixtures, strict Clippy and formatting. Immutable first-scope receipt
02521099975be601be178acad70b93283ee8fa229fa1d8fcdf2880e7a86da88d
preserves those sources and logs. Its broader workspace run is **nonpassing**:
an inherited application controller requests cancellation before publication,
awaits startup success, and therefore cannot release its cleanup gate. The
experiment's own blocked process was terminated; that exit is neither a passing
gate nor an intended assertion failure.

Exact controller draft a19ecfd4adaf4022c7698e2af7742cc6c34975339b3b36252234402c7142440c,
independent review
036946323c08fd4756a859569542f813e9cc20b8ebc8b975634ef49ad84d59c5
and coordinator inspection
07dec7e7a29d6bacf2b2ea24ea3440364c4dfd1ee1f790833fa4cc49af4bd035
justify extending this isolated experiment to one additional existing approved
path, application_runtime.rs, for +6/-7/net-1 test lines. Production and public
types remain unchanged. Cancel and initial Stop retain their original startup
receiver beside the joined result; only ordinary Stop awaits startup success.
The existing cleanup owner releases its gate before joining and obtains the
actual startup rejection afterward. All exact outcome assertions remain.
The 16-path stage remains within the approved 114-path task scope; no user
surface checkpoint is crossed. Broad verification must be rerun in both profiles.

The additional direct-port overlap test draft
7b186879089bbfef4c008d82b013f33ef5ab51bae400492be91cb9f8ce85fee6
has independent eligibility review
9f297bb12bbaf17c646528bf70136d8f887081745ca9a1c12ef1671118ff0dc3
and the same coordinator inspection. It adds 94 test lines in the already
assigned local.rs. The existing synchronous publication callback sends the
original cancellation during publication and retains the complete endpoint,
resolved snapshot and admission result. Accepted requests cross actual next;
rejected requests remain owned and skip that wait. Both paths retire the exact
initialization before their final oracle. Closing the receiver at publication
must therefore fail admission after cleanup without hanging. Run positive,
inverted and restored cases in both profiles before final independent review.

This proves a publication-first overlapping call, not the narrower interval
between the receiver snapshot and physical lease installation. The concrete
Address primitive exposes no guard or callback to control that interval;
no new production hook or atomic physical-publication promise is introduced.
Original visibility, generic suffix and shared envelope requirements remain
distinct. No completed whole gate or current-canonical integration is claimed.

### Reviewed publication repair and remote checkpoint

Final isolated receipt
8e8ae06474c178b036a21baee928eca124f531fff2b01fa7a178011682662aae
freezes the complete 16-path repair, patch
cf4e521765670738df1fba1f19ed5ce7383fe0eedf1298c16b4897b762588332.
Independent nonauthor reviews
5315c1b8897267832b951edad541210603f3a7c317d84e43be2dad02775f26ac and
2d4014a4bebe5a5316be250b353f15000331c72f82c2048e1988b53259c56a18,
and coordinator inspection
0ca7cbabc14d86197235e302358eec3eac48aeb9b560518a5186a01686571297
accept the bounded experiment. All 345 sources, 115 artifacts and 436 archive
files authenticate; 91 inherited generated corpus files remain unchanged.
Metadata supplement fa89636920d5d0450822f12cd646db6399bf89e10a483d507150a31985ce8cac
binds the unchanged 179-package/165-external selection and actual test names.

Both profiles pass the final 31 Local tests, 14 generic custody tests and 432
workspace tests across 61 summaries. Static denial fixtures, strict owning
all-target/all-feature Clippy and formatting pass. Five compiled counterfactuals
fail for the intended laws in both profiles: original publication omission,
ignored retirement request, repeated publication, erased retained settlement
and closed cancellation receiver. Exact restored positives pass. The interrupted
first broad run remains nonpassing; it is neither an inversion nor CI evidence.
Exact commands and logs are indexed in verification.md.

The original request moves once through the existing publication port into
Completion. Pending publication remains with the retiring environment; the
Driver keeps the original settlement suffix and obtains no subsequent event.
Sender absence permits publication; Empty keeps the receiver open for a later
accepted request. No production owner, type, field, variant or trait is added.
Against immutable foundation 4760, this increment is production +25/-4/net21,
tests +550/-28/net522, documentation +73/-32/net41; public types +0/-0.
The inherited foundation's net396 production lines remain separately measured.

At the user's request, each focused commit is pushed. Remote
[research/exec-publication-retirement](https://github.com/devrandom-labs/bombay/tree/research/exec-publication-retirement)
preserves foundation commit 8399e7f and exact final experiment commit
5df5b8ede3b001db4649f524ae7154c6c3b28d0f; all 345 final source hashes match.
This branch is a research backup, based on historical commit 1f70594. Its older
Driver fixtures are not current pure-fold evidence. The canonical EXEC branch
preserves the newer shutdown, scheduling, measurement and pure Driver fixes.
Fresh incremental integration and root/child/Entity cancellation witnesses are
still required; this accepts no full EV-06, DG-TASK, public API or merge gate.

### Current-source repair pre-edit contract

EXEC section 40 selects the isolated current-source repair experiment, not full
DG-TASK acceptance. The exact proposal directory is
/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-current-execution-assignment-j_m5mg1o.
Its preedit-receipt.json SHA-256 is
3ee12e8b83085b16c44b5b9916625dd6dfecd7a17918e54587b843a8d77cade0;
proposed.patch is
e02da633f382714b47779b7a2452e2f1e395fe97be13cef9a0d21a6508aae8c7;
execution-assignment.json is
f0bee5931fe5662abe825927a0c6401fdb87aafc38a3c334fe282ea2e9762090.
The assignment contains every section 11.1 field, sole-writer ownership, exact
ports, complete custody traces, dependencies, stop conditions and pinned argv.
The coordinator authenticated all 52 artifacts, all 345 virtual source entries
and clean application of every patch preimage to canonical 986447e. The fresh
execution archive must preserve newer unrelated documents; virtual181+39 is
explicitly a proposal, not a claim about that later execution snapshot.

Independent review is
/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-current-overlay-eligible-7giz4kg1/review.json,
SHA-256 0ee660c60b0c93b19e9440814829510e6a2098a9f24988bfd38502a554405b36.
Coordinator receipt /tmp/bombay-current-execution-root-preedit-review.json has
SHA-256 ed29af0de8291d159cbbff6b3edc05c315fd34198cde460ce437de83b8841308.
Final changes from the prior proposal add the missing module-scope task import
and remove one duplicated panic assertion before cleanup; all production text
is unchanged from its reviewed predecessor. Every available settlement lane,
allocation, accepted message, descendant, source event, first/late error and
unread cancellation occurrence remains part of the complete result oracle.

Only contract_inventory writes the 39 isolated paths and the exclusive
/tmp/bombay-api-private-composition-target cache with incremental disabled and
one build job. Canonical source and shared records remain coordinator-owned.
Observation and task-custody researchers independently review handoff evidence;
the author cannot accept its own gates. An unexpected ownership, cause/order,
public state/type, bound, consumer, native representation or out-of-scope path
stops dependent implementation. Equivalent import/format/assertion corrections
retain their exact diagnostic evidence and may not invent architecture.

### Native ownership permission and minimal next witness

EXEC section 41 records explicit user approval of passive native panic custody.
The earlier payload-loss regression remains the original-defect evidence. The
secondary-destructor proposal fe8a7488ffe3e9ecd093bada76eab671e7bfe0ffd92a106eabd3237ee1489ab2
has independent bounded review
424b54fb3a28618d036085029d6ccdbd6d3385682c859f5d276657274bae882d;
it is preserved as unexecuted characterization, not a new required production
contract. First compare preserving the already caught payload, which may remove
that secondary failure before any unavailable-child receipt is needed. Actual
original pre-Start failure remains required if an owning receipt amendment is
proposed. A conditional source gap cannot license that amendment by itself.

### Native initialization lower-model review

The frozen paper model at
/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-native-initialization-ownership-text-k4w07jig
has receipt 761a51bb2ae1dffab3ba39c7ae71f49ae83044bf46ab7568db8a1decffe9719a,
combined patch 1f78ec19325f8e1318ea0d98104903d7ff8c4a99e95ca49ad8ccfeb229f9a75c
and authority clarification
d3e8787acbb22a240a9e736383504ef3a04a3db645fac8668d54569539cfaa08.
The coordinator authenticated 15 artifacts, four immutable current-39 base
sources, three standard-library owner artifacts and 18 constructor/equality
consumer rows, and reconstructed the combined four-file diff byte-for-byte.
The individual actor_outcome patch is preserved as an intermediate import-order
draft; the combined patch and final texts are authoritative.

Existing DriverError, ActorExecutionOutcome, SpawnError and ActorRetirement
initialization-panic branches pass the original Rust-owned box through the
existing retirement waits. No extra catch, task, nominal type, alias, payload
inspection, dynamic routing or Behavior-state owner is proposed. Removing
DriverError/ActorExecutionOutcome equality derives is a real breaking surface:
an arbitrary native payload has no truthful equality law. The complete consumer
scan identifies review sites; it does not claim those sites are migrated.

The lower component is production +29/-11/net18 in four approved existing
files, public types +0/-0. Independent review
78f0b4e38785cac44d2d17c0c69cfdc076a75f8eb8ec38875a0dabb8bf8568e5 and
coordinator /tmp/bombay-native-lower-root-model-review.json,
SHA-256 896517f31989701ec71f2e6d4aac63311d5df4423320685bb894a10e64f5d58d,
agree only that this lower ownership comparison is coherent. Execution and
retention remain blocked on the actual child consumer: its existing Core report
returns the original Child, while the runtime must separately retain the native
payload and distinguish it from an outer task failure through all remaining
joins. Unit rejected bindings and Vec<Root> do not yet express that complete
result. No field-erasing pattern, fabricated lifecycle fact or partial root-only
fix is accepted. Full pattern/equality/constructor/docs migrations and the
original6096 omission witness in both profiles remain required. Source edits
and Rust commands for this model are zero; DG-TASK/DG-PROJECTION remain open.

### Executed borrowed application-work comparison

The private unit-family comparison now uses direct FnOnce(ApplicationHandle)
work, with actual Rc-owned values and a borrowed caller slice in its callable,
future and output. It reuses standard actor composition and one startup receiver.
A caller-local Drop publication owns unstarted inputs or uninvoked work;
invoked work remains lexical, while completed output belongs to the separate
result future. Static cleanup owns the raw actor join and an explicit permission
and joined acknowledgement. The cancellation authority remains caller-local
through the acknowledgement. No borrowed work or output enters a Tokio task.

Eight actual controllers pass in both profiles, including original-input
retries, actual address rejection, retrying the same borrowed result wait,
root termination with work still pending, owning work drop and final-receiver
surrender. After equivalent cfg-only lint corrections, all-target strict lint
and formatting pass, receipt696e81dacec63b0a26dd7057fecadb6ddea91b9d03c68b5ad3298105560a9fdc.
Independent679b3ca73d19c5830e2ab17885c99cda3232abbed94dd246122c0125209c8f4f
binds all345 actual inputs, original diagnostics and the post-join oracles.
Four counterfactual cuts fail after explicit cleanup in both profiles and
each complete eight-test restoration passes. The fifth initially fails E0282
because its omission removes the channel's only type-inference evidence;
that compiler failure is excluded. Its reviewed explicit existing result type
correction changes no owner, field, bound or runtime law.
Both corrected fifth failures and complete restorations are independently
accepted under67d76a403b50864c940f3d61a4dda08f54b7c32b1d081b0bf1782f17dd2314b7;
all345 source inputs are restored. The mutation warning remains separate
from warning-free positives. The application-work panic supplement in EXEC61
now passes ten warning-free positives/restorations in both profiles, strict
lint and formatting under2ddd02ffea4b5ce2d1f0de0501942d2312db68fb41acebe4a817eac616926c4b.
Independentc687c07c717f601262df0bb9099f3e4a61040bf49345173c9aece43b3b35ac0f
accepts its four compiled post-join classification failures and complete
source restoration. Actual user-code invocation/poll panic remains with the
caller as the original opaque Rust payload; the separate result owns exact
joined actor retirement and truthful interrupted work. No second copy of the
panic or new production catch is added.

These observations prove allocation retention/release, without inventing
destructor chronology or a full outcome after its final receiver was surrendered.
Four private nominal types and two test protocol/template aliases express the
comparison; no public runner, family cleanup, recursive child transport or
HTTP API is selected. Startup before-publication, panic custody, heterogeneous
children and complete independent API/task acceptance remain required.


### Executed preservation outside a panicking interpretation

Selected Core/Actors0.22.0 source is published1fc8fb55. The isolated local
comparison borrows the prepared interpreter or active Driver owners while
polling the current operation. After an unwind, it drops that future without
repolling, then retires the same owners. The original native payload remains
with the returned failure, outside Behavior state and protocols.

| Boundary | Ownership kept outside the failed future | Subsequent responsibility |
| --- | --- | --- |
| Initialization commitment | Prepared environment, reservation, child bindings and capability tasks | Release reservation; retire and join original accepted work |
| Active operation | Mutable Behavior, active environment and prior causal settlements | Retire environment; return original state, child results and completed work |
| Pure transition | Partially mutated Behavior | Preserve distinct transition cause; use the same retirement barrier |

Original8d3a2db6 establishes both outside-parent losses in both profiles.
Corrected positive2ba8965f preserves complete original parent/child allocations,
completed capability event, original opaque panic allocation, typed creation
identity/role/address and the earlier creation settlement. Parent2, pure1 and
atomic15 controls pass in each profile. Five cuts independently remove prepared
or active capture, native payload, prior settlements or pure-cause distinction:
all ten intended runtime failures and ten restorations execute after finite
cleanup. Independent2a1324fd binds345 restored sources and actual failure sites.
These are scoped ownership results, not complete task or projection acceptance.

The failed predecessor6874 expected a successful delivery unit receipt to
remain. The earlier prior-row assertion6865 passed. Selected
ActionItem::retain_accepted and Vec source offering explicitly discharge that
unit; the actual BirthsCommitted delivery is observed independently. The
corrected test preserves the full retained creation row and every other custody
oracle. Diagnostic066fabc7 is excluded scheduling evidence and restores all
inputs; correction9c0c63a4 identifies the earlier mistaken report descriptions.

Ordinary comparisonad602d4b uses existing InterpreterRequests and plain leaf
calls. Both complete traces match; keeping an earlier receipt and move-only
later command outside the consumed leaf preserves their original allocations.
Both ordinary tests and three inherited controls pass in both profiles; three
allocation/trace cuts fail and restore in each profile. Selected Core strict
all-target/all-feature lint and formatting pass; independent86068e4c binds all
806 sources and exact failure sites. It preserves the unchanged original
reusable-interpretation loss witness, which still fails in both profiles.

Reusable Actions, SendLayer and item iteration hold earlier facts across later
awaits. The broad runtime capture cannot preserve those inner owners. Nested
AssignWorker also owns its correlation receipt across a lower delivery wait;
a universal unavailable-item marker would erase it. Full task acceptance still
requires a reviewed typed interruption remainder, complete creation/source
offer custody, native initialization/child transport and final application,
family and receiver-lifetime integration. No partial owner is substituted for
those required facts.

### Executed source-tail ownership comparison

The exact owning Core SourceSettlements coroutine retains a current input and
an untouched iterator suffix across by-value admission. Source conversion can
unwind through this owner. The advanced generic comparison uses the actual
SourceActions unattempted product; it does not claim a standard live Driver
path or an accepted capability receipt. The Clear input genuinely enters
EventIngress and has no recoverable payload; the later Append owns its original
allocation. The original same-owned offer loses that untouched input before
explicit discharge, final count0 rather than1 in both profiles. The failed
future is dropped once and never repolled.

Actual822e2d62, independentc45ede73, binds all806 inputs and216 added test
lines in the existing total_interpretation file, production/public0. Open
admission transfers the whole original input; Closed returns current then tail;
an ordinary caller keeping the iterator outside the consumed admission preserves
the exact tail and allocation. Those three controls and five inherited controls
pass in both profiles, with selected Core strict all-target/all-feature checks
and full formatting passing. Native results remain opaque.

Actual1d415965, independent170a3bee, authenticates three distinct cuts:
copy the lexical allocation, omit the lexical tail, and reverse the actual Core
closure return order. Each fails its intended pointer/full-input oracle in both
profiles; all six targeted restorations pass and all806 sources are exact. The
original quality checks apply to those restored bytes; no rerun is claimed.
The temporary Core edit is restored, with zero retained production change.
This demonstrates the ownership cut without selecting new live receipt states,
source-unavailability markers or a public interruption interface.
