# External work decision record

Status: **open**. Owner: WP-API-DESIGN. Requirements: XO-17, XO-18,
XO-41–42, EV-24 and DG-WORK. This record identifies an existing typed port and
the ownership question that must be solved before it can count as the bounded
external-work witness. It does not select a new general work service.
The source hashes and 0.17.0 contract below are the 2026-09-29 research
snapshot. The current lock selects Behavior Core/Actors 0.21.1 and Macros
0.13.1; revalidate this open decision against those sources before using its
candidate. All 19 inventoried actor-owned requests now have typed Bombay
interpretation, so missing atomic interpreters are no longer its blocker.

## Exact selected sources and owner

At the snapshot, the root lock selected Tokio 1.53.1, Behavior Actors 0.17.0 (registry
release revision `435560ce7bea8ad3330ee2d42e5034f837a80602`), and the
Bombay-local `WorkerPreparationSource` in
[`worker_preparation.rs`](../../../crates/bombay/src/worker_preparation.rs).
The root lock hash is
`dfd3b2d7640281ed8cfbda6b022a281c4369a0cbf4b5ddbdd0623fa43537db07`;
the local port source hash is
`e5da82e1381a1f80b3be19e451b1a71fb71ec3cd18560d0f5da91c66f49e4507`.
The then-current capability integration in
[`application_runtime.rs`](../../../crates/bombay/src/application_runtime.rs)
has hash `37d03bff8bc32e38fece076bceacaffdcf204fbeb47163b8831ae4dea4e1687b`.
Tokio's pinned `task/blocking.rs` has hash
`22a0cffd06dd0accdeb4f332c9636205a51214ab45d27444d623639eea4ae54b`.
Any change to these sources reopens this evidence.

The existing Behavior Actors `PrepareWorkers<Source, Role, Worker, Plan>` is a
closed typed source action. Bombay's `WorkerPreparationSource` performs
`prepare_first` and `prepare_next` only when Bombay interprets that action;
the Behavior fold does no I/O. `settle_worker_preparation` retains the exact
request on first-source rejection through `ItemSettlement::Rejected { item,
reason }`. Later per-role rejection has a different typed result because the
source was already admitted. The first-source rejection is therefore the only
appropriate place to witness saturation with exact still-owned input.

The existing owner tests passed under the root lock with
`nix develop -c cargo test --locked -p bombay-rs --lib worker_preparation::tests`:
3 passed, 0 failed. They prove complete-request source rejection, exact
replacement-role preparation, and source retention after per-worker rejection.
They do not exercise bounded admission or running blocking work, so EV-24
remains open.

## Historical bounded candidate and ownership equation

The semaphore-in-source candidate below is rejected by the later selected
state-purity policy. Its 0.17.0 outer-rejection description is also superseded
by fresh selected-port evidence below; retain it only as research history.

The smallest candidate is a concrete worker-preparation source holding its
real domain input and an `Arc<Semaphore>` representing a fixed positive number
of concurrent preparations. `prepare_first` attempts
`Semaphore::try_acquire_owned` before `spawn_blocking` or another admitted
external operation. No permit means `SourceRejected` and
`ItemSettlement::Rejected` returns the original `PrepareWorkers` request;
no operation starts. An acquired `OwnedSemaphorePermit` moves into the
operation, so completion releases one slot even if the async waiter is
dropped. On completion, the typed `WorkerSubmission` is delivered through the
existing settlement path. `prepare_next` applies the same bound after source
admission, but its error is role-level by the selected Behavior Actors law; it
cannot claim whole-source rejection.

Both source methods borrow `&mut self`, whereas `spawn_blocking` requires an
owned `'static` closure. The witness must identify a concrete owned operation
input moved from or cloned out of the source only *after* permit admission,
with an exact account of any move-only input. It must not claim that the
`WorkerPreparationSource` trait itself moves its whole source into the
closure. Pre-admission rejection leaves the entire `PrepareWorkers` request
untouched and available to the source-action rejection path.

This is a candidate for a **concrete witness source**, not a Bombay public
semaphore adapter or new generic pool API. The exact domain input, submission
and rejection types must be chosen by the witness. It must compare a direct
async external operation with `spawn_blocking`, and use the latter only for
genuinely synchronous/CPU-heavy work. Tokio's `spawn_blocking` has a large
default blocking-thread limit and queues work at that limit, so a Tokio
builder's thread limit alone is not the requested admission bound. An
application-owned semaphore provides the concrete bound.

The unresolved custody equation is:

```text
accepted request + permit + started operation
  -> preparation future dropped during interpretation
  -> operation may continue; JoinHandle drop detaches it
  -> typed worker/result cannot be invented or silently discarded
```

Tokio 1.53.1 documents that abort cannot stop an already started blocking
closure. Runtime shutdown waits for such closures unless the runtime owner
uses `shutdown_timeout`, which stops waiting but still does not cancel them.
Neither behavior constitutes successful Bombay actor retirement. The
selected DG-TASK contract must identify who retains the operation's join and
settlement after caller cancellation. If that cannot be expressed through
the current interpreter/Environment port, this witness depends on a precise
CAP contract; it must not add an EXEC-specific service to evade the boundary.

## Required independent experiments before acceptance

| Witness | Required complete observation | Status |
| --- | --- | --- |
| Saturation | One accepted request holds the only permit; a second first-source request rejects before work starts and returns the exact original request/input. | Open. |
| Completion | The accepted operation completes, its typed worker submission reaches the existing settlement, and a later request obtains the released permit. | Open. |
| Pending cancellation | Drop before permit acquisition and after admission but before completion; distinguish request ownership, permit lifetime, operation lifetime and joined result. | Open; DG-TASK prerequisite. |
| Shutdown | Give the concrete semaphore/source an explicitly identified closure authority; close its admission and retire the owning actor while one operation is queued and one is running; report actual join/terminal outcomes without assuming preemption. `WorkerPreparationSource` itself has no `close_admission` method. | Open; DG-TASK prerequisite. |
| Normal versus destroyed runtime | Awaited completion has a terminal witness; destroying the executor makes no claim that running blocking work was stopped or joined. | Open. |
| Pure-fold discipline | Verify the witness Behavior's `init`/`receive`/`transition` has no direct source call or `spawn_blocking`, and that the emitted `PrepareWorkers` reaches the existing interpreter. Ordinary Rust cannot statically forbid arbitrary I/O in a user-written fold; this is a repository policy and observable-trace check, not a compile denial. | Open. |

Use a pinned-Nix debug and optimized command for the final in-crate witness.
Before any production edit, add the prescribed change ledger and original
failure witness. An isolated Tokio semaphore experiment can establish Tokio
mechanics but cannot accept this gate without the real `PrepareWorkers` path.
A custom pure Behavior emitting the actual request can exercise the typed
port without claiming complete pool/supervisor policy coverage.
The gate remains **open** until an independent reviewer signs the exact
source hash and all rows above pass.


## Fresh selected-port verification (2026-10-02)

Current 0.20.0 interpretation differs from the frozen equation above.
PrepareWorkers starts a capability task and immediately returns Accepted with
WorkerPreparationStarted; saturation inside the later source operation is
WorkerPreparation::SourceRejected with exact source and typed reason, not an
ItemSettlement rejection returning the outer PrepareWorkers request. The
owning fixed supervisor explicitly carries source and rejection separately
through its recovery and diagnostic policies. Lawful FIFO restoration returns
the source to Available and retains the rejection in its diagnostic; a corrupt
source-state path substitutes SourceStateCorrupt and must not be described as
preserving the original reason. This source distinction is recorded, not
resolved by rewriting the current interpreter to the old narrative.

An isolated comparison at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-index-admission-p35qpmzx`
implements the actual WorkerPreparationSource port with bounded direct async
indexing and a synchronous operation. Three tests pass in both profiles:
saturation returns the exact move-only Vec allocation without starting work,
closed admission returns exact input, accepted work returns the complete
WorkerSubmission, and a started synchronous operation retains its permit
through admission closure until actually joined. Dropping the direct async
preparation future releases its permit but returns no input or submission;
the source no longer owns that moved input. That unresolved custody is an
observed limitation, not an accepted cancellation law.

The candidate rejection type owns the exact operation input; the source then
has no operation input. The full pool/supervisor consumer, recovery and external
caller extraction have not been exercised. Current FIFO public diagnostics
do not expose extraction of that input. This comparison cannot satisfy EV-24
or DG-WORK without integrated owning-consumer and retirement evidence.

From the isolated directory:

```sh
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked -p bombay-rs --lib index_admission -- --nocapture
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked --release -p bombay-rs --lib index_admission -- --nocapture
```

Each passes three tests. Changing capacity from one to two causes the intended
saturation-variant assertion to fail in debug and release with exit 101 using
the bounded_index_admission_returns_exact_input_and_submission filter. This
is a bound inversion, not an original Bombay defect. Positive patch SHA-256:
`30f4aa4c9b4750cec7f1e27dd60d1c1c1ce2b21fa1afb871b97e05769fd17d4c`;
inversion patch SHA-256:
`853c915fdef1d4e619a5ef74f0f69ca7b002c57c0b6fb117e661ed921b734bb1`.
Receipt.json records unchanged baseline/lock and profile log hashes; commands.txt
SHA-256 is `d9e8e7b9be4c61e44c558d8218dc701eb99f067046032e230f64c71fd044c41e`.
Scratch: one path, tests +235 / -0, production/public types zero. Retained
repository delta from experiments: zero. Two dead-code warnings remain in
research error payloads; no strict-lint or integrated acceptance is claimed.

## Actual interpreter and owning FIFO custody comparison (2026-10-02)

Frozen `bombay-worker-admission-custody-wb8ytqps` uses the current unchanged
Communication 0.1.3 manifest/lock and selected Core/Actors 0.20.0. Actual pure
FIFO recovery emits PrepareWorkers; ApplicationCapabilities returns the exact
accepted start receipt and owns the asynchronous operation in ActivationTasks.
An admitted synchronous operation retains its move-only input allocation and
semaphore permit through admission closure. Actual capability retirement stays
Pending until that operation finishes and is joined. The late complete return
then reaches FIFO shutdown, whose opaque terminal diagnostic retains the
original sorted input until explicit discharge, observed exactly once.
The direct async comparison completes two sequential inputs using the same
bound. Saturated and closed requests return their untouched inputs at the
source port without starting another operation.

All nine owning send lanes, creates and transition choice are asserted in the
pure setup and return traces; observation correlation uses actually issued
CreationIds. Replacement.previous is only matched with `{ .. }`, so this does
not prove complete previous-incarnation lineage. Initial worker installation
uses the existing simulated fixture: no actual installed worker graph claim.

Seven tests pass in each pinned-Nix debug/release profile; strict Clippy and
formatting pass. Capacity one-to-two and omitted owning settlement each fail
the intended compiled assertion with exit 101 in both profiles; restored
positives pass. These are comparison inversions, not an original production
repair. Receipt SHA-256:
`8946adcff5e846d612dc9b352cbe688268c89e91d4f7f2a2877a0c4e486adb7f`;
patch `fe6bc0a2e339e9f66ac291498a8569dfe71969070e670d78871fecfea8b8052a`;
source `413af4253d272b19a32f80ff1159514fa5b9da7ac69061ab20622b293b42ddc2`.
The receipt records exact commands, profile logs and 22 authenticated artifacts.
Independent reviewer `/root/task_custody_research` read the complete candidate
and actual interpreter, retirement and FIFO owners, verified the unchanged
production prefix, and independently reran all seven tests in each profile
with a separate build target. Its signature covers this bounded comparison.
Isolated tests +509 / -32 / net 477, one existing test module; production,
public types and canonical retained source delta zero.

DG-WORK remains open. Shutdown with both an admitted queued operation and a
running operation is unproved: the second operation here is rejected.
Consuming original-input recovery through FIFO's opaque diagnostic remains a
selected requirement, still without an owning consuming API proof. More
fundamentally, the source contains semaphore and receiver
resources in Behavior state, contradicting AGENTS.md's state prohibition and
this record's proposed source construction. No I/O occurs inside the folds,
but that does not satisfy the stronger state rule. The selected policy below
rejects this fixture as semantic acceptance or a production composition.
Runtime destruction, panic, installed worker graph
and remaining DG-TASK dependencies also remain open.

## Selected state purity and rejected-input policy (2026-10-02)

Under the user's authorization to adopt recommendations, retain AGENTS.md's
prohibition on channels and runtime resources in Behavior state. A Behavior
owns semantic inputs and emits typed requests; Bombay's owning interpreter
owns permits, channels and concrete operation execution. No opaque-source
exception is selected. The current bounded source violates that law even
though its folds do no I/O. An ordinary-Rust comparison must establish the
smallest owning source/interpreter contract before a new API or service is
eligible; no such contract is inferred from this policy choice.

Rejected operation inputs must be recoverable by consumption for retry, with
their exact original contents and ownership. Merely retaining them inside an
opaque diagnostic is insufficient. Core/Actors owns reusable diagnostic
custody; Bombay owns concrete execution. Verify the existing consumers and
smallest owning extraction law before changing that interface. Independent
review and the full required witness table remain mandatory.

## Pure source and runtime-owned execution comparison (2026-10-02)

`bombay-pure-worker-context-o0f15uv1` replaces the earlier runtime-containing
source fixture for bounded state-purity and queued-work evidence. IndexSource
owns only the original Vec; runtime IndexInterpreter owns the semaphore,
operation and JoinSet and interprets existing Core InterpretItem and Actors
PrepareWorkers requests. Existing source/accept/reject ownership methods
express this advanced interpreter path without changing the owning source
algebra. No I/O or runtime resource enters Behavior state.

A runtime with one blocking worker starts the first operation before accepting
the second into its queue. Both exact inputs and permits remain owned; joining
stays Pending until each corresponding operation is released. Closing admission
does not destroy either admitted operation. Both original allocations return.
Direct async execution uses the same owning products. Saturated and closed
requests return exact inputs. Full nine-lane FIFO actions, creations and next
state are checked using actually issued observation correlations.

Receipt `e723a62f5bd4cd268bec7d9dd758602819236cfe792354598ce3a5ff5e18221a`;
patch `1582812db193b3b1a740f2cc01c6f7d1cbf880f4727ab6e14ff0526784ff4ac8`;
source `d9f82f67f0ea8acbaeb40e51542b881726ece433ae22968a8697e13fe247d947`.
Seven tests and restored positives pass in both pinned-Nix profiles; fmt and
strict Clippy pass. Increased capacity and omitted join each cause the intended
compiled assertion failure in both profiles. Independent reviewers
`/root/task_custody_research` and `/root/observation_research` read the complete
patch and authenticated all 32 artifacts and the selected owning ports.
Their signatures cover this comparison only; infrastructure failures and the
superseded assertion containing a consuming read are excluded.

Existing test module +374 / -44 / net 330; production, public types and
canonical source delta zero. This custom interpreter is not integrated into
standard ApplicationCapabilities, whose current source bound supplies no
runtime-owned context input. Actual actor capability retirement, native
families, multiple roles, previous-incarnation lineage and consuming opaque
diagnostic extraction remain unproved. DG-WORK and EV-24 remain open.

## Selected opaque-diagnostic extraction proposal (2026-10-03)

Actual selected FIFO diagnostics retain a source rejection privately and expose
only borrowed role/debug information. An existing approved Bombay test-file
compile comparison reaches E0616 when attempting to consume that private cause;
this establishes the extraction gap, not runtime acceptance. Receipt
`da8194e1820e449e9d3e7e3a7b09a99c9407e8fb602445ed7c268cdada864542`.
Source-purity
violations in the inherited fixture remain excluded from semantic evidence.

Read-only proposal `40266ce8032d49faf4caebd89768b91d2a865f70e75867af8b482171a704be24`
and external draft `0074f8a4cbf30df8de5e19efcfa72701dd727da8b75e1d3f3041822490c6c2f7`
compare a consuming existing method with a broad cause enum and visitors. The
method's successful standard Result tuple moves the original shared role Arc,
previous WorkerAttempt, whole ChildStopped, actually returned Source option and
typed SourceRejection; every other complete diagnostic returns original Self.
It adds no state, protocol, wrapper or public type, and no Role Clone bound.
Proposed two production files: +39 / -1 / net 38; one public method. The two
owning test files make four additional existing upstream paths, all outside
the currently authorized 114. No owning edits are authorized or made.

Independent bounded proposal review
`1b9b3c56b8f731068cd42e6128ae98eb05331840e523ef0318fb6c5384f40384`
authenticates five proposal artifacts and nine selected owner files and accepts
branch-local ownership completeness only. Caller syntax, inference, lint,
complete actual FIFO traces, original-input/role allocations, nonmatching
diagnostic return, inversions and all affected consumers remain required before
API retention. The visitor adds a callback without a demonstrated ownership
need; panicking after tuple extraction can also destroy values. A borrowed-role
visitor cannot return the original owned shared role.

Current producers replace the original source rejection with SourceStateCorrupt
when source restoration fails. Thus successful Some(Source) extraction is not a
current lawful producer trace and must not be fabricated as proof. The selected
source invariant below resolves reachability; this accessor does not repair or
authorize original-error erasure. Existing opaque
correlation allocation is not itself evidence of nondeterministic behavior:
heap allocation is permitted, and no paired lawful consumer trace establishes
a contract-visible mismatch. No alpha-renaming law is assumed.

## Actual actor retirement comparison (2026-10-03)

`bombay-index-actor-retirement-_di13ptd` extends the pure-source comparison
through a real actor and its existing LocalEnvironment retirement path.
IndexDepot owns only typed requests, start receipts and returned values;
IndexCapabilities owns execution resources. Two original FIFO-issued requests
enter the existing typed source lane. One blocking operation runs and the
second queues on a one-worker executor. The same actor retirement future stays
Pending until both operations finish; both original Vec allocations survive.

The complete returned actor state and residual are checked: two start receipts,
OwnerCancellation, two exact late control returns, and empty remaining request,
settlement, user, task, failure, descendant and unread-cancellation lanes.
Both FIFO owners accept their own actual correlations and original results;
all nine action lanes are checked. Admission permits return and the retired
endpoint disappears. This uses the existing advanced test-host boundary,
not a new EXEC work service or a selected public Application context interface.

Receipt `16be541f9191ff8da74560d69383ce6b41b7bf293eef3c7bb6211ae4b4ca5a69`;
patch `5348baff645154112395d0d1fe0c0f0a273b98accb90510ce26801871ae22222`.
One approved test module: +776 / -44 / net 732; production/public types zero.
Independent nonauthor review
`a60408bc54743b249f252d1e40ec5d4d1c785169665f55c92a27bd307c5c100f`
authenticates 345 sources, 51 artifacts and seven selected owners. It signs
the new witness only, not the reviewer's inherited core implementation.
Omitting the task owner or discarding late returns causes intended compiled
failures in both profiles; these are composition inversions, not reproductions
of an original runtime defect. Formatting and strict Clippy pass.

Coordinator receipt
`3c8810b79765ce4e7d66baf428fbeb8aad7f6835808bfc365fe1fa29213bcb38`
authenticates the freeze and records fresh pinned-Nix debug/release positives.
The `index` filter runs four tests: three work witnesses and one existing
Observe hashing test. Both profiles pass all four. Coordinator did not rerun
the mutations. Simulated FIFO endpoints do not prove an installed child graph
or complete replacement lineage. Executor destruction, panic, final receiver,
family cleanup and consuming opaque-diagnostic extraction remain unproved.
Inherited runtime-containing SearchWorkshop fixtures remain excluded.
No production retention or full DG-WORK/EV-24 acceptance is claimed.

## Selected source-restoration invariant (2026-10-03)

Read-only record
`ed38cbfba6cf726679ce0777c2bf72a7b56ca332e2ec643fb0c872bc0da4284a`
examines all six restoration calls in Actors 0.21.1 and its two preparation
issuers. Nine selected source/test hashes authenticate. Claiming the sole
available source moves its slot to AwaitingReturn before issuing an affine
request; another role waits. Matching terminal start/return consumes the
original expectation before restoration. Shutdown retains that same occurrence,
marks it Drained before restoration, and issues no new preparations. Foreign,
early, repeated or post-stop inputs cannot restore the source.

Within safe publicly constructed pools and successful folds, each restoration
therefore sees AwaitingReturn. No lawful occupied-source restoration trace is
established; the hypothetical SourceStateCorrupt branch does not justify a
production repair. This is a source invariant, not a new executable regression
or optimized replay proof. It excludes forged private state and resumption
after panic. Coordinator authenticated the nine owners and read the actual
claim/restore operations, six call sites and live/draining receipt acceptance.

A lawful source rejection restores Source into the pool and retains the original
typed reason in its diagnostic, with returned_source None. Consuming that reason
is the separate proposed extraction law. It does not transfer the pool-owned
Source or establish an external retry policy. No new source-state model, public
type, owning edit or full gate acceptance follows from this inspection.

## Completion versus executor destruction (2026-10-03)

`bombay-index-runtime-destruction-6d6ienie` adds one test to the preceding pure
source/interpreter comparison. Awaited completion preserves both original Vec
allocations through typed worker returns. `shutdown_timeout(Duration::ZERO)`
and `shutdown_background` instead return while the first blocking operation
remains gated and the second queues. Both source wrappers return their actual
cancelled JoinErrors. After releasing the first operation, the second runs on
the sole BUSY blocking worker despite shutdown. Tokio's separate IDLE shutdown
drain can cancel queued work; this witness establishes only its controlled BUSY
schedule, not a universal queue policy or preemption guarantee.

Each admitted operation temporarily owns its original Vec in a sole immutable
Arc and publishes only Weak to the host. Temporary strong observations end
before gate release. With no surviving waiter after cancellation, original
payloads are discharged by the actual worker-stop acknowledgment; this does
not claim retention until that acknowledgment. Normal Arc unwrapping instead
moves the original Vec into the typed result. No extra result owner preserves
values artificially, and no runtime resource enters Behavior state.

Receipt `0ce223e2a55d2cd6e0934d53cf680acf842d4575adf9ecef6b60eee7e3e4d219`;
patch `be3d79e987c9506a530d5e6fa60dcc1a6c1883d273101903bef3e29717e6ebf9`.
One approved test module: +248 / -1 / net 247; production/public types zero.
Nonauthor coordinator review
`d67cea0542e0d96e94a3453549860bd24cd7f49d573522b32ddfbf22805ef7f3`
authenticates 345 sources, 36 artifacts and five selected Tokio source/test
files, reads the full patch and ownership model, and records a fresh pinned-Nix
positive in each profile. Author preservation/restoration passes five tests per
profile: four work witnesses plus one existing Observe hashing test. Formatting
and strict Clippy pass. Substituting a cloned input compiles and fails the exact
allocation oracle in both profiles after all gates are released. This is a
counterfactual composition inversion, not an original production defect.

Immutable scope correction
`60a2a57a0a2ea589d2687ec61c3bbc29df73c49298287c54a6a0879fd7af8823`
clarifies that the destruction test checks permits, not actor endpoints.
Ordinary runtime Drop can wait indefinitely for noncooperative blocking work;
it is not tested while these gates remain held. No joined actor/family cleanup,
final receiver custody or successful retirement is claimed after executor loss.
Consuming diagnostic recovery and other complete acceptance evidence remain
open. No full DG-WORK/EV-24 or canonical implementation retention follows.

## Owning-scope approval (2026-10-03)

EXEC section 41 records explicit user approval of sections 33–34 together:
129 existing repository-qualified paths and at most three observation-owned
public types. Scope permission resolves the budget blocker; it does not select
a model, accept this gate or waive exact current-owner verification, ordinary
Rust comparisons, original-defect/inversion evidence or independent review.

## Owning consuming recovery experiment

EXEC section 43 assigns the exact four-file selected git5ca experiment at
/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-fifo-source-recovery-pqnw9sy8.
It adds one consuming method to the existing opaque diagnostic, returning its
original shared role, worker attempt, whole stopped fact, available source and
source-rejection reason. Every other diagnostic returns unchanged as `Self`.
No new semantic owner, recovery policy, public type or role-cloning bound is
added. This is an Actors API addition; Bombay interprets the existing work.

The first original-source runs also missed the existing `Activate` import;
they are preserved but excluded from isolated interface evidence. Independent
review 88bd06b03e4d8d9b8cb9442d4d017f415b0aafcac0bb2a7f3e1b35ab6a34ecd3
qualified that test-only import. The repeated original-source commands then
fail solely for the absent consuming method in both profiles. After the exact
reviewed production patch, both focused consumers pass in each profile.
Four deliberate changes compile and fail their intended tests in both profiles:
regenerated worker authority, changed stop time, cloned returned allocation and
erased nonmatching reason. The allocation change is a caller-composition
inversion, not an original library defect. A separate role-`Clone` constraint
fails statically. All five inversions restore to passing tests in both profiles.

Actual source manifest
728f4f44dbc45b08e476652b79a9fa1d1819c53af20c7b331f6c73da3a82a0f0
binds all 806 inputs: four changed, 802 unchanged, no lock/configuration change.
Measurement 9e1545245301aeededf426ef099e546e3c4a40eee5fecea4b3cf0069b11e6c8d
records production +39/-3/net36, tests +444/-7/net437, public types +0/-0 and
one new public method. Formatting passes. Strict Clippy then rejects two new
unreachable `let-else` branches for the existing `Infallible` diagnostic route;
its exit-101 log remains a nonpass and nextest did not run. The original nonpass remains preserved. Independent review
cd71b26553962601920e08b4476f2a24b4345df22e45effa7eec5cc60861e1dd
accepted the two-test correction: exhaustively match the existing empty route.
It adds no production, bound, lint suppression or path.

The final 806-input source manifest is
c0c3a6ad7fb6fd3efbb6647fb2947711dc0f4b4b3efd5d7e6ca5dc5adfdb1feb;
final four-path patch
d0ec1263b7526997ca3b8ac4ec4a7145a134fb250434a18142903ade833dfbe3.
Actual production is +39/-3/net36, tests +446/-7/net439, public types +0/-0;
one public consuming method is added. All four focused profile commands,
formatting and strict all-target Actors Clippy pass. Nextest passes 859 tests,
zero skipped. All ten owning Nix checks pass; command receipt
9588819ef03465496a0cc2a3d11c4f5c5a0eba3b78df1467dec0c983ec030c85
records the pinned Bombay shell and the owning derivations' Rust 1.95.
No toolchain, lock or configuration changed.

Final receipt 379de0d01b3c4fbfc52d804b25244858bdbc2cf4fb45dc335a618581a2c0649e
binds 87 artifacts and all 806 sources. Independent nonauthor review
4937b3f4ab6f26fa6bf7368ab4356d4b959ce25d8f5722e2399cfc6a3614c443
accepts bounded owning-contract retention and upstream delivery. The isolated
Git worktree matches every final source; 802 original inputs remain unchanged.
[Behavior PR #82](https://github.com/devrandom-labs/bombay-behavior/pull/82)
was independently approved by `trivejoel` at exact head
1a1c21dfbbaecd3f869246354b6be7d1c090974c; review receipt
2695cb59a8537aa992811f7a32164b2438faa927fa301be4b92cf17e8e27ff17.
All fourteen GitHub checks passed, including eight mutation shards, their
aggregate, Nix, the published API audit, advisories and CodeQL.
[CI run 37166077533](https://github.com/devrandom-labs/bombay-behavior/actions/runs/37166077533)
binds that head. The reviewed PR merged on 2026-10-04 at
52c3130d39117ccb772a72e2898b658b3a7815c8. Immutable reviewed-merge receipt
d62ae25b6e83bb9bc0b7e2cbae59773a884053e2471ab71a11edc3f6450c03bf
records the actual API responses. Publication and fresh selected-source
verification are recorded below; selected-build and full EXEC gates remain open. This is not full EV-24/DG-WORK acceptance or canonical Bombay retention.

The owning release [Behavior PR #83](https://github.com/devrandom-labs/bombay-behavior/pull/83)
merged after independent review and all fourteen checks passed at exact head
44000b862127293dcc8eb01fc448337fd81c4881. Merge commit:
edc2d466a50df7cd396f891e3da31fc9e3747bbd. Published Core/Actors versions
are 0.21.2; Macros remains 0.13.1. Original generated-head preflight fails
on three stale isolated workspace locks. Only their six local version entries
changed; third-party selections and both README installation requirements are
unchanged. Exact-head bundled preflight, packaged consumer and all ten owning
Nix checks pass, with 859 nextest passes and zero skipped. Local receipt
6ffd7211c0d272b4fd1c6030a4fc9f1828e8e6f903c9afaad347f2d67f3c59b3
binds all 806 sources; actual filtered Nix-source receipt
6e897f2727497c98efe75d3285621726f73094d80e738d736725530039441bbc
binds its 397 files. Independent review
1455b10f414eed733c7077a797ff2812691ca057cbb09f1aa307882944fb7994
authenticates source, locks, versions and results.
[Release-head CI](https://github.com/devrandom-labs/bombay-behavior/actions/runs/37167325568)
is green. [Verified-main CI](https://github.com/devrandom-labs/bombay-behavior/actions/runs/37167753677)
and [automatic publication](https://github.com/devrandom-labs/bombay-behavior/actions/runs/37168163742)
also pass at the release merge. Both published package VCS records and release
tags resolve to that exact merge. Core checksum:
67210103f2be49efc2ecbcf42f478f2beac07da5e4e722e4a74bc58b533c16f8;
Actors checksum:
2c7d8a69c8d713ae7b35887a5fcddc868a3ef948e4947ee9049cc58f057fc2fb.
Fresh independent receipt eea124f8edbd12367dd107dc63029a532db3ccc472b9db93c2f96056d9db6f64
authenticates all 216 packaged files, their original archives and all neighboring
selected contracts. Core Rust is unchanged; Actors changes only the reviewed
four FIFO source/test paths. Bombay workspace and fuzz locks now select this
release without changing third-party selections; EXEC section 46 records the
separate selected-build checkpoint. This release does not close full DG-WORK.
