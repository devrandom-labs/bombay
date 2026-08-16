# Bombay architecture backlog

This is the canonical executable backlog. It contains only current selection
rules, dependency state, unfinished-item contracts, and runtime invariants.
Completed campaign narratives are intentionally omitted from this public tree.

The accepted Driver contract is [`driver-law.md`](driver-law.md). DR1 replaces
the Transition/Machine Executor actor path with the one direct-Behavior Driver.

The accepted exhaustive verification contract is
[`driver-test-strategy.md`](driver-test-strategy.md). It requires a
machine-checked manifest for every exported actor template and strategy variant,
all declared composition edges, generated deeper stacks, negative compilation,
boundary/property/model/fuzz/mutation testing, and adversarial ownership and
cancellation campaigns before Driver completion can be claimed.

### Driver-law implementation verification — 2026-08-15

The 74 `D-*` laws were explicitly requested as mandatory implementation
requirements. That request is sufficient acceptance of the law set itself,
but it does not waive dependency-contract conflicts or the per-feature audit.
The dependency choice below was resolved explicitly by the owner. DR1's
standalone Engine implementation is feature-complete, but final law closure is
blocked at the external ownership boundaries recorded below.

The repository is at `fbfad444ec7999ada02dceae68ff3f6a6273ab3b` with an
already-dirty documentation worktree. DR1 deliberately aligns the lock and
workspace requirement to the clean local Behavior/Actors 0.12.0 checkout at
`40b39b2605416e3b88427e3289c4dac4568c78e0`. Communication 0.1.1, Observe
0.1.0, Timers 0.1.0, and the locally patched Address 0.2.0 at
`7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` remain selected. Transition and
Machine Executor 0.1.0 were audited but are no longer Engine dependencies.

The fresh source/API/test/documentation inspection establishes these exact
ownership facts:

- Behavior 0.12.0 owns the pure `Behavior` fold, `Actions`, typed
  `SendAlgebra`/`SendInput`, births, and move-only event/action ownership. It
  no longer exports `Compose`/`Active` from the core crate and has removed the
  former positional product/path API.
- Actors 0.12.0 owns consuming `Activate::initialize`, `Initialized<B>`,
  `Active<B>`, the wrapper-extension `Compose` trait, and the exported reusable
  template catalogue. `Activate::initialize` and `Active::transition` are the
  exact direct-Behavior lifecycle boundary used by the Driver; `Compose` is no
  longer a mandatory container or an initialization type.
- Communication 0.1.1 owns two-lane admission, selection, FIFO/fairness,
  closure, wakeup, cancellation, backpressure, and rejected-payload recovery;
  the Driver may consume only its already-selected event through Bombay's
  adapter.
- Observe 0.1.0 owns exact-generation retained outcome publication and
  observation races; it supplies no Driver lifecycle or fold operation.
- Timers 0.1.0 owns keyed schedule generations, replacement, cancellation,
  deadline order, and single expiration; timer policy remains a typed
  Behavior/Actors protocol interpreted by Bombay.
- Address 0.2.0 owns exact-generation claim/resolve/release and opaque resolved
  endpoint snapshots; the Driver owns no address value, lease, or generation.
- Transition 0.1.0 owns affine representable machines and topology
  composition. Machine Executor 0.1.0 owns exclusive/serialized/linearized
  execution policies and poison behavior. Neither is required by the proposed
  single-owner direct Behavior turn; Entity and genuine machine-topology
  consumers remain intentionally separate.
- Engine owns the universal causal sequence only; concrete Bombay
  environments own typed action interpretation; incarnation owns generation,
  task, cancellation classification, and publication; a later composition
  layer owns the narrow prepare/initialize/commit/publish transaction.

The owner explicitly resolved the apparent instruction/dependency conflict:
all positional product/path guidance is stale and must be removed.
Behavior/Actors 0.12.0 and its named semantic send structs are authoritative.

The owner also resolved the Driver lifecycle shape during implementation:
Engine is designed on its own terms before Bombay is adapted around it. The
universal public lifecycle is exactly `Driver::new` plus consuming
`Driver::run`; there is no prepared Driver object or split initialization/loop/
retirement API. Transactional publication remains later-layer policy
and may use an environment-owned acknowledgement of the first local action
commitment without exposing a Driver continuation. The earlier prepared-Driver
proposal is superseded and removed from current normative documentation.

### Core incarnation-layer verification — 2026-08-16

The owner authorized exactly one new layer above Driver in
`crates/bombay/src/core/`, explicitly excluding System, mailbox construction,
address publication, child policy, and handles from this step. The mandatory
feature-local dependency inspection was repeated before implementation:

- the locked and locally patched Behavior and Behavior Actors versions remain
  0.12.0, but the clean `split-behavior-actors` checkout has advanced to exact
  revision `40b39b2605416e3b88427e3289c4dac4568c78e0`; `Behavior`, consuming
  `Activate::initialize`, `Active::transition`, exact `Actions`, and named
  products remain the Driver contract;
- Communication is the exact locked crates.io 0.1.1 artifact with checksum
  `f2cadfc4173d780a22e05bd5abdbc4d6e96480f620120f6f6de7feb8acf49a83`;
  its two lanes, closure, wake-up, and payload-return contracts remain below a
  future environment adapter and are not part of incarnation;
- Observe is the exact locked crates.io 0.1.0 artifact with checksum
  `e3f88706f2ca473d2de465bded4a1df68af49b2ba1e837359a62ce91ec60ccf3`;
  its `Subject`/`Observation` generations can later implement terminal
  publication, but the core layer depends only on a static retirement port;
- Timers is the exact locked crates.io 0.1.0 artifact with checksum
  `5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d`;
  its generation queue remains environment-owned and is dropped with Driver;
- Address is patched to the clean 0.2.0 checkout at
  `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`; its registration lease remains a
  later retirement implementation detail, not incarnation state in this layer;
- Transition and Machine Executor 0.1.0 source, APIs, and tests were rechecked;
  their topology, reusable seats, scheduling, and poison contracts remain
  inapplicable to a single consuming Driver incarnation and are not restored;
- the existing Bombay runtime tree still targets `PreparedDriver`, `RunExit`,
  `RunError`, and `RuntimeEffects`. It is superseded input, not an API
  constraint. The new `core` layer will contain no second Driver path and will
  be tested independently before that tree is removed.

The resulting ownership contract is deliberately narrow: core incarnation
owns one Driver execution plus terminal classification and calls one static
retirement capability only after Driver-owned values have dropped. It does not
own or manufacture identity, publication, task spawning, mailbox selection, or
runtime capability interpretation.

### Repository closure and renewed Actors-contract audit — 2026-08-16

The remote Behavior repository was fetched after the owner reported extensive
updates. The former `origin/split-behavior-actors` branch has been deleted;
`origin/main` is `135d166a` and differs from the clean local merge
`40b39b2605416e3b88427e3289c4dac4568c78e0` only by the same foundational
Behavior value-contract tests already merged locally as `ba64c51`. The current
`docs/driver.md`, `docs/adapter-contract.md`, `docs/actor-catalogue.md`, Actors
exports, `Compose` implementation, source tests, and examples were re-read.
They still require representative wrapper-order tests and compile-time
rejection of unsupported compositions, but publish no finite supported and
rejected composition graph or maximal meaningful stacks. D-VER-3 therefore
remains planned rather than guessing an owner contract.

D-VER-4 is now passing. Its repository oracle recursively inspects every
current Rust, Markdown, TOML, JSON, and workflow artifact outside generated
and mutation-output directories. It rejects resurrection of the deleted
framework, Bombay runtime/mailbox/routing tree, Bombay examples/benchmarks/fuzz
target, and stale prepared/System-era contracts. Its inversion test injects
each stale path and contract and proves rejection. Current adversarial and
performance documents were replaced with executable Engine/core gates; older
runtime claims remain only in the explicitly historical ledger.

### Transactional activation-layer verification — 2026-08-16

Before changing the next layer, the feature-local dependency audit was
repeated from the exact locked artifacts and local sources:

- patched Behavior/Actors 0.12.0 at
  `40b39b2605416e3b88427e3289c4dac4568c78e0` owns consuming initialization
  and complete initialization `Actions`; neither crate owns publication;
- Communication 0.1.1, checksum
  `f2cadfc4173d780a22e05bd5abdbc4d6e96480f620120f6f6de7feb8acf49a83`,
  owns mailbox admission and closure but has no activation transaction;
- Observe 0.1.0, checksum
  `e3f88706f2ca473d2de465bded4a1df68af49b2ba1e837359a62ce91ec60ccf3`,
  owns generation-safe terminal publication, not address activation;
- Timers 0.1.0, checksum
  `5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d`,
  owns timer generations and contributes no publication operation;
- Address 0.2.0 at
  `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` owns immediate exact-generation
  claim/release; it exposes no provisional or initialize-before-claim state;
- Transition and Machine Executor 0.1.0 still own representable topology and
  reusable executor-seat policy respectively, neither activation ordering;
  they remain excluded.

The ownership conclusion is that initialization commitment is already visible
at the concrete environment's first successful `apply`. The minimum reusable
layer is therefore a static environment decorator that publishes exactly once
after that successful application. It must publish nothing when Behavior
initialization or the first application fails, preserve distinct inner and
publication errors, and delegate all later turns and retirement unchanged. It
does not construct an address, mailbox, task, or System object. A later exact
generation capability supplies the concrete publication operation.

CR1 is distilled. Its production surface is three focused files:
`Incarnation`, the exact `IncarnationOutcome` sum, and the consuming
`Retirement` port. A final decomposition attempt found no removable object:
folding the outcome into Incarnation would hide a required public terminal
contract, removing Retirement would couple later generation cleanup into core,
and exposing the private terminal guard would create lifecycle authority. The
old Bombay facade and runtime trees were deleted, so there is one compiled
Driver path. Eight focused unit tests cover ordinary completion, exhaustion,
both exact error payloads, panic, cancellation, drop-before-retirement,
exactly-once handoff, structural minimality, and deliberate semantic
inversions; the allocation oracle proves zero Incarnation-added allocations.
`cargo-mutants 27.0.0` examined five core mutations: four viable lifecycle
mutations were killed and the attempted default outcome replacement was
unviable because the exact outcome sum deliberately has no `Default`.

The owner refined the successful result after reviewing the information lost by
the former unit alias. `Completion::Stopped` records an explicit Behavior stop;
`Completion::Exhausted` records permanent input exhaustion. Both remain
successful `Ok` values. Exact Behavior/environment errors remain disjoint;
incarnation still owns panic and cancellation classification.

The standalone Driver retirement contract is also resolved without using the
current Bombay runtime as a constraint. `Environment::retire` is an
asynchronous completion barrier after the final Driver result has already been
determined, not a second action interpreter. Recoverable capability rejection
belongs to `Environment::apply` and its exact typed error; retirement has no
retry, rollback, successor turn, or alternate success classification. It
therefore returns `()`. A panic remains a panic, and cancellation remains
honest because dropping the Driver future does not claim the barrier completed.
This decision does not assert that the current Bombay retirement machinery is
already suitable; Bombay will be overhauled around the Driver contract later.

Current Engine evidence after distillation:

- `Driver<B, E>` stores exactly `B` and `E`; its only lifecycle operation is
  consuming `run(self)`.
- `Environment<B>` has exactly `next`, `apply(ActionsOf<B>)`, and `retire`.
  Its futures need not be `Send`; executor/spawn bounds belong at a later
  integration point.
- The Driver observes the stop marker by reference and moves the untouched
  complete `ActionsOf<B>` exactly once. There is no Engine-owned action DTO.
- `ActionsOf<B>` derives its address, phase, sends, and birth components
  directly from `B`; it does not reconstruct even the closed phase component
  as a hard-coded `Never`. The `B: Behavior<Ph = Never>` execution bound is
  imposed only where the closed Driver actually runs.
- `Completion::{Stopped, Exhausted}` preserves the two factual successful
  terminal causes without turning input exhaustion into failure;
  `DriverError::{Behavior, Environment}` preserves exact typed failures.
- An executable classification oracle rejects collapsed, swapped, or
  failure-valued stop/exhaustion results.
- Focused causal, controlled-failure, initialization-failure,
  initialization-commit-failure, partial-commit, move-only action/creation,
  stop, closure, self-send, panic, cancellation, and minimum-bound oracles pass.
- Cancellation is exercised while initialization commitment, input acquisition,
  turn commitment, and retirement are each pending; every case drops owned
  values without falsely reporting asynchronous retirement or completion.
- Initialization and event-fold panics are both exercised as consuming terminal
  paths: no later input is polled and the environment is dropped. Dedicated
  inversions cover recovery, repolling, and leaked ownership.
- All ordinary result paths—initialization failure, first commitment failure,
  Behavior failure, turn commitment failure, stop, and input exhaustion—prove
  exactly one retirement attempt. Missing and duplicate retirement mutations
  are rejected.
- Stop proves its complete final actions commit before success and forbids later
  ingress; closure proves no synthetic fold or repoll. Ordinary terminal edges
  are tested as fused, with targeted dropped-final-action, synthetic-event, and
  post-terminal-work mutations.
- Compile-fail conformance rejects preparation, split init/loop/retire,
  recovery, reset, restart, reuse, and poison-clearing controls. Structural
  mutations adding any such surface are also rejected.
- Initialization is counted exactly once across stop, closure, Behavior error,
  initialization-commit failure, and turn-commit failure. Every accepted event
  identity is folded exactly once; missing and duplicate initialization/fold
  mutations are rejected.
- Stateful decisions prove successor state and committed actions arise from the
  same fold. Controlled failures commit no fabricated actions and permit no
  continuation; stale-state, stale-action, fabricated-action, and continuation
  mutations are rejected.
- The same Driver executes unrelated custom Behavior shapes, including
  move-only/non-`Sync` state and initialization-stop definitions. A real closed
  `UserEvent` sum crosses ingress as one `B::Event`; complete output lanes cross
  untouched. Shape-specialization, side-channel ingress, output projection,
  template inspection, and dynamic downcast mutations are rejected.
- Real Behavior Actors integration now executes an inferred `Machine` directly
  and an inferred `Stash<Machine<...>>` composition through the same generic
  Driver environment. Neither test names the nested wrapper type or introduces
  template-specific Engine code. The Stash transcript is asserted according to
  the wrapper's own re-evaluated route semantics, proving Engine does not
  reinterpret held messages. These are concrete positive cells, not yet a
  claim that either template's complete boundary/composition matrix is closed.
- A deeper inferred `Deadline<Stash<Machine<...>>>` value now executes through
  that identical environment. Its outer `DeadlineEvent` delegates ordinary
  user input across both wrappers, its initialization action crosses before
  input, a stale timer generation remains a continuing ordinary event, and a
  matching typed elapsed event stops outside the base Machine without admitting
  the queued post-stop user event. This proves an event-sum and
  named-action-changing wrapper requires no Driver branch or named stack alias;
  the complete Deadline matrix remains planned until its action contents and
  remaining boundaries and orders are executed.
- The Deadline/StopOnShutdown composition edge now runs in both meaningful
  wrapper orders. Each test derives the final nested shutdown and user events
  through Behavior's `EventInput`/`UserEvent` contracts from the inferred value;
  neither names the nested sum. Shutdown commits one final stop action, prevents
  a queued user fold, and retires once in both orders through the same Driver.
- `FinalizeOnShutdown` now runs over a custom sending Behavior through a generic
  `Sends = Vec<u64>` environment. The environment observes the exact transcript
  `[initialization: 1, finalization: 99]`; the wrapper's final fold runs once,
  its typed final actions survive the forced stop verdict, queued post-stop user
  input is not folded, and retirement occurs once. No Driver or environment
  branch names the template.
- The relative-time wrappers now have real inferred Driver paths:
  `ReceiveTimeout` ignores a stale generation and stops on its live generation;
  `OneShot` likewise accepts its generation exactly once before stopping; and
  `Periodic` accepts generation zero, treats its duplicate as stale, rearms to
  generation one, then stops. Each queues a later event proving terminal fusion,
  applies every accepted wrapper decision once, and retires once. Typed events
  are constructed through `EventInput`, not nested event-type aliases.
- `Lease` now runs its full ordinary acquire/reject/renew/stale/release/reacquire/
  expire transcript through the Driver. A generic environment constrained only
  by `B::Sends = LeaseSends<LeaseReply>` records the typed factual outcomes and
  schedule generations `[0, 1, 2]`; it performs no lease transition. Source
  exhaustion returns `Completion::Exhausted` and retires once after every event
  action has crossed. Behavior alone owns occupancy and generation state.
- `Watch` now runs through an environment constrained only by
  `B::Sends = WatchSends<MailAddr, Vec<u64>>`. Initialization emits exactly one
  typed `ObservePeer` request for the selected peer. A normal result continues,
  an unrelated abnormal result remains inert after lossless inner routing, and
  an abnormal result for the selected peer commits its terminal action and
  fuses the Driver before a queued user event. The environment performs no
  observation or reaction state transition; it records the named lanes and
  supplies factual `PeerStopped` inputs.
- `Task` now runs both terminal commands through one static delivery
  environment. Completion moves a non-`Clone` boxed result into exactly one
  typed `TaskResult::Completed` delivery; cancellation emits the distinct
  `TaskResult::Cancelled` fact. Each terminal action crosses after the empty
  initialization action, each queued contradictory command remains unread,
  and each Driver retires exactly once. The Driver knows neither task state nor
  terminal-result semantics.
- A reusable test-only `CaptureEnvironment<B>` now accepts and retains the
  complete `ActionsOf<B>` without naming or projecting any send product. The
  first consumer, `Latch`, proves empty initialization, below-threshold silence,
  ordered threshold release, immediate late release, the zero-count boundary,
  source exhaustion, and one retirement through the unchanged Driver. This
  reduces template-test boilerplate without introducing production protocol or
  interpretation machinery.
- `Configuration` now exercises its complete ordinary Driver-facing command
  sum: unconfigured query, first acceptance, identical-version idempotence, and
  configured query all cross as complete actions before exhaustion. Separate
  executions prove stale and conflicting candidates return their owned strings
  through exact `DriverError::Behavior` variants, commit no fabricated action,
  fuse queued work, and retire once. The capture environment only retains typed
  actions; Behavior owns versioning and atomic state decisions.
- The named `Features` specialization separately runs as its inferred concrete
  Behavior through that same capture environment. Its duplicate feature input
  normalizes to one ordered explicit status per identity, then the configured
  typed state crosses in a query delivery before exhaustion and retirement;
  neither Driver nor environment knows feature policy.
- `Barrier` now has two Driver transcripts. The successful path rejects empty
  and duplicate definitions before ownership enters Driver, then preserves two
  generations of arrival-order releases and exhausts its event source. The
  rejection path separately proves unknown participant, future generation,
  duplicate arrival, and stale generation errors remain exact
  `DriverError::Behavior` values; each commits no error action, fuses queued
  work, and retires once. The owner suite remains the executable `u64::MAX`
  rollover oracle because public construction deliberately exposes no way to
  fabricate an arbitrary internal generation; that boundary is not falsely
  claimed as a Driver-reachable input.
- `Cache` now rejects zero capacity before Driver ownership, then runs every
  command variant through one inferred Driver: insertion, recency-refreshing
  hit, capacity eviction, replacement, miss, removal, and repeated absent
  removal. The captured typed results preserve every displaced `String` and
  complete evicted entry in exact action order before source exhaustion and one
  retirement. Cache requires `V: Clone` for hits by its own algebra, so this
  row does not falsely characterize its value type as non-`Clone`; Driver adds
  no such bound.
- `Registry` now runs its entire public command sum through the generic Driver.
  One transcript proves missing lookup, bind, found lookup with the exact typed
  recipient, unbind, and missing-after-unbind in complete action order. Three
  independent executions preserve owned string keys in exact `AlreadyBound`,
  `StaleBinding`, and `NotBound` errors, commit no fabricated rejection action,
  fuse queued mutations, and retire once. This is a pure Behavior registry;
  neither the Driver nor its capture environment acquires dynamic lookup or
  routing authority.
- `Resolver` now proves its duplicate-key constructor rejection leaves the
  borrowed definition owned by the caller, then runs both found and missing
  typed lookup facts through one inferred Driver until source exhaustion and
  one retirement. Its protocol contains no mutation command, while the generic
  capture environment merely receives deliveries; Engine gains neither
  resolver knowledge nor registry authority.
- `Topic` now proves idempotent insertion-ordered membership, ordered snapshot
  delivery, unsubscription, and later publication through the direct Driver.
  A separate empty-topic execution returns the owned `String` in exact
  `TopicError::NoSubscribers`, commits no action, fuses queued subscription,
  and retires once.
- `PubSub` runs the corresponding keyed protocol without a Driver change:
  duplicate subscription stays idempotent, snapshot deliveries retain order,
  unsubscription affects the next publication, and exact `UnknownTopic`,
  `NotSubscribed`, known-empty `NoSubscribers`, and unknown-topic
  `NoSubscribers` errors preserve their topic and publication ownership while
  fusing later work. Topic retention and membership semantics remain pure
  Behavior policy; the capture environment only receives typed deliveries.
- `Presence` now crosses its complete named `replies` and `schedules` product
  through the same generic capture environment. One causal transcript proves
  announcement, idempotence without rescheduling, conflicting and stale
  evidence, timer collision, refresh, unknown and stale elapsed facts, present
  report, matching expiry, retained tombstone report, revival, generations
  `0..=2`, source exhaustion, and one retirement. The private
  `TimerGeneration(u64::MAX)` injection needed for the exhaustion boundary
  remains in the Behavior owner suite; public construction cannot truthfully
  manufacture it for a Driver input. Engine interprets neither presence state
  nor clock semantics.
- `Acknowledgements` now runs one exhaustive 15-command lifecycle through the
  direct Driver. It covers empty immediate completion, duplicate key, unknown
  acknowledge/cancel, participant normalization, unexpected participant,
  intermediate acknowledgement, duplicate acknowledgement, completion,
  operations after completion, cancellation, and operations after cancellation.
  Every accepted or rejected fact is one exact typed delivery and the source
  exhausts with one retirement. These terminal phases are retained Behavior
  domain state—not Driver termination—and Engine adds no retry, quorum, or
  delivery-completion semantics.
- `Sequencer` now runs two direct-Driver transcripts with non-`Clone`
  `Box<u64>` payloads. The ordinary path buffers a future position, returns a
  duplicate value without replacement, releases the missing and buffered
  values in exact increasing order across the named `deliveries` lane, and
  returns a stale value with the factual next position through `outcomes`. The
  boundary path begins at `Sequence(u64::MAX)`, delivers it once, enters domain
  exhaustion without wrapping, rejects the next owned value, then lets the
  Driver end only on source exhaustion. Engine performs no sequencing,
  buffering, or reordering.
- `OrderGate` now runs all operation outcomes through one direct Driver using
  non-`Clone` `Box<u64>` values. It retains out-of-order keys, returns duplicate
  ownership, opens an inclusive watermark, releases held values in key order,
  immediately delivers a value at an already-open key, rejects a stale opening,
  and releases the remaining value on a later opening. Both named action lanes
  cross intact before source exhaustion and one retirement; Engine owns no
  watermark, holding buffer, or ordering policy.
- `Deduplicator` now runs its bounded FIFO retention policy through one direct
  Driver with non-`Clone` `Box<u64>` payloads. The transcript proves first-seen
  delivery, exact duplicate ownership return without retention refresh, full
  capacity, oldest-key eviction, and later readmission of an evicted key. Zero
  capacity is rejected before driving; both named lanes cross intact before
  source exhaustion and one retirement. Engine owns no idempotency or retention
  policy.
- `Correlator` now runs successful resolution and cancellation plus controlled
  duplicate, unknown, stale-completed, and stale-cancelled failures through the
  direct Driver. Non-`Clone` `Box<u64>` results and rejected replies preserve
  ownership, successful action prefixes remain exactly committed before a later
  behavior failure, and every terminal path retires once. Engine owns no
  correlation lifecycle, retention, or reply semantics.
- `Buffer` now runs all three exhaustive overflow policies through independent
  direct Drivers with non-`Clone` `Box<u64>` values. The transcripts prove
  bounded acceptance, FIFO release, empty release, explicit full/newest
  rejection, oldest eviction followed by acceptance, and exact ownership of
  every offered value across both named lanes. Zero capacity is rejected before
  ownership transfer; Engine owns no buffering, admission, or overflow policy.
- `WorkQueue` now runs duplicate availability, idempotent withdrawal, immediate
  dispatch, bounded FIFO waiting, full rejection with non-`Clone` payload
  return, waiting release, and later immediate dispatch through one direct
  Driver. Exact worker recipients and queue-depth facts cross both named lanes;
  Engine owns no worker selection, admission, or backpressure policy.
- `PriorityQueue` now runs positive construction, bounded acceptance, explicit
  full rejection with non-`Clone` payload return, greater-priority release,
  stable FIFO ties, complete draining, and empty release through one direct
  Driver. The private maximum insertion-token setup remains covered by the
  Behavior owner suite because public construction cannot manufacture it;
  Engine owns no priority, stability, or queue policy.
- `CircuitBreaker` now runs the complete public closed/busy/open/probing/reopen/
  recovered lifecycle through one direct Driver. It proves stale attempts and
  mismatched timer evidence are inert, consecutive failures and both reset
  generations remain exact, schedule and reply lanes cross together, and
  successful probe recovery restores ordinary admission. Private numeric
  exhaustion setup remains in the Behavior owner suite; Engine owns no retry,
  clock, protected-operation, or breaker-phase policy.
- `RateLimiter` now runs invalid initialization, successful token consumption,
  insufficient-token and over-capacity rejection with non-`Clone` payload
  return, saturating `u64::MAX` refill, full-bucket consumption, and later
  refill/re-admission through one direct Driver. Both named lanes and every
  remaining-token fact cross intact; Engine owns no clock cadence, token, or
  admission policy.
- `Health` now runs empty and populated reports, idempotent evidence, worst-
  status aggregation, removal tombstones, later higher-version resurrection,
  stale rejection, and same-version conflict through direct Drivers. Reports
  preserve configuration order and controlled failures preserve the exact
  committed prefix before one retirement; Engine owns no health aggregation,
  export, or version policy.
- `Readiness` now runs normalized fixed membership, unknown and partially-ready
  reports, idempotent observations, version advancement to complete readiness,
  empty-set readiness, and exact stale/conflicting/unknown controlled failures
  through direct Drivers. Each error preserves the factual committed prefix and
  retires once; Engine owns no dependency membership, admission, or export
  policy.
- `Workflow` now validates every construction rejection and runs successful,
  failed, and ready-cancelled lifecycles through direct Drivers. The transcripts
  preserve root and dependent activation order, blocked/unknown/duplicate input
  rejection, final success, explicit failure, cancellation, and terminal facts;
  domain terminal state remains available until source exhaustion and Engine
  owns no graph, participant execution, saga, or persistence policy.
- `Router<RoundRobin>` now runs deduplicated membership, cursor rotation,
  removal repair, idempotent add/remove, exact recipient selection, and owned
  empty-membership rejection through direct Drivers. `Router<Broadcast>` runs
  the same membership boundaries while preserving one cloned delivery per
  recipient in insertion order. Engine owns neither membership nor routing
  strategy and only commits the complete ordinary delivery vector.
- `Router<LeastLoaded>` now runs unknown-load exclusion, idempotent observations,
  membership-order tie breaking, newer lower-load selection, removal evidence
  disposal, newly observed membership, and exact routing through a direct
  Driver. No-evidence routing plus stale, conflicting, and unknown-recipient
  observations terminate with typed owned errors and factual committed
  prefixes; Engine owns no load gathering or selection policy.
- `Router<ConsistentHash>` and `Router<RendezvousHash>` now route generated key
  sets twice around a member removal through direct Drivers. Both prove stable
  deterministic selection and that keys not owned by the removed member do not
  move; concrete values and recipients cross intact. Rendezvous additionally
  proves same-version token conflict and its exact committed prefix. Engine
  owns no key hashing, member-token evidence, ring, or highest-weight policy.
- `Proxy` now runs initial installation, stale and matching creation results,
  running-only forwarding, queued replacement, exact child-stop provenance,
  fresh replacement creation, stale stop rejection, creation rejection, vacant
  behavior, and provenance-preserving retry through one direct Driver using
  non-`Clone` child definitions and payloads. Every create and named send lane
  crosses intact; Engine interprets no proxy or incarnation semantics.
- `Supervisor` now runs a custom statically typed inner Behavior through direct
  Driver initialization and continuation. Two configured non-`Clone` workers
  emerge as ordered proxy births with exact child-observation actions, and a
  worker-stop fact emerges as one typed one-for-one replacement command to the
  correct proxy. Engine owns no topology, restart eligibility, budget, or
  supervision policy.
- `WorkerPool` and `KeyedWorkerPool` now reject empty topology (and the ordinary
  pool rejects duplicate nonces) before ownership, then run validated two-slot
  initialization through direct Drivers. Both preserve ordered proxy births and
  exact child-observation actions for non-`Clone` worker definitions before
  source exhaustion and one retirement. Engine owns no backlog, affinity,
  dispatch, interruption, or pool supervision policy.
- Turn-order evidence now names and kills the exact event-before-initial-commit,
  next-before-turn-commit, early-second-input, and synchronous-self-reentry
  mutations for D-TURN-3, D-TURN-4, D-TURN-8, and D-TURN-9 respectively.
- Maximum active-fold instrumentation proves one synchronous fold at a time;
  a pending commitment prevents every later fold. Local commitment does not
  wait for external completion, and capability results re-enter only as later
  closed events. Structural mutations adding spawn/yield interleaving or an
  async Behavior fold are rejected.
- Driver action evidence now proves one complete commit per successful
  decision, exact lane and move-only payload preservation, local-only success,
  final stop action survival, terminal interpretation failure, factual partial
  prefixes, no implicit retry, and no state rollback after a successful fold.
  Creation precedence and creation-result scoping remain concrete-environment
  obligations and have not been pulled into Driver; a typed conformance
  environment now proves ordered creation-before-send commitment and exact
  same-action result alignment as a later event.
- Capability conformance now compile-rejects an environment missing the exact
  `Environment<B>` contract, runs one Behavior against two distinct concrete
  environments, and preserves disjoint exact Behavior/environment errors.
  Structural gates require one coherent `E` and reject ambient authority,
  dynamic registries, erased capability maps, and concrete-environment coupling.
- Identity and scheduling gates keep address/incarnation, scheduler, and mailbox
  policy out of Driver. Pending dependencies remain cooperatively resumable,
  source-selected event order is preserved exactly, and a deliberate event
  reorder mutation is rejected.
- Generated sequences match a deterministic reference model and replay
  exactly across empty, singleton, limit, stop, and post-stop boundaries.
- The exact selected Behavior Actors checkout's complete owner suite passes:
  84 template/unit tests, 39 algebra/composition tests, and 6 mutation-contract
  tests under `cargo test --manifest-path
  ../bombay-behavior/crates/actors/Cargo.toml --all-targets`. This proves the
  current templates remain pure closed Behaviors and their own algebra is
  sound; it does not by itself close the still-planned Driver runtime matrix.

The 2026-08-16 refresh to Actors revision
`40b39b2605416e3b88427e3289c4dac4568c78e0` was re-audited against
`docs/driver.md`, `docs/adapter-contract.md`, the complete public exports, and
the clean owner test suite. Engine now consumes `B` directly through
`Activate::initialize`; `Compose` is only an inferred wrapper-extension trait
and is removed from the actor-template inventory. The inventory therefore has
43 concrete template/strategy rows. The owner documentation's generic,
monomorphized, one-event/one-fold/one-complete-action boundary agrees with the
Driver. Its schematic references to `Send + 'static`, panic poisoning, and
mailbox closure as an error are not adopted: executor bounds and panic/
cancellation classification remain outside the consuming Driver, while the
owner-approved `Completion::{Stopped, Exhausted}` preserves input exhaustion
as a distinct successful fact.
- Compile fixtures prove `Send`-but-not-`Sync` Behavior and non-`Send` local
  environments pass, while non-Behavior inputs and lifecycle controls fail.
- The isolated allocation oracle reports zero allocations for construction,
  initialization, one turn, both commits, stop, and retirement.
- Production mutation testing found four Driver mutations: both viable
  stop-guard inversions were killed and both whole-function replacements were
  unviable. Thirteen deliberate causal inversions and structural authority/API
  inversions are also killed by explicit oracles.
- The final standalone documentation audit removed current/runnable claims for
  the superseded Bombay facade and its `Compose::new`/`RunExit` examples.
  `README.md`, `docs/cookbook.md`, `docs/module-boundaries.md`,
  `docs/minimal-lifecycle-ownership.md`, `docs/runtime-blocks.md`,
  `docs/adversarial-verification.md`, and `docs/coverage-map.md` now distinguish
  the implemented Driver from the target later Bombay adapter. Historical API
  descriptions remain only as explicitly historical ledger evidence or as
  negative conformance terms.
- Miri passes the inversion suite and the property/model suite with
  `MIRIFLAGS=-Zmiri-disable-isolation PROPTEST_CASES=16`.
- The standalone `causal_turns` libFuzzer target passes 1,000 ASan-instrumented
  runs. The one-turn Criterion path measured approximately 7.85 ns on this
  audit machine. Driver coverage is 100% of functions and was 85.25% of lines
  before the subsequently added initialization-commit failure branch oracle.

The 74-row law manifest and 43-row Actors-template inventory are present and their
consistency/unknown-evidence gates pass. A dedicated falsification oracle now
rejects missing, duplicate, renamed, reordered, unknown, and unexecuted law
rows rather than relying only on the real manifest's happy path. The 61
standalone Engine rows (`D-BEH`, `D-TURN`, `D-ACT`, `D-CAP`, `D-ID`,
`D-SCHED`, `D-TERM`, `D-API`, and `D-DEP`) now carry `passing` status after
their positive, boundary, ownership, compile, and inversion commands ran. The
deterministic full causal transcript and its algorithm mutations also close
`D-VER-1`. A targeted structural/mutation gate proves observation has no
Driver control surface and closes `D-VER-5`, for 63 passing rows total. The
execution gate intentionally remains red and now enumerates exactly the six
`D-INC` and remaining five `D-VER` rows
still marked `planned`. The owner explicitly deferred
adapting Bombay until after the best standalone Driver is fixed; therefore the
incarnation-publication and exhaustive template-runtime cells are not being
fabricated as Driver-only evidence. DR1 remains `active` until those rows are
actually executed and no `planned` status remains.

Manifest evidence names are now checked against the actual Engine test source,
so deleting or renaming a positive, killer, negative, boundary, adversarial, or
template oracle makes canonical validation fail. The ordinary exact row command
`cargo test -p bombay-engine` is green for implemented evidence. The two
all-evidence assertions are explicit ignored completion gates, invoked with
`cargo test -p bombay-engine --test law_manifest -- --ignored`; that command
remains intentionally red until every law and template status is `passing`.
The template inventory gate additionally rejects owner-suite placeholders:
every exported Actors row must name one or more concrete
`behavior_actors::...` Driver tests, and every reference plus the canonical
inventory killer must still exist in source. This prevents a complete-looking
43-row inventory from passing merely because the upstream crate has tests of
its own. The check is bidirectional: all 49 executable Tokio tests in the
Behavior Actors Driver suite must also be attributed by a manifest row, so a
new test cannot silently sit outside the reviewed template inventory.

The standalone Driver phase is now at its explicit integration boundary. The
minimal `bombay::core::Incarnation` closes `D-INC-4` and `D-INC-5`, and supplies
real partial evidence for `D-INC-2` and `D-INC-3`. Those two remain planned
until an exact-generation owner proves replacement, address release, and
terminal publication. `D-INC-1` and `D-INC-6` remain wholly owned by later
transactional construction/publication;
`D-VER-2`, `D-VER-3`, `D-VER-4`, `D-VER-6`, and `D-VER-7` require those real
adapters plus exhaustive template and repository execution. A test-only fake
incarnation would be false evidence and is prohibited. The owner has explicitly
sequenced that Bombay adaptation as later work, so these rows are deferred, not
a blocker on refining the standalone Driver. DR1 remains active and the Driver
is not widened to cross that ownership boundary.

The standalone distillation pass has now been repeated after every current
Behavior Actors export ran through the direct Driver. The production Engine
surface is irreducible at two owned values (`B` and `E`), one inference-friendly
constructor, one consuming `run`, one static three-operation `Environment<B>`
port, `Completion::{Stopped, Exhausted}`, and the disjoint
`DriverError::{Behavior, Environment}` sum. Removing any of these loses a
law-observable ownership or terminal distinction; adding preparation, address
identity, or split lifecycle controls would cross into later-layer authority.
The only `B: Behavior` bound on construction
prevents meaningless Driver values, and the run-time refinement
`B: Behavior<Ph = Never>, E: Environment<B>` is required by the closed direct
execution contract. No `Clone`, `Send`, `Sync`, `'static`, allocator, or erased
dispatch bound remains.

The repository-wide bypass scan finds no compiled compatibility adapter.
`bombay-engine` contains no Transition, Machine Executor, prepared lifecycle,
compatibility loop, or template-specific execution path. The former Bombay
runtime, mailbox, routing, facade, examples, benchmarks, fuzz target, and
compatibility tests were removed rather than retained as a second path. Old
API names remain only in explicitly historical ledger evidence or negative
structural test fixtures.

The latest standalone verification run used the pinned development shell and
passed `cargo fmt --all -- --check`, `cargo build -p bombay-engine
--all-targets`, `cargo test -p bombay-engine`, `cargo clippy -p bombay-engine
--all-targets -- -D warnings`, `cargo bench -p bombay-engine --no-run`, the
trybuild pass/fail fixtures, documentation tests, and `git diff --check`. The
separate fuzz workspace also passes `cargo check --manifest-path
crates/bombay-engine/fuzz/Cargo.toml`; its libFuzzer binary completed 10,000
structured causal-turn inputs through direct `cargo run --release` invocation.
The Criterion Driver benchmark also executed successfully in quick mode at
approximately 11.7 ns for initialize, commit, one turn, terminal commit, and
retirement on this machine. `cargo-mutants 27.0.0` generated three
production mutants: both viable stop-guard inversions were killed and the
whole-function default-result replacement was unviable because the exact
result vocabulary intentionally has no `Default` implementation; there were
zero surviving mutants. The pinned shell currently lacks the `cargo-fuzz`
wrapper and Miri; the fuzz binary was therefore run directly, while Miri
remains explicitly unexecuted rather than being inferred from successful
compilation.

The 2026-08-16 composition-contract recheck inspected the current clean Actors
source plus `docs/driver.md`, `docs/adapter-contract.md`, and
`docs/actor-catalogue.md`. `Compose` permits generic wrapper transformations
with conditional associated-type bounds, but the owner documents do not
publish the finite reviewed supported/rejected edge graph required by
`driver-test-strategy.md`. They state only that unsupported compositions fail
to compile. Engine therefore cannot truthfully invent all edges, absent edges,
depth-two/depth-three paths, or maximal stacks for `D-VER-3`. The 49 concrete
template tests are now accounted for bidirectionally by the manifest, but they
do not substitute for that missing owner contract.

DR1 is consequently blocked from final completion by two exact conditions:
(1) later exact-generation transactional construction and publication is not
yet designed, leaving `D-INC-1`, `D-INC-2`, `D-INC-3`, and `D-INC-6` open; and
(2) Behavior Actors does not yet specify the finite
composition graph needed for exhaustive `D-VER-3` evidence. These conditions
have persisted through repeated completion audits. The standalone Driver must
not gain incarnation authority or guessed template semantics to bypass them.

Repository history confirms there is no available revision that satisfies
both sides implicitly. The 0.10.0 release at
`4c756f5a6e8bf6aacfe6435250b95fb9fdcab985` still used the former positional
product/path API and predates the separate Actors catalogue. Commit
`58b1230` deliberately removes that positional product API, and commit
`52900f3` subsequently splits the current reusable catalogue into
`bombay-behavior-actors`; the current 0.12.0 head contains the complete
catalogue required by the Driver test strategy and named semantic send
products. The selected architecture is the current 0.12.0 Actors catalogue
over the current 0.12.0 Behavior core; the historical 0.10.0 product API is
not a compatibility target.

## Working rules

For every feature:

1. Inspect the exact locked source, public API, tests, and documentation for
   Bombay Behavior, Observe, Timers, Communication, and Address; also inspect
   Transition and Machine Executor when the execution path is affected.
2. Record the feature-specific verification and ownership map here before
   moving the item from `blocked` to `active` or changing implementation.
3. State the observable invariant and add an inversion test that proves it.
4. Compose existing typed primitives. Do not add `dyn`, `Any`, downcasts,
   parallel spawn paths, runtime policy, or a duplicate Behavior protocol.
5. Before completion, try to remove every added type, trait, alias, field,
   task, channel, adapter, and public export that lacks an independent
   invariant.
6. Reconcile this ledger, run workspace tests, rustfmt, Clippy, and
   rustdoc, then use `feature-complete`. Use `distilled` only after the final
   project-wide ownership and interface audit. Never use `done`.

Priority is `P0` through `P3`; points are relative size. States are
`unblocked`, `active`, `blocked`, `feature-complete`, and `distilled`.
`Blocked by` and `Unblocks` are inverse edges until the prerequisite is
complete. Repair any disagreement before selecting work. Select from the
dependency graph, not table order. An item blocked only by its own fresh
cross-crate verification is eligible for verification, not implementation.

## Current dependency graph

All runtime-foundation and engine-path items are distilled: A1-A4, A6,
B1-B8, C1-C3, C6-C8, D1-D5, F1, V1, X1-X7. Their evidence is historical and
must not be treated as a waiver of per-feature verification.

| ID | Work | Priority | Points | State | Blocked by | Unblocks |
|---|---|---:|---:|---|---|---|
| EP1 | Affine prepared/active Engine environment port and real-runtime conformance | P0 | 13 | feature-complete | — | CR2 |
| CR2 | Rebuild minimal Bombay core over EP1 | P0 | 8 | feature-complete | — | ML1 |
| ML1 | Minimum typed local mailbox/address environment | P0 | 5 | distilled | — | LR1 |
| LR1 | Tokio launch and typed live-reference boundary | P0 | 5 | distilled | — | future terminal-control layer |
| CR1 | Minimal core incarnation above Driver | P0 | 3 | distilled | — | DR1 |
| DR1 | Complete universal direct-Behavior Driver law | P0 | 13 | distilled | — | — |
| A5 | Executor conformance beyond Tokio | P2 | 3 | blocked | C5; a second executor | M2 |
| C4 | Hung-fold detection capability | P3 | 8 | blocked | fresh verification; concrete policy owner and oracle | operations policy |
| C5 | Additional router/clock implementations | P3 | 8 | blocked | fresh verification; concrete second consumers | A5, P1 |
| F2 | General control-protocol decision | P2 | 5 | blocked | concrete second priority protocol | M2 |
| F3 | Typed reply-to and timeout composition | P1 | 5 | feature-complete | — | M2 |
| F4 | Backpressured stream-ingestion adapter | P1 | 8 | blocked | fresh verification; `mnesis-bombay` Phases 0-4 | M2 |
| F5 | Typed heterogeneous fan-in decision | P2 | 5 | blocked | concrete fan-in invariant | M2 |
| F6 | Replacement-incarnation observation protocol | P1 | 5 | feature-complete | — | M2 |
| F7 | Transactional root activation | P0 | 5 | feature-complete | — | M2 |
| R1 | Supersede legacy Bombay with this runtime and publish renamed crates | P0 | 5 | feature-complete | — | M2 |
| L1 | Worker-pool and key-persistent routing library | P2 | 8 | blocked | concrete library consumer | M3 |
| L2 | Typed local pub/sub boundary | P2 | 8 | blocked | concrete group consumer; Zenoh boundary | M3 |
| L3 | Intake stashing and typed admission refusal | P1 | 5 | feature-complete | — | M3 |
| L4 | Graceful in-flight worker draining | P1 | 8 | feature-complete | — | M3 |
| L5 | Retry scheduling and backoff composition | P1 | 5 | feature-complete | — | M3 |
| L6 | Queryable application reporting | P1 | 5 | feature-complete | — | M3 |
| L7 | Reusable scheduler-library decision | P2 | 5 | blocked | invariant beyond Behavior timers | M3 |
| AF0 | Comparative actor-system feature research and canonical feature table | P0 | 8 | blocked | fresh Akka, Erlang/OTP, Kameo, and complete local cross-crate verification | S1, E1, E2, E3, E4, E5, E6, E7, E8, E9 |
| E1 | Facade builders and authoring macros | P1 | 8 | blocked | AF0 | E9 |
| E2 | Static type-inference diagnostics | P1 | 5 | blocked | AF0 | E9 |
| E3 | Cookbook and public usage guidance | P1 | 8 | blocked | AF0 | E9 |
| E4 | First-class actor definition and one-value spawn | P0 | 3 | blocked | AF0 | E9 |
| E5 | Bombay-owned application routing | P0 | 8 | blocked | AF0 | E9 |
| E6 | Concise nominal behavior authoring | P1 | 5 | blocked | AF0 | E9 |
| E7 | Live-actor capability and terminal-result ergonomics | P1 | 5 | blocked | AF0 | E9 |
| E8 | Facade, prelude, and system-construction coherence | P1 | 3 | blocked | AF0 | E9 |
| E9 | Expressive reference-application distillation | P0 | 8 | blocked | AF0, E1, E2, E3, E4, E5, E6, E7, E8 | M4 |
| P1 | Single-threaded/no-std portability decision | P3 | 8 | blocked | C5; concrete embedded consumer | M2 |
| Q1 | Reproducible CI and build matrix | P0 | 5 | feature-complete | — | M5 |
| Q2 | Coverage and integration breadth | P1 | 8 | feature-complete | — | M5 |
| Q3 | Concurrency, property, fuzz, and Miri gates | P1 | 13 | feature-complete | — | M5 |
| Q4 | Mutation-testing gates | P1 | 8 | feature-complete | — | M5 |
| Q5 | Allocation and competitive benchmark gates | P0 | 13 | feature-complete | — | M5 |
| Q6 | Doctest and panic-mode gates | P1 | 5 | feature-complete | — | M5 |
| Q7 | Lifecycle observation ordering contract | P0 | 3 | feature-complete | — | M5 |
| S1 | Comparative actor-model and composition synthesis | P0 | 13 | blocked | AF0 | M5 |
| M2 | Runtime-operations milestone distillation | P1 | 8 | blocked | A5, F2, F4, F5, P1 | FV1 |
| M3 | Optional-library milestone distillation | P2 | 8 | blocked | L1, L2, L7 | optional release |
| M4 | Developer-experience milestone distillation | P1 | 8 | blocked | E9 | FV1 |
| M5 | Competitive-verification milestone distillation | P1 | 13 | blocked | S1 | FV1 |
| FV1 | Competitive local-framework release audit | P1 | 13 | blocked | M2, M4, M5 | framework release |
| K1 | KERI identity/authority handoff | P2 | 8 | blocked | bombay/KERI integration repository and ledger | K2 |
| K2 | Zenoh transport/discovery handoff | P2 | 13 | blocked | K1; owning integration repository and ledger | K3 |
| K3 | Authenticated remote-actor handoff | P2 | 13 | blocked | K2; owning integration repository and ledger | remote framework |

### Actors catalogue and runtime-capability cross-reference — 2026-08-15

The new `bombay-behavior-actors` implementation inventory and its
[`runtime-backed actor capability record`](../../bombay-behavior/docs/runtime-backed-actors.md)
were compared with every unfinished item above. This is a scope and evidence
cross-reference, not a per-feature verification waiver. No ledger state changes
are justified by the documentation record alone: `feature-complete` still
requires the exact locked dependency audit, complete Bombay Driver/System path,
inversion tests, workspace gates, and feature-specific ownership record.

| Ledger item | Evidence supplied by Actors/runtime-capability work | Remaining condition | Result |
|---|---|---|---|
| `L1` worker-pool and key-persistent routing library | Concrete `WorkerPool`, `KeyedWorkerPool`, stable assignment/affinity policy, round-robin, broadcast, least-loaded, consistent-hash and rendezvous-hash router templates now exist with unit/model/property/fuzz coverage | The ledger's concrete library consumer remains absent; Bombay must align the Actors version and prove every pool/router lane through the current runtime | implementation body present; still blocked |
| `L2` typed local pub/sub boundary | Concrete typed `Topic`, keyed `PubSub`, `Registry`, `Resolver`, and `Presence` templates now exist with deterministic membership and ownership-preserving rejection semantics | The concrete group consumer and Zenoh boundary remain unresolved; lifecycle-backed subscription cleanup and the Bombay façade path are not proven | implementation body present; still blocked |
| `L7` reusable scheduler-library decision | The capability record separates pure timer policies from the existing `bombay-timers` mechanism and rejects a second clock/queue in Actors | A feature-specific decision record must prove whether any invariant beyond Behavior timer policies exists; the current row's blocker is not automatically discharged | decision evidence added; still blocked |
| `AF0` comparative actor-system feature research | The Behavior catalogue now supplies a broad actor-role inventory, ownership classification, implementation inventory, façade map, and runtime-backed capability backlog; the runtime record adds dependency order and an end-to-end admission gate; the existing AF0 progress record already traces the Engine and current local crates | AF0 still requires row-level primary-source comparison and negative evidence, explicit Engine black-box decisions and inversion oracles, the complete supported/rejected typed composition matrix, and the replacement implementation graph | materially advanced; still blocked |
| `S1` comparative actor-model and composition synthesis | The catalogue and capability record provide candidate synthesis inputs and prevent runtime facilities from being counted as pure templates | `S1` remains downstream of completed AF0 and requires its own research synthesis | evidence only; still blocked |
| `C4` hung-fold detection | Operations templates (`Health`, `Readiness`, configuration and features) clarify that reporting policy can be pure while elapsed-time detection is runtime-owned | No concrete hung-fold policy owner, watchdog capability, or independent oracle has been established | no completion |
| `F2` general control-protocol decision | Actors now contains several concrete typed service protocols for timers, observation, creation results and shutdown | Their existence does not prove that another Communication priority lane or generalized control envelope is required | no completion |
| `F4` backpressured stream ingestion and `F5` heterogeneous fan-in | The runtime record assigns materialization, demand, cancellation and physical backpressure to a future stream subsystem and keeps fan-in policy in Actors | No complete stream consumer, demand law, ingestion adapter, or heterogeneous fan-in invariant exists | no completion |
| `E1`–`E9` developer experience | The catalogue defines the reusable role vocabulary that the façade must eventually expose; `Compose` and `#[behavior]` supply component-level authoring mechanisms | Bombay does not yet depend on/re-export Actors, hide interpreter products, provide the complete actor-tree authoring path, or demonstrate the reference application | prerequisite evidence only; all remain blocked by AF0 |
| `M3` optional-library milestone | `L1` and `L2` have concrete template implementations and `L7` has a clearer ownership decision boundary | All three prerequisite rows remain blocked under their stated gates | no milestone completion |
| `A5`, `C5`, `P1`, `M2`, `M4`, `M5`, `FV1`, `K1`–`K3` | The capability record classifies their executor, portability, milestone, identity, transport and remote-actor ownership boundaries | It supplies no second executor/clock/router, embedded consumer, completed prerequisites, integration repositories, or authenticated transport implementation | unaffected |

Strict state-transition count from this cross-reference: **0 current ledger
rows**. This is not a count of actor-system features supplied by Actors. The
current graph predates the reusable template catalogue and compresses dozens of
separate capabilities into the umbrella rows `L1`, `L2`, and `L7`. The zero
means only that none of those coarse rows has yet satisfied its complete
runtime-integration and verification gate.

The implemented capability count is materially different:

| Coarse ledger area | Concrete Actors capabilities already implemented | Capability consequence |
|---|---|---|
| Fundamental composition and lifecycle | `Compose`/`Active`, `Machine`, `Stash`, `Watch`, `Task`, deadline/final shutdown policies | The deterministic actor-definition, initialization, state-machine, deferred-message, observation, task-result, and per-actor shutdown halves exist |
| `L1` worker pools and routing | `Proxy`, `Supervisor`, `WorkerPool`, `KeyedWorkerPool`, `Router<RoundRobin>`, `Router<Broadcast>`, `Router<LeastLoaded<_>>`, consistent-hash and rendezvous-hash routers, `WorkQueue`, `PriorityQueue`, `Buffer`, `CircuitBreaker`, `RateLimiter`, `Correlator`, `Acknowledgements`, `Sequencer`, `OrderGate`, and `Deduplicator` | `L1` is not one missing feature: it hides supervision, pools, five routing strategies, admission, resilience, correlation, acknowledgement, ordering, and deduplication capabilities whose pure implementations already exist |
| `L2` local discovery and pub/sub | `Registry`, `Resolver`, `Presence`, `Topic`, and `PubSub` | Typed binding, lookup, versioned presence, ordered topics, keyed subscriptions, and fan-out policy exist; receptionist/group lifecycle and runtime/façade integration remain |
| `L7` and time-backed roles | `Deadline`, `ReceiveTimeout`, `OneShot`, `Periodic`, and expiring `Lease` | Incarnation-local deterministic timer policy exists over `bombay-timers`; retry, heartbeat, debounce/throttle, idle passivation, durable reminders, and the general cancellation lane remain separate work |
| Workflow and coordination | `Workflow`, `Barrier`, and `Latch` | Validated dependency activation, fixed-member synchronization, and threshold release policies exist; durable process-manager and stream materialization capabilities do not |
| Persistence policy | Bounded deterministic `Cache` | Retention policy exists, but it does not complete Mnesis-backed event sourcing, durable state, projections, sagas, inbox/outbox, or recovery hosts |
| Operational actors | `Health`, `Readiness`, `Configuration`, and `Features` | Deterministic aggregation/versioning policy exists; probes, sources, exporters, metrics, tracing, audit, and resource adapters remain runtime work |

The Actors implementation therefore supplies roughly forty concrete reusable
roles or strategy variants even though it changes zero current row states by
itself. Two statements must remain distinct:

1. **Capability implementation:** the deterministic half of many framework
   features is already implemented and independently tested in Actors.
2. **Ledger completion:** a coarse Bombay row becomes `feature-complete` only
   after version alignment, its exact Driver/System interpreters, ordinary
   façade exposure, concrete consumer where required, inversion tests, and
   repository gates are complete.

AF0 must replace the coarse umbrella graph with feature-level implementation
and integration items. The replacement graph must not make all of routing,
pooling, discovery, messaging, and scheduling appear as three indivisible
features. It must create separate reciprocal dependency edges at least for:

```text
Actors template inventory
    |
    +--> supervision/proxy runtime integration
    +--> worker-pool runtime integration
    +--> router and delivery-policy integration
    +--> registry/receptionist integration
    +--> topic/pub-sub integration
    +--> timer-policy integration
    +--> workflow integration
    +--> operations-source/export integration
    +--> entity-host integration
    +--> durable-host integration
    +--> stream subsystem
    +--> transport/cluster subsystems
              |
              v
       Bombay façade and actor-tree authoring
              |
              v
       expressive reference application
```

The new feature-level rows should begin with truthful component status: many
will have their Actors fold marked as implemented while their runtime or façade
half remains blocked. They must not discard that completed work, and they must
not count a pure fold as an end-to-end framework capability.

The previous E1-E9 authoring campaign is not an adequate feature inventory for
the public actor system. Its artifacts remain historical evidence, but all of
those items are reopened and blocked by AF0. AF0 is the only next item and is
eligible for research and verification only. It must study established actor
systems and the complete local dependency/runtime implementation, then produce
the canonical feature table, actor-role inventory, composition matrix,
ownership boundary, and evidence-backed dependency graph. Implementation item
IDs and ordering may be created only from that research result. No engine,
runtime, macro, facade, example, or public-API implementation may start first.

### AF0 required research deliverables

AF0 is one architecture-research feature with three inseparable outputs:

1. **Canonical actor-system and ecosystem feature table.** Compare Akka,
   Erlang/OTP, Kameo, and the current Bombay repositories. Inventory every
   feature exposed by those systems—not only Bombay's current local-runtime
   scope—and list every actor role and reusable capability,
   including ordinary actors, supervisors, proxies, routers, pools, hierarchy,
   watching/linking, timers, shutdown/finalization, restart policy, stashing,
   state machines, actor references, spawning, and terminal observation. For
   each row record user intent, system-owned machinery, observable guarantees,
   failure semantics, authoritative evidence, and eventual classification as
   core runtime, ready actor template, optional library, external integration,
   or explicitly rejected capability. Classification happens after inventory;
   absence from today's Bombay code is not grounds for omission.
2. **Behavior Engine black-box contract.** Audit the current Engine, Behavior,
   Transition, Machine Executor, `Incarnation`, and `ActorEnvironment`. Record
   exactly what one engine run consumes, returns, and owns; where the event
   loop and executor live; where the Tokio task is launched; how cancellation,
   panic, initialization, effect interpretation, and retirement are ordered;
   and which currently public engine types or lifecycle phases are accidental
   implementation surface.
3. **Layer and ownership map.** Draw the complete dependency direction from
   application behavior and actor templates through Behavior composition,
   Engine execution, Bombay environment interpretation, incarnation ownership,
   shared system services, and the final public API. Every mechanism must have
   exactly one owner. The application layer may express domain behavior,
   topology, and policy only; it may not implement interpreter, routing,
   registration, executor, lifecycle-turn, or error-product machinery.

AF0 must also produce a typed composition matrix showing every supported and
rejected permutation of actor roles and capabilities. Only after these tables
are reviewed may the ledger create implementation features for Engine repair,
runtime layering, reusable actor templates, interpreter encapsulation, or the
public authoring API. The public API is necessarily the final implementation
layer.

### AF0 research record — actor-system feature inventory

Research is in progress; this table is the candidate inventory, not an
implementation plan or completion claim. Authoritative inputs currently
include Akka Typed's actor index, lifecycle, supervision, interaction,
router, timer, dispatcher, mailbox, stash, FSM, and coordinated-shutdown
guidance; Erlang/OTP's process, `gen_server`, supervision-tree, supervisor,
link, and monitor contracts; Kameo's current actor, spawn, supervision, link,
pool, pub/sub, stream, mailbox, and lifecycle documentation; and the complete
local Bombay source and dependency graph. Every row must be verified against
all applicable owners before AF0 becomes active.

| Candidate actor-system feature | User supplies | Actor system supplies | Current Bombay evidence/status |
|---|---|---|---|
| Ordinary typed actor | State, message protocol, domain reaction | Mailbox loop, serialization, task, reference, stop | Behavior and Engine exist; application boundary is not yet distilled |
| Actor definition/template | Constructor arguments and policy choices | Reusable recipe for every new incarnation | `Actor<B>` is only an inert value; no complete reusable template inventory |
| Spawn and typed reference | Address and actor template | Allocation, initialization, registration, task launch, live reference | Exists, but public generic bounds expose interpreter wiring |
| Mailbox and backpressure | Capacity/policy choice where relevant | Priority/control lane, bounded delivery, payload recovery | Communication-owned primitive exists and is composed by Bombay |
| Parent/child hierarchy | Child templates and topology | Ownership, start/stop ordering, recursive retirement | Typed births and child leases exist; authoring model needs research |
| Supervisor | Strategy, restart policy, child templates | Failure monitoring, restart decisions, budget, escalation | Behavior supplies a supervisor algebra; application composition is verbose |
| Worker | Domain behavior and construction arguments | Placement under supervisor/pool and incarnation replacement | Worker is a contextual role, not a separate engine type |
| Stable proxy | Worker protocol and replacement policy | Stable endpoint across replaceable worker incarnations | Behavior `Proxy` exists; topology and endpoint wiring leak into applications |
| Router | Routee template/group and selection policy | Forwarding, routee lifecycle, routing strategy | No distilled Bombay actor template; endpoint routing traits are lower-level machinery |
| Worker pool | Worker template, size/keying, policy | Routees/proxies, scheduling, supervision, replacement | Behavior pools exist; Bombay integration and public template remain unproven |
| Nested supervision | Child supervisor templates | Arbitrary typed supervision trees | Algebra appears composable; full permutation matrix is not recorded |
| Watch/monitor | Target reference and domain reaction | Exact-generation terminal notification and cancellation | Behavior/Observe/Bombay primitives exist |
| Link/failure propagation | Linked actors and reaction policy | Bidirectional relationship and failure propagation | No general public link contract; must not be inferred from watch |
| Timers/deadlines | Timer identity, schedule, reaction | Generation replacement, stale rejection, lifecycle cancellation | Behavior and Timers primitives exist; Bombay interprets them |
| Receive timeout | Duration and reaction | Rearming rules tied to actor traffic and incarnation | Behavior wrapper and Bombay oracle exist |
| Request/reply | Request protocol, reply destination, correlation policy | Typed destination and optional convenience pattern | Typed `Recipient<B>` composition exists; no runtime request registry is allowed |
| Stash | Capacity/order and release policy | Buffered protocol handling with deterministic replay | Behavior owns stash algebra; Bombay should add no duplicate stash |
| Finite-state behavior | States and transitions | Standard behavior/state composition | Behavior owns machine/state primitives |
| Graceful shutdown | Domain finalization/drain policy | Priority shutdown delivery, child ordering, terminal retirement | Current wrappers/runtime exist; application ergonomics remain unresolved |
| Coordinated system shutdown | Global phases and participants | System-wide ordered termination | Not established as a local Bombay feature; requires a concrete invariant |
| Lifecycle hooks/signals | Domain reactions only where semantically needed | Start, restart, stop, terminal ordering | Bombay lifecycle reporting exists but is overly public |
| Restarted incarnation | Fresh template plus restart policy | New engine, behavior, mailbox generation, timers, and observations | Supervisor/proxy path exists; exact template ownership needs audit |
| Delivery failure | Domain decision when recoverable | Preserve payload and distinguish unknown/closed/rejected | Typed delivery errors exist; composed runtime errors leak mechanically |
| Registration/discovery | Stable logical address where needed | Exact-generation claim, resolution, and release | Address/Bombay adapters exist; user-facing topology is unresolved |
| Dispatcher/executor selection | Explicit policy only for a proven use case | Scheduling and serialized turns | Engine uses Machine Executor; Tokio task launch is in `Incarnation` |
| Actor testing | Behavior or complete template under test | Synchronous behavior test and asynchronous runtime test facilities | Engine/runtime tests exist; one black-box engine API does not |
| Stream attachment | Stream and backpressure policy | Lifecycle-bound ingestion into actor mailbox | Existing F4 remains blocked by a concrete consumer |
| Stream processing | Sources, transforms, sinks, materialization policy | Backpressure, cancellation, supervision, fan-in/fan-out, lifecycle | Full Akka Streams/Kameo attachment comparison still required |
| Pub/sub | Topics, membership, publication protocol | Typed subscription, fan-out, membership lifecycle, failure handling | Optional L2 candidate exists; complete actor-template contract is unresearched |
| Event bus | Event classification and subscription policy | Local typed/untyped broadcast and lifecycle cleanup | Not inventoried in current Bombay scope |
| Broadcast router | Routees and broadcast policy | One-to-many delivery and aggregate failure semantics | Not implemented as a ready template |
| Round-robin/random/consistent-hash router | Routees and key/selection policy | Standard reusable selection algorithms | Behavior pools cover part of this space; comparative inventory required |
| Scatter-gather / first-response | Request and completion policy | Fan-out, timeout, winner selection, cancellation semantics | No core contract; must be researched |
| Tail chopping / hedged request | Request, delay, retry targets | Staggered requests and first-success selection | No core contract; must be researched |
| Work pulling / balancing pool | Work protocol and capacity | Demand-aware worker scheduling and replacement | Pool scheduling semantics require comparison |
| Throttling / rate limiting | Rate and overflow policy | Admission timing, buffering, refusal | No canonical Bombay template |
| Batching / aggregation | Batch limits and flush policy | Timers, buffering, downstream delivery | Expressible in domain behavior; reusable-template decision unresearched |
| Async result adaptation / pipe-to-self | Future and result mapping | Lifecycle-safe conversion into actor messages | No distilled public facility |
| Receptionist / service discovery | Service key and registration intent | Dynamic typed discovery and listings | Address routing is not a receptionist; capability absent |
| Actor selection / path lookup | Logical path and resolution policy | Hierarchical lookup and failure semantics | Bombay addresses are exact local designations, not paths |
| Cluster membership | Node participation and failure detector policy | Membership, reachability, convergence events | External/distributed scope; inventory still mandatory |
| Cluster-aware routing | Routee discovery and placement policy | Routing across live cluster members | Absent; transport/discovery handoff must be mapped |
| Cluster sharding | Entity identity and allocation/passivation policy | Single logical entity placement, rebalance, buffering | Bombay Entity/Nexus boundary must be researched, not assumed |
| Singleton | Singleton identity and failover policy | Cluster-wide unique active instance | Absent; distributed integration candidate |
| Distributed data / CRDT | Replication and consistency choice | Convergent replicated state | Outside local runtime but mandatory ecosystem inventory |
| Remote deployment | Placement and protocol contract | Remote spawning, serialization, lifecycle/failure mapping | Explicit external integration candidate |
| Remote actors | Remote protocol and authority integration | Transport, discovery, serialization, failure semantics | Explicitly outside local core; K1-K3 handoff |
| Serialization and protocol evolution | Wire schema and compatibility policy | Encoding, manifests, version negotiation, rejection | External boundary; no local-core duplication |
| Persistence | Durable event/state policy | Recovery and durable storage integration | Outside current local runtime; Nexus boundary remains separate |
| Event sourcing | Commands, events, state evolution | Journal, replay, snapshots, recovery lifecycle | Nexus/CQRS boundary requires explicit mapping |
| Durable state | State model and update policy | Durable store, recovery, concurrency semantics | External persistence integration candidate |
| Durable mailbox | Delivery durability and acknowledgment policy | Persistent queue and recovery | Communication currently owns in-memory two-lane mailbox only |
| Delivery guarantees | Idempotency/correlation policy | At-most/at-least/exactly-once mechanisms and failure visibility | Current local delivery is not a distributed guarantee |
| Passivation / idle entity lifecycle | Idle policy and recovery identity | Stop/reactivate buffering and ownership | Entity/sharding integration candidate |
| Scheduled jobs | Job and schedule policy | Durable or ephemeral scheduling, cancellation, ownership | Timers cover incarnation-local scheduling only |
| Coordinated shutdown phases | Named phases, dependencies, tasks | Ordered system termination and timeout handling | Not yet a Bombay system feature |
| Hot code upgrade | State conversion and version policy | Upgrade callback/system-message coordination | Erlang/OTP capability; unsupported unless deliberately designed |
| Runtime introspection | Metadata exposure policy | Actor tree, mailbox depth, status, lifecycle facts | Partial lifecycle facts exist; no coherent introspection API |
| Metrics | Instrument names and cardinality policy | Mailbox, throughput, latency, restart, failure metrics | Kameo comparison and Bombay ownership unresearched |
| Tracing/logging | Context and privacy policy | Actor/message/lifecycle spans and structured events | No canonical runtime integration |
| Dead letters / unhandled messages | Domain policy for observation | Collection, diagnostics, suppression, lifecycle | Bombay exposes typed delivery failures but no dead-letter facility |
| Dispatcher isolation | Workload class and blocking policy | Executor selection, fairness, throughput controls | Tokio is fixed today; portability item A5 remains blocked |
| Mailbox variants | Capacity, priority, fairness, overflow policy | Standard mailbox implementations and selection | Communication owns one two-lane design; full comparison required |
| Test probes and deterministic testkit | Protocol assertions and test policy | Spawnless behavior tests, async probes, virtual time, fishing/event filters | Existing tests are internal; no public testkit inventory |
| Fault injection / chaos testing | Failure scenarios | Controlled crash, delay, partition, and restart observation | Verification capability not inventoried |
| Deployment/configuration | Topology and policy configuration | Validation, defaults, runtime loading | No canonical Bombay configuration layer |

Primary comparative evidence:

- [Akka Typed actors and feature index](https://doc.akka.io/libraries/akka-core/current/typed/index.html)
- [Akka Typed lifecycle and supervision](https://doc.akka.io/libraries/akka-core/current/typed/guide/tutorial_1.html)
- [Akka Typed routers](https://doc.akka.io/libraries/akka-core/current/typed/routers.html)
- [Akka Typed interaction patterns and lifecycle-bound timers](https://doc.akka.io/libraries/akka-core/current/typed/interaction-patterns.html)
- [Akka Streams backpressure contract](https://doc.akka.io/libraries/akka-core/current/stream/stream-flows-and-basics.html)
- [Akka Cluster higher-level tools](https://doc.akka.io/libraries/akka-core/current/typed/cluster.html)
- [Akka Cluster Sharding and passivation](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)
- [Akka Typed persistence and event sourcing](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html)
- [Erlang/OTP design principles](https://www.erlang.org/docs/27/system/design_principles.html)
- [Erlang/OTP supervisor contract](https://www.erlang.org/doc/system/sup_princ.html)
- [Erlang process links and monitors](https://www.erlang.org/doc/system/ref_man_processes.html)
- [Kameo repository and feature inventory](https://github.com/tqwewe/kameo)
- [Kameo spawn and supervised-child API](https://docs.rs/kameo/latest/kameo/actor/trait.Spawn.html)

### Historical AF0 execution/layering snapshot — superseded by DR1

Everything in this subsection through the next explicitly current backlog
section is retained only as historical research evidence. Its `RunExit`,
`RunError`, `RuntimeEffects`, public phase-method, synchronous-Engine, and
prepared-Driver proposals are not normative. DR1 and `driver-law.md` supersede
them with the consuming direct Driver described at the top of this ledger.

The current execution path is factual evidence, not the desired API:

| Layer today | Takes | Does | Gives | Problem to research |
|---|---|---|---|---|
| `System::spawn` | `Actor<B>` plus shared router/lifecycle configuration | Prepares mailbox, observations, environment, registration, and incarnation | `Handle` or registration failure | Public bounds expose effect-routing and creation-observation machinery |
| `PreparedIncarnation::launch` | Committed incarnation resources and launch mode | Calls `tokio::spawn`, emits lifecycle facts, retains abort/completion edges | `Handle` | Task ownership is Bombay-owned but split from Engine lifecycle ownership |
| `Incarnation::run` | `Driver`, registration lease, observations, lifecycle reporter | Manually calls engine init, loop, and retire phases; publishes terminal result | Completion/peer observations | Bombay knows and orchestrates Engine internal phases |
| `Driver` | `Compose<B>` and `Environment` | Initializes, stores `Active<B>`, owns serialized event loop, passes effects to environment | `RunExit` or `RunError` | `Driver`, `RuntimeEffects`, phase methods, and retirement protocol are public |
| `ActorEnvironment` | Mailbox source and incarnation effect services | Supplies events, creates children, routes sends, retires children | Environment errors | Requires application send types to implement Bombay interpreter contracts |
| Application send type | Domain effects | Currently implements algebra, traversal, creation probing, routing, and error composition in complex examples | Routable runtime effect product | Application is incorrectly performing Bombay's job |

AF0 must answer and record, with inversion tests proposed for every answer:

| Required engine/layering decision | Required evidence |
|---|---|
| Exact black-box Engine input and output | Current Engine tests plus Behavior/Transition/Machine Executor contracts |
| Whether Engine owns retirement on return, error, panic, and cancellation | Current `Driver`, `Incarnation`, task-drop, and terminal-publication ordering |
| Whether Engine exposes any stateful driver or phase methods | Independent test requirements and the sole production call site |
| Exact boundary between the Engine loop and Bombay environment | Fake-environment tests and `ActorEnvironment` responsibilities |
| Exact owner of `tokio::spawn` and cancellation authority | Incarnation generation, address lease, task handle, and executor portability requirements |
| Representation passed from Behavior through Engine to Bombay | Behavior purity plus complete Bombay interpretation without application trait implementations |
| How each actor template obtains one engine and environment | Normal, child, supervisor, proxy, router, and pool permutations |
| Which traits must remain technically public for cross-crate compilation | Macro hygiene and Rust coherence, with ordinary docs/preludes kept clean |
| Final public application layer | Comparative actor APIs and the reference application after all lower layers exist |

The target dependency direction to validate is strictly one-way:

```text
application domain behavior and topology
    → ready actor templates and Behavior composition
    → one black-box Engine run per incarnation
    → Bombay-owned environment interpretation
    → Bombay-owned incarnation/task/registration/retirement
    → shared mailbox, address, observation, timer, and executor primitives
```

No lower layer may require an application to implement its interpreter,
executor, routing, registration, lifecycle-turn, wrapper-product, or composed
error contracts. AF0 must correct this diagram if source evidence disproves
any edge before implementation features are created.

### Historical AF0 synchronous-Engine experiment — 2026-08-15

This experiment correctly proved that a Behavior turn itself requires no
address value, async runtime, or environment. It incorrectly promoted that
kernel boundary into the complete Driver boundary. The normative layered
decision is now [`driver-law.md`](driver-law.md): the Driver composes the direct
Behavior turn with one typed environment and owns their causal async execution,
while the incarnation owns actor identity and task lifecycle. Statements below
that exclude the environment or async loop from the complete Driver are
historical conclusions, not current guidance.

The earlier proposal in this session made Engine a consuming async Driver over
an environment. That conclusion was not experimentally justified and is
superseded by this record. This correction does not unblock AF0 or authorize
implementation.

The repository does not currently provide a valid Engine baseline. A locked
`cargo check -p bombay-engine` fails: Engine imports `behavior::Active` and
calls `Compose::initialize`, but the declared and locked Behavior 0.10.0 does
not provide that API. The local patched Behavior checkout has advanced to
0.12.0, so its version no longer satisfies the workspace's 0.10.0 requirement
and Cargo does not select it. Dependency alignment is therefore an explicit
blocker before Engine implementation or repository-wide verification.

A disposable executable experiment was compiled and run against the actual
local Behavior 0.12.0 source plus locked Transition 0.1.0 and Machine Executor
0.1.0. It used a custom domain `Behavior`, a type-only address with no stored
address value, synchronous initialization, and two synchronous exclusive
turns. It required no environment, mailbox, Tokio runtime, async operation,
actor identity, address value, router, effect interpreter, or retirement
resource. The experiment was removed after it passed; its generated build tree
was moved to the system Trash.

This establishes a narrower boundary:

| Owner | Exact Engine-relevant contract | Explicit non-ownership |
|---|---|---|
| Behavior | Concrete domain/template composition, complete typed `Event`, and complete `Actions` | No execution policy, I/O, loop, or runtime interface |
| Transition | Affine `Machine::step`: input plus owned machine produces output plus one successor | No event source, async interpretation, or resources |
| Engine | Issue initialization exactly once; adapt the initialized behavior to one affine typed turn; split continue from terminal decisions | No address value, environment, event loop, async, effect interpretation, retirement, or actor identity |
| Machine Executor | Optional concurrency/borrowing policies for a machine when a concrete consumer proves one is needed | Not automatically part of the single-owner actor path |
| Bombay incarnation | Own the address value, mailbox/timer event selection, async loop, action interpretation, children, retirement, task, lease, cancellation, and terminal publication | No behavior folding or duplicate transition protocol |
| Communication, Address, Observe, Timers | Retain their previously verified mailbox, generation, publication, and scheduling contracts behind Bombay's incarnation | No Engine dependency or Engine capability interface |

`Behavior::Addr` remains an associated type because events, deliveries,
creations, and exits are statically typed by the address algebra. Engine does
not need or own a `B::Addr` value. The experiment's unit-like address proves
that distinction.

The minimal candidate is an affine synchronous black box, schematically:

```rust,ignore
struct Engine<B> { /* initialized behavior machine */ }

enum Decision<B: Behavior<Ph = Never>> {
    Continue {
        engine: Engine<B>,
        effects: EffectsOf<B>,
    },
    Stop {
        effects: EffectsOf<B>,
        exit: Exit<B::Addr>,
    },
}

impl<B: Behavior<Ph = Never>> Engine<B> {
    fn start(definition: B) -> Result<Decision<B>, B::Error>;
    fn turn(self, event: B::Event) -> Result<Decision<B>, B::Error>;
}
```

The names and exact effect projection are not approved API. The important
properties are that `start` and `turn` are synchronous, the engine is consumed
per turn, and only `Continue` returns a successor engine. This makes turning a
stopped or panicked engine impossible without a reusable state enum. Because
Transition already makes `Machine::step` affine, the sole actor task does not
need `ExclusiveExecutor` merely to regain `&mut` access or to report poison on
later reuse. A separate consumer must prove a need before another executor
policy is introduced.

Bombay's incarnation loop, not Engine, owns the causal runtime protocol:

```text
Engine::start(definition)
    -> await interpretation of complete initialization effects
    -> if Continue: optionally publish registration, then select next event
    -> Engine::turn(engine, event)
    -> await interpretation of complete turn effects
    -> repeat or retire
```

There is consequently no Engine `Environment` trait. Bombay may keep private,
independently tested source and interpreter collaborators, but bundling
`next`, `interpret`, and `retire` into an Engine-owned port gives the pure turn
engine runtime responsibilities it cannot enforce without becoming the async
actor loop. Application behavior is passed no runtime-capability interface;
its event/action algebra declares requirements statically.

The current `Driver`, `Environment`, `RuntimeEffects`, `RunExit`, `RunError`,
`run_init`, `run_loop`, `run`, `retire`, internal loop-state enum, and poison
re-entry behavior are therefore candidate deletions from `bombay-engine`, not
the target black-box surface. Terminal runtime errors and environment closure
belong to Bombay's incarnation result. Behavior errors remain Engine turn
results. Initialization remains a distinct Engine operation because Bombay's
transactional activation must interpret initialization before registration.

Required inversion oracles for the eventual aligned implementation are:

- Engine construction requires no address value or runtime capability;
- `start` initializes exactly once and produces either one successor or a
  terminal decision;
- every `turn` consumes one event and the prior engine exactly once;
- only a continue decision contains a successor engine;
- panic consumes the only engine value, so no poison-reentry API exists;
- Engine performs no polling, awaiting, interpretation, retirement, or task
  launch;
- Bombay interprets each complete effect batch before invoking the next turn;
- every template changes only the closed event/action types, never the Engine
  algorithm; and
- application code cannot obtain Bombay's source/interpreter collaborators.

### AF0 Transition contribution audit — 2026-08-15

The complete Transition and Machine Executor sources, tests, benchmarks,
history, Bombay/actorpass adapters, and Entity consumers were traced after the
Engine experiment. The reason for `bombay-transition` is concrete outside the
actor Driver: Bombay Entity constructs lifecycle machines from `Base`, uses
real validated lifecycle topology, composes machine structure, reduces typed
state transitions, and runs its directory boundary through the executor's
linearized policy. Transition owns a reusable representable state-machine
algebra for that domain.

The current Bombay Engine use is materially narrower. `BehaviorMachine` wraps
one already composed `Behavior`; production supplies a fabricated one-vertex
`executing` topology, never composes it with `then`, `product`, or `routed`, and
never inspects meaningful behavior structure. Its tests can inject a separate
topology, but no law derives that topology from the behavior or proves that it
corresponds to the behavior's transitions. The adapter uses Transition only to
convert `Behavior::transition(&mut self, event)` into affine
`Machine::step(self, event)`, allowing Machine Executor's `ExclusiveExecutor`
to provide a reusable `&mut` seat with poison-on-panic semantics.

History explains why this path exists but does not independently justify it.
The original actorpass research first rejected a shared runtime kernel, then an
operator-directed extraction fixed the production path as
`Behavior -> Transition -> Machine Executor -> Driver`. Subsequent tests and
checkers proved conformance to that chosen path. They did not prove that
Transition composition or topology was required by an ordinary actor turn.
The poison fence became necessary because the extracted Driver remained
publicly reusable after a caught unwind.

The existing direct-versus-executor benchmark was rerun on 2026-08-15. Median
payload turns were 3.363 ns direct and 3.545 ns through `ExclusiveExecutor`;
no-op turns were both approximately 0.64 ns. This shows the exclusive adapter
is inexpensive. It does not establish an otherwise missing actor-system law.
Serialized and linearized policies were respectively 112.876 ns and 25.528 ns
per payload turn and are not the current actor execution policy.

Verdict: Transition is a valid lower-level subsystem and a real dependency of
Entity and other explicitly representable machine components. The current
evidence does **not** justify making every Behavior transition pass through it.
For the universal actor Driver, that edge remains blocked pending one of two
proofs:

1. derive and verify meaningful Transition structure from every composed
   Behavior, making topology/composition observable actor-system capability; or
2. prove that a required Driver execution law cannot be expressed by Behavior
   plus the Driver's own private single-owner turn state.

Absent either proof, the honest layering is that actor templates may themselves
use Transition-backed machines where appropriate, while the universal Driver
drives the already composed Behavior directly and composes that turn with its
typed environment according to [`driver-law.md`](driver-law.md). Machine
Executor remains available to concrete multi-owner or alternate-order
consumers; its existence does not require the actor Driver to use it.

### AF0 actor-semantics research audit — 2026-08-15

The Driver discussion was checked against Agha's original actor semantics,
current Akka typed execution/dispatcher documentation, current Orleans request
scheduling, Erlang/OTP process scheduling and signal semantics, Plyukhin and
Agha's 2020/2021 distributed termination work, the 2021 failure-aware actor
model, 2025 actor-capability ordering research, the DS2 actor schedule-testing
work, and the published actor concurrency-bug taxonomy. The resulting ownership
decisions and primary links are recorded in `docs/driver-law.md`; the derived
schedule-exploration requirements are in `docs/driver-test-strategy.md`.

The research does not establish strict action interpretation before the next
event as a universal actor-model law. Bombay deliberately selects a stronger
non-reentrant **local commit** boundary. It also does not justify global order,
unconditional liveness, termination detection, retry, linearizability, or
exactly-once delivery in Engine. Those guarantees require explicit typed
protocols, capability contracts, scheduler assumptions, or higher runtime
layers. AF0 remains discussion-only and blocked on law acceptance and Behavior
version alignment.

## Historical dependency snapshot (superseded by Engine experiment)

The snapshot below captured the earlier local checkout and is retained only as
historical evidence. It is no longer current: local Behavior is now 0.12.0 at
`5d8c8e0b0294f92bd7ce90beb18646acd46393af`, while Bombay still requests and
locks 0.10.0. The patch is therefore not selected and a locked Engine check
cannot establish a valid baseline. The mismatch recorded above is normative.

The workspace selects the clean local Behavior 0.10.0 checkout at
`63045b4c60e0c652bbd024c60f5f49069683d54c` and local Address 0.2.0 checkout
at `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` through Cargo patches. It locks
Communication 0.1.1, Observe 0.1.0, Timers 0.1.0, Transition 0.1.0, and Machine
Executor 0.1.0 from crates.io. The exact manifest and lock are authoritative. Earlier
feature records naming Behavior commit
`31f33897dbcdf8fd92da39affe125c90f59d32a2` are historical evidence from the
temporary pre-release pin. The Bombay
Communication owner checkout's source matches the locked 0.1.1 package, while
its workspace manifest still declares 0.1.0; the registry version remains
authoritative.

The local Behavior revision is two commits after the 0.10.0 release. The local
Address revision supplies the 0.2.0 opaque resolved-endpoint contract. AF0 must
reverify these active sources feature by feature; the earlier migration audit
does not waive that requirement.

### Historical AF0 verification progress — 2026-08-15

This progress record predates the Behavior 0.12.0 checkout and the failed
Engine baseline check. Its ownership findings remain evidence, but its claim
that the exact selected Behavior source was verified is superseded by the
dependency blocker above.

AF0 remains `blocked`; this record is fresh evidence, not permission to begin
implementation. The dependency graph was reconciled before selection: the
pre-AF0 prose that called E5 active and M5 feature-complete is now explicitly
historical, matching the current table (`E5 <- AF0`, `M5 <- S1`). No inverse
edge disagreement remains among AF0, S1, E1-E9, M4, M5, and FV1.

The exact selected local revisions were rechecked: Behavior 0.10.0 at
`63045b4c60e0c652bbd024c60f5f49069683d54c` and Address 0.2.0 at
`7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`. The registry sources selected by
the lock were inspected for Communication 0.1.1, Observe 0.1.0, Timers 0.1.0,
Transition 0.1.0, and Machine Executor 0.1.0; the Observe and Timers owner
checkouts are respectively `43016e5f781e006e072e77af996c4da64466dee8`
and `4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2`. Source, public exports,
semantic tests, and the relevant adversarial/model tests establish:

- Behavior alone owns the pure definition/active typestate, typed user and
  service event algebras, named send composition, births, supervision,
  watching, shutdown, stash, timers-as-effects, machines, proxy, and pool
  policy. It owns neither an event loop nor I/O.
- Communication alone owns the bounded user lane, priority control lane,
  per-lane FIFO, configured aging, cancel-safe receive, closure, drain, and
  exact rejected-payload ownership. It assigns no actor meaning.
- Address alone owns typed lookup, exact-generation registration/release, and
  opaque read-only resolved endpoint snapshots. A resolved snapshot is not a
  lease and may remain valid after release or address reuse.
- Observe alone owns exact-generation retained terminal publication and sync
  or async observation. It does not define actor death or replacement policy.
- Timers alone owns keyed monotonic schedules, queue-branded generation
  tokens, replacement invalidation, cancellation, and equal-deadline FIFO.
  It does not launch tasks or define receive-timeout policy.
- Transition owns pure affine machine steps and static composition. Machine
  Executor owns exclusive, serialized run-to-completion, and linearized
  execution policies; it does not own mailbox polling, async effects, actor
  identity, or retirement.

The current Bombay/Engine production path was traced through `System`,
`ProvisionalIncarnation`, `PreparedIncarnation`, `Incarnation`, `Driver`,
`BehaviorMachine`, `ActorEnvironment`, terminal retirement, and their inversion
tests. One current Engine run consumes a `Compose<B>` plus one environment,
initializes the definition exactly once, interprets initialization effects,
then repeatedly consumes one environment event, performs one exclusive
machine turn, and awaits interpretation of that turn's complete effects. It
returns `RunExit<Exit<B::Addr>>` or a typed behavior/environment/poison error.
The Driver owns the active behavior machine and loop state, while Bombay owns
the concrete environment, address lease, mailbox, observations, cancellation
flag, Tokio task, and final publication.

Current retirement is deliberately split and therefore not yet the desired
black-box contract. `Driver::run` can retire its environment, but production
`Incarnation::run` calls public `run_init`, `run_loop`, and `retire` phases
manually. `PreparedIncarnation::launch` is the Tokio spawn point. On ordinary
return Bombay retires the environment, drops the driver, releases the exact
address lease, emits `Retired`, publishes detailed and peer outcomes, emits
`Completed`, and lets the task finish. Panic or future cancellation drops the
incarnation and its terminal guard, classifying panic/cancellation and
preserving lease-release-before-publication. Initialization failure follows
the same explicit environment-retirement path. These facts confirm that
`Driver`, `RuntimeEffects`, its phase methods, and the public retirement port
are candidate accidental surface; AF0 must decide the replacement contract
before any visibility or ownership change.

Fresh comparative evidence currently targets Akka Typed 2.10.20, Erlang/OTP
29.0.4 documentation, and Kameo 0.22.2 (local checkout
`90138758779d2260798c41cfaa47598db84f05b8`). The existing candidate table is
directionally complete enough to expose local, cluster, persistence, stream,
testkit, operations, and deployment families, but it is not yet canonical:
each row still needs row-specific guarantees, failure semantics, primary
evidence, and one final ownership classification. Kameo's pool and pub/sub
templates now live outside its core crate, which is direct evidence that
ready templates and optional libraries must be classified separately from an
engine capability.

AF0 remains blocked on the following inseparable research output:

1. Finish row-level Akka, Erlang/OTP, and Kameo verification, including
   negative evidence for capabilities each system does not promise.
2. Expand the candidate inventory into the required canonical columns:
   intent, machinery, guarantee, failure semantics, evidence, and
   classification.
3. Record the typed composition matrix for ordinary actors, wrappers,
   supervisors, proxies, routers, pools, nested supervision, watch/link,
   timers, stash, shutdown, request/reply, streams, and remote boundaries.
4. Convert the Engine observations above into explicit black-box decisions
   and an inversion oracle for every decision.
5. Derive replacement implementation IDs and reciprocal dependency edges
   from those completed tables. Existing E1-E9 descriptions may supply
   historical evidence but may not predetermine that graph.

Ownership remains:

- Behavior owns pure folds, event/protocol algebra, births, wrappers, and typed
  send products.
- Communication owns the sole two-lane mailbox, per-lane FIFO, backpressure,
  closure, and rejected-payload recovery.
- Address owns typed identity, registration, and generation fencing.
- Observe owns exact-generation terminal publication.
- Timers owns keyed, generation-safe monotonic scheduling.
- Transition owns the affine machine step; Machine Executor owns ordered
  execution turns; `bombay-engine` owns their Behavior orchestration.
- bombay owns addresses, `System`, `Handle`, spawning, effect
  interpretation, endpoint selection through `DeliveryRouter`, and exact
  incarnation retirement.

## Unfinished contracts

### Runtime operations: A5, C4, C5, F2-F7, P1, R1, M2

#### R1 verification — 2026-08-13

The repository identity decision is explicit: the legacy implementation in
`devrandom-labs/bombay` is removed, and the runtime formerly developed as
Actorpass supersedes it wholesale. This is not a merge of the two runtimes.
The exact imported source is Actorpass
`0f42fc27411944b93c8e30c68c3b44b3101274fc`, including F7 transactional
activation. The public core package is renamed to `bombay-rs` with Rust crate
name `bombay`; `bombay-behavior-engine` is renamed to `bombay-engine`; and
`actorpass-framework` is renamed to `bombay-framework`.

The locked neighboring contracts remain Behavior
`31f33897dbcdf8fd92da39affe125c90f59d32a2`, Communication 0.1.1, Address
0.1.1, Observe 0.1.0, Timers 0.1.0, Transition 0.1.0, and Machine Executor
0.1.0. Their source, public APIs, tests, and relevant documentation were
already rechecked for F7 at this exact runtime revision; R1 changes no algebra,
protocol, lifecycle ordering, or ownership boundary. Behavior remains the pure
algebra owner; Communication the mailbox owner; Address registration owner;
Observe terminal-publication owner; Timers schedule owner; Transition/Machine
Executor ordered-machine owners; the renamed Bombay core remains runtime
composition owner; and `bombay-engine` remains Behavior orchestration owner.

R1 must update every manifest, crate import, public re-export, test, example,
benchmark, fuzz target, research workspace, diagnostic probe, CI/build input,
and current document. Historical evidence may retain old commit/repository
provenance only when explicitly labeled historical. Completion requires one
package graph with no live `actorpass`, `actorpass-framework`, or
`bombay-behavior-engine` package/import and all repository gates passing.

The repository-wide rename audit found no live legacy package or import, and
the renamed manifests, public re-exports, tests, examples, benchmarks, fuzz
target, documentation, and CI/build inputs are synchronized. The repository
gates pass. R1 is `feature-complete` pending M2 and the project-wide release
audit.

- A5 requires the same temporal conformance suite for a real second executor;
  trait conformance alone is insufficient.
- C4 remains consumer-gated. Hung-fold detection needs an explicit policy
  owner and an oracle; it may not smuggle cancellation policy into the kernel.
- C5 and P1 require concrete second router, clock, or embedded consumers before
  generalizing the current Tokio runtime.
- F2 requires a second priority protocol. Typed shutdown alone does not justify
  a general control framework.
- F3 is ordinary application protocol: a request carries a typed `Recipient`,
  timeout uses existing timers, and late reply and peer retirement are explicit.
  No ask object, correlation registry, reply channel, or runtime handle exists.
- F4 belongs in a focused adapter owning one bounded pump, source cancellation,
  actor backpressure, and accounting for rejected payloads.
- F5 defaults to an explicit typed event sum. It may reopen only when a real
  consumer cannot preserve types and payload ownership that way.
- F6 is Behavior-owned replacement-resolution algebra first; bombay only
  interprets a runtime fact if that algebra requires one.

#### F7 verification — 2026-08-13

F7 is the focused operation requested by
`devrandom-labs/bombay#307`, with `bombay-entity` 0.1.0 and
`mnesis-bombay` as concrete consumers. The exact bombay source is
`6466232b00d36b021098499498f59f6f594023ed`; the locked dependencies are
Behavior `31f33897dbcdf8fd92da39affe125c90f59d32a2`, Communication 0.1.1,
Address 0.1.1, Observe 0.1.0, Timers 0.1.0, Transition 0.1.0, and Machine
Executor 0.1.0. Entity 0.1.0 was inspected at
`68d0f503205a569ddda88124f5add8e8a652e18f`. For each owner, current source,
public API, tests, and relevant documentation were rechecked for this feature.

The ownership map is exact: Behavior owns the pure initialization fold and
typed effects; Machine Executor and the bombay Behavior Engine own ordered
execution and effect interpretation; Communication owns the sole two-lane
mailbox and rejected-payload recovery; Address owns exact registration
generations; Observe owns exact terminal publication; Timers owns only keyed,
generation-safe schedules and is not on this activation path. Bombay owns
provisional resource construction, initialize-before-register ordering,
registration, launch, cancellation, and terminal retirement. Entity owns
stable entity identity, activation coordination, admission, fences, draining,
passivation, and the policy choice between graceful and forced retirement.

The public-contract gap is demonstrated by
`mnesis-bombay/tests/bombay_entity_contract.rs`: `System::spawn` commits
registration synchronously before the launched task polls `Driver::run_init`,
whereas Entity may commit its stable endpoint only after preparation and
transactional activation succeed. Bombay's child-birth path already proves
the required sequence: `prepare_provisional` → `Driver::run_init` → `commit`
→ `run_initialized`. The implementation must make that sequence canonical
for roots, preserve typed initialization/effect/registration errors, and
return a cloneable delivery-only capability separately from one affine
graceful/forced retirement capability without exposing private typestates,
mailbox senders, registration leases, observation subjects, executor handles,
`dyn`, `Any`, or downstream vocabulary.

The observable invariant is: successful return happens only after the entire
initialization fold and all initialization effects complete, then exact
registration commits and the initialized incarnation launches; failure leaves
no routable endpoint, and provisional resources retire exactly once. The
endpoint is cloneable, retirement authority is not, graceful and forced
retirement both await the exact terminal outcome, and no returned endpoint can
survive failed initialization. Inversion tests must fail if registration moves
before initialization, launch moves before initialization effects, a failed
provisional registration leaks, retirement authority becomes cloneable, or a
failed initialization yields a live endpoint. A downstream compile/conformance
probe must implement Entity 0.1.0's `LocalEntityRuntime` without private types
or erasure.

The implementation reuses the private provisional/committed typestates and
the single `PreparedIncarnation::launch` operation shared by ordinary root
spawn, transactional root activation, and child birth. That operation owns
the sole task, abort-control, completion, and lifecycle-start ceremony;
`System::activate` reaches it through the
`Driver::run_init`/`Incarnation::run_initialized` path. `System::activate`
returns `RootEndpoint<B>` separately from the affine
`BehaviorRetirement<R, B, L>` alias; the latter preserves typed terminal
outcomes and graceful/forced authority without exposing any underlying seat.
The final interface audit removed the initial behavior/system-specific wrapper
bounds from the retirement object: `RootRetirement<R, T>` now has only the two
independently required generic facts, while the public alias keeps downstream
Entity associated types concise and static.

Five activation inversion oracles prove init/effect/registration ordering,
typed stage failures, rollback, separate capability shape, exact cancellation
completion, exact-once provisional destruction after collision, registration
release, and immediate same-address reuse. The transition-driver Entity probe now
implements the released 0.1.0 `LocalEntityRuntime` directly with
`RootEndpoint<EntityActor>` and `BehaviorRetirement<EntityRouters,
EntityActor>`; its former retire-command channel and owner task were removed.
All ten Entity conformance tests pass. Workspace tests, rustfmt, Clippy, rustdoc,
the downstream probe, documentation/re-export synchronization, and the
repository-wide positional/duplicate-protocol audit pass. F7 is
`feature-complete` pending M2 and the project-wide release audit.

#### F6 verification — 2026-08-13

F6 uses Bombay Behavior commit
`31f33897dbcdf8fd92da39affe125c90f59d32a2` temporarily while its 0.9.5
release runs through CI. Bombay previously locked 0.9.4; the live crates.io
index still ended at 0.9.4 during this verification. The selected commit is a
clean repository object; unrelated uncommitted pool/test changes in the sibling
checkout are not part of the contract.

The feature-specific source, public API, tests, and documentation audit found:

- Behavior owns `CreationKind::ReplacementIncarnation`, creation installation
  and rejection events, worker-stop events, and the new interpreter-neutral
  `ReplacementResolution` projection. `WorkerCreationResolved::into_replacement`
  distinguishes an installed successor from a rejected attempt without nonce
  arithmetic; `WorkerStopped` remains the separate dead-incarnation fact.
  The nominal `#[behavior::behavior(...)]` macro is authoring syntax only and
  creates no observation or runtime path.
- Observe 0.1.0 owns retained publication for one exact outcome generation. It
  neither knows replacement provenance nor combines terminal and creation
  facts.
- Timers 0.1.0 owns keyed monotonic schedule generations and has no role in
  replacement resolution.
- Communication 0.1.1 owns the bounded two-lane mailbox, FIFO, closure, and
  rejected-payload recovery. It transports typed events but assigns no
  incarnation meaning.
- Address 0.1.1 owns exact registration identities, generation fencing, and
  address reuse. Its opaque registration generation must not be exposed or
  aliased with Behavior child nonces.

Therefore bombay owns only the existing interpretation edges: publish the
exact stopped-child fact and report the already-recorded creation result to the
stable proxy. It must add no replacement registry, observation request, lease
authority, correlation identity, or duplicate protocol. The F6 observable
invariant is that one replacement attempt yields a Behavior-owned installed or
rejected resolution naming its explicit predecessor, while predecessor death
remains independently observable. An inversion test must fail if birth is
misclassified as replacement, replacement provenance is inferred, or death and
creation resolution are collapsed.

The implementation audit found no missing bombay protocol: the existing
typed report edge already preserves all fields required by the Behavior
projection. Its oracle now covers both rejected and installed replacements and
was inverted by substituting the fresh worker nonce for the stable proxy; that
inversion failed. The existing child-stop oracle independently proves the dead
incarnation fact. The reference application also uses the new nominal macro
for an eligible plain behavior. Workspace tests, rustfmt, Clippy, and rustdoc
pass. F6 is `feature-complete` pending M2 and the project-wide final audit.
- M2 distills these operations together and must remove duplicate adapters and
  executor seats before it can unblock FV1.

### Optional libraries: L1-L7, M3

- L1 worker routing is Behavior policy. Stable key affinity must survive
  replacement or change only through explicit rebalance; extraction requires a
  second consumer and adds no runtime pool or erased recipient.
- L2 requires a concrete typed-group consumer and a decision separating local
  membership from Zenoh transport/discovery ownership.
- L3 keeps accepted intake in application state, preserves replay order, and
  makes overflow and draining refusal typed and observable.
- L4 keeps drain state, in-flight work, deadline, and completion accounting in
  the behavior; it creates no mailbox mode or runtime drain controller.
- L5 composes Behavior send products and typed `SendAlgebra::send`; it keeps
  retry generation and backoff in application state and introduces no timer
  adapter or handwritten product algebra.
- L6 exposes reporting through ordinary typed messages and reply recipients,
  without a registry or query subsystem.
- L7 remains blocked until an invariant exists beyond Behavior timers.
- M3 audits optional artifacts independently and does not block FV1.

### Developer experience: E1-E9, M4

- E1 may provide syntax only when it expands to the canonical public owners.
  `local_system!` must expand to `System::new`; Behavior's nominal attribute is
  preferred for eligible plain user-message behaviors. Semantic
  wrappers and service-event behaviors remain explicit.
- E2 keeps compile failures focused on actionable ownership and inference
  boundaries; diagnostic aliases may not become alternate construction paths.
- E3 keeps cookbook and public usage guidance synchronized with the locked APIs,
  typed send paths, sole spawn path, and kernel exclusions.

#### E4-E9 authoring-UX audit — 2026-08-14

Historical snapshot: the API counts and Behavior 0.9.5/0.10.0 descriptions in
this audit record the pre-E5 baseline. The later E5 local lifecycle migration
section and the current composition rules are normative.

The audit inspected the complete tracked repository, including the core and
framework public exports, `hello`, `local_runtime`, the 1,241-line `job_queue`,
all tests, benchmarks, fuzz inputs, README, cookbook, and current architecture
documents. It also rechecked the source, public API, tests, and relevant
documentation of the exact locked crates.io packages Behavior 0.9.5, Observe
0.1.0, Timers 0.1.0, Communication 0.1.1, Address 0.1.1, Transition 0.1.0,
and Machine Executor 0.1.0. The repository currently contains 96 spawn call
sites, 60 handwritten `Behavior` implementations, 19 `Handler`
implementations, 12 application `EndpointRegistry` implementations, and 24
application `DeliveryRouter` implementations. Counts are evidence for this
audit, not permanent API targets.

The minimal authoring target is not raw line-count reduction at the expense of
types. A small application should declare its domain behavior, declare each
real message route once, construct one system, spawn one actor value, and use
the returned capabilities. The job queue must keep domain policy visible while
removing duplicated endpoint type algebra, generated routing bodies, test-only
probe actors, repeated empty initialization folds, and lifecycle pattern
matching that adds no domain decision. Each item must measure the ceremonial
constructs it removes and must not replace them with hidden runtime policy.

##### E4 verification and contract

Behavior 0.9.5 already makes one behavior value the owner of application state
and statically maps its address, message, complete event, send algebra, birth
mode, phase, and error types; it does not contain a concrete address value.
Address 0.1.1 owns only exact registration generations, Communication 0.1.1
owns only typed mailbox endpoints, Observe 0.1.0 owns completion publication,
and Timers 0.1.0 owns schedules. Transition and Machine Executor accept the
behavior machine but deliberately know no actor identity. Therefore bombay is
the exact owner of the missing authoring concept: an inert `Actor<B>` pairing
`B::Addr` with `B` before runtime resources exist.

E4 adds one minimal public value with construction, borrowing, and consuming
parts operations. `System::spawn` and `System::activate` consume that value;
child `Create` remains Behavior-owned and distinct because it carries a
creator-local nonce and creation provenance. `Actor` implements no behavior,
contains no mailbox, task, handle, router, lifecycle state, or policy, and does
not survive as a second live runtime object. The observable invariant is that
the address value and behavior can enter preparation only as one statically
matched `Actor<B>`. An inversion compile test must reject `Actor<B>` built with
an address other than `B::Addr`, and runtime oracles must prove spawn and
activation ordering and outcomes are unchanged.

The implementation adds only the two-field inert `Actor<B>` and routes root
spawn, transactional activation, ordinary preparation, provisional
preparation, and the child-runtime handoff through it. Behavior's `Create`
remains unchanged. All 89 root spawn call sites and all six direct
transactional activation call sites now pass one actor value; the framework
prelude, public exports, examples, tests, benchmarks, README, cookbook, and
module-boundary documentation use the same concept. A compile-fail doctest
rejects an address outside `B::Addr`; a focused unit test round-trips exactly
the address and behavior; the process allocation oracle and every existing
spawn, activation, creation, lifecycle, panic, cancellation, routing, and
retirement oracle pass unchanged. Workspace tests, rustfmt, Clippy, rustdoc,
and the repository-wide old-call-shape audit pass. E4 is `feature-complete`
pending E9, M4, and the project-wide release audit.

##### E5 verification and contract

Historical contract record, superseded by “E5 local Behavior lifecycle
migration — 2026-08-14” below. Older public API names remain here only as
evidence for the migration decision.

Fresh verification on 2026-08-14 inspected the exact locked registry source,
public API, tests, and documentation for Behavior 0.9.5, Communication 0.1.1,
Address 0.1.1, Observe 0.1.0, Timers 0.1.0, Transition 0.1.0, and Machine
Executor 0.1.0. It also inspected the Behaviorpass repository instructions,
source, testkit, current documentation, every local branch, remote main
`fcc3aedeb76cf69a034920ec84556cd886bb141b`, and the complete Actorpass routing
implementation and guidance at `0f42fc27411944b93c8e30c68c3b44b3101274fc`.

The prior E5 contract was wrong to prescribe an application routing macro.
Behavior's then-current recursive routing work solved effect-lane selection:
typed product/path composition and Bombay's generic interpreter meant
applications did not implement or traverse effect routing. That did not solve
endpoint selection. `Delivery<A, M>` and
`Recipient<A, M>` contain only an address/route and message; they carry no
destination behavior, event protocol, or endpoint type. `Behavior` associates
its own input `Addr`, `Msg`, and `Event`, but an emitting behavior's output
delivery does not identify which receiving behavior owns `M`. Multiple real
behaviors in both framework examples accept the same address/message protocol,
so associated-type projection cannot select or even prove distinct endpoint
implementations.

Communication owns typed mailbox lanes and rejected payload recovery but no
heterogeneous endpoint space. Address owns homogeneous `AddressSpace<A, E>`
tables, exact claims, and leases; it deliberately has no `Any`, `TypeId`,
downcast, or runtime typemap. Observe and Timers add no delivery fact.
Transition and Machine Executor do not participate in routing. Bombay owns
endpoint registration, lookup, route resolution, and effect interpretation.
Consequently ordinary actor applications must not be made to own `Routes`,
`AddressRouter`, `EndpointRegistry`, `DeliveryRouter`, `IncarnationEndpoint`,
or endpoint aliases; those are runtime implementation concepts.

An attempted behavior-only declarative expansion was deliberately compiled and
rejected: Rust coherence cannot distinguish implementations whose address,
message, and event are only unrelated associated-type projections, and shared
message protocols are genuinely ambiguous under the current `Delivery<A, M>`
contract. Adding field names, modes, repeated message annotations, generated
route structs, or a system type-list merely moves the same missing destination
fact into user syntax. It is not accepted E5 work. Dynamic registries, erased
endpoints, `Any`, `TypeId`, downcasts, global envelopes, and runtime protocol
lookup remain prohibited.

E5 was blocked on a truthful static destination/endpoint contract.
That contract must make ordinary registration and delivery fully Bombay-owned,
preserve pure behaviors and rejected payload ownership, support repeated
message types without user routing declarations, and remain recursively
composable through births and wrappers. It may require a Behavior-owned typed
destination fact or a narrower Bombay mailbox/address composition, but must be
designed and verified in the owning repository before Bombay consumes it.
Until that contract lands, the existing low-level traits remain for the runtime
and current compatibility examples; they must not be promoted as the intended
application UX. Once it lands, E5 must remove routing vocabulary from the framework prelude, README,
cookbook, both framework examples, and all ordinary public signatures while
retaining focused internal adapter tests and explicit advanced extension
documentation only where a concrete non-Bombay interpreter requires it.

Behavior 0.10.0, released at
`4c756f5a6e8bf6aacfe6435250b95fb9fdcab985` on 2026-08-14, satisfies that
external prerequisite. Its exact release source, public API, changelog,
README, algebra tests, protocol-destination tests, compile-fail tests, model
tests, properties, fuzz consumers, wrapper compositions, and pool/supervision
consumers were inspected afresh. `Recipient<B>` and `Delivery<B>` are indexed
by the concrete destination `Behavior`; `B::Addr` and `B::Msg` determine the
address and payload, identical address/payload pairs remain distinct across
destination behaviors, `Recipient::resolve(parent)` is the sole public route
resolution operation, and the value carries no endpoint or runtime handle.
`Handler` and `Pure` now carry the complete send algebra, and an actor with no
ordinary sends uses `Vec<Never>`. Behavior deliberately exposes no route enum,
endpoint table, registration mechanism, or runtime topology object.

The Behavior 0.10 migration is complete across the locked dependency graph,
runtime implementation, unit and integration tests, doctests, examples,
benchmarks, mutation metadata, public documentation, and framework facade.
Bombay now interprets `Delivery<B>` and `Recipient<B>` exclusively, keys
registration and delivery compatibility adapters by `B`, uses `Vec<Never>`
for behaviors with no ordinary sends, and contains no consumer of Behavior's
private route representation. Identical address/message protocols are covered
by distinct destination behavior types in the request/reply and job-queue
oracles. Workspace build/tests, rustfmt, strict Clippy, all three executable
examples, and the repository-wide obsolete-pattern audit pass. This completes
the dependency migration but not E5: application-owned route structs and the
framework routing re-exports remain the next active seam and prevent a
`feature-complete` state.

The unchanged exact neighboring contracts were rechecked for this migration:
Communication 0.1.1 still owns only typed two-lane mailbox transport and
rejected payload recovery; Address 0.1.1 still owns homogeneous exact-generation
tables and leases; Observe 0.1.0 and Timers 0.1.0 add no delivery topology;
Transition 0.1.0 and Machine Executor 0.1.0 remain outside routing. Therefore
Bombay must key delivery and registration by destination behavior, resolve
only through `Recipient<B>::resolve`, and own every endpoint/topology object.
No compatibility copy of Behavior's private `Route`, address/message-indexed
delivery protocol, dynamic registry, or erased endpoint is permitted. E5 is
now `active`: upgrade the exact lock to Behavior 0.10.0, migrate the complete
runtime and repository, then remove ordinary application routing declarations
and routing vocabulary from the framework surface before feature completion.

##### E5 local Behavior lifecycle migration — 2026-08-14

Fresh verification for this follow-up inspected the exact locked registry
source, public API, tests, and packaged documentation for Behavior 0.10.0,
Communication 0.1.1, Address 0.1.1, Observe 0.1.0, and Timers 0.1.0. It also
inspected the source, public exports, algebra tests, compile-fail tests,
property/model/fuzz consumers, examples, README, domain-boundary guidance, and
wart audit in the clean local Behavior checkout at
`63045b4c60e0c652bbd024c60f5f49069683d54c`. The local Address checkout is at
`7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`; the local Observe checkout is at
`43016e5f781e006e072e77af996c4da64466dee8`; the local Timers checkout is at
`4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2`; and the Communication owner
workspace is at `e2e8f3995154dcc420a85b04e5f658e8af0f862f`. Untracked local
research/editor files in those neighboring checkouts are not dependency
contracts and were not modified. The registry packages remain authoritative
for every neighbor other than the explicitly requested local Behavior target.

The target Behavior revision removes `Handler`, `Pure`, `BehaviorFn`,
the positional send product/path types, `Transcript`, `run`, `Compose::from_fns`,
`Compose::from_behavior`, `Compose::build`, and direct public initialization or
transition on a definition. `Compose<B>` now owns an uninitialized definition;
its consuming `initialize` returns initialization actions and `Active<B>`.
Only `Active<B>` exposes `transition`, `receive`, and typed `on`; direct
`Behavior` implementations receive crate-minted `InitializationTurn` and
`ActiveTurn` capabilities. `BehaviorBase` and `Compose::base`/`Active::base`
replace positional wrapper inspection, and `StashStatus` exposes only the
semantic stashed-message count. Heterogeneous send products are now named
semantic wrapper/application structs that implement `SendAlgebra` and
`SendInput`; there is no public positional product or path language to copy.
The nominal `#[behavior::behavior]` attribute is the ordinary user-message
authoring path, while explicit `Behavior` remains for service-event protocols
and semantic wrappers.

The exact ownership map remains unchanged: Behavior owns pure definitions,
the consuming initialization/active typestate boundary, event and effect
algebra, semantic wrappers, births, supervision, and typed destinations;
Communication owns only the two-lane mailbox and rejected-payload recovery;
Address owns exact-generation registration and endpoint snapshots; Observe
owns exact-generation terminal publication; Timers owns keyed generation-safe
scheduling. Bombay Engine must consume `Compose<B>`, interpret the returned
initial actions, and store only `Active<B>` in its transition machine. Bombay
owns runtime preparation, task execution, effect interpretation, endpoint
selection, and retirement, but must not recreate Behavior initialization state
or expose a second way to mint lifecycle turns. This migration changes no
Communication, Address, Observe, or Timers protocol and introduces no registry,
request/reply mechanism, lifecycle framework, or supervision policy.

The local Address 0.2.0 follow-up was reverified from its complete public
source, semantics/reclamation/Loom tests, benchmark, guide, changelog, and
adversarial research suite. Its only contract change from 0.1.1 is that
`AddressSpace::resolve` returns `Resolved<E>`, an opaque read-only endpoint
snapshot. `Resolved<E>` dereferences and implements `AsRef<E>` but cannot be
constructed, destructured, mutated through the wrapper, moved out, or used as
registration authority. Resolution clones the endpoint outside the table lock;
the snapshot remains valid after release or address reuse. `Lease` remains the
sole exact-generation release authority and `RegistrationId` remains only an
opaque process-local correlation identity. Bombay must consume the resolved
capability through shared access, must not expose Address's internal `Arc`, and
must not add a second snapshot, reclamation, or registration mechanism.

The local lifecycle/address migration is `feature-complete`: the workspace now
selects the audited local Behavior revision and Address 0.2.0; Engine consumes
`Compose<B>` exactly once and stores only `Active<B>`; Bombay interprets named
semantic sends and typed optional event routing; obsolete Behavior exports and
call shapes are absent from current code, examples, benchmarks, mutation
metadata, and normative documentation. `cargo check --workspace --all-targets`,
`cargo test --workspace`, formatting, and workspace Clippy pass. Historical
records retain old names only in sections explicitly marked historical.

Historical pre-AF0 state: E5 remained `active` because its older, separate
application-routing objective still had advanced
`EndpointRegistry`/`DeliveryRouter` adapters in the framework examples. AF0
has since reopened E5 and all other E1-E9 items; E5 is now `blocked` by AF0 as
recorded in the current dependency graph. The traits were removed from the
ordinary framework prelude during this migration, but eliminating the concrete
endpoint-selection adapters remains candidate work whose ownership and order
must be derived from AF0 rather than resumed from this historical plan.

An address-integration audit on 2026-08-14 rechecked the locked Address 0.1.1
source, tests, benchmark, and public contract alongside Behavior 0.10.0,
Communication 0.1.1, Observe 0.1.0, and Timers 0.1.0, then traced every Bombay
registration, delivery, peer-observation, lifecycle, example, benchmark, and
test adapter. `EndpointRegistry<B, D>` must remain destination-behavior indexed:
Behavior intentionally distinguishes destinations with identical address and
message protocols, and Bombay routers use `B` as the static topology selector.
`AddressRouter` must also remain Bombay-owned rather than aliasing the foreign
`AddressSpace`; it hides table maintenance operations and names the typed
runtime topology boundary. Address registration generations remain distinct
from Behavior nonces and Observe completion generations.

Address's internal `Arc<E>` must remain hidden. A prototype opaque handle to
the shared registered endpoint benchmarked faster for Bombay-shaped endpoints,
but the Address adversarial cascade suite proved that it changes ownership:
snapshots then retain affine fields of the registered endpoint and delay nested
lease retirement. Endpoint-defined `Clone` is therefore a semantic boundary,
not redundant work. Address instead owns one opinionated lookup result:
`resolve` returns an opaque, read-only endpoint-defined snapshot. Bombay can
dereference that capability but cannot obtain the storage handle, mutate the
snapshot, choose a reclamation path, erase `B` or `D`, or reconstruct lease or
lifecycle authority. No local path override or duplicate Bombay lookup table
is accepted as a permanent integration.

##### E6 verification and contract

Historical baseline: its Behavior 0.9.5 authoring inventory is superseded by
the local lifecycle migration contract above.

Behavior 0.9.5 already supplies `Handler`/`Pure`, `BehaviorFn`, the nominal
`#[behavior::behavior]` macro, `Compose`, semantic wrappers, `Stash`, and both
`WorkerPool` and `KeyedWorkerPool`; its algebra and tests make those the only
authoritative behavior construction paths. Observe, Timers, Communication,
Address, Transition, and Machine Executor provide no behavior-authoring
syntax. Bombay may select and re-export Behavior facilities but may not clone
their protocols or generate a competing `Behavior` implementation scheme.

E6 rewrites every eligible plain application behavior to the existing nominal
macro or `Handler` path, uses existing pool/stash composition where its exact
contract matches, and records any irreducible macro or naming limitation as a
Behavior-owned upstream prerequisite. Semantic wrappers such as the job
queue's drain/retry coordinator remain explicit. Completion requires removing
repeated associated-type and empty-init ceremony without function-pointer type
aliases, positional send traversal, duplicate protocols, or a Bombay-owned
behavior macro. Compile diagnostics must still name the user's nominal type
and exact unsupported protocol.

##### E7 verification and contract

Behavior 0.9.5 owns `Exit` and `Crash`; Communication returns rejected payloads;
Address owns registration failures; Observe retains the exact published value;
Timers has no terminal-result role; Transition and Machine Executor expose
poisoning without classifying actor policy. Bombay therefore remains the owner
of `ActorRef`, affine `Handle`, `RootEndpoint`, `RootRetirement`, `RunError`,
`RunExit`, and `TaskOutcome`. The nested result retains materially different
return, behavior failure, environment failure, poison, panic, cancellation,
collection, and explicit-stop facts and cannot be flattened globally.

E7 audits repeated `handle.actor_ref()` access and terminal matching, then adds
only zero-policy conveniences that preserve ownership and every exact variant.
It must not make `Handle` cloneable, hide whether waiting consumes lifecycle
authority, turn shutdown publication into retirement, discard rejected
payloads, or normalize detailed root outcomes to peer outcomes. Each retained
method or helper needs an application call-site reduction and an inversion
test proving the original exact value remains recoverable.

##### E8 verification and contract

Communication 0.1.1 requires an explicit user-lane capacity and fixes the
priority default; Address and Bombay routing require a concrete router value.
Behavior, Observe, Timers, Transition, and Machine Executor own no system
configuration defaults. The current `local_system!` is a named-argument spelling
of `System::new`, while the framework prelude omits several Behavior 0.9.5
application facilities used by the reference domain.

E8 makes the facade, prelude, README, and cookbook one coherent authoring
surface. It must decide whether `local_system!` earns its existence, export the
selected current Behavior vocabulary, and remove stale or duplicate setup
spelling. Mailbox capacity and the concrete router remain explicit; no default
may silently choose backpressure, priority, topology, lifecycle, clock, or
executor policy. Compile examples must prove the canonical setup has one path
and retains its concrete inferred system type.

##### E9 verification and contract

The same dependency audit found that no neighboring crate owns application
presentation. Behavior's pool and stash types may replace matching handwritten
policy, but Observe probes, Timers probes, Communication stress shapes, Address
reuse oracles, and executor classification tests belong in focused tests rather
than the narrative application. Bombay-framework owns the reference program
that demonstrates how those exact primitives compose.

E9 rewrites `hello`, `local_runtime`, `job_queue`, README, and cookbook after
E4-E8 settle. The executable job queue must read primarily as address/domain
types, worker and queue policy, route declarations, actor construction, and the
scenario. Test-only reporter/collector actors and exhaustive lifecycle
assertions move to focused tests. The completion gate records before/after
counts for total lines and, separately, spawn arguments, endpoint/incarnation
aliases, manual registry/router bodies, repeated behavior associated-type
blocks, and nested terminal-result matches. At the canonical call site, each
actor is one value passed to one spawn operation and each real route is named
once. Repository-wide tests and inversion oracles must retain every existing
behavioral invariant.

Some explicit inputs are not UX defects: the sender address carries Behavior
event provenance and child-relative routing; mailbox capacity selects
Communication backpressure; a concrete router selects real endpoints; behavior
associated types preserve the complete static protocol; and detailed terminal
results preserve distinct ownership failures. E4-E9 may improve spelling and
local inference around those facts but may not erase them.

M4 is reopened and blocked by E9. After E9, M4 must compare the complete
authoring surface against the minimal target, remove helpers that merely move
ceremony, and only then return to `feature-complete`; FV1 remains blocked by
both M2 and M4.

### Verification and release: Q1-Q7, S1, M5, FV1

The local CI, integration, concurrency, property, fuzz, Miri, mutation,
allocation, benchmark, doctest, panic-mode, and lifecycle-ordering gates are
feature-complete. Historical pre-AF0 state: M5's 2026-08-13 cross-repository
audit verified each then-locked dependency against its owner checkout and
audited Bombay Entity, Nexus, `mnesis-bombay`, and CESR/KERI. AF0 has since
reopened the comparative inventory: M5 is now `blocked` by S1 as recorded in
the current dependency graph, and FV1 remains blocked by M2, M4, and M5.
Optional M3 artifacts publish independently.

### External handoffs: K1-K3

KERI authority, Zenoh transport/discovery, and authenticated remote actors are
ordered integration-repository handoffs, not bombay features. Local address
identity, KERI authority, Zenoh naming, and remote protocol identity may never
substitute for one another.

## Behavior composition rules

Derive effects from the exact locked Behavior API. Compose heterogeneous lanes
as named semantic send structs, implement `SendAlgebra` plus one `SendInput`
per semantic lane, and emit with typed `SendAlgebra::send`. Positional product
paths no longer exist. Keep `RouteSends`, `ObservesCreations`, and composed
routing errors at the runtime-adapter boundary rather than in domain behavior.
Behavior 0.10's `Delivery<B>` selects the destination behavior. The current
`EndpointRegistry<B, D>` and `DeliveryRouter<B>` implementations are temporary
Bombay runtime compatibility adapters under E5; they are not the intended
application authoring model and must disappear from ordinary examples and the
framework prelude before E5 is feature-complete.

Use Behavior's nominal attribute for plain `User`/`Never` behaviors. Keep
semantic wrappers explicit, and construct wrapper stacks through `Compose` so
only the runtime can cross from definitions into active behaviors.

Audit the entire repository—including examples, benchmarks, tests, research,
diagnostic probes, documents, and public re-exports—for obsolete positional
composition or duplicate protocol implementations before completion. Mark
retained old API descriptions historical.

## Distilled runtime invariants

- One canonical provisional/prepared typestate and `Incarnation` execution
  machinery, exposed as ordinary `System::spawn` or the focused
  initialize-before-register `System::activate` operation.
- Child liveness is retained by an affine typed `ChildLease`.
- Behavior birth mode infers child runtime without manual capabilities.
- Spawn, child creation, and routing contain no trait-object or `Any` erasure.
- Bombay Communication is the sole mailbox implementation.
- Incarnation-local timers distinguish timer identity and generation.
- Actor resources retire before executor terminal classification.
- The exact address lease releases before completion publication, so a public
  terminal outcome denotes a fully retired, reusable address.
- Tokio return, panic, cancellation, and terminal-detachment laws are directly
  tested.

Future work must reopen a concrete item before widening any invariant.

## DR1 final distillation record — 2026-08-16

This record supersedes every earlier DR1, runtime, framework, System, mailbox,
routing, lifecycle, and product-composition proposal in this historical ledger.
Those passages are retained only as decision history and are not normative.

DR1 is `distilled`. The final dependency order was Driver law manifest and
direct causal loop; then affine Incarnation; then transactional Activation;
then exact Address/Observe Generation; then repository closure, template
composition, adversarial verification, and API distillation. No System layer
was introduced.

The implementation consists only of the universal Driver in `bombay-engine`
and the next minimal ownership layer in `bombay-rs::core`. Core owns four small
concepts: `Incarnation`, `Activation`, `AddressPublication`, and
`ObservedRetirement`. The public root remains intentionally narrower than the
technical `core` module. Transition and Machine Executor have no Engine
dependency or adapter; no second production Driver path remains. Transition
continues only in external/template subsystems that independently own genuine
machine topology.

Fresh verification used Behavior/Actors 0.12.0 at
`40b39b2605416e3b88427e3289c4dac4568c78e0`, Address 0.2.0 at the local patched
checkout, Observe 0.1.0, Communication 0.1.1, and Timers 0.1.0. The Actors
owner does not publish a claim that every wrapper pair is valid. Engine proves
all closed meaningful wrapper pairs in both orders and a maximal stack; invalid
event-routing and open-phase compositions remain typed compile failures.

All 74 current `D-*` laws and all 43 exported-template rows are `passing` in
their machine-checked manifests. The completion gates reject missing,
duplicate, renamed, unknown, stale, or unexecuted rows. Workspace build, test,
doctest, Clippy, formatting, compile-pass/fail, allocation, fuzz-build,
benchmark-build, repository-closure, and mutation gates passed. Core mutation
testing caught all 8 viable mutants (one additional mutant was unviable). Miri
is absent from the pinned shell, so no Miri execution is claimed; current
Driver/core code introduces no unsafe code or primitive concurrency algorithm.

## EP1 prepared/active environment-port verification — 2026-08-16

EP1 supersedes the environment shape accepted by DR1; the Driver's causal laws
remain evidence, but DR1's single mutable `Environment::{next, apply, retire}`
surface is no longer authoritative. The owner selected the environment port
itself for redesign and required catalogue-wide tests plus tests using the real
runtime primitives. Existing Bombay System, framework, runtime, mailbox,
routing, example, and historical design code is explicitly excluded as design
authority.

The mandatory feature-local inspection was repeated before implementation:

- locked and locally patched Behavior/Actors 0.12.0 at `40b39b2` owns
  consuming `Activate::initialize`, `Initialized<B>`, `Active<B>`, complete
  `ActionsOf<B>`, the closed event type, births, and named semantic send
  products. Its adapter contract requires initialization commitment before
  ingress, one event/fold/commit at a time, creation before sends, no implicit
  retry or rollback, and later typed capability results rather than re-entry;
- locked Communication 0.1.1 was checked against its crates.io source and the
  local `fastpass` checkout at `e2e8f39`. It owns the bounded user lane,
  unbounded control lane, lane selection/fairness, cancellation-safe receive,
  exact rejected payloads, closure marker, and draining. It does not own actor
  initialization, action interpretation, or terminal classification;
- patched Address 0.2.0 at `7df3bed` owns immediate exact-generation
  `AddressSpace::try_claim`, opaque resolved endpoint snapshots, and affine
  `Lease` release. It supplies the concrete publish operation but no prepared
  actor lifecycle;
- locked Observe 0.1.0 and local source at `43016e5` own exact-generation
  `Subject`/`Observation`, retained single completion, waiter races, and
  cancellation. They supply terminal publication but no Driver lifecycle;
- locked Timers 0.1.0 and local source at `4e515ed` own queue-branded schedule
  generations, replacement, exact cancellation, deadline order, and due
  extraction. Clock sleeping and typed event injection remain runtime-adapter
  work;
- current Bombay Entity, its algebra/runtime research variants, and
  Transition/Machine Executor were inspected as neighboring higher-level
  consumers. They own stable entity activation/passivation or representable
  machine execution, not the universal direct-Behavior environment port;
- `mnesis-bombay` was inspected as an outer persistence integration. Its
  runtime-neutral command execution and Bombay adapter must consume later
  runtime layers and must not move persistence into Engine.

The exact ownership decision for EP1 is that Engine may distinguish a prepared
environment from its active environment because the Driver already knows the
initialization boundary. Successful environment activation consumes the full
initialization actions and returns the only value capable of ingress and later
commitment. Ordinary retirement consumes that active value. Bombay composes
Communication, Address, Observe, Timers, births, and named send interpreters
behind this port; Engine knows none of those mechanisms. `Incarnation` remains
the outer owner of panic/cancellation classification after Driver-owned values
are destroyed.

EP1's required negative evidence is: a prepared environment cannot produce
input or accept an ordinary turn; activation cannot occur twice; ingress
cannot precede successful initialization commitment and publication; a failed
activation exposes no active environment; later actions cannot use the
activation operation; ordinary retirement is consuming and exactly once; and
cancellation cannot claim asynchronous retirement. The existing 43-row Actors
catalogue manifest must remain exhaustive, and real Communication, Address,
Observe, and Timers integration tests must cross the new port before EP1 can
be feature-complete.

EP1 and CR2 are now feature-complete. Engine exposes an affine prepared
`Environment::activate(self, initialization_actions)` transition to an
associated `ActiveEnvironment`; only that active value owns `next`, later
`apply`, and consuming `retire`. `DriverError` preserves Behavior, activation,
and live-environment failures as three exact variants. Bombay Activation now
implements the prepared phase directly, and Incarnation preserves the expanded
terminal classification without acquiring environment authority.

The existing exhaustive Actors evidence remains live: all 50 catalogue and
composition executions pass, the machine-checked 43-row exported-template
manifest passes, and both explicit ignored completion gates pass. A new
compile-fail oracle proves prepared values have no ingress and active values
cannot activate. The real-runtime integration test composes Communication
0.1.1, Address 0.2.0, Observe 0.1.0, and Timers 0.1.0 in one Driver execution:
initialization schedules a real timer, activation publishes a real address
lease, a resolved endpoint admits a real user-lane message, expiration enters
through the real control lane, and consuming retirement releases the address
before completing the Observe subject. Workspace tests, rustfmt, and strict
Clippy pass. Final distillation remains responsible for challenging the
hidden `Direct` conformance adapter, generic error defaults, and naming before
either item may become `distilled`.

### Post-CR2 actor-composition boundary decision — 2026-08-16

The first Akka-style application slice was mapped again onto the exact locked
Behavior/Actors 0.12.0, Communication 0.1.1, Address 0.2.0, Observe 0.1.0,
Timers 0.1.0, EP1, and CR2 contracts before selecting further work. The
current `runtime_primitives` test proves that one concrete environment can
already compose all of those primitives, but it also exposes application-owned
mailbox selection, action interpretation, timer extraction, address
publication, and observation completion. Those are not missing Driver or core
incarnation laws; together they identify the next Bombay-owned runtime
composition boundary.

No further speculative core cleanup is selected. `Driver` remains the
actor-independent causal loop; `Activation` remains the prepared-to-live
publication decorator; `Incarnation` remains terminal classification after
Driver-owned values are destroyed. The outstanding `Direct` visibility,
generic error defaults, and naming questions stay CR2 final-distillation work
and do not block a vertical actor slice. Communication's
`Received::UserLaneClosed` is an input fact, not automatically an actor stop:
the next concrete environment must define and test whether control input can
remain live after that marker rather than baking the decision into Engine.

The next implementation candidate is therefore a narrow, statically composed
actor environment, not a `System` facade. Its acceptance slice is one typed
ordinary actor using the real Communication mailbox and Address publication:
external typed user delivery enters the user lane; initialization actions
commit before publication; later complete Actions are interpreted once;
mailbox closure has an explicit tested meaning; retirement releases the exact
address generation before publishing its Observe outcome. Births, timers,
watching, executor task ownership, handles, and a root guardian remain later
layers, admitted one invariant and one Akka-style vertical slice at a time.
Behavior Actors' named semantic send products and typed `SendAlgebra::send`
remain the only application effect-composition path; application code supplies
only concrete `DeliveryRouter` endpoint selection.

### Minimum next-layer API distillation — 2026-08-16

The proposed `PreparedActor`/`ActiveActor` naming pair was challenged and is
not selected. Engine's `Environment`/`ActiveEnvironment` already owns that
typestate and its authority transition; repeating it in Bombay would add names
without an independent invariant. Likewise, `Mailbox`, `ActionInterpreter`,
`Spawner`, `Handle`, and `System` are not admitted merely because the eventual
framework will need their machinery. The next layer is initially an internal
vertical composition, with public surface admitted only where a caller holds a
new capability.

The one candidate public value with an independent invariant is a typed live
endpoint indexed by destination `Behavior`. It owns only Communication's
cloneable user sender and injects `(origin: B::Addr, message: B::Msg)` through
`B::Event::user`, so callers cannot construct wrapper-owned event variants or
send a message to the wrong behavior protocol. It has no receive, control-lane,
close, address-registration, observation, task, cancellation, or behavior
authority. Whether this value is publicly named now remains subordinate to a
real external-delivery call site; the first implementation may keep it
crate-private and publish it later unchanged as the live-reference payload.

The actor environment itself should be one private concrete composition that
implements the existing Engine traits. Its prepared value owns the
Communication channel pieces, unpublished Address reservation, and a
statically selected complete-Actions interpreter. Activation interprets the
initial Actions, then publishes the endpoint, then yields its private active
value. The active value selects Communication events and interprets later
Actions. It does not create a second public environment trait, public action
sink, public mailbox abstraction, or public lifecycle protocol. Generic named
send-product traversal is Bombay implementation machinery; concrete endpoint
selection remains the narrow `DeliveryRouter` application seam required by the
repository contract.

The minimum API levels are therefore:

1. application level: no new API in the first proof; a behavior value remains
   the application input;
2. advanced routing level: only the existing concrete `DeliveryRouter` seam,
   when the first delivery-capable slice requires it;
3. Bombay crate level: possibly one typed endpoint value, admitted only with
   an external send test;
4. crate-private implementation: channel construction, prepared/active
   environment structs, action traversal, address publication, closure state,
   and error products;
5. existing public lower ports: `Environment`, `ActiveEnvironment`,
   `Publication`, `Retirement`, and the sibling crate primitives, unchanged.

The first slice deliberately uses `NoBirths`, no timers, no observation
inputs, and no executor task. It proves: an initialization action commits
before endpoint publication; a resolved typed endpoint injects exactly one
user event; each complete Actions value commits exactly once; an unresolvable
or closed delivery returns its exact message; the user-lane closure marker is
not silently equated with total mailbox exhaustion; and consuming retirement
releases the address lease. Only after this proof may terminal observation be
composed around it through the already-existing `Incarnation` retirement
port. This ordering prevents outcome publication, handles, and task ownership
from contaminating the minimum mailbox/environment invariant.

ML1 is feature-complete. The implementation adds one crate-private
`core::local` module and promotes Communication 0.1.1 from a test-only to a
production dependency. It reuses `Activation` and `AddressPublication` rather
than adding another prepared type. Its `LocalEnvironment` implements only the
existing `ActiveEnvironment` port; its private `CommitActions` seam receives
the untouched complete `ActionsOf<B>`; and its behavior-indexed `Endpoint<B>`
wraps Communication's non-owning `UserAnchor<B::Event>`. Address resolution
therefore cannot keep the user lane live or resurrect it, while each admitted
message is injected through `B::Event::user` and a closed delivery recovers the
exact original `B::Msg`.

Five focused tests use real Communication and Address primitives. They prove
initial commitment before publication, exactly one complete commitment for
initialization and each turn, exact origin/message passage, address release on
retirement, wrapper-safe injection through Behavior Actors'
`StopOnShutdown`, exact rejected-message recovery from a saved resolved
snapshot, failed-activation non-publication plus mailbox closure, and continued
control delivery after `UserLaneClosed`. The initialization recorder is also
the inversion oracle: publishing first makes its address-absence assertion
fail. Existing Activation and generation inversion suites independently kill
publication reordering, duplicate publication, and lease/outcome reordering.

The repository-wide source and documentation audit found no application
`SendAlgebra`, `SendInput`, positional traversal, or duplicate Behavior
protocol implementation. Current boundary, lifecycle, coverage, adversarial,
performance, and README guidance now records the private local slice without
presenting its fixture construction as user API. The test-only `Fixture` and
bounded `resolve` polling helper remain deliberately local scaffolding; neither
is exported or shown as application usage. The module-level dead-code allowance
is an explicit temporary wart because no executor/reference constructor is yet
authorized; ML1 must not be distilled until a later production constructor
uses the layer or the final audit removes it.

All workspace tests pass, including all 50 Behavior Actors executions. Both
ignored law/template completion gates pass. `cargo fmt --all -- --check` and
strict workspace/all-target Clippy with `-D warnings` pass. ML1 adds no Tokio,
executor, observation, timer, birth, handle, reference, or System API.

### ML1 single-preparation redesign — 2026-08-16

The owner rejected the incrementally accumulated preparation surface and
restricted this cleanup to Bombay Engine and `bombay/src/core` (neighboring
crate contracts remain mandatory evidence, but no other Bombay implementation
area is design authority). The exact locked Behavior/Actors 0.12.0,
Communication 0.1.1, Address 0.2.0, Observe 0.1.0, and Timers 0.1.0 ownership
contracts remain those recorded immediately above: Behavior owns consuming
initialization and complete Actions; Communication owns mailbox lanes and
payload recovery; Address owns exact claim/lease generations; Observe owns
terminal publication; Timers contributes nothing to this slice.

The audit found one legitimate preparation boundary and two duplicate
realizations. Engine's `Environment::activate(self, initial_actions) -> Active`
is retained because it structurally separates pre-ingress from live authority.
Engine's production `Direct<E>`/`Driver::direct` bypass is removed and replaced
only by integration-test-owned adapters. In core, an already-active-style
`LocalEnvironment` was wrapped by `Activation`, which delegated publication to
`Publication`/`AddressPublication`; this is collapsed into a prepared
`LocalEnvironment` implementing `Environment` directly and yielding a distinct
private `ActiveLocalEnvironment` after action commitment and exact Address
claim. There is then one production preparation path and one publication
operation.

`Incarnation`, `IncarnationOutcome`, and `Retirement` remain because terminal
classification across ordinary return, panic, and future cancellation is an
independent invariant. `ObservedRetirement` remains the concrete Observe
adapter. No executor, task, handle, System, timer, birth, supervision, or
application construction API is introduced.

ML1 is `feature-complete`. The simplified surface passes the complete workspace
suite, strict workspace/all-target Clippy, rustfmt, both ignored law/template
completion gates, and the Engine fuzz-target build. The final distillation
removed both duplicate preparation realizations without adding a replacement
public type: production callers construct `Driver` with an `Environment`, and
core's prepared local environment alone commits initial actions and claims the
address before yielding its active form. Historical ML1 and DR1 descriptions
above retain the removed names only as decision evidence and are not normative.

### LR1 launch-boundary verification — 2026-08-16

LR1 was selected only after repeating the feature-local audit against the exact
locked dependencies and their current source, public APIs, tests, and relevant
documentation. Behavior/Actors 0.12.0 owns initialization, the complete
`Actions` product, wrapper-safe `UserEvent::user`, named semantic send products,
and child intent; launch must neither inspect event-product positions nor copy
those protocols. Communication 0.1.1 owns the bounded user lane, non-owning
`UserAnchor`, exact rejected payload, and the `UserLaneClosed` fact. Address
0.2.0 (the workspace's patched sibling checkout) owns exact claim generations,
opaque resolved snapshots, and lease-driven release. Observe 0.1.0 owns retained
terminal publication and remains behind `ObservedRetirement`; LR1 does not copy
it into a task handle. Timers 0.1.0 owns only keyed timer generations and adds no
launch requirement. Engine's `Environment` activation is the sole
initialize-before-ingress boundary, and `Incarnation::run` is already the sole
terminally classified future.

The missing invariant is narrow: a caller may receive a typed live reference
only after that exact local environment has committed initialization and
claimed its address. LR1 introduces no second prepared/active type, generic
executor trait, `System`, registry, cancellation handle, observation protocol,
birth policy, timer policy, or supervision policy. Tokio is the first concrete
task owner. A behavior-indexed local namespace owns only Address's typed space
and Communication configuration; its `spawn` operation launches the existing
Incarnation and waits for the existing activation boundary before returning the
non-owning reference. Application effect commitment is supplied as an inferred
closure, avoiding a new public interpreter trait. LR1 stayed `active` until
ordering, failure, delivery, closure, retirement, and repository gates passed.

LR1 is `feature-complete`. `LocalActors<B>` is the shared typed Address space
and mailbox configuration; `ActorRef<B>` is the non-owning live user
capability; and `LocalActors::spawn` is the only new operation. It constructs
the existing LocalEnvironment, Driver, and Incarnation, launches that future on
Tokio, and awaits a one-shot notification emitted only after action commitment
and exact address claim. The public commit argument remains an inferred
closure. The single undifferentiated pre-publication failure is a unit
`SpawnError`, not a new error hierarchy; exact terminal outcomes remain owned
by the later observation/control layer.

Focused unit tests prove commit-before-reference ordering, complete typed
delivery, exact rejected-message recovery, failed-commit non-publication,
collision preservation, and lease release. `tests/local_launch.rs` proves the
external user path using only Bombay's public core. The repository-wide scan
found no handwritten application `SendAlgebra`, `SendInput`, product-routing,
or duplicate Behavior protocol implementation. The complete workspace suite,
all 50 Behavior Actors executions, strict workspace/all-target Clippy,
rustfmt, and both ignored manifest completion gates pass. LR1 adds no System,
generic executor port, task handle, cancellation API, observation protocol,
timer, child creation, or supervision policy.

### LR1 distillation reopening — 2026-08-16

The owner requested a complete core-layer distillation and real core execution
of every Behavior Actors template. The feature-local audit was repeated against
the same exact locked Behavior/Actors 0.12.0, Communication 0.1.1, patched
Address 0.2.0, Observe 0.1.0, Timers 0.1.0, and current Engine/core source and
tests. Their ownership remains unchanged. The audit exposed two LR1 defects,
not a reason for another layer.

First, LocalEnvironment currently sends only `()` after exact address claim and
`LocalActors::spawn` resolves the address again. An actor stopping during
initialization can release its lease before that second resolution, turning a
successful activation into `SpawnError`. The activation handoff must carry the
exact already-published `ActorRef<B>` and eliminate the redundant lookup.
Second, the optional Tokio one-shot stored directly in LocalEnvironment leaks
the concrete executor into the mailbox/address layer. It must become one
inferred, mandatory, one-use publication callback; only launch owns Tokio.

The Actors audit also shows that core's `NoBirths` bound is artificial:
LocalEnvironment commits the untouched complete `ActionsOf<B>` and need not
understand its Behavior-owned `Birth` product. The bound must be removed so the
canonical template suite, including typed child-intent templates, can cross the
same core environment. The shared canonical scenarios must execute through
real Communication queues, Address claim/release, LocalEnvironment, Driver, and
retirement rather than duplicating a weaker template inventory. LR1 returns to
`active` until immediate-stop, pre-publication failure, every canonical Actors
scenario, repository closure, formatting, lint, and completion gates pass.

### ML1/LR1 final core distillation — 2026-08-16

ML1 and LR1 are `distilled`. The core-wide audit attempted to remove every
remaining public type and operation. `ActorRef<B>` remains the sole live send
capability; `LocalActors<B>` remains necessary to own the shared typed Address
space and mailbox configuration; `spawn` remains the single Tokio launch and
post-activation handoff; `SendError<M>` alone preserves rejected payload
ownership; and unit `SpawnError` alone reports absence of a published actor.
Driver, Environment/ActiveEnvironment, Incarnation, IncarnationOutcome,
Retirement, and ObservedRetirement each retain a separately tested ownership or
ordering invariant. No second preparation, publication, execution, mailbox,
or terminal path remains.

The immediate-stop race is removed: LocalEnvironment's inferred mandatory
one-use publication closure receives the exact `ActorRef<B>` cloned before its
endpoint is claimed, and invokes only after successful claim. Launch returns
that value directly and performs no second resolution. Tokio no longer appears
in the mailbox/address layer. Behavior rejection, interpreter rejection, panic,
and collision before publication expose no reference or address; an actor that
successfully activates and immediately stops still hands back its exact
non-owning reference, which becomes closed after retirement. LocalEnvironment
now accepts every closed Behavior birth mode and forwards the untouched
complete Actions product; the interpreter's private retirement hook completes
before mailbox/address resources are dropped.

The canonical 50-test Behavior Actors scenario source now has two independent
runners without duplicated scenarios. Engine uses its minimal conformance
environment. Core uses real Communication control queues, exact Address
claim/release, LocalEnvironment action commitment, Driver, and active
retirement through Incarnation. Both runs cover every selected template, every reviewed wrapper
edge in both orders, the maximal wrapper stack, and typed child-intent products.
Focused launch tests additionally kill the removed second-lookup mutation and
cover initialization stop, Behavior failure, interpreter failure, panic,
collision, ordinary typed delivery, exact rejection, and release.

The complete workspace suite passes with 50 Engine template executions and 50
core template executions. Strict workspace/all-target Clippy, rustfmt, both
ignored law/template completion gates, repository closure, and the audit for
handwritten application send algebras or positional products pass. Remaining
termination polling exists only in tests because terminal control/observation
is a later independent layer; no test-only operation is public or compiled into
production.
## 2026-08-16 — Simple actor authoring verification

State: feature-complete; awaits project-wide distillation. Priority: next minimum user-facing layer. Unblocks: a runnable
`examples/basic.rs` whose behavior declaration does not expose the complete
Behavior algebra.

The locked runtime uses `bombay-behavior-actors` 0.12.0, locally patched to
`../bombay-behavior/crates/actors`; that crate re-exports the exact
`bombay-behavior` algebra and its sole `#[behavior]` authoring macro. Inspection
of the current macro source and algebra tests confirms that `#[behavior]`
mechanically generates `Behavior` but requires callers to repeat `Addr`, `Msg`,
`Sends`, `Birth`, and `Error`, plus the complete `Acted` return type. Neither the
locked source nor its tests contain the previously anticipated `BehaviorFn` or
`Compose::from_fns` constructor. The convenience syntax can therefore live in
Bombay only as a façade which emits the exact upstream `Behavior` trait and
`Actions` algebra; it must not define a second Behavior protocol.

Observe 0.1.0 owns terminal subjects/observations, Timers 0.1.0 owns timer
tokens and queues, Communication 0.1.1 owns the two-lane mailbox (no sibling
checkout is present), and Address 0.2.0 owns address claims and leases. None of
those contracts participates in constructing a pure Behavior value, so this
feature changes no mailbox, timer, observation, address, delivery, activation,
or retirement contract. The exact output remains the existing typed `Actions`
algebra consumed unchanged by `bombay-engine` 0.1.0. Nominal authoring uses a
Bombay façade `#[actor]` macro that generates only the existing upstream trait
and algebra through Bombay's public re-export; it adds no second protocol and
does not modify the Behavior repository. Functional authoring is explicitly
deferred to a later feature. Advanced named send products and births remain on
the exact existing `#[behavior]` API.

Implemented in the Bombay workspace as `bombay-macros::actor` plus the
Bombay-owned `Effect<S, A = MailAddr>` convenience result. The macro infers the
address and message from `receive`, accepts only the common infallible,
no-birth, no-phase shape, and emits the unchanged upstream `Behavior` trait.
The root `examples/basic.rs` is registered as a real Cargo example and was
compiled and executed successfully. The Behavior checkout has no changes from
this feature. Formatting, the Bombay test suite (83 tests including the public
runtime integration), and Clippy pass; the two pre-existing dead-code warnings
for private `ObservedRetirement` remain after the earlier public API reduction.
