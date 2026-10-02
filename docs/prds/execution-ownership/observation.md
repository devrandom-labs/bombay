# Exact observation relationship design evidence

**Historical baseline:** The 0.17.0 selection, names and copied-algorithm replay
below describe the 2026-09-29 research snapshot. Current EXEC selects Core/Actors
0.20.0 and Macros 0.13.0; PRD section 16 and `verification.md` record the fresh
baseline. Current polling uses `TerminationObservations` and ordered vector
removal. The separate established-observation map/task still exists. Its races
need witnesses against the actual compiled interpreter, not the old replay.
No gate is accepted by this reconciliation.

Date: 2026-09-29. Work package: `WP-OBSERVATION-DESIGN`. Decision gate:
`DG-OBSERVATION`. Status: **research complete; gate not accepted**. This note
contains a bounded design candidate and its falsifiers. It does not authorize
production edits or assert that Bombay integration tests have passed.

## Selected contract and current owner

The current `Cargo.lock` selects registry `bombay-behavior 0.17.0` and
`bombay-behavior-actors 0.17.0`; both registry archives' `.cargo_vcs_info.json`
records `435560ce7bea8ad3330ee2d42e5034f837a80602`. I read that revision's
complete `AGENTS.md`. Address is registry `0.2.0`, Communication is registry
`0.1.2`, Timers is the patched Git revision
`13e884da7ab41781f52337b0038060e375b00ee0`, Tokio is `1.53.1`, and Observe
is Bombay-private. No dependency upgrade is proposed.

Behavior Actors owns `ObserveEstablished<P>`, `CancelObservation<P>`,
`ObservationId`, `EstablishedObservation<P>`, and the
`InterpretEstablishedObservation<P>` transfer port in
`src/protocol/established.rs`. `ObservationId` is observer-local relationship
evidence; it is not actor identity or a target generation. The established
recipient transfers the *captured concrete endpoint*, so the exact observation
does not need Address re-resolution. The accepted request's return events are
typed `EstablishedObservation<P>` values. The source archive's
`tests/established_capabilities.rs` verifies the transfer and report algebra;
it does not verify Bombay's asynchronous interleavings.

Observe owns publication and cloning/waiting on a captured termination value.
`ActorRef::termination_observation()` returns the cloneable exact-generation
`Observation<Termination<MailAddr>>`. Communication owns control-lane admission
and returns `ControlClosed(event)` with the exact rejected event. Address owns
logical claim/resolve and exact lease retirement. Timers owns only actor-local
deadlines; the active Environment currently gives mailbox acquisition priority
over observation and timer acquisition. Bombay must own relationship
registration, cancellation, completion, and retirement.

Current Bombay ownership is split:

1. `observation.rs::FactQueue` retains and polls peer/child termination
   observations inside the actor's active Environment. Its pending vector is
   held across a cancelled `next()` poll. It preserves independent observers of
   one target and captures the exact resolved generation.
2. `application_runtime.rs::ApplicationCapabilities::exact_observations`
   stores `ObservationId -> oneshot::Sender<()>` behind `Arc<Mutex<HashMap<...>>>`.
   `EstablishedObservationInterpreter` starts a separate Tokio task for each
   exact relationship in `ActivationTasks`/`JoinSet`, then sends Started through
   the control lane. The task independently removes the ID and sends Stopped.
   Cancellation independently removes the ID and sends Cancelled. Retirement
   drains the map, signals cancellation, and later joins the tasks.

`docs/runtime-capability-interfaces.md` currently says exact observation uses
the actor-local queue and one authoritative queue state. That is a target
description, **not the present implementation**. This contradiction must be
corrected during the selected implementation's documentation migration.

## Original interleavings

The following operations are directly visible in
`application_runtime.rs::EstablishedObservationInterpreter`:

```text
Start(id):   map.insert(id, cancel_sender)
             spawn(task waiting on termination or cancel_receiver)
             control.send(Started(id))

Task stop:   map.remove(id)
             control.send(Stopped(id, outcome, Instant::now()))

Cancel(id):  sender = map.remove(id)
             sender.send(())
             control.send(Cancelled(id))
```

Two valid schedules violate the required law:

- For an already completed target on a multithread runtime, the spawned task
  may run `control.send(Stopped)` before the starting actor executes
  `control.send(Started)`. The unbounded control lane then admits Stopped first.
- The old task can select termination and pause before `map.remove(id)`.
  Meanwhile, the actor cancels that ID, then starts a new relationship with the
  same ID. The old task's ID-only removal consumes the new relationship. It
  may also send Stopped after Cancelled, because `oneshot::Sender::send` fails
  once the old task has selected and dropped its cancellation receiver, and
  the interpreter intentionally ignores that failure.

These are source-level schedules. A temporary isolated Tokio 1.53.1 replay of
those same operations at `/tmp/bombay-observation-gate/src/lib.rs` uses explicit
oneshot gates to force both schedules. The command
`nix develop -c cargo test --manifest-path
/tmp/bombay-observation-gate/Cargo.toml --offline` failed as expected: the
event trace was `["stopped", "started"]`, and the old task removed the newly
inserted ID. Both replays also failed in optimized mode with
`nix develop -c cargo test --manifest-path
/tmp/bombay-observation-gate/Cargo.toml --offline --release --lib <test-name>`.
The replay is a copy of
the relevant algorithm, **not a test of the compiled Bombay interpreter**.
This distinction keeps `EV-16` through `EV-18` open. An attempted focused
`nix develop -c cargo test --locked -p bombay-rs --lib observation::tests`
waited more than 60 seconds for another Cargo build-directory lock and was
interrupted; no in-repository test result is claimed here.

## Candidate single owner

Use the actor's existing `FactQueue<MailAddr, C::Event>` as the one relationship
authority. It already retains `ObservationFuture<Termination<MailAddr>>`, polls
without a task per request, and is shared by the actor's interpreter and active
Environment. Extend its *pending observation representation* to distinguish
peer, child, and established relationships. For an established relationship,
retain its `ObservationId`, captured exact-generation observation, and a
monomorphized injection function for its concrete `EstablishedObservation<P>`
event/path. Keep one observer-local ID namespace across all target protocols.
Do not add a second map, a global registry, a synthetic target generation, a
generic observation service, or another public type.

The actor task alone mutates this authority during request interpretation and
polls it during Environment acquisition. No independent task commits
completion. The proposed linearization operations are:

| Operation | Single-owner commit | Result |
| --- | --- | --- |
| Start with free ID | Insert captured observation into the actor-owned pending collection, then synchronously admit Started before returning from interpretation | New relationship exists; next actor acquisition may observe completion |
| Duplicate start | Detect ID already pending before changing the collection | Emit typed IdAlreadyBound; original observation remains |
| Cancel pending ID | Remove the exact pending relationship during interpretation | Emit one Cancelled; removed future cannot later produce Stopped |
| Cancel absent ID | Lookup finds no pending relationship | Emit typed NotObserved |
| Completion | `FactQueue::next` polls the completed observation, removes that same entry and returns its injected Stopped event directly as Environment input | One terminal event; later cancellation finds no relationship |
| Actor retirement | Drop pending observations with the actor Environment after accepted control ingress is drained | No live wait/task remains; already admitted control events retain normal residual custody |

This model needs no relationship generation token because no old asynchronous
agent can act after the owner removes a pending entry. Reusing an ID creates a
new entry with a new captured observation; a stale completion has no remaining
authority. Two independent IDs observing one target remain two independent
`ObservationFuture`s and terminal events. Peer and child observations remain
uncancellable and keep their existing typed report forms.

The queue must not be extended by hiding the exact protocol in `Any`, a trait
object, or an untyped event. Its current static injection-function pattern is
the bounded ordinary-Rust candidate, but the generic typing and event/path
proof still require a compile witness before selection. Inspect whether the
existing vector can own ID lookup and removal directly; do not add a second
index merely to optimize an unmeasured scan. A closed sum for pending source
kind is permitted only if it owns these actual alternatives and deletes the
old map/task state.

## Ordering, custody, and explicit limits

Start emits Started during action interpretation. `FactQueue::next` cannot
return Stopped until the actor resumes acquisition after that interpretation;
on initialization, the Driver likewise completes initialization
interpretation before active acquisition. The existing `local.rs` `tokio::select!`
is biased toward mailbox input over queued observations, so admitted Started
remains before Stopped even when the target was already terminated.

If a cancel event and target completion are both ready, the existing mailbox
priority lets cancel interpretation remove the pending relationship first.
If `FactQueue::next` has already removed and returned Stopped, the actor
processes that event before any later cancel action, which must return
NotObserved. These are actor-serialized decisions; the target may publish
concurrently, but publishing alone does not consume a Bombay relationship.
That distinction is deliberate Bombay policy, not an actor-model theorem.

The proposed completion path returns the typed event directly through the
Environment, so it does not use an external control send that can fail after
the consumer closes. The current task path *does* preserve failed sends:
`ActivationTasks::settle()` collects `Err(event)`,
`LocalResidual::settle_activation_tasks()` appends those events to drained
control ingress, and `launch.rs::settle_local_outcome()` runs this on terminal
paths. Any replacement must preserve already admitted control events and the
exact terminal residual; deleting `ActivationTasks` globally is outside this
gate because other capability tasks may remain.

Polling a completed observation records `Instant::now()` when the actor
selects it. The present spawned task records time when that task selects and
sends completion, not necessarily the actual target termination instant.
The selected design must document its timestamp as observation/notification
time; claiming publication time would require an upstream timestamped terminal
fact. `FactQueue::next` currently scans its pending vector in order and removes
with `swap_remove`; preserve the existing selection behavior unless a separate
policy decision changes it. With a continuously ready mailbox, the biased
`select!` can starve observations and timers. No bounded progress guarantee
is inferred.

## Required in-crate witnesses before accepting the gate

1. Reproduce the original old-task ID-reuse defect and Started/Stopped order
   in a temporary Bombay test or instrumented isolated checkout. The test
   must fail for the intended invariant on the original implementation. Keep
   final regression tests in the owning `crates/bombay` module, not the
   external replay crate.
2. Compile the exact queue representation with two unrelated concrete target
   protocols and two distinct injection paths; ensure cross-protocol ID
   collision is rejected in the same namespace. Check no erased state or
   second ownership map appears.
3. Drive the complete operation table: immediate completion, duplicate start,
   both cancel/completion orderings, cancel twice, replay completion, reuse
   after cancellation and after completion, and two independent observers of
   one target. Assert the complete typed event trace and retained endpoint
   generation, not only map size.
4. Race retirement against completed notification and control closure. Verify
   every accepted or rejected event's residual custody, and verify no
   observation task/registration remains. Preserve source priority and prove
   actual fairness limits with simultaneously ready mailbox, observation,
   source input, and deadline.
5. Run these focused tests through `nix develop -c cargo test --locked ...`
   in debug and release, then the PRD's applicable integrated gates. The
   current copied-algorithm replay does not substitute for either run.

The gate remains open until these witnesses select the concrete representation
and the coordinator records its source ownership and module path. If direct
actor polling cannot preserve a required typed event, timestamp, or custody
law under the locked contracts, report the smallest failing program and exact
owner dependency; do not add an unproven task/map fallback.


## Observation identity research (2026-10-02)

The user requested comparative research and pros/cons before selecting the
cross-protocol cancellation law. In selected Actors 0.20, `ObservationId` is
observer-local relationship evidence; `CancelObservation<P>` carries that ID
plus a protocol marker. Its `Cancelled` report contains only the ID and marker,
whereas `Stopped` contains the exact termination outcome and notification time.
The marker alone does not establish that cross-protocol cancellation
misclassifies a target. Verify whether it denotes the requesting event lane or
constrains the registered target; no policy is inferred from storage convenience.

[Erlang demonitor](https://www.erlang.org/doc/apps/erts/erlang.html#demonitor/1)
uses the monitor reference. It prevents future DOWN admission and separately
offers removal of an already queued notification. [Akka typed watching](https://doc.akka.io/libraries/akka-core/current/typed/actor-lifecycle.html#watching-actors)
uses the target ActorRef; unwatch suppresses termination processing even if the
notification was queued. These are distinct owning policies, not a universal
cancellation law. Bombay's PRD explicitly preserves already admitted events.

ID authority fits the current request and one observer-local namespace, but
requires truthful acknowledgement semantics and cross-protocol tests. A
protocol-matching law could prevent cross-lane cancellation, but requires a
verified static owning contract and a truthful mismatch outcome; it cannot
silently call a globally live ID absent. This paragraph records the
pre-selection comparison; the later amendment below records the user-selected
exact relationship law. Representation research remains open. Same-protocol
original-race experiments are independent evidence.


## Compiled original-race failures (2026-10-02)

Both race laws now fail against the actual Bombay interpreter from baseline
`2fccedf6eb636ac22143e7e01de7e784f96e2b4e`, in debug and optimized builds.
The isolated snapshot is
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-observation-interpreter-o0yu2gdh`.
Its unchanged lock hash is the PRD section-16 hash. The only feature difference
is Tokio `rt-multi-thread` for the contested valid schedule. Test-only barriers
pause the real interpreter between spawn and Started admission, and pause its
real completion task between selection and ID removal. No copied algorithm
stands in for the production authority.

`completed_observation_admits_started_before_stopped` observes the complete
trace `Stopped(41, Ok(Normal), at), Started(41)` rather than the required
`Started(41), Stopped(41, Ok(Normal), at)`. The exact notification timestamp is
retained and bounded by surrounding instants.

`old_completion_cannot_consume_reused_observation` observes
`Started(41), Cancelled(41), Started(41), Stopped(41, Ok(Normal), at),
Rejected(41, Cancel, NotObserved)` rather than
`Started(41), Cancelled(41), Started(41), Cancelled(41)`. The replacement target
is a distinct captured endpoint of the same protocol. Releasing the old
completion consumes the replacement registration and admits the old terminal.
The live consumer accepted all notifications; settling the real capability
tasks returned no rejected event.

Each command below ran once for each named test, then again with `--release`:

```sh
nix develop -c cargo test --locked \
  --manifest-path /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-observation-interpreter-o0yu2gdh/Cargo.toml \
  -p bombay-rs --lib completed_observation_admits_started_before_stopped
nix develop -c cargo test --locked \
  --manifest-path /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-observation-interpreter-o0yu2gdh/Cargo.toml \
  -p bombay-rs --lib old_completion_cannot_consume_reused_observation
```

All four compiled and exited 101 at the intended complete-trace equality
assertion. These are original-defect failures, not compiler denials or a fix.
Experiment patch SHA-256:
`0769a59d268ca3dea5f3f552ca0f55919ca99384600aa823f258ee468b9bbb22`.
Instrumented interpreter SHA-256:
`089481674f5b8dda7e4de66707e1e278b990c1e108e58929d6e7eb896a9c69c7`.
Isolated patch delta: two files, +159 / -3; no repository production/test or
public-API change. Temporary hooks are not a selected runtime interface.
A repaired candidate, full operation-table witnesses and independent review
remain required before DG-OBSERVATION can be accepted.


## Selected authority law and owning prerequisite (2026-10-02)

The user selected cancellation of the **exact relationship**, with target
protocol checked statically and acknowledgement routing separate from that
authority. This includes distinguishing an old cancellation from a newer
registration that reused its numeric ID and protocol. See the visible PRD
section-16 amendment. This is product-law selection, not DG acceptance.

The locked CancelObservation request has only a public ObservationId and a
protocol marker. Same-protocol old/new requests are indistinguishable. Fixing
the independent-task race through single actor polling cannot supply missing
request authority. The owning Behavior Actors contract must first express the
selected law, with verified producers, receipts, consumers and version edges.
No specific token or generation representation is accepted.

The ordinary actor-polling comparison against the old ID-authority law is
frozen at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-observation-polling-10_5pz_x`.
It compiles with no new public type or bound, removes the separate map/task,
and preserves acquisition order. Seven new complete-trace tests, five existing
observation tests and one exact start/cancel witness pass in both profiles.
Its ID-only cancellation fails eligibility for the newly selected authority
law and is not retained. Patch SHA-256:
`beead413a364319d8c8247216e56b73ed09179269bc7b7aa5b8144661cd9f198`.
Isolated production +79 / -86 / net -7; tests +559 / -2 / net +557;
public API zero. Repository production/test delta zero. Complete command and
candidate-review receipts remain required before any retention decision.

Independent original-race evidence review: `/root/contract_inventory`, which did
not author the race probe, inspected patch
`0769a59d268ca3dea5f3f552ca0f55919ca99384600aa823f258ee468b9bbb22`,
instrumented source
`089481674f5b8dda7e4de66707e1e278b990c1e108e58929d6e7eb896a9c69c7`
and all four logs. The reviewer confirms intended compiled trace failures with
the selected unchanged lock. The global research hooks require separately
filtered execution and are not parallel-safe retained tests. This is evidence
review only; no candidate or DG-OBSERVATION gate is approved.


### Ordinary-Rust exact-authority feasibility comparison

The isolated comparison at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-observation-authority-j8_mkbis`
uses the selected Behavior/Actors rlibs and no new macro. Its candidate has a
private fresh `Arc<()>` relationship identity compared by pointer identity,
and a protocol-indexed authority containing that identity and numeric ID.
Fresh issuance cannot recreate an existing identity; cloning preserves it.
The identity remains allocated while any stale authority exists, preventing
allocator address reuse from aliasing that authority. Cancellation requests
carry the authority. Report alternatives preserve exact rejected start/cancel
requests and the accepted cancellation request; termination retains relationship,
outcome and timestamp. This is a feasibility comparison, not an upstream API
implementation or an accepted naming/representation decision.

Three pure identity/custody tests pass in each profile: repeated/stale
cancellation, another observer with the same protocol/ID, and two protocols
using the same ID. Wrong-protocol and numeric-forgery fixtures each fail with
E0308 in both profiles. Tests use a small pure cancellation comparison, not
Bombay's interpreter; they cannot establish integrated linearization or
retirement. The separate compiled Bombay stale-request witness fails the
selected law in both profiles even after the ID-only queue migration.

Receipts: `opaque_authority.rs` SHA-256
`c65c1ee25a8d6543263f37b432f60c6dfa6d0eee5ba02a80eaf116686f0bc31c`;
exact eight pinned-Nix commands in `opaque-authority-commands.txt`, SHA-256
`bd1e7ab3ae009cc3a15d6a158b6abee8d93efc27e7e73d17302a967d07233bac`.
Both compilation commands exit zero, both test commands pass three tests, and
the four denial commands exit one with the intended type mismatch. The
compiled stale-request patch SHA-256 is
`4f2c1c2d59ed6eccae4a0953a2c669db0aa7a38f24c8107460ddcac4f8301f79`;
it adds 41 test lines to the frozen queue comparison and both runs exit 101
at the intended conservation assertion.

Scratch feasibility delta: prototype +48 / -0; embedded tests +92 / -0;
denial fixtures +16 / -0; two new public types plus two replacement protocol types. Retained
repository/upstream production, tests and public API delta: zero. These
scratch types are not approved additions. The existing
`TerminationMonitorWith`/`EstablishedTerminationTarget` consumers retain only
a numeric ID and a Copy phase. They must also consume and retain the exact
Started receipt and validate subsequent reports. A protocol-file-only
upstream proposal would omit required consumers and is rejected as incomplete.
Full ordinary-Rust alternatives, interface/ownership review and concrete
upstream scope remain required. DG-OBSERVATION remains open.


### Cross-crate comparison and frozen owning candidate

The actual cross-crate comparison tests two new opaque public types against
one authority type plus the existing RecoverEvent port. A manually authored
nominal observer implements lawful InjectEvent for two protocol paths and an
independent acknowledgement path. The two-type comparison compiles and passes
one test in each profile without a new observer bound. The single-type
comparison retains the entire root event to recover authority and additionally
requires RecoverEvent; the same InjectEvent-only observer fails with E0277
for that missing bound in both profiles. Private Arc extraction works in a
single-module probe but cannot cross the actual Actors/Bombay crate boundary.
Exposing raw allocation identity is an unaccepted public interface choice.
These comparisons establish interface costs, not an accepted production model.
The standalone probes cover different amounts of behavior and their line
counts are not an apples-to-apples reduction claim.

Artifacts reside at the authority-j8_mkbis directory above; exact ten commands
are crosscrate-authority-commands.txt. The two-type nominal consumer is
nominal_two_type_consumer.rs; the one-type nominal denial is
nominal_single_type_recovery_denied.rs. The earlier revised one-type recovery
comparison preserves the original event, exact termination outcome and
timestamp in an ordinary tuple on recovery rejection. Independent review
required that correction; its passing pure tests do not establish consumer
or runtime integration.

An isolated actual Actors library candidate at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-observation-owning-api-q16eql88`
was frozen after library compilation. The actual artifact directory suffix is
owning-api-q16eql88; its owning-api-frozen.patch SHA-256 is
`03c42b323334047408eb5281ebe30e16848b16e657b98c9d7093c9fc941a2942`.
Four owning production paths change: protocol/established.rs, protocol/mod.rs,
lib.rs and lifecycle/termination_monitor.rs; scratch manifest/lock bring the
count to six. Raw unformatted research production delta is +99 / -191 / net
-92, tests zero, two new public types. Compressed research syntax is not
distillation or credible evidence of production reduction. No retained
repository or owning-checkout production changed.

The candidate mints request identity in ObserveEstablished::new, issues
protocol authority through the existing interpretation transfer, and makes
the monitor retain the original request identity. Library compilation succeeds
against the selected Core; owning consumer tests remain untouched and broken
API migration/integration is not claimed. The candidate is blocked because
new request identity has observable pointer equality. The locked Core requires
pure deterministic folds and provides checked, state-owned CreationSequence
correlation, distinct from fresh runtime actor allocation. Heap allocation is
allowed; no rule authorizes treating semantically fresh request names modulo
alpha-renaming. Moving creation into a monitor constructor does not solve it: a
parent may construct that monitor inside a pure birth fold.

Reviewer `/root/contract_inventory` independently confirms this unresolved
owning-law issue against established.rs SHA-256
`f351de778f9b2ec27474ba366b324bb244e4d3c8bd6b89a09daa2b79d90a66de`
and monitor SHA-256
`6a256eaf03522ebf95e07662ff11670e13f2bfa5845aad6f7e5b2d260e5fa87f`.
No gate or candidate is approved. In response to the pending decision, the
user authorized the stronger exact preaccept request-correlation direction
with “yes authorize everything” and “just do these changes”. The earlier
exact cancellation law remains selected. This selects the required fact,
not an initialization API or permission to weaken fold determinism.
Dependent owning edits remain blocked until a concrete owning contract,
measured scope and independent review are established.


The exact static-route research confirms ReturnToEmitterFor proves
InjectEvent<Input,Path>, and Bombay injects into the emitting actor's original
path. It does not brand actor/request provenance. A pure composition forwarding
a genuine report through EventLayer::Owned to another same-protocol monitor
compiles in both profiles: forwarded_observation_report.rs SHA-256
`ed26595e57b2662f714c253b72823a46728b2e88cc68b8502d57123fe56a47ea`;
commands SHA-256
`20d0c9715c1c0fd45c734d217d79c6100a640114a610748d42b7156b1bf80eed`.
This does not assert that Bombay itself misroutes reports. A deterministic
local ordinal alone collides across observers; static roles do not distinguish
runtime instances. An opaque namespace supplied as explicit initial state or
input plus a checked ordinal could conserve purity, but current initialization
turns contain only phase authority and interpreter capability inputs do not
enter Behavior state. A general stronger preaccept law therefore needs a
verified owning input/continuation contract. The stronger product law is
selected; its representation remains unresolved. No new scope input, service
or effect has been invented.

## Ordinary affine inputs before a fold (2026-10-02)

Fresh ordinary-Rust comparison shows that a generic Core initialization change
is not established as necessary. A root input supplied before a fold can own
an immutable origin and checked affine branch counter. Deriving child inputs
from that known state is deterministic; actual selected `CreateChild` products
accept those already-constructed child Behaviors. The selected Core has no
`Behavior::allocate` or `Allocations` seam; Bombay's allocation inputs remain
private to its interpreter and are not incarnation identities.

The authority-j8_mkbis directory contains `affine_request_births.rs`, SHA-256
`a2f6ce410e4e183ed21fa7221c785ab37d3a037bdc0f89c69bd5f84d04d641f8`.
Five tests pass in debug and optimized builds, including actual pure root/child
folds with complete typed research request lanes. Identical known input/state
replays identically; sibling branches and separate root inputs remain distinct;
checked exhaustion does not wrap. Reusing the moved child input fails E0382
in both profiles. Command receipt SHA-256:
`d485f3a2ebff6b29c98c4098c181fd29275ec85f8fef51659f552607439e21aa`.
These are correlation feasibility witnesses, not integration with the actual
ObserveEstablished protocol, monitor or Bombay interpreter.

The proposal `affine-observation-input-proposal.txt`, SHA-256
`4733ecb8d7ff5e21629e4cae501a38dae8cd99f35b3486b76fda6cc8817e673a`,
forecasts three distinct public laws: affine request sequence, protocol-indexed
accepted authority, and non-authorizing relationship comparison. It is neither
an accepted API nor authorization for owning edits. Root issuance placement,
full consumers and failure custody remain unresolved. In particular, the draft
`observe(id, recipient) -> Option<Request>` loses exact inputs on exhaustion;
that shape is rejected pending a conserving ordinary-Rust comparison. Fresh
nominal issuance inside a fold remains forbidden. The earlier broad Core-input
migration counts are alternative costs, not proof that such growth is required.

## External typed request and receipt comparison (2026-10-02)

Scratch
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-observation-request-receipt-up9fwt6x`
contains 383 formatted private research lines and no owning production/API
change. Receipt SHA-256:
`cb0e2c046625108f1e39f70b68baac8cbc00ac8ee33662d1f77b8213bf1b4e23`;
source:
`4f099be052cea823489ba6f493b00ffd0aad5cd8469a9dbc5f3f7fcdec3af59a`.
Every selected registry/git dependency tuple matches Bombay's locked source.

Actual selected ObserveEstablished transfers the original endpoint through a
cloned existing recipient capability while a private stamped request remains
owned. Existing Core SendLayer aligns independent nested acknowledgement lanes;
a bare request lane with a nested report fails the actual SendsFor constraint.
The pure fold returns complete typed cancellation actions, and the rejected
report is retained in ordinary state. Checked sequence exhaustion borrows the
recipient so the caller still owns its exact endpoint. Replaying the same ID
and request correlation at the interpreter-side acceptance boundary produces
distinct accepted relationship identities; an old authority is rejected whole
against the new grant. No nominal identity is issued inside a fold.

Pinned-shell `cargo test --locked` and its `--release` variant pass three tests
each. `cargo check --locked --features wrong_protocol` fails E0308 and
`--features wrong_ack` fails E0277 in both profiles; strict Clippy and formatting
pass. All commands and fourteen artifact hashes are in the frozen receipt.
Independent reviewer `/root` read the complete source, authenticated those
hashes and compared resolved dependency tuples with Bombay's Cargo.lock.
This accepts the narrow endpoint, pure lane, identity, rejection-custody and
static-path feasibility claims; no gate is accepted.

Private request/report stand-ins are not the current owning public protocol.
Actual sealed Monitor integration, public issuance/static denials, heterogeneous
Stopped report custody, complete live-operation/replay tables and all original
interpreter regressions remain required. Actual cloned endpoint transfer does
not prove every future owning rejected-start receipt retains its complete input.
Existing FnMut worker factories can capture an affine sequence as pure explicit
factory state; this does not justify a universal Core input port or macro change.
The real factory/rejected-birth witness remains required. Public representation,
full owning path forecast and user change-budget approval remain open.

## Existing sealed Monitor ownership comparison (2026-10-02)

The isolated `bombay-observation-monitor-custody-cc54p8j_` stage exercises the
selected 0.20.0 generic TerminationMonitor with a private proposed target
implementing its actual sealed owning trait. Original production source is
unchanged; all 171 other copied Rust files byte-match the selected archive.
All 58 external dependency tuples match Bombay's current lock. No Core or
macro source change is needed for this comparison.

Three tests preserve the whole existing ObserveEstablished request and original
recipient on rejection, preserve the whole existing CancelObservation plus
protocol-indexed authority and exact rejection reason, and reject a foreign
Started report without consuming the expected request. Existing request
interpretation transfers the original endpoint allocation. Nested typed event
products keep acknowledgement routing separate from target protocol; wrong
target and wrong acknowledgement path compile-deny with E0308 and E0277 in both
profiles. Accepted Started replay is rejected by the existing Monitor phase.
Two host-issued acceptance identities for the same request correlation stay
distinct; this is not a runtime replay-admission policy proof.

The actual selected numeric-only EstablishedTerminationTarget accepts a
same-protocol/same-ID Started report associated by the test host with a distinct
recipient. Its intended correlation assertion compiles and fails one test per
profile. This exposes the report's missing distinguishing information; it is
not an end-to-end runtime delivery/reachability proof. The private candidate
passes three tests per profile, formatting and strict Clippy.

Receipt SHA-256:
`67c88dccd727df198958bc690c2e5585e66cb453bc31a04598f071322ff30a15`;
patch `7725b8aae65a14a754167708a13a836f2adf7a481c1ae3f205ccf5866b394e1f`.
The receipt records exact pinned-Nix commands and excluded preliminary runs.
Independent reviewer `/root` read the complete patch, authenticated twenty
artifacts, compared all unchanged sources and verified selected dependency
tuples. It accepts these bounded comparison claims, not DG-OBSERVATION.

One copied owning test module adds 565 lines; production/public types zero.
Private request/report stand-ins describe prospective fields on existing types,
not extra public wrappers. The host issues authority before later consuming the
original request; actual admission-before-Started ordering is unproved. Public
issuance, heterogeneous live relationship collection, completion/reuse races,
terminal authority discharge, accessors and full runtime integration remain
required. No public producer or representation is selected from this test.

## Whole-request producer feasibility (2026-10-02)

Frozen `bombay-observation-producer-custody-2tnj4sf5` adds 249 test lines to the
copied owning established.rs, retaining the preceding signed 565-line monitor
unchanged. The other 170 Rust files match selected Actors 0.20.0. A private
host consumes the whole ObserveEstablished through the actual affine Core
callback, inserts collection membership, then issues private typed authority
and Started. Duplicate admission returns the original request, supplied
correlation and reason; consuming it through the selected interpreter recovers
the exact original endpoint allocation and ID. Missing-member cancellation
returns the whole existing request and grant with NotObserved.

One test passes per pinned-Nix debug/release profile; formatting and strict
Clippy pass. Both profiles deny passing the whole request to the current
numeric-only public interpreter (E0308) and deny constructing authority from
its private fields (E0451). These compiler comparisons demonstrate an owning
API gap and privacy, not a selected new public producer.
Receipt `291b36cc59dbed26d48a454318cda8e6724801c12f917b81865c8307998a854d`;
patch `a0338363a8af5dfc1e85de95eb5050d6cdd491379a5d367c4cecabf3bd4f244e`.
Independent reviewer `/root/contract_inventory` read the complete extension,
authenticated all 18 artifacts and unchanged sources, and signs bounded
whole-request custody and admission-order feasibility only. Production/public
types zero; two copied test paths now contain 814 research lines in total.

The cancellation comparison checks authority membership but omits equality of
the original numeric request ID and authority's ID. A mismatched request with a
valid grant would therefore pass; the empty-member rejection test does not
prove this denial. Immutable scope correction:
`362a7839a16bfba76f37c466d438b3598918fa50b8785bf8c4d6dff634fe2f9b`.
A bounded isolated successor may prove that consistency without changing this
freeze. External public issuance, actual async collection, accepted removal,
replay policy and terminal discharge remain unselected/unproved. No full gate
or retained production is approved.
