# Exact observation relationship design evidence

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
