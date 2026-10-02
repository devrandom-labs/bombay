# Core integration: supervisors, pools and upstream changes

Source investigation: 2026-09-29. This report narrows the completion inventory
to executable core actors and the changes required in their owning repositories.
It does not depend on Zenoh, Selo or Mnesis. No production implementation was
changed during this investigation.

The selected 0.20.0 Behavior Actors release now supplies the diagnostic ingress
and split preparation contract described as missing below. The dated 0.17.0
investigation remains prior-representation evidence; use the ARC-010 retained evidence in [the status index](status.md) and
current lockfile for implementation status.

## Historical 0.17.0 revision and interpretation

At the time of this investigation, Bombay selected Behavior and Behavior Actors 0.17.0, registry source revision
`435560ce7bea8ad3330ee2d42e5034f837a80602`; Behavior macros 0.12.0; Address
0.2.0; Communication 0.1.2; private Observe; and Timers 0.1.0 patched to
`13e884da7ab41781f52337b0038060e375b00ee0`.

The sibling `bombay-behavior` checkout was also inspected at HEAD
`de8f1bf264366da29992c0061d5bae9888353bd5`, with existing uncommitted changes.
Its assignment/proxy rejection access and generic vector settlement discharge
still show the gaps described below. That checkout is evidence only; it is not
Bombay's selected dependency and was not modified.

The distinction is **implemented pure policy versus executable interpretation**.
Behavior Actors already defines the state transitions, correlations, restart
budgets, activation plans, pool assignments and shutdown policies. Its pure
tests supply many host outcomes directly. Bombay must produce those same
outcomes from real child tasks and delivery attempts. Several required
interpreter leaves are absent, and some upstream contracts do not yet permit
the full ownership-preserving implementation.

## Exact dependency paths

| Public composition | Existing owning policy | Missing runtime path |
| --- | --- | --- |
| StableProxy | Worker start/replace/stop and stable logical forwarding | InitializeWorker, BeginActivation, and truthful child startup outcome integration. |
| FixedSupervisor | Role topology, recovery/restart policy, stable proxy children | ProxyOperation and DiagnosticAction, plus the StableProxy path. PrepareWorkers already exists. |
| DynamicSupervisor | Dynamic membership and proxy operations/outcomes | ProxyOperation and DiagnosticAction, plus the StableProxy path. |
| FifoPool | Backlog, ordered assignment, recovery and customer outcomes | InitializeWorker, BeginActivation, AssignWorker and DiagnosticAction. Existing generic delivery handles its ordinary customer lanes. |
| KeyedPool | Key binding, assignment, correlation and recovery | The FIFO atomic capabilities plus CustomerDelivery with retained rejected-customer routes. |

Sources: [selected template/capability manifest](../driver-template-manifest.json),
[Bombay interpretations](../../crates/bombay/src/application_runtime.rs).
The manifest now records all 19 actor-owned capability types as implemented;
this excludes ordinary structural lanes and is not a completion score. The
earlier Behavior Actors 0.19.0 fixed-supervisor diagnostic parent-ingress
gap blocked the executable supervisor probe. The current 0.20.0 release and
Bombay runtime tests cover that path; see ARC-010 in [the status index](status.md).

## Already implemented: retain and compose

- PrepareWorkers has a concrete interpreter and WorkerPreparationSource port.
  First-role source rejection returns the full request; admitted preparation
  can return a role-specific failure while retaining the source for recovery.
  Three focused tests passed in this investigation.
- Creation routing, child establishment, logical/exact delivery, child input,
  observation, shutdown, timer requests and parent/terminal reporting already
  have runtime paths. Presence of a path does not prove every new atomic race.
- Existing child bindings own endpoint/control/task custody. ActivationTasks
  already owns async capability tasks and returned control events. Reuse those
  owners before introducing anything new.
- Driver already supports explicit retained settlement custody. It should not
  acquire supervisor/pool-specific branches.
- ActorRef, external interfaces and Entity's local stable routing already
  exist. Completing core supervision does not require distributed placement.

## Required work in bombay-behavior

The repository contains both foundational Behavior and Behavior Actors; these
are separate semantic owners even though upstream changes share one repository.

### 1. Behavior Actors: exact assignment rejection return

Confirmed blocker in `crates/actors/src/atomic/pool/assignment.rs`:

1. AssignWorker owns the exact target, move-only assignment and receipt.
2. `into_parts` is public and transfers those values for delivery.
3. A closed recipient returns the assignment from Communication.
4. Behavior requires `ItemSettlement::Rejected` to own the complete original
   AssignWorker, not just the job or a reason.
5. AssignWorker's fields are private and `returned` is restricted to
   `crate::atomic`. Bombay cannot perform that reconstruction.

Required owner contract: consume an assignment attempt and provide a lawful
accepted receipt or exact rejected original request. Compare an owner-provided
consuming attempt operation and an affine return continuation. Do not simply
expose unchecked parts reconstruction: a receipt/target from one operation
must not be paired with another assignment. Do not require Clone on jobs or
claim that a preflight liveness check eliminates the close race.

Upstream acceptance: move-only payload; accepted and closed delivery; close
between resolution and send; foreign receipt/assignment denial; double-return
denial; observable preservation of target, assignment and correlation. Include
a downstream interpreter fixture outside the owning crate's privacy boundary.

### 2. Behavior Actors: exact proxy-operation rejection return

Confirmed blocker in `crates/actors/src/atomic/stable_proxy/operation.rs`:
`into_parts` transfers creation, ProxyControl and operation identity.
ProxyInputReceipt has a public accepted constructor, but there is no public
inverse that retains the exact rejected ProxyOperation. Rejection before
decomposition can return the request; rejection after attempted control
admission cannot be solved by pretending the attempt never occurred.

Required owner contract: the same complete ownership equation as assignment,
while preserving proxy creation, operation identity and source routing.
Compare whether one existing lower-order construction can express both laws;
do not publish a generic abstraction merely because the shapes look similar.

Upstream acceptance: missing creation, retired control target, accepted input,
close-race payload recovery and prevention of cross-operation substitution.
The current `proxy_operation_settlement` test checks type compatibility; it
does not attempt a real rejected downstream delivery.

### 3. Behavior / Behavior Actors: terminal diagnostic custody

This investigation found and reproduced an additional blocker beyond the six
missing interpreters:

- DiagnosticAction can return `DiagnosticAccepted::Terminal(diagnostic)`.
- Its InterpreterRequests settlement is `Vec<ActionItemResult<Request>>`.
- Behavior's blanket SourceSettlementCustody implementation for that vector
  always returns `SourceCustody::Exhausted(self)`.
- On a continuing turn, Driver discards Exhausted settlement custody.
- FifoPool's `diagnose` can emit the diagnostic with `Actions::cont()`;
  terminal retention does not imply the actor itself stops on that turn.

Therefore, a naive interpreter returning Accepted(Terminal(...)) would lose
the value on that continuing path. Immediate actor stop is a different path
and must not be used to claim this continuing path is safe.

Required owner contract: explicit typed accepted-value disposition that can
retain terminal evidence without turning every successful ordinary send
receipt into permanently retained state. Alternatively prove an existing
owner composition can transfer that custody lawfully. The generic Behavior
settlement owner and Behavior Actors diagnostic policy must agree. Do not add
a Driver branch that recognizes diagnostics by name or force the actor to stop
when the selected policy only requested custody.

Upstream acceptance: a non-Clone diagnostic survives several later turns and
is present in final retirement; ordinary discharged receipts do not accumulate;
delivered diagnostics are not retained as undelivered; rejected routed
diagnostics preserve their request; mixed source-action products still progress.
Test debug and optimized execution, plus an independent drop/ownership witness.

### 4. Joint contract decision: child creation versus initialization outcome

This needs a joint decision, not an assumed new API. The current Bombay
`establish_child` awaits `spawn_owned_with`, which waits for activation
publication. Behavior Actors then emits InitializeWorker for the established
worker and expects ReadyForActivation, EffectsRejected or Stopped.

Bombay must correlate the real already-performed initialization outcome; it
must never call Behavior initialization again. Specify what creation success
proves, when an initialization failure is reported as a creation result versus
a later worker result, and where the complete settlement/terminal custody lives.

Current Bombay child establishment converts Panicked, Cancelled and Ended
startup results into a panic. The selected ChildCreationOutcome only has
Established, InitializationRejected and HostRejected; HostRejected specifically
contains initialization Actions that have **never been interpreted**. Already
consumed effects cannot be reconstructed into that alternative.

Required experiment: trace initial fold rejection, rejected initialization
effects, interpreter corruption, immediate stop and panic through creation,
InitializeWorker, independent observation and parent retirement. Reuse an
existing lawful outcome if possible; if one is absent, change the exact owning
Behavior creation contract and its Behavior Actors consumers together. This
is an unresolved contract scope, not a proven need for four new public types.

## Required decision in bombay-address, then Bombay

Address 0.2.0 `try_claim` immediately inserts a resolvable endpoint. Bombay's
LocalEnvironment calls it **before** committing initialization. The existing
ignored publication regression fails with `Present` versus expected `Absent`.

Moving the claim after commit is not sufficient by itself: a later claim can
fail, while earlier initialization effects may already have happened. The
current HostRejected product cannot truthfully return those consumed Actions.

Recommended first experiment: an Address-owned hidden reservation followed by
publication/promotion with exclusive, non-reused registration authority. Compare
it with an explicit Behavior-owned post-interpretation rejection contract.
Select the smallest complete solution; changes in both repositories are not
automatically mandatory. A reservation must not create a second Bombay registry.

If reservation/promotion is selected, Address must prove collision exclusion,
no resolution before promotion, one-shot publication, abort/drop release,
generation exhaustion, stale retirement and concurrent resolve/promote/retire.
Bombay must prove accepted-effect conservation, initialization exactly once,
publication only on accepted activation and complete cleanup on failure.

## Bombay implementation after owner contracts are selected

| Work | Existing owner to reuse | End-to-end proof |
| --- | --- | --- |
| InitializeWorker | Child binding, real startup/termination evidence, request.resolve | Report exact ready/rejected/stopped outcome without second initialization. |
| BeginActivation | Request.started/start_rejected/activate and owned task hierarchy | Started is admitted before plan polling; result is correlated; shutdown cannot orphan or indefinitely hide pending work. |
| AssignWorker | Exact recipient and Communication payload-return path | Accepted receipt or exact rejected assignment under close races. |
| ProxyOperation | Exact child binding/control admission | Correct creation and operation; rejection returns whole request. |
| CustomerDelivery | Existing logical/exact delivery interpretation | All four variants preserve outcome and original customer route where required. |
| DiagnosticAction | Existing delivery plus selected settlement-retention contract | Routed failure and terminal diagnostic survive with the selected disposition. |
| Startup failure propagation | SpawnError, child bindings, terminal projections | Parent receives a truthful failure fact instead of an unconditional establishment panic. |
| Pending preparation/activation retirement | Existing preparation and ActivationTasks | Never-ready work has an explicit cancellation/drain policy and exact retained result; no blanket task abort that discards owned plans/results. |
| Publication ordering | Selected Address/Behavior contract | All three current ignored activation regressions execute successfully. |
| Public examples | Existing owner constructors and Application | Real fixed/dynamic restart, stable proxy replacement, FIFO/keyed jobs, rejected customer delivery and shutdown. |

`ActivationTasks::settle` currently awaits tasks and resumes task panics.
That is concrete existing behavior, not proof of a safe future BeginActivation
shutdown policy. A never-resolving plan and a panic need focused regressions
before deciding whether the owner requires a change. Likewise, the existing
ChildInput closed-control branch is unreachable by assertion; ProxyOperation
must prove its own close contract instead of copying that assertion blindly.

## Other core owners: what is actually required

| Repository / layer | Required change established by this investigation? |
| --- | --- |
| bombay-behavior / actors | Yes: lawful assignment/proxy rejection return, terminal diagnostic custody, plus joint startup-result contract resolution. |
| bombay-address | A publication contract is required; hidden reservation/promotion is the recommended experiment, not an already selected public API. |
| Bombay runtime | Yes: six interpreters, startup/publication integration and executable witnesses. |
| bombay-engine inside Bombay | Generic Driver already distinguishes retained/exhausted custody. Add contract regression evidence; no supervisor/pool-specific engine semantics justified. |
| bombay-communication | No new API requirement established. Existing exact rejected payload recovery and control/user lanes must be reused and tested in the new integration. |
| Observe inside Bombay | No new public lifecycle abstraction justified. Reuse exact observations and prove independent consumer/shutdown behavior. |
| bombay-timers | No new timer service needed. Retain the selected patch and test actual restart/backoff/stale-generation behavior. |
| Behavior macros / Bombay authoring macro | No new macro required to unblock these core features. Existing typed products and source routing remain authoritative. |
| Bombay Entity | Local entity work is separate from missing atomic interpretation. Integrate the same corrected startup/lifecycle facts; do not copy supervisor policy into Entity. |
| mnesis-bombay / Mnesis | Needed later for durable execution; not prerequisites for local supervision/pools. |
| Selo / Zenoh | Not prerequisites for any of the local core work in this report. |

Other local product work remains multicore/caller-owned execution, mailbox
configuration and catalogue integration verification. Those are distinct
Bombay workstreams; they do not justify changes in every primitive repository.
See [local requirements](local-runtime.md).

## Implementation order and upstream handoff

1. In bombay-behavior, isolate the assignment and proxy rejected-delivery
   witnesses outside the owner crate; select and implement the smallest
   ownership-preserving APIs and their invalid-construction tests.
2. In the same repository, prove/fix diagnostic terminal retention through
   ordinary generic settlement and continuing Driver execution.
3. Jointly resolve child initialization/publication and post-effect failure
   custody with Address. Prototype both approaches before retaining API.
4. Release/pin immutable compatible owner revisions in Bombay. Do not use a
   dirty sibling checkout as the implicit production contract.
5. Implement the six Bombay leaves in reviewable slices with real delivery and
   retirement regressions; reuse PrepareWorkers and existing generic routing.
6. Run StableProxy, fixed supervisor, dynamic supervisor, FIFO and keyed pool
   public applications, including failure/restart/pressure/shutdown scenarios.
7. Broaden to the 45-template support inventory and the separate execution/DX
   work. Report pure, compile-only and live-runtime evidence separately.

Each implementation PR still needs feature-local verification, its own active
change ledger and the repository's production-surface checkpoints. This report
does not authorize guessing public types or patching around compiler errors.

## Reproduction and verification

Commands and actual outcomes are recorded below. The diagnostic probe is a
temporary integration test, preserved as a code listing for reproduction; it
is removed from the workspace after execution so this research does not add
an intentionally failing test to the ordinary suite.

```text
nix develop -c cargo test --locked -p bombay-rs --lib worker_preparation::tests
  PASS: 3 tests.

nix develop -c cargo test --locked -p bombay-rs --lib local::tests::address_is_absent_until_accepted_initialization_commit_completes -- --ignored --exact
  EXPECTED FAILURE: Present != Absent at local.rs:1334.

nix develop -c cargo test --locked -p bombay-rs --test terminal_diagnostic_custody_probe
  EXPECTED FAILURE: terminal diagnostic was classified as exhausted.

nix develop -c cargo test --locked --release -p bombay-rs --test terminal_diagnostic_custody_probe
  EXPECTED FAILURE: same terminal diagnostic classification, optimized build.
```

Diagnostic probe, using only public exports and the selected dependencies:

```rust,ignore
use core::convert::Infallible;
use bombay::atomic::{DiagnosticAccepted, DiagnosticAction};
use bombay::behavior::{
    ActionItemResult, ItemSettlement, SettledItem, SourceCustody,
    SourceSettlementCustody,
};

#[tokio::test]
async fn terminal_diagnostic_requires_retained_custody() {
    let settlements: Vec<ActionItemResult<DiagnosticAction<Infallible, String>>> =
        vec![SettledItem::Attempted(ItemSettlement::Accepted(
            DiagnosticAccepted::Terminal(String::from("worker failure evidence")),
        ))];
    let mut host = ();
    let custody = SourceSettlementCustody::<(), ()>::offer_next_to_source(
        settlements, &mut host,
    ).await;
    match custody {
        SourceCustody::Retained(_) => {}
        SourceCustody::Exhausted(_) => {
            panic!("terminal diagnostic was classified as exhausted")
        }
        SourceCustody::Admitted(_) | SourceCustody::Closed(_) => {
            panic!("terminal diagnostic is not a live source input")
        }
    }
}
```

This probe establishes the generic custody classification, not a completed
DiagnosticAction interpreter or full end-to-end diagnostic regression. The
Driver discard and pool continuation paths were traced directly in source.

Primary source locations:

- [Bombay runtime](../../crates/bombay/src/application_runtime.rs),
  [preparation](../../crates/bombay/src/worker_preparation.rs),
  [local activation/task custody](../../crates/bombay/src/local.rs),
  [launch](../../crates/bombay/src/launch.rs),
  [Driver](../../crates/bombay-engine/src/driver.rs).
- [Selected assignment API](https://github.com/devrandom-labs/bombay-behavior/blob/435560ce7bea8ad3330ee2d42e5034f837a80602/crates/actors/src/atomic/pool/assignment.rs),
  [proxy API](https://github.com/devrandom-labs/bombay-behavior/blob/435560ce7bea8ad3330ee2d42e5034f837a80602/crates/actors/src/atomic/stable_proxy/operation.rs),
  [generic settlement custody](https://github.com/devrandom-labs/bombay-behavior/blob/435560ce7bea8ad3330ee2d42e5034f837a80602/crates/behavior/src/effects/sending.rs),
  [creation results](https://github.com/devrandom-labs/bombay-behavior/blob/435560ce7bea8ad3330ee2d42e5034f837a80602/crates/behavior/src/actor/creation.rs).
