# FIFO shutdown while a worker source is preparing

Historical ARC-010 probe. The current `Cargo.lock` selects Behavior
Core/Actors 0.20.0; its integration evidence is recorded under ARC-010 and
TEST-025 in `docs/open-design-ledger.md`. References to "selected" and
"current" below describe the 0.19.0 snapshot used for this probe.

Status: reproduced against the then-selected Behavior Core/Actors 0.19.0
registry releases (`e5c703e966eba4d2a15fe2c129594f57ae2270fc`). This is
an ARC-010 contract gap, not a selected runtime fix. The positive
`fifo_pool_recovery.rs` tests remain in the ordinary suite; the failing
extension is preserved as
[`fifo-shutdown-during-preparation.patch`](fifo-shutdown-during-preparation.patch).

## Expected policy trace

The selected FIFO owner pure-fold test
`shutdown_waits_for_exact_worker_preparation_without_restarting` accepts a
shutdown command while its `PrepareWorkers` action is outstanding. When the
exact preparation later returns, it stops without creating a replacement.
This is an Actors template policy choice, not a general actor-model law.

The live probe starts a permanent one-role pool. Its first worker completes a
job and stops. A test source announces that replacement preparation has begun,
then waits on an explicit oneshot. The external caller sends the pool's own
`FifoCommand::shutdown()` and a second job while the source is held. The second
job must be returned with its exact submission, payload, and
`AdmissionRejection::ShuttingDown` before the source is released. That reply
proves the shutdown command reached the pool fold. Only then does the probe
release preparation and inspect the terminal tree. The desired tree has one
worker child: the stopped original, with no replacement started after the
shutdown fold.

## Reproduction

From a clean copy of the Bombay 0.19.0 snapshot used for this probe, apply
the patch to
`crates/bombay/tests/fifo_pool_recovery.rs`:

```sh
patch -p1 -i docs/research-probes/fifo-shutdown-during-preparation.patch
nix develop -c cargo test --locked -p bombay-rs --test fifo_pool_recovery shutdown_while_worker_source_is_held_avoids_replacement -- --exact --nocapture
```

The first probe released preparation immediately after submitting shutdown.
It failed its terminal assertion with two worker descendants, but submission
alone did not prove shutdown had folded. The refined probe compiles and fails
earlier, at `shutdown must fold while preparation is held: Elapsed(())`, in
both the exact locked build and the isolated Bombay copy with the separate
proxy-diagnostic owner candidate. It releases the source before asserting the
timeout, so the source operation is not left held by the test. The ordinary
test source was restored byte-for-byte afterward (SHA-256
`8fbd4a5cf845176b18660d61a366c62dd3c402838526383c17a86c306db5a3b6`).

## Ownership finding

Bombay's `InterpretItem<PrepareWorkers<...>>` awaits the complete affine
`WorkerPreparationSource::prepare_first`/`prepare_next` sequence before it
returns the selected `ItemSettlement`. Engine's D-TURN-1 applies the complete
actions before reading another event. The shutdown command can enter the
mailbox during that await, but the FIFO fold cannot see it until the source
returns. The second submitted job likewise cannot receive its
`ShuttingDown` rejection while preparation is held. The earlier trace created
the replacement first and processed shutdown afterward.

The current owner action has no immediate accepted receipt plus later typed
preparation result that would let Bombay start the source operation, finish
the turn, and fold shutdown while the source is still outstanding. Simply
spawning a task inside the existing interpreter cannot fabricate the
`WorkerPreparation` receipt required by the selected `PrepareWorkers`
`SourceAction` settlement. A cancellation side channel or untyped placeholder
would duplicate policy and break exact source custody. The owning Behavior
Actors and Behavior source-action contracts must be reviewed before Bombay
changes production interpretation for this law. ARC-010's preparation
checklist remains open; the independent proxy-diagnostic ingress blocker also
remains.

The selected type equation makes the timing conflict concrete:
`ActionItem for PrepareWorkers` fixes `Accepted = WorkerPreparation`, while
`SourceAction` returns the exact `ActionItemResult<PrepareWorkers>` to the pool.
`WorkerPreparation` contains the affine source and exact preparation ticket;
the owner constructs it only by advancing or rejecting the ordered request.
Bombay cannot truthfully return `ItemSettlement::Accepted` at source start,
because that value must already contain the completed worker submissions or
the exact rejected role. `Rejected` requires an actual source rejection and
returns the original item; `Blocked` is uninhabited because this action's
prerequisite is `Never`; `Corrupt` declares an interpreter violation.
Returning `Unattempted` would claim the interpreter never attempted the
request. The Driver's D-TURN-1 waits for the complete
`Environment::apply` settlement before receiving another mailbox event, so
the existing source-action equation cannot represent a preparation that starts
in one turn and reports its exact result in a later turn. An owner contract
would need to name both facts and their custody explicitly before any runtime
implementation is eligible.

## Owner contract acceptance

The required interleaving is a deliberate FIFO pool policy already exercised
by the selected owner's pure transition tests. A compatible owner contract
must let the interpreter commit the preparation start before its asynchronous
source finishes, then deliver the exact later outcome through a typed event.
The pool must be able to fold shutdown while that source is held and reject a
subsequent job as `ShuttingDown`. When the held source returns, it must retain
the original source, exact correlation, submissions or rejection, and must not
start a replacement after shutdown. A foreign or duplicate return must not
settle the outstanding request.

The same contract has three real consumers: FIFO and keyed pools each request
one role; the fixed supervisor can request an ordered group. The selected
keyed-pool lifecycle test also folds shutdown before its in-flight preparation
returns. The fixed supervisor's existing
5,040-order pure shutdown test includes a late preparation alongside three
proxy operations and three proxy exits. A replacement of the source action
must preserve those orderings, the prepared prefix and rejected suffix for a
multi-role failure, and the complete result through actor retirement if source
admission has closed. A task abort or panic must have an explicit terminal
path that cannot leave the actor waiting forever; a closed destination must
retain the completed affine source and prepared workers in retirement custody.
These are acceptance conditions for an owner API change, not a proposed second
Bombay actor or runtime policy.

Bombay already owns a `JoinSet` for delayed activation tasks, but the active
Environment reads only mailbox, fact, timer, and owner-cancellation ingress.
It joins those tasks at retirement. Reusing that storage for worker preparation
without an active failure path would leave a pool waiting forever if the source
task panicked before sending its result. The owner result protocol and Bombay
task ownership must therefore be designed together; the current activation
task storage alone does not close the gap.

The ordinary-Rust composition audit has one viable direction, pending the
owner's contract decision. Waiting inside `InterpretItem` reproduces the live
failure. Spawning the current request leaves the Driver awaiting a settlement
that cannot yet be constructed. Returning another existing settlement variant
would misstate acceptance, rejection, blocking, or attempt status. Polling
shutdown inside that interpreter would enter the Behavior fold before the
current action settled and violate D-TURN-1. A typed start commitment followed
by an independently typed late result can express the interleaving, but it
requires the owner to define the exact source/result custody and the runtime
to report task failure. No macro, new actor trait, or runtime policy is implied
by this comparison.
