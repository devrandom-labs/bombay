# External work decision record

Status: **open**. Owner: WP-API-DESIGN. Requirements: XO-17, XO-18,
XO-41–42, EV-24 and DG-WORK. This record identifies an existing typed port and
the ownership question that must be solved before it can count as the bounded
external-work witness. It does not select a new general work service.
The source hashes and 0.17.0 contract below are the 2026-09-29 research
snapshot. The current lock selects Behavior Core/Actors 0.20.0 and Macros
0.13.0; revalidate this open decision against those sources before using its
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

## Bounded candidate and ownership equation

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
