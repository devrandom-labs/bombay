# Minimal actor lifecycle and ownership

This is the ownership oracle for bombay's composed runtime. It describes
resources, not a public lifecycle framework or supervision policy.

## Smallest state machine

```text
Absent --prepare--> Prepared --publish and start--> Live --terminal--> Absent
   ^                    |                               |
   |                    +-----------rollback-----------+
   +--------------------------cleanup------------------+
```

`Prepared` remains transaction-local and carries no external capability. An
optional sink can observe completed transition facts, but those facts confer no
state access or authority. Completion publication and address release
are ordered terminal actions, not durable states: no operation may interleave
between them on behalf of the retired generation. `Claimed`, `Running`,
`Closing`, `Completed`, and `Released` remain decorative as runtime states
because no ownership transfer or operation exists at those boundaries.

Each spawned incarnation executes the same transaction. A later incarnation at
the same address is a new transaction and cannot inherit the old mailbox,
timer, task, endpoint, or completion generation.

## Ownership table

| Resource | Absent | Prepared (not externally visible) | Live | Terminal transition |
|---|---|---|---|---|
| bombay-address lease | none | spawn transaction | actor-task retirement guard | exact lease drops before terminal publication |
| bombay-communication counting user handle | none | prospective edge `Handle` | every cloned edge `Handle`; parent child capability when applicable | all are dropped/rejected; last edge closes user lane |
| bombay-communication non-owning user anchor | none | prospective lease endpoint | bombay-address entry | removed by exact lease; never keeps user lane open |
| bombay-communication consumer and queued items | none | spawn transaction | actor task | actor task drops consumer; queued items and blocked producers are released |
| bombay-timers queue | none | empty incarnation-local queue | actor environment | dropped with the environment before terminal publication |
| application driver | none | prospective `Driver { Behavior, Environment }` | `Incarnation` | retires the environment on ordinary return; drops with the incarnation on unwind/cancellation |
| Tokio actor task | none | future not started | Tokio runtime; represented by abort authority and private `Completion` | task termination proves all future-owned values have dropped; it does not classify actor semantics |
| abort authority | none | prospective runtime `Handle` | runtime `Handle` | remains useful until Tokio reports terminal outcome |
| bombay-observe subject | none | spawn transaction | actor-task retirement guard | completed exactly once, then subject releases its generation |
| bombay-observe observation | none | prospective runtime `Handle` | private seat inside `Completion` | retains only the captured outcome generation, never runtime resources |
| child lease | none | prospective parent-owned runtime lease | parent actor environment; observation transfers only its completion seat | dropped after observed completion or with the parent; failed child birth adds no lease and leaves its nonce reservation |
| lifecycle reporter | none | optional value derived after exact registration | actor reference, task entry, and retirement guard | emits completed facts only; owns no lifecycle resource |

## Transaction and terminal order

Spawn prepares the observation subject, mailbox, generation-bound endpoint,
empty timer queue, and child capability before exposing an edge or starting
user code. Claiming the address is the publication point. `Prepared` is emitted
only after that claim and reporter derivation. Any failure before task
start drops the prepared resources in reverse ownership order and starts no
task. Once claimed, task start cannot fail under Tokio's synchronous spawn API.

Terminal cleanup is composed inside one actor task from two independently
tested, declaration-ordered fields:

1. the `Driver` retires its environment on ordinary return; cancellation or
   panic drops the driver and its mailbox consumer, timer queue, and child capabilities;
2. its `TerminalRetirement` guard classifies an explicit return, panic unwind,
   or ordinary cancellation drop;
3. the guard releases the exact Bombay Address lease;
4. it emits `Retired`;
5. the guard publishes both fully retired outcomes through Bombay Observe;
6. it emits `Completed`;
7. only then can Tokio task termination release `Completion::wait`; the actor
   outcome itself comes from the already completed Observe generation.

Abort authority may remain privately owned by a `Handle`, but it is exposed
only through the `abort` operation, is powerless after executor termination,
and is not part of incarnation cleanup.

The externally important edge is `release address -> publish completion`, as
required by `docs/runtime-blocks.md`. Cleanup and release before publication
ensure every observation denotes a fully retired incarnation: stale timers and
blocked deliveries can no longer mutate it, and its address is already
available for a replacement.

## Current invariants

- A collision or injected preparation failure starts no task and restores the
  pre-spawn resource counts.
- Immediate return and panic cannot outrun mailbox, address, or observation
  preparation.
- Return, panic, and abort publish exactly once and retire only their captured
  generations.
- The registry anchor cannot prevent last-edge closure.
- Consumer retirement wakes blocked sends and returns/rejects their payloads.
- Bombay retains Communication's control sender only to publish the
  Behavior-owned typed shutdown event; it defines no control event itself.
- Replacing a keyed schedule invalidates its older generation; dropping the
  incarnation prevents every pending expiration from being observed.
- No mailbox, timer, task, endpoint, completion, or child resource is
  transferred between address incarnations.
- Child preparation failure removes only transaction-local child resources.
- Observing a child takes only its exact completion seat. Its liveness lease
  remains in the parent scope for coordinated shutdown and retirement.
- Every child receives only its creating parent's non-owning event edge and
  its fresh nonce beneath that parent. A worker report stamps that nonce and
  preserves the Behavior-provided outcome and timestamp; parent closure makes
  the report inert.
- A child nonce leaves a tombstone for the full parent incarnation. Address
  retirement never makes that actor identity fresh again.
- Reusing a logical address produces a distinct Address-owned registration
  identity; all facts for one incarnation carry that exact identity.
- Instrumentation failure cannot alter preparation, execution, retirement, or
  completion.
- After the last observation is dropped, all mailbox, timer, task, endpoint,
  completion, and child resources can return to baseline.

Supervision strategy and restart budgets remain pure Behavior policy. Remote
discovery, persistence, distributed transport, lifecycle hooks, fact storage,
formatting, and export are deliberately outside this state machine.
