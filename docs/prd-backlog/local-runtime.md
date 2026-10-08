# Local runtime completion

**Selected-contract update (2026-10-01):** Bombay implements all six
previously missing atomic `InterpretItem` contracts against Behavior Actors
0.20.0. The selected fixed supervisor admits the typed `StableProxy`
diagnostic report. The public supervisor executes worker replacement and
shutdown with exact terminal custody. A live FIFO regression proves shutdown
folds while its affine replacement source waits: a later job receives its
exact `ShuttingDown` rejection before the source returns, and no replacement
starts afterward. The selected Bombay build runs one FIFO search to completion and
waits for orderly worker drain through the public `worker-pool` example. A
separate runtime regression admits work while activation is held and returns
the next job's exact payload on a capacity-one `BacklogFull` rejection.
ARC-006 has also made the activation visibility regressions executable: the
address remains invisible through initialization commitment and a failed
commit never publishes it. The ACT rows below retain the original snapshot's
requirements and marks; use [the status index](status.md) for current verification.
Broader capacity accounting and keyed-pool proofs remain open. A second runtime
regression now observes one permanent replacement after a worker stops,
followed by another completed job and two-worker terminal custody; broader
replacement and broader supervisor policy coverage remain open.
Independent runtime traces also prove both FIFO interruption dispositions:
`Retry` completes the interrupted job on a replacement, and `Fail` returns
the assigned payload and worker-stop reason before that replacement serves
another job.
The marks below are the earlier requirements inventory; use [the status index](status.md) for current verification state.

See [classification marks](README.md#reading-the-inventory) and
[evidence](evidence.md). The standard local composition remains Behavior plus
Driver inside Environment, with Address, Communication, Observe and Timers.
Do not implement another supervisor policy, mailbox, timer service or actor
trait while completing these requirements.

## ACT — Activation and authoritative settlements

Owners: Bombay activation, Address publication, Behavior settlement algebra.
At this inventory's capture, the prerequisite was a contract consistent with
both endpoint publication and irreversible accepted initialization effects.
ARC-006 subsequently selected invisible Address reservation followed by
publication after accepted commitment and ran the former ignored regressions.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| ACT-01 | D | Resolve reservation, claim and publication order with Address ownership. | A documented transition table states exactly when resolution becomes possible and who releases a failed reservation. |
| ACT-02 | P | Prevent endpoint visibility for initialization that cannot become an active actor. | Concurrent resolution never sees an endpoint for the rejected/corrupt initialization cases currently ignored. |
| ACT-03 | D | Define partial initialization settlement when earlier effects succeeded and a later effect fails. | The result preserves each authoritative accepted effect; no fictional rollback or blanket rejection. |
| ACT-04 | P | Turn the ignored activation-order witnesses into executable regressions after the owning contract is resolved. | Each witness fails against the original behavior for its intended law and passes in debug and optimized builds. |
| ACT-05 | V | Preserve one initialization and one activation per permit. | Replaying readiness, cancellation or activation completion cannot initialize twice or consume a permit twice. |
| ACT-06 | V | Verify observation attachment across the publication/activation boundary. | A child terminating immediately is observed once by every independent registered consumer. |
| ACT-07 | V | Verify cleanup at every activation failure boundary. | No retained address lease, timer, observation or unjoined task remains after a rejected startup. |
| ACT-08 | P | Resolve the local erased shutdown-authority boundary against typed ownership requirements. | Ordinary existing concrete composition is attempted first; the selected path preserves exact target authority without adding a second lifecycle system. |

## INT — Missing atomic capability interpretation

Owners: Behavior Actors owns requests and policy; Bombay owns interpretation.
All six named leaves below are absent from ApplicationCapabilities. Existing
PrepareWorkers interpretation is reused. The assignment/proxy rejection
ownership issue must be resolved in the owning API before its consumer is built.
The [core integration investigation](core-integration.md) additionally reproduces
a terminal-diagnostic custody mismatch in the generic settlement contract and
traces the joint creation/initialization integration decision.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| INT-01 | X | Obtain an affine rejection/return contract for AssignWorker from the selected Behavior Actors revision. | A runtime can return the exact rejected assignment without forging or interchanging another assignment's receipt. |
| INT-02 | X | Obtain the corresponding owned rejection contract for ProxyOperation. | Consuming the operation into delivery still permits exact rejected-operation recovery, with compile-time prevention of mismatched ownership. |
| INT-03 | M | Interpret InitializeWorker using the existing worker plan and permit. | ReadyForActivation, EffectsRejected and Stopped retain their exact typed outcomes; initialization occurs once. |
| INT-04 | M | Interpret BeginActivation with the existing admission and launch order. | Started input is installed before the activation plan can run; rejection retains the correct correlation and permit disposition. |
| INT-05 | M | Interpret AssignWorker against the exact established worker incarnation. | Accepted assignment returns the owning receipt; closed/stale delivery recovers the original work through the upstream contract. |
| INT-06 | M | Interpret ProxyOperation against the selected proxy creation and control authority. | Start, replace, stop, query and cancellation inputs reach the intended proxy; rejection does not become successful admission. |
| INT-07 | M | Interpret all CustomerDelivery variants. | Logical, exact, rejected-logical and rejected-exact outcomes preserve customer route, payload and provenance. |
| INT-08 | M | Interpret DiagnosticAction under explicit typed policy. | Routed and terminal diagnostics follow their specified dispositions; neither is silently discarded or converted only into a log line. |
| INT-09 | V | Prove mixed atomic request products preserve declared settlement order. | An independent trace sees the complete lane, including successful prefixes and rejected suffixes. |
| INT-10 | V | Verify cancellation and shutdown during pending capability work. | Late results are correlated and discharged explicitly; no result reaches a replacement incarnation accidentally. |
| INT-11 | X | Resolve terminal diagnostic retention across continuing turns in the Behavior/Behavior Actors settlement contract. | Accepted terminal diagnostics survive later turns and reach retirement; ordinary discharged receipts do not accumulate. The current generic classification probe fails in debug and optimized builds. |

## SUP — Live supervisors and stable proxies

Owner: existing FixedSupervisor, DynamicSupervisor and StableProxy policies;
Bombay supplies INT. Prerequisites: ACT and INT for the selected composition.
These rows complete executable integration, not new supervision algorithms.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| SUP-01 | P | Run a fixed supervisor through the public Application path. | All declared workers activate and receive real messages; evidence is not merely inspecting initial Actions. |
| SUP-02 | P | Run a dynamic supervisor through the public Application path. | Add, query, replace and stop exercise actual child tasks and return exact typed results. |
| SUP-03 | P | Run a stable proxy across worker replacement. | Its stable logical route reaches the new worker while an old exact recipient cannot address the replacement. |
| SUP-04 | V | Exercise owner-defined restart eligibility and terminal disposition. | Restartable and terminal outcomes choose distinct existing policy branches without runtime-imposed escalation. |
| SUP-05 | V | Exercise restart delay and budget exhaustion. | A repeated failure sequence produces the owner's configured timing and final disposition, including stale timer replay. |
| SUP-06 | V | Verify failure during replacement preparation and activation. | The old worker and failed replacement have independently correct terminal facts; no phantom active child remains. |
| SUP-07 | V | Verify concurrent status, replacement and cancellation requests. | Every request has a correlated outcome; a late completion cannot settle a newer operation. |
| SUP-08 | V | Verify independent supervision and external observation. | Supervisor restart handling does not consume or cancel another observer's original failure fact. |
| SUP-09 | V | Verify shutdown during backoff, preparation and worker activation. | No restart starts after shutdown commits; tasks join and permits/leases retire exactly once. |
| SUP-10 | P | Replace the pure-only supervision example with an executable policy example. | A scripted worker failure visibly causes the configured restart and subsequent successful request. |

## POOL — Live worker pools

Owner: existing FifoPool and KeyedPool. Prerequisites: ACT, INT and exact worker
protocol contracts. Do not assume a failed worker makes arbitrary work safe to
retry; use the owning assignment and customer policies.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| POOL-01 | P | Execute FIFO pool preparation, initialization and activation. | Real jobs reach ready workers through Application and receive correlated results. |
| POOL-02 | P | Execute keyed pool binding and dispatch. | Jobs use the specified key binding; stale binding receipts cannot select a superseded worker. |
| POOL-03 | V | Verify FIFO policy under occupied workers and queued work. | An independent assignment trace matches the owner policy; completion order is not falsely promised to be FIFO. |
| POOL-04 | V | Verify queue saturation and exact rejected work recovery. | Every rejected job remains recoverable and no capacity accounting leaks after cancellation. |
| POOL-05 | V | Verify worker death before and after assignment admission. | Unassigned, admitted and outcome-unknown work remain distinguishable under the existing policy. |
| POOL-06 | V | Verify work requeue versus terminal customer failure. | Only explicitly selected safe retries occur; each original customer receives the policy's terminal result. |
| POOL-07 | V | Verify stale worker results after replacement. | The old generation cannot complete a new assignment or release its worker capacity. |
| POOL-08 | V | Verify slow, closed and replaced customer endpoints. | CustomerDelivery preserves exact rejected payload and logical/exact routing distinctions. |
| POOL-09 | V | Verify keyed unbind/rebind races. | A late completion cannot clear a newer binding or deliver to a different customer's request. |
| POOL-10 | V | Verify shutdown with queued and in-flight work. | Every retained job has an explicit returned, completed or uncertain disposition; no silent drop. |
| POOL-11 | P | Publish executable FIFO and keyed pool examples. | Both examples perform real work and one adverse worker lifecycle, rather than only constructing Actions. |

## EXEC — Parallel execution and embedding

Owner: Bombay task composition over Tokio; the Driver remains actor-independent.
The existing tasks already use tokio::spawn and substantial Send constraints.
Changing a runner builder alone is not sufficient proof of safe parallelism.

The [execution ownership PRD](../prds/execution-ownership.md) specifies the
required contracts, source hierarchy, bounded decision experiments, failure
witnesses and multi-agent assignments. It prioritizes responsibility segregation
and exact ownership over line-count reduction. Its open decision gates block
dependent implementation; they are not permission to invent missing APIs.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| EXEC-01 | D | Select public execution configuration for current-thread and multithread runtimes. | Thread count, runtime ownership and defaults are explicit; no ambient global runtime is introduced. |
| EXEC-02 | M | Enable and wire Tokio multithread execution in ordinary runners. | Two actors execute on different worker threads while each actor retains serialized folds. |
| EXEC-03 | M | Provide a caller-owned async application entry point. | An application starts and stops inside an existing Tokio runtime without nesting a runtime or blocking its worker. |
| EXEC-04 | V | Check actor-local serialization under concurrent senders and timer events. | Overlap instrumentation never sees two folds on one incarnation, while independent actors make progress. |
| EXEC-05 | D | Define support for blocking and CPU-heavy external work. | Blocking work has an explicit bounded execution boundary; Behavior folds perform no I/O and have documented cooperative limits. |
| EXEC-06 | V | Verify shutdown and task joining on multithread execution. | Repeated concurrent retirement cannot double-publish completion or leave an orphaned child task. |
| EXEC-07 | D | Decide whether non-Send actors are supported. | Either a deliberate separate host contract exists or compile diagnostics explain the ordinary Send requirement; no implicit unsafe escape. |
| EXEC-08 | V | Verify executor panic and capability-task failure semantics. | The observable terminal fact identifies the source and shutdown still joins the owned hierarchy. |
| EXEC-09 | M | Add execution-mode examples and a meaningful concurrency benchmark. | Demonstrate overlap and throughput across independent actors; do not assert one actor becomes internally parallel. |

## APP — Ordinary application configuration

Owner: Bombay public application composition. Preserve current ActorRef,
ApplicationHandle, ActorInterface, external customer and entity mechanisms.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| APP-01 | P | Expose validated user-mailbox capacity where applications need it. | The current fixed capacity of 1024 is not the only ordinary option; saturation preserves rejected payloads. |
| APP-02 | D | Specify external-client capacity and pressure policy. | A slow boundary cannot create unbounded user-result accumulation; shutdown behavior is explicit. |
| APP-03 | P | Decide the ordinary entry point for existing advanced entity-family hosting. | A public example admits, hydrates, passivates and reactivates a local entity without duplicating Entity's state machine. |
| APP-04 | V | Verify root/child startup failure visibility through each public runner. | Callers receive the exact configured failure rather than a successful handle to a dead application. |
| APP-05 | V | Verify external ingress closure during application shutdown. | New ingress rejects truthfully while admitted work follows the documented drain policy. |
| APP-06 | D | Select bounded graceful shutdown and forced-stop semantics. | Deadline expiry has an explicit outcome and does not falsely claim user work completed. |
| APP-07 | V | Check authoring diagnostics under aliases, crate renaming and nested child products. | Macro hygiene/compile fixtures prove naming variations do not change semantic expansion or erase typed errors. |
| APP-08 | P | Correct documentation that describes pure atomic examples as live runtime support. | Every advertised executable feature links to a public-path witness; remaining blockers are stated precisely. |

## CAP — Typed external capability composition

Owner: existing Behavior request leaves and Bombay interpreters. No general
provider registry, dynamic service bag or second Effect language is required.
Zenoh, identity and Mnesis integration need this boundary verified concretely.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| CAP-01 | D | Prototype the smallest static application composition for an external interpreter. | Compare existing functions, products, associated types and extension methods before adding a public wrapper or macro. |
| CAP-02 | P | Make the selected external interpreter composition usable from an ordinary application. | A real typed request reaches a supplied capability without replacing the entire standard Environment. |
| CAP-03 | D | Define pending external work and later completion correlation. | A slow network/storage operation cannot accidentally freeze unrelated actors or fabricate synchronous completion. |
| CAP-04 | D | Preserve rejection versus uncertain external completion. | A timeout after possible delivery is not reported as definitive non-delivery with a safely retryable payload. |
| CAP-05 | M | Bind capability work to incarnation lifetime and cancellation policy. | Late I/O completion cannot mutate a replacement incarnation; committed durable facts survive caller cancellation. |
| CAP-06 | V | Verify exact typed errors across nested capability products. | No erased aggregate or positional routing error loses the source fact. |
| CAP-07 | M | Supply a deterministic capability test host for external work. | Tests explicitly release or reject correlated completions without doing I/O inside a fold. |

## CAT — Existing template coverage

Owner: Behavior Actors templates; Bombay verifies composition. The 45-entry
[template appendix](evidence.md#template-inventory) is the inventory, not a list
of templates to reimplement. These requirements close support-evidence gaps.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| CAT-01 | V | Verify every claimed lifecycle composition through the ordinary runtime. | Independent observers, planned child shutdown and terminal propagation preserve full traces and provenance. |
| CAT-02 | V | Verify discovery templates with live typed delivery. | Presence expiry, registry replacement, resolver denial and subscriber closure execute the owner policy. |
| CAT-03 | V | Verify routing templates with pressure and rejection. | Buffering, acknowledgements, correlation, ordering, deduplication, rate limiting and circuit breaking have policy-specific witnesses. |
| CAT-04 | V | Verify operational templates with real state changes. | Health, readiness and configuration examples answer actual application conditions rather than constant placeholders. |
| CAT-05 | V | Verify timing, stash and workflow compositions beyond construction. | Every advertised wrapper performs its policy; Workflow, Lease and message adaptation receive explicit live witnesses where not already covered. |
| CAT-06 | V | Keep evidence levels separate in published support tables. | Pure owner tests, compile-only proofs and executable Bombay integration are separately identified for every public template. |
