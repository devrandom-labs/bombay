# Application execution API decision record

**Frozen research snapshot:** ARC-011 on 2026-10-01 retained the distinct
`Application` and advanced `App` construction policies and factored their
root launch into one private transaction. The duplicated-root descriptions
below apply to the recorded 0.17.0 baseline; current selected code and tests
are in the ARC-011 retained evidence in the backlog status index.

Status: **open**. The current work is DG-API research, not an accepted public
interface or authorization for production edits. Owner: WP-API-DESIGN; reviewer
must differ from this record's author. PRD requirements: XO-01–12, XO-37–39,
XO-43–45. Evidence and dependent task contracts remain under review.

## Selected contract and current consumer syntax

At the recorded baseline, `Cargo.lock` selects `bombay-behavior` and
`bombay-behavior-actors` 0.17.0 from registry release source revision
`435560ce7bea8ad3330ee2d42e5034f837a80602`; Behavior Macros 0.12.0,
Address 0.2.0, Communication 0.1.2, Tokio 1.53.1, Bombay-private Observe,
and Timers 0.1.0 patched to `13e884da7ab41781f52337b0038060e375b00ee0`.
The dirty-tree snapshot is
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-exec-goal-qyt0kpye/baseline.json`;
this PRD had SHA-256
`4e0382f71648daa4961b5fb77b1bdb815c76b8e844debcf56ba4afe5769e2fe7`.
The selected Behavior revision's full `AGENTS.md` and Bombay's required owner
documents were read before this design experiment.

The [current implementation](../../../crates/bombay/src/application_runtime.rs)
has seven public runner methods that each construct a current-thread Tokio
runtime and call `block_on`. `LaunchSystem::launch` already specializes
`launch_with` with unit work; that reuse must not be counted as a separate
actor loop. `LaunchSystem::launch_axum` repeats root construction/activation
instead of consuming `launch_with`. `App::run_with_entities` installs Entity
families, executes root work, then shuts families down even when root execution
returns `Err` on its ordinary path. It currently drops the typed family
shutdown product from the returned value in that error case.

Current examples and fixtures consume `.run()`, `.run_with(...)`,
`.run_axum(...)`, or `.run_with_entities(...)` synchronously. A breaking
async-first change must migrate those callers deliberately and update the
compile diagnostics; the exact migration set is obtained by searching all
tracked docs, examples, benchmarks and tests when the API gate is accepted.

## Required law and complete observable sequence

This is deliberate Bombay policy over the one Behavior/Driver algebra:

1. A pure `Application` declaration or advanced `App` owns its root and static
   declarations. It does not create a Tokio runtime while being built.
2. Caller-owned async execution polls inside the caller's Tokio host, stages
   the declared application, installs required local resources, starts the
   root, waits for accepted activation, invokes application work once, joins
   the root and relevant families, and returns exact typed results.
3. An owned-executor convenience creates a Tokio runtime before actor
   activation and drives that *same* application future. Construction failure
   has no activated actor. The convenience cannot silently nest a runtime.
4. Application work is awaited before the root join. Root termination does not
   interrupt arbitrary pending work; work completion does not stop the root.
   If work returns `Result<T,E>`, the exact `E` remains the work's value.
5. HTTP bind precedes root activation/router construction. Serving occurs as
   application work and coordinates its own shutdown/error with the root.
6. Installed Entity families always receive their owning shutdown/join attempt
   after ordinary root execution. Independent root and family outcomes cannot
   be silently collapsed merely for a convenient `?` operator.

The post-drop law is delegated to DG-TASK; no API candidate may be accepted
until that record identifies the cleanup owner at each future-drop point.

## Ordinary Rust API alternatives to compare

All signatures and syntax below are **proposed experiments**, not compiled or
selected Bombay APIs. No macro is proposed. The candidate that survives must
be compile-checked with inferred types for `Application`, advanced `App`,
heterogeneous declared children, Entity families and HTTP.

| Candidate | Application syntax | Mechanism and anticipated cost | Current disposition |
| --- | --- | --- | --- |
| Async inherent methods | `Application::new(root).run().await`; `.run_with(work).await`; advanced `App` equivalents | Consuming methods can delegate directly to existing private launch futures. Current synchronous callers need mechanical migration. | Preferred experiment; gate open. |
| Additive async twins | `.run_async().await`, `.run_with_async(work).await`, plus every HTTP/Entity combination | Retains current synchronous methods and duplicates public spelling/runner forwarding for the same application lifetime. | Reject unless a concrete compatibility constraint outweighs duplicated public contract; no such evidence recorded. |
| Public free async function | `run_application(application).await` | Must recover associated root/terminal/role types through generic bounds already present on inherent methods. Likely increases type annotations and a second spelling; measure actual compiler diagnostics before final rejection. | Open comparison. |
| Tokio builder as owned input | `application.run_blocking(Builder::new_current_thread())` or a multi-thread builder, with the same application future underneath | Expresses scheduler, worker count and enabled drivers with Tokio's existing validated value; one application-specific construction/error boundary, no Bombay mode type. | Preferred experiment; gate open. |
| New Bombay execution-mode value | `application.run_blocking(mode)` | Would introduce a public policy product and duplicate Tokio builder configuration. Needs a specific invariant or accepted default unavailable from Tokio's concrete builder. | Not justified by current evidence. |
| Generic public `block_on` forwarding function | `bombay::block_on(builder, application.run())` | Works for unrelated futures and gives nested construction/application results; owns no Bombay-specific invariant unless proven otherwise. | Presumed forwarding-only abstraction; do not add without contrary evidence. |

The owning convenience need not mirror every async spelling. A synchronous
caller can explicitly create a Tokio runtime and call `block_on` on the async
application future. The accepted surface must nevertheless include a small
Bombay-owned execution convenience for the ordinary root and demonstrate both
current-thread and multithread mode. Whether advanced `App`, application work,
HTTP and Entity receive corresponding convenience methods is an explicit
surface comparison, not an implied omission or automatic list of seven twins.

Tokio 1.53.1 `Builder::new_multi_thread` requires the `rt-multi-thread`
feature; Bombay's current Tokio dependency enables only `macros`, `rt`,
`sync`, and `time`, with `net` through Axum. An accepted owned-multithread API
must name the feature selection and its validation. Tokio `Runtime::block_on`
panics in an asynchronous execution context; an owned Bombay entry cannot
pretend that nested execution works. `Handle::try_current` is an available
preflight candidate, but the returned failure must use a truthful typed error
and occur before any actor side effect. Tokio runtime destruction does not
guarantee spawned tasks finish and can wait indefinitely for blocking work;
this belongs in the accepted shutdown/Drop contract.

## Error and terminal equations to resolve

The selected API must write and compile the exact result types for all paths:

```text
async simple:         root startup failure OR exact terminal
async with work:      root startup failure OR (work's exact value, exact terminal)
owned simple:         runtime construction failure OR async-simple result
HTTP:                 bind failure OR root startup failure OR
                      (serve failure with exact root terminal) OR terminal
Entity families:      runtime failure before install OR
                      (root's exact result AND exact family shutdown product)
```

This is an ownership equation, not a chosen enum or signature. Current
`RunError::Runtime(io::Error)` is truthful only for an entry that constructs a
runtime. Retaining that variant on a shared error sum is possible; producing it
from an async entry that never builds a runtime is not. Current
`AxumRunError::Serve { source, terminal }` preserves both outcomes and must
survive HTTP consolidation.

For Entity families, a root-startup error and the family shutdown result can
coexist. The present `Result<(Output, Terminal, Shutdowns), RunError>` cannot
return both. Compare direct standard products/sums and a named product only if
it owns this coexistence law. Reject a nested positional error whose fields
need source-position decoding, blanket aggregate error, discarded family
report, and a `?` that skips shutdown. This is an unresolved public-surface
decision; no source migration may assume its answer.

A direct ordinary-Rust candidate for the async family result is
`(Result<(Output, Terminal), RunError<RootError>>, Shutdowns)`. It preserves
the independent root and family facts without a new public type, including
the case in which root activation fails after families were installed. An
owned executor could place a runtime-construction `Result` outside that
product, since no family exists if building Tokio failed. This is only a
candidate: exact error ownership, successful caller syntax, nested inference,
and the fate of the family product on caller-future drop still need compile
and runtime experiments. The current implementation's sequence is concrete:
install families, call `launch_with`, await `installed.shutdown()`, then map
the outcome; the final `outcome.map(...)` is the exact point that discards
`shutdowns` on `Err`.

The accepted record must also state the result of polling async execution
without an appropriate Tokio host or time driver: either a documented precise
precondition/panic boundary or a checked typed failure before any actor starts.
No implicit runtime fallback is allowed. HTTP's `net` requirement is separate
from the ordinary actor's time/sync requirements.

## Source and verification evidence still needed

| Evidence | Smallest experiment | Status |
| --- | --- | --- |
| Current async syntax denial | Compile an external consumer using `Application::new(root.stop_on_shutdown()).run().await` with inferred terminal type; record exact compiler diagnostic against current code. | `nix develop -c cargo check --manifest-path /tmp/bombay-exec-api-probe/Cargo.toml --target-dir /Users/joel/Code/devrandom/bombay/target` fails with only `E0277` at `run().await`: `Result<_, bombay::RunError> is not a future`; the fixture has a valid shutdown template. The first fixture omitted that template and also failed the required `InjectEvent<ShutdownRequested, Here>` bound, so its diagnostic is discarded. |
| Current sync positive | Existing `run_with`, Axum and terminal-custody tests, then a direct external consumer compile. | The same corrected external consumer compiles when `.await` is removed: `nix develop -c cargo check --manifest-path /tmp/bombay-exec-api-probe/Cargo.toml --target-dir /Users/joel/Code/devrandom/bombay/target` exited 0. Fifteen focused runtime tests passed in the preceding audit. |
| Async inherent versus free function | Compile pass/inference and intentional wrong-protocol/role fixtures for both, using existing concrete root/child types. | Open; requires proposed code only in isolated research. |
| Builder input versus new mode | Compile two Tokio builders, invalid worker count and missing feature; capture diagnostics and resulting error ownership. | Open. |
| Nested-runtime behavior | Call the existing blocking entry inside Tokio, then call the selected owned candidate and prove truthful preflight occurs before construction/activation. | The isolated external fixture `#[tokio::test]` calling current `Application::run()` passes `#[should_panic(expected = "Cannot start a runtime from within a runtime")]` under pinned Nix. The selected candidate's typed preflight/activation trace remains open. |
| Full application lifecycle | Independent observable trace of declaration, activation, work, root terminal and family shutdown across simple/advanced/HTTP/Entity. | Open; DG-TASK is prerequisite. |
| Owned/caller executor equivalence | Compare complete typed output and terminal under both scheduler modes; two independent actors overlap, one actor folds serially. | Open; runtime implementation prerequisite. |
| Public migration | Rewrite exact repository consumer set, no dummy callbacks, aliases or retained sync spelling for compatibility alone. | Open; accepted API prerequisite. |

The current public syntax experiment uses an isolated Cargo project under
`/tmp/bombay-exec-api-probe`. It selects the local Bombay path and the exact
Timers Git patch. Cargo may choose different *transitive* registry patch
versions than the root lock; its diagnostics are syntax probes, not a claim of
full locked-workspace verification. The production regression must run under
the workspace's `Cargo.lock` through pinned Nix.

Independent DG-TASK review identified two further acceptance constraints.
Giving a blocking method a Tokio `Builder` does not itself retain root or
Entity cleanup when the callback panics and `block_on` unwinds. The selected
owner must be demonstrated while the Tokio host survives, including a pending
callback after root termination followed by caller-future drop. Also,
`Builder` is not a proof that time or I/O drivers are enabled; the chosen API
must either enable required drivers or document/check the precise precondition.
`worker_threads(0)` panics while configuring the builder, so Bombay must not
claim its runtime-construction result validates that input.

## Aggregate-drift and abstraction checkpoint

No current application control states have been changed. Before an accepted
production edit, DG-TASK must supply the complete execution/retirement
authority states and the future-drop alternatives. This API layer owns no
second actor state machine. The direct values to test first are the existing
`Application`, `App`, Tokio `Builder`/`Runtime`, `Result`, typed terminal,
and existing static Entity family products.

Current public runner spellings: seven; current owned runtime constructions:
seven. Candidate async inherent methods would retain four semantic operations
across `App` plus three across `Application` but change execution timing and
source compatibility. The final count of blocking conveniences, public
errors, modules, task states, branches, production lines and compile-time cost
remains open until a concrete candidate exists. No alternative is retained
based on a predicted line reduction. The disposition is **reopen / no
production candidate selected**.

## Stop conditions and handoff

This record cannot be accepted until it contains exact compiled signatures,
errors/defaults/features, complete ownership/drop outcomes, positive and
negative consumer diagnostics, all applicable baseline and optimized trace
results, a public migration map, and independent reviewer agreement. If the
task/shutdown decisions require a different caller authority, reopen this
record before migrating examples. The coordinator alone updates the PRD gate
status and the feature PRD. No worker may add an async twin, runtime
wrapper or new error type to make one compile fixture pass.

## Current EXEC work stage (2026-10-02)

This decision record is current work; its 0.17.0 source observations above are
research inputs. The [execution checkpoint](../execution-ownership.md#15-exec-execution-checkpoint-2026-10-02)
resolves Core/Actors 0.20.0, Macros 0.13.0 and Address 0.3.0 and records 20
passing preservation tests. Current source still has seven owned current-thread
runner constructions and a separate HTTP coordination body. Begin by comparing
async inherent methods against the free-function candidate with ordinary inferred
caller types. No candidate is accepted until the signatures, errors, cancellation
custody and independent review required by DG-API/DG-TASK are recorded.

Inspected runner source SHA-256: `0b8e2c1c475ab46c50c451082b8d67dadb0a138e1d3ee8ccca9129ee7b7f57c4`.
