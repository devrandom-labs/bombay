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


## Fresh ordinary-Rust signature comparison (2026-10-02)

An isolated archive of baseline `2fccedf6eb636ac22143e7e01de7e784f96e2b4e`
at `/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-application-signatures-b8m4lay2`
compares async inherent methods with an ordinary free `run_application` using
the same concrete bounds and launch body. The exact lock remains unchanged.
No trait, macro, erasure, runtime wrapper or new public type is required for
these signatures. No production candidate is selected.

The clean original stopped-root `.run().await` fixture fails only E0277:
`Result<_, RunError<Never, _>> is not a future`. The isolated candidate changes
seven runners to async methods awaiting the existing LaunchSystem futures.
Compile checks then pass for inferred simple/root-work, advanced App,
heterogeneous declared children, two Entity families and HTTP signatures.
The free-function simple and heterogeneous-child callers also infer root and
members; an ordinary result annotation supplies the terminal type.

The family product candidate
`(Result<(Output, Terminal), RunError<Root::Error, Terminal>>, Shutdowns)`
also compiles with the same consumers and adds no public type. This establishes
representation and inference only; it does not select cancellation, panic,
family-failure or receiverless-value policy.

Commands ran in the isolated workspace through the pinned shell:

```sh
nix develop -c cargo check --locked -p bombay-rs --test application_signature --target-dir /Users/joel/Code/devrandom/bombay/target
nix develop -c cargo check --locked -p bombay-rs --features axum --test application_signature --test run_with --test axum --test application_terminal_custody --test entity_application --target-dir /Users/joel/Code/devrandom/bombay/target
nix develop -c cargo check --locked -p bombay-rs --test application_protocol_denial --target-dir /Users/joel/Code/devrandom/bombay/target
nix develop -c cargo check --locked -p bombay-rs --test application_role_denial --target-dir /Users/joel/Code/devrandom/bombay/target
```

Original first command exits 101; candidate first and combined positive command
exit zero. The combined command also passes after direct family-product
substitution. Wrong protocol exits 101 with E0631 (Root versus Other handle);
wrong child role exits 101 with the intended missing concrete ProjectTerminal
implementation. These are signature/static-denial evidence, not runtime tests.

Patch SHA-256:
`bda9172c2a5f0393263b6ea013f362cc64e3b5fe686123984cc5dce1ec96a21e`.
The directory retains the patch, source hashes, receipt and compiler logs.
Isolated delta: nine paths, production +49 / -57 / net -8;
tests +188 / -69 / net +119; public types +0 / -0;
one experimental public free function. Repository production/test delta zero.
Complete task custody, executor convenience/defaults/features, truthful error
ownership, HTTP consolidation, runtime traces, migration and independent
review remain required. DG-API stays open.

## Startup projection conservation (2026-10-02)

Current `DirectRoot::startup_error` returns only the reason/error for allocation,
initialization and host rejection, discarding the remaining recoverable actor
custody. `DeclaredRoot` has corresponding projections. This is a Bombay
application-boundary issue: the existing local `SpawnError` and `ActorRetirement`
already retain the exact values; Behavior needs no new contract for this law.

An isolated initialization-rejection witness calls the actual DirectRoot
projection with move-only actor/error/descendant Vec allocations and a queued
control event. The original exits 101 in debug and optimized builds at the
intended lost-custody assertion, one executed test per command. A plain comparison
uses the existing `into_retirement` and terminal projection instead of destructuring
with `..`; the complete trace and original allocation addresses then pass once
in each profile. This proves the narrow loss and an existing ownership-preserving
conversion, not all startup paths or a final error API.

Scratch: `/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-startup-custody-iatm4s5j`.
Baseline and lock are unchanged. Original witness patch SHA-256:
`38f6ba15fb0da6afded54e79af6bbf1c278b4df6550cdfa4e1326895d35079d5`;
comparison patch:
`0218f38fc18f571c9a70183132028cd2a24c6956e9de701ae929a6c97e7c8da4`;
receipt:
`045bc0f108dafaa6bf67b043e06c27af31dacea74eff8c7f63b5f55fc5df41c9`.
The receipt contains all four exact log hashes and profile-specific target paths.

Each profile uses the pinned shell and the same filter:

```sh
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked -p bombay-rs --lib startup_projection_preserves_exact_failure_and_remaining_custody --target-dir /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-startup-custody-iatm4s5j-target
nix develop /Users/joel/Code/devrandom/bombay -c cargo test --locked --release -p bombay-rs --lib startup_projection_preserves_exact_failure_and_remaining_custody --target-dir /var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-startup-custody-iatm4s5j-release-target
```

Isolated delta: one already authorized application-runtime path;
production +3 / -3 / net 0; tests +72 / -1 / net +71;
public types +0 / -0. Repository production/test delta remains zero.
The comparison uses `RunError::Unpublished` only to test existing representation;
its current committed-root wording does not cover every startup rejection.
Independent reviewer `/root/observation_research` authenticated every source,
patch and log hash and accepted the bounded failure/feasibility claim. Its review
rejects treating the probe variant spelling as the required law. Truthful final
names, staging/allocation/host/declared-root custody, coexistence with family
outcomes and independent design-gate acceptance remain required.

## Current selected runner reconciliation (2026-10-03)

Core/Actors now select the verified 0.21.1 registry release at
`5ca96444f0a66e9a013b6989e3e53d345cbabf65`; Macros 0.13.1, Address 0.3.0,
Communication 0.1.3 and the exact Timers patch remain selected. Earlier
0.17.0/0.20.0 source signatures and probes above are dated research, not
current dependency authority. Canonical source still has seven synchronous
public runner constructions. Ordinary Application/App signatures and exact
startup errors remain unchanged by the dependency selection.

Current `LaunchSystem::launch_with` obtains the root through the shared
`launch_application_root`, awaits user work, then joins and projects the root.
HTTP also uses that same root transaction after binding its listener; its
server coordination body remains separate. Thus the earlier description
of duplicated root construction is obsolete, while HTTP coordination
consolidation is still required. Entity ordinary-error shutdown products and
dropped execution/startup results retain the separately witnessed losses.

The independently reviewed source-retirement correction remains an isolated
comparison. The lower ownership handoff has independent bounded review,
recorded in task-custody.md; its complete application integration remains open.
Neither selects this public async API
or implements its complete root/family/result ownership. Keep DG-API open
until those owners and the exact inferred consuming signatures, executor
preconditions, returned values and consumer migration are accepted. No new
runner spelling, type or compatibility contract is inferred from compilation.

Current canonical runtime source SHA-256 after the accepted static shutdown
correction:
`ebb679fe18c2afc493c392e4f38259a2c6d502ea46eaeadea5e0a822504a0357`.
The earlier inspected 0b8e2c1c source above remains historical; the public
synchronous runner behavior described here is unchanged by that correction.

## Independently reviewed paired-work comparison (2026-10-03)

The signed selected-version startup handoff is now independently reviewed in
the task record. Its isolated successor compares an inherent
`Application::execution_with(work)` with the same ordinary free function;
both return separate opaque Send execution/result futures. Construction owns
the untouched inputs before polling. The original callback, its work future and
output can borrow caller values without acquiring a `'static` requirement.
Actual root construction uses the same existing startup/cancellation/raw-join
transaction; no second actor loop or runtime wrapper is introduced.

Five tests pass in both pinned-Nix profiles: original unpolled Application;
original advanced App address-space identity; initialization Stop with the
uninvoked callback and complete retirement; immediate borrowed pending-work
release with independently retained root result; and exact completed borrowed
output retained or explicitly surrendered. A surviving execution owner still
owns executable inputs when its result receiver is surrendered. Receiver drop
does not grant authority to revoke that live execution; undeliverable values
are released once when their owner attempts result transfer.

Three comparison inversions erase untouched inputs, the uninvoked callback or
completed output; each compiles and fails in both profiles. The fourth mutation
merely disables the pending-work destructor's observation counter. It proves
instrument sensitivity, not a work-ownership inversion: the work future still
drops. Exact restored positives, strict library/test Clippy and formatting pass.

Increment: tests +480 / -0 / net 480 in one already approved existing path;
production/public types zero. Receipt:
`e47e3bd21a0fab41217cfd387689bb516a696b1741c8818d37490b32897317ec`;
patch `f7d461d40093aa997c74433cb0b65454be50dccc2415896f90fd4b8b516abbb7`;
inspected source `3dc6e3722a915c8d9200badeb4a886efe2d62092911cce2b3d7755e8b8a9112e`.
Independent reviewer `/root` read the complete patch and verifier script,
authenticated 20 stage artifacts and all 414 nonbuild archive hashes, checked
the unchanged foundation/source prefix and both-profile positive, mutation and
restoration logs. This is bounded source/artifact acceptance without fresh
independent Rust execution, not DG-API or a public runner selection.

Callback Err/panic, stopped-root/pending-work ordering, blocking runtime
ownership, HTTP, installed families, heterogeneous births and integrated
failure/projection custody remain required. The unstarted callback's concrete
captured allocation/drop oracle also remains to strengthen. Existing actor
executor and capability errors are intentionally excluded from this comparison;
its unwraps establish no recoverability law for those errors.

## Independent idle-family comparison review (2026-10-03)

Root read the complete incremental family patch, actual paired kernel and all
eleven caller tests, authenticated 23 receipt artifacts and the 195-source
inventory, and reran all eleven tests in both profiles through pinned Nix.
Both fresh runs pass. Independent review receipt:
`/tmp/bombay-family-root-review.json`, SHA-256
`32a091e0487c0a0d1d526fcd3e878a418821d2825d91b4ca338b198cf97ba69f`.
Frozen family source receipt SHA-256:
`ce21a1a6c40716964aeb4965d9ba9bf8206269b833b77cd7142cb11d142c1fff`.

The actual installed family and actor join have one cleanup owner; its completion
acknowledgement follows installed.shutdown. Original uninvoked callback, borrowed
completed output, root result and full nominal family shutdown/metrics product
coexist at the separate receiver. All six recorded omission failures compile
and fail at runtime in both profiles; the earlier E0282 mutant is excluded.
The cleanup-handle omission proves publication loss, not failure to join.
Current ordinary callback policy remains unchanged under XO-07.

This bounded review accepts the incremental comparison only. It covers an idle
family with zero represented entities. Active NativeEntity retirement, a delayed
family completion barrier, arbitrary non-Send child declarations, borrowed custom
installed-family contracts and integrated HTTP ownership still need proof. The
family kernel removes an unconditional I:Send bound; legacy paired/blocking
wrappers retain it and therefore do not establish the complete public surface.
No production or public interface is retained and no full gate is approved.

## Independent HTTP bind-custody comparison review (2026-10-03)

Root read the complete incremental HTTP patch, ordinary binder, actual public
run_axum witness and both verification scripts, authenticated all sixteen logs/
artifacts plus five manifest pins, and reran the original and comparison tests
in both profiles through pinned Nix. The original public bind path fails the
intended custody assertion in both profiles after reporting AddrInUse: it
releases the uninvoked router’s captured allocation. The comparison’s thirteen
tests pass in both profiles. Independent receipt SHA-256:
`8289e476433c20411dde6181149c56f2e65f0d17a82b240217b18af3488ef563`.

A first shared-target comparative run reused the preceding original fourteen-
test binary and is excluded. Refreshing only the comparison source timestamp
forced a fresh compile; source bytes remained exactly frozen. No artifact
reuse is treated as semantic comparative evidence. The E0283 preliminary Router
inference error also remains excluded.

The ordinary binder returns the exact I/F/io::Error and retry uses the same
paired execution core. Its preparation future still loses inputs if dropped
during binding and is not integrated with the paired result receiver or serving
the recovered listener. The unchanged public serve witness independently checks
complete root origin/terminal; it is not proof of integrated HTTP drop ownership.
This bounded review selects no public surface or production repair and approves
no full gate.

## Independent active-family comparison review (2026-10-03)

Root read the complete two-path incremental comparison, native laws and
publication mutants, authenticated 195 source hashes, six manifest hashes and
39 artifacts, then reran all twelve family tests in both profiles through pinned
Nix. Both fresh runs pass. Frozen source receipt SHA-256:
`d668deffc8c85b9f51bf9aaa73bd6e78ea893f2f39aed8586e1c9813295fd309`.
Independent review receipt `/tmp/bombay-active-family-root-review.json`, SHA-256:
`b333f61ca93f836d36eb8fb1e4013843f9cddf8a5331e95825656e2372d2d61c`.

Actual Directory/NativeEntity activation admits a genuine ledger command through
a pure fold. A runtime-definition callback holds the exact retirement report
and delays family completion. Dropped execution preserves completed borrowed
output at the independent receiver; the result stays pending until that callback
finishes, then returns output, root retirement and the complete family role/
shutdown/seven-metric product. Receiver surrender releases output once while
surviving execution still joins cleanup. Two publication-loss inversions compile
and fail their intended assertions in both profiles. They prove publication
custody, not a family-join omission law.

A separate pure Vec-owning actor reproduces the original Native pre-owner-wait
loss: dropping the actual pending retirement future cleans up the task but loses
the definition callback result. Both original runs fail the custody assertion;
ordinary original-lease retirement preserves the exact Vec/queued shutdown and
all typed remainder lanes in both profiles. The integrated cooperative actor
can instead stop gracefully under the original wait; no original integrated
deadlock is claimed.

This accepts bounded research only. Delta is tests +470 / -7 / net 463 plus
two source lines for a conditional test-only wait substitution/comment; no
non-test behavior or public types change. That substitution is not a production
repair. Native retirement-future drop still requires ownership transfer even
without the wait. Recursive child/failure integration, universal public API,
initial non-Send declarations and exact undeliverable static-result destruction
remain open; no full decision gate is approved.

## Independent absent-work comparison corrections (2026-10-03)

Nonauthor /root/contract_inventory authenticated all 31 artifacts and the
345-path workspace inventory of the frozen absent-work comparison. Independent
review receipt `/tmp/bombay-work-absence-independent-review.json`, SHA-256
`9c525ba1e312b2a6f614da52801ad7558b7f36401bb01529f519ec1b5c17df2f`.
It accepts only bounded absence/natural-root/pre-poll custody evidence and
requires two corrections before retention. The optional-work kernel discards
startup Err in its None arm, and the absence adapter removes the corresponding
Result distinction. Preserve the real startup Result with coexisting absent
work/root/family results; no unreachable or success substitution is lawful.
The declared LedgerChild also embeds a runtime destructor-audit cell in Behavior
state. Remove that instrument, keep pure domain Rc<Vec>, and observe ownership
externally. Its role is unit; the evidence proves an uninstalled non-Send child
declaration, not a non-Send role or installation. Preserve the original frozen
source/receipt and establish a corrected successor with original-defect failures
and independent review. No production or full gate is accepted.

The corrected absent-work successor is independently accepted as bounded
research by /root/contract_inventory. Source receipt SHA-256:
`3372d34c028ea17ea763539fee58ab34a678f404e491bb098a2d04fb5a79d074`;
independent review SHA-256:
`eebf9658888820927524605c62709eb7c3930b2846455efd35aebc8cd3725b43`.
The reviewer authenticated all 345 copied files, 46 artifacts and three exact
mutations. All fifteen final/restored tests pass in both profiles; startup-error
erasure, missing original-input publication and early owner drop each compile
and fail the intended law in both profiles. Formatting and strict lint pass.
Actual RecvError is kept once beside None/Some original work and complete root/
family reports, including mandatory adapters; pure Rc<Vec> child ownership is
observed externally. Its role remains unit and installation remains unproved.
Incremental change is cfg tests +93 / -59 / net 34, no production/public/private
new types. This internal error is not the selected final public RunError shape;
full API/task gates remain open. Original flawed receipts remain historical.

## Current-selected work ownership comparison (2026-10-03)

The corrected private paired execution/receiver comparison now runs against
canonical Rust 1.99 and published Core/Actors 0.21.1, Macros 0.13.1 and
Communication 0.1.3. Author receipt SHA-256:
`3f875c553908b3163a588c1f934716892ec9d725c8bc543bc487e09fcdf68d7b`.
Root independently authenticated all 399 frozen files and reran all twenty
cases in debug and release through pinned Nix. Both pass. Reintroducing the
output's unnecessary `Send` bound fails with E0277 in both profiles; omitting
completed-output publication compiles and fails the exact Rc custody assertion
in both. Restored source passes all twenty again. Independent review receipt
`/tmp/bombay-current-work-bounds-root-review.json`, SHA-256:
`4f23ec28d57a0a217439bb04fcc06f7c904fbf5f3687686fd7daa77b8defe826`.

Application work stays with its caller: callback, invocation, future and output
need no work-side `Send` bound. Existing root/installed-family cleanup retains
its executor requirements. Completed Rc output and borrowed values survive
execution drop at the separate receiver; unfinished work drops immediately;
final receiver surrender releases output. Pure actor state and complete root/
idle-family reports remain observable. Existing legacy adapters are unchanged.

This accepts bounded ordinary-Rust evidence, not a public API or full DG-API.
The isolated two-path comparison includes 53 net production lines of previously
reviewed startup support and 1,547 transplanted test lines; no new public types.
Sixteen historical private fixture declarations are new relative to canonical,
not novel relative to the prior probe. Family-definition `Send` minimality,
capability/outer-executor failure integration, recursive graph results and full
HTTP serving remain open. No canonical semantic implementation is retained here.

## Minimum uninstalled-family input comparison (2026-10-03)

The successor tests the private optional kernel's Families:Send predicate.
Installation consumes the original family inputs synchronously before the
first startup await. Only the installed family owner crosses into cleanup;
its owning Send requirement and explicit static lifetime remain unchanged.
Receptionist, shutdown, EntityDefinition and legitimate role bounds also stay
unchanged. Every supported sealed concrete family product is already Send;
this is generic-bound minimization, not support for a non-Send Entity definition.

Author receipt
cdd1f7652701c4de96294f19a1ad6e22a10db444a5821c3248fbadc4baed5e94
binds application_runtime.rs
b28c7d6ab4d76cc6fd12d77faddfc20b679aff51740df15ca51b341b98ed5c07
and incremental patch
9fcb91faf33ce133e1d2fdebef695e708091c3a066f02c988ced1c07387b32bc.
All twenty cases pass in both profiles through pinned Nix; the original extra
bound and its restoration produce intended E0277 diagnostics in both.
An additional compile-only generic body verifies both returned futures remain
Send when their actual caller captures are Send, without Families:Send or
spawning arbitrary application work. Final strict Clippy and formatting pass.
These uninvoked generic probes are static evidence, not runtime custody tests.

The incremental cfg(test) delta is +72/-2/net70; production and public types
remain zero. Two generic probe bodies and one borrowed trait predicate add
three private functions. The complete historical workspace still inherits
53 net startup-support production lines and 1,617 test lines; it is not the
current canonical tree and must never be transferred wholesale.

Coordinator inspection
2bd3d7c9ce83bb151f02087bae271e32ecbb34805d305e18712c6fb1e8e7ba5f
authenticates 345 sources and 42 artifacts and reads the complete incremental
patch, optional kernel, actual sealed installation contracts and pre-edit
model. It does not include a fresh coordinator Rust rerun. Independent non-author
review 637af0202b9cbfe6eae058954e466a8afb0dd582910ab57bc7bf3a42ac326ce3
accepts this bounded minimum-bound comparison after authenticating all sources,
artifacts and original/restored diagnostics; it includes no fresh Rust rerun.
No source retention, public API selection or full DG-API acceptance is claimed.
The nominal family has zero admitted incarnations; active family cleanup remains
a separate required integration.

## Ordinary inherent/free execution comparison (2026-10-03)

The isolated current-source archive at 77da777 compares private inherent and
free conveniences over the same paired execution/result kernel. The current
public run().await fails solely because its Result is not a Future (E0277 in
both profiles). No new actor loop, public interface, runtime/mode product or
unconditional work Send/static bound is introduced. A synchronous constructor
creates original-input custody before the first poll; conveniences await that
same pair. Genuine absence does not supply a dummy work callback.

Author receipt
94af0ee1be427dc18e04c4170560b1af0209eacf20ea763b593e3055bcec9a1e,
patch 5f079fe64d660d6e59a7da6f41f2f02bf5764c8d79cff12b538bbd4782c2453a,
and independent non-author review
7d894979703eb461d8ebb97f0195ba88d7c2592e320ab2ac2ec6d3d4c333d0e9
bind eleven positive/restored cases in each profile and four compiled custody/
preflight counterfactuals failing in both. Strict formatting and package Clippy
pass. The reviewer reads the entire module, guards, inherited handoff, tests,
mutations and scripts and authenticates 345 inputs, 54 artifacts and 165 locked
external tuples; no fresh reviewer Rust rerun is claimed.

The conditional foundation is the separately reviewed launch handoff:
production +60/-7/net53. New API code is 846 cfg(test) lines, zero production
or public types. Current spawn-body, shutdown, EV-25/measurement suffixes and
the application production prefix remain exact. No old whole file is copied
to canonical. Original Rc/borrowed work error output, unstarted application/
work, normal root retirement and owner cancellation retain complete available
lanes and original allocations. Cold non-Send child and advanced App declarations
are preserved without claiming installed non-Send actors. Nested blocking
preflight returns the original builder/application/work before construction.

This accepts bounded comparison only. Receiver retry/drop with surviving
execution, root completion while callback stays pending, automatic Send,
full owned blocking lifetime, actual build errors/modes/drivers/defaults,
heterogeneous live children, advanced installed graph, meaningful HTTP and
active Entity remain required. Foundation actor/capability/cleanup failure,
native payload custody, recursive provenance and exact public result/migration
are not proven by this kernel. Initial script aborts that expected a later
assertion remain excluded; earlier actual Weak zero-versus-one failures are
qualified as the intended custody violation. No canonical source retention
or full DG-API/DG-TASK approval is claimed.

## Normal family lifetime correction required (2026-10-03)

Independent source-order review
50634326607c231f783d472b86bea17c704ef1d30123b2875dd3230dbfbfeeb2
and model 81274a2e7c8f0ddcaa12684e8ff3c2b06b1988d60942ef1948eb91c453bfffa6
identify a concrete limitation of the frozen 94af0ee ordinary API prototype.
Canonical launch_with awaits application work before joining the root;
run_with_entities then calls installed.shutdown. The prototype's independent
cleanup instead joins the root and immediately shuts families down, while
caller work can remain pending. EntityRuntime::shutdown closes admission at
its owning boundary. A still-running callback can therefore lose a family it
is legitimately using when only the unrelated root has finished.

XO-07 and XO-10 require the existing normal order. Do not reinterpret early
root termination as authority to cancel work or close families. Early internal
root joining and early family shutdown are distinct; neither is accepted by
the earlier zero-incarnation or caller-drop witnesses. Their bounded syntax,
untouched-input and result-custody evidence remains valid within its stated scope.
The cleanup acknowledgement currently points toward execution after cleanup;
it is not a work-completion signal authorizing cleanup.

The reviewer and coordinator read the actual kernel, canonical runners,
installed-family product and Entity shutdown boundary. Twelve source/document
hashes are authenticated. This is source evidence, not a newly executed race
or original-defect regression. Before kernel retention, add an actual admitted
Entity family witness: root naturally ends while work remains pending; normal
family admission stays usable until work completes. Preserve the counterpart
where execution drop releases unfinished work and retained cleanup joins.
Compare ordinary ownership transfers before introducing any signal or product;
no additional owner, public type, policy or canonical production edit is selected.
Full API and active-family acceptance remain open.

## Owned executor comparison prerequisites (2026-10-03)

Read-only model
fbdff97186cbc92acc1cdc43fdf0d16bf3652fa4377eca6b0578666db2572a5a
compares caller-owned Tokio Builder/Runtime with one private convenience driving
the same execution/result pair. Coordinator pre-edit review
3f9eaf7300c5cf226dd4d5f57918b6de43e37da26ba9d1dc5c283883131ef309
authenticates forty source hashes and 165 external locked dependency tuples.
This permits an isolated cfg-test comparison, not public API selection,
canonical production retention, independent final approval or DG-API acceptance.

Selected Tokio block_on accepts borrowed futures and their concrete results
without Send/static bounds. Actor tasks keep their existing requirements.
The proposed convenience builds before constructing the pair and returns the
original application, work, Builder and actual io::Error on construction failure.
Selected current-thread construction propagates the I/O-driver error before
creating its blocking pool and consuming random seeds. Mio's Darwin kqueue and
Linux epoll selectors require file descriptors. A child-only descriptor limit
can therefore supply an authentic error experiment; it has not yet run.
Owning Poll/Waker tests cover construction and closure, not this failed-build
custody law. Worker-spawn panics are not returned build errors.

The comparison must separately check package-default and axum features, both
schedulers, original-input retry after a real build error, and the current
public runner's input loss. Compilation and executable launches use pinned Nix;
only the already compiled child lowers its own limit. No fabricated error,
parent limit change, native payload inspection or enabled-driver inference is
authorized. Conditional inherited production remains +60/-7/net53 and inherited
tests remain 846 lines; new comparison source is cfg-only in the approved owning
file. Family ordering, native exceptions, HTTP and the final public error and
migration contracts remain open.

## Receiver retry and surrender comparison (2026-10-03)

Corrected author receipt
0614a4dd00d6679fc9d1ca8234cd1c4d12d1efc8ec4d86c27ad47e45a8d75ad8
and independent non-author review
04e281673898aa99d6cb0d8fbe4d942023d06c15eb54e1c97444075bd2bc57b7
accept bounded receiver evidence. The same pinned result future survives dropping
a borrowed await. Completed borrowed work and its original Vec allocation return
on retry. Dropping the final owning receiver releases an available work result
once; surviving pending execution work continues, and its later undeliverable
result releases once at the closed publisher. Root completion alone does not
cancel work. Completing or surrendering that work retains the same normally
completed root rather than rewriting it as owner-cancelled.

The initial review required checking the returned actor's disposition field.
The corrected three received branches now observe its actual Wait variant;
the inherited oracle covers entries, allocation and complete remaining lanes.
Selected actor macros add no hidden instance state; StopOnShutdown owns only
its inner actor. Allocation identity is not an actor-origin proof. A fourth
counterfactual corrupts disposition during the actual pure Finish transition
and fails the new assertion after work release and cleanup join in both profiles.
All four counterfactuals compile and fail their intended runtime assertions;
fifteen restored positives pass in each profile. Strict package Clippy and
formatting pass. These are research counterfactuals, not original public receiver
bugs: the current public runner has no result receiver.

Both reviewer and coordinator authenticate 345 sources and 39 artifacts; neither
claims a fresh Rust rerun. The receiver increment is tests +271/-1/net270,
production zero and public types zero. Aggregate conditional tests are 1,116
lines; inherited launch production remains +60/-7/net53. After receiver surrender,
Normal termination and execution joining are observed; no unavailable full root
receipt or destructor count is fabricated. Families remain unit in this evidence.
The signed normal-family ordering blocker remains, and no canonical kernel,
public result API, native failure, recursive-origin, HTTP or full gate acceptance
follows from this bounded comparison.
