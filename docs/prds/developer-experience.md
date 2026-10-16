# Developer experience coordination

Status: active. Work is isolated on `codex/devx-policy-interface`;
the other Bombay checkout is untouched. Initial inspected baselines were
Bombay `c3afb3011090ac4c9112fd39e4a76980db856df0` and Behavior
`d69f992b371c12ab34e73b18e45b8112c90a1508`; current Bombay selects published
Behavior/Actors0.26.0, macros0.14.2, Communication0.1.6 and Timers0.2.0
without repository patches.

Latest-main integration, pre-edit (2026-10-15): isolate upstream8d809b3 on
`codex/devx-client-integration` from fully gated ea2b5b4; keep the unfinished
DX15 interpreter work separate. The selected registry owners remain
Behavior/Actors0.26.0, macros0.14.2, Communication0.1.5 and Timers0.2.0.
Upstream's Application-owned export client derives exact association from the
existing allocation owner, preserves the complete original recipient on refusal,
and keeps native nonwaiting Full/Closed command custody. This is deliberate
Bombay authority policy, not a new actor-model law. Preserve its original root,
child, foreign-application, closed-target and supervisor recovery witnesses;
no new interpretation engine, actor state, policy default or public type is
proposed. Reuse the existing Application client and sender owners and audit
ordinary syntax after merging. Expected integration edits are limited to
overlapping interface, startup and verification contracts, with no additional
production capability. Upstream test hashes and exact inputs are frozen in
`CLIENT-INTEGRATION-INPUTS.json`. A clean merge alone proves no acceptance row;
focused debug/release tests, static denials and combined repository gates remain
required before delivery advancement.

The first combined compile fails E0061 at the imported export-client allocation:
the verified DevX owner already requires its configured reply capacity. Reuse
`ApplicationHandle::client_capacity` exactly as `client_to` does; do not invent
a default, new limit or allocation path. Before correcting that call, add a
two-slot certified reply-client journey with three owned boxed commands: two
accepted messages retain their origin/allocation, Full returns the exact third,
and ordinary shutdown returns complete retirement. This is the same DX06
resource-owner law through the newly imported client, not a new abstraction.
An explicit default-capacity inversion must fail that black-box witness.

Current integration evidence: all363 selected native/library tests pass on
Rust1.99 in debug and release (337 library,5 interface,16 client,5 supervisor),
including the existing static denials. Both actual Node hosting journeys pass
in both profiles. Focused strict Clippy, Rust formatting and Nix formatting pass;
all385 frozen Rust/configuration hashes remain unchanged. The separate-source,
separate-target capacity inversion fails at the intended third-command check
in both profiles; exact restoration passes both controls. Two independent
review passes confirm all five imported test bodies retain their original
assertions/effects after only complete-product acquisition changes. Their
admission traces are bounded evidence; they do not independently prove every
retirement lane. The new capacity witness checks normal retirement lanes too.

Integration checkpoint: the allocation correction reuses an existing validated
capacity and changes one production call argument (+1/-1); no state alternative,
transition branch, module, public type, bound or lifecycle authority is added.
Existing scope capacities and WorkOutcome/ApplicationOutcome custody are reused,
with no history, repeated cause, semantic boolean or structural caller alias.
The upstream association feature retains its own new remote-only owner reference
and ExportError; those are imported source, not further DevX abstractions.
Cross-check DX04/06/20/25/27, the selected Behavior receiving laws, Driver law and
execution ownership. Disposition: pass for this bounded reconciliation. MSRV,
layout costs and all combined-source Nix gates remain pending; delivery stays
at fully gated ea2b5b4. Evidence: `CLIENT-INTEGRATION-*`, `CLIENT-HOSTING-*` and
`CLIENT-CAPACITY-*` under the existing evidence root.

The frozen committed c3b74ab is being integrated on
feat/remote-devx-integration; its active authoring worktree is untouched.
Records below retain their original source and verification provenance.
The combined remote-custody integration has its own change, source and test
records in [the remote PRD](static-remote-actors.md#devx-reconciliation--pre-edit-integration-record-2026-10-13).
Its reviewed PR and passing required CI remain pending; imported historical
gates do not certify this combined source or close the complete DX programme.

One Application candidate owns ordinary roots, cold actors, declared topology,
HTTP and Entity families. Clients/senders select the root, another exact recipient
or a stable Entity reference; lifecycle delegates to its existing owner. Topology
derives finite, heterogeneous and recursive custody. The complete38 receiving
journeys and five HTTP runtime journeys preserve all504/103 original assertions
and pass both compilers/profiles/features. Real native timer/shutdown wrapper
orders and all cold/family receiving/refusal journeys also pass. There is no
second Environment or actor engine.

Pre-root endpoints now use `ApplicationScope`: establish the exact recipients a
pure constructor needs, then consume the scope into one Application. Missing
hosts, prepared setup and ordinary/Entity/HTTP staging rejection retain the
same namespace with original inputs. The real queue and dynamic-supervisor
journeys use it without placeholder policies; queue construction infers its
types through `WorkQueue::new(1).stop_on_shutdown()`. Endpoint equality compares
the existing weak admission allocation, so clones remain equal after closure
and equal numeric addresses in distinct applications remain distinct.

The bounded scope/equality checkpoint passes: 418 adjacent recovery test
executions and40 native-identity/HTTP-staging/guide executions across both
compilers/profiles/features, plus the earlier68 focused executions and24
authority denials. All127 family,514 receiving and106 HTTP assertion bodies
remain unchanged and in order. Eight extracted-package guide configurations
also pass48 complete queue/dynamic journeys and24 executable doctests; three existing schematic fragments remain
ignored per configuration. All61 packaged Rust source files and the guide are
byte-identical to source. The archive consumer uses actual extracted unpublished
Engine/Bombay macros and101 unchanged resolved registry dependencies; it is not
a registry-only publication claim. All21 native Nix gates for this batch pass in2244.019 seconds;
the captured runtime source, manifests, locks and compiled guide match8011d49.
Four later edits only correct dependency-version prose and coordination; no
signing or disk waiver applies. The owning PRD records the complete model and measured deltas; library
production grows184 lines for one public scope capability, with no actor state
or dependency added. Five paired counter journeys measure an incremental8 bytes
of peak heap and no additional allocation blocks. Constructor policy lists and
the remaining programme criteria stay open.

The keyed-pool consumer now proves same-generation affinity across two jobs,
exact original key/document rejection for an unknown role, generation-exact
unbind and complete joined pool/worker retirement. Its56 assertions pass
current/MSRV, debug/release and default/all-feature configurations; strict lint
passes. Dynamic supervision and the remaining preparation/failure cases remain
open. Actual fixed/FIFO/keyed consumers independently expose the direct initial
factory gap: returning WorkerSubmission without a Result annotation is denied.

The initial-factory change is delivered through authorized PR94/95. Core and
Actors0.25.0 are published from2bb4510 through normal Release38319955004 after
successful main CI38318921164. Both registry versions and annotated tags are
verified; all136 downloaded Rust sources match the reviewed release. Macros
remains0.14.1. Owner963 tests, ten native Nix gates, exact-head CI and normal
archive preflight pass. The same fixed/FIFO/keyed constructors accept direct
or genuinely fallible factories and retain complete refusal custody. Five
paired keyed journeys show identical incremental factory heap measurements.

The prior0.25 adoption selected registry crates without repository patches. Both workspace and
independent engine-fuzz locks change only the upgraded owner records. Twelve
caller/test files remove pure-factory Result/error annotations and infer the
constructor; genuine asynchronous preparation/refusal contracts remain.
The72-command matrix passes all eight configurations:560 test executions
and16 complete example runs, with zero warnings. Strict workspace/all-target/
all-feature Clippy passes. The pinned nightly causal fuzz campaign completes
2048 runs (seed20260928, address sanitizer). All1284 existing assertion bodies
in the touched files remain unchanged; library production/API0, example
production+4/-10/net-6 and tests+28/-31/net-3. Aggregate states, subordinate
alternatives, branches, modules and ownership are unchanged. The fresh full
Nix run passes all21 native gates in2177.895 seconds at e197d4d. Its source
contains the complete0.25 migration and compiled guide; subsequent unretained
scope/equality candidates require their own final gates.

Counter main is98 lines and FIFO184; terminal inspection and owned result
consumption remain independent of cleanup. Focused counter/FIFO/supervision
fixtures now use the proven direct client/external and lifecycle ports; the
supervision example follows the shared-caller path without an intermediate
interface. All88 original assertions remain unchanged;40 test passes and eight
complete example runs pass on both compilers/profiles/features. Library production
is unchanged; example caller code decreases2 lines and test callers17 lines.
The full0.25 Nix checkpoint passes all21 native gates in2177.895 seconds. The template and heterogeneous-topology examples
also use direct Application sender/lifecycle ports. Their90 original assertions
are unchanged, one unused reply protocol is removed, and both binaries pass
all eight configurations (16 executions); strict lint passes. These two caller
migrations are included in that0.25 source capture. HTTP admission removes its unused
reply protocol, target wrapper and admission-only clone; all five example tests
pass current/MSRV debug/release, including exact original allocation on closure.
The heterogeneous example removes two managed aliases without changing its
birth types, effects or retirement assertions; debug/release execution and
strict lint pass. Full tracked/untracked Rust production remains net-positive;
the owning PRD records complete deltas and per-stage provenance.

Full native workspace nextest passes625 tests in default and all-feature
configurations, with one existing subprocess-only skip. Strict workspace Clippy
and all60 MSRV declaration/host denials pass. The fresh Nix run exposed vendor
path normalization and a masked phase denial; focused actual Nix coverage now
preserves both phase denials and all13 topology denials. The latest complete
flake check passes all21 native gates (2177.895 seconds) at e197d4d, including
the keyed witness, direct-client migrations, all later Rust examples and the
compiled/package guide. No signing or disk waiver applies. The retained application implementation is
committed as76b2b0f on the isolated feature branch. Registry mapping
formatting is committed as2800432; pinned nixfmt and evaluation of all21 native
check definitions pass. Final-source gates remain required;
signing/disk failures have not been waived.

Before registry adoption, the complete public creation-correlation regression
fails E0599 at the two unavailable typed-reason assertions on Core0.25. Select
published Core/Actors0.26.0 and macros0.14.2 only; locked Communication0.1.5,
Timers0.2.0 and the other registry identities stay unchanged. Release98983b4’s
complete AGENTS blob4996c149 matches the inspected instructions. The existing
native kernel still allocates before checking its creator-local map; rerun the
regression before adding its guard. Freshness is the research law; checked local
correlation and rejection before allocation are derived/Bombay policy. Return
original request ID, child, route and explicit birth/replacement kind; preserve
accepted prefix, later sibling, original map/order and joined retirement. Reuse
the current HashMap and typed ItemSettlement rejection: no new type, bound,
state, trait, capability or dependency family. Detailed pre-edit law, drift
measurements and verification are in the owning PRD.

The actual registry regression first fails E0599 on0.25, then reproduces the
occupied-map panic on0.26 before the guard. The retained six-line check returns
the original RoutedCreation before allocation. Native birth/replacement/replay
and joined prefix/suffix retirement pass Rust1.99/1.96, debug/release. Removing
that guard from an exact source snapshot restores the panic in release. All207
downloaded owner Rust files match98983b4. Workspace161 and fuzz29 registry
identities change only the three owner records (two in the engine fuzz graph).
No new state, public type, trait, bound, module or retained field is added.
CreationBinding remains Starting | RetiringStartup | Established | Rejected |
StartupRejected, with the same payloads and single runtime transition authority.
Lexical matches16/10, arms47/31, enum declarations3/3 and public declarations3/16
stay unchanged in creation/children; one map-membership conditional is added.
Production+11/-2/net9 (including contract prose); integration test+288/-0.
The final adoption passes all21 native Nix gates in2306.566 seconds, including
646 optimized workspace tests (one existing ignored subprocess-only case). All459
captured source hashes match. Current/MSRV debug/release package journeys prove
correlation, fixed/FIFO recovery and all four compiled-guide configurations; the
final three archives contain61 identical production Rust files and195 current
other Rust files. Only the three owners change among140 package registry identities.
The archive tests use the actual extracted unpublished Engine/Bombay macros; this
is not a registry-only Bombay publication claim. No signing/disk waiver applies;
Linux and x86_64 Darwin are unexecuted. Prior0.25 gates retain their revision limits.

A real duplicate-creation correlation panicked and lost lawful retirement.
Behavior commit821380e adds its missing typed reason, passing962 pinned tests,
strict Clippy and all ten owner Nix gates. A private native guard returns the
original birth/replacement/replay request before allocation and preserves the
accepted prefix/later sibling and joined children on both compilers/profiles.
Both defect inverses reproduce the panic. PR93 merged externally as6eb4f73;
its exact main CI and normal Release pass. PR91's pure birth-alias fix merged
under explicit authorization as79cae26. Release PR97 at8064f22 passes the
corrected four-workspace/archive preflight, all ten native gates and exact-head
CI38350983446. It merged as98983b4 under the same authorization. Main
CI38352087285 and normal Release38353101159 pass; independent registry and
GitHub release checks confirm Core/Actors0.26.0 and macros0.14.2 are published
and un-yanked. Bombay now selects those registry owners; fresh downstream
verification is recorded below, separately from the owner release.

WorkerFactory now owns an ordinary callable for replacement preparation, so
fixed/FIFO callers need neither a custom source struct nor two trait impls.
Direct and fallible submissions use the existing exact ordered preparation and
retirement path. Three fixed native tests prove every rejection position,
accepted prefix/untouched suffix and both stash/shutdown orders; a separate FIFO
journey proves customer completion, replacement and joined graph retirement.
Six capability denials cover invalid callability/output/thread ownership,
forged source refusal, cloning and affine reuse. Real source-task failure and
cancellation custody retain their existing distinct contracts. Current/MSRV
and debug/release native matrices pass; actual empty/captured factory sizes
remain0/0 and8/8 bytes, with a24-byte preparation future. The example deletes33
production lines and preserves25 assertion bodies. Library production grows99
lines for one public runtime capability; no actor state, dependency or trait is
added. All21 final-source native Nix gates pass in705.439 seconds after freeing
completed task-owned caches. The preceding1906.182-second disk-exhausted run is
a failed attempt, not a waiver. All18 captured changed files match source.
Fresh extracted Engine/macros/Bombay archives contain61 identical Rust files
and the identical guide; current and MSRV debug/release journeys/doctests pass.
The144 resolved registry identities match the earlier archive graph. Current
trybuild snapshots pass all six denials; MSRV checks use the separately passing
category/span corpus, rather than treating current-compiler snapshots as portable.
The three existing schematic doctests stay ignored. Constructor settings and
source hierarchy remain selected work: the user requires familiar nouns,
meaningful parent/child ownership and startup vocabulary, with private details
curated by each owner. Those naming changes are not yet registry-delivered.

Five paired complete counter journeys use identical77 registry dependencies.
Allocation counts remain74, while peak heap is122916→123644 bytes (+728,
0.592%). Profiles attribute the increase to task storage; no memory reduction
or throughput claim is made. Broader pool/Entity/recursive memory, resource
controls, cancellation/late replies, package gates, distillation and delivery
remain selected acceptance work.

The existing API guide is the package readme and crate rustdoc. Its complete
leaf Application/shutdown/retirement example executes on all eight configurations
both in the workspace and the extracted runtime/engine/macros archives. The
archive guide is byte-identical;59 local source links resolve. Three schematic
topology fragments remain explicitly ignored. This documentation port changes
no runtime API or state; guide minimization and rendered links remain open.

The owning specification is Behavior's
`docs/engineering/developer-experience-prd.md`, with its source coverage appendix.
Read that file from the paired branch while this work is unmerged. This local
record deliberately does not copy its 28 requirements or invent a second
acceptance authority.

## Selected direction

Use the existing actor facade for domain actors, `ActorExt` for genuine
same-actor decoration, owning constructors for standalone policies, named
`Actions` lanes for effects, and one `Application` spine for local execution,
HTTP and Entity. The user approved one explicit application topology declaration
on 2026-10-11, with structural spaces and complete recursive custody derived
behind it. Selection is application-scoped and validates actual decorated birth
rows. Keep real policy and exact failure handling explicit.

Bombay owns application assembly, hosting, external clients and terminal
consumption. Behavior owns the static transition and effect algebra; Actors
owns reusable compositions. Simplify at the owner that can derive the repeated
information. Preserve affine custody, creation provenance, service lanes,
initialization order and static capability distinctions.

## Ownership and delivery

Startup custody retains receipts, controls and tasks before awaiting; native
failure retains original definitions and explicit birth/replacement provenance.
Closed child control and parent-report admission return their original typed
inputs. Root/family receiving and root-first Entity shutdown remain independent.
DirectoryConfig validates immutable limits before affine application inputs
transfer. Complete terminal control sums and ActorCustody remain authoritative.

Bombay3a80848 selects published Behavior/Actors0.26.0 and macros0.14.2 from
98983b4 without patches, plus Address0.3.0, Communication0.1.5 and Timers0.2.0.
Current native21 gates,646 optimized tests and current/MSRV package journeys pass;
exact results and their scope limits belong in the owning PRD. Policy delivery
PR98 changes owning paths/configuration/startup names and remains unpublished.
Keep that migration separate from the registry-backed decoration work.

Networking, durability, identity and deployment remain separately deferred.
The evidence appendix owns coverage of270 backlog rows,100 failure records,
seven examples,45 catalogue entries and19 capabilities. This coordination file
adds no requirement checklist or second acceptance authority.

The initial documentation cleanup replaced the large execution campaign with
its compact [preservation contract](execution-ownership.md) and deleted nine
chronological companions. Git preserves their history. The owning evidence
appendix records that disposition; do not restore retired ledgers.

Verification binding correction (pre-edit): current Driver/template evidence still
names0.23/d69f992b while the actual graph selects0.26/98983b4. Existing hard-coded
manifest tests check mutual agreement but cannot detect that stale selection.
Add a focused test comparing the existing manifest owner versions with locked,
offline Cargo metadata; its expected RED is0.23 versus0.26. Then rebind manifest
versions/revision, existing assertions and current guidance to the verified207
registry sources. Reuse serde_json and the existing pinned Cargo; add no dependency,
production type, state, bound or protocol. Check the template inventory against
actual0.26 owners before retaining its new binding. The interrupted first Nix run
is not a completed gate; rerun all21 checks after this correction.

Binding RED fails for the intended0.23/0.26 mismatch. After updating the two
manifest headers/evidence revisions, all ten Engine manifest tests pass; the new
test also compares the template owner against the locked package graph. Actual
registry source confirms every45 template and19 capability spelling/source and
evidence location; this is presence/inventory evidence, not a new exhaustive
semantic proof. Versions now name98983b4 and0.26/0.14.2. Test-only+46/-5/net41,
with no production/API change. The first full run was deliberately interrupted
after426.907 seconds for this correction; no pass is inferred. The final all21 gates
execute the corrected metadata and all original law mutations. Current/MSRV
manifest suites pass all ten tests in both profiles. Complete adoption delta:
production+11/-2/net9; tests+334/-5/net329; public types+0/-0; no new
production module or retained field. Documentation/manifest/lock changes accompany
this bounded fix. Other DX requirements and unpublished policy changes remain open.

Final shutdown decoration (DX09/21): implement ActorExt::finalize_on_shutdown
using the existing registry FinalizeOnShutdown and ShutdownReaction. The owning
PRD records the pre-edit law, full native missing-method witness and effect/order/
retirement checks. This coordination note adds no acceptance checklist or
Behavior/runtime contract. Existing published versions/interpreters are verified;
no publication dependency blocks this independent facade work.

The finalization facade at b2c208b adds one method and no stored state or owner
contract. Its pure two-order test distinguishes outer stop from outer finalization.
The final optimized workspace suite passes651 tests with one existing subprocess
skip; strict lint and both MSRV profiles pass. All106 original assertion statements
remain. The owning PRD records the order law and exact counts. All21 native Nix gates pass at80348a6 in2358.777 seconds with source unchanged.
Extracted-package current/MSRV debug/release runs pass21/21/20/20 cases, using
all61 identical production Rust files, the complete guide and native tests. No
signing/disk waiver or registry-publication claim applies. Other DX criteria
and policy publication remain open.

Timer ownership (DX13): the owning PRD records the pre-edit law and baseline
journey. The selected registry interpreter keys timers by TimerId; distinct
wrapper occurrences cannot acquire automatic identity by sharing a fixed ID.
The next proof uses actual delivery, closure and full retirement, with no
production change or dependency upgrade before the ownership equation is proven.

The timer baseline now retains two complete native session witnesses, passing
both compilers/profiles and strict focused lint. Distinct IDs deliver every
notice; equal explicit IDs replace the earlier schedule. No-ID syntax fails
E0061. Reifying the existing event lane into the unchanged queue's key compiles
but breaks named replacement in both profiles; the exact interpreter is restored.
The owning PRD and DEAD_ENDS record the falsifier and counts. Automatic and
explicit names need separate meaning before production/migration; DX13 remains
open. These focused tests do not replace the final repository gates.

Exact termination decoration (DX09): ActorExt::observe_termination returns the
existing registry EstablishedTerminationMonitor with the complete original
request and reaction. The owning PRD contains the pre-edit law and native
constructor baseline, missing-method RED, custody equations and change budget.
The real external peer's original terminal report crosses both shutdown orders.
Shutdown while observation is pending retains an independently accepting peer,
complete joined monitor custody and the exact later rejected command. This
coordination note adds no checklist or new Behavior contract. Four native cases pass all eight compiler/profile/feature configurations;
all17 existing interpreter regressions pass both compilers/profiles. The actual
three archives preserve61 production Rust files, the guide and observation.rs;
four packaged compiler/profile runs pass the native cases and executable guide.
All21 native Nix gates pass atfcb85ca before main integration; the combined
revision requires fresh verification.

Main integration: the user requested the isolated DevX branch adopt current
Bombay main. Fetch selected fb533b5 (#362); #359 squashed the DevX work through
c3b74ab. Reconcile from that imported source revision, preserving later main
retirement/notification custody and remote work together with DevX worker
factories, creation correlation, finalization and exact observation. No new
actor semantics or public types are selected. Adapt only added consumers to
main's independent work/native/notification receiving product, checking every
original value before assertions. Manifest owner selection remains the released
Behavior/Actors0.26.0 and macros0.14.2. Focused merged regressions, examples,
strict lint and all native gates remain required before claiming integration.

Pre-merge fcb85ca: all21 aarch64-darwin Nix checks passed in2504.413seconds;
all485 captured tracked-file hashes remain unchanged. Darwin signing and disk
failures were not waived. This result does not verify the merged revision.

Merged consumer verification:39 focused all-feature cases pass, with one
existing ignored subprocess case. Six lifecycle/protocol denials and six
worker-factory denials retain their exact current-compiler snapshots. Strict
workspace/all-target/all-feature Clippy passes. Imported production is retained;
relative to selected main, existing DevX production is+203/-8/net195 across
four files, with no merge-invented public type or capability. The merged optimized workspace subsequently passes717 all-feature tests;
all24 combined native Nix gates pass ataf87ef4 after isolated lock repair.
Remaining programme consumers and final reconciliation remain open.

The user requires every upstream integration to improve its developer
experience. The independent work/native/notification product preserves
correctness but exposes cleanup bookkeeping in ordinary applications. Retain
this advanced receiving capability while proving a simpler complete ordinary
result path; do not wrap the tuple just to rename storage or hide originals.
Counter and unrelated application consumers must exercise completion, startup
refusal, work interruption, native retirement failure and notification failure
without lost values. This remains selected DX01/05/17/19 work, not a completed
merge-only requirement. AGENTS now records the standing integration rule.

Combined bdaf904 optimized all-feature nextest passes717 tests, with one
existing skip (860.467seconds including compilation). The complete native
Nix check fails after1001.945seconds with528 source hashes unchanged: main's
protected-counter vendor uses its isolated lock's Behavior/Actors0.25.0 and
macros0.14.1, incompatible with runtime0.26.0. These imported native checks
add three gates, bringing the combined scope to24. This failure is not waived.
Before editing, retain the failed vendor resolution as the regression; update
only the isolated lock to the already selected published owner contracts.
Expected delta: three package versions/checksums, zero production symbols,
semantic states, APIs or dependencies. Verify its exact debug/release/fmt/lint
gate and re-run the combined native checks; preserve all remote custody tests.

Combined verification ataf87ef4: all24 native aarch64-darwin Nix checks pass
in2786.262seconds with all528 captured tracked-file hashes unchanged. The
protected-counter check retains55 debug and55 release passes, strict lint and
formatting; no disk/signing failure is waived. Other platforms remain locally
unexecuted. Those full gates cover the merged baseline, not the subsequent
joined-result changes below.

The feature branch implements one ApplicationOutcome owning work, native root,
notification and family results. Completed selection yields the business value
and ApplicationRetirement; every other phase returns the complete report.
FamilyRetirement supports inferred role selection. Cleanup is performed by
Bombay; reading its result is optional. Commit f0020c2 owns this contract and
its source-backed counter, pool, supervision, document and timer examples.

The HTTP router receives ApplicationHandle directly through
`run_axum(address, http::router)`, deleting two structs and a generic command
port. The runtime's five tests execute the actual example source, including
retained order-book state, original rejection allocation and live TCP shutdown.
Twenty current/MSRV debug/release test executions, four actual example binaries,
six defect failures and six restored controls pass. Concrete router state
measures104→64 bytes in all four configurations; no total-heap or throughput
improvement is claimed.

The combined source passes720 optimized workspace tests, with one existing
skip, in840.301seconds (65.448seconds runtime). All539 captured files remained
unchanged during that run. Strict workspace/all-target/all-feature lint,
formatting and three active doctests pass; three schematic doctests stay ignored.
Counter main51→36, pool source191→90, supervision332→99 plus61 domain lines,
documents383→34 plus95 domain lines, timer188→56 and HTTP main128→32. Their
original custody oracles are retained in runtime tests.

Entity, final package/MSRV/full native gates, compact documentation and the
remaining owning DX checklist requirements stay open. The combined workspace
pass does not close those requirements or authorize publication.

The Entity owner now provides `application.retire_entity(role, &id).await`,
returning the existing graceful/forced mode after exact-slot release. The
request-only `passivate_entity` remains available. Error cases are precisely
no active incarnation and family shutdown. Completion is shared, cancellation
of one wait preserves other observers, and missing publication stays pending
while the original task failure remains in family shutdown. Original native
and callback failures remain independent of the completed mode.

Six native witnesses pass current/MSRV debug/release (24 executions), along
with six compile denials and an ordinary error consumer in all four builds.
Three defect inversions fail in debug/release; six restored controls pass.
The affected owner23-test corpus and downstream2+11 cases pass debug/release;
strict all-target/all-feature lint, formatting and active doctests pass. The
added publication field is16 bytes; one observation pair allocates72 bytes and
releases them after its owners leave scope. No dependency or generic bound
changes. The public example is distilled to95 main lines,26 account lines and176
family-policy lines (401→297 total; main body102→64). It awaits retirement instead
of coordinating a semaphore, and retains all four complete callback products.
The runtime tests include the actual actor and policy source: seven tests plus
the real binary pass current/MSRV debug/release, with source hashes frozen during
the final matrix. The focused report-product RED fails on the original example,
then passes on all four builds. Callback and associated-type simplification,
final full/package checks and other owning DX requirements remain open.

Reply mailboxes now accept `Application::client_capacity(MailboxCapacity)` and
the same setting on ApplicationScope before early endpoint creation. The selected
limit reaches external actors, bound clients and exported interfaces; existing
endpoints retain their original limits. Validation rejects zero/overflow with the
original minimum and reports Communication's effective power-of-two/floor2 size.
Four focused laws plus the existing interface/client/scope corpus pass current/
MSRV debug/release; new rustdoc denials preserve validated construction. Saturated
reply queues preserve exact rejected values and accepted prefixes while root
retirement still completes. Missing-host retry retains the chosen configuration.
Three debug/release inversions fail and all restored controls pass.

An equal two-reply endpoint journey at capacity1024 versus2 measures35442 versus
2738 peak tracked heap bytes:32704 fewer bytes, with unchanged25 allocations and
56 end bytes in every compiler/profile build. This is a scoped capacity comparison,
not a whole-application or framework ranking. Root/birth/Entity mailbox limits,
complete resource accounting, final package/full gates and the other owning
requirements remain open.

The c2c2abb full native check failed in the isolated protected-counter target:
old tuple consumers remained, and a prior mechanical result migration malformed
one string line. The correction reuses complete ApplicationOutcome::into_parts;
no library, protocol, policy or transport feature changes. All829 original
fixture assertions remain, with only two borrow patterns gaining the empty
family product. The isolated53 native cases pass debug/release plus strict lint
and formatting. Its two former example Counter tests remain in the owning
five-test runtime target and pass there. The failed full run's548 source hashes
were unchanged. All24 full native gates still require a fresh final-source run;
cancelled gates are not counted as passed.

Feature-isolation correction (pre-edit): the frozen c8f871c full Nix run
passes ten gates, then remote-debug's strict all-target lint fails because
outcome.rs imports optional Axum and compiles its two HTTP journeys without
the axum feature. Remaining thirteen checks were cancelled, not passed.
Bombay policy: optional HTTP code and its tests are present exactly when the
existing axum feature is selected; non-HTTP outcome laws remain executable
without it. Preserve both HTTP tests and every assertion. Add feature conditions
only to their four exclusive imports and two test functions. Expected delta:
one test file +6 lines, production0, public types0; no state, interpreter,
dependency, bound or capability changes. The existing failed native gate is
the RED witness. Verify remote-only strict lint and default outcome tests,
then axum outcome tests in debug/release before another full run.

Feature-isolation checkpoint: all seven non-HTTP outcome tests pass with
no default features; all nine pass with axum in debug/release. Remote-only
all-target strict Clippy and workspace formatting pass through pinned Nix.
The six conditions are the entire Rust delta: removing them reproduces the
original source byte-for-byte; all131 assertions remain unchanged. Aggregate
and subordinate states, branches, modules and public spellings are unchanged;
production+0/-0/net0, tests+6/-0/net6, public types0. No structural caller
syntax, placeholders, flags, bounds or repeated causes were introduced.
Cross-check: DX04/25/27, existing optional HTTP contract and complete outcome
custody. Disposition: pass for feature isolation. Evidence:
OUTCOME-FEATURE-VERIFIED.json and OUTCOME-FEATURE-ORACLES.json. Full24-gate
verification remains open; c8f871c failed at861.140 seconds with548 unchanged
source hashes (ten passed, one failed, thirteen cancelled).

Actor mailbox scope (DX06 retained focused stage): deliberate Bombay runtime
configuration policy. Application::mailbox_capacity and the matching pre-root
ApplicationScope method select one validated user-message limit for root,
fresh births and Entity execution; client reply capacity is independent.
The pre-edit actor RED fails on missing setters and native Entity RED fails
on the missing validated launch input. Reuse Communication0.1.5 Config and
existing scope, child startup/capability and Entity family/runtime owners.
Two private constants across three allocation sites are deleted; no new
public type, trait, bound, generic, event, effect, phase or lifecycle authority.
The selected immutable capacity remains with exactly the owners constructing
future mailboxes, and startup rejection returns it in the original application.
Expanded cumulative authorization remains2026-10-09.

Five actor journeys plus one native Entity case pass Rust1.99/1.96 debug/release:
direct root, early scope, full original rejection/retry/accepted prefix, fresh
child, native cancellation custody, public Entity family propagation and
missing-host retry. Existing client_capacity tests4 and application_clients
tests8 (including both timer/shutdown orders) pass in all four builds:18 tests
per build total. All550 source hashes stay unchanged. Strict workspace
all-target/all-feature Clippy/fmt pass; six active rustdoc tests pass and the
three prior ignored schematic snippets remain ignored. Raw integers and forged
capacity construction remain denied. Four independent ignored-limit defects
(root, birth, native Entity, family propagation) each fail for their exact
saturation law in debug/release; all four restored controls pass and all
mutated source hashes restore.

Drift/provenance: production+94/-16/net78; tests+804/-0/net804, including existing
private fixtures preserving their previous1024 default and two wrapper settings.
Nine owner-source physical lines13851→14030 (including existing tests),
matches148→148, arrows300→300, enum declarations22→22, structs69→69, modules24→24,
public methods24→26. ApplicationWorkCustody remains StagingRejected/Unstarted/
Prepared/NotInvoked/Completed; WorkPresence remains Supplied/Absent. Creation
and native Entity outcome/retirement sums, transition authority and all
interpreter ordering are unchanged. Scope, capability inputs/owner, PendingChild
and Entity runtime each need the current limit for future construction.
No arrival history, repeated cause, false cardinality, nested transition
authority, semantic flag or structural user syntax is introduced. Existing
receiving acquisition order is preserved. Cross-check: DX06/20, Communication
admission and native creation/retirement laws. Disposition: pass for the tested
mailbox paths; complete cold-descendant ordinary inference, resource accounting
and final package/full gates remain open. Artifacts: ACTOR-CAPACITY-VERIFIED.json,
ACTOR-CAPACITY-CHECKPOINT.json, ACTOR-CAPACITY-LINT.json,
ACTOR-INVERSES-VERIFIED.json and ACTOR-CAPACITY-DOCTEST.log.

Cold inference falsifier: the first cold Library member creating Archive through
the ordinary topology DSL fails E0275 in BirthNodeAt projected self-equality,
independently of mailbox configuration. Preserve actual source/log/hash in
COLD-MEMBER-CAPACITY-INFERENCE.{rs,log,json}. No caller annotation, recursive
terminal enum or new bound is retained to hide it. Reopen DX03/11 cold inference;
the separate codex/devx-topology owner correction and native consumer must pass
its required composition, denial and final-source checks before integration.
The user guide explains actor/client limits separately and explicitly retains
control events, waiting sends, actor state, pool backlogs, Entity resident/
hydration limits and terminal custody as independent memory resources.

Cold-descendant topology pre-edit law (DX03): derived static metadata, no
actor-model or runtime semantic change. A child inferred directly from one
creator's BirthNodeAt projection already has that exact associated child type;
generated bounds must require the projection to exist without asking Rust to
prove the projection equals itself. Other parents, explicit target actors and
wrong roles still require the existing equality/role checks. Desired caller:
one topology declaration with an ordinary cold Branch creating Worker, plus
Application::with_topology(root, topology).child(role, branch). No structural
alias, recursive terminal enum or extra bound in the application.

Current source already avoids this self-equality in ordinary actor_bounds, but
cold terminal and native eligibility generation unconditionally emits it. Reuse
the existing selected-edge map and exact BirthNodeAt/ChildPosition contracts;
no new trait, constructor, public type, erased protocol or default generic.
Write the failing native compile/custody consumer first on unchanged9c7e37a.
It must execute actual fresh child creation, same-action typed read/reply, full
retirement and retained Box identity after correction. Preserve all existing
wrong-role/host/birth/authority denials and wrapper/cold/recursive consumers.
Expected files: owning topology generator, focused consumer, existing compile
consumer corpus and owning evidence. Expected production net below20 lines;
public types0; aggregate/control sums, states and runtime interpreters unchanged.
Expanded cumulative authorization remains2026-10-09.

Cold topology RED retained: unchanged owner9c7e37a fails the native Branch/Worker
consumer with exactly E0275, the same self-equality as the capacity falsifier.
No mailbox API appears in this independent witness. Before lowering, topology
parser/generator has {'lines': 1428, 'matches': 2, 'branches': 9, 'structs': 7, 'enums': 0, 'modules': 0}. Runtime control-state sums and every
subordinate native outcome remain unchanged. Retain projection-existence and
ChildPosition proof; omit equality only when the selected edge defines that
exact projection. Cold native ExecutedRoot is a distinct composition, so its
equality must remain. Native cold member creators use their own selected
projection and need the same omission already implemented by actor_bounds.
No new production symbol, public type, trait or source bound is selected.

Denial baseline audit (before snapshot edit): all24 topology inputs still fail
for their required laws;22 stored diagnostics match. Missing Trace/Readiness
have one-character rustc source-snippet truncation differences on1.99. Restore
unchanged9c7e37a macro source and rerun the exact six-case test: the identical
two snapshots mismatch, and complete actual diagnostics match the candidate
byte-for-byte. Restore candidate hash afterwards. Correct only these two
truncated display lines; no fixture, bound, expected error, omission or static
denial changes. COLD-TOPOLOGY-DIAGNOSTIC-BASELINE.json records the exact delta.

Cold topology retained checkpoint: parameter/cold-native generation now uses the
ordinary selected-edge rule. Runtime work control states remain
StagingRejected/Unstarted/Prepared/NotInvoked/Completed; work presence remains
Supplied/Absent. Every native creation/retirement sum, independent failure and
transition authority is unchanged. The selected-edge map retains exactly the
creator/child relation needed to derive each associated child; existing actor
and custody parameter sets retain their substitution dependencies. No new
subordinate state or result is added. Generator matches2→2, arms9→9, structs7→7,
enums0→0, modules0→0; two static equality-selection branches added. No arrival
history, duplicate cause, semantic flag, cardinality inference, nested actor
engine, caller structural path or new placeholder. Cross-check DX03, existing
BirthNodeAt/ChildPosition proofs, native creation/retirement and driver-law;
normalized atomic-actor architecture/features/verification/DevX laws unchanged.
Disposition: pass for this inference correction; programme completion remains open.

Production+21/-3/net18 including rustdoc; executable generator+19/-2/net17.
Tests+140/-2/net138 including two proven display-only snapshot lines; new native
consumer138 lines. Public types/traits/generics/bounds/constructors0; new test
module1, production modules0. Actual Branch→Worker creation and same-action reply
retain the full original Box through joined retirement. Frozen549-file matrix:
27 native/composition cases on Rust1.99/1.96 debug/release,108 executions pass.
Both original self-equality defects independently fail E0275 in debug/release,
and restored controls pass. Owner and actual consumer strict lint/fmt pass.
Renamed downstream tests and macro rustdoc pass. All24 topology compile denials
pass on1.99. MSRV rejects all24 too, but its snapshot harness returns101: eight
stored diagnostics differ from1.99. Unchanged9c7e37a reproduces exactly the same
eight complete diagnostic outputs; candidate is restored byte-for-byte. This
records preserved denials, not a passing MSRV snapshot gate. No snapshot weakening
or overwrite. Final full Nix gates and integrated actor-capacity cold journey
remain required. Evidence: COLD-MATRIX-VERIFIED.json, COLD-TOPOLOGY-INVERSES.json,
COLD-FINAL-VERIFIED.json, COLD-MSRV-DENIAL-BASELINE.json and existing denial logs.

Combined cold-capacity consumer pre-edit (DX03/06): integrate the already-proven
mailbox and selected-edge corrections; no new production design. Restore the
original cold Library→Archive failing consumer without an actor annotation or
terminal alias. Its two-slot fresh-child saturation must return the original
third Box, preserve the accepted prefix and return every original accepted
allocation through joined cold-member retirement. Expected delta: one test
module and its parent declaration, production0, public types0. The recorded
COLD-MEMBER-CAPACITY-INFERENCE source/E0275 is the independent RED; both owners
are now integrated at333dddb. Preserve all existing tests and static denials.

Cold witness review: first review maps original creation/reply, allocation and
capacity observations to DX03/06. Independent recursive_factory second review
finds the defining projection rule sound, preserving explicit targets, other
parents, named roles and ExecutedRoot. It also finds that custody()/behavior()
alone accept failure-bearing retirement variants. Before retaining the combined
journey, strengthen both native witnesses to require Completed(Stopped) for the
root, OwnerCancelled for descendants under the existing parent-retirement law,
successful independent notifications and empty child/capability/additional/
retirement failure lanes. Production0, no new type or bound. Existing source
inversions remain the regression falsifiers. The first frozen combined matrix
passes80 executions; it does not certify these added retirement observations.

Combined cold-capacity checkpoint: all80 mailbox/client/native Entity executions
pass current/MSRV debug/release with552 frozen files; the two strengthened cold
witnesses additionally pass all eight executions on final source. Independent
second review confirms failure-bearing retirements can no longer pass the
oracle; required operations remain outside assertions. Final strict workspace
all-target/all-feature lint, formatting and all24 current-compiler topology
denials pass with source hashes unchanged. A Clippy assertion-style failure
on Vec<Never> was corrected using the existing count assertion style; its log
is preserved and no type, bound or production edit was introduced.

Production+0/-0/net0; tests+174/-8/net166 (new cold consumer136, parent+3,
existing native witness+35/-8), public types0; test modules+1, production modules0.
All control/subordinate sums, interpreter branches and public spellings remain
unchanged. Exact current values are the original rejected Box, accepted document
prefix, child origins, actor state and independent notification/failure reports;
no arrival-history label or repeated cause added. No structural caller syntax,
semantic flag, false cardinality or nested transition authority. Cross-check
DX03/06, owning creation/cancellation/retirement contracts and preceding normalized
actor-model checkpoint. Disposition: pass for cold inference and limit inheritance;
resource accounting, archive/full gates and programme acceptance remain open.
Evidence: COMBINED-CAPACITY-VERIFIED.json, COLD-REVIEWED-VERIFIED.json,
COMBINED-FINAL-VERIFIED.json and the preserved independent review.

Latest-main reconciliation pre-edit (DX01/03/16/22): user-authorized integration
of origin/main c1d5b2b (1320dfa charged-ingress originals,406df11 native Node and
release versions) into an isolated codex/devx-main-reconciliation worktree.
Preserve the selected registry Behavior/Actors0.26.0 and all local DevX changes.
Upstream networking remains its separately owned programme; this stage adds no
networking policy. Derived custody law: every existing Application execution
port must return its original Work disposition plus independent root, notification
and family receiving products in the established ApplicationOutcome product.
Node lends its original Session and callable through existing invocation custody;
no result/error may be dropped, recreated or turned into a success classification.

Inspect upstream Node, execute_with_invocation and public native consumers before
lowering. Existing upstream Node still spells the obsolete split receiving
carrier; merging must first retain that mismatch as executable RED, then write
or preserve the smallest complete original-work/native retirement consumer before
any adaptation. Reuse the tested ApplicationOutcome/ApplicationRetirement and
existing WorkOutcome/execute_work invocation spine. No new public types, bound,
wrapper, trait, generic policy or placeholder is selected. Expected custom owner
files: execution/node, focused native consumer and current guidance/evidence;
expected custom production delta below30 lines beyond imported upstream changes.
Cumulative expanded authorization remains2026-10-09. Require normal/refusal/
cancellation custody, exact source restoration, all relevant downstream tests,
strict lint, archive and full pinned-Nix gates before crediting integration.

Imported-owner RED: pinned existing application_outcome consumer fails E0107
and E0271 before adaptation; execute_with_invocation declares obsolete tuple
receiving while execute_work already returns the established complete product.
Focused native Node witness must first refuse absent host without invoking Work,
then retry that exact move-only callable, process real counter commands, return
its original Box, check the borrowed Session is live, and inspect normal root retirement plus
both independent notification results through try_into_completed. Configuration
uses upstream's supported listening TLS profile and existing fixture PEM files;
no new network responsibility. Existing borrowed-wait and late Session lifetime
witnesses remain required after this focused proof. Keep original assertions.

Native callback decision and rejected alternatives: the user allows the advanced
SDK parameter `session: &Session`; ordinary local applications remain inferred.
The original raw FnOnce port still fails same-Node Host retry with only that
annotation (regular and async closures: E0499). Do not claim that gap solved or
remove its real lifetime capture. The following isolated comparisons were vetoed
and removed; production retains no added dependency, trait or metadata bridge:

| Comparison | Falsifier |
| --- | --- |
| Fixed-loan AsyncFnOnce, ordinary closure | E0282 |
| Fixed-loan AsyncFnOnce, async closure | same E0499 retry/close |
| Private loan-indexed result family, selected + universal proofs | non-general callable/E0499/E0597; universal variant E0283 |
| Private result-family forwarding/supertrait | E0282 and private-interface leakage |
| Published async_fn_traits0.1.1 | same E0282; optional dependency/lock probe removed |

Exact source, diagnostics, restoration and original pre-edit records remain in
NODE-CALLBACK-COMPARISON, NODE-OUTPUT-FAMILY-RESTORED, NODE-REGISTRY-CALLBACK,
NODE-SDK-TYPE-DEBUG, NODE-SDK-ASYNC-DEBUG and NODE-DESIGN-PROVENANCE artifacts.
Local signature-only carrier reconciliation separately passes19 current debug
journeys; those passes do not prove raw callback inference.

Selected joined-execution law (recorded before production): deliberate Bombay
ownership policy mirroring Application.run_with. `node.run_with(async
|application, session: &Session| { ... }).await` owns the existing execution and
receiving join. One business result type is independent of the temporary SDK
loan; the future may use Session across awaits. Raw execute_with remains the
lending-result/separate-operation port. Both pass original Work separately from
Invoke into the SAME Application.execute_with_invocation. Host refusal restores
the original declaration and returns uncalled Work; Issued precedes Closed.
No Send/static restriction on caller-local Work or its result. Canceling a
borrowed wait preserves the same operation; surrendering the whole future does
not establish joined retirement.

Pre-edit scope/budget: node.rs, focused tests and current guidance; expected
production net140–180, public types/traits/dependencies0. Reuse existing
ApplicationOutcome, WorkOutcome, NodeExecutionRefusal, Session and invocation
custody; extract declaration checks into an owned-only take_application. The
focused absent-method RED is E0599 (NODE-JOIN-RED). No second actor engine,
interpreter, cleanup policy, output-family bridge or caller structural annotation.
Independent model review accepted this responsibility before lowering.

Verification so far: final four native joined/raw tests pass current debug and
optimized. They prove actual outside-Tokio polling and exact original Box Work
retry, typed counter reply, Session after awaits, caller-local Cell/non-Send Rc,
same-operation borrowed-wait resume, raw Session-borrowing result, complete normal
root custody plus both notifications, and original Work on Closed/Issued with
Issued-before-Closed precedence. Two compile denials prove join output cannot
borrow the temporary Session and mutable close cannot overlap a raw borrowed
result; generation and normal snapshot replay pass with the intended lifetime
diagnostics. Independent two-pass review found and closed partial-retirement
oracle gaps; no outstanding focused test finding.

Separate mechanical consumer stage: five public_node receivers use into_parts
and WorkOutcome::Completed. Imported transport adds four actual receiving
operations in three sites requiring the same product decomposition. Reverse
transformations reproduce every other original byte; assertions and operations
are unchanged (NODE-UPSTREAM-MIGRATION, NODE-TRANSPORT-MIGRATION). Actual upstream
native target passes64 tests in each current profile, with one unchanged TLS
subprocess-only ignored case; all six public_node cases execute. Full TLS
subprocess verification remains its existing gate. New focused tests are included
in the existing remote debug/release Nix target lists, with borrowing denials in
debug; default-feature skipping cannot silently omit them.

Post-edit drift: node288→404 physical lines, matches8→10, arms18→22,
if-selections3→3, enum1→1, struct1→1, public functions3→4; execution1722→1719,
matches33/arms63/enums4/struct1 unchanged. Custom library production+155/-42/net113;
public types0, traits0, dependencies0, methods+1 public/+1 private. Control remains
Option<Application> (declared/transferred), original SDK Session and the same
three refusal alternatives; all Work/root/notification custody axes are reused.
No arrival history, duplicated cause, semantic boolean, false cardinality,
structural caller syntax or subordinate transition authority added. Cross-check
original Node/refusal/Session law, Application.run_with/execute_with_invocation,
current driver-law and selected Behavior ownership/actor-model checkpoint.
Disposition remains pending final MSRV, inversions, strict lint, package and full
Nix gates; no programme requirement is closed by this focused result.

Strict lint initially fails new documentation wording, a misplaced type-complexity
expectation, and an imported startup-only Counter fixture's unused message
variants. Use ordinary prose, scope the existing representation expectation to
the private complete-owned-product signature, and expect dead code only on that
startup fixture module. No new semantic type, bound, callback or visibility.
Original strict failures remain preserved; rerun the exact workspace gate.
Fresh upstream check now finds81dc144 after c1d5b2b (fairness366, child isolation365,
release367/SDK1.10.9). Complete this focused merge record before integrating that
new frozen head; final MSRV/package/full gates must cover the final combined source.

Current-compiler node checkpoint: expanded remote-debug gate passes36 tests in
seven targets (including both borrowing denials); final workspace all-target/
all-feature strict Clippy and isolated native-fixture strict Clippy pass.
Host-restoration inverse fails at the intended original-Work retry, then the
exact restored source passes. Explicit type-complexity expectation names the
complete private owned product; tests use ordinary let-else refusal extraction.
No semantic bound/type added from lint. Both test reviews remain satisfied.
Record NODE-CURRENT-VERIFIED freezes source and qualified results; final main
81dc144/MSRV/optimized inverse/package/full gates remain open. This scoped model
checkpoint passes; it does not close the programme or raw SDK inference work.


Latest-main81dc144 reconciliation, pre-edit: frozen commits fairness366,
capability-isolation365 and release367. Preserve current Behavior/Actors0.26.0,
Application mailbox/client limits, complete custody and joined/raw node ports.
SDK1.10.9 is present in the registry cache (vcs1ea4bd1); inspect its actual
Session/open/close/zid/is_closed and opening IntoFuture contracts before merging.
No new SDK API or project dependency is proposed; inherit upstream's paired SDK
and transitive lock updates and package0.1.2. Original research and driver-law
classify cyclic source selection as deliberate scheduling policy, not an Agha
latency guarantee. Cancellation stays first; ordinary lane aging may admit one
waiting user before later control; control-only source settlement preserves the
causal barrier. Existing validated mailbox capacity must survive config aging
and flow unchanged to fresh/Entity descendants.

Expected imported files21; custom production delta0/types0/bounds0/dependencies0.
Expected mechanical test changes: existing complete-outcome consumption in new
fairness and hosting consumers; new private fairness fixtures reuse their actual
mailbox Config's validated capacity. Strengthen fairness terminal inspection,
keeping exact Box/order/timer service traces and both notifications. Prefer the
proven Application.sender path over newly imported interface/external setup;
no new facade or protocol. First build must expose original merge fallout before
adaptation. Run actor-capacity/cold, node/refusal/lending, event service and real
hosted-child consumers on the combined source, plus all required gates.

Independent new-test review: sustained service-gap oracle distinguishes broken
fixed priority, and queued report/ChildStopped order and original closed-FIFO
payload checks are meaningful. Hosting's local association check is application
permission, not automatic runtime rejection of all foreign capabilities. Preserve
its A=1/B=2 independent observations and strengthen origin correlation only from
actual retained birth commitments; never predict IDs/nonces/addresses. No public
scheduling parameter or new caller structural bound is added. Control selectors
are private service entitlements needed by the policy; no actor lifecycle state
or transition authority is duplicated. Review found the fairness terminal oracle
partial; resolve that during consumption migration, with a second review.


Concrete sender gap exposed by new main consumer: fairness requires synchronous,
nonwaiting admissions before the actor can run. The existing Application.sender
wrapper only exposes async send; switching to it gives E0599 for try_send
(LATEST-FAIRNESS-CAPACITY-DEBUG). Awaiting admission can yield and invalidate the
fully-preloaded independent fairness witness. Reopen the owner instead of hiding
interface/external/type ceremony in the new test.

Pre-edit sender law: deliberate API composition of the existing local admission
contract. An ApplicationSender bound to an exact EstablishedRecipient may attempt
one command synchronously, using its original identity, with complete original
Full/Closed command return and unchanged accepted prefix. Stable Entity resolution
may await; do not add nonwaiting delivery there. Desired syntax:
`application.sender()?.try_send(command)` and `.sender_to(recipient)?.try_send(command)`.
Reuse ExternalActor.try_send, actual endpoint/Communication TrySendError and sealed
existing exact-recipient capability; no new trait, bound on the general sender,
policy, type or dependency. One specialized inherent method, expected owner
interface.rs net15–25 lines. Focused RED before production: existing fairness
preload plus application_clients full→one dequeue→exact original retry→prefix→
Closed ownership test. Two real protocols (Flood, MailLedger); no wrapper actor
semantic or initialization/lifecycle policy changes. Model control sums,
subordinate states, branches and storage remain unchanged. Record static
capability denial, independent review and both profiles before retention.

Sender refinement: the focused test closes admission with two accepted commands
still queued, then recovers that ordered prefix and every original allocation;
Full/retry/Closed remain separate exact values. It also inspects every normal
root retirement lane and both notifications. EntityRef<D> with a valid existing
ExternalTarget capability is rejected specifically for absent try_send (E0599),
not an unrelated bound. All seven existing client denials remain unchanged; the
new eighth denial's generation passes. Two independent reviews approve the
actual method and tests, including non-Debug commands and possible wrong-origin,
reordered-prefix, destructive-close and reconstructed-payload counterexamples.
Owner production+17/-0, one method, zero type/trait/dependency/state/storage or
semantic branch changes (BOUND-SENDER-CHECKPOINT). This is new capability syntax,
not a claimed source reduction. Current combined debug and optimized corpora each pass360 tests, including336
owner tests, six capacity tests,12 client tests and the cold/node/fairness witnesses.
The current-source MSRV debug corpus also passes all360 tests and all nine client/shutdown diagnostic fixtures. MSRV optimized/native, session-loan fixtures and full gates remain required. Both independent ownership inversions now fail for their intended laws in debug/release, and all four restored controls pass (LATEST-OWNERSHIP-INVERSES). Final workspace all-feature/all-target strict Clippy and the isolated protected-transport fixture strict Clippy pass (LATEST-WORKSPACE-CLIPPY-RETAINED, LATEST-NATIVE-CLIPPY).

Combined SDK1.10.9 native corpus passes65 tests per profile, including the added
hosted-child isolation witness; one existing subprocess-only test is separately
executed by all ten controlled TLS campaigns. Their64 cases and160 role processes
pass with actual compiler-artifact executable hashes and372 frozen source files
unchanged (LATEST-TLS-VERIFIED and per-campaign receipts). Do not infer a networking
feature-complete claim from these preserved extension witnesses. The current
source remains an uncommitted latest-main merge until remaining checks finish.
Complete tracked/untracked reconciliation is LATEST-MERGE-DELTA:28 paths, no
untracked files, imported source including embedded tests +951/-96, separate
tests +922/-5, and documents/manifests/gates +1642/-69 at that snapshot. These
include upstream work; custom production is only +17/-0, with zero public types
added or removed. Custom test-fixture adaptations and complete custody assertions
remain test source. LATEST-MERGE-CHECKPOINT and BOUND-SENDER-CHECKPOINT retain
the control-state, owned-value, provenance and residue disposition; final gates
remain open.

User correction2026-10-14: ordinary remote actor usage must hide Session and
communicate through typed recipients. The present Node.run_with callback is an
advanced native-SDK join, not the finished ordinary remote actor API. Keep that
extension port explicit in guidance. A Session-free facade with no actual typed
remote delivery would conceal missing integration; do not add it as a cosmetic
wrapper. Typed exports/permission/admission remain the separately deferred remote
programme, carrying this ordinary-use requirement. Ordinary local Application
callbacks remain inferred and SDK-free.

Final strict lint finds the strengthened fairness journey now110 lines. Retain
its complete independent ownership oracle and scope too_many_lines expectation
to that single integrated trace; add no test wrapper/state or weakened assertion
merely to satisfy the line metric. Rerun strict gates rather than omit custody.

User ownership clarification: the user will implement the ordinary remote API
and merge it into main; this programme will integrate that delivered source and
improve its caller experience afterward. Keep the current SDK adapter as advanced
access. No competing remote-delivery implementation is selected here; continue
the selected local programme and required combined-source verification.

Remote extension design clarified by user: callers explicitly choose local or
remote hosting/recipients. Local actors require no Session or transport codec;
a remote runtime adapter owns SDK access while Behavior remains the same pure,
typed Actions contract. Keep that hosting distinction explicit in capabilities,
with no second actor algebra or transport requirement imposed on local protocols.
The user owns delivery of this separate remote work to main.

Session-loan diagnostics: the original MSRV snapshot fails only on closure
description (LATEST-NODE-BORROWING-MSRV). A named, independently valid borrowing
callback replaces that closure; the same callback is accepted by raw execute_with
and rejected by joined run_with at its independent Output bound. The raw
compile-pass control, both denials and existing real borrowed-result journey
retain distinct evidence. Two independent reviews approve the paired fixtures;
a static-result candidate was rejected because it would fail even through raw
execution. LATEST-NODE-LOAN-REVIEW retains both reviews and byte-exact comparison;
LATEST-NODE-LOAN-PROVENANCE preserves the pre-edit record. Final paired current/MSRV replays pass all three fixtures. The first current
paired run reports two unexpected successes during concurrent disk exhaustion;
its raw log remains a failure, with no passing credit. After freeing inactive
owned cache, the same current test binary passes in the pinned Nix environment.
Selected trybuild ignores Cargo exit status when no matching JSON diagnostic is
parsed, so the infrastructure cause is plausible but not independently proven.
No production signature or capability changes in that diagnostic correction.
The2e21756 current/MSRV debug/release combined corpora each pass360 tests and
each native corpus passes65 with one existing controlled-subprocess ignore.
Final repository gates for that frozen integration remain required; later sender
changes have their separate evidence below.

One-way sender storage, pre-edit (DX06/20): deliberate Bombay capability policy.
A bound one-way sender owns one fresh application address and its exact target.
It grants neither a reply recipient nor receiving/lifecycle authority. Current
ApplicationSender privately establishes ExternalActor<Never>, allocating a
mailbox, admission state and termination observation that cannot be exposed.
ApplicationAddresses.allocate already reserves its never-reused identity without
heap allocation. ExternalTarget.send_from and ActorRef.try_send_from already
accept that identity directly; reply clients keep their complete ExternalActor.

Desired syntax stays application.sender(), sender_to(target), and exported
interface.sender_to(target). Ownership equation: (target, allocator) -> either
(target, fresh address) or (original target, exact AllocationRejection). Sending
still preserves the original command and truthful sender; sender destruction
must not close its selected target or consume accepted commands. The private
Never mailbox has no obtainable observer/recipient and supplies no observable
contract to preserve. No actor allocation or lifecycle law changes.

Before production, add one public dhat witness for all three constructor paths,
measured synchronously on a current-thread host: no sender-owned mailbox/storage
allocations. Complete actual root increment/read, exact-origin peer delivery,
original Closed-command rejection and every normal retirement/notification lane
before checking the allocation law. Existing application_clients additionally
prove original Full/retry/closed-prefix values and exhaustion returns the original
Entity target. Both compile-denial fixtures continue to deny receiving/lifecycle.

Expected production edit interface.rs only: replace the private ExternalActor
field with MailAddr, remove the unused capacity input at three constructors, and
reuse the same native send methods. Expected net reduction0–15 lines; public
spelling/type/trait/bound/dependency additions0. Test and canonical guidance update
together after RED. Control-state sum unchanged (sender is an immutable product);
no subordinate states/outcomes added. Remove its hidden mailbox/admission/observe
state only; those owners remain for real reply clients. Count the exact diff and
prove both compilers/profiles, allocation-exhaustion custody, existing native
consumers, static denials and inverse before mechanical migration. The frozen
2e21756 full-gate run is independent of this next isolated stage.
Selected registry owners for this record: bombay-address0.3.0, bombay-behavior0.26.0, bombay-behavior-actors0.26.0, bombay-behavior-macros0.14.2, bombay-communication0.1.5, bombay-timers0.2.0. No patches.
Reviewed RED completes the full public journey and fails only its final storage
law: every sender constructor allocates8 blocks and retains16880 heap bytes;
each sender value is176 bytes on this host. Record SENDER-HEAP-REVIEWED-RED.
Review corrections compare all senders to the actual root and exercise sender
destruction before target admission/Counter reply; original accepted commands and
exact sender identities survive. This is the pre-production falsifier.

Auto-trait checkpoint before retaining the representation: Communication Consumer
contains a private raw queue cursor and is Send but not Sync. Removing the
unused receiver therefore also permits immutable one-way senders to be borrowed
by multiple producing threads. This follows the selected address+target model
and existing thread-safe mailbox producer; no receive/lifecycle authority is
granted. Prove it explicitly instead of silently changing that public promise
or adding a meaningless non-Sync marker. Restore original production first,
add a real scoped two-thread Counter journey (both commands processed; exact
reply and complete retirement), and obtain its focused Sync denial before
reapplying the no-reply representation. Existing affine receiving denials remain.
The earlier memory GREEN remains scoped evidence, not final retention.
The scoped-thread RED now fails specifically E0277 at borrowing the one-way
sender: its hidden Consumer<Never> raw cursor is not Sync. Original production is
byte-identical to2e21756 at that falsifier (SENDER-SHARING-RED). Reapply only the
already reviewed address+target representation; no marker, unsafe impl or new
trait/bound is needed. Both receiving denials remain independent requirements.

Current sender checkpoint: production +16/-19/net-3 in interface.rs; no public
spelling/type/trait/bound/dependency additions. Owner production802→799 lines,
six matches/nine arms, six structs, one enum and35 public functions unchanged;
no module/state/result alternatives added. Private sender fields remain a
product of two values, now exact target and fresh address; they are the only
values later delivery needs. Remove its private reply recipient/admission/control/
consumer/termination values; actual clients retain those legitimate owners.
No arrival history, repeated cause, false cardinality, transition authority,
semantic boolean or structural caller syntax added. SENDER-MODEL-CHECKPOINT
records the complete law/reuse/residue review. Both independent reviews approve
the memory/drop/origin and shared-producer witnesses, including original-source
REDs before retained production. Current debug/release focused witnesses pass:
each constructor0 allocations/0 retained heap bytes and sender48 bytes, compared
with8 allocations/16880 retained bytes and176-byte values. These are constructor
measurements on this host, not whole-application/RSS or throughput claims. The
first broad current-debug corpus passes369 tests and ten diagnostic fixtures;
other profiles/MSRV, original-source comparisons and final gates remain open.

HTTP diagnostic portability, pre-edit (DX19/25): the MSRV sender-consumer run
passes owner336, accounts7 and client9 tests plus all client denials, then its
HTTP target passes six runtime cases but fails the existing wrong-root snapshot.
Both compilers emit E0631 for the same invalid callback;1.99 shortens imported
paths while1.96 prints complete paths (SENDER-CONSUMERS-msrv-debug). Preserve
that failed harness result. Remove the fixture's glob/type imports and name the
same public types explicitly, including the existing ActorExt call. This changes
no type equation, callback shape, behavior, capability or bound. Require both
compilers to reject the same wrong root with one identical reviewed snapshot;
run the six independent HTTP runtime cases unchanged. No production edits.
The fully qualified fixture hypothesis does not stabilize the inner event
paths: both compilers still reject the exact wrong root with E0631, but print
those paths differently. Preserve both attempted outputs, restore the original
fixture and pinned snapshot unchanged, and keep the MSRV snapshot obligation
open. Do not alter application types or weaken the negative program to hide
formatting. Continue independent runtime witnesses with no-fail-fast so one
known harness mismatch cannot prevent later ownership tests from executing;
the full MSRV run still reports failure and receives no whole-suite credit.

Final sender result: both focused tests pass pinned1.99/1.96 debug/release;
all three constructors measure0 blocks/0 retained bytes and48-byte sender values.
Exact original production in a separate build directory fails the final heap
oracle in all four builds (8 blocks/16880 bytes;176-byte values) after completing
the entire delivery/retirement journey, and independently rejects thread sharing
with E0277 in all four builds. One shared-cache comparison executed an apparently
stale candidate binary and is excluded; separate build artifacts supply evidence.
Current debug/release consumer runs each pass369 tests plus ten static fixtures.
MSRV debug/release no-fail-fast runs each pass368 tests and fail only the unchanged
HTTP snapshot (same E0631; different qualification); retain exit101, no suite pass.

Commands inside pinned Nix: cargo test --locked -p bombay-rs --features axum
--test sender_allocations --test sender_sharing [--release] -- --nocapture;
broad corpus additionally --lib and --test application_clients/accounts/axum/
event_fairness/orders/processor, with separate --test arguments. Strict workspace
all-target/all-feature Clippy, fmt, warning-denied rustdoc and actual counter,
actor-templates and Entity binaries pass. New tests receive two independent
reviews; the final four assertion changes only expose existing Vec<Never> values
on failure. Both existing receiving/lifecycle denials remain unchanged.

Checkpoint: production+16/-19/net-3; tests+268/-0; public types+0/-0, no new
traits/bounds/dependencies/modules or spellings. This reduces sender storage;
reply clients and all actor/control phases stay unchanged. Disposition: pass for
the focused sender law and consumer stage. Full new-source Nix/archive gates and
remaining DX06/20 obligations stay open; frozen2e21756 gates test their own source.

Nix resource inheritance, pre-edit (DX06/27): build configuration policy only.
The remote-debug and documentation gates add env.RUSTDOCFLAGS through a shallow
commonArgs // override, replacing all common env entries. Actual remote-debug
derivation exposes only RUSTDOCFLAGS; active native logs confirm unoptimized +
debuginfo and duplicate dependency builds. Desired contract: every check retains
CARGO_PROFILE_DEV_DEBUG=0, CARGO_PROFILE_TEST_DEBUG=0 and CARGO_INCREMENTAL=0;
these two also retain RUSTDOCFLAGS=--deny warnings. This removes debugger/cache
artifacts while preserving debug assertions and every test/warning gate.

Before editing, evaluate all24 real check derivations and assert those actual
attributes; expect exactly these two gates to fail the shared-env requirement.
Fix only their env merge using commonArgs.env, without changing source, compiler,
features, test list, assertion configuration or gate scope. Expected flake.nix
+2/-2 statements and documentation only; production Rust/public surface0.
Control states, branches, ownership and interpreter composition unchanged.
The frozen2e21756 full check remains separate and is never converted to passing
by this correction. Re-evaluate all24 checks and the preserved rustdoc flags.
Both independent reviews approve the exact two env merges. Actual all24 gate
derivations now preserve all three resource attributes, with identical names
and both warning denials; exactly two gates change null→0 for those settings
(NIX-ENV-RED/GREEN). Nix formatting and full flake evaluation pass. Physical
flake delta +6/-2, Rust/public types/modules/control states/branches0. No source,
feature, test list, compiler or assertion setting changes. Native builds for
the corrected configuration still require verification; the frozen original
full check keeps its own pending result.

Compiler diagnostic evidence, pre-edit (DX19/25): deliberate verification policy.
The same invalid Rust programs must remain rejected on pinned1.99 and supported
1.96. Exact diagnostic snapshots may differ by compiler; accepting an arbitrary
error, skipping fixtures or modifying their type equations is not evidence.
Current raw MSRV runs fail one HTTP snapshot and eight topology snapshots while
rejecting every affected program. The failed qualified-path hypothesis above is
removed. Reuse strict trybuild, its existing fixtures and disposable source copies.

Compare reviewed minimum-compiler snapshots with canonical snapshots using a
finite versioned patch and exact before/after hashes. A maintainer verification
command may apply only those expected diagnostic changes in a disposable copy,
then execute the unchanged tests with overwrite forbidden. Preserve raw status,
compiler identity, logs and source/lock hashes; reject unknown compilers, altered
inputs and any unrelated snapshot change. Canonical source and1.99 snapshots stay
unchanged. Generate expected output separately, review its actual denial causes,
and freeze it before verification. No Rust production/API/dependency change;
expected files: one maintainer command, compiler snapshot patch/manifest and this
record. Aggregate states, alternatives, branches, modules and public spellings
unchanged; exact diagnostic expectations are the sole additional owned evidence.
Require healthy runs on both compilers plus a deliberately damaged expectation
that fails, and verify caller/snapshot source conservation. This closes only the
reviewed diagnostic portability obligation, not the remaining DX19 corpus.

Diagnostic result: exactly nine differing expectations are frozen in
crates/bombay/tests/compile/diagnostics/{compilers.json,minimum.patch}. Independent
two-pass review confirms each original rejection cause; two shorter diagnostics
remove duplicate NativeChild reports only. All other567 generation files are
byte-identical. Canonical .rs/.stderr files and every Cargo lock remain unchanged.
The first verifier was rejected for accepting exit0 without named coverage and
for allowing a receipt0 before final caller conservation; neither preliminary
run receives final verifier acceptance. The retained command requires all25
fixture successes, all four named harnesses, both18/7 test summaries without
ignored/filtered tests, exact compiler identity, immutable expectations and full
snapshot/caller source inventory plus hash conservation. A fresh private build
directory prevents artifact sharing between source copies.

Pinned1.99 and1.96 final commands each pass all25 tests, including24 topology
compile denials and one HTTP protocol denial; all25 denial records and four
harnesses are present. Both full caller receipts preserve all579 owned files.
Three independent controls pass: rawCargo0 with no tests yields verifier1;
rawCargo0 plus an added source file yields verifier1; a deliberately damaged
HTTP expectation produces its named mismatch/E0631 and verifier101. Raw command
statuses are retained separately. Two independent final code reviews pass.

Reproduce from the workspace root (choose a new empty output directory):

```sh
nix develop -c python3 tools/diagnostics.py --output /tmp/bombay-diagnostics-current
nix develop -c env RUSTC="$MSRV_BIN/rustc" RUSTDOC="$MSRV_BIN/rustdoc" \
  python3 tools/diagnostics.py --cargo "$MSRV_BIN/cargo" \
  --output /tmp/bombay-diagnostics-minimum
```

MSRV_BIN names the separately installed pinned1.96 toolchain; the command checks
its exact compiler/Cargo identity and rejects unknown versions. Expected-output
generation is a separate reviewed operation, forbidden in this command. This
maintainer check complements the ordinary unchanged1.99 Nix gates; it does not
turn earlier failed raw MSRV runs into passes or waive final package gates.
Checkpoint: Rust production/tests+0/-0, public types/traits/bounds/modules/states/
branches+0/-0; tooling+143, frozen diagnostic evidence+838, coordination+record.
No actor state, runtime authority, fixture type equation or dependency changes.
Disposition: pass for these compiler snapshots; complete DX19 support stays open.

Latest-main protected JSON integration, pre-edit (2026-10-15): import792529e
from main while preserving the local Application/sender/diagnostic work. This
adds borrowed protected-text checks owned by the remote module, not ordinary
actor semantics or another application spine. Its named Limits product keeps
five distinct acceptance limits explicit; check borrows both originals and
leaves verification, typed decoding, admission, replay and retirement with their
existing owners. Token limits apply after native token materialization; preserve
the explicit absence of a whole-heap bound. No new DevX wrapper or remote API.

Inspect exact new dependency json-event-parser0.2.3 and locked manifests before
crediting integration. Retain all five remote_json tests plus the actual
application_clients/json_receiving journeys and their original rejection,
accepted-prefix, closure and retirement oracles. Preserve node_outcome and
node_borrowing in both remote Nix gates while adding remote_json; retain common
env inheritance and both rustdoc warning denials. Reuse all imported code and
Tests unchanged; resolve only true overlap, then run current/MSRV debug/release
consumers and full integrated gates. The frozen2e21756 run is independent and
stays frozen. Source delta/public surface below distinguishes upstream capability
from DevX edits. Local/remote ownership and existing control-state sums unchanged
by the merge; new parsing alternatives belong to the imported checker alone.

The first actual imported JSON consumer fails E0308 at exactly two receiving
triple destructures: the established Node contract returns ApplicationOutcome.
This is the existing DX04 terminal-product migration, not an owner gap. Before
editing those callers, retain JSON-INTEGRATION-current-debug exit101. Consume
receiving.await.into_parts() into work/root/notifications/unit families, then
leave every original work selection, cleanup, native custody, query allocation,
accepted-prefix and rejection assertion unchanged. Expected test delta2/-2;
production/API/bounds0. Both actual JSON journeys must pass before retaining the
merge, with two reviews checking the complete original assertion bodies.

Protected JSON merge result at6cb42ef: current/MSRV debug/release each pass all26 tests across
application_clients, node_borrowing, node_outcome, remote_json, sender_allocations
and sender_sharing. All nine client/shutdown denials and all three Session-loan
fixtures pass; both actual JSON receivers preserve their original custody oracles.
Workspace fmt passes; two independent merge and caller reviews pass. Locked graph
adds only remote-optional json-event-parser0.2.3 (MSRV1.74), with no parser runtime
dependencies selected. Inspect its exact event/complete-input/max-stack contracts;
all prior package versions/metadata/dependencies remain unchanged. Relative to the
pre-merge head: upstream production+148/-0, DevX production+0/-0; tests+976/-0
with exactly+2/-2 caller-line substitutions relative to upstream; public JSON
Limits/Error are upstream additions, no DevX types. Both original-source counters,
local capacity and typed retirement/authority laws are unchanged. Remaining:
final integrated Nix/archive gates. Final workspace all-target/all-feature strict
Clippy, warning-denied rustdoc and fmt pass; every frozen tracked source hash
matches. Frozen2e21756 separately passes all24 native Nix gates in4860.772 seconds
with unchanged source hashes and clean Git; its raw log names all24 successes.
The delivery worktree now points to2e21756. A later cache inventory finds13 outputs
present and11 absent, so continued artifact availability is not claimed. This
full run predates sender storage, diagnostic tooling and JSON integration and
does not certify their newer combined source. No prior signing/disk failure is waived.

Stash construction, pre-edit (DX14/19): deliberate API composition policy over
an existing derived replay law. A release may emit accepted Actions for several
held inputs; those actions have no rollback. Existing Stash execution therefore
requires the sealed StaticallyInfallible error capability. Bare Stash::new and
ActorExt.with_stash currently admit fallible inner actors that cannot execute,
release anything, expose mutable state or recover a valid inner actor. Require
that SAME existing capability at construction, with a diagnostic at the caller's
composition expression. Preserve Never and MachineError<A,M,Never> witnesses,
closed phases, wrapper order and complete FIFO prefix/stop/tail custody.

Before production changes, prove owning construction with its existing u8 rejection and ordinary facade
construction with an owned Box<[u8]> rejection both unexpectedly compile. Change the
existing owning compile-fail rustdoc so bare construction is the oracle; add a
focused facade compile-fail fixture and retain the prior closed-phase denial.
Only then put Error:StaticallyInfallible on the two construction methods and
annotate the existing sealed trait with actionable diagnostic text. No broader
struct/inspection bound, new trait/type/default/dependency or transition change.
Reuse existing stash properties, actual worker/scope queue orders and Machine
rollback tests. Add a compile-pass infallible Machine caller beside an ordinary
infallible actor. Expected owner production+one trait diagnostic+one method bound;
Bombay+one method bound; test/guidance updates together. No catalogue migration.

Checkpoint before: control states unchanged (inner B plus owned FIFO queue),
subordinate state/results0 added; the queue owns exact deferred inputs needed
for replay/retirement. Existing StashRoute has3 alternatives, no new branch or
module. Public type/trait/spelling count unchanged. Count exact retained deltas
and replay tests after; scan for semantic flags/history/duplicated causes/nested
authority/structural caller syntax. Cross-check DX14/19, Actions ownership and
Stash statically-infallible replay documentation. Independent design review
finds no supported non-executable storage use lost; constructor eligibility is
the sole narrower promise. Bounded retention/overflow remains separate open work.

Both pre-production REDs now fail for the intended oracle: owner rustdoc says
construction compiled despite compile_fail; Bombay's sole new invalid-error
fixture likewise unexpectedly succeeds, while its prior four fixtures pass.
Preserve those original logs. Retain only the existing capability at the two
constructors; no runtime implementation or type declaration changes.

Selected-owner patch403a4f1 is based on published98983b4; the earlier0.23
documentation-branch prototype receives no0.26 acceptance credit. Corrected
constructor denial,44 owner tests per profile, all965 workspace tests and all
ten Nix gates pass (305.388 seconds; frozen source hashes unchanged). The
four-workspace/archive preflight passes; all137 packaged Rust files match.
Paired source consumers pass invalid-error/valid-Machine checks and four
unchanged native stash cases per current/MSRV debug/release configuration;
this is not publication or whole-suite credit. Both independent test reviews
and final consumer review pass. Owner production+9/-1/net8, rustdoc+1/-2,
tests+57/-0; no new types/states/branches/modules. Bombay adds the same existing
bound and rustdoc, one denial and one inferred Machine positive. Current
debug/release pass10 tests and12 fixtures; strict lint/rustdoc/fmt pass. MSRV
registry execution fails one existing worker-factory diagnostic expectation
(three error headings use "a" instead of "an"); new stash fixtures pass.
Keep that failure recorded. Owner review/publication, normal registry adoption,
final consumer gates and bounded retention/overflow remain open.

Supported authoring diagnostics, pre-edit: the normal MSRV registry matrix
fails only worker_factory_not_callable: three E0277 headings say "a" instead
of "an"; source-span labels and every rejection remain unchanged. Extend the
existing immutable compiler replay with this exact reviewed patch and hashes,
retaining all25 prior denials and adding the12 actual authoring/worker fixtures
(35 denials/two positives total), six compile harnesses and target counts
18/7/1/4. Derive commands/counts from one manifest; check each path's pass/fail
mode, exact occurrence counts and complete source conservation. No Rust source,
canonical golden, dependency or production API change. Expected delta: one
tool, two expectation records and this coordination record. Independently
review the three heading-only changes and prove omitted targets, wrong mode,
added source and a damaged named expectation are rejected before retention.

Retained replay result: corrected current/MSRV runs each pass37 fixtures
(35 denials/two positives), six compile harnesses and30 outer tests in114.391/
109.693 seconds. Actual compiler identities are1.99/1.96; all585 caller hashes
remain unchanged. The first parser ignored unexpected fixture paths; its real-log
counterexample is retained and that candidate receives no final verifier credit.
The corrected parser checks paths before membership and rejects duplicates and
wrong pass/fail modes. Nine transcript controls and both independent reviews pass.
Three full-driver controls reject omitted targets despite raw exit0, added source
despite raw exit0, and a deliberately damaged named stash expectation with actual
Rust exit101. Canonical fixture/expectation files, Cargo graphs and every prior
snapshot payload remain unchanged. Only three reviewed E0277 heading articles
are appended to the minimum-compiler patch. Rust production/API+0/-0; tool+26/-13,
expectation records+55/-4. No actor state, effect, queue, branch or ownership law
changes. Final integrated Nix/archive gates and owner publication remain open.

Native interpreter construction (DX15), pre-edit: derived extension law, using
the existing ActionItem/InterpretItem receiving loans and named send products.
Application::new(actor).with_interpreter(services) must admit the supplied
concrete owner beside standard timers and delivery without replacing Environment.
First write a caller that emits real OpenFile and ReadEnvironment requests in
initialization and composes an actual terminating timer/shutdown pair; establish
the missing-method compiler failure before production changes. This first
construction witness supplies no runtime, cancellation or retirement acceptance.
Services preserve complete OS rejections and return acquired File/String values
through the existing receiving loan. Their ActionItem::retain_accepted must keep
those actual owned results; the default intentionally discharges a receipt and
cannot prove terminal custody. Reuse prepare_item/finish_item and existing pure
actor authoring. No production type, bound, default, wrapper or dispatch change
is authorized by this construction test alone. Before lowering, the smallest
native journey must also prove both wrapper orders, standard delivery, unsupported
leaves, exact failure, interrupted/late completion and full retirement; the
external coherence probe does not replace those obligations. Expected first
batch: one test source and this record, zero production changes.

Construction RED: pinned1.99 compilation of application_services fails solely
with E0599 at with_interpreter; all existing request, product and wrapper types
compile. Both independent reviews accept only this construction scope: a method
that dropped Services could satisfy it. It supplies no runtime acceptance.

Graph ownership decision before lowering: the application owns a constructor,
not one interpreter instance that must somehow move into several actors.
Canonical syntax becomes with_interpreter(|| Services). Each applicable actor
receives one owned instance before its initialization effects; it retains that
instance through interpretation and disposes it only after the existing complete
native retirement. Root, ordinary/fresh/cold children and Entity must use the
same construction policy; do not silently deliver a root-only port. Only
construction authority is shared; Fn + Sync does not imply immutable captured
state. Each actor owns its returned instance, which requires neither
Clone nor Sync; independent backing resources remain the constructor’s policy.
Host refusal returns the original Application and callable;
accepted/rejected/pending request/result custody stays in the existing action
and source products. No service-instance-return API is promised by this policy.
Constructor panic and instance-destructor panic must follow the existing native
failure/retirement paths without losing independently acquired lanes.

Entity reference constraint, inspected before lowering: the actual consumer
protocol contains Admit(EntityRef<Profiles>, u64). That domain reference cannot
acquire an anonymous application-constructor parameter merely because Entity
incarnations receive services. Current Entities/EntityRef own ReceptionistFor<D>,
which fixes BombayEntityRuntime<D>; its concrete port is retained throughout
EntityRuntime. Threading a new Factory through every one of those types is not
an accepted model by itself. Preserve the same concrete domain reference under
different constructors, or establish and review a necessary capability distinction
before editing. No erased factory, second directory engine or root-only exception
may repair this constraint. The standalone factory proof does not cover it.

Reuse Fn()->Service and the existing native receiving-loan traits. A compiler-only
probe confirms a local factory equation for ordinary absence () can coexist with
the Fn()->Service implementation. The external route must additionally require
the actual Fn constructor and exact request::Interpreter equality, so ordinary
applications gain no user request capability through the absence value. This
probe is not native execution evidence. Expected production responsibilities:
one public request-owner trait, one Application constructor method, an inferred
constructor parameter on the same Application spine, and private per-actor
instance/constructor ownership at the existing capabilities. No new actor engine,
public marker, fallback, effect language, service Clone bound or no-op input.
Before production, extend the failing witness to express a genuinely pending
owned request and the complete smallest native journey. Before retention, prove
root/child instance separation, both wrapper orders and ordinary absence denial. Exact
affected owners and measured delta must be recorded with that retained patch.

Memory falsifier in the disposable factory probe: storing Arc<()> for ordinary
absence grows the local interpreter product128 versus120 bytes. That candidate
is not retained. The factory equation must preserve a zero-sized shared value
and instance for absence; supplied constructors alone own shared construction
authority. This permits independent, non-Clone, non-Sync service instances while
avoiding an ordinary-actor pointer/allocation for an unused extension. The new
associated shared representation is justified by this measured memory law, not
by a compiler diagnostic. Native allocation/layout evidence remains required.


Factory ownership probe (DX15), scoped result: the standalone actor-services
workspace executes actual File acquisition, environment lookup and the existing
TimerQueue with two separately owned, non-Clone/non-Sync service instances.
Three tests pass on pinned current1.99/MSRV1.96 in debug/release. Four corresponding
absence-denial builds fail specifically because () is not a callable constructor,
with the real request and service implementation otherwise available. The eight
commands, compiler identities and unchanged source hashes are retained in
SERVICE-FACTORY-MATRIX.json. The measured absent-service product now equals the
native120-byte timer product; the earlier128-byte Arc<()> candidate remains a
recorded falsifier. This is object layout in the standalone probe, not native
Application heap or execution evidence. Both independent reviews pass only this
scope. Request prepare/finish, replay, source delivery, constructor timing,
pending cancellation, native root/child/cold/Entity ownership, host refusal and
complete retirement remain open. No production symbols or bounds are retained
from this probe. Ordinary callers never supply the private () absence value.

Next native witness, before production: preserve actual File/String receipts and
complete original OS rejections through the existing source and retirement
products. Emit accepted requests in both initialization lanes, then rejected requests
in the actual timer’s stopping transition; a lawful rejection must not skip a
later lane. The Driver rejects initialization settlements on any rejection,
so that startup failure is a separate witness rather than claimed as normal
completion. Separately suspend a file request before
consumption, drop its interpretation future, and resume the same receiving loan:
accepted prefix, pending original request, untouched suffix and exact late result
must survive. Model no new aggregate phase or actor engine. Reuse existing
InterpretationProgress::{Original,Interpreting,Completed}, ItemSettlement,
prepare_item/finish_item and the selected native retirement products. Production
remains unchanged until the complete native failing journey exists.


Receiving-loan verification: three source-backed service_custody tests execute
real File/environment acquisition, cancellation of a pending interpretation,
resumption, completed replay and closed-permission rejection. Both File offsets
are advanced before resumption/replay, detecting receipt replacement. Exact
request bytes, allocations, permission variant and Notify identity are checked.
All three tests pass on current1.99/MSRV1.96 debug/release; two actual premature
consumption inversions fail at the intended pending-request assertion and both
restored controls pass. Focused warning-denied Clippy passes. Its suggested
async-fn rewrite fails with Event/Path E0309; explicit existing receiving-loan
future signatures remain, with a narrow documented lint expectation and no new
lifetime bounds. Raw failure is retained, not credited as a passing build.

Native application_services is an intentionally RED pre-edit witness: the
existing missing-method receipt records two E0599; explicit associated-owner
declarations subsequently expose two E0405. The earlier verifier expected the
old error and rejected the new transcript; preserve that verifier failure and
use the corrected exact-error receipt. No native semantic acceptance is claimed.
The test specifies accepted initialization, a rejected ordinary timer Stop,
newest-first complete action custody, native delivery and cancellation with an
acquired File, original pending request and two independent untouched suffixes.
Do not weaken the Driver's initialization rejection to satisfy this witness.

Current batch changes zero production lines/types/traits/modules/states/branches.
The existing control sums remain InterpretationProgress(3), Interpretation(2),
ItemSettlement(4), SettledItem(2) and the existing complete ActorRetirement sum.
Only the test request's FileAccess(Ready|Waiting) describes actual permission
custody; it owns no actor transition authority. Production generic bounds,
exports and dependencies are unchanged. The retained request loan owns the
pending PathBuf, permission receiver and notification endpoint; its acquired
File owns the advanced offset, and each untouched suffix owns its exact request.
No arrival-history state, repeated cause, semantic boolean or structural caller
syntax is added. Cross-check selected0.26 receiving/source contracts, Driver
D-INIT-1/D-TERM-1/D-RETIRE-1 and execution-ownership. Both reviews pass for the
request-loan proof; the native witness remains unproved. Production+0/-0/net0,
public API types+0/-0. Complete tracked/untracked test delta and commit are
recorded at the checkpoint below. Disposition: pass for the request-loan proof;
no production candidate is retained and DX15/16 remain open.

Checkpoint delta against616205d, including every untracked test: production
+0/-0/net0; tests+799/-0/net799 across three sources; public types+0/-0. All73
production Rust files are byte-identical to the baseline. ActorRetirement retains
its22 variants. Native Application witness is intentionally RED on this design
branch; it is not merged into the passing delivery branch or credited as a
runtime result. Request sources and exact measurements are preserved in
SERVICES-CUSTODY-CHECKPOINT.json; no new chronological audit document is created.

Named interpreter configuration (DX15), pre-edit probe: the existing callable
factory test does not prove that a nameable configuration can satisfy the same
request-owner equation. A family reference must keep one nominal type while
configuration values vary. Test a non-Clone Config returning the same separately
owned non-Clone/non-Sync Services, with two different Config values and an actual
child instance, using real file/environment requests beside native timers. The
existing ServiceFactory port already owns sharing and construction; no new
factory trait, request wrapper or state is proposed by this probe. First show
that the present external dispatch's Fn-only bound rejects this otherwise valid
construction port, then select the existing ServiceFactory<Service =
Request::Interpreter> equation for dispatch. Preserve callable factories,
zero-sized absence and compile denial of unsupported requests. This is isolated
constructor/dispatch evidence only: it cannot claim to fix NativeEntityHost or
EntityRef. Ordinary applications must remain inferred. Topology/family ownership
and the pending user preference about named configuration remain independent
requirements. Expected changes: the isolated probe's one dispatch bound and one
focused test; native production/API +0/-0, no new control state or lifecycle owner.

Configuration-probe falsifier: its four positive/current-MSRV profiles and four
negative examples pass, but independent review finds the negative only proves
Service type inequality. A real local OpenFile request can choose Interpreter=()
and implement File-opening InterpretItem for unit. The generalized guard then
admits it through the ordinary absence factory. SERVICE-CONFIG-ABSENCE-FALSIFIER
executes that actual unsolicited File acquisition successfully; the model is
reopened and is not lowered to native code. Frozen service-configurations source
is retained separately, with no native production representation to remove.

Next isolated hypothesis, before edits: explicit supplied construction must own
an operation distinct from internal absence. A public constructor port may
accept both ordinary Fn()->S and a named Config with create(&self)->S, while the
private sharing equation still has separate zero-sized absence and Arc<Factory>
cases. Unit may be the returned service, but ordinary absence must not implement
the supplied-constructor port. The actual unit File interpreter must be rejected
under absence and work under an explicit ||() constructor. Config authors state
only real construction; they do not supply markers or no-op policies. Reuse the
existing private shared representation and per-actor service storage; add no
state, wrapper, fallback or clone/sync requirement on services. Prove the concrete
absence counterexample and lawful explicit-unit control before crediting the
constructor model. This still does not select a native host projection.

Supplied-constructor probe result: the explicit InterpreterFactory operation
(Fn blanket plus named Config) remains separate from private sharing/absence.
Sixteen pinned current1.99/MSRV1.96 debug/release commands pass their exact
expectations: four four-test positive runs, four original absence denials,
four real unit-interpreter File denials, and four explicit ||() File controls.
The last controls prove actual file content after acquisition; unit can be a
stateless interpreter without granting authority to absent construction. All
seven source/lock hashes and both independent review passes are retained in
SERVICE-CONSTRUCTOR-MATRIX.json. No native production or public API changes
are retained. Actor control states and all22 native retirement alternatives
remain unchanged; native projection, constructor ordering/panic, root/child/
cold/Entity installation, cancellation and full retirement remain open. Do not
require existing manual hosts to provide a no-op service policy to lower this
probe. Disposition: pass only for the isolated constructor/dispatch law.

Native construction review narrows the remaining responsibility to three actual
assembly sites: root (`application/execution.rs`), child
(`local/effects/creation.rs`) and Entity (`entity/bombay.rs`). Cold members use
root/child startup. Each site must preserve native inputs, exact child bindings
and the parent-report port. The existing CommitActions owner remains the sole
dispatcher and retirement authority. A complete-capabilities constructor would
own real work, but does not itself solve selection: the existing universal
NativeChildStartup/NativeEntityHost implementations cannot be overridden for a
configured topology, and Entity runtime retains only Arc<D::Hosts>. Requiring
every manual host to supply an empty service policy remains rejected.

Constructor panic is a separate unproved custody cut. The current native
constructor closure runs synchronously in LocalEnvironment::prepare; child
Starting custody is recorded only after spawn_owned_execution returns, and root
handoff has already removed its actor from Prepared. Existing operation-panic
tests do not prove custody here. Before retaining a native factory design, its
first complete journey must inject a real constructor panic and inspect original
actor/configuration and acquired native values. This review establishes source
constraints, not an executed native failure or a retained production design.

Configured descendants (DX15), pre-edit native regression: a topology that
declares a real interpreter configuration must construct a separate service for
the root, a fresh child, that child's fresh child and a cold member. The same
nominal topology must accept different configuration values in two applications.
This is a deliberate extension policy, not an actor-model allocation law.
Desired syntax is `interpreter: Config;` in the existing topology declaration,
`Topology::new(config)`, and a real `InterpreterFactory::create(&self)` operation.
Ordinary topology declarations retain inferred native construction without a
caller-supplied absence policy. The supplied operation may construct a unit
service, but ordinary absence must not grant external-request authority.

Ownership equation: one shared configuration owns construction authority; each
actor owns its own constructed service and original typed request/receipt loans.
The regression reuses the existing File/environment request definitions,
ActorExt shutdown decoration, recursive birth roles, cold staging and complete
native retirement. It observes four real File receipts and exact original actor
allocations per application, independent constructor counts for both config
values, normal root completion and every child retirement/failure lane. A
service-using grandchild falsifies accidental host unwrapping. No new dispatcher,
source engine, lifecycle engine or retirement schema is selected by this test.

Expected test-only change: one integration target and this change record;
production/API +0/-0. The proposed declaration and constructor remain unproved
until the native witness executes. Lowering must reuse the three existing root,
child and Entity assembly sites and CommitActions. Compatibility experiments
must separately prove advanced manual recursive hosts: a foreign owning wrapper
cannot transparently preserve every existing generic NativeChild impl under
Rust's orphan rules. Constructor panic and the fixed EntityRef<D> equation are
separate remaining gates; this success journey must not be credited for them.

Pre-edit aggregate checkpoint: actor transition authority remains the generated
Behavior; control sums remain InterpretationProgress(3), Interpretation(2),
ItemSettlement(4), SettledItem(2), and all24 ActorRetirement alternatives. The
test actor's optional next child owns exactly one pending recursive actor,
not an arrival-history phase. Config owns actual construction inputs; retained
File owns the OS resource and each actor owns its original Box allocation.
Production states, subordinate sums, branches, lines, modules and public
spellings all change by zero. No runtime lookup, semantic boolean, repeated
failure cause, second transition authority or positional user effect access is
proposed. Cross-check DX02/03/15/16, execution-ownership, Driver initialization
ordering and the selected0.26 receiving-loan law. Disposition: test hypothesis;
no production model is retained before its independent reviews and falsifiers.

Constructor unwind (DX15), revised pre-edit law: invoke the supplied service
constructor during the existing guarded Environment::activate operation, after
the pure Behavior initialization returns its Actions and before reservation,
private commitment/ACK or interpretation. Driver still owns the actor, while
the borrowed environment owns Original initialization. Its existing
ActivationPanicked retirement already becomes HostCommitPanicked; child startup
already returns HostRejected with the original actor/actions and retains the
exact native cause. Entity uses that same owning launch path. No new failure
protocol, actor engine or retirement variant is needed. This is Bombay policy;
it preserves the existing ordering of actor effects and fresh commitment.

The earlier Prepared/configuration-retry hypothesis atc34f8d2 is reopened: it
unnecessarily ran user construction before Driver ownership and missed this
existing guard. Accepted runtime configuration authority retires normally;
original domain actor, untouched initialization Actions, native cause and
uninvoked Work remain independently owned. The root regression must inspect
HostCommitPanicked, recover the original request and retry the original actor
and callable with a fresh supplied runtime configuration. Pure init rejection
must never construct services; retirement must work when construction never
completed and must never invoke the constructor. Repeated activation polls must
reuse the actual constructed service. Ordinary absence remains zero-sized; an
Option<()> field for every native actor fails that memory requirement.

CommitActions currently has no explicit interpreter-initialization operation;
that owning operation remains to be proven. Do not overload commit, deadlines
or source settlement as a hidden constructor hook, and do not require manual
interpreters to provide empty callbacks. The claim concerns only the supplied
service callback: existing make_interpreter, ChildBindings::new_in and namespace
selection callbacks in LocalEnvironment::prepare need their own evidence.
Both independent source reviews confirm these cuts; native execution remains
unproved. Test-only production/API delta stays zero. Root evidence does not
close child or Entity constructor failures.

Native witness checkpoint: c34f8d2 adds694 test lines with zero production/API
changes. Its prepared-scope recovery test and corresponding source hashes in
NATIVE-CONSTRUCTOR-WITNESSES-CHECKPOINT.json are superseded by the guarded law
above; the original raw RED receipts remain evidence only for that revision.
The unchanged descendant witness checks the actual single-child report shape,
per-actor File counts, role-specific original actors, explicitly committed birth
receipts and actual sender/origin correlation. Real typed replies follow each
File lane, avoiding any stronger startup-order assumption.

The revised root witnesses preserve every original Actions lane and exact native
cause, recover the original actor/request/callable with a fresh runtime config,
and prove pure init refusal never constructs services, including during cleanup.
Both independent source reviews pass for these hypotheses. Pinned1.99 check
remains intentionally RED on missing ExternalRequest; complete type checking
and native execution are unproved. Raw command: GUARDED-CONSTRUCTOR-PRE-EDIT-RED;
current hashes/deltas: GUARDED-CONSTRUCTOR-WITNESSES-CHECKPOINT.json. The standard
24 ActorRetirement alternatives and all production control sums, branches,
modules and public spellings are unchanged. Notice owns an optional actual typed
refusal, and the constructor owns an optional actual native cause; neither is a
semantic flag or another transition authority. Production/API delta remains
zero. DX15 stays open: pure init panic, dropped/recreated activation, child/Entity
constructor panic, manual-host selection, non-Sync service custody and complete
native compiler/runtime proofs remain required. No production candidate is
retained from these test-only hypotheses.

Native interpreter candidate — DX15/16

Pre-edit law (derived native composition policy): namespace metadata and native
execution selection remain distinct. NativeHost selects the existing complete
capability constructor through Runtime. Ordinary ActorSpace/generated Topology
select it automatically; a manual local host owns its selection and retains its
own NativeChild implementations through descendants. Pure Hosts,
RootRegistration, ApplicationTopology and EntityDefinition do not require that
execution capability. Entity keeps nominal D::Hosts and EntityRef<D>.

Caller syntax: Application::new(actor).with_interpreter(factory), or one declared
`interpreter: Config;` with Topology::new(config). InterpreterFactory accepts a
callable or named configuration; ExternalRequest associates a leaf with its
concrete interpreter. One actual service belongs to each actor. Services require
Send, not Clone or Sync; native absence is unit and supplied custody is Option<S>.
Ordinary callers supply no absence policy. A real unit-valued interpreter still
requires its explicit constructor. Root/child/Entity reuse one construction body;
macros only derive syntax and static selection. No new dependency or dispatcher.

Guarded startup constructs the service after pure initialization hands over its
original Actions, before reservation/binding ACK/effects. Initial unit/None state
is determined statically; no configuration callback runs before that guard.
Retirement never constructs a missing service. Pre-edit witnesses6826a61 and
GUARDED-CONSTRUCTOR-PRE-EDIT-RED precede production. Existing Driver/native
terminal protocols retain constructor panic without another child failure sum.
Expected change was +250 to +450 production lines, four public contracts and the
existing assembly/leaf/macro owners; expanded cumulative budget is authorized.

Independent disposal law, recorded before its fix: Services and Runtime<Config>
can each own a panicking destructor. The real File subprocess witness produces
SIGABRT under whole-product disposal (GUARDED-INDEPENDENT-DISPOSAL-NATIVE-RED).
Final disposal now exhaustively separates Services, catches its destruction while
configuration remains owned, then catches the unchanged native product. Both
original causes append to existing retirement_failures. The operation owns only
synchronous disposal; async child/report retirement stays with its existing
owner. No new actor state, lifecycle authority, protocol or error alternative.

Native focused evidence: six application journeys plus the subprocess parent
and three original request-loan tests each pass on1.99/1.96 debug/release,
with all source hashes conserved (GUARDED-NATIVE-FINAL-MATRIX). The deliberate
ignored child is invoked by its parent. Reviewed replay also passes all four configurations
(GUARDED-NATIVE-REVIEWED-MATRIX), requiring exactly one matching child and exact
service-before-constructor cause order. Coupled-disposal and omitted-child
inversions fail in both profiles; restored controls pass
(GUARDED-DISPOSAL-INVERSION). Descendants use actual
Cell-owned non-Sync services: two configuration values each construct four
separate instances, each used once; actual File receipts, original actor boxes,
creation provenance, ready sender/origin addresses and every retirement lane are
checked. Constructor panic retains original actor/Actions/cause/uninvoked work
and permits real recovery. Pure initialization refusal constructs no service.
Interpreter panic retains the acquired File, pending request and untouched
suffixes. Native cancellation waits for the in-flight batch and then retains its
complete settlement. The earlier tests that assumed return-from-work cancellation,
pending-effect preemption or retained empty initialization are falsified and
receive no credit; their raw failures remain. A real cancellation-preemption
inverse fails the waiting oracle in debug/release; both restored controls pass
(GUARDED-CANCELLATION-INVERSION).
Focused strict lint passes after separating synchronous owned disposal and
preserving complete test oracles. Final extended formatting and strict lint pass.

Aggregate checkpoint: ActorRetirement has24 alternatives before and after
(including Panicked/Cancelled; the earlier count22 omitted those two). No
production control enum or existing native-leaf transition body changes. The
only new subordinate runtime sum is supplied Option<S>, whose live payload is
the actual actor-local interpreter. Standard absence remains zero-sized unit.
No arrival history, repeated cause, false cardinality, semantic boolean,
structural caller type path or second transition authority is introduced.
Measured whole-candidate delta, public spellings and source hashes are retained
in GUARDED-NATIVE-EXTENDED-COUNTS; include untracked services.rs and tests.
Production +520/-127/net393; tests +854/-96/net758 (Git diff plus untracked files).
One production module and four public types are added, none removed; twelve
Rust public spellings plus one DSL clause are added. No new actor transition
branch or terminal alternative. These are new capabilities, not code reduction.
Cross-check DX15/16, Driver D-INIT/D-TURN/D-RETIRE, execution-ownership, native
creation/ACK and normal atomic-actor contracts. Two independent source/test
reviews identify and correct the custody/oracle faults above. Overall candidate acceptance remains open: dropped/recreated activation,
layout/costs, final package/full gates and catalogue remain unproved. Entity
constructor panic now has the focused current-thread evidence below. Manual hosting, child constructor panic and pure init panic now
have the focused evidence below; they do not close those remaining obligations. The second wrapper order now uses the
existing pure timer-stop policy: File/env receipts remain complete, while the
unemitted second requests stay with the actor at their original allocations.
An initial base_mut accessor assumption was false and is removed without any
owner API change. A supplied unit interpreter performs actual IO and retains
its complete native initialization rejection; the first Completed oracle was
false because initialization rejection overrides Stop. On both supported
compilers, removing the explicit constructor is denied E0277 and assigning the
wrong interpreter owner is denied E0271; both restored native controls pass
(GUARDED-SELECTION-DENIALS). All twelve focused tests pass current/MSRV debug/release with all593 source
hashes conserved (GUARDED-NATIVE-EXTENDED-MATRIX); one deliberate ignored child
runs through its checked parent. Exact command durations are7.395/12.652/4.087/
13.752 seconds. This remains focused native evidence, not full repository gates. No migration, publication or whole-programme acceptance follows.

Manual-host proof obligation: the configured runtime remains a field of one
local namespace owner, so generic NativeChild implementations keep local Self
and the same configuration through grandchildren. The concrete witness reuses
Root/Document actors and generated terminal custody, but writes the existing
manual registration/edge ports explicitly. Execute two genuine creator types:
ordinary Root with owner cancellation and StopOnShutdown<Root> with requested
normal stop. Each must acquire three real File receipts, preserve original actor
allocations and correlate actual ready sender addresses with creation receipts
and complete joined retirement. Factory identity/use counts remain distinct.
This is test-only proof of the pre-edit extension law: zero new production
symbols, bounds, phases, states or protocols. Cold-member/generated-host proofs
remain separate. A compiler failure requiring another owner wrapper or widened
capability falsifies the candidate rather than authorizing such a repair.

Remaining startup witnesses use the existing native equations: pure init panic
must return InitializationPanicked and its original actor/request/cause without
constructing a service, including during retirement. Missing native host must
return the original Application and callable untouched; retry under an entered
host must use the same constructor inputs and actor/work allocations exactly
once. Notice's test startup input becomes one exhaustive Ready/Rejected/Panicking
sum instead of coordinating optional refusal and panic fields. The production
control-state model and public API remain unchanged. Pre-edit request/retirement
laws already require these cases; test-only additions supply their native oracles.

Child constructor failure proof, pre-edit DX15: a real root File/ready lane must
settle even when its requested child's service constructor panics after that
child has produced pure initialization. Use existing Root/Document actors and a
separate named fault configuration with two explicit constructor outcomes. The
first owns real root IO; the second owns the original boxed panic cause. The
child's emitted File/ready/recursive creation stay in HostRejected, while its
original actor and unopened grandchild remain owned without binding ACK. Exact
CreationId/CreationKind/route must agree with the independent startup failure
origin; complete retirement and both notification results retain their facts.
No Restarted/Established receipt is permitted. This tests existing guarded
creation and StartupRetirementFailed equations; no new production symbol, bound,
state, public spelling or transition is authorized. Overall candidate remains
reopen until this and the other named proofs pass.

Child startup oracle correction: the first test wrongly assumed an accepted
HostRejected outcome was an ItemSettlement rejection and waited for an unrequested
root stop. The test process was terminated after59.717 seconds; raw transcript
GUARDED-CHILD-CONSTRUCTOR-OWNER-DEBUG is retained without passing credit. Selected
Behavior classifies ItemSettlement::Accepted independently of its owned creation
outcome. Native creation.rs returns Accepted(HostRejected), so the creator
continues and receives startup readiness; its separate child failure retains
the original cause. Correct the test to await actual root ready, request shutdown
and require normal root completion plus exact failed-child custody. Do not alter
production or collapse these independent facts.

Manual cancellation oracle correction: current debug/release each passed all17
focused tests, but the frozen MSRV debug replay exposed an unsupported assumption
that receiving all three actor-ready messages proves caller work has started.
Those separate facts can race; cancelling before work entry legitimately retains
NotInvoked rather than Interrupted. Preserve raw GUARDED-NATIVE-STARTUP transcripts
without four-configuration credit. Add an outside-work oneshot entry signal, keep
driving execution until that actual callback signal is received, then end the
execution owner's scope. No channel is used inside a pure Behavior transition and
no production scheduling or cancellation rule changes. The two native phases
remain distinct rather than broadening the expected result to either phase.

Final startup/manual witness evidence: GUARDED-NATIVE-WATCHDOG-MATRIX passes
all17 exact named tests on1.99/1.96 debug/release, with all595 tracked/untracked
source hashes conserved in every run (6.532/22.851/7.177/25.439 seconds).
One deliberate ignored subprocess fixture is actually executed by its checked
parent. Both independent review passes accept the corrected child and manual
oracles; work-entry observation has a five-second watchdog. Final strict lint
passes all six focused test targets in2.688 seconds; pinned formatting passes.

This test-only batch is +1034/-6/net1028 test lines and zero production/API
changes. The full candidate remains +520/-127/net393 production lines in15
files, with four public types and one production module added. ActorRetirement
has24 alternatives before/after and zero production transition branches added.
Notice's test-only Ready/Rejected/Panicking input replaces optional refusal; the
fault constructor uses a direct ordered queue of explicit Result outcomes. Both
own current test inputs, never production arrival history or transition authority.
Residue scan finds no production semantic boolean, repeated cause, inferred
provenance, widened capability or structural caller path. Cross-check Driver
D-INIT/D-TURN/D-RETIRE, creation/ACK, execution-ownership and normalized actor
contracts. Disposition: pass for this bounded test batch; the extension itself
remains uncommitted pending Entity, activation retry, costs and final gates.

Entity interpreter witness, pre-edit DX15/22: the same nominal configured
Topology is D::Hosts for two independently owned configuration values. In each
application, hydrate a command-owning Ledger with real File/ready effects and
existing recursive ManagedDocument children; await actual command/child reports
before graceful retire_entity. Then hydrate another original Ledger whose
explicit constructor outcome panics before binding ACK. Return its original
command as Refused/Unavailable, retain original actor/initialization/cause in
EntityActivationError::Launch, and inspect callback custody after joined family
shutdown (a rejected send alone precedes activation_failed). Preserve every
callback value, native report, provenance and resident/launch metric separately.
Reuse EntityDefinition, NativeEntityHost/NativeConstruction, existing Actions
and generated terminal schemas; no new production symbol, bound, lifecycle
protocol, dispatcher or wrapper. This is a test-only proof of the already
recorded derived native-construction law. The original Document Msg=Never stays
unchanged; a real Ledger command triggers hydration. No caller placeholder or
no-op callback supplies the missing semantic journey.

Entity witness oracle correction: the first native run expected a historical
shutdown roster containing both EntityIds. Actual shutdown correctly reports
zero currently represented slots: directory::remove_matching removes both
retired and failed inactive slots. Original identities remain in their separate
retired/activation_failed callbacks. Preserve GUARDED-ENTITY-FIRST-DEBUG (the
intended roster assertion fails) without passing credit; require Settled with
represented=0 and an empty roster, retaining complete callback identities and
values independently. No directory retention or production change follows.

Entity retirement oracle correction: GUARDED-ENTITY-ROSTER-DEBUG reaches
original actor inspection but falsifies Completed-only retirement. Graceful
reports the successful processing fence. Native retirement queues typed shutdown
then requests owner cancellation before awaiting join. On this current-thread
host both requests are ready before the actor can resume, and cancellation
precedes ingress (environment.rs::next). Require OwnerCancelled with the exact
one queued ShutdownRequested, complete work/IO/command custody and no acquired
ingress; recursive Never children are also owner-cancelled, with empty controls.
This preserves the stricter concrete transition without changing scheduling or
treating Graceful as normal actor completion. Broader executor alternatives
remain covered by existing native Entity witnesses. Raw failures remain.

Entity proof evidence: GUARDED-NATIVE-ENTITY-MATRIX requires18 exact passing
names in each1.99/1.96 debug/release run and conserves all596 source hashes.
Durations:7.263/29.185/8.196/32.678 seconds. The one deliberate ignored child
executes through its checked subprocess parent. Strict configured-descendants
lint passes in1.940 seconds; pinned formatting passes. Both independent review
passes accept the final current-thread control/retirement oracle. Each Entity
case runs twice with independently owned LedgerConfig values and the same
nominal D::Hosts; all callback products, constructor counters, Files, original
commands/actors/actions/cause and birth provenance are inspected after joins.

Aggregate checkpoint: this batch adds745 test lines and zero production/API
lines or symbols. Candidate production remains +520/-127/net393 in15 files,
four public contracts and one module; all production hashes match the prior
checkpoint. ActorRetirement stays24 alternatives and adds zero production
transition branches. Ledger is one ordinary actor authority with independent
request/birth custody and owned commands; test fault inputs use a direct queue
of Result outcomes. No repeated production cause, arrival-history state,
semantic boolean, widened bound, structural user path or second dispatcher.
Cross-check DX15/22, Driver D-INIT/D-RETIRE, native Entity launch/ACK, directory
slot removal and normalized actor contracts. Disposition: pass for this bounded
Entity proof; candidate production remains uncommitted pending activation retry,
costs, final package/full gates and migration.

Activation retry witness, pre-edit DX15: dropping a borrowing activate future
while private binding ACK is pending must retain the same native environment,
original initialization and actor-local interpreter. Recreate the operation
twice against the same outside slots; construction stays exactly once and its
real File remains undisposed until actual native retirement. ACK/publication
still follow their existing order, and accepted cancellation is acquired before
complete residual/resource disposal. Use native construction and the actual
LocalEnvironment/ActiveEnvironment ports in the owning services module's unit
tests; no new production accessor, callback, state, bound or capability. A real
constructor opens the original manifest File and its drop records complete
metadata/error custody. An isolated inverse that replaces get_or_insert_with
with unconditional reconstruction must fail before ACK for duplicate creation.
Aggregate production states,24 terminal alternatives, branches, public surface
and production lines stay unchanged; the new test is cfg(test) only.

Namespace-free native construction, pre-edit derived law: the empty namespace
product () owns no shared logical registration but can execute actual exact-only
children through existing NativeChild::registration. Its constructor is standard
Runtime, with unit native service absence; it must imply neither Hosts<P> nor
RootRegistration<P> and must not accept ExternalRequest. Existing unchanged
delivery::generic_proxy_child_input_returns_exact_non_clone_input_after_native_shutdown
and observation::committed_distinct_event_targets_transfer_to_unrelated_shutdown_interpreter
are complete real child/proxy/worker/refusal/retirement witnesses. The owning
unit build GUARDED-ACTIVATION-RETRY-FIRST-CHECK is RED because their existing
NativeChild-for-() paths lost native selection; do not replace them with a marker
test or cfg(test)-only impl. Add production NativeHost for () selecting actual
standard construction (expected +8 lines, no new type/bound/phase). Explicit
selection in three named manual-host fixtures is a separate mechanical adoption
of the already proven manual-host contract; keep local Self child ports and all
original assertions/disposal values. ActorSpaces remains pure metadata. Both
independent design reviews accept this law; constructor absence remains unit
and configured unit interpreters still require explicit construction.

Activation retry proof: the initial empty-Actions test was insufficient for
affine custody. Its replacement retains one original boxed EstablishedDelivery
through two dropped ACK-pending operation futures. The actual recipient stays
pending before ACK, then receives the same allocation/content/origin exactly
once. Published ActorRef equals the original private endpoint; complete native
settlement and retirement retain the accepted receipt and dispose the original
File exactly once. This proves endpoint identity, not global address freshness
across independent application scopes. Both independent review passes accept.

GUARDED-AFFINE-ACTIVATION-INVERSION simulates unconditional reconstruction in
an isolated source copy: debug/release both fail the intended constructor count
(left2/right1), and both restored controls pass with the original source hash.
GUARDED-NATIVE-AFFINE-MATRIX passes337 owning unit tests plus18 native consumer
tests in each1.99/1.96 debug/release run, conserving all596 source hashes.
Durations17.460/57.124/30.125/171.338 seconds. One deliberate ignored subprocess
fixture executes through its checked parent. Focused strict lint passes in4.015
seconds and pinned formatting passes. These results include both unchanged real
namespace-free child witnesses; external manual-host catalogue migration remains
separate and has not passed all-target checks.

Aggregate checkpoint against6826a61, including untracked source: production
+531/-127/net404 across15 files; tests+3032/-104/net2928. These physical-line
counts conservatively include cfg(test) methods inside production impls; an
independent balanced-module count reproduces every file. NativeHost for() adds
11 physical production lines including spacing; the other new source in this
batch is test-only. Public types remain four, with one production module,
12 Rust spellings plus one DSL clause, 24 terminal alternatives and zero new
production transition branches. Driver's complete control sum stays seven:
Prepared/Activating/Initializing/SettlingInitialization/Active/Retiring/
RetirementAttempted. LocalResidual stays Prepared/Uncommitted/Retired (three);
Publication stays Pending/Published (two). There is one transition authority.
Option<S> owns absence
before guarded construction or the exact acquired interpreter, whose File stays
live through ACK and is disposed at retirement. No duplicated cause, inferred
provenance, semantic boolean, extra dispatcher, widened protocol or structural
caller syntax is introduced. Existing opaque native panic custody is reused;
there is no protocol erasure. Cross-check DX15/22, Driver D-INIT/D-RETIRE,
creation/ACK, execution-ownership and normalized actor contracts. Disposition:
pass for this bounded proof. Candidate production is retained for integration after the focused gates below;
final repository/archive gates and delivery remain open.

Standard counter cost comparison, DX15/26: final
NATIVE-STANDARD-MEMORY-FROZEN-MATRIX passes 40 fresh processes: five before and
five after in each current/MSRV debug/release configuration. Both probe sources
are byte-identical and compare clean120cc2d with bf107a4 plus the recorded native
interpreter overlay. All 79 normalized dependency records match. Probe source,
manifests and locks are hashed before execution and conserved after every run;
both repository inventories are conserved. Both independent reviews pass after
adding actual reply-origin and both nested notification checks. Earlier unfrozen
or weaker-oracle measurements receive no final credit.

| Compiler/profile | Allocation blocks, before = after | Peak bytes, before = after | End bytes, before = after |
| --- | ---: | ---: | ---: |
| 1.99 debug | 80 | 142060 | 56 |
| 1.99 release | 78 | 138724 | 56 |
| 1.96 debug | 80 | 142100 | 56 |
| 1.96 release | 78 | 138724 | 56 |

Every complete heap product agrees across all ten runs within its configuration,
including total bytes and peak/end blocks. The actual increment/read/reply,
shutdown/termination, joined cleanup, notification results and complete native
root custody are checked. This is the exact default-feature counter comparison;
remote code and the configured service's own memory are outside its scope.
Shared caches and concurrent unrelated builds prevent comparative compilation
or latency claims. No memory reduction or programme completion is claimed.

Manual-host catalogue adoption, pre-edit DX15/25: the complete all-feature
workspace check (GUARDED-NATIVE-CATALOGUE-ALL-FEATURES-RED) fails only four test
targets using seven existing manual native hosts. Reuse the proven NativeHost
port with standard Runtime for AccountHosts, QueueRegistration, HostAccountSpaces,
UnwindingAccountSpaces, GardenSpaces, SupervisorHosts and CounterSpaces. Their
actual native child/Entity/queue/replacement/refusal journeys already define the
required constructor choice; pure Hosts/RootRegistration metadata is unchanged.
The two generic generated/manual comparison functions execute standard native
applications, so require NativeHost<Runtime = Runtime> at those execution cuts.
Both have two real substitutions. Accounts<Spaces>::EntityDefinition stays pure.
This is adoption of the established derived native-construction law: no new
production type, trait, bound, field, callback or policy. Preserve every original
assertion, accessor panic, child port and terminal value. Expected delta: eight
test files, about 80 test lines, zero production/API changes. Both independent
design reviews accept. Before/after Driver control states stay seven, residual
alternatives three, publication alternatives two and terminal alternatives 24;
no actor transition authority or branch changes. Run the four real downstream
targets plus owning/service matrix and strict lint before retaining this batch.

Static-authority checkpoint: the first catalogue run fails four existing
snapshots. Three retain exactly the same error-code sequences and differ only
in the source excerpt adding NativeHost at native execution. The forged
constructor now reports missing NativeHost before its existing sealed-startup,
non-Send actor and report-interpreter failures. Do not accept that earlier
rejection as proof of independent creation authority. Supply the existing
CounterScope with only the proven standard Runtime selection, preserve its
original Rc actor and counterfeit EstablishNativeChild body, then require the
creation-authority denial independently. This is a test refinement of the
existing sealed native-creation law, not a production change or new bound.
Compare both supported compiler transcripts before changing reviewed expected
files and their minimum-compiler patch; preserve every original invalid program
except this explicit prerequisite, and retain the initial mismatch evidence.
The matrix driver's predicted338 owning tests was also false: actual all-feature
execution has337. Its planned total is421 (337 owner +18 services +66 advanced
consumer tests), independently of any snapshot outcome; correct that count
without granting the failed first run passing credit.

Refined authority evidence: after the explicit standard Runtime prerequisite,
both1.99 and1.96 report identical E0119 at the original counterfeit impl. The
actor's Rc state and fake execution body remain unchanged. Only diagnostic line
numbers and truthful capability type arguments differ from the old expectation.
The other three current snapshots change one existing NativeHost bound excerpt;
MSRV swapped/duplicate-role transcripts remain exactly their prior reviewed
values and omit that excerpt. Preserve all original error-code sequences. The
minimum patch is rebased over these reviewed canonical changes, with all ten
expected patched hashes checked; the first patch-preparation assumption that
MSRV retained both role excerpts was rejected before manifest changes.

Final verification separates responsibilities: the existing immutable
supported-compiler diagnostic driver owns all37 named fixtures, six suites and
30 outer tests on each compiler. The native all-feature runtime matrix runs
418 outer tests per compiler/profile (the same421 selected tests with exactly
three topology compile harnesses delegated to that driver). Count outer test
names separately from compile-fixture paths, require exact filtered counts and
preserve the failed first combined replay. No failing runtime or static denial
is excluded: the moved harnesses execute in the independent diagnostic check.

Catalogue retention checkpoint, DX15/19/25: all four reviewed all-feature
runtime configurations pass418 outer tests each (1.99 debug18.047s,
release441.382s;1.96 debug118.710s,release470.600s). Every configuration conserves
all596 source hashes and delegates exactly three topology compile harnesses to
the unchanged immutable diagnostic driver. That driver independently passes all37
fixtures, six suites and30 outer tests on both supported compilers, preserving
caller and scratch sources. Full workspace/all-target/all-feature checking and
strict Clippy, all-feature rustdoc with denied warnings, and formatting pass.
The first failed replay and reviewed four diagnostic refinements remain recorded.
No final repository/archive acceptance is inferred from these focused gates.

Native request-owner CI law, pre-edit DX15/19/25: permanently check the existing
native execution distinction between absent services and an explicitly supplied
unit interpreter. OpenFile declares unit ownership and its unit interpreter
performs actual File IO through the existing Services implementation. The same
ordinary application must reject an absent constructor (E0277) and a Services
constructor with the wrong declared owner (E0271). The real explicit-unit runtime
journey independently checks accepted File, original rejection, actor and full
retirement; no empty interpreter or placeholder proves the law. External caller
probes already compile the explicit unit case and reject both alternatives for
these exact errors. Add two invalid programs and one compile harness using the
same actual request owner, then enroll them in immutable supported diagnostics.
This is test adoption of the existing static capability law; no production or
public interface change. Driver states7, residual alternatives3, publication
alternatives2 and terminal alternatives24 stay unchanged; no branches or actor
transition authority are added. Reuse ActionItem, ExternalRequest, InterpretItem,
Services, Application::run and the existing diagnostic owner. Keep fixture counts
separate from outer tests, preserve prior expectations and review both compiler
transcripts before accepting them. Cross-check DX15/19/25, Driver D-INIT/D-RETIRE,
execution ownership and native Services capability selection.

Owner-denial transcript review: both supported compilers produce identical
expected bytes: two E0277 diagnostics for missing interpretation and two E0271
diagnostics for the wrong declared interpreter owner. Their actual receiving
loans and real File service compile before the final Application::run capability
check; no unrelated error establishes rejection. New immutable coverage is39
fixtures, seven suites and32 outer tests. The existing ten compiler-specific
snapshot entries and their reviewed minimum patch remain unchanged by these two
identical transcripts. Both initial capture runs fail only because the two new
expectation files did not yet exist; their raw logs are retained.

Native extension retention, DX15/19/25: both immutable39-case checks pass
(current114.506s;minimum117.159s), with seven suites,32 outer tests and600
conserved caller/scratch source hashes. Both exact invalid programs compile on
both compilers when only their constructor becomes explicit unit; these four
positive controls isolate the rejected capability. The two-test selection target
and formatting pass. Both independent source and terminal-receipt review passes accept the law. The canonical Cargo-readme guide now
describes constructor ownership, one topology clause, guarded construction and
manual NativeHost selection, and links the actual IO/failure consumers.
Cumulative Rust delta against6826a61: production+531/-127/net404 across15
production files; tests+3265/-114/net3151;36 changed Rust files. Four public types,
12 Rust spellings, one DSL clause and one production module remain unchanged.
Complete Driver states7, residual3, publication2, terminal24, and actor transition
branches remain unchanged. The same exact owned request/reply, current service,
configuration, ACK and endpoint are needed at each surviving alternative; residue
scan finds no new arrival history, duplicate cause, inferred provenance,
semantic flags, false cardinality, structural user syntax or second authority.
Cross-check DX15/19/25, D-INIT/D-RETIRE, execution ownership and native creation.
Disposition: pass for focused integration retention; final combined repository,
archive, configured-service cost and delivery requirements remain open.

Integrated delivery verification: exact120cc2d passes
`nix flake check --keep-going --max-jobs 1 --cores 2 --no-update-lock-file -L`
in4663.558 seconds. All24 expected aarch64-Darwin checks succeed; all586 tracked
hashes, HEAD and clean Git remain unchanged. Optimized workspace summaries
contain749 passed, zero failed and one intentional parent-invoked subprocess
fixture ignored. Expected Driver-law defect runs fail independently inside
their successful evidence gates. Independent review reconciles the full gate
set, source inventory, four363-test compiler/profile runs, four two-test Node
hosting runs and both capacity inversions/restored controls. Delivery advances
ff-only ea2b5b4→120cc2d. Receipts CLIENT-INTEGRATED-FULL-NIX and
CLIENT-INTEGRATED-GATE-AUDIT retain raw commands, hashes and exact check names.
This proves the integrated baseline only: the separate configured-interpreter
candidate, Linux/x86_64-Darwin, new archive checks and publication receive no
credit. Earlier signing/disk failures remain failures. This record is a
documentation-only successor to the frozen verified source.

Latest-main integration: fresh fetch at2026-10-15T10:42Z selects75c8db4/PR371.
An isolated release-integration worktree merges it into delivery ac1375c as
cf6c5ef. All11 incoming paths merge without conflict. Root and isolated fixture
versions advance0.1.2→0.1.3; the fixture adds alloc_count0.4.0 only as a pinned
verification dependency. Main changes synchronous allocation measurements and
retains Dhat stack diagnostics; remote/protected policies and every original
assertion remain. Required commands add nocapture, CI timeout becomes120 minutes.
The merge changes no crates/bombay/src production relative to ac1375c, preserves
the DevX outcome migrations and configured client-capacity correction, and adds
no ordinary caller ceremony or public API. Owner dependency selection remains
Behavior/Actors0.26.0. No custom-interpreter candidate is included.
Exact combined source790fa08 passes all24 native aarch64-Darwin Nix checks
in5404.589 seconds with all586 tracked hashes, HEAD and clean Git conserved.
Workspace verification has104 summaries:749 passed, zero failed and one
intentional parent-invoked subprocess fixture ignored. Both independent reviews
reconcile exact gate names, derivations, source inventories and the unchanged
production contract. Delivery advances ff-only from ac1375c after this record.
RELEASE-INTEGRATED-FULL-NIX and RELEASE-INTEGRATED-COMPLETION-AUDIT retain the
complete command, raw24 successes and source evidence. Other systems, new
archive checks, publication and the separate interpreter candidate stay open.

Current store availability is separate: two successful outputs were absent,
were rebuilt from their exact recorded derivations, then four other successful
outputs were absent. A concurrent store-GC process was observed. Both strict
cache-audit failures and exact restoration receipts remain; the final completion
audit records20 available outputs with exact derivations and four absent outputs.
No continued-cache claim, failed-gate waiver or further restoration loop is made.
The first audit parser also missed literal terminal fragments in summary lines;
the reviewed finite parser and independent104-summary count agree.

Latest-main conservation, pre-edit DX01/15/20/25: merge42ba1dc includes PR372
without new production conflict resolution; source retains native constructor
selection at all three launch cuts and main's exact application association.
Fresh all-feature/all-target checking fails15 incoming remote tests still peeling
the former three-part receiving tuple, plus the owning ACK test's native prepare
call missing main's optional application association. Mechanically peel the same
complete ApplicationOutcome with into_parts, preserving all original values and
assertions. The isolated native ACK test has no Application-associated endpoint,
so its explicit optional association remains None, matching other direct native
hosts. No production interface or capability changes are justified by this check.

Joined Node export law, pre-edit DX01/20/25: the existing Node::run_with must
supply the same actual Node Session to its ApplicationHandle as execute_with.
Both are public entries to one Application invocation. Main currently adds this
association only to execute_with, so a real joined export must fail NodeAbsent.
Use the incoming Counter export-construction journey with its two genuine
receipt profiles as the focused witness: replace only paired execution with
run_with(async move |application, session: &Session| ...).await, retain every
native Session identity, actual declaration/undeclare, shutdown, retirement,
notification, empty request/ingress and refusal assertion. The incoming Ledger
journeys continue exercising paired execution. Expected correction after RED:
reuse the same Session clone assignment at the existing invocation cut; zero
new types, bounds, states, dispatchers or policies. Ownership equation is one
borrowed original Session plus its existing handle clone, with full work/root/
notification custody unchanged. Cross-check native Node invocation, exact actor
association, DX01/20 and D-RETIRE. Broader remote protocol semantics remain owned
by PR372 and its separate PRD.

Receiving migration correction: the initial single-line script matched13 sites,
not its predicted15, and stopped before the joined witness or owning ACK change.
The next attempted command failed only remaining old tuple consumers and receives
no joined-law credit. Two multiline paired sites and two local completed results
make17 actual consumers. All use the already proven complete into_parts product;
no compiler-suggested reference adjustment is accepted for a cascading inference
error. Preserve both failed checks and require the actual joined runtime RED.

Joined export RED: NATIVE-JOINED-NODE-EXPORT-RUNTIME-RED compiles the real
combined consumer and fails both receipt-profile tests with exact NodeAbsent,
zero passing and no other failure. No production correction existed during that
run. Reuse the existing Session association assignment from paired invocation
in joined invocation. Aggregate checkpoint before/after: no new state/result,
branch, type, module, public spelling, actor authority or protocol alternative;
the existing optional Session retains exactly native Node association or its
absence on a local-only handle. This is an existing capability value, not an
arrival-history label. Original work, Session identity and complete application
custody remain independently observable. Expected production delta: two physical
lines in the existing Node method, plus formatting; test migration remains
separate. Residue scan: no structural syntax, inferred ownership/provenance,
semantic flag, repeated cause or nested dispatcher. Disposition requires both
runtime profiles, original paired cases and independent review.

Joined export correction checkpoint: all four actual construction cases pass
on1.99/1.96 debug/release (27.209/145.918 and40.700/165.227 seconds). Counter
uses joined execution in both receipt profiles; Ledger retains paired execution.
The complete workspace/all-target/all-feature check passes15.202 seconds. Both
independent source reviews preserve all369 original assertion calls across the
seven migrated files, with no original effect or input dropped. The optimized
original-defect replay also fails exactly both joined cases with NodeAbsent;
its restored control is still running. Production+7/-3/net4 in the one existing
Node method; tests+44/-25/net19 including two cfg lines in the owning ACK fixture;
no public types/spellings/modules/states/results/branches/authorities added.
All existing optional association alternatives retain the actual Session or its
meaningful local absence. Disposition: pass this focused correction; the fresh
combined runtime, supported diagnostics, strict lint, full24 native Nix checks
and archives remain separate acceptance gates. The upcoming source freeze must
include the complete merged main and this correction.

Actor-only Node work (DX01/20): deliberate Bombay interface policy makes the
network SDK internal. The pre-edit Counter compile witness fails only E0282 at
node.run_with(|application| async move { /* typed actor work */ }).await.
Reuse Application's existing FnOnce(ApplicationHandle) -> WorkFuture contract,
Node's one declaration/connection owner, take_application refusal and the existing
private connection association. Remove the public SDK callback argument and its
higher-ranked loan requirement. No new constructor, wrapper, type, state, generic
parameter, dependency or capability lookup is selected. Work/future/result gain
no Send or static bounds. Runtime/transport tests can still inspect their SDK.

The complete ownership equation stays original Work on refusal; accepted work
returns its business value plus independent cleanup, actual native root and both
notification results. Missing-host refusal restores the same declaration for
retry. Successful startup still precedes invoking Work. Node closing stays a
separate owned operation. The focused stage changes only joined run_with;
configuration, Query-bearing exports and paired SDK execution remain open.
Existing SDK-based callers are migrated only after the focused journey and
association proof, using the established one-argument contract.

The four final Node tests cover real increment/read and exact root origin/value1;
missing-host retry of a real non-Clone File and original Box allocation;
borrowed-wait cancellation with original Rc and caller-local non-Send Cell;
Closed/Issued refusal and one successful reoffer of each original Work; and the
unchanged paired borrowing contract. One shared retirement oracle requires actual
Counter state0/1, both notifications, every independent residual lane and the
exact one retained Stop(Stopped) settlement with empty creation/send lanes.
The earlier Accepted-only oracle was vacuous for an empty collection. An actual
completion-owner settlement-loss inverse fails0-versus1 in7.937 seconds and
restoration passes4.160 seconds. Both invariant and counterexample reviews accept
every changed test; repeated green executions do not replace these reviews.

Both receipt-profile Counter export constructors now use the same actor-only
callback. Their existing declaration/drain/refusal/native/notification observations
remain. The returned export connection is open before actual Node.close and closed
afterward, independently proving shared lifecycle without a work SDK parameter.
Removing the actual private association fails both optimized cases with NodeAbsent
in81.804 seconds; restoration passes83.853 seconds. Ledger paired cases are unchanged.
These construction tests issue no Query and establish no provider/replay semantics.

The direct SDK-argument denial returns only shutdown, avoiding the unrelated old
borrowed-output veto. Its exact source compiles on originalfb295b6 (44.100 seconds)
and fails solely E0593 at the new one-argument contract. Current/MSRV immutable
suites pass all three exact borrowing/arity fixtures in17.402/16.440 seconds;
their sole arity golden is byte-identical. The obsolete loan-output denial is
replaced explicitly; remaining paired lending/close exclusion stay independent.
Both reviews validate fixture equality, actual original owner source, all609
source hashes, exact suite/mode coverage and raw-log hashes.

Shared target directories contaminated two current denial attempts, including
a sequential replay: Cargo reused the control's executable. Those failures and
the overlapping control receive no acceptance credit. Final verification uses
dedicated checkout/compiler targets after pinned cleaning of all three owner
packages, with exact listed test names. All40 Node/client tests and nine client
denials pass current debug/release in47.301/106.050 seconds and MSRV debug/release
in47.839/121.274 seconds; all609 frozen hashes remain conserved. Full workspace/all-target/all-feature strict debug lint passes20.387 seconds;
format checking passes2.458 seconds. Final repository/package gates remain open.

Aggregate checkpoint: Driver states7, residual3, publication2 and terminal24 are
unchanged. No new aggregate/subordinate state, result alternative, transition
branch, public spelling, production module or actor authority exists. The existing
optional connection retains actual Node association or meaningful local absence;
no arrival-history label, semantic flag, repeated cause or structural user alias
is introduced. Production+14/-18/net-4 (including rustdoc); Rust tests/fixtures
+138/-94/net44; diagnostic snapshots+73/-27/net46; public types+0/-0. Owning method
and ordinary guide use the same callback syntax. Driver initialization/retirement
law and the existing complete Application outcome remain authoritative. Disposition:
pass this focused callback layer, with SDK configuration/query/paired surfaces,
full gates, archive delivery and the broader DevX programme still open. Cleanfb295b6
Nix checks in the separate interpreter worktree cannot prove this later change.

Broader debug lint found one unchanged incoming export-client test over its
line limit. Its law accepts exactly ExportError::Endpoint and inspects that
original native cause/source by address; every other variant must fail the test.
The equivalent single let-else preserves every operation, assertion and drop
order without a lint exemption. Separate test-only commit94f607a removes10 lines;
its exact native unit test passes current debug26.869 seconds and MSRV optimized
122.647 seconds. Both source reviews pass; the earlier40-case integration matrix
does not cover this later cfg(test) edit. Whole branch Rust test delta is net34
including that separately reviewed edit; production remains net-4.

Final native gate correction (DX25/27), pre-edit: immutablefb295b6 completes24
Nix checks in4510.859 seconds with20 passing and4 failing. Test/coverage fail on
the cold wrong-role expectation's stale NativeHost/Services source hints; the
same11 E0277 role denials remain. The protected-counter gate finds two receiving
acquisitions still expecting the former triple. Remote debug lint finds the
test-length issue now separately fixed94f607a. The Engine phase mismatch is the
intended prepared-ingress mutation KILL, not a baseline defect; keep it unchanged.
No Darwin signing or disk failure occurred in this attempt; no failed gate is
waived. Archive verification ona1da19d separately passes all8 configurations of
the4 exact Node journeys,609 repository/491 extracted-consumer hashes, all75
production files and all3 actual crate hashes, with both independent reviews.

Reuse the existing complete ApplicationOutcome::into_parts product at both
protected receiving joins, leaving every original effect, assertion, borrow and
retirement value intact. The apparent reference errors are cascades; introduce no
new reference, bound, wrapper or protocol to address them. Refresh only compiler
expectations for unchanged cold fixtures from actual current/MSRV transcripts,
and add the existing cold_member_retirement target (4negative/1positive fixtures,
2UI suites,7outer tests) to the supported diagnostic corpus:44 cases,9suites,
39outer tests. Keep fixtures immutable during verification and review every
expectation difference. Production delta0; existing driver/terminal states,
policies and authority stay unchanged. Exact protected source failure precedes
this mechanical migration. Native-profile, compiler, source-conservation and
full24-gate reruns remain required. Record the user's internal-network rule in
AGENTS so lower-level access guidance cannot imply public SDK access.

The protected receiving compile repair passes all6 existing public_node_ cases,
but that filter does not execute its actual recorded/scheduled live requester
journeys. Run those2 directly on both compilers/profiles and add their explicit
nonempty debug/release replay to the existing protected-counter Nix gate. Preserve
the gate's other groups and serial native checks. No production change or new
test oracle is implied. Existing live-test limits stay explicit: the generic
retirement check accepts empty accepted settlements and does not independently
inspect the returned receiver Counter value; its native reply1 and exact requester
state[ReadReply(1)] remain independently observed.

Diagnostic completeness replay, pre-edit tooling law: both immutable compiler
runs execute all44 exact fixtures and39 outer tests with unchanged source, but
the receipt rejects the cold positive case because trybuild omits mode brackets
for a pass-only suite. Accept that real bare-pass format as well as explicit
`[should pass]`; continue rejecting explicit fail mode on pass paths, explicit
pass mode on fail paths, duplicate/missing/extra cases and incomplete summaries.
This changes only transcript recognition, not fixture acceptance or compiler
expectations. Demonstrate the current false rejection before editing and replay
counterexamples afterward; production Rust/API/state delta remains zero.

Retained gate-repair evidence: actual recorded/scheduled live requester journeys
pass current debug2.562/release196.190 seconds and minimum debug59.979/release
223.962 seconds. Wrapper receipts prove named execution, not independent per-run
source hashes. Both invariant/counterexample reviews retain the oracle limits
above. Rust/Nix format checks pass1.820/0.911 seconds; strict workspace and actual
protected-feature consumer lint pass1.540/26.549 seconds.

The original44-case immutable commands remain failed parser receipts. The
real-transcript RED rejects only the two actual bare-pass cases; after the
one-line correction all20 controls pass, including opposite mode, omitted,
duplicate, extra, missing-suite, changed-summary and empty-output rejections.
Fresh immutable runs pass current117.091/minimum120.988 seconds:44 unique cases,
9UI suites,39 outer tests, all13 reviewed snapshots and exact log hashes. Both
copied and caller source sets remain unchanged. Their610 files include one Python
bytecode file generated by importing the tool; it was removed only after both
runs ended. The609 repository files remain; no fixture was edited to obtain green.
No production Rust/API/state change is introduced. Full24-gate acceptance still
requires a new clean-source run; the earlier20/4 result stays failed.

Gate-repair aggregate checkpoint: control states7, residual alternatives3,
publication alternatives2, terminal alternatives24, production branches/modules/
public spellings and retained subordinate values are unchanged. No history,
repeated cause, false cardinality, nested authority, semantic boolean or user
structural syntax is added. Cross-check DX04/19/25/27, Driver law and the existing
protected custody contract. Production Rust+0/-0; test source+4/-2; expectation/
compiler records+781/-16; verification tools/gates+10/-1; public types+0/-0.
Disposition: pass the bounded receiving/expectation/tool repair; full gates remain
open. The742 patch lines represent three compiler-format snapshots, not new Rust
architecture, tests or relaxed capability proofs.
Both independent final evidence reviews accept the exact44-case logs, compiler
identities, all13 hash bindings and copied/caller conservation. Their post-result
comparison finds only this PRD narrative changed and generated bytecode removed;
tested source is unchanged. This is scoped retention, not programme completion.

Paired Node work (DX01/20/23): both entry points receive only the ordinary
ApplicationHandle: `node.run_with(|application| async move { ... }).await` and
`node.execute_with(|application| async move { ... })`. Raw Session is not a
business capability. This deliberate interface policy reuses take_application,
execute_with_invocation, private connection association and complete
ApplicationOutcome; it adds no callback family, bound, state or public type.

The law and focused regressions precede production. Original c6590d7 rejects the
first inferred actor-only paired caller solely E0593 in11.913 seconds. The new
paired SDK fixture compiles against that exact original owner in49.504 seconds,
then the candidate denies it solely E0593. Original owner/fixture/restoration
hashes bind that inverse. Requiring Send rejects actual Rc and &Cell captures;
requiring 'static rejects the actual stack Cell loan. All three owning-source
inversions fail for the intended law, and byte-exact restoration passes.

The complete counter journey recovers the original non-Clone File and Box
callable on missing-host refusal, retries once on the same Node, increments/reads
Counter and verifies reply provenance, retained state1, exact one Stop settlement
and every independent native/notification/cleanup lane. The other paired journey
drives execution before receiving, drops only a borrowed execution wait, proves
both original futures pending while permission is held, then resumes those same
owners and recovers the original Rc and caller-local &Cell, state0 and complete
retirement. Whole-operation surrender and connection association are separate
obligations. Both invariant/counterexample reviews pass every changed test.

Those exact bodies replace the deliberately superseded SDK-output test in
node_outcome; setup, complete retirement oracle and the other3 joined/refusal
tests remain byte-identical. The five runtime cases plus an initial2-denial
harness pass all8 current/minimum, debug/optimized, remote/all configurations
with614 frozen source hashes. A case-insensitive filename collision overwrote
row JSONs with wrapper receipts; every original row remains in raw stdout.
NODE-PAIRED-REVIEWED-MATRIX.json binds all8 recovered rows,16 list/run hashes and
final source conservation. Both evidence reviews close that artifact finding;
there is no rerun or extra pass credit.

Final node_borrowing uses the existing Nix-selected target: one positive for
both entry points retaining actual &Cell/Rc, joined and paired SDK-argument
denials, and E0499 for attempted close while retaining the paired futures.
The obsolete SDK-output positive/overlap fixtures are explicitly superseded.
Both compilers pass all4 final fixtures in debug and optimized profiles;
debug also replays all5 runtime cases (12.828/52.999 seconds), while optimized
commands run only the static harness (38.331/35.124 seconds). Production/runtime
body hashes still match the8-configuration proof. Both independent reviews
accept these exact scopes. Wrapper receipts alone establish no per-run source
hash claim. Final focused all-feature library/test lint passes1.916 seconds.

Aggregate checkpoint: Node retains its existing two fields (original optional
Application and Session); Driver7/residual3/publication2/terminal24 and all
subordinate current values, transition branches, modules and public types are
unchanged. Delete the now-unused SDK-result lifetime parameter; ordinary future
self-loans preserve close exclusion. Production+11/-9/net2 including comments;
signature/body+4/-4/net0; Rust tests/fixtures+258/-98/net160; snapshots+48/-26;
public types+0/-0. No history, duplicated cause, false cardinality, nested actor
authority, semantic flag or caller structural path appears. Cross-check Driver
initialization/retirement, ApplicationOutcome and DX01/20/23. Disposition: pass
the focused callback law; catalogue retention remains blocked on native-test
ownership. Do not add a session accessor, second callback family or dummy export
for those callers. Public SDK configuration/export/query surfaces remain open;
this candidate is not whole-repository or complete SDK-hiding acceptance.

Native-test ownership, pre-edit DX01/20/23/25: actual SDK query drivers belong
under Node's private test owner; public constructor/admission/refusal tests must
remain external. The business callback stays actor-only. Capture the owning
Node's actual shared SDK connection before invocation inside the private owner;
introduce no session accessor, callback variant, feature marker or fake session.
First prove the existing recorded/scheduled Counter and Ledger query/replay
journeys there, preserving all protocol values, calls, accepted prefixes,
provider/native observations and retirement oracles. Full SDK-dependent public
client compilation fails on the existing two-argument callbacks before this cut.

Keep one test-owned Ledger actor/protocol source and one shared finite
request/provider/configuration source for the real external/private consumers.
Parent visibility supplies only the existing plain protocol products and
provider functions needed by those tests; no validated runtime contract is
widened. The external constructor suite intentionally never executes provider
verification; any dead-code expectation must be limited to that partial fixture
use, not suppress interface or invariant failures. Reuse existing self-crate
alias, actual example Counter source, existing providers and full retirement
oracle. Expected production/API delta0 (only cfg(test) module curation); fixture
moves are test-only. Control7/residual3/publication2/terminal24 and all current
owned values/authority remain unchanged. Broad transport-test migration waits
for these four real private-owner journeys and both review passes. Public
configuration/export/query hiding remains a separate open interface obligation.

First-cut compiler veto: the moved Ledger's generated send product retained
private visibility while its actor became parent-visible. Use the existing
named-lane visibility at exactly the actor's test-parent scope; no public
runtime type, new bound or production macro change is involved. Remove imports
whose definitions moved to the shared protocol source. Independent reviews
conserve every original journey action and assertion. They also require an
additional observable: each retained connection clone is closed after its
owning Node closes. This proves the new private alias shares close authority;
an omitted actual close must fail this assertion even if an Ok receipt is used.

Focused ownership proof: all four original recorded/scheduled Counter/Ledger
journeys pass Rust1.99/1.95 in debug and optimized builds (16 executions).
Each command lists and runs exactly those four names; all615 source hashes are
identical across both compilers/profiles. Replacing only Node.close's native
close with Ok makes every journey fail specifically at connection.is_closed()
in both profiles (eight failures, no compiler error). Byte-restoring the owner
restores all eight passes; the complete615-source snapshot also matches.
Both independent evidence reviews verify all12 raw log hashes and the exact
mutant/restored owner bytes. The inverse reaches the first sender-close check;
it does not establish three independent close-site inversions. Existing generic
retirement/Counter-state and finite nonshipping provider limits remain.
NODE-PRIVATE-QUERY-{CURRENT,MINIMUM}-ROWS, NODE-PRIVATE-CLOSE-ROWS and
NODE-PRIVATE-QUERY-VERIFIED bind commands, actual names and raw log hashes.
Both source reviews conserve the original protocol/actor bodies, every action,
replay/native observation and retirement assertion; the new clone shares the
SDK's existing SessionInner and adds no opening or lifecycle authority.

Aggregate checkpoint: Node still owns its original optional Application and
connection. Driver remains Prepared/Activating/Initializing/
SettlingInitialization/Active/Retiring/RetirementAttempted, with unchanged
settlement3/action2/terminal24 alternatives and their current values. Runtime
branches, modules and public spellings are unchanged. Test source +1390/-1329/
net61; test-only module curation +4; four test source modules added; runtime
production and public types +0/-0. No arrival-history state, repeated cause,
false cardinality, nested transition authority, semantic boolean or structural
caller syntax is added. Cross-check actual Driver, Node association/close,
ApplicationOutcome and DX01/20/23/25. Disposition: pass the focused owner cut;
the remaining native catalogue and SDK-facing configuration/query contracts are
still open. No whole-repository or full SDK-hiding acceptance is claimed.
The external no-default-feature consumer also passes its seven runtime cases;
its two static harnesses are explicitly excluded from that command. Its only
follow-up is gating the moved Receipts import to its actual remote consumers;
the private native test bodies and owning runtime remain unchanged.

Native catalogue adoption, pre-edit DX01/20/23/25: reuse the proved private Node
ownership for JSON delivery, cancelled receives, A/B reply association and
same/foreign-export permission tests. Move their original bodies and finite
sources without adding a port, callback, bound, state, SDK opening or fixture.
Each real Node supplies its existing shared connection before actor-only Work;
each original close still owns shutdown. Preserve all originals, native causes,
accepted prefixes, replay and terminal lanes. The pre-edit16-E0593 public-client
failure is the caller witness; four actual private journeys are now proven at
48c730d before this broader move.

Public constructors, pressure and refusal cases stay external. Delete unused
SDK callback parameters. Move the Ledger constructor's exact original-Node vs
Export-returned connection-ID observation to existing private read_study:
compare Export's returned session to receiver_session, never the sender's
connection. That shared real query journey covers both provider modes for
Counter and Ledger. The external Ledger constructor gains the same independent
open-before/closed-after observation as Counter; retain every other constructor
value, action and assertion. Record this explicit oracle relocation rather than
claiming a lifecycle predicate proves connection identity. Expected runtime/API
delta0, no new state/type/fixture, existing native test module count unchanged.
Driver7/settlement3/action2/terminal24 and their owned values remain unchanged;
cross-check actual Node/Export association, ApplicationOutcome and the retained
request-key, cancellation and permission laws before retaining this batch.

Adoption matrix finding: current debug/optimized and minimum debug pass all353
library plus21 external cases and nine denials. Minimum optimized passes353
library cases and20 external cases, but scheduled Counter construction fails at
native opening before Work. The initial panic discarded its cause. A diagnostic
amendment preserves the native cause in the panic; the exact single-case retry
passes, which does not explain or waive the earlier failure.

Test-resource law, pre-edit: constructor-only cases have no connecting peer and
need no known port. The shared reserved_address releases its TCP listener before
SDK opening, so it cannot reserve authority over that address. Exercise the
actual constructor with that released address occupied on both loopbacks; it
must expose the native bind refusal. Then let these two constructor fixtures
request TLS port0, retaining all existing lifecycle, original-value and complete
retirement checks. Native opening must succeed while the unrelated claimed port
remains occupied. Reuse existing configuration and Node opening; add no runtime
API, retry, sleep, dependency, arbitrary port or relaxed assertion. This proves
the fixture's port-allocation weakness, not the unavailable cause of the initial
failure. The catalogue batch still requires final verification and two reviews.

Final catalogue proof after that bounded fixture change: each supported compiler
passes353 library plus21 external cases and all nine compile denials, in debug
and optimized profiles. Each configuration lists and runs the same374 names;
all615 initial/final source hashes match. This includes14 actual private native
cases and one pure diagnostic case per configuration, not15 network journeys.
NODE-PRIVATE-CATALOGUE-FINAL-{CURRENT,MINIMUM}-ROWS and FINAL-VERIFIED bind all
eight log hashes and1496 outer/36 denial executions. Preserve the original
failed minimum optimized receipt; its historical startup cause is unknown.
All-feature/all-target bombay-rs strict lint passes7.758 seconds. Isolated protected
transport consumers remain separate, unported callers; no whole-repository,
all-features-runtime or complete SDK-hiding result is inferred.

Both source reviews recover all four moved files byte-for-byte after reversing
only the nine actual connection captures, callback arity and additional close
observations. Existing external refusal/pressure actions remain exact; the
constructor's connection-ID observation now belongs to the private receiver
journey, with external lifecycle observations retained. The port probe's actual
bind failure/automatic success and final source also pass both reviews.

Aggregate checkpoint: Node fields and Driver7/settlement3/action2/terminal24,
their current owned values, runtime transition branches/modules and public
spellings are unchanged. No new state, type, bound, fixture, dependency, history,
repeated cause, false cardinality, nested authority, semantic boolean or caller
structural path appears. Four native modules move to their owner, net module0.
Runtime production/public API +0/-0; Rust tests +76/-38/net38 with four detected
renames (+3487/-3449 without rename detection). Cross-check actual Node/Export,
Driver, ApplicationOutcome, request-key/receive cancellation and DX01/20/23/25.
Disposition: pass this catalogue adoption; remaining SDK surfaces and final
programme gates stay open.
Both independent final evidence reviews accept all eight raw log hashes, exact
test/denial selections and615 frozen sources. Only this requirement evidence
prose is added afterward; the tested Rust is unchanged. Existing generic
retirement/receiver-state and first-sender inverse limits remain as recorded.

Full c6590d7 Nix check exits1 after4761.443 seconds:23 of24 gates pass;
protected-counter alone fails the isolated manifest's formatting check. Its
actual live requester cases already pass both profiles. All609 source hashes,
HEAD and clean status are conserved; NODE-INTEGRATED-FULL-NIX-AUDIT binds the
24 declared outcomes, raw log hash and23 valid store paths. No signing or disk
failure occurred. Normalize that one split join statement (+1/-2 test lines);
runtime production, public surface and every action/assertion stay unchanged.
The full check remains failed until fresh verification; do not transfer its
23 passes to the newer SDK-callback candidate.
Both independent source/evidence reviews confirm the exact whitespace-only
repair and bounded23/1 result. Fresh selected protected-counter verification
passes atb4a5e9e in424.968 seconds with609 unchanged source hashes and clean
status: base53/public6/each hosting1/native Query3/live requester2 cases per
profile, then isolated formatting and both strict lint commands. Grep echoes
are not additional executions. Both evidence reviews pass. This is one repaired
gate; the earlier full-flake result remains23/1, with no24-check rerun or credit
for the newer actor-only candidate. NODE-INTEGRATED-PROTECTED-FORMAT-NIX binds
the actual command, source maps and raw log hash.

Protected-requester ownership, pre-edit DX01/20/23/25: the isolated consumer
still fails20 diagnostics, primarily the removed two-argument callbacks with
inference fallout (NODE-PROTECTED-CALLBACKS-PRE-EDIT-RED). Reuse the proven Node
test ownership: actual SDK query/reply work moves under private Node tests,
with the same receiver/caller connections captured before actor-only Work.
No SDK accessor, alternate callback, dummy export, new session, dependency or
feature marker is allowed. Preserve every native effect/original/receipt and
complete requester/receiver retirement observation before further migration.

Share the existing Requester actor and its finite protocol from one test source,
plus the original complete owned-retirement oracle. Parent scope needs those
plain fields, variants and the actor's existing generated send product at the
same test-parent visibility. Do not change the actor transition bodies or
protocol equations. The isolated root's global allocator and53 base cases stay
isolated; do not import that whole root into library tests. The Requester keeps
its original five states/five inputs/four service alternatives/one error;
meaning8/status7/refusal14/purpose7/operation3/permission2 also stay unchanged.
Node fields, Driver7/settlement3/action2/terminal24 and all runtime branches,
modules and public spellings remain unchanged. Expected runtime/API delta0;
two shared test modules replace their inline source and the native test moves.
Cross-check actual Node/Export/Requester, complete owned retirement and the
finite provider scope; first prove both real provider modes before adopting
the remaining external public/hosting callers. Gate selectors must follow the
owning library's already-remote test gate, without widening the isolated vendor.

Extraction checkpoint: both focused private journeys pass current/minimum debug.
The13 original pure-test calls invoke the facade's existing inherent hook,
including its measured allocation behavior. The facade inserts MailAddr into
that hook (macros/lib.rs193–200); Behavior owns transition, not receive. Initial
private-hook E0624 vetoes and a rejected UFCS assumption (E0782) remain in raw
receipts, without acceptance credit. Preserve the hook's existing test-owner
access with pub(super), just like the extracted plain fields and Sends. This is
confined to test parents, with no external/runtime capability or new operation;
keep its body and original13 calls unchanged. A lifecycle wrapper would change
the measured operation; include! would disperse dependency ownership. Independent
source review confirms this bounded ownership decision. Remove only the root
settlement import now owned by the extracted oracle, then repeat both profiles.

The bounded extraction passes the exact original53 isolated tests plus both
private native Requester journeys in current/minimum debug/optimized profiles
(220 executions,617 frozen source hashes per compiler). Independent name audit
compares the isolated set with the original gate, deduplicating its printed
line echo. This proves the actor-only adoption, with the previously identified
close-observation ordering limitation still requiring correction.

Retained observer correction, pre-edit: move the same captured caller connection
through the existing receiver Work product, then inspect both connections only
after query undeclaration/drain, both native retirements/notifications and outer
close have been acquired. Preserve the original operation order and owned values;
introduce no status flag, replacement handle, wrapper or runtime API. Both source
reviews identify and accept this acquire-before-observe correction. A temporary
owning Node::close omission must fail the final observer, then byte-exact owner
restoration must pass; do not infer a separate caller-site inversion.

Remaining protected ordinary callers, mechanical adoption: retain the original
six public Node tests and both hosting journeys. The first public test's SDK-ZID
borrow observation is explicitly superseded by a caller-owned borrowed Cell:
entry observes0; permission release resumes the same Work and changes that
original owner to1. Retain the pending-wait observation, all original payload
allocation/refusal/permission checks, actual reply0 and complete retirement.
Existing current paired borrow tests already prove this actor-only contract.
Remove only unused Session callback parameters elsewhere. Hosting's two SDK IDs
were discarded without assertions; remove that unused product plumbing, keeping
all foreign/hosted recipient, original payload and independent retirement checks.
No new annotation, bound, callback family or production type is allowed.

Move the exact nonempty Requester gate selections to the existing remote library
debug/release gates, which already run those owners. Keep all isolated base,
public, hosting and native Query gate commands intact. Do not expand isolated
vendor inputs or dependencies. Runtime state/branches/public surface remain0;
new test ownership contributes two shared modules, without nested authority.

Lint adoption checkpoint: the owning package adds pedantic checks absent from the
isolated crate. Six style diagnostics concern existing domain names and the
existing borrowed validation signature. Both independent source reviews accept
function-local expectations only: live_read preserves native receiver/received
values and projection/protection distinctions; current_scope preserves the
original finite fixture's borrowed signature. It is not a JSON callback port.
No name, body, argument, effect or runtime/API shape changes. Initial strict lint
failure remains recorded. Renew final source/profile checks after these metadata
edits; the earlier617-source matrix and close inverses retain their byte scope.

Final metadata matrix limitation: minimum1.96 completes all rows, but current1.99
optimized protected run parks in the missing-host retry test after65 other passes
and one existing ignore. After252.881 seconds and a native process sample, that
specific test process is terminated (SIGTERM/exit101); the run is failed, never
waived by its same-binary isolated pass. All617 source hashes match the completed
minimum matrix before termination. The sample locates the second block_on but
cannot select execution, receiving or connection-close await. Preserve the raw
failure/sample and investigate with temporary test-driver tracing around those
existing awaits. No fold effect, synchronization, timeout policy, test
serialization, automatic retry or production API is justified by this evidence.
Tracing changes only diagnostics and must be byte-exact restored; successful
traced runs cannot explain away the original hang or establish final acceptance.

Handoff disposition: preserve this coherent actor-only consumer migration as a
reviewable DevX milestone, while the selected programme and final gates remain
open. Current/minimum debug/optimized pre-metadata matrix passes272 actual
executions plus four existing ignores with617 frozen sources; exact final
metadata source passes minimum both profiles and current debug, while the
current optimized protected suite remains the explicit252.881-second failed
hang described above. Its private Requester pair passes optimized. Strict
all-feature/all-target bombay-rs lint, both isolated lint configurations, both
formatting checks, four callback compile fixtures (one pass/three denials) and
five Node runtime cases pass. Two independent source/evidence reviews preserve
all original actions and ownership checks. Actual close omission fails both
Requester modes in debug/optimized; byte-exact restoration passes. A detached
Cell counterexample fails the independent caller-owner assertion; restoration
passes. No separate caller-close inverse or stronger settlement/receiver-state
oracle is claimed.

All temporary tracing is removed byte-exact; diagnostic original/traced binaries
and source/build/sample hashes remain outside the repository. Bounded diagnostic
trials do not resolve the original hang and receive no acceptance credit. Source
scan retains actor-only business callbacks and intentional compile denials.
Runtime production/public API delta0 for this batch; test source +1285/-1236/net49,
two shared test modules and one native-module move. Requester5/input5/service4,
Node fields, Driver7/settlement3/action2/terminal24 and their owned current values
remain unchanged; no new runtime branch/state, history, repeated cause, false
cardinality, nested authority, semantic boolean or structural caller path.
Cross-check Node/Export, actual facade hook, complete retirement and finite
provider laws with DX01/20/23/25. Disposition: retain the mechanical migration
for review; final integrated acceptance is open because of the unresolved hang.
Raw configuration/export Query custody, automatic timer ownership and remaining
selected criteria are not closed. No full24-gate result is claimed for this head.

Supervision/pool caller refinement, pre-edit DX02/10/11/17/20: current public
examples still name Status/Capability/CustomerProtocol and repeat those types in
topology hosts. Runtime retirement/composed actor aliases have already gone.
The source-backed consumer probe removes those declarations while preserving
actual replacement, FIFO admission/completion, shutdown and retirement work.
Both ordinary callers use exact established reply endpoints. Required law: an
exact external recipient must not demand an unused logical namespace; genuine
logical replies still require a static domain binding. Classification: derived
capability composition, not a new actor-model guarantee.

Inspect selected registry0.26.0 ReplyRoute, ReplyDeliveries, CustomerDelivery,
FixedCommand/FifoCommand, request LogicalProtocols and Bombay Hosts/interpreter
paths before selecting a correction. Their combined reply sums currently
advertise logical capability independent of the chosen exact value; this is a
source lead, not yet an executed fault or permission for a runtime fallback.
Reuse existing DeliveryRoute implementations for exact/logical targets and
complete customer rejection custody. Preserve mixed-routing use cases and
original rejected jobs/routes; no no-op host, type erasure, arbitrary bound,
example-only convenience helper or unconditional logical-capability widening
may repair the probe. Separate the worker startup associated-type question and
policy Config delivery from this capability proof. Initial budget: two copied
actual consumer mains/worker modules outside the repository and this record,
production/API/control states delta0. No owning abstraction selected yet.

Executed source-backed REDs: FIFO fails three missing-Hosts E0277 diagnostics;
supervision fails six for status/capability replies. Neither fails inference.
Both independent reviews confirm the four original source hashes and unchanged
worker bodies, work actions, replacement, FIFO results, shutdown and retirement
handling. These printed examples are construction witnesses, not complete
failure/retirement oracles. Metadata is truthful for today's broad ReplyRoute;
removing it would hide real logical capability. Narrowing must survive command,
storage and sends; independent supervisor query generics must not require an
unconstrained Status route for capability-only work. Production/API/state delta0.


Current owner integration: exact reply endpoints are inferred for ordinary
queue/roster construction. Logical routing opts in independently per operation;
four genuine host combinations compile and each missing selected host remains
denied. The discarded combined query choice required an unused host. Startup
uses with_startup(worker, move || async move {...}) through existing StartupPlan;
no worker alias or named plan is required. Measured stored plans16bytes named,
32anonymous future,16deferred closure; inline pools256bytes each. These unpublished
owner changes use explicit three-owner path patches in native archive consumers:
16focused owner tests and7native tests pass both profiles/current and minimum
compilers. Frozen730-file evidence, full failure-path limits, migration and costs
remain in the canonical Behavior PRD. This is not registry or combined-source
acceptance; old Stash/shutdown layouts do not execute inner shutdown and receive
no genuine two-policy credit.

Independent rejected-reply correction184354b (draft Behavior PR101) changes only
existing source custody (+33 production lines, public API0) with6regressions.
All970 nextest tests, ten native Nix gates, four-workspace release preflight,
packaged consumer and all14exact-head GitHub checks pass. Unmodified6c5c18a with
all59 original registry selections preserves complete logical Status/Capability
rejections beside normal retirement; locked debug/release pass, original owner
fails retained rows1 versus3. This proves neither per-action row partition nor
new panic/cancellation laws. Merge/publication authorization is pending.

The retained public regression crates/bombay/tests/policies.rs exercises genuine
Stash and OneShot policies in both fluent orders with registry Behavior/Actors
0.26.0, no patch. Raw closed-worker rejection controls the business law. Both
original boxed invoices, origins, FIFO balances, original destination, once-only
reminder, every Actions lane and terminal Stop are checked. Two independent
reviews pass after tying the destination oracle to input MailAddr59. Real
Bombay debug/release pass all3tests (13.820/33.352seconds); final lint-only edits
merge equivalent route arms and explain the complete-trace function's length.
Final debug/release pass (1.161/1.187seconds), strict Clippy passes (0.787seconds),
formatting passes (0.631seconds). Isolated exact owner inversions dropping timer
Actions or reversing FIFO each fail both orders while the raw control passes;
restored702source hashes and forced recompilation pass all3. One stale mutated
Cargo artifact after preserved-mtime restoration stayed red and is not passing
evidence. Production/public API0; tests+266/-0 in onefile. Empty inner init,
native scheduling acceptance and joint startup remain unproven by this fixture.
The canonical Behavior PRD retains full commands, source/log bindings and drift.


Latest-main provider integration (DX20/23/25), pre-edit law: the recorded
DEVX-LATEST-PROVIDER-MERGE-COMPILE-RED fails with13 errors: requests.rs imports
an allocator from the isolated owner, uses the old two-input callback and old
three-part result. Selected registry contracts are Behavior/Actors0.26.0,
Address0.3.0, Communication0.1.5 and Timers0.2.0. This test-ownership correction
changes no actor-model law or runtime capability. Reuse Node's existing private
connection clone, actor-only callback, complete four-part ApplicationOutcome and
existing Requester Scope; no Session reaches an application callback.

Sharing-cost law (derived ownership measurement): verification precedes four
Rc::new windows on original concrete Receipts. Each measures one alloc, no
realloc, at least size_of Receipt bytes and equal size across four cases. A clone
of the first Rc measures zero alloc/realloc/bytes. The same complete recorded/
scheduled provider source is loaded by private Node and isolated allocator
owners; Receipt fields remain private with the original projections. No SDK
operation, JSON decoding, verification or Export lookup enters a cost window.
The isolated parent alone expects unused protection/reply code because the
complete native family executes in the Node owner. These measurements do not
inspect the identical SDK-owned process objects or total native memory.

Native pending/protection/replay, actual Export-owned sharing, original buffers,
claims/proof pointers, reference counts, duplicate/conflict custody and complete
cleanup assertions remain. New connection.is_closed observations check actual
Node close authority. Current Clippy changes only assertion spelling, pointer
method references and the local reply_proof name. Both source reviews conserve
all original unaffected function bodies and native actions/assertions. Remote
gates explicitly require all six original cases; featured isolated gates require
the two sharing names and2/0failed/0ignored in both profiles, replacing an obsolete
filter that selected no tests.

Frozen593-file proof passes six native cases plus two sharing cases on Rust1.99
and1.96, debug/release (32 executions). Four measurement calibrations fail both
sharing cases: omitted Rc::new window and added live Box in the clone window,
each profile. Node.close replaced by Ok fails both native cases at the new
closed assertion in each profile. Byte restoration plus debug/release reruns
pass16 further executions. The stronger post-run audit independently requires
all18 command keys, canonical exact case names, exactly-once results, zero ignored tests,
key-derived exit status and each failed case's own panic header at the
source-bound primitive assertion.
A first global-substring guard could not attribute both failures; it grants no
credit independently. All593 original/restored source hashes match. Variant
hashes are reconstructed from frozen originals/immutable driver, not sampled
per-run inventories. Compiler versions are observed post-run. Raw argv/times,
source/log hashes and scope are in provider-sharing/{final-sources,results,
verified,verified-modes}.json. Final strict remote all-target/isolated Clippy and
both formatting owners pass (38.551seconds). Measured Rc blocks are376bytes recorded/392scheduled;
verification and transport memory are outside this cost claim.

A fresh direct optimized featured suite passes68tests/one controlled TLS-child
ignore; that child is exercised only by its controlled parent fixture. The exact
missing-host retry passes debug; neither success explains the earlier optimized
hang. Full combined gates and registry owner inference delivery remain open.
Incoming main76dbd84 selects Communication0.1.6; its published archive checksum,
VCS963067e and all21 packaged files match. Existing send_map is unchanged;
capacity observation reserves no slot or permission, while Consumer retains the
original through notification panic. Its integration and both complete graph
comparisons follow this frozen0.1.5 checkpoint.
The version-only merge expects Cargo.toml, both Cargo.lock files and upstream
documentation; every Rust file must equal cd77c8b. Production/API/state delta0.
Derived law: notification failure retains the original queued payload; capacity
observation is neither reservation nor authority. Ordinary Application syntax
and complete outcomes stay unchanged. Reuse the existing mailbox, Driver and
Node contracts; verify exact399/324 neighbor conservation and all24 native Nix
checks after merging. No owner or interpreter abstraction is added.

Final drift: production+0/-0, public types/spellings+0/-0; tests+551/-389/net162 in
five files; gates+13/-3/net10 in one file. Seven files including this record.
Node still owns Some(Application)|None and an independently owned connection;
provider sums remain four enums/nine alternatives, with unchanged products.
No Behavior control state, transition branch, production module, cause, history,
semantic boolean, nested actor authority or structural application syntax changes.
Two test source files add three parent module declarations; one common cost
equation has two actual provider substitutions. Cross-check Node-private close,
Driver custody and authoritative-fact conservation. Both independent evidence
reviews pass. Disposition: pass for this test-only batch; no programme completion or newer-main acceptance is claimed.
