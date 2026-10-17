# Developer experience coordination

Status: active. The
[owning Behavior PRD](https://github.com/devrandom-labs/bombay-behavior/blob/codex/devx-delivery/docs/engineering/developer-experience-prd.md)
contains the single DX01–DX28 checklist, current contracts, verification results
and explicit deferrals. Its evidence appendix preserves the initial inventory.
Current source and executable evidence outrank stale prose. Earlier experiments
and their bounded results remain available at commit4fcf68d and in the owning
PRD; they are not passing certificates for the current dependency graph.

## Selected path

Use the existing actor facade for domain actors, ActorExt for same-actor
policies, owning constructors for standalone policies, named Actions lanes and
one Application across ordinary execution, supervision, pools, HTTP and Entity.
One explicit application topology declaration is user-authorized; it derives
spaces, occurrences and complete recursive custody. Real policy and complete
failure handling remain explicit. Advanced users can peel back one layer at a
time without ordinary callers assembling the lower-level machinery.

Bombay owns application assembly, hosting, clients and terminal consumption.
Behavior owns the pure static transition/effect algebra; Actors owns reusable
policies. Preserve affine ownership, fresh creation/provenance, accepted effect
prefixes, initialization order, lifecycle authority and cancellation custody.
Compiler friction cannot invent bounds, unused callbacks or structural caller
work. Networking, durability, identity and deployment products remain separate;
Zenoh stays internal and the remaining SDK configuration/query surface is an
open interface obligation, not a transport extension port.

## Current delivery

The isolated branch is `codex/devx-stash-limits`. Main49cc897 is integrated;
a fresh fetch found no newer main. Other AI worktrees remain untouched. Behavior
PR113/114 and reviewed release PR115 are merged. Normal publication delivered
Behavior/Actors0.29.0 and macros0.16.0 from55dbac2. Registry checksums, clean
archive provenance, tags and releases agree; all228 shipped Rust files match
the tested candidate. The release passed14 GitHub checks,11 native Nix gates,
1,035 owner tests,123 doctests and four-workspace package preflight. These owner
results do not substitute for the remaining Bombay gates.

All three tracked root, protected-consumer and Engine-fuzz locks select the
published owners without Behavior patches. The root preserves all396 complete
unrelated package records, including the original four Windows-only dependency
edges. Both compilers accept locked all-feature Windows metadata; the354-node
native all-feature graph is unchanged after normalizing workspace paths. This
is dependency conservation evidence, not Windows execution. Driver inventories
bind the published source, preserving eight law rows,24 historical revisions
and44 unrelated template rows. All11 inventory checks pass. Engine fuzz uses
the same Behavior protocol owner as Driver; no Rust bounds or source changes
were needed to align that dependency.

`application.shutdown()` is the canonical short operation on ApplicationHandle
and ApplicationLifecycle. It preserves `Result<(), ShutdownRejection>`, inferred
lane and static root authority. Admission is distinct from termination and
joined retirement; no compatibility alias is retained. InstalledActor keeps its
separate lower-level operation. The pre-edit caller fails exactly two E0599
errors, while its old spelling passes. Three native shutdown journeys pass
both supported compilers in debug/release. The mechanical rename preserves
original assertions and complete wrapper-order effects/retirement.

Finite holding uses `actor.with_stash(limit, refused, route)`. Overflow returns
the complete original input and sender through Actions. Explicit UnlimitedStash
serves advanced unbounded/Never callers; `stash.take()` returns the still-held
FIFO without consuming its actor or route. The23-reference caller migration keeps
all255 original assertions. Recorded accepted-refusal, closed-recipient and
replay-selector-panic journeys preserve original allocations, still-owned queue
contents, cleanup and notifications. They do not promise rollback of inputs
already delegated to an actor. Earlier source-selected results keep their
original version/compiler/profile limits.

The user authorizes153 cumulative native paths: the previous148-path dependency,
shutdown and verifier batch plus the Engine/macros manifests and three
changelogs. The README teaches
shutdown(); the existing immutable verifier now covers Bombay, Engine and
Observe. Its136 existing cases,32 suites,28 targets, exact fixture modes and
controlled child remain mandatory. Two reviewed compiler-specific expectation
hunks preserve Engine's nine E0599 denials and Observe's E0382 move denial.
No invalid Rust fixture, runtime behavior or public type changes. A further
remote snapshot correction removes exactly eight stale underline columns on
both compilers, preserving the complete E0499 borrow denial. That snapshot path
was already included in the authorized rename set.

## Verification and remaining work

The user authorized186 cumulative source paths for DX04/24 report retention.
`outcome.root_report()` and `retirement.root_report()` inspect the existing
runtime-issued root/subtree report after Work returns its business value. The
pre-edit real FIFO archive consumer denied both methods; its control passed.
The complete product now retains work, optional report, native receiving,
notifications and families independently; `into_parts` exposes all five axes,
with the report second. Completed selection transfers all results; rejection
returns the original outcome. Missing publication remains absent.

Existing Observe/cleanup publication and joining own this transfer. Family-only
cleanup has no root observation; cold work allocates none earlier. Production:
+101/-28, net+73 lines in three existing owners; public types added/removed0/0,
two getters. Control sums,74 match arms, modules, effects and actor authority
stay unchanged. The aggregate checkpoint passes without arrival history,
semantic flags or structural caller syntax. All7,639 original assertion sites
survive; six native regressions and two product laws add55. All other caller
changes insert the report slot; three fixture hashes change, no expected
diagnostic bytes. Two test reviews and an independent report-erasure inversion
prove failure detection: five preservation regressions fail in each profile,
while the cold/absent control passes.

Actual owner debug/release each pass344 library plus19 outcome tests. Both
wrapper orders, startup refusal, cancellation and real notification panic
preserve the independent native custody. Isolated native1.96 debug/release and
real pool/supervisor debug/release journeys pass. Workspace/Axum all-target
checks, strict Clippy and both immutable compiler corpora pass (136 cases,
32 suites,28 targets,633 unchanged source inputs). In the same-source FIFO
measurement on both compilers, preservation adds8bytes peak heap, no allocations
and no retained heap; System RSS remains noisy. These scoped results do not
replace fresh full-gate verification. Actual cleanb4b21f9 archives verify on both
compilers, with all82 production Rust files and exact dependency checksums.
Their consumers pass32 native cases per debug/release profile, three renamed
tests and six running examples; all501 archive files/284 caller Rust files stay
exact. The isolated host's HTTP feature-forwarding/count-classifier failures
remain failed. Its HTTP example compiles; native HTTP lifecycle/refusal cases run.

First Linux CI exposed two missed `matches!` tuple patterns in the already
approved protected transport source and nested formatting. The correction adds
only the report slot to those two patterns; removing the slots restores every
original token apart from whitespace in all five affected files. No original
assertion, production code, public surface or dependency changes. The default
workspace formatter does not cover this separately rooted target. Its explicit
format check and protected all-target strict Clippy pass. The first native full
run is interrupted at707.143s with eight valid retained outputs; it is not a full
pass. The user then authorizes the186-path scope for the pool/supervision entrypoints.
Four existing examples inspect the report after execution; Work returns only the
business result or unit. No example callback now waits for retirement solely to inspect the report.
The existing `run_with` join also covers natural timeout completion: a focused
consumer proves native Stopped completion and the original completed business
state in both profiles without an extra wait. Domain policies, assignments,
replacement and phased shutdown stay unchanged. The example source delta is
+20/-9 lines, with no new runtime or public surface. The frozenf0fdae3 full run is interrupted at593.909s to include this
approved example migration in one final source freeze; it is not a full pass.
Final native and Linux gates remain required. No merging or publication is
authorized for this batch.

The owning PRD records exact commands, source identities, failures and limits.
The initial actual-registry immutable128-case runs pass both compilers with all
633 inputs conserved. The prepared136-case verifier passes both compilers with
the same conservation checks. The minimum default/Axum workspace passes791
outer cases with two existing parent-invoked child skips; remote coverage is
separate. Bounded sanitizer campaigns pass2,048 Driver cases and1,024 cases for
each of four Observe targets. Coverage floors remain unchanged: Driver90%,
actor execution93%, actor outcome90%, Observe90%.

All25 native Nix gates pass on cleaneadea00 in3459.500s, with all633 inputs
unchanged and every output's deriver/content hash checked. The workspace passes791
outer cases/136 UI fixtures; remote debug/release each pass426/20 UI. Protected
consumers pass132 outer cases; parents verify two deliberate failed child controls
and four zero-match controls. Both minimum remote profiles and the minimum
workspace pass their corresponding matrices. These results precede the release
version correction below and are not relabelled as its fresh gate results.

The superseded native full run was interrupted after21 of25 outputs registered:
both compilers independently reproduced the stale remote E0499 underline.
Its logs, exact source hashes and individual outputs are preserved; it is not
a passing full gate. The corrected isolated minimum remote matrix passes.
The corrected frozen source passes both immutable compiler replays and all25
native gates. No signing or disk failure is
permanently waived. The older optimized original-work-retry hang remains
unexplained; later passing runs do not resolve its cause.

Actual runtime-only package verification fails on eadea00: registry Engine0.2.3
uses Behavior0.25 while this runtime uses0.29. An unchanged historical Engine
consumer proves that labelling current Engine0.2.4 would break existing callers.
An unchanged published-runtime topology consumer separately proves that current
macros labelled0.1.4 generate unavailable Runtime/NativeHost contracts. Therefore
the approved release correction coordinates Engine0.3.0, macros0.2.0 and
runtime0.2.0 across ten existing files, including exact nested locks. It changes
no Rust source, public types or unrelated lock records. Both source/oracle
reviews preserve the historical passing consumers and intended failing
compatibility inversions. All-feature three-package preview verification passes;
all seven examples and the renamed consumer compile against its extracted
archives, and six terminating local examples execute. At41812fce all25 native
Nix gates pass in3185.671s with all633 inputs unchanged. The workspace passes791
outer cases/136 UI; remote debug/release each pass426/20 UI. Protected consumers
pass132 outer cases, with two deliberate failed children and four zero-match
controls checked separately. Actual three-package/all-feature archives and24
native cases per profile pass both supported compilers; all501 archive files and
19 caller Rust files remain exact. Linux CI and release review remain separate
obligations. Eight successful Nix outputs were unavailable during initial hash
inspection; exact re-realization passes in584.829s, and all25 outputs now have
their expected derivers/content hashes verified and local GC roots retained.
The preview and source-scope authorization do not authorize merging or publication.

The guide links its three former partial ignored topology copies to complete
compiled examples/tests. Both supported compiler previews retain all six passing
doctests and have zero ignored cases; no Rust fixture or assertion is removed.
Actual native all-feature doctests also pass six cases with zero ignores.
An isolated optimized
two-actor FIFO journey measures mailbox minimum1 (effective2) versus1024 at
equal work: peak heap142770 versus396226bytes, with complete retirement,
cleanup and notifications. Joined heap is10768bytes for both. The remaining
56bytes after host shutdown trace to Tokio's thread-local parker. Source-specific
build targets, allocator inversion, calibrated child RSS and rejected-status
controls replace the superseded shared-cache samples. These are bounded buffer
capacity costs, not whole-framework performance claims; the owning PRD retains
the exact measurements and limits.

Use the existing immutable verifier inside pinned Nix, with fresh empty output
directories; verification never overwrites expected diagnostics:

```sh
nix develop -c python3 -B tools/diagnostics_test.py
nix develop -c python3 tools/diagnostics.py --output /tmp/bombay-diagnostics-current
nix develop -c env RUSTC="$MSRV_BIN/rustc" RUSTDOC="$MSRV_BIN/rustdoc" \
  python3 tools/diagnostics.py --cargo "$MSRV_BIN/cargo" \
  --output /tmp/bombay-diagnostics-minimum
```

MSRV_BIN names the pinned1.96 toolchain; compiler identities are checked.
Rust/Cargo/example/fmt/lint commands always run inside pinned Nix. Complete
resource accounting, proportional failure inspection, Machine/deferred custody,
SDK privacy, comparable memory/compile costs and final documentation acceptance
remain in the owning checklist. Partial receipt counts, normal retirement and
readiness notices do not establish complete affine custody or committed Ready.
The programme is not complete merely because examples or selected checks pass.
