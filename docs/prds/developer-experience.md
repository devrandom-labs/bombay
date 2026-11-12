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

The isolated branch is `codex/retirement-recovery`, based on integrated
main3bf270cb (PR412). The retained recovery port and formatting correction are
local commits; source361d7728 passes all28 host Nix checks. CI, independent
review and delivery remain open. Other AI worktrees remain untouched. The
runtime still selects registry Behavior/Actors0.29.0 and macros0.16.0; separate
0.30.0 adoption stays with Bombay Codex until reconciled. Behavior0.30.0 is
published and the release notice was acknowledged; the owning PRD holds those
publication receipts. The earlier PR113/114 and reviewed release PR115 merged
and normal publication delivered
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

### Owned retirement recovery, before implementation (2026-11-11)

Resume Stage1/DX04 against actual main2b7acf1266968e87595fae568bd0d2dc053011c2.
The selected contracts are registry Behavior/Actors0.29.0, Macros0.16.0,
Address0.3.0, Communication0.1.6 and Timers0.2.0, plus this revision's Engine
and Observe. No Behavior patches are selected. The selected Behavior instruction
revision is55dbac2d1068b29907498b3a7fa32b3080e0cd30. Source, documented teardown,
publication and affine-return contracts are inspected before the experiment.
Bombay Codex owns the separate030 registry adoption after its PR412 gate.

The derived custody law is D-RETIRE-1: consuming an available actor retirement
must move its original actor, queues, descendants and failures together with
the exact primary outcome and phase-specific unfinished operations. Selecting
custody does not certify successful execution or cleanup. A non-custody case
returns the entire original retirement. No actor clone, default replacement,
fabricated report, lifecycle authority or disposal policy is supplied by the
library. Application work, root report, receiving, notifications and families
remain independently owned. This changes result access, not actor semantics;
the concrete interface is Bombay policy rather than an Agha guarantee.

First establish a source-selected native control with the existing phase match:
completed work beside a genuine behavior failure, and an actual shutdown wrapper
returning a whole non-Clone/non-Default actor. Then write the consuming caller
syntax and capture its failure before any production edit. The existing
ActorCustody product already owns14 coexisting fields in20 of24 terminal cases;
an extra mutable getter cannot by itself move the whole wrapped actor out.
The first control has two fixture-only E0277 errors: ProjectTerminal owns a
vector of descendant-schema failures; the leaf's schema failure is Never.
Keep the required absence of child failures and check the existing vector's
emptiness. No production bound, type or constructor changes to repair this.
The overflow control then passes; orderly shutdown rejects the fixture's
empty-settlements assumption. D-TERM-1 retains final Actions beside Stop, and
the existing native consumer pins one terminal product. Require exactly that
one accepted settlement, as application_outcome's current control does, rather
than discarding it. The failed source/logs are preserved; no production fix or
weakened original test is involved.
Allocation rejection and three executor cases retain their current distinct
ownership. No new representation or public type is retained at this point.

Both corrected native controls pass1.99 debug and optimized against the exact
selected graph, preserving actor/command/output allocations, all custody lanes,
original report, cleanup, notifications and the real shutdown policy. The next
pre-edit caller asks for `retirement.try_into_custody()`: success transfers
`(ActorCustody, RetirementOutcome)`; lack of native custody returns the complete
original retirement. The outcome must own every original primary cause and
phase-specific unfinished operation, so extracting the actor never requires a
clone, a replacement, or dropping its cause. The proposed protocol name is not
an accepted representation: trace all20 custody-bearing cases before defining
its complete sum, and reject a duplicate or untruthful phase model.

The static-regression capture changes no production state: ActorRetirement has
24 alternatives,20 own the14-field custody product, and four lack that product.
Driver transitions and all current generic bounds remain unchanged. The two
candidate production owners currently contain3093 physical lines and terminal.rs
has90 `=>` spellings and12 public type/trait declarations. No production edit
may precede the complete new-symbol/protocol/aggregate checkpoint. Forecast for
the later focused implementation is at most120 net production lines, one public
protocol type, terminal.rs/lib.rs plus its focused native test, this PRD and the
existing backlog row; the owning Behavior PRD remains the sole requirement
checklist. With those five new native paths, the resumed cross-repository task
would have30 unique paths. The user's2026-11-11 standing scope approval applies;
it does not waive the semantic, ownership, review or gate requirements. Bulk
caller migration is a separate stage after the focused law and interpreter path.

The six-case consuming caller is now written before production and fails with
exactly one E0432 (missing RetirementOutcome) and six E0599 errors (missing
try_into_custody), exit101. It reuses both native controls without reducing their
assertions, and adds allocation refusal with both actual reasons, unreturned
panic/cancellation and a real executor JoinError whose task identity survives.
Receipts: `retirement-recovery-before.log/.exit` in the existing restart evidence.
The old phase-match control remains independently executable and passes both
profiles. These tests establish the absent ownership port, not completeness of
a new retirement representation. No production symbol or caller migration is
retained until its full phase/custody map and checkpoint are recorded.

Two author test reviews accompany this pre-edit capture. The first maps actor,
command and business allocations, every custody lane, report and notification
to the ownership law; the stopped control also requires the accepted final
Actions product. The second challenges a success-only selector, failure
reclassification, terminal-action loss and fabricated state on the four
non-custody alternatives. No independent reviewer is claimed. Nonempty partial
interpretations, descendant failures and family disposal remain unproved by
these focused cases; preserve their existing tests and qualify that coverage
before crediting complete recovery or whole DX04 acceptance.

Rust's ownership/enumeration rules support moving existing products without
copying payloads; mem::replace still requires a real replacement value:
<https://doc.rust-lang.org/book/ch04-01-what-is-ownership.html>,
<https://doc.rust-lang.org/book/ch06-01-defining-an-enum.html>,
<https://doc.rust-lang.org/std/mem/fn.replace.html>.
The focused caller remains outside production under the existing restart
evidence directory. Its exact failing regression is captured. The following
pre-edit model permits only the focused design experiment; catalogue migration
and acceptance remain separate. Existing tests and expected values stay intact.

#### Recovery representation and provenance, before production

The existing terminal sum has24 alternatives,20 with the same ActorCustody
product. Factor those20 into `ActorRetirement::Retained { custody, outcome }`,
where `RetirementOutcome<B>` owns their existing primary cause and unfinished
operations. Keep AllocationRejected, ActorTaskFailed, Panicked and Cancelled
unchanged. This is the distributive law for a sum of products with one shared
factor:20 logical retained outcomes plus four unavailable cases remain24.
It is a passive returned protocol, not a new actor or transition authority.

`try_into_custody(self)` moves the existing `(custody, outcome)` product on
Retained and returns `Err(self)` unchanged on the other four cases. Availability
is independent of execution success. The two native pre-edit regressions require
the same consuming operation for actual overflow and orderly wrapped shutdown;
the four other tests require exact rejection. These six failures precede every
new production symbol. No clone, replacement, new runtime operation, trait,
constructor, policy input, associated type or generic default is needed.

Complete outcome map below. `active` means the already-retired live residual;
`prepared` means before initialization effects; `uncommitted` retains those
initialization effects before host binding. P denotes the unchanged optional
interpretation/source progress plus ordered settlement vector. All rows also
retain the exact existing14-field custody, including malformed simultaneous
received inputs/replies, cleanup failures and descendant failures.

| Outcome | Existing producer and residual | Other retained values |
| --- | --- | --- |
| InitializationRejected | BehaviorFailed/prepared; startup SpawnError | original behavior error |
| InitializationPanicked | InitializationPanicked/prepared; startup SpawnError | original panic payload |
| HostCommitPanicked | ActivationPanicked or HostCommitPanicked rejection, prepared/uncommitted | original payload and optional initialization progress |
| TransitionPanicked | TransitionPanicked/active | original payload and P |
| HostExecutionPanicked | HostExecutionPanicked/active | original payload and P |
| HostRejected | ActivationFailed/uncommitted; startup SpawnError | original ClaimError and initialization progress |
| BindingAbandoned | ActivationFailed/uncommitted; startup SpawnError | initialization progress |
| Completed | Completed/active with Stopped or Exhausted | exact Completion and P |
| BehaviorFailed | BehaviorFailed/active | original behavior error and P |
| EffectsFailed | SettlementFailed/active or uncommitted | original SettlementFailure, P and optional initialization progress |
| OwnerCancelled | Completed/active with actual acquired owner request | P; no new cancellation authority |
| CapabilityFailed | Completed/active with actual acquired capability failure | original JoinError and P |
| PreparedRetirementPanicked | RetirementPanicked/prepared | original payload |
| PreparedInterpreterContractFailed | InterpreterContractFailed/prepared | no additional payload |
| InitializationRetirementPanicked | RetirementPanicked/uncommitted | original payload and initialization progress |
| InitializationInterpreterContractFailed | InterpreterContractFailed/uncommitted | initialization progress |
| ActiveRetirementPanicked | RetirementPanicked/active | original payload and P |
| ActiveActivationPanicked | ActivationPanicked/active | original payload and P |
| ActiveInterpreterContractFailed | InterpreterContractFailed/active | P |
| PreparedEffectsFailed | SettlementFailed/prepared | original SettlementFailure |

AllocationRejected still owns the original actor and actual allocation reason;
it does not gain fabricated native custody. ActorTaskFailed retains its original
JoinError. Panicked/Cancelled still report no returned actor. No interpretation,
settlement, additional failure or report is recomputed by the consuming method.
Phase names retain operation provenance required to interpret exact unfinished
work; they are not newly stored arrival history.

New-symbol provenance: Retained establishes common custody availability;
RetirementOutcome owns the original complete primary/phase sum after custody
is moved; try_into_custody owns the affine selection law. Existing completion,
behavior and custody inspections retain their contracts and inspect the factored
product. RetirementOutcome's single B parameter is needed for the already
concrete error and interpreter products; the two real native actors exercise it.
Root/ChildFailures remain only on ActorCustody/ActorRetirement. No generic or
visibility change may be introduced from a compiler diagnostic. The canonical
crate export is needed by the pre-edit ordinary consuming caller. No prelude
expansion is required. The result tuple is the complete two-part owned product,
not a new wrapper that restates Result or hides a structural route.

This replaces the20 root alternatives rather than maintaining two competing
copies. The four startup producers in launch.rs must construct the same new
product; they are part of the smallest interpreter witness, not catalogue
migration. This adds that existing path to the forecast:31 cumulative resumed
cross-repository paths under standing scope approval, at most120 net production
lines and one new public type. Engine, Behavior, ActorExecutionOutcome, Driver,
Application's five axes, mailbox draining, Observe and family disposal stay
owned by their existing contracts. No source compatibility shim is retained;
the explicit pattern migration is a later measured stage after both profiles
prove the focused law.

Pre-edit aggregate checkpoint: runtime control states and authority do not
change. Passive terminal alternatives24 become outer5 plus inner20 (25 raw
tags, still24 inhabited logical outcomes); common custody has one owner.
ActorCustody remains one14-field product. The outcome's current values are
exactly the table's primary failures, completion, pending initialization,
interpretation/source progress and accepted/rejected settlements; each is
needed for disposal/recovery and failure inspection after moving the actor.
Direct sum/product factoring expresses that invariant, with no subordinate
dispatcher or lifecycle engine. There is no new cardinality, policy, semantic
boolean, repeated cause, structural caller route or arrival-history field.
Optional acquired lanes keep original malformed evidence rather than inventing
a phase restriction. Cross-checks: D-RETIRE-1, D-TERM-1, authoritative-fact
conservation, Application's independent axes and the selected Behavior's
explicit Actions/custody law. No normalized actor transition is changed.

Reproducible baseline: terminal.rs has2969 physical lines, of which2033 precede
its test module, with56 `=>` spellings,12 public type/trait declarations and11
public methods in that production prefix. Whole-file counts90 arms and2969
lines include tests and are not production measurements. lib.rs has124 lines;
launch.rs has7232 physical lines including tests. All three exact baseline
files/hashes are preserved as retirement-recovery-before-*.rs. These lexical
counts diagnose source growth, not semantic acceptance. Measure the retained
diff, branches, modules and public surface after the experiment. Disposition:
pass for the pre-edit hypothesis only; no retained-batch pass or DX04 completion
is claimed until the focused checks, inversions and full custody coverage pass.

The first strict library Clippy run rejects three diagnostics: the factored
large enum, its complete original Err value, and missing Errors documentation.
Document the exact error contract. Keep the already-proven allocation-free
owned equation and locally explain the two representation lint expectations;
do not box custody, add a generic or change the selection law to follow a
diagnostic. These are explicit lint dispositions, not new architectural surface.
The failed receipt remains retirement-recovery-clippy-lib.log/.exit=101; the
corrected gate needs its own receipt before migration begins.

#### Focused recovery result and migration prerequisite

The six pre-edit consuming cases pass with the new representation on Rust1.99
and1.96, each debug/release. The same six cases now live in the owning integration
test retirement_recovery.rs and pass native1.99 debug/release. Its actor is local
to that test target; it adds no production fixture module or helper API. The
source/oracle review is repeated after moving it: all ownership assertions and
actual native operations are conserved; the same failure and shutdown
counterexamples apply. There is still no claim of an independent reviewer.

Two deliberate source inversions fail exactly their intended native case in
both profiles: accepting only Completed loses the failed overflow actor;
discarding terminal settlements loses the one Accepted shutdown Actions product.
Each run has five controls passing and one intended failure, exit101. Both
inversions are removed and the exact candidate restored; all six pass again in
both profiles. Logs/sources are retirement-recovery-{success-only,terminal-actions}-
inversion-* and retirement-recovery-restored-* under the existing restart evidence.
Minimum receipts are retirement-recovery-minimum-*. The initial restored-current
wrapper had an unset RUSTC variable before executing any test; its corrected
invocation uses the shell's pinned rustc. It receives no earlier test credit.
Strict library Clippy now passes in retirement-recovery-clippy-lib-corrected.

Full producer conservation independently checks22 terminal and four startup
expressions in original order: all14 custody fields remain exact, and every
other original field moves unchanged into its corresponding outcome. Only
whitespace and rustfmt's optional final field comma are normalized. All20
outcome payload declarations remain exact after removing their common custody
field. Existing terminal test source and launch source after startup conversion
remain byte-identical. Receipts are retirement-recovery-source-conservation.json
and retirement-recovery-producer-conservation.json. These source checks support,
but do not replace, running the existing nonempty progress/failure tests.

Focused aggregate checkpoint, disposition `pass`: inhabited terminal cases
remain24; raw tags24 become5+20. Driver control states, subordinate runtime
states, descendant cardinality, interpretation/retirement ordering, and actor
authority are unchanged. Every surviving outcome owns precisely the current
values in the preceding table, rather than a second label for a cause. Common
custody remains one14-field product. There are no new semantic booleans, arrival
history, repeated causes, restrictive malformed-input phases, lifecycle engines
or positional caller routes. The terminal production prefix2033 becomes2067
physical lines; lexical arms56 become58 (two arms for affine selection), public
type/trait declarations12 become13, public methods11 become12, modules unchanged.
Root public spelling Retained and method try_into_custody are added; the20
outcome variant names move to RetirementOutcome rather than being duplicated.
Production across the three existing owners is +203/-167/net36; tests +263/-0;
public types +1/-0. This is a new capability with positive source growth.

Cross-checks include Bombay driver-law D-RETIRE-1/D-TERM-1 and capability/module
ownership; Behavior atomic-runtime-settlement's terminal-custody and total-item
laws; normalized atomic-actor-architecture's authority map/classification,
atomic-actor-features' terminal transfer and exact final actions,
atomic-actor-devx's complete rejection and application evidence, and
atomic-actor-verification's provenance, inversion and unchanged-Driver rules.
The five atomic transition aggregates are unchanged; no catalogue policy is
inferred from this passive native protocol. Outcome extraction interprets none
of their retained values. The new generic interpretation gate is not triggered:
no interpreter, Behavior product, settlement law or wrapper order is changed.

The design stage is now closed for the focused port. A separately measured
mechanical stage must replace old root variant patterns with Retained plus the
same RetirementOutcome variant, preserving every original custody binding,
primary error, progress field, assertion, fixture denial and policy. The source
scan finds108 Rust/document paths with the old qualified variants, including
the already-counted terminal/launch paths. Record the exact path union before
editing; the user's standing scope approval covers this expansion. The exact
tracked-source scan and six owning stage paths form112 distinct native paths,
137 with the25 earlier Behavior paths. The pre-edit source hashes/path union
are retirement-recovery-migration-inputs.json. The nine known retirement aliases
add no further old-case references; WorkerRetirement's test-local alternatives
in fixed_supervisor_recovery.rs are a different domain enum and stay intact.
No new
semantic type, bound, constructor, visibility, default, wrapper or interpreter
change is permitted during migration. Any such need reopens the design instead
of becoming compiler fallout. Existing tests remain present and must run after
their syntax is updated. The full DX04 custody/family surface, final matrix,
resource costs, CI/review, merge and runtime publication remain open.

#### Mechanical caller migration, local checkpoint

The migration changes353 old variant sites across107 Rust files and introduces
117 ordinary module imports. It adds no production semantic surface. The four
already-factored startup constructions are also included in the independent
357-case product comparison. Each original binding, primary cause, progress
field and literal payload is preserved. Replacing each migrated pattern with
its original and removing only the new imports yields byte-identical surrounding
source after pinned rustfmt normalization in all107 files. This includes original
feature attributes, assertions, fixture denials and every other Rust item.
Receipt: retirement-recovery-migration-conservation-final.json, with exact
current input hashes; retirement-recovery-migration-canonical-inputs.json binds
the original/reversed pairs. The earlier raw token comparison passed before
formatting; its post-format failure was the formatter's optional trailing comma
after an expanded two-element tuple. Canonical pinned formatting checks that
equivalence instead of weakening literal or ownership comparisons.

The first migration check exposes a source-editing mistake: the inserted type
import took an existing first import's cfg attribute. An attempted correction
after formatting still leaves the attribute on a different existing import.
Both failed checks are preserved as migration-check-tests and
migration-check-tests-corrected, exit101; neither receives law credit. Restore
all107 files from their exact pre-migration snapshots, then generate the same
353 transformations with imports placed before the entire original attribute
sequence. All original surrounding source and attributes now pass the canonical
comparison; migration-check-tests-final passes the default native test targets.
No bound, public type, default, wrapper, policy or visibility was invented to
repair these editing errors. The one-off editing/comparison code remains only
in external restart evidence, not repository tooling.

The first review compares all complete old/new case products and native producer
ownership; the second reverses the syntax and checks all original surrounding
source against its frozen baseline, including literal bytes and feature guards.
These are author reviews, not independent review credit. Runtime debug passes
348 library cases and33 selected integration cases,381 total. The single existing
ignored interpreter child is explicitly invoked with --exact/--ignored by its
parent to isolate an abort; no test registration or expected value changes.
Optimized execution also passes all381 outer cases. Strict all-target Clippy
then rejects collect_documents at106 lines: its two retained-outcome patterns
grew through nesting while the original recursive custody assertions stayed
exact. Preserve the one coherent test and add a local source-size expectation
with that ownership reason, rather than inventing another helper to hide the
count. This changes no assertion, input, action or runtime policy. The failed
lint receipt remains retirement-recovery-clippy-all-targets.log/.exit=101;
the corrected gate is still required. Conservation after this disposition must
exclude only that exact documented lint attribute and retain every other
original item/literal. Later compiler/feature/full Nix gates remain required.

The corrected all-target lint pass then exposes duplicated common custody in
eight existing or-patterns, including child_service_stop's102-line collector.
Apply the already-selected distributive representation to caller patterns:
`Retained { custody: C, outcome: A } | Retained { custody: C, outcome: B }`
is `Retained { custody: C, outcome: A | B }` only when C and every other common
binding are identical. This preserves the original alternatives and bindings;
it introduces no operation, public symbol, bound or policy. Establish equality
of the common patterns before editing, retain every inner outcome field and
assertion, and extend conservation to compare the expanded logical alternatives.
Do not add another helper or suppress the redundant-pattern lint. This is the
same pre-edit sum/product equation, not architecture invented from a diagnostic.
The source comparison must also account explicitly for the one documented
collect_documents source-size attribute; all other original items remain exact.

All eight common products now compare against the original alternatives,
including exact literal bytes and every binding. Six outer or-pattern parentheses
become unnecessary after factoring and are removed. The canonical reversal
restores the original grouping when comparing source. All107 files/357 logical
cases pass the formatter-normalized comparison with exactly the one recorded
metadata exception. The new regression's five empty-value checks use assert_eq
with the same empty value, and four exact rejection selections use let-else;
no new production bound or expected domain value is introduced. Final strict
default/all-target Clippy passes: retirement-recovery-clippy-all-targets-final.
Follow-up debug/release each pass54 cases across the five affected retirement,
child, supervision, receiving and configured-descendant targets; receipts are
retirement-recovery-patterns-*. The new integration source is259 lines after
those style corrections. Its two author reviews preserve the same absence,
pointer, failure, report, notification and original-self laws; the pre-edit
external source and inversion inputs remain unchanged and available.

Next preserve the existing static denials through the two compiler matrices.
Run the actual topology-negative harness before changing any golden text;
compare error codes, counts, trait/role causes and original invalid operations,
not merely exit status. Any source-position changes must be disputed from the
captured output and source mapping before editing expectations. Forecast up to
five additional existing paths: three topology stderr files, the compiler
manifest and minimum.patch, bringing planned resumed scope to142 (currently136
actual paths). Standing scope approval applies, without changing the denial
laws or adding a fixture. Archive all five originals before any update, preserve
unrelated manifest/patch entries, and execute both pinned compilers. Feature,
resource, full Nix and delivered-review gates remain open; DX04 is not complete.

The actual pre-update topology harness fails exactly three of six snapshots,
exit101 (retirement-recovery-topology-ui-before). Missing capabilities and forged
construction still pass their existing negative expectations. The two role
fixtures each retain exactly two E0277 errors; erased cold origins retain exactly
two E0308 errors. For all three actual outputs, subtracting one from fixture
locations and displayed source-line numbers reproduces the entire original
stderr byte-for-byte. Columns, underlines, source text, concrete types, role
causes, errors and counts are unchanged. The source change is the one new import;
the invalid operations themselves are conserved by the full source comparison.
Receipt retirement-recovery-topology-ui-shift-proof.json records this dispute
before editing expectations, with each raw actual stderr separately preserved.

Accept only this evidenced +1 location correction in the three canonical
snapshots and their existing minimum-compiler patch sections. Preserve the
minimum compiler's previously reviewed text differences, then execute that
compiler; carrying source locations forward is not minimum-compiler pass credit.
Update only the three existing snapshot/fixture hashes and patch checksum in
the unchanged compiler manifest. All five original inputs were clean and are
archived with hashes under retirement-recovery-diagnostics-before. No malformed
fixture, denial, error cause, public contract or unrelated expectation changes.

The corrected current-compiler topology harness passes all six original
denials, with overwrite unset: retirement-recovery-topology-ui-after. Applying
the updated existing minimum patch to a disposable copy succeeds without fuzz;
all25 canonical, minimum and fixture hashes match the manifest. Exactly22 other
patch sections and all unrelated manifest fields remain byte/data identical.
This is patch/hash conservation, not execution of the minimum compiler. The
next gate is the repository's existing pinned minimum-compiler Nix check, with
the new native regression explicitly staged so Git-backed Nix source includes
it. No new verifier, tool suite or fixture is added for this matrix.

The existing minimum-compiler Nix gate now passes on the frozen native source
tree e400fb235bae0282b25627e19305a31ca08a5b5c, base2b7acf12. Actual Rust1.96.0/
Cargo1.96.0 execute114 outer cases and136 unique compiler fixtures with zero
failures; one existing parent-controlled child registration remains ignored by
the outer harness. The suite and target inventories stay unchanged; this
execution prints28 Rust test summaries. Receipt retirement-recovery-minimum-nix
is exit0, with full log and machine-readable summary. Its initial output becomes
unavailable before hash inspection; exact xjq7zgyyqbsmbyvrrlzdxj3qdcg7k8l2
derivation re-realization also passes, with a task-owned Nix root now retaining
the output, its deriver, NAR metadata and target archive. No source/check changes
or GC command are used to recover it. Both original and restored receipts remain.

The final259-line owning recovery target separately passes all six cases on
Rust1.96 debug/release (retirement-recovery-owner-minimum-*). Current1.99 also
passes all five original positive application/custody compiler fixtures and
their original child denial, without overwrite (retirement-recovery-positive-ui).
These close the selected compiler blocker, not the full feature/resource/Nix
matrix or delivery. A fresh fetch finds main3bf270cb (merged PR412): seven paths,
with remote access tests and failure-log printing, no new ActorRetirement variant
references in its changed Rust files. Preserve these completed base2b receipts
and integrate current main before the final source freeze and review. Behavior
main remains c5c5863. The paired programme and complete DX04 remain active.

#### Protected Counter formatting correction

The post-integration full Nix run at18f266b completes27 of28 checks, including
minimum remote and optimized workspace/all-target tests. The latter reports122
Rust summaries and983 passes, with all six owning recovery cases present. Its
six existing ignored registrations remain unchanged. Protected Counter completes
its configured test commands, then fails its existing nested-manifest formatting
check; the final flake command is therefore not reached. Session49267 is
terminal1, not a running build or a passing full gate.

The formatting diff names five existing migrated modules under
crates/bombay/tests/protected_counter: hosting, mod, public_node, retirement and
transport. Ordinary workspace formatting does not cover this separate test
manifest. Preserve the failed log and these five exact inputs before correction;
27 successful outputs now have explicit task-owned Nix roots. Apply the pinned
formatter through the existing transport-custody manifest, then compare the
complete source and ownership products against the retained migration proof.
Expected scope is these five test files and this existing record, with no new
production code, public API, test assertion or expected domain value. The
formatting failure itself is the pre-edit regression; no semantic law changes.
Run the affected format/lint gate and the authoritative full flake check before
crediting this stage. Full recovery/family-disposal acceptance remains open.

The pinned nested-manifest format check now passes. Exactly the five forecast
test modules change: +156/-37/net119 physical test lines, solely formatting and
import order; production +0/-0, public types +0/-0. Repeating the complete107-file,
357-product conservation check preserves every field, literal and surrounding
source after reversing the accepted migration and pinned formatting. The one
previously documented lint attribute remains the sole metadata exception.
Both author checks pass: original assertions and custody products are retained,
and the complete source comparison rejects changes outside that equation. No
independent review is claimed. Both standalone Clippy commands skipped by the
format failure now pass through the pinned shell: default and protected-transport,
with their original strict warning policy. The corrected full Nix gate remains
required; the27 successful earlier outputs stay qualified to18f266b.

The corrected immutable source361d77281839ec76d755ad536020d594cab98dc4 now
passes all28 existing aarch64-darwin checks and the authoritative `nix flake
check`, both exit0. The exact input/output manifest,28 verified Nix roots,
derivers, NAR hashes/sizes and complete log hash are retained in the restart
evidence's retirement-recovery-corrected-full-gate-summary.json and
retirement-recovery-corrected-output-metadata.json. Workspace results retain983
reported passes/122 summaries and all six owning recovery cases. Minimum
compiler1.96 executes114 outer passes/136 compiler fixtures; minimum remote
passes3 cases. Each remote profile reports523 passes; the complete protected
Counter gate, strict Clippy, examples, docs, formatting and Loom pass. All
unchanged coverage floors and existing Driver/execution law witnesses pass.
Other platforms and CI are not executed by these host receipts. This closes
the retained source's full local gate, not complete DX04 or programme delivery.

#### Nonempty unread-command recovery witness

Before editing the next test, retain D-RETIRE-1 unchanged: consuming native
retirement transfers the complete original custody and primary outcome even
after failure. This is a derived ownership law, not a new actor-model ordering
guarantee. The existing Counter/MAX overflow witness checks only empty unread
lanes. Hypothesis: the existing ordinary consuming port also preserves a queued
non-Clone command and its actual allocated sender beside the failed command,
original Counter, completed Work, reports, cleanup and notifications.

Use only the existing Counter/Increment/Overflow and Application APIs. On the
current-thread host, two synchronous `sender.try_send` calls without an await
admit overflow then an unread command into a validated two-slot mailbox. Native
actor execution cannot intervene between those synchronous calls. Work awaits
termination and returns its independent owned value and the allocated sender.
Then consume completed Work, root retirement and `try_into_custody`; check the
complete original User, both distinct command allocations, Counter and all14
custody fields. No sleep, guessed sender, cloning or changed runtime policy.

Falsifier: clear only the returned unread user lane in the consuming port. The
new witness must fail its exact queued-input assertion in both profiles while
the original six cases pass; restore the exact production source and repeat.
The prior missing-port compile regression remains the design-stage evidence;
this is a missing adversarial test of that proven interface, not another API.
Review the oracle before execution and again against its actual inversion.

Pre-edit checkpoint: all runtime control sums, subordinate states/results,
transition branches, production lines, modules and public spellings stay
unchanged. Terminal alternatives remain24 logical/5+20 tags, with one14-field
custody product; each existing outcome retains its original table's current
values. Reuse the mailbox's FIFO queue, allocated sender and original User;
introduce no state, policy, result wrapper or second transition authority.
Residue scan: no arrival-history storage, repeated cause, false cardinality,
semantic boolean or structural caller path. Cross-check the same D-RETIRE-1,
D-TERM-1 and normalized atomic documents listed in the focused checkpoint.
Expected edits: this record and the existing retirement_recovery.rs test;
production/API +0/-0. Whole DX04's rejected progress, descendants, family
inspection, costs and delivered/archive acceptance remain open. Record actual
test counts, measurements and disposition after the experiment. Markdown record
changes also alter Nix source; the28-check receipt remains qualified to361d7728.

Result: all seven owning cases pass on actual pinned Rust/Cargo1.99 and1.96,
debug and release, with zero ignored tests. Strict current-compiler target
Clippy and formatting pass. Clearing the consuming port's user lane makes
only the new queued-input assertion fail in each profile (six controls pass,
one intended failure, exit101). Exact production restoration is hash-verified;
both current profiles then pass again. Receipts are retirement-recovery-unread-
{owner,inversion-*,restored,minimum,conservation*} in the existing restart
directory. Minimum toolchain is the exact Fenix output selected by the passing
minimum check, not a host-installed substitute.

Author review1 maps the complete User to its independently allocated second
sender and payload, the primary overflow to the first distinct allocation,
and Work/Counter to their original allocations. Current-thread synchronous
admission establishes the required unread suffix without scheduler timing.
Review2 checks the actual falsifier: the original six empty-queue cases all
miss this loss; the added case catches it in both profiles. It also rejects a
wrong sender, substituted payload, missing/extra unread input, changed actor,
lost primary cause and missing independent retirement results. No independent
review is claimed. Reversing only the added test and capacity import, then
applying pinned formatting, reproduces the entire original six-case source
byte-for-byte; no original oracle is weakened.

Retained checkpoint disposition: `pass` for this test-only witness. Production
+0/-0/net0, public types +0/-0; test +102/-2/net100 (one new100-line case and
formatted import). Runtime/control states, subordinate alternatives, branches,
production modules/spellings and the existing14-field product stay unchanged;
the same current values and normalized laws listed above still apply. The
residue scan finds no new historical state, repeated cause, false cardinality,
nested authority, semantic boolean or structural user syntax. Cumulative feature
scope stays116 native/141 paired paths; production net36 and one public type
remain unchanged. All28 full-gate credit still belongs to361d7728. Complete
nonempty effect/progress, descendant/family, cost and delivery work remains open.

#### Existing accepted/rejected-effect consumer, before editing

Next extend the same D-RETIRE-1 consuming-port proof using the existing
interpreter_selection.rs real-file case, rather than adding another actor or
interpreter. Its native EffectsFailed result already owns one usable accepted
File, one original refused OpenFile/path allocation and its actual OS cause,
alongside the original Document and all14 custody fields. The exact selected
0.29/0.16 graph and existing request/Services contracts above remain unchanged.
Replace only the test's Retained pattern with consuming `try_into_custody`,
then match the same outcome and retain every original assertion. Move the
two existing iterator advances out of assertions into named local values so
assertions only observe; preserve the exact traversal and absence checks.
No production type, effect, interpreter, state or policy changes are needed.

Hypothesis: complete failed initialization custody remains available through
the ordinary consuming port, including both accepted and rejected effects.
Falsifier: only for EffectsFailed, clear the returned settlements. The owning
file case must fail at its complete-lane assertion in debug/release while all
seven Counter cases still pass. Restore the production source exactly and
repeat the focused file case on current1.99 and minimum1.96 in both profiles.
Retain the two existing static interpreter denials unchanged for final gates.

Pre-edit checkpoint:24 logical terminal cases/5+20 tags, one14-field product,
all runtime/subordinate states/results, branches, modules, production lines and
public spellings remain unchanged. Reuse the original passive outcome and
settlement vector; every surviving current value is still required by the same
phase table. No new arrival history, repeated cause, cardinality restriction,
semantic flag, nested actor authority or positional user route. Cross-check
D-RETIRE-1/D-TERM-1, total-item custody and the same normalized actor documents.
Expected paths: the existing interpreter_selection.rs and this record; API and
production +0/-0. Review the oracle twice, measure the final test diff and
record actual source inversion/restoration before retaining this witness.

Result: the real-file consuming case passes on actual Rust/Cargo1.99 and1.96,
debug/release. Strict current target Clippy and formatting pass. The deliberate
EffectsFailed-only settlement erasure fails exactly at the complete initialized
file lane in both profiles; all seven Counter controls pass in each. Restoring
the exact production hash passes the file case and all Counter controls again.
The two static denial fixtures are unchanged and remain for final full gates;
these focused runs do not claim to execute the filtered compiler harness.

Review1 maps the existing independent file read, exact rejected-path allocation,
actual NotADirectory cause, primary failure, Document and all14 custody fields
through the consuming pair. Review2 checks the actual settlement-erasure
counterexample and unchanged controls; accepted File and rejected request must
coexist, so preserving only one is insufficient. Both iterator advances now
precede their original absence assertions. Reversing exactly the four forecast
edits reproduces every original test/fixture/assertion byte. No independent
review is claimed. Receipts and exact hashes are retirement-recovery-effects-
{before,inversion,owner,restored,minimum,summary} in the existing handoff.

Retained checkpoint: `pass` for this existing consumer. Test +15/-14/net1;
production +0/-0/net0, public types +0/-0. All pre-edit state, alternative,
branch, module, production-line and public-spelling counts stay unchanged;
the existing phase-table values and normalized-law cross-checks still apply.
No new history, duplicated cause, cardinality, authority, boolean or structural
caller route remains. Native scope stays116 paths; paired scope141. Nonempty
partial interpretation/source progress, descendants, complete family recovery,
costs and delivered acceptance remain open. The last full28-check receipt is
still the immutable361d7728 source, not these later test/record commits.

#### Partial interpretation consumer, before editing

Continue D-RETIRE-1 with the existing application_services.rs guarded-panic
case. Current sourceb1bfb46d is clean; root Cargo.lock still selects registry
Behavior/Actors0.29.0, macros0.16.0, Address0.3.0, Communication0.1.6 and
Timers0.2.0 without patches. The existing Documents, OpenFile/Services and
InterpretationProgress contracts already own this trace: an acquired usable
File, the original panicking pending request/provider, an untouched later file
and an untouched independent environment lane. The original panic allocation,
actor, Work, cleanup and all14 custody fields remain observable. No transition,
interpretation, scheduler or actor-model law changes.

Replace only that test's Retained pattern with consuming `try_into_custody`,
then match its same HostExecutionPanicked outcome. Preserve every original
assertion and allocation/provider identity check. Move its one final iterator
advance out of the assertion into a named value. No import, fixture, actor,
interpreter, generic, policy or production symbol is needed. Forecast: this
existing test path and this record, production/API +0/-0; measure test lines.

Falsifier: in the consuming port, only for HostExecutionPanicked, discard its
interpretation while returning all other fields. The complete partial-action
assertion must fail in debug/release; the other three service cases and seven
Counter cases must pass. Restore the exact production source, rerun both
profiles, strict target Clippy/formatting, then run actual minimum1.96 in both
profiles. Review the oracle against the law and actual falsifier separately.

Pre-edit aggregate checkpoint:24 logical cases/5+20 tags and one14-field custody
product; all runtime/subordinate states, results, transition branches, production
lines/modules/public spellings unchanged. Each existing progress alternative
owns its same current original/accepted/pending value needed for exact return;
no history or second transition authority is added. Reuse the existing sum,
named effect lanes and ordered vector. Residue scan: no repeated cause, false
cardinality, semantic boolean, new arrival state or structural caller input.
Cross-check D-RETIRE-1/D-TERM-1, total-item/terminal custody and the same normalized
atomic documents in the focused checkpoint. Complete descendant/family recovery,
costs and full current-source/CI/delivery acceptance remain open.

Result: all four service cases pass actual Rust/Cargo1.99 and1.96 in debug and
release. Seven Counter controls also pass both current profiles. Strict target
Clippy and formatting pass. Discarding only HostExecutionPanicked interpretation
makes the intended partial-action assertion fail in each profile, with the
other three service cases and seven Counter controls passing. Exact production
restoration passes all current cases again. No test is ignored or weakened.

Review1 traces the original panic carrier, acquired usable File, pending path
allocation/provider and both untouched suffix allocations through the consuming
pair; all14 custody fields and independent Work/notification/cleanup results
retain their original oracles. Review2 checks the actual erasure failure and
ten passing controls, including cancellation and opposite wrapper order. Both
reviews are by the author. Reversing only the forecast pattern replacement and
iterator advance reproduces the complete original four-case source byte-for-
byte. Receipts are retirement-recovery-progress-{before,reversed,inversion,
owner,restored,minimum,summary} in the existing restart evidence.

Retained checkpoint: `pass` for this partial-interpretation consumer. Tests
+11/-11/net0, production +0/-0/net0, public types +0/-0. All pre-edit aggregate,
subordinate, branch, module, production-line and public-spelling counts and
current-value owners stay unchanged; the same normalized laws still apply.
Residue scan retains no new history, cause, cardinality, authority, boolean or
structural user input. Native116/paired141 paths remain unchanged. Nonempty
source progress, descendants/families, costs and delivered acceptance stay open;
the full28-check receipt remains bound to361d7728 before these test-only stages.

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

The supported compiler checks use Cargo and trybuild through pinned Nix. The
current compiler runs the workspace check; the minimum compiler applies the
existing reviewed diagnostic patch only inside its disposable Nix build source:

```sh
nix build .#checks.aarch64-darwin.bombay-test --no-link
nix build .#checks.aarch64-darwin.bombay-minimum-compiler --no-link
```

Replace aarch64-darwin with the host system when needed. `nix flake check`
includes both checks. Cargo owns the minimum supported version; the existing
compiler manifest selects its exact reviewed release and target corpus. The
[compiler record](compiler-diagnostics.md) retains scope and execution evidence.
Rust/Cargo/example/fmt/lint commands always run inside pinned Nix. Complete
resource accounting, proportional failure inspection, Machine/deferred custody,
SDK privacy, comparable memory/compile costs and final documentation acceptance
remain in the owning checklist. Partial receipt counts, normal retirement and
readiness notices do not establish complete affine custody or committed Ready.
The programme is not complete merely because examples or selected checks pass.
