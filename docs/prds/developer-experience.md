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
work. Networking, durability, identity and deployment products remain separate.
Zenoh stays internal: configuration conversion, sessions and queries are private;
public foreign causes use the standard error contract. Final interface acceptance
still requires the selected source, consumer and delivery gates.

## Current delivery

The scoped retirement stage merged through [PR415](https://github.com/devrandom-labs/bombay/pull/415)
as `d575c614418b19476bd0f5dc481ed76ccb4671c6` after all eight submitted-head
checks passed. Reviewed/tested head `0c9ef555` and delivered main have identical
tree `a1fc212d`; all 718 frozen inputs remain exact. This includes main PR414's
schedule corpus and PR413's registry Behavior/Actors0.30.0 adoption. All 28
aarch64-darwin checks and authoritative flake check pass; Linux CI also passes
the flake gate, Driver 2,048-run campaign and four Observe 1,024-run campaigns.
Miri is scheduled/manual only and was skipped in this PR run. Two author
source/oracle reviews pass; no independent review is claimed. Active rules
required zero approving reviews, and there were no unresolved review threads.

The owning evidence merged through [Behavior PR121](https://github.com/devrandom-labs/bombay-behavior/pull/121)
as `0e104494` after all 14 checks passed, including the strict mutation aggregate.
Its delivered tree equals tested `f1e34025`; all 11 local owner checks, 1,051 optimized
Nextest cases and 125 doctests pass. Runtime production net 53/tests net 1,581,
one added public type and two added public methods retain the recorded 121-path
scope. Complete traces, source conservation, archives, costs and loss inversions
remain qualified below. `retirement-delivery-summary.json` in the restart
handoff links exact merge trees, CI and final logs; earlier receipts keep their
original revisions. Feature branches and pre-adoption backups remain preserved.

Permanent root-transport coverage merged through [PR417](https://github.com/devrandom-labs/bombay/pull/417)
as `402ff67f95af3f1163caf492ee6b4101148771b0` after all eight checks passed.
Its delivered tree `46e31eba` equals the reviewed and tested tree; all 721
frozen inputs remain exact. All 28 local Nix checks and authoritative flake
check pass, as do Linux flake, Driver 2,048 runs and four Observe campaigns of
1,024 runs each. Miri was skipped for the PR event. Two author reviews pass;
independent review is unclaimed. The separate main-push run remains pending.
The six verified package archives retain their actual `46b34034` provenance;
`d1414998` changes only the debug log guard and this coordination record.

DX01–DX28 remains active. Selected consumer and policy acceptance, broader
costs and runtime registry delivery remain open.
Engine 0.3.0/macros 0.2.0/runtime 0.2.0 candidates are unpublished; a merged PR is
not publication. Main-push CI is separate from the completed PR gates. Other
active worktrees remain untouched. Behavior 0.30.0 and unchanged macros 0.16.0
are published; the owner release notice was acknowledged. The earlier
PR113/114 and reviewed release PR115 merged and normal publication delivered
Behavior/Actors0.29.0 and macros 0.16.0 from `55dbac2`. Registry checksums, clean
archive provenance, tags and releases agree; all228 shipped Rust files match
the tested candidate. The release passed14 GitHub checks,11 native Nix gates,
1,035 owner tests,123 doctests and four-workspace package preflight. These owner
results do not substitute for the remaining Bombay gates.

### Transported roots

From delivered main `d575c614`, DX05 needs permanent coverage of the existing
Rust task-transfer contract: transported root state must be `Send`, while
caller-local work retains its separate contract. The already-proven inferred
`Application::new(Counter { value }).execute_with(...)` twins accept `Arc<u8>`
and reject otherwise identical `Rc<u8>` state with E0277 naming Counter/Rc/Send,
on 1.99/1.96 debug/release. This is a derived Rust ownership law, not new actor
semantics; external counter-transport receipts retain the pre-edit proof.

Register those twins in the existing run_with compile harness and compiler
inventory: six to eight harness fixtures, 136 to 138 inventory cases. Preserve
every existing source, expected diagnostic and minimum-compiler patch. Scope:
two Rust fixtures, their required diagnostic expectations, harness/inventory and
the existing three coordination records; at most nine native paths, production/
public API0. No new bound, callback, marker, route or runtime abstraction.
Check useful denial causes and the valid control on both compilers/profiles;
invert the carriers to prove both registrations execute and reject the intended
counterexamples. Existing host/cancellation/receiving witnesses stay unchanged.
Runtime control states remain five outer/20 retained/24 logical alternatives
with 14 custody fields; no new state, protocol or transition authority is needed.

Fresh locked metadata retains Behavior/Actors0.30.0, macros0.16.0,
Address0.3.0, Communication0.1.6 and Timers0.2.0 without patches; Observe stays
Native-owned. Relevant owner source, transfer contracts and existing host/work
tests were inspected. Behavior/Actors impose no executor policy; Native's
`execute_with` owns the transported `Actor: Send` bound and leaves
`WorkFuture: Future` local. Address inherits endpoint transfer capabilities,
Communication owns mailbox transfer and Timers retains generation tokens.
No dependency contract changes. `execution-transport-before.json` records the
original 718 inputs,136 cases and25 compiler-specific expectations before edits.

The permanent run_with harness now registers both source twins. Actual Rust
1.99/1.96 outer debug/release profiles each pass all eight fixtures. The new
snapshot contains exactly two E0277s naming Counter/Rc/Send at `execute_with`
and disposal of its opaque result; both compilers accept the same snapshot.
All 136 original cases,25 compiler-specific expectations and the original
minimum.patch remain unchanged. Removing only the two new harness calls
reproduces the original entire run_with source byte-for-byte. The initial
missing-expectation run failed only while creating this new snapshot; it is
not credited as a production defect.

Both carrier inversions fail exactly their intended fixture with seven healthy
controls on both compilers/profiles: accepting Arc cannot satisfy the Rc denial,
and rejecting Rc cannot satisfy the Arc control. All four restored runs pass;
all 721 focused input files remain exact, with only the existing25 minimum
diagnostic substitutions in that isolated copy. Two author source/oracle
reviews pass; no independent review is claimed. Workspace/fixture formatting
and strict owning-target Clippy pass. Production/public API0; Rust tests+46,
one new shared diagnostic and two existing harness calls, no new outer test.
This is compile-contract evidence; host/work/cancellation behavior retains its
separate existing witnesses. Full repository and hosted gates subsequently
passed and PR417 delivered this batch as recorded above. Joint package and
extracted fixture-consumer checks pass on both compilers, with their original
`46b34034` source limits. Receipts are execution-transport-* in the existing
restart handoff.

The first full run at46b34034 ends1:18 checks pass, nine are cancelled after
remote-debug returns101, and authoritative flake verification is not reached.
Its redirected Cargo log is hidden by `set -e` before the following `cat`;
the original Cargo cause is unproven. The exact check is rerun with failed
build-directory retention. A separate pinned Cargo missing-target control
reproduces the logging defect: both recipes return101, but debug prints no
error while the existing release recipe prints the original error. Reuse that
release catch/print/exit sequence in debug, without changing its Cargo command
or six execution markers. This adds flake.nix to the scoped change (nine paths);
production Rust and public API remain unchanged. Logging proof is
execution-transport-log-control.json; it does not claim the remote failure fixed.
The exact original debug derivation then passes in isolation:528 tests across
12 summaries, zero failures/ignores, followed by strict remote Clippy and docs.
This does not recover the first Cargo error or establish a causal repair.
Keep both attempts. The corrected full gate retains failed logs and bounds
local build concurrency to two; it still runs every original check and marker.

### Recovery startup diagnostics

The separate PR415 main-push run43239095715 failed before request recovery:
the scope-refusal test could not open its Node. Its generic setup panic discarded
the existing `NodeStartupRefusal`. The historical startup cause remains unknown;
successful later runs do not explain it. The actual raw log and
retirement-main-failure.json preserve this distinction from successful PR CI.

Before the project test edit, retain this engineering observability law: the
shared recovery setup must print the actual typed refusal and its existing
standard `Error::source` when Node opening fails. This is test observability,
not a new actor-model or runtime law. Preserve all four recovery scenarios'
original network selection, watchdogs, requests, assertions and cleanup.
The independent request-key scenario keeps its existing contract; no broader
startup rewrite, retry policy or test serialization belongs to this defect.

Fresh locked all-feature metadata at merged main `402ff67f` confirms registry
Behavior/Actors0.30.0, macros0.16.0, Address0.3.0, Communication0.1.6 and
Timers0.2.0 without patches; Observe remains Native-owned. The selected Behavior
revision is `628649da`, whose complete AGENTS.md applies. Source inspection
confirms `Application::open_node` already returns the original Application and
Network with the typed refusal, and `NodeStartupRefusal::source` exposes the
actual cause. Reuse those owners; preserve the deliberately redacted public
Display/Debug. No algebra, interpreter, dependency or public contract changes.

The pre-edit external copy proves the defect using a real held IPv4 listener:
the original setup fails101 with only its generic panic; the same setup with
the proposed diagnostic fails101 and exposes actual TLS AddrInUse/os48 through
the standard source. Neither forced setup failure earns recovery semantic
credit or establishes the historical Linux cause. Restoring the original setup
and every assertion leaves only the module-scope Error import and diagnostic;
the scope-recovery scenario then passes. Owning formatting and strict Clippy
also pass. These controls used actual Rust1.99 debug only. Two author source/
oracle reviews pass, independent review unclaimed. Exact sources, raw logs and
hashes live in node-startup-diagnosis-{plan,inputs,progress}.json in the existing
restart handoff. The original test source is identical at d575/d141/402ff.

Expected project scope: four Native paths, the owning recovery test plus this
PRD and the two existing backlog records. Tests +7/-1; production +0/-0/net0;
public API +0/-0. Reuse standard Error and the existing typed opening result;
add no type, bound, callback, marker, route, runtime state or transition branch.
All aggregate states, subordinate alternatives, modules and public spellings
remain unchanged because the edit only formats an existing test failure.
The applied project source equals the validated proposal byte-for-byte. Actual
Rust1.99 project checks pass: all five recovery/request-key scenarios, workspace
formatting and strict remote application_clients Clippy. The complete diff
retains every original assertion and all other Rust files. Two author source/
oracle reviews pass; independent review is unclaimed. Full repository gates
and delivery subsequently passed: PR418 merged as `899af54f` after all eight
hosted checks and28 local checks passed. The historical opening cause remains
unknown. The separate main-push run is qualified below.

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

### Listener address selection, before implementation

Main `899af54f` run43314686083/job131519649903 failed the Linux remote-debug
gate while opening the second Counter Node in the overlap witness. The other
49 cases passed, including the recovery cases. Its generic opening panic
discarded the refusal, so the actual cause remains unknown. Earlier intentional
allocation-role child panics are not this gate failure. Keep the original raw
log and distinguish this event from the older recovery opening failure/hang.
The runner-type work is preserved separately on codex/runner-types, with its
exact mechanical candidate in the recorded stash and two independent patches.

Inspection found a concrete fixture mismatch: reserved_address binds IPv4
127.0.0.1 but returns a localhost TLS endpoint. Selected
bombay-zenoh-link-tls1.10.4 resolves localhost and takes the first address;
Tokio1.53.3 delegates that lookup to standard ToSocketAddrs. On this host the
first address is IPv6 ::1, and an actual bind there succeeds while the IPv4
reservation remains held. This demonstrates the mismatch; it does not prove
the historical Linux refusal's cause. The standalone pinned1.99 source/log
are retained in the existing restart handoff.

Engineering law: test listener selection must use the same host/address family
as the actual TLS listener. A held reservation must prevent binding the first
address returned by the TLS owner's actual Tokio lookup; releasing it must
allow binding that same address. The existing two-Node overlap trace remains
the end-to-end Node witness, with every original ownership oracle retained.
This is test-host policy, not an actor-model law. Before changing selection,
factor the existing IPv4 bind into one private reserve_listener function that
returns the original standard TcpListener. The existing address function keeps
the same operation. Add a focused owning test that holds that actual returned
socket, resolves localhost through Tokio, checks the real native bind refusal,
then releases the reservation and binds the same resolved address. This tests
the socket owner directly, without inventing an actor fixture or a second TLS
implementation. If binding succeeds while the reservation is held, close both
sockets before the intended failure. Establish this baseline
before replacing IPv4 selection with localhost; do not change the oracle.

Fresh owner verification must retain registry Behavior/Actors0.30.0,
macros0.16.0, Address0.3.0, Communication0.1.6, Timers0.2.0 and Native Observe.
The selected Behavior revision remains628649da. Controlled networking selects
bombay-zenoh1.10.10/config1.10.2/link-tls1.10.4; inspect their actual source.
Reuse those owners, standard socket ownership and the existing typed refusal.
Add no runtime API, transport escape, retry, serialization, semaphore, callback,
registry or dependency. Preserve all seven certificate inputs and every
original overlap operation, watchdog, ownership assertion and cleanup.

Expected scope:22 Native paths,19 test-source paths plus three existing records,
under the user's standing automatic approval. The shared test network owner, its
existing address callers, the overlap test and three existing coordination
records. Production+0/-0, public types+0/-0; tests expected net40–90 lines.
After the focused law passes, rename the address function to listener_address
so a returned string does not promise a held reservation. Preserve all callers'
original operations. Give both actual overlap openings the same existing
standard Error::source diagnostic already used by recovery. No source-selected
probe or successful replay establishes the old Linux cause. Finish focused
debug/optimized controls, full gates and normal reviewed delivery before
resuming the preserved runner migration. The full DX01–DX28 goal stays active.

The original-selection baseline now fails for the intended law on pinned1.99
debug: IPv6 ::1 accepts a real listener while the same port on127.0.0.1 is
still held. Both sockets close before the failure. Keep that test unchanged
and replace only the selector's IPv4 host with localhost. The baseline raw
log/source hashes remain in listener-address-progress.json; no historical
Linux cause is inferred.

The unchanged regression now passes on actual1.99 debug and optimized builds;
the original two-Node overlap witness also passes optimized. Two author
source/oracle reviews pass; independent review is unclaimed. The mechanical
rename touches19 existing test paths, including cfg(test)-owned Node and export
consumers, plus three records. Both overlap openings now retain the established
standard Error::source diagnostic on failure. Production/API0; original
configuration, certificates, actors, watchdogs and business/custody assertions
remain unchanged. Complete scope, source inversions, both-profile owning
suites, full gates and delivery are still pending.

The exact0d44 candidate subsequently passed all28 local checks, authoritative
flake check and content verification with all721 inputs unchanged. Restoring
only the old selector in an isolated optimized build makes the unchanged
regression fail for the intended namespace mismatch. Those receipts retain
their original source. Main then advanced through PR416/b3833e50, adding one
more shared-helper caller in the existing outbound overlap witness. Rebase
preserves that peer's complete tests and records; migrate only its helper name.
The revised scope is23 paths,20 test-source paths plus three records, with
production/API0. Locks and production source did not change in that main
advance. Fresh metadata, both-profile real overlap controls and full gates on
the reconciled tree remain required before delivery.

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

#### Child failure and surviving descendant consumer, before editing

Continue D-RETIRE-1/D-TERM-1 against clean1d57a776 and the unchanged selected
dependency graph. The existing application_terminal_custody.rs panic journey
already retains a failed uncommitted child's original creation/state and panic
carrier, plus a later established sibling's joined retirement. Inspect its
actual NativeChildren extraction and topology schema: ActorCustody independently
owns the descendant vector and typed child-failure vector. The failure's
original report, notification, address, nonce and birth provenance coexist with
the surviving child. No new child/runtime policy or protocol is needed.

Replace that test's root and survivor Retained patterns with consuming custody
pairs, matching the same primary outcomes and destructuring the same complete
products. Preserve every original assertion, exact panic allocation and shared
payload lifetime oracle. Additionally inspect the existing runtime-issued root
report before consuming: retirement Established and FailuresFound must coexist
with the root's own Completed outcome. This is the existing joined-subtree
classification law, not an assumption that root success implies child success.
Expected paths: this existing test and this PRD; production/API +0/-0. Measure
the exact test delta and prove original-source conservation after reversing
only these forecast changes.

Falsifiers, separately restored: clear only the consuming custody's descendant
vector; then, at its actual NativeChildren owner, discharge the complete
InitializationPanicked failure instead of appending it to the original vector.
Each must fail this panic journey for its missing descendant/failure assertion
in debug/release while the other three terminal cases and seven Counter cases
pass. Preserve exact production inputs, restore each file and repeat current
and actual minimum1.96 profiles, strict target Clippy and formatting. Review
the original carrier/lifetime/provenance oracle and the actual failures twice.

Pre-edit checkpoint:24 logical terminal cases/5+20 tags and one14-field custody
product remain; runtime/subordinate states/results, branches, production lines,
modules and public spellings stay unchanged. The original child cause, sibling,
report and current original creation values each retain their distinct owner.
Use the existing typed vector and complete sum, with no new history, repeated
cause, cardinality restriction, semantic flag, transition authority or positional
user input. Cross-check the same normalized terminal-custody/creation laws and
explicit provenance. Family recovery, nonempty source progress, costs, full
current-source/CI and delivered acceptance remain open.

Result: all four terminal cases pass actual Rust/Cargo1.99 and1.96 in debug and
release; seven Counter controls pass both current profiles. Strict current
target Clippy and formatting pass. The root's retained report establishes
retirement and failure together while its original primary outcome is Completed.
Both original survivor custody and failed-child carrier/provenance/lifetime
oracles now run through the consuming port.

Separately clearing descendants and dropping only the initialization-panic
failure at NativeChildren each fails exactly the corresponding length assertion
in the intended case, in both profiles. Three other terminal cases and seven
Counter controls pass each time. Exact restoration of both production files
passes again. The first minimum invocation has a mistyped RUSTC store path and
exits101 before executing the compiler/tests; retain that failed receipt with
no test credit. The separately named minimum-corrected invocation executes
actual1.96 and passes both profiles.

Review1 maps the independent sibling and failed child, original creation/state,
exact panic carrier, report/notification and final shared-payload discharge to
the consuming pairs. Review2 checks both actual erasures and ten passing
controls, including the distinction between root completion and subtree failure.
No independent review is claimed. Reversing only the two consuming sites and
new report oracle reproduces the complete original four-case source byte-for-
byte. Receipts are retirement-recovery-children-{before-*,reversed,inversion,
owner,restored,minimum,minimum-corrected,summary} in the existing handoff.

Retained checkpoint: `pass` for this child/descendant consumer. Tests +53/-52/net1;
production +0/-0/net0, public types +0/-0. All pre-edit state, subordinate,
branch, module, production-line and public-spelling counts and current-value
owners remain unchanged. The same normalized terminal/creation laws apply;
residue scan adds no history, repeated cause, cardinality, flag, authority or
structural user input. Native116/paired141 paths stay unchanged. Family recovery,
nonempty source progress, costs and delivered acceptance remain open.

A fresh remote fetch now finds main814c96d1: PR413 merges registry Behavior/
Actors0.30 adoption and579 lines of native Machine consumer tests, plus the
existing remote-delivery records (16 upstream paths total). This work belongs
to main, not our feature delta. Preserve all029 receipts at their actual sources.
After closing this stage, retain the pre-adoption branch, read the exact030
owner instructions/contracts, then integrate main. Its Machine file has two
old ActorRetirement patterns requiring only the already-proven mechanical
representation migration; archive their original source and conserve every
assertion/field before assigning030 verification credit. No new API, bound,
wrapper, placeholder or production policy may arise from that migration.

#### Main030 reconciliation, before caller edits

All six local commits rebase onto814c96d1 as a10b3248. The sole conflict is
backlog text: preserve the scoped DX1 feature row and current upstream NET1
delivery row, retaining every other upstream update. The original6cb916ac
branch and all029 receipts remain. Exact030 Behavior instructions are read in
full, including the separately retrieved middle after tool-output truncation.
The pure Behavior production algebra is unchanged from selected029; the seven
changed Actors owners implement the already-released retained-progress Machine
and finite-input contracts. Inspect their actual Machine/InputLimit/HeldInputs
surface and registry archive provenance at628649d before migrating callers.

The fresh native Machine test has exactly two old root retirement patterns.
Apply only D-RETIRE-1's proven Retained/custody+outcome representation and one
canonical module import; preserve all579 original lines modulo those patterns
and pinned formatting, all bindings, literal values, assertions and native
sender/refusal/cleanup observations. Archive original source/hash first. This
adds one native test path:117 native/142 paired cumulative paths under standing
scope authorization. Production +0/-0, public types +0/-0. Do not repair any
other compiler diagnostic by adding a symbol or policy. Run owning recovery,
service, terminal and Machine consumers on actual1.99/1.96 debug/release plus
strict target Clippy/formatting before crediting the integrated graph.

Checkpoint before the mechanical edit: runtime/control/subordinate sums,
branches, production lines/modules/public spellings unchanged;24 logical
terminal alternatives/5+20 tags, one14-field custody product. Same original
outcome/current-value owners and normalized custody/creation laws apply. No
new arrival history, repeated cause, cardinality, authority, semantic boolean
or positional user syntax. Report measured test delta, full-source conservation
and both author reviews after execution. Family recovery, source progress,
costs and complete current-source gates/CI/delivery remain open.

Result: current1.99 runs all five targets in both profiles,23 outer passes per
profile (service4, terminal4, interpreter2, Machine6, recovery7), zero ignored.
Both original interpreter-denial fixtures pass each profile with overwrite
unset. Actual minimum1.96 passes22 outer cases per profile: the same four
native targets plus the focused real-file case; its compiler harness is
explicitly filtered and receives no minimum credit here. Strict current Clippy
passes all five targets. Both compilers pass all11 unchanged Driver inventory
checks. Workspace and the separate protected-consumer format checks pass.

Two original Machine ownership products retain every binding, cause and literal;
reversing only the proven patterns/import reproduces all579 upstream source
lines byte-for-byte. Both author reviews confirm exact refusal origins, payloads,
retained progress, finite refusal/closed-customer custody, Work and cleanup
remain checked; no independent review is claimed. Production sources are also
byte-identical between the preserved029 branch and rebased030 feature. Registry
checksums and actual archive provenance remain those published from628649d.

Retained checkpoint: `pass` for this mechanical reconciliation. Tests +17/-10/
net7, production +0/-0/net0, public types +0/-0;117 native/142 paired paths.
All pre-edit aggregate/subordinate alternatives, branches, production lines,
modules/public spellings and current-value owners remain unchanged. The same
normalized laws apply; no new history, repeated cause, cardinality, authority,
flag or positional user input. Receipts are retirement-recovery-main030-
{plan,machine-conservation,current,minimum,inventory-format,inventory-minimum,
summary} in the restart handoff. The next source freeze runs all28 existing
host Nix checks and authoritative flake check on the integrated graph, keeping
the old029 full result bound to361d7728. Family/source-progress, cost and whole
programme delivery acceptance remain open; these focused passes do not close it.

#### Complete030 host gate and consuming family witness

Immutable0da4c952 now passes all28 existing aarch64-darwin checks and the
authoritative `nix flake check`, both exit0. All28 output roots, NAR hashes,
sizes and derivers match the frozen manifest. Workspace/all-target optimized
execution reports990 passes/123 summaries, zero failed summaries and six
unchanged parent-controlled ignored registrations. Minimum1.96 executes114
reported passes/136 compiler fixtures; minimum remote passes3 cases. The
registered law artifacts match628649d: eight Driver positives/boundaries and
killed inversions; execution has ten kill records, two ownership denials and
four healthy/restored child-preparation controls. All four unchanged coverage
floors pass. Exact receipts use retirement-recovery-main030-full-* in the
existing restart evidence. Other platforms, independent review/CI/delivery and
whole DX01–DX28 remain open. This receipt precedes the following test-only edits.

Before editing, retain D-RETIRE-1's complete ownership transfer and Bombay's
explicit family-disposal policy: acquired Work/root/family results survive a
later family-definition destructor panic. This is native policy and a derived
custody law, not an actor-model promise to reconstruct consumed values. The
actual FamilyRetirement already owns shutdown, metrics and original disposal
failure; its consuming triple needs no new API, wrapper, trait or state.

Two existing completed_application_receiving.rs traces supply independent
oracles: three families preserve the acquired prefix/tail and exact panic
carrier, and an actually admitted live account preserves its join, callbacks,
nonzero metrics and original family fault. Both currently borrow the returned
family reports. The separate two-live-family consumer already consumes reports
but exercises no disposal failure. Keep every existing assertion and snapshot;
after borrowing, consume the original returned root through try_into_custody
and every family through into_parts. Keep all owned products until the original
final drop, with no cloning/reconstruction. In the live-account trace, compare
owned shutdown/metrics/failure availability with the independently captured
borrowed values as well as the existing final lifetime oracle. All original
receiving errors and independent siblings must remain in their original lanes;
do not short-circuit the product with `?`.

Falsifier: only in FamilyRetirement::into_parts, return no disposal failure
instead of the original one. Both fault cases must fail the retained-cause or
owned-failure-availability assertions, while the two no-fault cases pass in
debug/release. Original carrier/lifetime assertions remain unchanged.
Restore the exact production source and repeat positives on actual1.99/1.96
in both profiles plus strict target Clippy/formatting. Review the complete
oracle before execution and against its actual falsifier. Forecast: this PRD
and the one existing test path; production/API +0/-0, no new test registration.

Pre-edit checkpoint:24 logical terminal cases/5+20 tags and one14-field custody
product; all runtime/subordinate states/results, branches, production lines,
modules and public spellings unchanged. The existing original Work, root,
notifications, family shutdown/metrics/cause and typed receiving failures each
retain their current owner. Direct products and Result::map transfer them; no
new historical state, repeated cause, false cardinality, semantic flag, nested
authority or structural caller input. Cross-check D-RETIRE-1/D-TERM-1, complete
application-result and family-disposal laws, and the same normalized atomic
documents. Source progress, combined root/family failures, costs and delivered
acceptance remain open. Record actual test delta/conservation and disposition
before retaining this witness.

Result: all four existing cases pass on actual1.99/1.96 in debug/release,
before and after the inversion; each compiler/profile reports four passes,
zero failures/ignores. Strict current target Clippy and formatting pass. Returning
None instead of the original disposal failure kills both fault cases in both
current profiles; both healthy cases pass. The production source is restored
byte-for-byte. The first evidence locator selected the earlier borrowed live
assertion at3131; preserved raw logs prove the owned assertion at3169 failed.
Correcting only that locator changes no source or expectation. Initial failed
evidence processing remains recorded without credit.

Two author reviews pass: first, the exact original Work/root/role/carrier,
joined account, nonzero metrics and final lifetime oracles survive the consuming
bridges; second, the actual cause-erasure inversion distinguishes both fault
cases from their healthy controls. No independent review is claimed. Removing
only the two bridges reproduces the complete original test byte-for-byte;
there is no new clone, reconstruction, test registration or production behavior.

Retained checkpoint: `pass`. Tests +45/-0/net45, production/API +0/-0. All
pre-edit control/subordinate alternatives, transition branches, production
lines, modules/public spellings and current-value owners remain unchanged;
the same normalized laws apply. No arrival history, repeated cause, false
cardinality, nested authority, semantic state boolean or new structural caller
syntax. Complete diff:117 native/142 paired paths; Rust110 paths +2546/-1482/
net1064, production net36, tests net1028, public types +1/-0. The external
initial whole-src-file classifier included inline tests and receives no
production-count credit. Exact receipts are retirement-recovery-family-
{before,source-conservation,inversion,summary}.json and the owner/minimum/
restored logs. This focused test-only batch follows the full0da4c952 gate;
source progress, combined root/family failures, comparable costs, current-source
CI/review/delivery and whole-programme acceptance remain open.

#### Nonempty receipt-source recovery witness

Before editing atc53afad6, retain D-RETIRE-1 and the existing Driver source-return
law: a panic after one receipt is admitted but before its source reply transfers
must preserve the complete original source remainder, original row position,
admitted receipt, actor, exact primary panic and independent cleanup results.
This is a derived affine-custody law/native host policy, not an Agha exception
contract. Exact030 source laws and interpreter are unchanged: LocalEnvironment
keeps SourceProgress across an interrupted offer, Driver keeps source_index,
and ActorRetirement::from_local projects both into the existing public product.

The existing transitive_source_cancellation fixture already executes real
ScheduleAfter interpretation, native timer receipts and source admission. Its
first-offer observer rejection panics after the underlying source reports
Completed(Admitted), before the environment acquires that report. A new focused
test will deliberately close that observer, execute the real native actor,
project its retirement and consume try_into_custody. Required observations:
unfolded actor state0; exact primary panic identity; current source index0;
no acquired source reply; one already admitted accepted id1/gen1; nonempty
Completed(Admitted) holding the untouched accepted id2/gen1; all remaining
custody fields accounted for, successful notification publication and address
retirement. No new fixture type, trait, runtime policy, protocol or public API.
Keep the original cancellation test and every original assertion unchanged.

Falsifier: only in the HostExecutionPanicked projection, replace its original
source with None. The new test must fail its required nonempty-source assertion
in debug/release while the existing cancellation and recovery controls pass.
Restore exact production source and run the new case/control on actual1.99/1.96
in both profiles, plus strict owning-target Clippy/formatting. Two author
reviews must map the complete oracle and actual falsifier; no independence is
claimed. Forecast: this PRD and one existing launch.rs test module, no production
edit, no new public type, one test registration. Baseline/frozen source hashes
are retirement-recovery-source-before.json and its exact source snapshots.

Pre-edit checkpoint:24 logical terminal cases/5+20 tags, one14-field custody
product; all runtime/subordinate states, result alternatives, transition
branches, production lines/modules/public spellings unchanged. Current source
remainder, row index, admitted control event, original actor/cause and cleanup
products retain existing owners. Direct existing sums/products suffice. No
arrival history, repeated cause, false cardinality, nested authority, semantic
state boolean or new structural caller syntax. Cross-check D-RETIRE-1/D-TERM-1,
Driver source-return/retirement law, selected030 source-settlement admission
tests and the same normalized atomic documents. Record measured growth,
source conservation, both reviews and disposition before retaining the test.
Combined root/family cleanup failures, comparable costs and delivery stay open.

Result: the new source case and original cancellation control pass actual1.99/
1.96 debug/release, including final restored runs (two passes per profile,
zero failed/ignored,347 filtered). Discarding only the original source through
source.and(None) in the HostExecutionPanicked projection fails exactly the
nonempty-source assertion at3383 in both current profiles; cancellation remains
healthy and all seven original Counter controls pass in each profile. Exact
projection/environment hashes restore. Strict current all-tests Clippy and
formatting pass. Initial strict Clippy rejected three new empty assertions;
their equivalent length0 checks keep every observation/expectation. Initial
failed lint, source and conservation receipts remain preserved without lint credit.

Both author reviews pass. The first maps real receipt admission, unfolded state,
nonempty source classification/remainder, original row, exact panic identity,
distinct receipt identities and independent notification/address retirement to
their owning laws. The second verifies the actual loss falsifier and healthy
controls; no fabricated residual, duplicated/replayed receipt, weakened original
assertion or production change is retained. No independent review is claimed.
Removing only the new test/import reproduces the whole original launch.rs
byte-for-byte. This witness covers actual native Completed(Admitted) interruption,
not every source phase or an end-to-end Application callback.

Retained checkpoint: `pass`. Tests +160/-0/net160, one registration; production/
API +0/-0. All pre-edit runtime/control/subordinate alternatives, production
branches/lines, modules/public spellings and current-value owners are unchanged.
The same D-SETTLE-1/D-RETIRE-1/D-TERM-1 and normalized laws apply; no arrival
history, repeated cause, false cardinality, nested authority, semantic state
boolean or new structural caller input. Complete diff:117 native/142 paired
paths, Rust110 +2706/-1482/net1224; production net36, tests net1188, public types
+1/-0. Source-qualified receipts use retirement-recovery-source-{before,
conservation,inversion,summary}.json and owning/minimum/restored/lint logs. The
full28/flake result remains bound to0da4c952. Combined root/family cleanup,
operational inspection, comparable costs, review/CI/delivery and whole-programme
acceptance remain open.

#### Coexisting root and family disposal failures

Before editing at4fce7bfb, retain D-RETIRE-1's independent original failures and
the existing Application/Entity disposal policy: completed Work, joined root,
notifications, family shutdown/metrics and each original cleanup cause coexist.
Root cleanup does not subsume family disposal, and family disposal must not
reclassify the earlier root report. These are derived custody/native policy
laws, not actor-model guarantees about arbitrary user destructors.

Source inspection rules out a misleading address-space fault: families retain
the same Arc<Spaces>, so its last destructor is not necessarily root disposal.
Use the existing separately constructed actor service instead. Its actual
ApplicationCapabilities::dispose catches that service's original destructor
panic before releasing its construction owner. The independent family shutdown
captures its definition destructor separately. EntityDefinition::Hosts must
equal Application Spaces; preserve that real typed host, rather than adding a
generic marker/default, wrapper, or compatibility port to force inference.

Add one test-only AccountFactory owning the original root cause until service
construction, AccountService owning its actual disposal cause, and AccountFamily
owning the independent family definition cause and exact service-host type.
Reuse the existing ReceivingAccount, hydration/callback policies, ordinary
Application::new/with_interpreter/entity_family/run_with_entities, completed-work
selection, consuming actor custody and family parts. Construction/disposal
counters prove that the actual root service existed and retired once; this is
a lifecycle resource witness, not an unused callback required by a new API.
No new production abstraction or caller obligation is proposed.

Four cases cover neither fault, root only, family only, and both. Require the
same original non-Send completed Work, root state, nominal family role, all
retirement/notification lanes, exact boxed cause pointers and payload lifetimes
through final consuming discharge. The root remains primary Completed/Stopped;
its native retirement-failure vector contains only its service cause and its
report assesses only root/subtree cleanup. The zero-incarnation family owns its
actual settled empty shutdown/zero metrics and only its own disposal cause.
No claim of admitted-family work is made by this separate composition witness.

Falsifiers are independent: discard only the root service panic instead of
pushing it into native retirement failures, and separately erase only the
family into_parts disposal cause. Both-fault and corresponding single-fault
cases must fail their exact cause assertions; unaffected cases and original
recovery controls must pass in debug/release. Restore every original producer
byte-for-byte and run all four positives on actual1.99/1.96 in both profiles,
strict target Clippy/format, source conservation and two author reviews.

Forecast: this PRD and the existing completed_application_receiving.rs path;
about300 test lines, three private fixtures/four test registrations, production/
API +0/-0. Pre-edit checkpoint:24 logical terminal alternatives,5+20 tags,
one14-field custody product; all runtime/control/subordinate sums, production
transition branches/lines, modules/public spellings unchanged. Surviving Work,
root, report, notifications, family shutdown/metrics/role and two independent
causes keep their existing owners. The factory's optional original cause and
each resource's cause are genuine single-value/absence products, not correlated
phase flags. No arrival history, repeated cause, false cardinality, nested
transition authority, semantic boolean or structural caller input. Cross-check
D-RETIRE-1/D-TERM-1, total application/family disposal laws and the same normalized
atomic documents. Exact baseline/snapshots are retirement-recovery-joint-before*.
Record measured growth, surviving values, both reviews and pass/reopen before
retention. Remaining source phases/Application witnesses, operational inspection,
comparable costs and programme delivery stay open.

Before inversion, all42 existing/new owner cases pass actual1.99/1.96 in both
profiles; strict current target Clippy/format pass. The original entire test
is byte-identical after removing only the added fixture/trace block and two
imports, then pinned-formatting the import wrap. Measured test delta is +352/-2/
net350 (forecast about300); production/API0, three private fixtures/four tests.
Root service omission also changes the root report, so its first required
failure is the root failure-assessment assertion; later exact cause-pointer/
lifetime assertions remain unchanged. The family omission first fails the
new exact family carrier assertion, and also the two existing consuming fault
traces. No assertion or expected value is changed for either falsifier.

Result: all42 owner cases pass actual1.99/1.96 debug/release before and after
both inversions, zero failures/ignores. Strict current target Clippy/format pass.
Root omission fails exactly the root-only/both-fault cases at root assessment;
the other seven disposal cases pass each profile. Family omission fails exactly
the family-only/both-fault cases at original carrier identity plus both existing
consuming fault traces; five disposal cases pass each profile. All seven Counter
controls pass per profile for each mutation. Every producer restores exactly.
No failed test, source input or expectation is removed or weakened.

Two author reviews pass. The first maps actual single service construction/
disposal, completed non-Send Work, root state, role, settlements, reports,
notifications, family shutdown/metrics and independent original causes to their
complete consuming ownership equation. The second checks erasure/swapping/
reboxing and early discharge against pointer/lifetime/report observations and
the actual selective falsifiers. No independent review is claimed. This actual
public Application composition needs no new consuming port or runtime policy.
It uses a zero-incarnation family; the retained admitted-family/nonzero-metrics
traces remain the separate witness for that case.

Retained checkpoint: `pass`. Tests +352/-2/net350; production/API +0/-0; three
private resource fixtures/four registrations. All pre-edit runtime/control/
subordinate alternatives, production transition branches/lines, modules/public
spellings and original-value owners are unchanged. Exact whole owner files are
root producer5936 lines, family producer840, family parts84, root parts3012 and
application parts327 before/after, including their existing inline tests; these
are file counts, not production-only counts. Public type/trait spellings in
those files stay7/14/2/13/2. No arrival-history state, repeated cause, false
cardinality, nested authority, semantic state boolean or structural user input.
The same normalized laws apply. Complete diff:117 native/142 paired paths;
Rust110 +3058/-1484/net1574, production net36, tests net1538, public types +1/-0.
Exact baseline, conservation, inversions/restoration, two reviews and current/
minimum whole-target receipts are retirement-recovery-joint-* in the existing
restart evidence. The full28/flake result stays bound to0da4c952. Remaining
source/Application inspection, comparable costs, independent review/CI/delivery
and whole-programme acceptance remain open.

#### Comparable recovery costs, before measurement

Use the existing isolated pool-memory-report/DHAT workload and the two immutable
030 dependency-graph source archives in restart-2026-11-11/retirement-costs:
upstream814c96d1 and custody0da4c952. Do not edit these snapshots. Latest retained
changes after0da4c952 are tests/docs only; verify that production source agrees,
using the existing exact launch.rs test reversal for its one cfg(test) addition.
No029 measurement is comparative030 evidence, and upstream Native0.2.0 is not
being labelled published. Both sources select the identical root lock/checksums.

Preserve the actual one-root/one-worker FIFO policy, domain worker, input text,
accepted submission/completed job, business result and complete joined native
root/child retirement. Remove the old probe's redundant retirement wait inside
Work in both consumers, matching the current ordinary example. Inspect the
root report after Work; retain every original returned owner through the joined
heap cut and explicitly discharge it afterward. The two source adapters differ
only in the old variant versus Retained/outcome borrowed retirement patterns;
no new runtime code, generic alias, actor, effect or ownership policy.

First validate standalone consumers, known4096-byte allocator calibration and
unmodified business/complete retirement observations on both supported pinned
compilers. Record inferred execution-future, complete outcome, native root and
worker sizes. Independent heap/System modes use the same workload, capacities1
(native effective2) and1024, and source-specific Cargo targets. Preserve every
input/hash and exact graph; use supported Cargo/DHAT/system measurement tools,
without project-owned tooling. Functional builds are not timing samples.

Before timed comparisons, inspect actual competing processes and run repeated
same-source controls to establish noise. Then alternate at least five process
samples per source/capacity/allocator/compiler. Record peak heap/resident memory,
allocation counts, heap at join/after discharge/host stop and complete workload
duration; System timing is separate from DHAT instrumentation. Benchmark clean/
incremental Cargo checks with immutable inputs and fresh source/compiler targets;
keep binary/metadata sizes secondary. Set no invented percentage correctness
gate: this port adds no payload clone/allocation, so new allocation counts or
post-discharge retention must be explained or reopen the model. Enum/future/
task layout changes are measured costs, not assumed improvements. Contended
host timings receive no comparative performance credit. This bounded FIFO case
does not establish catalogue/remote latency, macro cost or all DX26 acceptance.

First functional calibration rejects the upstream adapter: it incorrectly put
the14 custody fields directly in Completed, although814c96d1 already owns them
in ActorCustody. The compiler's E0026/E0027 reject that candidate; its consequent
worker-reference diagnostic supplies no design authority. Correct only the
existing custody nesting/import from the actual upstream source, retaining the
same18 field equations, business code and every oracle. The initial source,
conservation calculation, failed build and custody-only functional controls are
preserved at retirement-costs/initial-pre-correction; none is a comparative cost
result. No runtime type, generic, bound, protocol or workload is changed.

Corrected functional consumers pass both capacities in System/heap modes on
actual1.99/1.96 (eight runs per compiler). Their identical consumer lock is
a054d3fb..., with66 default/81 heap-mode resolved packages/features. Metadata
differs only by the exact extra retirement_recovery test target, which this
external --bin build does not compile; the initial unqualified metadata check
is not credited. All116 crate/src files match latest except the source-conserved
cfg(test) launch addition; no latest-test compilation timing is claimed.

Repeated measurement:40 fresh same-source controls then80 alternating paired
processes pass, five samples per source/compiler/allocator/capacity. Every exit,
business result, complete root/worker custody shape and immutable binary hash
verifies; before/after snapshots show no competing build worker. Both sources
have identical complete heap traces within each compiler/capacity. Current1.99
peak heap is161650/415106 bytes at effective2/1024; minimum1.96 is161874/415330.
Joined heap is28656/current and28880/minimum; both release to8648 before host
drop and56 bytes/one existing Tokio parker afterward,176 total allocations.
These are two immutable030 sources with the same workload; older workload
measurements are not substituted for this comparison.

Measured sizes agree across both sources: execution future8936/current and
9048/minimum, complete outcome3224, application retirement2936, native root2840
and worker712 bytes. System RSS medians range3.8–4.1MB with16KiB page variation;
source deltas do not establish a memory improvement. Peak-footprint readings
and one-job durations are retained separately; control outliers preclude a
general latency claim. No invented percentage gate is imposed.

The allocator substitution fails exactly0 versus4096; wrong business result
fails3 versus4, both exit101/no success marker. A known nonzero32MiB touched
allocation raises measured child RSS1671168→35241984 bytes. Both author reviews
pass; no independent review is claimed. Source/oracle conservation, exact
graph/compiler/binary identities, raw stdout/resources/exits/process snapshots,
120 rows/CSV, noise ranges, controls and limits are in retirement-costs/
{prepared-consumers,prepared-heap-consumers,consumer-conservation,functional-*,
negative-controls,resident-calibration,memory-samples,memory-summary}.

Checkpoint: `pass` for this bounded FIFO memory/layout comparison. Production/
test/API changes0/0; runtime/control/subordinate alternatives, transition
branches/lines/modules/public spellings and existing current-value owners remain
unchanged. The isolated benchmark retains original complete native owners at
the joined cut; no arrival state, repeated cause, false cardinality, nested
authority, semantic flag or user plumbing is added. The same custody and
normalized laws apply. Native source scope remains117/paired142 paths, Rust
+3058/-1484/net1574, production net36, tests net1538, public types +1/-0.
This measures a healthy one-root/one-worker/one-job path, not error-path costs,
remote/catalogue latency, macro cost or full DX26. Clean/incremental checks,
binary/metadata diagnostics, remaining acceptance and delivery are still open.

#### Comparable compiler costs, before measurement

Retain the same immutable814c96d1/0da4c952 sources, pinned1.99/1.96 and exact
consumer lock/work/18-field conservation. Use separate compiler/source copies
of the verified external consumer so measured memory sources/binaries remain
immutable. No production or repository test edit. Five rounds alternate source
order within each compiler. Each clean check starts with an absent Cargo target;
registry/source/Nix caches remain warm and are not called cold OS caches.

For each clean target, measure five unchanged checks as Cargo/cache noise
controls, then five genuine one-file edits alternating the Work input text
between "bombay behavior" and "bombay behavior!". Both have the same accepted
job and three matches; every oracle, policy and type stays unchanged. These
are incremental business-input checks, not no-op checks or wrapper/child-addition
costs. Restore exact consumer bytes after each round. Use the normal development
check profile with incremental compilation enabled, debug info disabled and two
Cargo jobs, identically in both sources. Preserve stdout JSON/artifacts, wall/
user/system CPU, peak RSS and source hashes for every command.

Record before/during/after process snapshots and the measured process ancestry;
an unrelated compiler/linker invalidates comparative wall timing for that sample.
Do not stop another project. Retain contended samples without performance credit;
use paired controls/noise before interpreting a difference. Save dependency/
native/macro metadata sizes from actual Cargo artifacts, and release binary
sizes from the already verified immutable positive binaries as secondary facts.
No invented percentage threshold, faster-build promise or broad compiler claim.
This establishes one representative consumer's clean and small-edit costs;
macro expansion, larger topology/wrapper edits and full DX26 remain open.

Pre-measurement checkpoint: all runtime/control/subordinate alternatives,
production branches/lines/modules/public spellings and custody owners stay
unchanged. Only this existing PRD and isolated measurement inputs/receipts change.
No generic, bound, actor, effect, protocol, no-op policy or new tooling enters
the repository. Same D-RETIRE-1/D-TERM-1 and normalized laws apply. Record actual
controls, restored input identities, reviews and bounded disposition afterward.

Result: all220 checks pass: five fresh-target clean,25 unchanged controls and
25 real edited checks per source/compiler. Cargo JSON confirms unchanged
consumer artifacts are fresh and every clean/edited consumer is rebuilt.
All source/lock identities restore exactly. Current1.99 clean wall medians are
7.71s/upstream and7.39s/custody; edited0.20/0.19s; unchanged0.04/0.04s.
Minimum1.96 clean7.81/7.67s, edited0.20/0.20s, unchanged0.04/0.04s. Clean CPU
medians10.83/10.74s current and11.09/11.02s minimum overlap their sampled ranges.
This shows no clear regression for this consumer, not a general speedup.

Captured process ancestry shows no unrelated build worker. The collector
captures before/during frames and one post-campaign frame, not a separate
immediate post-exit frame for each short command; that limit stays explicit.
Clean peak-RSS medians are about403MB/current or386MB/minimum for both sources;
small-edit ranges and0.01s timing precision do not support a broader claim.
All20 completed task-owned targets are removed after preserving raw evidence,
artifact sizes/hashes and restored sources (2255316KiB total), without global GC.

Secondary diagnostics: Native check metadata3481159→3467936 bytes current,
3386091→3373755 minimum; Engine changes182591→182582 and176467→176458, with
source-path spelling a known confounder. Macro binaries stay5214128/current or
4792512/minimum. The consumer check artifact is zero bytes and is not called a
meaningful binary size. Validated System release binaries4180464→4180592 bytes
current and6140800→6112304 minimum; instrumented binaries remain separately
recorded. These file facts do not stand in for runtime memory.

Both author reviews pass: first verifies identical inputs, genuine rebuilds,
fresh targets, source order and restored originals; second checks process-scope
limits, no-op noise, overlapping timing/CPU/RSS ranges and secondary file-size
confounders. No independent review is claimed. Exact220 command/JSON/resource/
process/timing-HTML receipts, CSV, toolchains, cleanup and artifacts are
retirement-costs/{check-inputs,check-progress,check-results,check-summary,
artifact-sizes}.json and compiler-checks/*. Checkpoint `pass` for this bounded
consumer comparison: no production/test/API delta, runtime/control/subordinate
alternatives, transition branches, modules/public spellings or ownership change.
The same normalized laws apply; no history, repeated cause, false cardinality,
nested authority, semantic flag or user plumbing is introduced. Source scope
and prior Rust counts remain117/142 paths,+3058/-1484/net1574, production net36,
tests net1538, public types +1/-0. This is small business-edit compilation, not
wrapper/child addition, macro expansion, latest tests or full-workspace costs.
Remaining DX26 and whole-programme acceptance/delivery stay open.

#### Incoming schedule main reconciliation

Before integrating mainf09303c8 (PR414) over retainedfa25bbd7, preserve both
complete contracts: upstream's fixed/exhaustive/generated incoming schedules,
persisted replay and CI evidence, and the proven retirement custody/outcome
product. Main changes eight paths, including one existing cfg(test) schedule
owner and its regression seed; it changes no selected package or runtime API.
Retain every upstream delivery, permission, request/reply, retirement and replay
oracle. The sole overlapping Rust change is the already-proven normal-retirement
pattern/import, with the same owned custody/interpretation/source/settlements.
Keep upstream workflow/flake/doc changes and this branch's independent backlog
status. No new semantic type, bound, fixture, expectation or policy is authorized
by compiler fallout. Expected resolution production/API0; feature scope remains
within approved186. Preserve the exact pre-merge branch/source and compare the
whole final schedule against upstream after reversing only that pattern/import.
Run the existing five-case incoming owner corpus in both profiles through pinned
Nix, including finite orders, generated cases and saved replay; retain exact
configuration, source/seed hashes and results. Recheck affected owner gates for
the reconciled source. Earlier full/SDK gates retain their original source limits.

The merge is automatic. Whole-source comparison againstf09303c8 proves every
schedule byte outside that one retained pattern/import unchanged; the six other
independent upstream paths are byte-identical. Backlog status differs only in
this feature's existing DX1 row. An initial Ruby encoding comparison incorrectly
rejected a UTF-8 document; binary comparison repairs the checker without any
source/oracle edit. Exact records are incoming-main-conservation.json. Runtime
and public API delta0; no new transition authority, state, cause, flag or user
plumbing. Existing tests/seed and CI configuration are retained, not recreated.
Verification remains pending on the merged source.

Frozen0e4851b2 passes both existing remote Nix checks: current1.99 debug/release
each528 outer cases/12 summaries/31 UI fixtures, zero failures/ignores; strict
remote all-target Clippy and remote docs pass. Actual1.96 all-feature incoming
corpus passes five cases in both profiles, each180 finite orders and256 generated
successes plus one persisted replay (257 observed completions). Configuration
uses ChaCha/seed20261112 with shrinking enabled, in-process and no Proptest
timeout; the outer watchdog supplies no admission evidence. All718 source/seed
hashes stay exact. Two output roots retain derivers/NAR hashes. Pinned workspace
format and two author conservation/execution reviews pass; no independent review.
PR414's eight hosted checks pass on51d8909a, not on this feature branch. The
retirement representation, production branches and public contracts are unchanged;
checkpoint pass. Exact receipts are incoming-main-*. Earlier full28 and SDK
gates retain their source limits. Completed cache cleanup73077 removes3581232KiB
from five verified task-only build targets, retaining all source/locks/raw logs
and eight separately hashed positive benchmark binaries; no global GC.

#### Borrowing the primary retirement outcome

Before editing production at0e4851b2, DX04 still has one concrete inspection gap:
callers can borrow common custody, but must match ActorRetirement::Retained to
borrow a typed primary outcome. The existing public RetirementOutcome already
owns that exact phase/cause. Derived observation law: outcome() borrows that
resident value while leaving all originals in their retirement. Some means
availability, not success; allocation refusal and the three executor cases
without returned custody have None and keep their complete original alternative.
This is a read-only API choice over the retained model, not a new actor law.

Required inferred syntax is retirement.outcome(), followed by a domain match
on RetirementOutcome::BehaviorFailed or EffectsFailed; consuming access through
try_into_custody still transfers the same original cause, command/request,
receipts and actor. First extend the existing live Counter overflow consumer
with a borrowed original-command pointer observation, before production edits;
the prior API must fail specifically with E0599 for outcome. Then add the smallest
inherent immutable accessor with no new bound, trait, state, wrapper or allocation.
Keep all four unavailable alternatives absent and original owned return intact.
Exercise normal wrapped completion and the unrelated real-file interpreter's
accepted/rejected original requests, retaining every original oracle. No broad
catalogue migration belongs to this design batch.

Expected four existing paths: terminal.rs, retirement_recovery.rs,
interpreter_selection.rs and this PRD; production about17 lines including docs,
public methods +1/types0, no registration/fixture/diagnostic changes. Existing
Behavior/Actors0.30 (628649d), runtime/Driver interpretation, 14-field custody,
24 logical outcomes (five outer alternatives,20 retained primary alternatives),
wrappers and all producers stay unchanged. Pre-edit checkpoint: one aggregate
transition authority and no new control/subordinate alternatives, historical
state, cause, cardinality, flag, nested dispatcher or structural caller input.
One observation match is forecast, with no runtime transition branch. Run focused
callers on both supported compilers/profiles, strict owning lint/format, and an
accessor-only absence inversion; conserve all original test bodies/oracles and
production outside the accessor. Record actual deltas and two author reviews.

The pre-edit Counter caller fails only E0599 for outcome(), with production
unchanged. Its initial new observation also produced E0614 while the method was
absent; standard as_ref preserves its exact expected pointer/bytes and isolates
the missing method. Retain both logs; no type-identity or actor-law change is
claimed. The16-line inherent const accessor borrows the resident primary outcome
and adds no bound, trait, wrapper, state, allocation or visibility change.

Actual1.99/1.96 debug/release each pass nine existing owner cases: four live
Application journeys, one real-executor fixture, three pure unavailable-state
cases and one compile harness with two unchanged static denials. The Counter
command and rejected real-file request are borrowed at their original allocation
before complete consuming recovery; wrapped normal completion is available and
all four unavailable alternatives stay absent with exact original return.
Accessor-only Some→None inversion fails exactly the three intended availability
observations while six named original controls stay healthy in every configuration.
The initial audit incorrectly expected five controls; source and original positive
logs establish nine cases, so six is correct. Preserve the rejected audit and its
unchanged raw current-debug run; only the classifier is corrected. Earlier reports
of eight owner cases are superseded. No Rust oracle is changed to fit that count.

Exact production restoration and final nine-case reruns pass on both compilers/
profiles. Current strict target Clippy, pinned workspace format and denied-warning
docs pass; the rendered signature is the exact const borrowed Option contract.
Only new observer style changes resolve manual_let_else and chain formatting.
Removing only the declared additions reproduces all three original files byte
for byte, including every original assertion, literal and producer/consumer body.
Two author reviews check original ownership/availability and falsifier/controls;
no independent review is claimed. Receipts are borrowed-outcome-{before,regression,
conservation,inversion,summary} and raw per-profile logs in the restart handoff.

Retained checkpoint pass: five outer/20 retained/24 logical alternatives and
14-field custody unchanged; no runtime transition, module or stored cause change.
One new immutable observation match and public method; types +0/-0. Production
+16/-0/net16, tests +43/-0/net43, no new fixture or registration. Owner physical
lines3012→3028; existing test files359→378 and181→205. Same normalized actor/custody
laws apply, with no arrival history, false cardinality, repeated stored cause,
nested transition authority, semantic flag or structural user input. Cumulative
native120/paired145 paths, Rust113 +3124/-1490/net1634; production net53, tests
net1581, public types +1/-0. Earlier incoming/SDK/full28 receipts keep their exact
source scopes; complete programme, final package/CI/review/delivery gates remain open.

#### SDK-independent foreign error spelling

Before editing at95e2436c, retain the project policy that networking SDK types
stay private, while native foreign causes remain their original owned values.
Current public Request::try_into_parts, Export::into_parts and Node::into_parts
still name zenoh::Error, also used in their three private storage fields. Exact
selected zenoh-result1.10.1 defines that alias as Box<dyn IError+Send+Sync+'static>;
the verified std feature makes IError std::error::Error. This is already the
same standard boxed error used by ExportError and other native public causes.
No new error wrapper, conversion, allocation, trait, phase or generic is needed.

The frozen0da4c952 doc source matches all three current files byte-for-byte.
Rendered signatures show an unlinked Error alias, not an SDK hyperlink; the
initial hyperlink assumption was rejected and receives no regression credit.
The observable documentation improvement is an explicit standard boxed error
with its standard Error/Send/Sync links, and source signatures no longer name
the SDK. A Rust compile-fail witness for changed type identity would be false:
identity is deliberately unchanged. Keep the exact selected alias/feature
source as that proof and add no source-mirroring test for this spelling edit.

Change only those six type spellings to Box<dyn std::error::Error+Send+Sync>
using module-scope imports (Error is already imported in two owners). Existing
producer and consuming bodies stay exact; the new spelling's default object
lifetime is the same 'static. Forecast three existing source paths plus this
PRD, one import, public type/method additions0. Cumulative native120/paired145
paths remain within approved186. No actor/runtime control or wrapper order is
changed; all request/publication/withdrawal/close causes keep their owners.

Verify reversible whole-source conservation and actual rendered method
signatures, current/minimum remote owners and existing privacy/typed-completion
denials, strict owning lint/format and no changed Rust oracle/diagnostic. No
behavioral inversion is credited for an identity type spelling. Two author
reviews must check exact alias equality, no conversion/reboxing and public
surface/visibility conservation. Pre-edit checkpoint: all control/subordinate
alternatives, production branches, modules and ownership equations unchanged;
one import and the measured type-only line delta, no new historical state,
repeated cause, false cardinality, nested authority, semantic boolean or caller
plumbing. Same normalized actor/custody laws apply. Exact baseline/source/doc
identities are sdk-privacy-before*. Remaining source/API inspection and final
gates/delivery stay open; this edit alone does not certify all SDK privacy.

The frozen717-input source passes all four existing host checks with
`nix build --max-jobs 1 --cores 4 --no-link --json`
`.#checks.aarch64-darwin.{bombay-remote-debug,bombay-remote-release,bombay-minimum-remote,bombay-doc}`
(the four attribute arguments are expanded explicitly). Current1.99 debug and
release each pass524 outer cases/12 summaries/31 UI fixtures, zero failures or
ignores. Actual1.96 minimum-remote passes3 outer cases/11 UI fixtures; it does
not execute the whole current remote corpus. Existing configuration/query and
actor-only callback denials remain unchanged. Strict all-target remote Clippy,
remote docs, workspace docs with denied warnings, pinned formatting and diff
checks pass. Four output roots retain exact derivers, NAR hashes and raw logs
under restart-2026-11-11/sdk-privacy-*. The earlier28-check full gate remains
qualified to0da4c952; four focused checks do not replace final full-source gates.

Rendered Request/Export/Node extraction signatures now show the exact standard
boxed Error/Send/Sync product with standard-library links. The59 generated
remote declaration/method pages expose no SDK handles, configuration or SDK links.
Source inspection confirms Network conversion is pub(crate), Session fields and
Queryable declarations are private, and Query custody belongs to a pub(super)
owner; this corrects the older claim that raw configuration/query APIs remain
public. This bounded declaration audit is separate from runtime/privacy policy.

Author review one confirms selected alias/std-feature equality, identical static
types/lifetimes, six type-only substitutions and exact producer/consumer bodies;
there is no conversion, reboxing or additional erasure. Author review two checks
all three rendered signatures, original cause ownership, visibility and existing
denials, with no changed test/fixture/expected diagnostic. Neither is independent
review. Retained checkpoint: actor/control/subordinate alternatives, transition
branches, modules and public spellings unchanged; six representation-equivalent
positions and one import only. Same normalized actor/custody laws apply; no
arrival history, repeated cause, false cardinality, nested authority, semantic
flag or structural caller syntax. Disposition pass for this spelling batch.
The three owner files measure1452→1453 physical lines, three modules, nine named
public types and eleven public methods before/after; their24 match sites and
every transition body remain exact. These physical counts include documentation.
Production +7/-6/net1, tests +0/-0, public types/methods +0/-0. Cumulative native120/
paired145 paths, Rust113 +3065/-1490/net1575; production net37, tests net1538,
public types +1/-0. Full programme, final registry runtime delivery and CI remain open.

#### Current runtime archive verification

Before packaging the retaineda2ef6ce5 production source, reuse the established
three-package Cargo workflow: `cargo package --locked -p bombay-engine`
`-p bombay-macros -p bombay-rs --all-features`, through pinned Nix on actual1.99
and1.96. These are unpublished Engine0.3.0/macros0.2.0/runtime0.2.0 candidates;
no publication is implied. Keep the source clean and frozen while each package
set is produced/verified. Inspect actual archives, normalized manifests/locks,
VCS records and every shipped production byte against the selected Git source.
Only these three unpublished packages may be locally staged/patched for joint
verification; Behavior/Actors0.30 and macros0.16 remain registry selections.
Then exercise extracted-package callers, preserving current inferred syntax,
original retirement/failure oracles, facade/renamed use and executable examples.
This verification changes no production model, dependency, expected diagnostic
or caller policy. Existing source tests and package consumers remain immutable
inputs; use isolated task-owned caches and retire them after preserving evidence.
Older archive receipts retain their exact source/compiler limits and do not
certify this candidate. Final repository/CI and full programme gates stay open.

The initial clean e41da292 current-compiler package set passes all-feature
verification, with all shipped src files exact: Engine3, macros2 and runtime108
(including source-local test modules, not a production-line count). VCS records
identify e41da292. Those exact archives/logs are preserved as the pre-guide
preview. Inspection finds a concrete documentation defect: the packaged guide
still teaches the removed ActorRetirement::Completed shape. Correct that prose
to borrowed outcome inspection and consuming the complete custody/outcome pair;
retain all 24 logical alternatives and exact unavailable return. Remove the
redundant text-only Work sketch using an undefined boundary function; compiled
examples/doctests and their assertions stay untouched. This adds the existing
guide path to cumulative native121/paired146 within approved186, production/API0.
Produce fresh final-source archives after the correction; the earlier set does
not certify final documentation or package delivery.

Corrected clean706810b8 passes the same joint all-feature package verification
on actual1.99 and1.96. Both produce byte-identical archives with VCS706810b8:
Engine28 files/3 src, macros7/2, runtime535/108. All shipped src and the runtime
guide match the frozen source; source-local test modules are included in src
counts. All718 original input hashes stay exact. Extracted570-file source sets
remain immutable throughout the following consumers; no registry release occurs.

The isolated workspace retains all28 current Rust files across eight canonical
examples and the renamed consumer. Nine manifest adapters select exact0.2.0 via
the actual three candidate archives; Core/Actors030 and macros016 remain registry
packages. Native tests run an actual archive copy with its source/test/readme
bytes unchanged. Initial CLI-only Engine/macros patches do not reach trybuild's
child Cargo, causing a setup failure after Application42 and real IO pass.
Keep that failure separate. Trybuild's existing manifest-patch propagation and
Cargo's [standard patch configuration](https://doc.rust-lang.org/cargo/reference/config.html)
support appending just those two unpublished candidate overrides to the isolated
test manifest; production, fixture and expected-diagnostic source stay untouched.
The original archives keep their original normalized manifests too.

With that setup, both compilers pass all eight examples/all-targets, native51
per debug/release profile (Application42, interpreter2, retirement7), including
both original interpreter denials, renamed3, six terminating local examples and
ten actual all-feature packaged doctests with zero ignores. Counter/pool output
markers retain the actual1/3 business results. HTTP and remote examples compile;
their server/process execution is not credited by this corpus. Minimum example
checking reports the existing unfulfilled dead_code expectation in the unchanged
remote process controller at processes.rs:26; no minimum warning-free claim or
test/source suppression is made. Both prepared locks,570 extracted files,533
native-copy source/test/diagnostic/readme files and28 caller Rust files stay exact.
Only the native-copy manifest patches and prepared lock differ by design.

Author review one checks VCS/source/guide and nominal dependency selection;
review two checks all exact test/fixture counts, real outputs, unchanged inputs,
setup-failure attribution and unexecuted HTTP/remote/platform limits. Both pass,
without claiming independence. Production/API delta0; model/control states,
branches, modules and all ownership laws unchanged, checkpoint pass. Receipts
runtime-archive-{packages,consumer-inputs,consumer-locks} and runtime-consumer-
{current,minimum}-summary retain exact inputs, commands/logs and qualifications.
The frozen706810b8 archive/consumer proof does not replace final full-source
Nix/CI/review/delivery or close every remaining DX01–DX28 criterion.

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
