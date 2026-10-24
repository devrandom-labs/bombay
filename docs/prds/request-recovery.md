# Recovery after invalid requests

Status: active recovery implementation within Static remote actors R09
(ID-10/AUTH-03). This record owns the narrow liveness conflict; the parent
remote PRD and literal acceptance row retain every broader requirement.
Original source baselineb19ca648cec41a9f971b6a74e4fcbdec13990568 is a
research checkpoint. Current main50e0de42 contains delivered PR397–403;
this recovery implementation still awaits its own delivery.
Those independent delivery receipts supply no additional recovery evidence.

## Problem and retained contract

Literal R09 and the resource table require eventual valid progress after
invalid-ID pressure stops under declared fair-capacity assumptions. Original
Export deliberately reserves an append-only lifetime Request row before any
provider invocation. Every acquired refusal counts; once rows reach the
configured maximum, later native Queries are discharged without verification.
Existing quota tests correctly establish that selected lifetime law. They use
an always-refusing policy or repeat one canonical request, so they cannot
establish invalid-then-valid recovery. No accepted fair-capacity assumption
currently authorizes permanent suppression by invalid traffic.

Preserve exact original causes and typed dispositions, bounded memory/work,
R12 replay denial, R25 diagnostic conservation and normal Node ownership.
Do not reset/reopen the Node, silently delete refusal facts, manufacture a
provider result for an unverified query or weaken the literal recovery promise.
The original stage selected no repair. The reviewed implementation decision
below now selects an explicit retention policy; acceptance remains pending.

## Prerequisites

Fresh review034c2ff2 and input ledger5696c4a3 verify current b19 before edits;
root independently rechecks every listed input. Complete selected Behavior
instructions760/ec0ad868 and relevant APIs/docs/tests are freshly read.

| Owner | Exact selection and retained role |
| --- | --- |
| Behavior/Actors |0.29.0/55dbac2d; macros0.16.0. Pure Counter Actions and existing shutdown template. |
| Address |0.3.0/9f058dc0. Certify actual actor recipient; numeric origin is not authentication. |
| Communication |0.1.6/963067e0. Distinguish admission from processing; retain actual reply/closure. |
| Observe |Local0e63b34d. Existing independent native outcomes and notifications; no new observer. |
| Timers |0.2.0/9369d1f6. Unchanged generation owner; watchdog is not progress evidence. |
| Entity/Driver |Local b19. Empty families and sole causal fold/retirement composition. |
| Native |SDK1.10.10/c34eb350, config1.10.2, TLS1.10.4, protocol1.10.1, Tokio1.53.3; explicit TLS only. |

Root lock83f01ca3, no owning patch override. Current quota/peer/wire/protocol,
Node/Export/provider custody and native Query/FINAL contracts are verified.
There is no prerequisite blocker to this finite failing-law comparison.
At the original preparation checkpoint compiler fitness was unexecuted and
production was blocked pending the intended failure and a policy resolution.
The subsequent implementation/execution sections supersede that checkpoint.
Reuse standard Results and the already selected Tokio oneshot; no dependency
selection, competing actor/runtime, clock or test framework is needed.

## Smallest failing law and change record

Use the same live public Counter Node/export for two actual native Queries.
First send an unregistered proof pair through the existing deterministic
provider; after its real native EOF, stop invalid traffic and submit a
separately registered permitted IncrementAndReply. Compare retained_requests1
with matched2; both reserve two queued/native query slots and the same fixed
TLS profile. EOF is not proof that a local native-query count permit retired.
No positive is credited from an always-denying provider or reopened export.

After both actual Query streams finish, an existing oneshot wakes Work to
perform an independent local Counter Read, then normal shutdown. Work moves
its actual shutdown Result through a second oneshot to the private peer;
only after joined Node close does that peer close. Retain exact native replies,
all row/provider/admission/publication causes and root/notification outcomes.
The matched2 must execute the valid increment and read1. Under1, source
predicts one invalid row, one provider invocation, no increment and local0.
Only the last independent local Read1 oracle supplies intended failure credit;
all source-specific row/cause, native EOF and cleanup controls precede it.
A compiler/setup/watchdog failure supplies no semantic credit.

Reuse existing node_exports/quota.rs, peer.rs, wire/protocol, real Counter,
Application, Node, Export, Tokio oneshot and standard Result products. The
private quota peer accepts a finite pair instead of repeating one input, and
its existing shutdown hook awaits the actual result. Original quota controls
supply two exact input copies and wrap the same shutdown in an async closure.
No new test controller, public interface, runtime, dependency or framework.

Expected scope: one new owning recovery.rs, its consumer registration, the
existing private peer and quota caller, this record, a parent PRD pointer and
two backlog pointers; eight paths. Expected production+0/-0/net0, roughly
300–400 authored/modified test lines, public types+0/-0. Numerical budgets are
removed. Measure complete tracked/untracked deltas at each checkpoint; do not
mix unrelated audit defects or legacy retirement into this law.

## Acceptance and remaining work

Review the new test twice, including independent counterexamples; first
execute matched and constrained cases in both debug and optimized builds
through pinned Nix. Preserve actual selected source, profile, native cleanup,
intended failing oracle and exact input identities. Only that original
failure may select a concrete recovery policy and smallest repair. Record
ordinary-Rust alternatives, owners and API consequences before production.
Then repaired paired positives, owning quota/consumer regressions, intended
counterfactual/restoration, appropriate full checks, source minimization,
focused PR, required new-head CI and protected main delivery remain required.
This one law does not establish complete flood/CPU/cache/delegation bounds,
R12/R14/R25 or the full remote PRD. The three code-body scans and their
current-source reviews are recorded below; their separate findings remain open.

## Original execution and policy blocker

Reviewed sourcee7b2e03d becomes formattedfe2094d1, with private peerbfd23bc1
and quota1bccc649. Two source reviews resolve the unexecuted initial proposal's
temporary native-result borrow and incomplete local drain before application.
Reviewf3b116fd qualifies the amended source;80a2cff8 confirms formatting only.
No compiler/setup failure is credited as a regression.

Actual original debug and optimized commands each select exactly two cases:
matched2 passes, constrained1 fails ONLY the last local Read oracle at376,
actual User(MailAddr0,0) versus required1. Both exit101 with1passed/1failed,
zero ignored and43 filtered; runtime0.04s/0.03s, compilation16.41s/1m02s.
The invalid row, exact failed verification and inputs, truthful capacity
disposition, matched native increment431/reply proof and all real cleanup
controls run before that oracle. Ten selected input hashes match before and
after each profile. Raw logs2ae4f062/9e511496 and independent paired review
fc5a67c1 retain the original source epoch; no production change is present.

The pinned-shell commands are `cargo test --locked --offline -p bombay-rs
--all-features --test application_clients node_exports::consumer::recovery::
-- --nocapture --test-threads=1`, with `--release` for the optimized run.
This establishes the narrow lifetime/recovery conflict, not an accepted repair.
Reopen only the needed retention/discharge policy. Required bounded custody,
original facts, replay and private SDK ownership remain mandatory.

Complete current working tree against b19, including the untracked381-line
test: eight paths; production+0/-0/net0, tests+399/-9/net390, records+148/
-0/net148; public types+0/-0. Full main-relative inherited source is separate.

The shared Git `session-archives/2026-10-21/request-recovery-original-20261024.tar.gz`
preserves the original failing pair, sources, prerequisite/input manifests and
reviews before any repair. It qualifies no repaired source or CI delivery.

## Reviewed implementation decision, before production edits

Original checkpoint079938dc and capsule1ac274e4 remain immutable. Independent
critical reviewb47af58a accepts the candidate only with four concrete ownership
refinements, all adopted here. The amended outside selection records the full
ordinary-Rust comparison and five abstraction questions. No wrapper, builder,
trait, macro, dependency or second Export implementation is needed.

Add one explicit public `RefusalRetention::{Retain, Discharge}` enum and a
required `refusals` field to existing `ExportLimits`, without a default.
Retain preserves lifetime charging and exact later cause inspection. Discharge
deliberately surrenders completed unkeyed ordinary refusal originals in return
for reusable capacity; remote-counter selects it. Existing cause-inspection
consumers select Retain. This is new capability, not production code reduction.

Reuse Export's bounded pre-reserved vector as `Vec<Option<Request>>`; choose
an existing vacancy before rejecting capacity or appending. Never move,
compact, release or reuse a position that issued any canonical/duplicate key.
No map, free-list, mirrored occupancy counter or generation is introduced.
Consuming extraction keeps its existing `Vec<Request>` product, omitting
vacancies. Under Discharge this storage order is not acquisition chronology.

A release candidate must have completed, surrendered its actual native Query,
remain Unassigned and contain no acquired factory/poll/disposal panic in any
of the six slots. Take it during a short entries borrow, end all loans, and
reuse existing `Request::try_into_parts` to establish sole custody. Restore
the SAME Request at the SAME position on Err, without an intervening await or
user call. Only Ok transfers all real originals for explicit destruction.
Completion alone is not proof that producer loans have ended.

Discharge runs only in serving's completion path after publication, inside
Node's caught execution poll. Slot removal and all entries/custody loan ends
precede user Drop. Do not discharge in drain, withdraw or extraction: Node's
later withdrawal is outside that catch. Preserve any actual caught poll panic
and independent root, family, notification and connection-cleanup facts.
No second hostile-destructor-panic guarantee is implied.

Exact blocker: original same-Node invalid-then-valid N1 fails local0vs1 while
matched N2 passes. First repair that law, with a borrowed exactly-once original
error Drop probe and its genuine provider result. Retain's existing move-only
cause controls remain unchanged. Re-run a real Retain/no-release inverse in
debug and optimized builds, reaching local0vs1 after all cleanup; do not give
unreached later assertions inverse credit.

Add the causal key regression before retaining this storage change: A's real
verifier remains pending, B completes canonically, A refuses/releases, C uses
the vacancy, then duplicate B must preserve B's actual key and genuine cached
protected bytes. Compare actual keys, exactly-once B execution and independent
Counter2 after complete cleanup; never predict physical positions/nonces.
An actual compaction inverse must fail its intended key oracle. Also verify
live producer custody prevents release, acquired producer panics retain their
facts under Discharge, and a safe borrowed user-error Drop panic is caught
once by the existing Node with independent cleanup preserved. Two source and
outcome reviews are required for changed tests, including an independent pass.

Conditional progress requires an ordinary unkeyed refusal to complete without
an acquired producer panic and with no surviving private custody loan. Issued
keys and panic evidence still occupy finite lifetime capacity. This does not
close R09 accepted-cache/delegation/flood/CPU bounds, R12, R14 or R25.

Expected owners: remote/export/mod.rs, export/query.rs and curated remote
reexport; existing Export caller configurations and diagnostic fixtures,
remote-counter deployment and current guidance. Roughly28 caller files,
30–40 paths in total; forecast production+80/-25/net55, public types+1/-0 and
one required field. Reuse native peer/protocol, actual Counter, existing
provider/custody/Node owners and ordinary Results; no new runtime or tooling.
All previous original-stage costs remain recorded. Numerical budgets are
removed; complete tracked/untracked production/test/API measurement remains.
Focused debug and optimized regressions precede broader production migration,
full checks, minimization, new-head CI and protected main delivery.

## Initial repair and actual release inverse

Initial productionde33afb2/d6605b40 with recoveryb010c633 passes both actual
N1/N2 traces: debug2/0 (17.56s compilation,0.06s execution) and optimized2/0
(59.82s,0.03s). All selected source/input guards pass before and after each.
Independent two-pass source review8494a842 finds no veto. These positives do
not qualify pending stable-key, live-custody or panic-containment requirements.

The actual production-call inversec858d81a disables Discharge's release while
retaining the same verification/native/publication path. Testf433da9d differs
only by binding expected User products outside assertions. Both profiles
select EXACTLY the constrained N1 case and exit101 only at recovery374,
actual local User0 versus required1 AFTER all native, Work, root, notification
and connection cleanup. Debug16.60s/0.03s; optimized1m03s/0.52s. Both select
one failed case, zero ignored and44 filtered. Later healthy-only counters and
Drop oracles remain unexecuted and receive no inverse credit. Raw logs
f4786681/99eb9dd5 and exact pre/post source guards preserve that epoch.

Healthy d6605b40 is restored byte-for-byte before further changes. Private
live-custody testbe315b85 now exercises existing affine extraction using a
deliberately private completed/no-Query row and an actual extra Rc lease:
while leased, the exact request stays in the same position; after the last
lease ends, discharge vacates it and destroys its original refusal exactly
once. This is a custody mechanism law, not native completion or Node evidence.
Source reviewa864f890 accepts both passes. Debug passes1/0/427filtered;
optimized execution and restored public pair qualification are still pending
in the same guarded pinned runner at this record's checkpoint.

The ordinary remote-counter and its existing process fixtures now explicitly
select Discharge; their accepted keyed operations retain their existing facts.
Current API/capability/example guidance states both meaningful choices,
conditional recovery and extraction ordering. This is required caller/API
migration after focused N1/N2 debug and optimized positives, not unrelated
legacy cleanup or full acceptance. No new numerical budget applies.

## Extended focused positives and next falsifiers

The completed custody/restored epoch is independently sealed asdaa97bee:
custody1/0/427filtered and the real N1/N2 pair2/0/43filtered in each profile,
with all26 source guards passing. This does not transfer execution to later
source revisions.

At that checkpoint recovery8826c7e2 adds the causal held-A/B-key/C-hole/duplicate-B law
and the ordinary framing/scope cases. Source author reviewsa00fce16/eabee425
and independent key review7f8e1669, refusal extension review and current
source reviewbe0033f7 find no veto. Existing private peer methods merely become
visible to their private sibling. Unwinde18ff78a adds the safe borrowed
original-u64 refusal Drop panic trace, reviewed against proposald26405b6.
Refusals45fe5abe deliberately selects Discharge for its six producer-panic
stages while every ordinary original-cause inspection control keeps Retain.

Actual debug and optimized each pass the same19 scoped cases: recovery5/0
(43filtered;0.08s/0.03s), existing refusals13/0 (35filtered;0.13s/0.05s),
and Node refusal disposal1/0 (428filtered;0.02s/0.02s). The last optimized
build takes3m20s. All per-command source guards pass; independent two-pass
terminal reviewf9c74b2c binds all six receipts and exact current sources.
Positive key/proof/pointer/cache/drop oracles all execute. No unfiltered suite,
full R09, CI or main-delivery acceptance is implied.

Framing sends an actual complete array, refused before verification. With
Discharge its exact JSON cause is deliberately no longer inspectable; native
EOF, verifier count and independent progress are the observable controls.
Only a real retained no-release row can expose that original OuterObject
cause. Scope uses a genuinely registered wrong-actor pair and the existing
full current-scope check, moving its real Invalid cause through the borrowed
Drop probe. No separate checker manufactures a replacement cause.

The following plan preceded the actual source cuts recorded below. Execute
independent cuts one at a time,
with exact guarded sources, debug and optimized failures, full cleanup before
the named oracle, exact restoration and fresh positives:

- Compact vacancies once at END of discharge, outside its fixed-index loop.
  This moves B only after A is removed and before C arrives. The intended
  first key oracle compares the actual optional duplicate key with actual B's
  key. Later cache/key/count oracles receive no negative credit if unreached.
  The earlier suggested end-of-publication cut is rejected before execution:
  it runs before A's vacancy exists and might never compact a hole.
- Discard the SAME Request returned by failed sole-custody extraction instead
  of restoring it. The private custody test must observe a missing original
  position after all leases/export disposal, rather than Some(true).
- Disable the ordinary release call for the three constrained proof/framing/
  scope cases. Each must reach actual local0vs1 after cleanup; retain the real
  JSON/scope originals in that counterfactual rather than synthesize causes.
- Discard only Node's actually acquired poll panic before its ordinary kernel
  disposal/join/withdrawal/close. The safe Drop test must reach the missing
  original poll-payload oracle after cleanup. Later pointer/drop checks are
  unexecuted in that cut. This is conservation sensitivity, not a second panic
  guarantee or a claim the original pre-policy code exercised discharge.
- Omit the verification-disposal panic guard for the existing refused-result/
  destructor-panic case. Its actual original request and independent acquired
  panic must remain retained under Discharge; loss must fail after cleanup.

At that planning checkpoint, outside reviewed cutsbcaeb974/99a298f0 and their
reviewbe0033f7 were proposals, not executed inversions. The later actual cuts
below supersede that execution status. No new permanent validator or layout-printing
test is introduced. Vacancy/discharge scans are bounded O(N), and the vector
retains its high-water allocation. No unmeasured layout, speedup or aggregate
memory improvement is claimed.

Checkpoint before this record: all36 paths including untracked custody.rs,
against original b19: production+103/-20/net83, tests+1460/-40/net1420,
records+276/-0/net276; public types+1/-0, plus one required field. The four
query test-registration lines are attributed to tests. Complete ledger is
extended-census-20261024.tsv. The patch tool's unexecuted backup14d875dd was
moved outside the project before this census; no required source is deleted.
Earlier original work and rejected proposals remain recorded. This positive
production delta is capability growth, not cleanup. Later record/caller/
minimization changes require a final full census.

## Actual five counterfactuals and restored source

Each independent cut below actually exits101 in both debug and optimized
builds at its intended first oracle, after the test's real cleanup. The32
listed input hashes match before and after each command. Later assertions
are unexecuted in failing cases and receive no negative evidence credit.

| Sole causal change | Actual first failure; selected failures/filtered |
| --- | --- |
| querycf630549 compacts after discharge | recovery825: duplicate key None versus actual B key;1/47 |
| queryf9c05ee4 disables ordinary release | recovery483: independent local User0 versus1 in proof, framing and scope;3/45 |
| query2a4a5d79 omits verification-disposal panic guard | refusals949: original refused request missing;1/47 |
| query8c467990 drops failed sole-custody extraction | custody93: original position None versus Some(true);1/428 |
| Node8e6fdf0a discards its acquired poll panic | unwind1026: original poll payload missing;1/428 |

The framing/scope no-release cases inspect their actual retained OuterObject
and Invalid originals before the final progress oracle. The private custody
case remains a synthetic completed-row/Rc ownership law. Node's panic case
does not establish a double-panic guarantee. Independent two-pass receipts
c82763a0/c0185caf retain these precise source and execution boundaries.
Every changed production file is restored by byte comparison before the next
cut. No intervening positive runs are invented: the final shared healthy
workspace matrix is the restoration control for all five cuts.

Final source review found one incorrect caller migration before its execution:
the denied-Stop inspection fixture needs Retain to preserve its three original
rows and Proof error. Its assertions stay intact; the shipping application
keeps Discharge. Amendment review28898e32 confirms that correction, removal
of a redundant inherited Retain field, and the equivalent query if-let and
explicit unit patterns. The final recovery978c5f31 uses the same two-byte
array and whole Ready value; earlier8826 negatives are not relabelled.

Final query007abdca/custody283db33a/recovery978c5f31 and all35 selected inputs
pass strict pinned-shell workspace/all-target/all-feature Clippy, exit0 in
9.49s. Two earlier lint exits concern style only and are preserved rather
than credited as semantic failures. The full pinned-shell command
`cargo test --locked --offline --workspace --all-features` exits0, including
429 owning library cases,48 application-client cases and the separate
remote-counter process tests. The same full command with `--release` also
exits0, after17m42s compilation. Both profiles pass429 owning library cases
and48 application-client cases, including every recovery/key case. Separate
remote-counter main3 and process8 outer cases pass; its four ignored child
registrations are invoked by their exact-named parents. Other deliberately
isolated child registrations remain identified in the raw workspace logs;
no zero-selected or ignored case is relabelled a direct pass. All four
pre/post selected-input logs contain35 successful checks. Full debug log
a289a38a and optimized5b23d45e qualify this final healthy source epoch as the
shared restoration control after all five cuts, not between them.

Three independent immutable850 code-body scans now each include full reads
of all32 changed Rust bodies,22,074 lines, with every current hash matching
manifest642ebbc6. Unchanged body credit is limited to byte-identical selected
files. Receipts5ffdb358/9401bb88/bc86f98c record scope and historical-prose,
opaque-fixture and generated-content exclusions. No new R09 source veto is
found. Separate unexecuted Entity/preparation/correlation/Observe-reentry
hypotheses and current-guide/test debt remain open, without mixed production
repairs. These scans are source review, not execution, full PRD acceptance,
distillation or main delivery.

Final pinned-shell formatting also exits0. Local `nix flake check --no-build`
evaluates the ordinary checks but exits1 on the optional Darwin Miri shell's
missing local store derivation4c5593l3-channel-rust-nightly.toml. Retrying with
evaluation caching disabled returns the same failure. Neither run executes
the build gate or supplies semantic acceptance evidence; original logs remain
preserved. No repository/toolchain change is selected from this environment
failure. The submitted recovery head still needs its own complete required
Nix CI gate and normal protected delivery.

The updated observation-disposal record in this change only records actual
PR403 delivery, all-six-green CI and complete tree identity. It adds one
documentation path, no unrelated production repair. Its separate capsule
c3b4a2c0 preserves44 payloads; root independently verifies all44 and recovers
the complete premerge tree plus patch into the actual703-file merged tree,
including modes. Original source/failure/Miri scopes remain separate.

## Final containment and delivery boundary

Retain the existing Export, Request, consuming products, provider stages and
Node interpreter. One closed RefusalRetention enum expresses a genuine choice
about surrendering original ordinary refusals. One required ExportLimits
field makes that policy explicit without a second constructor or default.
The sole optional-slot vector owns vacancies; failed sole-custody extraction
returns its exact request to the same position. There is no second counter,
free-list, allocation arena, generation, wrapper, trait, macro or dependency.
Key-bearing and panic-bearing rows stay occupied. Scans remain bounded O(N)
and retain high-water allocation; no aggregate-byte or speedup claim is made.

The ordinary counter application chooses Discharge. Exact original-cause
inspection fixtures choose Retain; production-panic conservation fixtures
deliberately exercise Discharge. This one implementation supports both
explicit policies. No verification controller enters example application
code or a Behavior fold. The original counterexample, actual five cuts and
reviewed source epochs stay preserved; no test oracle is weakened.

Final complete census against originalb19, including staged new custody.rs
and all tracked/untracked changes:37 paths; production+103/-20/net83,
tests+1459/-40/net1419, records+496/-12/net484;
public types+1/-0 and one required existing-product field. Query's four
test-registration lines count as tests. This is positive production capability
growth, not code reduction. Final two-pass source/outcome/minimization
amendmentff8555d9 accepts focused commit/PR preparation, with full R09 and
distillation still unclaimed. The shared Git archive
`session-archives/2026-10-21/request-recovery-qualified-20261024.tar.gz`
retains the exact qualified sources, original cuts, raw outcomes and reviews.
Exact submitted-head
Nix/other CI, independent delivery review and protected main merge remain
required. Full R09/cache/delegation/flood/CPU, R12/R14/R25 and the other remote
requirements remain open.
