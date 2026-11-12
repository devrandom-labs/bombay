# Incoming request schedule

Current checkpoint: PR411 delivers the fixed incoming schedule; PR413 now
delivers the paired Behavior0.30 prerequisite. The exhaustive/generated stage
below starts from main814c96d1 with fresh owner verification. Host extraction,
all180 finite orders, generated/shrunk replay and the narrow Nix seed proof
qualify locally below. Routine/extended CI is source-reviewed; actual hosted
CI and reviewed delivery remain open. Earlier checkpoints retain their epochs.

Status: active for complete R24; the first fixed incoming schedule is delivered
within Static remote actors. Original baseline is merged main
`937fecc3f7b7bc5efe5dd7519e1bf223af76c350`. Complete network simulation,
native Query/Client overlap, restart and full R24 remain open.
The user's complete R01–R25/AUTH1/NET1 goal remains unchanged.

## Exhaustive and generated incoming schedules — 2026-11-11

This pre-edit record follows reviewed PR413 delivery and normal worktree
removal. Starting source is main814c96d16d23045065650080a9c7e82ed136680b;
3d485fae records its independently reviewed delivery reconciliation only.
No shipping or test source changed before this record. The selected host is
crates/bombay/src/remote/export/schedule.rs, mounted as query::schedule.
Its SHA256 isfa52c802b7f248a741c889c776ed4c8433388251086d6e7b0dcc59558555e4a6;
request owner query.rs isdfaae0a0a533f2485dc8ae0dcbc1850cc807ed2be12b8781eeee200d3a46a4ed.
Declaration source remains5dae08876623893b236a2d4487ed8063b4fb5ec7a6e69b10333d6a37e33f4eb9.

### Fresh selected owners and eligibility

Root Cargo.lock9bb2db13348df5df86703c3d67abc8a8c580e9dee2f0533bc80e11d358c6d394
selects registry Behavior/Actors0.30.0 at628649da014a527d47f21567a859c5c78494df13
and Macros0.16.0. There are no patch overrides. Root and all three reviewers
reread the complete selected775-line AGENTS before this feature's design;
its SHA256 isaa5c4e70e38e52a1fbae8948ba30ee1c16fe88eddc3ec4fe8bfcf07f494a9207.
Fresh source/API/test/document reads verify pure Behavior/complete Actions,
Actors' existing shutdown composition and the real Counter's typed reply fold.
No Machine or template policy is reproduced in the test scheduler.

Fresh registry/archive/VCS inspection selects Address0.3.0/9f058dc03d1239e1ef5c3147a893b134a1e74a83,
Communication0.1.6/963067e0e6bdef15549088966cde354fd820be7e and
Timers0.2.0/9369d1f6c7fcda91e7de5bf3c19e6769f92145bd; none depends on Behavior.
Address owns exact registrations/opaque resolution (reservations/semantics
tests); Communication owns FIFO, closure and original refusals (mailbox
retirement/lane lifecycle tests). Timers owns queue-specific generations and
explicit clock inputs; this logical test clock supplies no native timer proof.
Fresh local Observe at814c96d1 preserves exact-generation publication and
independent cancellation through its contract/future-cancel tests. Engine's
D-TURN-1/D-RETIRE-1 preserve causal work and surviving original custody.
Relevant runtime/module/Driver contracts and actual preflight/request/tests
were checked. Fresh primitive evidence is temporarily at
/tmp/bombay-r24-owner-audit.FF5Ei1; it must be sealed with final source/results.

Export::request remains the sole real verifier/canonical lookup/construction/
final-permission/nonwaiting-admission owner. The existing test privately inserts
query=None rows and uses actual Application/Counter ownership. It does not
exercise native callback capacity, Query publication, Client or a Session.
There is no owner-contract blocker to this scoped incoming campaign. The
backlog AUTH1→NET1 dependency remains reciprocal and incomplete; source fitness
does not unblock their entire release scope.

### Law and ordinary-Rust experiment

This is derived test policy, not a new actor-model law. Reuse the six externally
named blocks Initial, Repeat, Expiring, Revoked, ConstructorExpired and Fresh.
Initial precedes every Repeat; Expiring precedes ConstructorExpired. The latter
two remain indivisible blocks setting time10 then20. Revoked may occur at0,
10 or20. Every legal word must produce the independent FIFO Counter Read2
after actual joined cleanup, before detailed trace/custody assertions.

First parameterize the existing ordinary async host with borrowed delivery
word and expected Event slice. Preserve the original test's complete literal
word AND literal expected trace. Derive actual schedule blocks mechanically;
derive expected release order from the input word before execution. Replace
keys.first()/positional comparisons with an actual-key map named by Initial,
Fresh and ConstructorExpired; every Repeat must equal Initial's actual key.
Retain every original allocation/byte/proof, typed row, provider completion,
attempt, refusal, panic slot, notification and native-retirement check.
One authoritative bound8 governs issuer/limits and validation before row push:
each non-Repeat label once, Repeat1..3, length6..8 and both precedence laws.
No capacity increase, predicted nonce, copied interpreter or extra wrapper.

Review this first extraction twice, then run its focused fixed regression in
debug and optimized builds, plus both existing canonical/final-permission
source cuts and byte-exact restored controls in both profiles, before adding
campaign code. Root alone runs Rust
through the existing pinned Nix environment/shared external target. Root owns
locks, Nix, CI and records; acceptance owns schedule.rs; the existing two
reviewers independently check source laws, counterexamples and actual results.

Then enumerate all180 unique base words using ordinary bounded position loops:
15 Initial/Repeat pairs ×6 Expiring/ConstructorExpired pairs ×2 remaining
Revoked/Fresh orders. An independent180-cardinality/uniqueness/validity check
must reject omission of one legal word. This proves finite grammar coverage,
not arbitrary networking. A pure expected-trace function consumes only external
chronology and authored block laws, never live grants/permission/admission
outcomes. The original literal remains an independent control.

Existing direct dev dependency proptest1.11.0, checksum4b45fcc2344c680f5025fe57779faef368840d0bd1f42f216291f0dc4ace4744,
supports TestRunner, sample::select, sample::Index and Vec shrinking. Exact
source/primary documentation confirms MIT/Apache2 and manifest Rust1.85;
its README describes passive maintenance. No dependency/feature change is
needed. Compatible itertools0.14 would add a direct dependency edge and generate
720 vectors to filter180; the direct grammar construction is selected instead.
Ordinary functions and supported combinators express the complete seam, so no
simulator type, custom Strategy/ValueTree, macro or new production interface
is eligible. Source clarity and actual costs still require measurement.

Generate a legal base word plus0..2 extra repeats using sample::Index to choose
post-Initial gaps, constructing the expanded word once. No semantic rejection
or filtering may discard a failed case. Config::with_source_file(file!()) keeps
ordinary environment case settings; select seed20261112 only when its seed is
Random, preserving explicit Fixed choices. Require positive case/shrink counts;
log actual algorithm, resolved seed, case/shrink/time limits and input/observed
trace after cleanup. Default256 routine cases and proposed4096 weekly/manual
cases retain their distinct scope; measure runtime/output before qualifying
the larger count. Persisted replays and shrinking add work beyond fresh cases.

### Falsification, replay and completion gates

Against then-current real query.rs, suppress canonical lookup alone: the
post-cleanup Read should become3+k for k extra repeats. Restore byte-exact
source and healthy D/R before removing final permission revalidation alone:
its Read should become3. These are proposed outcomes until actually executed.
Retain actual TestError::Fail reason/minimized word, standard persisted case
seed and literal Rust replay; replay the actual returned word with each cut
and restored source in both profiles. Watchdog/setup/compiler failures supply
no semantic-law credit. Coverage omission179 earns only coverage evidence.

Standard SourceParallel persistence resolves to
crates/bombay/proptest-regressions/remote/export/schedule.txt. Current Crane
source excludes that .txt; later include only this owned path and prove actual
saved-file/source-filter/replay behavior. A campaign u64 seed differs from its
persisted algorithm-tagged case seed (for example cc/hex); strategy changes
can alter either resulting word.
Keep literal/source/lock/strategy identity with actual minimized evidence.
Use the existing PR/routine and weekly/manual CI workflow, ordinary pinned
Cargo commands and standard artifact upload; no new runner/controller.
Seed inclusion and CI wiring follow semantic campaign qualification.

Expected complete branch: the three prior delivery records, this PRD,
schedule.rs, one standard regression file, flake.nix and checks.yml (eight
paths, subject to measured final need). Shipping Rust +0/-0/net0; test forecast
net250–450 lines; public types +0/-0. Existing six Delivery, six Scheduled,
two Grant, five Refusal and nine Event alternatives remain test inputs only;
actor control states, domain branches, interpreters and public spellings stay
unchanged. This is additional verification, not production code reduction.
Current checkpoint: shipping +0/-0, tests +0/-0, public +0/-0; only records
changed. The user's removal of implementation budgets remains in force.

Pre-edit owner/source/ordinary-syntax reviews by acceptance, review_bytes and
review_cold pass; root agrees with the bounded grammar and independent Read
oracle. Actual code, both-profile gates, negative executions, shrinking/replay,
final minimization, reviewed CI and delivery remain required. Native incoming
Query/Client combinations, arbitrary overlap, actual cancellation/crash,
volatile restart and the rest of full R24 remain open; no original acceptance
criterion is replaced by this bounded campaign.

### Verified host extraction — 2026-11-12

The first ordinary-function experiment is locally qualified. The original
fixed word and complete34-event literal are preserved; both independent
reviews mechanically match the literal to bd7330e8. The host now borrows the
delivery/expected slices, validates the bounded grammar before Application
construction, uses one8 for validation/issuer/limits, derives release order
from input chronology and retains actual keys by semantic delivery name.
All original row, allocation, provider, native, refusal and retirement checks
remain. Shipping owners, Counter, dependencies and public surface are unchanged.

Formatted701-line source SHA256 is
dd8868ce170ed8fa404de6cd42ebabe4fcdfb8f833c63acdc957b8ffe180dfe2.
Actual pinned Rust1.99.0 b940084d7/Cargo1.99.0 5f94df478 Darwin runs execute the
one named fixed case in both profiles. Canonical-lookup suppression then fails
only the first post-cleanup Read oracle at553, actualUser3 versus required2,
in both profiles; byte-exact restoration passes both. Removing only final
permission revalidation independently fails that same intended oracle in
both profiles; exact restoration again passes both. The ten runs therefore
contain six healthy passes and four intended failures, each1 selected case,
433 filtered and no ignored cases. Later negative assertions are unexecuted
and uncredited. No timeout, setup failure or compiler denial earns law credit.
Pinned formatting and strict owning all-feature library/test Clippy pass.

Author/root and both independent two-pass source/outcome/nonvacuity reviews
pass. The reviewed-only unformatted94db9f14 epoch is separate from all actual
dd8868 executions. Current query source exactly restores dfaae0a0; root lock
9bb2db13 is unchanged. The29-payload capsule in common Git's
session-archives/2026-11-12/incoming-parameterization/ preserves Git base,
patch/full source, both exact cuts, fresh owner audit, all13 logs and reviews.
Its manifest SHA256 is267a90824531085bff839acbe902f3a3ee9cdfbf6d6d7de841a01b9c79e67a32.
Command/status receipt explicitly transcribes observed terminal handles;
raw logs contain stdout/stderr, not invented original exit wrappers.

Complete five-path branch census against main814c96d1:
shipping +0/-0/net0; tests +163/-76/net87; records +244/-11/net233;
public types +0/-0; no untracked paths.
This closes the extraction gate only. The180-word domain, generated/shrunk
and persisted replay, new CI wiring, native Query/Client and restart work,
full R24 and reviewed merge delivery remain open. Enumeration may now follow
the recorded ordinary-Rust plan without changing shipping contracts.

### Verified finite orders — 2026-11-12

The next ordinary-function experiment qualifies all180 legal six-label orders.
The measured std position construction is37 formatted lines, within the
35–50-line forecast. It needs no dependency, new product or generic permuter;
the recorded itertools alternative would add a direct dependency/lock edge.
This is a source/ownership comparison, not a performance superiority claim.
Separate coverage checks require180 total/unique words, all six labels and
both precedences. Expected traces depend only on the supplied word and
authored chronology. The original34-event literal remains the fixed host's
input and calibrates the derived oracle without being replaced.

Formatted857-line source SHA256 is
223f0023146a537d378f65cfc976f0f6a3f1b8363359ff7c21ce9c1166bf7383.
Actual pinned current Darwin debug/optimized runs and all three exact-restored
pairs pass3 selected tests each, zero failures/ignored and433 filtered. Both
independent reviewers enumerate the180 legal permutations separately and
compare every actual positive word set. Each of the eight healthy logs has
180 distinct completed words, each actual Counter2/full34-event trace, then
one separate fixed control. These nested executions are not181 Cargo tests.

Omitting one generated word fails only the coverage assertion215,179 versus
180, in both profiles; it earns no actor/cleanup credit. Canonical lookup and
final-permission omissions independently fail only the first post-cleanup
Counter oracle709, actual3 versus required2, in both profiles. Their actual
first word is Initial/Repeat/Expiring/ConstructorExpired/Revoked/Fresh.
The other179 words and later assertions are unexecuted in those negatives
and uncredited. All six negative commands return101 after the intended test;
all restorations are byte-exact and their healthy D/R pairs return0. No
compiler, setup or watchdog failure earns semantic credit. Formatting and
strict owning all-feature library/test Clippy pass. Shipping querydfaae0a0,
root lock9bb2db13, Counter and public surface remain unchanged.

Author/root and both independent two-pass source/outcome/nonvacuity audits
pass. All17 original command/log/exit sets, Git basec943bc61, full source,
three isolated cuts, readbacks and reviews are sealed as79 payloads in common
Git's session-archives/2026-11-12/incoming-orders/. Manifest SHA256 is
b283cc775e27c0da85a09799deaf6334552e152c2b3531e0b8786bf8f8e85250.
The unformattedd0674b8b review epoch remains separate from actual223f runs.
Finite-stage tests +157/-1/net156; production/public types +0/-0.
Complete five-path branch census against main814c96d1: shipping0;
tests +319/-76/net243; records +291/-11/net280; public types +0/-0; no untracked paths.
Generated cases, actual minimized/persisted replay, CI wiring and delivery
remain open, together with native Query/Client, restart and the rest of R24.

### Verified generated orders and retained replay — 2026-11-12

The bounded generated stage qualifies locally on the same selected owners.
Ordinary supported Proptest combinators select one legal base word and insert
zero to two additional repeats after Initial. Every generated or shrunk word
retains the grammar; no semantic rejection, custom strategy, new network
framework or interpreter is needed. Environment configuration stays authoritative;
only Random becomes default seed20261112. Zero cases/shrinking, fork/timeout
and disabled persistence are refused before callbacks. An explicit seed42
and16-case control actually completes16. These are configuration checks, not
actor-law evidence.

Final983-line source SHA256 is
5dc152215ea96b5ff4e1eb0625e3cd195a7a59c1e2ec9650d7e8bed360539b6c.
The fixed34-event literal and independent finite oracle are unchanged. Each
callback owns an ordinary Tokio host, awaits real Application cleanup and
asserts independent Counter Read2 before complete trace/row/original custody.
Current Darwin debug/optimized controls pass5 selected tests, zero failed or
ignored,433 filtered. Each has439 actual host executions:180 finite,
257 generated (one saved plus256 fresh), actual minimized literal and fixed
control. These nested executions are not439 Cargo tests.

Canonical-only and final-permission-only source cuts fail the actual first
post-cleanup Read in both profiles: respectively3+k for k extra repeats and
3, instead of2. Proptest performs10 failing callbacks and returns the actual
word Initial/Repeat/Expiring/ConstructorExpired/Revoked/Fresh. Its standard
saved cc seed lives in crates/bombay/proptest-regressions/remote/export/schedule.txt;
the actual word also has an ordinary literal Rust test. Changed campaign
seed42 replays the saved case and returned minimum in both profiles. Original
and current source epochs remain distinct in the evidence. Full-module cuts
on final source each report one coverage pass/four semantic failures; only
the first finite word executes before its failure. Later assertions and the
other179 finite words receive no negative credit. Exact restorations and
both healthy profiles pass. No setup/compiler/watchdog failure substitutes
for the post-cleanup oracle. Formatting and strict owning Clippy pass.

Measured4096-case D/R campaigns each pass5 tests with4097 generated callbacks
completing,4279 total actual host executions and all complete traces. The
ordered generated inputs match across profiles;1226 distinct extended words
are observed, not exhaustive extended coverage. Actual test times are1.26s
debug/0.41s optimized; logs are4,827,699/4,825,522bytes. Their test bodies are
byte-identical; the2,177-byte size difference is pre-test Nix/Cargo output.
Summary runtimes differ as reported; these are observations, not bounds.

The seed was staged before Nix source inspection. Existing Crane filtering
omitted it; one explicit maybeMissing path now includes exactly that file.
Two temporary focused overrides of the existing Darwin remote-debug check
run the same locked/offline exact generated test on immutable before/after
sources. Both use Rust1.99/Cargo1.99 and identical9bb lock/dfaae query/5dc test
inputs. Seed absent gives256 completed callbacks; exact seed present gives
257 with the same256-case configuration. The saved prefix precedes identical
256 fresh frames, all Counter2/full traces. Both commands pass one selected
test,437 filtered. This proves focused Darwin debug Nix replay, not the full
flake gate, Linux/MSRV, optimized Nix or native Query/Client behavior.

The existing CI workflow now defines independent debug/release incoming jobs:
256 fresh cases ordinarily and4096 on scheduled/manual runs, plus standard
saved replay. A single Cargo argv owns execution and command recording.
Artifacts retain actual revision/compiler/input hashes, words/traces, saved
seed and separate raw Cargo/combined qualification exits on step success or
failure. A source-review finding established real Cargo0/zero selected tests;
the corrected output guard requires5 passes plus generated execution evidence.
Verbatim old/current block controls prove zero-test acceptance0 becomes1,
real routine/extended logs remain0, missing configuration becomes1 and an
original Cargo101 remains101. Pinned actionlint1.7.12 passes. These are local
source/fragment qualifications; hosted jobs/uploads remain unexecuted here.

Root and both independent reviewers pass source, counterexample, outcome and
scope reviews, resolving the initial CI vacuity finding. Ordinary functions,
existing primitives and the one standard regression file own this complete
scoped construction; no runtime/public interface or permanent tool is added.
Exact commands, raw outputs, sources, isolated cuts and two-pass reviews are
sealed in common Git's session-archives/2026-11-12/:

- generated-canonical/:107 payloads; manifest
  f4044410a4034230ed5d9eccc8705b65a346b8d76cc368d6566932e762b57d71;
- generated-permission/:71 payloads; manifest
  a2be89c5e1522aad4d0f2283de709e9cb4951211b834ca1a31b52ca68612c637;
- incoming-ci/:69 payloads; manifest
  1e1bfe11dee1cc5e5b654f09f155e9b1d252a0bb18b051ee8ebd115ee5c9af70.

Both reviewers verify every payload hash and the exact hidden/ignored inventory.
The source/lock/seed guards remain unchanged; no example or shipping Rust was
edited. Scoped tests add126 source lines plus the seven-line standard seed
since38241d4a; public types +0/-0.

Local eight-path census against main814c96d1: shipping Rust +0/-0/net0;
tests +452/-76/net376; verification configuration +84/-0/net84;
records +409/-14/net395; public Rust types +0/-0; no untracked source paths.

Actual hosted CI, final submitted-tree audit,
reviewed PR/normal merge, native Query/Client, arbitrary overlap, cancellation,
crash, volatile restart and the rest of R24 remain open.

## Delivery of the fixed schedule — 2026-11-11

[PR411](https://github.com/devrandom-labs/bombay/pull/411) merged as
`2b7acf1266968e87595fae568bd0d2dc053011c2` at16:38:57Z after all six checks
passed on `88ee6f52de1ce6b73bbaafce0b762e038308a3f2`. Delivered tree
`31b5221484805e2849571ddc6adfd5c5b3e95c5f` equals the reviewed and tested
tree; merge parents are f2832eea and88ee6f52.

The [required Nix job](https://github.com/devrandom-labs/bombay/actions/runs/43038348972/job/130583123557)
executes the actual fixed schedule in both Linux debug and optimized
430-case library cohorts; both pass without failures or ignored cases.
The fifty-client cohort, public Counter, current/minimum compiler gates,
Observe28/Entity6 models and native campaign retain their separate scopes.
Artifacts prove Driver2048 and Observe4×1024 ASan executions; those are
primitive campaigns, not new networking campaigns. Conditional Miri is skipped.

Independent source/outcome/CI review seals508 payload hashes in common Git's
`session-archives/2026-11-11/ci-review/pr411/`; its manifest SHA256 is
`5b6c67847ffcf1bce3fc3f40665d54386085b8243984a704ec26f7cb1b428e49`.
The fully clean worktree was removed with ordinary git worktree remove after
all users released it. Production/public API remain unchanged. Generated and
exhaustive schedules, retained minimized replay, volatile restart, scheduled
network campaigns and complete R24 remain open.

## Fresh source fitness and exact selected owners

At the pre-edit baseline, Cargo.lock SHA256 is
`83f01ca3ff82f49f485cb017d14279a654fafc2dfb592f4818b9b726686a3578`;
flake.lock is `13ea961730a74f4bc222ac4a7d879e1b5cd64e4fc820949211e01f8e3df10dc1`.
Root/crate manifests have no patches. Registry VCS records select
Behavior/Actors 0.29.0 at `55dbac2d1068b29907498b3a7fa32b3080e0cd30`, macros 0.16.0,
Address 0.3.0 at `9f058dc03d1239e1ef5c3147a893b134a1e74a83`, Communication 0.1.6 at
`963067e0e6bdef15549088966cde354fd820be7e` and Timers 0.2.0 at
`9369d1f6c7fcda91e7de5bf3c19e6769f92145bd`. The selected Behavior revision's
complete instructions were read in this session. The exact public algebra,
shutdown composition, opaque address/recipient ownership, mailbox admission/
closure and timer generation contracts and their relevant owning tests were
inspected. Current local Observe includes PR403's disposal correction.

The concrete seam was freshly checked against current source and tests:
private Export::request owns actual verification, canonical receipt lookup,
initial permission, concrete command preparation, final permission,
nonwaiting mailbox admission and complete native/refusal custody. Existing
Exports::preflight binds a real Application actor recipient to ActorInterface,
control and lifecycle without requiring native declaration or a Session.
The existing private custody test constructs Request/Custody with query=None.
The owning poll_verification helper records actual Ready before disposing the
producer; the schedule will reuse it rather than invent completed facts.
Normal Application and the existing Counter source execute all domain folds.

Pre-edit request/query/declaration SHA256 prefixes are respectively
`f96bd6a4`, `007abdca` and `5dae0887`. Their existing tests and the normative
runtime/module/Driver contracts were read for this exact seam. No dependency
contract or consumer gap blocks authoring this internal fixed witness. The
coordinator's separate Loom advisory correction and minimum-compiler work
retain their own source/integration gates; this baseline is not relabelled as
those corrected locks and cannot supply their delivery proof.

## Selected fixed law before authoring

An independent, closed schedule supplies original protected Increment pairs,
logical time and revocation. Its fixed externally issued request sequences
are distinct except for the intentional identical repeat. Authority lives in
one request-indexed grant map; time advances monotonically. The trace is:

1. Deliver a currently permitted Increment; interpret its actual native send.
2. Deliver the identical protected pair again. It must reuse the first actual
   canonical key and perform no second command construction or native send.
3. Hold another known pair in its actual verification future, observe Pending,
   advance logical time past its grant deadline and release it. It refuses
   Expired before construction/admission.
4. Hold a different known pair, revoke only its own grant, then release it.
   It refuses Revoked; other grants retain their exact independent windows.
5. Deliver a request whose initial permission is valid. Its existing command
   constructor consumes the explicitly scheduled later clock event, outside
   every Behavior fold. The final permission check must refuse Expired before
   the actual native attempt. Preserve its constructed command and canonical
   key even though admission was not attempted.
6. Deliver a genuinely fresh independent grant with a window beyond that new
   time. It admits. A separate ordinary local FIFO Counter Read must return 2.
   Perform normal shutdown and acquire the real native retirement, report,
   Work cleanup and notification results before any semantic success oracle.

The primary independent post-cleanup oracle is the actual Counter value 2,
before detailed row/trace assertions. Expected policy calls, constructor events,
grant changes and delivery/release order are written directly from this fixed
law, not calculated from implementation outcomes. Compare all original bytes,
proofs, allocations, actual key equalities, completed provider results,
retained commands, native attempts, refusal causes and producer panic slots.
query=None and empty publications establish no native request reception,
callback bound, reply publication, transport, TLS, Client or Node claim.

## Pre-edit scope, source cuts and model checkpoint

Add one private schedule test beside the existing query-owned custody test,
registered under cfg(test). Reuse the existing export-tests Counter module by
widening only that private test module to its parent; do not copy or re-expand
the Behavior source in a second module. Use plain closed schedule/event/grant
sums, standard VecDeque/BTreeMap/Cell storage and existing oneshot verification
gates. No custom executor, generic simulator framework, runtime hook, new
production abstraction, type erasure or private implementation clone.

Expected scope: new schedule.rs, cfg(test) registration in query.rs,
one existing private test-module visibility change in tests.rs, and this PRD.
Expected production +0/-0/net0, tests roughly +450/-1 and public types +0/-0.
These are estimates, not budgets. Production actor states, subordinate protocol
alternatives, transition branches, modules and public spellings remain exactly
unchanged. New sums belong only to the fixed test input and independent trace;
one grant map owns current authority without arrival-history state. No new
actor/Behavior transition authority or timer service appears. Disposition:
source fitness passes; actual implementation/review/execution follow below.

First intended source cut suppresses only the owning canonical lookup while
preserving typed iteration. The repeat then admits again and the primary
post-cleanup Counter Read must be 3 rather than 2. Restore exact healthy source
and rerun controls before the next cut.

Second cut deletes only the existing drop of initial permission immediately
before final revalidation and that final current_request match. The existing
attempt and drop then reuse the initial permission, requiring no invented
generic guard or compiler workaround. The constructor-expired command admits;
its later reply policy cannot undo that actual mutation. The same independent
post-cleanup Read must be 3. A compiler/setup/timeout failure earns no law credit.
Restore exact production/test sources and rerun healthy controls in both builds.

Before accepting either cut, source review maps the test to the documented
law and independent native outcome. Independent second review checks vacuity,
missing outcomes and whether the original omissions could escape its oracle.
Run all Rust through pinned Nix, serialize shared target builds with the
coordinator, then perform required current-source integration and CI separately.
Full R24, exhaustive/generated schedules, minimized seed replay, crash/restart,
native callback/Client seams and routine/long scheduled campaigns remain open;
durable-commit and cryptographic obligations remain with their external owners.

## Author and independent source review

The first source review maps each scheduled input to the real request
interpreter, complete typed row and independent post-cleanup Counter outcome.
Current-request and current-reply observations remain distinct. The original
revoked grant and both actual verifier-release Results are retained separately;
neither is replaced by a boolean or fabricated completion. The same pinned
request future survives its observed Pending poll and is completed through
the owning poll_verification helper. Every Counter fold remains unchanged.

Independent two-pass `review_bytes` source/vacuity review passes this bounded
law before execution. Its ledger review adds an explicit equality of original
and row counts before zip, preventing silent truncation. The fixed proof's
sequence-to-byte setup uses checked standard conversion. The final formatted
test is larger than the 450-line estimate; actual census is retained below,
not called cleanup or production reduction. No source/state/API change was
invented from a compiler diagnostic. The complete fixed expected trace does
not claim the remaining simulator or native transport seams.

At this source checkpoint: four paths; production +0/-0/net0, tests +620/-1,
public types +0/-0. This includes four cfg(test) registration lines and one
private test-module visibility replacement, not new production state. Actual
Rust execution, both intended cuts and byte-exact restoration remain pending.

Final source review replaces an unconditional held label plus separate pending
ledger with the actual Poll result recorded once in the complete trace. The
expected trace requires Pending for both controlled verifiers. Independent
review rereads this amendment and the complete row/original length equality;
source/vacuity disposition remains pass before execution. The formatted test
is 615 lines, SHA256
`3c945c2029279c1cb5ff412a09fc8dfd10614f631e2e36676ef0b17ad1fd71b2`.
Only ordinary pinned rustfmt has executed at this checkpoint; no compilation,
runtime result, inversion or full R24 acceptance is yet credited.

## Verified lock integration before first compilation

The coordinator selects verified `899b457257721eec5349c975119a5afa01ce2c17`
before any schedule compilation. The branch fast-forwards from 937fecc3;
the four contribution paths remain unchanged. This updates Loom to 0.7.3 and
removes its unmaintained pinning dependency through the owning locked graph.
Root lock is now
`3a4c840ce3db066cd6d2d802c9243df1f34fbeba619ba3a145c530cb90e7266a`;
isolated transport lock is
`6f6f3caf99875f99e1c55c79f18486b8ef36f03c1745ef1e07cf90ed0b690ec0`.
Behavior and the five other selected owners, source, fixed schedule, test
registration and public interfaces remain unchanged; both frozen query and
test files compare byte-identically. Original eight-input guards pass before
integration and their old lock epoch is preserved separately. Refresh the
guards for these actual new locks before the first compiler invocation.
No earlier runtime result is relabelled: none has executed for this schedule.
The coordinator retains advisory PR delivery and complete integration gates.

## Initial compiler veto and unchanged owning seam

The first exact debug command exits 101 before execution. Both E0599 messages
come from the generic serde_json::from_slice function item not satisfying the
existing higher-ranked Decode Fn input. They are not an inaccessible request
method or missing Application contract. Preserve initial source 3c945c20 and
`/tmp/bombay-network-schedule-healthy-debug-initial.log`; no runtime or inversion
credit follows. Select the existing owning-consumer spelling instead:
`|bytes: &[u8]| serde_json::from_slice::<Increment>(bytes)`. This capture-free
closure implements the already selected exact decoding operation; it introduces
no callback contract, type, bound, adapter, visibility or production change.
Independent source review must confirm that equivalence before acceptance.

Independent `review_bytes` readback accepts the exact one-line closure
refinement: same JSON parse operation and existing constructor port, with no
new semantic surface. Refined test SHA256 is
`5fa5d5076d8c900e5ad963313165256cb2f102c728dfef676dd4167c7063dc49`;
the earlier function-item source remains separately preserved. Actual exact
debug execution now passes 1/0 with 429 filtered after 48.17s compilation and
0.01s runtime. Both actual Pending observations, both held refusals, E's
construction/final-check distinction, fresh progress, original typed rows,
native retirement and the entire independent expected trace execute. The
primary post-cleanup local FIFO Read is 2. Refreshed nine-input guards match
before the separate optimized control; no optimized or inverse result is yet
credited at this checkpoint.

The same refined source also passes exact optimized 1/0 with 429 filtered
after 3m49s compilation and 0.00s runtime. This completes the paired positive
control before any source cut. Independent review accepts both prepared cuts:
canonical lookup adds only take(0), SHA256
`5f4946c34d82673258a77bae7a275fbfa037894636c0ee5c36f8d8cb1f837d5f`;
final revalidation deletes only the initial-permission drop and final match,
SHA256 `9626f2e6732bc6bf718b8133cd3e634953dbde61484d71ee608c9d38ccb953d0`.
Healthy query remains `dfaae0a0`. Neither cut introduces a new guard or
compiler workaround. Actual cut executions and restored controls follow;
prepared source alone supplies no inversion credit.

## First canonical cut and transient linker failure

The exact canonical-only cut runs in debug and fails only the first
post-cleanup oracle: actual User3 versus required User2 at schedule.rs438,
one failed leaf/429 filtered (0.04s), exit 101. Later row/trace assertions
remain unexecuted and receive no negative credit. Its optimized attempt also
exits 101 but fails during linking because the Nix 1.99 sysroot's named
libtest/libstd/libcore rlibs are reported missing. It executes zero tests and
provides no optimized law credit. Raw failures remain in
`/tmp/bombay-network-schedule-canonical-{debug,release}.log`.

The command restores healthy query byte-for-byte and all nine source/lock
guards pass. Subsequent read-only inspection finds the cited symlink targets
present; nix-store --verify-path succeeds for both the exact combined 1.99
toolchain and its rust-std component. The original transient cause is unproved;
no repair, package/toolchain change or source workaround is selected. Rerun
the exact optimized cut on these verified current inputs, keeping its new
result distinct from the original failed link and earlier debug execution.

The unchanged optimized canonical cut retry actually reaches the intended
post-cleanup User3-versus2 oracle, 1 failed/429 filtered, exit 101 (0.00s),
after 3m47s compilation. Both old-source canonical negatives are now real;
the original linker failure remains separately uncredited. Exact healthy
query restoration and all nine original input checks pass afterward.

Before subsequent controls, the assertion audit identifies one late encoding
matches that binds an owned Box inside assert. Select borrowing &encoding
instead, so the assertion observes its existing value without transferring
ownership. Independent review accepts this one-character correction; predicate,
expected bytes and the complete prefix through the primary Read oracle are
unchanged. Earlier positives/canonical negatives retain exact source 5fa5d507
and are not relabelled as final-body execution. Run final-source healthy and
canonical controls before the final-permission cut; do not rely only on prefix
equivalence when actual final-source execution remains available.

The final borrowed candidate is 615 lines, SHA256
`ee2203a626673cac00848986f392f724425bbdd24da4b847ee120b491fb3e7eb`.
Its queued paired positive command remains inside pinned Nix entry before
Cargo starts. Nix explicitly reports its store SQLite database busy. A
read-only sample locates the client in RemoteStore::addCAToStore and the daemon
connection read; a concurrent Determinate-owned automatic garbage collection
is observed, without proving the daemon-side cause. Preserve the raw shell
log and client sample. No runtime credit, package replacement, process restart
or host-tool bypass follows from this environment wait. Healthy query and all
nine input guards remain intact while the original command is live.

The original pinned command subsequently enters Cargo without intervention.
The final borrowed source passes debug 1/0 (429 filtered, 0.01s runtime,
2m01 compilation) and optimized 1/0 (0.00s runtime, 4m10 compilation).
All detailed rows and the complete expected trace execute after actual native
cleanup and the primary Read2. All nine frozen inputs pass afterward. These
results belong to ee2203a6; the Nix wait remains separate environment evidence.

The final-source canonical-only cut also reaches the intended primary Read3
versus Read2 oracle in both profiles, each exit 101 and one failed leaf with
429 filtered. Debug compiles in 56.81s and executes in 0.04s; optimized compiles
in 3m58 and executes in 0.00s. These are ee2203a6 outcomes rather than prefix
equivalence claims. The complete nine cut inputs pass unchanged, healthy query
restores byte-for-byte, and the nine healthy input guards pass before the next
paired restored controls. Later row assertions remain unexecuted in each
negative and receive no inversion credit.

Exact restored controls after canonical omission pass debug and optimized,
each 1/0 with 429 filtered; compilation 44.73s/3m52, runtime 0.01s/0.00s.
All nine healthy inputs pass before the separately reviewed final-permission
cut. Shared disk headroom is 1.6GiB; execute only these focused controls in the
existing target, with no broad build or new target.

The final-permission-only cut also fails at the same first post-cleanup
Read3-versus2 oracle in debug and optimized, each exit 101 with one failed leaf
and 429 filtered. Compilation takes 44.85s/3m45 and runtime 0.04s/0.00s.
Canonical lookup remains healthy; the sole source difference retains initial
permission through the constructor instead of revalidating it afterward.
The negative supplies no later-row credit. All nine cut inputs pass, then
healthy query restores exactly and all nine healthy guards pass. The final
healthy pair remains required before acceptance.

The final restored source passes both profiles, each 1/0 with 429 filtered;
debug compilation/runtime is 41.85s/0.01s and optimized 3m45/0.00s. All detailed
rows and the complete expected trace execute after real Application cleanup.
Pinned workspace fmt check exits 0. All nine frozen source/lock guards pass
afterward. No mutant remains; the query is dfaae0a0 and test is ee2203a6.

## Frozen pre-lint scope and exact commands

The frozen pre-lint contribution contains four paths: production +0/-0/net0,
tests +620/-1/net619, public types +0/-0, plus this focused PRD. The private
Counter module visibility permits reuse of the same nominal owning source;
it adds no shipping mechanism or API. Source, outcome and minimization review
retain the plain fixed schedule rather than a new simulator abstraction.

All exact leaf executions use these pinned commands, with Cargo target set to
the coordinator's existing target, build jobs 2, incremental 0 and dev/test/
release debug info 0. Each source epoch, cut input manifest, actual exit and
raw log remains separate in the session evidence.

```sh
nix develop -c cargo test --locked --offline -p bombay-rs --all-features --lib remote::export::query::schedule::scheduled_duplicates_and_authority_changes_preserve_native_admission -- --exact --nocapture
nix develop -c cargo test --locked --offline --release -p bombay-rs --all-features --lib remote::export::query::schedule::scheduled_duplicates_and_authority_changes_preserve_native_admission -- --exact --nocapture
nix develop -c cargo fmt --all -- --check
```

Final independent review_bytes reads the complete retained source, PRD, raw
outcomes, restorations and nine-input manifests. Its source/outcome/vacuity/
minimization review passes for this bounded slice; no source veto or further
edit is requested. Shipping production/API delta remains zero.

This qualifies only the fixed incoming law and its two omission sensitivities.
The coordinator retains broader integration, Clippy, submitted-head CI and
delivery. The full R24 simulator, generated campaigns and minimized replay,
restart, native query/callback, completion consumer, Client and transport
witnesses remain open. No minimum-compiler or full R24 credit follows from
these current-compiler leaf results. No commit or push is made by this author.

## Strict Clippy source veto and final spelling

The first strict private-unit Clippy invocation reaches actual Cargo after
materializing the existing pinned rust-analyzer dependencies. It exits 101
with exactly one manual_let_else at schedule.rs307. All nine frozen inputs
pass afterward. Preserve its raw log/exit and the already independently
verified frozen source capsule; its passing law outcomes remain ee2203a6.

The coordinator and independent reviewer accept only the ordinary let-else
spelling of the same Deliver|Hold selection, binding and divergent panic.
No event, policy, expected outcome, lint exception, bound, public interface
or production source changes. After formatted source readback, rerun strict
Clippy, healthy debug/optimized, each already reviewed omission in both
profiles with exact restorations, and final healthy debug/optimized. The
coordinator permits byte-exact restoration guards between these two cuts
without another intervening healthy pair. No earlier log is relabelled.

The formatted let-else source is 614 lines, SHA256
`fa52c802b7f248a741c889c776ed4c8433388251086d6e7b0dcc59558555e4a6`.
Independent exact readback confirms only that ordinary spelling changes.
Its strict Clippy exits 0 in 14.86s. Healthy debug/optimized each pass 1/0
with 429 filtered; compilation is 46.99s/3m59, runtime 0.01s/0.00s.
Both omissions then independently fail only the first post-cleanup Read3
versus 2 oracle at the new line 437, each exit 101/one failed leaf. Canonical
compiles 45.68s/3m46 and permission 43.85s/3m52; negative runtime is
0.04s/0.00s for each pair. All cut and exact healthy restoration guards pass.
Later negative row/trace assertions remain unexecuted and uncredited.

The final restored debug also passes 1/0 (45.33s/0.01s). Its optimized
attempt exits 101 during linking, executing zero tests: xcrun warns that the
pinned SDK cannot be found, then cc reports the exact pinned 1.99 sysroot
rlibs missing. The queued fmt check does not execute. Preserve raw warning,
linker output and exit separately; final optimized-restoration credit remains
open. The command restores healthy query and all nine fa52c802 input guards
pass before releasing the shared target to the coordinator.

Read-only inspection then confirms the exact combined Rust, rust-std and
apple-sdk14.4 store paths cited by that invocation are absent; nix-store
--verify-path exits 1 because the combined path is invalid. No alternate SDK,
compiler, source workaround, host Rust command or service mutation is chosen.
The coordinator owns cache relocation and exact pinned-closure restoration
before another invocation. Test/source fitness and earlier actual outcomes
remain separate from this environment failure. Current retained test delta
is +619/-1/net618, production/API zero; full R24 and delivery remain open.

## Exact environment restoration and final retained evidence

The coordinator moves the existing Cargo cache by filesystem metadata to
`/tmp/bombay-remote-20261111-target` before retry. No new cache remains under
the earlier PR worktree. The ordinary supported Nix profile
`/tmp/bombay-network-20261111-shell` records this branch's actual baseline 899b4572
environment. Entry exits 0 and reports the original 1.99/b940 compiler and
Cargo 1.99, exact combined 45c8 sysroot and SDK 0p9q (14.4). Requisites include that
combined compiler, std lb011 and SDK; the roots query names this temporary
profile. Content verification of all three exact paths exits 0. This retains
the selected closure without a project/global configuration, toolchain or
service change; it does not assume the coordinator's newer environment is
identical. Raw profile entry, closure, roots and verify records are preserved.

Using only that own profile and relocated target, the unchanged fa52c802
final optimized control actually passes 1/0 with 429 filtered after 3m46
compilation and 0.00s execution. The full original rows and expected trace
execute after native cleanup and Read2. The separately queued pinned fmt
check now executes and exits 0. All nine source/lock guards pass afterward.
The earlier failed final link remains separate and earns no runtime credit.

Final complete source census: four paths; production +0/-0/net0,
tests +619/-1/net618, public types +0/-0, plus this focused PRD. No mutant,
production edit, lint suppression, new abstraction or changed expected value
remains. Current source is the reviewed 614-line let-else candidate; earlier
source epochs remain frozen evidence. Strict Clippy, healthy controls, both
omissions and exact restorations now have their own actual current-source
results. Broader integration, minimum compiler, submitted-head CI and delivery
remain coordinator-owned. The full R24 obligations remain open as above.

## Integration pre-edit record — 2026-11-11

Integrate the independently qualified four-path contribution on reviewed
parent256ad8b0. The source/results capsule723f7c95 remains immutable; its
baseline899 environment and earlier epochs are not relabelled. The parent
contains the current Loom correction, obsolete research retirement, ordinary
minimum compiler gate and R01 contribution. Preserve all nine guarded runtime,
actor and lock inputs byte for byte after integration. No further production,
test expectation, dependency or public interface change is selected.

Reconcile actual PR408/407 delivery and worktree cleanup, R01's later retained
minimum-remote execution and this fixed incoming witness in the existing
owning PRDs and backlog status. Expected scope is the same three test-owned
paths, this record and three existing record/index files; production/API0.
At this pre-edit checkpoint main is14729ceb and PR409/410 remain open;
later delivery reconciliation above retains their actual subsequent states.
Record final source equality, complete census, independent integration review
and required submitted-head CI before delivery; enable normal auto-merge under
the user's instruction. Delete this worktree after its merge and active use.

### Integrated source conservation

The ordinary fast-forward to256ad8b0 preserves the complete three-path test
patch and all nine qualified input hashes. Pinned Behavior/Actors0.29.0,
Address0.3.0, Communication0.1.6, local Observe, Timers0.2.0 and their public
contracts remain those inspected above. Current Loom0.7.3, native SDK1.10.10
and Tokio1.53.3 remain selected. No dependency, consumer or ownership blocker
is found for this test-only integration. Earlier locks/compiler/profile
epochs keep their original meanings; fresh CI covers the combined commit.

Final local source/outcome/custody review passes by author, independent
reviewer and coordinator. The final source/results capsule is
`session-archives/2026-11-11/network-schedule-qualified-20261111.tar.gz`
in common Git, SHA256
`723f7c95a10df1b04ed601a5b2fa6209aa38e6b896d3833ae9dac98a13bb58f7`;
all133 payload hashes independently pass. Its exact four-path patch SHA256 is
`c84eb27aaeb424e3ad712e7f675f956700520c4bfc8fa429c0357979fb8361e6`.
The earlier pre-lint archive remains separate and unchanged.

The retained comparison is one fixed incoming schedule, with real actor
processing and complete owned facts. It adds no public abstraction or second
runtime and does not stand in for Client/native Query, finite exhaustive or
generated campaigns, minimized replay, volatile restart, scheduled network
campaigns, complete R24, AUTH1, NET1 or final whole-feature distillation.
Complete integrated census and independent record review follow below.

### Final integration review and containment

Independent integration review confirms all nine hashes and the exact
three-path source patch, retained r72 execution, actual delivery receipts and
clean worktree removals. Its archive-location finding is resolved with the
complete common-Git prefix. No shipping code, public type, dependency, compiler
expectation or Nix/workflow configuration changes in this contribution.

The complete tracked/untracked delta against256ad8b0 is eight paths:
production +0/-0/net0; tests +619/-1/net618; public types +0/-0;
records +588/-19/net569; total +1207/-20/net1187. This is verification and
delivery-record growth. Required submitted CI, reviewed merge and worktree
removal remain open; complete R24 and all other incomplete rows stay open.
