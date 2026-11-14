# Native ACL and application permission

Status: active, scoped R07 verification. Original source parent is reviewed
R01 commit256ad8b0; integration now uses reviewed88ee6f52. This work does not
close R07, AUTH1 or NET1.

Current checkpoint: PR412 delivers the four native access/application-grant
controls with all six checks, independent reviews and a matching merged source
tree. Both policy omissions retain their qualified failures and exact restored
controls. Gateway provenance, field substitutions and the rest of R07 remain
open. Earlier checkpoints retain their original source epochs.

## Delivery of the access crossing — 2026-11-11

[PR412](https://github.com/devrandom-labs/bombay/pull/412) merged as
`3bf270cbc5822da80f81155fa743fd8b2833e55a` at19:17:58Z after all six checks
passed on `fe3310de1ef8588df8e0a58a93cc3b69ebe0c53a`. Delivered tree
`fcddd357f938e1509394f9d08540081b904f3198` equals the reviewed and tested
tree; merge parents are2b7acf12 andfe3310de.

The [required Nix job](https://github.com/devrandom-labs/bombay/actions/runs/43059276427/job/130657105316)
executes all four access cases in Linux debug and optimized profiles. Each
profile passes523 tests across the twelve selected remote targets, including
434 library cases, with no failures or ignored cases. Current/minimum compiler
checks, public Counter, Observe28/Entity6 models and both native campaigns
also pass. Driver2048 and Observe4×1024 ASan artifacts retain their primitive
scope; conditional Miri is skipped and earns no credit.

Independent source/outcome/CI review seals504 payload hashes in common Git's
`session-archives/2026-11-11/ci-review/pr412/`; its manifest SHA256 is
`a40690a21fe6baa4bbba8e6cd89a1cd769dfdd5c751a7ef0f09274d070798e3b`.
The fully clean worktree was removed normally after all users released it.
Production/public API remain unchanged. Original head582bc964's hidden
Cargo101 failure remains preserved with its cause unknown. The diagnostic
change restores failure-log visibility; successful fresh fe CI does not prove
a retrospective repair or close complete R07, AUTH1 or NET1.

## Pre-edit record — 2026-11-11

The missing regression is a live native ACL allow/deny crossed with an
application grant present/absent. Current native ingress controls have no ACL;
current permission controls use a Session-local Query. Neither proves the
crossing. A TLS open failure or query timeout earns no ACL-denial credit.

Keep the public Network fixed. The existing private native conversion and
Config ACL owner support an external controlled Router. Ordinary receiver
Application startup uses NetworkLayout::RouterClient with real mTLS.
The private native caller uses the same supported layout and exact root/name
checks. A healthy native request/reply before and after the protected Query
must traverse those same caller and receiver connections through that Router.
Matching declaration status precedes the protected Query; its timeout exceeds
the outer watchdog. Every tested denial must acquire actual Query EOF and
complete routing, Application and native retirement evidence.

| Native Query permission | Configured application grant | Independent outcome |
| --- | --- | --- |
| Allow | Present | Exactly one correlated Counter increment; local Read returns1; complete native admission/processing/reply custody. |
| Allow | Absent | Verified original receipt and typed GrantRemoved cause; no decode, construction or admission; local Read returns0. |
| Deny | Present | No protected Export row, provider call, decode or admission; local Read returns0 despite valid application authority. |
| Deny | Absent | Same native rejection; no application permission check; local Read returns0. |

Actual matched secure controls, query EOF, exact protected bytes/proof,
provider/permission stage counts and actor-origin reply custody are distinct
oracles. Configure native Query permission only for the protected key and
actual authenticated caller certificate subject; declarations, replies and
the independent health key stay allowed. No sleeps supply readiness.

Intended inversions change only the effective native deny to allow in the
present-grant row and only the application grant denial to acceptance in the
native-allow row. The ingress and independent Counter effect oracles must
fail respectively. Capture actual intended failures and byte-exact source
restoration, then run focused debug and optimized controls. Review the source
against invariant/outcome, then counterexamples/vacuity before expensive runs.

Expected files: this record; `application/node/tests/access.rs`; its existing
module index; and two existing test-private permission item visibility changes
if needed to reuse the actual authority loan/check. Expected production delta
+0/-0/net0; product tests approximately +400/-2/net398; public types +0/-0.
No change to remote/export/tests.rs, shipping Rust, examples, dependencies,
canonical compiler diagnostics, workflows or public Network surface is planned.
A test operation enum describes one increment, and native Permission plus the
existing optional Binding express policy without semantic booleans.

## Fresh selected contracts

Cargo.lock SHA256 is
`3a4c840ce3db066cd6d2d802c9243df1f34fbeba619ba3a145c530cb90e7266a`;
flake.lock is
`13ea961730a74f4bc222ac4a7d879e1b5cd64e4fc820949211e01f8e3df10dc1`.
Registry dependencies have no patch overrides. Exact source archives and
owning API/tests were inspected; selected Behavior's complete760-line
AGENTS.md was already read from the exact selected Git revision.

| Owner | Exact selection and reused boundary |
| --- | --- |
| Behavior | 0.29.0, `55dbac2d1068b29907498b3a7fa32b3080e0cd30`; pure Counter receive and complete named Actions. Macros0.16.0. |
| Behavior Actors | 0.29.0, same revision; existing StopOnShutdown policy through ActorExt. No new actor law. |
| Address | 0.3.0, `9f058dc03d1239e1ef5c3147a893b134a1e74a83`; actual application-issued recipient and origin, exact retirement. |
| Communication | 0.1.6, `963067e0e6bdef15549088966cde354fd820be7e`; existing native typed admission and affine reply client; no alternate mailbox. |
| Observe | Local selected256ad8b0; actual Node root/native/termination/retirement products and unchanged completed_actor assertion. |
| Timers | 0.2.0, `9369d1f6c7fcda91e7de5bf3c19e6769f92145bd`; unchanged ordinary runtime integration. External test watchdog grants no timer/admission evidence. |

Node owns execution/declaration/withdrawal/session close. Export owns original
Query/protected bytes, verifier result, permission loan, native attempt and
reply custody. Existing generic recorded provider owns nominal exact-byte
receipts. Existing permission owner supplies actual current Binding checks and
GrantRemoved, reused within cfg(test) only. Counter owns mutation and correlated
processing receipt; a separate ordinary local Read observes actual state.

Selected native owners are bombay-zenoh1.10.10, config1.10.2 and TLS/transport
1.10.4. Config source revision is
`5bebbcb07a7650aff33e08736b8434cf2ccf1706`. AclConfigRule owns key expressions,
Query message, ingress flow and Permission. AclConfigSubjects supplies
cert_common_names and TLS link_protocols; policies link rule and subject IDs.
Selected TLS reads the peer's actual leaf CN into LinkAuthId::Tls; the native
ACL matches that authenticated link fact. Fixture CNs are
BombayCandidate-caller/recipient/router under BombayCandidate-root.
The private ingress owner already creates an actual controlled Router through
Network::to_configuration and set_mode, without changing ordinary public API.

Native demux emits ResponseFinal for rejected Requests with pre-chain QoS;
ACL precedes ingress Query QoS overwrite. This finite crossing claims neither
stalled-egress nonwaiting FINAL nor flood/latency/resource bounds. Empty reply
alone never means an exact public ACL error.

## Scope still open

A Router relay proves transport hop behavior. It does not execute an application
gateway that receives and re-envelopes a command. R07's original-caller versus
gateway authority counterexample, field substitutions, visible-field and
hop/end-to-end protection record remain separate. Recorded/scheduled test
providers supply deterministic policy evidence, not production cryptography,
freshness, delegation or a completed provider security integration.
No broad acceptance or delivery claim follows from this focused contribution.

## First source review — 2026-11-11

The initial source has four independent tests over one concrete finite trace.
Invariant mapping checks actual mTLS Router connections, protected and health
matching status, health payload/reply/EOF before and after, verified original
request/caller/target, unchanged or exactly incremented Counter through a local
Read, and complete Node/root/declaration/native retirement. Test-only visibility
of the existing permission checker and refusal sum changes in two lines; the
checker implementation and all earlier permission oracles stay unchanged.

Counterexample review removed implicit health-route readiness: the health
Querier now acquires its own real matching fact. Both 60-second Query timeouts
exceed the 15-second watchdog, so a timeout cannot satisfy a denial. Both
intended inversions encounter the independent Counter value oracle before the
request-stage/custody checks. Native EOF alone remains insufficient: unchanged working
secure controls, matched declaration, zero ingress and source inversion are
required. Every mutation, send, reply, withdrawal and shutdown occurs outside
assertions, and all I/O remains outside the unchanged pure Counter fold.

Source review also corrected closure capture and queryable ownership; these
are uncompiled source corrections, not executable proof. Pinned formatting was
attempted but cancelled while Nix materialized the shared shell, before Cargo
or rustfmt execution. No compilation or test result is claimed yet. Independent
second source review precedes native execution. Final census/results follow.

Independent review found two consuming Result/Box extractions inside successful
reply assertions. Both originals are now acquired before observational byte
comparisons. Expected bytes and the four-row law remain unchanged; no execution
is credited before the correction is reviewed.

The corrected source and both exact source cuts passed independent third
invariant/vacuity review before execution. This is source review, not a test,
minimum-compiler result or full R07 acceptance.

## Source checkpoint before execution

The complete tracked/untracked source is four paths: production +0/-0/net0;
product tests +596/-2/net594; public types +0/-0. The existing permission
implementation is unchanged; its two visibility lines permit sibling reuse.
Pinned rustfmt expands the corrected source to593 lines without widening its
scope. No library build or native test has executed at this checkpoint.

`nix develop -c cargo fmt --all` and its subsequent `-- --check` both exit0.
The materialized pinned shell reports rustc1.99.0 (`b940084d7`) and Cargo1.99.0
(`5f94df478`). Formatting check log is `/tmp/bombay-r07-access-fmt.log`.
The earlier cancelled shell materialization has no Rust/test credit. Strict
Clippy and all debug/optimized positive and intended-failure runs remain pending
until the shared Cargo target is released and moved outside merged PR worktrees.

## First strict source gate and minimal corrections

The first actual pinned Clippy invocation on source SHA256
`e5bcb0bd96fa868fe5902b65f315fc753f415b6b204f241a66ac63caa8c1bf51`
exits101 with six findings: the four test entrypoints each carry the same
16,496-byte future; the temporary verifier-table Ref crosses a Ready await;
and the optional-grant observation uses a single-pattern match. No live test
ran. The raw log remains `/tmp/bombay-r07-access-clippy.log`, SHA256
`22dbf7a939d3fcf7cc0881805903808584bfc9ab4c7ab382804c6d4546fea754`.

Reviewed minimal corrections pin the same concrete bounded trace in one Box
inside its timeout, consume the existing standard Ready with into_inner, and
spell the grant-presence observation with is_none/else. There is no trait
object, new owner, suppression, policy/input/expectation change, or altered
source cut. The original six-finding run keeps its own source epoch; neither
formatting nor the proposed corrections establish a passing strict gate.

## Corrected strict gate and first live controls

Independent readback confirms source
`d404158527912b1490eb7b9250369a988fee7f2f2373f15537fdd493efb3f63e`
changes only those three approved details and formatting. Corrected strict
Clippy exits0 with no suppressions; log
`/tmp/bombay-r07-access-clippy-corrected.log` has SHA256
`8acb27cbd2e86a9ce09a2bbc2928a5b826d8cb589d3a7edb0f4c2901eaf66a93`.

The actual four native controls then pass in debug:4passed/0failed/0ignored/
429filtered,2.28s. Each independently acquires same-session secure health replies
before and after, matching routes, protected Query EOF, the expected original
permission/Export custody, Counter Read0 or1 and complete joined retirement.
Raw `/tmp/bombay-r07-access-debug.log` has SHA256
`3b9588712d7e44d2ad4d7c76ee0f13d764be7ade017ac1b973ede9bcb9309b2c`.
This is the debug control only; optimized controls, both actual source cuts and
restorations remain required before acceptance.

Commands use the ordinary pinned entry
`nix develop --profile /tmp/bombay-remote-20261111-shell -c cargo`, with
`CARGO_TARGET_DIR=/tmp/bombay-remote-20261111-target`, build jobs2,
incremental0 and development/test/release debug symbols0. The target is outside
PR worktrees so merged worktree cleanup cannot discard shared build state.
Strict arguments are `clippy --locked --offline -p bombay-rs --all-features
--lib --tests -- -D warnings`. Focused native arguments are `test --locked
--offline -p bombay-rs --all-features --lib application::node::tests::access
-- --nocapture --test-threads=1`; optimized runs additionally use `--release`.
No host compiler or directly invoked test binary supplies evidence.

The same four controls also pass optimized:4passed/0failed/0ignored/
429filtered,2.06s. `/tmp/bombay-r07-access-release.log` SHA256 is
`66789b21266a67925ba0aaaff95b357daf5e27a97978153f3a8fb7dda33973c6`.
Existing SDK artifacts are reused; the source remains the reviewed d404 epoch.
Both current Nix remote debug/release checks already execute the complete
owning `--lib` target, so this cohort needs no additional check or runner.

## Native rule inversion — execution in progress

The actual native cut changes only the serialized rule Permission to Allow;
the input Permission::Deny, present grant, all secure controls and expected
Counter0 remain unchanged. Cut source SHA256 is
`ca4a055c79a2cf96e21c53f141391fa0cc314b3deb977b9c800b272910ffaac0`;
`/tmp/bombay-r07-native-inverse.patch` has SHA256
`360b88c804f2beb49a1f6ba2ade2aca76077a91e2ff95a1421d86bd9e88e51a2`.
The exact native-deny/present-grant test actually runs and fails in debug at
the primary independent Counter comparison: acquired User(origin0,1) versus
expected User(origin0,0). Secure controls and complete cleanup precede this
comparison; later request-row checks earn no failure credit. Cargo exits101
with1failed/0ignored/432filtered,0.58s. Raw
`/tmp/bombay-r07-native-inverse-debug.log` has SHA256
`407fe666b4691161a9727d7bd911af208a20547dcd760d99bfe83bc5e6d741fa`.
This is an intended test failure, not a compiler, TLS or watchdog failure.
Optimized reproduction of this exact cut is still in progress.

The same native rule cut also fails optimized at the identical primary
User(origin0,1) versus User(origin0,0) comparison after complete cleanup:
Cargo101,1failed/0ignored/432filtered,0.55s. Raw
`/tmp/bombay-r07-native-inverse-release.log` SHA256 is
`cf87103e4698b0441a162a9e9b7251720f3755a76676b8ed51edb2a0bf773939`.
Independent readback confirms this one-line cut and both actual outcomes.
Access source is then restored byte-exact to d404; source equality alone earns
no restoration execution credit. Final pristine four-case runs remain required.

## Application grant inversion — execution in progress

With access source restored, the only authority cut changes existing
local_current's `None => Err(GrantRemoved)` to `None => Ok(guard)`. It preserves
the actual borrow, original metadata/scope/time checks, None input grant and
expected Counter0. Cut permissions source SHA256 is
`dbdfc3ab6c5c0f0e802dc74cf1d2169c81da25e96074469def672bc4602b50c1`;
`/tmp/bombay-r07-authority-inverse.patch` has SHA256
`7b68afd600e899ffeafba8d36cacb248e557abc66ca50d322387fda05521762f`.
A verified Nix store-path wait occurred before Cargo; it resolved without a
restart, GC, configuration/profile change or alternate compiler. Waiting earns
no law credit.

The exact native-allow/absent-grant test then compiles and actually fails in
debug at the same primary postcleanup Counter comparison: acquired1 versus
expected0. Cargo101,1failed/0ignored/432filtered,0.59s; raw
`/tmp/bombay-r07-authority-inverse-debug.log` SHA256 is
`636e2fd942e402fed2aec02871d6997fdbbff3bf1f31d4813889c45cefa6d946`.
The mutant naturally warns that GrantRemoved is no longer constructed; no
suppression or extra edit is introduced. The actual executed test, rather than
that warning, supplies the intended failure. Optimized reproduction and final
restoration remain in progress.

## Final local qualification and containment

Authority's exact optimized cut also compiles and fails at the intended primary
Counter1-versus0 comparison after all controls and cleanup: Cargo101,
1failed/0ignored/432filtered,0.54s. Raw
`/tmp/bombay-r07-authority-inverse-release.log` SHA256 is
`67a7118f6c0ecdf5f79fd02a3864bab796858f28b6e8787c8f70879d145232d9`.
Independent readback verifies both authority outcomes, their sole source cut
and the warning/failure distinction.

Both original owners and the module registration are then restored byte-exact:
access d4041585, permissions a120e04f, module8212f421. The complete four-case
original-source cohort passes again in both profiles,4/0/0/429filtered each.
Restored debug log SHA256 is
`a9c0b774232bb05024f6baf8dbc0eb5f0dd131559cb5e6bba046bfec67862069`;
restored optimized is
`e0a5c7cb8db57c9daa419edc284ec41610609f616a10ea90f37378992e64a0bf`.
These actual runs establish both restorations; file equality alone does not.
Final pinned formatting exits0 (log8970413f) and strict Clippy exits0 without
warnings or suppressions (log SHA256
`a6476947e95d01cde6a273f292f042b76837d4091e9da8f9157f3d2b2810b3fc`).

The complete contribution against immutable parent
`256ad8b028a94be77686cb544ee13b837a5f22be` is four paths: production
+0/-0/net0; product tests +601/-2/net599; public types +0/-0. Documentation is
additional verification evidence. All shipping Rust, public interfaces,
examples, manifests, locks, compiler snapshots, Nix gates and workflows remain
byte-identical to the parent. This is test expansion, not production reduction.

Containment reuses the existing Counter, typed permission loan/refusal owner,
Node/Export retirement, generic finite provider and exact native ACL owner.
The two cfg(test) visibility changes reuse actual permission law instead of
copying it. One private operation enum names the real increment; no new actor,
transport, policy interface, provider registry, runner or framework is retained.
The concrete Box merely bounds the existing test future's stack footprint.
Native health controls and matching are necessary to exclude TLS/route/timeout
substitutions, and the separate local Read is necessary to reject both actual
missing policies. No additional production seam is justified by these results.

This qualifies four finite native access/grant rows on the captured Darwin
Rust1.99 graph. It grants no minimum-compiler or Linux runtime result, gateway
provenance, field-substitution matrix, production cryptography/freshness,
resource/latency bound, full R07/AUTH1/NET1 completion or merged delivery.
Required submitted-head CI and final combined-source review remain pending.

Qualified sources, both one-line cuts, locked selections, exact commands,
raw logs and actual exit codes are sealed in common Git's
`session-archives/2026-11-11/remote-access-qualified-20261111.tar.gz`, SHA256
`c7344f76f389947c6134c8ce0510561e4edb4fe673dbbb5d180aae9c5ea033c8`.
Fresh extraction verifies all37 payload hashes. The capsule contains no
compiled artifact and makes no transitive compiler or executable-closure claim.

## Integration pre-edit record — 2026-11-11

Select reviewed parent88ee6f52 after the independently qualified four-path
native access contribution. Its existing incoming schedule and delivery
records are separate accepted source; no incoming-schedule result is credited
to these four access cases. Preserve the exact three qualified test files,
both selected locks, actual Counter/provider and native owner contracts.
No production, public API, dependency, runtime adapter or expectation change.

Add only this feature's current pointer to the parent remote PRD and backlog
status. Expected scope: the same three test-owned paths, this record and those
two existing indexes; production/API0. All earlier source/compiler/runtime
epochs remain explicit. Fresh submitted-source CI must execute the combined
private unit cohort; do not relabel older captured runs. Automatic protected
merge and post-merge worktree cleanup remain authorized by the user.

### Integrated source conservation

The ordinary fast-forward to88ee6f52 preserves all three qualified test files
byte for byte: accessd4041585, permissionsa120e04f and module index8212f421.
Both locks, the actual Counter/provider and all shipping native/runtime
contracts are unchanged. Behavior/Actors remain0.29.0 at55dbac2d, macros0.16;
Address0.3.0, Communication0.1.6, local Observe and Timers0.2.0 retain the
verified roles above. No contract, consumer or version blocker is found for
this test-only integration. No earlier execution is relabelled as execution
of the combined commit; its fresh required CI remains mandatory.

The existing remote unit-test Nix checks select these four owning cases. No
new check, runner, target, public surface or configuration is warranted.
Only this record and the two existing parent/status pointers change beyond
the exact qualified test patch. Qualified source, policy cuts and raw execution
evidence stay in the immutable capsule; the retained tests assert acquired
request, verification, admission and cleanup facts. Complete R07's gateway,
field substitutions and production security obligations remain separate.

### Final integration review

Independent source/record/minimization review passes. All three qualified
source copies, six owning contracts, locks and toolchain remain conserved.
The record distinguishes Read execution from its post-cleanup result oracle,
and archived source/log evidence from the live facts asserted by the tests.
No additional abstraction, configuration or verification runner is justified.

Complete tracked/untracked census against88ee6f52: six paths;
production +0/-0/net0; tests +601/-2/net599; public types +0/-0;
records +384/-0/net384; complete contribution +985/-2/net983.
These are verification additions. Required combined-source CI, reviewed
delivery, post-merge worktree removal and the remaining full R07 stay open.

## Failed submitted gate and diagnostic correction — 2026-11-11

Submitted head582bc964 fails CI run43045536333, Nix job130608371739, at
`checks.x86_64-linux.bombay-remote-release`. Its Cargo/builder status is101.
The four access controls actually pass in the separate Linux debug gate.
Earlier visible allocation and Driver failures are expected inversion checks;
they do not identify this optimized gate's cause.

The optimized command redirects its output into remote-release.log and prints
that file only after success. Nix's failing shell exits before printing it;
the retained job log and available native artifacts contain no actual failing
test or compiler diagnostic. Do not infer an optimized test defect or retry
the unchanged head as a repair.

Pre-edit scope is this record plus the existing flake.nix release command.
Add a failure branch that prints the same captured output and exits with the
original Cargo status. Keep the command, selected tests, success witnesses,
expectations, dependencies and required gate unchanged. Expected production,
test and public API deltas are all zero; Nix configuration adds four lines.
This restores failure visibility, not remote acceptance. Validate the evaluated
shell with a deliberately failing Cargo substitute and review the exact diff
before submitting a new head for the required checks. The original failed
head and its evidence remain distinct from that later diagnostic run.

The actual evaluated Linux build phase runs under the pinned shell with a
Cargo substitute returning73 and printing a unique failure line. It prints
that captured line, returns exactly73 and never reaches postBuild; the wrapper
checks pass. Independent source/outcome review confirms unchanged Cargo argv,
success witnesses and all three qualified Rust file hashes. Nix evaluation,
pinned `nixfmt --check flake.nix` and `git diff --check` exit0. No remote test
result follows from this shell check. The diagnostic delta is configuration
+5/-1/net4 plus this record; production/tests/public types remain zero.
