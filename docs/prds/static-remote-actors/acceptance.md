# Static remote actor acceptance

The dated prerequisite checkpoints below are historical evidence at their
stated revisions. The [current feature status](../static-remote-actors.md#current-status-2026-10-23-utc)
owns current dependency, implementation and verification facts. R10's selected
semantic witness is locally satisfied by the [current evidence](#r10--explicit-test-identity-and-production-refusal-2026-10-23).
R17 is locally satisfied for its [recorded fixed TLS profile](#current-r17-support-record--fixed-tls-profile-2026-10-24).
Other rows retain their independent states. Semantic acceptance, exact-head
CI, record delivery and full feature completion remain separate; historical
failures and passing runs keep their original source scope.

## Current R17 support record — fixed TLS profile (2026-10-24)

R17's literal dependency/configuration witness is locally satisfied for the tested profile below. This records existing executed evidence; it adds no networking implementation, public API or support for an untested platform. Its documentation delivery remains subject to the owning PR's review/CI/main merge. Every other acceptance row retains its independently recorded state.

### Reproducible inputs

Delivered main `277e6dfe2c5377b8b31810c025509322640a588e` contains PR392's public two-program application, PR395's patched dependency and PR393's required-gate execution. Root lock SHA256 `83f01ca3ff82f49f485cb017d14279a654fafc2dfb592f4818b9b726686a3578`; isolated native lock `667a182086669aab92209509ad8646b189818918a68328655c318fd81a242982`.

Bombay's `remote` feature is explicit; crate defaults are empty. Its manifests select `bombay-zenoh=1.10.10`, `bombay-zenoh-config=1.10.2`, `zenoh-protocol=1.10.1`, defaults disabled and SDK `transport_tls` only. Locked TLS link/transport are1.10.4 and Tokio1.53.3. These are declared/locked profile facts, not a relabelled fresh all-features metadata expansion. No experimental, UDP or shared-memory transport is selected.

| Configuration and exact owner | Executed evidence | Supported claim and limit |
| --- | --- | --- |
| `NetworkLayout::Peer`, explicit listen-only or connect-only; public example `src/network.rs` | Delivered `examples/remote-counter/tests/processes.rs::separate_programs_increment_read_and_stop_through_real_tls`: distinct actual OS PIDs/runtime bindings; real encoded Increment→processed101→Read1→Stop; both clean application exits. Required patched-lock Nix reruns it in debug and optimized profiles. | Direct TLS Peer-to-Peer on tested Unix localhost; current required CI is Linux. This is public application evidence, not shared-process Sessions or a configuration-only example. No container, Kubernetes, arbitrary multi-host/DNS or Windows assignment-socket certification. |
| Peer connect-only/listen-only/both-direction and `NetworkLayout::RouterClient`; `Network::to_configuration` | `remote::network::tests::layouts_preserve_addresses_credentials_and_the_fixed_profile` compares actual selected native getters: exact supplied address lists with native AllOf, independent deployment/connect/listen credential sources, mode and query limits, TLS-only/mTLS/name verification. Current patched-lock Nix passes debug and optimized. | All four configuration alternatives are mapped without implicit direction or credential fallback. IPv6/address-list forms have getter/configuration coverage; that is not runtime deployment certification for each address/platform. |
| RouterClient to an external Router; same Network owner | `remote::network::tests::ingress::refused_queries_complete_while_accepted_queries_remain_retained` opens actual TLS Sessions for both a Peer listener and a controlled Router listener; the Router branch uses RouterClient. Real queries, transfer refusals, native FINAL/EOF, permit return, undeclarations and both Session closes are acquired. Current patched-lock Nix passes both profiles. | Selected RouterClient connectivity is tested at the actual native transport boundary. This retained witness has multiple Sessions in one OS process. Router remains external infrastructure, not a third public Network layout. There is no delivered public two-process Router application claim or invented requirement that every alternative have one. |
| Missing/empty required directions; same Network owner | `empty_directions_and_absent_peer_connections_refuse_without_consuming_inputs` observes exact typed `NodeStartupRefusal::Unsupported` for no Peer direction and every supplied empty direction, including RouterClient, preserving originals. Native getter tests also reject injected endpoint options. Both profiles execute in current required Nix. | Disabled required configuration cannot silently choose another layout/address or discovery profile. This is typed configuration refusal, not a promise that an asynchronously reconnecting remote peer's absence makes Session open fail immediately. |
| Missing required TLS input; actual public `Application::open_node` | `public_node::public_node_native_open_refusal_preserves_usable_application_and_original_configuration` removes the actual required PEM, requires native `NodeStartupRefusal::Open`, exact standard source identity and original inputs, then uses the recovered Application for a real local Read and joined cleanup. Current required Nix runs the six-case public_node cohort debug/optimized,6passed/0failed/0ignored/63filtered each. | Actual native startup fails closed with originals/cause retained and no profile substitution. Missing-PEM failure is not a wrong-trust-root, peer-authentication or ACL-denial witness; R20 and R07 remain distinct. |

The fixed profile disables **all three** discovery controls: multicast discovery `enabled=false`, multicast `listen=false`, gossip `enabled=false`. Only TLS is permitted, mutual TLS/name verification are enabled, public PKI is disabled, and directions carry explicit credentials. This is enforced by the exact Network source and independent native-getter assertions; successful native/application traces use those same owners. There is no discovery/experimental fallback on configuration or native-open failure. Matching visibility is not identity, actor readiness, hosting authority or delivery assurance.

### Actual provenance and durable evidence

Required [Nix job120622350998/run40014264183](https://github.com/devrandom-labs/bombay/actions/runs/40014264183/job/120622350998) succeeds on submitted `90ecb176982844c057aae3fab05bf6ca6e27f926` at2026-10-23T21:57:46Z; reviewed/merge source tree matches main277e6dfe. Raw job log SHA256 `9b1d2ddd9200cce088564af614382cc50a1e173ac905e421ee5ab033b9322905` names the mapping/native-ingress/application passes and actual public_node commands/results in both profiles. Public native-open refusal appears at raw lines20018/20032; six-case summaries at20022/20038. The separate empty `pr393-final-nix-job-20261024.json` API attempt is not evidence; actual check metadata is `pr393-merged-20261024.json`, SHA256 `662eb82f0a57208e623e88fb28014a7a1892fe072fb14869c2395e9f4ec497d2`.

Durable original CI evidence is `/Users/joel/Code/devrandom/bombay/.git/session-archives/2026-10-21/child-retirement-patched-ci-20261024.tar.gz`, SHA256 `ccbc947310b69c858a1156423c59562ce34c81db0fa2960c46de28c26d02e1af`, including that exact raw Nix log. This support record is not a compiler/binary closure or certification of every historical external artifact.

### Completion boundary

This populates the literal R17 record: exact versions/features, explicit tested topologies, real processes using the declared profile, and typed failure when required configuration is absent. No further implementation or mandatory every-layout public application was identified for that scoped witness. The selected PRD explicitly defers broader ZEN-04 container/Kubernetes deployment certification; that backlog debt remains. Neither this table nor Peer success closes authenticated metadata/availability/deployment isolation in R21, R23 pressure/exit/scenario mapping, security, production identity, full AUTH1/NET1 or the full PRD. No existing row's literal required oracle is reduced.

## Historical source checkpoints

Main reconciliation (2026-10-16, historical): mainf25e4a0c includes PR375 and
the reviewed, CI-passing PR382 test-only compatibility repair. It selects
Behavior/Actors0.26.0 and complete Application/Work outcomes. The earlier
connection-entry prototype is retired from current source. Its distinct
configuration, startup-disposal, connection-retirement, public-entry and family
controls below retain their historical source scope; upstream tests do not
replace all of those laws. Current service ownership and consumer witnesses
remain required. No full remote acceptance row closes from this cleanup.

Current input-privacy work uses the same Node opener with Network instead of
raw SDK Config. Four public input tests, eight owning mapping tests, migrated
callers and six native public Node controls pass in debug and optimized builds.
Mapping, credential disclosure and original-input reconstruction inversions
reach their intended assertions; exact source is restored. Concrete Config now
receives the intended E0308 denial; existing Session and close-borrow denials
retain their original diagnostics. Default workspace execution and strict
all-feature lint pass. Full-feature execution passes847 tests and7 doctests,
failing only the kept Query privacy target. Query privacy, peer
handshake and full service acceptance remain open. Historical raw
Config/Session/privacy statements below retain their original source scope.

The following family-composition checkpoint uses the same Node owner and
existing family invocation, with a real reply before and after root retirement
while Work remains live, exact family receiving and original borrowed/non-Send
Work. Both50-case consumer cohorts and the report-loss/restored controls pass
their intended profile-specific checks. Default workspace773/0/2 and all-feature
848/1/2 (only the known Query privacy failure), plus7/0/3 doctests in each run,
bind its unchanged production. Two later equivalent test assertion spellings
receive independent two-pass review and focused1/0/0 reruns in both profiles.
Strict lint/format pass; isolated native fixture68/0/1 per profile leaves its
controlled subprocess unexecuted. Node's existing public type gains Families;
empty-Node staging and prepared Arc products change explicitly. No additional
runtime, service/privacy acceptance or full witness is established.

The separate declaration-diagnostic correction keeps the exact native error
source and original target while bounding outer Display/Debug. Its real native
constructor-refusal oracle fails on prior source in both profiles; fixed focused
and all21 Application-client controls pass in each profile. Strict lint and
formatting pass. Focused main delivery merged through PR384 asf25e4a0c after
all six CI checks and independent review; its exact main-specific evidence is
in the [owning record](../static-remote-actors.md#declaration-diagnostics--focused-main-contribution-2026-10-16).
This closes no Query privacy, full R25 or service lifecycle requirement.

Normative companion to the [PRD](../static-remote-actors.md). This document
contains full planned oracles and scoped prerequisite evidence, not proof of
feature completion.
Each full R01–R25 witness receives credit only when its complete evidence record
is populated; R10 and R17 have their scoped local records populated here. The following
shared-native replay checkpoint retains its historical source scope: that design stage was recorded in the
PRD: real Counter processing/value/Stop and unrelated Ledger native consumers
must pass focused controls and intended inversions before caller migration.
Cold retention, entered-constructor uncertainty, permission loans and permanent
native-acquisition suppression are independent gates; no executed service or
full-row credit follows from the existing compiler-only comparison. Executed prerequisite probes do not close a full witness. The PRD's
profile limits and user-steering requirements apply to every row. The user
now delegates automatic selection of recommended choices until revoked; the
PRD records those decisions. Delegation does not establish any passing witness.

## Historical prerequisite evidence

The following checkpoints retain their exact earlier revisions and execution
scope. Removed research is not a current API or current passing witness.

The connection-entry stage now executes five public Application caller tests
in debug and release: original cold input recovery, actual startup-refusal
custody, cancelled borrowed execution waiting, run_remote result products, and
acquired Ready output preservation when actual Work producer disposal panics.
The last control catches its original allocated panic and acquires native actor,
both notification and joined connection-close results before asserting output
custody. Delaying Completed publication past producer disposal fails its intended
post-cleanup oracle in both profiles; exact production source is restored. This
does not prove recovery of future-local service rows from Interrupted Work.
Restoring the old coupled cold/prepared Work mapper fails compilation at the
intended outcome projection in both profiles. This is static denial only.
These local Counter controls establish no remote admission, handshake, service
or whole-PRD witness. A separate actual nonempty Entity-family control now
receives before and after root retirement while Work remains live, then acquires
the exact family shutdown after Work completes. Its missing-publication cut
fails after cleanup in both profiles; restored Application/completed-receiving
controls pass81 cases per profile. This establishes the scoped local family
composition law only. The owning connection association/surrender test retains
its separate execution record in the PRD.
The startup-acquisition prerequisite PR379 merged38774de with all six checks
and required/native execution reviews passing. It closes no full remote row.
PR378 and replacement PR380 were closed unmerged by the release bot; PR381
is the current successor. PR374 merged67a286c; the full service and private
transport API remain open.

Communication adoption [PR377](https://github.com/devrandom-labs/bombay/pull/377)
merged76dbd849 at2026-10-16T00:31:25Z after all six checks and scoped reviews
passed. Reviewed/submitted329, actual CI429 and merged main share tree25bc63d2.
Required Nix38820049280 succeeds: workspace703/0/1, remote406/profile,
requester6/profile, primitive fuzz passing and Miri skipped. Required/native
reviews005a8147/98d0443b and actual merge receiptcf93122f preserve attribution.
This delivers the dependency prerequisite, not a full remote witness. Below,
older preparation facts retain their historical source and execution scope.

Earlier private composition adds actual missing-host and executor-loss controls
and generated reply protection for both Counter/Ledger provider families.
Restored combined library348 and application38 cases pass in both profiles;
the intended failure-erasure, literal-proof, metadata and bound cuts fail their
specific oracles after cleanup. Strict lint remains blocked by eight unused
private connection declarations; the scoped command does not reach test lint.
The public Application service and SDK privacy replacement remain absent, so
these facts close no full row. Owning Communication PR13 merged as39fd717e
with all five checks passing and exact reviewed/tested/main tree agreement.
It separately verifies capacity readiness and exact custody. Its version-only
release PR14 merged963067e0 after all five checks passed. Actual registry,
archive and tag verification passes. The coordinator selects0.1.6 with every
other package retained and348 library/38 Application cases passing per profile;
independent main-based adoption PR377 is ready after its eight actual checks
and independent source/execution reviews pass. Main controls are333 library,
36 Application and70/0/1 protected-counter per profile. Required CI/merge remain.
Actual Application integration remains required.
Provider PR376 mergedc249044a after all six exact-head checks and required-Nix/
native/source reviews passed; tested/reviewed/main trees match54451654.
Actual six requester cases execute once per profile, with primitive fuzz passing
and conditional Miri skipped. Actual merge receipt095a3c42 retains provenance.
PR377 integrates that main as3299340; fresh combined checks/CI remain required.
Neither prerequisite closes any full acceptance row.

The private acquired-Connection preparation handoff adds two focused ownership
laws (plus two inherited Counter cases), passing debug and optimized builds.
Dropping only the prepared Connection fails the intended None-versus-actual-ID
oracle in both profiles after cleanup; exact restoration passes. Combined
Application/HTTP/Entity coverage passes421 cases per profile on the original
test source. Sharing Counter removes two duplicate inherited cases; fresh350
library controls pass in both profiles. Formatting and local strict lint pass.
Remote strict lint returns101 for eight unused private production declarations
only, with no test diagnostics. This remains an explicit delivery gate; this does not solve startup-producer disposal,
pre-spawn unwind, the public service or SDK privacy. No full row closes.

The final pending-protection comparison32c534a5 executes both finite providers
in debug and optimized builds; restored complete Counter targets pass72/0/1
per profile. Borrowing instead of sharing is statically denied; substituting an
equal receipt or including authority revision in replay identity reaches the
intended post-cleanup failures in both profiles. This proves one original
receipt can support concurrent immutable consumers while another native request
progresses. It proves no Query traffic, current revocation, protected publication
or finished public service. All full rows stay open. The public API must expose
only Bombay contracts, with transport configuration, sessions, queries, replies,
declarations and callbacks private even for advanced consumers. PR374 merged
its prerequisite and PR373 was superseded; release PR381 and full-service
delivery remain held pending that integration.

Current compiler counterexamples make the privacy defect executable:
`remote_surface` fails its expected-denial assertions because ordinary Bombay
consumers can infer native configuration, access the lent Session and acquire
a native Query without importing the SDK. All three snippets compile; both
outer debug/release test invocations return101 for that intended reason.
Trybuild uses dev compilation in either outer profile. This is an intentional
red acceptance gate, not a passing witness. Actual Application replacement and
a complete public-surface audit must establish privacy before delivery.

### Earlier main-delivery evidence

The following preparation snapshot predates the PR377 merge recorded above.
Its pending-delivery statements retain their original source qualification.

Communication0.1.6 publication is verified after reviewed owning PR13 and release
PR14 merges with passing CI. Narrow Bombay adoption is selected; its main-based
tests and PR delivery remain pending. Capacity observation neither reserves a
slot nor substitutes for fresh permission checks. Arbitrary foreign-waker-panic
sibling progress is unsupported. This prerequisite closes no full row.

[Provider conservation PR376](https://github.com/devrandom-labs/bombay/pull/376)
mergedc249044a0425f3704e7836b31f696069f5d78d63 at2026-10-15T22:50:52Z.
All six checks passed onfc85ec10, including required Nix38807714831;
reviewed, tested and merged trees equal54451654. Independent required/native
reviews9ce15cf1/ba0e2a35 and merge receipt095a3c42 retain actual execution and
source attribution. Six provider cases execute per profile: two finite refusals,
two actual two-Node TLS requester flows and two local pending-protection cases.
Primitive fuzz passes; conditional Miri is skipped. This test-only contribution
adds no production/API/dependency; full R01–R25/AUTH1/NET1 and public transport
privacy remain open. PR373 was superseded; PR374 merged67a286c after all six
checks passed.
Full-service delivery and the private transport API remain open.

The DevX integration prerequisite is delivered through reviewed
[PR359](https://github.com/devrandom-labs/bombay/pull/359), merged7fccf08b at
2026-10-13T22:17:06Z after all six checks pass ondf6144f. Source trees match.
Actual required Nix and fresh native artifacts have recorded technical review;
optimized workspace749/0/1, current comparisons, coverage and primitive fuzz
pass. Miri does not execute. Historical pending-delivery paragraphs below refer
to their original checkpoints. No full R01–R25 row closes from this integration.

[PR361](https://github.com/devrandom-labs/bombay/pull/361) delivers public
`remote::verify`, mergedb2beaa9 at2026-10-14T00:02:44Z with all six checks passing.
Its required Nix executes16 public remote tests in each profile; fixture native
evidence remains separate. The relocated protected Counter target executes55
plain tests per profile through its isolated package, with native campaigns
verified separately. [PR362](https://github.com/devrandom-labs/bombay/pull/362)
merged `fb533b5bddb1ba20d6514f590167faef60b028d0` at
2026-10-14T01:22:54Z with all six checks passing. Neither acquisition nor
relocation closes any full remote witness or delivers remote-counter hosting.

[PR363](https://github.com/devrandom-labs/bombay/pull/363) delivers public ingress
retention, merged1320dfa9 at2026-10-14T03:07:08Z after all six checks and recorded
source/native/required-Nix reviews pass. It retains an original/count-permit through
queueing, active use and unread output, with exact distinct native refusal causes.
Required CI executes four ingress tests in each profile; native SessionLocal counts
are separate from TLS campaigns. The Reply pool remains an explicit per-query
fixture input, not a caller-wide default or byte/heap bound. This prerequisite does
not authenticate/admit an actor command or close any full R01–R25 row.

Current raw metadata composition uses the actual public fifteen-field owner with
Counter/Ledger fixed vectors. Seven structural schema tests pass in both profiles,
and the intended unknown-field inverse fails before exact restoration. Its Serde
array control exposes a receiving-boundary prerequisite: shipping input must have
the selected object shape. No authentication, zero-admission, full bound or full
R02 acceptance is credited from raw decoding. Node/profile composition is now
delivered in [PR364](https://github.com/devrandom-labs/bombay/pull/364), merged
`406df116dd081593e1099a1f3301d2fb91bfc4fc` at2026-10-14T17:43:09Z after scoped
minimization, final source/artifact review and all six checks passed on04c47413.
Required Nix38596070399/job115963560778 executes six Node cases and35 public
remote/Application tests per profile. Fresh native evidence retains its separate
transport scope; primitive fuzz passes and conditional Miri is skipped. Missing
certificate packaging and selected Rustls0.23.46 are verified in that actual
source. Controlled Zenoh PR12 merged1ea4bd16 with passing exact-main CI; its
guarded publication38638728105 succeeded. Fresh registry consumer runs pass on
Rust1.99 and1.88, and independent review556699b5 verifies112 exact owning Query/
Config executions. Both candidate PRs now select SDK1.10.9 with five package
updates per lock;394/319 unrelated complete records remain unchanged. Combined
verification and exact-head CI remain required before their merges. Bounded
decoding, shipping exports/admission/replay and all full rows remain open. The new local foreign-child isolation test and its real owner inversions
are a subsequent prerequisite, not delivery of Node-owned export association.

The selected count-before-copy comparison covers accepted queued, active and
unread native requests only. Both real providers fail the original count3-versus2 oracle after cleanup in
debug and optimized. Corrected controls and all five causal cuts pass both
providers in both profiles. Scoped minimization, independent exact-head review
and all six checks pass; [PR357](https://github.com/devrandom-labs/bombay/pull/357)
merged `671ba860` at2026-10-13T02:41:04Z. It cannot close R14/R19 byte, overflow, congestion or
whole-service fault obligations; the PRD records its scope and release policy.

The separate provider-result correction preserves the actual `Ready` result
before completed-future disposal can panic. The exact original request, result
allocation and later opaque panic remain owned. Original and replayed defects
fail the intended oracle in debug/optimized; corrected and byte-exact restored
controls pass. Combined Cargo645/0/1, Nextest644 passed/1 skipped and protected
59/0/1 per profile retain distinct provenance. Actual TLS controls pass64 cases/
160 roles and child-stop2/0/0 per profile, with frozen source/copied-binary guards.
Independent combined review `96248e45` permits focused PR preparation; its own
submitted-head CI, review and merge remain pending. This is a custody prerequisite,
not full whole-service, identity, pressure or R01–R25 acceptance.
The preserved Result must have been returned by the supplied Future. The actual
async-adapter defect now fails its intended oracle in both profiles. All18
acquisition wrappers instead expose their actual Future using one request borrow;
four held tests retain their preceding delay and exact closed-delay error. Four
owning cuts fail as intended and their restored controls pass both profiles;
final protected59/0/1 and strict checks pass on5f929068/5cdbce9c. Integration,
replacement exact-head CI and delivery remain pending, separately from full rows.

PR368 delivers crate-owned `remote::json::check` with the
existing selected event-parser crate. Five pure public tests and two actual
Node consumers pass in debug and optimized builds. The latter are SessionLocal
queries with deterministic proof records: they do not establish TLS handshake
or production identity. Counter observes its exact final value; terminating
Ledger retains original appended allocations and rejects the later closed send.
All cleanup, native results, notifications and Node close are acquired before
those domain oracles. Eight intended production cuts each fail their exact assertion in both profiles,
and byte-exact restored controls pass. Independent source and execution review
retain the native Counter2-versus1 and LedgerClosed oracles. Final combined
checks and reviewed delivery are now recorded: PR368 merged792529ea after all
six CI checks and scoped final reviews passed. Required Nix executes the five
JSON cases and both Node consumers once in each profile. No full R02/R14 or
other row closes.

The user requires crate-owned remote capability behind Cargo feature `remote`.
Merged PR370 (8d809b3d) certifies actual root,
sibling and nested-child recipients against their Application's existing source
before reply allocation. It preserves closed-target membership and native
allocation refusals. Six ownership cuts reach their intended failures in both
profiles, followed by six fresh restored controls. The same existing client gains
a remote-only nonwaiting attempt with exact native Full/Closed originals and
allocated sender identity. Two complementary consumer controls and five admission
cuts pass in both profiles; both consumers pass after exact restoration. The
independent test/source/execution reviews and uncredited disk/lint failures remain
in the PRD. Combined-source local checks and all six required/submitted checks
pass; source/native/Nix reviews precede actual merge8d809b3d. This is
local target/admission evidence, not protected publication, replay, permission
coherence, real remote Stop or closure of a full R01–R25 row.

A dependent test-only composition now carries an actual protected Counter Read
through two public Nodes to the existing live requester actor. Both finite
provider families pass in debug and optimized; four deliberately altered
forwarding values fail the independent native final-state oracle after cleanup,
and four fresh byte-exact restored controls pass. Actual typed Actions trigger
the query and acknowledge consuming its reply. This supplies the narrow live
consumer evidence. A subsequent comparison now protects the actual generated
reply bytes using each finite provider's original Future and the existing
result-preservation operation; it removes preregistered expected replies.
Both profiles and the same four forwarding corruptions/restored controls pass.
This does not supply production replay ownership, secure reply protection,
adverse identity cases or a complete R01 witness.

Examples consume its public API; example-only implementations cannot pass R01.
Keep the local counter in `examples/counter` and the remote counter in
`examples/remote-counter`. Each teaches one complete application through public
composition. Assertions and transport/fault campaigns stay in owning tests or
isolated research; relocating a fixture does not prove shipping remote support.
Ordinary remote composition must use Bombay APIs without importing Zenoh,
opening or closing its Session, declaring queryables, or writing transport
request/reply loops. Bombay owns those operations and their cleanup within the
Application lifecycle. The public remote-counter application must demonstrate
this boundary. Advanced public APIs also use Bombay contracts and must not
expose native Zenoh Config, Session, Query, Reply or declaration types. Review
all public signatures, callbacks, exports, examples and rustdoc for those leaks;
transport fixtures cannot substitute for the session-free Application witness.
First reconcile the committed DevX Application APIs with the proven admission
and retirement laws, preserving the active worktree's uncommitted changes.

The current DevX prerequisite now has six owning macro/manual comparisons in
debug and optimized builds. Actual generator custody/registration cuts reach
their intended post-cleanup failures; exact source restoration and final fresh
builds pass. Independent review verifies all16 campaign records and325 captured
source inputs. The private two-protocol scope comparison also passes both
providers and three selected cuts/restorations on styled source. Combined
workspace Cargo753/0/4, Nextest749 passed/one skipped, build, strict Clippy and
formatting pass on cleanup-first04d, with scoped source/receipt reviews. These
are prerequisites; revised-head CI and actual merge remain pending.
No full R01–R25 row is closed by this evidence.

## Requirement traceability

The inventory IDs below map exactly to the PRD's selected scope. Several IDs
may need multiple witnesses. A mapped ID means an obligation is accounted for;
it is not a passing test or permission to mark a whole inventory family complete.
Partial QUERY, SIM, OBS and RELEASE claims retain the limits in the PRD.

Each falsifier must violate the named law in the real owning implementation
or protocol configuration. The matching positive control must pass first.
Compilation failure, setup failure, timeout without the intended oracle, and
an unrelated assertion are not semantic inversion kills. Static denial cases
instead require the expected compiler diagnostic with a compiling positive.
R-labels below are documentation identifiers, never source/test names.

| Witness | Inventory obligations | Required independent observable assertion | Deliberate falsifier / adverse case |
| --- | --- | --- | --- |
| R01 | WIRE-01, WIRE-03, NET-01, NET-02, ID-04, RELEASE-02, RELEASE-06 | Two actual public Applications exchange an explicitly exported concrete command/reply. The sender's complete Actions contains the request; the receiver's public trace contains exactly its permitted domain effect. An unrelated ordinary local protocol stays unexported. | Remove the interpreter or typed completion consumer: intended compile denial. Attempt export by generic serialization alone: denied. A custom Engine host cannot substitute for this witness. |
| R02 | WIRE-02, WIRE-08, ZEN-05 | Declared encodings round-trip declared variants and match fixed vectors. Unsupported/malformed bytes, unknown Bombay-owned metadata fields, noncanonical selected proof representations, excessive nesting/length and wildcard/path escapes produce the exact boundary classification, bounded allocation and zero actor admissions; payload/provider schemas retain their declared profile rules. Valid JSON whitespace/field-order alternatives with their own valid exact-byte proof remain eligible subject to schema, authorization and replay checks. Altering bytes while retaining the original proof fails; fresh proof cannot enable repeat execution. | Skip a size/scope/selected-proof-canonicality check or authenticate altered bytes with the original proof and show the intended parser/allocation/admission oracle fail. If signatures are not selected, document the alternative authenticated-byte binding; no signature-proof credit. |
| R03 | WIRE-04, WIRE-05, WIRE-06, ID-01, ID-02, PLACE-06 | Distinct caller, actor, host, runtime and authority identities remain distinguishable. Same local numeric address in separate hosts cannot cause cross-delivery. Restart retains the selected stable name but changes exact target; delayed old-target input is refused. | Drop one generation/identity check; replay the old request in debug and optimized runs and observe the forbidden admission. Restore original sources. |
| R04 | WIRE-07, NET-10, QUERY-06 | Each reply settles only its matching principal/deployment/target/operation/request/session. Count one acquired reply per operation; retain late/duplicate disposition and preserve unrelated pending requests. | Substitute each correlation field independently; remove the matching check so the named oracle detects a wrong or second settlement. |
| R05 | WIRE-09, DISC-03, RELEASE-07 | Record a supported version/feature matrix. Every supported pair exchanges the declared protocol; every unsupported pair returns explicit incompatibility before actor admission. Local-only builds remain usable. | Treat an unknown version as an older valid schema or ignore a required feature. A single-version happy path cannot establish mixed-version behavior. |
| R06 | WIRE-10, NET-03, NET-12, DISC-05 | Observe transport enqueue, mailbox admission and application processing separately. Reports preserve acquired evidence and allowed provenance; local native failures/private payloads are not serialized. Timeout/presence loss yields uncertainty, not exact actor death. | Relabel enqueue as processing, or link loss as termination; the independent receiver trace contradicts the reported fact. |
| R07 | WIRE-11, WIRE-12, ID-03, ID-08, ID-09, AUTH-06, ZEN-09 | Raw claims cannot construct trusted actor input outside the provider port. Change deployment/purpose/actor/caller fields or forward through a gateway; admission retains the verified caller rather than gateway authority. Document visible fields and actual hop/end-to-end protection. With valid secure connectivity, exercise real ACL allow/deny crossed with application allow/deny: only double-allow may admit. | Trust path shape, replace caller with gateway identity, or accept mismatched proof scope. ACL denial must stop protected ingress even for a valid application grant; ACL allow does not waive application denial. A TLS setup failure is not an ACL-denial witness. Test identity supplies policy evidence only. |
| R08 | ID-05, ID-06, ID-07, ID-11, AUTH-04, AUTH-08 | Two independent deterministic provider implementations exercise accepted, invalid, stale and unavailable evidence plus rotation/revocation. Trace admission around each chosen freshness/capacity boundary with controlled time and retain exact classification. After decoding, the final coherent permission/time check and capacity-nonwaiting mailbox attempt form one local operation; permission updates cannot interleave. Full-mailbox retries retain the exact typed message and recheck authority on every attempt. Prove known revocation and expiry during a capacity wait refuse admission, while an independently observed permitted control admits. | Treat unavailable as invalid/accepted, accept expired precheck after a capacity wait, or let observed revocation bypass its selected boundary. Restore a precheck followed by an awaited capacity send and establish the intended admission-count failure in debug and optimized builds. No remote instantaneous-revocation or physical queue-publication nanosecond claim. |
| R09 | ID-10, AUTH-03 | Measure verification queue/concurrency/cache/delegation bounds; a valid principal eventually succeeds under the declared fair-capacity assumptions after invalid-ID pressure stops. Delegation cannot expand tenant/protocol/actor/operation scope. | Accept broadened delegation, leave a negative cache entry beyond its allowed lifetime, or create an unbounded verifier waiter queue. |
| R10 | ID-12, RELEASE-04 | Test profile must explicitly select fake evidence. Production configuration with a fake/missing required provider refuses startup before any export. The provider port compiles with both deterministic implementations without registry/type erasure. | Implicit fake fallback, production config bypass or caller-dependent provider casts. The real Selo example remains outside this milestone. |
| R11 | AUTH-01, AUTH-02, AUTH-07, NET-13, PLACE-01, PLACE-02 | Correctly assigned host accepts permitted increment/read. Authorized stop of the exact actor closes new admission and reports completion only after its exact native joined-retirement result establishes retirement of its owned tasks/resources. Preserve cleanup failures; prove an unrelated actor still progresses. Record control acceptance, observed exact termination and joined retirement separately; none substitutes for the selected completion promise. Hold genuine target-owned activation work after termination publication: completion stays absent and the sibling progresses until that work settles and the native result is acquired. Publish the derived receipt after actual actor-task join and before parent conversion, while the parent retains the complete original native result. For roots, the existing cleanup task acquires the join and makes the report available while application work remains live; final native handoff and Application-owned family shutdown retain their existing work-completion barrier. Parent projection failures remain separate and cannot erase that published retirement fact; joining alone does not prove clean retirement. When an acquired task result cannot establish full retirement, permitted observers receive an explicit typed inability result; the parent retains original causes. Existing runtime owners derive the common report with separate typed retirement and failure assessments, conserving child ownership evidence before application conversion. Assessment covers all recorded actor/runtime failures in the owned subtree; normal graceful stop or fully settled owner-retirement requests are not themselves failures. Known failure presence survives unestablished retirement; incomplete assessment cannot become no failures found. Send-only, wrong host/actor/generation and broadened-grant requests fail with bounded public detail. | Reuse send permission for stop, accept a grant for another host, or report completion from acceptance/termination before joined retirement. An always-denying lifecycle path fails the authorized-stop positive. Suppressing the published retirement fact after later parent projection failure fails the independent observer oracle. Erasing known failure presence, treating incomplete assessment as no failures, or discarding required child proof fails the corresponding independent source-fault/resource oracle. Contain both ordinary termination-notification and joined-report notification unwind; preserve their distinct original causes in the selected ActorNotificationReceipts termination/retirement fields, and keep the same acquired native original for its parent. Ordinary termination publication must not destroy native state or skip owned task settlement. Guard cancellation/unwind must retain its available notification cause independently of discarded return values. Child notification transfer uses the selected Tokio oneshot before consuming conversion; root receiving keeps native and notification receipts beside the work outcome, independently of later cleanup failure. Local report/error/access and the two-stage receipt product are selected in the PRD. The first-publication Tokio receipt, Entity error extension, explicit owning-receipt surrender and private refused-original custody product are selected. Remaining concrete source contracts and regressions must be verified under the recorded delegated-choice policy. Unsupported operations remain explicitly denied. |
| R12 | AUTH-05, NET-06 | Duplicate request identity does not produce a second admission; same identity/different command rejects. Exercise live retention, eviction, reconnect, session retirement, restart and identifier exhaustion. Old/unknown freshness never becomes new eligibility. | Forget a replay entry and accept replay as new, wrap/reuse an ID, or automatically resend an uncertain increment under a fresh identity. No durable deduplication credit. |
| R13 | NET-04, NET-05, QUERY-05 | Distinguish rejection before any possible transmit from possible delivery and known admission with unknown completion. Move-only original recovery exists only where the actual value is retained. Timeout/cancellation after processing preserves truthful uncertainty. | Return a decoded reconstruction as the original, report cancelled wait as nonexecution, or erase an acquired admission receipt when the reply is lost. |
| R14 | NET-07, NET-08, QUERY-04 | At capacity and one beyond, retained counts/bytes including blocked sends and received-but-unconsumed replies obey the configured bounds; each overflow has a typed disposition and no false success. | Bound mailbox slots but spawn unlimited blocked producers, bypass user ingress via control, or discard a completed reply to make the memory counter look bounded. |
| R15 | NET-09 | Enumerate the approved ordering scope and compare the whole receiver sequence under reorder/reconnect; allow legal reordering while forbidding scope violations and stale-correlation settlement. | Remove the enforcement for an actually promised order; do not kill a mutation solely because the test demands an unpromised global order. |
| R16 | NET-11, DISC-02, DISC-04, DISC-07, PLACE-05 | Failed initialization produces zero ready exports. Export only after committed activation and valid static grant. Old advertisement/withdrawal cannot override a newer exact export; closed endpoint rejects stale traffic. | Publish before activation, accept unauthorized withdrawal or let old generation withdraw the replacement. Replay lifecycle inputs in optimized tests. |
| R17 | ZEN-01, ZEN-04 | Exact version, features and supported endpoint topologies form a reproducible dependency/configuration record. Actual processes start without unintended multicast or experimental feature fallback. | Disable required connectivity/configuration and observe typed startup failure; do not silently switch to an undeclared profile. Only tested topologies receive support credit. |
| R18 | ZEN-02, ZEN-03, ZEN-10, ZEN-14 | Inject failure/drop at every session/declaration startup and retirement boundary. Acquired results and coexisting failures survive; task/declaration counts return to baseline. Retire one of two exports and prove the unrelated actor/export progresses before final owner cleanup. Preserve each actual terminal classification during that separate cleanup; reconnect never repeats actor initialization. | Detach an owned task, close the shared session from one export, or duplicate activation during reconnect. Source-only task counting is insufficient. |
| R19 | ZEN-06, ZEN-07 | Flood selected message classes and stall consumers. Actual callback paths remain bounded and do not fold Behavior or block network execution on actor work; documented QoS/overflow behavior matches full traces. | Block callback on a full mailbox, enqueue unlimited work or silently drop a command while acknowledging it. |
| R20 | ZEN-08 | With actual selected Zenoh security, valid configured peers connect; wrong trust roots, invalid credentials and insecure fallback fail before protected traffic/exports. Record router visibility and scope of confidentiality. | Bypass peer verification or fall back to insecure connectivity. A deterministic application verifier cannot replace this test. |
| R21 | ZEN-11, ZEN-12, DISC-01, DISC-08 | Record explicit selection/omission of discovery/recovery services. Authenticated export metadata binds deployment, node/runtime, protocol and availability; same names in different deployments remain isolated. If recovery caches are enabled, prove their finite scope and limits. | Treat matching/liveliness as hosting authority, collide deployment namespaces or assume cached history proves durable completion. |
| R22 | QUERY-01, QUERY-02, QUERY-03 | Before domain execution, the user-selected targeting path selects at most one authorized instance. Under overlapping responders, count at most one permitted admission in the declared static-host model. Every promised milestone survives actual query consolidation if queries carry commands. | Fan out a mutation and merely reject duplicate replies afterward, or suppress a promised milestone. For another transport operation, retain query-specific backlog debt and prove equivalent command safety. |
| R23 | NET-14, SIM-01, SIM-09 | Public counter/service runs in separate OS processes with isolated local spaces. Observe success, pressure, exit, authorized/denied stop and lost-reply uncertainty; map each to the deterministic scenario's semantic events. | Route via shared process-local state, omit real encoding, or replace the real adapter with the fake. These tests must fail those shortcuts. |
| R24 | SIM-02, SIM-03, SIM-04, SIM-05, SIM-07, SIM-08, SIM-10 | Independent scheduler controls delivery, expiry, crash and authority evidence. Trace invariants hold over finite exhaustive cases and generated campaigns; replay a retained minimized failure with the same seed/schedule. Destroy volatile state on simulated restart. Add both routine CI and longer scheduled networking campaigns with selected version/topology/seed/trace artifacts and user-approved budgets. | Oracle copies implementation decisions, fake restart preserves volatile replay state, a dropped event disappears from the trace, or existing Driver/Observe campaigns are mislabeled as network campaigns. SIM-04 durable-commit and SIM-05 cryptographic cases remain external obligations. |
| R25 | OBS-02, OBS-05, OBS-08, RELEASE-05 | Bounded metrics/inspection distinguish pressure, unavailable verification, uncertain work and cleanup failures without raw private payloads or unbounded actor labels. The selected bounded diagnostic route identifies the relevant uncertain command and authority scope, with a truthful disposition after retention expires. Support table names exact profile/version/evidence and remaining limitations. | Slow/broken telemetry blocks retirement, rewrites authoritative outcomes, substitutes aggregate uncertainty counters for command-scoped diagnosis or labels a construction-only example as verified support. Diagnostic API/retention choices still require user approval. |

## Evidence records and assertions

For every row, record: owning contract revision; exact source/lock hashes;
user-approved policy/representation and crate choices; evaluated reuse alternatives
for any custom mechanism; test target and descriptive test
name; pinned-Nix command; profile/feature/topology; deterministic schedule/seed;
expected complete observable trace; actual exit status and assertions; inversion
edit and intended failure; restored positive; and durable artifact location.
An empty field remains an open obligation. The initial table contains no
`passed` claim and no invented future executable path.

An assertion is independent when it compares actual public events and owned
values against the specified law, not labels produced by the same implementation
branch. Record at least request identity and protected scope, admission count,
business effect/reply, acquired outcome evidence, surviving input ownership,
generation and owned-resource counts where applicable. Keep native causes opaque
and exact. Do not open their private payload merely to inspect an implementation.
Record original creation correlations independently from acquired native
retirement routes; do not predict route values or assert identity from allocation
arithmetic. Preserve each descendant's actual owning terminal variant rather
than assuming universal completion. Assert optional residual presence and
custody through owning public APIs where exposed; for opaque values prove
retention, return or explicit discharge through their typed consumer paths.

Perform transitions, sends, polls, cancellation and ownership transfers before
assertions. A failing oracle must demonstrate the stated violation; repeated
assertions of one field, discarded Actions and predicted internal nonces do not
establish the law. Use externally issued correlation values from the approved
contract, not knowledge of internal allocation arithmetic.

Every generation/replay/lifecycle row runs focused debug and optimized controls
and intended inversions. Where the contract prohibits reacceptance, replay the
same issued identity/fact against the same consuming operation and prove no
second acceptance there; independent typed consumers still coexist. Distinct
fresh IDs prove a separate terminal-refusal case, not same-fact lifecycle or
network replay protection. All other semantic changes also receive their required
debug/optimized focused checks before broadening. Static denials retain positive
counterparts. Tests for unrelated primitive implementation details remain with
their owners; this suite proves the actual integration.

## Combined failures and liveness

Run these in addition to individual rows, reusing the same independent oracle:

| Campaign | Selected sequence and assertions |
| --- | --- |
| C-03 | Rotate simulated identity evidence, partition its provider, revoke the static grant, then release a delayed request. Keep evidence unavailability, freshness and authority separate under the user-selected policy. |
| C-04 | Saturate admission, start shutdown, release a previously pending send, then replay old timer/reply inputs. Each payload remains admitted once or refused/retained truthfully; no later generation changes. |
| C-08 (partial) | Mix incompatible protocol, router restart, unavailable verification and drain expiry. No false readiness, rollback or termination; resources stay bounded. Storage unavailability remains MNE1. |
| C-10 | Repeated loss/duplicate/reorder at finite capacity, then stable communication and fresh authority. Safety holds throughout; newly admitted finite work reaches its declared outcome under fair scheduling. Unknown old mutation is never silently retried. |

C-01/02/05/06/07/09 require durable ownership, storage or restore and stay with
MNE1/PLACE1/DIST1. R22 still tests overlapping responders here without claiming
C-07's durable fencing. Simulated cryptography is not real cryptographic evidence.
Keep per-boundary disposal/panic tests from R18 and processing-before-reply loss
from R13 even when their larger distributed campaign is deferred.

Safety and liveness have separate oracles. Finite waiting limits are harness
failure detectors, not proof that a remote request did not execute. Gate B sets
explicit fair recovery and work bounds before selecting any elapsed-time limit.
Fuzz/property budgets, seed sets, shrink retention and CI time limits must be
recorded before campaign execution; never lower a floor to obtain acceptance.

## Readiness and exit conditions

1. **Design preparation:** read exact owners, run independent probes, and bring
   each new design choice to the user. This work is allowed now; no retained
   production interpretation or dependency selection is authorized by an open gate.
2. **AUTH1 implementation eligibility:** user decisions at I, relevant C/W
   semantic contracts and B bounds pass with an actual local Application
   verification request/result consumer. Serialized wire bytes and live Zenoh
   are not prerequisites for this local proof, preventing an AUTH1/NET1 cycle.
3. **AUTH1 acceptance:** R07–R12's identity/permission obligations pass through
   both deterministic providers and their local application consumer; all ID
   and AUTH rows receive exact evidence, independent review and a reviewed
   merge. Networking-specific forwarding/stop end-to-end witnesses remain NET1
   obligations. Fake evidence does not close SELO1.
4. **NET1 implementation eligibility:** C/I/W/V/B decisions applicable to the
   candidate owner are user-approved and source-verified; the explicit change
   record and scope budget precede production edits. Isolated prototypes need
   not wait for full feature CI. Adapters may develop against a frozen AUTH
   contract; final NET1 acceptance requires the actual compatible AUTH1 delivery.
5. **Feature acceptance:** every applicable R01–R25 obligation and selected
   combined campaign has valid evidence, including actual Linux subprocesses,
   real secure links, supported features and debug/optimized inversions. Partial
   inventory contributions are explicitly retained as partial. No unexecuted
   required test, unresolved policy, missing source consumer or open blocker remains.
6. **Distillation and delivery:** full PRD checks and ownership/API review pass
   on combined exact sources. Required CI/review passes on the submitted head;
   the actual merge and durable evidence are recorded before changing NET1 to
   `merged`. This does not complete PLACE1/MNE1/DIST1/OPS1/SELO1.

The full PRD remains incomplete. The specification audit and partial local
prerequisites do not establish feature-complete or merged AUTH1/NET1.

### Local per-request Read ownership prerequisite

The ordinary Counter test
`separate_read_ports_preserve_cancelled_waits_and_refuse_late_replies` uses actual
public Application ports, one per Read. It independently acquires values0/1/2,
proves cancellation drops only a borrowed pending wait, observes settled-port
closure, and retains the exact late direct-send payload and native ClosedRecipient
settlement. The actor continues; joined retirement records established retirement
and failures found. Work, native and both notification outcomes are acquired
before semantic assertions. The PRD records debug/optimized controls, three
intended inverse pairs, exact restores, combined workspace checks and review.
This is a prerequisite for R04/R06/R13, not their full protected remote witness;
production identity, wire correlation, transport and resource obligations remain.

### Local complete configured Scope prerequisite

The authorized Counter's two concrete provider consumers retain the original
receipt and compare deployment, caller, controller, actor, host, protocol,
runtime generation and actor generation before replay lookup. Current grant
issuer/revision and allowed operation are checked there and on every native
nonwaiting mailbox attempt. Actual Full retries preserve the accepted prefix
and refuse a waiting command after any independently changed grant input.
Both actual Applications acquire their original Work/native/notification results
before effect assertions. Borrowed live-runtime provenance, checked issuance,
old assignment and same-owner proof rotation have local controls.

The PRD records21 tests per profile and19 paired deliberate defects, separating
setup failures from intended oracles and retained-binary restorations from fresh
Cargo checks. These are prerequisites for R03/R08/R12, with no production KERI,
global freshness, durable fencing, actual reconnect or full protected remote
acceptance credit. Combined-source checks and independently reviewed submitted-head
CI passed; [PR350](https://github.com/devrandom-labs/bombay/pull/350) merged
20fb999dfe57f72d85afb1a2ccd07d42942cc6c8 at2026-10-11T14:23:31Z. Actual Nix
executes the21-test suite (19 owning tests plus2 imported Counter laws) in
coverage/debug/release; both native fixture campaigns remain separate evidence.
Observe Miri was skipped. This records only prerequisite delivery; full rows
remain open.

### Native routed-query and requester-wait correction prerequisites

Original native router exhaustion loses an old query's reply consumer in actual
Face routing in debug and optimized, after four task-controller joins and Runtime
close (receipts723d4596/c5edb97f, sourceff032dfa). Private checked issuance
df63e5a6/production+14/-17/net−3 passes that same unchanged regression in both
profiles. Boundaryfb8b0863 adds204 tests within220 selected lines; native complete
queryables ranked by distance exercise all three target modes, MAX once, repeated
exhaustion, another live branch and BestMatching no failover. Exact caller/tag
multiset and pre-duplicate-FINAL Weak/token/map observations follow independent
review72b4b07e. Full forwarding28/28 debug passes; optimized/strict/inversions
and actual reviewed merge/publication remain open. Native FINAL is query stream
completion, not protected actor refusal; no actual timeout-expiry proof is claimed.

Requester Work Err after actual retained Begin and normal local Work Ok both
leave the original exchange Pending: actual post-native negatives execute in
both profiles (2e530d78/f5d071e6). The selected outside-owner borrowed exchange
retains all acquired originals and ends after actual local execution completion;
it preserves unfinished Pending and uncertainty rather than inventing Ready,
remote success or nonexecution. New positive oracles must prove actual normal
return/cleanup/native custody and retained partial facts. Both originals and
actual negative executables remain immutable. Requester correction now retains
the selected originals: combined root40e2/fecac passes all four workspace commands
(641/0/1), focused optimized51 and14 intended custody failures/14 retained healthy
controls. Counter-root outer-service cancellation proof and reviewed delivery
remain open.

These are scoped prerequisites; no full R01–R25 row closes.


Exact-main993 CI38186444808 independently passes all three jobs, with610/0/1
visible release results and26 successful/four deliberate uncertainty native
records per profile. Miri is skipped. Parent-report witness executes in coverage
debug and release; plain debug runs zero tests. This also corrects the earlier
PR353 three-profile claim. Router correction has28 forwarding passes per profile
and protected Counter has51 per profile plus all four combined workspace checks.
Four custody cuts have14 actual intended failures and14 retained healthy controls;
final fresh restoration passes. The feature-dependency compile failure was
corrected through the existing CloseBuilder factory; exact a0762901 local full
release/package verification exits0. Independent archive evidence review and
submitted-head CI remain delivery gates. Final independent protected codec review
also passes, preserving original typed causes without selecting generic
Error::source chaining. No full R01–R25 row closes.

### Historical protected native Read/Stop comparison before successor adoption

Corrected frozen native source4fc88f71, parentf0695664, controller88a26f6f and
lock1d2f9634 pass ten real TLS matrices across both build profiles:64cases,
160 subprocess roles. Both concrete deterministic providers and peer/router
layouts cover roots and children; origin challenges use actual distinguishable
child siblings. Grant acceptance follows an acquired authenticated send-only
refusal; EOF and wrong trusted token keep Stop unauthorized. Read replies retain
actual request-specific originals, and duplicate Stop reuses the actual joined
target report. Both child reports precede parent shutdown during fixture cleanup.
Independent review6223e922 rehashes all160 role logs and original executables.

This is an unmerged cfg(test) comparison. Numeric root addresses in separate
Applications are not global identity proof. Initial optimized EOF failures remain
unmet fixture cleanup prerequisites; their actual native variants were not logged.
Six deliberate source cuts fail their intended post-cleanup oracles in both
profiles:80negative cases and80matched retained-binary healthy controls. Final
receipt65c6744f and independent reviewfd57790c bind exact restoration, fresh55
tests per profile, strict Clippy both and formatting. Combined receipt7801a9b3
records workspace643passed/0failed/1ignored plus build, formatting and strict
Clippy; default workspace runs53 Counter tests and excludes the native feature.
At this historical checkpoint, required CI, dependency adoption and reviewed delivery remain pending. Remote held target-owned work,
outer-service fault custody, combined ingress bounds, a supported non-test network
entrypoint and real KERI remain open. No full R01–R25 row closes.

### Local held child retirement comparison

Intermediate frozen sourcee8d49e7a proves a real Counter activation resource is
held inside its existing proxy/supervisor subtree. Accepted root Stop leaves both
termination and joined retirement pending; a separate public Application performs
useful Increment/Read work while the resource remains held. Cancelling a borrowed
report wait preserves its owner. Releasing the actual activation permits complete
native acquisition and a stable replay of the established report. Both Applications
retain their original work/native/two-stage notification outcomes before assertions.
Creator-local attempt and native child origin are preserved as independent facts.

Final source82183b76 corrects the incomplete recovered-control oracle: the complete
owned slice must contain exactly one recovered Ready activation and no other
alternative. Receipt3e6b6441/campaignb3bfbaaa retains both healthy profiles, four
intended failures, four fresh byte-exact restores, strict Clippy and formatting.
Independent review414af23c/evidencec43f4c4c rehashes every source/log/executable
triple and confirms original outcomes remain owned. Earlier e8d intermediate
receiptbdc62461/campaign370ad57a remains historical. Each intended failure follows
actual native acquisition. The cuts test held-resource ownership and a consumer
distinguishing Stop acceptance from joined retirement; they do not prove a runtime
early-publication defect. This local evidence is included in the scoped PR356 prerequisite merge; it earns
no remote held-target Stop, global identity or full R11 acceptance credit.

### Historical 1.10.8 dependency and combined verification checkpoint

That earlier isolated stage selected published Zenoh1.10.8 and its corrected owning
closure. Independent adoption review84e556cb verifies both complete graphs,
402/324 nodes and394/316 preserved records, all nine registered pins and unchanged
TLS-only features. Registered consumer/owning evidence passes on Rust1.99 and1.88;
independent review93433662 binds112 named query/Config executions. These are scoped
prerequisites, not complete remote actor acceptance.

Independent native fixture review9c886700/evidence18d0cd8d verifies60 cases/
168 worker roles,243 frozen inputs and both Cargo-selected copied binaries.
Late exits preserve uncertainty and failed cleanup; killed roles receive no close
credit. Invalid fixture RELEASE refusals are not TLS authentication denials.
This historical transport fixture is separate from the protected Counter campaigns.

Both owning all-features dependency-policy checks pass. Adopted ordinary root
Counter suites pass54/0/0 in debug and release, including the held witness;
protected and nested suites pass56/0/1 in both profiles, with ignored subprocess
roles earning no network credit. Independent review52013cb4/evidence860a17d3
verifies workspace644/0/1, all64 protected cases/160 roles with exact single
execution and clean exits, and both child-stop pairs2/0/0. Source minimization
29edd721 and endpoint accounting73439a5d find no scoped blocker. [PR356](https://github.com/devrandom-labs/bombay/pull/356) merged2572bf30 after
exact-head review and all six passing checks, with required Nix38322598237.
Actual Linux optimized workspace643/0/1 remains distinct from local644/0/1;
Counter54 executes plain/coverage debug and release, owner floors pass, and
Driver2048/Observe4×1024 finish. Miri is skipped. Native64cases/160roles pass
separately; held controls retain local inversion provenance. All fullR01–R25 stay open.

Persistent private-request ownership now passes the independent32-case replay/
custody campaign and12-case cancellation campaign with exact intended failures
and restored controls. Combined Application clients pass22/0/0 per profile,
strict scoped lint and formatting. Actual Node-associated shipping export,
current mutable authority and full R01–R25/AUTH1/NET1 remain open.
### Historical release0.1.3 preparation checkpoints

Release0.1.3 now tracks successor PR371 on merged main8d809b3d; PR369 was
closed without merging by the release bot. Its initial required check stopped
on the isolated fixture path-package version mismatch before protected Counter tests.
The one-record correction and fresh combined checks are pending delivery; old
PR369 checks do not qualify the successor. All full remote acceptance stays open.

The successor lock correction now passes fresh pinned locked metadata, the
intended stale-lock failure and exact restored control, plus protected Counter
68/0/1 in each profile. Exact-head CI/review/merge/publication remain pending.

Fresh PR371 corrected-head required CI reaches the optimized allocation test
and fails its exact comparison-byte equality (925 versus781), despite debug
passing55 cases. Preserve that failure; release delivery remains blocked.
Owning DHAT stack/window diagnostics are selected before any assertion, bound
or measurement-owner change. No passing retry can substitute for diagnosis.


PR371 diagnostic headbfa passes its actual Nix flake check, but required job
116290114414 is cancelled by the configured90-minute timeout. Driver's cancelled
step and absent artifacts, skipped Observe and Miri receive no completion credit.
The original925-versus781 allocation failure remains unattributed. A separately
calibrated caller-thread measurement correction now applies locally, with one
isolated test dependency and323 unchanged existing package records. Only the
required job's time allowance is selected to increase90→120; campaign scope and
all original test assertions remain intact. Corrected-head execution, review,
CI, merge and publication remain pending; all full remote rows remain open.


The selected local correction passes all eight actual pinned commands: focused
allocation1 per profile, plain55 per profile, protected68/0/1 per profile, strict
isolated all-target/all-feature Clippy and format. Independent source5474865f /
execution6dcc69da reviews pass twice. This qualifies submission of a corrected
PR371 head, not required CI, TLS campaigns, merge or publication. The earlier
144-byte excess remains unattributed and all full remote acceptance stays open.

Current release result: PR371 actually merged75c8db4f with all six checks and
scoped final source/native/Nix reviews passing. The registry archive, tag and267
Rust files for bombay-rs0.1.3 match that reviewed tree; publication is verified.
Earlier failures above retain their original scope and are not overwritten.
PR372 actually mergede2a4e7f3 after all six checks and final scoped reviews passed.
Its per-request native clients and explicit Full-only retry retain focused
controls, intended inversions and exact-restored results in both profiles.
Required Nix38726056219 executes406 remote cases per profile, workspace703/0/1
and bounded Driver/Observe fuzz; conditional Miri is skipped. The separate
concurrent-receive contribution remains unmerged: six current-source cuts fail
their intended assertions in both profiles. Restored affected406 passes in both
profiles before lint-only corrections; final source passes fresh focused controls,
strict workspace Clippy and formatting. Its own CI and delivery remain required.
No full R01–R25, AUTH1 or NET1 obligation closes from these results.


Earlier private startup checkpoint (before PR379 delivery): original owner loss fails deterministically in
both profiles; separate success/refusal publication omissions each fail their
report oracle in both profiles. Exact restored source passes353 owning library
tests per profile. Generic Application correction is submitted separately as
PR379, with reviewed capacity dependency integrated early; actual required CI
and merges remain pending. This closes no full remote acceptance or privacy gate.

The generic startup law delivered in PR379 preserves the actual acquired
affine owner when its Ready producer disposal panics. Prior source fails the
release oracle in both profiles; the merged correction and193-line owning
test preserve cold Application/Work and the allocated panic without a new API.
All six CI checks pass; merged38774de is recorded above.

## R10 — explicit test identity and production refusal (2026-10-23)

**Locally satisfied for the selected milestone.** This assembles the whole R10
row rather than crediting one missing-provider constructor diagnostic. It closes
neither other remote rows nor the broader RELEASE-04 production-provider example.
R10 explicitly leaves the real Selo example outside this milestone. PR392's
application merged as632bf184 after all six checks passed on95c1105a. This
R10 evidence/interface follow-up still needs its own required CI and delivery.

| Required clause | Current owning source and independent observation |
| --- | --- |
| Fake evidence is selected explicitly in tests. | [Command::read](../../../examples/remote-counter/src/main.rs) requires the literal mode `test` or `production-required` after exact Serve5/Call6 arity. There is no default test mode. The trusted assignment separately supplies an explicit finite profile; [Deployment validation](../../../examples/remote-counter/src/deployment.rs) requires it and all three grants/six paired records, without serde defaults. The actual two-program Test journey executes Increment/Read/Stop successfully. |
| Production cannot start using fake or missing required identity. | The application has only the finite test provider; it has no production-capable identity implementation. ProductionRequired therefore returns the bounded provider refusal before constructing Configuration, opening Node, connecting for assignment or declaring Export. Fake-only availability and missing production capability share this truthful refusal. The [process owner](../../../examples/remote-counter/tests/processes.rs) executes both roles, reaps them, observes no assignment acceptance and checks exact exit1/empty stdout/provider diagnostic. The [five CLI cases](../../../examples/remote-counter/tests/processes/arguments.rs) also check role-specific arity and required positive caller duration. |
| The refusal boundary is falsifiable. | Removing only ProductionRequired's return lets the program reach deterministic existing endpoint-metadata refusal. Both profiles then fail the exact provider diagnostic after both real outputs/reaps and directory cleanup: actual `counter Node could not open` instead of the required provider refusal. The initial absent-TLS watchdog attempts are excluded. Healthy/restored controls inspect both roles; the negative reaches only the first role's diagnostic assertion. |
| Both deterministic provider implementations compile with the same port. | The existing cold [construction](../../../crates/bombay/tests/application_clients/network_replay/construction.rs), [replay](../../../crates/bombay/src/application/node/tests/replay.rs) and [proof refusal](../../../crates/bombay/src/application/node/tests/protection.rs) owners actually execute their Recorded/Scheduled cases in both workspace profiles. The construction/replay [provider owner](../../../crates/bombay/tests/application_clients/protocol.rs) and the separate [protection providers](../../../crates/bombay/tests/protected_counter/providers.rs) return distinct nominal receipts and concrete Ready/async futures through the same generic callbacks. No provider registry, downcast, erased future or caller-dependent cast selects them. |

The trusted-provider assumption remains explicit: these tests prove Bombay's
policy interpretation, not cryptography or protection from a malicious trusted
Rust implementation. Callback/receipt generic types remain concrete. Actual
validated Deployment and Receipt fields remain private. Missing-verifier UI
controls strengthen the required-input interface separately; they are not the
whole R10 witness and do not create a dynamic provider-presence requirement.

### Sources and commands

The policy source is main `11087d2af47b385f3a516e69a09db799b381b2092f436e84dcf086eebbb95abb`
and Deployment `816f6b391512b94f1879fa2d7c030f5ed9349990090de806b8c94680a3955e78`.
Final process and arguments owners are respectively `c932574883884647c5797658e9b610202efc1f2fb9a1c5919c96d1b2545d6873`
and `5fe49c99ec3720b8bb35cbdc8b98c34326c35bc36bec4c87e3a109416f4b6771`.
The guard cut is main `f7dd3c694f7a2d2c581ab2fae906dc893cccf8001895071eba6ffa2c159b871c`:
only its ProductionRequired arm continues instead of returning. Exact healthy
source is restored before replay; later directory/lint epochs do not rewrite
that earlier source-bound inversion.

The pinned-shell workspace commands are `cargo test --locked --offline --workspace --all-features`
and the same command with `--release`. The actual `counter-workspace-{debug,release}`
receipts exit0 and explicitly contain all ten names below as `ok`, not filtered:

- `network_replay::construction::recorded_counter_joined_node_issues_export`
- `network_replay::construction::scheduled_counter_joined_node_issues_export`
- `network_replay::construction::recorded_ledger_public_node_issues_export`
- `network_replay::construction::scheduled_ledger_public_node_issues_export`
- `application::node::tests::replay::recorded_counter_node_reuses_first_read_outcome`
- `application::node::tests::replay::scheduled_counter_node_reuses_first_read_outcome`
- `application::node::tests::replay::recorded_ledger_node_reuses_first_read_outcome`
- `application::node::tests::replay::scheduled_ledger_node_reuses_first_read_outcome`
- `application::node::tests::protection::recorded_outbound_request_protection_refuses_untrusted_declarations`
- `application::node::tests::protection::scheduled_outbound_request_protection_refuses_untrusted_declarations`

Current provider/construction/replay/protection hashes are respectively
`c142411edf2197b2b283b52f82667973cf62fe4cad72650d78e66380cb49bbba`,
`c1b5dc839c0d21f857a7a6b30ddbcca4906037e03d8545a46259e25b25051917`,
`39aa7b1a1bd3942991676052c87ae3564e3f267837afac416f26a4f106788da8`
and `b48408f06a8ca6be7289630342fc9716bb38ca84cb6637af4be04720d9e8646c`.
The final two proof-refusal cases use the separately mounted
`protected_counter/providers.rs`, SHA256
`5bc4a39f6f8c39d75b54634cf1db53ab6afe9169c09f708e49ea7047c659c88e`;
the first eight construction/replay cases use the protocol provider above.

Workspace runs precede the directory contraction and bind Cargo.lock
`4edede11cc08549f422a75fdd21d511f5ba1a3732b0d89040fc1f3b910ef2073`
through the sealed final-source review1ae5cd93 in the application archive.
The final package/TempDir runs bind
`af4406dbc4338dfd77e13cc89ffa59d8c85a8da3f2eead9a4c6baf057b85f4ed`.
That later lock adds the already selected tempfile dev edge; provider source
and selected owning dependency versions remain unchanged. The workspace logs
are not described as a recompilation of that final lock.

The final package commands select `-p bombay-example-remote-counter` in each
profile, without a case filter. The guard campaign selects its actual
`production_required_refuses_before_tls_or_assignment` process leaf separately
in each profile, applies only the recorded source cut and restores it before
the four-control replay. Command/harness custody is retained in the owning PRD
records; no optional capture flags are inferred from the raw output.

### Raw receipts and limits

Raw logs are retained under `/tmp/bombay-outbound-client-comparison` and in the
application's recorded evidence custody. SHA256 pairs below are debug/optimized.

| Receipt | Actual result | Raw log SHA256 pair |
| --- | --- | --- |
| `counter-workspace` | Both exit0; all provider construction4/replay4/refusal2 names above execute. | `e155e80d0d00d3196ad863e8c1a52fce094e052af4c912e90c3bcc40215ac1ea` / `38a472cffdd6ae6b82280adf96bcdf9d04d620d4911e960926bd7baae9ff5b8c` |
| `counter-tempdir` | Both exit0; five process parents and three pure tests pass, none filtered. Relevant production/CLI names and ordinary Test success all execute. | `58b784211628ff70cd9e626b5e2c46e7dadb02782f4a646e8f41143d372e3ee8` / `96d982d6819bbe523382c2a0cdf09c0a55c6ef8fca020d4195132cb0d542fa1c` |
| `counter-provider-controlled-inverse` | Both exit101 only at the intended post-cleanup provider-diagnostic assertion, .91/.39s; five other leaves filtered. | `a0d102157fe7d6b53bf655c277f69e6cbb5a6bed23696278580d73e0ec2f6f29` / `141056dd3aed82f783c770f3634587d232955a3c6561df5492d8514604628b86` |
| `counter-provider-restored` | Both exit0, all four selected process/CLI controls pass, two unrelated leaves filtered. | `dadff12809f4bab99045d8b9f45a569227e455de9afd1fa14fa27b6cd767d808` / `21b34f0497020577745b38e2af80f4b3e40d8a6b625c6928719db670eb82c586` |

Two source/evidence passes checked the entire clause mapping, actual named
provider outcomes, shared guard policy, exact source cut, reached negative
oracle, restoration and final source epoch. The actual inverse's second role
diagnostic assertions are unexecuted; acquired outputs do not substitute for
them. No watchdog, setup failure, zero-case binary harness or filtered case
supplies credit. These facts satisfy the selected R10 semantic row without
claiming production authentication, Selo, the complete RELEASE inventory,
required submitted-head CI or merged feature delivery.
