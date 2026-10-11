# Static remote actors and typed admission

Date: 2026-10-08. Owners: Bombay networking and application composition;
identity verification remains provider-owned. Backlog: NET1 and AUTH1, with
explicit static-placement and verification requirements below.

**Status: blocked for Bombay networking implementation.** Authorized dependency
correction and verification stages are recorded below. The selected local
contracts were reverified; remaining identity, protocol, admission and resource
contracts remain decision gates. Independent research, ordinary-Rust probes
and oracle design can continue. AUTH1 remains
`candidate`; NET1 remains `blocked`. This document is a specification, not a
claim that its proposed API compiles or that its acceptance witnesses pass.

Current readiness (2026-10-11 UTC): reviewed local joined-retirement, exact native
nonwaiting admission, child shutdown and held root-work prerequisites have
merged through PRs330–334. PR335 delivered ordinary Application verification
composition and exact native payload/proof research; its full feature witnesses
remain open. The nine controlled dependency copies are published; top-level
1.10.2's Unicode repair is merged in controlled PR5, passed exact-main CI and
is published and verified through a registry-only consumer. Wake custody PR336
merged after independent review and all six CI checks, including native TLS in
both profiles and the full Nix gate.
Actual two-Application Counter integration PR337 and local admission PR338,
stalled-verifier progress PR339 and exact-target overlap PR340 merged after
independent review and all six checks passed. Controlled interest-retirement PR6
also merged after required CI; successor1.10.3 is actually published with all-nine
registry archive and clean consumer verification. Its narrow Bombay adoption
PR341 is reviewed with all six local affected checks passing; submitted-source CI
and actual merge remain gates. Replay comparison controls and intended inversions
are executed, with final fixture refinement/review/integration still pending.
Historical observations below describe their recorded source stage and must not
replace this current readiness.

**Specification audit:** the scope and proof obligations below supersede the
initial draft's aggregate completion claims. [Acceptance](static-remote-actors/acceptance.md)
maps every selected inventory ID to an observable oracle and falsifier. All
full R01–R25 witnesses remain unexecuted; the successful EXEC tests certify
only their existing local laws. The final section records the audit corrections.

## User steering before design choices

Current instruction (2026-10-09 UTC, reaffirmed 2026-10-10 UTC): the user said **"from now on autoselect
the recommended answer everytime there is a question for me till I say
otherwise"**. Adopt the recommended choice without another question or wait,
recording its alternatives, rationale, consequences and remaining uncertainty.
This includes recommended scope checkpoints; record their concrete expanded
surface before production edits. This explicit delegation supersedes the prior
per-choice approval/wait requirement below until revoked. It does not waive
source verification, correctness laws, regressions, cumulative accounting,
review, required CI or actual merge evidence. Do not infer that an unverified
contract or unexecuted witness passes from this delegation.

Historical steering requirement before this delegation:

The user requires an explanation and an opportunity to steer **each new design
choice before adoption**. An agent must present the concrete choice, viable
alternatives, its recommendation, why it prefers that option, effects on the
application API/correctness/performance/compatibility, and what remains unknown.
Ask the user and wait for an explicit answer before retaining the dependent
design, code, dependency selection or migration. Silence, elapsed time, a green
probe and approval of this PRD are not approval of its unresolved choices.

Record the user's answer beside the decision, including its scope and evidence.
Do not ask again for the same already approved choice. Independent inspection,
falsifying probes and work under previously approved decisions may continue.
If an experiment invalidates a selected option or reveals another choice,
return that choice to the user before changing direction. Explain necessary
terms in plain language and ask one coherent decision at a time where possible.

This applies to scope, identity/naming, trust/freshness/replay/ordering policy,
wire format, dependency versions/features, topology, public API, capability
attachment, limits/defaults and shutdown behavior. Existing owning laws remain
requirements; name their source when an option would violate one. Distinguish
those constraints from agent recommendations. Routine edits implementing an
already accepted decision do not require repeating its approval.

The serializer decision below selects JSON. “Wire encoding” is the representation of
typed messages as transmitted bytes and their reconstruction at the receiver;
it is distinct from encryption. Protocol schema, compatibility and remaining
proof-profile rules require the user's choices at W/V; exact protected bytes
are selected below.

Accepted scope decision (2026-10-08): the user selected **include authorized
remote stop**, after being offered inclusion with positive/negative authority
proof versus deferral with NET-13 remaining incomplete. This selects the
operation and its correctness obligation, not its API, wire representation,
transport or implementation. An always-denying implementation cannot pass NET-13.

Accepted topology decision (2026-10-08): the user selected **both direct peers
and router/client connections**, after comparison with router/client-only
support and deferred peers. Run the same actor correctness contract on both;
retain separate security, interruption/reconnect and disposal witnesses for
each layout, including router-restart campaigns. This selects supported layouts,
not a Zenoh version, link/security settings, wire format or application API.

Accepted admission-policy decision (2026-10-08): the user selected **refuse a
waiting request and recheck permission at actor-mailbox admission**. The
gateway's earlier verification/acceptance does not reserve authorization across
a capacity wait. Known revocation or expired applicable evidence before the
selected coherent admission decision must prevent insertion. Preserve already
admitted/executed work; this decision does not add processing-time rechecks,
instantaneous remote-revocation knowledge or a chosen freshness/partition policy.
The local admission boundary is selected below; its clock model and public
primitive projection remain C/I/B design questions and must not be silently
inferred from `send`.

Accepted admission-boundary decision (2026-10-09 UTC): after clarifying that
"serialized" meant one-at-a-time execution rather than JSON serialization,
the user selected **one local check-and-attempt operation**. With remote JSON
already decoded into a typed Rust message, acquire a coherent permission/time
snapshot, check authority and attempt the existing mailbox without waiting for
capacity. Local permission updates cannot interleave with this operation. A
full mailbox retains the exact message; every later attempt requires a fresh
check. This defines the logical admission decision, not permission validity at
an exact physical nanosecond of queue publication. Ordinary local messages need
no JSON conversion; remote serialization cost remains unmeasured. The alternative
was investigation of validation integrated into Communication's queue commit,
whose required port does not currently exist. No mechanism for coordinating
permission updates, clock source/skew rule, retry scheduling, resource limit,
public API or owning-crate change is selected. The existing awaited external
send is insufficient: it can wait after its earlier authority check.

Accepted serializer decision (2026-10-08): the user selected **JSON with
serde_json 1.0.151**, after comparison with Ciborium 0.2.2 (standard compact CBOR)
and Postcard 1.1.3 (compact Rust-oriented encoding). Reuse the already locked
serializer and typed Serde support rather than a handwritten codec. Readability
and avoiding another serializer dependency outweigh larger messages for this
milestone. Schema/version compatibility, authenticated representation, features,
and resource bounds remain separate choices. Candidate library probes are not
feature acceptance or a proof of canonical encoding or bounded verification work.

Accepted metadata-schema decision (2026-10-09 UTC): the user selected
**reject unknown metadata fields**, after comparison with ignoring them for
easier extension. Use the selected Serde/serde_json schema checks for
Bombay-owned request/reply metadata; no custom parser or new crate is selected.
Refuse unsupported metadata before actor admission rather than implying its
requested meaning was honored. Future metadata additions need an explicit
compatibility policy. Actor payload schemas and provider-owned standard proof
formats retain their separately declared profiles; this decision does not
override their rules or select any field layout, public type, version/default
or resource limit. Exact authenticated JSON spelling remains independently
governed by the approved byte-protection rules.

Accepted Zenoh-version decision (2026-10-08): the user selected **the most
recent released version**. GitHub's latest non-prerelease and crates.io's newest
stable release were both checked immediately: **1.10.1**, published 2026-09-07.
Pin that version when its dependent gates permit production integration; do not
silently float to a later release. Its exact archive source revision is
`1211779c3647f5a96713dade452c546a07823580`. The comparison with 1.9.0 considered
the newer reconnect, malformed-input and query-shutdown fixes, source inspection
and pinned-Nix candidate compilation. Features, transport/security settings,
operations, callback ownership and resource limits remain separate decisions.
Sources: [latest release](https://github.com/eclipse-zenoh/zenoh/releases/tag/1.10.1)
and [preceding minor's fixes](https://github.com/eclipse-zenoh/zenoh/releases/tag/1.10.0).

Accepted transport decision (2026-10-08): the user selected **TLS over TCP
only**, after comparison with QUIC-only and both transports. Use Zenoh 1.10.1
with `default-features = false` and `transport_tls`; do not retain other
transport features merely because Zenoh enables them by default. This limits
the milestone's supported transport surface while preserving both approved
connection layouts. QUIC remains future work. Certificate authentication,
trust provisioning, TLS configuration, application-level protection and
resource limits remain separate choices. TLS through a router protects each
connection; it does not hide application payloads from that router.

Accepted connection-authentication decision (2026-10-08): the user selected
**mutual TLS**, after comparison with server-authenticated TLS. Both ends must
present and verify trusted certificates; an unauthenticated connecting node
must be refused before application requests. This adds certificate provisioning
and rotation obligations. Connection credentials do not grant actor permissions
or substitute for the intended KERI-backed application identity/authority.
Trust roots, certificate provisioning, expiration/rotation policy and endpoint
verification settings remain separate decisions. Production KERI verification
remains the separately scoped Selo milestone; its intended use does not remove
TLS's confidentiality requirement.

Accepted TLS-root decision (2026-10-08): the user selected **only explicitly
configured deployment CA roots**, after comparison with configured roots plus
Zenoh's built-in public WebPKI authorities. Enforce the same trust restriction
in both directions; an unrelated public issuer must not authorize a connection.
This is a requirement decision, not approval of a patch or custom TLS stack.
Pristine Zenoh 1.10.1 outgoing trust could not enforce it through supported
configuration at inspection. The subsequently approved owning-crate correction
and current local verification are recorded below; registry delivery and
application integration remain open. Keep crate-owned TLS verification; no
handwritten verifier or transport replacement is selected.

Accepted dependency-correction decision (2026-10-08): after clarification of
the small change and upstream timing, the user selected **our pinned Zenoh copy
with the existing CA-selection fix**. Base it on approved 1.10.1, backporting
the inspected upstream change rather than adopting its 1.10.0 branch. Use the
owner's explicit `use_public_pki` opt-out for Bombay's configured-only policy;
retain the upstream legacy default for other users and fail an empty selected
trust store. Shipping must not depend on upstream merge/release timing. We own
maintenance until a verified official release includes the correction. Exact
source packaging, publication, cumulative scope and any additional testability
design remain separate gates. No patch had been applied at this selection;
the subsequent isolated correction and verification are recorded below.

Accepted registry-delivery decision (2026-10-08): the user selected **normal
crates.io installation with our published corrected dependency copies**, after
comparison with a mandatory consumer-root Cargo override. Bombay's published
networking dependency must resolve the correction without application patches,
Git sources or unpublished paths. Maintain the complete affected owning package
closure and verify a fresh registry-only consumer of the extracted/published
Bombay package. Source location, package names, versions, release ownership,
cumulative scope and actual publication require separate review; this decision
does not authorize publishing packages.

Accepted dependency-feature decision (2026-10-08): the user selected **TLS-only
published copies**, after comparison with preserving every upstream transport
option. The selected TLS profile requires at least seven affected owner packages;
the broader transport surface requires at least sixteen. These copies are
purpose-built for the approved Bombay profile. Reject unsupported transport
features explicitly rather than retaining apparently usable broken options.
Preserve the owning TLS semantics and public-root legacy default unless an
additional change is approved; Bombay explicitly opts out of public roots.
Exact package closure, feature manifests, ordinary installation and router
builds remain to be verified. SHM stays separate research, not an enabled feature
or delivery gate; future transport support requires its own reviewed release.

Accepted transport-exclusion decision (2026-10-08): the user selected **remove
unsupported Cargo options and dependencies while retaining inactive upstream
source**. Remove implicit optional-dependency features and forwarding references
as well as named unsupported transports. Cargo must reject unsupported feature
requests; per-owner `check-cfg` lists only inherited dormant values, preserving
typo diagnostics without enabling those branches or blanket lint suppression.
Use standard Cargo/rustc configuration, with metadata, compile-denial, typo
inversion and packaged-consumer witnesses. This is an approach decision, not
authorization of the later manifest stage. Defaults, other optional features
and network scouting remain separate choices.

Accepted source-location decision (2026-10-08): the user selected **a separate
controlled source repository**, after comparison with vendoring the seven owner
packages into Bombay. Preserve the exact selected upstream base and history,
the isolated trust correction, package relationships and release evidence.
Normalized seven-package archives would import 295 files, including 83,794
production/build Rust lines; keeping source separate avoids that Bombay import
without reducing our maintenance obligation. Count authored changes across both
repositories at cumulative scope checkpoints. Repository/package names,
repository creation, expanded scope and publication remain separate approvals.

Accepted configuration-generator correction decision (2026-10-08): the user
selected **fix the owning generator and deliver its dependency copies**, after
comparison with a configuration-crate style-lint exception. Replace its same-name
field initializer with equivalent Rust shorthand, preserving strict checks.
The ordinary registry release closure now requires at least nine maintained
packages, including corrected `validated_struct`/`validated_struct_macros`
copies. Verify nominal error/trait identities, macro aliases, generated behavior
and extracted consumers; this is no new configuration generator or semantic
implementation. The later safety decision below extends the one-line fix after
inspection exposed invalid uninitialized references. Exact expanded scope,
implementation and external release actions remain separate gates. No lint
suppression was selected.

Accepted generator-source location decision (2026-10-08): the user selected
**keep both corrected dependency copies in `bombay-zenoh`**, after comparison
with two additional controlled repositories. Keep each owner clearly separated
with exact source revisions, license/provenance and release records. Coordinate
the nine-package minimum in that repository. The import includes approximately
684 production Rust source lines plus the owning example; count its full file,
test/documentation and public surface in the next checkpoint rather than
reporting only the one-line emitter replacement. Exact directories, package
names/versions, expanded scope and implementation were pending at this selection;
their subsequent decisions and applied stages are recorded below.

Accepted generator-package naming decision (2026-10-08): the user selected
**`bombay-validated-struct` and `bombay-validated-struct-macros`**, after
comparison with the purpose-specific `bombay-config-validation` family. Preserve
upstream Rust library names and dependency aliases `validated_struct` and
`validated_struct_macros`, including the generated paths. Both proposed names
returned crates.io API HTTP 404; this is not name reservation or publication.
The package closure and versions still require actual release verification.

Accepted initial dependency-version decision (2026-10-08): the user selected
**track upstream base version numbers**, after comparison with an independent
0.1.0 series. First verified releases use 1.10.1 for the seven distinctly named
Zenoh copies and 2.2.0 for the two configuration copies. Record exact upstream
revisions and our changes; numbers do not claim source identity with official
releases. Subsequent controlled changes receive our own version bumps. Actual
publication remains contingent on reviewed, verified package delivery.

Accepted controlled-source naming decision (2026-10-08): the user selected
repository **`devrandom-labs/bombay-zenoh`** and published package prefix
**`bombay-zenoh`**, after comparison with `devrandom-zenoh`. Identify these as
Bombay-maintained copies, preserving upstream Rust import spellings through
Cargo aliases and proving macro compatibility. The seven proposed names are
`bombay-zenoh`, `bombay-zenoh-config`, `bombay-zenoh-link-commons`,
`bombay-zenoh-link-tls`, `bombay-zenoh-link`, `bombay-zenoh-plugin-trait`, and
`bombay-zenoh-transport`; verify the final closure before releases. All seven
returned crates.io API HTTP 404 at inspection; availability is not reservation.
No repository was created and no package published. Initial versioning is
selected above; release ownership, scope and concrete external actions remain
pending.

Accepted trust-testability decision (2026-10-08): the user selected **one private
function for deterministic trust tests**, after comparison with leaving root
construction inline and using a live public-server exclusion campaign. Move the
existing selection into `outgoing_trust_anchors`, consumed by the real TLS client;
retain Rustls verification and the existing root parser. The measured text proposal
adds six net production lines in the same owning file, with no public API or
dependency. Require complete anchor equality, missing-root refusal, legacy
controls and an inversion restoring unconditional public-root seeding. This
approval does not discharge real TLS/Application gates or the scope checkpoint.

Accepted TLS-dependency decision (2026-10-08): the user selected **require
Rustls 0.23.45 and update the dependency lock narrowly**, after comparison with
a full lock refresh. Raise the declared minimum in the corrected packages'
dependency relationships so registry consumers cannot select affected older
Rustls; changing only our lock is insufficient. Preserve other compatible
versions and present any additional necessary changes before adopting them.
0.23.45 is the advisory's fixed version, the currently inspected latest stable
and the earlier probe's version. Manifest/lock retention belongs to a later
scope approval; the six-file constructor stage keeps its original lock specimen.

Accepted protected-representation decision (2026-10-08): the user selected
**protect the exact transmitted JSON bytes**, after comparison with RFC 8785
JCS normalization. Whitespace, field-order or content changes alter the
protected representation; forwarding must preserve the original protected
bytes, and verification must bind the accepted typed request/reply to them.
Use serde_json without an added canonicalizer. This avoids normalization and
the examined JCS candidate's large-integer rounding and duplicate-input gap.
Malformed authenticated JSON still fails schema validation before admission;
byte authentication alone is not permission or replay eligibility. Protected
fields/domain scope, proof packaging, cryptography and finite bounds remain
separate decisions; no signature algorithm or proprietary envelope is selected.
WIRE-02's canonical signing-input obligation remains required. Exact-byte
protection does not itself make payload JSON canonical; the accepted receiver
policy below distinguishes payload spelling from the selected proof's signing
representation. The standard proof representation remains unselected.

Accepted JSON-spelling decision (2026-10-08): the user selected **accept valid
authenticated JSON spellings**, after comparison with one prescribed JSON
spelling. Different whitespace or field order is eligible only with a valid
proof over those actual bytes and successful schema, authorization and replay
checks. Reusing the original proof after any byte alteration must fail; a fresh
valid proof does not bypass replay checks or authorize repeat execution. Require
the selected standard proof/signing representation's canonicality, rather than
normalizing payload JSON. This resolves the receiver-policy ambiguity in R02
without selecting a proof container/algorithm or weakening WIRE-02's oracle.

Accepted proof-format ownership decision (2026-10-08): the user selected
**the provider owns its declared standard proof format**, after comparison with
one mandatory Bombay-selected container for all providers. Bombay preserves the
exact protected JSON and bounded proof bytes; the statically selected provider
verifies its explicit profile and returns typed facts. Peers require compatible
declared profiles, with no automatic downgrade/fallback. Select evaluated owning
crates and strict parsing/canonicality witnesses before retaining any standard
proof integration. Deterministic evidence may prove test-profile byte binding,
but must not claim cryptographic, standard-signature-canonicality or production
KERI credit. No container, algorithm, crate, provider public API or production
KERI implementation is selected by this ownership decision.

Accepted node-identity decision (2026-10-08): the user selected **delegated
node KERI identifiers**, after comparison with independent node identifiers
carrying registration/access credentials. A credential-authorized creator may
establish multiple nodes, each with its own identifier and keys. Its delegator
must approve establishment and later key rotations, adding coordination at
those events. Separate explicit grants define actor/operation access; creation
authority does not grant unrestricted node interaction. Issuer, delegation
scope, permission products and key custody remain separate choices. A stable
node identifier alone does not distinguish runtime restarts or authorize stop.
This selects the intended provider contract, not production KERI implementation
or a new node-registration operation in this static milestone. The identity
direction and executable service wiring have separate decision records.

Accepted service-composition decision (2026-10-08): the user selected **reuse
existing service wiring**, after comparison with research into a new runtime
extension. Actors emit named typed requests through Actions to application-owned
external services; those services call Zenoh or the provider outside Behavior
and return concrete correlated actor messages. Plain Rust function inputs
require the correctly typed result ingress, as the public Application probes
and compile denials establish. No new runtime attachment, effect algebra,
actor trait or macro is selected. Local delivery acceptance and later results
remain distinct; failure/cancellation custody, service polling and bounded
ownership still need their full C/B acceptance witnesses.

Accepted SHM-staging decision (2026-10-08): the user selected **verify shared
memory in parallel for later inclusion**, rather than adding it to this PRD's
delivery gates. ZEN-13 remains an independently measured optional workstream;
the selected production feature/profile does not enable SHM. Actor correctness
must hold without it. Measure complete typed JSON encode/transfer/decode and
memory costs, not transport-only throughput; SHM does not eliminate the selected
serialization or receiver-owned struct reconstruction. Before later inclusion,
prove actual same-host use, protected-byte custody, OS trust, bounds, peer-crash
cleanup and fallback. Explicit provider APIs, settings and numeric limits are
unselected. This staging decision does not grant a new buffer/lifetime API.

## Reuse crates before implementing mechanisms

User-selected engineering policy (2026-10-08): use existing owning primitives
and suitable established crates for standard mechanisms; handwritten substitutes
are a last resort. Bombay specifies its domain protocol (messages, fields,
authority, outcomes and compatibility); it must not invent a proprietary byte
format, serializer/deserializer, parser, cryptographic primitive or transport
stack when an existing crate meets the contract.

For each new mechanism, first check the locked owners, then research suitable
crates at exact candidate versions. Present the user with alternatives and a
recommendation supported by maintained source/tests, semantic fit, failure and
cancellation behavior, resource limits, security record where applicable,
static typing, license, MSRV, dependency/features and interoperability. Popularity
alone is not evidence of correctness. Crate internals do not replace tests at
Bombay's actual integration boundary.

The user chooses before adoption or pinning. Prefer ordinary typed composition
and the selected crate's maintained APIs/derives; add only the domain policy and
integration code the existing owners cannot supply. Do not replace Communication,
Address, Observe, Timers or Behavior with a competing crate-owned runtime.
If no candidate meets a concrete required law, document the evaluated versions,
failing witnesses, upstream contribution/extension options and smallest custom
implementation needed. Explain that exception and obtain the user's explicit
choice before custom production work. A compiler error or shorter example is
not evidence that a handwritten subsystem is necessary.

Accordingly, WIRE requirements below mean **select and integrate an existing
encoding crate**, specify Bombay's typed protocol using it, and verify its
limits. JSON is selected; its protocol/protection rules, proof verification,
request-ID mechanism and other reusable facilities remain open for evaluation.

## Outcome and priority

Two separate OS processes run ordinary Bombay Applications. One explicitly
exports a concrete actor protocol; the other emits a typed remote request
through Actions and receives a correlated typed outcome. Admission verifies
caller, operation, deployment, intended actor and configured host authority.
Malformed, unauthorized, stale, duplicate and unavailable inputs have distinct
outcomes. Lost communication cannot manufacture nonexecution or remote death.
All queues, verification work, declarations and tasks have bounded ownership.

The first executable example is a volatile counter hosted on a statically
assigned node, with increment, read and user-selected separately authorized
stop using the existing actor shutdown policy and exact target authority.
A send-only credential must fail while an
authorized stop actually retires the target. Denying every lifecycle operation
is not evidence for NET-13.
Losing the increment reply
must produce uncertainty, without transparently issuing a second increment.
A read result cannot settle an increment or a request from another caller.
The same scenario runs through deterministic faults and actual Zenoh processes.

Correctness determines the complete scope. Large changes are acceptable when
their laws and evidence require them. Parallel ownership, early contract proofs
and incremental integration reduce elapsed time; weaker tests or premature
completion claims do not. The [EXEC audit](execution-ownership/post-delivery-audit.md)
records the evidence behind this ordering.

This milestone proves a deterministic test identity profile and the provider
substitution contract. It does not certify KERI, make the fake production-safe,
or deliver automatic failover. Production authentication requires a separately
verified configured provider; SELO1 remains a downstream milestone.

## Requirements and completion boundaries

The table selects obligations for this milestone's declared profile, not whole
backlog families. The inventory remains authoritative for each original row.
NET1 and AUTH1 have separate exit conditions in the acceptance document;
one merged component never closes the other's row. Partial contributions below
remain partial even when this milestone ships. No requirement may be silently
dropped because a chosen transport API makes it inconvenient.

| Inventory | Selected scope | Completion boundary |
| --- | --- | --- |
| WIRE-01–12 | Closed exports, crate-based versioned bounded encoding, distinct identities, exact targeting, correlation, source verification and protected scope. | All twelve require witnesses; no custom codec, serialized local Address lease or arbitrary native Rust error. |
| NET-01–14 | Typed outbound and inbound delivery, truthful acknowledgements, pressure, ordering, replay, retirement, observation, independent lifecycle permission and negative application examples. | NET1 closes only after the actual two-process and fault-host suites pass. |
| ID-01–12 | Typed admission with deterministic valid/invalid/stale/unavailable evidence, explicit freshness, rotation and bounded verification. | Provider contract and test profile; cryptographic provider correctness remains SELO1. |
| AUTH-01–08 | Explicit operation permissions, scoped static hosting grants, delegation limits, replay, revocation timing, forwarding provenance and bounded denial detail. | Positive send/read/stop and negative cross-permission witnesses. Unsupported operations deny explicitly. Production cryptographic validation remains SELO1. |
| ZEN-01–12, ZEN-14 | Exact dependency/features, sessions, supported topology, key mapping, ingress, QoS/security configuration, reconnect and cleanup. | Record explicit decisions for optional services. ZEN-13 specialized links remain optional. |
| PLACE-01, PLACE-02, PLACE-05, PLACE-06 | Static assignment, grant-gated publication and stale-generation admission denial. | These witnesses do not complete PLACE1's automatic placement or storage-fencing contract. |
| DISC-01–05, DISC-07–08 | Explicit export metadata, publication/withdrawal, compatibility, authenticity and freshness. | Configured destinations plus authenticated export metadata. DISC-06 dynamic resolution/cache is deferred; adding it requires a scope/acceptance update first. |
| QUERY-01–06 | Single-target command safety, reply authenticity, bounded response lifetime and truthful timeout; target/consolidation settings when queries carry commands. | Partial contribution to the broader query service. If another Zenoh operation is selected, prove the equivalent command laws and retain query-specific implementation as uncompleted; no fan-out/streaming completion credit. |
| SIM-01–05, SIM-07–10 | Isolated hosts; loss, reorder, duplication, expiry, crash and replay; independent traces and real-network equivalents. | SIM-04 covers volatile processing/crash only, not durable commit; SIM-05 uses simulated key evidence. SIM-06 and SIM-04 durable cases remain MNE1; cryptographic cases remain SELO1. |
| OBS-02, OBS-05, OBS-08; RELEASE-02, RELEASE-04–07 | Bounded network diagnostics, uncertainty visibility, local-to-remote examples, explicit profiles and supported feature combinations. | No whole-platform operations or release-completion claim. |

Deferred: durable execution/outbox/reminders, automatic ownership transfer,
resource-enforced fencing, distributed subscriptions, general fan-out services,
Kubernetes deployment certification, Selo implementation and additional authoring
macros. Broader inventory requirements remain in the backlog. Deferral never
permits a false success, unsafe retry, unbounded queue or weakened local law.

OBS and RELEASE contributions apply only to this profile. In particular,
RELEASE-04's executable production-provider example and RELEASE-05's broader
platform support evidence remain incomplete. PLACE1, DIST1, OPS1 and SELO1
remain incomplete when this milestone finishes.

## Trust, deployment and progress assumptions

The deterministic identity implementation is selected explicitly in test
configuration. Its accepted/invalid/stale/unavailable results prove Bombay's
policy interpretation, not real signatures, KERI rotation or resistance to a
malicious provider. A production configuration must reject the fake or a missing
required provider before publishing exports. The provider is an explicit trusted
implementation port; compile denials prevent untrusted application inputs from
constructing its verified products, not a malicious trusted provider from lying.

ZEN-08 secure-link and ZEN-09 ACL tests use actual selected Zenoh security
mechanisms and invalid/untrusted credentials. Fake application identity cannot
pass those tests by substitution. Specify hop versus end-to-end protection,
router trust, visible routing metadata and authenticated fields at gates I/W/V.
This milestone's public example remains a test-identity deployment until a
production identity provider is separately verified.

Static placement means one configured hosting assignment per actor scope and
authority generation. No timeout, presence signal or reconnect transfers it.
Reject conflicting assignments detectable in configuration. Under inconsistent
operator configurations or cloned host credentials, this milestone does not
prove global single-writer ownership. Each mutating command must select at most
one authorized host/runtime/actor instance before domain execution; detecting
two replies after two executions is too late. Gate W presents exact-target
versus logical-target resolution choices to the user and proves that the
selected path cannot silently retarget an exact recipient. The generation issuer, restart uniqueness,
expiry clock/skew assumptions and rejection of stale configuration are gate-I/W
decisions with explicit witnesses. Local numeric addresses are never their proof.

Safety holds under the declared crash, delay, loss, duplication, reorder,
partition, stale-evidence and finite-resource fault model. Progress additionally
requires a live polling executor, finite actor/provider work, valid authority,
available capacity and eventual communication. Permanent partition may leave a
request unresolved. Restoring connectivity permits fresh authorized work; it
does not silently retry an uncertain non-idempotent command. A volatile counter
restarts with newly initialized state and a new exact target, without a durable
deduplication or recovery claim.

## Fresh selected-contract verification

Current source stage: main2104559072b4321ab55beaea50dfa3a805a6723e,
root lock936110853718f053d4eda91d6b37db0a87e46385e20ef68add895b4491a06152.
Behavior/Actors/Macros d69f992b and Timers13e884da remain the exact selected
owners; Communication is actual registry0.1.4. Observe and Bombay runtime source
include reviewed delivered retirement/admission changes, so their initial audit
byte-equality observations below are historical. Current owner-source proof hashes
Application interface/execution, installed endpoint, Environment, delivery,
launch, retirement and Entity family. The final .2 transport research lock and
actual registry archive are recorded separately; root production still has no
Zenoh dependency. Initial baseline evidence below does not replace fresh selected
source or establish execution of current feature witnesses.


Original audit baseline: Bombay `de09609e76cdcc6892ccf0e8f7cf3df21834714b`,
clean worktree before this documentation task. Before the first commit, the
requested branch was fast-forwarded to freshly fetched main
`c3afb3011090ac4c9112fd39e4a76980db856df0`. That release-only change updates
Engine to 0.2.2 and Bombay Macros to 0.1.2; every Rust source, owning instruction
and normative document is byte-identical between the two commits. Behavior,
Actors, Address, Communication, Observe, Timers and the pinned Nix lock are
unchanged. The refreshed root Cargo.lock SHA-256 is
`0c2f7ebde1e0f99a84525e2a73e3b3b3ecbbf6b657fb1a81ad46c864fc38ef87`;
Cargo.toml is `02c1d01103bbfc3cb1c1ba61bdee1e05311d7ae2117b14e7fa029b95c63b1e2c`.
Historical probes below retain their original artifact/version identity; source
equivalence does not claim new execution. All seven current documents were
preserved before the fast-forward at
`/var/folders/3t/tds_sn397djg1g8d8c471g780000gn/T/bombay-before-main-refresh-271pv7ab`.
Inspect the lock again when implementation starts. The following source/docs/tests were read for this feature; inspection
does not claim fresh execution of every dependency's test suite.

The specification audit rechecked Cargo.lock, its Timers patch, registry VCS
metadata, current owning APIs and representative tests on 2026-10-08. The
selected Behavior instructions were already fully read in this task and remain
the same revision. All listed local production sources and normative owner
documents remain byte-identical to the baseline. A cached registry Timers 0.1.0
archive is a different revision and supplies no evidence for the Git patch.

| Owner and exact selection | Inspected contracts and witnesses | Consequence |
| --- | --- | --- |
| Behavior 0.23.0; Actors 0.23.0; Macros 0.14.0; all archive VCS `d69f992b371c12ab34e73b18e45b8112c90a1508` | Complete revision AGENTS.md; Core `src/lib.rs`, `src/effects/sending.rs`, `tests/action_item_contract.rs`; Actors `src/lib.rs`, `tests/exact_shutdown_action.rs`. | Reuse Actions, ActionItem, InterpretItem, named products and exact input/reply custody; no second effect algebra or template policy. |
| Address 0.3.0; archive VCS `9f058dc03d1239e1ef5c3147a893b134a1e74a83` | `src/lib.rs` and `tests/reservations.rs`. | Reservation is invisible until publication; local addresses/leases are not distributed identity or authority. |
| Communication 0.1.4; archive VCS `6b6d588e5ea94971590d03ccf063c5b38d07d107` | `src/lib.rs`, `tests/mailbox_retirement.rs`; reviewed owning closure correction and publication below. | Reuse bounded user admission and exact rejected payloads. The trusted control lane is unbounded and must not become raw network ingress. |
| Observe, private source in the baseline Bombay commit | `src/observe/mod.rs`, `external_tests/affine.rs`; EXEC observation disposition. | Reuse acquired-result ownership and independent observation. Network suspicion is not an exact local terminal publication. |
| Timers 0.1.0, root patch and lock Git revision `13e884da7ab41781f52337b0038060e375b00ee0` | `crates/timers/src/lib.rs`, `tests/semantics.rs`. | Reuse current-generation cancellation; process-local monotonic time is not portable expiry evidence. |
| Engine 0.2.2, Bombay Macros 0.1.2 and Bombay 0.1.1 workspace sources | Driver law/strategy; `local/effects/{mod,delivery}.rs`, `application/{execution,interface}.rs`, `address.rs`, `entity/family.rs`; manifests and application custody tests. Rust sources match the original Engine 0.2.1 / Bombay Macros 0.1.1 baseline exactly. | Keep the current affine Driver and paired execution/result ownership; no network-specific Driver loop. |
| JSON serializer | User selected the already locked serde_json 1.0.151; no new serializer dependency. | W/V must still verify actual typed integration, compatibility, authenticated representation and resource bounds. |
| Zenoh 1.10.1; archive VCS `1211779c3647f5a96713dade452c546a07823580` | Version, TLS-over-TCP-only transport and mutual TLS selected: defaults disabled, `transport_tls` enabled. Approved controlled-copy trust/profile correction verified locally; not yet in production Cargo.lock. | V must still verify the approved trust/configuration in Applications, cancellation, buffers, target selection, subprocess security and integration. |
| Mnesis-Bombay, independent sibling evidence | Clean sibling commit `b3fb0441ebcc2144638069c3712a580c92c3a928`; manifest/lock, execution attempts/failures/tests, Bombay exports. Selects Behavior 0.9.5, Bombay 0.1.0, former Entity 0.1.0 and Mnesis/store 0.3.1. | Migration is real independent work, not a compatible integration today. No sibling HEAD is silently adopted. |

Package checksums remain authoritative in [Cargo.lock](../../Cargo.lock).
The pinned [Behavior instructions](https://github.com/devrandom-labs/bombay-behavior/blob/d69f992b371c12ab34e73b18e45b8112c90a1508/AGENTS.md)
and Bombay's [capability contract](../runtime-capability-interfaces.md),
[module ownership](../module-boundaries.md), [Driver law](../driver-law.md)
and [verification strategy](../driver-test-strategy.md) apply throughout.

Fresh isolated research at the same Bombay baseline establishes narrower
readiness facts, without closing any R01–R25 acceptance witness:

- Actual public Application composition with two useful named external services
  passed four research probes in both debug and optimized builds. Complete
  processing receipts distinguish local mailbox acceptance from processed
  replies; removing that wait made the final returned trace fail as intended.
  A further plain-function result binding passed debug/optimized correlation
  tests; removing its typed ingress produced E0599, and substituting another
  operation's result constructor produced E0308. Static result ingress alone
  no longer proves a need for a new interpreter extension. Selection at C and
  complete local delivery/cancellation/liveness obligations remain open.
- The public awaited send suspends at capacity and consumes its original;
  cancelling it drops that original. Communication's existing nonwaiting
  `try_send` returns exact Full/Closed payloads, but ActorRef has no public
  projection. A projection is a candidate, not an approved API or expiry proof.
  Fresh owner inspection confirms that awaited `send` acquires its admission
  permit before waiting; owner closure intentionally lets that acquired send
  finish (`tests/mailbox_retirement.rs`). That permit is not a reservation of
  actor authority. Rechecking permission before creating the awaited send
  therefore cannot meet the approved revocation law. An actual public Application
  counterexample now reproduces in both debug and optimized builds: fill its
  1,024-entry mailbox, poll the candidate send to Pending, record revocation,
  release capacity, and collect the complete 1,025-entry Actions receipt and
  original boxed-payload allocations. Cleanup and joined retirement complete
  before the intended assertion fails: one revoked command was admitted instead
  of zero. The granted control passes in both profiles. The 204-line disposable
  witness SHA-256 is
  `8600a8bf7db5b41ea91e1fb002464a4dd994ab1fc20971d43c36f76810b355c3`.
  This proves the unchanged awaited path is insufficient; it is not a defect in
  Communication's documented permit law or a passing protected-admission gate.
  `try_send` returns exact rejected payloads but takes a mutex and may retry CAS;
  describe it as capacity-nonwaiting, without a bounded execution-time claim.
- Zenoh's default FIFO callback can block a networking thread; its ring callback
  discards oldest entries. Queries create timeout tasks that can outlive early
  replies; unstable cancellation/join APIs must not be assumed available.
  Library-only same-process TLS probes are not Linux subprocess acceptance.
- In the selected Zenoh source, outgoing TLS trust starts with public WebPKI
  roots and adds configured roots; listening mutual-TLS trust uses configured
  roots. A configured CA alone does not prove private-root-only outgoing trust.
  This describes the pristine dependency. The approved correction and its local
  proofs are recorded below; real transport-security gates remain unexecuted.

These probes are disposable isolated worktree evidence. No production source,
workspace manifest or lock changed, and no identity/provider, admission API,
protected representation or resource policy was selected by their results.

The configured-CA-only blocker now has an actual unchanged-source witness in
debug and optimized builds: the existing Zenoh TLS link accepts a public-CA
server at its correct hostname despite an unrelated configured deployment CA.
The requirement assertion therefore fails as intended. Six refined codec
controls and six TLS controls pass in both profiles; their same-process/internal
library scope still supplies no R20 acceptance credit. A separately assembled
Rustls verifier is not a substitute for Zenoh's actual outgoing trust policy.

An existing [upstream CA-selection proposal](https://github.com/eclipse-zenoh/zenoh/pull/2766)
is open and unmerged at head `0b10f9d274c3142aa7ac5730e53a539c1402350e`.
Fresh GitHub release/PR queries on 2026-10-09 UTC confirm that
[1.10.1 remains the latest stable release](https://github.com/eclipse-zenoh/zenoh/releases/tag/1.10.1)
(published 2026-09-07T11:55:18Z), and this proposal remains open with that same
head and no merge date. No official replacement for the corrected copies is
selected from that evidence.
It adds an explicit public-root opt-out while preserving the owner's legacy
default. Its base is version 1.10.0, so directly selecting that branch would
violate the approved 1.10.1 contract. The user approved a controlled pinned copy
with the backported correction, as recorded above. Normal registry installation
and TLS-only published copies in a separate controlled source repository are
now selected. Repository/package identities, owning testability corrections
and local graph/archive verification are recorded in the accepted decisions
and preparation evidence. Registry publication, consumer installation and
Bombay integration remain open.
The correction is implemented in isolated owning source; it has not been
integrated into Bombay or published.

Unchanged-source Cargo metadata comparisons refine the packaging gate:
one coherent Git revision preserves the registry candidate's 302 packages,
20 Zenoh owners, versions, features and edges. Patching only three Git owners
creates ten duplicated registry/Git owner identities instead; it is not an
acceptable default graph. Normalized archive copies avoid that split but carry
the measured vendor footprint. These are graph probes, not compilation of a fix.

Registry delivery has a selected profile but still needs implementation: Cargo substitutes the
registry version when a Git-plus-version dependency is published, and a
dependency's patches do not apply to consumers. A pinned-source deployment,
documented consumer-root patch, and namespaced registered owner releases have
different support obligations. The user selected normal registry-only
distribution of TLS-only copies: seven Zenoh owners plus the two subsequently
approved configuration-generator owners. Preserving the broader upstream
transport feature surface was not selected. Verify an actual extracted-package consumer and the published closure;
a repository Git build cannot certify ordinary registry installation.
Sources: [dependency locations](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#multiple-locations)
and [patch scope](https://doc.rust-lang.org/cargo/reference/overriding-dependencies.html#the-patch-section).

The selected upstream source has a committed lock distinct from the earlier
302-package library probe graph: 565 entries, Rustls 0.23.43, Tokio 1.53.1 and
serde_json 1.0.151. Its untouched lock hash is
`aad65a808c49b3e400840d9c0eea2daefc98058f26c6c178b5aba92dce4128a4`.
Rustls 0.23.43 is affected by
[RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285.html):
0.23.13–0.23.44 accept some TLS 1.3 handshake messages at the wrong encryption
level; 0.23.45 fixes it. The approved isolated constructor/root-store tests
exercise the unchanged source lock without network ingress. They do not select
that graph for deployment. Final source and Bombay integration graphs must use
reviewed fixed versions and repeat owning regressions and real TLS acceptance.
The earlier candidate already resolved Rustls 0.23.45; lock-update breadth and
retention remain separate design/scope gates.

The user selected parallel shared-memory verification with later inclusion.
ZEN-13 remains optional; production inclusion and settings are unselected.
Source inspection distinguishes stable
automatic optimization from unstable explicit provider APIs, the existing
1.10.0 watchdog fix from a stale configuration warning, and real mount-failure
loss from receipt/processing. Same-host trust, protected-byte custody, resource
and crash cleanup need separate witnesses before claiming SHM support.

Parallel owning tests now pass for 41 distinct cases in both debug and optimized
builds (82 executions): six allocator/buffer/segment/metadata targets contribute
33, POSIX array contributes six, and two explicitly selected ignored watchdog
tests contribute two. Commands use pinned Nix, `cargo test -p zenoh-shm --features
test --locked [--release] --jobs 2`, and serial test threads. Ten initially ignored
metadata tests remain unexecuted. These are macOS same-process owner tests;
they do not prove Linux subprocess behavior, TLS/SHM authentication, protected
bytes, crash recovery or performance. Source inspection also identifies an
unchecked descriptor-index path before validity checking; an isolated intended
failing bounds witness is required before SHM adoption. No malformed-descriptor
reproducer or safety correction is claimed, and SHM remains excluded from release.
An attempted preparation of that bounds witness was interrupted by an automated
subagent content filter. Status recovery confirms no native probe, binary or
source edit was produced, and no process remains live. This interruption supplies
no semantic evidence; the bounds finding remains source-only.

## Ownership and preserved information

| Value or operation | Sole owner | Required conservation |
| --- | --- | --- |
| Actor state and emitted operations | Behavior and existing actor templates | Pure folds and complete named Actions; no session, verifier, callback, clock or task in state. |
| Claimed identity and verification evidence | Statically selected provider | Claim, invalidity, stale evidence and unavailable evidence remain distinct; only verified evidence reaches authorization. |
| Permission and hosting decision | Explicit application/deployment policy | Principal, delegation, deployment, actor, operation and authority generation remain bound together. |
| Encoding and remote request custody | Selected encoding crate owns byte conversion; Bombay network capability owns protocol mapping and request custody | Local rejection retains the actual original request; possible delivery preserves uncertainty and correlation. |
| Session and declarations | Bombay application-owned network work using Zenoh | One actor's retirement cannot close unrelated exports; all partial startup and shutdown resources have custody. |
| Actor admission | Existing Communication owns mailbox insertion; Bombay ingress owns verification and policy checks | Authenticated traffic still obeys user pressure and closure; remote identity is not inferred from local User::from. Communication does not verify remote principals. |
| Local lifetime and publication | Existing Bombay, Address and Observe | Export only after committed activation; stale export cannot admit into a replacement incarnation. |
| Volatile deadlines | Existing Timers | Generation-safe completion and cancellation; expiry cannot establish remote nonexecution. |
| Durable completion and writer fencing | Mnesis/mnesis-bombay, later placement composition | Network acknowledgements never assert durable commit or unique cluster ownership. |

## Delivery evidence and irreversible transitions

The following is a protocol specification, not proposed Rust type names or a
second execution state machine. Preserve the strongest acquired evidence and
any coexisting failure. Network knowledge and actual receiver execution are
different: the independent oracle observes both and checks that reported
knowledge never exceeds its evidence.

| Boundary / acquired evidence | Permitted reported conclusion | Required ownership and adverse case |
| --- | --- | --- |
| Original request retained; no operation that may transmit has begun | Definitive local rejection with original request. | Borrow for encoding/validation or otherwise retain the actual value. Non-Clone payload and destructor probes reject decoding/reconstruction as exact recovery. |
| Transport operation may have transmitted, including cancellation during that call | Possible delivery with unknown admission/completion, unless stronger evidence already exists. | Never infer rejection from missing receipt, timeout, local cancellation or disconnect. The transmission boundary must precede any foreign call capable of sending. |
| Authenticated receiver refusal proving this exact attempt was not admitted | Definitive remote refusal for that attempt. | Correlate principal, deployment, target, operation and attempt. This is not recovery of a consumed original Rust value; return it only if still retained locally. Refusal cannot contradict a prior admission for that attempt without a protocol-failure result. |
| Authenticated exact mailbox-admission receipt | Remote admission established; processing may remain unknown. | Later timeout retains the admission evidence. A transport enqueue acknowledgement cannot construct it. |
| Authenticated correlated application reply | The specific application outcome is established. | A read reply cannot settle increment/stop. Processing success is volatile, not durable commit; preserve independent cleanup/receiving failures. |
| Deadline, stop request or shutdown after any of the above | End the local wait according to policy while retaining acquired evidence. | Stop acceptance is not joined retirement. If a terminal observation is exposed, specify its exact scope and authenticated producer; silence never constructs it. |

Before mailbox insertion, verify exact target, operation authority and replay
eligibility at a documented admission boundary. A request waiting for capacity
must not rely on an expired precheck. Gates I/B must establish how authority
updates observed locally before that boundary prevent admission, how concurrent
updates are ordered, and what bounded cached evidence permits during a partition.
No global instantaneous revocation is promised by a local cache. Revocation
after admission does not undo a processed command; any stricter recheck needs
its own actor/application policy and witness.

Within the supported request/session scope, duplicate identity cannot cause a
second mailbox admission; conflicting content rejects. Bounded replay-record
eviction must never make an old request newly eligible. Select expiry/session
rules that reject requests whose freshness can no longer be proved. Reconnect
preserves that rule; restart retires the old session/target generation. Neither
expiry nor restart automatically reissues an uncertain command under a new ID.
The request-ID issuer, exhaustion behavior and replay retention equation must
be proven at W; no particular queue, counter or newtype is selected here.

Gate W must also state ordering for each promised scope (sender, target,
incarnation and connection). The oracle must permit documented reorderings
without accepting stale correlations. Do not infer causal/global ordering from
one successful ordered-link run.

## Decision gates before dependent implementation

| Gate | Required decision and falsifying probe | Blocks |
| --- | --- | --- |
| C: capability composition | Selected existing ActorInterface/ExternalActor service composition; public Application and plain typed result bindings have positive/static-denial feasibility evidence. Complete source settlements, later results, cancellation, exact actor admission/stop and unrelated capability consumption through that path. Retain no new interpreter attachment without a newly proven gap and user choice. | Production retention of the selected service integration and any necessary actor capability projection; NET-02 outbound work. |
| I: identity and admission | Choose actor naming, stable node versus runtime generation, trust root, signed/session-authenticated scope, freshness, static grant issuer, delegation limits, revocation timing and test-profile configuration. Prove raw claims cannot construct trusted inputs. Exercise two independently implemented deterministic providers (for example, fixture-backed evidence and scheduled evidence responses) through the same contract, with meaningful accepted/refused/unavailable behavior. | AUTH1 implementation and trusted remote admission. No Selo or second production verifier dependency. |
| W: wire and outcomes | Explain and obtain the user's choices for request/reply identity, declared protocol schema, version/ordering rules and crate-provided encoding bounds; preserve the delivery/replay/cancellation laws below. Every result has a real protocol consumer. | Encoding-crate integration and remote actor implementation; shared semantic scenario interface. No bespoke codec work is selected. |
| V: selected libraries and networking owner | Compare suitable existing crates and exact versions/features, including the codec and Zenoh's supported topology/security APIs. Present evidence and obtain the user's choices before pinning. Inspect target selection, consolidation, callbacks, allocation, cancellation and reconnect; prove required settings in probes. | Production adapter retention; no guessed API, automatic latest dependency or handwritten replacement for an available mechanism. |
| B: bounds and lifecycle | Use the resource table below to fix finite limits/overflow outcomes and ownership at each await/drop. Prototypes establish feasibility; retained slow-peer, unavailable-verifier and partial-startup witnesses complete acceptance. | Retention of affected resource owners; network startup/retirement acceptance. |

C retains unresolved admission/stop composition questions; the existing service
path is selected, without proof that a new trait is needed.
The current `ApplicationCapabilities` and local Environment are private, while
Behavior's `InterpretItem` is public. A custom Engine test host proves an Engine
port, not availability in ordinary Bombay Application. Prefer existing concrete
composition if it meets the full law. No new wrapper is retained without the
five abstraction-budget answers and ordinary-Rust comparisons required by AGENTS.

`ExternalTarget` remains sealed and its `send_from` path awaits bounded
mailbox capacity. Merged PR331 now supplies `ExternalActor::try_send` for an
exact established recipient, reusing native Full/Closed original recovery;
it does not add stable-family hydration or a capacity reservation. Gate C still
requires fresh authority on every retry, bounded pending sends and cancellation
ownership on the actual public path. ExternalActor's fixed 1,024-slot reply
mailbox is an existing bound, not a bound on arbitrarily many pending futures.
Do not open the sealed trait or add admission machinery without a failing law.

Historical source-bound admission proposal (2026-10-09 UTC), unadopted at
that checkpoint. The [next prerequisite decision](#next-prerequisite-exact-recipient-nonwaiting-service-admission)
records subsequent selection and current verification; the following probe
details remain historical evidence:
compare inherent `ActorRef::try_send_from` and
`ExternalActor::try_send(&EstablishedRecipient<Target>, message)` with extending
the sealed `ExternalTarget` contract. The narrow pair can reuse Communication's
existing Full/Closed exact-payload outcomes and the external actor's allocated
origin. Extending every `ExternalTarget` would also require a new nonwaiting
stable-Entity admission law: current `EntityRef::admit_from` may resolve,
allocate or hydrate and await its affine result, with additional typed failures.
An already-installed Entity mailbox is a different exact endpoint and must not
be confused with that stable-family admission path. No method, error exposure,
trait change, coordination mechanism or Entity scope expansion is selected.
The existing public Application revocation counterexample supplies the semantic
failing regression; ordinary function access cannot extract its private endpoint.
The prepared narrow pair is estimated at production **+65/-2/net +63** across
`local/endpoint.rs`, `application/interface.rs` and `lib.rs`, plus approximately
120–215 owning test lines; drafts are not compiled, formatted or applied.
Public surface would add an external-actor method and deliberate existing-owner
error re-export; whether direct actor delivery is public remains unselected.
Communication's derived error Debug can print the payload, unlike current
Bombay `SendError`; public diagnostic policy must account for that difference.
Existing Closed covers admission closure as well as consumer disappearance.
Do not map Full to the current closed-only `SendError`. No duplicate error type
is justified by the existing Full/Closed law alone.

The prospective public call now has a matched compile-only witness. A 43-line
actual `Application::run_with` control establishes its typed external actor,
obtains the root's exact established recipient and calls ordinary awaited send;
strict Rust 2024 metadata compilation succeeds in both configurations. Changing
only that call to `service.try_send(&destination, message)` produces exactly
one E0599 for missing `ExternalActor::try_send` in each. Control source SHA-256:
`172bfdeb3132debb5f123b033096f77f9a89fe624aa2077b524be4b339e48e96`;
candidate: `20a0cb3f4b42293a00e68a6e8aaa0f5f375d570f16f9640b486fa4e8cfaf9941`.
The actual wrapper is root pinned `nix develop -c python3 -`, invoking rustc
with stdin, `--edition=2024 --crate-type lib --emit=metadata -D warnings`,
`CARGO_PKG_NAME=bombay-rs`, `CARGO_CRATE_NAME=public_service_admission` and
the research Bombay crate as `CARGO_MANIFEST_DIR`. It uses only the previously
recorded exact Bombay externs `ef17c34147750d40` / `8c25a350b882000d` and their
matching debug/release dependency directories; the latter adds opt-level 3
and disabled debug assertions. These are optimized-configured metadata checks,
not optimized execution or Clippy. Initial setup failures receive no gap credit.
Control metadata is ignored target output; candidate errors create no artifact.
Sources, manifests and locks remain unchanged. This proves the public method gap,
not authorization, nonwaiting runtime behavior or full acceptance.

I and W must jointly decide which bytes/fields are authenticated; neither team
may invent an independently incompatible identity product. Timeout/uncertainty,
permissions and serialization are deliberate product policies, not actor-model
laws. Public names and signatures remain undecided until caller experiments and
the official Rust API Guidelines [type-safety](https://rust-lang.github.io/api-guidelines/type-safety.html)
and [visibility/extension](https://rust-lang.github.io/api-guidelines/future-proofing.html)
review pass. No new macro is selected.

Each gate records one accepted contract revision, alternatives, measured syntax,
source-bound probe results, remaining limits, the user's explicit design choice
and technical review. Reviewer acceptance does not substitute for user steering. Start with
ordinary functions/inherent methods, inference, associated types, named products
and existing composition; compare extension traits/builders/typestate only when
their law applies. A design gate may use isolated executable probes before
production retention. It may not require the final production suite to exist
before anyone can implement it. Later failures reopen only affected decisions
and consumers; no blanket repeated research of unchanged accepted contracts.

## Resource bounds and cancellation accounting

Before retaining each resource owner, gate B proposes finite test
configuration and supported production limits, units, overflow result and
release point. These are mandatory missing decisions, not guessed defaults.
The user chooses their values/policy; enabled resources need nonzero capacity.
Test each limit at capacity and one beyond, including coexisting resource use.

| Resource | Required bound and observable assertion |
| --- | --- |
| Wire input and decoding | Bytes, nesting, name/proof length and decoder allocations bounded before expansion; oversized input causes zero actor admissions. Bound compressed expansion too if compression is selected. |
| Pending remote operations | Count and retained bytes bounded across queued, transmitting, waiting and completed-but-unreceived results; no unbounded future/task per request. |
| Verification and evidence | Concurrent work, queued requests, positive/negative cache and delegation depth bounded. Invalid-ID floods do not permanently suppress a newly valid principal. |
| Ingress and reply delivery | Account for Zenoh callbacks/queues, all blocked mailbox sends and external reply mailboxes. Bounded mailbox slots alone do not bound producers retaining payloads. |
| Replay and correlation | Live entries and retained bytes bounded; expiry/eviction never permits duplicate admission. Bound orphan/late-reply work as well. |
| Sessions, exports and tasks | Limit sessions, declarations per export, reconnect attempts in flight and owned tasks. After quiescent disposal, owning counts return to the recorded baseline; allocator caching has a separately measured RSS plateau. |
| Diagnostics | Bound buffers, labels and any per-request inspection retention. Metrics/log delivery cannot block retirement or consume authoritative outcomes. |

Record an aggregate retained-memory upper bound from these terms, including
selected library internal buffering; report OS/socket/allocator overhead
separately. A queue is not considered bounded if a producer creates an
unbounded waiter queue in front of it. Reserve reply/correlation capacity before
admission or select a truthful bounded overflow outcome; never drop an acquired
application outcome while reporting success.

The await/drop inventory must include session open, declaration, verification,
capacity wait, possible transmit, receipt wait, reply acquisition, reconnect and
retirement. For each, name the current owner, cancellation transfer/discharge,
remaining remote uncertainty and result consumer. Retiring one export first
prevents new admission, settles its accepted/pending work under the chosen
policy, retires its exact local endpoint and disposes only its declarations.
Shared sessions stay live for other exports. Reuse the existing runtime's
authoritative retirement ordering rather than introducing a second lifecycle.

## Required laws and adverse witnesses

The [acceptance matrix](static-remote-actors/acceptance.md) is the single
requirement-to-oracle mapping. It specifies positive traces, intended
falsifiers, combined failures, evidence records and distinct AUTH1/NET1 exits.
All full R01–R25 witnesses remain unexecuted. The protocol and ownership laws
above constrain those oracles; a passing aggregate test cannot waive a row.

Cross-check [failure contracts](../prd-backlog/failure-contracts.md) LAW-01–09,
LAW-11, LAW-14, LAW-17–22 and F-13–29/F-32–33 for this profile. Exact local
laws stay with their current owners; durable and production cryptographic
claims remain with the named later milestones. Combined campaigns and their
explicit partial/deferred scope are recorded in the acceptance matrix.

## Parallel execution and critical path

Parallel work is allowed at independently owned contracts and source areas.
It is not permission for concurrent edits to shared production files or for
tests to target different dependency revisions.

| Track | Can start now | After its gates | Shared handoff / write ownership |
| --- | --- | --- | --- |
| Capability and identity | C/I ordinary-Rust probes, admission outcome table, verifier substitution and static denials. | Typed admission and the selected minimal composition. | Own identity contract; one integrator owns any Application/local capability changes. |
| Wire and Zenoh | W/V source research, version/codec comparison, bounded real-session experiments. | Encoding, static exports, remote operations and session lifecycle after C/I/W/V agreement. | Own network implementation; share identity/wire definitions once, never duplicate them. |
| Independent verification | Derive scenario traces, fault schedules, resource oracles and cancellation matrix from these laws. | Fake host and real-process contract suites after W freezes; fuzz/inversions as implementations become runnable. | Own tests; oracle must not copy production branches or assert unavailable guarantees. |
| Durable local execution (MNE1) | Own PRD, fresh Mnesis/backend audit and migration probes in mnesis-bombay against the selected published Behavior. | Preserve Once/Replay/conflict/uncertain commit; current Bombay integration after its graph and any shared C seam pass. | Separate repository/worktree; shared extension changes go through the same integrator. No wait for Zenoh. |

Suggested scheduling with three workers: capability/identity, wire/Zenoh and
verification; a coordinator integrates. With a separate repository worker,
MNE1 proceeds concurrently. If capacity is limited, alternate MNE1 preparation
with blocked networking decisions; do not launch enough heavy builds to exhaust
the host. This is a work allocation plan, not a claim of measured speedup.

The ordering is:

1. Establish one baseline and independent expected traces; prepare C/I/W/V in
   parallel, with I/W agreement and C acceptance as shared integration gates.
2. Implement verified identity, codec/session work and test-host work in separate
   branches. Integrate the smallest complete public Application round trip early.
3. Run fault, pressure, replay, compatibility and teardown campaigns while
   completing the full selected scope. Reject contract drift before broad migration.
4. Freeze the combined source and run the complete acceptance/minimization/CI
   gates. Record reviewed merges separately for AUTH1 and NET1; neither closes
   merely because its producer compiles.
5. Combine completed NET1, AUTH1 and MNE1 with an independently selected
   placement/fencing authority for PLACE1, then DIST1 and OPS1. Placement law
   research may start earlier; its safety claim waits for those real consumers.

Each contribution supplies its base and head, changed ownership surface,
complete typed trace, original-defect/inversion result and exact commands.
One integrator owns shared manifests, Cargo.lock, public exports, Application
composition, backlog state and the user decision register. Parallel workers
submit alternatives/evidence to that integrator; they do not adopt conflicting
answers or bury unresolved user choices in branch-local defaults.
Rebase on accepted contract changes; invalidate
affected evidence explicitly. Reuse unchanged source-bound evidence, while
retaining the complete final integration checks. Independent review may run
alongside implementation on a frozen contribution; integration remains ordered.

## Verification and delivery

Run all Rust/tool commands through the pinned Nix shell. First run focused
positive, boundary, cancellation and intended-negative tests in debug and
optimized profiles. Compile-fail tests cover forged authority, wrong protocol,
missing interpreter/consumer and lifecycle escalation. Compare complete typed
Actions or independent external traces, including coexisting failures.

Add bounded deterministic/property/fuzz campaigns, replayable failure artifacts,
actual two-process tests on Linux, selected secure-link configuration tests,
startup/reconnect/teardown resource measurements and supported feature builds.
Use Loom only for new synchronization owned here; reuse upstream primitive
models. Preserve existing coverage/mutation floors and source-bound evidence.
Compiler failure or an unrelated assertion does not count as a killed inversion.

Final required commands include `nix develop -c cargo build --workspace`,
`nix develop -c cargo test --workspace`,
`nix develop -c cargo fmt --all -- --check`,
`nix develop -c cargo clippy --workspace --all-targets -- -D warnings`,
`nix develop -c cargo nextest run --workspace`, and `nix flake check -L`.
Add the selected networking feature matrix and real-process campaigns to the
required checks before acceptance. Measure supported resource configurations;
do not promise progress for an unbounded fold or a destroyed host.

Minimization must prove each retained public type and owner, audit all examples,
benchmarks, tests, diagnostics, research probes, docs and re-exports, and remove
obsolete competing spellings. Complete review and exact-head CI before merging
focused PRs to main. Record actual merge commits, durable evidence links and
remaining inventory rows. A finite campaign is evidence for stated laws and
assumptions, never proof of every possible distributed execution.

## Change record and current checkpoint

Delivery has begun from the preserved post-EXEC audit and specification work,
with independent isolated research and explicit decisions above. The root
cumulative change contains seven documentation paths, zero retained
production/test changes and zero public types. This feature has no manifest or
lock delta against current main; its release-only baseline refresh is recorded
above.
The [audit checkpoint](execution-ownership/post-delivery-audit.md#verification-and-change-record)
owns its complete tracked/untracked counts. Isolated research remains separately
measured and must be included in the cumulative checkpoint before retention.

Documentation delivery: [PR #329](https://github.com/devrandom-labs/bombay/pull/329)
merged to main at `2026-10-09T04:04:46Z`, commit
`a9c5b7d4a1cf2b15504acef43c20c0da9b5e320e`. Its reviewed head is
`80d888dea26e812da4e955ae3aa7ec9ee76400eb`; the merged tree is identical.
This delivers the specification and post-delivery audit, not AUTH1/NET1 feature
implementation or acceptance. Independent agent document review is recorded
in the [final-head review](https://github.com/devrandom-labs/bombay/pull/329#pullrequestreview-5465408768);
this is a comment review, not a human approval. All four observed checks passed:
[Nix CI](https://github.com/devrandom-labs/bombay/actions/runs/37879168840),
[Rust analysis](https://github.com/devrandom-labs/bombay/actions/runs/37879168827),
[dependency policy](https://github.com/devrandom-labs/bombay/actions/runs/37879168839)
and [CodeQL](https://github.com/devrandom-labs/bombay/runs/113656003567).
The Nix job passed the flake check and both bounded Driver/Observe fuzz campaigns
and artifact uploads. Its optional Observe Miri campaign and upload were skipped;
they supply no Miri evidence. Existing local-runtime campaigns are not remote
actor witnesses. AUTH1 stays `candidate`, NET1 stays `blocked`, and all full
R01–R25 witnesses remain unexecuted. Review of the initial frozen head independently
confirmed all 93 inventory IDs, 25 witnesses and 12 reciprocal edges across
eight acyclic programme rows. It found the unmarked historical first-correction
proposal below; that wording is corrected without reopening any approved choice.
Required checks must pass on the final document head before preparation delivery.
The Nix check on `4108e11` subsequently failed its existing obsolete-guidance
guard because historical probe notes named a retired derive. The exact guard
reproduced that intended failure locally in debug and optimized builds, each
exit 101 identifying only this document. The notes now retain the direct owning
macro dependency limitation while retiring that authoring form. No test or
production source changed. Root pinned-Nix `cargo test --locked [--release]
-p bombay-engine --test law_manifest` passes all nine tests in each build on
Engine 0.2.2. The successful final-head remote CI above closes preparation
delivery's documentation failure, without supplying networking acceptance.
Local verification also recovered from a full-disk error before Rust execution:
only the superseded stock-Zenoh candidate's marked Cargo cache was removed
(approximately 2.8 GiB). Candidate sources, fixtures, locks and the nine verified
controlled archives remain retained; final local law checks pass in both builds.
The next branch, `feat/static-remote-actors-implementation`, starts at the fetched
merge commit above. The original feature branch and all source/probe evidence
are preserved. This merge record does not authorize an unresolved design or
expand the approved production stage.

The merge-record follow-up against `a9c5b7d` changes three already-accounted
documentation paths: production/test/public API delta zero; documentation
`+45 / -14 / net +31`. Pinned-Nix law-manifest tests pass all nine in both builds.
All 47 relative file links resolve; `git diff --check` passes.

Pre-production cumulative scope checkpoint (2026-10-08), authorized by the user:
the root and three isolated research trees contain 97 distinct non-snapshot
paths: documents, probes, fixtures, candidate manifests/locks and generated
metadata. They additionally contain 2,332 unchanged downloaded dependency-source
snapshot paths, inspected as evidence rather than retained production. Initial
seven-path documentation copies are counted once across worktrees; no root
production/test/manifest/lock or public type has changed. This preparation exceeds
the 15-file stop threshold, even without counting source snapshots.

The proposed first correction stage is limited to the six listed owning Zenoh
paths, preserving the complete upstream backport including its disabled QUIC
helper. Apply the approved private extraction in the same TLS file. Expected
production delta: net +44 (upstream +46/-8, then extraction +28/-22, measured
separately; measure the combined final diff after integration). Tests: upstream
+159/-9/net +150 plus at most 150 new constructor/root-anchor regression lines,
for net at most +300. Documentation: upstream +3 plus decision/evidence records.
Zero new public types/dependencies/verifiers; one existing approved upstream
configuration field and endpoint key. Exact patch applies cleanly to selected
1.10.1 in `git apply --check` before the implementation below.

Accepted envelope: current preparation and this correction stage, at most 110
non-snapshot paths cumulatively, at most 50 net new production lines for this
stage, and at most 300 net new owning test lines. Keep unchanged downloaded
snapshots separately accounted and unretained. This does not authorize namespace
manifest changes, package versions, router packaging, remote repository creation,
publication, Bombay networking production or any other unresolved design.
Another checkpoint is required before broadening beyond this envelope.
The user's explicit answer was "Authorize this bounded correction stage".
Implementation and owning verification are recorded below; authorization is
not feature acceptance evidence.

First correction stage, implemented and restored after inversions (2026-10-08):
local source `/tmp/bombay-zenoh-trust-correction` starts at selected commit
`1211779c3647f5a96713dade452c546a07823580`, tree
`8e35caa81448ac72ef2a21dafd397751a3524f17`. Exactly six owning paths changed:
production +58/-14/net +44; tests +299/-9/net +290; documentation +3;
zero new public types/dependencies. The actual new TLS test module is 140 lines.
The committed lock remains byte-identical at the hash recorded above.

The pristine constructor witness failed because configured-only construction
unexpectedly succeeded; its legacy public-root control passed. Corrected owning
tests passed 7/7 in debug and optimized builds. Restoring unconditional public-root
seeding made three configured-only tests fail in each build while four controls
passed; byte-exact restoration then passed 7/7 again in both builds. Failed tests
cover exact deployment anchors, empty/missing configured trust and typed
session-to-endpoint propagation. Restored TLS source hash:
`06232af498fd893e339494ed759c88887eb5d9086a01b46b03f1981acb89bafb`;
complete source diff hash:
`12f1744332b43858dbc4dbbb13c4e13e6672dbd932301516ebcf48090f44fce9`.

Executed from that owning checkout through Bombay's pinned Nix:

```text
nix develop /Users/joel/orca/workspaces/bombay/main-2 -c cargo test --manifest-path /tmp/bombay-zenoh-trust-correction/Cargo.toml -p zenoh-link-tls --locked --jobs 2 outgoing_tls_trust_tests -- --test-threads=1
nix develop /Users/joel/orca/workspaces/bombay/main-2 -c cargo test --manifest-path /tmp/bombay-zenoh-trust-correction/Cargo.toml -p zenoh-link-tls --locked --release --jobs 2 outgoing_tls_trust_tests -- --test-threads=1
```

Selected five Rust files passed pinned-Nix rustfmt check at upstream edition
2021; `git diff --check` passed. Coordinator strict Clippy of the TLS library/tests
with `--no-deps` passed. Without `--no-deps`, it failed with 167
`redundant_field_names` errors in dependency `zenoh-config`'s generated code;
pristine-source comparison reproduced 166 such errors with the same unchanged
lock. The new optional field adds the 167th expansion warning. Locked
`validated_struct_macros` 2.2.0 emits redundant same-name field initializers;
its current published/latest source has no inspected fix. No suppression or
unrelated rewrite was applied. Source lint compatibility remains unresolved
for delivery; bypassing dependency linting does not resolve that prerequisite.

Generated-configuration lint correction has a selected owning fix but is not
implemented. The exact
locked macro revision `3eebb3c2e9147ac6c7c8cde987d89b22c9ab098a` emits
`quote! {#field_name: #id}` from the same identifier; proposed shorthand replaces
one line (+1/-1/net 0), preserving the Rust construction law without new public
types or runtime semantics. Both owning packages are currently 2.2.0; no newer
inspected release/master fix exists. Their existing input attributes do not
decorate the generated constructor. A crate/module lint exception also exempts
handwritten future literals in that scope. Normal consumer installation of a
patched generator needs its owning dependency closure, at least two additional
published copies, with nominal public error/trait identity and macro-alias
proofs. No patch, new owner source, package name or release was adopted here.

These are owning constructor/root-store witnesses without network ingress.
The upstream transport open/close tests, real TLS subprocess/Application gates,
R20 and registry consumers remain unexecuted for corrected source. Do not assign
new feature acceptance credit or merged status from this stage. Namespace,
secure deployment graph and publication remain subsequent gates.

New configuration-generator safety blocker (2026-10-08): complete inspection
of the selected 2.2.0 macro source found `StructSpec::flatten_go` constructs a
mutable `StructSpec` reference from uninitialized `MaybeUninit` storage before
calling `mem::swap`. The selected source is the previously recorded exact macro
revision. Rust's [pointer validity rules](https://doc.rust-lang.org/std/mem/union.MaybeUninit.html#method.as_mut_ptr)
explicitly forbid turning that pointer into a reference before initialization.
This affects nested macro expansion during compilation, independently of the
redundant-field style warning. Existing successful builds are not memory-safety
proof; no Miri regression has yet been executed.

Reopen the generator change record because this newly verified gap changes the
previous one-line-only correction. Keep import/implementation blocked pending
a separately explained and approved safe ownership transformation. Compare
standard safe moves over existing `syn::Punctuated` fields, preserving comma
tokens/order/attributes, with safe AST cloning. Neither requires a new macro,
state model or dependency. Do not treat unsafe original execution as a semantic
oracle; require independent expected nested structure, typed validation traces
and actual owning-helper memory-safety evidence where the pinned toolchain permits.
Update measured scope before retention; the submitted 130-path/750-line envelope
alone does not approve this new design.

Independent source reviews confirmed the invalid reference occurs before the
first swap; restoring the value afterward cannot establish validity, and an
unwind can encounter invalid owned fields. The actual correction build selects
Syn 2.0.119, whose `Punctuated::into_pairs`, `Pair::into_tuple`, `Pair::new` and
pair-preserving collection support a safe owned transformation. The package's
historical embedded lock selects 2.0.106 and is not this build contract.
Text-only candidate measurements, not implemented or compiled: safe movement
+15/-12/net +3; safe nested cloning +2/-8/net -6. Movement avoids additional
subtree copies; the existing algorithm already clones completed output nodes,
so this is not a claimed change in asymptotic complexity. Neither alternative
adds public types, dependencies or a placeholder state.

Accepted generator-safety decision (2026-10-08): the user selected **move
existing fields safely**, after comparison with cloning each nested structure.
Use the existing owning Rust/Syn APIs to preserve field order, attributes and
each original comma token while recursively moving valid nested structures.
Remove the uninitialized reference/swap sequence; do not introduce a dummy
structure, new public API, dependency or generator framework. The estimate is
net +3 production lines in the existing macro file; implementation and safety
verification remain unexecuted. The earlier stage-scope question is superseded
pending a revised checkpoint that includes this repair and its required tests.

Required safety oracle: a private unit test invokes the actual owning
`StructSpec::flatten_go` on a nested parsed declaration under Miri, Rust's
memory-safety interpreter. Bombay already pins an opt-in Miri shell at
nightly 2026-06-15; its toolchain is not installed locally and no safety test
has run. Public macro compilation alone does not interpret the generator's
compiler-host execution. Restore the unsafe implementation only as an expected
failing safety experiment. Independently expected complete structure must
prove child-before-parent and sibling order, transformed nested fields,
visibility/constraints, local/inherited attribute isolation and preservation of
original punctuation. Compare the initializer spelling only between safe
expansions. Rerun the seven TLS witnesses in both builds on the corrected owner
and secure dependency graph; earlier passing executions retain their recorded
unsafe-generator provenance and are not a soundness proof.

Separate read-only public-API finding: `validated_struct::split_once` accepts a
`char` separator but advances one byte, which would slice inside a multibyte
UTF-8 separator. Current generator separators are ASCII; no failing owning
regression has run and no repair is selected. Keep this independent gap visible
for the package audit; do not silently include it in the safety transformation.

Additional inherited contracts observed by source inspection, without an
executed regression: parsed structure visibility is ignored by the generator;
runtime `serde_json` alone does not enable generated JSON access (`json_get`
does); generated paths require the original dependency aliases. Constructors
and setters check local predicates, while checked deserialization checks nested
predicates. Nested map insertion delegates to a child without rechecking its
parent's predicate; selected Zenoh queue-size and username/password declarations
have cross-field predicates. Do not claim arbitrary configuration mutation
preserves recursive validity. Record and isolate these gaps for subsequent
owning regressions/decisions; none is silently repaired or accepted as delivery
evidence by this stage. Unchecked Serde deserialization remains distinct from
the owner's checked `from_deserializer` path.

Revised configuration-owner checkpoint (2026-10-08), authorized by the user:

- Fresh complete tracked/untracked union across Bombay and all three research
  trees: 107 distinct preparation paths plus the six corrected Zenoh paths,
  totaling 113 non-snapshot paths. Additionally, 2,442 downloaded dependency
  snapshot paths are separately accounted research input, not retained source
  changes. The earlier 110-path envelope is exceeded by preparation; no further
  production edit is authorized from it.
- Import the two selected owner packages into the already selected separated
  directories, with their four production source files, existing example,
  active/original manifests, upstream VCS records, two licenses and provenance:
  14 paths. Apply the selected safe field movement and initializer shorthand.
  Preserve original library targets/dependency aliases and selected initial
  versions. Add the two owners to the local controlled workspace and update its
  manifest/lock, including Rustls's selected declared minimum and narrow update.
- Mandatory import conformance: remove the two generated inline `Any` imports,
  qualify their single standard-library cast, and move the example's existing
  import to module scope under its existing condition. Inject no caller-scope
  import and add no public API. This follows AGENTS.md's import-placement rule;
  it is not a replacement generator or changed validation policy.
- Expected production: import +684; safe movement +15/-12/net +3; shorthand
  +1/-1/net 0; generated import conformance +1/-3/net -2. Stage production
  +701/-16/net +685; cumulative including trust correction +759/-30/net +729.
  Retain the four inherited public declarations and existing validator macro
  export already listed below; add zero handwritten public abstractions.
- Required tests: actual private-helper Miri witness and intended unsafe-source
  inversion; independent complete nested-shape/punctuation/attribute assertions
  and semantic inversions; safe constructor spelling equivalence; exact
  accepted/rejected allocations and unchanged rejected state; checked nested
  deserialization and JSON-map observations; external package-alias/nominal
  identity positive and negative compilation. Use existing `json_get`/`json5`
  profile for these observations without silently redesigning feature ownership.
  Preserve broader compatibility claims as unproved. Rerun seven TLS witnesses
  debug/optimized and required strict owning Clippy on the resulting graph.
- Revised requested envelope: at most 140 non-snapshot paths cumulatively;
  at most 750 net owning production lines cumulatively; at most 400 stage
  test/example/consumer-fixture lines, including the inherited example and
  fixture manifests. Expected path total is approximately 132 including one
  external consumer manifest/source/generated lock. Use Bombay's existing
  pinned Nix shells, including its on-demand Miri shell; no new toolchain pin,
  dependency or verifier is selected. Stop if an additional version/contract
  repair or any measured budget expansion becomes necessary.
- Exclusions remain remote repository creation, publication, seven-owner Zenoh
  namespace/profile/default/scouting changes, the separate inherited validation
  gaps, actor/admission/public protocol design and Bombay networking production.
  This approval authorizes this concrete source/verification stage only,
  not feature acceptance, registry delivery or merged status. The user's explicit
  answer was "I FUCKING APPROVE IT!" following the explanation of these limits.

Configuration-owner stage progress (2026-10-08): the 14 selected source,
manifest, upstream custody, license and provenance paths are imported in the
local controlled checkout. Independent review matched their original source
and archive checksums. The lock remains 565 packages: only original runtime and
macro identities are replaced by the selected Bombay names, and Rustls moves
0.23.43 to approved 0.23.45; all other versions/checksums remain unchanged.
Pinned-Nix metadata verifies explicit original library names/aliases. The
reviewed actual private-helper tests add 159 lines; the separate external
consumer currently has 105 Rust/manifest lines and has not compiled or run.

Pinned Miri nightly 2026-06-15 is now installed. The original owning unit-test
command compiled and exited zero but explicitly reported that procedural-macro
unit tests are unsupported; **zero tests executed**. This is no safety pass,
inversion or defect falsifier. The selected safe repair remains unimplemented
pending a way to execute the actual original helper before correction.

Test-target comparison, rejected in favor of preparing the shared-module
alternative: use Cargo's standard
integration-test target with a three-line `tests/structure.rs` that loads the
same owning `src/lib.rs`. Guard only its procedural-macro entrypoint and
TokenStream import with `cfg(not(test))`, expected +2 production lines; normal
macro dependency builds preserve both. The parser/structures/flattening code
is shared verbatim, not copied into a model. Existing private tests execute
under Miri in the ordinary target. The alternative extracts the owning syntax
model into a shared private production module, requiring a larger refactor
and measured proposal. The user selected **prepare the shared-module
alternative**; no split or entrypoint guards have been implemented. Prepare the
exact private ownership, visibility, source/test delta and actual-source Miri
target for review before adoption. The standard-target proposal
fits the approved 140-path/750-production/400-fixture envelope; independently
verify actual test counts and deny skipped execution before credit. References:
[Cargo integration targets](https://doc.rust-lang.org/cargo/reference/cargo-targets.html#integration-tests)
and [Rust test configuration](https://doc.rust-lang.org/reference/conditional-compilation.html#test).

Accepted shared-module decision (2026-10-08): the user selected **implement
the prepared shared module**, after reviewing its private ownership and measured
scope. Move the
existing `FieldType`, `FieldSpec`, `StructSpec`, flattening, parser and display
into private `src/structure.rs` with parser/display children under
`src/structure/`. Root code retains macro entrypoint and emission. The same
three existing syntax types cross that private module boundary; expose only
the fields/methods actually consumed by root emission using `pub(super)`.
Keep structure visibility/recursive attributes, validated-map flag and
`flatten_go` private. Existing owning tests move with their syntax owner.

Text-only independent measurements put the partition at approximately +10
production lines, expected cumulative +739 after selected repairs, within
the authorized +750 ceiling. Count old and new relocation paths: current 132
non-snapshot paths become approximately 136 after the shared owner, two moved
children and one standard integration target. The target loads the actual
structure module; no source model or macro implementation is copied. Native
tests cover emission/constructor spelling, while Miri covers actual tree
transformation. Semantic projection assertions must exercise leaf types and
recursive-accessor policy, rather than adding no-op uses or suppressing unused
code. Require measured total stage fixtures within 400 lines. This introduces
no public type/dependency/feature/entrypoint guards. The approved split and
actual-source safety verification remain to be executed; stop if the measured
scope or retained interface exceeds the approved envelope.

Owning lint conformance discovered during the authorized stage: strict Clippy
reports one inherited redundant borrow in the structure display implementation.
Remove `&` from the existing `write!` argument; Rust's formatting machinery
borrows it already. This is a one-line +1/-1/net 0 conformance edit in the already
imported/moved display file, preserving its formatting, API and ownership.
The owning example reports the same redundant formatting borrow; remove its
`&` in the existing `println!` argument too (+1/-1/net 0 example lines).
No suppression, new design or scope expansion is selected.

Configuration-owner stage, implemented and locally verified (2026-10-08):
the shared syntax owner is `src/structure/mod.rs`, with unchanged parsing
and the formatting-conformance edit in its display child. The ordinary test
target loads that exact owner. The first `structure.rs` loader failed module
resolution before execution; moving to the repository's `mod.rs` convention
preserved the approved ownership and resolved both native/test paths. Neither
that compile failure nor the earlier Miri unit-test skip earns test credit.

Actual Miri negative executed one nested-helper test and reported uninitialized
`StructSpec` storage at the original swap (`attrs.buf.inner.cap.0`), with the
owning `flatten_go` in its backtrace. The selected safe transformation uses
standard owned `Punctuated` pairs and an exhaustive field-type match, retaining
original punctuation. No handwritten unsafe remains in these owning sources.
Corrected final Miri target passed both tests; native macro tests passed three
library tests plus two source-loaded tests in debug and optimized builds.

Independent complete-shape inversions each failed exactly the intended test
in both builds: omitted attribute-stack retirement leaked parent/sibling
attributes; prepending completed structures violated postorder; dropping final
punctuation mismatched `None` against the required comma. Eight positive
controls, including restoration after each mutation, passed. Model restoration
hash: `0e7f37c11e212c12cc868815a31fb8bc62b560647a85998a2fdb8e319cf96cf6`.

Both safe initializer spellings passed the unchanged constructor-semantic
witness in both builds. Restoring the redundant initializer caused exactly
167 `clippy::redundant_field_names` errors; restoring shorthand passed strict
configuration Clippy. No unsafe execution served as a semantic oracle.
Final emitter hash: `04e810154e340146dafc7b046f70372f705ad6f5b43ce77e98e1d86004c70127`.

The external consumer at `/tmp/bombay-zenoh-configuration-consumer` passed its
20 observable assertions in both builds, then again after the intended wrong
error-type inversion failed with one E0308 in each build. It exercises actual
Zenoh configuration trait/error reexports alongside the corrected runtime,
allocation/rejection preservation, checked nested deserialization and JSON-map
operations. Source restored at
`c375931bd62819a625f4bba6df69e480f822158214fea97f9b95122038a15c7e`.
Its lock prunes unused entries from 565 to 188, adds only the consumer root and
preserves all 187 remaining package identities/versions. This is actual path
consumer evidence, not an original-versus-fork identity inversion or a public
registry installation. Existing configuration mutation gaps remain uncorrected.

The combined corrected source/Rustls graph passed all seven TLS trust tests
in debug and optimized builds. Strict Clippy passed for both configuration
owners with all targets and selected `json_get,json5`, and for the TLS library
and tests including dependencies (no `--no-deps`). Formatting of both owners
and the consumer passed. The controlled lock remains 565 packages, with only
the previously recorded two owner replacements and Rustls update; hash
`7a97fd226fa2ae68cf639da8da725706fa8879807f31737cae50f34d4517b0a8`.

Cargo archive verification first refused its reserved `Cargo.toml.orig` name.
Preserve those original manifest bytes as `UPSTREAM_CARGO.toml`; count both
old and new names in cumulative scope. Standard Cargo then packaged and
verified both owners together, using its temporary local registry for their
dependency relationship, without publication. Normalized runtime metadata
retains the selected macro package and Rust alias with no path dependency.
Eight macro and five runtime custody/source files match their archives exactly.
Archives: macro 12 files, SHA-256
`2b608c4d0fd9a3fe0d27f352efd6159099bc3d191f90c3572698325acbd3ab21`;
runtime 9 files, SHA-256
`59f6e677f33ec0e673c41d2bb1cdf06ad5089e3352273af603d31118fabdcccd`.
This proves local package construction/verification, not registry availability.

Executed commands use `nix develop /Users/joel/orca/workspaces/bombay/main-2 -c`
or the existing `.../main-2#miri -c`, from the selected controlled source.
Native owner tests: `cargo test -p bombay-validated-struct-macros --locked`
and the same with `--release`. Miri: `cargo miri test
-p bombay-validated-struct-macros --locked --test structure`; the negative used
the exact nested-helper filter. TLS commands are the earlier seven-test commands
with the corrected source/lock. Strict checks: `cargo clippy
-p bombay-validated-struct -p bombay-validated-struct-macros --all-targets
--features json_get,json5 --locked --jobs 2 -- -D warnings` and `cargo clippy
-p zenoh-link-tls --lib --tests --locked --jobs 2 -- -D warnings`.
Packaging: `cargo package -p bombay-validated-struct-macros
-p bombay-validated-struct --features json_get,json5 --allow-dirty --locked`.
Consumer commands and restored inversion logs are under
`/tmp/bombay-configuration-consumer-*.log`; semantic inversion logs under
`/tmp/bombay-generator-inversion-*.log`. All are scoped local evidence.

Configuration-stage checkpoint, before packaging: production +756/-14/net +742 (including 698
retained imported/corrected source lines); tests/fixture sources
+698/-9/net +689, of which this stage adds 399 lines including the owning
example and consumer manifest. Source classification starts inline test modules
at their `cfg(test)` attribute; separator blanks count with surrounding source.
Public surface: four inherited declarations and the existing validator export;
zero new handwritten public abstraction. Distinct cumulative paths: 139/140,
including five retired intermediate source names; 2,442 downloaded snapshot
paths remain separate research input. These remain within authorized 750
production/400 stage-fixture limits. No commit, remote source repository,
publication, feature acceptance, distillation, required CI or merge is claimed.

Accepted connection-discovery decision (2026-10-08): the user selected
**configured TLS addresses**, after comparison with automatic discovery.
Deployment supplies connect/listen addresses for both approved layouts; disable
Zenoh multicast and gossip discovery. Address configuration must be maintained
by deployment/application code; intended KERI identifiers remain a distinct
identity input. This selects discovery policy, not an address-authoring API,
certificate provisioning, resource defaults or a broader source-edit envelope.
Selected Zenoh source
enables multicast and gossip scouting by default (`defaults.rs` scouting
modules); its runtime orchestrator opens UDP scouting sockets independently of
Cargo's UDP message-transport feature. TLS-only feature selection does not
disable this mechanism. Compare explicit configured TLS connect/listen
addresses, disabling multicast/gossip discovery, with existing automatic
discovery plus its additional traffic/resource/security witnesses. Both must
preserve the approved peer/router layouts, mTLS, configured-only roots and
separate actor permissions. The selected policy is not yet implemented or
verified in the Bombay profile. The package-feature selections recorded below
also require their implementation and verification.

Accepted published-default decision (2026-10-08): the user selected **TLS alone
by default**, after comparison with TLS plus inherited authentication,
compression and multiple-link defaults. Use top-level
`default = ["transport_tls"]` and no optional defaults in the transport owner.
The current explicit TLS probes already disable these
extras; they do not prove every published default-consumer graph. The transport
owner's inherited defaults enable `test` and `transport_multilink`, which also
enables RSA authentication. Cargo feature unification can enable these through
another direct dependency even when Bombay disables dependency defaults.
Compilation enables capability; it does not prove runtime use: compression is
configured off by default and authentication requires configured credentials.
This selects defaults only; availability of explicit optional features,
the next cumulative edit envelope and release actions remain separate gates.
Sources: selected `zenoh/Cargo.toml`, `io/zenoh-transport/Cargo.toml`,
[Zenoh's feature documentation](https://docs.rs/zenoh/1.10.1/zenoh/#features),
and [Cargo feature unification](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification).

Fresh package inspection confirms that ordinary public Session composition for
peer, router and client needs none of the optional runtime extras. The inherited
nonoptional plugin-trait dependency nevertheless enables keyexpr's internal and
unstable features; a stable top-level API profile must not claim that every
dependency disables those flags. Upstream path-only `zenoh-test` is absent from
all seven normalized published manifests, so it is not a tenth release owner.
It enables broader APIs for source tests; extracted normal consumers must be
verified separately. Five renamed owners require explicit original library
target names; the top-level and plugin-trait owners already declare theirs.
The nine-package minimum remains a source/manifest finding, not a verified
nine-package release. Its later bounded edit authorization is recorded below.

Accepted optional-runtime decision (2026-10-08): the user selected **TLS runtime
profile only**, after comparison with retaining optional extras and adding their
applicable verification to this delivery. Reject explicit requests for compression,
multiple links, separate Zenoh password/RSA authentication, plugins, statistics,
additional task-tracing instrumentation and SHM in the first published copies.
Ordinary logging and core publish/subscribe and query/reply remain available.
SHM remains its approved parallel research workstream; enabling it later requires
its witnesses and a separately verified release. Reuse the selected approach of
removing Cargo options/dependency references and retaining inactive upstream
source with narrowly declared inherited cfg values. This selects supported
runtime capabilities, not an exemption from package graph, source-test,
subprocess, security or resource verification. Existing internal/test APIs needed
for owning verification remain a separate seam and are not selected application
APIs; their accepted maintainer seam and bounded package authorization are
recorded below.

Accepted maintainer-feature decision (2026-10-08): the user selected **retain
marked maintainer flags**, after comparison with stripping them from release
manifests and testing the same source under a separate development profile.
Retain owning `internal`, `unstable` and `test` feature paths needed by existing
source tests; document that they are not supported ordinary Bombay application
interfaces. Cargo cannot make these features private: consumers can explicitly
enable them. Do not claim a private test-only surface or complete absence of
unstable/internal dependency features. Verify the normal/default and explicit
maintainer graphs separately under the same source/manifests, and continue to
reject every excluded runtime-extra option in both. This approval selects the
test seam, not new actor APIs, internal production use, or the next edit envelope.

### TLS package stage checkpoint — packaging and bounded feature correction authorized

Authorization (2026-10-08): after the concrete checkpoint and subsequent
parallel-work/local-source status discussion, the user instructed **"just
continue, anyways"**. Proceed with this bounded 160-path/750-production-line/
600-additional-fixture-line package stage. Subsequently the user explicitly
selected **"Apply feature correction; raise cap to 770"**. This authorizes the
prepared six-file feature-availability correction (+18/-4/net +14 production
lines), with the cumulative production ceiling raised to 770. The 160-path and
600-additional-fixture-line ceilings remain unchanged. Neither approval permits
remote repository creation, publication or any new design choice.

The current approved correction is verified locally; the later review artifact
brings preparation to 140/140
cumulative paths, production +756/-14/net +742, and 399/400 stage fixture lines.
This next stage cannot fit that envelope. All runtime/default/maintainer feature
choices above are accepted; the following proposal authorizes their measured
implementation and verification, without selecting additional runtime policy.

- Blocker: the seven Zenoh owners still declare official package identities and
  unsupported feature options. They cannot deliver our corrected nine-package
  closure through the approved ordinary installation route. Before edits, record
  failing assertions for their actual names, defaults and unsupported Cargo
  options; package-name absence alone is not a transport or trust witness.
- Change exactly these eight manifest owners: root `Cargo.toml`,
  `zenoh/Cargo.toml`, `commons/zenoh-config/Cargo.toml`,
  `io/zenoh-link-commons/Cargo.toml`,
  `io/zenoh-links/zenoh-link-tls/Cargo.toml`, `io/zenoh-link/Cargo.toml`,
  `plugins/zenoh-plugin-trait/Cargo.toml`, `io/zenoh-transport/Cargo.toml`.
  Reuse selected names and 1.10.1 versions, preserve dependency import keys and
  all original Rust library names, and update the seven owning dependency edges.
  The two corrected validation owners retain selected names and 2.2.0 versions.
- Apply approved TLS defaults, excluded feature/dependency removal and narrowly
  declared dormant cfg values. Keep the approved maintainer flags, with separate
  normal/default and maintainer graph checks. An in-memory eight-manifest
  candidate parses as TOML and measures approximately +82/-187/net -105 lines
  before documentation/obsolete distribution-metadata edits; it has not been
  applied, compiled or packaged at proposal time. The later approved feature
  correction adds the separately measured Rust delta recorded below.
- Restrict workspace membership to the selected owner/test dependency closure;
  explicitly exclude unsupported binaries, plugins, examples and link owners
  while preserving their source files. The candidate retains 24 of 44 members:
  nine controlled owners, thirteen unchanged normal owners, `zenoh-test`, and
  the unchanged optional SHM owner required by codec's existing path edge.
  That workspace member does not enable SHM in the supported runtime graph or
  complete the separate SHM workstream. Prove this with resolved feature graphs.
- Narrowly reconcile `Cargo.lock` for renamed identities and pruned membership.
  Preserve compatible remaining versions/checksums and the Rustls 0.23.45 minimum.
  Any necessary additional version, dependency or semantic correction returns
  to the user before adoption; compiler warnings do not authorize architecture.
- Audit package provenance, license inclusion, selected feature documentation,
  docs.rs settings and obsolete Debian/plugin advertising. Update owner-facing
  guidance and the inherited feature documentation in `zenoh/src/lib.rs` without
  changing Rust behavior. Preserve upstream custody and identify our copies.
- Verify all nine actual archives, including normalized identities, preserved
  library targets/aliases, licenses, source equality and every affected edge.
  Exercise ordinary external consumers and a distinct explicit-maintainer macro
  fixture. The negative must restore a real incorrect dependency/library edge
  and fail the intended identity/compilation oracle; no arbitrary error counts.
- Request every excluded named and implicit dependency feature individually and
  assert Cargo's specific refusal. Prove unsupported endpoint schemes separately
  through the owning library result. Invert one dormant cfg spelling and show
  strict checking rejects the typo; recognize inherited flags without enabling
  them or suppressing the entire diagnostic category.
- Run pinned-Nix build, focused debug/optimized regressions, strict Clippy,
  formatting, package verification and restored controls on the combined source.
  Keep default consumer graphs separate from source-test feature unification.
  Local package verification is a release rehearsal, not crates.io availability.

Authorized cumulative envelope: **160 non-snapshot paths**, **770 net production
lines**, **600 additional stage verification/consumer fixture lines** including
fixture manifests; existing 399 stage lines remain counted cumulatively.
Expect no new functional Rust or public declarations; preserve inherited owner
APIs under the selected package identities, with no handwritten abstraction.
The seven manifests alone take cumulative paths to 147; updating existing Rust
feature documentation takes the minimum to 148. The envelope also covers the
existing lock, provenance, package guidance and bounded verification fixtures.
Downloaded unchanged source snapshots remain separately accounted.

Exclusions: no new actor/admission/provider API, network configuration owner,
numeric resource policy, runtime semantic repair, extra dependency/version,
remote repository creation, package publication, new CI architecture or
production networking implementation. Actual subprocess/security acceptance,
registry-only delivery, review, required remote CI and merges remain open.
Stop before further edits if this envelope or an accepted contract is exceeded.

Pre-edit continuation evidence (2026-10-08): the coordinator prepared a
source-hash-bound eight-manifest draft in memory. All TOML parses, preserved
library targets, selected names, retained 24-member closure and declared owning
feature edges pass independent assertions. Its exact manifest delta is
+82/-199/net -117, including obsolete Debian metadata removal and removal of
the empty TLS-owner feature table. Nothing was applied; this is not Cargo
resolution, compilation or archive verification of that candidate.
The temporary review artifact is
`/tmp/bombay-zenoh-tls-package-proposal.patch`, SHA-256
`a25380e658fe983a2742cf2e6287023f0c427fac2bca79b002384c75644a26c2`.
`git apply --check` against the exact corrected source passes without applying
it. This 528-line research artifact adds one cumulative path, taking the
authorized preparation envelope to 140/140; no test or production source grows.
It contains the manifest proposal, not the unselected Rust correction below.

The original feature refusal baseline is now executable:
`nix develop /Users/joel/orca/workspaces/bombay/main-2 -c cargo check
--manifest-path /tmp/bombay-zenoh-trust-correction/zenoh/Cargo.toml
--no-default-features --features auth_usrpwd --locked --jobs 2` exits 0.
The excluded password-authentication option is still accepted before the
proposed package edit. A future restored-control refusal must identify that
option, not an unrelated package, compilation or network failure.

New prerequisite discovered before package edits: strict checking of the actual
approved runtime feature selection fails. The pinned-Nix command
`cargo clippy --manifest-path
/tmp/bombay-zenoh-trust-correction/zenoh/Cargo.toml --no-default-features
--features transport_tls --lib --locked --jobs 2 -- -D warnings` exits 101 with
exactly five source diagnostics: the unused `DynamicRuntime` import in adminspace;
unused timestamp-instrumentation builder trait; unused dynamic-runtime
`downgrade`/`get_inner`; unused weak-runtime declaration; and unused weak-runtime
`upgrade`. The previous generator/TLS-link strict checks remain valid for their
own targets; they do not certify the whole Zenoh library profile.

Read-only owning-source analysis identifies a feature-availability correction
candidate across six existing Rust files, estimated +18/-4/net +14 lines.
The exact owners are `zenoh/src/net/runtime/adminspace.rs`,
`zenoh/src/net/runtime/mod.rs`, and
`zenoh/src/api/builders/{sample,publisher,query,querier}.rs`.
Retain the strong dynamic runtime used by ordinary sessions; gate only its
weak/instrumentation methods and their actual callers' imports. The existing
internal-only API publicly exports an empty instrumentation trait, and the
owning macro always emits its implementations. Gating the trait on `unstable`
alone would break that accepted maintainer interface. The faithful candidate
uses the union of `internal` and `unstable` for the trait, four implementations
and three imports, preserving the existing internal-only identity.

The user approved this exact correction and a 770-line production ceiling;
estimated cumulative production becomes 756. No lint exception is selected.
Required evidence includes
normal, test-only, internal-only, unstable-only and combined maintainer checks;
an external internal-only trait/implementation witness; and an inversion
restoring the original source that fails strict TLS-profile checking for these
same diagnostics. The executed subset and remaining prerequisite are recorded
below; the safe generator source remains byte-identical to its verified version.
The coordinator also prepared the six-file correction as a separate in-memory
diff with exact pre-edit hashes: +18/-4/net +14. At approval it remained
unapplied, unformatted and uncompiled. Its artifact is
`/tmp/bombay-zenoh-owner-feature-gates-proposal.patch`, SHA-256
`f48cb779466314804238300f4c8a193a0d3689619d674800b4071aadc30e0a23`.
The complete owning macro inspection
confirms `internal` removes its annotated facade when disabled, while
`internal_trait` emits implementations; do not infer these semantics from names.
Applied package/correction evidence (2026-10-08): the approved eight manifest
edits and six source edits are now applied locally. Formatting changes the
six-file correction to +22/-9/net +13, taking cumulative owning production to
+778/-23/net +755 before the remaining documentation/fixture work. No public
declaration or dependency was added by this correction. The narrow workspace
lock update reduces 565 entries to 367: seven controlled identities replace
their originals and 198 other unneeded entries disappear. Every retained
name/version/checksum is unchanged. New lock SHA-256:
`bd79996e5d265618fa9cfd46badbd48c5b6ac3d067c1e5a9ed8f40d28eb4cba9`.
The root Bombay lock and manifest remain unchanged.

Strict pinned-Nix library Clippy now passes `transport_tls`,
`transport_tls,test`, `transport_tls,unstable`, and
`transport_tls,internal,unstable,test`, with defaults disabled and
`--locked --jobs 2 -- -D warnings`.
The internal-only profile independently exposes two unused existing event
constructors in `api/info.rs`; those were not in the approved six-file correction
and remain unchanged pending owning-source analysis and user steering.
Restoring the exact original six source files fails with all five intended
diagnostics in both debug and optimized Clippy; restoring the approved correction
passes both. The initial inversion driver expected an unused-import diagnostic
for the timestamp trait instead of its actual dead-code diagnostic and stopped;
it restored all source bytes. Correcting that diagnostic assertion and rerunning
established both negative/control pairs. No unrelated failure receives credit.
Focused trust tests (two configured-only cases) and all five generator tests
also pass in both profiles against the renamed selected graph.
Full internal-only strict checking remains open. No result here closes a
remote-actor witness.

Actual nine-package rehearsal now passes: pinned-Nix `cargo package` selected
all nine owners together with `--features json_get,json5 --allow-dirty --locked
--jobs 2`, without skipping verification. Cargo constructed every archive and
compiled each against its temporary local registry in dependency order.
The 180-line independent auditor at
`/tmp/bombay-zenoh-configuration-consumer/verify_archives.py` (SHA-256
`ccba7063138b2b6f1f2bf4edb4d442a8147df9dc46d9c33f5e49f5cd403aaee3`)
passes all nine actual archives: 266 Rust source files match their source
inventories and bytes, original library targets/aliases and corrected registry
edges are preserved, excluded feature names/forwarding are absent, both Rustls
minimums remain 0.23.45, and licenses and exact upstream revisions are retained.
The existing root license is inherited through standard Cargo metadata;
Cargo emits its advisory about retaining both SPDX and license-file metadata,
but verification succeeds. No license text or licensing policy changed.
In-memory restoration of a real original validation dependency edge fails the
intended identity oracle; reintroducing TCP forwarding fails its specific feature
oracle. The original archive controls pass with unchanged hashes. These are
manifest/custody assertions, not protocol security or real consumer diagnostics.
The local registry rehearsal is not crates.io availability or release permission.

Consumer and feature evidence (2026-10-08): the existing external consumer now
uses the corrected top-level and link owners as well as configuration. Its normal
debug/optimized executable and strict all-target Clippy pass. Complete constructor,
rejected-payload allocation, nested validation, borrowed projection and nominal
runtime/error assertions are retained. The actual top-level `zenoh::Config` is
an existing wrapper, not a reexport of `zenoh_config::Config`: an initial fixture
assignment/trait assertion failed E0308/E0277. Source inspection corrected the
fixture to the owning `From` conversion and inherent methods; no dependency API
was changed and the mistaken fixture gets no inversion credit. Top-level config
retains explicit public-root exclusion. Link selection accepts exactly TLS and
refuses eight unsupported protocol schemes with the owning specific error.
These are library selection results without network access, not TLS handshakes.

The explicit internal-only consumer proves the timestamp trait and all four
builder implementations in debug and optimized checks. Changing the trait's
gate to `unstable` alone fails E0432 for the missing owning reexport; changing
only the four implementation gates fails four E0277 builder bounds. Every
restored control passes and all owning source bytes are restored exactly.
The separately identified internal-only constructor failures are resolved by the
approved correction and verified below; this trait evidence alone did not resolve them.

Pinned-Nix Cargo rejects 69 removed named/implicit feature requests separately:
top-level 21, config one, link-commons four, TLS-link one, link 19 and transport
23; plugin-trait has none. Each refusal names the exact unavailable feature.
The pre-edit password-feature acceptance baseline remains its positive control.
The consumer lock is derived from the selected source graph; every retained
external package preserves that graph's version and checksum. An initial
fresh consumer resolution selected fourteen newer transitive versions; those
were not adopted, and the selected graph was restored before the passing runs.
An actual consumer dependency inversion selects the original validation runtime
instead of the corrected owner. Both debug and optimized checks fail at the
intended nominal boundaries: configuration lacks that runtime's `ValidatedMap`
and associated-types traits, and error identities mismatch. Restoring the exact
consumer manifest/lock passes the complete executable in both profiles. The
restored consumer lock SHA-256 is
`de8b17340be68289823774cc00b30118a5fd76256fc9ed4e13aaacbf0054a68d`.

Fresh complete package-stage checkpoint: **160/160 distinct cumulative
non-snapshot paths**, including five retired intermediate names; 41 current
controlled-source paths and five consumer paths. The 2,442 unchanged downloaded
snapshot paths remain separately accounted. Owning Rust production, including
changed Rust documentation, is **+798/-73/net +725**; the documentation change
alone is +17/-50/net -33. A conservative comparison excluding that deletion
keeps functional production at net +758, below the approved 770 ceiling.
One separator blank now counts with production rather than the test module;
this explains the one-line classification difference from the preceding record.
Owning/current-consumer tests and fixtures are **+988/-9/net +979**; this package
stage adds **291/600** fixture lines including manifests and the archive auditor.
Public surface retains four inherited validation types, the inherited public
`split_once` function and the existing validator macro export, with zero new
handwritten public abstractions. The independent
research probes/curated Rust inputs contain another 18 paths/5,083 lines, counted
in the cumulative path union but not retained as production or feature tests.
The approved constructor correction and main-package README are included in
this checkpoint. Consumer fixtures total 396 lines including manifests and the
archive auditor; the selected consumer lock is now
`8753602a2e409ecac130eaf3815f05a1ef5311bd0d64bd7573aed77f8e244b5a`.
This narrow update enables the retained macro witness using the source graph;
every external version/checksum remains unchanged. The earlier nominal-runtime
inversion retains its exact historical lock and is not credited as a run against
this later lock.

Constructor correction authorization (2026-10-08): the user instructed **"go on"**
after the prepared two-line correction had been presented and reiterated as the
remaining decision. Proceed with the recommendation that preserves every
accessible interface and matches the existing owning feature flags, rather than
adding lint allowances. This remains within the approved 160-path/770-production/
600-additional-fixture envelope; no additional scope or design is selected.

Constructor correction: internal-only Clippy fails for `TransportEvent::empty`
and `LinkEvent::empty` in existing `zenoh/src/api/info.rs`. Both event types have
only an unstable public reexport, and their constructors already require
`internal`. The prepared recommendation adds `cfg(feature = "unstable")` to
these two constructors (+2 production lines, one path), preserving all currently
accessible combinations. Keep event delivery, callback implementations,
`Transport::empty` and `Link::empty` unchanged; the latter types are reachable
through the internal runtime even without unstable. The alternative is two
narrow dead-code allowances. The subsequent continuation authorization above
selects the prepared correction; implementation and verification follow.
Verification of the applied correction passes through pinned Nix: owning
internal-only strict library Clippy in debug and optimized builds; actual public
consumer calls of both constructors under internal+unstable, typed event payload
access and event-kind assertions; existing key-expression macro alias witness;
and consumer executable/all-target strict Clippy in both builds. Restoring only
the two original constructor gates causes exactly two `dead_code` errors for
`empty` in `api/info.rs` in both builds. Restoring approved source passes both
strict controls; final source SHA-256 is
`48e847f8c4e1e72974877dfd4e2d9117eafe38324d028cce08967d9c44edd0ca`.
The first compatibility fixture incorrectly compared independent fresh default
IDs and failed; that assertion was corrected, receives no inversion credit,
and required no production change. Constructor bodies are unchanged.
Archive/source reconciliation after this accepted source edit passes as recorded
below; package guidance still has a separate pending scope checkpoint.
The final combined source repeats all seven deterministic outgoing-root tests in
both builds and passes workspace formatting. The strengthened 185-line archive
auditor rejects the actual stale main archive for source divergence (exactly the
two constructor gates) and the stale config archive for manifest custody.
These are intended evidence-refusal controls, not runtime feature regressions.
A subsequent pinned-Nix `cargo package` run verifies all nine actual archives
without `--no-verify`; the strengthened auditor passes exact custody of 266 Rust
files, each active `Cargo.toml.orig`, selected README bytes, licenses, provenance,
registry edges and approved features, including its intended identity/feature
refusal controls. Main archive SHA-256 is now
`2d3c10187525e1167bbec56905b8b836db22ce53fad6145f05fdb138ef3701e2`;
the other eight hashes match the earlier verified artifacts. This closes source
reconciliation, not guidance correction: six inherited README contents remain
unchanged. Neither real TLS handshakes nor R20 receive credit from these tests.
Independent rerun confirms all nine archives. Corrupting actual main-archive
README bytes in memory fails exactly `zenoh: profile readme bytes`; corrupting
`Cargo.toml.orig` fails exactly `zenoh: active manifest custody`. The baseline
and both restored actual-archive controls pass with unchanged archive hashes;
no source, manifest or archive writes occur in those refusal tests.
The existing owning `api::info::tests::test_new_from_fields_equals_new_from_peer`
also executes and passes in debug and optimized builds under requested
TLS+internal flags. Its dev-dependency graph includes maintainer feature
unification; this is an owning test, not an isolated internal-only feature proof.
The isolated internal-only strict-library checks above supply that distinct proof.

Parallel local lifecycle investigation (2026-10-08): a 331-line disposable
Rust-stdin witness uses the public `Application`, existing actor facade for the
child and a nominal root with selected owning typed birth, observation and
shutdown products. Debug and optimized compilation/execution pass through the
root pinned Nix shell; coordinator independently reruns both binaries and
confirms exit 0. It proves exact child A stops with matching observation,
stale endpoint refusal returns its original allocation, a second stop returns
`AlreadyStopped` with its original request, and sibling B processes a later
complete receipt. Native cleanup joins both original descendant products:
A `Completed`, B `OwnerCancelled`, with original behavior/message allocations,
complete record traces and acquired native origins retained. Original creation
correlation IDs and native retirement routes remain separate; no nonce values
are predicted. Stops use the same exact actor with distinct shutdown IDs;
this is a second exact-target terminal refusal, not same-fact lifecycle or
network replay proof.

Source SHA-256 is
`e08b1d41bdc5882d1f70ee3df25c6d479daf20fc91ff4bc3e2a4a3ebbbca33d5`.
It remains in the research agent's tool store, not in retained source; binaries
are ignored `target/debug/two_child_stop` and `target/release/two_child_stop` in
`/tmp/bombay-static-admission-research`. Production, manifest/lock changes and
retained source paths are zero. Exact executions are:

```sh
nix develop /Users/joel/orca/workspaces/bombay/main-2 -c /tmp/bombay-static-admission-research/target/debug/two_child_stop
nix develop /Users/joel/orca/workspaces/bombay/main-2 -c /tmp/bombay-static-admission-research/target/release/two_child_stop
```

The research agent retains exact Rust-stdin compile commands in
`two_child_stop_debug_command` and `two_child_stop_release_command`; the
coordinator cannot access that agent-local store. Compilation supplies the
already-selected Bombay, Behavior, Behavior Actors, Behavior Macros and Tokio
artifacts. Debug uses Rust 2024; optimized adds `-C opt-level=3
-C debug-assertions=no`. Selected macro version 0.14.0 has registry checksum
`1fd089402f68bf543645785b61bc2b0742159abcc22d265f62dfe09fde161d7a`,
VCS `d69f992b371c12ab34e73b18e45b8112c90a1508`, owning package path
`crates/behavior-macros`. Debug/release macro artifact SHA-256 values are
`8d8fd711ac13cd34ce06ab80111ad3821cc52c8b987bd14effa78b48f9281df0`
and `ef4db30a121db078f6505d6537b28ac6dd0389116f7a579c2c681662c4defd27`.

Disposable authored model: five private structs, six enums, seven aliases,
nine trait implementations, 16 matches/39 arms, eight `let else` patterns,
zero `if` expressions or submodules, one source module and six module imports.
Root commands have three alternatives and root events five. Four options own
unconsumed children, issued creation correlations and the service recipient;
five named effect lanes carry two creation-observation roles, shutdown,
termination observation and service receipts. Child state owns its closed role,
original allocation and accepted record allocations; `StopOnShutdown` supplies
shutdown policy. Folds are pure and return Actions; observation identity
construction occurs outside folds. Imports are at module scope; no handwritten
routing traits, positional traversal, nested-lane mutation, semantic booleans,
erasure, unsafe code or runtime lookup are introduced. Full native products are
retained by borrowed inspection; optional native residual fields are not
exhaustively asserted. Model/source is research-only, not retained feature
implementation; permanent CI-source retention and the open authoring baseline
remain required before promoting this evidence. This source-bound investigation does not retain
feature implementation or silently expand the package-fixture stage.

The historical probe required direct access to the exact already-selected
Behavior Macros crate through Rust stdin extern arguments. That authoring form
is retired from retained application guidance; its narrowly scoped lifecycle
traces remain recorded above. No dependency was added to any manifest.
The needed derive is not reexported; the existing owning behavior macro
can generate a reusable named send product, but the ordinary-Rust comparison
for this nominal typed-event root remains open. No new export, dependency,
macro, handwritten interpretation trait or lifecycle API is justified here.
The original assertions that both joins were `Completed` and that native route
equaled creation ID were incorrect test-model assumptions, corrected against
owning source; neither receives feature-inversion credit. Remote authorization,
provider/admission freshness, replay policy, real transport/export/session
isolation and ordinary Bombay-only root authoring remain unproved. R01–R25's
full feature witnesses remain unexecuted; this is local prerequisite evidence.

Ordinary application-to-service comparison (2026-10-08): the same selected
owners provide a meaningful alternative to a nominal root with arbitrary
shutdown-result events. A real root uses existing `#[bombay::actor]`, named
births, named service delivery and the owner’s `return_to_creator` creation
settlement callback. It retains the complete original returned creation batch
and sends both full exact `EstablishedActor` proofs through Actions to the
public `ExternalActor` service. Both children process typed messages; final
cleanup acquires both original `OwnerCancelled` native products and complete
record-allocation traces. This schedule does not stop one child early.

The 180-line/11,781-byte in-memory source has SHA-256
`3fe9f460db60bb6b9dd12a060827c9fc61b2fefff6296173f3a5001b0516953f`.
Strict `-D warnings` Rust-stdin compilation and execution pass in debug and
optimized builds through root pinned Nix, with no `--extern behavior_macros`.
Both stops from the earlier 331-line witness remain separate evidence; its
source and binaries are unchanged. Substituting the root recipient for the
service recipient produces the single intended E0308 in each profile: expected
`EstablishedRecipient<StopReceipts>`, found
`EstablishedRecipient<StaticExports>`. Denials emit metadata to `/dev/null`;
positive source and binaries remain unchanged. Two meaningful actors’ same
`send_receipts` method name requires ordinary existing UFCS disambiguation,
`ChildLedgerActions::send_receipts` and
`StaticExportsActions::send_receipts`; no trait or routing machinery is added.

The disposable model contains four private structs, three enums, five aliases,
eight matches/19 arms, eight `let else` patterns and one `if let`; one module,
zero submodules and five module imports. Two root options own original children
and service recipient; the complete returned-creation vector remains retained.
No semantic booleans, erasure, handwritten interpreter traits, nested-lane
mutation or effects within folds. Full native products remain owned under
borrowed inspection; optional residual fields are not exhaustively asserted.
Production, retained source paths, public API, manifests and locks remain
unchanged. The candidate creation-disposition policy is measured, not adopted.
This closes ordinary authoring for capability export to the existing service;
it does not prove private service shutdown, authorization/freshness, replay or
production export policy. The exact owning `InstalledActor::request_shutdown`
operation remains crate-private, correctly preserving the messaging/lifecycle
boundary. No public operation or direct dependency is selected from this probe.

Exact executions (compilation uses the same selected debug/release Bombay,
Behavior, Behavior Actors and Tokio artifacts as the earlier witness, without
the separate macro extern, with crate name `ordinary_static_exports`):

```sh
nix develop /Users/joel/orca/workspaces/bombay/main-2 -c /tmp/bombay-static-admission-research/target/debug/ordinary_static_exports
nix develop /Users/joel/orca/workspaces/bombay/main-2 -c /tmp/bombay-static-admission-research/target/release/ordinary_static_exports
```

Public import-closure control also passes strict debug/optimized compilation
and execution with only Bombay and Tokio externs: foundational names already
come through `bombay::behavior`, `ActorExt` through `bombay::actors`, and the
remaining `Exit`/`StopOnShutdown` imports use the existing prelude. No missing
export or direct primitive dependency is needed. Source has the same model and
180 lines/11,781 bytes, SHA-256
`2c517cfd08ffe62b3c2105755f4e8c78f99b7b9260561ca2e84192fe6f1e8e6e`.
The coordinator independently executes both ordinary and facade-only variants,
confirming exit 0. Facade-only binaries are `target/debug/facade_static_exports`
and `target/release/facade_static_exports` in the same isolated research tree;
execute through the same root pinned Nix commands. Original 180/331-line
sources and all their binaries remain unchanged. This closes the selected
public import surface, not registry packaging, crate-renaming hygiene or the
private service-stop integration. No new export, trait, facade or dependency
may be justified by the earlier nominal-root derive obstacle alone.

Independent acceptance review now requires same-issued-fact replay against the
same consuming operation, separate creation correlations/native routes,
actual descendant terminal classifications and optional residual custody, and
sibling progress before separate owner cleanup. Those clarify existing laws;
no reply semantics, representation, resource limit or acceptance credit is
selected. The full R01–R25 gates remain open.

Fresh controlled-source transport verification (2026-10-09 UTC): the existing
owning `openclose_tls_only_with_mtls_and_no_public_pki` case was executed through
root pinned Nix with `cargo test --manifest-path
/tmp/bombay-zenoh-trust-correction/Cargo.toml -p bombay-zenoh-transport
--test unicast_openclose --no-default-features --features transport_tls,test
--locked openclose_tls_only_with_mtls_and_no_public_pki -- --exact`.
Debug and optimized executions both exit 101 at the same intended assertion.
The first actual configured-CA mTLS link establishes, but step 2 at test line
209 assumes a second link is permitted and fails on the owning `MAX_LINKS`
rejection. The selected profile explicitly excludes `transport_multilink`.
Its builder correctly gates the two-link limit on that feature; the test’s
second-link success block currently guards only low-latency mode. No production
or source correction is adopted, and no complete-test/R20 acceptance credit is
claimed. Three inherited test-target warnings also remain to be classified.
Prepare a bounded test-only feature correction and its intended refusal control;
any retained additional path belongs at the next authorized scope checkpoint.

The prepared smallest test correction is one existing-feature guard before
that second-link block: `cfg(feature = "transport_multilink")`. Its in-memory
source SHA-256 is
`87c1dd2b8a5a1f126d874c683352ab5eb99686502d1041723148501ee99904b6`,
from unchanged owning source
`5aa660bdd0c0d625b0aac752bbb50d0c041daa02e56558f14ab1f022a0b5f594`;
the prepared patch SHA-256 is
`7b2678e968834648c0aae0b26ece1a3da18da9235c83a1ba47583be799acb883`.
A Rust-stdin debug test compilation against exact dependencies from Cargo’s
recorded `test-integration-test-unicast_openclose` fingerprint passes, retaining
the same three warnings. The guarded existing case then passes one real test
in about 0.88 seconds, including repeated-link refusal, reconnect/capacity
recovery and listener/manager cleanup. Its feature union is maintainer
`test,transport_tls,unstable`; controlled transport artifact SHA-256 is
`ea15ce8a3c13b6584a8d66af5e4456186b6432758d196a724439b4f9f3812d8b`.
Binary `target/debug/tls_single_link_retirement` is ignored build output.
No source, manifest, lock, feature or production policy changes occur. This
source-equivalent candidate is not an adopted Cargo test or strict-lint pass.
The exact same candidate also compiles and passes the selected real test in
optimized mode (one test, about 0.84 seconds), preserving those three warnings.
Native Ring paths and every direct dependency artifact were resolved through
Cargo's recorded fingerprints, without selecting an arbitrary recent artifact.
The original optimized Cargo test fails specifically at line 209 with
`MAX_LINKS` and `assertion failed: res.is_ok()`; the first real mTLS connection
succeeds in that original control too. Both original-versus-corrected profiles
therefore distinguish the inherited second-link expectation from TLS failure.

Scope recount correction: this test file already carries the approved trust
backport (+159/-9 test lines) and is included among the current 41 controlled
source paths. Retaining this existing-feature guard adds **zero** cumulative
paths, **one** test line and **zero** production lines/types/dependencies.
The previous statement that it would add a path was incorrect. Apply the guard
within the existing 160-path/770-production/600-additional-fixture envelope:
it implements the already-approved TLS-only, single-link profile without
selecting any new transport policy or API. The six README paths remain the
separate unanswered 166-path checkpoint. Run the actual owning Cargo test in
both profiles after adoption; the ignored stdin binaries alone do not certify
the adopted source. The three warnings remain open and broad strict checks are
not credited by this one-test correction.

The adopted guard now passes the actual pinned-Nix owning Cargo test in debug
and optimized profiles (one selected test each, about 0.86/0.85 seconds).
Source SHA-256 is the prepared `87c1dd2b...` above; source and consumer locks
are unchanged. Current owning tests/fixtures become **+989/-9/net +980**,
with **292/600** additional stage lines and still **160/160** paths.
The existing transport archive contains the earlier test bytes and must be
rebuilt and re-audited before claiming all current archives match source.
Earlier nine-archive verification remains valid for its recorded source only.

Independent security-witness audit found that the inherited
`transport_unicast_tls_only_mutual_no_client_certs_failure` test in
`io/zenoh-transport/tests/unicast_transport.rs` catches any panic while starting
another Tokio runtime inside its Tokio test. Its sole `result.is_err()`
assertion could pass on the nested-runtime panic before any TLS handshake.
The adjacent wrong-client-certificate case also catches any panic without
asserting the intended cause or healthy before/after controls. Neither is
accepted as client-certificate refusal evidence; both retain default public
roots and do not prove the selected configured-only profile. No changes to
that additional file are authorized or adopted. The retained independent
certificate campaign and exact outgoing-root tests must supply truthful
evidence against the corrected dependency graph.

The remaining three warnings independently fail focused test-target strict
Clippy at exactly the unused UDP port import and two unused transport adapters.
The smallest correction gates each existing import/function by its existing
callers' transport/OS conditions. It leaves dormant upstream code and the
already-declared inactive cfg values in place, restores no excluded Cargo
feature, grants no lint allowance and changes no runtime behavior. The prepared
warning-only test delta is **+17/-1/net +16**, in the same already-counted test
file; combined with the second-link guard it is **+18/-1/net +17**. Apply this
ordinary consequence of the approved feature profile within the current
envelope: owning tests/fixtures become **+1006/-10/net +996**, additional stage
lines **308/600**, paths **160/160**, production/API/dependencies unchanged.
Verify formatting, the selected real test in both builds and focused strict
Clippy; wider inherited diagnostics remain separate findings if exposed.

Current final test source SHA-256 is
`d03a84609b38e3987ea816e180652293ce8098a38d0572ff3c1bbc50d71f3e9a`.
Pinned-Nix package formatting passes. The selected actual Cargo mTLS test passes
again in both profiles after these gates (one test each); focused strict
test-target Clippy passes in debug and optimized modes with `-D warnings`.
The three Rust warnings are resolved without allowances. Cargo still emits
the previously recorded license/license-file metadata advisory; no license
policy was changed. Actual owning test delta is now **+177/-10/net +167**
against upstream, exactly **+18/-1/net +17** beyond the initial trust backport.
Manifests and root/source/consumer locks remain byte-identical. This verifies
one owning target, not workspace all-targets Clippy or the full security suite.

All nine actual archives were rebuilt with the same previously verified
pinned-Nix package command (all nine selected owners,
`--features json_get,json5 --allow-dirty --locked --jobs 2`, verification
enabled). All nine package compilation checks pass; the retained independent
185-line archive auditor again passes exact source inventories/bytes,
manifests, registry edges, selected features, licensing/provenance and intended
dependency/feature inversions with restored controls. Current transport archive
SHA-256 is
`0bdc4669006b2a0f94cb0e2f88a4c8c8bac66bee6ac07f7bd0a6ab6d5620adf0`;
current main archive SHA-256 is
`42218fb99fd331ca6eac9d1e0f9022f6915a5d168cdd8b690d1c5e26c1e38b95`.
The seven other archives preserve their preceding SHA-256 values. The transport
test is packaged and is part of its exact 71-Rust-file inventory; a contrary
research suggestion that it was excluded was checked against the actual archive
and rejected. Current archives match adopted source, while the six README
guidance corrections, real registry availability, review, CI and merges remain
open. The 29 checksum-bearing controlled-package edges in actual archived
Cargo locks also match this rebuilt archive cohort. No feature acceptance or
delivery state is advanced by this rehearsal.

Existing prior TLS probe source/fixtures under
`/tmp/bombay-static-network-research/research-source/zenoh-candidate` remain
bound to stock Zenoh 1.10.1 and original owners. Their binaries do not certify
the corrected nine-package graph. Their mTLS/name-verification/TLS-only/manual
address settings match the selected direction, but outgoing public-root trust
is not explicitly disabled; a fresh controlled-source rehearsal must set the
already-approved false option. Certificate fixture metadata is valid from
2026-10-08 15:27:59Z through 2027-01-06 15:27:59Z, with distinct leaf keys,
localhost SANs, and separate wrong-name/foreign-CA cases. These algorithm,
lifetime, port, queue and timeout values remain test fixtures, not selected
production policies. Public-internet probes stay disabled. Supplemental Rustls
error classification is distinct from Zenoh’s public connection-failure error.
The corrected server already requires configured roots when mTLS is enabled;
the new configured-only root selection concerns outgoing trust. Current macOS
library evidence cannot replace Linux subprocess/Application/security campaigns.

Fresh current-graph library rehearsal (2026-10-09 UTC): the retained six enabled
tests above now pass in both builds against the actual corrected nine-package default
TLS graph. The only source setting addition is the already-approved
`use_public_pki: false`; that 469-line/17,139-byte variant exists only in memory,
SHA-256 `f71c19d04732ccfa2422a84ecd4ccc637147f4383d01649e3ea2b66526778a01`.
The original source, manifests, lock and certificate fixtures are unchanged.
Pinned-Nix locked/offline Cargo build JSON and exact owner fingerprints select
the library artifacts and Ring build-script native search path. The actual
Zenoh cases cover peer and router/client query payloads, wrong server name at
the same endpoint with before/after healthy controls, untrusted router CA,
and missing/foreign client certificates with restored healthy controls.
The sixth test separately classifies exact Rustls errors; it is supplemental
and does not change Zenoh's public error contract. Six tests pass in each profile
in about 5.72/5.62 seconds with one test worker; both public-internet tests
remain ignored. The ignored binaries are
`/tmp/bombay-zenoh-configuration-consumer/target/debug/tls_corrected_graph`
and its `target/release` counterpart. Cargo's ordinary default graph selects
TLS-only Zenoh/link/transport, Rustls 0.23.45, Tokio 1.53.1, serde_json 1.0.151
and rustls-pemfile 2.2.0; actual Ring native paths come from each build-script
receipt. Five tests exercise actual Zenoh; the sixth is supplemental Rustls.
No public-chain acceptance/refusal is claimed from the ignored internet cases;
exact deterministic root-anchor proof remains the separate existing witness.
No production limits,
algorithm, API, dependency, fixture values or proof profile is selected here;
this strengthens corrected-library evidence without retaining another source
path or closing full R20/actor/subprocess/production-identity obligations.

Rehearsal command/artifact receipt: exact Cargo build events were obtained with
`nix develop /Users/joel/orca/workspaces/bombay/main-2 -c cargo build
--manifest-path /tmp/bombay-zenoh-configuration-consumer/Cargo.toml --locked
--offline --jobs 2 --message-format=json`, and the same command with `--release`.
Each stdin compilation used `rustc --edition=2024 --crate-name
tls_corrected_graph --test`, the original fixture directory as
`CARGO_MANIFEST_DIR`, and Cargo-recorded dependency/native paths. Optimized
source adds `-C opt-level=3 -C debug-assertions=no`; neither compilation selects
extra cfg flags or claims strict Clippy. Exact selected extern suffixes:

| Existing crate alias | Debug | Optimized |
| --- | --- | --- |
| rustls | `dbaed114f31c8ba6` | `2200b40f66dd2310` |
| rustls_pemfile | `e1528a8a105ec4c1` | `3cbfbb8ee0776319` |
| serde_json | `7d960919eefb1d25` | `92cebad33fbc89bb` |
| tokio | `a4d1ae494f3dc898` | `757e44f6ff6c7cbd` |
| zenoh | `1b82e5df3a4ef500` | `0d10fbadf3e320cf` |
| zenoh_link_commons | `c7ad0702f6613988` | `7891da7077fdb1b3` |
| zenoh_link_tls | `1a18310f40db6081` | `89b0791b99239882` |
| zenoh_protocol | `95240c94cb7f6ca3` | `425951e8db50f24d` |
| flume | `4fb7158cb37064f7` | `90f9ababf7772e9f` |

The native build outputs are `ring-12b16c264ea981c9/out` and
`ring-6769e321d29755b1/out` under the respective consumer target build directories.
Each binary ran through root pinned Nix with `--test-threads=1 --nocapture`.
Debug binary SHA-256 is
`41405a3d5c09532a1e021d884e8f7cdf501c34dfb2b0a2b4d0f603934d6b77f5`;
optimized binary SHA-256 is
`3deb03c550e5a4f61b2df0388449a3d2b127893c14adfe8dc2cda4bd6f378d68`.
The 22 unchanged PEM fixture filename/checksum entries have canonical JSON
inventory digest
`aa55acd3d28f10ff49ff3a21ec935d65d668c243b36f4f36338c69348bf1ef31`.
Recorded root/source/consumer lock hashes remain unchanged; retained path delta
is zero. The original source and in-memory variant remain agent-held research
evidence, without silently retaining another source file.

Accepted stop-completion decision (2026-10-09 UTC): the user selected
**"Actor ended and retirement finished"**, after comparison with request
acceptance and exact termination observation with separate cleanup. Report
completion only after acquiring the exact actor's native joined-retirement
result and establishing the promised retirement of its owned tasks/resources.
Preserve cleanup failures and let unrelated actors continue. This selects the
promise, not its yet-unproved private service implementation. Request
acceptance closes new ordinary admission; previously acquired
Communication permits may still enqueue. Termination alone does not establish
joined cleanup, and acquiring any native product does not automatically prove
clean retirement or processing of earlier messages. Preserve incomplete cleanup,
coexisting failures and original residual custody; lost replies/timeouts retain
uncertainty. Native concrete errors/payloads stay local. Record ordinary existing
owner composition and its precise residual gap before proposing an adapter;
do not substitute whole-Application shutdown for one actor's joined retirement.
The separately selected admission boundary does not choose its clock model;
public outcome detail, limits and private service projection remain separate
decisions; this approval selects no API, dependency or wire representation.

Source-bound stop ownership review distinguishes an actor's owned retirement
from parent projection work. `ProjectTerminal::project` receives the original
native result after the exact actor task has joined; the parent-owned projection
can subsequently fail. Such a failure remains an independent native fact and
cannot erase an already acquired truthful actor-retired fact. Current private
child-product retirement requests every installed sibling's retirement before
collecting their joins, so that whole-product path cannot prove exact A join
while B stays live. No additional child join API or global publication path is
adopted from this source finding.

A separate ordinary-Rust public hosting candidate now passes strict compilation
and execution in both builds: two existing `Application::run_with` roots and
one B-owned typed service. The 153-line stdin source SHA-256 is
`591f0e02b0f032238b52d0a1726426c60ca3c1e67afada78d59f7346b0d44f7f`.
It receives complete Actions receipts for B before/during A, acquires A's
original native `Completed` result with every residual field checked, recovers
the original rejected stale-A box, refuses repeated A shutdown, then acquires
B's later complete processing receipt before separate B stop/join. Both whole
native products and issued origins remain owned. Both roots use the same local
`MailAddr::APPLICATION_ROOT`; opaque capabilities distinguish them here, not a
selected distributed identity scheme. Only `--extern bombay` and `--extern
tokio` are used; three structs, two enums, one alias, five module imports, one
two-arm match and four let-else statements implement the probe, without semantic
booleans, erasure, a new macro or handwritten interpreter. Pure Ledger folds
return existing named receipts Actions. Retained source/production delta is zero.

The root pinned-Nix compiler uses Rust 2024, `-D warnings` and the baseline
research dependency artifacts `bombay-ef17c34147750d40`/`tokio-70c5ed43eba9e93a`
in debug and `bombay-8c25a350b882000d`/`tokio-26dde056739bdfa0` in optimized mode.
Optimized source adds `-C opt-level=3 -C debug-assertions=no`. The ignored
`target/debug/separate_application_join` and `target/release` counterpart each
execute through the root pinned Nix shell and exit zero. Binary SHA-256 values
are `2eedf02a27761c00b949c625c89610bb07afe34f865c46413b5c2917cd74b043`
and `38ee34896d7f22689cd96c9c3e5e8910eb8619a411df0cb283df8f2ac0a5f4e9`.
Locks remain unchanged. This is a hosting candidate, not a topology choice,
child-export integration, held-activation-task or cleanup-failure proof, intended
early-completion inversion, transport/authentication or full R11 acceptance.

Accepted local-hosting decision (2026-10-09 UTC): the user selected
**support roots and child actors**, after comparison with root-only exports
using the existing public stop/join APIs and deferred individual child exports.
Both are required for this milestone, so the root-only candidate cannot close
the feature. Prove the existing child's native owner/service connection without
duplicating lifecycle or custody. This selects supported local targets, not a
new projection/API, private-method visibility change, outcome representation,
wire format or resource default. The approved peer/router layouts are unchanged.
The user also reiterated idiomatic Rust as a standing requirement; prototype
and retain ordinary ownership/borrowing, typed sums and existing compositions
before considering additional machinery.

The current child service gap is concrete: `InstalledActor` contains only its
recipient/control authority and protocol marker, while `CreationBinding` owns
the exact projected task and joined-result slots. A forwarding `joined` method
on the installed capability cannot expose a result it does not own.
`ProjectTerminal::project(origin, terminal) -> Self` is static and context-free;
ordinary service sends await their target. A captured service, global callback,
detached publication task or runtime handle in Behavior state is therefore not
a lawful existing composition. Preserve the child's real join owner.

Accepted child-retirement custody decision (2026-10-09 UTC): the user selected
**parent keeps the result; service receives a typed receipt**, after comparison
with full native transfer to the service and explicit parent disposition.
Keep the complete original native result with its existing parent owner; derive
a separate local service receipt only after the actual actor join. Preserve
final state, rejected messages and original errors; do not clone native errors
or serialize them onto the network. At this decision, receipt schema,
attachment, public API, failure policy and implementation were unselected.
Subsequent accepted decisions below select the report representation, access
and publication timing.
Accepted receipt-observation decision (2026-10-09 UTC): the user selected
**shared independent observations**, after comparison with one affine consumer
and explicit redistribution. Reuse the existing Observe shared pair: permitted
consumers can acquire the same small derived receipt and cancel their own waits
without cancelling or consuming another observer's fact. Parent native ownership
is unchanged. Access grants, observer counts/bounds, receipt fields, attachment
and failure policy remain separate decisions. This is reuse of the owning
observation primitive, not another observation implementation.
Accepted retirement-publication decision (2026-10-09 UTC): the user said
"go on with recommended stuff" in reply to the pending choice between
publication after the child's actual join and waiting for parent conversion too.
Select **after actual actor-task join, before parent conversion**. Publish
successful retirement only when owned settlement is established; otherwise
publish the approved inability result. Control acceptance or termination
observation remains insufficient. Borrow the
native result without cloning its errors or consuming the parent's original.
Later parent conversion failure is a separate fact and cannot erase the already
published retirement fact. Joining alone does not establish clean retirement:
receipt classification, failure details, attachment, service/API shape and
bounded waiting remain explicit decisions, not approval supplied by this timing
choice. No implementation or new production stage is authorized by this record.
The former alternative was acquiring the exact projected task into its existing
child-binding joined slot while the parent remains live. Current whole-product
retirement cannot supply that alternative unchanged because it cancels all
siblings. Any retained capability is growth; neither placement deletes current
production code. Reuse owning Observe and task machinery.

Fresh resumed investigation (2026-10-09): the clean starting tree was
`73da8cf1d58f3ffb010bdfe43faa53f9d3e6c812`; lock/patches and all three
Behavior archive revisions still match the table above. The complete selected
Behavior instructions were reread (SHA-256
`2b7a9195b27f073fec18426da43e9840f8ef668f333b9ad55934f520a37ae226`).
Current `ProjectedTask::project` awaits the actual actor task before converting
its native result; `settle_local_outcome` joins the retained activation tasks
inside that actor task. Existing termination observation can precede those
joins. `ActorRetirement::Completed` can retain late activation failures, typed
child failures and descendant projections; it is not a clean-retirement flag.
Observe publishes its outcome before notifying waiters and can resume a user
waker panic after attempting every notification. Receipt derivation/publication
must preserve the untouched original native result across those producer faults.
No new receipt code or full R11 witness was executed in this investigation.

Accepted failure-visibility decision (2026-10-09 UTC): the user selected
**explicit result: retirement not established**, after comparison with leaving
observers pending until the separately selected waiting policy ends. Notify
permitted observers when the acquired task result cannot establish full
retirement; retain original causes with their native owner. Do not claim success
or clean cleanup from that inability result. This selects failure visibility,
not receipt fields, public API, classification policy or wait limits.
Earlier independent preparation compared concrete borrowed leaf and aggregate
policies against adding a universal classifier. The current owners have no
verified universal predicate for application-owned descendant/failure products.
The common report family, outcome assessments, runtime derivation ownership
and exact fields are selected below; classification rules remain separate.

Fresh owning-regression checks use the root pinned Nix shell, current Engine
0.2.2 and Bombay Macros 0.1.2. Each command below passed in debug and again
with `--release`; each exact filter ran one test:

```text
nix develop -c cargo test --locked -p bombay-rs --lib local::environment::tests::dropped_finish_waiter_settles_actor_owned_activation_task -- --exact
nix develop -c cargo test --locked -p bombay-rs --lib local::environment::tests::owner_retirement_preserves_later_activation_task_panic -- --exact
nix develop -c cargo test --locked -p bombay-rs --lib launch::tests::startup_failure_retains_original_native_payload_lifetime -- --exact
nix develop -c cargo test --locked -p bombay-rs --lib local::effects::creation::child_projection_panic::actual_projection_panic_keeps_origin_native_cause_and_later_sibling -- --exact
```

`nix develop -c cargo test --locked -p bombay-rs --lib
observe::external_tests::panic_safety::` also passed all four tests in each
profile. These existing owner controls prove held activation settlement, late
native failure retention, exact panic-payload lifetime, projection-panic custody
and notification/published-outcome preservation after a user waker panic.
They do not execute a new receipt, classifier, service connection, protected
remote stop or R11 inversion. The creation test's outside-fold publication is
a private fixture, not authorization for a production global callback.

Accepted report-family decision (2026-10-09 UTC): the user said
"yes go with recommended" in reply to the pending report-family choice.
Select **one small Bombay-defined local report for all exported actor shapes**,
after comparison with application-specific types carried through each
export/observation API. Preserve typed service composition without cloning or
storing complete native errors; the parent still owns the full original.
This does not select fields, public names/API, classification ownership or
implementation. Reuse Observe; no type erasure or second observation primitive.

Accepted report-outcome decision (2026-10-09 UTC): after the user requested
the pros and cons, the user said **"go with the recommendation please"**.
Select two separate typed assessments: retirement established/not established,
and failures found/none found after a complete assessment/assessment incomplete.
The rejected alternative was three exclusive outcomes (clean retirement,
retirement with failures, retirement unproven), which hides known failure
presence in the unproven case. Preserve both facts without cloning native
errors; all original causes remain with the parent. A successful task join
alone cannot justify either full retirement or a completed failure assessment.
The additional combinations require construction and inversion tests against
actual owning facts. Exact field/type names, public API, classifier ownership,
classification scope and implementation remain separate decisions.

Accepted report-derivation ownership decision (2026-10-09 UTC): the user
selected **Bombay's existing runtime owners**, after comparison with a
borrowed application-supplied classifier for each exported actor type.
Derive the common report from actual owning task/resource evidence, conserving
each child's small report before application conversion and carrying that
evidence through existing typed child ownership. Keep the parent's complete
original result and causes; require no application classifier or new generic
classification trait. The current implementation does not conserve this
evidence yet. Exact classification rules/scope, attachment/public API, root
join ordering, notification-fault custody and production-stage budget remain
unselected; this ownership choice alone does not authorize production edits.

Accepted failure-scope decision (2026-10-09 UTC): the user selected
**all recorded actor/runtime failures** in the exported actor's owned subtree,
after comparison with cleanup failures only. Include Behavior errors, native
panics, failed/abruptly cancelled tasks, descendant failures and cleanup
failures. A normal graceful stop or fully settled owner-retirement request is
not itself a failure. Later notification and parent-conversion faults retain
their own attribution and cannot rewrite a previously published actor report.
Native original causes stay with their owners. Complete evidence is required
before claiming no failures found; absence of a result alone is not a known
actor failure. Exact classification rules and notification-fault retention
mechanisms remain unselected. Reporting failure presence does not select a new
supervision or propagation policy; existing Behavior policies retain ownership.

Accepted notification-fault disposition decision (2026-10-09 UTC): the user
said **"go with recommended"**. Contain the report-notification panic,
preserve the committed report and same native original, continue owning cleanup
and retain the acquired cause as a separate typed parent fault. The rejected
alternative secures custody/cleanup first and then propagates the panic through
the containing task. Do not swallow the cause, rewrite the actor report or
drop original native custody. This selects disposition, not exact storage,
error/API representation, receiving abandonment policy or production budget;
no production edit is authorized by this decision alone.

Accepted child notification-transfer decision (2026-10-09 UTC): the user
selected **existing Tokio oneshot**, after comparison with Observe's affine
pair and a combined result that catches application conversion. Use one
sender/receiver pair per existing projected child, transferring the acquired
fault-or-no-fault result into its existing parent owner before consuming
conversion. Preserve the genuine actor/projector `JoinError` contracts and
the same native original passed to the parent. A disappearing sender produces
an explicit receiving error, not a no-fault result. Observe's incomplete
publisher remains pending; catching conversion changes its native task-error
contract. Add no dependency or detached task. Retention/allocation, additional
transfer-waker faults, cancellation, receiving abandonment and coexisting
public error fields must be proved. The small shared actor report continues
to use Observe. This selects the child mechanism only, not the root receiving
shape, exact types or a production-stage budget.

Fresh notification-custody source review (2026-10-09 UTC), no edits or probes:
the existing root `ApplicationOutcome` stores cleanup as
`Result<Cleanup, ApplicationCleanupError>`; the error owns only actual cleanup
publication/task errors, not a caught notification payload alongside native
success. Four ordinary-root retention callbacks return the native result;
the Entity path instead already transfers native root/family results to
independent receivers. Reusing that receiving pattern is a candidate, with
an additional coexisting contained-cause component, but changes public
receiving/cleanup shape and is not a catch-only patch. Delayed rethrow alone
cannot preserve an earlier cause if later retention/family cleanup fails.

The child's existing nested actor/projector join result also has no independent
notification-cause slot. The selected Tokio oneshot transfer to its existing
owning binding before consuming `Root::project` must preserve both the acquired
notification cause and a later genuine projector `JoinError`. The source
comparison above preceded that selection. Catching application projection
into a result product was the rejected alternative because it changes the
current native task-error contract. Do not fabricate
`JoinError`, clone an arbitrary origin to create duplicate failure rows, lose
normal descendant results when a reporting fault coexists, or silently insert
an application-reporting fault into the actor's own retirement failure lane.
Storage, receiving abandonment and public fields/variants need review first.

Observe tries every notification and resumes the **first** waiter panic; that
is the available propagated cause, not custody of every waiter payload. Keep
the owning primitive's policy. Bombay already requires unwinding panics in
`src/lib.rs` and has pinned-Nix unwind/abort-denial checks in `flake.nix`.
The matching Rust 1.99.0 standard-library
[catch_unwind documentation](https://doc.rust-lang.org/std/panic/fn.catch_unwind.html)
confirms original-payload capture and warns that dropping a payload can panic.
No new panic strategy or dependency is selected. The official Rust API
[type-safety](https://rust-lang.github.io/api-guidelines/type-safety.html) and
[naming](https://rust-lang.github.io/api-guidelines/naming.html) guidance were
checked for upcoming representation comparison; no new public API is retained.

The root review identifies eleven public execute/run entry points in
`application/execution.rs`; receiving changes require auditing those signatures,
HTTP integration and concrete callers. Root containment/receiving preparation
is estimated at net +70–150 production lines before public migrations; child
fault custody at +50–100 beyond report attachment/aggregation. These are
conditional estimates, not disjoint additions, a measured patch or an approved
stage budget. Reuse the exact locked Tokio 1.53.1 source. Its oneshot send stores
the original and commits completion before waking the receiver, and returns
the exact value on closure refusal. A registered receiver's waker may still
panic: any proposed cause-transfer boundary must account for that fault and
actual receiving order, not assume sending cannot unwind. The child oneshot
selection is recorded above; root component types are selected below.

Completed HTTP/caller audit extends that impact to four HTTP execution methods
and two HTTP identity-retention callbacks: fifteen public entry points and
six identity callbacks plus the existing Entity transfer in the whole root
composition. Preserve caller-local/non-Send work, bare original outputs,
startup refusal, cold partial inputs and actual HTTP serving errors. Adding a
notification field alone does not preserve native root custody when later
cleanup fails; independent native receiving is necessary in either layout.

Accepted root public-receiving layout decision (2026-10-09 UTC): the user
selected **independent receipts beside work outcome**. Follow the existing
Entity receiving pattern: retain the work-phase outcome beside independently
received native-root and notification outcomes; Entity applications also
retain their family receipt. Preserve the work-phase enum's six variants and
work-presence conversions, with two additional ordinary bindings and one
additional Entity component. The rejected alternative adds root/notification
receipts to individual phase fields, broadening enum/generic/conversion
migrations. Cold states must not synthesize native/report receipts or infer
actor absence from a closed publication. Retain family outcomes, caller-local
non-Send work, bare original outputs and the existing cleanup barrier.
Subsequent decisions below select component/error types, report-access methods
and public names. The measured production budget remains a separate choice;
this layout decision alone supplies no production authorization.

Accepted notification-error family decision (2026-10-09 UTC): the user selected
**one common notification error type** for roots and children. Distinguish the
original observer-notification panic payload from the actual error receiving
a notification result whose sender disappeared. Existing root/child owners
identify the affected actor. Keep these acquired causes separate from its
retirement report and original task errors; do not clone an arbitrary panic
payload or manufacture a native task error. The rejected alternative duplicates
the variants and conversions in separate root and child error families.
This adds one expected public type; its exact name and owning fields are
selected below. Receiving-abandonment policy remains unselected. Scope approval
must precede implementation.

Accepted report-access decision (2026-10-09 UTC): the user said
**"go with recommended approach"** in response to the pending question. Use ordinary
`ApplicationLifecycle::retirement()` and `InstalledActor::retirement()` methods,
each returning an independently cancellable future for the same small shared
report through Observe. Calling either requests no stop; service capability
export follows existing Application/typed Actions wiring. Message-only
`ActorRef` gains no report method. The rejected alternative introduces a
dedicated public read-only retirement capability and export path; no demonstrated
need for that additional authority split was found. Waiting limits and the
implementation-stage budget remain open; report fields are selected below. Do not re-ask
this selected projection absent new evidence.

Accepted common local report/error API decision (2026-10-09 UTC): the user
selected **runtime-issued read-only report**. Keep runtime-issued
report fields private, with read-only `retirement()` and `failures()` accessors.
The report owns exactly the two already selected assessments; its enums express
the approved distinctions. The existing native `ActorRetirement` retains full
state, payloads and errors. No public constructor, mutation API, serializer or
new report wrapper is proposed. A report snapshot can be `Copy`, `Clone`,
`Debug`, `Eq` and `PartialEq`; original panic/task errors remain affine.

```rust
pub struct ActorRetirementReport {
    retirement: RetirementAssessment,
    failures: ActorFailureAssessment,
}

pub enum RetirementAssessment {
    Established,
    NotEstablished,
}

pub enum ActorFailureAssessment {
    FailuresFound,
    NoFailuresFound,
    Incomplete,
}

pub enum RetirementNotificationError {
    Panicked { payload: Box<dyn Any + Send> },
    ReceiptClosed { error: RecvError },
}
```

The panic payload is the original standard unwind cause already owned by the
runtime, not erased application protocol content. The receiving variant keeps
Tokio's actual error. The proposed root receiving components are the existing
`Result<(RootOrigin<Owner>, ActorRetirement<Actor, Terminal, ChildFailures>), RecvError>`
and, as subsequently extended by the two-stage decision below, a separate
`ActorNotificationReceipts`, beside the existing work outcome with unit cleanup.
Each notification result distinguishes a
proved no-fault transfer from either acquired fault; receipt closure does not
infer actor absence or retirement success. Entity family receipts coexist.

The rejected public alternative exposes the report's fields for direct construction and
mutation. That is shorter for application-created fixtures, but permits callers
to manufacture a value styled as a runtime assessment. Private construction
keeps the observation/reporting owner explicit; fixtures can acquire real
reports through the owning composition. This selects the four public names,
report accessors and concrete error/receiving representation shown above.
Child fault storage is selected below. Classification rules, abandonment policy
and stage scope remain separate choices. A fresh public-name/interface review must precede
retention of the final implementation; no production edit is authorized by
this API decision alone.

Accepted child coexisting-fault representation (2026-10-09 UTC): the user
selected **extend existing child-failure records**. Extend existing
`ChildFailure::ActorTaskFailed` and `ProjectionTaskFailed` with
the notification receipt beside their native task error. Its initially selected
single-result shape is superseded by the two-stage product below. Keep their
same genuine `JoinError`, exact actor and single origin. When application
projection succeeds but notification fails, retain the original projected
child result in the existing retired lane and add
`ChildFailure::RetirementNotificationFailed { id, kind, origin, actor, error }`
to the existing failure lane. Its `error` owns the common notification error.
The existing retirement-origin policy supplies that row's actual origin;
do not clone or recreate an origin from an acquired task error. This preserves
coexistence without a new public type or a second failure list, and cannot
rewrite the child's already published report.

The rejected alternative adds a separate notification-failure list correlated by actual
creation identity and occurrence, leaving native failure variants unchanged.
That avoids new fields on those variants but broadens receiving products and
requires reconciling separate lists. Either representation must retain the
successful projected value when a notification cause coexists, and retain
both genuine task/notification causes when task projection fails. Exact cause
transfer order and receiving-abandonment policy require their own reviewed law.

Approved-method baseline compiler evidence (2026-10-09 UTC): locked Cargo
builds of `bombay-rs` 0.1.1 with default features pass in debug and optimized
profiles. Cargo's JSON artifact record selects the owning
`target/debug/libbombay.rlib` (SHA-256
`f315c58990701920ea4b226739e1a4c001bb9d0211b82cc44c80211a1a634274`)
and `target/release/libbombay.rlib` (SHA-256
`49c0db5cbc956d53c17fbfafe1d2e24a87142baba5fca54893d3783c1bf1ca11`).
Using those artifacts, an ordinary module-imported generic function calling
`ApplicationLifecycle::termination()` and one cloning `InstalledActor` compile
without diagnostics in both profiles. Changing only the root call to
`retirement()` fails solely with E0599 on that owner at stdin line 5; changing
only the child call fails solely with E0599 on `InstalledActor` at line 12.
No speculative report type or macro is needed to isolate either API gap.

All commands run inside `nix develop /Users/joel/orca/workspaces/bombay/main-2 -c python3 -`,
using stdin source and standard subprocess execution. The build command is
`cargo build --manifest-path Cargo.toml --locked -p bombay-rs --lib --jobs 2 --message-format=json`
with `--release` for the optimized artifact. Compiler controls use
`rustc --edition=2024 --crate-name retirement_observation --crate-type lib --emit metadata -D warnings --error-format=json`,
Cargo's exact `--extern bombay` artifact, the matching `target/PROFILE/deps`
dependency search, stdin `-` and ignored target metadata output; optimized
controls additionally use `-C opt-level=3 -C debug-assertions=no`.
Control stdin is thirteen lines, SHA-256
`19696c30a9d995501f95997f7568efe0596402f20324f66a41fd898cf0db8d10`;
root and child candidates are respectively
`c31d3f485684f9ee27007fb5fda58de250af6c2c7c224b4fa499c39ab3458e0c`
and `d46907b67598ff1e8d6f18112bd2d0f2a576f291f7c34d83aa6dbb4b880ef4ae`.
The selected source instructions and lock/manifest hashes are unchanged;
no retained source path, production code, manifest, dependency or public type
was added. An initial wrong dependency-search path produced E0463 and was
corrected before the six matched controls; that setup failure receives no
proof credit. These results prove only the public API gap, not timing,
retirement conservation, a semantic inversion or full R11 acceptance.

Accepted notification-receiving order (2026-10-09 UTC): the user selected
**receive after producer join**. Retain the notification
receiver in its existing owner, and poll it only after acquiring the actual
projector join for children or cleanup join for roots. Producers still transfer
their acquired notification cause before consuming child conversion or later
root-family cleanup. By the receiving cut, that producer has finished or its
sender has disappeared, so the receiver cannot register a notification waker
while the sender is active. Preserve the native join outside the receiving
attempt and acquire the genuine notification result before disposing its
owner or running origin/application policy. Test cancellation between those
acquisitions and resume without reaccepting either fact. Shared report
observation remains independent and available at the approved earlier cut.

The alternative polls notification results concurrently with their producers,
providing earlier fault inspection but allowing the oneshot's registered
receiver waker to panic after send commits. That needs another cause-custody
boundary. The proposed ordering reuses existing receiving barriers, adds no
task, dependency or lock, and does not defer shared-report availability until
application work finishes. Receiving abandonment remains an explicit separate
policy. This selects polling order, not a new publication/cause source or
receiving-abandonment policy.

Verified existing termination-publication custody blocker (2026-10-09 UTC):
an actual public Application probe reproduces in debug and optimized builds.
Outside-fold work registers a custom `Wake` on the exact root's termination
future, holds that future without replacing its waker, requests ordinary stop
and waits on an independent signal from that waker. With a normal notification,
the parent obtains the complete native `Completed` result, same original actor
state allocation, every residual lane and sole complete Stop Actions. With
a panicking notification, the same `Ok(Exit::Normal)` fact remains published,
but the original state allocation is destroyed and the native result becomes
`ActorRetirement::ActorTaskFailed`, retaining the exact observer panic allocation
and genuine task identity. Both probe binaries exit zero by asserting this
counterexample; this is not a passing conservation regression or a repair.

The source cut is `LocalRetirement::retire` in `launch.rs`: it owns the acquired
Driver outcome while invoking `TerminationPublication`, without containing
Observe's propagated waiter panic. Observe commits first, then resumes the
first available waiter cause. Unwinding destroys the outcome and skips the
subsequent activation-task settlement. This actual probe contains no activation
tasks, so skipped settlement is source evidence only. Protecting solely the
new after-join report publisher is too late to preserve that earlier fact.
The owning correction is a prerequisite, not an unrelated runtime refactor.

Pinned-Nix Cargo-JSON-selected debug/release builds pass with the unchanged
lock. The stdin probe is 181 lines, SHA-256
`52e30e14eadae5613a57705d75e370aa2a97f9081c70e31204f80061a6cad8fc`.
Only Bombay/Tokio externs are used, Rust 2024 with `-D warnings`; optimized
compilation uses `-C opt-level=3 -C debug-assertions=no`. Actual execution
commands are `nix develop -c target/debug/termination_notification_custody_stdin`
and its `target/release` counterpart. Binary hashes are respectively
`09e5996f5d454f805497de8703cc6965d284157260b5bcbaaf5c000bc6648ca2`
and `ebcd107961ead6a37690133d5280b7a6ee8e9ccf2875e2a612000f0f900cca34`.
Normal and panic cases execute unchanged in both profiles. Corrected fixture
syntax/model mistakes receive no inversion credit. Retained source paths,
production, tests and public API additions remain zero; only ignored build
artifacts exist. No old publication policy is changed by this evidence.

Accepted two-stage notification-receipt decision (2026-10-09 UTC): the user
selected **two named notification receipts** after the new counterexample
reopened the earlier single-result shape. The
existing termination publication and the later joined-retirement report
publication can each produce an independent acquired fault. A single
`Result<(), RetirementNotificationError>` cannot retain both causes. Use
one `ActorNotificationReceipts` product with named `termination` and `retirement`
fields, each `Result<(), RetirementNotificationError>`, beside the same native
result. Child failure records keep this product in their existing record,
preserving the approved coexisting-storage law and one origin. Keep the two
original causes independently; the report publication cannot rewrite an
already published assessment or the earlier termination fact.

The rejected alternative expands one composite error enum into flat variants for all
combinations of the two publication phases and their missing-receipt errors.
It avoids a fifth public type but repeats stage-specific cases and conversions.
The named product owns a newly demonstrated pair of coexisting authoritative
facts, rather than shortening a nested spelling. This raises the
expected public additions from four to five and requires a revised stage
record. The first-publication mechanism is selected below; guard-unwind/
cancellation verification, native handoff, receiving abandonment and exact
source-classification rules remain open; no
production edit is authorized by this representation decision. The earlier
child coexisting-storage policy remains selected; exact child field/variant
spelling must account for both stages before implementation.

Required-law baseline regression precursor (2026-10-09 UTC): the same public
Application setup now asserts native conservation rather than accepting the
counterexample. Its normal observer control passes the full original
`Completed` result, state allocation, every residual field and complete sole
Stop Actions. Its panicking observer control independently observes
`Ok(Exit::Normal)` and the genuine joined task failure, then fails at stdin
line 107: the original actor-state allocation has zero strong owners, where
the required joined native result must retain one. Both debug and optimized
executions intentionally exit 101 for that exact custody assertion. The
following full-native assertion runs in the normal control and is unreachable
after today's panicking-control failure; it receives no separate failing-proof
credit. No unimplemented notification receipt is fabricated by the fixture.

The 156-line stdin source has SHA-256
`584c2a845c1630c0ea39f644cc4138688c836188eec611c3d8aa386c1ee79d25`.
Compilation uses the same pinned-Nix Cargo-selected Bombay/Tokio artifacts,
Rust 2024, `-D warnings`, matching profile dependency search, and stdin `-`;
optimized compilation adds `-C opt-level=3 -C debug-assertions=no`.
Executions are `nix develop -c target/debug/termination_notification_native_conservation_stdin`
and the corresponding `target/release` path. Binary SHA-256 values are
`b1362a343fa43a507aff07433cb39640e58b2a186b977d27fb0ef0654eda8f50`
and `45eee29d3c6ed5d450f4d879490f5b0311853eba1506b7a8c39960a44feb1a23`.
The original counterexample remains available independently. Runtime source,
manifest and lock are unchanged; retained source/test paths and public API
additions remain zero. This establishes the intended failing local law before
a repair; it does not satisfy full R11 or a passing feature witness.

Accepted first-publication repair mechanism (2026-10-09 UTC): the user answered
**"yeah approved"** to the additional Tokio receipt question. Create one
additional locked Tokio oneshot pair per standard actor at launch.
`LocalRetirement` owns its unique sender; the existing actor-task owner retains
its receiver. Keep the acquired native outcome outside the unwind-catching
closure, contain the existing termination publication's propagated observer
panic, transfer its exact original cause or proved no-fault result, then return
the same native outcome for actual activation-task settlement. Receive this
cause only after acquiring the actual actor task join, retaining both facts
outside any later conversion/disposal. The receiver remains unpolled while its
producer runs, so that transfer cannot invoke a registered receiver waker.

The same local retirement owner is invoked by `Terminal::Drop` during
cancellation or panic unwinding. Its returned value is discarded there; the
separate receipt would conserve the notification cause independently. This
does not reconstruct an unavailable native result or establish retirement
from cancellation. Returning a cause only in the actor task's result misses
that guard path and may lose it on a later task failure. Existing Observe's
affine pair retains move-only values but leaves a dropped publisher pending;
it needs a further missing-publication mechanism and does not directly supply
the selected actual Tokio receipt-closure error. These are evaluated reuse
alternatives, not adopted replacements.

The selected mechanism adds one allocated channel per actor, no new task,
dependency or public type beyond the selected notification product. Allocation,
async-frame size, startup refusal, abrupt task failure, cancellation, guard
unwind and same-fact replay need concrete owner tests and resource accounting.
A closed receiver returns the original cause to the producer; abandonment and
potentially panicking panic-payload destruction require an explicit separate
disposition, not silent successful notification. This approves the additional
channel and its ownership/receiving order, not a receiving-abandonment policy
or expanded production budget. No production patch or verified repair exists
yet. Do not re-ask this selected mechanism without new contrary evidence.

Independent first-publication review confirmed the causal source cut and the
required precursor's intended assertion, using the reported fixture/evidence
and current owning source without claiming another binary execution. The
review requires a retained passing native/state regression, held actual
activation-task settlement, guard cancellation/unwind custody, unchanged
termination classifications and affine publication replay denial before
broader implementation. The two real notification-panic combination remains
an unexecuted obligation before the second publisher can be retained.

Mandatory Entity compatibility consequence:
`entity/bombay.rs` acquires the actor join before consuming its native result
through `EntityDefinition::retired`; keep the exact result until that existing
explicit application transfer. Existing `EntityRetirementFailure` in
`entity/family.rs` distinguishes unavailable actor retirement, shutdown
conversion, forced callback and final user callback faults. A caught Observe
termination-notification fault is none of those causes; neither returning
`Ok(())` nor storing it as a user-callback panic is truthful. Its existing
failure type can be extended with notification-only and coexisting custody,
without adding another public type or changing the generic Entity runtime.
The exact existing-enum extension is selected below.

This Entity route currently has no second joined-report publisher. Do not
populate the selected two-stage product with invented successful retirement
notification or a closed receipt for a publication that was never attempted.
Retain the actually produced first-stage common notification result in Entity's
existing failure contract, as selected below, rather than adding a second
internal report publication merely to fill the product. Stable `EntityRef` remote export,
hydration and Entity-specific remote replay remain outside this PRD. Existing
family runtime receiving already preserves its concrete retirement failure
rows with their actual entity/activation/mode; reuse it unchanged where proven.

Compatibility impact adds the two native Entity owner paths above, and requires
review of existing `tests/{entity_runtime,entity_family,entity_errors,entity_directory}.rs`
beside already inventoried Entity Application callers. `entity/mod.rs` and
`worker_preparation.rs` need export/receiving compatibility inspection, not an
assumed production rewrite. The earlier forty-two-path stage inventory and
210-path ceiling are incomplete once these consequences are retained; revise
the complete union and line budget before requesting expanded authorization.
Required Entity regression: full original native retirement reaches its existing
callback; the original Observe cause survives in the actual family failure row;
resident permits and metrics settle; forced/shutdown/final-callback causes can
coexist. Dropping only the new notification cause must fail that exact payload
oracle in both profiles. No such test or Entity API change is implemented.

Accepted Entity first-stage representation (2026-10-09 UTC): the user selected
**extend existing Entity errors**. Retain the actual
`termination_notification: Result<(), RetirementNotificationError>`
beside each current `EntityRetirementFailure` variant's existing causes. Keep
`ActorRetirementUnavailable`, `ShutdownRequestPanicked`,
`ForcedRetirementPanicked` and `RetirementPanicked` with unchanged native/callback
meaning and original opaque payloads. Add
`TerminationNotificationFailed { error: RetirementNotificationError }` when
the native join and all existing callbacks succeed but the real termination
notification receipt contains a fault. The current failure-selection order
remains; its selected variant retains the independent notification result.
Return success only when the existing path and that acquired receipt both
succeed. Receive the first-stage cause after actual actor join and before the
consuming callback; hold it outside that callback. Preserve metrics/resident
release and original Entity identity/activation/mode. This keeps
`EntityDefinition::retired` and the generic Entity runtime unchanged, introduces
no public type, and gives family receiving the original independent cause.

The rejected alternative changes the application callback to consume the new receipt
beside the native result, making notification-fault retention/discharge an
application obligation. That permits application-specific disposition, but
requires callback migrations and may destroy the cause inside a panicking
consumer, as with its existing consuming native transfer. Merely adding a
second internal report publisher to fit the roots/children product introduces
an unproved extra publication with no current Entity consumer; it is not the
recommended repair. No Entity report-export API is selected by either option.
The selected existing-enum extension is a public variant/field change; its
revised scope checkpoint must precede production edits. Independent source
review confirmed the exhaustive existing cause precedence and coexistence
without executing a new Entity regression. Do not re-ask this representation
absent new contrary evidence.

Accepted receiving-abandonment policy (2026-10-09 UTC): the user selected
**explicit surrender; continue cleanup**:
cancelling a borrowing receive preserves the original receiver and independently
acquired native/notification destinations in their existing owner. Dropping
the entire owning receiving capability explicitly surrenders its unread
results; it cannot be reported as successful notification, successful remote
stop, or proof of nonexecution. Existing producer/cleanup tasks continue their
already owned retirement, without a new retention registry or detached task.
Keep private first-stage receivers alive through actual actor join wherever
the existing receiving owner remains live.

If complete owner abandonment makes `oneshot::send` return the original cause,
its disposal must not unwind before actual owned activation settlement. The
existing producer must retain that refused original through its cleanup barrier
before the selected explicit discharge. The exact fallback custody/disposal
mechanism remains a separate implementation prerequisite; a bare ignored send
or immediate payload drop is not accepted evidence. Guard paths retain their
actual cancellation/panic classification and cannot claim unavailable native
or asynchronous cleanup. Standard abort/double-panic/unbounded recursive panic
destruction is not represented as recovered retirement.

The rejected alternative provides an explicitly configured typed fallback consumer
after primary-owner abandonment. It can retain abandoned results for another
live application consumer, but requires an additional real receiving owner,
public selection/retention contract and concrete ordinary-Rust proof; it cannot
be an implicit global diagnostic store, leak or dynamic registry. The recommended
explicit-discharge policy adds no alternate receiving API. This is
not permission to erase causes held by a still-live parent or by an
independently cancellable shared report observer.

Selected refused-original custody mechanism under delegated recommendation
(2026-10-09 UTC):
existing root cleanup and child projection tasks can hold the private first
receiver independently of a dropped public receiving future or parent join
handle; the Entity retirement task can hold it across its native join. Those
live-owner cases cannot justify a universal no-refusal claim. Dropping the
consuming startup/retirement future or the whole `OwnedTask` can drop its sole
receiver before actor completion, making Tokio's exact `send Err(original)`
reachable.

Use ordinary private
`(LocalOutcome<B, Descendants>, Option<RetirementNotificationError>)` as
`LocalRetirement`'s existing `Retirement::Output`. The optional value owns only
an available original fault returned by refused receipt delivery; it does not
duplicate a successfully transferred receipt or store delivery history. The
existing actor task receives both, settles the original native residual, then
disposes the refused cause under the selected abandonment policy. Public/native
task output remains unchanged. This is an affine payload's custody across the
existing settlement barrier, no new wrapper, public type, task, dependency or
generic retirement method. Incremental estimate: +15–30 production lines in
`launch.rs` beyond the channel mechanism, not a measured patch. Production
edits still require the concrete expanded-scope record.

The alternative proves receiver retention in every existing owner and startup/
drop cut, leaving no refused payload during settlement. Current source does
not establish that invariant, so ordinary live-owner examples alone are
insufficient. A new retention registry, leaked cause or detached fallback task
is not justified. Actual guard `Panicked`/`Cancelled` paths synchronously drop
`Retirement::Output` and contain no acquired residual to settle; the return
product does not turn them into established asynchronous retirement. Subsequent
payload-disposal panic can remain an actual failure after settlement; exact
disposal policy and guard assertions still require their concrete law/proof.

Focused first-repair preparation estimates +150–275 production lines across
the existing launch, root/child/startup receiving, common terminal and Entity
owners, before this optional return product, report derivation/attachment and
public migrations. This is an independently reviewed conditional estimate,
not a production delta, bounded stage approval or temporary public API. The
two-stage public product cannot contain a fabricated successful second
notification before its real report publication exists. Establish private
first-owner laws first; public delivery must integrate the actual second
publisher and already selected final receiving contracts.

Independent startup-conservation review (2026-10-09 UTC), no source edits:
`local/effects/creation.rs` can return the child's original initialization
error in its Core creation receipt and record a `StartupRejected` binding
without a primary retirement error. `local/children.rs` omits an otherwise
quiet rejected-startup row from retirement order. That is lawful existing
native custody, but an empty collected retirement-error vector cannot prove
complete failure assessment. Preserve the approved common assessment from
the actual acquired task result at `startup_failure`, before
`SpawnError::from_local` converts it, and through its existing creation binding.
Allocation refusal remains distinct: it started no actor task. Established
child reports similarly precede opaque parent conversion. Completed traversal
alone cannot substitute for descendant resource proof. An origin remains
affine; coexisting projection and notification causes cannot require duplicating
it into two failure rows.

The same review identified an unverified startup cut: the creation interpreter
uses its invariant panic for unexpected `SpawnError` phases, including
`ActorTaskFailed`. Establish reachability and the exact retained source cause
with a focused regression before deciding on any repair. This finding does
not authorize expanding the existing startup contract.

Historical local-retirement implementation-stage proposal:

- Exact blocker: a service inside Application work cannot currently obtain a
  joined root report, and child/native products do not independently conserve
  descendant assessments and notification faults before opaque conversion.
  The ordinary termination publisher can already destroy acquired native
  state before actor join; its debug/optimized failing precursor above must
  become a passing regression before broader feature production edits.
  This stage implements the selected local prerequisite for R11; it does not
  substitute local evidence for remote authorization, freshness or transport
  acceptance.
- Smallest actual public regression: existing `paired_account` and
  `ReceivingAccount` fixtures in `completed_application_receiving.rs`; work
  requests exact root shutdown, observes termination separately, awaits the
  proposed report and remains open until the outside controller releases it.
  The controller must acquire the report before releasing work, then acquire
  the same original work output and native root receipt. The current API lacks
  the report method; that compile denial identifies the public seam, not the
  new timing law. After implementation, restoring permission-before-join must
  fail the report-readiness assertion in debug and optimized builds.
- Independent custody regressions: reuse actual late Entity-family cleanup
  panic/cancellation while retaining root, notification and acquired family
  receipts; extend `child_projection_panic` with an actual Observe waiter
  panic followed by the existing consuming projection panic. Assert both
  original causes, genuine projector task identity, original actor/origin and
  continued sibling joining. Sending the cause after conversion or dropping
  independent native receiving must fail those exact assertions.
  First protect the actual first-publication native/state law recorded above,
  held activation-task custody, and the terminal guard's cancellation/unwind
  notification transfer. Preserve each existing termination classification and
  prove no second publication after the affine capability is consumed. Before
  enabling the second publisher, cause both real observer stages to panic and
  retain their distinct original payloads in the two named receipt fields.
  Swallowing either cause, overwriting the first or using a single result must
  fail the corresponding independent custody oracle in both profiles.
- Expected owner edits: `application/{execution,interface,http,mod}.rs`,
  `lib.rs`, `launch.rs`, `terminal.rs`, `local/{children,endpoint,environment}.rs`
  and `local/effects/{creation,mod,observation}.rs`: thirteen existing
  production paths from the earlier full-stage proposal. First-publication
  investigation also reaches `entity/bombay.rs`: its existing native actor
  retirement consumes only the task result before invoking
  `EntityDefinition::retired`. The additional notification cause cannot be
  discarded there. Audit its actual failure/callback receiving before fixing
  the revised complete source union and budget. Include a path only when its actual owning obligation
  requires it. No new module, macro, dependency or executor task is proposed.
- Public compatibility audit: the current `ApplicationOutcome` /
  `ApplicationCleanupError` scan identifies twenty-six paths: five source
  files, thirteen integration-test files and eight executable-example files.
  Migrate affected callers after the focused owner laws pass; preserve cold
  inputs, non-Send/borrowed work, bare original outputs, HTTP errors, native
  actor errors and Entity family receipts. Audit compile fixtures and current
  capability/module/API guidance alongside those callers.
- Expected public additions: one common small report, its two closed
  assessment enums, the selected common notification error and the named
  two-stage notification receipts: five types, zero removals. The report
  names/accessors, receipt product and first-publication repair mechanism are
  selected; final child field/variant spelling remains unselected.
  Reuse the approved ordinary existing lifecycle capabilities;
  do not add another public observation wrapper
  merely to hide existing concrete composition.
- Expected net production growth: +650–1,000 lines including public caller
  migration; proposed stage ceiling +1,100. These are source-informed
  estimates, not measured edits. Proposed test/fixture ceiling: +2,000 net
  lines; every retained test needs an independent observable law and intended
  failing inversion. Recalculate the union before asking for scope approval.
- Reuse Observe shared publication/waiting, locked Tokio oneshot transfer,
  existing actor/projector and cleanup tasks, affine original receiving,
  runtime child bindings, local residuals and acquisition barriers. Delete
  the obsolete native-root-as-cleanup identity callbacks when independent
  receiving replaces that custody; do not invent a classifier trait, erased
  error or second lifecycle owner. Preserve true actor/projector `JoinError`s.
- Integrate in order: actual owner assessment/conservation; root and child
  report publication/cause custody; public service consumption; caller
  migration; combined checks and minimization. Independent regression review
  and child/root implementation may run concurrently only after the common
  contract is frozen; the coordinator owns exports, shared terminal types,
  manifests, lock and integration.

The current cumulative correction envelope remains 166 accounted paths and
758 conservative functional lines under its approved 770-line ceiling. It is
not authorization for this feature stage. The proposed stage adds up to forty-two
paths in its earlier inventory (including compatibility/compile/documentation migration), for a proposed
cumulative ceiling of 210 paths and 1,858 conservative functional lines.
Retain the separately accounted downloaded snapshots and earlier test fixtures;
the proposed +2,000 fixture ceiling is additional stage work, not a replacement
for their existing accounting. The report/access/error/receipt selections are
recorded above, including the first-publication Tokio receipt and Entity error
extension. Before production edits, settle any refused-original fallback
custody/disposal, final two-stage child fields/variants, receiving-abandonment
policy and actual classification rules, record the revised complete path union
and budget, then obtain explicit expanded-scope authorization required
by AGENTS.md. No cap or design in this proposal is adopted by preparation.

Delegated expanded-scope checkpoint (2026-10-09 UTC), before production edits:
select the recommended complete local-retirement stage under the user's
automatic recommendation authorization. Its smallest existing public failing
regression is the recorded native-state conservation precursor (debug and
optimized exit 101). Keep that law and the actual held-task/guard/replay
regressions passing before broadening to joined-report publication or caller
migration. This stage implements the selected local R11 prerequisite only;
full AUTH1/NET1 remote witnesses remain unexecuted and mandatory.

The checked conservative impact union contains 51 existing paths: 18 owning
source paths, 17 integration callers, eight example callers, five current
guidance paths and three existing compile fixtures. Source additions to the
earlier inventory are `entity/{bombay,family,mod}.rs`,
`worker_preparation.rs` and `actor_execution.rs`; the last two may need owning
test/caller changes rather than new production semantics. Additional integration
paths are `tests/{entity_runtime,entity_family,entity_errors,entity_directory}.rs`.
Every existing path was verified on the tracked tree. Up to three additional
descriptive compile-pass/fail/diagnostic paths may establish runtime-issued
report construction denial. The four decision/index documents are already
accounted. This is an impact envelope, not a claim that all listed files
require edits.

Recommended adopted cumulative path ceiling: 230, starting from the existing
166-path correction envelope; the identified 54 potential additions reach 220.
Expected local production growth: +1,000–1,800 lines; adopted stage ceiling
+2,200 net production lines, for at most 2,958 conservative cumulative
functional lines with the existing 758. Adopt +3,000 net stage test/fixture
lines, additional to the prior recorded correction tests. Five new public
types and zero removals: `ActorRetirementReport`, `RetirementAssessment`,
`ActorFailureAssessment`, `RetirementNotificationError`,
`ActorNotificationReceipts`. Existing public enums/capabilities gain the
selected fields/variants/methods; no new trait, macro, dependency, registry,
mailbox, lifecycle owner or executor task is authorized. Record actual
production/tests/public-surface changes at each logical checkpoint; raise a
recommended envelope explicitly before exceeding it rather than hiding growth
in separate branches or commits.

Reuse the existing typed child bindings, runtime cleanup/projector/actor tasks,
Observe shared facts, Tokio oneshot transfer, standard owned sums/products,
native residuals and original result lanes. Replace obsolete native-root-as-
cleanup callbacks with independent native and notification receiving. Root
receiving keeps `Result<ActorNotificationReceipts, RecvError>` for the actual
outer transfer: producer disappearance cannot invent either inner receipt.
Choose `notifications: ActorNotificationReceipts` in existing actor/projector
child failure records and a `NotificationsFailed` variant for coexisting
notification faults with a successfully retained projected native value.
This updates their original single-stage spelling without duplicating origin.
Use the already locked `thiserror` for the flat common error; retain opaque
standard panic payloads and exact actual receiving errors.

On receiving-owner abandonment, retain refused originals in existing producer
frames until their owned cleanup barrier completes, then explicitly dispose
them. An actual later disposal panic may remain its genuine task failure;
never claim cleanup or native recovery unavailable at guard/abort cuts. Root
native/notification transfer refusals similarly cannot preempt existing family
shutdown. No new fallback consumer or unbounded retention store is selected.

Before the first production edit, finish the fresh selected-dependency/source
verification and freeze the concrete shared contracts. Coordinator owns
`terminal.rs`, public exports, Application receiving, manifests/lock, questions,
scope accounting and integration. Isolate launch/child/Entity contributions
where those contracts permit; no conflicting shared-file edits. Integrate
early and run focused debug/optimized laws and intended inversions before
mechanical public migrations. Required combined checks, minimization, review,
CI and actual PR merges still gate delivery. This expanded stage is authorized
by the user's delegated selection, not by the earlier 770-line allowance.

Fresh implementation readiness (2026-10-10 UTC): the complete selected
Behavior instructions and the current owning sources/tests were reread.
`Cargo.lock` remains SHA256
`0c2f7ebde1e0f99a84525e2a73e3b3b3ecbbf6b657fb1a81ad46c864fc38ef87`.
Behavior/Actors 0.23.0 and Macros 0.14.0 select
`d69f992b371c12ab34e73b18e45b8112c90a1508`; Communication 0.1.3 selects
`272a2343187b40615ab26c2d0d2e136010a16e77`; Address 0.3.0 selects
`9f058dc03d1239e1ef5c3147a893b134a1e74a83`; the Timers 0.1.0 patch and
checkout both select `13e884da7ab41781f52337b0038060e375b00ee0`.
Tokio remains 1.53.1. The selected Behavior instruction checksum remains
`2b7a9195b27f073fec18426da43e9840f8ef668f333b9ad55934f520a37ae226`.
The independent review inspected Behavior's issued child shutdown authority,
Communication close/drain/rejected originals, Address exact lease retirement,
Timers' owned queue disposal, Observe's commit-before-wake behavior and Tokio
oneshot's refused-original custody. No owning dependency change is required.
These are fresh source checks, not passing new feature witnesses.

Freeze the startup conservation contract under delegated recommendation:
`SpawnError::AllocationRejected` remains genuinely unstarted, with its
original behavior/reason and no invented actor report. Every started variant
retains the required report and first notification result after actual join,
before flattening native custody. Existing tuple/unit started variants become
named variants; no correlated optional report/receipt fields. The Entity host
keeps the private complete startup error until activation conversion. Extend
the existing `EntityActivationError` sum with `AllocationRejected { behavior,
reason }` and named `Launch { retirement, retirement_report,
termination_notification }`. Replace its current native-only conversion.
The alternative would erase an acquired notification cause or manufacture
retirement for an allocation that started no task. No sixth public type,
Entity report publisher, export or hydration policy is selected. Source
feasibility is verified; startup, quiet rejection and Entity custody still
require actual regressions.

Keep report observation out of messaging-only `ActorRef` and Environment.
Application owns the root Observe pair; child creation/projector/binding owns
the child's pair, with the observation in `InstalledActor`. Existing bindings
preserve child assessments before opaque application projection. First repair
the reproduced ordinary-publication custody law in isolation; introduce the
remaining report types and migrated public receiving only after its focused
debug/optimized gate passes. The first shared production type is the already
selected common `RetirementNotificationError`, retaining the exact panic
allocation or actual oneshot receipt error. Its names, getter convention and
opaque Debug representation were checked against the official Rust API
Guidelines. No default, clone, serialization or replacement error is added.

Connected first-repair lowering (2026-10-10 UTC): the mandatory first-result
receiver cannot be added to launch without updating its root, child and Entity
consumers; Rust checks those owners even for the focused launch unit target.
Use truthful first-only products in this draft connected source until the
first custody gate passes. Preserve the original result in a separate field
or tuple component; never fabricate the second publication's success or error.
This is intermediate implementation of the already selected final contract,
not a released alternative public API. Keep catalogue migration and actual
joined-report publication behind the debug/optimized first-repair gate.
Every started startup variant retains its first result. Its consuming native
conversion returns `Result<(ActorRetirement, Result<(),
RetirementNotificationError>), (Behavior, AllocationRejection)>`, so unstarted
allocation cannot manufacture an actor notification. Child task failures and
successful-projection notification failures preserve one origin and the same
affine first cause; the final two-stage product replaces the draft first-only
field when the real report publisher exists.

Controlled dependency delivery decision (2026-10-10 UTC): the recommended
separate `devrandom-labs/bombay-zenoh` repository is public, preserving exact
upstream history and base `1211779c3647f5a96713dade452c546a07823580`.
Bootstrap that unchanged base, then deliver corrections through a reviewed
branch/PR with controlled required CI. Replace/remove inherited upstream
automations that publish official packages or assume unsupported profiles;
reuse Bombay's exact pinned Nix/toolchain inputs and pin CI actions. Keep
publication manual after review and passing required checks. Adopt an extra
18-path, 900-line CI/test/provenance envelope with no runtime production/API/
dependency growth; raise the complete cumulative path ceiling to 250 before
those edits (the earlier local potential union plus these paths reaches 238).
Correct the same already-counted test's inline import and stale root guidance.
The alternative retains incompatible upstream release automation or waits for
an external release, neither satisfying controlled delivery. Nine registry
names remain unregistered and latest official Zenoh remains 1.10.1 on this
fresh check. Registry publishing credentials are currently unavailable;
repository/PR/CI preparation can proceed independently. This is a concrete
deployment prerequisite, not permission to mark dependency publication or
the remote milestone complete.

Dependency CI artifact accounting (2026-10-10 UTC): the prepared delivery
surface contains exactly 18 additional paths and 649 verification/provenance/
toolchain lines. Also count the copied exact Nix lock (116 lines) and the
independently locked package consumer graph (2,974 lines) as retained generated
artifacts. Adopt a 4,000-line complete CI/test/configuration artifact envelope,
with the 900-line human-authored envelope unchanged. The 3,090 serialized lock
lines do not become runtime production code or disappear from cumulative
accounting. Runtime/API/dependency growth remains zero for this delivery stage;
the local runtime stage's separate production/test limits remain unchanged.

First retained repair evidence (2026-10-10 UTC): the launch-owning 95-line
regression in isolated commit `96da855` runs actual local launch, Driver,
ActorExecution and LocalRetirement. Its normal-observer control passes before
the panicking observer case fails at the original-state conservation assertion
(`strong_count` 0, required 1), in both debug and optimized builds (exit 101).
The committed Observe fact is still normal termination; exact native fields,
Actions decisions, payload allocation and released address remain separate
assertions. The repair and connected first-result consumers are being
integrated; no passing repair gate, full retirement report or remote witness
is claimed yet. Shared common-error library checking passes through pinned
Nix; that check alone is not the feature regression.

Controlled repository delivery is now concrete:
<https://github.com/devrandom-labs/bombay-zenoh> preserves the exact upstream
base/history on `main`. Actions were disabled during unchanged-base bootstrap,
before removing incompatible inherited workflows. Adopt required
`Controlled TLS dependencies` CI, current-head/base verification, enforcement
for administrators and resolved review conversations. Set required GitHub
approval count to zero, matching the available Bombay delivery model; retain
an independent source/CI review before merge. The authenticated PR author
cannot approve their own PR, so inventing an approver or using an administrator
bypass would not establish review. This selects enforceable delivery gates,
not a pass, review completion, merged correction or registry publication.

Entity conversion refinement before edit: preserve the approved total startup
sum through one private inherent `EntityActivationError::from_launch`
conversion. Its two real consumers are address refusal before task creation
and acquired launch failure after join. It maps the unstarted alternative to
the exact allocation rejection and started alternatives to their independent
native/first-notification product. This replaces the inline conversion match
and keeps the existing private allocation alternative meaningfully owned;
constructing and immediately destructuring a wrapper just to suppress a lint
would add no law. The method uses exactly the existing Behavior bound, with
no new trait/type/policy/default. The independent source review confirms this
domain provenance rather than inventing an interface from compiler output.

Registry authority investigation corrects the earlier local-credential
assessment: the organization exposes an existing `CARGO_REGISTRY_SECRET`
name to all repositories, already used by Bombay publication. No secret
value was read or logged. Thus absence of a local token is not sufficient to
classify publishing as blocked. Recommend a separately reviewed manual
`workflow_dispatch` publisher using that existing secret, pinned Nix, verified
current `main`, passing required CI and serial dependency-order publication.
Adopt up to two additional CI/script paths and 150 lines, raising the dependency
delivery path envelope from 18 to 20; the complete cumulative 250-path and
4,000-artifact-line ceilings remain sufficient. Verify actual token/package
authority without exposing the credential, then record registry checksums
and the clean normal-install consumer. No push/schedule publication trigger,
invented token authority or registry success is implied by secret availability.

Receiving conservation refinement before edit: an outside native slot may
already contain an independent result. Its occupancy is not proof that this
owner's actual producer was joined. Preserve the existing coexisting-slot law
for native-only, notification-only and both occupied destinations. Store the
existing affine join capability as `Option<JoinHandle<_>>` in OwnedTask and
ProjectedTask: while it is present, occupied destinations refuse acquisition
without consuming the owner; after actual join, its consumed absence permits
resuming first-result acquisition without polling a completed handle again.
Do not substitute `is_finished`, arbitrary destination content or a semantic
boolean for actual join custody. Add actual held-producer/hostile-waker tests
and resumed-acquisition controls in both profiles. This adds no public type,
task, registry or duplicated native result; it corrects a source-level
custody gap in the connected first repair.

Publishing authority refinement: crates.io's account APIs require session
authentication and do not provide a general independent API-token authority
probe; Cargo dry-run does not authenticate publication. Reject a fabricated
`/me` preflight or custom registry upload. The manual workflow verifies exact
reviewed/CI-passing source, secret presence, registry availability and package
rehearsal; ordinary Cargo/crates.io enforces actual PublishNew scope on the
first authorized upload. Stop on its real refusal without bypassing it.
Record the limit plainly: secret availability and dry-run success alone do
not prove publishing permission. This reuses the owning standard client and
adds no custom wire mechanism or hypothetical authority claim.

First connected implementation checkpoint (2026-10-10 UTC), source
`1cada80`/`eb6fa06`: all four launch `termination_notification` regressions
pass in debug and optimized builds through pinned Nix:

```text
nix develop -c cargo test --locked -p bombay-rs --lib termination_notification -- --nocapture
nix develop -c cargo test --locked --release -p bombay-rs --lib termination_notification -- --nocapture
```

Each campaign reports four passing tests, zero failures, 290 filtered tests.
The original native-state law previously failed in both profiles before the
repair. The held-task witness preserves native state and the exact first
cause across a cancelled borrowed wait. Actual terminal-guard cancellation
and unwind preserve their real task outcomes and committed classifications;
reobservation does not invoke publication twice. Whole first-receiver
abandonment keeps genuine owned work pending until release, then disposes
the original error after its settlement; a destructor panic remains the
actual later task failure. Coexisting-slot coverage and targeted disposal/
propagation inversions are pending, so joined-report broadening remains gated.
Actual Entity coexisting callback/notification and unstarted-allocation tests
are also being verified independently. These local tests prove no remote
authorization, replay protection or transport witness.

Complete local tracked delta at this checkpoint, before this record update:
18 paths, including 14 existing source paths and four already-accounted
documents; no untracked files. Conservative production (all owning source
before its first test module, including pre-module test-only declarations):
+781 / -401 / net +380. Tests: +786 / -69 / net +717. Public API: +1 type /
-0 types; existing enums and receiving signatures are being integrated into
the selected five-type final contract. Documentation: +1260 / -43 / net
+1217. This is net-positive capability/custody code, not code reduction.
The extra `local/effects/reports.rs` owning test path is now counted in the
impact union. Parallel unintegrated edits, controlled dependency source and
CI artifacts remain separately counted under the cumulative stage envelope;
passing focused tests alone do not establish distillation or delivery.

Updated first-repair checkpoint (2026-10-10 UTC), combined source `5daa052`:
seven launch controls pass in both debug and optimized builds. They extend
the four earlier controls with actual coexisting native-only,
notification-only and jointly occupied destinations for both owning task
forms, and the same first observer panic followed by a consuming child
projection panic. Actual joined custody, resumed borrowed acquisition,
complete native transfer and both original causes are independent assertions.
The launch source SHA256 is
`be74e5fb71ca460cba0a0281dd05d1c697fb88c2e60552848c41e5a9a4f3e1f3`.
Targeted inversions and warning-clean checking are underway; joined-report
production remains gated until restored controls pass.

Five Entity controls pass in both profiles, including actual unstarted address
exhaustion and coexisting notification/shutdown/retired-callback failures.
An intended mutation erasing the acquired first cause after the real consuming
retired callback fails in both profiles (exit 101) at the original allocation
custody assertion: strong count 0, required 1. Complete native identity,
lanes and permit release are asserted before that failure. Mutant SHA256
`37e1523062443818c4b015f013a865e13b4b7a5a828106e6dcce3e6a2c20327a`
was not retained. Restoring source SHA256
`cd2c01125d8304aa5cc8851d13a56af50d97f1888b6d6e630124b0db76a44451`
restored five passing controls in each profile. The newly integrated real
notification-only Entity case has not yet executed; it receives no pass credit.
No remote or full joined-retirement acceptance follows from these tests.

The complete combined tracked delta against `a9c5b7d` is 18 paths and no
untracked files. Conservative source-prefix accounting includes test-only
declarations before the first owning test module:

```text
production: +834 / -404 / net +430
tests:      +1617 / -70 / net +1547
public API: +1 type / -0 types
documents:  +1296 / -43 / net +1253
```

This remains inside the selected local stage envelope. Shared public products,
root ordering, manifests and integration remain coordinator-owned; one agent
at a time uses the shared local Cargo target with incremental compilation off.
Isolated source work and the controlled dependency campaign continue in parallel.

Controlled-source delivery review (2026-10-10 UTC):
[draft PR 1](https://github.com/devrandom-labs/bombay-zenoh/pull/1), candidate
`464715ca9d1403b4bf54836704bd9e072cc4c870`, preserves the upstream base and
full history. Required controlled CI is running; neither a merge nor registry
publication is established. The independent review found that archived
`Cargo.lock` identities and all 29 corrected checksum-bearing edges need a
persistent CI check, not only a supplemental rehearsal. Add that check with
wrong-checksum and official-identity inversions. Enforce each archive's exact
clean source revision as well as its manifest, source inventory and licenses.

The selected Cargo 1.99.0 revision
`5f94df4789f005f9a352888e8355ffc645b7ed0e` regenerates actual publish candidates
in `target/package/tmp-crate`. Earlier `cargo package` archives are a different
cohort. A post-upload checksum of the earlier path cannot prove which bytes
Cargo sent. Use standard Cargo's nine-package publish selection and its owning
dependency ordering, preserving the selected `json_get,json5` feature graph.
Auditing the actual publish candidates before release and registry receipts
after release remain mandatory. Publication can commit a prefix; Cargo rejects
an already published selection rather than silently skipping it. Preserve that
prefix and exact source/checksums, stop on a real refusal, and verify a concrete
standard-client resume selection before retrying. Never claim an atomic release,
overwrite a version, or treat dry-run as authenticated permission.

Recommended pre-upload custody choice, adopted under delegated steering
before edits: use the Cargo team's existing `cargo-credential` SDK 0.4.11
for a private, unpublished release credential executable. Fresh crates.io
inspection confirms latest stable 0.4.11, not yanked, archive SHA256
`5de74202293b2bc090da20d0b475aa74821caf0e901f29ba97003353886a5b42`.
Its actual archived `src/lib.rs` is byte-identical to the inspected selected
Cargo revision's SDK source; Rust 2024/MSRV 1.95 fits pinned Rust 1.99.
Cargo computes `Operation::Publish { name, vers, cksum }` from its retained
actual tarball immediately before upload. The SDK owns typed operations and
standard JSON IPC; no handwritten credential protocol, codec or uploader.

The direct private `ReleaseCredential` implements the existing `Credential`
trait. An immutable standard map retains only audited package/version/digest
identities needed for the release decision. Permit the exact crates.io registry,
its preparatory read and those nine exact publish operations; reject every
other registry/action/operation without fallback. Load the original secret
only after the requested operation passes the policy. Return the SDK's
`CacheControl::Never` and operation-dependent credentials, explicitly configure
this sole provider, and keep the workflow secret outside Cargo's builtin token
environment names. The SDK-required boolean is inherited protocol metadata,
not a new Bombay semantic state flag. This adds no actor API or runtime policy.

The alternatives are Cargo's existing token-from-stdout provider, whose session
cache does not recheck every uploaded digest, and source/dry-run determinism
plus post-upload comparisons, which can detect changed bytes only after an
irreversible upload. The selected owning SDK permits a veto before that boundary.
It does not establish registry authorization, protect against a compromised
runner, or make a multi-package upload atomic. Actual SDK subprocess tests with
synthetic fixture credentials must prove valid operations, independent read
and publish decisions, and refusal of changed names/versions/digests/registries
before credential return. No real token may be read into logs or test evidence.

Concrete revised dependency-delivery envelope: four additional private tool
paths (`Cargo.toml`, `Cargo.lock`, `src/main.rs`, owning integration test) raise
the extra path allowance from 20 to 24; the cumulative 250-path cap remains
sufficient. Current human CI/provenance source is 833 lines. Allow at most
170 new private Rust production lines, 140 test lines, 20 manifest lines and
75 existing script/CI/document lines: expected at most 1,238 human lines,
bounded by 1,300. The exact per-owner archive closure proof additionally needs
at most 15 Python lines: derive each required transitive closure from the
approved 16 manifest edges, reject missing/duplicate/self registry identities,
and invert a removed required edge. An aggregate count of 29 alone cannot
establish this law. The concrete expected maximum becomes 1,253 human lines;
the recommended 1,300 bound is adopted before these edits. Count the tool's
generated lock separately, at most 350 lines
beside the existing 3,090 generated lines. Raise the complete delivery artifact
cap from 4,000 to 4,800 lines before edits; 1,300 + 3,440 fits it. The runtime
dependency graph remains unchanged. Only this private tool adds the owning
SDK dependency and existing locked `serde` 1.0.229 / `serde_json` 1.0.151;
record the complete
locked transitive graph and checksum relationships. New public types: zero.
Actual private SDK geometry is 65 production Rust lines, 169 integration-test
lines, 16 manifest lines and 221 generated lock lines. Adopt the recommended
180-line owning-test subcap before retaining the complete tool contribution;
the original 140-line estimate was too small for actual typed subprocess
request/reply coverage. The complete 1,300 human / 4,800 artifact / 24-path
delivery bounds are unchanged; no new runtime/public surface follows.
The separate local runtime stage's 2,200 production / 3,000 test / five-type
limits remain unchanged; the private release tool's production is additionally
counted in the cumulative functional source record, not hidden as documentation.

Exact Cargo request-argument correction before retention: the selected Cargo
credential-process owner starts the SDK executable with `--cargo-plugin`;
configured additional arguments arrive in typed `CredentialRequest.args`,
not executable argv. Require exactly the audited cohort path in that existing
SDK argument slice. The corrected actual invocation test fails three of four
controls against the preliminary executable for this intended mismatch;
publication/read are incorrectly refused before the owning args correction.
Reuse the standard protocol rather than adding a second argument convention.
This removes about eight private production lines. Restored SDK controls,
strict checks and actual Cargo callback wiring without an available token
remain required before publication; synthetic subprocess success alone does
not prove that Cargo selected the provider or the real registry authorized it.

Independent archived-reference falsifier before retaining the validation fix:
delete the live `bombay-zenoh-config` root's `bombay-validated-struct`
dependency reference while retaining every package record and checksum. The
preliminary checker accepted this mutation: inventory and transitive closure
alone do not prove actual graph edges. The retained intended regression fails
with `lock reference inversion unexpectedly accepted`. Adopt the recommended
at-most-25-line validation in the same auditor: require exactly one source-free
owning node, resolve each controlled Cargo dependency reference by its actual
name, optional version and source, and compare every direct controlled edge
to the selected manifest graph. Keep the complete inventory/checksum proofs.
This fits the existing 1,300-line envelope and adds no path/runtime/API/dependency.
The corresponding restored and missing-owner negatives must pass before release.

The SDK's four actual-invocation controls now pass in debug and optimized
builds, strict Clippy passes in both, and removing only its digest comparison
makes the existing changed-publication test fail in each profile. Restored
controls pass after exact source restoration. Actual Cargo selects the explicit
singleton provider and refuses the preparatory read with the credential absent
(exit 101), before any upload. This proves callback wiring and refusal, not
real token authority or successful publication. Use the selected Cargo owner's
`registry.credential-provider` setting: an empty global provider list restores
builtin defaults and is not an exclusion mechanism. No builtin-token fallback
or command-line token belongs in this controlled invocation.

Required Linux controlled CI passed for candidate `464715ca` in
[run 38024228511](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38024228511),
completed 2026-10-10 04:49:11 UTC. That pass applies to the earlier candidate;
the persistent archive corrections and pre-upload veto require fresh checks
on their combined committed source before review, merge or publication.

First connected repair gate closed (2026-10-10 UTC): restored launch source
SHA256 `87a7d3581bc1df099c67fa260da3eb3d53bf18d1d6e97dff01c2b20a60573761`
passes all seven controls in both profiles, formatting and strict library
Clippy. Four source-only mutations fail with intended exit 101 in both profiles:

| Deliberately restored defect | Independent failed law |
| --- | --- |
| Propagate the first observer panic through native ownership | Original native-state allocation strong count 0, required 1. |
| Dispose of a refused first cause before owned-task settlement | Disposal occurs while genuine owned work is still held. |
| Infer join from occupied destinations and weaken refusal to joint occupancy | Both actor/projector tests detect consumption despite an occupied native-only or notification-only destination. |
| Transfer the first cause after consuming child projection | The original cause is lost when that real projection panics. |

Each mutation was restored byte-for-byte before the final seven-control run;
no mutation was committed. An earlier first inverse failing only at receipt
closure receives no native-conservation credit; the strengthened native-first
oracle supplies the accepted proof. Exact commands use pinned Nix, locked
Cargo, the owning library test target and both profiles; filters are
`termination_notification_preserves_original_native_retirement`,
`termination_notification_receiver_abandonment_cannot_preempt_owned_task_settlement`,
`termination_notification_preserves_coexisting_destinations`, and
`termination_notification_survives_later_projection_panic_with_original_native_custody`.

All six Entity controls now execute and pass in each profile, with fresh
strict production-library Clippy passing. Source SHA256 is
`8b842279c2f41dcb52a50a4c065bf12948dbb0a1d9ce2aa0ad6f0315bac529b6`;
the family/lock/manifest are unchanged. Exact enumeration includes actual
allocation exhaustion, selected host, direct/wrapped native retirement,
panicked task, shutdown-conversion fault and sole termination-notification
fault. The shared target initially reused a four-test binary from another
worktree; that run receives no six-test credit. Invalidate only the owning
crate's freshness metadata when switching worktrees and require exact test
enumeration, rather than assuming Cargo rebuilt from source timestamps.
Actual fresh debug/release test binaries are respectively SHA256
`9c0809f7aadde13955c34695f83c8587e46af970a6588d880293a8b915e8eb3f`
and `63451d79fe3290798d9790212c28f3d2560761d9b53ec543a55481eb49f8371c`.
The earlier Entity after-callback error-erasure inverse remains separate proof;
no new inverse is claimed for the sole-notification case. These verified
first-stage laws permit the already scoped joined-report lowering, not remote
acceptance, minimization or delivery credit.

Freeze the next connected report contracts before production lowering.
Retain exactly the five selected public types. Private report construction
and commutative combination belong to `ActorRetirementReport`; its two
read-only assessments remain independent. Known failures dominate incomplete
checking; complete absence of failures requires complete evidence. Capture
the existing `ClassifySettlement` result inside the standard
`ActionInterpreter::commit`, before Core consumes it, and carry its closed
`ActorFailureAssessment` through the existing capability/residual owners.
At actual join, derive the report only from closed native outcomes, retained
assessments, unfinished progress and the first notification result. Invoke no
application callback or new classifier trait at that handoff. Advanced/test
interpreters without complete evidence explicitly retain `Incomplete`.

`CapabilityRetirement` and all `LocalResidual` phases retain
`operation_failures: ActorFailureAssessment` and
`descendant_report: ActorRetirementReport`. Standard child retirement receives
its aggregate in a caller-owned lane; the actual empty child product supplies
the positive identity. Preserve a child's report before opaque application
projection and quiet startup-receipt discharge. Combine acquired child proof
before a later origin conversion; parent-owned projector/notification failures
affect the parent's failure assessment without rewriting the child snapshot.
The started `SpawnError` variants retain `retirement_report` beside their
original native/first result; unstarted allocation retains no invented report.

Child creation owns the Observe pair, passes its publisher to the existing
projector and its observation to the binding/`InstalledActor`. Root Application
owns its pair and passes the observation through `ApplicationHandle` to
`ApplicationLifecycle`. Keep `ActorRef` and Environment free of report authority.
The projector and existing root cleanup task catch only actual publication
around a borrowed native result, retain both real causes in
`ActorNotificationReceipts`, and transfer the product before consuming child
projection or root-family cleanup. Root cleanup acquires the actor join and
publishes while application work remains live; its existing permission barrier
still precedes native handoff and family shutdown. Product receipt closure
remains distinct from either inner notification result. Mechanical caller and
guidance migration follows focused report laws, not compiler pressure.

Coordinator owns terminal types, exports, root Application, lifecycle/endpoint
APIs and integration. One isolated contributor owns launch/startup/projector;
another owns child/capability/residual propagation, Entity startup custody and
affected owning test fixtures, including existing worker-preparation forwarding.
No new trait, macro, runtime task, registry, mailbox or dependency is selected.
This remains within the approved local 2,200 production / 3,000 test / five-type
stage and 250 cumulative paths; measure before any concrete excess.

Connected receipt refinement under delegated recommendation: the projected
child transfers one actual `ActorNotificationReceipts` product. Its receiver
therefore preserves `Result<ActorNotificationReceipts, RecvError>` in the
outside destination and the existing `ChildFailure` record, matching roots.
One missing product cannot manufacture two inner receipt-closure causes or
claim either stage succeeded. Successful native projection stays in its native
lane, with `NotificationsFailed` retaining the separate outer/inner result.
Startup conversion returns `(native, retirement_report, first_notification)`;
it has no second publication. No additional public wrapper/type is needed.

Resource and failure assessment remain independent. The private standard
environment acquires every residual phase only after closing admission and
retiring its actual interpreter, control and Address owners. Actual activation
settlement and descendant proof must also complete. A caught cleanup panic is
retained as failure evidence; its presence alone cannot negate separately
proved resource retirement. The required standard-owner panic/held-resource
observables still gate this classification. Likewise, a `Completed` native
result with `Completion::RetirementRequested(CapabilityFailed(original))`
contains a genuine runtime failure even when later failure vectors are empty;
the joined classifier must inspect that actual primary request. Normal stop,
source exhaustion and settled owner-retirement remain separate no-failure
reasons. No classification callback runs after the joined cut.

An independent source scan rejects the speculative custom-origin panic as a
current defect: only private `StructuralOrigins::origin -> ChildOrigin::new`
is implemented, with total concrete construction. No public arbitrary origin
port is selected. Retain its current creation ordering rather than broaden
production for an unsupported substitute. The actual public consuming
`ProjectTerminal` remains a real panic boundary with report/cause conservation
requirements. A projector that fails before issuing a shared report leaves
absence, not successful retirement; preserve its actual parent join/outer
receipt closure and conservative subtree assessment. No generic Observe
publisher-drop synthesis is selected. Prove the required known actor-task
failure notification and explicitly resolve any required service visibility
gap for a pre-publication projector failure before final acceptance.

The earlier proposed new-path union was checked against the actual tracked tree:
thirteen owning source files plus the twenty-six existing public callers have
thirty-four distinct paths, because five sources occur in both sets. Add the
five current guidance paths below and three existing compile fixtures for
forty-two new accounted paths, not forty-two newly created files. The four
existing decision/index documents are already accounted separately. Every
listed file exists; this is an impact inventory, not a measured patch. The
subsequently identified Entity neighbor is not yet included in that forty-two;
the final union must include its proven affected ownership and callers.

| Additional migration category | Exact existing paths |
| --- | --- |
| Integration callers, under `crates/bombay/tests/` | `actor_interface.rs`, `application_terminal_custody.rs`, `axum.rs`, `completed_application_receiving.rs`, `entity_application.rs`, `external_customer_templates.rs`, `fifo_pool_recovery.rs`, `fifo_pool_runtime.rs`, `fixed_supervisor_recovery.rs`, `fixed_supervisor_runtime.rs`, `run_with.rs`, `template_application.rs`, `terminal_projection.rs` |
| Example callers, under `examples/` | `actor-templates/src/main.rs`, `application-topology/src/main.rs`, `axum/src/http.rs`, `axum/src/main.rs`, `counter/src/main.rs`, `entity/src/main.rs`, `supervision/src/main.rs`, `worker-pool/src/main.rs` |
| Current guidance | `docs/runtime-capability-interfaces.md`, `docs/module-boundaries.md`, `docs/public-api-audit.md`, `examples/README.md`, `docs/prds/execution-ownership.md` |
| Compile fixtures, under `crates/bombay/tests/compile/pass/` | `advanced_runtime_imports.rs`, `application_children.rs`, `shutdown_authority.rs` |

Further source review distinguishes the known startup controls from an actual
creation-interpreter reachability witness: `startup_failure_retains_original_native_payload_lifetime`
and `startup_failure_retains_cancelled_task_failure_source` acquire genuine
Tokio failures through the private startup helper, but inject their task
directly. They do not prove that the normal privately committed child can
reach that interpreter's unexpected-startup branch. No reachability claim,
repair or R11 evidence is inferred from those controls. The existing standard
pre-ACK initialization-rejection path does independently show why the common
assessment must survive before its primary error transfers into the Core receipt.

Prepared independent two-assessment oracles, all **unexecuted**:

| Actual owning evidence | Required observation / intended falsifier |
| --- | --- |
| Actor termination published while genuine actor-owned activation work remains held | Actor join and report remain pending; publication count stays zero. Publishing an incomplete-assessment report before join fails this timing oracle. |
| Leaf's actual standard owners are retired, all retained tasks joined and every selected failure lane completely assessed without finding a failure | Establishment and completed no-failure assessment require those independent facts. An empty local error vector alone cannot establish either assessment. |
| Full resource settlement independently proved, with a native error in the selected failure scope | Preserve establishment and failure presence together, plus the parent's exact original error. Classifying every native failure as unestablished retirement fails this case. |
| Acquired actor-task failure or absent residual, with a known failure in the selected scope | Do not establish retirement; preserve known failure presence and the original cause. A normal settled owner-retirement request is a separate control. |
| Parent owners acquired, but retirement evidence for an actually created descendant remains missing/unproved | Do not establish full subtree retirement or claim completed no-failure assessment. A genuine no-child product is the positive empty-ownership control. |
| Missing descendant evidence coexists with a known in-scope failure | Preserve both non-establishment and known failure presence. Substituting incomplete assessment for an already known failure fails conservation. |
| Shared receipt publication wakes an application waiter that panics | The published fact remains observable and the same native original must still reach its parent owner. Successful receipt reading alone does not prove native custody. |

Use actual outside task/resource acquisition and original native lanes as the
oracle, not a second call to the classifier. The failure scope is selected;
exact classification rules and the implementation stage remain prerequisites.

Fresh standard-owner classification preparation (2026-10-09 UTC), no edits:

| Actual acquired evidence | Consequence of already selected independent assessments |
| --- | --- |
| Actor join or genuine activation work remains pending | No retirement report yet; inability/incomplete is not a substitute for the required actual join. |
| Standard mailbox/address/control/interpreter retirement is acquired, every retained activation task is settled, and every actually created descendant has established resource proof | Retirement establishment is eligible independently of known native failures. `Prepared`/`Uncommitted`/`Retired` phase spelling alone is insufficient. |
| A recorded Behavior, host, task, descendant or cleanup failure exists | Preserve known failure presence. Complete resource proof can coexist; missing proof does not turn the known cause into incomplete assessment. |
| Fully settled normal stop, exhaustion or ordinary owner-retirement request, with complete coverage and no recorded scoped failures | Neither requested disposition alone is a failure; complete no-failure assessment still requires all owned proof, not an empty vector. |
| Actual task panic/abrupt cancellation or residual-free panic/cancellation without full acquired owner results | Do not establish retirement; retain the actual known failure separately and preserve its original native cause. |
| A real descendant's proof is missing, with no known scoped failure | Do not establish full subtree retirement or complete no-failure assessment. A typed genuinely empty child product is a distinct positive identity. |
| Quiet startup initialization failure transferred into the Core creation receipt | Preserve its known failure assessment before native conversion, even when the later retirement binding contains no primary error or is absent from retirement order. Allocation refusal started no child task. |
| First termination-notification fault acquired before report derivation | The already known runtime cause prevents complete no-failure assessment; it does not replace the original native completion or committed termination fact. |
| Second report-notification fault occurs after report commitment | Keep that report immutable and preserve the new cause independently; a later parent subtree assessment can include its own subsequently acquired descendant/runtime causes. |

These consequences follow the approved ownership, scope, publication timing
and two-assessment law; do not re-ask their independence. Establishing resource
proof across a caught cleanup panic still needs the actual standard-owner
observable witness. A `RetirementPanicked` label neither defeats independently
proved completion nor establishes missing completion. Runtime owners must
retain descendant assessments before opaque projection, then aggregate them
through existing bindings and the acquired `CapabilityRetirement`/`LocalResidual`
while leaving original child values/errors in their owning lanes. This is
current required evidence, not permission for a second classifier trait or
traversal-history state.

Selected refusal boundary under delegated recommendation (2026-10-09 UTC):
actual recorded final runtime-operation failures, including child allocation
refusal, contribute failure presence even when the actor handles the original
typed result. Keep resource establishment independent and retain the exact
original refusal in its existing action/creation receipt. The rejected
alternative limits the summary to terminal/task/cleanup faults, leaving these
failed runtime requests outside it. Ordinary negative domain replies do not
become runtime faults, and retryable capacity pressure is not automatically a
final failed operation. Use the existing owning contract's distinction, never
generic `Err` introspection or a new settlement-classifier trait. This changes
no supervision/propagation policy and stores no event history.
Existing raw-task controls are in `launch.rs`, actual
public Behavior failure in `tests/run_with.rs`, held/late activation settlement
in `local/environment.rs`, multi-task custody in `local/execution.rs`, and
later projection-panic custody in `local/effects/creation.rs`. These controls
do not execute new report conservation. Inversions must fail for the intended
law in debug and optimized builds and restored controls must pass. Shared
reobservation is not a second stop acceptance or replay-protection witness.

Fresh owner-cut investigation (2026-10-09 UTC), independently checked against
the unchanged locked source: `LocalResidual::Prepared`, `Uncommitted` and
`Retired` describe different acquisition phases. Standard environment cleanup
can finish in any of those phases; `settle_local_outcome` subsequently joins
every retained activation task, conserving its original events and failures.
Execution failure and acquired cleanup outputs can coexist. Residual-free
cancellation/panic and an actor-task join error cannot establish full retirement.
The child handoff in `ProjectedTask::project` can borrow the actual `LocalOutcome`
after actor join and before native conversion and application projection.
However, recursive child traversal currently retains opaque projected `Root`
values or failed task handles; completing that traversal does not prove every
descendant's resource settlement. A candidate is preserving each child's
owner-derived common report before projection and retaining/composing that
evidence in existing typed bindings. This would avoid requiring applications
to inspect arbitrary descendant products, but the evidence is not conserved
by the current implementation. No new classifier trait or concrete assessment
implementation is selected; Observe and the previously approved Tokio cause
transfer retain their recorded selections.

Root integration has an additional concrete prerequisite:
`application/execution.rs` currently waits for application-work permission
before awaiting the root actor join, and sends that permission only after the
application work finishes. A networking service awaiting a root-retirement
report from inside that work cannot obtain an early report through this current
cut. Separate-Application stop/join probes do not prove this service integration.
Investigate the existing cleanup task's join/permission ordering while preserving
the original native root result and the application's family-shutdown policy;
this finding alone authorizes no ordering change, extra task or public API.
The subsequent ordering and API choices are recorded separately below and above.
The full roots-and-children stage must account for this owning source path;
the smaller child-only estimate below does not cover it. No new implementation,
timing inversion or full R11 acceptance was executed for these findings.

Accepted root-ordering decision (2026-10-09 UTC): the user selected
**acquire root join while work runs**, after comparison with keeping the
current ordering and hosting the network/report consumer outside target
Application work. Acquire the actual root join inside the existing cleanup
task before application-work permission; derive and publish a borrowed common
report while retaining the same full native original. Keep the permission
barrier before native parent handoff and Application-owned family shutdown.
The earlier join itself requests no stop. Work panic/drop must preserve actual cancellation
authority, permission-drop handling and original surviving outputs. Root actor
retirement does not imply Application-owned work/session/family retirement.
Account for earlier native retention, observation resources and async frame
size; do not claim zero resource growth merely because the existing task is
reused. Observe can commit a fact and then resume a waiter panic, so native
custody and that separate producer cause must survive publication. No recovery
of values destroyed inside a consuming application conversion is promised.
This selects ordering only; notification-fault custody, exact APIs and the
production budget remain separate choices. No production edit is authorized
by this decision alone.
The intended prior-order inversion holds application work open, joins genuine
root-owned work and expects the report before releasing the application gate;
restoring permission-before-join must fail that specific readiness assertion.
This is a planned oracle, not a current passing test. The public report methods
were subsequently selected above; their implementation remains scope-gated.

Historical preparation checkpoint against merge `a9c5b7d`: four already-accounted
tracked documentation paths; no untracked files. Production: +0 / -0 / net 0;
tests: +0 / -0 / net 0; public API: +0 types / -0 types.
Documentation: +1092 / -42 / net +1050.
Previously recorded pinned-Nix law-manifest controls pass all nine in debug
and optimized builds; they were not rerun for these documentation edits.
Current document-link and whitespace checks pass; all twenty-five unexecuted
acceptance rows remain. These checks verify the decision records, not any new
retirement or networking implementation.

Historical smaller child-only attachment proposal, **unadopted**:
reuse the existing projected-task handoff's publication authority, retain its
shared observation in the established creation binding and full installed-actor
capability, and borrow the actual native result for receipt derivation before
passing that same original result to the parent lift. This would affect
`launch.rs`, `local/endpoint.rs`, `local/children.rs`, `local/effects/creation.rs`,
`local/effects/observation.rs` and possibly `terminal.rs`, plus owning tests.
Attachment and private classification are estimated at net +80–170 production
lines before service integration; these are preparation estimates, not measured
edits or an authorized budget. Its then-unselected public types and service
projection are now selected above; the attachment itself remains unadopted.
Even private fields in public `InstalledActor`
require size/auto-trait/interface review. Keep messaging-only `ActorRef` authority
separate. Actor-task failure, residual-free cancellation/panic and a dropped
publisher cannot establish clean retirement. Observe intentionally leaves an
unpublished receipt pending; bounded waiting and failure classification still
require explicit decisions. Parent projection failure after publication remains
a separate fact. No root/Entity constructors, ownership or public API change is
approved by this proposed child attachment.
The earlier classification estimate is conditional and excludes preservation
of descendant evidence before opaque application projection. Runtime-owned
derivation is now selected; revise that estimate using the actual owning cuts
before proposing a production stage. Require no application classifier or
generic trait to inspect projected descendant/failure products. A new receipt
producer's panic must not consume the original result before parent custody is
retained. Neither the attachment estimate nor the publication decision supplies
that proof.

Existing typed parent Actions can request exact child stop without a new public
`InstalledActor` method. `ShutdownChild` selects a typed occurrence and actual
CreationId; it has no ShutdownId. The successful two-child probe instead uses
`ShutdownEstablished` with installed authority and separate shutdown correlations.
This proves local stop mechanics, not remote freshness: a verified request queued
through a parent could become stale before the child control request, and the
existing lifecycle interpreters carry no network permission/time context.
Receipt exposure, final stop authorization, cancellation/uncertainty and ordinary
stop-result authoring remain separate gaps. No parent routing choice or public
stop method is adopted from this comparison.

The intended child witness holds genuine actor-owned activation settlement
after exact termination publication, proves no completion receipt and real B
progress before release, then acquires A's full native result and proves B
progress afterward before separate parent cleanup. Native late-task failures,
unread events, cancelled service waiting and later parent projection failure
must retain their own exact custody. Publishing on control acceptance or
termination must fail the intended early-completion oracle in both builds.
Existing private owner tests supply the actual held-task and original-payload
laws; no new public test-only runtime abstraction is justified. This witness
and either new owner connection remain unimplemented and scope-gated.

Package guidance review and prepared scope checkpoint (2026-10-08): independent
review found that Cargo packages `zenoh/README.md`, not the corrected root README.
That current main-package README is now corrected within the approved scope;
the inherited configuration reference is explicitly labeled and its values are
unchanged. Six internal-owner README files still recommend official `zenoh` and
excluded `zenoh-ext`. A standard workspace README-inheritance experiment compiled
but Cargo explicitly preferred each existing local README, so it does not repair
the published guidance. The ineffective manifest inheritance was rolled back.
That rehearsal's code compilation is valid, but its documentation is incomplete.

The prepared six-file documentation correction changes only
`commons/zenoh-config/README.md`, `io/zenoh-link-commons/README.md`,
`io/zenoh-links/zenoh-link-tls/README.md`, `io/zenoh-link/README.md`,
`plugins/zenoh-plugin-trait/README.md`, and `io/zenoh-transport/README.md`.
Each identifies its controlled package/owner, retains internal-interface and
upstream-license guidance, recommends the selected `bombay-zenoh` dependency
with Rust alias `zenoh`, and links the single canonical controlled profile.
The draft applied cleanly in `git apply --check` before authorization.
Authorization (2026-10-09 UTC): after the status update explicitly named the
six README corrections and 160-to-166 path checkpoint, the user replied
**"okay go on!"**. This approves that bounded documentation correction and
raises the cumulative path ceiling to **166**; production 770 and additional
fixtures 600 remain unchanged. No code, type, dependency, default, license
policy, remote creation or publication is selected by this approval. Apply
the six ten-line owning READMEs (expected **+48/-36/net +12** documentation)
and reconcile all actual archive guidance and source/metadata custody afterward;
compilation alone does not close it. Do not re-request this approved checkpoint.

The six corrections are applied: **+48/-36/net +12** documentation, 47 current
controlled-source paths and **166/166** cumulative non-snapshot paths. Production
remains **+798/-73/net +725** (functional comparison excluding documentation
deletion: net +758/770); tests/fixtures remain **+1006/-10/net +996** with
**308/600** additional stage lines. Public surface is unchanged: four inherited
validation types, inherited public `split_once` and validator macro, no new
handwritten abstraction. The applied six-file Git diff SHA-256 is
`40f90e5c83dbcdd6c9b849ca201045374b8b46d982908a2420c197ad0cb8e52a`.

The same pinned-Nix nine-owner actual package verification passes again,
followed by the independent archive auditor and its restored identity/feature
inversion controls. All six actual packaged READMEs match owning source,
identify the maintained package/profile/provenance, gate installation guidance
on verified publication, and omit excluded `zenoh-ext` recommendations. All
29 checksum-bearing controlled dependency edges in archived Cargo locks match
the rebuilt cohort. Current archive SHA-256 values are:

| Package | SHA-256 |
| --- | --- |
| bombay-zenoh | `994189681342420af40ccfa02ad8fcaff454700d9fe4e1a11843de28eda3944a` |
| bombay-zenoh-config | `d545503a9a0af8bd8b1b8d9c27636dc46d042f27a3b06dce62960ea3cb205c02` |
| bombay-zenoh-link-commons | `f2468a5b60f0d9ce46e934bd23a6a02618549e16a28f051f92c6516730be70b7` |
| bombay-zenoh-link-tls | `420b2d5ea057cb4095e22c93be81ee839c8c0e7bb6fece6565e2d0e1ee0481a3` |
| bombay-zenoh-link | `3c1d86202b75a90c692c4582836c68161197afebbc65e331f3c8e3676ca65f7a` |
| bombay-zenoh-plugin-trait | `f2d4e87aedd6ca2abf5d7097e19e29ea201f64d164bbe5235e80dda146acf3de` |
| bombay-zenoh-transport | `57c130b14d495e7f03e0cddf51c10cfc5fa5f4c5238478620475a7c54c1f5bcb` |
| bombay-validated-struct | `d8ec6cb8eb742cf12650fb520cf10873acf8c98fd5cd48151724a35477105235` |
| bombay-validated-struct-macros | `ae632a9145d716e8a9d5601a3f7e866f645a7f65a61225ca6c06427c9097fb8d` |

This closes the local package-guidance correction, not real registry delivery,
dependency-source distillation, reviewed PRs, required CI or merges. Original
Bombay and current controlled/consumer manifests/locks retain recorded hashes.
The stronger selected remote-stop promise and other networking contracts remain
unimplemented; no full acceptance row is marked passed.

Independent correction reviews (2026-10-09): separate read-only agents reviewed
the actual controlled TLS/feature patch and configuration-generator repair.
Both found no blockers within those approved scopes; neither edited source or
reran tests. Review binds the unchanged controlled manifest/lock and owning
source hashes recorded above. The TLS review traced the real client through
the private configured-root selector, incoming mTLS's existing verifier,
missing/empty-root refusal, exact legacy controls and matching feature gates.
The generator review compared original unsafe source with safe owned field
movement, preserved parser/runtime bytes, field order/attributes/punctuation,
shared test owner, generated shorthand/imports, aliases and provenance. It
also inspected the recorded Miri and semantic-inversion failure artifacts.
The pre-existing public validation findings below the original safety audit
remain outside this repair; review does not certify the whole inherited API.

Coordinator archive revalidation matched all nine actual checksums in the table
and their published package identities. All 16 controlled edges in the
normalized top-level dependency/build/dev tables use registry version
requirements with no path or Git override. This is a narrower metadata check
than the previously recorded full archive auditor and 29 lock checksum edges;
it supplies no new compilation, registry or feature-acceptance credit.
Production Bombay configuration still must enforce the approved private roots,
mutual TLS, server-name checks and explicit permitted addresses; the dependency
retains legacy configuration defaults and endpoint overrides. Actual Application
and Linux subprocess security, resource/cancellation proofs, controlled-source
PR/CI/merge and registry delivery remain outstanding. Bounded source review
alone does not establish dependency-source distillation or PRD completion.

Cached published archives for all seven upstream Zenoh owners contain the
dual-license metadata expression but no license/notice file. License inclusion
must therefore be proved on our actual archives, rather than assumed from the
upstream package metadata. No new license or publication policy is selected.

Next configuration-owner stage proposal (2026-10-08), not yet authorized and
superseded pending the safety decision and revised verification budget above:

- Blocker and failing witness: strict Clippy of the exact original generated
  configuration reproduces 166 redundant-field errors; the added root-policy
  field produces the 167th. Replace only the owning same-identifier initializer
  emission with shorthand. No duplicate generator or lint suppression.
- Import the measured lean source copies under
  `dependencies/validated-struct` and `dependencies/validated-struct-macros` in
  the controlled source repository: four production Rust files, one existing
  example, two active manifests, two original manifests and two original VCS
  records (11 paths). Preserve source revisions/checksums recorded by research.
- Include one copy of the existing dual-license text in each package directory
  and one configuration-source provenance record. These three additional paths
  make the package evidence explicit and self-contained; verify actual archives.
  Exclude old package-local locks/cache markers; keep the controlled workspace
  lock as the selected build contract.
- Reuse the chosen package names/versions, original dependency aliases and library
  target names. Wire those two owning packages through root `Cargo.toml` and
  `Cargo.lock` without namespacing the seven Zenoh owners yet. Apply the already
  selected Rustls minimum 0.23.45 and narrow lock update; present any additional
  necessary version changes before adopting them. No discovery/default/transport
  feature policy changes belong in this stage.
- Add at most 150 regression/consumer fixture lines, reusing owning constructor,
  validation, getter, setter and JSON-map laws. Include the existing 93-line
  example in the test/example count. Prove exact accepted/rejected allocations,
  unchanged rejected state, original-versus-corrected observations in both
  builds and the intended Clippy inversion. Archive/alias tests must not infer
  real registry availability from a local build or root patch.
- Expected complete added source is production net +684, plus the one-line
  +1/-1/net 0 correction; cumulative owning production becomes net +728 including
  the trust stage. Retain four existing public declarations with distinct package
  identities: `GetError`, `InsertionError`, `ValidatedMap`,
  `ValidatedMapAssociatedTypes`, and the existing `validator` macro export.
  Add no handwritten public type, trait, wrapper or macro.
- Requested cumulative envelope: at most 130 non-snapshot paths and at most 750
  net owning production lines; stage test/example additions at most 250 lines;
  four inherited public declarations and zero new handwritten abstractions.
  The expected 18 new/touched paths include the 14 import/license/provenance
  paths, root manifest/lock and at most two alias-consumer fixture paths. Count
  all current isolated preparation and the previous six-path correction once.
  This crosses all three original repository thresholds and requires explicit
  authorization before copying or editing owning source.
- Exclusions: publishing, remote repository creation, seven-owner Zenoh namespace
  manifests, default features, network scouting, new actor/admission APIs and
  Bombay networking implementation remain separately gated. Stop before any
  further production edit if measured scope exceeds this envelope or a new
  dependency contract/semantic gap appears.

Historical first-correction proposal (2026-10-08), superseded by the approved
and locally verified package checkpoint above. Pending identities, scope and
local verification in this initial record are not current decision gates:

- Blocker: outgoing Zenoh trust includes public roots despite the user's
  configured-only policy. The actual TLS-link witness above violates this rule
  before any application request, in debug and optimized profiles.
- Existing upstream proposal paths: `DEFAULT_CONFIG.json5`,
  `commons/zenoh-config/src/lib.rs`, `io/zenoh-link-commons/src/tls.rs`,
  `io/zenoh-links/zenoh-link-tls/src/utils.rs`,
  `io/zenoh-link-commons/src/quic/utils.rs`, and
  `io/zenoh-transport/tests/unicast_openclose.rs`. Bombay remains TLS/TCP-only.
- Its measured semantic patch is production +46 / -8 / net +38; tests
  +159 / -9 / net +150; documentation +3. New public types: zero; one public
  configuration field and endpoint key still expand the external API.
- Reuse Zenoh's configuration, root-store construction and TLS ownership,
  Rustls certificate verification and existing transport tests; remove
  unconditional root inclusion when explicitly disabled. No new verifier.
- Add a precise outgoing-client regression and intended root-seeding inversion;
  broad transport failure alone is insufficient. Any new testability seam
  requires its own measured comparison and user decision before implementation.
- Selected testability design, applied above: move the TLS constructor's existing root
  selection into one private `outgoing_trust_anchors` function in the same owning
  file. The actual constructor consumes its `RootCertStore`; tests compare all
  actual anchors, including subject, key and name constraints, against independent
  configured/public-root controls. Text diff: +28 / -22 / net +6 production
  beyond the backport, zero new public types/APIs/dependencies. This replaces the
  constructor's inline block; no policy or verifier is duplicated. Rustls exposes
  no getter for the built client's root store. The alternative leaves construction
  inline, uses the actual client's empty-root regression and relies on a live
  public-CA handshake campaign for exclusion evidence. That campaign needs
  external network/certificate availability and cannot by itself supply a stable
  offline CI regression. Real transport/Application security gates remain
  mandatory with either test arrangement.
- Packaging determines the retained file/source footprint. Copying three owner
  archives would add 38 files/8,558 Rust lines and cross repository checkpoints;
  a separate immutable source must instead prove a coherent sibling dependency
  graph and the selected namespaced registry closure. The separate controlled
  source location is selected; exact identities and cumulative scope are not.
  Present the final cumulative scope before production edits.

Before remaining Bombay networking implementation, replace its unresolved gates with concrete decisions
and add a pre-edit record naming the smallest failing end-to-end witness,
expected paths and production delta, proposed added/removed public types and
reused/deleted owners. Candidate areas are a Bombay network module, application
composition only if C proves the gap, named admission/remote operations,
integration and compile fixtures, real-process examples, CI, manifests and docs.
Exact files/type counts are intentionally unselected until the ordinary-Rust
experiments identify the actual required surface; this is an implementation
blocker, not authorization to invent it during compiler repair.

The user's priority is extreme correctness, including broad scope when needed.
Keep one cumulative change ledger across these tracks; splitting PRs does not
reset it. Apply repository scope checkpoints to the concrete measured proposal
before crossing them. Broader capability work must preserve full acceptance,
even when its reviewed budget exceeds the default thresholds. This PRD does
not inherit EXEC's 277-path or 13-type allowance.

## Specification audit findings and disposition

The initial draft was a useful milestone outline but was not sufficiently
precise for implementation. This audit found and corrected these specification
defects; it does not claim any new runtime defect or completed feature test.

| Finding | Correction / remaining gate |
| --- | --- |
| Aggregate requirement ranges could claim whole QUERY/SIM/RELEASE completion from a narrower example. | Exact selected-ID mapping and partial residuals; separate AUTH1/NET1 exits and no completion credit from a mapping alone. |
| Denying every lifecycle request could appear to satisfy authority separation. | User explicitly chose authorized remote stop; require its successful exact-target execution plus send-only denial. |
| A second real verifier made the deterministic-first plan depend on a future production provider. | Two meaningful independent deterministic implementations prove the static port; Selo remains downstream. Real secure-link tests stay mandatory and distinct. |
| Wire encoding could be read as authorization to implement a custom codec. | User-selected crate-first policy; evaluate existing libraries and obtain the user's choices. Bespoke mechanisms require a demonstrated gap and explicit exception approval. |
| Delivery labels omitted the irreversible transmit boundary and could lose an earlier receipt. | Evidence/ownership table covers local rejection, possible transmit, remote refusal, admission, processing and cancellation; facts and independent failures coexist. |
| Replay retention and static ownership lacked failure assumptions. | Eviction cannot renew eligibility; scope restart/reconnect/ID exhaustion explicitly; no global writer-safety claim from static config or cloned identity. Concrete policy/mechanism remains a user decision. |
| Verification before waiting for mailbox capacity could become stale. | Admission freshness/cancellation probe on the actual sealed external-target path; privacy of the interpreter is not the only composition concern. |
| Bounded queues did not constrain producer futures, acquired replies or library buffers. | Per-resource bounds, aggregate accounting, capacity-plus-one assertions and explicit cancellation custody. Numeric limits await user-approved gate B. |
| Design and acceptance gates were conflated and could create AUTH1/NET1 circular waiting. | Isolated source-bound probes close design gates; local AUTH consumer precedes live networking; full combined evidence follows implementation. |
| Broad scenario names lacked a falsifier and interactions. | R01–R25 specify observables and adverse/inversion conditions, with selected combined campaigns and durable/cryptographic exclusions. |
| Agent recommendations could silently become approved design. | Mandatory user steering before every new choice; record answers, rationale and scope, and reopen affected choices when evidence changes. |

Open decisions remain within C/I/W/V/B, exact implementation budget and their
user-approved alternatives. After the specification audit, the user selected
both connection layouts, admission-time permission rechecking, JSON with
serde_json 1.0.151, exact protected bytes and Zenoh 1.10.1 over mutual TLS/TCP
with configured-only CA trust, as recorded above. The approved owning trust
correction is locally verified; registry delivery and networking integration
remain open.
The local retirement report/error/receipt names and access methods are selected
above. Remaining network abstractions, default limits and protocol choices are
unselected. Existing safety
laws constrain the options; they do not authorize agent-selected representations.

### Continued delegated recommendation policy

The user reiterated that recommended answers are to be selected automatically
until revoked. This replaces the earlier per-choice waiting requirement; retain
the decision, alternatives and verification obligations before dependent edits.
For the test-only child-retirement adapter, preserve its independently acquired
report beside native results and failures in a three-element return product,
rather than discard it. Two existing observation test callers receive the
report and assert its actual assessment. No production interface or type is
added; the existing observation source path remains within the 250-path cap.
The actual standard-interpreter settlement regression and capture-deletion
inversion are selected within the existing effects test module (90–120 expected
test lines, no new production law or public API). This is owning-mechanism proof,
not public Application or full remote acceptance evidence.

### Controlled dependency source minimization and required candidate CI

Coordinator independently reviewed the exact committed controlled candidate
`9b6f308e3557e4b9c1c9a782bb0121255640002a`, including TLS/private-root
selection, safe owning generator, supported feature graph, actual archived
manifest/lock relationships, source/provenance inventory and Cargo publication
custody. All identified archive-reference and candidate-custody gaps have
concrete restored controls and intended inversions; no review finding remains.
The private release credential owns one concrete standard SDK implementation
and one audited version/checksum product: 57 production lines, 173 test lines,
16 manifest lines and 221 generated lock lines, no new public API. Cargo's
existing SDK owns credentials/protocol, exact archive-digest authorization and
upload; no replacement verifier, uploader, codec, cache or fallback is retained.
The corrected source is independently reviewable from its delivery automation.

Complete controlled source change: 71 paths, including 24 delivery paths;
delivery human-authored 1,168/1,300, generated 3,311, all artifacts 4,479/4,800.
Original runtime correction remains net 725 (conservative 758/770); private
release-tool production adds 57 separately. Delivery public API +0/-0. This
is the scoped ownership/interface minimization result, not proof of full remote
actor acceptance. The current source is clean, including generated bytecode.

Required `Controlled TLS dependencies` check passed the exact candidate on
2026-10-10 at 05:23:06 UTC: [run 38026158685](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38026158685).
Coordinator read the actual PR check and head independently; draft PR 1 was
`CLEAN`. Select normal protected PR merge, then required CI on the actual merged
main before manual publication using standard Cargo and the configured secret.
No admin bypass or author self-approval is selected. Actual registry permission,
publication receipts and ordinary registry consumers remain unproved until
those steps execute. Neither a candidate check nor its archives substitutes
for the merged-main source, artifacts, CI or full Bombay feature delivery.

### Quiet startup and independent report conservation

Auto-select the compact startup-assessment correction before dependent edits:
a rejected initialization can already have offered and explicitly discharged
its original error through the existing Core settlement path, while its
independently acquired report still truthfully records a failure. Merely adding
that summary to creation-order eligibility would manufacture a native
StartupRetirementFailed row with no original cleanup failure. Preserve the
compact report in the existing occurrence owner and combine it into its later
subtree assessment, retaining the original native-row eligibility. This keeps
the report without restoring a discharged error or creating history, a list,
a registry or a new type (one owning path, estimated 12–20 production lines).
The actual quiet-startup regression must prove this distinction; ordinary
creation rejection and genuine startup-cleanup failures retain existing custody.

### Entity native conversion conserves the same acquired assessment

The connected source audit found one additional real native flatten: successful
Entity retirement also converts LocalOutcome into ActorRetirement. Auto-select
passing the already approved runtime-issued ActorRetirementReport beside the
original native result through EntityDefinition's existing consuming retired
callback. Derive it from actual joined custody and the first notification before
conversion; no second publisher, callback, wrapper, task or public type is added.
The alternative explicitly discharges these summaries from a native-only Entity
contract, which would withhold already acquired failure evidence when settlement
policy has consumed the corresponding native error. Keeping that independent
evidence is preferred. This extends an existing callback by one report argument;
its implementations require explicit migration. Existing family failure rows
continue preserving actual callback, task and notification causes; callback
panics cannot rewrite the report already supplied. Estimated 5–10 production
lines plus necessary existing caller parameters and assertions, within the
2,200-production/five-type/250-path stage. Reverify the six owning Entity laws
with observable report assessments in debug and optimized builds, including the
original native and notification custody inversions. Full remote Entity routing
and production durability remain outside this milestone.

Entity report scope remains the joined actor's owned subtree. External family
owner callbacks (forced retirement, shutdown-request conversion and consuming
retired notification) have separate existing native family failure custody;
they are not failures inside that actor merely because they occur before its
report is derived. Preserve those original causes independently without
inserting them into the actor report. The alternative widens the same report
to unrelated caller/family-owner operations and loses its exact target scope.
Runtime-owned effect/interpreter and descendant failures inside the actor
remain included, as selected. This freezes the existing scoped report law and
adds no production mechanism.

Connected root checkpoint after 4ce918d, including all tracked/untracked changes:
21 paths, no untracked files; conservative production +1,448/-418/net +1,030,
tests +2,017/-86/net +1,931, documentation +1,667/-42/net +1,625, manifest/lock
+0/-0, public API +5 types/-0. Source prefixes before their first test module
are conservatively production. Exact source-bound measurement remains in
`/tmp/bombay-report-connected-checkpoint.json`. The root library passes
`nix develop -c env CARGO_INCREMENTAL=0 cargo check --locked -p bombay-rs --lib --jobs 2`
with no warnings. This is syntax/integration evidence only; new report tests
and their inversions are not yet executed. The five-type, 2,200-production and
3,000-test local stage and 250 cumulative-path bounds still apply.

Select the actual quiet-startup oracle using the existing closed child/parent
fixture: one rejected initialization retains its original error and input
allocation in the complete Core settlement; a later live child actually
retires; the native child-failure lane remains empty while the independently
retained parent assessment is Established/FailuresFound. Estimated 120–170
test lines plus fewer than 15 owning fixture lines. Deleting the actual
startup-summary acquisition must fail the Found assertion in both builds.
This directly tests the selected compact correction and does not substitute
a hand-copied binding model or infer a creation route. Existing projector-panic
coverage retains its original stop disposition. Combined source accounting
will precede any concrete excess of the current stage envelope.

### Actual controlled-source merge and continued local integration

Controlled source [PR 1](https://github.com/devrandom-labs/bombay-zenoh/pull/1)
merged by the ordinary protected PR path at 2026-10-10T05:26:19Z, commit
`e82481825e313ff14e5ea3a1d6e842040945372a`. Required merged-main CI is
[run 38027503275](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38027503275);
it subsequently passed at 2026-10-10T05:44:54Z. This records controlled
dependency-source delivery only, not NET1/AUTH1 implementation or full actor
acceptance. The later publication-prefix record below identifies the actual
five published versions and the four pending versions. Fresh merged-main standard Cargo publish dry-run and
non-dry package commands produced identical hashes for all nine packages, with
all 29 archived checksum/live edges and six inversions passing. Actual uploads,
registry authorization and ordinary no-override consumers remain open.

Combined local production also compiles after the Entity callback correction.
The first actual `cargo test --locked -p bombay-rs --lib --no-run --jobs 2`
through pinned Nix failed at 54ae576 with 23 test-source migration errors and
three warnings: report fields/imports and the new exact observation constructor
were missing from some existing fixtures. No new test ran and no semantic
inversion credit is assigned. Owners are repairing their existing fixture
contracts before the combined debug/optimized report campaign.

Recommended release scheduling under delegated selection: after required
merged-main CI succeeds, dispatch the existing manual publish operation once.
Its owning script already enforces exact current main and its passing required
check, executes standard Cargo dry-run, audits actual candidates, gates the real
archive checksum and retains before/after registry receipts. A separate manual
verify run immediately before that would repeat the same cold-job packaging
without adding a correctness obligation. Keep verify available for future
preview use, but no redundant verify-then-publish pair is required here. This
changes no workflow, source, gate, credential authority or scope.

### Actual publication prefix and bounded source-preserving continuation

Required merged-main CI passed exact e82481825e313ff14e5ea3a1d6e842040945372a
at 2026-10-10T05:44:54Z. The single manual publication run
[38028659029](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38028659029)
then published FIVE packages successfully: both configuration packages 2.2.0,
link-commons, config and link-tls 1.10.1. Their live registry checksums match the
original audited artifacts. Crates.io refused plugin-trait with HTTP 429 new
crate rate limiting; link, transport and zenoh also remain absent. Cargo exit
101 and before/after receipts are retained. Actual token authority succeeded
for the published prefix; this is a rate-limit interruption, not an authorization
blocker. Do not rerun the complete nine-package selection or regenerate a new
source revision's artifacts under the already published versions.

Auto-select the reviewed source-preserving continuation. Standard Cargo dry-run
selecting exactly the four missing packages at the clean original e824 source,
using their actual default/dependency features rather than flags for packages
not selected, produces all FOUR archives byte-identical to the original Linux
publication ledger. This is artifact-custody evidence, not another upload.
The original retained cohort ledger SHA is
`7de1e464c87156b91c735b086d886324d2d5bbbe5c28ef3c4c6229ed17962d4f`.

The existing dispatcher cannot express a partial cohort. Before edits, authorize
the bounded correction under the user's delegated recommendation: five CI and
provenance paths, three new paths; new resume-release shell at most 110 lines,
cohort policy using standard Python libraries at most 140, owning tests at most
100, existing workflow and README at most 15 net lines each. Expected total
human-authored expansion at most 380. Raise the delivery human ceiling from
1,300 to **1,700** and complete artifact ceiling from 4,800 to **5,300**;
generated baseline remains 3,311, so projected human 1,548 and artifact 4,859.
Delivery extras grow from 24 to 27, controlled changed paths from 71 to 74;
the 250 cumulative path ceiling and local Rust report-stage ceilings remain.
The new shell/Python policy is private CI production, estimated at most 250
lines, counted explicitly rather than described as documentation. It adds no
runtime Rust, public type, dependency, credential codec or upload replacement.

The new automation revision must pass its own protected PR, independent review
and required main CI. It may resume only the fixed original failed dispatch
run on e824 with exactly one named artifact and that exact ledger hash. Both
the original source's passing required CI and current reviewed automation's
passing required CI remain mandatory. Package source is a clean detached e824
worktree, distinct from automation; no arbitrary old or failed source is allowed.
Use standard GitHub artifact metadata/download with narrowly added actions:read.
Audit all nine original candidates with the original source auditor, prove all
five live prefix checksums, require precisely four missing versions, generate
and compare the four standard Cargo candidates, re-audit the full cohort and
restrict the existing SDK ledger to only the four original identities. Retain
all nine post-operation receipts even if another refusal occurs. Another prefix
needs a new reviewed exact continuation; no automatic retry or skip is selected.

Policy controls and intended inversions cover wrong source/run/artifact ledger,
prefix checksum mismatch, an unexpectedly registered remainder, missing/extra
selection and changed candidate bytes. Original archive audit and actual Cargo
regeneration remain separate observed proofs. The source-preserving mode is
preferred over changing VCS and silently publishing different artifacts.


### Delegated recommendations and root report regression evidence

The user's latest standing instruction is to select each recommended answer
until they say otherwise. This replaces the earlier per-choice waiting rule.
Continue recording each concrete recommendation, alternatives, consequences,
uncertainties and scope before dependent implementation; do not reopen approved
choices without changed evidence or claim that delegation supplies missing
verification. The explicit scope checkpoints remain recorded under this
continuing authorization.

The actual work-barrier inversion moved the existing permission wait before
root joining/report publication. Both debug and optimized public Application
regressions failed with the intended assertion: the joined report was unavailable
without the work-completion barrier. This proves the real service/work deadlock,
not just a private classification predicate. The original execution source SHA
was `300f5c7fd688c931f1a344a2c19e9a8725d86025f321130dc7a99419720cf775`;
mutant `24519040c1eda2507f5b330663309455d99233ec97e3150f857b0dcc390d5ea3`.
Both logs are retained at `/tmp/bombay-root-report-work-barrier-{debug,release}.log`.
The ten-second limit detects the regression and is not a selected remote timeout.

The primary-task-failure inversion changed only CapabilityFailed classification
to NoFailuresFound. Both profiles failed the actual joined report assertion:
Incomplete instead of FailuresFound, while later failure vectors were empty.
The actor's original task JoinError and native payload remain independently
asserted. Healthy terminal source SHA
`0a114e17ffcb8a07cd82bfb4a9d5624a00494e64b3bca38bc866d5ac5de09810`;
mutant `793edd24712c638956b18937aefedfae28511f5528cf8a24477b63242e250733`.
Logs: `/tmp/bombay-primary-capability-report-{debug,release}.log`.
Both owning files were restored byte-exact before the final healthy controls.

Controlled partial-release candidate
`d54c4be3ce971a8e044303bb6f1c321037edadf4`,
[PR 2](https://github.com/devrandom-labs/bombay-zenoh/pull/2), touches six paths
(the initial five-path estimate omitted the existing verification entrypoint).
Actual private CI production is +173 lines; owning policy tests +80;
workflow +13/-2, documentation +11; total +279/-2/net +277.
Cumulative human-authored delivery is 1,445/1,700, generated baseline 3,311,
complete artifact 4,756/5,300, controlled changed paths 74 and delivery extras 27.
No runtime/public API/dependency change. The independent source review verifies
fixed original run/source/ledger custody, exact five-prefix/four-absent guards,
original-source Cargo archive equality, four-only SDK authority, current main
recheck immediately before publication and original Cargo refusal/post-receipt
preservation. Required CI, reviewed merge, merged automation CI and actual
resumed upload remain pending; this is not publication completion.


Final restored root controls pass: both actual public Application report tests
and the primary capability failure report test each executed exactly once in
both debug and optimized builds (three tests per profile, 305 independently
listed combined library tests). Commands use pinned Nix, CARGO_INCREMENTAL=0,
`cargo test --locked -p bombay-rs --lib [--release] FILTER --jobs 2 -- --nocapture`.
Filters are `root_retirement_report_is_available_before_application_work_finishes`,
`root_report_notification_fault_retains_native_result_and_committed_report`, and
`joined_report_preserves_primary_capability_failure_without_later_failures`.
The complete final control log is `/tmp/bombay-root-restored-controls.log`.
These focused witnesses do not establish the remaining child/Entity campaigns,
full workspace checks, distillation or remote acceptance.

PR 2 readability follow-up only, head
`3a8de94736f692172cc16f852f215330debe3b93`: workflow input now names current main
versus retained source for resume; README retains its heading first. Actual
stage +280/-3/net +277, unchanged scope and contract. Fresh exact-head required
CI is [38030056828](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38030056828).


### Public caller migration checkpoint before edits

The combined root controls pass in both profiles, and the fresh owning launch
campaign independently lists all 305 tests and passes its selected eight tests
in both profiles. Its intended source inversions remain running. Complete the
same report prerequisite by migrating existing callers; this adds no new actor
or retirement law. There are 69 obsolete native-in-cleanup patterns across 19
existing integration/example paths, plus ten Entity callback implementations
and current guidance. The smallest existing end-to-end regression is that these
actual public examples/integration targets fail to compile against the new
independent native and notification receipt return contract. Their original
phase, exact payload, failure and static-denial assertions must remain intact.

Auto-select direct ordinary Rust tuple/struct matching, with no compatibility
wrapper or reconstruction of the old combined cleanup result. Work-phase cleanup
now checks its own unit result; native actor custody and the two named original
notification results are inspected alongside it. Cold or failed startup still
retains its actual receipt closure rather than fabricating an actor or successful
notification. Entity callbacks retain the read-only report in their existing
native-result owner. The alternative is a new compatibility adapter that folds
independent receipts back into cleanup; reject that because it hides the proven
independent ownership and adds a second application spelling.

Expected migration: the 21 existing caller/example paths, affected compile-denial
fixtures and at most four current guidance documents; estimated +100–250 net
example production and +900–1,300 net tests/fixtures. Existing owners and values
are reused. New public types/traits/macros/dependencies: zero; no legacy result
wrapper is retained. Source report stage remains bounded by 2,200 net production
and exactly five cumulative public types. Raise the cumulative local test/fixture
ceiling from 3,000 to **4,300** before these necessary migrations; source-bound
recount and an explicit further bounded recommendation precede any excess.
The global 250-path ceiling remains. Isolated Entity migration preparation and
root Application migration have disjoint owning edits; the coordinator alone
integrates their imports/contracts, documentation and scope accounting.


Public migration checkpoint, complete root working tree against a9c5b7d:
44 changed paths, no untracked paths; production +1,565/-456/net +1,109;
tests/fixtures +4,159/-536/net +3,623; documentation +1,949/-66/net +1,883;
manifest/lock +0/-0; public API +5 types/-0. Exact source-bound rows are retained
in `/tmp/bombay-public-receipt-checkpoint.json`. Source prefixes before the first
cfg(test) module are counted conservatively as production. Current stage bounds
2,200 production, 4,300 tests/fixtures and five public types are respected.

The 69 ordinary native-in-cleanup patterns have been replaced by direct Rust
work/native/notification matching. Existing cancellation and prepared/cold
receivers are also migrating: actual closed native/notification receivers cannot
be converted into successful actor absence. Original consuming projection,
allocation ownership, native panic and exact callback failure assertions stay
with their concrete consumers. Four current guidance documents now describe
the independently retained receipts, read-only report axes, early root report
publication and paired Entity native/report callback. Historical execution
ownership delivery documents remain evidence of that earlier contract.

Parallel compilation uses the separate metadata-only target
`/tmp/bombay-public-receipt-check-target` (196 MiB at the first checkpoint),
while isolated owning debug/optimized inversion campaigns keep exclusive use
of ROOT/target. Pinned command:
`nix develop -c env CARGO_TARGET_DIR=/tmp/bombay-public-receipt-check-target CARGO_INCREMENTAL=0 cargo check --locked --workspace --all-targets --keep-going --jobs 2`.
Three observed checks retain logs at `/tmp/bombay-public-receipt-check{,-second,-third}.log`.
They correctly expose uncompleted public consumer migration; they are not failing
semantic regressions. The third leaves Entity callback/source consumers and one
HTTP bind-refusal return pattern, whose direct tuple correction is now applied.
Combined workspace compilation, all required verification and delivery remain
pending. Core source and Cargo.lock are unchanged during this caller migration.


The owning launch campaign passes all eight restored controls in both profiles;
all three source inversions fail their intended native/report/notification
custody assertions in both profiles, with byte-exact restoration and matching
initial/final binaries. Its strict production Clippy run found one genuine
`match_same_arms` veto at local/environment.rs:924: Original/Offering and
Completed(Admitted) source states both correctly assess Incomplete. Before the
owning correction, select one exhaustive grouped pattern rather than a lint
suppression. Expected one existing source path, -3 net production lines,
zero semantic/API/type/dependency change. No new mirrored test is needed;
existing focused assessment controls remain, and combined strict checking must
pass. The Entity/standard settlement/quiet-startup campaign now owns the shared
test target; coordinator verification uses the separate metadata target.


Exact owning Entity caller migration ae4396d is integrated after its twelve
fresh controls passed both profiles. Coordinator resolved only the nested
notification/report import and added the independent notification receipt to
the two existing Entity Application return sites. Callback/native/report
assertions are unchanged from the contribution. Mechanical .stderr span updates
remain unverified until actual trybuild. Combined workspace/all-target compilation
then passed with one test-only HTTP import warning; that import is now cfg(test).
Strict production library Clippy passes on the corrected grouped match (actual
+5/-6/net -1, versus the earlier -3 estimate). All checks use pinned Nix.

Workspace strict checking also identified one caller test's 105/100 line count.
Its existing pre-projection match merely forwarded every successful retirement
and separately rejected ActorTaskFailed; the owning complete terminal assertion
already denies every noncompleted native variant. Pass the unchanged original
native enum directly to existing ProjectTerminal in the two pool callers,
preserving every native error for that consumer and removing the redundant
pre-projection branch. This adds no helper, type, trait or lint suppression.
The complete pool/worker observable assertions remain intact. Full strict
workspace and combined tests still remain required.


Controlled resumption automation [PR 2](https://github.com/devrandom-labs/bombay-zenoh/pull/2)
merged at 2026-10-10T06:39:56Z through ordinary protected review, commit
`373d25a9261e4d3a6b5e2ab739142fad448c2ed0`. Exact candidate required CI
[38030056828](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38030056828)
passed at 06:37:46Z. Independent coordinator
[review](https://github.com/devrandom-labs/bombay-zenoh/pull/2#pullrequestreview-5477927259)
is a COMMENTED review, not a fabricated formal author approval. Required exact
merged-main [CI 38031719465](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38031719465)
is running. Actual resumption remains gated; package source stays original e824.

Before the registry-only consumer's first Cargo resolution, account for its
fourth existing-evidence path, Cargo.lock, beside the three previously counted
consumer source paths. Authorize at most 3,500 generated lock lines as separate
registry-consumer graph/checksum evidence, not controlled source or handwritten
production. The controlled delivery remains human 1,445 plus generated 3,311 =
4,756/5,300; including this additional evidence the planned aggregate is at most
8,256 artifact lines. There is no new handwritten source, dependency selection,
Cargo override or public type. The selected normal registry graph and all nine
controlled checksums must be verified before consumer credit. Reconcile the
complete unique source/evidence path union before the generated addition; if
that concrete union exceeds 250, record a bounded recommended expansion first.
No real upload or registry-consumer success is claimed from this preparation.


Registry consumer reproducibility recommendation: seed its fourth evidence
path with exact Cargo.lock bytes from the already audited original
bombay-zenoh-1.10.1 archive, then let ordinary Cargo resolve the registry-only
consumer against that lock. This avoids adopting unrelated fresh semver upgrades.
Cargo owns the transition from source-absent controlled package nodes to actual
published registry identities; all nine checksum receipts must match the original
cohort. Retained external name/version/checksum records must stay selected.
No manual lock rewriting, dependency override, new manifest or dependency choice
is added. Any other concrete graph difference is a finding before adoption.
This resolution remains after all nine actual publication receipts.

All-target strict Clippy exposed test-only diagnostic quality obligations that
library-only Clippy cannot cover: observable empty assertions, an explicit unit
pattern, semicolon/style choices, stale length expectations, long complete
native/temporal witnesses, and a literal Result unwrap in a classifier control.
Retain every full trace and original-cause assertion. Prefer direct existing
native projection over a redundant pre-projection match where the owning
terminal assertion already denies noncompletion; no new compatibility adapter
or assertion helper is selected. Use ordinary explicit patterns and length
assertions where element equality would impose irrelevant traits. For a complete
inseparable exhaustive native-product or temporal controller, a narrowly scoped
`expect(too_many_lines)` with its actual ownership rationale is preferable to
inventing wrappers solely to hide lines; remove stale expectations. No blanket
lint allowance or semantic relaxation is selected. Expected existing paths only,
zero public API/dependency growth, at most 80 net test annotation/assertion lines;
current 2,200/4,300/five-type stage bounds still apply. Run combined strict checks
and affected debug/optimized controls after these nonsemantic test corrections.


### Combined local report verification checkpoint

The complete ROOT working tree against a9c5b7d now contains 50 changed paths,
zero untracked paths: production +1,592/-472/net +1,120; tests/fixtures
+4,322/-636/net +3,686; documentation +2,068/-66/net +2,002; manifests/lock
+0/-0; public API +5 types/-0. This measurement precedes this evidence paragraph
and status-index correction. Rows and current source hashes are retained in
`/tmp/bombay-public-receipt-checkpoint.json`. The existing stage limits remain
2,200 net production, 4,300 net tests/fixtures and exactly five new public types.

Combined strict all-target checking passes:
`nix develop -c env CARGO_TARGET_DIR=/tmp/bombay-public-receipt-check-target CARGO_INCREMENTAL=0 cargo clippy --locked --workspace --all-targets --keep-going --jobs 2 -- -D warnings`.
Actual exit 0; log `/tmp/bombay-public-receipt-clippy-third.log`.
The nonsemantic corrections preserve complete traces, remove redundant native
identity matches, and use narrowly reasoned expectations for inseparable
controllers; no blanket lint allowance, public abstraction or dependency change.
Pinned formatting and Git whitespace checking pass.

Combined default library debug verification executes all 305 tests: passed,
zero failed/ignored/filtered. Command:
`nix develop -c env CARGO_INCREMENTAL=0 cargo test --locked -p bombay-rs --lib --jobs 2`;
log `/tmp/bombay-combined-report-debug.log`. This includes all migrated owning
root, launch, Entity, quiet-startup and settlement controls together, rather than
claiming combined proof from isolated binaries. Optimized combined verification,
full workspace checks, source/API minimization, reviewed PR and required CI
remain pending. No remote acceptance row is closed by this prerequisite.


### Reconciled cumulative source scope and publication resumption

Independent accounting finds that the earlier 246-path subtotal omitted retained
proposal/inversion patches. It was historical-plus-measured arithmetic, not a
complete enumerated union: historical 166 (including five retired intermediate
names), ROOT 50 minus four confirmed historical overlaps, controlled source 74
minus its original 47, registry consumer four and migration scripts three.
The complete historical 166-member ledger is unavailable; do not invent it.
Three launch inversion patches and three prepared migration/style patches add
six counted paths under the existing proposal-patch precedent; the distinct
counter draft and two source-byte backups bring the known conservative count
to at least 255. Count these rather than silently treating research as free.

Automatically adopt the recommended bounded cumulative expansion from 250 to
**300 retained non-snapshot source/probe/proposal/fixture/document paths** before
further source additions or the registry consumer lock. This preserves the
necessary real inversion fixtures and exact-restoration custody. The alternative
is deleting useful review/inversion evidence merely to fit the old count; reject
that. Runtime-generated logs, binaries, Cargo/Nix outputs and archive receipts
retain their separate artifact records and byte/line custody; this expansion
does not exclude handwritten source or proposed source patches. The actual
retained artifact inventory and historical-count uncertainty must remain
visible. Local 2,200-production/4,300-test/five-type ceilings and controlled
1,700-human/5,300-artifact ceilings remain unchanged. Recount any new source
before exceeding this bounded recommendation.

Controlled automation exact merged-main CI 38031719465 passed at 06:59:08Z on
373d25a9261e4d3a6b5e2ab739142fad448c2ed0. The single authorized four-package
[resumption run 38032849420](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38032849420)
is dispatched against immutable original package source e8248182. Actual final
registry receipts and registry-only consumer results remain pending.

Combined optimized default library controls now also pass: all 305 tests,
zero failed/ignored/filtered. Command:
`nix develop -c env CARGO_INCREMENTAL=0 cargo test --locked -p bombay-rs --lib --release --jobs 2`;
log `/tmp/bombay-combined-report-release.log`. Required workspace build passes:
`nix develop -c env CARGO_INCREMENTAL=0 cargo build --locked --workspace --jobs 2`;
log `/tmp/bombay-combined-workspace-build.log`. Fresh remote main is still the
a9c5b7d baseline; work remains on the requested feature branch.

The full default workspace test build failed from host disk exhaustion while
writing the giant consumer test's debug symbols, not from a semantic oracle.
Log `/tmp/bombay-combined-workspace-tests.log`. No passing workspace claim.
After Cargo exited, reclaim only that failed build's generated object files and
obsolete previous-version giant-test debug-symbol objects, preserving successful
executables, source, locks, all proof logs and dependency archives. Exact removed
paths/bytes are recorded in `/tmp/bombay-failed-debug-output-reclamation.json`;
6.1 GiB is available after reclamation. Recommended local retry disables debug
symbols only, retaining debug assertions, ordinary ownership/drop behavior and
the exact selected toolchain/dependency graph:
`nix develop -c env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --locked --workspace --jobs 2`.
This remains a debug test profile, not optimized proof or a relaxed assertion
policy. The full required Linux Nix/coverage/law/loom/fuzz gates remain mandatory
in CI; resource failure supplies no semantic inversion or completion credit.


### Runtime-issued report static-denial witness before edits

The read-only report law already excludes application construction and mutable
fields, but the public suite lacks its own attempted-forgery denial. Recommend
one compile-fail fixture that tries to construct ActorRetirementReport from its
two public assessment values. Add it to the existing interface-authority suite;
the actual public Application report tests are the executed positive consumers.
This observes the selected public law rather than mirroring its classifier.
Expected two new fixture paths (Rust plus actual stderr), one existing test path,
at most 30 net test/fixture lines, zero production/API/dependency growth. A
source-byte backup for its owning visibility inversion also counts in the
300-path envelope. The alternative is relying only on source inspection of
private fields; reject that because an accidental later visibility change must
fail the public regression. Temporarily expose the exact two owning fields,
require the denial suite to fail because the forbidden fixture compiles, restore
byte-exact and rerun. No such production visibility is retained.

All-target local debug verification also passes, including actual benchmark
smoke execution: 556 passed, zero failed, one parent-controlled ignored test
across 57 target summaries. Workspace/doc command separately passes 557 tests
across 62 summaries; the ignored descriptor-limited child is exercised only by
its owning parent. Both actual changed Entity trybuild stderr fixtures match.
Log `/tmp/bombay-combined-all-target-tests.log`; command is the prior low-symbol
pinned workspace invocation with `--all-targets`. Draft
[PR330](https://github.com/devrandom-labs/bombay/pull/330), head49e0a46, has required
Linux Nix CI38033069536 running. Ownership review and any resulting focused
correction remain before readiness; there is no merged report claim.


Static-denial refinement before the second fixture: independent review confirms
both Rust construction routes need coverage. An accidental public constructor
could forge a report even with private fields. Add a separate private-constructor
fixture and its actual stderr beside the literal denial; expected four new
fixture paths, one existing suite, at most 60 net test/fixture lines, zero
production/API/dependency changes. Each exact visibility inversion must fail
because that otherwise forbidden case compiles; restore and rerun. This is the
same already-approved runtime-issued report law, not an additional report owner.


### Actual standard-capability disposal-panic witness before edits

Independent minimization identifies no additional production owner/API defect,
but the completed-retirement-with-cleanup-failure promise lacks an actual final
standard-capability destructor panic witness. Existing Established/FailuresFound
cases cover Behavior, activation or first notification failures. Automatically
select the smallest real public Application regression: extend the existing
ShutdownLedger report/work-barrier controller with normal disposal and an
original-payload panic in a test-only Hosts<LedgerProtocol> product's Drop.
The outside controller retains only the real ActorSpace clone; prove the final
ApplicationCapabilities owner actually disposes the product after owned resources
retire. No opaque interpreter fixture may certify standard resource completion.

Both cases preserve the full native state, exact allocation and complete action
lanes. The panic case requires Established plus FailuresFound, the original
opaque cleanup cause retained in the native result, successful independent
notification receipts, and an already absent root address. Observe original Box
allocation identity without downcasting or opening the private panic; Arc Weak
custody persists until explicit native discharge. Normal disposal keeps the
complete empty-failure control. Hold Work until the report is acquired in both.
Expected one existing source/test path, +80–120 net test/fixture lines, zero
production/public types/dependencies. The existing 4,300-test envelope remains.
A retained prepared patch is counted under the 300-path scope. The alternative
is weakening the completion/failure promise; reject that because the user
selected preserving cleanup failure after full retirement.

The intended owner classification inversion makes resource completion
NotEstablished solely when native retirement_failures is nonempty. Both
profiles must fail the independent completion assertion, then restored controls
must pass. This is a verification blocker before report-prerequisite distillation,
not authorization for another runtime owner or broader unrelated refactor.


### Controlled dependency prerequisite: actual delivery

The controlled dependency stage is delivered, independently of AUTH1/NET1.
Reviewed source PR1 merged at e82481825e313ff14e5ea3a1d6e842040945372a;
required candidate CI 38026158685 and merged-main CI 38027503275 passed.
Reviewed resumption PR2 merged at 373d25a9261e4d3a6b5e2ab739142fad448c2ed0;
required candidate CI 38030056828 and merged-main CI 38031719465 passed.
[Final publication 38032849420](https://github.com/devrandom-labs/bombay-zenoh/actions/runs/38032849420)
succeeded at 07:03:17 UTC using reviewed automation 373d and original package source
e824. Artifact 11662807342 digest
`ac2a47921204237a149b60fd1f004fa72ba516e72376cc6800483a0e139c70c6`
retains original and remaining ledgers, actual before/after receipts and all nine
archives at `/tmp/bombay-resumed-published-cohort/retained-source/target/package`.

Coordinator independently compares all nine actual Cargo-fetched registry
archive bytes against original audited archives and checks every checksum below.
The final after receipt exactly matches the original nine-package ledger; before
retains the exact five published records and four actual absent responses.

| Published package | Version | Exact registry/archive SHA256 |
| --- | --- | --- |
| bombay-validated-struct | 2.2.0 | `b1b9764c93caf9628a9c391a92132f1bc84a8816b01ff486cc18b08101e2c3c7` |
| bombay-validated-struct-macros | 2.2.0 | `602d3538d65be50613f90d69dd21c504d4e894ae2ae13c41ba89163fd4b33b55` |
| bombay-zenoh | 1.10.1 | `708e420be07e49c00326fcebb5daf7df72dd92769eae28d9aed649c53a7648b2` |
| bombay-zenoh-config | 1.10.1 | `84f764e6cc4f3c122b87f5ad715460c54e09fc95b690919740defe4e264ed482` |
| bombay-zenoh-link | 1.10.1 | `40ff7d86f5e5da473ca7e05ca452c2782971ef3eb45ee1ac8c80a7b7fd9a5c91` |
| bombay-zenoh-link-commons | 1.10.1 | `ef9224e682a1cf8f57bb4fc88f98ac6a1f51036fc6f82b7b1c05dfcbafa8e37c` |
| bombay-zenoh-link-tls | 1.10.1 | `29075698e973b6d270f1d7e170209b70b4a2d14eac7aca61b559a29861c0246c` |
| bombay-zenoh-plugin-trait | 1.10.1 | `7f706fdfff554904c5c87ffee53ceeb5c16c81d720444e4add2877753af09b64` |
| bombay-zenoh-transport | 1.10.1 | `3adae45298b79e80fc6c3494a4e26b84b5f347ee71aa83d5db85df9db8f6b627` |

Normal registry-only consumer verification passes with no Cargo source override,
Git dependency or path dependency. Pinned Nix runs formatting, strict all-target
Clippy and executable assertions for default, retained maintainer and maintainer
macro profiles. Logs `/tmp/bombay-zenoh-registry-consumer-{metadata,clippy,default,maintainer,format}.log`;
consumer `/tmp/bombay-zenoh-registry-consumer`. Assertions check selected TLS and
unsupported transports, private-root selection configuration, corrected owning
macro payload/allocation preservation and nominal error identities.

Coordinator independently reads the original archive lock and resolved normal
consumer lock: all 296 retained external name/version/source/checksum records
unchanged; 15 archive-only dev/test nodes pruned; only the local consumer root
added. All nine controlled nodes are actual registry entries with original
checksums. Consumer lock 3,018 lines respects its 3,500-line allowance. Controlled
human 1,445 + generated 3,311= 4,756/5,300; with consumer lock the observed artifact
subtotal is 7,774 rather than the earlier planned 8,256 ceiling. No dependency
upgrade or replacement of an official package identity. Both controlled source
trees are clean at their exact delivered commits.

This closes the dependency publication/ordinary-install prerequisite only.
It does not supply Bombay protected admission, remote authentication, protocol,
resource/correlation/replay bounds, actual remote Applications or any full
R01–R25 acceptance witness. Root manifests/lock still have no Zenoh integration.


The actual standard-disposal control passes debug and optimized, observing one
final host-product disposal before Work release, exact lease disappearance,
Established/FailuresFound and its original opaque Box/Arc custody. The resource
classification inversion fails exactly NotEstablished versus Established in
both profiles; owning source is restored byte-exact. Strict Clippy vetoes the
test's intentional Box<Arc<Vec<u64>>> allocation, not production code: this Box
is the exact original Rust panic carrier, while Arc Weak independently observes
cause lifetime. Automatically select one variant-scoped, reasoned
`expect(clippy::redundant_allocation)` rather than reboxing the cause, removing
its independent lifetime witness, or inventing a test wrapper to hide the type.
Expected four annotation lines, no semantic/production/API change. Full strict
checking and final combined controls must pass; this is no blanket allowance.


Current-guidance residue audit confirms the remaining native-in-cleanup Rust
patterns belong only to advanced internal Application tests, whose deliberate
kernel contract remains distinct from ordinary public App. EXEC's delivered
PRD and API decision still described the superseded public spelling without a
clear historical notice; add two short source-bound notices pointing to current
caller guidance rather than rewriting earlier delivery evidence. Two additional
existing document paths count under 300; zero Rust/API change. The old native
root callback has no production occurrence.


### Joined-retirement prerequisite minimization and final local verification

Independent bounded source review finds no production ownership/API blocker.
Retained ownership is one existing runtime composition: Observe publishes
shared completed facts; actual actor/projector/cleanup tasks transfer affine
native and notification causes; the parent keeps complete native results.
Report acquisition precedes consuming application conversion and the root Work
barrier. Entity family/callback failures remain separately owned and cannot
rewrite an issued actor-subtree report. The old root-retirement callback is
removed, and public callers use ordinary named products/tuple matching rather
than compatibility wrappers. No new trait, task, registry, mailbox, runtime
framework, cryptographic implementation, macro or dependency is retained.

Exactly five added public types each express a proven distinction: retirement
establishment, independent failure completeness, a runtime-issued read-only
report, the two original notification results, and their original failure sum.
Fields and constructor stay private; no Default, serialization, raw-report
constructor or ActorRef stop authority is introduced. Existing owners derive
facts; applications receive or explicitly discharge original results. Actual
root/child/Entity/capability/startup tests and public Application integration
justify these seams. Removing either report axis confuses finished cleanup
with clean execution; folding native/notification custody back into Work loses
facts after independent failure. The read-only report owns no original payload.

The two external construction-denial fixtures execute their intended E0451 and
E0624 diagnostics. Exposing the two exact fields causes only the literal fixture
to compile unexpectedly; exposing only the constructor causes only its fixture
to compile unexpectedly. Both owning inversions exit101 with the intended
trybuild "Expected test case to fail to compile, but it succeeded" assertion;
restored suites pass. Healthy terminal SHA
`e18d1563fd48349660b65241589635d007f7efa11a61bbf74b62e012cb8f4fff`;
field mutant `f06c230af343c79abf17babc1c5086cdacf3e982c962e0d90c423ea16cfe849a`;
constructor mutant `2d2a9e55c3ef52ec6ec4b5d96c6ab0952b31a4affdb9576d5f236805fc0cdf72`.
Logs `/tmp/bombay-runtime-issued-report-{fields,constructor}-inverse.log`.
Actual diagnostic acquisition used TRYBUILD=overwrite only for new fixtures;
final full suite runs normally and preserves all unrelated diagnostics.

Actual standard final-host disposal panic control passes normal/panic cases in
both debug and optimized; independent count proves one real final-owner Drop
before Work release and the real lease is absent. Complete native lanes, exact
original panic Box allocation and independent Arc lifetime survive. Resource
completion remains Established while failures are Found; both original
notification results are successful. The intended resource-classification
inversion fails NotEstablished versus Established in both profiles, exit101.
Healthy Environment SHA
`ddcd78c0d4f601ce1991543894661c53031851e96a0be5495f8b639c8d618295`;
mutant `b1ede6fbd271eef93cda71374a5df192c5c9c18dc06904ac941fecb15f0d3bc2`.
Source restored byte-exact. Logs `/tmp/bombay-actual-cleanup-report-inverse-{debug,release}.log`;
final exact restored optimized control `/tmp/bombay-actual-cleanup-report-final-release.log`.

Final combined all-target debug suite passes556 tests across57 target summaries,
zero failures, one deliberately parent-controlled ignored child; both new report
construction denials and changed Entity denial fixtures match. Command:
`nix develop -c env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --locked --workspace --all-targets --jobs 2`;
log `/tmp/bombay-final-report-workspace-all-targets.log`. The earlier workspace
command additionally passed documentation tests. All305 default library tests
also pass optimized after restoring the cleanup classification; final annotation
has its focused optimized restored control. Strict Clippy across the workspace
and all targets passes, log `/tmp/bombay-final-report-clippy-restored.log`.
Pinned final formatting and Git whitespace checking pass. Cargo.lock/manifests
remain unchanged. The small intentional test panic carrier has one justified
variant-scoped allocation expectation; no blanket allowance.

Complete root measurement before this final evidence paragraph:56 changed
paths including four new denial fixtures; production +1,592/-472/net +1,120;
tests/fixtures +4,429/-636/net +3,793; documentation +2,320/-69/net +2,251;
manifests/lock +0/-0; public API +5 types/-0. Source/row hashes remain in
`/tmp/bombay-public-receipt-checkpoint.json`; the final committed checkpoint
recounts this complete evidence text too. No source/test/API ceiling exceeded.

Disposition: the scoped local joined-retirement prerequisite is distilled;
its independent review's two verification blockers are now executed and closed.
This disposition covers the report composition and migrated callers only.
A dedicated real pre-publication projector-cancellation campaign and independent
finite report algebra table are not claimed as executed; the missing-report
fallback remains conservative. Full remote R01–R25, production identity,
durability and SHM obligations remain exactly deferred/unexecuted. The complete
networking PRD is not feature-complete/distilled/merged. Draft PR330 still needs
fresh final-head required Linux CI, final review and actual protected merge.

### Actual joined-retirement prerequisite delivery

[PR330](https://github.com/devrandom-labs/bombay/pull/330) merged through the
protected normal PR route at2026-10-10T08:11:03Z, commit
`017ec7603e0193e26129e0af6d10b8c04fdad78e`. Exact reviewed head
`690e6283140ed238e6a62241960d8924aaa6eac6` passed all four observed checks:
required [Nix CI38035053836](https://github.com/devrandom-labs/bombay/actions/runs/38035053836)
completed successfully at08:06:48Z; CodeQL Analyze38035053829, CodeQL summary,
and dependency Deny38035053856 also passed. Linux Nix reports269 checks passing;
both bounded Driver/Observe fuzz campaigns and their artifact uploads pass.
Optional Observe Miri steps were skipped and receive no execution credit.
The final log is retained at `/tmp/bombay-joined-retirement-final-ci.log`.

The [recorded review](https://github.com/devrandom-labs/bombay/pull/330#pullrequestreview-5478186173)
reports independent nonauthor source review and retained proof inspection;
it is correctly a COMMENTED review, not fabricated author approval or an
independent test rerun. Active ruleset18433270 requires a PR and passing Nix
check, zero formal approving reviews, no bypass; merge matched the exact head.
Scoped prerequisite disposition is now merged. Full AUTH1/NET1 and every remote
R01–R25 obligation retain their incomplete status.

Final committed root delta against preparation maina9c5b7d covers56 paths,
zero untracked: production +1592/-472/net1120; tests/fixtures +4429/-636/net3793;
documentation +2401/-69/net2332; manifests/lock +0/-0; public API +5 newly
defined types/-0. The complete row/source-hash checkpoint is retained at
`/tmp/bombay-public-receipt-checkpoint.json`. This records capability growth,
not code reduction. Cargo.lock remains byte-identical.

### Next prerequisite: exact-recipient nonwaiting service admission

Prepared 2026-10-10 under the user's reiterated automatic recommended-choice
instruction. PR330 source remains frozen while its required final-head CI runs;
this preparation is isolated and will receive a branch from latest main before
any commit. No complete AUTH1/NET1 witness is claimed by the following stage.

The public service can currently check permission and then await capacity;
permission can change during that wait. Communication already owns a synchronous
capacity-nonwaiting attempt with exact Full/Closed payload return. The existing
matched public Application compile probe fails specifically because
`ExternalActor::try_send` is missing. Reusing that operation permits the selected
one-local-check-and-attempt law without a second queue, permit or mailbox.

Automatically select the recommended narrow inherent operation on ExternalActor
for an exact `EstablishedRecipient`, implemented by a private ActorRef
projection. Deliberately re-export the existing Communication `TrySendError`
through Bombay so callers can exhaustively match Full and Closed. Alternatives
are extending the sealed ExternalTarget to stable Entity hydration, exposing raw
senders, or a new error wrapper. Those either require a different unproven
admission law, expose excess authority, or duplicate the existing truthful sum.
This selection leaves stable Entity hydration unchanged and adds no public
ActorRef operation. The actual external actor's allocated address supplies
User::from; this does not authenticate a remote principal. Native error Debug
requires a Debug payload and can print it: callers with move-only non-Debug
messages use exhaustive matching, and the later bounded remote diagnostic path
must not format private payloads. No Clone or Debug message bound is added.
Capacity-nonwaiting is not wait-free: the existing admission mutex and queue
implementation keep their own synchronization laws. The naming, exhaustive
native sum and deliberate owner-error exposure were checked against the
[Rust API naming guidelines](https://rust-lang.github.io/api-guidelines/naming.html),
[type-safety guidelines](https://rust-lang.github.io/api-guidelines/type-safety.html)
and [future-proofing guidelines](https://rust-lang.github.io/api-guidelines/future-proofing.html).

Fresh source investigation also proves a smaller owning diagnostic defect.
Communication selected0.1.3, exact source272a2343187b40615ab26c2d0d2e136010a16e77,
is still its repository's latest main. Its existing mailbox-retirement test
closes admission, obtains Closed, then uses the still-live Consumer to drain
accepted work. UserClosed and TrySendError::Closed incorrectly display
"consumer dropped". Select an owning correction to "user lane closed", with
documentation distinguishing admission closure and consumer disappearance;
leave ControlClosed unchanged. Alternatives retaining the incorrect inference
or wrapping it in Bombay are rejected. This changes diagnostics only, not
delivery, custody, public types, or retirement. A corrected published version
and narrow locked selection require actual owning review/CI/publication evidence;
no version is invented or integrated from this preparation alone.

Change record before production: Bombay's three expected production paths are
local/endpoint.rs, application/interface.rs and lib.rs, estimated +65/-2/net63,
bounded at net70; owning public-Application test expansion is bounded at320
lines. Communication uses one existing production source path (diagnostic text
and rustdoc only, maximum8 net lines), its existing mailbox-retirement test
(maximum45 net lines), and owning guidance/release records as required. Public
types +0/-0; no trait, dependency family, codec, coordination owner, clock,
resource default, registry, task or macro. Existing native mailbox, exact
recipient interpreter, typed User origin and Application composition are reused.
The public surface adds one service method and one deliberate existing-owner
error re-export; zero newly defined types is not a claim of zero API growth.
Before applying the prototype, narrow its target contract to
`Target: Protocol<Addr = MailAddr>`: synchronous mailbox delivery creates no
future or task, and the owning recipient/mailbox interpretation requires no
message Send or static bound. The awaited ExternalTarget's stronger bounds
belong to its Send future and must not be copied here. Existing endpoint
establishment keeps its own requirements. A borrowed-message public control
will prove that no unnecessary static message restriction has been retained.
These additions stay within the recorded cumulative2200 production/4300 test
ceilings and300 retained-source-path ceiling; recount before implementation.

Verification first extends the actual existing owner closure control so both
diagnostics are asserted while the Consumer drains the full accepted prefix;
restoring either original text must fail its intended assertion in debug and
optimized builds. Bombay's public control fills a real external mailbox,
recovers the identical move-only allocation on Full, drains one slot, admits
that original once, and preserves all remaining accepted inputs after closure.
Repeated rejection of the same Closed payload must retain its allocation and
truthful sender origins. Complete native root and notification results remain
independently checked. That native projection witness is distinct from future
permission/expiry provider tests and remote replay-ID protection. Coordination,
time, retry scheduling and retained-byte limits remain unresolved contracts;
this method alone does not complete R08 or authorize a remote command.

Fresh pre-production consumer check executes the prepared public test against
unmodified690e628 source: pinned Bombay Nix `cargo check --locked -p bombay-rs
--test actor_interface --jobs 2`, separate existing metadata target, symbols0,
incremental0, exits101. Exactly E0432 for the selected owner-error re-export
and six E0599 occurrences for the missing service operation; no unrelated
diagnostic. Log `/tmp/bombay-exact-admission-before-api.log`. Prepared owning
test +183/-11/net172, SHA
`a6ec17589fed88b6a871725142ee58570fb2eaffb9316f668b013808ba450c82`.
This is the actual missing public operation regression, not a runtime policy
inversion. Communication's old diagnostic fails its new assertion after the
complete live-consumer prefix drain; owning debug proof exits101 as intended.
The separate owning Rust1.95 pinned Nix/CI remains the strict owner gate; newer
Bombay Rust1.99 reports inherited fetch_update deprecation during exploratory
owner compilation. Do not fix that by inventing an atomic protocol, raising
the owning minimum or suppressing its warning inside this diagnostic stage.

Delivered-Zenoh runtime source readiness remains investigation rather than a
selected command mechanism. At exact sourcee82481825e313ff14e5ea3a1d6e842040945372a,
query-builder into_future invokes synchronous wait/send_request before polling;
mark possible transmission before that foreign call. Auto consolidation can
become Latest and collapse/delay replies; explicit None avoids that map but
does not prove all count/byte bounds. Stable Querier retirement removes its
pending local queries, not remote actor work; session timeout tasks may remain
until timer/session cancellation. Routed per-face pending-query maps have no
discovered count ceiling and accept request timeouts without a discovered
maximum clamp. FIFO callbacks can block at capacity; Ring callbacks evict.
Pub/sub avoids native query tables but needs an explicit typed receipt protocol.
Compare actual bounded callback custody, cancellation, late replies, router
retention and close/restart behavior in both approved TLS layouts before
retaining either mechanism. No unexplored native capacity setting is assumed,
no command/correlation schema is selected, and no full remote gate passes here.

The new public controls now compile without warnings and pass in both debug
and optimized execution, including the borrowed-message control. Before
expanding tests, record the concrete remaining owning branch witness:
EndpointMailbox::Entity uses the same private ActorRef projection but wraps
User inside Message ingress. Add at most106 owning test/import lines in the
existing endpoint.rs test module, within the existing320 stage-test ceiling.
Use an explicit existing Communication capacity2, real Admission, mailbox,
Observe pair and owning MessageProtocol; no new test protocol or publisher law.
Observe full/closed exact payloads, repeat the same closed original, and drain
both accepted User inputs with their supplied origin and the complete terminal
trace. The private component test does not prove actor installation, stable
Entity hydration, joined retirement or authentication. Independently invert
each Entity Full/Closed projection in debug and optimized profiles, restore
byte-exact, then rerun controls. Production projection remains unchanged.

### Owning closure correction delivery and registry selection

Communication [PR9](https://github.com/devrandom-labs/bombay-communication/pull/9)
merged at2026-10-10T08:21:31Z, commit
`c9fb729df68d305f1bf7094a74fa117fba96e1e1`. Reviewed source
`a95b61e489ff9d26f8d7774074655359e2da5f31` passed both observed owning Nix
runs38037124854/38037094384, CodeQL38037124882/summary, and
advisories/licenses38037124888. Coordinator source/proof review is a truthful
COMMENTED review; no independent rerun or formal self-approval is asserted.
Three existing owner paths: production rustdoc/text +6/-4/net2; tests
+24/-10/net14; guidance +54/-0/net54; zero public type/dependency/lock change.
Only the diagnostic prerequisite is delivered; the registry version remains
unselected until publication is observed.

Automatic release-plz produced [release PR10](https://github.com/devrandom-labs/bombay-communication/pull/10),
head `6b6d588e5ea94971590d03ccf063c5b38d07d107`, proposing0.1.4. Before its
merge, automatically select the recommended normal owner patch release and
Bombay minimum0.1.4 with a narrow locked update after actual publication.
Alternatives are retaining known misleading diagnostics or a downstream
Git/patch/source copy; they leave a correctness defect or complicate ordinary
installation unnecessarily. The reviewed bot proposal changes three existing
owner paths +11/-5/net6: workspace version, four workspace package versions
inside Cargo.lock, and changelog. External versions/checksums, production source,
public APIs, dependencies and pinned toolchain remain unchanged; only the
existing communication package publishes, support packages stay unpublished.
No manual version bump or release workflow dispatch/rerun is selected.

Bombay's subsequent coordinator-owned manifest/lock edit is bounded to two
existing paths, no new package family, and the one corrected registry node.
Prove the actual archive checksum/source and unchanged remaining graph before
integration. Existing native controls/inversions keep the original lock until
their campaign completes. Required release-head CI/review, real publication
and combined-root verification remain mandatory; a successful release workflow
alone does not establish a published version. Existing cumulative ceilings
apply unchanged and all owner/release paths count in the retained-source union.

Separate SHM readiness source inspection confirms encoded-byte optimization,
not elimination of JSON encode/decode or local typed Rust messages. Native
segment compatibility/access is not KERI identity or actor permission; mapped
bytes need stability throughout verification/decoding. Watchdog reclamation,
handoff queues, crash retention, complete count/byte bounds and identical TLS
fallback still require real subprocess proof. Current published TLS-only copies
remain unchanged; this is the separately approved later-inclusion research,
not current feature acceptance or a measured performance claim.

Actual release PR10 merged at2026-10-10T08:30:59Z, commit
`77bed2813e4d08af57457779152d767ddbbf69a0`, after coordinator review and all
observed Nix/CodeQL/advisory checks passed on6b6d588. Release workflow38038147886
passed. More strongly, crates.io and sparse-index metadata independently show
0.1.4 published at2026-10-10T08:31:28Z, unyanked, Rust minimum1.95.0. Actual
downloaded archive checksum equals registry/index checksum
`d500dd06337251a02642e8170010fcf64d3c2dcf2a605397ae0abcbfbcbe541f`;
archive VCS source is the reviewed release head6b6d588. Its lib.rs is byte-equal
to the reviewed correction, SHA
`003b01d35f0589a799ebcf24dde53637b8de0fe3f7423dd542e18f0a8928870e`.
The initial API read lacked an acceptable User-Agent and returned403; the
correct identified client read succeeded. That response was not absence or
publication proof. Registry JSON/index, actual archive and release log are
retained under `/tmp/bombay-communication-0.1.4*` and the registry-index record.

The coordinator applied the selected minimum0.1.4 and pinned-Nix narrow
`cargo update -p bombay-communication --precise 0.1.4`. Only that registry
node's version/checksum changed; all other178 complete package records and
the communication dependency contract are unchanged. New lock SHA
`aa2b8c7f9b4fb917976a13167718b6cb462ec01420ea426578ae5951f7cc5bab`.
Graph proof `/tmp/bombay-communication-narrow-lock-proof.json`; no Git/path
override or new family. Behavior/Actors, Address, Observe, Timers, their patches
and selected complete Behavior instructions stay unchanged. The native32-event
campaign belongs to the original0.1.3 lock; restored combined controls and all
required checks must now execute on this actual corrected registry selection.

### Exact-recipient admission proof and minimization checkpoint

The isolated branch `feat/static-remote-admission` starts at latest main
`017ec7603e0193e26129e0af6d10b8c04fdad78e` before its first commit.
The original-lock32-event campaign passes three actual public Application
controls and one owning Entity projection control in debug and optimized builds.
Five source inversions independently misclassify Standard Full/Closed, replace
the actual sender origin, or misclassify Entity Full/Closed; every inversion
fails its intended observable assertion in both profiles, then byte-exact
restoration passes. Adding an unnecessary static message bound independently
rejects the actual borrowed-message caller with E0597 in both profiles. This
is compile-time evidence, not a runtime execution. Campaign commands, results,
source custody and binary identities are retained in
`/tmp/bombay-exact-native-admission-campaign.json`.

After the narrow registry selection, the actual three public controls pass
in each profile against Communication0.1.4. An initial Entity filter
`nonwaiting_entity` matched zero tests; those exit0 commands receive no
witness credit. Correcting it to the actual owning function
`exact_entity_endpoint_keeps_full_and_closed_originals_and_ordered_prefix`
executes one control in each profile and passes. Registry control logs and
explicit executed counts are retained in
`/tmp/bombay-exact-admission-registry-entity-controls.json` and the registry
public-control logs. No regression is inferred merely from a command exit.

Independent nonauthor source/minimization review finds no production blocker.
The operation projects the existing mailbox refusal sum without changing
its ownership or synchronization. No new state, result alternatives,
transition authority, modules, wrappers, traits or defined public types are
introduced; two existing endpoint alternatives each interpret the same
operation. The caller needs only an exact recipient and original message,
not an endpoint path or alternate runtime. The application-owned service
consumer is concrete, and the borrowed-message control denies an unnecessary
static restriction. The Entity Fence refusal branch is unreachable solely
because Communication returns the exact submitted Message ingress; it is
not a claim that Fence itself is uninhabited.

Aggregate-drift disposition: pass for this local projection. Actor control
states and subordinate sums are unchanged; existing Full/Closed alternatives
retain their original message for the caller's next decision. No arrival
history, duplicated cause, semantic boolean, nested actor authority or
structural user syntax is added. The pure Behavior boundary and runtime
capability/module contracts are unchanged. Production grows by57 net lines
across three existing source paths; public surface grows by one method and
one deliberate native-error re-export. This is a new local capability, not
code reduction. Historical0.1.3 proofs retain their exact selections; current
README, API audit, capability table and backlog evidence use0.1.4.

Combined workspace build/tests/docs, formatting and strict all-target Clippy
are running on the restored current source and registry lock. Required CI,
final reviewed head and actual PR merge remain outstanding. No protected
remote admission, authentication, expiry/revocation retry, remote replay ID,
transport fault campaign or full R01–R25 gate is established by these local
controls.

Strict all-target Clippy rejected four new test-only wildcard refusal arms.
Select exhaustive Full and Closed matches with separate diagnostic reasons,
preserving the already selected native sum and non-Debug payload support.
No production shape, dependency or warning suppression changes; this is
assertion specificity inside the existing320-line test envelope. Rerun the
affected public controls in both profiles, formatting and strict Clippy.

The subsequent complete lint traversal also rejects similar `received` and
`receiver` names in the owning Entity control. Name the obtained value
`accepted_ingress`, which states its domain role; no semantic or production
change. Recheck that owning control in both profiles and rerun strict lint.

### Local admission final verification and delivery readiness

The restored source passes all required local commands through Bombay's
pinned Nix shell, with the actual registry0.1.4 lock, symbols0, incremental0
and two build jobs:

- `cargo build --locked --workspace --jobs 2`: exit0.
- `cargo test --locked --workspace --jobs 2`: exit0,559 passed including
  documentation, one existing parent-controlled ignored law.
- `cargo test --locked --workspace --all-targets --jobs 2`: exit0,558 passed,
  the same one ignored law; benchmark targets also complete.
- `cargo fmt --all -- --check`: exit0 after final assertion/name edits.
- `cargo clippy --locked --workspace --all-targets --jobs 2 -- -D warnings`:
  exit0 after exhaustive test refusals and domain naming, with no allowances.
- `cargo nextest run --locked --workspace --jobs 2`: exit0,558 passed across57
  binaries, one skipped. This separately verifies the final exact test source.

The affected public controls pass again after their exhaustive-match edits:
`cargo test --locked -p bombay-rs --test actor_interface external_actor_
--jobs 2`, and its `--release` counterpart, execute three each. The owning
Entity control passes again after its binding rename in both profiles, one
executed each. Combined/final/restored command receipts and logs are retained
under `/tmp/bombay-exact-admission-{combined,final,restored}-checks.json` and
the matching per-command logs. Production is unchanged by these test fixes.

Independent nonauthor final source/ownership review approves this scoped
capability and its current documentation. The reviewer inspected source and
evidence, without independently rerunning Cargo. The stage is locally
feature-complete and distilled: existing concrete primitives express the law,
no retained competing abstraction or semantic owner was found. This applies
only to exact-recipient local admission. Required final-head CI—including the
authoritative Nix flake checks and owning law/fuzz gates—review and actual
merge remain necessary before recording this prerequisite as merged.

Final source/API checkpoint: production +59/-2/net57; tests +293/-16/net277;
manifest/lock +3/-3/net0; newly defined public types +0/-0, one public service
method and one exposed existing-owner error name. Complete root cumulative
production +1651/-474/net1177 and tests +4722/-652/net4070 remain below the
recorded2200/4300 ceilings. The tracked-and-untracked count and source hashes
are recorded in `/tmp/bombay-exact-admission-stage-checkpoint.json` and
`/tmp/bombay-exact-admission-cumulative-checkpoint.json` before commit; documentation
is counted in those complete records as well. Cross-repository owning source
and release changes remain separately accounted in their delivery records;
this root recount does not invent an exact historical global-path union.

Complete stage:13 tracked paths, zero untracked; documentation +384/-11/net373.
Complete root cumulative task:60 tracked paths, zero untracked; documentation
+2784/-79/net2705. No production/test count is replaced by a document count.

### Parallel research checkpoint: actual local permission and transport custody

Prepared while PR331 final-head CI runs, in the isolated
`research/static-remote-contracts` branch created from latest main017ec760
before any new commit. The coordinator fast-forwards that research branch to
exact locally verified native candidate49254c3 for its public-API experiments;
this is research against an unmerged candidate, not a claim of delivered API.
The submitted PR331 tree remains frozen. The research lock selects actual
registry Communication0.1.4; complete selected Behavior instructions, owning
primitives and normative runtime/Driver documents remain unchanged.

Automatically select the recommended ordinary-Rust local experiment: two
independent provider functions through the existing Application Work/service
and typed Actions result path, before introducing a provider trait or runtime
attachment. Compare fixture lookup with explicitly scheduled typed provider
transactions using the already locked Tokio oneshot. Provider observations
remain Accepted, Invalid, Stale or Unavailable; a label alone cannot issue
authenticated authority. These are deterministic fixture providers, without
KERI, cryptographic or production freshness credit. Immutable verified-byte
evidence and current permission/freshness usability remain separate concerns;
a fresh local check does not necessarily rerun cryptography. Cache usability
during a provider partition remains a production policy decision.

Use controlled permission/time inputs only in application-owned service code.
Prototype one directly borrowed owner, then compare a single owning service
event loop with a short guarded state using standard Mutex or the already
locked parking_lot0.12 if shared synchronous callers genuinely require it.
No new synchronization dependency is selected. All fixture revocation,
rotation and time updates must go through the same owner. The permission
check and exact nonwaiting mailbox attempt contain no await; relinquish the
owner before any provider/capacity wait. Plain functions and exclusive Rust
ownership are the recommended first comparison because they add no runtime
coordination state. Distinct atomics/watches or an earlier verdict followed
by an awaited send cannot substitute for a coherent operation. Production
clock/skew/freshness rules and coordination API are not inferred from fixtures.

The smallest actual-Application regression uses current-thread Work to fill
the real root mailbox without yielding, observes actual Full, and retains the
same protected move-only allocation. Commit revocation or advance controlled
fixture time beyond expiry before releasing capacity; each retry makes a
fresh local check. A still-granted control admits the original once. Refused
controls retain the identical allocation and have zero protected actor effects.
An ordinary completion command receives its Actions-emitted processing receipt
before stopping; the complete native root and both notification receipts
conserve the accepted prefix and protected effect count. Restore the original
precheck-plus-awaited-send defect and require the intended effect-count
assertion to fail after cleanup, in debug and optimized builds. Do not copy
implementation branch labels into the oracle, or claim a full R08 witness
from this narrower local admission/identity substitution experiment.

In parallel, prepare an actual subprocess transport comparison against the
exact nine published dependency copies, using existing TLS fixtures and
configured mutual TLS/private roots in peer and router/two-client layouts.
Compare query/reply with explicit None consolidation and scoped Querier to
publish/subscribe with typed test receipts. Native ingress ACL can refuse
Query/DeclareQueryable before query routing; native low-pass filtering bounds
payload plus attachment bytes through checked addition. Neither bounds all
declarations, selector/header bytes or outstanding allowed queries. Source
inspection finds native per-route pending-query maps and timeout tasks with
no demonstrated count ceiling or supplied-timeout clamp. Therefore retaining
native query routing needs an owning limit contract or a proved existing
mechanism; local admission limits/querier drop do not bound router retention.
Do not select an owner patch or production command mechanism without real
subprocess/configuration/cancellation/custody evidence. This comparison can
reuse owning primitives rather than inventing a transport guard or codec.

Research change record before fixture implementation: no production changes,
no new defined public types or production dependency/API/default. Maximum
1400 research test/fixture lines in at most four new source paths: one local
public-Application experiment, one transport executable source, one subprocess
coordinator, and one exact evidence/experiment record. Existing source/manifest
paths and supplied certificate fixtures are reused where possible. Coordinator
owns manifests, lock, shared contracts and integration; subagents may author
disjoint owning test/probe paths, with one serialized local Cargo lane.
Automatically select a bounded cumulative test/fixture ceiling5500, replacing
4300 before this experiment: current root4070 plus at most1400 fits5470.
Production ceiling2200 and retained-source ceiling300 stay unchanged; all
handwritten cross-repository research source counts, with exact local deltas
and conservative historical global-path accounting retained. These bounds are
review checkpoints, not runtime resource policies. Real Linux subprocess,
cryptographic identity, full resource, replay and R01–R25 gates remain open.

Transport fixture contract selected before retaining its executable source:
closed WorkerRole alternatives Router/Recipient/Caller; ConnectionLayout
alternatives Peer/RouterClient; TransportOperation alternatives Query/Publication.
The subprocess coordinator supplies role/layout/operation, explicit TLS address
and the existing certificate directory. Use Serde/serde_json for typed control
commands and observation lines, rather than a handwritten framing/codec.
Control commands Run/Cancel/Release/Exit and events Ready/PotentiallyTransmitted/
Received/Receipt/WaitEnded/Closed are research observations only. They do not
issue authenticated authority, actor admission, replay identity or shutdown
completion. A literal JSON request with different whitespace/field order
provides an independent exact-byte preservation oracle. A typed test receipt
carries that original text and a finite observation count; the caller compares
its bytes, not a reconstructed equivalent JSON object.

The coordinator can release the recipient's retained query/publication receipt
after cancelling a caller wait; a cancelled wait cannot establish nonexecution.
Mark possible transmission before entering foreign into_future/wait logic.
Explicit native fixture channel capacity4 and query timeout5seconds are
experimental settings, not production defaults or a proof of complete bounds.
Use stable scoped Querier with None consolidation and matching-query reply
selection, or existing subscribers/publications; no production operation is
chosen. Every acquired declaration/session retains its explicit close path.
Add direct access to the already selected Tokio1.53.1 family in the isolated
registry consumer only, with its existing runtime/time features. Alternatives
are handwritten scheduling or a new async dependency; neither is justified.
Prove that its narrow lock operation preserves every existing package record.
The standalone manifest remains coordinator-owned; no Bombay production
manifest, crate default, certificate lifetime or algorithm changes.

Preserve the delivered registry consumer as original evidence: the transport
experiment instead uses `/tmp/bombay-zenoh-transport-custody`, with exact copied
manifest/lock inputs and one research package rename/direct existing Tokio
edge. The original consumer manifest is byte-restored before any Cargo command;
its lock and source remain untouched. The experiment's manifest and copied
lock are two additional counted fixture paths; maximum new retained paths6
replaces4, without changing the300-path cumulative ceiling. Its generated lock
is separately accounted as an exact reused artifact plus narrow graph delta.
No external package version may change during this research-only root update.

Transport controller also has Wait: Run invokes native get/put then retains
the pending receiver, allowing Cancel before a separate Wait. Cancelling
undeclares the owned Querier or receipt subscriber, without inferring remote
nonexecution. Prefer native stable matching-status readiness under the explicit
fixture deadline to blind propagation sleeps. Recipient Release consumes its
native request, observes the original exact text, then publishes the fixture
receipt. Native FifoChannel capacity4 is a comparison setting, not a new queue.

Local fixture profile selected before retaining source: controlled now9 and
deadline10, with expiry when now is at or beyond deadline; fixture revision3
and marker71 distinguish accepted lookup/scheduled observations. The marker
has no cryptographic meaning and must not be called a signature or KERI proof.
The exclusively borrowed PermissionAdmission owns Granted/Revoked and current
fixture time. Lookup consults its finite fixture table; a scheduled provider
receives an actual typed Tokio oneshot transaction and validates its marker
and revision, preserving actual receipt closure as Unavailable. Invalid,
Stale and Unavailable stay distinct and retain the original command. Both
providers exercise granted/revoked/expired/invalid/stale/unavailable controls.
Actual Actions-emitted snapshots and final native actor counts are independent
of provider verdict labels. The old precheck-plus-awaited-send inversion must
fail the protected effect-count assertion after complete cleanup in both
profiles. No production expiry/skew/cache rule is selected from these numbers.

Keep the actual provider result owned beside the final local policy rejection.
The permission attempt borrows that result and returns the exact command with
its distinct usability/refusal classification; it cannot erase a scheduled
provider's original RecvError into an unavailable label. Valid evidence first
observes actual mailbox Full; invalid/stale/unavailable evidence refuses before
insertion while that same real mailbox is full. Processing snapshots establish
accepted-prefix progress before retry, and complete native counts are checked
after cleanup. This shares facts by observation without duplicating their cause.

Use truthful transport fixture milestones InvocationReturned and
SessionCloseReturned; replace the proposed Closed event rather than suggesting
full callback/resource retirement. A native close return is narrower than the
required owning cleanup proof. Matching readiness proves declarations only,
not identity. Dedicated subprocess stdin control may block its own control
thread; it must not run inside a Behavior fold or native networking callback.

Formatted-source checkpoint before expansion: local draft252 handwritten lines
projects486 under owning rustfmt, and transport draft346 projects up to600.
Retaining the full native custody oracle and real provider-result consumer
requires a larger per-file estimate, rather than compressed Rust or omitted
assertions. Automatically select local650 and transport600 formatted-line
ceilings, subprocess coordinator150, combined1400; cumulative test ceiling5500
and source-path ceiling300 remain unchanged. No production/public/dependency
expansion follows from this formatting estimate. Minimize by truthful ownership,
not by removing evidence to satisfy a line target.

The local result consumer must be an actual typed Behavior input supplied by a
required ordinary function parameter shared by both provider bindings. Its
private verification-observation sum preserves Accepted/Invalid/Stale/Unavailable
as a derived view, while Work keeps the original native provider cause. The
actor returns a useful typed Actions processing receipt after consuming it,
not a no-op command. An explicit named private processing-receipt product
carries accepted-prefix and protected-command counts, avoiding positional
count ambiguity. These are fixture-only semantic products, not public
authority or runtime contracts. Before filling capacity, await that actual
processing receipt so verification notification does not steal a prefix slot.
Compare complete final native settlements against the independently specified
input/processing trace, including that additional genuine verification turn.

Retain the public actor address acquired from the actual Application in the
observation and compare it with native RootOrigin; do not predict it from
MailAddr constants or allocation arithmetic. Delete the draft's forwarding
async retry function: a direct fresh owner attempt already expresses the law,
and the original-defect inversion can replace that actual service call site.
No function is retained solely as an inversion seam. Both changes preserve
the same authority law rather than introducing another policy or abstraction.

Transport source review before execution found the draft control parser
matching raw uppercase words, contrary to the selected Serde control path.
Use serde_json to decode the closed control-command sum (JSON strings
"RUN"/"WAIT"/"CANCEL"/"RELEASE"/"EXIT"); the controller encodes them with its
standard JSON library. Also preserve worker outcome and native session-close
result as two named coexisting native receipts through the terminal fixture
consumer. Returning only the close error would erase an earlier worker error.
The fixture's terminal consumer explicitly observes/discharges both originals
after the close attempt, with failure exit if either fails; this is neither
a new production error API nor proof of full Zenoh retirement.

The concrete transport vector contains amount9007199254740993 and the fixture
marker in reordered/whitespace-varied JSON. It exercises exact text and a
large integer without normalization. QueryTarget::All is only a finite
comparison configuration with one recipient, not an at-most-one authorized
production targeting decision. Connection timeout2seconds and runtime2threads
are explicit fixture settings. Their actual callback/resource/liveness limits
remain unproved, and no numeric setting receives production-default credit.

First actual local debug controls reach complete native cleanup, then fail
an incorrect fixture oracle: the selected source discharges consumed earlier
settlements under D-SETTLE1 and retains one final settlement, not a turn-history
collection. This is an oracle error, not a semantic inverse or passing
acceptance. Correct it to the complete retained Stop/empty-send/empty-creation
settlement product. Earlier turns are proved by the independent ordered actor
trace and all actual typed processing receipts; close and drain the service
receive lane to establish no extra replies. No runtime retention contract or
architecture changes. Existing BehaviorBase/NoSends module imports are ordinary
source requirements, not newly invented interfaces. Final custody and complete
actor/notification assertions remain. Record both failed setup commands and
restored control results honestly before claiming the experiment passes.

For that fixture terminal consumer, recommend a private named product of the
two native Results implementing Rust's existing std::process::Termination,
so ordinary main returns the preserved product through Result. Alternatives
are a duplicate boxed aggregate error or dropping one original to fit the
existing unit Result; neither is needed. The existing standard trait reports
both results after the actual close attempt, attempts both observations before
selecting exit status, then explicitly consumes/discharges the two originals.
Notification failure must produce failure exit too. This is a private fixture
terminal policy, not a new trait, public product or runtime lifecycle owner.

Narrow the isolated transport manifest to its actual consumers: serde,
serde_json, Tokio and the top-level published Zenoh copy. The copied generator/
config/link aliases and maintainer feature declarations belonged to the
delivered configuration-consumer test, not this executable; remove them.
Select the already approved explicit TLS profile with defaults disabled.
Owning transitive packages still resolve through the published graph, with
no new family or external version selection. Preserve the original consumer
as evidence and compare its complete external lock records after the update.

The research root lock update selects no new external package or version.
Removing inherited maintainer options prunes five unused TOML-feature packages
and the corresponding optional Config-to-TOML edge. Two surviving TOML
records shorten dependency labels after duplicate versions disappear; their
resolved edges remain identical. The initial name-only record comparison
caught these textual differences and stopped before formatting/compilation;
it cannot distinguish duplicate package versions. Replace that invalid check
with exact (name,version,source) identities, checksums and normalized resolved
edges. Admit only the deliberate unused-feature prune, preserving every
remaining external version/checksum. This is not a general lock refresh.

### Exact-recipient local admission delivered

[PR331](https://github.com/devrandom-labs/bombay/pull/331) merged normally at
2026-10-10T09:45:37Z, commit
`9fc225ee0e96e697f0b48b619abe6ea8c6721e7c`, matching reviewed head
`49254c3d4114eb1ce8661ab61adae951f52a7784`. Required Nix run38039822580
passed at09:42:14Z, including the authoritative flake checks, bounded Driver
and Observe fuzz campaigns and their retained artifacts. CodeQL analysis
38039822624/summary and Deny38039822614 also passed. Independent nonauthor
source/evidence review is recorded in the truthful coordinator
[COMMENTED review](https://github.com/devrandom-labs/bombay/pull/331#pullrequestreview-5478399384);
no formal self-approval, independent Cargo rerun or protection bypass is claimed.
Ruleset18433270 required the normal PR and passing Nix check; all four observed
checks passed on the exact head. Full CI log remains
`/tmp/bombay-exact-admission-final-ci.log`. This prerequisite is now merged.
AUTH1/NET1 and all full R01–R25 obligations retain their incomplete status.

### Actual local permission research evidence

The final owning integration file permission_admission.rs is550 formatted test
lines, zero production/API/dependency changes, source SHA
`a2c112be25dfa979ed332ce5802b7c8f320c9502e1c5790260b955c93f08a69f`.
Both independent fixture providers pass six actual Application cases each
in debug and optimized profiles. Distinct revoked and expired inversions
restore the old cached precheck plus awaited send at the actual service call
site; each fails for both providers at the intended after-cleanup actor effect
count1 rather than0, in both profiles. Byte-exact restoration passes. The
semantic campaign source was e23463bf6f054cb7bafcd56cbd56ebe542c15d99e5511cc12bb4ec61954e4809;
two subsequent style-only edits have separate final positive controls and
format/strict target Clippy success, without claiming renewed inversions on
the later hash. Exact commands, native original custody, source/lock/binary
identities and setup failures remain in `/tmp/bombay-permission-campaign.json`.
Independent source/ownership review passes without claiming a separate rerun.

Accepted-prefix order and truthful origin, identical protected allocation,
real provider-result Actions acknowledgement before capacity filling, snapshot
processing receipts and service exhaustion, final retained settlement, complete
root native results and both notification receipts are asserted. The scheduled
provider is an immediate real oneshot handoff; it is not a timed scheduling,
pending-provider or cancellation proof. Its actual RecvError stays owned
outside Application cleanup; fixture lookup invents no native cause. Fixed
now9/deadline10 and marker/revision values do not select production clocks,
cache freshness, identity proof or concurrent shared-state coordination. No
static authority-construction denial, op correlation, retry/resource bound,
transport or full AUTH1/R08 acceptance is inferred.

### Initial native TLS subprocess observations

Exact published-package transport executable compiles through pinned Nix,
with defaults disabled/TLS enabled, source546 formatted lines. All eight
initial debug campaigns pass: receipt and cancelled-wait cases, query and
publication, peer and router/two-client layouts. Processes have separate
address spaces and configured mTLS/private roots, discovery disabled. Both
request and receipt preserve the literal JSON bytes including whitespace,
field order and integer9007199254740993. After caller cancellation, the
recipient still obtains the transmitted request and invokes its reply. Native
invocation/close milestones are recorded without claiming actor admission,
full callback retirement or remote nonexecution. Exact worker argv, binary
hash, event traces and exit statuses are in
`/tmp/bombay-transport-custody-evidence/campaigns.json` with per-case stderr.

Strict source-target Clippy finds two fixture-only issues: a redundant Ok/?
return and a router control loop that executes at most once. Select direct
return of the existing Config result and a single standard iterator next,
with exhaustive control-command matching. No policy, dependency, warning
allowance or production changes; rerun the eight affected controls after
formatting, then strict lint. Optimized, adverse configuration, pressure,
restart and semantic-inversion campaigns remain outstanding. These transport
observations do not close R17/R20/R23 or select a production command mechanism.

### Fresh native resource and shutdown source audit

The published Zenoh1.10.1 TLS graph has configured session limits and queue
batch inventories, but these do not prove an aggregate retained-resource
ceiling. Native ACL passes DeclareKeyExpr before owning registration and
low-pass passes declarations. Ordinary wire expression IDs/suffix lengths
are u16; expanded prefix-plus-suffix names have no found configured quota,
and Resource stores full expressions along its retained tree. RX pool
exhaustion falls back to allocation, so buffer_size is an inventory rather
than a hard retained-byte limit. Decode bounds available bytes before vector
allocation, but occurs before ingress filtering. Eight receive priorities
each have reliable/best-effort reassembly; their per-channel configured bound
requires aggregate accounting. Source audit identifies obligations, not a
measured end-to-end resource proof or selected production correction. Owning
pressure assertions must precede new limits, interceptors or patches.

Existing ShutdownEstablished carries exact established actor authority and
correlation through typed Actions; InstalledActor publicly exposes its joined
retirement observation. Investigate the ordinary application-owned service
as existing InterpretInstalledActor consumer before retaining a new accessor
or stop API. A queued parent request still does not prove fresh authorization
at its later child-control attempt. Root/child mechanics and fresh remote
permission remain distinct required witnesses. Prepare the smallest genuine
public Application compile experiment, with no new production abstractions.

For the current native transport fixture, automatically select an additional
negative control: an invalid caller RELEASE must retain its worker failure,
attempt Session.close, publish the distinct close milestone and exit failure.
This uses existing role/control sums and native-result terminal policy. It
adds no injected network failure, production policy or dependency. Add a
separate exact-byte inversion by simulating decode/re-encode of the approved
JSON; the independent literal-byte assertion must fail in both profiles,
then byte-exact restoration must pass. Cancellation evidence remains limited
to explicit local declaration relinquishment and observed later recipient
execution, not remote cancellation or full resource retirement.

### Portable evidence and parallel child-authoring checkpoint

Automatically select retaining the already exercised transport fixture and
its seven existing test certificate/key files beside this PRD. Their keys are
public fixture credentials, not deployment identities. The alternative is
leaving only temporary absolute-path probes; that prevents reviewers from
reproducing the evidence. Require explicit certificate and evidence-directory
arguments in the Python controller instead of embedded /tmp locations. Copy
existing bytes, preserving provenance and hashes; no regeneration, production
trust roots or TLS-policy change is implied. These fixtures expire in January
2027 and do not select a deployment certificate-rotation policy.

This research stage expands to at most16 new source/manifest/fixture paths,
including one local admission test, the standalone transport manifest/lock,
Rust executable, Python observer, seven certificate files and one meaningful
child-authoring probe. The certificate files add167 fixture lines. Current
local+transport+observer source is1229 lines; with certificates1396. Reserve
up to430 lines for the independent ordinary-Rust child-authoring experiment
by automatically expanding the research ceiling1400 to1900 and cumulative
test/fixture ceiling5500 to6000 (existing4070 plus1900 is5970). Production
ceiling2200, retained-source path ceiling300 and zero new public types remain
unchanged. Generated lock bytes remain separately counted manifest/artifact
material; copying them cannot erase their review cost. Research/test evidence
may be delivered separately from any later production correction. A source
compile failure is evidence of its specific gap, never positive feature proof.

### Owning declaration-pressure research authorization

Automatically select the prepared isolated owning-test alternative: extend
Zenoh net/tests/tables.rs with a real TLS producer subprocess and borrowed
private routing-table snapshots, reusing the existing standard test runner.
This avoids a new public instrumentation API, binary target or production
interceptor. The receiver owns the oracle: receiving peer for peer layout,
router for router/client layout. Existing fixture credentials and native ACL
with publication keys restricted are reused. Retain eight then sixteen native
declarations outside the allowed namespace and observe exact remote mappings
and unique namespace nodes/UTF-8 expression lengths. String capacity is only
its allocation inventory, never total heap. Borrow under the real table lock;
retained Arc clones would distort cleanup. Collect the producer exit and local
session-close outcome even when the semantic test fails.

Forecast one existing owning test path, up to300 formatted test lines, no
production/public API/manifest/dependency edits. Omitted declaration must fail
the growth oracle; omitted undeclaration must fail restoration. These are
sensitivity inversions for a measured blocker, not enforcement or full R22
acceptance. Automatically expand the research ceiling1900 to2200 and cumulative
test/fixture ceiling6000 to6300 before this parallel test: root4070 plus2200
is6270. Existing production2200/path300 ceilings remain; no resource quota
or handwritten transport replacement is selected. Whole-heap/RX/reassembly/
interest/query/accept-concurrency obligations remain separate and unresolved.

### Parallel Linux transport evidence before CI edits

Automatically select two independent Linux CI jobs, debug and optimized, for
the retained standalone native TLS fixture. Reuse the existing pinned checkout,
Nix-install and artifact actions; run all Cargo and subprocess commands through
the repository's pinned Nix. The alternative is preserving only manual macOS
observations, which leaves the Linux campaign unreproducible in normal review.
Both profiles run the same twelve existing controls and retain complete JSON
worker traces/stderr as artifacts. They add no actor/identity acceptance or
production runtime, do not replace the required Nix check, and cannot close
R17/R20/R23. Keep the original full-source required checks unchanged.

Change record: one existing workflow path, at most80 new configuration lines,
zero Rust production/API/dependency/lock changes, no new actions or runtime
limits. Reuse the existing fixed published dependency graph and fixture
credentials. Treat Linux results as unexecuted until these jobs actually pass
on the reviewed source head. Recorded production2200 and path300 ceilings
remain; workflow lines count as configuration production in full accounting.

### Independent subprocess-oracle correction before retention

Independent source review found close() skipped milestone/exit validation when
a child had already exited. Existing passing traces did include both facts,
but this branch could wrongly accept a later crashed worker. Automatically
select requiring a live worker followed by its real close milestone and exit0.
The deliberately rejected caller is excluded only after separately observing
its worker failure, close milestone and actual exit1. This reuses the existing
case sum and avoids another state wrapper. Preserve native process outcomes
in every record. Simulate an actual late caller exit after its last successful
TLS event; corrected close must refuse it in debug/optimized campaigns, and
the original skip branch must fail that refusal oracle. Existing twelve-case
positive controls must pass again. No production/dependency/profile changes.

### Child result-oracle formatting checkpoint

The ordinary nominal child-service mechanism executes, but its initial native
result was explicitly discharged without complete inspection. Adding exhaustive
native actor/sibling/root products, both notification receipts and borrowed-wait
cancellation formats to478 test lines, exceeding the430 forecast. Automatically
select a500-line ceiling for this same single research source before further
edits, preserving the required observations. Alternatives are dropping custody
assertions or forced compact formatting; neither improves correctness or
reviewability. No production, public types, dependency or new source path.
Current1398 local/transport/certificate fixture lines plus500 child and300
pressure lines fit2198 within research2200/cumulative6300. The expanded draft
has not compiled; only its earlier mechanism control has execution credit.

### Retained late-exit oracle and final research reserve

Retain the real late-exit control in the existing observer, rather than only
a temporary command. After the actual receipt, kill and join the caller, then
require close() to refuse that already-dead worker. Preserve the native-9
exit and full events; distinguish this expected refusal from healthy controls
and require its exact one failure, so unrelated errors cannot pass. This adds
four cases per profile without Rust/manifest/profile changes. On injected
crashes the fixture may abandon remaining workers; their-9 exits are retained
without close/retirement credit. Returned worker errors remain independently
covered by the prior close-and-exit1 controls. Neither case proves cleanup on
an arbitrary panic or cancellation of a pending receive future.

Automatically reserve research2300/cumulative6400 test lines before this
retained oracle edit, replacing2200/6300. Current ordinary sources/fixtures
plus500 child and300 pressure forecasts exceed2200 by only the added observer
lines; the larger bounded reserve avoids omitting meaningful failure tests.
Python observer remains below150 lines. Production2200, path300, source-path16
and zero defined public types remain. Measure actual deltas before delivery.

### Retained transport source verification and reproduction

Retained Rust source SHA ce440e955a0e4428b1b1e2f9a75d0f4963f7bf3fc0be9af6277fea98a000de19
passes pinned-Nix format/strict binary Clippy and both native build profiles.
Controller SHA aa0fa98a46f92ab2c791d1d7225d79bf4344b75f7c3cfd90b30747adcff9f147
is143 lines and now runs sixteen cases per profile: twelve healthy/returned-
error/cancellation controls plus four actual late-exit refusal controls. All
restored cases pass their distinct oracles. Restoring the old skip branch
makes both campaigns fail specifically because late-exit records contain no
refusal, while all twelve unrelated controls still pass. Source restoration
is byte exact; `/tmp/bombay-transport-late-exit-inversion.json` retains exact
argv/binary/controller identities, traces and command exits.

Exact-byte inversions simulate JSON decode/re-encode only at the native send
call sites. Eight controls per profile fail the receiver's exact-byte assertion
at source line243; four returned-error controls remain healthy. The first
harness bookkeeping expected stale line253 and interrupted its audit; the
source was restored, actual assertion identity rechecked, optimized inverse
executed, and both restored twelve-case controls passed. That bookkeeping
failure has no semantic acceptance credit. Full command/oracle records remain
in `/tmp/bombay-transport-custody-byte-inversion.json`. Subsequent observer-only
changes did not mutate Rust bytes or broaden those source inversions.

From the checked-out repository root, reproduce the retained debug controls:

```sh
nix develop -c cargo build --manifest-path docs/prds/static-remote-actors/transport-custody/Cargo.toml --locked --bin transport_custody --jobs 2
nix develop -c python3 docs/prds/static-remote-actors/transport-custody/verify_transport_custody.py docs/prds/static-remote-actors/transport-custody/target/debug/transport_custody docs/prds/static-remote-actors/transport-custody/certificates /tmp/bombay-native-tls-debug-observations
```

For optimized controls add `--release` and use the corresponding target/release
executable and a separate evidence directory. A preexisting CARGO_TARGET_DIR
changes the executable location. Root workspace tests do not invoke this
standalone manifest; the new separate Linux jobs explicitly do. Those jobs
remain unexecuted until CI passes. Neither local nor future CI campaigns prove
actor admission, authenticated proofs, complete cleanup or full remote gates.

### Actual declaration-pressure assumption falsifier and owning alternative

The first real TLS producer control completed its public declarations and
cleanup, with child exit0 and native session close success, but the receiving
peer retained zero remote mappings. Source inspection explains the result:
public Session.declare_keyexpr registers its local face without independently
forwarding a declaration. This falsifies the initial API assumption, not the
resource gap, and has no remote-growth or boundedness credit. Preserve its
command and actual complete outcomes before changing the fixture.

Automatically select the existing owning TransportUnicast.schedule alternative
for this pressure witness: obtain the established native transport, construct
its existing typed DeclareKeyExpr/UndeclareKeyExpr protocol values, and schedule
them through the native TLS encoder/decoder/ACL/resource path. The alternative
uses public subscriptions/publications plus extra permissions/interests and
changes the baseline; it obscures the precise ingress resource law. This is
no handwritten encoding, production transport or interceptor. Explicitly
match native Ok(true)/Ok(false); successful queue insertion is not remote
admission. Retain every scheduling and cleanup result and always close/join.
Use sixteen isolated fixture ExprIds40000..40015, not actor identities or
predicted incarnation nonces. The narrowed claim is a node possessing fixture
TLS credentials can emit valid native declarations outside allowed publication
keys; it is not bare public declaration transmission. Forecast remains one
existing owning test path, at most300 formatted lines; zero production/API/
manifest/dependency edits. Stop and account if this estimate is exceeded.

### Owning pressure receipt and ordinary fold lint checkpoint

Preserving all native connection-acquisition, scheduling and undeclaration
receipts projects the single pressure test to310–315 formatted lines.
Automatically raise its same-source ceiling300 to325 before retention;
current root research1896 plus325 is2221 within2300 and cumulative6291 within
6400. No new source path, production/API/manifest/dependency changes. Preserve
the earlier zero-growth public-API source as a derived diagnostic patch,
separately accounted evidence rather than another selected implementation.

Root strict Clippy rejects three unnecessary private Result wrappers in the
child probe, the intentionally complete native enum's size/type, its contiguous
trace length and two missing assertion semicolons. Automatically select
inlining those three pure computations into their existing Behavior fold
entrypoints, preserving Actions and complete state; no generic plumbing or
new helpers. Keep the owning TerminalProjection native field shape with
narrow reasoned expectations for its measured1080-byte enum/type complexity,
and the one contiguous custody trace length. Alternative boxing/aliasing would
change fixture allocation or obscure the exact derive contract to silence
style checks. Fix assertion semicolons normally, then rerun affected controls
and all required checks. No warning suppression applies to production code.

### Native schedule outcome modeling refinement

The foreign owning schedule operation returns Result<bool>. Automatically
select a bijective private DeclarationScheduling sum, Queued/NotQueued, for
the new fixture function's result, preserving every original native error.
This prevents a new Bombay research function from encoding the semantic queue
outcome in a boolean without replacing the owning native API or implying
remote admission. The alternative forwards the semantic bool unchanged; it
violates the repository's modeling rule. Forecast+10–12 test lines, up to324
within the recorded325 pressure ceiling; no public/production/dependency/path
change. Apply only after restoring the frozen semantic inversion source.
The previous native-byte/cleanup campaign hashes remain distinct evidence.

Final child fixture naming/minimization uses explicit target/sibling actor and
creation names, and removes a redundant native amount predicate already
checked against each exact role. No protocol, Actions or ownership changes.
Affected debug/optimized controls and strict lint are rerun on the final hash;
prior full workspace results retain their own source identities. Required
CI will verify the exact combined reviewed head.

The pressure fixture's exact rustfmt projection for the closed schedule sum
is327 lines. Automatically adjust that same-source ceiling325 to330 before
retention; no other surface changes. Overall research2300/cumulative6400
reserves still cover current1894 plus330. Preserve native receipts and normal
formatting. Earlier frozen source inversions retain their separate identities.

### Actual ordinary child-service composition and final local checks

The retained child_service_stop.rs was498 lines at the naming checkpoint, source SHA
acea060539bbaaf8dc3479e910f516a2c0eb404799cacdaef64bacce3b9f0d0d.
It uses a genuine application-owned service implementing existing
InterpretInstalledActor, existing EventLayer/SendLayer products and the real
correlated ShutdownEstablished interpreter. Target A performs useful work,
then publishes an established/no-failures joined report while sibling B still
replies. Cancelling a borrowed report wait preserves the original observer.
Later parent acquisition retains full root/child state, every native terminal
field, creation batch/settlements and both root notification receipts. Native
OwnerCancelled's empty acquired slot is grounded in Driver.ingress.take and
Completion conversion for this healthy trace, not assumed for every such phase.

Historical source46af8ce1e72645337267c3aea0b483d9472a01bd6ee89edc3129dfe5d63ff334
passes complete/restored debug and optimized controls. Same-callsite send-only
substitution fails exactly E0308 in both profiles; direct installed shutdown
fails exactly E0624 because the method is private. The nominal ordinary Rust
construction succeeds; facade E0277 identifies its unsupported runtime-result
ingress without proving a new macro necessary. Commands/diagnostics/actual
final binaries remain `/tmp/bombay-child-stop-campaign.json`. Setup import,
nonexistent parent accessor, unused native state and wrong optional-slot
assumptions are author/model failures with no inversion credit. Final pure-
fold/naming changes have separate debug/optimized positive controls and strict
all-target Clippy in `/tmp/bombay-child-retained-final-checks.json`; they do not
claim renewed static inversions on the later source hash.

Combined workspace build, workspace unit/integration/doc tests, all-target
tests and format check pass through pinned Nix on the recorded combined source;
strict all-target Clippy passes on the final source. Exact argv, source/lock
identities and logs remain `/tmp/bombay-remote-contract-combined-checks.json`.
Final naming-only change has affected profile controls and unchanged contracts;
required CI still needs to pass on the frozen reviewed candidate. Full AUTH1,
NET1 and R01–R25 stay incomplete, including fresh remote stop, held actor-owned
task barriers, pressure/restart/security and production identity obligations.

### Automatically selected final research review corrections

Independent review found the completed child's settlement loop accepted an
empty vector. Select an explicit singleton assertion for Completed target A:
the final Stop product is authoritative and must survive. Keep OwnerCancelled
sibling B's cardinality unconstrained because its trace depends on timing.
Simulate erasure of A's acquired settlements and prove refusal in debug and
optimized builds, then restore the exact positive source. This is a test oracle
repair, not a runtime defect or full R11 proof. Raise the same-file child test
ceiling500 to510 before editing; no new paths, production Rust, dependencies,
public types or interfaces. Existing overall reserves cover this correction.

Select matrix fail-fast:false so one failed native TLS profile cannot cancel
the other profile's independent evidence campaign. This adds one CI configuration
line and changes no runtime policy. The alternative uses default cancellation
and may lose diagnostic evidence. Required Nix checking remains unchanged;
actual Linux CI execution is still required before delivery.

### Final reviewed research candidate and complete change record

The completed-child Stop-product correction passes debug and optimized positive
and restored controls. Simulated erasure fails both profiles at the intended
singleton assertion (observed0, required1), after complete native acquisition.
The restored child file is501 lines, SHA
04cb6bec3ddb7f030e334250b67ba120676a99c06c75a03422fc5de687e78eae.
Strict workspace/all-target Clippy and formatting pass on this exact source;
commands and logs: `/tmp/bombay-child-settlement-campaign.json`.
Earlier combined workspace562 and all-target561 passing tests retain their
recorded pre-final source identities. Exact-head CI remains a delivery gate.

Complete tracked and untracked delta against latest merged main9fc225ee:
16 paths; production configuration +51/-0/net51, production Rust0;
tests/fixtures +1907/-0/net1907; documentation +677/-1/net676;
manifest/generated lock +2983/-0/net2983; public API +0/-0 types, methods
and reexports. The root cumulative delta against a9c5b7d4 has74 paths;
production +1702/-474/net1228; tests +6629/-652/net5977;
documentation +3460/-79/net3381; manifest/lock +2986/-3/net2983;
public types +5/-0 from the previously merged runtime prerequisite.

The separate controlled Zenoh pressure candidate is one owning test path,
+326/-3/net323, source42340b0473addd065169bc02f61b855453e2af2a489ba6e14c797ab95ad53450.
Root research plus pressure2230 remains within2300; cumulative root tests plus
pressure6300 remains within6400. These are exact source delta totals, not
whole-process heap or a runtime resource limit. Generated source/locks and
upstream imports retain their separate cross-repository accounting. Historical
retained-path union remains a conservative lower bound because prior inventory
was incomplete; do not fabricate a precise global path count. Current stage
introduces13 retained source/manifest/fixture paths within its16-path reserve.
No new runtime abstraction or dependency is selected. Full remote feature,
resource enforcement, identity, restart and remaining acceptance witnesses stay
incomplete. Reviewed PR, exact CI and actual merge must precede delivery status.

### Automatically selected exact-child service shutdown stage

An application-owned service already receives the exact InstalledActor through
InterpretInstalledActor, but its existing request_shutdown is private (actual
E0624 in both profiles). The parent Actions route works, yet checking permission
before queueing that parent request leaves a later unchecked shutdown attempt.
Select public visibility for the existing InstalledActor.request_shutdown with
its unchanged generic ingress, InjectEvent bound, implementation and owning
control mailbox. The full installed capability remains runtime-issued; its
constructor/fields and send-only projection remain private. No new runtime
attachment, callback, trait, task, method spelling or authority store is needed.
The alternative borrows private Application capabilities or adds another service
attachment; that increases machinery without changing the owning operation.
Root-only public lifecycle targeting cannot shut down the actual child.

Keep a fixture-local exclusive permission/time owner around fresh validation
and this existing synchronous operation. It closes user admission before
submitting the control event and does not await mailbox capacity. Ok proves
submission only, not actor termination or joined retirement. Earlier acquired
messages retain their existing rights. A refusal never promises to reopen
already closed admission. InjectEvent and registered wakers can unwind after
closure or after submission; no rollback or successful retirement is implied.
Document these exact partial-effect boundaries. This is not authorization for
Behavior folds to perform effects or for production clock/provider selection.

Smallest failing end-to-end regression: the genuine local service holding the
installed target cannot invoke the exact existing operation after its fresh
check because visibility is private. Extend the existing public Application
child witness: known revoked/expired stop is refused with both target/sibling
still useful; allowed exact stop publishes joined report while sibling works;
repeated request after the report refuses AlreadyStopped. Preserve all native
results and independent notifications. Independently invert permission checks,
restore visibility denial, and retain send-only/no-event static denials where
supported. Held actor-owned task/resource barriers remain a separate R11 gate.

Forecast eight existing paths: owning endpoint.rs, child_service_stop.rs,
module-boundaries.md, public-api-audit.md, runtime-capability-interfaces.md,
this PRD, backlog status and the historical execution-ownership module map.
Mark the former private-method description as the exact EXEC1 delivery record
and point to this later choice rather than presenting contradictory guidance.
Production Rust at most30 net documentation and
visibility lines, zero executable-body growth; zero new defined public types
or dependencies, one existing method made public. Child same-file ceiling600,
up to99 additional test lines over501 before simplification. Existing production
2200/test6400 reserves cover root1228+30 and cross-owner6300+99. Raise combined
research reserve2300 to2400 before editing; no extra retained source paths.
Source revision/lock remains exact research2b770de with merged9fc225ee contracts;
all178 non-Communication lock records and owning private shutdown implementation
remain unchanged. Preserve prior failing-law hashes rather than assigning older
inversions to this new source. Independent research PR CI runs concurrently;
this dependent feature branch must integrate its actual merge before final PR.

### Research prerequisite submitted for actual delivery

Research [PR332](https://github.com/devrandom-labs/bombay/pull/332) targets main
at frozen head2b770def0b4adcaf241bb9c844480dfcea1ca95c. Independent non-author
[comment review](https://github.com/devrandom-labs/bombay/pull/332#pullrequestreview-5478716751)
finds no remaining ownership/API/minimization blocker after the concrete oracle
corrections. It does not grant full feature acceptance or claim unexecuted CI.
Required Nix and separate native debug/optimized Linux jobs run concurrently
in38046976021; CodeQL38046976027 and advisory/license38046976072 are separate
checks. No merge or completed CI is recorded at this checkpoint. The exact-child
stage is isolated from the preserved user workspace and will integrate the real
research merge before its own final review and PR.

### Automatically selected portable owning declaration-pressure delivery

Select the measured owning pressure witness's six-path retained stage: existing
zenoh/src/net/tests/tables.rs and ci/verify-release.sh, three test PEM files and
one fixture README under zenoh/tests/fixtures/declaration-pressure. Reuse exact
literal certificate bytes already tested in controlled373d25a's native
io/zenoh-transport/tests/unicast_openclose.rs::get_tls_certs. These localhost
server/client fixtures remain valid until March2123. The seven distinct-role
research certificates expire January6,2027; retaining those instead would give
this owner campaign an unnecessarily short CI lifetime. No deployment roots,
identity, certificate lifecycle or production rotation policy changes.

Both producer/receiver use the existing test TLS principal but actual separate
native sessions. The retained law is measured resource retention outside ACL
publication keys and measured restoration, not identity isolation or quotas.
Preserve source hashes, exact retained newline policy, minica attribution,
EPL-2.0 OR Apache-2.0 provenance and public fixture-key warning in README; do
not import a CA private key. This is fixture extraction, not cryptography code.

Forecast ≤445 net test/fixture/CI/documentation lines: owning test ≤330 net,
three PEMs67 lines, README≤40, existing CI≤12 configuration lines. Four new
paths, six stage paths, production Rust/API/dependencies/manifest/lock0.
Reuse the existing pinned-Nix controlled TLS CI job and its debug/release loop:
exact ignored owning test with TLS-only features and portable certificate path,
then strict owning library/test Clippy. No new workflow, publication or package
version. Original omission inversions keep historical source identity; new
retained fixtures require fresh native profile controls and exact-head CI.

Automatically expand cumulative cross-owner test/fixture ceiling6400 to6600
before retaining PEMs: current6300 plus child forecast99 plus fixtures67 is6466.
Production ceiling2200 remains sufficient; CI increase≤12 is counted separately.
The combined research2400 reserve covers1907+99+330+67=2403 only after raising
it to2450. Raise retained-path reserve300 to320 rather than asserting the prior
incomplete global inventory has an exact count. Unique source imports, generated
locks and artifact copies remain separately accounted. Recommendations preserve
all previous acceptance limits and require actual reviewed merge before delivery.

### Authorized-control oracle refinement before final retention

Coordinator review found the permitted-control test awaits retirement even if
submission was refused. An always-denying implementation would hang instead of
failing its intended permitted-result assertion after cleanup. Select retaining
an optional actually acquired report and repeat receipt only after successful
submission; perform ordinary parent cleanup in the refused case, then assert
the required Ok/report/repeat facts from acquired originals. This adds at most15
test lines in the same600-line file ceiling, no production/API/path changes.
Independently simulate always-denying permitted admission and require its exact
typed result mismatch after cleanup in both profiles. Keep older revocation and
expiry inversions with their recorded source identities; rerun affected positive
controls on the refined source. Avoid replacing this with an elapsed timeout
whose expiry would establish no actor-law violation.

### Actual Linux transport evidence and owning pressure fixture failure

Research PR332 native Linux debug/release jobs114198354425/114198354400 pass.
Downloaded artifacts11667797212/11667902224 each contain16 unique cases:12
ordinary complete controls and4 exact late-worker-death refusals. Independent
receipt/exit validation is `/tmp/bombay-remote-contract-linux-native-proof.json`;
source/profiles remain frozen2b770def. Required Nix38046976021 remains pending.
No merge or full remote gate is inferred from these native-only campaigns.

Controlled pressure [PR3](https://github.com/devrandom-labs/bombay-zenoh/pull/3)
head9f40c343ab00791dcf859655848a59e75435397b has actual non-author
[comment review](https://github.com/devrandom-labs/bombay-zenoh/pull/3#pullrequestreview-5478745287).
Its required Linux run38047637706 fails: producer native open times out,
owner measures0 mappings, broken-pipe request and actual child exit101 survive,
and owner native close succeeds. This is a fixture connectivity failure and
has zero retention-law or semantic-inversion credit. Preserve exact logs
`/tmp/bombay-zenoh-pressure-ci-failed.log`.

Selected TLS utils.rs613–629 resolves only lookup_host.next and derives server
name from the endpoint host; unicast.rs340–374 connects that address,415–457
binds the same resolver result. The fixture listens127.0.0.1 but connectslocalhost;
the same actual Linux log records localhost resolving::1 in its preceding
successful TLS control. Select changing its single listener to tls/localhost:0
so both endpoints share native resolution and DNSlocalhost certificate identity.
Allow a two-line explanation, at most2 net test lines: same six paths, total≤425
within445, owning test≤330, no production/certificate/timeout/verification change.
An explicit SNI override is absent in this selected API; no custom resolver or
fallback is needed for the fixture. Rerun native profile controls, strict checks
and actual exact-head CI. Do not blindly rerun the failed source or weaken a check.
The first local-cache origin push was not GitHub delivery; actual controlled
remote base373d25a ancestry and correct branch push were verified before PR3.

### Research prerequisite actually reviewed and merged

[PR332](https://github.com/devrandom-labs/bombay/pull/332) merged at
2026-10-10T11:47:54Z, commit a3a018790b4a0edb61d67e190d8978b1ce38464f,
after exact head2b770def0b4adcaf241bb9c844480dfcea1ca95c independent comment
review and all six observed checks passed: required Nix38046976021,
its separate native TLS debug/optimized jobs, CodeQL38046976027 plus summary,
and Deny38046976072. Required Driver/Observe fuzz and artifact uploads passed;
optional scheduled Miri was not executed. Exact full CI log is
`/tmp/bombay-remote-contract-final-ci.log`; Linux native artifacts retain16
case-specific observations per profile. No full AUTH1/NET1/R01–R25 is closed.

### Exact-child service shutdown final local verification

Final endpoint61a872762a8261f5308fe0813551ad1863be489f7753f6fdb8b2f95729596c32
has the exact prior method signature/bounds/body039a4394e2510c44f5a1c98cc1a4b180ef2fb42c7dac85bbf65cc648867c943f.
Only documentation/visibility changes; locked graphaa2b8c7f9b4fb917976a13167718b6cb462ec01420ea426578ae5951f7cc5bab
is byte-identical. Final test592 lines, SHA
a59515a6ab23749c9794ad23d0d5937e5e3f86d54226457a8ed56f4e2c8647d6.
Its17-command exact-source campaign preserves positives, three independent
revoked/expired/always-denied inversion pairs, restored pairs, private visibility
denials at both actual call sites (two E0624 each profile), final strict workspace/
all-target Clippy and formatting. Every semantic inverse fails its named typed
assertion after actual native/notification acquisition; no timeout-only credit.
Evidence: `/tmp/bombay-child-stop-final-oracles.json`.

Prior send-only E0599/no-shutdown E0277 static pairs retain their actual earlier
source identities in `/tmp/bombay-fresh-child-campaign.json`. Standard Report and
repeated AlreadyStopped controls use real installed authority. Fixture scope is
local permission/time and leaf-child native ownership; production authentication,
replay and genuine child-owned held work remain open. A separate agent reviewed
the coordinator API/oracle changes; that agent authored the original test, so
this is not described as wholly non-author review of every line.

All five combined pinned-Nix commands pass on the exact final Rust sources:
workspace build, workspace unit/integration/docs, all-target tests, format and
strict workspace/all-target Clippy. Counts and exact argv/hashes/logs are
`/tmp/bombay-exact-child-combined-checks.json`. Reviewed PR and its required
exact-head CI and actual merge remain before marking this prerequisite delivered.

### Automatically selected identity scope and issuance research

Accept verifier handoff evidence binding the original protected JSON and its
original caller, deployment, purpose, exact host/runtime/actor, protocol,
operation and grant-authority scope. Application authorization separately compares
that verified scope to the actual static export and permitted operation. Neither
routing path, gateway local origin nor mTLS peer identity may replace original
caller authority. Verifying caller alone is viable only with independent evidence
that the exact claims were authenticated; otherwise scope substitution is possible.
No concrete field spelling, AID grammar, generation issuer, proof container,
signature algorithm or public provider API is selected by this decision.

Select ordinary Rust comparison before new abstractions: two private provider-
issued fixture receipt products with no Deserialize/raw public constructors,
two concrete async verifier functions, required typed result-message constructors
and ordinary explicit projection functions. Compare with the smallest common
provider-private fixture receipt; its private constructor must not be advertised
as a final public API for external Selo. A public raw constructor would weaken
issuance denial. Only an actual ordinary-Rust residual gap can justify a trait.
Use already selected SerdeJSON1.0.151/Serde1.0.229/Tokio1.53.1 and core AsyncFn;
no new manifest, dependency, codec or cryptographic mechanism. Rust-stdin against
Cargo-selected rlibs proves syntax, not ordinary consumer dependency closure.

Strict DTO parsing uses existing derive deny_unknown_fields and duplicate-field
rejection; generic Value insertion can erase duplicate keys. Own decoded strings
so valid escaped JSON spellings remain eligible. Exact byte owner remains outside
verification awaits. Accepted/invalid/stale/unavailable retain separate types and
actual acquired unavailable cause. Explicit fixture startup mode permits selected
deterministic functions; production-required mode with fake/missing provider must
refuse before actual application/export startup, with no per-message fallback.
No production-positive real provider is invented. Closed fixture identities do
not select production actor naming; controller-AID-scoped stable names remain a
recommendation pending fresh owning Selo evidence, with host/runtime/grant epoch
separate. Production clock conversion/skew/cache/delegation remain undecided.

Forecast up to400 authored research lines in one explicitly owned scratch/stdin
probe including ordinary-product comparison, zero production/adopted public API/
dependency/manifest paths. Static controls both profiles precede raw Deserialize,
private-construction, raw substitution, omitted result constructor and mismatched
consumer denials; no runtime/crypto/bounded-allocation credit from compilation.
Count the unique authored probe separately from byte-identical variants/artifacts.
Automatically expand total research2800 to3200 and cross-owner test/research6900
to7300 before authoring, covering retained2730 plus400 and6449+351+400=7200.
Source/API/public-type/path320 and production2200 reserves otherwise stay unchanged.
Replay/correlation/reply slots must be reserved before native insertion/transmit:
waker unwind after actual enqueue cannot reopen replay eligibility. Those W/B
policies remain open; this scope/issuer probe must not claim to implement replay.

### Exact-child stage complete change record before submission

Combined workspace/docs562 passing and all-target561 passing tests each retain
one existing ignored test; build/fmt/strict Clippy pass. Source and lock remain
exact final hashes above. Independent coordinator-API/oracle review has no
remaining blocker; full non-author PR review and exact-head CI remain required.

Complete delta against research prerequisite0733f6f, including all tracked and
untracked files: eight paths, production +23/-1/net22 (documentation/visibility,
zero executable-body growth); tests +160/-69/net91; documentation +285/-11/net274;
manifest/lock0; defined public types +0/-0; one existing method newly public,
zero export changes. Cumulative root delta against a9c5b7d4:75 paths,
production +1725/-475/net1250; tests +6720/-652/net6068;
documentation +3743/-88/net3655; manifest/lock +2986/-3/net2983;
defined public types +5/-0 from earlier runtime delivery. Exact machine records:
`/tmp/bombay-exact-child-stage-checkpoint.json` and
`/tmp/bombay-exact-child-cumulative-checkpoint.json`.

Controlled pressure hostname successor is one existing path +3/-1/net2 over
9f40; complete owning stage is tests+319/-3/net316, fixtures67, CI6, provenance36,
total425net acrosssix paths, no production Rust/API/manifest/lock. Root6068 plus
controlled383 is6451 tests/fixtures. Separate held-root test351 projects6802;
new identity-scope research up to400 projects7202 within7300. These actual/forecast
terms are distinct; do not add older snapshots again. Production configuration
CI increments and generated lock artifact cost remain separately visible.
Rebase onto actual main a3a01879 before final PR; source bytes must stay identical.

### Automatically selected public root owned-work retirement witness

Ordinary public FixedSupervisor composition compiles in debug/optimized
metadata probes against exact selected0.23.0 Behavior/Actors and merged9fc225ee
Bombay contracts. Candidate stdin7fd8c7e69a2e4144307b025ba7bd777974ac6f8212b010a44e764e2738754531,
control333d94ed06e063ec6b6ec68a335109d6bdfa2514e2a455a4d16f52ad58de1a08;
metadata-only compilation grants no runtime law or R11 completion credit.

Select an optional affine first-preparation gate in existing test Workshop,
using already locked Tokio1.53.1 oneshots and OwnedSemaphorePermit. Its genuine
WorkerPreparationSource::prepare_first signals reaching the gate, waits release,
then returns the existing WorkerSubmission. I/O stays inside the runtime-owned
capability port, never a Behavior fold. Actor A's normal termination must occur
while its actual owned preparation work/permit remains held and shared retirement
observation stays Pending. Another ordinary Application B on the same caller-
owned runtime must produce useful Actions replies before A's report is checked
again. Explicitly release actual A work, acquire its report, then preserve both
Applications' complete native results and independent notification products.

Alternative fabricated ActivationPermit or injected arbitrary task would bypass
private owning authority. A direct StopOnShutdown<StableProxy> start is not proven
with current public ChildInputIngress lifting; no new API or macro is justified.
Root-only public composition supplies the needed existing source owner. This
witness does not establish same-parent CHILD or shared-network-session isolation;
those remain distinct obligations. Do not drop/ignore native results as proof.

Forecast two existing paths: fixed_supervisor_recovery.rs and this PRD. Tests
expected160–230 net, ceiling300 including imports and full custody assertions;
production Rust/config/API/dependencies/manifest/lock0. The private test gate
owns only the real affine release/permit, no new runtime primitive. Automatically
expand cumulative cross-owner test ceiling6600 to6900 and combined research
reserve2450 to2800 before edits; retain production2200/path320 limits. Preserve
all parallel candidate source identities and count each actual source delta.
The source base is freshly fetched main9fc225ee, created before first commit.

Run focused debug and optimized healthy controls first, then simulate bypass of
spawn_local_execution's real settle_local_outcome(outcome).await at launch.rs1376.
The existing independent permit/Pending oracle must fail after B's useful work;
a timeout-only outcome is no inversion credit. Restore exact production bytes
before positive/lint checks; retain only tests. Complete native returned
WorkerPreparationReturned ingress must remain owned without opening its private
payload. Any unexpected source-policy classification must be resolved from its
actual original, not reclassified as successful retirement.

### Held-work test formatting and custody scope checkpoint

The first complete held-work draft projects +365/-5/net360 test lines in its
one existing source path, exceeding the initial300 per-file estimate. The agent
stopped before Cargo or further edits. Full root/proxy/worker fields and both
receiving products, including a shared65-line exhaustive residual oracle, explain
the increase. Select retaining those independent custody assertions and raise
this same-file ceiling300 to380 before continuing. No production/API/dependency/
manifest/lock changes. Do not weaken assertions to fit an underestimated forecast.

Existing cross-owner tests6449 plus360 is6809 within6900; combined research2379
plus360 is2739 within2800. No global ceiling or retained-path expansion is needed.
Plain Rust source syntax compiled earlier; actual runtime behavior, settlement-
bypass inversion and native failure classification are still unverified. A
later control failure must be investigated from exact ownership rather than
ignored or relabeled as proof.

The held-work review also requires both final root Stop products' eight owning
FixedSupervisor send lanes and outer NoSends, rather than only creation/verdict
checks. Select those complete lane assertions before execution, up to20 further
test lines, and raise the same-file ceiling380 to400. Existing global6900 and
research2800 reserves cover6449+400=6849 and2379+400=2779; no other surface change.
These are observable assertions, with every required operation evaluated before
assertion. Private owning payloads remain opaque and retained.

### Held-work inversion reaches independent progress before refusal

The first real settlement-bypass mutant fails the initial Pending assertion,
not the later B-progress assertion named by the original receipt matcher.
Correct the historical evidence to the actual first-law failure; do not claim
B progressed in that negative run. Positive controls prove both actual roots.
Select capturing the first report using an independent shared observation and
retaining its first permit count, then checking both initial facts only after
B's useful Actions reply and the later Pending check. A completed Future must
never be polled again. This reuses approved independent Observe consumption,
adds at most10 test lines before style reductions within400, and preserves the
native producer and explicit release. Fresh debug/optimized inversions must
fail the named later Pending law after actual B progress. No production or API
retention. Remove Clippy-proven obsolete expectations; ordinary let-else and
sibling-role naming reduce fixture syntax without weakening native facts.

### Automatically selected identity-probe scope correction

Pinned rustfmt exposed409 authored lines against the400-line forecast before
compilation. Remove the no-op cause-borrowing function; preserve actual native
causes with the already selected thiserror2.0.20 derive, and pass startup mode
and provider profile into the actual application composition. A separate guard
with hardcoded test-mode startup would not prove refusal before startup. Select
a450-line one-source research ceiling before further edits; forcing400 through
line packing or generic projection machinery would weaken reviewability. This
adds no production/public API/manifest/dependency or retained repository path.
All tests must still distinguish compilation from runtime or crypto evidence.
Retained research2732 plus450 projects3182 within3200; cross-owner test/research
6802 plus450 projects7252 within7300. Source growth remains explicitly counted.
The user's automatic recommended-choice instruction supplies authorization.

### Held root work: exact final witness and delivered prerequisites

Retain test source5f62609ba957f597c67c336309345a481ddf037c5213d89fd6c9bccd3f2420a8:
+360/-9/net351, no production/API/dependency/manifest/lock change. All three
owning controls and restored controls pass debug and optimized; the real
settlement-bypass inverse fails the later named Pending assertion after the
other Application's useful reply in both profiles. Exact owning launch source
c61f14e7c16ed22f3330131e67098dcb654cf6785fe827087b249c4b429d8656 is restored.
Pinned fmt and owning strict Clippy pass in both profiles. Exact commands,
source identities and logs: /tmp/bombay-root-owned-work-final-campaign.json.
The original late returned-source ingress stays owned until native results
are explicitly dropped; weak ownership counts prove that exact custody.
Full combined checks, independent review, required CI and actual merge remain.
This proves two separately owned Applications on one caller-owned runtime,
not same-parent child or shared-session R11 isolation.

Research prerequisite PR332 merged a3a018790b4a0edb61d67e190d8978b1ce38464f
at2026-10-10T11:47:54Z after all six checks, including required Nix38046976021
and both actual Linux native-TLS profiles. Independent exact-head review:
https://github.com/devrandom-labs/bombay/pull/332#pullrequestreview-5478716751.
Exact-child local service PR333 head6cdb63679e40b5f0a7d2a4fa29c97af72c07451b
is submitted with five combined local commands passing; its reviewed CI/merge
are still pending, so no merged credit.

Controlled dependency pressure PR3 merged1291c3e019de19c2a2e1e713380383526347bb59
at2026-10-10T12:14:36Z. Required Controlled TLS dependencies38048704318 passed
on exact headed016e8ecc5ae37a28aa69548813d406767bdbb8 at12:06:23Z, including
debug/release pressure controls and both real configuration-owner Miri tests.
Full final CI log: /tmp/bombay-zenoh-pressure-final-ci.log. Exact successor review:
https://github.com/devrandom-labs/bombay-zenoh/pull/3#pullrequestreview-5478893647.
Initial38047637706 failed loopback address-family setup and gains no retention
credit. Observed native declaration retention remains a blocker to actual bounds;
the six-path425-net test/fixture/CI/provenance stage adds no quota enforcement.

Independent read-only review of final test5f62609b authenticated actual source,
lock and restored owner. No blocker; no whole-negative-cleanup credit: the
negative assertion panic precedes release/native-product collection. Healthy
controls establish that custody. Final source remains unchanged after moving
onto actual merged maina3a01879. Review preserves the difference between local
root isolation and future same-parent/shared-session requirements.

### Parallel delivery of the combined source

Select a draft held-work branch descended from reviewed child-stage6cdb636 while
PR333's required CI runs, targeting main so existing branch-filtered CI executes.
The temporary draft diff includes its explicitly named parent prerequisite.
After PR333 actually merges, the retained PR diff must be the two held-work paths;
final review, required CI and merge must concern this combined source. This avoids independent incompatible
branch-local contracts; coordinator alone resolves the PRD append conflict,
preserving both exact records. No source body or lock bytes change during this
integration. Alternative serial submission would delay independent CI. A dependent
draft is preparation, not completion or authorization to bypass failed checks.

Prior combined checks on maina3a01879 pass: build, workspace/docs563, alltargets562,
each retaining one existing ignored test, fmt and strict Clippy. Evidence writer
initially used incorrect local/launch.rs path after a successful build; corrected
to actual launch.rs without Rust rerun and preserved the build log. No new law
credit arises from that bookkeeping correction. Its build duration is unrecorded.
Repeat required combined checks because the parent child-stage source now enters
the exact combined tree. Preserve the prior report separately.

### Automatically selected stable actor namespace semantics

Select stable actor identity as deployment plus namespace-controlling principal
plus the application's explicit static local export name. The controller is
the principal authorized to own that namespace; it is not silently the creator,
delegator, current hosting node or local gateway. Same local spelling under
different controllers identifies distinct actors. Conflicting assignments under
one controller/deployment must refuse configuration. Keep host principal, runtime
generation, actor generation, grant issuer/authority generation and request attempt
separate; host/restart or key rotation must not silently replace an exact target.
This preserves static placement without introducing a dynamic actor registry.

Alternative globally unscoped strings require an additional collision/namespace
authority policy. Runtime-local addresses cannot supply stable identity. One
KERI identifier per actor adds keys and delegation not required by the approved
node hierarchy. Select ordinary concrete Rust/static export products before any
parser, trait, new public type or macro. No wire field names, normalization,
length default, AID grammar, generation issuer or public API is selected here.
Those remaining uncertainties require independent source/protocol witnesses.

Fresh downstream Selo source80289b6b7cfb3f11ab2ff866c0e141599c835801 stays clean.
Its selo-naming0.1.0 owns44-character shape-only AID/SAID and Http/Tcp routing
qualification, not this actor contract. Its parser collects split chunks before
arity checking; that is not a hostile-input allocation bound. Its selected graph
contains cesr-rs0.11.1/keri-rs0.0.15; corrected registry candidates remain unadopted
there. Neither source is a Bombay dependency or production acceptance proof.
Source references: downstream docs/naming.md; crates/selo-naming/src/id.rs,
parse.rs, resource.rs; docs/next-cesr-migration.md. No Selo migration/durability
scope is introduced.

Established KERI Authority::verify accepts original arbitrary byte slices; the
existing crypto owner can therefore protect original Bombay JSON in a future
compatible declared provider profile. Candidate keri-rs0.1.0 source5704a438
explicitly states Verified does not encode authority/message bytes; that result
is not a transferable certificate for a different request. KEL event receipts
prove their own accepted event/endorsement facts, not command permission, actor
admission or retirement. This research selects no crypto crate/proof container
and claims no runtime verification. Keep trusted provider scope bound to original
request bytes; do not reserialize JSON as a KERI event to invent that binding.

### Ordinary identity issuance comparison: bounded static evidence

Final formatted scratch /tmp/bombay-scope-issuance.rs is398 authored lines,
SHA0d7095d605a7f56a9382314b817002203d49f86f90d5e4f0c2e1c0d8a106f8bf,
plus five unique negative statements (byte-identical variant copies excluded).
Exact healthy/restored debug and optimized metadata compilations pass with
-Dwarnings; six independent static-denial classes fail in each profile:
raw Deserialize E0277, private issuance E0451, raw DTO result E0308, missing
required typed constructor E0061, wrong consumer associated Msg E0271, and
wrong scope projection E0308. The latter consumer diagnostic was initially
predicted E0308; correct the harness to actual E0271 without changing its law.
Initial missing existing target Msg:Send+'static bound and redundant empty
terminal product were setup findings, not intended static-denial credit.
Empty descendants use existing Never; no wrapper or no-op reader is retained.

Automatically select distinct provider-owned receipts and ordinary async
functions, explicit scope projections and required result-message constructors
for this seam. They compile with two different concrete Application consumers.
The comparative common provider wrapper adds44 lines forwarding original
closed provider receipts and proves no unique state/transformation is needed;
do not retain a new common wrapper or trait. Future production receipt issuance
and provider API remain separate owning decisions. Evidence:
/tmp/bombay-scope-issuance-campaign.json and final-fmt.log. No executable,
startup refusal, actual native outcome, crypto, replay, time or bounded-parser
credit: metadata-only output is deliberately not an execution artifact.
Extern artifacts preserve actual Cargo provenance and child-stage lockaa2b8c;
the Nix shell's main-2 lock0c2f7e is recorded separately.

### Held-work combined verification and complete scope record

After integrating reviewed child-stage6cdb636, all five required pinned-Nix local
commands pass on exact held-test5f62609b/endpoint61a872/child-testa59515/owner
c61f14/lockaa2b8c: build, workspace/docs563 and alltargets562 (one existing
ignored test each), fmt and strict all-target Clippy. Exact commands/source
identities/logs: /tmp/bombay-root-owned-work-combined-checks.json. Prior isolated
checks retain separate preintegration provenance. Public/API/body and source
bytes remain exact. Independent full-test review found no blocker.

Complete tracked/untracked stage against child6cdb636: two paths, production
+0/-0/net0; tests+360/-9/net351; documentation+258/-0/net258;
manifest/lock0; public types+0/-0; no method/export change. Cumulative root
against a9c5b7d4:75 paths, production+1725/-475/net1250; tests+7080/-661/net6419;
documentation+4001/-88/net3913; manifest/lock+2986/-3/net2983;
defined public types+5/-0 from the already merged retirement prerequisite.
Exact complete accounting including untracked: /tmp/bombay-root-owned-work-
stage-checkpoint.json and cumulative-checkpoint.json. Cross-owner root6419
plus controlled pressure383 plus unique identity research403 totals7205
within7300; retained research2732+403=3135 within3200. Generated locks and
upstream snapshots remain separately counted. Reviewed required exact-head
CI and actual merge to main remain before marking this witness delivered.

Initial draft334 targeting the feature branch triggered no workflows: the existing
CI/CodeQL/Deny filters require a main target. Correct the target before pushing
this documentation successor, which triggers ordinary required CI. Keep parent
333 actual merge as a delivery prerequisite; do not weaken branch filters or
claim missing checks passed. No Rust/lock bytes change.

### Automatically selected UTF-8 routing reproduction stage

Fresh owning source inspection found Resource::split_first_chunk slices suffix
at byte1 although the first root chunk can be a multibyte UTF-8 character.
Selected KeyExpr accepts UTF-8. This is a concrete unexecuted robustness
candidate, not a defect proven by source inspection or remote acceptance.
Select an owning regression on the actual Resource registration/resolution
path before any production edit. Compare valid two-/three-/four-byte leading
characters and slash/ASCII/empty controls; preserve the complete expected
name/path split and routing result. Run pinned-Nix debug and optimized and
require the intended Unicode boundary panic before granting defect credit.
No mocked replacement splitter or new codec/grammar is allowed.

Forecast controlled latest-main1291c3e two existing Rust paths:
zenoh/src/net/tests/tables.rs (+up to35 test lines) and, only after the
executed regression isolates its gap, owning dispatcher/resource.rs
(prepare but do not apply a standard Rust str/char-boundary proposal,
up to10 net production lines). Public types/API/dependencies/manifest/lock0.
Existing Resource/KeyExpr/routing table owners and standard str APIs suffice;
no new abstraction or crate gap is demonstrated. Coordinator must record
the concrete recommendation/inversion before dependent production edits.
Separate this narrow defect from larger quota investigation.

Cross-owner tests/research7205 plus35 projects7240 within7300; research3135
plus35 projects3170 within3200. Preserve generated artifacts/source snapshots
separately. Source path union remains within320; root and controlled stage
records remain independent and aggregate. Exact source, baseline, positives,
inverses, restores, strict checks and delivery remain required. Any scope or
diagnostic mismatch is recorded honestly, not credited as the intended law.
The user's automatic recommended-choice instruction authorizes this choice.

### UTF-8 reproduction and selected owning correction

Actual owning Resource registration/resolution regression327fee75c5008299
(+21 test lines) fails101 in debug and optimized at resource.rs653: byte1 is
inside valid two-byte character é. Both runs name the intended character-
boundary panic and one failed test; all initial source bytes match owner
1f4722ebf2eec57616d24c0127af7daeaf491a104684c0ac805709aa9315ded2.
Evidence /tmp/bombay-zenoh-utf8-original-campaign.json and actual profile logs.
This establishes the narrow owner defect, not a TLS/remote or quota witness.

Automatically select deriving the first character's UTF-8 width with standard
str::chars().next()?.len_utf8(), then retaining the existing byte-search/split
on the proven boundary. Empty suffix returns None through ?, ASCII/slash
width remains1, and no allocation or semantic parsing replacement is added.
Alternative char_indices scan is valid but replaces the existing efficient
substring search with a full character scan. Special-casing ASCII/non-ASCII
branches adds unnecessary duplicated policy. No new crate is needed for the
standard string boundary operation; handwritten codec replacement is not used.
Forecast owning production -2 net lines, existing test+21; no types/API/dep/lock.
Keep every correct split and original Resource mapping/resolution fact.

Add the exact owning test to the already existing debug/release controlled CI
loop (up to4 config lines, third existing path). This concrete scope supplement
is recorded before edits. Run focused healthy controls both profiles before
broader owning library checks; original production bytes already demonstrate
both intended failures. A final original-byte restoration/reapply sequence
must leave only corrected source. Review strict format/lint and required CI,
then actual reviewed PR merge, before recording this correction delivered.
Larger declaration quotas remain independent investigation; no quota code or
limits are authorized by this UTF-8 correction. Published1.10.1 archives remain
their original bytes until a separately verified release delivers a successor.

### UTF-8 independent tree-boundary oracle correction

Initial corrected source0780c09f passes the21-line regression327fee75 in both
profiles, but independent review found registration and whole-name resolution
share the splitter. A flat-child mutant could pass those assertions. Select
standard split_once('/') expected-prefix lookup, empty lookup preserving the
original root Arc, and an ASCII-leading path with a later Unicode chunk before
claiming complete path conservation. Forecast at most35 test lines still holds.
Production remains the same minimal correction (actual+4/-7/net-3, not the
earlier forecast-2), CI+3. A flat-child simulation must fail the independent
prefix assertion; final original-byte inverse must still fail the original
character-boundary law in both profiles. Retain prior source/campaign provenance
separately; never assign old21-line proof the final stronger source hash.

### Exact-child prerequisite delivery

PR333 merged to mainacb6cc188f03ded29863d8d2565e3fd093e65558 at
2026-10-10T13:25:15Z. Exact reviewedhead6cdb63679e40b5f0a7d2a4fa29c97af72c07451b
had all six observed checks passing: Nix38051236713 completed12:48:37Z,
CodeQL38051236701 plus summary, Deny38051236687, native TLS debug/release
12:22:43Z/12:24:19Z. Required Driver/Observe fuzz and artifacts passed;
scheduled optional Miri skipped, no Miri credit. Full CI log
/tmp/bombay-child-stop-final-ci.log. Independent exact-head review:
https://github.com/devrandom-labs/bombay/pull/333#pullrequestreview-5478906505.
Actual merge uses normal PR rules with no bypass. This delivers the existing
exact-child local shutdown exposure, not production authorization or NET1.

### Held root work prerequisite delivery

PR334 mergedfbc714c176792411ea00fdf73876c30aa17fd4ca at
2026-10-10T13:27:30Z. Exact reviewedhead165d4340a4bfe6687297eef8c92ea857f5b4996b
had all six observed checks passing: required Nix38052058178 at13:10:00Z,
CodeQL38052058202 plus summary, Deny38052058176, native-TLS debug/release
12:33:49Z/12:38:26Z. Driver/Observe fuzz and uploads passed; optional scheduled
Miri skipped, no Miri credit. Full CI /tmp/bombay-root-owned-work-final-ci.log.
Independent stage review5478950938 and final-main review5479106901 are recorded at
https://github.com/devrandom-labs/bombay/pull/334. After333 actually merged,
actual Git and GitHub compare showed only the two held-work paths; mainacb6cc1
was tree-identical to reviewed parent6cdb636. GitHub PR's cached base/file list
still showeda3a0187/cumulative paths, separately identified as stale metadata.
Exact local comparison /tmp/bombay-root-owned-work-final-main-diff-proof.json
retains authoritative source hashes. Normal PR merge, no bypass.

This delivers real root-owned-work waiting and independent Application progress
while retaining all original native/notification facts. Full same-parent child,
shared-session, AUTH1/NET1 and production identity/resource gates stay open.

### Automatically selected executable verification-service witness

The existing permission550 test directly delivers a derived verification
observation; the398-line issuance comparison compiles typed completions but
emits no verification request and executes no startup. Neither proves the full
ordinary service seam. Select two meaningful pure requestors (counter/arithmetic)
that emit a named verification request in existing typed Actions, receive the
original provider-owned typed completion, and emit a processing observation in
Actions. Application-owned service code verifies the original borrowed protected/
proof buffers through the selected ordinary async function, preserving exact
request/result and allocated origin. No I/O, channels or clocks inside a fold.

Each private untrusted request message owns its original bounded-fixture Boxes
and correlation across the existing mailbox. Keep exact typed reply authority,
provider, pure scope projection and mandatory result constructor as ordinary
service-entrypoint arguments, following the measured398-line syntax. Copying
those arguments into every request would duplicate configuration and needlessly
create a generic wrapper. The actual domain request groups coexisting inputs;
existing external-service/Actions interpretation performs its transformation.
It deletes no production code and adds no production abstraction.
Existing EstablishedDelivery/EstablishedRecipient, ordinary functions, Never,
Observe-derived runtime reports and native receiving products remain owners.

For both independent fixture providers execute accepted/invalid/stale/unavailable
and one-field scope substitutions with each alternative's own valid byte-bound
fixture proof. Compare to independently configured expected scope, never scope
reparsed from incoming claims. Transfer original cause/payload allocations to the
exact typed completion and preserve them in final native state. Each correlation
gets one actual processing observation; later final Stop retains every lane,
complete native residual/failure fields and both notification receipts. These
protocols contain no protected-domain command/target capability: verification
processing is never counted as remote command admission. Real admission ownership
and final coherent permission/time check remain separate gates.

Startup takes explicit mode/provider profile before creating the actual Application
or export. Fake/missing production configuration returns original cold inputs
and records zero actual startup/export observations; explicit test configuration
executes the permitted service trace. No real-production positive is fabricated.
First independent inversions: omit scope check; unavailable-to-invalid conversion;
always-deny accepted evidence; implicit fake fallback/startup guard bypass. Each
named assertion must occur after acquired native/notification cleanup where the
trace permits it. Compile denials keep the required constructor/consumer/issuance
constraints. Prepare ordinary compile controls before full runtime assumptions.

Forecast four paths: new owning test verification_service.rs (650–850 expected,
ceiling900), crates/bombay/Cargo.toml (+2 dev-only Serde/JSON workspace edges),
Cargo.lock (only bombay-rs dependency edges), and this PRD. Coordinator owns
manifest/lock edits. Zero production runtime/public types/API/new package identity
or version/checksum refresh. Already selected Serde1.0.229/JSON1.0.151/Tokio1.53.1/
thiserror2.0.20 supply the mechanisms. Fixture scope/generation labels are finite
explicit conventions, not production identifiers, issuers, crypto/profile/defaults.
Preserve prior403 authored metadata research without subtracting it as a reduction.

Automatically raise cross-owner test/research7300 to8300 and research3200 to4200
BEFORE authoring: actual7205 plus Unicode28 plus900 projects8133; research3135
plus28 plus900 projects4063. Production2200/path320/defined public-type reserves
otherwise remain unchanged. Broad quota/protocol/public-provider implementation
is not included by this concrete witness selection. Fresh selected contracts and
current mainfbc714c are required; private request scope contains no new public API.

### Automatically selected native byte-pair packaging

Select complete protected JSON in the native Zenoh ZBytes payload and the
provider's unchanged opaque proof bytes in its native attachment. This reuses
the selected transport and serializer without a new envelope/codec/dependency.
Native publication, queries and sample replies preserve both buffers, including
leading/trailing JSON whitespace. Both lengths and their checked sum must be
bounded before copies/typed parsing; bounds remain a separate gate. Provider
verification receives the original pair. Topic/encoding/native rewritten request
IDs do not replace authenticated application scope or correlation.

Alternative RawValue envelopes preserve internal token formatting but trim
surrounding whitespace (selected serde_json de.rs1285–1292/raw.rs186–190), violating
arbitrary already-protected whole-buffer conservation. They also require an
unselected raw_value feature. Standard JSON-string envelopes preserve the full
text through unescaping and remain viable, but add escaping/decoding/allocation
and do not improve this selected native transport's typed surface. No handwritten
framing, canonicalizer or crypto container is selected.

For authenticated application refusals/results select ordinary protected sample
replies using the same payload/attachment pair. Native Query::reply_err lacks an
attachment slot; keep its errors/timeouts as native transport observations with
original custody and uncertainty, never as authenticated actor verdicts. A
standard JSON error envelope is an alternative but introduces a second packaging
path. This policy selects the packaging distinction, not concrete reply fields,
provider proof profile, signing authority, protocol version or public API.

Fresh source evidence is published bombay-zenoh1.10.1 api/bytes.rs154–166,468–503,
565–578; api/builders/query.rs102–109; api/builders/reply.rs125–133,234–283;
zenoh-codec zenoh/mod.rs399–434; existing attachments.rs tests (not newly executed
TLS/router proof). Vec conversion transfers ownership; borrowed buffers copy;
to_bytes can allocate for fragmented buffers. Native slices/reader remain owning
alternatives. Next ordinary Application prototype must verify both layouts,
whole-buffer/Unicode/order/large-integer conservation, swapped/missing proof,
fragmented/over-limit buffers and native-error versus signed-reply classification.
No full transport authentication or protocol acceptance is claimed by inspection.

### Verification fixture scope-result representation

Automatically select an ordinary Result success pair (original provider receipt,
closed ScopeMatch::Matched or Mismatched), retaining the original verifier error
in Err. The required concrete completion constructor consumes original raw request
and that result. This records the application's scope comparison separately from
the provider's authenticity without fabricating Invalid for valid wrong-scope
evidence. Scope is checked by the plain projection before receipt transfer.

Alternative Result plus a three-state ScopeAssessment alongside it admits false
Err-with-Matched combinations and duplicates the absence of verified evidence.
Repeating the check inside both concrete requestors duplicates policy/configuration.
A new generic checked-receipt wrapper adds surface unnecessary for the standard
Result/pair composition. No public provider API is selected by this fixture.
The private two-variant sum owns one proven policy distinction; no bool, common
trusted receipt, wrapper, trait or runtime is introduced. Native originals and
raw allocations remain conserved. Update static controls to this concrete
fixture constructor; earlier398-line syntax retains its own provenance.

### Broader owner checks expose inactive-transport tests

All ten final28-line UTF-8 campaign outcomes match their intended laws: healthy
positive/restored pairs pass, flat-path inverses fail independent ancestor lookup,
original-owner inverses fail the Unicode boundary, both profiles. Corrected
owner0780c09f is exactly restored. Broad owning debug library then has32 passes,
nine failures and two existing ignored cases. All nine failures invoke tcp in
interceptor-cache/link-weight fixtures despite the approved TLS-only Cargo
profile; no Unicode/root split failure remains. This is a verification failure,
not an all-tests pass or authentication/quota result. Evidence:
/tmp/bombay-zenoh-utf8-combined-checks.json and combined-owning-debug.log.

Select first restoring original owning production bytes for a broad baseline
control, preserving current stronger test. Expected identical nine unsupported-
TCP failures plus the independently proven Unicode failure; restore correction
afterward. If source confirms both entire fixture modules require excluded TCP,
match their existing test-module declarations to the inherited inactive
transport_tcp flag, retaining upstream test code. This is the already approved
remove-unsupported-options/retain-inactive-source policy, not a new transport.
Alternative turning TCP back on violates the selected runtime profile; blanket
ignoring or lint weakening would claim unsupported fixtures passed.

Forecast one additional existing test-module path, two cfg lines (up to4 net
test lines) plus at most explanatory comments; no production/public/dependency/
manifest/lock expansion. New total owner stage four paths: source-3, tests28 plus
up to4, CI3. Record actual baseline outcomes before dependent eligibility edits,
then run all eligible owning tests in debug/optimized and strict checks. Existing
full native TLS campaigns remain separate. This budget fits cross-owner8300 and
research4200. No bigger resource-quota redesign is included.

Original broad baseline reproduces exactly31 pass/10 fail/two existing ignored:
all same nine unsupported-TCP fixture failures plus the intended Unicode boundary
regression. Source restores exactly0780c09f afterward. Evidence
/tmp/bombay-zenoh-utf8-original-broad-baseline.json/.log. Both complete modules
select only tcp locators; existing inherited transport_tcp check-cfg flag names
are already registered while that Cargo feature remains unavailable. Apply the
two matching test-module cfg lines. No test is marked ignored; unsupported
transport source remains inactive, eligible tests and native TLS campaigns remain
required. Actual added test-module delta+2, within forecast4.

### Verification witness complete-custody size checkpoint

Pinned formatting measures587 test lines for the raw fixtures/providers, both
pure consumers and ordinary service functions. The agent stopped before the
900-line forecast was exceeded. Two actual public Application entrypoints,
exhaustive native receiving, post-cleanup cause/allocation checks and cold-start
controls forecast another400–550 lines; the earlier estimate omitted their
complete custody syntax. Select retaining those assertions rather than line
packing, erasing native facts or adding a generic runtime wrapper/macro.

Automatically raise the same new-file ceiling900 to1200 before remaining source
edits, cross-owner8300 to8600 and research4200 to4500: actual7205 plus Unicode30
plus1200 projects8435; research3135 plus30 plus1200 projects4365. Unicode30
includes the28-line final regression and two transport-eligibility cfg lines;
its CI/config/production deltas remain separately accounted. Four fixture-stage
paths and zero production runtime/public types/API/package/version changes
remain fixed. The owning helpers must have real independent concrete uses;
reject no-op readers, nested aliases, generic runtime plumbing or discarded
original results. Compile controls and actual targeted inversions remain gates.

### UTF-8 correction final local verification and minimization

Final controlled stage has four existing paths, zero untracked files: production
Rust +4/-7/net-3; owning tests +30/-0/net30; CI configuration +3/-0/net3;
public types +0/-0; no manifest, lock, dependency or public API change.
The private character-width correction reuses standard Rust str/char APIs;
independent ancestor/root/mapping assertions retain the owning registration law.
Source and accounting: /tmp/bombay-zenoh-utf8-final-checkpoint.json.

All six final eligible checks pass: owning library tests debug and optimized
each32 passed/0 failed/two inherited ignored producer/pressure cases; strict
Clippy library/tests both profiles; workspace formatting; CI shell syntax.
The existing CI explicitly executes the ignored pressure parent separately.
Exact commands/outcomes and source hashes are in
/tmp/bombay-zenoh-utf8-eligible-checks.json. Initial evidence-writer regex omitted
counts; recover counts from the original successful logs, with no command rerun.
Preserve original nine unsupported-TCP failures and baseline Unicode panic in
the earlier records. Ten focused positive/inverse/restored outcomes remain
separate, including the independent ancestor inversion. Read-only independent
review found no retained source/minimization blocker.

Proceed with a focused controlled PR against main, actual-head review and
required CI before merge. Published1.10.1 still contains the original defect;
a separately verified successor release remains a delivery prerequisite. This
is no routing quota, production identity or full remote-feature acceptance.

### Native payload and opaque-proof custody witness

Before transport-fixture edits, select extending the already retained native
TLS custody fixture with Zenoh's existing payload/attachment builders and
Query/Sample accessors. Both request operations carry complete JSON payload
and a distinct opaque request-proof attachment; both ordinary sample replies
carry their own distinct reply-proof attachment. Compare both buffers at the
actual recipient/caller and in the independent subprocess controller. Include
leading/trailing whitespace, a multibyte JSON marker and the existing integer
above2^53. Provider bytes include non-text values and remain opaque; these finite
fixture constants are not a cryptographic proof/profile or production defaults.

The native API/source checks in the preceding packaging record apply to the
unchanged published1.10.1 graph. The Unicode routing fix concerns resource names;
this fixture retains ASCII routing keys and exercises multibyte payload only.
No protection algorithm, identity port/public schema, codec or crate is added.
Original enqueue/receipt/cancellation/late-worker-death/close distinctions remain
observable and their original errors remain retained. Actual sample reply is
used, while native reply errors remain transport facts without proof attachment.

Smallest failing witness omits/substitutes a request or response attachment and
fails the actual buffer equality/required attachment before crediting receipt.
Run healthy and original-byte conservation controls in debug and optimized,
then invert each transmission direction independently and restore exact bytes.
No authentication, actor admission, fragmented-buffer allocation bound, native
reply-error campaign or full R02/R07/R13 acceptance is claimed.

Expected two existing fixture paths plus the selected PRD; no manifest/lock or
production runtime changes, zero public types. Forecast up to100 net test lines
across Rust/Python (including any independent inversions retained): conservative
cross-owner8435+100=8535 within8600; research4365+100=4465 within4500.
Reject a new envelope/wrapper/codec: native typed builders already own both
transport buffers. Keep the two existing semantic receiving functions as owners
of their respective request/reply observations. Fresh branch from actual main
before first commit, separate from verification-service source.

### Verification witness concrete custody lint checkpoint

The999-line first complete draft passes all three debug runtime controls:
both providers process18 cases each; fake/missing production provider refuses
before creation/export. This is fixture evidence, not production identity.
Strict Clippy initially identifies type_complexity at seven concrete native
record/receipt/constructor sites, large_enum_variant in both concrete protocols
(original raw/result328 bytes beside recipient80), too_many_arguments in the
plain eight-input service and too_many_lines in the two temporal controllers
(109 logical lines each through actual native receiving).

Automatically select narrowly located #[expect] annotations with their exact
custody reasons for those four style lints, retaining strict -D warnings for
every other lint and requiring each expectation to remain fulfilled. Existing
ordinary destructuring exposes complete native facts. Boxing adds allocation;
a wrapper/alias hides original owners without a semantic gap; splitting the
small complete temporal trace scatters its acquisition boundary. The functions
and concrete types have independent provider/consumer uses. No blanket
allowance, semantic lint relaxation or production/public API change is allowed.
Resolve remaining ordinary style issues directly, including synchronous record
lookup lifted into the selected async service closure, module imports and owning
generated-method UFCS. Actual diagnostic sites/source hashes retain their own
evidence; a compiler/style complaint is no architecture or feature-denial proof.

### Native attachment negative custody detail

Both original positive transport profiles pass their16 cases and strict Clippy;
formatting passes. Before inverted attachment tests, preserve an unexpected
worker-failure observation by acquiring that worker's actual native close and
exit1 before rejecting its trace in the existing Python observer. Forecast
nine additional controller lines, still within the100-line test allowance.
This makes missing-request/reply-proof error custody visible; other workers
may still be deliberately killed after a negative and receive no cleanup credit.
A failed close remains a separate failure and cannot count as successful close.
No network authentication is inferred from the fixture proof bytes.

### UTF-8 owning correction delivered; successor separately selected

Controlled PR4 https://github.com/devrandom-labs/bombay-zenoh/pull/4 reviewed
exact6c7bb4bf4ab77741f239c47354c4dc85df0a5621 and merged through the normal
PR after required Controlled TLS dependencies38058616998 passed14:34:37Z.
Actual merge4bbd1d6f04f6512de9bf675b74db1dd832f5bb14 at20:33:26Z
2026-10-10. The preserved CI log /tmp/bombay-zenoh-utf8-final-ci.log confirms
actual Unicode and declaration-pressure owners in both profiles and two real
configuration Miri tests. Review commit/custody and local inversions remain
separate. Source merged is not published dependency correction yet.

Adopt the source-grounded successor recommendation: publish only top-level
bombay-zenoh1.10.2 with eight unchanged exact published dependency identities.
The top owns this fix; no reverse production dependency requires its version,
and bumping/rebuilding all nine would replace immutable archives unnecessarily.
Use the explicit single-package archive/publication path and unchanged existing
credential SDK, retaining initial-cohort/resumption semantics separately.
Proposal /tmp/bombay-zenoh-successor-release-proposal.md SHA
2fb012b4f1e15f885b6f9bdf717c821ee3326a67e544ddd1748c8e866fe14ee8
records11 existing paths, forecast110–190 net script/verifier/docs lines and
220 ceiling before recount; production Rust/public types/dependencies0.
Narrow both locks: only top source version changes; all other complete records
remain identical. Audit one current-source successor archive plus eight exact
published archives/checksums/original VCS instead of falsely assigning current
VCS to them. Registry availability, exact checksum and ordinary registry-only
consumer remain required. No publication or release-preparation pass claimed.

### Integrated service and transport witnesses

Copy the independently prepared1054-line verification fixture byte-exact into
the coordinator tree: SHAabc2659e5d217cc330877621e15a6ac689f9be8029934b3de84e45c2e3b107e7.
Two actual public Application requestors emit typed verification Actions, receive
original distinct provider results through required concrete constructors, and
emit genuine typed processing Actions. Eighteen cases/provider retain original
allocations/correlation/origin/scope and original malformed-JSON/oneshot errors.
Fake/missing production startup retains original cold input and refuses before
creation/export. Final Stop lanes and complete native/both notification receipts
are acquired before semantic assertions. Scheduled provider proves real oneshot
transfer/closure with sender before await, not delayed/concurrent verification.

Three controls pass both profiles; strict owning Clippy both profiles and fmt
pass. Five semantic inversion pairs—scope omission, unavailable-to-invalid,
always-deny accepted, fake fallback and missing fallback—fail after native
acquisition, each exact restoration passes. Five static denial pairs at actual
service calls: missing constructorE0061, wrong constructor/projectionE0308, both
receipt DeserializeE0277 and private constructionE0451. An earlier predicted
E0271 was harness setup, excluded from static credit. Exact commands/artifact
custody: /tmp/bombay-verification-service-final-campaign.json; inverse-campaign
and static-final-campaign retain separate source/diagnostic provenance.

Native transport extension is copied exact0aaac48c04fc64d282904d5886465f60f668c45005a5a5dfdc7b1c50652d28db;
controller3d89e5248a582dde7a0f6ae86c033a3b824d313f7809d0d2b98c0792355b282c.
Opaque distinct request/reply buffers and complete JSON whitespace, Unicode and
integer above2^53 survive actual TLS peer/router-client query/publication.
Healthy/restored each profile has12 ordinary complete-close cases plus four
intentional late-death close refusals. Omit request attachment:12 actual worker
failures/profile; omit reply:8/profile, each affected worker native close and
exit1 acquired before independent trace rejection. Other negative workers are
killed and receive no cleanup credit. Original byte-equality laws remain.
Evidence /tmp/bombay-native-proof-positive-checks.json and inversions.json.
Rust test delta+65/-20/net45; controller+14/-1/net13, total58 within100.

No production runtime, public provider API, cryptography, replay, resource limit
or full R01–R25 acceptance is claimed. Source review, combined checks, required
CI and actual research PR delivery remain gates. Root coordinator owns shared
manifests/locks/documents; separate agents verify opposite contributions.

Verification campaign command provenance: healthy/final/inverse/static JSON
records inner subprocess Cargo argv. Actual producing launcher was
`nix develop /Users/joel/orca/workspaces/bombay/main-2 -c env
CARGO_TARGET_DIR=/Users/joel/orca/workspaces/bombay/main-2/target
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
CARGO_BUILD_JOBS=2 python3 -`, with orchestration script on stdin and cwd
/tmp/bombay-verification-service. Every recorded Cargo subprocess ran in that
pinned shell/environment. Standalone initial/refined Clippy used the identical
launcher/environment followed directly by Cargo. Single-file formatting used
`nix develop /Users/joel/orca/workspaces/bombay/main-2 -c rustfmt --edition 2024
crates/bombay/tests/verification_service.rs`; final workspace fmt used recorded inner Cargo argv.
No release-debug override was set; selected workspace release policy applies.
Observed rustc1.99.0(b940084d7), Cargo1.99.0(5f94df478). Provenance clarification
does not rerun or retrospectively change earlier commands/results.

Combined-source checks pass on /tmp/bombay-remote-next-contracts: pinned Cargo
build workspace, test workspace including docs, all-target tests, rustfmt check
and strict workspace/all-target Clippy. Exact full outer commands/counts/source
hashes are in /tmp/bombay-next-contracts-combined-checks.json. Opposite-source
independent agents find no source/minimization blockers in either retained
witness, within the explicitly limited research claims. Verification provenance
clarification above resolves the inner-argv receipt issue without rerunning that
campaign. Production/public API remains unchanged; do not mark full acceptance.

### Service/proof research final change record

Stage against actual mainfbc714c: seven paths, zero untracked; production
+0/-0/net0; tests+1133/-21/net1112; manifest/lock+4/-0/net4;
public API+0/-0 types and zero new methods. Documentation +490/-1/net489.
Cumulative root against a9c5b7d4:77 paths, production+1725/-475/net1250,
tests+8192/-661/net7531, manifest/lock+2990/-3/net2987,
defined public types+5/-0; documentation +4490/-88/net4402.
Cross-owner retained test bound8347/8600; research4277/4500. Source snapshots
and generated lock inventories remain separately classified. Complete tracked/
untracked measurement algorithm and per-path hashes are retained in
/tmp/bombay-next-contracts-measure.py, stage-checkpoint.json and cumulative-checkpoint.json.
Required CI and actual PR merge remain unproved at this checkpoint.

### Successor release preparation checkpoint after submitted research head

This append is later evidence, outside submitted PR335 head73f8a12. Controlled
source starts actual main4bbd1d6f before any release commit. Prepared11 existing
paths: script/config/docs+112/-41/net71, within220; manifest/lock
five+1/-1 substitutions, production Rust+0/-0, public Rust API+0/-0.
Complete worktree zero untracked. /tmp/bombay-zenoh-successor-prepackage-checkpoint.json
records exact path hashes; /tmp/bombay-zenoh-successor-narrow-lock-proof.json
proves only top source version1.10.1→1.10.2 and366/305 other complete records
unchanged. No dependency or initial-cohort/resume/credential-SDK source change.

Existing audit gains an explicit closed release-kind selection; historical
default stays initial-cohort. Successor mode acquires/hash-checks eight actual
published archives before normalized-manifest, source, clean original VCS and
29-edge proof; only the new top archive may enter the credential ledger.
Registry metadata/download receipts and expected source are recorded separately
from actual later source verification. Package/verify cannot imply publication.
Publication refusal retains priority over subsequent registry receipt success.
Independent read-only review finds no blocker in this bounded preparation.

Pinned shell syntax and historical release-cohort tests pass. Historical full
archive audit passes all nine original artifacts,29 checksum edges and existing
lock/manifest inversions using actual original e824 source. Initial invocation
used the parent artifact directory and failed missing-file setup; corrected
/tmp-crate directory passes. Both logs remain, no initial pass credit.
Commit the reviewable preparation to obtain a clean exact-source Cargo package;
actual successor package/source/archive inversion checks, full matrix/required
CI, merge, registry publication and registry-only consumer remain unexecuted.

### Successor immutable-archive reader regression before correction

Actual pinned Cargo package succeeds at clean7d58f613; original candidate SHA
2b474d12734f28219507a7b7f739a33c12ebac2dfe01b8d919ca5b79167fe88f.
Actual successor audit downloads/validates eight registry archives, verifies
all source/VCS and29 edges, and emits only the top candidate in its ledger.
These passes do not establish publication.

A new independent inversion changes only the gzip timestamp header in an actual
published dependency archive: tar members, Rust/source manifests and original
VCS remain identical, but the immutable published checksum differs. Direct
existing verify_archive unexpectedly accepts it; baseline exits1 at the named
exact-archive custody law (/tmp/bombay-zenoh-successor-immutable-baseline.json/.log).
The acquisition path checks initial downloaded bytes, but its later reader also
needs this check rather than assuming source equality implies archive equality.
Select a four-line owning reader checksum guard for successor dependencies before
member inspection, and a standard temporary-directory regression in the same
auditor using that real owning reader. No replacement parser, credential change
or dependency is needed. Historical original-cohort reads remain unchanged.
Forecast up to30 additional script/test lines, within220. Repackage from the
new clean commit and compare source/VCS/registry closure again; earlier7d58
archive retains its own hash and cannot represent the new commit.

Archive custody refinement before further verifier edits: use standard BytesIO
to parse the same immutable bytes whose checksum was checked. Capture expected
candidate checksums before source audit and pass them to later lock/source reads;
keep published dependencies pinned to their immutable registry hashes. This
prevents a second path read from silently becoming a different artifact between
checksum and parsing. Re-reading a restored inversion is explicit revalidation,
not a new authority. No custom parser or verified-archive wrapper is needed;
extend the existing ordinary reader with an optional expected checksum and
reuse existing source/lock checks. Forecast up to15 further lines, still220.

### Public capacity-attempt wake-fault prerequisite decision

The selected Communication0.1.4 owner publishes a user item before invoking its
registered consumer waker. A waker can unwind after actual publication, so loss
of the try_send return cannot establish refusal. Automatically select a small
public Application witness using two actual ExternalActors: one allocated
service sender, one exact service recipient with its real receive future polled
using a standard panic Wake implementation. Keep that borrowed future alive
through the actual sender try_send, catch and retain the original typed panic,
then poll/acquire the actual queued original with exact allocation and sender.
An ordinary root actor must independently process a useful Actions reply before
full native/both-notification acquisition. No custom Engine host or fold I/O.

This tests the public external endpoint seam, not injection into a Counter's
Tokio-owned actor-task waker and not a full remote R08/R12 witness. The replay
reservation-before-effect requirement remains mandatory. Use native Poll/Result
and closed typed cause distinctions; do not turn unwind into Full or claim
caller original recovery. Prefer ordinary std Wake and catch_unwind over a
new executor/observation framework or unsafe RawWaker.

Independent test-only stage: one new owning integration path
crates/bombay/tests/admission_wake_custody.rs, forecast≤350 test lines, production
Rust/API/dependencies0. Fresh branch from actual latest main before any commit;
root retains shared manifests/lock/contracts. Automatically raise cross-owner
8600→8800 and research4500→4700 before edits:8347+350=8697 and4277+350=4627.
Actual complete worktree/source evidence must be measured before retention.
Focused debug/optimized and intended inversion/restoration remain gates.
Any actual source-order inversion requires a disposable exact Communication
source copy and coordinator-controlled temporary Cargo patch, with provenance/
lock restoration; do not mutate the registry cache or retained production.

Wake witness refined before native-source inversion: initial228-line positive
source stopped queued-Pending inside Work and receives no post-native negative
credit. Retain actual Poll<Option<User>> as Work output instead, continue useful
root Actions reply and full native/both notifications, then require exact queued
original. Refined244-line source4f81ce0308841d221058f280dba5c082fb1e9527c44093170711151f269527bd
passes both profiles, strict owning Clippy and rustfmt. Use this exact source
for the owning Communication ordering inversion. Original registry source
003b01d35f0589a799ebcf24dde53637b8de0fe3f7423dd542e18f0a8928870e
is copied intact to /tmp/bombay-communication-wake-order-source; only disposable
copy moves actual wake_consumer before try_push. Coordinator backs up exact
manifest/lock and adds only a temporary same-version path patch in the isolated
wake worktree. No cache edit/version change/retained production choice.
Original/all other complete lock records and manifests must restore before final
controls or integration. /tmp/bombay-admission-wake-override-preparation.json
records origin/byte/source custody; no mutation outcome is yet credited.

### Submitted research Linux artifact custody

PR335 head73f8a12 remains unchanged while required Nix runs. Both actual Linux
native TLS jobs pass at run38084734427; downloaded debug/release artifact
campaigns each contain16 unique layout/operation/outcome cases. Independent
inspection proves12 ordinary complete-close controls and four intended caller
late-death close refusals per profile. Twelve recipient observations preserve
whole JSON whitespace/Unicode/large-integer bytes and opaque request proof;
eight caller receipts preserve exact reply proof and protected text. Native
invocation milestones retain potentially-transmitted-before-return ordering.
/tmp/bombay-pr335-linux-native-proof.json records original artifact and binary
hashes; this is transport custody, not identity or actor admission acceptance.

### Successor final candidate and inversion checkpoint

Clean corrected head e7c8d2df yields actual top1.10.2 package SHA
489ee61e4da7f2f8f7637e011a2626bf365b8d0490fd2a8a97641e3b7b78e5d9.
Source/VCS157 Rust files, eight immutable published dependencies and29 registry
edges pass the corrected same-byte archive reader. Historical original nine-
package audit and four release-cohort tests also pass. Ten additional actual
archive/lock inversions refuse old Unicode source, wrong version, dirty/wrong
VCS, path/wrong-version edges, missing/duplicate closure, Git source and wrong
checksum; each restored actual candidate passes. Immutable gzip-header mutation
is now rejected despite identical members; the retained owning regression runs
inside successor audit. /tmp/bombay-zenoh-successor-final-package-proof.json,
final-archive-audit.log, final-historical-audit.log and archive-inversions.json
retain separate provenance. Eleven paths, script/config/docs+150/-50/net100,
Rust/API0, within220; both narrow locks preserve366/305 unrelated complete
records. Local complete matrix and required PR5 CI38085801087 are running.
Candidate package/audit does not mean merged or published; publication must
repackage exact merged main and record its distinct actual immutable archive.

### Successor complete local matrix and wake restoration

The complete pinned-Nix ci/verify-release.sh exits0 at clean e7c8d2df. Both
profiles exercise credential tests, actual TLS trust/open-close, shared Syn
structure, declaration-pressure subprocesses, Unicode owner regression, strict
profile/consumer Clippy and consumer execution; actual final successor packaging
and archive audit also pass. Full outer command/script/log custody is retained
in /tmp/bombay-zenoh-successor-full-matrix-proof.json. This local command does
not run Miri; required remote CI separately owns its real Miri execution.
Independent source/artifact review is posted at PR5 review5480816581 on exact
e7c8d2df. Required CI, main merge and publication remain pending.

Public wake-order inversion succeeds as a falsifier in both profiles: actual
Communication wake-before-publication exits101 at the named queued-original
assertion after complete native/both-notification acquisition and independent
root Actions reply. No setup/timeout failure; actual mutant artifacts and exact
command/source/lock records are in wake-inversion-campaign.json. Coordinator
restores Cargo.toml and Cargo.lock byte-exact, confirms no retained Git diff,
and independently rechecks cached Communication source003b01d unchanged.
/tmp/bombay-admission-wake-restoration-proof.json records this restoration
before final positive/lint/fmt controls, which are now running.

### Restored public wake witness retained-source review

Final244-line source4f81ce03 controls pass debug/optimized after byte-exact
restoration, plus strict owning Clippy/fmt. Actual Cargo artifacts again select
registry Communication0.1.4; restored cached binaries equal the earlier fresh
healthy binaries, rather than claiming new rebuilds. Original manifest5b73f388
and lockaa2b8c7f remain unchanged. Exact commands/log/artifact custody is in
/tmp/bombay-admission-wake-restored-campaign.json. Independent opposite-source
review checks both inversion/restored logs and finds no source/minimization
blocker within the endpoint-waker law. No historical Communication defect,
actor-task-waker injection, repeated-wake law or full remote acceptance credit.

Root copies the restored test byte-exact into local combined source beside the
submitted verification/native-proof witnesses. Required combined build/tests/
all-targets/fmt/Clippy are now running; PR335 head remains73f8a12, with this later
untracked test and PRD records kept outside that submitted head. Deliver them
from a fresh actual-main branch after PR335 merge. No production/public API or
retained manifest/lock change. Cross-owner tests including the244-line witness
and72-line extra successor inversion runner:8663/8800; research4593/4700.
/tmp/bombay-successor-archive-inversions.py is separately accounted test research,
not production Rust or retained CI. Exact stage and cumulative records remain
required before the next commit.

### Resource-interest reclamation prerequisite before quota design

Independent fresh source inspection at controlled e7c8d2df identifies an
unexecuted retention candidate: register_expr_interest retains resources in
remote_key_interests; interest_final removes that entry and native hat interest
without calling existing Resource::clean. Actual face closure cleans mappings
and local interests, but does not explicitly drain/clean remote_key_interests.
Resource::clean owns parent-tree unlink; reference removal alone may retain
parent/child cycles. This is source evidence, not an executed defect or a quota.

Automatically select the smallest owning regression before any production
repair: extend only existing zenoh/src/net/tests/regions/interest.rs using its
genuine no-runtime gateway harness and Future + KEYEXPRS. Capture Weak resources
and borrowed tree node/full-expression-byte inventories, without cloned strong
references distorting native cleanup. Exercise finalization, repeated interest
ID against a distinct expression, two faces sharing one resource, and actual
DeMux::closed followed by dropping caller face ownership. Native map/face
removal, baseline inventories and original weak expiry are independent oracles.
Retain another face's resource until its last owner retires. Compare both
profiles on unchanged source first; only a named failing law can authorize a
later minimal repair and deletion-of-cleanup-call inversion.

Alternatives are implementing quota first or treating source inspection as
proof; neither establishes reclaimable ownership. Reuse native harness/Weak/
Resource owner rather than a second registry or custom budget object. Test-only
forecast one existing source path,150–180 lines, no production/API/dependency/
configuration edit. Automatically raise cross-owner test cap8800→9000 and
research4700→4900 before test edits:8663+180=8843;4593+180=4773. Any growth beyond
180 or production repair needs its own recorded recommendation/checkpoint.
Fresh isolated branch starts latest actual controlled main before commits;
root keeps manifests/lock/release integration. This campaign cannot prove TLS
subprocess reclamation, message bounds or full R14/R18/R19 acceptance.

### Combined wake-source local verification

Five pinned combined-source checks pass with exact restored244-line wake source
beside PR335's unchanged service/native-proof witnesses: workspace build,567
workspace/documentation tests with one ignored,566 all-target tests with one
ignored, workspace rustfmt and strict all-target Clippy.
/tmp/bombay-wake-combined-checks.json records exact outer commands, source/lock
hashes, exits and per-binary counts. The owning selected lock93611085 remains
PR335's original graph. Nextest exits0 on the same source:566 passed, one skipped; exact command/log
custody is in /tmp/bombay-wake-nextest-proof.json. Actual reviewed PR/required
CI/main delivery of this added witness remain gates.

### Successor reviewed source merged; publication still open

Controlled PR5 merged65e97beaf08ef532fc58421d778edb9e8f4b5721 at
2026-10-10T21:24:34Z, exact reviewed head e7c8d2df, after required
Controlled TLS dependencies38085801087 passed. Full actual CI log confirms
Unicode controls and declaration pressure in both profiles, successor archive
inversions/restored controls and two actual shared-Syn Miri tests.
/tmp/bombay-zenoh-successor-final-ci.log SHA
0c84889c0f53e6f054ad8ba34914a8aff22b86fe374e67bca0b05ffcb31aed70
retains full log; review5480816581 names exact source. Normal match-head merge
used no bypass. Current exact-main CI38087509710 is running; the release owner
requires that main check before packaging/publishing. Top1.10.2 remains
unpublished; local e7 archive cannot represent merged main's later VCS archive.

### Interest retirement original-source laws executed

Exact175-line test source5b9f4c6e96bafc4610e072604313dcddbb7f60f2de654f94639888f8f99df89a
on actual controlled main4bbd1d6f leaves all production/manifests/locks unchanged,
with touched production bytes equal corrected e7 successor. Existing owning
finalization controls3/3 pass first in each profile. All three new regressions
then fail101 at intended original-Weak custody assertions after finalized-map
removal, other-owner preservation or actual native face removal. No setup or
unsupported-transport failure; finalization inventory snapshots precede native
close. Debug/release-test-feature-campaign.json and interest-source-proof.json
record commands/source/artifacts. Initial zero-match command omitted the owning
test feature and receives no regression credit. Pinned formatting passes.
Production repair remains unimplemented and needs a smallest owning proposal;
this establishes neither quota enforcement nor TLS subprocess reclamation.

### First actual counter-command integration recommendation

Automatically select a fixture-only two-public-Application Increment/Read
witness before retaining any network abstraction. Preserve verification_service
source byte-exact, including its18 cases. Reuse native TLS configuration/session/
matching/declarations/byte attachment and controller cleanup; add a private
counter_application module and explicit counter caller/recipient comparison
roles. Parameterize existing native request construction only where it deletes
duplicated calls. Importing a complete test, introducing a generic provider
trait/common trusted receipt or replacing owners would add unjustified surface.

Fixture JSON request has configured scope, supplied request identity and closed
Increment{amount}|Read command; Read has no invented amount. Reply has independent
reply purpose, exact matching scope/request/operation and value. Existing Serde
checks reject unknown metadata and ordinary checks refuse command/operation
disagreement. These are private finite vectors, not production wire grammar,
version policy or canonical proof representation. Both independent deterministic
providers own distinct private RequestReceipt and ReplyReceipt with exact known
byte/proof tables; record and oneshot implementations remain independent. No
cryptography or provider-common certificate. Request verification alone cannot
verify a different reply schema.

Use initial balance41, two fixture identities1/2 and increment1/read vectors42/42.
Caller pure Actions emit concrete local service requests; receiver service
acquires actual native ingress and exact protected/proof allocations, verifies
request and independently configured scope/op, and applies fixture Granted/time
9<deadline10 followed by existing exact-recipient try_send without an intervening
await. Receiver pure Counter processes exactly increment then read and emits
its actual typed reply in Actions. Service acquires that reply before protecting
and publishing it. Caller separately verifies reply and uses a required concrete
constructor to transfer its original ReplyReceipt exactly once into Behavior,
whose complete native result is its sole eventual owner. Recipient Work retains
original RequestReceipt; caller Work retains separate original raw buffers and
transport facts. Concrete command/value projections are explicit, not cloned
verification receipts. Actual local User::from is allocated service provenance,
separate from the verified configured remote principal.

Independent final oracle requires receiver native balance42/ordered processed
trace and caller native original replies42/42 plus useful typed Actions
acknowledgement, after acquiring complete Work/native/final Stop/both notification
products and actual Session close/worker exit. Echo inversion emits valid known
reply42 while bypassing actor admission/Actions processing; caller verification
may still pass, but complete cleanup must precede the failing receiver native
balance41/empty-trace assertion. Required-constructor, wrong provider receipt,
raw DTO, private/Deserialize receipt and unrelated local protocol denials need
actual expected diagnostics and compiling positives. Two providers × both
approved layouts × query/publication run both profiles; no product operation
default is selected by those comparisons.

Forecast seven paths: private Rust module plus existing native binary/controller,
CI≤15 lines, standalone manifest/lock and this PRD. Whole added Rust/Python
ceiling1400, warning1300; production runtime/publicAPI/root Cargo files0.
Automatically expand cross-owner tests9000→10500 and research4900→6300 before
code:8663+175+1400=10238 and4593+175+1400=6168. CI/config and generated lock
records remain separately classified; standalone generated-manifest/lock cap
4200 replaces3500, retaining all unrelated complete locked records. Ordinary
Application introduces existing Bombay/Engine/macro path packages only for this
source witness; normal published-consumer proof remains separate. Add the already
selected thiserror2.0.20 edge only if its derive is used. The standalone workspace
must repeat exact Timers13e884 patch and verify every actor owner against the
root lock; dependency-root patches are not inherited. Coordinator alone edits
manifest/lock/CI/shared exports.

Author independently while release delivery runs. Existing registry1.10.1 may
be used solely as an explicitly attributed ASCII-fixture composition comparison;
final retained transport support must select and verify actual published1.10.2
without dependency patches. No source-only comparison receives successor
publication credit. Replay/reservation/conflict/eviction, session/generation/ID
issuer and exhaustion, public provider/schema/clock/cache coordination, numeric
limits and remote authorized stop/held child scope remain open. Two finite
correlations do not prove R12. No full R01–R25/AUTH1/NET1 completion is claimed.

### Counter prototype graph reconciliation before source lowering

Fresh research/counter-command-integration branch starts actual mainfbc714c,
fast-forwards reviewed preparation73f8a12 and then actual main8cb82e5 before
any author commit. Coordinator adds only standalone Bombay path/thiserror edges
and exact Timers patch; root runtime Cargo files stay unchanged. Pinned offline
metadata adds the expected owning packages and preserves every prior300
external complete record; its own fixture dependency list changes as intended.

Initial equality proof incorrectly required complete new path-package records
to equal workspace lock records, including dev/optional feature edges. This
assertion fails and receives no graph-alignment credit. Inspection separates
legitimate omitted dev/feature edges and disambiguation from a real source
difference: standalone existing Syn3.0.4 would compile the newly introduced
Bombay/Behavior macros, whereas selected root uses3.0.5. Automatically select
a narrow Syn3.0.4→3.0.5 update to align actual syntax ownership, using the already
verified selected root package/checksum. Staying3.0.4 would require a distinct
macro transitive-source proof. No serializer/identity/transport change or broad
lock refresh. Record every affected dependency-edge spelling separately;
preserve all other compatible locked package identities/checksums/edges.
Standalone comparison still selects published controlled Zenoh1.10.1 only;
final support selection awaits actual published successor1.10.2.

### Research prerequisite actual PR335 delivery

PR335 merged8cb82e5b785fcd04b0d991c51758fba6c75096f6 at
2026-10-10T21:27:53Z on exact reviewed head73f8a12, after all six observed checks
passed: Nix38084734427, both native TLS profiles, CodeQL38084734417/summary
and Deny38084734437. Review5480747684 names exact head. Flake and Driver/Observe
fuzz steps passed; optional Observe Miri was skipped and receives no credit.
Full log /tmp/bombay-remote-contracts-pr335-final-ci.log and before-merge-proof.json
retain actual statuses; downloaded32 native case observations are independently
checked in pr335-linux-native-proof.json. Normal match-head merge used no bypass.
These research prerequisites do not mark AUTH1/NET1 or any full R witness merged.

### First counter prototype selected graph

Pinned offline metadata and narrow Syn update produce standalone lockbc14c553:17
new package identities/checksums equal selected root, including exact actor
owners and Timers source;290 prior external complete records unchanged. The
only updated existing package is Syn3.0.4→3.0.5; nine previous macro dependency
edge spellings change accordingly. Three existing compatible dependency nodes
remain at the already tested native graph versions rather than root versions:
cc1.4.4, smallvec1.15.2 and bitflags2.13.1. New package edges may omit workspace
dev/optional feature edges; their identities/checksums stay exact.
/tmp/bombay-counter-comparison-lock-proof.json records every difference, rather
than claiming complete edge equality across different feature graphs. Initial
strict edge assertion and JSON parsing of shell-banner-prefixed metadata fail
and receive no alignment credit; complete raw metadata is preserved separately
from parsed Cargo JSON. Retained comparison still uses registry Zenoh1.10.1;
successor source/support is not implied. Root source Cargo files are unchanged.

### Interest regression scope expanded before correction

Source refinement shows populated native remote-interest owners in broker and
peer hats; client hat has no insertion and stays untouched. A two-file KEYEXPRS
repair could omit non-keyexpr ownership and peer disconnect. Automatically
select test expansion before any four-path production proposal: keep local
replay test, parameterize shared-final/disconnect over actual remote Client/Peer
kinds and KEYEXPRS/SUBSCRIBERS options, and add None-resource wildcard-final/
repeated-final controls for both real hats. Native Gateway factory supplies the
actual broker/peer stores, no inserted test state. Any cloned native diagnostic
view must be dropped before transitions to avoid perturbing strong-count cleanup.

Forecast65–90 further test lines, total240–265; raise this single-path ceiling
180→280 before edits. Existing cross10500/research6300 ceilings accommodate
this expansion plus pending1400-line counter witness:10328 and6258 respectively.
Original controls then all actual original failures must execute in both
profiles. Production remains unchanged until these independent laws and a
smallest owning correction proposal are recorded. No quota/API/dependency or
transport layout is added.

### Counter terminal native custody control flow

Automatically select ordinary role control flow returning existing
TransportReceipts directly. Each concrete counter worker acquires the complete
Application receiving product, then actual Session.close, then checks the
independent processing law while keeping original provider/raw/native facts
owned through this terminal inspection/discharge. Expected fixture-law mismatch
returns a concrete existing invalid-control diagnostic through native terminal
reporting; it must not panic before close observation. No generic retained-
inspection wrapper/trait or extra runtime task. Explicit record/scheduled
counter comparison roles reuse the existing caller/recipient certificates;
these roles select finite fixture providers, not a public production API.
The original16-case transport path stays semantically unchanged.

### Actual workspace branch and scoped wake delivery

User workspace was clean before switching to new
feat/static-remote-actors-integration from latest actual main8cb82e5. Original
feat/static-remote-actors-implementation branch is preserved. Exact244-line
wake source and all later PRD records are copied without alteration; source
contracts/manifests/lock/flake match the already verified combined source.
/tmp/bombay-workspace-latest-main-branch-proof.json records clean-before-switch
and source/main custody. The earlier isolated coordinator worktree remains
preserved; this workspace now owns shared integration/documents.

This delivery adds only the public endpoint wake-unwind witness and prerequisite
delivery/decision records. Existing selected owners and ordinary public
Application/ExternalActor composition suffice; no runtime/API/dependency or
new semantic wrapper. Independent review, focused original-order inversions
and restored debug/optimized controls, five combined commands and nextest all
pass with the scoped limitations recorded above. Actual required CI and reviewed
PR merge for this new test remain open. Other agents' counter prototype and
interest expansion stay isolated, outside this patch. Expanded interest current
source219 lines has conservative authored-variant bound250 (original175 plus75
new lines,31 replaced); within280. Its extra laws are not yet executed.

### Scoped wake delivery complete change record

Against actual main8cb82e5: three paths, production+0/-0/net0,
tests+244/-0/net244, documentation+467/-0/net467, manifest/lock0, public
API+0/-0 types/methods. Complete tracked/untracked measurement includes the
added test. Cumulative against a9c5b7d4:78 paths, production+1725/-475/net1250,
tests+8436/-661/net7775, documentation+4957/-88/net4869,
manifest/lock+2990/-3/net2987, defined public types+5/-0.
/tmp/bombay-wake-delivery-measure.py and stage/cumulative-checkpoint.json retain
per-path counts/hashes. Byte-equality transfer proof compares existing source,
manifests, compiler diagnostics and exact added test against the already fully
verified combined worktree; base73f8a12 tree equals actual main8cb82e5 tree.
No repeat or new source claim is inferred from moving the branch. Required
remote CI/review and actual merge remain outstanding for this three-path patch.

### Counter first compilation privacy checkpoint

First lowered counter draft compiles far enough to report exactly two E0446
privacy errors: generated pub(crate) Counter/Client send products expose private
CounterReply/RawRequest types inside the private module. This is an incorrect
copied root-test visibility spelling, not a new domain/composition gap.
Automatically select inherited private visibility for both existing generated
send products, matching the preselected private fixture protocols. Do not widen
raw DTOs/receipt fields, add wrappers/aliases, change the macro or public API.
/tmp/bombay-counter-initial-debug-build.log retains actual diagnostics; no
positive compilation or static denial credit yet. Private NoBirths roots reuse
existing run_with::<Never> composition instead of an unnecessary child-result
wrapper. Actual missing-host errors retain original TryCurrentError until
Session.close; cold inputs discharge explicitly only after that close.

Current first draft1078 Rust lines plus native bin60 and controller70 is1208
net test lines within1400, before final custody assertions and inversions.
Verification_service sourceabc2659e remains byte-exact. Counter agent releases
the controlled Cargo slot for expanded interest original-source baselines;
independent source edits may continue, compilation resumes only after handoff.
This later evidence remains outside submitted wake PR336 head6af82e1.

### Counter custody and interest setup corrections before execution

Automatically select retaining actual incoming Queries/Samples and caller reply
Samples in separate native Work fields through Session.close. Newly owned parse
buffers are copies; only same-process moves preserve their allocation identity.
Retain an actual TrySendError<CounterCommand> on refusal rather than dropping its
rejected command before cleanup. Retain emitted/processed Users and caller
acknowledgements in Work receiving fields; compare origins, request identifiers
and observed acknowledgements after native results, both notification products
and Session.close. Provider verification and routing remain pre-admission.
These corrections add no type, authority, dependency or production policy.

Select one private request_ingress function in the existing native bin, used by
both old and counter recipients, replacing the byte-equivalent declaration
duplicate. Existing FIFO, locality and congestion settings remain exact.
Rename the ordinary exhaustive native result projection completed_actor to
expose its domain. It validates every residual field before explicit discharge;
no wrapper or alternate result contract. Forecast custody/assertion additions
55–85 lines and duplicate removal about26, from actual1201 net, below warning1300
and ceiling1400. All old16 controls still require rerunning.

Expanded interest baseline has no new nine-law or optimized credit: original
three debug finalization controls passed, Client wildcard passed, and Peer
wildcard failed at setup in Hat::Uninit.region before resource assertions.
Peer Hat::new creates Uninit; owning Gateway.init_hats requires the ordinary
HarnessBuilder Runtime initialization. Automatically remove start_runtime(false)
only from the three expanded remote-matrix controllers and reuse its existing
default Runtime with empty listen/connect endpoints and disabled multicast,
adminspace/plugins and asynchronous tree computation. Keep original local replay
without Runtime. Alternative manual hat initialization would duplicate owning
setup and is rejected. No production/API/dependency/default changes; test source
219→216. Excluded receipt remains interest-expanded-baseline-campaign.json.
Run actual original controls and all nine failures plus two None controls in
both profiles before selecting production correction. Resource agent owns the
controlled Cargo lane until explicit handoff; counter source edits may proceed.

### Expanded original interest laws and independent acceptance audit

Initialized original-source campaign now executes both debug and optimized:
existing finalization3/3 and wildcard None/replay2/2 pass first in each; all
nine local/shared/disconnect deallocation regressions fail101 at their intended
original-Weak custody assertion. Both real Client/Peer hats and KEYEXPRS/
SUBSCRIBERS paths execute. Source216 lines SHA
215ec62e51a8de078325e609727d2c499665ef310f623acff94cb37c7a7c85a6
on actual main65e97beaf/package1.10.2; production and graph unchanged.
initialized-baseline-campaign.json retains six actual commands, names/counts,
source/artifacts and log hashes. Earlier Uninit setup and zero-match receipts
are excluded. Resource Cargo lane explicitly released to counter integration.

Independent read-only acceptance audit confirms every full R01–R25 remains
open. Next replay integration needs separate actual mailbox-admission outcomes,
receiver command trace and business balance, with a real same-user-lane barrier
before terminal assertions. Balance alone cannot prove no duplicate admission.
Required separate adverse cases: duplicate while pending; admitted request with
lost reply then retry; same identity/different validly authenticated command;
fresh proof or valid alternative JSON spelling; exact Full payload followed by
known revoke/expiry; reservation surviving publication/wake uncertainty; valid
authenticated wrong independently configured scope. Mutated bytes with an old
proof test byte binding, not permission; duplicate replies test neither unique
admission nor unique execution. These are planned assertions, not new API,
replay retention, identity issuance or numeric limit decisions.

Native QueryTarget::All/ConsolidationMode::None comparisons still require a
future overlapping-responder falsifier before exact-recipient credit. Bounded
pending originals, verifier work, replay memory, message/proof/name lengths,
restart/session freshness and capacity-plus-one witnesses remain prerequisites.
Resource deallocation repairs establish no quota. Preserve executable feature
inversions and campaign receipts in repository/CI artifacts before full
acceptance; temporary evidence paths alone do not satisfy durable evidence.

### Exact-main successor publication and counter custody review

Controlled exact-main run38087509710 passed on65e97beaf, including both-profile
Unicode/declaration campaigns, actual successor archives and two executed Syn
Miri tests. Full log SHA
2978a9fb58d3687829636232a5595d054a5b2de63a551f40b6d7b3a5c62dd6f1
is retained as successor-main-ci.log. Current-main and the owning successful
GitHub check were rechecked before dispatching authorized publish workflow
38089508115 on that exact source at2026-10-10T21:56:05Z. Publication remains
unproved until actual release/registry receipts and registry-only consumer pass.

Independent counter source review found real premature-disposal paths before
any runtime credit: ingress declaration error skipped Session.close; by-value
receiving shape denial replaced acquired native/notification/close facts; raw
requests, Samples, outgoing commands and provider receipts entered retained
vectors only after later fallible work. Fresh debug build passes with zero
warnings, but compilation does not excuse these ownership defects.

Automatically select standard control flow: declaration errors retain their
original error in existing TransportReceipts beside actual Session.close;
each acquired original enters its existing owning Work collection immediately,
then later fallible code borrows it. For the fixture's healthy native shape,
borrow the complete receiving tuple and acquired close result for an explicit
post-close observational shape assertion before consuming the proved product.
An unexpected native shape is a terminal fixture assertion failure after actual
close, not a replacement actor verdict or an orderly-failure campaign credit.
Its original tuple remains owned through that inspection; no generic error
wrapper, erased receipt, public product or alternate retirement owner is added.
Alternative retained error aggregation would invent a new fixture abstraction
without establishing the later production error contract. Echo mismatch remains
a concrete existing post-close control error on the normal native shape.
Forecast30–55 additional lines from formatted1231, at most1286 below warning1300
and ceiling1400. Repeat positive compilation before actual campaign/inversions.

Concrete lowering refines that forecast before edits: retain recipient decoded
RawRequests outside the fallible inner Work future until exact pop/try_send;
retain each provider's pending ReplyReceipt in its own existing Vec until scope
passes and the required constructor takes it; retain an actual rejected
SendError<ClientCommand> on delivery failure. These are affine existing receiving
lanes, not a provider-common envelope, cloned authority or new product/type.
Borrowed parsed values cannot replace their original custody. Automatically
select these necessary retained lanes over early disposal or a common wrapper.
Revised correction70–105 from1231 forecasts1301–1336: explicitly crosses warning
1300, remains within already selected1400 ceiling. No production/API/dependency
growth. Final measurement must include all retained variants and static probes;
do not broaden silently if1400 is exceeded. Echo must bypass admission and
Counter Actions wait, return a valid known42 reply, and reach the independently
named receiver native balance/trace mismatch after close, rather than a timeout
or an earlier processed-reply shape assertion.

### Successor registry publication and first actual counter comparison

Authorized release38089508115 completed successfully on65e97beaf, with original
primary publish exit0, registry-before404, then actual top1.10.2 upload at
2026-10-10T21:58:48.874463Z. Registry checksum
97d520b46cf4706e227f3880fd8c17eccaec5dbef2476124f4daad0915204b92
matches actual retained artifact11683317086 and freshly downloaded registry
bytes. Archive VCS is exact merged65e97beaf, with corrected resource source
0780c09f; all eight unchanged dependency copies independently match their
registered bytes/checksums and original e8248182 VCS. All nine are unyanked.
successor-published-registry-proof.json retains complete receipts; publish log
SHA3a44b24e9956e62d3b8a6c14eb9b72295406b94e38d0af8aec844b6ee9b1f8ad.
The earlier local e7 candidate checksum is distinct and retains only its local
packaging credit. Registry-only consumer was prepared as an exact copy without
target cache or source override; verification and final research graph update
remain gates.

Corrected counter source is formatted1224 lines, plus bin67/controller61:
1352 net, exceeding forecast1336 but within1400. First actual peer/query/record
debug comparison passes: receiver41→42 with Increment/Read trace, original caller
ReplyReceipts42/42 and complete both Application/native/notification/Session
close products followed by process exits0. This still selects registered1.10.1
for attributed finite-vector comparison only. Optimized control, other layouts/
providers/transport operations, actual echo and static denials remain open.
Automatically move the independent receiver native balance/trace mismatch before
healthy-only pending-buffer emptiness assertions so deliberately retained
unadmitted echo originals reach the named post-close processing oracle.
Reordering assertions adds no transition/policy or new semantic mechanism.

Registry-consumer selection before edit: use the existing published-copy
configuration consumer unchanged except exact top version1.10.1→1.10.2;
retain its prior lock and every other compatible complete package record.
Cargo must resolve the real registry archive/checksum, with no path/Git/patch
override. Verify normal and both existing maintainer configurations through
pinned Nix, then select that same actual registered top archive in the counter
research graph and rerun its affected composition. Rebuilding a source override
would not establish normal installation and is rejected. No new crate, transport
feature, public API or default. Coordinator owns both manifest/lock updates;
controlled Cargo target remains exclusive with an explicit author-to-root handoff.

### Bounded interest-retirement owner correction selected before production

Automatically select the smallest native owner correction now that all nine
original failing laws and five controls execute in both profiles. Blocker:
finalized/disconnected native interest references are removed without invoking
existing Resource::clean after their last owner retires, retaining resource-tree
nodes/name bytes. Reuse actual dispatcher/hat ownership and its existing locks;
no second registry, wrapper, quota, task, lock or unsafe mechanism.

Expected production files: dispatcher/interests.rs holds the removed key-interest
original, performs existing native unregister/northern routing, independently
cleans the returned native original, lets that reference retire, then cleans
the held key original even when unregister returns None; dispatcher/face.rs
drains/cleans remaining remote key interests after hat retirement before face
removal; broker/mod.rs and peer/mod.rs clear their existing local declaration
owners then drain/clean their existing native remote-interest originals.
Client hat stays untouched because it owns no inserted remote-interest store.
No Option::or substitution between coexisting originals, broad tree sweep,
cloned inventory, new public type, dependency or API. Existing callback/routing
unwind uncertainty is preserved; this repair promises no rollback.

Forecast25–40 net production Rust; hard stage ceiling50, CI additions≤8 and
conservative authored test variants≤280 in the existing interest test file.
Six paths including owning verify-release.sh, no new paths except project
records. Automatically raise old functional controlled-production cap770→810:
recorded758 less later Unicode3 plus maximum50 is805. Including historical
documentation deletion, controlled net725−3+50 is772; root1250 plus that maximum
is2022 within cumulative2200. No new public type/method; retained native owner
code is changed rather than complemented by another abstraction.

Before broadening, prove controls then all corrected9+None2 tests in both
profiles, actual test-feature strict lint/fmt, and five separate omission
inversions: final key cleanup, final native cleanup, disconnect key drain,
broker native drain, peer native drain. Each must fail its intended independent
custody law and restored controls. CI must execute top owning region tests with
transport_tls,test and check that owning test profile; default TLS-only library
commands cannot imply those tests ran. Coordinator alone integrates CI/manifests.
Active-ID overwrite, quotas, real TLS reclamation and complete heap bounds remain
separate prerequisites. Production source edits may proceed independently while
counter/root own Cargo; verification waits for explicit slot handoff.

### Wake witness reviewed and merged; next actual-main branch

PR336 merged2104559072b4321ab55beaea50dfa3a805a6723e at
2026-10-10T22:52:11Z after review5480915687 on exacthead6af82e1 and all
six CI checks. Nix38088677433 passed44m7s with both existing bounded Driver/
Observe fuzz campaigns; optional Observe Miri remains unexecuted. CodeQL and
Deny checks passed. Actual Linux native artifacts11683206330/11683707355 each
contain16 unique cases: twelve ordinary full close outcomes, four late caller
exit−9 refusals, and exact opaque payload/proof custody. Ordinary completed
cases independently show eight request and four reply proof observations;
late-death observations retain separate uncertainty credit. No cryptographic or
actor/replay proof is inferred. Full CI log SHA
02ae881c85bdc30e01c6866f943a4078b68cda705d916baf4e20adde95b5c3a7,
pr336-before-merge-proof.json and pr336-linux-native-proof.json retain evidence.
Normal exact-head merge used no bypass. This delivers the wake test prerequisite,
not full R08/R12 or the networking PRD.

Coordinator fetched actual main2104559 and created
feat/static-remote-counter-integration from it before further authored commits.
The prior integration branch and pending PRD changes are preserved; binary
pending-document patch is retained. Counter/resource worktrees stay isolated.

Normal successor consumer lock updates only top1.10.2 to its actual registry
checksum;305 other complete records (304 external) stay byte-equivalent.
All nine controlled copies have registry sources; the consumer root itself
correctly has no external source. Initial receipt assertion inadvertently
included that root and is excluded; corrected lock proof passes. First Nix
consumer lint attempt failed resolving cache hostname before Cargo; pinned
Nix offline retry passed strict normal lint and downloaded the actual published
archive. Remaining normal/maintainer executions and strict feature checks run
in the same pinned shell with exclusive Cargo custody. No host Cargo fallback.

### Verified registry consumption and early combined source

Normal and both existing maintainer consumer executions pass their actual
configuration/identity assertions; all three strict lint profiles and formatting
pass through pinned Nix. Registry proof now records consumer_verified with six
campaign commands plus separately completed normal strict lint. No source
patch overrides the controlled copies. Consumer unchanged-other lock records305
include304 external; only actual top1.10.2 archive/version changed.

Counter worktree fast-forwarded actual main2104559 before any author commit,
preserving its draft and exact root93611085 lock. Its narrow actual registry
update keeps317 other complete records unchanged. New standalone lock SHA
692c2612237df978fe40c1314746bd09d0c20caa40fc4e0b1a27de1653b08931
and manifest51ea30eb retain the exact Timers patch and current owning source.
Coordinator integrated the five fixture/graph paths early on the same actual-main
branch; early-integration-proof.json hashes every transferred source. Counter
1352-line source still needs final .2 matrix, echo/static denials and combined
source checks. No prior .1 comparison is relabeled as .2 proof. Prepared external
mutant statements forecast≤35 further authored variants, conservative1387 within
1400; their actual execution remains pending.

Four-owner resource correction actual production+30/-10/net20, test+216/-0,
CI+5/-0, six existing paths, public types/methods+0/-0. Existing Resource imports
suffice. Coordinator added the two owning CI commands to each existing profile
loop; shell syntax and whitespace pass. Positive semantic/lint/inverse execution
remains gated on the controlled Cargo slot, now handed explicitly back to
counter. Active-ID overwrite and quota work have no implementation credit.

### Combined counter verification and complete echo cleanup order

Registered1.10.2 counter matrix passes all16 preserved native cases and all8
provider/layout/query-publication cases in each profile before any mutation.
Independent stable-source review resolves all three earlier custody blockers;
its exact sourcecb8b4a65323c1deea046eca2bcd3b0cd462c8c7d7dfc87d41fc7a6ff23f63a4b
has pure folds, distinct private receipts and a native counter trace preceding
ancillary assertions. Unexpected native shape remains a post-close panic with
no orderly-failure credit. Root runs five combined workspace commands through
pinned Nix on a separate target while the author owns controlled Cargo.

Echo source compiles, but full echo execution is not yet credited. The
controller currently closes optional router only after a successful recipient
oracle; an intended echo rejection would abandon/kill it. Automatically select
closing router after caller cleanup and completed requests/replies, before
recipient EXIT/native inspection. This reuses the existing close receipt and
exit assertion, adds no type/state/policy and changes net0–1 lines. All positive
cases must rerun with this fixture order before inversions. Alternative peer-only
inversion proves only two Application sessions; killing router gets no retirement
credit. Old16 native path remains unchanged. Echo must acquire caller/router/
recipient actual close outcomes before the intended independent mismatch.

### Actual echo inverse and combined workspace results

The approved complete-close controller order passes all eight counter cases in
both profiles first. All eight echo failures per profile then reach the named
actual-Counter-processing mismatch: caller and optional router close/exit0;
recipient acquires its native and both notification products, closes, reports
the intended failure and exits1. No timeout, unrelated assertion or forced router
kill receives credit. Echo campaign retains actual mutable source/binary/graph
hashes and all terminal traces; source restores byte-exact cb8b4a65 and lock
692c2612. Five authored echo statements count conservatively toward1400.
Static denials and final restored complete matrices remain pending.

Actual combined source passes all five root commands through pinned Nix:
workspace build; workspace/docs567 passed plus1 ignored; all-target566 passed
plus1 ignored; formatting; strict all-target Clippy. Combined-final-proof.json
records commands, source/selected lock/fixture hashes and every log digest.
Nested transport verification is separate and does not follow from root tests.
No full R01–R25 or production identity/replay/resource acceptance is closed.

### Counter research final verification and minimization

The final restored registered1.10.2 source passes all24 controller cases in
**each** debug and optimized profile:16 preserved native cases plus8 actual
Counter cases (two layouts × query/publication × two explicit fixture providers).
Counter's real final balance42 and ordered Increment/Read trace are inspected
only after acquiring its complete native result, both notification receipts and
actual session closure. Caller consumes both original provider-specific replies
through typed Actions and retains its native result; optional router also closes.
The finite fixture's independent scope/permission/time observations are not
cryptographic authentication, distributed freshness or replay evidence.

All8 echo mutants/profile fail at the intended actual-Counter-processing oracle,
with caller/router exit0 and recipient orderly close followed by intended exit1.
All12 static denials/profile fail with the intended compiler codes: missing
required receipt E0061; provider/raw-DTO/protocol mismatches E0308; four receipt
Deserialize attempts E0277; four private-field constructors E0451. The E0061
case removes the concrete completion function's required ReplyReceipt result;
it does not independently test a missing generic result-constructor parameter.
The previously delivered verification-service comparison owns that latter law.
All restored positives and strict standalone Clippy/formatting pass afterward.

Final source SHA256:
- counter_application.rs: cb8b4a65323c1deea046eca2bcd3b0cd462c8c7d7dfc87d41fc7a6ff23f63a4b
- transport_custody.rs: 02f3c23c8c55cc717b13ffc43855013257122fbc0015ddae67ca9bd771504814
- verify_transport_custody.py: 816245f2050d6ce3530de0004458f46843230df42d2b45b06db69492acd53a00
- standalone Cargo.lock: 692c2612237df978fe40c1314746bd09d0c20caa40fc4e0b1a27de1653b08931

The root93611085 lock, prior verification fixtureabc2659e and delivered wake
fixture4f81ce03 remain unchanged. Both actual compiler artifacts select the
published registry bombay-zenoh1.10.2 with transport_tls alone. Byte-exact
restoration follows all temporary mutants. Retained authored fixture net1352
plus actual conservative temporary variants40 is1392≤1400; the earlier forecast35
was an estimate, not the final measurement. No additional scope is needed.

Reproduction uses the root pinned Nix environment, two Cargo jobs, incremental
compilation off and profile debug information off. In each profile (add
--release for optimized), run cargo build --locked --bin transport_custody with
--manifest-path docs/prds/static-remote-actors/transport-custody/Cargo.toml, then
run python3 docs/prds/static-remote-actors/transport-custody/verify_transport_custody.py
against that Cargo artifact and a distinct evidence directory, inside Nix.
Standalone strict cargo clippy uses the same manifest/locked/bin with
-- -D warnings; cargo fmt uses that manifest, --all -- --check. Static comparisons
use the same owning cargo check --locked --bin transport_custody
--message-format=json in both profiles, inside Nix. Existing native-TLS CI runs
the restored24-case controller, owning lint and formatting in both profiles.
Temporary echo/static mutation campaigns are local research receipts; this
record does not claim they are durable CI acceptance gates for the full feature.

Actual local receipts: /tmp/bombay-counter-final-campaign.json,
/tmp/bombay-counter-echo-campaign.json, /tmp/bombay-counter-static-campaign.json,
/tmp/bombay-counter-independent-negative-proof.json and
/tmp/bombay-counter-combined-final-proof.json. They retain actual selected Cargo
artifacts, commands, output/log hashes, expected outcomes and source restoration.
All five combined root commands pass through pinned Nix: cargo build --workspace
--locked; cargo test --workspace --locked (567 passed,1 ignored); cargo test
--workspace --all-targets --locked (566 passed,1 ignored); cargo fmt --all --
--check; cargo clippy --workspace --all-targets --locked -- -D warnings.

Minimization retains ordinary public Application composition, existing typed
service lanes, actual native query/sample custody and private provider receipts.
It deletes duplicate native ingress declaration; no production runtime, public
product, provider trait, export API, protocol schema, dependency override or
second registry is added. Original refusals and acquired outcomes remain in the
owning fixture Work/native products until session closure. Copied parse buffers
are explicitly copies; allocation identity across processes is not claimed.
Unexpected native receiving shapes panic after closure and receive no orderly
failure credit. No full R01–R25, production identity, replay, resource bounds,
exact global routing or protected remote-stop acceptance is closed.

Final counter submission checkpoint (tracked and untracked, base2104559):
production:+0/-0/net0; tests:+1374/-22/net1352; public API:+0/-0 types.
Stage7 paths: documentation+438/-4/net434; manifest/lock+198/-14/net184.
Cumulative Bombay79 paths: production+1725/-475/net1250; tests+9788/-661/net9127;
documentation+5391/-88/net5303; manifest/lock+3174/-3/net3171; public types+5/-0.
The separately counted controlled correction retains its authorized caps and
exclusive Cargo lane; counter PR delivery is independent.

### Interest correction successor preparation (no release credit)

Under delegated recommendations, select top-level bombay-zenoh1.10.3 for the
verified interest correction's next publication. Actual published1.10.2 is
immutable and contains only its recorded prior repairs; it must not be relabeled
or overwritten. Alternative source-only delivery leaves ordinary registry
consumers without the correction; republishing changed1.10.2 is unavailable.
Keep the other eight published package versions, checksums and source unchanged.

Pre-edit release record: six additional existing metadata paths only—root
Cargo.toml/Cargo.lock, zenoh/Cargo.toml, configuration-consumer Cargo.toml/
Cargo.lock and verify_archives.py's exact successor version. Six one-line version
substitutions, net0 lines, no new public types/API/dependency identities. Combined
with the six-path correction, twelve unique controlled paths (CI auditor adds
one changed line to the existing five-line CI delta, within eight). Update only
the top path-package version in both locks, preserving every unrelated record.
Coordinator performs these edits after the resource agent restores its exact
source and explicitly releases controlled Cargo. Then verify actual successor
archives, exact graph, consumer and required combined owning CI before release.
No version edit, archive validation, new-version CI, merge or publication has
occurred from this preparation. The bounded native owner proof remains separate
from complete TLS reclamation, memory quotas or full remote acceptance.

### Next local admission policy selected before ordinary-Rust comparison

Delegated recommendation selects an application-owned exclusive service loop,
plain functions and existing typed Actions/ExternalActor composition first.
No shared mutex, provider trait, universal trusted-receipt constructor, task per
request, actor contract or runtime attachment is selected. One exact configured
grant is sufficient for the first comparison; no grant registry is introduced.
Its current permission, locally established evidence deadline and retained
original request/result prefix are distinct from actor mailbox ownership.

Each retry obtains one coherent current authority/time snapshot, validates it
and performs existing try_send without an intervening await. Observed revocation
wins over otherwise valid cached evidence; now>=deadline refuses as expired.
Missing usable local freshness refuses as unavailable, preserving any acquired
cause. Production local time uses std::time::Instant; providers own any conversion
from their evidence into a local deadline. No remote-clock conversion, skew
allowance, negative-cache lifetime or instantaneous remote revocation is inferred.
Controlled test clock functions permit deterministic observations without a new
clock trait. Alternative mutex ownership adds unneeded shared-caller/unwind
semantics; precheck-plus-awaited-send violates the already proven law.

Preserve Invalid, Stale, Unavailable, scope refusal, Revoked, Expired, native
Full/Closed originals, mailbox acceptance and later processing distinctly.
Provider-owned opaque receipts remain concrete and require their corresponding
typed consumer; raw DTOs cannot substitute. The application selects its concrete
verifier/projection as a trusted configuration choice. No arbitrary generic
projection is claimed to prove universal authentication. A possibly published
message after insertion/waker unwind retains uncertainty and the actual cause;
no automatic second attempt is permitted. Borrowed-wait cancellation retains
originals; deliberate whole-owner surrender follows the already selected law.

Require explicit nonzero pending count, retained-byte and individual-footprint
configuration; no production default is selected. Count reservations through
verification/retries and completed-but-unreceived dispositions. Overflow returns
the exact original with its typed limit classification, including checked
arithmetic failure. Each selected provider/protocol must demonstrate actual
owned-allocation accounting or a conservative maximum; size_of_val and JSON
length alone are insufficient. This contract does not bound upstream queues,
transport decoding, allocator/socket overhead or arbitrary application state.
Verification concurrency/wait and automatic retry scheduling remain open where
needed; the first operation comparison does not add a verifier queue or scheduler.

Next experiment: ordinary private functions/owned fields for two concrete
provider/consumer bindings, using the actual permission_admission Application
law and independent complete prefix/processing/native-retirement oracles.
Compare direct ownership before retaining any public admission abstraction.
Estimate250–450 additional test lines, bounded at500 including temporary variants,
up to three owning test/source-record paths, production/public API+0/-0 at this
comparison. No manifest/lock/dependency change. Any actual production owner
needs a fresh measured pre-edit record, proven invariant, caller machinery it
deletes and names/signatures review; the forecast180–320 production lines and
2–3 public types is not adoption. No full AUTH1/NET1/R01–R25 acceptance follows
from these selected local policies or the pending comparison.

Scope checkpoint before that comparison: automatically select cumulative authored
test/fixture cap12000, replacing10500, while retaining production2200,
public-types5, retained-source-path320 and research-document6300 caps.
Current complete Bombay counter checkpoint has test net9127; this proposed
comparison contributes at most500, and controlled owning test/publication
contributions remain separately accounted across their accepted stages. Raising
only the test ceiling accommodates those combined source obligations without
splitting budgets or treating stage commits as a reset. No new production/API
surface is authorized by this test-cap choice. Record measured combined totals
before retaining the next comparison or any production owner.

Concrete pre-edit comparison: one new owning test path
crates/bombay/tests/authority_admission.rs, forecast410–470 retained lines plus
at most25 unique temporary mutation lines, all-authored hard ceiling500.
Reuse application_support::RootTerminal, actual native complete retirement,
Behavior's existing generated typed lanes and two concrete Application run_with
calls. Counter adds; Arithmetic multiplies. Distinct private provider receipts
contain no heap and accompany each original protected Box into their genuinely
matching domain consumer. Plain generic functions/owned queue state may compose
these inputs; no generic Application runner, hidden protocol trait or public type.

Scope of allocation assertion: actual protected Box storage plus measured fixed
queue/receipt state in this closed fixture, with conservative explicit charges.
Do not infer arbitrary opaque-message/provider/whole-heap bounds. Two-slot and
byte capacity-plus-one tests conserve overflow originals; Full retains its slot;
completed-unreceived results retain their charge. Cancelling a borrowed existing
oneshot notification wait (unit notification only) cannot remove original/result
custody from the service queue. This proves custody, not an automatic readiness
scheduler. Native1024 capacity is the existing mailbox, not a new default.
Both concrete providers exercise granted/revoked/deadline retries after a same
user-lane processing barrier, complete native cleanup and original-allocation
checks. Cache/precheck or bypass-revalidation inversions must fail the actual
protected-effect count in both profiles, then exact-source controls restore.
Record unavailable original causes where exercised; do not fabricate universal
provider/freshness or complete resource acceptance from the finite comparison.

### Exact-target overlap comparison selected before source edits

Delegate the recommended in-model overlap comparison, using existing protected
host/runtime/actor-generation fields and two independently configured local
receiver assignments. Sender targets A; both real queryables receive the same
provider-verified A-targeted bytes. A alone may admit Increment/Read. B must
retain its original Query, copied buffers and original private provider receipt
with a typed local target refusal, emit no successful command reply, and retire
with balance41, empty command trace and no rejected queued domain command.
Acquire A balance42/two-command trace, caller two replies and all four actual
session closes/native products before the independent terminal oracle.

Alternative cloned identical assignments demonstrate an out-of-model fanout
hazard, not this static single-assignment law. Post-reply deduplication cannot
repair two admissions. Zenoh BestMatching alone is insufficient: the selected
owning dispatcher falls back to All without a complete match. Keep current
All/None query settings while proving exact local target refusal before execution.
This does not select production routing keys, distributed generation issuers,
global fencing or new authenticated fields. Provider remains an explicit fixture.

Actual gap: counter_application.rs valid_request compares only with one global
configured hosting tuple. Parameterize the receiver's own assignment rather
than deriving it from the incoming claim. Reuse existing concrete Scope, typed
provider receipts, exact try_send, Counter Behavior/Actions and native retirement.
Any private product must own the independently configured assignment and delete
the hardcoded receiver assumption; avoid forwarding wrappers/new public types.
Three existing fixture paths (counter module, binary, Python controller):
forecast120–190 Rust plus100–150 controller lines; all-authored stage cap600
including temporary inversions, production/public API+0/-0, no new crate,
manifest/lock/schema field/default or shared runtime owner. This is a new
independently reviewed stage, not a reset of the cumulative12000-test cap.

Focused both-profile controls then bypass B's target check and substitute each
hosting component independently; require the intended B-native/admission oracle
rather than timeout or reply-count failure. Hold all actual acquired values
through close. Existing24 expected controller outcomes remain intact. Matching
presence only proves at least one receiver, so both actual received observations
are mandatory for overlap credit. Never relabel missing second delivery as
single-target safety. Full R22 also requires the promised milestone/consolidation
campaign; neither that nor global single-writer safety is closed here.
An obsolete binary-file header excluding all actor-admission observations must
be clarified with this fixture update. Source authoring may run independently;
Cargo execution waits for the shared controlled-lane handoff.

### Admission comparison corrected source estimate before broadening

The initial private draft has334 unformatted lines; pinned rustfmt preview
shows approximately832 actual owning-source lines (unchanged support/banner
excluded). The earlier410–470 estimate was wrong. No positive/inversion execution
or production/API change is credited. Under delegated recommendation raise this
one private comparison's all-authored test ceiling500→950, including at most25
unique temporary mutation lines. Cumulative authored-test cap12000 and all other
recorded caps stay unchanged; this is no commit/stage reset.

Retain actual ordinary Rust formatting and both real concrete Application/native
consumers plus independent pending/budget/cancellation laws. Alternative scope
reduction to revocation/expiry would leave those selected obligations untested;
compressed syntax or speculative generic Application abstraction to conceal the
size is rejected. Review the complete private source/model before executing it,
then measure actual formatted source and final tracked/untracked cross-owner
delta. This larger test estimate does not authorize any public admission owner,
provider trait, runtime attachment, additional semantic policy or production line.

Pre-execution review reopens the first admission draft: final refusal assertions
inside application work would veto a bypass-recheck mutant before the required
independent post-cleanup actor-effect oracle. Retain the observed attempt results
and classify them only after acquiring native/notification outcomes and checking
actual protected effects. Configure each receiver's expected grant target
independently: Counter and Arithmetic cannot both inherit a hardcoded Counter
scope. This is the selected exact-target law, not a new identity scheme.

Remove unnecessary Send/static bounds from the synchronous try_send operation;
awaited send consumers keep their own justified bounds. Narrow private provider
and generated-send visibility to their actual parent/private consumers. Name the
protected payload/proof product and verification/mailbox/uncertainty alternatives
for the values they own, including an unambiguous retained-byte overflow cause.
Extend original-recovery assertions to both allocated payload and proof boxes.
No public API or extra owner is needed for these corrections. First-source
comparison remains unverified until the corrected ordinary-Rust model, focused
both-profile controls and intended post-cleanup inversions pass.

The corrected comparison's first debug and optimized controls both pass3 tests,
including16 real Application/provider cases/profile and the two-provider budget
law. Initial compilation's generated-trait ambiguity and uninferred generic
budget construction are setup findings, resolved by qualifying existing owning
traits and spelling the existing receipt parameter; no architecture was invented.
Strict owning lint exposes only local style findings. Select ordinary naming/
exhaustive-match refinements and narrowly documented too_many_lines expectations
on the continuous work trace and concrete native caller. Alternative splitting
custody into speculative generic Application machinery is rejected. Preserve the
actual closure-error value without a meaningless explicit drop of its zero-sized
type. Recount final formatted source/temporary variants within950 before mutation;
no inverse, final lint or production acceptance is credited from these controls.

Interest correction local verification now completes:25 owning interest cases
pass in both profiles, including the9 original resource laws and2 None/replay
controls. All5 separate cleanup omissions execute exactly1 test and fail at the
intended Weak-retention assertion in each profile; every restored control passes.
Final owning TLS,test library/test strict Clippy and formatting pass both profiles.
Actual34-command receipt SHA cb1676e2c2f05f8d40c3267a5a5dcb720665bdcf62b05ee9185d3b6756b63105
retains one disk-full LLVM compilation failure and its successful retry; that
compile failure has zero semantic credit. Generated object intermediates were
inventoried and removed only after verifying their target idle; source, binaries,
rlibs, manifests, locks and evidence survived. All healthy source/log hashes
independently match. Native interest reclamation remains narrower than actual
TLS reclamation, heap quotas, overwrite policy or unwind rollback.

The successor metadata checkpoint also includes two existing current-profile
README paths (root and zenoh), forecast≤20 net documentation lines, to distinguish
actually published1.10.2 from prepared1.10.3. Fourteen unique controlled paths;
productionRust remains+30/-10/net20, tests+216, public API+0/-0. Six version
substitutions preserve all other complete lock records. This documentation
reconciliation does not add a release/publication claim before actual gates.

### Counter integration delivery and next admission checkpoint

Counter research PR337 (https://github.com/devrandom-labs/bombay/pull/337)
merged through reviewed exact head013b71d3ce0acd4c577837dc2fdd6d1781bac197
to main65bb88456aeee13570b706b629b25de0341abb0b at2026-10-11T00:07:13Z.
All six submitted-head checks passed. Independent technical review
PRR_kwDOTCRE7M8AAAABRrQaDw covers that head. Nix run38094876926
passed, including actual Driver and Observe fuzz campaigns; constructing the
optional Miri shell does not establish execution of Miri tests. Linux native
artifacts11684918856/11685598210 independently confirm the sixteen expected
transport outcomes and eight healthy Counter cases per profile, with acquired
native results and actual session closure. Deliberate denial and late death
remain distinct from healthy completion. Submitted-head source accounting
excludes the subsequently prepared records below; no full remote requirement
is marked merged from this finite contribution.

Coordinator creates feat/static-remote-authority-admission from actual latest
main65bb884 before any next commit, preserving the227 pending PRD lines.
The completed private authority comparison is retained for integration: one
944-line owning test, production/public API+0/-0, unchanged lock936110853718f053d4eda91d6b37db0a87e46385e20ef68add895b4491a06152.
Its source SHA4cda26b49fd458e5a5859f2dec6ad55241b2ba7c2e312f89c57175e6df13f238
and campaign SHA10b38ff3645063110b8ea025f368c3f8dfafa69d8c4b8863cc187a7654d9bc91
are independently checked, including every recorded log hash. Three focused
tests pass in each profile, exercising sixteen actual provider/Application
cases and the two-provider pending limit law. Revocation and expiry-equality
bypasses each fail both concrete native protected-count oracles after complete
retirement in each profile. Wrong constructor and receipt projection each
produce E0308 in both profiles. Exact-source restored controls and strict
owning lint/format pass. Four unique mutation statements give948 authored
lines, within950. One prematurely read orchestration log is explicitly excluded.

This comparison proves finite explicit local Box/header accounting, original
payload/proof custody, completed-unreceived reservation and cancelled borrowed
notification custody. It does not prove stalled-verifier authority-update
liveness, actual waker uncertainty, whole-heap bounds, replay, cryptography,
remote transport or a public admission API. Aggregate-drift disposition: pass
for this private comparison. Before/after production states, modules and public
spellings remain0/0. The private admission sum has five alternatives: original
awaiting verification, original with its concrete receipt, original/evidence
with refusal, established mailbox acceptance, and potentially published
uncertainty. Each retained alternative owns its current required custody or
known admission status; no arrival-history submachine or second Behavior
authority is added. Permission and configured target remain independently
needed inputs. The two concrete consumers preserve the existing pure Actions
law; residue review finds no semantic boolean, inline import, erased provider,
new runtime attachment or production abstraction. PossiblyAdmitted names an
unexecuted uncertainty obligation, not a completed witness.

The controlled1.10.3 combined verification ran all preceding profile tests,
strict lints, consumers and package creation, then its archive command failed
because the local CARGO_TARGET_DIR override differed from its fixed PWD/target
path. This is an orchestration failure, with no correctness credit. An ignored
local target symlink to the actual controlled target lets the unchanged archive
auditor execute: all nine actual archives,29 controlled checksum edges and
restored archive/lock/feature inversions pass. Required PR6 CI, actual merge,
main verification and publication remain pending. No source patch was introduced
for this local path correction. The controlled Cargo lane is then explicitly
handed to the independent exact-target overlap campaign.

### Stalled-verifier authority progress: selected bounded follow-up

Delegated recommendation selects ordinary split-field borrowing and the existing
Tokio select!/borrowed oneshot primitives for a separate follow-up after delivery
of the verified comparison. A provider must actually remain Pending while one
independently queued typed local update changes the exclusive permission/time
fields. Capture acknowledgement and actual state before releasing verification;
retain observations until complete native cleanup. Granted, revoked and
deadline-reached scenarios run through both existing concrete consumers. A valid
control admits once; revoked/expired inputs admit zero. Behavior stays pure.

Alternative await-verifier-first prevents known revocation from progressing and
is the intended finite-poll inversion. Cancel only that borrowed operation,
release the blocked evidence, finish native cleanup, then reject the captured
missing acknowledgement. An acknowledge-without-update mutation must fail the
independent state/native oracle. No timeout supplies nonexecution evidence.
Select! does not promise general fairness for simultaneously ready branches;
this witness has one ready update and one genuinely blocked verification future.

Existing providers receive an optional borrowed release receiver, preserving
the actual closure cause as Unavailable. A small private AuthorityUpdate sum
names revocation/time observation; any observation product only conserves the
actual pre-release evidence, not authority or another lifecycle owner. No task,
mutex, Arc, provider queue/trait, runtime attachment, dependency, public type,
production code or default limit is added. Selected Tokio1.53.1 source documents
select branches on the same task and borrowed oneshot cancellation safety.

Change record: one existing test path, forecast114–159 added formatted test
lines plus2–4 unique inverse lines, hard ceiling180 for this follow-up. It
retains the prior944-line/eight-scenario comparison and950 authored-stage
record, rather than silently expanding that stage. Cumulative test cap12000
remains shared across both repositories and overlap work; measure before
execution and retention. Read-only independent proposal precedes source edits.
This follow-up does not justify a public admission API: ordinary composition
must first show a concrete repeated state burden that an owning interface
deletes. Root controls integration and shared contract selection.

Pre-follow-up cumulative test reconciliation finds an omitted80-line controlled
release_cohort_tests.py contribution in the CI-script category. Count it
explicitly: Bombay10071 + controlled1596 + release Python tests80 + prepared
overlap186 =11933 retained test/fixture lines. The proposed180 authored lines
for stalled verification would exceed the12000 ceiling. Automatically select
recommended ceiling12500 before authoring/execution/retention of that follow-up;
all prior stage ceilings remain distinct and unchanged. This correction also
counts the existing95-line validation example and194-line registry consumer as
verification fixtures, while retaining57 publication-authority Rust lines as
production automation. The conservative raw source classifications are kept;
this is an honest correction, not a commit/stage budget reset. No new production
API or additional requirement follows from the test-only expansion.

### Admission comparison combined-source verification checkpoint

Exact944-line source4cda26b49fd458e5a5859f2dec6ad55241b2ba7c2e312f89c57175e6df13f238
is integrated against actual main65bb884. All five required coordinator commands
pass through pinned Nix with the unchanged lock: cargo build --workspace
--locked; cargo test --workspace --locked (including documentation); cargo
test --workspace --all-targets --locked (569 passed,1 existing ignored); cargo
fmt --all -- --check; cargo clippy --workspace --all-targets --locked --
-D warnings. No Rust source changed after these commands. Required submitted
CI and exact-head independent review are still necessary before a merge.

This stage retains one944-line test and two existing documentation paths, with
production+0/-0/net0, tests+944/-0/net944, documentation+384/-4/net380
and public API+0/-0 types. The
complete tracked/untracked measurement retains cumulative Bombay production
+1725/-475/net1250, tests+10732/-661/net10071 and80 changed paths against
a9c5b7d4a1cf2b15504acef43c20c0da9b5e320e. Defined public types remain5 from
the previously delivered retirement prerequisite; this stage adds none.
Prepared follow-up/overlap source remains in independent worktrees and is not
silently included in this submitted test. Controlled contributions and their
raw category reconciliation remain separately measured across the same task.
Independent final source/evidence review finds no blocker for this comparison;
all14 logged command hashes match, and the exact selected owner graph is unchanged.

Complete controlled baseline measurement retains86 paths and its raw categories.
Conservative runtime Rust net837 includes the existing95-line validation example;
current owning-library Rust net742 is distinct from functional775 (including
Rustdoc classification), within the approved810 functional ceiling. CI Rust251
contains194 verification-consumer lines and57 publication-authority lines.
Cross-owner Rust production1250+742+57+Communication2=2051. Report automation
separately: executable delivery scripts/workflow+804/-1192/net-388; retained
licenses/provenance980 and default configuration11 are separate source material.
The raw CI-script bucket's net683 is neither all production nor all fixtures.
No automation deletion offsets the recorded core capability growth.
Controlled fixtures1676 include the previously omitted80-line Python tests.
Current cross-owner tests10071+1676+prepared overlap186=11933, before the bounded
stalled-verifier follow-up; the newly selected12500 cap covers that stage.
Independent read-only reconciliation confirms those owning classifications.

Admission comparison submitted as PR338
(https://github.com/devrandom-labs/bombay/pull/338), exact head
c7ae1952e494778e36c91e988f24dafc4f4d85e7. Independent technical review5481290851
(PRR_kwDOTCRE7M8AAAABRrXcYw) covers that source. Required CI38097652057
is running; no merge or full feature completion is recorded. Final submitted
checkpoint:3 paths, production+0/-0/net0; tests+944/-0/net944; documentation
+384/-4/net380; public types+0/-0; lock/manifests unchanged. Cumulative submitted
Bombay checkpoint:80 paths, production+1725/-475/net1250, tests
+10732/-661/net10071, documentation+5771/-88/net5683 and5 previously delivered
public types. Later working-tree records are excluded from that submitted view.

Prepared stalled-verifier source formats to1117 lines: compared with the
separately submitted944-line source, tests+203/-30/net173. The180 estimate
covered net growth but its all-authored wording must also count the replaced
original statements and proposed inverse statements. Before execution/retention,
automatically select the recommended220-line authored-stage ceiling (203+4
anticipated inverse statements=207); cumulative12500 remains unchanged. The
prepared estimate overrun is not hidden as net reduction or counted as verified.
No production/API/graph source changes. Alternative compressing the custody
trace or dropping a concrete consumer would weaken the required comparison.

Coordinator reviews the prepared model before compilation: verification is
actually polled with its release held; independently enqueued update is applied
through split exclusive permission/time fields. The borrowed select operation
can be cancelled without dropping the provider future or pending original; any
early acquired provider result is retained and never polled again. Capture
acknowledgement and grant/time before evidence release. Both concrete native
results and notification products precede progress/refusal oracles. Existing
capacity-wait revocation/expiry scenarios remain intact. This is one ready local
update competing with one blocked provider, with no general fairness claim.
Prepared source SHA426f1fd6f4dc5aeeb54da4cd2ee37e97e70674b578d6875f35f234a62f13fd34
has no identified semantic blocker; compiler output may still veto it.

Prepared426f stalled-verifier source compiles without model changes; healthy
debug/optimized controls each pass3 tests, including22 actual Application
cases and the existing two-provider budget test. No inverse or combined-source
delivery credit follows yet. Owning Clippy exposes one107-line concrete Counter
receipt oracle above its100-line style threshold. Select the same narrowly
documented too_many_lines expectation already used on the Arithmetic native
oracle, preserving the continuous exact custody/retirement trace. Four authored
attribute lines plus the207 forecast give211/220; no generic runner, lint-wide
allowance or new architecture. Rerun affected controls before inversions.

The stalled-verifier acknowledgement-without-update mutation stops each native
consumer at revoked input, so it cannot also certify the later deadline case.
Select the additional independent Time-only omission, retaining Permission
application. Its one unique statement uses the already forecast fourth inverse
statement: formatted retention+207, await-first2, all-update omission1 and
time omission1 =211/220 authored lines. Both consumers/profiles must fail the
intended post-cleanup native effect oracle at expiry, then restore exact source.
No broader policy, production/API change or new budget is introduced.

Independent production-contract review rejects a public check-and-attempt
facade: it would delete no production caller machinery and would merely rename
existing native try_send. Keep ordinary application-owned verification/grant
functions and exclusive reservation state. Profile-owned verified conservative
maximum accounting before verification is already selected; headers or JSON
length cannot certify opaque provider/error/message allocations. Freeze a
concrete supported profile and actual production consumer before retaining a
private networking admission core or public abstraction. Existing ExternalActor
continues owning its ordinary local endpoint contract. No production/API design
is adopted from the current comparison; request identity/replay contracts remain
an independent prerequisite investigation.

### Stalled-verifier follow-up local proof and early integration

Final private source c3b847e04e1a219ae95b9ae62e3c11aad18a1de0c6fefb09f59d260725ed53be
formats1121 lines, tests+207/-30/net177 against the separately submitted944-line
comparison. Four unique inverse lines give211/220 authored lines. Campaign
SHA3d1e3922d0b238eb6b7bd35ae2698b7ed66280cf9f7fbd344ae6abbad6bd6ecf
records14 actual commands/logs, independently hash-checked by the coordinator.
Healthy and restored debug/optimized controls pass3/3, including22 actual
Application cases/profile. Restored binaries equal their healthy binaries.
Await-verifier-first produces4 intended post-native progress failures; false
acknowledgement produces4 native protected-count failures on revocation; Time-only
omission produces4 independent native protected-count failures at equality expiry.
No timeout, compilation or unrelated failure receives semantic credit. Final
strict owning lint/format pass after exact source restoration.

Aggregate-drift disposition: pass for this private follow-up. Existing admission
control sum and production/public surface remain unchanged. Two verification
timing alternatives are test schedules, not another owner. AuthorityUpdate's
two variants carry the independently queued current grant/time values. The
observation product conserves actual provider-poll, acknowledgement, authority,
time and release outcomes until native cleanup; it cannot mint authority.
Split existing fields and borrowed select suffice without a new task, mutex,
provider trait or public admission facade. No fairness/global generation/replay,
opaque whole-heap or cryptographic conclusion is inferred.

Early integration worktree /tmp/bombay-authority-progress-integration and branch
feat/static-remote-authority-progress begin at actual main65bb884, then fast-forward
to pending prerequisitec7ae195 without changing PR338's submitted source. No
follow-up commit yet; reconcile actual reviewed prerequisite merge/latest main
before delivery. Shared normal Cargo is coordinator-owned for combined checks;
the independent controlled target remains assigned to the overlap campaign.

The integrated stalled-verifier source passes all five combined pinned-Nix
commands: workspace build, workspace/documentation tests, all-target tests,
formatting and strict workspace/all-target Clippy, all --locked where applicable.
Source and selected lock stay exact after verification. Scoped final minimization
review finds no blocker, preserving all custody and post-cleanup oracle claims.
Submit a dependent focused PR targeting main so its complete-source CI can run
in parallel with PR338. Merge prerequisite338 first; recheck exact head and
actual combined required CI/review before the dependent merge. No feature status
is advanced from these local checks or an open dependent PR.

Final follow-up checkpoint against submitted prerequisitec7ae195: two existing
paths, production+0/-0/net0, tests+207/-30/net177, documentation
+124/-4/net120 and public types+0/-0. Complete
tracked/untracked cumulative Bombay retains80 paths, production
+1725/-475/net1250, tests+10909/-661/net10248 and5 delivered public types.
No manifest, lock, dependency, public method or schema changes. The dependent
PR temporarily includes pending338 source; merge338 before retaining this
follow-up delivery and preserve the exact combined head's required CI.

### Exact-target overlap local proof and integration

The three existing fixture paths retain tests+220/-34/net186, with no production,
public API, dependency, manifest, lock or protected schema change. Counter
source3490b6d9d1faa0981a0ff35709d65a3fdc8b85428a2df044e88cd3c20633894d,
binary source69413f3c2acecddcd7b9fdc4b9d5aeec000b4cdae2833454fab918e4761d8ff5
and controller6003911b9506880b0b99ba19a2bd45232a35d361c4329a57b156305b9e614894
match the final restored source. Manifest51ea30eb and lock692c2612 stay exact.
Final campaign SHA f1cbe5ffb68052fc365a8300d8d6c0ed0048fdb968c693866fd8052e3303c10b
records initial/restored30 expected outcomes per profile: native16 (four deliberate
forced late deaths remain uncertainty), original Counter8 and overlap6. All
12 overlap positive rows observe both real deliveries and four orderly closes,
A balance42/two commands and B balance41/no commands. Three independent host,
runtime and actor-generation omissions produce12 intended B-native failures
in debug/optimized profiles, with A/caller/router exit0 and B exit1 after actual
Session::close. The B close observation is emitted after its worker_failed event;
source closes the actual session before the native oracle and the controller
acquires that close and exit before rejecting the failure. No timeout/setup
failure supplies a safety proof. All exact-source restored controls pass;
healthy/restored binary hashes match in each profile. Strict owning lint/format
and diff checking pass. Coordinator independently checks restored source,
actual cases/exits/oracles and captures22 log hashes; the agent receipt contained
no original log-hash fields, so this is hash custody, not an invented comparison.

Early coordinator integration starts at actual main65bb884 then fast-forwards
to pending combined38d61a5 without changing PR338 or PR339. Six affected
pinned-Nix commands pass: nested fixture debug/release builds and full30-case
controllers, strict all-target Clippy and formatting. Root workspace inputs
remain exact to the already verified38d combined source/lock; the complete
submitted-source CI must still pass before delivery. Merge338, then339, then
the overlap contribution; their immutable-head CI may run concurrently.

Aggregate-drift disposition: pass for the finite fixture. Independently
configured receiver assignment replaces the shared hardcoded receiver assumption.
TargetRefusal's host/runtime/actor alternatives conserve the local refusal and
provider/native originals until retirement; no new actor transition authority,
lookup registry or production/public owner is added. Both metadata comparison
and typed construct/recover paths remain ordinary Rust. Actors retain pure
Actions; the binary and controller headers now describe the actual finite
Counter proof. The receivers share the existing test TLS certificate. Local
assignment separation therefore supplies no distinct production node identity,
cryptographic application authentication or global fencing evidence. FullR22
milestone/consolidation/restart obligations and the remaining profile gates stay
open. No fixture version update or successor publication is inferred.

Controlled native interest-retirement PR6 merged3e45ba6b1098b3589b242935284610d0113e13a8
at2026-10-11T00:28:29Z after independent review5481241216 and required
run38096526759 passed, including actual Miri and archive checks. Main
run38098545513 is in progress;1.10.3 is not yet published or registry-consumed.
Required CI log SHA092291ae4f917f17368ae4a2bb561a40e56ea491c6e7c4f3cf959eba9ca67056
retains exact evidence. This prerequisite merge does not close NET1.

Final overlap checkpoint against combined38d61a5: five paths, production
+0/-0/net0, tests+220/-34/net186, documentation
+73/-3/net70, public types+0/-0. Complete
tracked/untracked Bombay totals:80 paths, production+1725/-475/net1250,
tests+11095/-661/net10434 and5 previously delivered public types.
Across repositories current retained tests10434+controlled1676=12110 within
12500; unique inverse statements remain within their separately recorded stage
ceilings. Core Rust production2051 remains separately reported from automation.
Independent scoped source/evidence review passes. Hash inventory now includes
all22 captured command logs; the first18-path expression omitted the underscore
in actor_generation names and supplied no hash comparison for those four logs.
Corrected custody inventory preserves all original evidence and case assertions.

### Protected request identity: checked sequence selected before implementation

Delegated recommendation selects one sender-owned checked u64 request sequence
inside a receiver-authorized authenticated logical caller-to-exact-target session
namespace. The session binds deployment, caller, declared protocol and exact
receiver runtime/actor assignment; incoming raw claims cannot establish that
binding. Exhaustion returns a typed local refusal with the original request
before a foreign call, with no wrap or reset in the same namespace. Transport
reconnect preserves this logical namespace and sequence. Operation, signing-key
revision and grant revision do not create alternate replay lookup keys: changed
authenticated request content under one identity conflicts, and rotation alone
cannot clear eligibility. This selects identity policy, not a public type, wire
field, start value or namespace-issuer implementation.

Use standard checked arithmetic first, verified against pinned Rust1.99.0
b940084d7: https://doc.rust-lang.org/std/primitive.u64.html#method.checked_add.
Compared candidates: UUID1.28.0 exact archive SHA
7cc1186384beb7dd8eedea376413fd654937285ea6c9cfbb928dc3043ea4b606
(https://static.crates.io/crates/uuid/uuid-1.28.0.crate), and ULID3.0.0 SHA
947dde63b6d514cc5e044edad4e0ca7261afd1099d16c83d942cb2b2f348689c
(https://static.crates.io/crates/ulid/ulid-3.0.0.crate). Research verified those
actual primary archives without selecting/installing either. UUID adds entropy
handling and probabilistic uniqueness; ULID adds clock/ordering/overflow policy.
Neither supplies replay protection or removes the authenticated namespace.
The checked sequence gives exact within-namespace uniqueness with no new
dependency. It requires a separately verified fresh namespace on restart.
No particular random namespace, persistent counter or fresh issuer is adopted.

Ordinary bounded standard-map storage is a candidate, not selected replay policy.
Retention, expiry, full-state behavior, authorized duplicate results and their
resource equations remain separate gates. Uncertainty never authorizes silent
reissue under a fresh identity. This identity decision supplies no replay witness.
Selected native Zenoh RequestId is u32 with a session-local fetch_add counter
(source1211779, request.rs20–22; session.rs199/2721); transport correlations and
random transport peer IDs cannot supply protected actor identity. Native query
ID reuse in a long-lived session remains a separate V/B prerequisite requiring
verified reply binding and an issuance/lifetime strategy; no SDK patch or
transport-session limit is selected from this finding.

Coordinator workspace now follows combined head9b6d0b8 on a separate branch
created from actual main65bb884. The earlier coordinator-only pending PRD text
is archived byte-for-byte before replacement; its decisions/proofs are present
in the submitted descendant records. No user source changes were discarded.
Open338,339,340 complete-source CI remains concurrent and their merge order
remains338→339→340. Required merged controlled-main run38098545513 is pending
before any1.10.3 publication. All full acceptance and deferred scopes remain.

### Logical binding and replay retention: delegated recommendations selected

Problem: a fresh sender nonce or transport reconnect must not reset a request's
right to execute. Select receiver/deployment-authorized, provider-owned logical
caller-to-target bindings, established before the exact static export. The
provider confirms deployment, caller, protocol, receiver principal and exact
runtime/actor assignment. A sender nonce remains an untrusted proposal until
confirmed. Reuse the existing private concrete provider receipts and explicit
projection functions; no common wrapper, dynamic registry or public admission
facade is justified by the measured ordinary Rust consumers.

Alternative sender-issued namespaces simplify local generation but cannot
authorize a new receiver assignment or prevent replay-ledger resets. Finite
explicit application configuration is selected for this static milestone.
Production startup must obtain the declared provider's fresh authorized
assignment before export; missing, unavailable or fake production evidence
refuses startup. Restart must retire the old assignment before replacing it;
old bindings cannot authorize the replacement. Reconnect preserves the binding
and checked sequence. A new binding after exhaustion is explicit and cannot
automatically retry uncertain work. The actual fresh issuer remains unverified:
randomness, a KERI identifier and fixture generation numbers alone prove neither
authorization nor absence of cloned live deployments. No entropy mechanism or
new dependency is selected.

Replay policy selects finite standard-library ordered-map retention inside the
existing exclusive application service. Alternatives timed eviction and LRU
eviction can make still-eligible requests executable again; retain every
identity for the entire authorized logical binding instead. Lookup follows
provider authentication and exact binding validation. Reserve entry and retained
request/result capacity before any actor attempt. Same identity and identical
protected request bytes observes the existing strongest status; only a known
Full refusal may retry with a fresh permission check. Different authenticated
request bytes conflict even with a fresh proof. Operation and key/grant revision
do not form independent lookup namespaces. Inflight, admitted-without-outcome
and possibly-admitted dispositions never authorize another admission.

Borrow cancellation, completion and consuming a reply do not erase replay
eligibility protection. Full retention refuses a new identity with its original
request, while existing identity lookup remains available. No TTL or LRU removes
live entries. Whole binding retirement closes eligibility before custody is
released; disconnect or timeout cannot manufacture freshness. Duplicate
observation has no unbounded waiter queue. Returning cached protected replies
requires separately verified current consumer authorization; no such response
policy or public result API is selected here. This finite volatile policy
provides no durable deduplication or production identity proof.

Actual authority comparison PR338 merged7139409ac7d98f222008153e3917689773ae759e
at2026-10-11T00:48:40Z. Exactc7ae1952 received review
PRR_kwDOTCRE7M8AAAABRrXcYw and all six successful checks. Nix
run38097652057 includes actual Driver2048-run and four Observe1024-run
campaigns; construction of the optional Miri shell is not Miri test execution.
Both actual Linux transport artifacts independently preserve16 native expected
outcomes and8 healthy Counter cases per profile. Four deliberate late forced
deaths remain uncertainty. Exact-head premerge custody is retained in
/tmp/bombay-pr338-before-merge-proof.json; the scoped contribution does not close
full AUTH1 or NET1. PR339 and PR340 remain open delivery gates.

Pending binding/replay decision checkpoint: two documentation paths, production
+0/-0/net0, tests+0/-0/net0, documentation+108/-3/net105 before this
checkpoint paragraph, public types+0/-0. Complete tracked/untracked Bombay
source still has80 paths, production+1725/-475/net1250,
tests+11095/-661/net10434; no untracked paths. Across owning repositories
core Rust production2051, retained tests12110 and new Bombay public types5
remain within their recorded ceilings. The next replay comparison must record
its measured source forecast and any recommended expanded cumulative test/doc
ceiling before implementation; this decision record authorizes no unmeasured
production owner, dependency edge or public API.

### Replay comparison construction and scope: selected before source edits

The prepared ordinary Rust comparison uses one nested private replay concern
in the existing authority_admission.rs test. It reuses both pure actors, typed
reply lanes, recover functions and actual native/notification receiving paths.
The existing admission queue's consume operation legitimately removes pending
custody; it cannot substitute for a live-binding replay ledger. Retained
identity/content/status therefore belongs to the finite ordered map, separate
from admission capacity. Its complete phase sum must preserve known Full,
possible admission, known admission and acquired completion without allowing
a second send from uncertainty. No subordinate actor transition engine is added.

Both concrete test providers require independently configured protected records
that bind request identity and exact target. Their private receipts gain an
optional authenticated replay identity; existing fixed-payload verification
explicitly has no such identity, and replay admission rejects that absence.
No common provider wrapper or trait is added. Different protected bytes must
independently authenticate before exercising the same-identity conflict oracle;
invalid evidence cannot masquerade as replay protection. Binding issuance is
explicitly finite test configuration, supplying no production fresh-generation
authority or KERI proof. Raw construction remains private.

Alternatives duplicating the two actors/native consumers or treating queue
consumption as replay retirement are rejected for duplication and loss of live
eligibility protection. Current exact source is1121 lines/SHA c3b847e0.
Forecast: one existing Rust test path,650–800 added test lines, production
+0/-0/net0, public types+0/-0, no manifest/lock/dependency changes. Up to
eight unique inversion statements are separately counted. Existing retained
tests12110 plus the808-line maximum reaches12918, exceeding12500.
Recommended expanded cumulative retained-test ceiling13000 is automatically
authorized by the user's delegated recommendation instruction before edits;
stage retained-test ceiling800 plus at most8 unique inverse statements gives
a separate total authored-test ceiling808. Documentation ceiling
expands6300→6800 to retain complete decision/evidence records. Core Rust
production2200, public types5, retained paths320 and controlled functional810
remain unchanged. If measured source exceeds the forecast, stop and record a
revised recommendation before additional source.

Required actual native oracles cover duplicate/conflict, known-Full retry,
completion/borrow-cancel/receipt consumption, full retention, reconnect, old
authorized binding against replacement and checked identifier exhaustion. Both
providers and debug/optimized controls precede their intended inversions. All
original native and notification results are acquired before terminal assertions.
Map count/profile-local storage evidence cannot claim allocator or upstream
whole-heap bounds. Cached protected reply authorization remains a separate gate.

Research checkout correction before authoring: actual latest main7139409 is
the PR338 merge and therefore cannot fast-forward to pending descendant9b6.
The author created its isolated branch from actual latest main before any
commit, then uses a detached exact9b6 source checkout for the uncommitted
comparison. This preserves both histories and the source-bound baseline
without inventing an integration merge. Final coordinator delivery will use a
branch created from actual latest main and apply only the verified contribution.
No source edits or commits existed when the setup correction was selected.

Controlled main3e45ba6b verification run38098545513 now passes, including
actual25 owning interest tests in each profile and2 configuration-owner Miri
tests. Full log SHAb8146af9306bd13435dac07086e045b00833a9b0ea8060f2e8d1f4c7ead9774e.
The exact-current-main manual publication run38099991563 was dispatched at
2026-10-11T00:54:06Z with operation publish and source3e45ba6b. Publication
and registry-only consumption remain unproved until their actual receipts.

Actual Linux artifacts for PR339 independently pass24 expected cases/profile
and PR340 passes30/profile, including6 healthy overlaps. Their tested synthetic
merge trees equal their submitted source trees exactly. PR340 root additionally
verifies all117 extracted-file hashes and full native job log per profile.
Binary hashes are controller-recorded hashes of actual Linux executables;
executable bytes were not uploaded and are not independently rehashed. Four
caller-refusal cases have actual closes and exit1, while four forced late exits
remain uncertainty with no orderly-close claim. PR339/340 full Nix and actual
merges remain gates. Exact evidence remains /tmp/bombay-pr339-linux-native-proof.json
and /tmp/bombay-pr340-linux-native-proof.json.

Replay authoring checkpoint and revised scope before further source: the
formatted uncompiled draft changes one owning test path,+973/-1/net972,
total2093 lines. It exceeds the650–800 forecast and800 retained ceiling.
Authoring stopped immediately after formatting; no compiler/lint/test result
is claimed. Formatting expanded explicit constructors and native-custody
controllers beyond the pre-format estimate. No production, public API or
dependency path changed. The forecast error remains part of this record.

Recommended revised retained-test ceiling1100 plus at most8 unique inverse
statements gives authored-stage1108; cumulative retained-test ceiling raises
13000→13300 before further edits. Prior12110 plus maximum1108 is13218.
Splitting or omitting scenarios would not reduce cumulative scope and would
weaken the same-identity lifecycle evidence, so it is rejected. Delegated
selection authorizes this expanded test surface. All other ceilings stay
unchanged. Independent ownership/model audit and minimization must precede
retention; syntax expansion supplies no justification for a competing runtime
owner or decorative wrapper. Required focused positives, intended inversions,
restoration and combined CI remain unexecuted.

Replay aggregate-drift disposition: reopen before retention. Independent source
audit of uncompiled draft SHAe69ff94250055ab04106f838bdd2f9ae5bbab22b332fca209dfc8c2c42b669af finds
a competing Delivery/Retained/Disposition admission state and notification
owner; retry also duplicates the already proved permission gate and reports
known authority refusal as mailbox Full. Work-phase expect/expect_err would
preempt post-native inversion oracles. A replacement generation chosen for one
Application does not exercise old-owner retirement; cloning the same recipient
does not exercise reconnect. Admission notification alone cannot establish
completed actor processing. These are model/evidence defects, not compiler
requirements. No passing or retained replay witness is claimed. Exact rejected
draft is archived /tmp/bombay-logical-replay-rejected-draft.patch.

Recommended correction must reuse existing Admission/PendingAdmission custody
and the one exclusive attempt interpretation, keeping only ledger-owned
identity, protected comparison bytes and necessary completed evidence. Remove
the duplicated admission/notification/send/recovery gate and false Full label.
Actual results are carried out of Work and classified after native/notification
receiving. Actual restart/reconnect boundaries need execution or remain
explicitly unproved. The author stops before additional source/Cargo and
prepares a revised complete control sum/custody/deletion forecast. This rejected
private model supplies no production contract or public interface decision.

Revised replay construction selected before correction: delete the new
Delivery/Disposition control representation and reuse Admission/PendingAdmission
directly in each map entry with only additional protected comparison bytes.
Extract one ordinary attempt_pending over the borrowed existing entry and
explicit current permission/time/target; both queue and map use that same
check/send/recovery/publication interpretation. Exact authority refusal remains
Refused. Existing VerifiedCommand is retryable only after an actual native Full
in the externally observable retained entry; Accepted/Refused/PossiblyAdmitted
cannot cause another native send. Admission notification consumption retains
the entry. Domain processing is established by actual typed Actions snapshots
and native state, never by renaming admission notification Completed.

Issuer state is a separate sender-owned checked sequence, not receiver replay
retention state. Use the standard Option/checked arithmetic without introducing
an issuer framework. Work retains actual results, including unexpected success,
for post-native classification. A completed old Application and discarded old
volatile ledger precede the replacement Application and old-binding refusal.
For the local connection law, create an actual new ExternalActor, close/drain
the old service and retain the same authorized binding/map. This is an executed
local endpoint replacement, not a claim of native transport reconnect.
Original admission provenance records each actual sender independently.

Forecast after deleting duplicated interpretation:900–1050 total added test
lines, within retained1100/authored1108. Preserve the comparison-byte copy: it
owns information needed after the original moves to the actor. A read-only
status projection is not itself an architectural violation; the rejected
draft's competing custody and repeated gate were the defects. Fresh-proof
duplicate, raw/unbound refusal and exact conflict/capacity original custody
need explicit observable evidence or remain declared open.

Author retained a later rejected draft SHAe69ff94250055ab04106f838bdd2f9ae5bbab22b332fca209dfc8c2c42b669af,
+975/-1 after two compile-setup corrections within the revised scope. Initial
compile exited101 with seven generic Debug diagnostics; log
/tmp/bombay-replay-initial-compile.log SHA
c17f6a582ef9d01fa98b8aeec0db745cc91487460257789daa97c2ffc67c6130.
No executed test or semantic inversion credit follows. No Debug bound or
wrapper is justified by those diagnostics. Both exact rejected versions remain
preserved; source correction begins only after this model record.

Controlled bombay-zenoh1.10.3 actually published in run38099991563 from
merged main3e45ba6b; registry checksum
dabfd8ff4a541ce3ba5fafdec15639cb4c5c0b81aa18265c80dace6151b6201a.
Independent audit verifies unyanked status, actual archive bytes, source VCS,
all9 controlled archives and29 normalized dependency/checksum edges. Eight
dependency archives remain immutable. Actual clean registry-only consumer
passes all7 pinned-Nix commands: normal/maintainer/maintainer-macros strict
Clippy and executable assertions, plus formatting. Exact graph preserves304
other external records (306 full lock records including consumer; default
metadata resolves301 records). All9 controlled packages resolve from crates.io,
with no override and only the consumer itself as a path package.

Final combined publication/consumer proof is
/tmp/bombay-zenoh-interest-published-registry-consumer-proof.json. The independent
archive-only proof retains consumer_verified=false as its earlier checkpoint;
the later combined proof conserves actual consumer evidence. Preparation
miscounted duplicate-version lock entries and a later metadata parse encountered
the Nix banner; corrected full-record comparison and preserved raw/parsed
metadata are recorded, with no verification credit for those failed attempts.
Bombay's nested transport fixture still selects1.10.2. Adopting1.10.3 requires
its narrow graph update and fresh affected native transport checks; publication
and configuration consumers do not prove those campaigns or full NET1.

Corrected replay checkpoint remains unretained: formatted draft+1146/-8/net1138
exceeds the1100 retained-stage ceiling by46 added lines, despite deleting the
competing interpreter. Actual endpoint replacement and explicit custody controls
expanded more than forecast. Recommended disposition is minimization within
1100, preserving all observable obligations, rather than another scope increase.
Only deletion of redundant observations/forwarding and necessary ordinary
ownership corrections are authorized before the next measured checkpoint.
No distinct native cause or actual boundary may be deleted to meet the count.
Rejected archived source remains separately identified research evidence, not
a reset of cumulative retained-source accounting.

The same pinned root flake first failed offline shell setup from the research
checkout; no Rust ran in that attempt. The exact pin then established normally
and root-cwd invocation works. Corrected draft compile-only exit101 reports two
Work closure borrowing diagnostics and one unused import; generic Debug
diagnostics were eliminated by preserving actual results. Ordinary move custody
for the owned Application handle is permitted by the already selected Work
ownership law; no new bound, wrapper or public interface follows. Compilation
was initiated before the formatted count was inspected, so it provides only
excluded setup evidence, never accepted test or inversion credit. Next
measurement must precede additional verification.

### Published Zenoh1.10.3 adoption: bounded change record before manifest edit

Blocker: Bombay's retained native transport fixture still uses1.10.2 and does
not consume the reviewed published resource-interest retirement correction.
Select actual published1.10.3 with checksum
dabfd8ff4a541ce3ba5fafdec15639cb4c5c0b81aa18265c80dace6151b6201a.
Existing actual registry/consumer proof and owning25-test debug/optimized
correction suite establish the source candidate, not the fixture integration.
The alternative retains a known uncorrected prerequisite and is rejected.

Expected stage: two nested fixture manifest/lock paths plus PRD/backlog evidence,
production+0/-0/net0, test source+0/-0/net0, public types+0/-0. Replace only
one top package version/checksum; preserve every other complete locked record,
root Cargo.lock and all fixture sources. Reuse the existing30-outcome native
controller in both profiles, both topology inputs and the existing owning strict
lint/format checks. Actual Linux submitted-head artifacts, review and required
CI remain delivery gates; existing runtime/Behavior source is unchanged.

A coordinator integration worktree starts from actual latest main7139409,
then merges pending combined9b6 without changing parent PR heads. The merge
represents actual integration ancestry, not a new semantic design. Exact source
tree must equal9b6 before the two dependency edits. Parent delivery order
remains339→340→adoption; their immutable-head checks may overlap. The existing
root dirty records and untracked rejected-model index are preserved separately.
No raw fixture/fake provider evidence becomes production identity or full NET1.

Replay minimization checkpoint before verification: one owning test path
+1093/-7/net1086,total2207, SHA
0a5946ea42631f823fd64773050d9028672d4608a219ef1da8963445b8432666.
Shared pending-entry interpretation now replaces the competing owner; duplicate
conflict/history fields and identical prefix-oracle syntax were removed while
both actual actor traces remain checked. No new compilation restarted.

Recommended finite provider-record representation uses independently specified
identity/[u8;2] protected bytes/[u8;1] proof tuples, rather than constructing
expected records through the same request allocation function as incoming
requests. Standard literal arrays remove the mirrored setup dependency and
extra allocations without changing incoming Box custody. Explicit expected Box
literals are equivalent but expand irrelevant allocation syntax; the standard
array tuple is selected before its edit. This is finite deterministic fixture
evidence only, no wire schema, production parser/profile or public API choice.
Final formatted measurement precedes positive execution and intended inversions.

Replay actual controls: minimized source e25ca1b2 remains+1093/-7/net1086.
Debug replay positives pass2/2; optimized owning file passes5/5, covering six
actual Applications plus all three original admission controls. No inverse
credit yet. Strict Clippy exposes two style sites: dropping a borrowed Pin
value and one112-line coherent post-native assertion function. Select an
ordinary lexical block to end the borrowed poll, and one narrow expected
too_many_lines lint on that inseparable full-custody assertion. Alternatives
boxing or splitting the retained outcome trace add allocation/plumbing with no
semantic need. Forecast adds at most5 lines within1100; actual formatting and
measurement still precede verification. No runtime/API/model choice changes.

Prepared replay inversion policy: forget accepted retention, ignore capacity,
ignore exact binding and wrap the checked issuer must reach their actual
post-native admission-count violations. Omitting byte conflict or allowing
unbound fallback instead reaches the typed refusal oracle after complete native
receiving; existing accepted retention still prevents another send. Do not
perturb extra admission state to manufacture a native violation for those
refusal laws. Exact unique inverse-line accounting precedes execution and
restored controls remain mandatory.

PR339 merged44a4e3961bb444f5c26afc31f42d03248f60248e
at2026-10-11T01:11:23Z after exact38d61a5 review
PRR_kwDOTCRE7M8AAAABRraKVg and all six successful checks, including Nix
run38098482087. PR340 then merged1f8673350dcd30ad45015c62f65524a2e005fadb
at2026-10-11T01:11:42Z after exact9b6d0b8 review
PRR_kwDOTCRE7M8AAAABRrcLUA and all six checks, including Nix38099128194.
Both full logs retain actual Driver2048 and four Observe1024 campaigns;
optional Miri shell construction is not executed Miri evidence. Actual Linux
artifacts and exact source trees are independently verified; forced deaths
remain uncertainty. Premerge custody remains
/tmp/bombay-pr339-before-merge-proof.json and
/tmp/bombay-pr340-before-merge-proof.json. Full AUTH1/NET1 is still open.

Published1.10.3 adoption's six local commands now pass with all30 expected
outcomes in each profile. Existing nested source and317 other locked records
remain unchanged. Independent graph/source/artifact review passes. The final
delivery branch must reconcile actual latest main1f867 before its focused
commit/PR; submitted-head CI and actual merge remain necessary.

Final successor-adoption source checkpoint: actual latest main1f867335 and
tested integrationbee94c1 have identical complete source trees e72efe13.
Delivery branch feat/static-remote-zenoh-retirement-delivery is created directly
from that actual main before its focused commit. The earlier integration branch
and all coordinator dirty records remain preserved. Only top nested version
and checksum change; all317 other full records, root lock and three fixture
sources remain byte-identical. Local binary hashes are independently rehashed:
debug92be3a1ba8648c02d22465e6e9cc2f5812bd20a5626dfee0d44a83bfea618330
and release24b04a5d31e452826d63b5272d1dd200ed6f98ec381ae8a64f5ec345ee8bfecb.
All six affected pinned-Nix commands and each profile's30 expected outcomes pass.
Independent review retains four deliberately forced late deaths as uncertainty,
four caller-refusal cases as actual closes with exit1, and six healthy overlaps.

Measured stage before this checkpoint paragraph: five tracked/untracked paths,
production+0/-0/net0, tests+0/-0/net0, manifest/lock+3/-3/net0,
documentation+416/-6/net410, public types+0/-0. Complete cumulative Bombay
source:81 paths, production+1725/-475/net1250,tests+11095/-661/net10434,
documentation+6371/-88/net6283, previously delivered public types5.
Across owners core Rust2051 and retained tests12110 remain separately accounted;
the unretained replay comparison has its own measured checkpoint. Owning
retirement inversions/Miri and publication/consumer evidence precede adoption;
no new law is inferred from a version edit. Required submitted-head review/CI
and actual merge remain necessary before recording this adoption delivered.

Replay final healthy-source checkpoint: SHA
1590600fdc3ba48d21a86ff98a315c4d86c2e7c26acd59c78c5b71d953312a25,
+1099/-7/net1092 within retained1100. Strict owning Clippy passes; final-style
full debug/optimized controls remain separately collected before inversions.
Pinned rustfmt measurement of the six prepared cuts finds16 unique added
source lines: forget-admitted9, binding omission0, capacity1, checked-wrap1,
conflict1, unbound fallback4. Original context copies do not count as newly
authored source, but the full formatted negative edits do.

Recommended unique-inverse ceiling increases8→16 before mutant application.
Retained1100 plus16 gives authored-stage1116; cumulative13300 remains
sufficient for prior12110 plus1116=13226. The four actual native-count and
two post-native typed-refusal laws remain intact; no source packing or extra
state perturbation is authorized. The earlier separate provider sealing proof
is not claimed as a new replay private-construction inversion. Every negative
run must reach its intended independent oracle and restore exact healthy source.

Successor adoption PR341 is open at exact2d2d3ecdcdf6b071def338d6af9a13fba49166b2
from actual main1f867335, with reviewPRR_kwDOTCRE7M8AAAABRrhkIA. Required
complete-source CI and actual Linux artifacts are in progress; no adoption merge
or full feature completion is recorded. Its immutable submission includes the
verified .3 graph and then-current decision/delivery records. Later replay
measurements are coordinator-only pending records until their own delivery.

Next implementation gate from independent eligibility audit: ordinary local
Application/service composition, typed completion constructors, exact native
admission/retirement and exclusive authority-update progress have concrete
proof. Stop re-researching those owners. First AUTH1 local implementation does
not require wire compatibility, live Zenoh or production KERI. The actual
remaining B gate is one source-verified bounded provider/command/error profile
and application-owned consumer: reserve its conservative original/evidence/
comparison/result custody before verification and retain charges through
unreceived completion. Fixture Box/header and replay-map counts alone cannot
claim that production profile. Current provider deadline arguments and finite
assignment numbers also do not prove production clock sampling, provider-owned
freshness or configured assignment-to-recipient authority. Freeze those precise
contracts before production edits, using existing standard/Tokio/native owners;
no new public facade/trait/macro follows from the comparison.

Replay exact1590600f campaign completed24 commands: twelve intended negative
commands exit101 at both providers' executed independent oracles; twelve
restored commands pass2/2. Log-hash custody and exact source restoration are
retained in /tmp/bombay-replay-inverse-campaign.json, SHA
cdf8baa9eed6ff1d97a09ed73dcf61587f8fc74672b49530ed2cb579383125b5.
Final independent diagnostic/source review remains a gate.

Select one final finite fixture refinement before edits: Exhaustion's separate
Application receives a distinct configured runtime assignment29, rather than
reusing Live19 after Replacement23. This preserves the selected test-level
no-reused-assignment policy; it supplies no production fresh issuer or global
fencing. Keep within1100 by removing redundant observations, without weakening
any custody/oracle. Preserve completed1590600f campaign as the prior exact-stage
evidence. Final controls/lint and the affected checked-wrap inverse pair must
run on the refined exact source; unaffected cuts remain explicitly tied to their
original source stage. The source header accurately labels private admission/
replay evidence with no production identity/network protocol.


Replay final source and independent custody checkpoint: retained source
28412beeedb9ff581a5165bf4904fa14b14a6c6577a8250df35418b93950d4e2,
2212 lines, tests+1099/-8/net1091; production/API/dependencies unchanged.
The six-law campaign remains tied to exact1590600f. Final source changes only
the truthful private-evidence header, distinct Exhaustion assignment29 and
stronger complete old-binding assertions. Eight additional commands on exact
28412 include full five-test controls in debug/optimized profiles, checked-wrap
negative/restored pairs, owning strict Clippy and formatting. All32 command logs
are independently rehashed in /tmp/bombay-replay-root-custody-proof.json.
Four native-count omissions produce6/2,1/0,3/2 and2/1 admissions respectively;
conflict omission yields Capacity rather than Conflict and unbound omission
loses the required typed refusal, after native/notification custody is acquired.
Do not describe these latter two as extra native admissions. All restored
controls pass; final campaign SHA
f95e437199c22fd93cb7a835962166e45251f19c388b3465b6b10a0364dc47f9.
Existing local sender replacement is not a Zenoh reconnect. Numeric configured
assignments are not a production freshness issuer. No protected-reply cache,
whole-heap bound or complete AUTH1/NET1 acceptance follows from this comparison.

PR341's corrected submitted head is5621637b6568e87084f02200b11824fe0bdc30e8,
reviewPRR_kwDOTCRE7M8AAAABRrjSvw. Its two-document correction aligns readiness
and AUTH1 prerequisites without changing source, graph or earlier semantic
inputs. Only checks/artifacts on5621637 can authorize its actual merge; the old
2d2d3ec submission remains historical evidence, not current-head CI credit.

Next resource-profile preparation: two protected Box buffers and heap-free
provider receipts/errors do not by themselves bound verification cells,
notification cells, replay-map allocation or returned-but-unconsumed originals.
Select ordinary pre-reserved standard storage as the first comparison; evaluate
nonwaiting Tokio owned permits as an alternative before selection. Awaited
reservation introduces waiting callers, and mailbox capacity ends before
service result consumption. Prepare one actual application-owned service
consumer that reserves explicit count/storage before verifier invocation,
retains charges through stalled verification, borrowed cancellation and unread
completion, and releases operation reservation only on explicit consumption.
Replay protection remains separately retained. No public facade is authorized
by this preparation, and no default limits are inferred. Existing arbitrary
waker resources, allocator/RSS and shared runtime allocations require separate
claims; the supported profile must state exactly which storage it accounts.


Combined replay delivery checkpoint: all five required pinned-Nix commands pass
on the integration source: workspace all-target tests571 passed/0 failed/one
existing ignored test; workspace build; workspace tests including documentation;
rustfmt check; strict workspace all-target Clippy. Exact command/log hashes are
in /tmp/bombay-replay-combined-checks.json. The source-only independent coordinator
review finds the retained entries installed before native attempt and the same
existing authoritative admission states conserved across Full/refusal/acceptance;
no extra lifecycle, mailbox or runtime owner is introduced. Author evidence
reconciliation is separately identified as author provenance, not independent
approval. Required submitted-head CI and actual reviewed merge remain gates.

Complete tracked/untracked checkpoint before this paragraph: stage two paths,
production+0/-0/net0,tests+1099/-8/net1091,documentation+98/-0/net98,
public types+0/-0, manifests/lock unchanged. Cumulative Bombay81 paths,
production+1725/-475/net1250,tests+12186/-661/net11525,
documentation+6496/-89/net6407, five previously delivered public types.
Across-owner retained tests13201 remain below13300;16 unique negative-edit lines
are separately authored, not repeated copies. Core Rust2051 remains unchanged.
No production feature is marked merged by these private comparison checks.
