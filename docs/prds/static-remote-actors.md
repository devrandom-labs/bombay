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

**Specification audit:** the scope and proof obligations below supersede the
initial draft's aggregate completion claims. [Acceptance](static-remote-actors/acceptance.md)
maps every selected inventory ID to an observable oracle and falsifier. All
full R01–R25 witnesses remain unexecuted; the successful EXEC tests certify
only their existing local laws. The final section records the audit corrections.

## User steering before design choices

Current instruction (2026-10-09 UTC): the user said **"from now on autoselect
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
| Communication 0.1.3; archive VCS `272a2343187b40615ab26c2d0d2e136010a16e77` | `src/lib.rs`, `tests/mailbox_retirement.rs`. | Reuse bounded user admission and exact rejected payloads. The trusted control lane is unbounded and must not become raw network ingress. |
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

`ExternalTarget` is sealed and its present `send_from` path awaits bounded
mailbox capacity; it supplies no public capacity-reservation/try-admit port.
Gate C must therefore test freshness while waiting, bounded pending sends and
cancellation ownership on the actual public path, not merely its type
visibility. ExternalActor's fixed 1,024-slot reply mailbox is an existing bound,
not a bound on arbitrarily many pending futures. Do not open the sealed trait
or add admission machinery without the precise failing law.

Source-bound admission projection proposal (2026-10-09 UTC), **unadopted**:
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

Resumed decision checkpoint against merge `a9c5b7d`: four already-accounted
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
