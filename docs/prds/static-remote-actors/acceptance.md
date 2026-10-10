# Static remote actor acceptance

Normative companion to the [PRD](../static-remote-actors.md). This document
contains planned oracles, not implemented tests or proof of feature completion.
Every full R01–R25 witness is **unexecuted** until its evidence record is
populated. Executed prerequisite probes do not close a full witness. The PRD's
profile limits and user-steering requirements apply to every row. The user
now delegates automatic selection of recommended choices until revoked; the
PRD records those decisions. Delegation does not establish any passing witness.

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

The PRD is currently at design preparation. This audit verifies the specification
and coverage mapping; it does not advance any feature to active or feature-complete.
