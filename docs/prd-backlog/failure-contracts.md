# Stack failure contracts and required properties

Research snapshot: 2026-09-29. This is a **proposed acceptance contract for the
completed stack**, grounded in [selected source evidence](evidence.md). It is
not a statement that the current runtime passes these properties. Existing
local laws remain owned by their normative documents and dependency APIs.

## Failure model and limits

The initial distributed profile should tolerate process crash/restart,
arbitrary message delay/loss/duplication/reordering at the application boundary,
network partitions, paused hosts, stale caches, bounded resource exhaustion,
storage errors and commit uncertainty. It should reject unauthorized and
malformed remote input. Each property below is a deliberate Bombay integration
policy unless identified as an existing owner contract.

Safety must hold during a partition: do not claim a false success, accept a
superseded durable writer or silently lose a committed effect. Progress is
conditional: an authorized owner, required verification evidence, sufficient
storage/coordination availability, eventual communication, finite work and fair
executor scheduling must become available. Permanent partitions may leave
operations unavailable indefinitely. No universal recovery-time bound follows
from authentication or reliable links.

The initial profile does not promise progress after loss of every durable copy,
security after compromise of every relevant trust root, or Byzantine agreement
for arbitrary application state. Key compromise, duplicity and corrupted
storage have explicit containment/recovery cases below; surviving them depends
on selected trust thresholds, backups and backend contracts. A malicious fully
authorized actor can violate its domain policy unless a separate trusted
resource enforces that policy.

“Every possible failure” is an open-ended space. The tractable obligation is to
enumerate each external boundary, every pre/post-commit crash point, the fault
classes below and their interactions, then verify invariants over generated
sequences. A finite scenario table alone is not a proof of all executions.

## Stack research conclusions

| Layer | Existing responsibility | Required failure distinction |
| --- | --- | --- |
| Behavior | Pure transition and complete typed Actions. | Domain rejection versus emitted request versus interpreter settlement. |
| Behavior Actors | Supervision, pool, timing, routing and shutdown policy. | Policy decision versus actually completed worker activation/restart. |
| Driver | Universal causal execution through one affine Environment. | Fold error, effect interpretation error, cancellation and retained terminal custody. |
| Address | Local endpoint claim, resolution and lease retirement. | Stable local name versus exact generation; invisible reservation and committed publication are implemented locally under Address 0.3.0 (ARC-006). Remote export/hosting authority still needs separate proof. |
| Communication | Two-lane delivery, user pressure, closure and rejected payload custody. | Mailbox acceptance versus application processing; network input must not bypass bounds through control traffic. |
| Observe | Completion publication and waiting. | An observed authoritative local outcome versus unavailable knowledge of a remote process. |
| Timers | Actor-owned scheduling and generation safety. | Due signal versus valid current generation; volatile timer versus durable reminder. |
| Bombay/Entity | Local activation, task hierarchy, stable local entity admission and passivation. | Existing local recovery coordination versus unimplemented cluster ownership and durable hydration. |
| Tokio | Cooperative task execution and runtime scheduling. | Actor failure versus host failure; multiple threads do not preempt an unbounded synchronous actor turn. |
| Mnesis | Aggregate/event-store contracts, optimistic append, snapshots and projections. | Conflict, definitely uncommitted append, unknown commit, committed events and optional snapshots. |
| mnesis-bombay | Bounded conflict replay and factual command phases/outcomes. | In-process phase evidence versus durable command-result identity across restart. |
| Zenoh | Connectivity, named publication, query/reply and discovery services. | Network acceptance versus remote admission/commit; matching/liveliness versus exclusive hosting. |
| KERI/Selo | KERI specification supplies identity evidence; Selo implementation currently supplies naming. | Identifier shape, valid cryptography, accepted key history, application permission and fresh hosting authority. |
| Kubernetes | Container scheduling and lifecycle orchestration. | Pod replacement versus confirmed death/fencing of an old application writer. |

Zenoh query target selection may select multiple queryables; BestMatching is
not an actor ownership election. Query consolidation can suppress replies
according to its mode. These settings therefore need command-specific review.
[QueryTarget](https://docs.rs/zenoh/latest/zenoh/query/enum.QueryTarget.html),
[ConsolidationMode](https://docs.rs/zenoh/latest/zenoh/query/enum.ConsolidationMode.html)
(inspected version 1.10.1; not selected in Bombay's lockfile).

KERI's witness and duplicity mechanisms concern key-event evidence and its
trust assumptions. Application hosting grants and command ordering require an
additional defined protocol. That conclusion is an application-design inference,
not a limitation on using KERI to authenticate those grants.
[KERI specification](https://trustoverip.github.io/kswg-keri-specification/).

Kubernetes StatefulSets supply stable pod identity and storage associations.
Bombay still needs its own node runtime generation and durable writer checks;
pod identity is not an actor commit authorization protocol.
[StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).

## Properties

These IDs name proof obligations, not new Rust types. Existing means the
owning contract exists; it does not waive integration verification.

| Property | Origin | Required invariant |
| --- | --- | --- |
| LAW-01 | Existing Behavior | Every externally observable actor effect is represented in Actions and interpreted outside the fold. |
| LAW-02 | Existing local composition | One active actor incarnation has at most one fold executing at a time. Independent actors may run concurrently. |
| LAW-03 | Existing local activation law; ARC-006/EXEC evidence retained in the status index | No resolvable ready endpoint represents rejected initialization. Earlier accepted effects remain facts even when later initialization work fails. Remote export extends this law and needs its own witness. |
| LAW-04 | Existing ownership law | Every affine request is accepted once or returned once on definitive rejection; no reconstructed/duplicated custody. |
| LAW-05 | Existing generation law, extended to network | Exact recipients, receipts, timer signals and I/O completions cannot affect a newer incarnation by name reuse. |
| LAW-06 | Existing authoritative-fact conservation | Independent consumers receive their required facts; one policy's restart/discharge does not erase another observer's outcome. |
| LAW-07 | Proposed distributed knowledge law | Timeout, lost connectivity and cancelled waiting never establish nonexecution or definitive remote death. |
| LAW-08 | Proposed authentication law | A trusted principal is constructed only from sufficient verified evidence, never from a path, claimed sender or identifier shape. |
| LAW-09 | Proposed authorization law | Node identity, actor identity and operation/hosting permissions are distinct; each accepted operation has applicable authority. |
| LAW-10 | Proposed single-writer profile | Each accepted durable write is checked against the authoritative current hosting grant at the resource's commit boundary. After supersession, old grants cannot commit. |
| LAW-11 | Proposed ownership monotonicity | Stale renewal, release, advertisement or backup state cannot supersede a newer authoritative generation. Epochs are scoped and cannot wrap/reuse silently. |
| LAW-12 | Existing Mnesis append contract | Expected-version check and event insertion are atomic; version gaps/duplicates within a stream are rejected. |
| LAW-13 | Proposed durable command contract | When duplicate recognition is promised, command scope, fingerprint, result and mutation are recorded atomically or under an equivalent proven contract. Same identity/different command is rejected. |
| LAW-14 | Existing execution distinction, extended remotely | Definitely uncommitted, committed, accepted no-op and uncertain commit remain distinct; only evidence can refine uncertainty. |
| LAW-15 | Proposed reliable effects | Every promised post-commit effect has a recoverable durable intent; relay retries cannot make the command execute again implicitly. |
| LAW-16 | Existing checkpoint principle | A durable checkpoint never advances beyond the state/effects it claims have been committed under the consumer's contract. |
| LAW-17 | Proposed bounds | Untrusted ingress, verification, pending requests, retries, replay and retained results have explicit limits and overflow outcomes. |
| LAW-18 | Existing lifecycle ownership, extended to I/O | Every spawned task/declaration/lease has a lifetime owner; retirement joins or explicitly discharges it without losing committed facts. |
| LAW-19 | Proposed confidentiality/isolation | Tenant, deployment and protocol scope are authenticated where relevant; credentials/private payloads do not leak through normal diagnostics. |
| LAW-20 | Proposed conditional liveness | Under declared availability, fairness and finite-retry assumptions, admitted work eventually produces its promised outcome or an explicit terminal disposition. |
| LAW-21 | Proposed compatibility | Unknown protocol/storage versions cannot be interpreted as a different valid message/state; mixed-version operation follows a declared compatibility relation. |
| LAW-22 | Proposed evidence honesty | Simulation, pure tests, real-network tests and backend crash tests establish different claims and are reported separately. |

LAW-10 allows an old commit that linearized before ownership supersession to
be observed afterward. It forbids old authority from committing after the
supersession boundary; it does not require deleting legitimate earlier work.
Two hosts may simultaneously believe they own an actor. The invariant is about
accepted authority and resource effects, not the contents of their memories.

## Failure matrix

Each row gives the minimum rule, recovery owner and negative witness. Scenario
IDs are distinct from backlog IDs. Groups reference the PRD requirements that
must implement or verify them. No row is marked as currently passing.

### Local actor execution — ACT, INT, SUP, POOL, EXEC

| Scenario | Failure / race | Rule and recovery owner | Required witness |
| --- | --- | --- | --- |
| F-01 | Initialization domain error | Bombay preserves the error and retires acquired local resources; LAW-03/18. | Concurrent resolution never finds a ready endpoint. |
| F-02 | Later initialization effect rejected after earlier send succeeds | Preserve accepted prefix and exact rejected custody; Behavior/Bombay, LAW-03/04/06. | Recipient still sees the earlier accepted message; caller is not told everything rolled back. |
| F-03 | Address collision | Address/Bombay reject fresh installation; no implicit replacement. | Existing actor remains reachable and owns its lease. |
| F-04 | User mailbox full | Communication returns/retains payload according to send contract; LAW-04/17. | Saturation never silently drops or duplicates the rejected input. |
| F-05 | Close races with reserved send | Communication retirement owns the linearization; LAW-04/18. | Every racing input appears exactly once in admitted custody or rejection. |
| F-06 | Duplicate worker readiness/activation receipt | Behavior Actors plus interpreter correlate exact attempt; LAW-05. | Replay in optimized build cannot initialize or activate twice. |
| F-07 | Worker fails after assignment admission | Pool policy classifies retry, failure or uncertainty; LAW-06/14. | Job is not silently reexecuted under an assumed nonexecution fact. |
| F-08 | Restart budget exhausted | Existing supervisor policy decides terminal disposition. | Actual runtime trace matches owner policy including backoff and denial. |
| F-09 | Observer cancelled while another waits | Observe/Bombay preserve independent consumers; LAW-06. | Second observer receives the full original termination fact. |
| F-10 | Stale timer fires after cancellation/replacement | Timers and owning behavior reject obsolete generation; LAW-05. | Current deadline/worker state remains unaffected. |
| F-11 | Actor panic or capability task panic | Bombay records the correct failure origin and retires descendants; LAW-06/18. | No normal-completion report or orphaned child. |
| F-12 | Synchronous actor work never yields | Cooperative execution cannot guarantee bounded progress; EXEC policy must bound work. | Document loss of liveness; verify other actors only under the selected executor assumptions. |

### Network and protocol — WIRE, NET, ZEN, DISC, QUERY, TOPIC

| Scenario | Failure / race | Rule and recovery owner | Required witness |
| --- | --- | --- | --- |
| F-13 | Connection fails before request leaves | Networking returns definitive local rejection only with evidence; LAW-04/14. | Original request remains recoverable without possible remote admission. |
| F-14 | Connection fails after possible delivery | Networking returns uncertain outcome; LAW-07/14. | No automatic non-idempotent retry based solely on timeout. |
| F-15 | Reply lost after processing | Caller reconciliation uses original request identity; durable integration if promised. | Same operation does not mutate twice; volatile path truthfully remains uncertain. |
| F-16 | Duplicate or reordered messages after reconnect | Protocol enforces its declared duplicate/order scope; LAW-05/13. | Replayed request/reply cannot settle a different operation. |
| F-17 | Multiple queryables match a command | Placement/authorization selects valid owner independently of Zenoh routing; LAW-09/10. | Two matching responders cannot both commit under incompatible ownership. |
| F-18 | Query consolidation drops intermediate progress | Configure reply contract intentionally; QUERY owns interpretation. | Accepted and committed milestones are preserved when both are promised. |
| F-19 | Malformed/oversized message or wildcard injection | Decode/namespace boundary rejects before trusted admission; LAW-08/17/19. | Allocation remains bounded; no unintended subscription scope. |
| F-20 | Unknown schema/version | Protocol rejects incompatibility; LAW-21. | No fallback decoding as a different command. |
| F-21 | Slow subscriber or unavailable destination | Networking applies explicit overflow/expiry policy; LAW-17. | Retention stays bounded and outcome reports loss/pressure truthfully. |
| F-22 | Router restart or topology split | Zenoh reconnection plus Bombay pending-operation policy; LAW-07/18. | Declarations recover without duplicate actor initialization or false acknowledgements. |
| F-23 | Stale discovery/presence entry | Treat as route hint, not current hosting proof; LAW-09/11. | Stale endpoint cannot bypass grant/actor generation checks. |
| F-24 | Network peer floods control-like messages | Network gateway validates and bounds before local control admission; LAW-17. | Unauthenticated peers cannot fill the unbounded trusted control lane. |

### Identity and authority — ID, AUTH, SELO

| Scenario | Failure / race | Rule and recovery owner | Required witness |
| --- | --- | --- | --- |
| F-25 | Attacker publishes another AID in path/from field | Identity verification establishes principal; LAW-08. | Shape-valid spoof remains untrusted. |
| F-26 | Valid node attempts unauthorized actor operation | Application/hosting policy denies; LAW-09. | Correct signature cannot grant stop/spawn/write authority by itself. |
| F-27 | Verification evidence unavailable | Identity returns unavailable/stale under explicit policy; LAW-07/08. | No implicit allow and no false claim of invalid signature. |
| F-28 | Key rotation races with delayed signed command | Selo checks event context and policy-selected freshness; AUTH owns acceptance timing. | Valid historical signature and currently authorized command are not conflated. |
| F-29 | Grant revoked while operation is pending | Commit authorization policy applies at its specified boundary; LAW-09/10. | A precheck does not defeat a mandatory commit-time revocation/fence check. |
| F-30 | Two pods clone one node's keys | Provisioning detects/prevents unintended reuse; placement still fences runtime lifetimes. | Valid identical identity proof does not establish uniqueness of running process. |
| F-31 | Controller/key compromise or KERI duplicity evidence | Selo trust policy quarantines/revokes/reconciles using selected recovery rules. | Conflicting verified evidence does not silently select arbitrary application authority. |
| F-32 | Signed request replayed to another actor/deployment | Signed scope binds destination, purpose and deployment; LAW-13/19. | Valid bytes from one domain cannot authorize another. |
| F-33 | Forged reply through an otherwise trusted router | End-to-end proof or an explicit trusted-gateway contract protects sender/target binding. | Caller rejects an unrelated node's correlated-looking reply. |
| F-34 | Identity bootstrap service unavailable | Explicit local bootstrap/evidence retrieval path; SELO owns trust. | Cold start does not recursively require its own unavailable distributed identity actor. |

### Ownership and failover — PLACE, FAIL

| Scenario | Failure / race | Rule and recovery owner | Required witness |
| --- | --- | --- | --- |
| F-35 | Concurrent claims for one actor | Selected placement authority orders/denies claims; LAW-10/11. | No two incompatible grants can authorize post-boundary writes. |
| F-36 | Old host pauses, new host takes ownership, old resumes | Storage/effect resource rejects superseded grant; LAW-10. | Old host reloads latest stream version and still cannot write under obsolete authority. |
| F-37 | Ownership renewal reply lost | Authority evidence determines current rights; timeout preserves uncertainty. | Host cannot assume unlimited extension or blindly release a newer grant. |
| F-38 | Placement authority partition/unavailable | Follow selected consistency profile; restrict ownership changes without required proof. | Minority/uninformed side cannot invent an authoritative new owner. |
| F-39 | Delayed release/renewal/advertisement arrives | Compare scoped generation and issuer authority; LAW-11. | Old message cannot overwrite newer placement. |
| F-40 | Migration crashes before cutover | Old grant/state remains authoritative under the selected handover protocol. | Partial transfer does not publish a second unfenced writer. |
| F-41 | Migration crashes after cutover | New authority recovers from committed position; LAW-10/15. | Old host cannot reclaim ownership from an unfinished local migration record. |
| F-42 | Lease deadline affected by clock jump/pause | Authority contract specifies clock bounds or avoids local-time exclusivity assumptions. | Inject clock error and prove resource fencing still holds. |
| F-43 | Grant epoch exhausted or restored from old backup | Stop unsafe issuance and reconcile authority; LAW-11. | No silent wraparound or reuse of previously valid grant numbers. |
| F-44 | Node dies with volatile-only actor state | Report state loss/reinitialize only under explicit application policy; LAW-07/14. | No invented reconstruction of final Behavior or unsent volatile values. |

### Persistence and effects — DUR, OUT, REM

| Scenario | Failure / race | Rule and recovery owner | Required witness |
| --- | --- | --- | --- |
| F-45 | Repository load fails before decision | mnesis-bombay reports storage failure with no append claim. | No domain success or retry classified as a version conflict. |
| F-46 | Optimistic append conflict | Existing bounded replay reloads current state; LAW-12. | Only confirmed conflicts retry; attempt budget is enforced. |
| F-47 | Disk full/connection error during append | Backend evidence classifies definitely uncommitted versus ambiguous; LAW-14. | Error name alone does not imply rollback if commit may have happened. |
| F-48 | Process dies after commit before reply | Durable command record reconciles original identity where promised; LAW-13/14. | In-process PhaseTracker is not used as post-crash proof. |
| F-49 | Reused command ID with different payload | Durable command scope includes fingerprint; LAW-13. | Reject identity collision; never return unrelated cached success. |
| F-50 | No-op or domain rejection then identical retry | Selected result-retention policy defines whether decisions are durably remembered. | No claim of durable deduplication when existing Ignored issued no append. |
| F-51 | Snapshot save fails after event append | Existing Mnesis best-effort snapshot policy retains committed outcome. | Recovery replays events; caller is not told committed command failed. |
| F-52 | Corrupt event, unknown schema or incompatible snapshot | Stop/quarantine or verified fallback according to owner contract; LAW-21. | Never skip authoritative events and silently continue with fabricated state. |
| F-53 | Lost post-commit wake/notification | Subscription catches up from durable position; LAW-16. | Event remains discoverable despite missed wake. |
| F-54 | Crash after outgoing send before relay checkpoint | Outbox retries under explicit duplicate policy; LAW-15/16. | No committed intent lost; recipient mutation deduplicates where promised. |
| F-55 | Projection/checkpoint write interrupted | State and checkpoint commit together; LAW-16. | Restart cannot skip an event whose state was not committed. |
| F-56 | Deduplication retention expires before a late retry | Product states retention horizon and rejects/reconciles old requests appropriately. | Never advertise lifetime exactly-once behavior from a bounded forgotten history. |
| F-57 | Storage restore loses recently acknowledged commits | Disaster-recovery contract reports RPO/data loss and invalidates unsafe authority. | Restore is not mislabeled as lossless recovery; command/grant reuse is addressed. |
| F-58 | Reminder delivered twice after host crash | Durable reminder identity and consumption enforce selected semantics. | Duplicate wake cannot duplicate a protected durable mutation. |
| F-59 | External side effect succeeds, acknowledgement lost | External idempotency/reconciliation is required; LAW-14/15. | No promise of atomicity with an unrelated nontransactional service. |
| F-60 | Stale replica answers a post-commit read | Honor required read-your-writes position or explicitly return stale/unknown. | Caller cannot mistake lagging projection state for proof its command did not commit. |

### Deployment, overload and upgrades — OBS, K8S, RELEASE, PERF

| Scenario | Failure / race | Rule and recovery owner | Required witness |
| --- | --- | --- | --- |
| F-61 | SIGTERM during startup or commit | Withdraw readiness, stop new admission and preserve known command facts; LAW-14/18. | Deadline expiry reports unfinished/uncertain work rather than success. |
| F-62 | SIGKILL/OOM/node isolation | Recover from durable state without assuming cleanup ran; LAW-10/15. | Old writer fenced; retained durable work is replayed. |
| F-63 | DNS/router unavailable during startup | Explicit bounded startup/reconnect policy; readiness stays truthful. | No healthy advertised node with mandatory networking unavailable. |
| F-64 | Certificate expires or insecure link is discovered | Fail closed under configured secure-link policy; LAW-08/19. | No silent downgrade during reconnect. |
| F-65 | Mixed binary/schema versions during rollout | Declared compatibility gates admission/hydration; LAW-21. | Unsupported combination fails explicitly before mutation. |
| F-66 | CPU/memory/I/O exhaustion | Enforce configured bounds and explicit rejection; LAW-17. | Measure retained memory under prolonged overload and recovery afterward. |
| F-67 | Telemetry sink fails or cardinality explodes | Bound/isolate observability work; LAW-06/17. | Business facts and actor termination do not depend on successful metric export. |
| F-68 | Operator restores keys/config/storage inconsistently | Validate deployment/authority generations before readiness. | Stale credentials and fresh storage, or the reverse, cannot silently create authorized duplicate ownership. |

## Combined-failure campaigns

Single-fault tests are insufficient. At minimum generate these sequences:

| Campaign | Sequence | Invariant / recovery result |
| --- | --- | --- |
| C-01 | Admit command → commit → partition reply → move actor → retry original identity | One protected mutation; exact recorded outcome or explicit unresolved evidence. |
| C-02 | Pause owner → expire/supersede grant → new owner commits → resume old owner → old owner reloads latest version | Stream version alone cannot authorize the old writer; fence rejects it. |
| C-03 | Rotate key → partition verifier → revoke hosting grant → deliver delayed request | Identity freshness and hosting authority are decided independently under declared policy. |
| C-04 | Fill mailbox → begin shutdown → reserved send completes → stale timer/reply arrives | Every admitted/rejected value has custody; no later generation is mutated. |
| C-05 | Commit outgoing intent → publish → crash before checkpoint → recipient migrates → republish | No lost committed intent; duplicate policy survives destination relocation. |
| C-06 | Snapshot fails → append succeeds → process dies → old-format history replays | Durable success remains valid; schema-aware replay reconstructs accepted state or reports incompatibility. |
| C-07 | Clone node credentials → overlap queryables → replay a signed command → partition authority | Valid identity alone cannot produce two unfenced durable owners. |
| C-08 | Roll out incompatible binary → router restarts → verification/storage unavailable → drain deadline expires | No false readiness, no invented rollback/termination, bounded retained resources. |
| C-09 | Restore old backup → receive stale renewals → encounter newer external effects | Reconcile restore boundary and authority before issuing new work; report any unrecoverable loss. |
| C-10 | Repeated duplicate/delay/loss plus finite resource limits, followed by stable recovery | Safety holds throughout; conditional liveness resumes without unbounded queues/retries. |

## Verification plan

1. Pure owner traces and compile-fail tests prove policy and authority shape.
2. Local interpreter regressions prove actual delivery, activation and custody;
   replay lifecycle/generation inputs in debug and optimized builds.
3. Loom/exhaustive small-state tests cover shared-memory races where applicable;
   property tests generate longer event sequences with independent oracles.
4. Deterministic multi-host tests inject every F scenario at every relevant
   operation boundary and shrink failing schedules. Begin with two hosts,
   one actor and one command; expand to competing ownership and multiple actors.
5. Real Zenoh subprocess tests exercise actual serialization, query settings,
   declarations, pressure, disconnect and reconnect. Fake tests cannot replace
   these network-library contracts.
6. Selected persistent-backend tests kill/restart processes around transaction
   boundaries. Power-loss/replication claims additionally require backend and
   infrastructure evidence appropriate to those failure modes.
7. Kubernetes experiments cover graceful and forced termination, isolated old
   pods, rolling versions and key/config mistakes. Real Selo tests add actual
   cryptographic validation, rotation/delegation and trust-threshold failures.

Every scenario record must include versions, deployment assumptions, injected
fault point, observable trace, relevant LAW IDs and the result. A failing law
blocks that advertised guarantee; a missing test is reported as unverified.
No full distributed campaign has been run as part of this documentation task.

## Decisions still required before these properties are implementable

- Production backend and actual durable-commit/replication configuration.
- Placement grant issuer and its consistency, renewal and recovery contract.
- How an authoritative hosting generation is checked atomically with a write.
- Command-result persistence, identity scope, retention and no-op/rejection policy.
- KERI evidence freshness, witness/trust policy, compromise recovery and Selo bootstrap.
- Zenoh version/features, command query-target and consolidation settings.
- Whether the initial release includes durable reminders, migration and read replicas.

These are named research/design obligations. They are not assumptions silently
discharged by choosing KERI, Zenoh, Kubernetes or optimistic event append.
