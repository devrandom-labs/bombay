# Durability, deployment and product completion

See [classification marks](README.md). Persistence belongs to Mnesis and its
Bombay integration. Existing command execution, replay and conflict handling
in mnesis-bombay must be migrated and integrated, not reinvented inside Bombay.

## DUR — Durable actor execution

Owners: Mnesis storage/transaction contracts, mnesis-bombay command execution,
Bombay capability interpretation and entity lifetime. Prerequisites: a verified
compatible dependency graph and CAP. Local durable execution can be completed
before networking or Selo.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| DUR-01 | X | Select mutually compatible Behavior, Bombay and Mnesis versions. | The real integration compiles against the current selected actor algebra, not a stale example-only dependency graph. |
| DUR-02 | X | Migrate existing ExecuteRequest and execute_command integration. | Preserve existing Once/Replay, phase and conflict semantics with differential traces across the migration. |
| DUR-03 | M | Interpret durable execution requests from a running Bombay actor. | A pure fold emits a typed request and receives its correlated result through the ordinary runtime. |
| DUR-04 | D | Define the durable command acceptance and commit boundary. | Client acknowledgement states whether work is merely admitted or atomically committed. |
| DUR-05 | X | Verify atomic storage of command identity, state/events and recorded outcome. | A crash cannot leave a successful outcome recorded without its state change, or vice versa. |
| DUR-06 | X | Integrate hydration/replay into current entity activation. | A stopped process restarts with the committed actor state; ordinary cache hydration is not mistaken for durable recovery. |
| DUR-07 | X | Preserve existing conflict retries and uncertain-commit outcomes. | Conflict exhaustion, definite rejection and unknown commit remain distinct typed facts. |
| DUR-08 | M | Define cancellation after durable execution begins. | Caller or actor shutdown does not erase a committed command or report it as definitely unexecuted. |
| DUR-09 | X | Verify a real storage backend's durability assumptions. | Process crash and configured storage failure tests match the documented fsync/transaction/replication guarantee. |
| DUR-10 | D | Define snapshot, event and command schema migration policy. | Old durable records either upgrade deterministically or fail with an actionable typed incompatibility. |
| DUR-11 | X | Bound replay, hydration concurrency and storage I/O pressure. | Large recovery workloads respect configured limits and do not bypass existing Entity admission bounds. |
| DUR-12 | M | Integrate durable passivation and reactivation. | No committed state is lost, and delayed I/O cannot mutate a newer incarnation. |
| DUR-13 | X | Apply hosting-generation checks in the durable write transaction. | A former owner is fenced by storage authority, not merely its local belief or a prior authorization check. |
| DUR-14 | M | Publish a restartable durable entity example. | Commit a command, kill the process, restart, retry its identity and observe the recorded outcome without repeating the mutation. |
| DUR-15 | D | Define command-ID scope, payload fingerprint and result retention. | Same ID with different content rejects; late retries beyond retained history have an explicit outcome rather than an unsupported lifetime deduplication promise. |
| DUR-16 | D | Decide whether no-op and rejected command results must be durable. | Existing Ignored issues no append; any promised remembered result has a verified persistence path. |
| DUR-17 | X | Preserve committed success across best-effort snapshot and wake failures. | Failed snapshot/notification never turns an accepted append into a reported rollback; recovery catches up from authoritative events. |
| DUR-18 | X | Verify post-commit read-your-writes across projections/replicas. | A returned commit position is honored or lag is explicit; stale reads cannot masquerade as proof of nonexecution. |

## OUT — Reliable effects and delivery after commit

Owner: Mnesis transaction/log semantics and mnesis-bombay execution integration;
Zenoh transports publications. Prerequisite: DUR. This is a separate guarantee
from reliable transport and from local actor send acceptance.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| OUT-01 | X | Persist intended outgoing effects with the originating command transaction. | Crash after state commit but before send leaves recoverable publication work. |
| OUT-02 | M | Interpret committed outgoing work through Zenoh with bounded relay concurrency. | A backlog drains without unbounded memory and without recomputing the business command. |
| OUT-03 | D | Define publication acknowledgement and checkpoint meaning. | A transport acknowledgement is not mislabeled as a recipient's durable business commit. |
| OUT-04 | X | Make publication checkpoint updates crash-safe. | Crash between send and checkpoint permits explicit duplicate handling rather than silent loss. |
| OUT-05 | X | Integrate durable recipient deduplication where exactly-once state mutation is promised. | Deduplication and mutation share the required transaction; repeated delivery returns the recorded result. |
| OUT-06 | D | Define poison-message and permanent-rejection disposition. | Failed work remains inspectable/recoverable under explicit policy instead of blocking the queue forever or disappearing. |
| OUT-07 | M | Fence publication workers across ownership changes. | An obsolete relay cannot advance a newer owner's checkpoint or publish outside its authority unnoticed. |
| OUT-08 | D | Define guarantees for nontransactional external side effects. | Payment/webhook/file examples require idempotency or reconciliation; no general exactly-once claim exceeds the external system's contract. |

## REM — Durable scheduling

Owner: Mnesis durable schedule records plus Bombay delivery; actor-owned Timers
still owns volatile timer generations. Whether durable reminders are required
for the first durable release is a product decision, not implied by OneShot or
Periodic support.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| REM-01 | D | Select durable reminder semantics and clock assumptions. | Deadlines use an explicit persistent time representation; monotonic process-local instants are never serialized as durable time. |
| REM-02 | X | Persist reminder creation/cancellation with the relevant command. | Crash cannot create a reminder for an uncommitted command or lose a committed cancellation. |
| REM-03 | M | Recover and claim due reminders with ownership fencing. | Multiple scheduler hosts cannot both claim unchecked authority after a partition. |
| REM-04 | D | Define missed-deadline and periodic catch-up policy. | Long downtime produces bounded skip/coalesce/replay behavior chosen by the application. |
| REM-05 | M | Correlate reminder delivery and durable consumption. | Duplicate wakeups cannot apply the same durable mutation twice where that guarantee is selected. |
| REM-06 | M | Test cancellation, rescheduling and actor migration races. | A stale wakeup cannot revive a cancelled generation or bypass the new actor owner's grant. |

## OBS — Observability and administration

Owners: Bombay reports runtime facts; application policy consumes them. Observe
already provides termination observation and Entity already has metrics; neither
should be replaced by a new lifecycle or telemetry framework.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| OBS-01 | P | Expose useful runtime metrics beyond existing entity counters. | Mailbox pressure, active incarnations, activation failure, task count and shutdown duration are measurable without inspecting private state. |
| OBS-02 | M | Expose networking metrics by bounded dimensions. | Pending requests, uncertain outcomes, reconnects and rejects are visible without actor-ID cardinality explosion. |
| OBS-03 | M | Expose durable execution and recovery metrics. | Commit latency, replay lag, outbox age and ownership-fence rejection can explain stalled work. |
| OBS-04 | M | Propagate trace correlation across local, remote and durable boundaries. | One command trace distinguishes admission, execution, commit and reply without using trace IDs as authorization. |
| OBS-05 | D | Define structured diagnostic events and sensitive-field policy. | Logs retain useful typed provenance but omit secret keys and protected payloads by default. |
| OBS-06 | M | Wire operational readiness to actual configured dependencies. | A node unable to verify required authority or access mandatory storage does not advertise readiness falsely. |
| OBS-07 | D | Define authenticated administrative inspection and drain operations. | Administration uses explicit permissions and bounded responses; it does not become an unauthenticated wildcard actor endpoint. |
| OBS-08 | M | Expose uncertain/stuck operations for diagnosis and reconciliation. | Operators can identify the relevant command and authority scope without inventing a successful outcome. |
| OBS-09 | V | Verify observation/telemetry failure isolation. | An unavailable metrics sink cannot consume authoritative termination facts or stop actor progress unexpectedly. |
| OBS-10 | M | Document operational failure signatures and recovery actions. | Lost router connectivity, unavailable verification, storage failure and fenced ownership have distinguishable symptoms and procedures. |

## K8S — Kubernetes and concrete deployment

Owner: deployment artifacts plus Bombay/Zenoh lifecycle integration. Kubernetes
can run the processes, route service traffic and replace pods. It does not by
itself prove that a former actor owner has stopped writing.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| K8S-01 | M | Supply a minimal explicit-endpoint Zenoh/Bombay deployment. | Pods communicate through configured service DNS without relying on multicast discovery. |
| K8S-02 | D | Define supported router/peer topology and router redundancy. | Failure of one router follows documented availability and reconnection expectations. |
| K8S-03 | D | Define node AID lifecycle for ephemeral and stable pods. | Pod recreation deliberately preserves or replaces node identity; runtime incarnation always distinguishes restarts. |
| K8S-04 | M | Prevent accidental duplicate node-key provisioning. | Two replicas do not silently mount one node identity as if they were independent nodes; deliberate sharing has explicit fencing rules. |
| K8S-05 | M | Configure secrets, certificate rotation and secure-link policy. | Rotation succeeds without exposing keys in images/logs or silently falling back to an insecure connection. |
| K8S-06 | M | Supply network-policy and port guidance for selected Zenoh links. | A restrictive deployment works with documented traffic; unsupported implicit discovery paths are unnecessary. |
| K8S-07 | M | Implement SIGTERM drain and Kubernetes readiness withdrawal. | New placement/ingress stops before termination; admitted work has explicit dispositions within the grace period. |
| K8S-08 | M | Test forced pod death and node network isolation. | Recovery respects placement and Mnesis fencing even when shutdown hooks never run. |
| K8S-09 | D | Define storage, backup and restore deployment assumptions. | The durability claim identifies volumes/services and recovery constraints rather than relying on pod-local ephemeral storage. |
| K8S-10 | M | Verify rolling upgrades, disruption budgets and resource limits. | Compatible versions coexist, constrained pods remain bounded, and restarts cannot revive stale ownership. |

## RELEASE — User experience and support commitments

Owner: public Bombay product surface and integration documentation. Examples
must teach existing ActorExt templates first; standalone supervisors/pools use
their owning constructors. No new authoring macro is implied by networking.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| RELEASE-01 | D | Define separate local, durable and distributed release guarantees. | Users can determine what a release supports without interpreting one ambiguous complete-framework label. |
| RELEASE-02 | M | Document the local-to-remote application progression. | Users start with one typed actor, explicitly export its protocol, then run a second process without rewriting the Behavior. |
| RELEASE-03 | M | Document the local-to-durable progression. | A user selects the supported Mnesis integration and command semantics without implementing a second runtime. |
| RELEASE-04 | M | Document identity configuration with fake and production examples. | Test-only identity and production Selo requirements are explicit; normal users do not implement KERI verification themselves. |
| RELEASE-05 | P | Publish an evidence-backed feature support table. | Construction-only examples cannot substantiate executable runtime or distributed support. |
| RELEASE-06 | M | Add compile/execute gates for all newly advertised public examples. | Code shown as current API compiles on the selected dependency graph; proposed syntax stays labeled. |
| RELEASE-07 | D | Define feature flags and supported combinations. | Minimal local builds, networking builds and durable integration builds have intentional dependencies and checked combinations. |
| RELEASE-08 | M | Extend existing CI with integration/version compatibility gates. | Real Zenoh and durable-backend failures produce retained diagnostics; existing Nix and Driver gates remain intact. |
| RELEASE-09 | D | Define configuration validation and startup error contracts. | Invalid endpoints, capacity, credentials and incompatible protocols fail before false readiness. |
| RELEASE-10 | M | Publish compatibility, migration and operational support policy. | Users know supported protocol/storage upgrades and how to recover an interrupted upgrade. |

## PERF — Performance evidence

These are measurement requirements, not claims that the current runtime lacks
all benchmarks or is slow. Use comparable semantics and publish environment,
versions and acknowledgement boundaries.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| PERF-01 | V | Establish local message throughput, latency and allocation baselines. | Measurements include mailbox saturation and actor counts, with reproducible settings. |
| PERF-02 | M | Measure scaling across executor worker counts. | Report independent-actor workloads separately from one hot actor and from blocking work. |
| PERF-03 | M | Measure remote delivery through supported Zenoh topologies. | Report payload sizes, concurrency, secure-link settings and whether latency ends at admission or completion. |
| PERF-04 | M | Measure actor/session/declaration resource cost. | Large actor counts have bounded memory and startup cost; session sharing assumptions are verified. |
| PERF-05 | M | Measure durable command and recovery throughput. | Include transaction durability settings, replay size and deduplication overhead. |
| PERF-06 | M | Measure failover and drain recovery distributions. | Separate detection, authority transfer, hydration and backlog recovery times. |
| PERF-07 | M | Measure overload fairness and tail latency. | Slow customers and noisy actors do not produce unexplained unbounded retention or starvation. |
| PERF-08 | D | Define regression thresholds for representative workloads. | A release gate has a stable baseline and noise policy rather than an arbitrary comparison against unrelated actor benchmarks. |

## OPT — Explicit additional product decisions

These are legitimate actor-platform features, but an exhaustive list of other
runtimes' features is not a coherent release definition. Select each commitment
deliberately and write a separate PRD before calling its absence a blocker.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| OPT-01 | O | Multi-region placement and disaster recovery. | Define geography, latency, failure independence, RPO/RTO and authority availability before implementation. |
| OPT-02 | O | Read replicas and deliberate multiwriter actors. | Define consistency/conflict semantics; authenticated replication alone is not conflict resolution. |
| OPT-03 | O | Remote spawn using a closed catalog of allowed actor constructions. | Authority and resource bounds are explicit; do not introduce arbitrary code loading or a second actor contract. |
| OPT-04 | O | Online actor code/state upgrades. | Specify compatibility and state conversion at an exact lifecycle boundary. |
| OPT-05 | O | Durable workflows, sagas and compensations. | Reuse Mnesis ownership and existing workflow policy; specify crash-safe compensation without claiming arbitrary rollback. |
| OPT-06 | O | Cross-actor transactional commands. | State the storage/coordination assumptions and failure semantics; do not infer them from reliable messages. |
| OPT-07 | O | Durable stream processing and projections. | Define offsets, checkpoints, replay and sink transaction boundaries using owning persistence contracts. |
| OPT-08 | O | Autoscaling and automatic rebalance. | Stability, placement constraints and migration cost have explicit controls; do not merely react to queue length. |
| OPT-09 | O | Sandboxed/untrusted actor execution. | Specify resource isolation and threat model; Rust type safety and node identity are not a sandbox. |
| OPT-10 | O | Additional execution targets such as embedded or WebAssembly hosts. | Establish a separate host/dependency contract; do not promise them from the current Tokio runtime design. |
