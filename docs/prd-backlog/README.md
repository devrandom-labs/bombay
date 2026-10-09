# Bombay completion requirements

The requirement inventory began as a planning snapshot on 2026-09-29.
[Current status and dependencies](status.md) records remaining product work and
retained evidence from the completed local audit. Historical marks below do
not establish implementation eligibility. The selected template manifest has
typed Bombay interpretation for all 19 inventoried actor-owned capabilities;
the six missing-interpreter marks describe the earlier snapshot.

## Execution workflow

1. Select requirements from this backlog and verify their prerequisites in
   [the status index](status.md).
2. Create or reconcile a focused PRD in `docs/prds/` using the authoring
   requirements below. Record exact selected dependencies, ownership, accepted
   decisions, acceptance witnesses and the change budget in that PRD.
3. Implement the complete accepted scope on a feature branch. Keep the PRD's
   verification and change record current, including remaining blockers.
4. Finish acceptance tests, required pinned-Nix checks and minimization; open
   a focused PR targeting `main` with concrete behavior and verification evidence.
5. Resolve review and CI failures, then merge through the PR once required CI
   and review gates pass. Record the PR, passing CI and merge commit in the PRD;
   update the status index and retain any deferred requirements in the backlog.

The repository instructions define the feature states. Local verification
alone does not establish merged delivery. Git preserves the retired audit
chronology; active verification and decisions belong beside the feature PRD.

The inventory contains 260 individually identified requirements, including
verification obligations and optional decisions. The failure contract adds 22
properties, 68 failure scenarios and 10 combined-failure campaigns.

## Product direction

Build a complete local actor runtime and then a durable distributed actor
runtime using **Zenoh as the sole production networking substrate**, Mnesis as
the persistence owner, and mnesis-bombay as the durable execution integration.
Identity must have a typed, testable boundary. Selo will implement the production
KERI integration downstream, using Bombay and Mnesis itself. Bombay must be
buildable and testable before Selo exists.

The earlier completion proposal considered interchangeable transports. This
inventory records the subsequent product decision: an alternative production
transport is **not** a completion requirement. A deterministic network test host
is still needed. Local mailbox delivery remains local; making Zenoh the network
substrate does not mean serializing every message inside one process.

KERI AIDs can anchor actor names and verified authority. Zenoh can provide
connectivity, publication, queries, discovery signals and configured security.
The combined stack can implement distributed actors, but the application must
still specify placement authority, delivery outcomes, persistence boundaries,
and partition policy. These are contracts to build on this stack, not reasons
to select another transport. See [the service mapping](zenoh.md).

## Reading the inventory

Each row is a separately testable requirement or a decision needed to write a
PRD, **not a claim that an entire feature is absent**. Group headers identify
ownership and dependencies; row acceptance criteria identify the observable
result, usually including an adverse case.

| Mark | Meaning |
| --- | --- |
| M | Missing from the inspected Bombay runtime path. |
| P | Partly implemented; a concrete integration or behavior gap remains. |
| V | Verification required; do not claim the underlying feature is missing. |
| D | Product or semantic decision needed before implementation. |
| X | Work belongs in, or requires coordination with, a dependency/downstream repository. Existing implementation there must be reused. |
| O | Optional expansion; decide explicitly whether it belongs in the release. |

These marks are not the status index's implementation states. None of the counts in
this inventory is a completion percentage. A small missing capability can block
many already implemented templates; a large optional feature may not block the
first release at all.

| Inventory | Scope |
| --- | --- |
| [Local runtime](local-runtime.md) | Activation, the six interpreter gaps found at this snapshot, supervisors, pools, execution, authoring and template verification. |
| [Core integration and upstream changes](core-integration.md) | Source-level supervisor/pool diagnosis, reproduced contract failures and repository-by-repository implementation handoff. |
| [Execution ownership PRD](../prds/execution-ownership.md) | Detailed EXEC contracts, module ownership, cancellation and observation laws, decision gates, verification matrix and coordinated multi-agent work packages. |
| [Static remote actor PRD](../prds/static-remote-actors.md) | Next selected NET1/AUTH1 scope, fresh dependency verification, correctness gates and parallel work plan; [post-EXEC audit](../prds/execution-ownership/post-delivery-audit.md) records the selection rationale. |
| [Networking](networking.md) | Wire contracts, Zenoh sessions, remote delivery, discovery, queries and subscriptions. |
| [Identity and placement](identity-and-placement.md) | Mockable identity, permissions, placement, failover, simulation and downstream Selo. |
| [Durability and operations](durability-and-operations.md) | Mnesis integration, recovery, outbox, reminders, Kubernetes, diagnostics, performance and releases. |
| [Zenoh architecture](zenoh.md) | What Zenoh supplies, what Bombay owns, AID naming and the intended user experience. |
| [Evidence](evidence.md) | Version boundary, concrete findings, existing functionality and template-by-template evidence. |
| [Failure contracts](failure-contracts.md) | Whole-stack research, 22 properties, 68 failure scenarios, combined-failure campaigns and verification obligations. |

## What already exists

Bombay has executable local applications, typed addresses and delivery,
two-lane communication, causal Driver execution, typed births, actor-owned
timers, observation, shutdown/retirement, external interfaces, an Axum boundary,
and an advanced local entity path with hydration/passivation. It has an actor
authoring facade and ActorExt composition over existing Behavior Actors.

Behavior Actors implements supervision, stable proxies and worker-pool
policies. The selected manifest lists 45 public compositions and 19 actor-owned
capability types, all with typed Bombay interpretation after ARC-010. This
inventory count does not prove every template policy end to end; structural
Behavior lanes and other runtime capabilities are outside that denominator.

EXEC delivered caller-host async execution and configured Builder-only owned
blocking over one paired execution/result owner. Its accepted gates,
verification and actual PR326 merge are recorded in the EXEC PRD. There is no standard
integrated Zenoh, verified remote identity, distributed placement or
Mnesis-backed actor execution path in this checkout. Neighboring repositories
contain useful code; they are not interchangeable with a working integration.

## Candidate PRDs and dependency order

Requirements within a group should be split further when a PRD would cross
ownership boundaries or repository change limits.

| PRD group | Requirements | Prerequisites and release witness |
| --- | --- | --- |
| Local activation and settlements | ACT, INT | Locked owner contracts verified; rejected initialization never publishes an endpoint and exact rejected requests remain owned. |
| Executable supervision | SUP | ACT and relevant INT leaves; real worker failure, replacement and bounded shutdown through Application. |
| Executable pools | POOL | ACT and relevant INT leaves; customers receive correlated job outcomes across worker replacement. |
| [Execution and application embedding](../prds/execution-ownership.md) | EXEC and the explicitly selected ACT/APP requirements | Ownership and API decision gates precede implementation; existing Driver laws preserved, real concurrent execution and caller-owned runtime examples. |
| Capability composition and template coverage | CAP, CAT | Existing owner types reused; executable witnesses before claiming support. |
| [Static remote actors over Zenoh](../prds/static-remote-actors.md) | WIRE, NET, ZEN, DISC and selected ID/AUTH/SIM | Local activation, identity test implementation and explicit static placement; two OS processes exchange a typed protocol. Identity, networking and verification research proceed in parallel; production depends on the PRD's exact contract gates. |
| Distributed service interactions | QUERY, TOPIC | Remote envelope and authorization; bounded replies and subscriber pressure semantics. |
| Identity and permissions | ID, AUTH | Existing Behavior effect composition; deterministic fake first, Selo adapter later. |
| Durable local entities | DUR | Selected Mnesis contract migration; restart a process and recover a committed command. Can proceed independently of networking. |
| Durable external effects | OUT, REM | DUR; crash between commit and publication, then recover without silently losing committed work. |
| Automatic placement and recovery | PLACE, FAIL | Remote delivery and identity, plus resource fencing for durable ownership; partitioned old owner cannot commit new writes. |
| Distributed verification | SIM | Begin alongside WIRE/ID/PLACE design, not after implementation; same scenario families exercise fake and real networking. |
| Deployment and operability | OBS, K8S, RELEASE, PERF | Gates grow with each product stage; Kubernetes is a deployment target, not the actor ownership authority. |
| Production Selo integration | SELO | Bombay foundation and downstream Selo services; identity bootstrap has no circular startup dependency. |
| Additional product commitments | OPT | Explicit selection and independent PRDs; not silently part of the initial completion claim. |

Start with the exact local blockers and a small two-process static remote actor
slice. In parallel at the workstream level, migrate and prove the existing
mnesis-bombay execution path. Automatic relocation comes after those contracts
work separately. This ordering does not require implementing a generic
transport framework, choosing a consensus algorithm immediately, or waiting for
Selo.

## PRD checklist

For each selected group, record:

1. User problem and a concrete executable application example.
2. Included requirement IDs and explicit deferred requirements.
3. Exact locked package revisions and patches; current source and tests from
   every affected owner. This snapshot does not waive fresh verification.
4. Ownership: state, commands, effects, authoritative facts, cancellation and
   terminal outcomes. Explain what existing owner is reused.
5. Ordinary Rust API experiments before any new public abstraction or macro.
   Snippets in a proposal must be labeled as proposed until they compile.
6. State transitions and exact outcomes for rejection, timeout, crash, replay,
   shutdown and partial external completion.
7. Negative and inversion witnesses, independent observable traces, optimized
   replay tests for lifecycle/generation laws, and proof the original defect
   fails the regression.
8. Resource bounds, pressure policy, security assumptions and deployment limits.
9. Dependency edges, migration impact, compatibility and version skew policy.
10. Smallest implementation slice, expected files/line delta/public types and
    required gates. Record the feature change budget and verification in the PRD before
    implementation under repository rules.

## Evidence boundary

The dated inventory and research reports identify observed gaps, proposals and
historical findings. They do not certify the current workspace or authorize a
production interface. Repeat feature-local verification when selecting a PRD;
use current source, owner contracts and executable witnesses for completion.
