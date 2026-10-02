# Zenoh, node AIDs and the intended application experience

## Recommended direction

Use Zenoh as Bombay's only production networking substrate. Give each logical
node its own identity, with a separate runtime incarnation on restart. Keep
actor identity independent of its current host. Use explicit hosting grants
and enforce durable ownership at Mnesis's write boundary. Keep identity
verification replaceable through typed capabilities so the runtime can be
completed with a fake implementation before Selo is ready.

This is an architectural recommendation, not an implemented interface or a
claim that identity alone provides distributed execution. KERI provides
self-certifying identifiers and verifiable key-event history, including key
rotation and delegation. Bombay must define the application meaning of actor
hosting authority and how consumers enforce it.
[KERI specification](https://trustoverip.github.io/kswg-keri-specification/).

An AID per node is useful when nodes must retain independently verifiable
identity across administrative/deployment boundaries. It adds key custody,
rotation, evidence availability and provisioning work. For a private cluster,
these costs may exceed basic service-account authentication needs; the choice
is justified here by the intended Selo/KERI ecosystem, not by a requirement of
the actor model.

## What transport means here

At its narrowest, transport moves encoded messages between processes. Zenoh
offers more: named publication/subscription and queryable services over a
distributed key space. Its storage abstraction can retain values and answer
queries. Bombay should use these facilities directly where their contracts
fit, rather than rebuilding them over a send-bytes-only interface.
[Zenoh abstractions](https://zenoh.io/docs/manual/abstractions/).

That still leaves several application contracts:

| Facility | Reuse | Contract Bombay and its owners must supply |
| --- | --- | --- |
| Connectivity and routing | Zenoh sessions, links, peers/routers/clients | Session lifetime, supported deployment configuration, bounded ingress and failure reporting. |
| Named publication | Zenoh keys and subscriptions | Exported actor protocol, decoding, authorization, mailbox admission and acknowledgement meaning. |
| Query/reply | Zenoh queryables | Command versus read semantics, responder selection, correlation, timeout and outcome uncertainty. |
| Discovery signals | Zenoh scouting, liveliness and matching | Authenticated membership facts, readiness and the distinction between unreachable and terminated. |
| Recovery/history extensions | Selected Zenoh extensions | Replay policy, cache limits and application deduplication; these do not establish a durable business commit. |
| Secure links and ACLs | Zenoh security configuration | Binding to verified KERI principals, actor permissions and hosting authority. |
| Stored key values | Optional Zenoh storage facilities | Mnesis remains the selected command transaction/recovery owner; no implied replacement of its log or atomicity contract. |
| Actor state and local execution | Existing Bombay owners | Pure Behavior folds, mailbox delivery, timers, local leases, observation and task lifetime. |

The session/topology facilities are documented in the
[Zenoh Rust API](https://docs.rs/zenoh/1.10.1/zenoh/).
Advanced subscriber history/recovery is marked unstable in the inspected API;
adopting it requires an explicit version/feature decision.
[AdvancedSubscriber](https://docs.rs/zenoh-ext/1.10.1/zenoh_ext/struct.AdvancedSubscriber.html).
TLS and access control need deliberate deployment configuration and are not
automatically KERI verification.
[TLS](https://zenoh.io/docs/manual/tls/),
[access control](https://zenoh.io/docs/manual/access-control/).

## Names and authority

Keep these concepts distinct:

| Concept | Example | Lifetime / responsibility |
| --- | --- | --- |
| Node identity | node AID N1 | Identifies a node/controller across permitted restarts and key rotations. |
| Node runtime incarnation | N1, incarnation R7 | Identifies one running lifetime; rejects obsolete callbacks/messages after restart. |
| Actor identity | actor AID A, or controller AID plus actor key | Stable application identity; does not change when the actor moves. |
| Actor incarnation | A, incarnation I12 | One activated local lifetime. |
| Hosting grant | A may run on N1 under ownership epoch E9 | Explicit authority; superseded grants cannot commit as current owner. |
| Request identity | caller C, command Q | Correlation and, when durably supported, duplicate-command recognition. |

Not every actor needs a separate AID. A controller-AID-scoped entity key can
provide stable actor naming with fewer key-management obligations. Conversely,
independently controlled actors may warrant their own AIDs. The PRD must select
the model by actor family; it should not assume identity, incarnation and
location are one string.

For example, Selo currently has names shaped like
`agents/<aid>/mailbox/<protocol>`. This can anchor a stable service route. The
wire envelope or validated route binding must still identify the intended
actor/protocol, relevant incarnation, caller, and hosting authority. Which of
those belongs in a key versus an authenticated envelope is a WIRE/SELO design
decision. This inventory does not silently replace Selo's naming contract.

An AID in a path is a claim about the destination. Anyone who knows the text
can construct that path. Authentication and authorization determine who may
publish, receive, reply or host under it.

## Two failures the composition must handle

**Old host survives a partition.** Actor A previously ran on N1. N2 acquires a
new hosting grant and recovers A. N1 is still a legitimate node with valid
keys. Both node identities can verify correctly. The grant issuer must define
which authority is current, and Mnesis must reject writes carrying the old
generation. A cached grant checked only before a transaction is insufficient
if ownership can change before commit.

**Commit succeeds but reply disappears.** N1 commits command Q and then loses
connectivity. The caller cannot determine the outcome from a timeout. The
durable execution path must look up or safely retry Q under its original
identity and return the committed result. A network resend, signature or
stable address alone does not answer whether Q already executed.

These are solvable using the proposed stack. They require explicit contracts
and tests rather than an additional transport.

## Kubernetes deployment

Use configured Zenoh endpoints and service DNS as the initial Kubernetes
profile. Zenoh supports client, peer and router deployments; explicit
connectivity is available alongside discovery.
[Zenoh deployment](https://zenoh.io/docs/getting-started/deployment/).
Applying those modes to Kubernetes services is a deployment recommendation;
this document does not claim Bombay already ships a chart or that multicast
works across every cluster network.

Provision node identities deliberately. Multiple replicas mounting the same
node private keys are not independent authenticated nodes. A stable node AID
may be reused after restart only under an explicit custody and overlap policy;
a new runtime incarnation still distinguishes the new execution lifetime.

Readiness withdrawal, SIGTERM drain, secure links, network policies and forced
pod-death recovery belong in the deployment PRD. Kubernetes replacing a pod
does not prove that an isolated former pod has lost access to storage; durable
fencing remains necessary.

## How users should use the completed framework

This is intended behavior, **not proposed Rust method names**:

1. Define typed actor state/protocol and pure Behavior actions using the
   existing authoring facade where it fits. Decorate with existing ActorExt
   policies; use owning constructors for supervisors and pools.
2. Run locally with the standard Application composition. Users do not choose
   Address, Communication, Observe or timer adapters.
3. For networking, explicitly export selected protocols and configure a Zenoh
   deployment profile, node identity and allowed peers/permissions. The
   runtime owns session and declaration lifetimes.
4. Address an actor by stable logical identity. Users receive typed outcomes
   whose meaning distinguishes rejection, acceptance, completion and possible
   delivery with an unknown outcome.
5. For durability, select the Mnesis integration and its command/recovery
   policy. Actor location can later change without changing durable identity.
6. For automatic distribution, select an implemented placement policy and its
   partition guarantees. Ordinary users should not implement leases, routing
   caches or storage fencing themselves.
7. Tests use deterministic identities and network scheduling. Production
   deployments use the Selo integration once its contracts and implementation
   are available.

The public API should make those choices explicit without exposing nested
effect-product routing machinery. Prove it with ordinary Rust and executable
examples before adopting new builders, wrappers or macros.

## Why this is not a drop-in arbitrary transport abstraction

One concrete Zenoh integration can interpret several typed capability leaves
using one shared session. Tests substitute deterministic behavior at those
capability boundaries. Selo substitutes identity verification, not networking.
This preserves testability and ownership without promising that every Zenoh
service has an equivalent implementation in an unrelated transport.

The costs of choosing only Zenoh are intentional dependency, API/version and
deployment coupling. The benefit is a smaller integration surface and direct
use of its services. Isolate that coupling at the networking boundary, while
keeping actor policy, identity facts and durable command semantics owned by
their existing layers.
