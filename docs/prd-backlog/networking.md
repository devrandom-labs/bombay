# Zenoh networking requirements

Classification and scope are in [README](README.md). Owners: Zenoh supplies the
networking services; Bombay supplies typed actor integration. This inventory
requires one production networking substrate, not interchangeable TCP, QUIC,
HTTP and Zenoh adapters. Zenoh's own links and deployment modes remain usable.

Prerequisites: verified local activation, static typed capability composition,
an identity test implementation, and initially an explicit static placement
map. Dynamic ownership/failover is a later layer. Each process may have a node
AID; an AID in an unverified message is not proof of that node's identity.

## WIRE — Remote protocol and identity binding

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| WIRE-01 | D | Define the closed set of remotely exportable protocols and their schema identifiers. | A local protocol cannot accidentally become externally callable through generic serialization. |
| WIRE-02 | D | Select versioned encoding and canonical signed bytes where signatures are used. | Equivalent decoded values cannot bypass signatures through alternate encodings; unsupported versions reject explicitly. |
| WIRE-03 | M | Implement typed encoding/decoding at the network boundary. | Every accepted payload maps to a declared protocol variant; malformed input produces a typed boundary failure. |
| WIRE-04 | D | Distinguish actor identity, node AID, caller identity and actor incarnation. | A message addressed to an actor cannot substitute a host's identity for its authority to serve that actor. |
| WIRE-05 | M | Define logical actor addressing independently of process-local Address internals. | A serialized address contains no pointer identity or local lease implementation; restart does not corrupt stable identity. |
| WIRE-06 | M | Carry exact incarnation/ownership fencing where the protocol requires it. | An old exact recipient cannot silently retarget a replacement or a migrated actor. |
| WIRE-07 | M | Bind correlation identifiers to caller, target and operation scope. | A reply for a different request, actor or caller cannot settle the current request. |
| WIRE-08 | M | Bound message sizes, nesting, key length and decode allocation. | Oversized and adversarial input fails before unbounded allocation or actor admission. |
| WIRE-09 | D | Define protocol evolution and rolling-upgrade compatibility. | Old/new nodes either interoperate under declared rules or return an explicit incompatibility result. |
| WIRE-10 | D | Define which local error and terminal facts may cross the wire. | Remote reports retain promised provenance without attempting to serialize arbitrary Rust errors or exposing private state. |
| WIRE-11 | M | Authenticate any claimed source before constructing trusted actor input. | A forged from-AID is never promoted to a verified caller merely because it appeared in a Zenoh path. |
| WIRE-12 | M | Separate public routing metadata from protected application content. | Confidentiality and authenticated fields have explicit coverage; routing visibility is documented. |

## NET — Remote actor delivery and outcomes

Prerequisites: WIRE and the chosen capability completion contract. Local
Communication remains the mailbox owner. A network subscriber is an ingress
boundary, not a second actor mailbox implementation.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| NET-01 | M | Export an explicitly selected local actor protocol over Zenoh. | A second OS process sends a typed request to a live public Application actor. |
| NET-02 | M | Implement outbound typed remote delivery as an interpreted effect. | Behavior only returns Actions; no session, callback or network handle enters the fold. |
| NET-03 | D | Define the meaning of each advertised delivery acknowledgement. | Queued in sender, accepted by remote mailbox, processed and durably committed are not conflated. |
| NET-04 | M | Preserve definitive pre-delivery rejection and original payload ownership. | Local encoding or admission rejection can return the original request exactly when the delivery contract permits it. |
| NET-05 | M | Represent possible delivery with unknown outcome explicitly. | Disconnect after remote admission cannot be reported as proven rejection or automatically safe retry. |
| NET-06 | D | Select retry policy and idempotency scope per operation. | Retrying after a lost reply follows explicit durable/non-durable semantics rather than an unconditional retry loop. |
| NET-07 | M | Bound remote ingress and propagate pressure to the selected boundary. | Flooding a slow actor cannot create an unbounded callback queue or exhaust the control lane. |
| NET-08 | M | Define and enforce outbound pressure and retained-request limits. | Disconnection does not retain unlimited requests; each rejected/expired request has a truthful disposition. |
| NET-09 | D | Specify ordering guarantees by sender, actor, incarnation and reconnect. | Tests match the promised scope and do not infer global ordering from one ordered link. |
| NET-10 | M | Handle late, duplicate and mismatched remote replies. | A late reply cannot settle a newer request or recreate a retired actor. |
| NET-11 | M | Retire network exports with actor lifetime. | A stale advertisement cannot admit work into an endpoint whose local lease has retired. |
| NET-12 | M | Define remote observation separately from node reachability. | Link loss yields unavailable/unknown knowledge, not an invented authoritative actor termination fact. |
| NET-13 | M | Authorize remote lifecycle operations independently of ordinary messages. | A permitted caller cannot stop, replace or inspect an actor without the corresponding authority. |
| NET-14 | M | Provide a two-process counter/service example with explicit error cases. | Demonstrate successful delivery, mailbox pressure, target exit and lost-reply uncertainty through the public API. |

## ZEN — Concrete Zenoh service integration

Owner: Bombay's concrete Zenoh capability composition. One session may provide
publication, queries and presence; avoid both a universal provider object and
an interface restricted to only send(bytes). Version selection remains open;
1.10.1 documentation was inspected, not adopted into Cargo.lock.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| ZEN-01 | D | Select and pin Zenoh version, feature set and supported API stability. | Required unstable or experimental extensions are explicitly justified; default builds do not acquire them accidentally. |
| ZEN-02 | M | Own session startup and shutdown in the Bombay application task hierarchy. | Startup errors reach the caller; session closure releases subscribers/queryables and joins owned work. |
| ZEN-03 | D | Select session sharing and resource ownership across actors. | Actor count does not imply one connection/session per actor; one actor's retirement cannot close unrelated actors' service. |
| ZEN-04 | M | Configure client, peer and router connectivity for supported deployment profiles. | Explicit endpoints work without multicast, including container and Kubernetes networks. |
| ZEN-05 | M | Map validated actor/service names to key expressions. | Path segments cannot inject wildcard subscriptions or escape the intended tenant/authority namespace. |
| ZEN-06 | M | Translate Zenoh callbacks/channels into bounded runtime ingress. | Callback execution never folds a Behavior directly or blocks the networking thread on application work. |
| ZEN-07 | D | Select QoS, congestion and reliability settings by message class. | Commands, replies, telemetry and bulk streams have explicit loss/pressure expectations with measured saturation witnesses. |
| ZEN-08 | M | Configure secure links and supported peer authentication. | Misconfigured certificates or unauthorized peers fail closed; discovery cannot silently select an unapproved insecure link. |
| ZEN-09 | M | Integrate Zenoh ACL policy with the actor authorization boundary. | A key-expression permission alone is not treated as proof of Selo/KERI actor authority. |
| ZEN-10 | M | Handle session interruption and reconnection with bounded recovery. | Reconnect restores declarations as required without duplicating actor activation or falsely acknowledging pending operations. |
| ZEN-11 | D | Select discovery, liveliness and matching services actually used by Bombay. | Each signal has a documented meaning; subscriber presence is not equated with actor readiness. |
| ZEN-12 | D | Decide whether advanced publisher/subscriber recovery is required. | If selected, cache limits, history scope and unstable API dependency are explicit; replay does not imply durable command completion. |
| ZEN-13 | O | Evaluate shared memory or specialized transport links for measured workloads. | A benchmark proves value and fallback behavior; correctness never depends on zero-copy delivery. |
| ZEN-14 | V | Verify networking cancellation and resource leaks under repeated startup failure. | Sessions, declarations and runtime tasks return to a bounded baseline after repeated failed attempts. |

## DISC — Discovery and publication

Owners: Zenoh signals plus Bombay export/placement facts. Existing Behavior
Actors discovery templates remain available for application policy; do not
confuse their local availability with an implemented cluster directory.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| DISC-01 | D | Define an authenticated node advertisement anchored in the node AID. | Identity, runtime incarnation, reachable services and supported protocols have distinct meanings. |
| DISC-02 | M | Publish actor availability only after the chosen activation boundary. | Discovering an actor does not expose rejected initialization as a ready endpoint. |
| DISC-03 | M | Publish protocol and deployment compatibility metadata. | A caller can reject an incompatible destination before admitting application work. |
| DISC-04 | M | Expire or supersede stale advertisements. | A restarted node's old advertisement cannot replace its newer runtime incarnation. |
| DISC-05 | D | Specify directory answer authority and freshness. | A route hint can be stale without becoming an authoritative ownership grant. |
| DISC-06 | M | Resolve logical destinations through a bounded cache and refresh policy. | Stale routing yields a typed retry/redirect/unknown result, not an unbounded redirect loop. |
| DISC-07 | M | Authenticate advertisement updates and withdrawals. | Another node cannot withdraw or overwrite a service simply by publishing the same path. |
| DISC-08 | D | Define namespace, tenant and deployment isolation. | Identical local role names in two deployments cannot collide in discovery or delivery. |

## QUERY — Requests, replies and queryable services

Owner: Zenoh query/reply mechanics; Bombay owns actor request semantics. Treat
query-target selection and reply consolidation as correctness settings, not
only performance settings.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| QUERY-01 | D | Distinguish read queries, single-target commands and fan-out queries. | A mutating actor command cannot execute on several queryables merely because its key expression matches them. |
| QUERY-02 | M | Configure query target selection and duplicate responder handling. | Overlapping advertisements cannot produce two valid command owners without triggering the ownership policy. |
| QUERY-03 | D | Define reply consolidation for each response protocol. | An accepted response and a later committed response cannot silently overwrite one another when both are semantically required. |
| QUERY-04 | M | Bound concurrent queries, replies and streaming response buffers. | A nonterminating or malicious responder cannot retain unbounded caller resources. |
| QUERY-05 | M | Distinguish query deadline from server-side cancellation. | Expiry of the caller's wait does not claim the server rolled back a committed operation. |
| QUERY-06 | M | Bind reply authenticity to the selected target and ownership grant. | A different node cannot settle a command by replying under the same path. |

## TOPIC — Distributed publication and subscriptions

These are separate from exact actor delivery. Select the scope explicitly;
Zenoh already supplies the networking primitive.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| TOPIC-01 | D | Define ephemeral versus replayable topic contracts. | A subscriber knows whether absence means no event, missed event or unknown history. |
| TOPIC-02 | M | Integrate authorized typed subscription effects. | Subscription creation, revocation and actor retirement preserve owned resources and topic permissions. |
| TOPIC-03 | M | Specify slow-subscriber overflow behavior. | Drop, disconnect or backpressure is explicit and observable; no undocumented unbounded queue. |
| TOPIC-04 | D | Specify duplicate and ordering handling across reconnect. | Application policy sees enough sequence/provenance information to enforce its selected semantics. |
| TOPIC-05 | X | Bind durable subscription positions to Mnesis when requested. | Restart resumes from a durable checkpoint; a Zenoh memory cache is not mislabeled as a transactional log. |
| TOPIC-06 | M | Provide separate command and topic examples. | Users can see the different acceptance, fan-out and recovery guarantees without learning internal structural effect paths. |
