# Identity, placement and distributed verification

See [classification marks](README.md). A node may have its own KERI AID.
An actor may have a separate AID, or a stable actor name scoped under a
controller AID; choose this deliberately. Hosting authority relates these
identities rather than making them interchangeable.

Example: actor A moves from node N1 to N2. A, N1 and N2 retain their identities.
The accepted hosting grant changes from an earlier ownership epoch to a newer
one. N1 can remain a legitimate authenticated node while no longer being
authorized to execute A's commands or commit A's durable writes.

No consensus protocol is selected by this inventory. Static placement is a
useful first release slice; automatic safe failover needs an explicit authority
and partition contract. KERI key-event agreement is not silently repurposed as
an actor placement algorithm.

## ID — Mockable identity boundary

Owners: Bombay consumes typed verified facts; the identity implementation owns
verification. First use a deterministic test implementation. Selo integrates
later, without making Bombay depend on a service built on Bombay.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| ID-01 | D | Define node identity separately from runtime incarnation. | Restarting a node can retain its AID while generating a fresh runtime incarnation that fences stale messages. |
| ID-02 | D | Choose actor AID versus controller-AID-scoped actor naming. | Many actors can be hosted without assuming every actor must operate an independent key-event log. |
| ID-03 | D | Define typed verified principal and claimed identity boundaries. | Raw path bytes and decoded claims cannot construct trusted actor input without verification. |
| ID-04 | M | Prototype a statically composed identity capability using existing effect lanes. | Ordinary Rust expresses verification requests/results without a dynamic provider registry or I/O in Behavior. |
| ID-05 | M | Implement deterministic fake identities and authority changes for tests. | Tests create valid, invalid, rotated, revoked and unavailable identities without Selo or live cryptography. |
| ID-06 | M | Distinguish invalid identity from unavailable or stale verification evidence. | A network outage cannot be misreported as proof of forgery or treated as verification success. |
| ID-07 | D | Define freshness and trust-root requirements for verification. | Accepted cached evidence has a documented scope; stale identity state follows explicit policy. |
| ID-08 | M | Bind identity proofs to the intended deployment and purpose. | Credentials or signed commands from one deployment cannot be replayed into another unintended domain. |
| ID-09 | D | Define node session authentication and per-message proof requirements. | Users know which facts secure-channel authentication establishes and which need separate end-to-end proof. |
| ID-10 | M | Bound verification concurrency, cache size and negative caching. | Adversarial identities cannot exhaust CPU/memory or permanently suppress a subsequently valid identity. |
| ID-11 | M | Preserve identity rotation without changing stable actor routes. | A new authorized key works; retired-key use follows the selected freshness and replay policy. |
| ID-12 | M | Make test identity unmistakable in configuration and examples. | A production profile cannot silently select the permissive fake verifier. |

## AUTH — Permissions and delegated hosting authority

Owners: identity implementation verifies proofs; explicit application/cluster
policy decides authorization. An AID authenticates an identity, not every
operation that identity asks to perform.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| AUTH-01 | D | Define permissions for send, query, subscribe, observe, spawn, stop and administer. | A caller authorized for one operation cannot infer access to unrelated lifecycle operations. |
| AUTH-02 | D | Define hosting grants binding actor scope, node AID and ownership epoch. | A legitimate node cannot host an arbitrary actor merely by knowing its key-expression path. |
| AUTH-03 | M | Validate grant scope and delegation chains at admission. | A delegated node cannot broaden protocol, tenant, actor or operation scope. |
| AUTH-04 | D | Define grant expiry, revocation and disconnected operation policy. | The allowed behavior during unavailable revocation evidence is explicit, including any availability tradeoff. |
| AUTH-05 | M | Enforce replay limits for authenticated requests. | Reusing a signed message cannot bypass command idempotency or extend expired authority. |
| AUTH-06 | M | Preserve verified caller identity through proxies and forwarding. | A gateway cannot accidentally transform untrusted caller claims into its own broad authority. |
| AUTH-07 | M | Return typed authorization denials with bounded public detail. | Callers can distinguish actionable denials without receiving private actor or key material. |
| AUTH-08 | M | Verify authorization changes during pending requests. | The selected admission/commit policy specifies whether and where revoked authority is rechecked. |

## PLACE — Actor location and ownership

Prerequisites: WIRE, ID/AUTH, valid local activation and discovery semantics.
Owners: Bombay's selected placement capability and its authoritative store or
coordination service. Zenoh disseminates facts; dissemination alone does not
select a unique winner. Reuse local Entity for local incarnation ownership.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| PLACE-01 | D | Specify placement modes: static ownership first, automatic placement separately. | Each mode states its availability, uniqueness and operator responsibilities. |
| PLACE-02 | M | Implement explicit static actor-to-node assignment. | Two configured nodes reject a request on the wrong host and report a bounded, authenticated routing outcome. |
| PLACE-03 | D | Select the authoritative grant issuer and consistency contract for automatic placement. | Concurrent claim attempts have a defined outcome under partitions; no vague claim that discovery elects an owner. |
| PLACE-04 | M | Implement acquire, renew, supersede and release using that authority. | Every accepted change has a comparable ownership generation; stale renew/release cannot affect a newer owner. |
| PLACE-05 | M | Bind local activation to a valid hosting grant. | A node cannot publish a ready distributed actor before its local activation and ownership prerequisites hold. |
| PLACE-06 | M | Fence admission with the current ownership generation. | A delayed command for a retired owner is rejected or redirected under an explicit policy. |
| PLACE-07 | X | Enforce ownership at durable commit boundaries in Mnesis integration. | A partitioned former owner fails its write even if its local cache still believes it owns the actor. |
| PLACE-08 | D | Define placement eligibility and resource capacity inputs. | Nodes lacking required protocol, durability or resources cannot be selected solely because they are reachable. |
| PLACE-09 | D | Define affinity, anti-affinity and capacity policy scope. | Selected constraints have deterministic conflict outcomes; Kubernetes scheduling is not assumed to satisfy actor-level constraints automatically. |
| PLACE-10 | M | Bound activation storms and concurrent ownership recovery. | Many callers requesting one absent actor do not produce unlimited hydration or competing active writers. |
| PLACE-11 | M | Separate directory cache invalidation from ownership changes. | Stale route hints cannot roll back authoritative placement or prolong a stale grant. |
| PLACE-12 | D | Define allowed replication modes per actor family. | Single writer, read replicas and deliberate multiwriter behavior are distinct commitments, not inferred from multiple subscribers. |

## FAIL — Failure, relocation and recovery

Prerequisites: PLACE and, for durable actors, DUR. A transport failure only
establishes loss of communication. Do not manufacture exact remote termination
facts from heartbeat expiry.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| FAIL-01 | D | Define suspected, unreachable, ownership-expired and definitively retired states. | Each state has its own allowed actions and evidence; a timeout does not erase uncertainty. |
| FAIL-02 | M | Stop or restrict work when a node loses valid ownership. | A former owner cannot continue accepting commands under an expired/superseded grant. |
| FAIL-03 | M | Recover an actor on a replacement host after authority transfer. | New activation uses the correct durable version and generation; old messages cannot mutate it. |
| FAIL-04 | D | Define volatile actor state behavior after host loss. | The product clearly chooses reinitialization, application recovery or unrecoverable loss; it does not promise nonexistent durable state. |
| FAIL-05 | M | Handle commit-before-reply failure during relocation. | A retried durable command returns a consistent recorded outcome rather than executing its business effect twice. |
| FAIL-06 | M | Support graceful host drain with explicit actor disposition. | Each actor is stopped, migrated or left with an explicit uncompleted outcome before the drain deadline. |
| FAIL-07 | D | Specify migration quiescence and cutover. | In-flight commands, observations and durable state have an exact handover boundary. |
| FAIL-08 | M | Reject obsolete handover completions and callbacks. | A late migration result cannot reactivate the prior owner or overwrite a later placement decision. |
| FAIL-09 | D | Define behavior after restoring stale storage or node snapshots. | Restored epochs and credentials cannot bypass current ownership fencing; recovery requirements are documented. |
| FAIL-10 | M | Test repeated partitions, reunions and simultaneous host restarts. | The selected safety invariants hold while liveness is claimed only under the stated recovery assumptions. |

## SIM — Deterministic distributed test host

Owners: Bombay testing boundaries and concrete capability implementations.
This is not a second supported production transport. Fake identity tests
policy integration; they cannot certify KERI cryptography or real Zenoh behavior.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| SIM-01 | M | Model multiple independent runtime hosts with isolated local addresses. | A test cannot accidentally deliver through shared process-local state instead of its network seam. |
| SIM-02 | M | Control network delivery order, delay, loss, duplication and disconnection. | A recorded seed and schedule reproduce the same observable external trace. |
| SIM-03 | M | Control timer and authority-expiry inputs without wall-clock sleeps. | Time advancement exercises the real typed contracts without violating fold purity. |
| SIM-04 | M | Simulate crashes before admission, after commit and before reply. | Outcomes match definitive rejection, committed success and uncertainty at the correct boundaries. |
| SIM-05 | M | Simulate stale routes, stale grants and key rotations independently. | Tests distinguish routing freshness, placement authority and cryptographic identity changes. |
| SIM-06 | M | Persist selected state across simulated process restarts. | Recovery tests do not accidentally retain volatile state or lose committed storage by construction. |
| SIM-07 | M | Use independent observable safety assertions. | The oracle checks ownership, command outcomes and fact conservation without duplicating implementation branches. |
| SIM-08 | M | Shrink and retain failing schedules. | A distributed failure produces a replayable small scenario artifact. |
| SIM-09 | M | Run equivalent contract scenarios over real Zenoh subprocesses. | Encoding, declaration, reconnect and topology behavior are tested beyond the simulator. |
| SIM-10 | M | Add bounded failure campaigns to CI and longer scheduled runs. | Failures retain seed, topology, version and trace; routine CI has a defined resource/time budget. |

## SELO — Downstream production identity integration

Owner: Selo, built on Bombay, Mnesis and mnesis-bombay. Current inspected Selo
source is a naming crate, not an implemented identity/network runtime. Its
existing naming contract must be evolved explicitly rather than silently
reinterpreted as Bombay's final wire protocol.

| ID | Mark | Requirement | Acceptance / adverse witness |
| --- | --- | --- | --- |
| SELO-01 | X | Define Selo's production identity verification contract. | KERI evidence yields the exact verified/invalid/unavailable/freshness facts required by ID. |
| SELO-02 | X | Implement node AID provisioning, custody and rotation policy. | Node restart and rotation preserve intended identity without baking secrets into actor paths or images. |
| SELO-03 | X | Define node-to-actor authorization and delegation semantics. | Signed grants have explicit issuers, scopes and revocation rules consumable by AUTH/PLACE. |
| SELO-04 | X | Reconcile Selo mailbox/query/presence naming with actor protocols and incarnations. | Existing names remain compatible or migrate explicitly; protocol labels are not confused with physical links. |
| SELO-05 | X | Build Selo bootstrap services without a recursive identity dependency. | A cold cluster has an explicit minimum trust/bootstrap path before distributed Selo services are available. |
| SELO-06 | X | Implement the production adapter to Bombay's proven identity boundary. | The same positive and negative contract suite used for the fake runs against real verification. |
| SELO-07 | X | Test KERI-specific rotation, delegation, equivocation and evidence availability. | Cryptographic and trust-policy tests supplement rather than replace Bombay's simulated identity tests. |
| SELO-08 | X | Publish a composed production example. | Users deploy node identities, durable actor hosting and Zenoh connectivity with documented bootstrap and recovery steps. |
