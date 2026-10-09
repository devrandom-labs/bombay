# EXEC post-delivery audit and next dependency frontier

Date: 2026-10-08. Audited checkout:
`de09609e76cdcc6892ccf0e8f7cf3df21834714b`. Scope: assess EXEC's retained
findings, current source correspondence, proof limits and the next programme
dependencies; prepare the [static remote actor PRD](../static-remote-actors.md).
This is a post-delivery audit, not a reopening of accepted EXEC experiments or
a claim to have rerun its entire native/coverage/fuzz campaign.

## Delivery and findings

GitHub's live PR326 record confirms `MERGED` at 2026-10-08T10:13:33Z,
reviewed head `6cf7736d6fdbec8cdb764af1664220b0c87d3b07`, merge
`81256c8e00b580f37f4a1036ac28ec53b1ea948b`. All four required checks
are successful. `git diff` between reviewed and merged trees is empty.
The only changes from that merge to this audit baseline are the three delivery
record documents merged through PR328. EXEC therefore remains `merged`.

Sources: [PR326](https://github.com/devrandom-labs/bombay/pull/326),
[Linux CI](https://github.com/devrandom-labs/bombay/actions/runs/37756875261),
[recorded final delivery](../execution-ownership.md#277-actual-reviewed-delivery).
The fresh exact dependency/source inspection is recorded once in the
[next PRD](../static-remote-actors.md#fresh-selected-contract-verification).

| EXEC gate | Retained finding and current source correspondence | Consequence for next work |
| --- | --- | --- |
| API | `application/execution.rs` exposes paired execution/receiving, async run convenience and configured Builder-only blocking. Complete acquired work and root/family results remain distinct. | Network startup, work and receiving must compose with this owner, including partial startup. |
| Task custody | `launch.rs`, local execution and application execution own actor/capability/cleanup tasks; native failures and incomplete custody are not fabricated completion. Fresh application custody tests pass. | Every new session/declaration/task needs an await/drop custody table and later result consumer. |
| Shutdown authority | `application/interface.rs` separates lifecycle from messaging; local endpoint and Behavior Actors own exact target capability. | Remote send permission must not grant stop, spawn or administration. |
| Observation | The accepted observation record and actor-owned observation module retain independent relationships and exact generation facts. | Network reachability must not be published as exact remote termination; keep untrusted keys outside trusted internal tables. |
| Projection | The accepted two-task representation preserves separate native actor/projector failure ownership. | Task reduction is not a correctness goal; retain projection unless a new measured law requires change. |
| Wrappers | The twenty individual dispositions retain semantic owners and delete forwarding-only layers; whole EXEC still adds capability code. | Do not reopen settled representation comparisons to make the next feature smaller. |
| Modules | Current module map matches application, local capabilities, Driver and Entity ownership. | Assign parallel writers by these owners; shared composition is an integration gate. |
| External work | The retained bounded-work witness does not establish a universal provider/transport attachment API. Local `ApplicationCapabilities` is private. | Prove actual public Application attachment before claiming arbitrary typed effects are pluggable. |

The final ledger records production +13872/-6385/net +7487, tests
+34261/-4703/net +29558, public API +13/-1 and 150 endpoint paths under
the separately approved 277-path cumulative scope. The two final count receipts
still exist locally and their reported totals agree with section277. This audit
does not independently reclassify every historical diff line or reopen its
approved scope. Size alone neither proves a defect nor justifies weaker laws.

## Corrections and outstanding limitations

1. **Missing dependency edges.** The networking inventory requires an identity
   test implementation, but NET1 did not name AUTH1. PLACE1 promises denial of
   a stale durable writer but did not name MNE1. Add both reciprocal edges.
   Static routing belongs to NET1; it does not depend on automatic PLACE1, so
   these additions do not create a cycle. Local MNE1 remains independent of NET1.
2. **Stale current guidance.** The failure-contract table still called invisible
   Address reservation unresolved and LAW-03 implementation blocked, although
   ARC-006/EXEC retain its witnesses. Correct those claims without declaring
   new remote activation/fencing proof. Label the status index's 0.21.2 release
   paragraph and retirement ledger historical; the selected Core/Actors is
   0.23.0. Update the README's execution delivery statement to point to the
   actual merge and repair EXEC's broken observation-scheduling anchor.
3. **Extension eligibility is unproven.** Core has ActionItem/InterpretItem;
   the standard local capability product and Environment are private. Existing
   external actor delivery is a concrete alternative worth testing. Neither a
   public Core trait nor an Engine-only fake proves a new service works through
   ordinary Application. Resolve this once for genuine consumers before NET/MNE
   teams independently build incompatible attachment mechanisms.
4. **Durability migration is still real.** Fresh sibling inspection confirms
   Behavior 0.9.5/Bombay 0.1.0/former Entity versus current 0.23.0/native Entity.
   Preserve existing Once/Replay, conflict and ambiguous-commit semantics.
   Networking does not block that migration; shared Bombay extension edits may.
5. **Evidence portability needs care.** Final count and measurement receipts
   exist in this machine's `/tmp`, but machine-local paths and short reviewer
   IDs are not portable evidence links. The tracked PRD and GitHub checks retain
   delivery evidence; the current CI upload policy uses 14-day retention for
   its uploaded artifacts. Before relying on raw old receipts elsewhere, retain
   immutable source/command/result manifests and durable artifacts. No archive
   migration or new retention guarantee is claimed by this audit.
6. **Known proof limits remain.** EXEC records inherited impure compatibility
   fixtures and Observe assertion-style debt; current affine observation tests
   still poll inside assertions. These are not independent sole evidence for
   a new network law. Preserve historical nonpass Darwin classifier/disk events,
   host-liveness/destructor limits and the lack of a fresh PR-event Miri run.
   None is silently promoted to a whole-repository correctness certificate.
7. **Performance evidence is bounded.** Actual actor-overlap controls pass;
   the cost report explicitly records task/allocation growth and differing
   old/new compiler/dependency epochs. It cannot establish a causal platform-wide
   speedup or predict how much faster parallel development will be.

These findings distinguish verified delivery, fresh source/test evidence,
inherited campaign evidence and future obligations. No new production defect
was demonstrated by this audit; that is not a proof that none exists.

## Why elapsed time grew, and what can run together

The record contains 277 numbered EXEC sections and eight interdependent design
gates, plus upstream publication, broad caller migration, test/diagnostic repairs,
final source reconciliation and resource-related retries. Section277 reports
17 integrated agent contribution groups. EXEC already used parallel work;
adding workers alone does not remove its shared-contract and final-integration
critical path. No reliable time accounting attributes the reported five days
to individual causes, so no numerical speedup estimate is justified.

The next parallel frontier is identity/admission, wire/Zenoh selection,
independent fault-oracle design, and Mnesis migration in its own repository.
Contract research can start together. Production consumers wait for their
exact owning contracts, and shared Application/lock/export edits have one
integrator. A complete early round trip exposes composition errors before a
large caller migration. Final exact-source integration checks remain serial
with respect to accepting a combined revision; independent check jobs may run
concurrently within measured machine limits.

After those foundations, automatic placement/fencing and distributed recovery
become eligible. Kubernetes/operations and production Selo integration follow
their own dependency contracts. This ordering preserves the full programme;
it does not trade correctness for a small patch.

## Verification and change record

Initial working tree was clean. Authorized output is repository documentation:
this audit, the next PRD, links in the execution PRD/backlog and corrections
to the dependency/status/failure guidance. Initial scope was six documentation
paths. The requested specification audit adds one acceptance companion, for
seven paths cumulatively, zero production/test changes and zero public types.
Reuse all existing semantic owners; no manifest, lock, abstraction or runtime
behavior changes. Subsequent approved dependency choices belong in the next PRD.

The specification audit checks every selected requirement against an independent
oracle/falsifier and separates partial inventory contributions, user-approved
scope and pending design. The user selected authorized remote stop, explicit
steering before each new design choice, and existing crates before custom
mechanisms. Subsequent topology, admission, JSON, Zenoh-version and mutual-TLS
decisions are recorded in the next PRD. No representation was silently selected.
Counts include the original documentation work, audit and readiness refinement.

Fresh checks:

- `gh pr view 326 --repo devrandom-labs/bombay --json state,mergedAt,mergeCommit,headRefOid,statusCheckRollup`: merged; all four required checks successful.
- `git diff 6cf7736d6fdbec8cdb764af1664220b0c87d3b07 81256c8e00b580f37f4a1036ac28ec53b1ea948b --stat`: empty.
- `nix develop -c cargo test --locked -p bombay-engine --test law_manifest` and the same command with `--release`: 9 passed in each after correcting the historical derive wording; the unchanged obsolete-guidance guard failed for the original document in both builds.
- `nix develop -c cargo test --locked -p bombay-rs --test completed_application_receiving --test application_terminal_custody`: 38 + 4 passed.
- `nix develop -c cargo test --locked --release -p bombay-rs --test completed_application_receiving --test application_terminal_custody`: 38 + 4 passed.
- Local Markdown validation: all 110 relative file/heading links in the seven changed documents resolve after repairing the stale observation anchor.
- Requirement validation: all 93 selected inventory IDs exist and map exactly to the 25 planned witness groups; no missing/extra ID or duplicate witness label. This checks specification coverage, not execution of those witnesses.
- Dependency validation: all 12 edges across eight programme rows are reciprocal and acyclic.
- `git diff --check`: passed.

No new runtime inversion or completed final-head flake/coverage/mutation/fuzz
or remote CI gate is claimed for this documentation task. Current delivery
checkpoint: complete cumulative document delta against main. All seven files
are now tracked on the requested feature branch in draft PR #329. The branch
is refreshed to main's release-only
`c3afb3011090ac4c9112fd39e4a76980db856df0`; its inherited version/lock updates
are not edits made by this documentation task, and all Rust sources are unchanged.

```text
production: +0 / -0 / net 0
tests: +0 / -0 / net 0
documentation: +2763 / -13 / net 2750
other: +0 / -0 / net 0
public API: +0 types / -0 types
cumulative changed paths: 7 (all tracked)
```

The next PRD records the controlled dependency source's current package-stage
checkpoint and verified trust/generator corrections, nine actual archives,
consumer assertions and feature-gate inversions. Four inherited validation
types, the public `split_once` function and the validator export are retained; no handwritten public
abstraction is added. Those changes are additional to the preserved Bombay
tree. The next PRD owns exact cumulative scope, source, tests/inversions and
remaining delivery gates; local verification is not networking acceptance or a merge.
