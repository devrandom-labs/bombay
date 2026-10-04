# Bombay Driver test strategy

This is the executable verification contract for
[`driver-law.md`](driver-law.md). It covers Engine's eight retained laws and
does not claim ownership of Behavior, actor-template, concrete-runtime, or
primitive concurrency semantics.

## Selected contract and ownership

The workspace selects Behavior Core and Actors 0.21.2 from release revision
`edc2d466a50df7cd396f891e3da31fc9e3747bbd` and Behavior Macros 0.13.1
from `5ca96444f0a66e9a013b6989e3e53d345cbabf65`. Evidence must name the
revision of each owner it exercises.

Behavior owns initialization, synchronous folds, complete `Actions`, ordered
interpretation, total settlements, source custody, and child products. Behavior
Actors owns every template's pure topology and lifecycle policy. Engine tests
only the causal protocol by which one closed Behavior and one typed Environment
execute. Bombay tests concrete mailbox, address, timer, observation,
publication, and incarnation composition. Primitive crates retain their own
concurrency models.

The previous strategy required a 68-row cross-product and template campaign
inside the Driver manifest. That representation was ownership-stale: it turned
upstream and downstream obligations into unexecutable Engine status strings.
The template inventory is now recorded in `driver-template-manifest.json`;
[The backlog status index](prd-backlog/status.md) retains the TEST-008 evidence
location; the retired audit chronology remains in Git history. Catalogue support is
tracked separately in [the completion inventory](prd-backlog/evidence.md);
it cannot be used to pass or block an Engine law.

## Actor-template boundary inventory

`driver-template-manifest.json` schema 3 is the revision-bound ownership
boundary for the selected Behavior Actors 0.21.2 package. It records the exact
45 public Behavior compositions by family, public spelling, event boundary,
ordered effect lanes, composition edge, owning source, and upstream evidence.
It separately records all 19 actor-owned interpreter-request/source-action
types, their emitters, and whether Bombay currently has a matching
`InterpretItem` implementation. Behavior-owned delivery, child-input,
creation, and parent-report products are listed as structural lanes rather
than redefined as actor capabilities.

`engine_does_not_mirror_actor_template_laws` compares both inventories with
independent exact sets, rejects duplicate or malformed rows, checks each
template record has its boundary fields, derives interpreter status from the
Bombay source, requires an empty mirror list, and rejects an Engine dependency
or import of Behavior Actors. Removing one template or adding one mirror is a
failing mutation. This remains an Engine ownership gate only: the manifest
points to upstream source/tests and does not copy template transition law into
Engine.

The audit identified six Bombay interpreter boundaries in the locked
atomic surface: `BeginActivation`, `CustomerDelivery`, `DiagnosticAction`,
`InitializeWorker`, `AssignWorker`, and `ProxyOperation`. Each now has a
typed Bombay interpreter and a compile-contract witness under ARC-010; the
manifest records all 19 actor-owned request types as implemented. The
`PrepareWorkers` source action also has a direct typed interpreter. Selected
Actors 0.21.2 supplies typed `ProxyDiagnostic` ingress and split worker
preparation; Bombay's live fixed-supervisor and held-source FIFO regressions
exercise those contracts. This manifest alone is not end-to-end template
proof. `ObserveEstablishedCreation` and `CancelObservation` are
public capability contracts with no current public-template emitter and stay
listed so a later emitter cannot appear outside the inventory.

## Schema-2 manifest

`driver-law-manifest.json` contains exactly one row for every canonical `D-*`
identifier in the law document and no other row. Each row owns three structured
records:

- `positive`: one caller-observable test of the promised causal behavior;
- `boundary`: one distinct edge that constrains the law; and
- `inversion`: one unique edit to the real owning source plus the exact test
  that must kill it.

Every record contains a unique evidence identifier, the selected Behavior
revision, an exact claim, and a pinned-Nix reproduction command. Positive and
boundary records name an integration-test target and exact test. Inversion
records additionally name the edited repository path, the semantic mutation,
and the exact killer. There are no `status`, blanket negative, generic
adversarial, or empty template fields.

The ordinary `law_manifest` suite checks:

- exact law/row identity and order;
- exact dependency versions and Behavior revision;
- uniqueness and completeness of every evidence record;
- resolution of every positive, boundary, inversion, and killer reference;
- agreement between each reference and its pinned-Nix command;
- one real mutation implementation for every inversion identifier; and
- absence of the former ignored status-only completion test.

Source scanning remains useful for the narrow D-SURFACE-1 repository policy and
for detecting stale paths. It is supplemental everywhere else. Appending a
forbidden string to an in-memory copy is not a semantic Driver mutation.

## Executable evidence gate

The required gate is:

```console
nix build path:.#driver-law-evidence --no-link
```

It is also a required `nix flake check` check. The gate copies the current
filtered repository source to a temporary workspace, so neither a successful
mutation nor an interrupted run can alter the caller's worktree. For every law
it then:

1. runs the exact positive test;
2. runs the exact boundary test;
3. applies the named mutation to the real Driver, Environment, or manifest
   source in the temporary workspace;
4. runs only the named killer;
5. rejects compilation failure, an unrelated failure, or a surviving mutant;
6. restores the pristine temporary source; and
7. writes the command outcomes to `driver-law-evidence.json` in the immutable
   Nix output.

The artifact contains one record per canonical law with `passed` positive and
boundary results and a `killed` inversion result. The gate derives work from the
manifest, so adding a law or evidence identifier without an implementation
fails closed. A static `passing` string cannot satisfy it.

Focused reproduction of one row uses its manifest command, for example:

```console
nix develop -c bash crates/bombay-engine/tests/driver-law-evidence.sh --law D-TURN-1
```

The script's inner Cargo invocations are valid only because the script itself
runs inside the pinned Nix shell or check derivation.

## Oracle responsibilities

Deterministic Driver tests assert complete transcripts: initialization,
publication, event acquisition, fold, complete action commitment, source
settlement, terminal selection, retirement, final Behavior, and exact residual.
They use move-only payloads and typed errors where ownership or distinction is
part of the law.

The retained boundary witnesses cover:

- initialization rejection and prepared-environment retirement;
- pending input without spin or self-wake;
- retained and transitive settlement products;
- corrupt or rejected final initialization and turn settlements;
- cancellation at every await boundary;
- prepared/active Environment phase separation and minimum bounds;
- repository closure and forbidden lifecycle surface; and
- manifest row deletion and stale-reference rejection.

Debug and optimized executions remain separate required focused gates. A test
must perform every transition or ownership transfer before its assertions and
must compare complete typed state, effects, errors, custody, order, and terminal
outcome. Mutation failure is accepted only when the named test runs and reports
`FAILED`; a compiler error is an unviable mutation, not a kill.

## Separate queued obligations

The TEST-006 property model now compares typed, complete causal traces over
initialization, activation, ordinary and source turns, settlement outcomes,
creation, and heterogeneous send lanes. The separate fuzz target explores
malformed and long operation streams, pending cancellation, and structural
custody invariants. The pinned `.#fuzz` shell runs its fixed-seed, 2,048-run
campaign through `fuzz/verify-causal-turns.sh`; CI retains the corpus and crash
artifacts. Driver panic and cancellation-stage tests remain separate focused
ownership witnesses.

Redundant Driver-test cleanup, Miri, Loom, coverage, sanitizer, allocation,
and performance findings retain their historical audit records in Git.
Their absence cannot be hidden in this manifest, and their eventual results
cannot silently broaden the eight Engine laws. TEST-008's
separate actor-template inventory is executable, and ARC-010 now supplies all
19 inventoried actor-owned capability interpretations. Broader policy proofs
remain explicit backlog requirements. Concrete Bombay adapter tests continue to
prove capability ordering and lifecycle integration without copying those laws
into Engine.
