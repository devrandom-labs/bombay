# Open design ledger

This ledger contains only the current Bombay dependency graph, selected
contracts, distilled features, and genuine downstream blockers. Historical
research narratives and discarded experiments are not part of the repository.

## Selected contracts

The workspace and every retained independent lock select these exact owners:

| Owner | Selected contract |
|---|---|
| Rust | 1.96.0, edition 2024 |
| Behavior Core | 0.14.0 at `a272adf8d2cbb6a2784d565f47c74adff3e7d01b` |
| Behavior Actors | 0.14.0 at `a272adf8d2cbb6a2784d565f47c74adff3e7d01b` |
| Behavior Macros | 0.11.4 at `a272adf8d2cbb6a2784d565f47c74adff3e7d01b` |
| Address | 0.2.0 |
| Communication | 0.1.2 |
| Timers | 0.1.0 at `13e884da7ab41781f52337b0038060e375b00ee0` |
| Observe | Bombay-private implementation |

The exact Git selections are pinned in `Cargo.lock`, the root patch table, and
the Engine fuzz lock. A sibling checkout or a previously released crate is
evidence only and never overrides the selected build contract.

## Current dependency graph

```text
DX53 atomic Behavior migration (distilled)
  -> DX49 application-native Entity (distilled)
       -> MNE1 durable Mnesis execution (downstream, blocked externally)
  -> DX58 Behavior 0.15 crates.io adoption (blocked upstream)

DX54 Triomphe feature minimization (feature-complete, independent)

DX55 module-scoped test imports (feature-complete, independent)

DX56 Observe retained-key naming (feature-complete, independent)

DX57 Machine output consumer (feature-complete, independent)

DX59 Machine topology identity rendering (feature-complete, independent)

DX60 normative runtime contract alignment (feature-complete, independent)

```

DX58 has no unresolved Bombay prerequisite, but the published Behavior Actors
contract is incomplete as recorded below. MNE1 requires Mnesis-Bombay to select
the current Bombay and Behavior graph and implement its own durable command
execution contract. Bombay deliberately does not classify mailbox admission as
durable completion.

## Ownership map

- Behavior owns the deterministic `Behavior -> Actions` algebra, closed typed
  products, named capability requests, initialization, activation, atomic
  action settlement, source-result custody, and the owning behavior macro.
- Behavior Actors owns reusable actor templates and their topology,
  supervision, shutdown, timing, terminal, pool, and customer-route policies.
- Engine owns only the universal causal `Driver` and affine `Environment`
  protocol.
- Address owns endpoint allocation, claim, resolution, and exact lease
  retirement.
- Communication owns two-lane mailboxes, admission, backpressure, closure, and
  exact rejected-payload recovery.
- Observe owns private retained and affine completion mechanics.
- Timers owns branded generation-safe scheduling state.
- Bombay owns concrete local composition, actor and Entity tasks, capability
  interpretation, application activation and retirement order, external actor
  boundaries, and exact terminal custody.
- Mnesis-Bombay owns aggregate hydration, durable command execution, append and
  conflict policy, and durable outcomes.

## DX53 — atomic Behavior migration

- State: `distilled`.
- Requirement: consume the selected Behavior atomic settlement algebra without
  preserving a second compatibility runtime or recreating owner semantics.
- Result: the Driver returns the final behavior and environment residual;
  Bombay interprets complete action settlements, preserves exact source and
  child custody, and returns role-indexed terminal trees through one retirement
  barrier.
- Application result: `Application::new(root)` is the ordinary single-root
  path. Named child composition uses `.child(Role, actor)`. Advanced concrete
  hosting uses `App` and `ActorSpaces` deliberately rather than through an
  erased or inferred registry.
- Authoring result: `#[bombay::actor]` supplies Bombay's fixed local boilerplate
  and delegates to the Behavior-owned macro. It defines no second actor
  contract. `ActorExt` returns the existing Behavior Actors template types.
- Public-surface result: the migration adds `DriverRetirement`, removes eight
  obsolete topology types and one forwarding function, and removes unsupported
  foundational names from the ordinary prelude.
- Invariants: every accepted, rejected, blocked, corrupt, and unattempted item
  retains exact typed custody; creation precedes dependent delivery; no fold is
  re-entered while commitment is pending; and every terminal edge retires once.
- Inversions: compile fixtures reject missing capabilities, wrong protocols,
  foreign roles, duplicate roles, reinitialization, lifecycle authority through
  actor interfaces, and structural machinery through the ordinary prelude.
- Blocked by: none.
- Unblocks: DX49.

## DX49 — application-native Entity

- State: `distilled`.
- Requirement: install nominal asynchronous Entity definitions in the
  application, preserve truthful origin and typed lifecycle facts, and settle
  every admitted operation and lifecycle task during application shutdown.
- Result: `EntityDefinition` owns hydration and the typed failure, refusal,
  forced-retirement, and final-retirement policies. `Entities<D>` owns the
  family receptionist; `EntityRef<D>` binds one stable domain identity.
- Capacity result: waiter, hydration, and resident bounds remain distinct.
  Hydration finishes before address allocation and routability. Capacity
  refusal, hydration failure, and actor launch failure are separate facts.
- Runtime result: external and actor-originated admissions retain their actual
  caller address. Actor-originated admission crosses Behavior's exact
  `InterpreterRequests` lane. A private receptionist prevents native host proof
  from recurring through actor messages.
- Lifecycle result: passivation fences the exact incarnation; the same stable
  reference can activate a replacement; stale facts cannot retire that
  replacement; forced drain retains the complete `DrainFailure`; and child
  terminal custody reaches the definition policy.
- Shutdown result: Bombay settles the root first, closes every family, drains
  every represented slot, joins every Entity task, and returns typed family
  shutdown summaries plus bounded metrics with zero residents.
- Application result: advanced `App::entity_family`,
  `ApplicationHandle::entities`, `ApplicationHandle::passivate_entity`, and
  `App::run_with_entities` form the one native path. Standard tuples and
  Behavior positions carry the static family product.
- Public-surface result: seven contracts were added—`EntityDefinition`,
  `EntityCapacity`, `EntityActivationError`, `EntityMetrics`,
  `EntityAdmission`, sealed `EntityApplicationFamilies`, and sealed
  `EntityFamilyAt`. The obsolete `EntityFamily` and
  `EntityFamilyInstallation` contracts were removed.
- Change containment: the native stage touched 45 logical files, changed
  production by net `+373` lines against its recorded pre-stage baseline, and
  stayed below its authorized `+400` ceiling with public API fixed at `+7 / -2`
  types.
- Inversions: no `dyn`, `Any`, `TypeId`, boxed future, dynamic host registry,
  callback registry, global allocator, target-as-origin delivery, public
  Observe primitive, polling shutdown, or abort-only normal owner satisfies
  the contract.
- Blocked by: none.
- Unblocks: downstream Mnesis-Bombay adoption.

## DX54 — Triomphe feature minimization

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: Behavior Core 0.14.0, Behavior Actors 0.14.0, and
  Behavior Macros 0.11.4 at
  `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`; Address 0.2.0;
  Communication 0.1.2; Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; and Bombay-private Observe.
- Requirement: retain Triomphe's weak-free `Arc` ownership in Observe without
  selecting unrelated optional integration contracts.
- Ownership: Observe owns completion synchronization and uses only
  `triomphe::Arc`. Triomphe owns that allocation and reference-counting
  primitive. No Bombay production or test consumer uses Triomphe's Serde or
  `StableDeref` integrations.
- Exact blocker and regression: Triomphe 0.1.16 enables `serde` and
  `stable_deref_trait` by default, so the locked normal dependency graph
  contains unused edges. The smallest end-to-end regression is the normal
  `cargo tree` graph: before the change both edges originate at Triomphe;
  afterward neither may do so, and Observe's public behavior must remain
  unchanged.
- Dependency edges: independent of DX53 and DX49. No downstream contract
  depends on either optional integration.
- Blocked by: none.
- Unblocks: none.
- Change ledger: expected tracked files are the root `Cargo.toml`,
  `Cargo.lock`, and this ledger. Expected production source delta is
  `+0 / -0 / net 0`; expected test delta is `+0 / -0 / net 0`; expected public
  API is `+0 / -0` types. The experiment reuses the existing
  `triomphe::Arc`, adds no owner or interpreter, and deletes only unused
  dependency feature edges. Any required source change or verification failure
  falsifies the experiment.
- Result: Triomphe remains at the exact locked 0.1.16 release with only its
  `std` feature. Its unused `serde` and `stable_deref_trait` edges are absent
  from Bombay's normal graph, and `stable_deref_trait` is absent from the
  lockfile. No Rust source or public contract changed.
- Verification: the dependency-tree regression, 122 focused Observe tests,
  the complete locked workspace test suite, formatting, and strict all-target
  workspace Clippy pass through the pinned Nix shell.
- Actual checkpoint: tracked files `3`; production source
  `+0 / -0 / net 0`; tests `+0 / -0 / net 0`; public API
  `+0 / -0` types; manifests `+1 / -11 / net -10`; documentation
  `+45 / -0 / net +45`.

## DX55 — module-scoped test imports

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the complete owner graph recorded above, including the
  exact locked Behavior and Timers revisions, was rechecked before this
  repository-wide structural audit.
- Requirement: imports belong to their owning module scope. Test functions and
  generated bodies receive no exception.
- Ownership: each affected test module owns the names needed by its tests; no
  runtime owner, Behavior lane, or production interface changes.
- Exact blocker and regression: an AST query finds 12 functions containing 19
  block-local `use` declarations across Machine executor tests, Observe tests,
  and Entity error tests. The same query must find zero after the change while
  the complete affected behavior remains unchanged.
- Dependency edges: independent of DX53, DX49, and DX54.
- Blocked by: none.
- Unblocks: none.
- Change ledger: expected tracked files are this ledger and six test-bearing
  Rust files. Expected production source delta is `+0 / -0 / net 0`; test code
  is expected to be net-negative by moving and merging imports; expected public
  API is `+0 / -0` types. Existing modules and names are reused; no wrapper,
  trait, owner, interpreter, or dependency is added. Any cfg-scope change,
  ambiguity, production/API edit, or verification failure falsifies the
  experiment.
- Result: all 19 imports moved into their six owning test modules and repeated
  declarations were merged. The repository-wide AST query now finds zero
  function-local imports in crates or examples.
- Verification: affected Machine, Observe, and Entity error tests, the complete
  locked workspace suite, formatting, and strict all-target workspace Clippy
  pass through the pinned Nix shell.
- Actual checkpoint: tracked files `7`; production source
  `+0 / -0 / net 0`; tests `+14 / -41 / net -27`; public API
  `+0 / -0` types; documentation `+37 / -0 / net +37`; complete tracked delta
  `+51 / -41 / net +10`.

## DX56 — Observe retained-key naming

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph remains unchanged.
- Requirement: private names must state the responsibility they own and must
  not claim false provenance. Observe's measured retained-key hashing policy is
  distinct from current rustc-hash and belongs only to its non-adversarial
  internal key table.
- Ownership: Observe owns its retained-key table and the measured hashing
  policy selected for that table; no public caller selects or observes the
  implementation.
- Governing invariant and regression: only private identifiers, formatting,
  and adjacent prose may change. The arithmetic operations and multiplier
  value remain identical; distribution, dependencies, behavior, and
  performance may not change. Existing promotion, model, allocation, and Loom
  tests remain the caller-visible regression.
- Dependency comparison: a separate `rustc-hash` 2.1.3 experiment passed all
  semantic gates but regressed five-run 8- and 16-thread medians by roughly
  9.5% and 9.9%, so it was reverted rather than retained as a nominal cleanup.
- Dependency edges: independent of DX53, DX49, DX54, and DX55.
- Blocked by: none.
- Unblocks: none.
- Change ledger: expected tracked files are `observe/mod.rs` and this ledger.
  Expected production source is approximately line-neutral; tests remain
  unchanged; expected public API is `+0 / -0` types. No wrapper, dependency,
  owner, branch, or algorithm may be added.
- Result: `RetainedKeyHasher`, `RETAINED_KEY_HASH_MULTIPLIER`, and
  `BuildRetainedKeyHasher` now name the private responsibility and the
  constant's actual role. The obsolete Fx/rustc-hash provenance claim is gone;
  arithmetic operations and the multiplier value are unchanged.
- Verification: both normal Observe suites (122 and 121 tests), formatting,
  and strict all-target workspace Clippy pass through pinned Nix. The preceding
  replacement comparison also passed all 28 isolated Loom models before the
  exact algorithm was restored.
- Actual checkpoint: tracked files `2`; production source
  `+16 / -13 / net +3`; tests `+0 / -0 / net 0`; public API
  `+0 / -0` types; documentation `+41 / -0 / net +41`; complete tracked delta
  `+57 / -13 / net +44`.

## DX57 — Machine output consumer

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph remains unchanged. Machine
  is actor-independent and this change does not enter the Behavior fold or
  capability stack.
- Requirement: serialized and linearized execution need one synchronous
  `Fn(Output)` consumer, not a Bombay trait that merely forwards to `Fn`.
- Ownership: the standard `Fn` contract owns invocation syntax; each caller
  owns its concrete output consumption. Machine owns only ordering, poisoning,
  receipts, and dispatch custody.
- Exact blocker and regression: public `OutputHandler` adds a nominal extension
  port with no production implementation other than its blanket closure
  adapter. All real repository consumers are closures; two private test-only
  implementations model reentrancy. A compile-pass fixture preserves closure
  syntax, while a compile-fail fixture must reject the obsolete trait port.
  Runtime reentrancy, panic, ordering, and exact-drop tests remain independent
  behavior regressions.
- Dependency edges: independent of DX53, DX49, DX54, DX55, and DX56.
- Blocked by: none.
- Unblocks: none.
- Change ledger: expected tracked files are Machine's manifest and lock entry,
  `executor.rs`, four compile-fixture files, and this ledger (`8` files).
  Expected production source is net-negative; tests are net-positive for the
  public syntax fixtures; expected public API is `+0 / -1` types. Existing
  closures, executors, receipts, and output evidence are reused. No replacement
  trait, wrapper, callback registry, owner, or policy may be added. Any caller
  syntax regression, weaker static denial, runtime failure, or performance
  regression falsifies the experiment.
- Result: `OutputHandler` and its blanket implementation are gone. Serialized
  submission and linearized dispatch now accept the exact borrowed
  `Fn(Output)` contract already used by every production caller. Reentrancy and
  poisoning probes retain exact owned-output consumption through closures; no
  replacement trait, wrapper, bound, state, or branch was added.
- Verification: the closure compile-pass fixture and obsolete-port compile-fail
  fixture pass; all 17 normal Machine tests and both optimized Loom models pass;
  the complete locked workspace suite, 502-test Nextest suite, formatting, and
  strict all-target Clippy pass through pinned Nix. Against the retained parent
  revision, serialized output consumption measured 111.300 ns versus 109.817 ns
  per turn and linearized dispatch measured 25.053 ns versus 25.051 ns; the
  removed forwarding trait has no measured performance value.
- Aggregate-drift checkpoint: executor control sums remain
  `Idle | Running | Poisoned` and the existing dispatch/turn outcomes; no
  subordinate state or result alternative changed. Production control-flow
  branches remain `26`, modules remain `1`, and public executor spellings fall
  from `11` to `10`. The production portion of `executor.rs` falls from 532 to
  508 lines (`+18 / -42 / net -24`). Every surviving state still owns the same
  current execution, queue, poison, receipt, or dispatch value. The residue
  scan found no arrival history, repeated cause, false cardinality, nested
  transition authority, semantic boolean, or structural caller syntax added.
  Cross-check against Driver law, Machine runtime tests, and the
  actor-independent ownership boundary: `pass`.
- Actual checkpoint: tracked files `8`; production source
  `+18 / -42 / net -24`; tests `+119 / -26 / net +93`; public API
  `+0 / -1` types; manifests and lockfile `+2 / -0 / net +2`;
  documentation `+60 / -0 / net +60`; complete tracked delta
  `+199 / -68 / net +131`.

## DX58 — Behavior 0.15 crates.io adoption

- State: `blocked`; the immutable upstream releases are published, but the
  caller-side contract audit found an upstream ownership gap that Bombay cannot
  repair.
- Selected release candidates: `bombay-behavior` 0.15.0,
  `bombay-behavior-actors` 0.15.0, and `bombay-behavior-macros` 0.11.5. All
  three crates identify source commit
  `ba0dcb5549dcb4e79ddc32b827905f1a4244d414`; their release tags resolve to
  that commit. Its complete `AGENTS.md` has blob
  `4996c149c5762d8057e6d207517459284b3cb9ed` and is byte-identical to the
  audited pre-release owner contract.
- Neighbor verification: Address 0.2.0, Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe were
  rechecked at their selected source. Their ownership and protocols do not
  change in this feature; only their Bombay interpreters may adapt to the
  owner's total-settlement interface.
- Requirement: replace the mutable Behavior Git patch with the immutable
  crates.io releases and consume the exact published algebra directly. Bombay
  must not retain the superseded two-leg commit API, re-create catalogue
  ownership in core, or add a compatibility runtime.
- Ownership: Behavior Core owns `ActionItem`, `ItemSettlement`,
  `SettledItem`, `Interpretation`, source-result custody, creation ordering,
  and total `Actions::interpret`; Behavior Actors owns catalogue protocols,
  templates, and the unified `TimedEvent`/`TimedReaction`; Bombay owns only
  concrete local interpretation, application activation and retirement, and
  exact terminal custody. Engine remains actor-independent.
- Governing invariants: creation preparation and settlement precede dependent
  sends; every accepted, rejected, blocked, corrupt, and unattempted action
  retains its exact typed source item; closed control retains the exact source
  action; initialization and activation settle through the same contract; the
  final Behavior and affine Environment residual remain recoverable at every
  terminal edge.
- Exact regression: an isolated worktree changed only dependency selection to
  the published releases and ran `nix develop -c cargo check --workspace`.
  The prior representation failed with 31 compile errors in two coherent
  groups: catalogue names still imported from Core, and Bombay's superseded
  `SendInterpreter`/`CreationResults`/two-leg commit machinery. Those failures
  locate the obsolete representation but do not determine the replacement
  architecture. Existing complete settlement, source-custody, closed-control,
  creation-order, initialization, activation, and Driver-retirement tests are
  the caller-visible behavioral regressions.
- Upstream blocker: `bombay-behavior-actors` 0.15.0 declares
  `ReportTerminalOutcome`, `ObserveEstablished`, `CancelObservation`, and
  `ObserveEstablishedCreation` as `InterpreterRequest` values but supplies no
  `ActionItem` implementation for any of them. `InterpreterRequests<Request>`
  implements `SendSettlements` and `InterpretSends` only when
  `Request: ActionItem`, so ordinary termination propagation and exact
  observation cannot participate in the published total-settlement algebra.
  The isolated caller probe at
  `.research/probes/behavior-0.15-action-items` fails on all four bounds under
  the pinned Nix shell. Both the trait and request types are upstream-owned, so
  Rust's coherence rules prohibit a Bombay implementation; a wrapper would
  duplicate the owning request contract and is rejected by the falsification
  rule.
- Dependency edges: depends on the distilled DX53 ownership model and the
  published upstream contracts; independent of DX54 through DX57.
- Blocked by: a published Behavior Actors revision that gives every
  `InterpreterRequest` used by its catalogue an owner-defined `ActionItem`
  settlement contract.
- Unblocks: immutable downstream graph alignment, including the version
  prerequisite of MNE1.
- Change ledger: expected tracked files are the root manifest and lockfile,
  Engine's fuzz manifest and lockfile, Bombay's manifest, `lib.rs`,
  `actor_interface.rs`, `actors/actor_ext.rs`, `interpret.rs`,
  `application_runtime.rs`, `child_bindings.rs`, `local.rs`, `launch.rs`,
  `reports.rs`, `observation.rs`, `time.rs`, `termination.rs`,
  `entity/bombay.rs`, `entity/family.rs`, affected caller-visible regression
  fixtures, and this ledger. Up to 24 tracked files are expected. Production
  must remain at or below net `+500` lines and public API at or below `+3`
  types; crossing either limit requires another explicit checkpoint. Existing
  Behavior settlements, actor catalogue products, Driver, Environment,
  capability owners, actor tasks, and terminal trees must be reused; obsolete
  Bombay interpreter and raw creation-result machinery must be deleted rather
  than wrapped.
- Authorized scope: adopting the coherent Orders 1–6 custody contract
  necessarily crosses the 15-file threshold. After that checkpoint was
  reported, the user explicitly supplied the published versions and directed
  Bombay to use the crates.io versions directly. That authorizes the expanded
  file count for this release migration only; it does not authorize a broader
  redesign or relax the production-line and public-type limits.
- Falsification: reject the migration if it needs a second effect algebra,
  catalogue copy, erased action or result, dynamic capability map, hidden
  callback, inferred provenance, compatibility layer, or weaker terminal
  custody. Role-first application assembly remains separate from this release
  adoption.

## DX59 — Machine topology identity rendering

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the workspace remains on Behavior Core and Actors
  0.14.0 and Macros 0.11.4 at
  `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`, Address 0.2.0,
  Communication 0.1.2, Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`, and Bombay-private Observe.
  Their source and public contracts were rechecked; none owns or consumes
  Machine's renderer. `bombay-machine` owns `Topology`, `VertexId`, display
  labels, validation, and Mermaid rendering. Bombay lifecycle metadata is the
  only downstream renderer regression.
- Exact blocker and regression: `Topology::write_mermaid` discards each
  `VertexId` and uses its human-readable label as Mermaid identity. Two
  distinct vertices with the same label therefore collapse into one rendered
  state even though execution and validation distinguish them. The smallest
  failing regression declares two reachable vertices with one shared display
  label and requires two state declarations plus start and transition edges
  that reference their distinct generated IDs.
- Governing invariant: `VertexId` remains the identity in every structural
  interpreter. A Mermaid state description presents `Vertex::label` but never
  substitutes for identity. Every declared vertex is emitted exactly once,
  and start and transition edges refer only to the generated ID derived from
  `VertexId`. Existing unknown-reference formatting failure remains unchanged.
- Syntax evidence: Mermaid's official state-diagram grammar explicitly
  separates a state ID from its description and requires later transitions to
  reference the ID. The renderer will use that ordinary grammar directly; no
  escaping abstraction, syntax framework, allocation, or dependency is
  justified by this identity defect.
- Dependency edges: independent of DX53 through DX58; no downstream feature is
  blocked on this correction.
- Blocked by: none.
- Unblocks: truthful diagrams for any structural consumer whose distinct
  vertices intentionally share presentation text.
- Change ledger: expected tracked files are this ledger,
  `crates/bombay-machine/src/machine.rs`, and
  `crates/bombay/src/entity/lifecycle/machine.rs` (`3` files). Expected
  production source is no more than `+14 / -12 / net +2`; tests are no more
  than `+34 / -12 / net +22`; public API is `+0 / -0` types. Existing
  `Topology`, `VertexId`, `Vertex`, and `Transition` remain the sole owners;
  the obsolete label-to-identity lookup is deleted. Any change to validation,
  execution, topology construction, or public Rust syntax falsifies the stage.
- Result: every declared vertex now has one Mermaid declaration whose stable
  `vertex_<id>` identity is derived from `VertexId` and whose description is
  the existing human label. Start and transition edges use only those stable
  identities. Unknown references retain the same checked lookup and formatting
  error; no topology, validation, execution, allocation, or public Rust
  contract changed.
- Verification: the new duplicate-label regression failed against the prior
  renderer by producing `ready --> ready`, then passed in debug and optimized
  builds after the correction. Bombay's exact lifecycle rendering regression,
  all 18 Machine tests and compile fixtures, the complete locked workspace
  suite, 503-test Nextest suite, formatting, and strict all-target workspace
  Clippy pass through pinned Nix.
- Actual checkpoint: tracked files `3`; production source
  `+10 / -10 / net 0`; tests `+34 / -2 / net +32`; public API
  `+0 / -0` types; documentation `+62 / -1 / net +61`; complete tracked delta
  `+106 / -13 / net +93`.

## DX60 — normative runtime contract alignment

- State: `feature-complete`; final fixed-point audit remains pending.
- Selected contracts: the exact owner graph is unchanged from the table above.
  In particular, Timers is selected from Git revision
  `13e884da7ab41781f52337b0038060e375b00ee0`; the obsolete registry checksum
  and checkout `4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2` are historical provenance only.
- Exact blocker and regression: `runtime-capability-interfaces.md` calls itself
  the current normative runtime contract but identifies the obsolete Timers
  artifact as selected and twice describes exact-capability work as pending
  DX37 slices. `Cargo.lock`, the selected-contract table, current source, and
  caller documentation instead show the Git Timers revision, exact established
  delivery/observation, and deliberate logical `ActorSpace` hosting. The
  smallest regression is a repository scan that finds those three stale
  current-contract claims in the normative document.
- Governing invariant: a normative document names the exact selected artifact
  and describes current ownership without presenting historical probes or
  retired ledger IDs as live architecture. Historical provenance in the
  explicitly historical Driver test derivation remains unchanged.
- Dependency edges: independent of DX53 through DX59.
- Blocked by: none.
- Unblocks: reliable version and ownership review for the next fixed-point
  audit and eventual Behavior release adoption.
- Change ledger: expected tracked files are this ledger and
  `docs/runtime-capability-interfaces.md` (`2` files). Production and tests are
  `+0 / -0`; public API is `+0 / -0` types. No code, Cargo selection, owner,
  interface, or future feature is changed. Any statement not directly proved
  by the selected graph and existing implementation falsifies the repair.
- Result: the normative contract now names the exact locked Timers Git
  revision, describes actor spaces as the deliberate host for logical
  recipients and advanced multi-protocol composition, and describes exact
  established delivery and observation as current behavior. The retired DX37
  roadmap wording and obsolete current-version claim are gone; the explicitly
  historical Driver-strategy provenance remains intact.
- Verification: the pre-edit oracle found all three stale claims. The same
  document now contains none of the obsolete revision or DX37 references, and
  its replacement revision matches `Cargo.toml`, `Cargo.lock`, and every
  current selected-contract table. Complete diff and whitespace checks pass.
- Actual checkpoint: tracked files `2`; production and tests
  `+0 / -0`; public API `+0 / -0` types; documentation
  `+56 / -12 / net +44`.

## Public examples

Seven public packages exercise caller-visible behavior:

- `counter`: state, request/reply, domain error, and lifecycle shutdown;
- `actor-templates`: timeout and shutdown composition through `ActorExt`;
- `application-topology`: heterogeneous children, same-action delivery, and
  ordered shutdown;
- `supervision`: failure, replacement, backoff, and budget exhaustion;
- `worker-pool`: admission, assignment, completion, interruption, restart, and
  shutdown;
- `axum`: HTTP translation through a truthful external actor boundary;
- `entity`: hydration, stable identity, passivation, reactivation, exact
  retirement, and joined family shutdown.

Most examples use `Application`. Entity deliberately uses advanced `App`
because its definition statically names the concrete logical protocols it
hosts. Hiding that product would require erasure or dishonest automatic host
materialization.

## Verification baseline

The final tree passed the following gates through pinned Nix on 2026-09-15:

```text
nix develop -c cargo fmt --all -- --check
nix develop -c cargo build --locked --workspace
nix develop -c cargo test --locked --workspace
nix develop -c cargo nextest run --workspace
nix develop -c cargo clippy --locked --workspace --all-targets -- -D warnings
nix develop -c env RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps
nix develop -c cargo test --locked --release -p bombay-rs --test entity_application
nix develop -c cargo run --locked -p bombay-example-entity
nix develop -c cargo run --locked --release -p bombay-example-entity
nix flake check path:.
git diff --check
```

Nextest passed 503 tests across 49 binaries. All 17 declared flake checks
passed. The four private-Observe fuzz targets each passed 10,000 bounded runs
through the pinned fuzz shell. Entity, Machine, and Observe concurrency laws
also pass their optimized Loom gates.

## Downstream boundary

### MNE1 — durable Mnesis execution

- State: `blocked` by an external release and dependency-graph alignment, not
  by a missing Bombay runtime contract.
- Requirement: Mnesis-Bombay must select current Bombay and the same exact
  Behavior generation, hydrate aggregate state before Entity routability,
  execute its existing durable handler contract, and preserve admission,
  decision, append/conflict, uncertain commit, and durable completion as
  distinct typed facts.
- Bombay support: native Entity installation, stable identity, truthful
  provenance, passivation, retirement, capacity, family shutdown, and task
  joining are available.
- Rejected shortcut: an admitted mailbox command is not evidence of a durable
  commit. Bombay will not add a second persistence abstraction, erased command
  registry, or compatibility actor to conceal version skew.
- Unblocks: a future Mnesis-Bombay release using the current Bombay contract.
