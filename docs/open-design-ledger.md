# Open design ledger

This ledger contains only the current Bombay dependency graph, selected
contracts, distilled features, and genuine downstream blockers. Historical
research narratives and discarded experiments are not part of the repository.

## Selected contracts

The workspace and every retained independent lock select these exact owners:

| Owner | Selected contract |
|---|---|
| Rust | 1.96.0, edition 2024 |
| Behavior Core | 0.16.0 at `b9642e84e5719c4e2018f752822a392d3c23e164` |
| Behavior Actors | 0.16.0 at `b9642e84e5719c4e2018f752822a392d3c23e164` |
| Behavior Macros | 0.11.6 at `b9642e84e5719c4e2018f752822a392d3c23e164` |
| Address | 0.2.0 |
| Communication | 0.1.2 |
| Timers | 0.1.0 at `13e884da7ab41781f52337b0038060e375b00ee0` |
| Observe | Bombay-private implementation |

The exact immutable registry selections and checksums are pinned in
`Cargo.lock` and the Engine fuzz lock. The root patch table selects only the
unreleased Timers correction. A sibling checkout or a previously released
crate is evidence only and never overrides the selected build contract.

## Current dependency graph

```text
DX53 atomic Behavior migration (distilled)
  -> DX49 application-native Entity (distilled)
       -> MNE1 durable Mnesis execution (downstream, blocked externally)
  -> BEH1 creation-settlement disposition (upstream, blocked externally)
       -> DX58 Behavior 0.16 crates.io adoption (blocked)

DX54 Triomphe feature minimization (feature-complete, independent)

DX55 module-scoped test imports (feature-complete, independent)

DX56 Observe retained-key naming (feature-complete, independent)

DX57 Machine output consumer (feature-complete, independent)

DX59 Machine topology identity rendering (feature-complete, independent)

DX60 normative runtime contract alignment (feature-complete, independent)

DX61 Machine test transition policy (feature-complete, independent)

DX62 Observe sequential model state (feature-complete, independent)

DX63 Observe exhaustive model state (feature-complete, independent)

DX64 Driver allocation fixture input ownership (feature-complete, independent)

DX65 Driver panic injection policy (feature-complete, independent)

DX66 Driver custody failure ownership (feature-complete, independent)

DX67 Entity Loom admission algebra (feature-complete, independent)

DX68 Entity Loom activation claim (feature-complete, independent)

DX69 Entity hash-gate phase (feature-complete, independent)

DX70 Observe fuzz state ownership (feature-complete, independent)

```

DX58 is blocked by BEH1. Behavior 0.16.0 supplies the exact behavior-settlement
projection and one-at-a-time source custody required by Bombay's lifecycle
host, but its `Births<C>` custody contract requires a creation-result ingress
that ordinary generated birth declarations cannot express. MNE1 requires
Mnesis-Bombay to select the current Bombay and Behavior graph and implement its
own durable command execution contract. Bombay deliberately does not classify
mailbox admission as durable completion.

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

## BEH1 — creation-settlement disposition

- State: `blocked`; the selected immutable Behavior 0.16.0 contracts do not
  provide an ordinary generated actor a way to satisfy creation-result source
  custody.
- Exact blocker: `SourceSettlementCustody` for every `Births<C>` settlement
  requires the root event to implement
  `EventIngress<Births<C>, CreationsSettled<A, C>>`. A
  `#[behavior(..., births = { ... })]` declaration generates the closed birth
  product but does not generate that event lane, request an owning receiver,
  or expose a typed disposition that proves the result is intentionally
  retained without return. Bombay's `#[actor]` delegates to that exact macro
  and therefore cannot supply the missing owner contract.
- Regression: the caller-visible `application_terminal_custody` fixture has a
  generated actor that creates one declared child and stops. Under 0.16.0 its
  application fails to compile because its generated
  `EventLayer<ShutdownRequested, User<MailAddr, Never>>` cannot admit the
  required `CreationsSettled` input. A second inversion with application-owned
  children fails on the same law for the appended closed birth product.
- Required owner decision: Behavior must expose one explicit typed policy for
  creation settlement—either generate/name the matching ingress and receiver,
  or represent an intentional no-return disposition in the birth algebra.
  Bombay must not silently discard the authoritative settlement or invent a
  parallel event/effect contract.
- Neighbor finding: `BehaviorSettlements::Settlements` also has no declared
  equality or classification contract tying it to `Actions<B>` in a generic
  interpreter. Bombay can localize the concrete equality at its single
  `ActionInterpreter` seam, so this is friction rather than a separate blocker.
- Blocked by: a newer immutable Behavior Core contract and regression.
- Unblocks: DX58.

## BEH2 — pool-worker facade resolution

- State: `blocked`; `bombay-behavior-macros` 0.11.6 resolves its Actors path
  through a `bombay-rs`-only dependency as `bombay::behavior`, then expands
  `#[pool_worker]` with `bombay::behavior::atomic::Completion`. Foundational
  Behavior deliberately has no `atomic` module; Bombay correctly exposes the
  owning catalogue as `bombay::atomic`.
- Regression: the worker-pool example imports the re-exported
  `bombay::atomic::pool_worker` with no direct Actors dependency and fails at
  the attribute expansion because `atomic` cannot be found in Behavior. The
  same source compiles when the owner crate is an explicit dependency and the
  macro resolves `behavior_actors::atomic`.
- Required owner correction: Macros must make `actors_crate()` resolve the
  Bombay facade's Actors path (`bombay::atomic` for this expansion) rather than
  reuse its foundational Behavior path. Bombay must not introduce a second
  procedural macro or fake an `atomic` module under Core.
- Current downstream disposition: the public worker-pool example declares the
  exact Actors owner dependency for `pool_worker`; all other runtime and
  foundational imports continue through Bombay. This is an explicit owner
  import, not a compatibility layer.
- Blocked by: a newer immutable Behavior Macros contract and regression.
- Unblocks: facade-only use of the owner `pool_worker` macro.

## DX58 — Behavior 0.16 crates.io adoption

- State: `blocked`; upstream 0.16.0 resolves the recorded terminal-custody,
  source-order, and observation-settlement prerequisites, but BEH1 prevents
  every birth-owning generated actor from satisfying the new custody law.
  Independent downstream migration and regressions remain active.
- Target releases: `bombay-behavior` 0.16.0 and
  `bombay-behavior-actors` 0.16.0 resolve to source commit
  `b9642e84e5719c4e2018f752822a392d3c23e164`; the
  `bombay-behavior-macros` 0.11.6 tag resolves to the same commit. The release
  tags and Cargo registry search were independently resolved before dependency
  selection. The target revision's full
  760-line `AGENTS.md` has blob
  `4996c149dc7600c75972e2c041e1cee4a79352a0` and was re-read before this
  migration.
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
- Resolved upstream blocker: `bombay-behavior-actors` 0.15.0 declares
  `ReportTerminalOutcome`, `ObserveEstablished`, `CancelObservation`, and
  `ObserveEstablishedCreation` as `InterpreterRequest` values but supplies no
  `ActionItem` implementation for any of them. `InterpreterRequests<Request>`
  implements `SendSettlements` and `InterpretSends` only when
  `Request: ActionItem`, so ordinary termination propagation and exact
  observation cannot participate in the published total-settlement algebra.
  The isolated caller probe at
  `.research/probes/behavior-0.15-action-items` fails on all four bounds under
  0.15.0. Both the trait and request types are upstream-owned, so Rust's
  coherence rules prohibit a Bombay implementation. Actors 0.15.1 now owns the
  four exact `ActionItem` contracts and its
  `interpreter_request_settlement` regression proves accepted, unattempted, and
  creation-prerequisite settlement. The unchanged caller probe is selected for
  0.15.1 as the downstream eligibility gate.
- Falsified blocker candidate: a stopping initialization is not an
  unrepresented pre-publication child result. The Driver records the stop
  verdict, but the local Environment commits initialization and publishes the
  endpoint synchronously before the Driver retires the active incarnation.
  `spawn_owned_with` therefore returns the established capability, after which
  ordinary termination custody applies. The remaining pre-publication
  `Panicked`/`Cancelled` cases and interpreter corruption are outside ordinary
  returned settlement under Bombay's existing unwind/cancellation law; no
  Behavior change is required for this candidate.
- Resolved terminal-contract blocker: Core 0.16.0 adds the blanket
  `BehaviorSettlements` projection. A lifecycle owner can retain the exact
  settlement as `<B as BehaviorSettlements>::Settlements` without repeating
  `SendSettlements` or creation-product bounds through actor tasks, Entity
  leases, terminal projections, or application types. The value remains
  concrete; Bombay must neither erase it nor reconstruct its product shape.
- Resolved source-order blocker: Core 0.16.0 replaces bulk source admission
  with `SourceSettlementCustody::offer_next_to_source`. `SourceCustody`
  distinguishes `Exhausted`, exactly one `Admitted` input, and `Closed` while
  retaining the exact residual. Creation precedes sends, generated named
  products and `SendLayer` preserve their declared order, and each admitted
  result returns control to the Driver so its complete transitive chain can
  become quiescent before the next older result is offered.
- Resolved Actors inconsistency: Actors 0.16.0 adds direct `settle` operations
  for `ObserveEstablished` and `CancelObservation`, matching
  `ShutdownEstablished`. Bombay's one-line `InterpretItem` implementations are
  required static-dispatch leaves and now delegate the settlement law to each
  request owner instead of repeating `ItemSettlement::Accepted(())`.
- Dependency edges: depends on the distilled DX53 ownership model, BEH1, and
  the published upstream contracts; independent of DX54 through DX57.
- Blocked by: BEH1.
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
- 0.16 checkpoint: both retained locks now select the immutable registry
  releases and checksums, and the independent action-item and terminal-custody
  probes pass in debug and optimized builds. The Engine fuzz target also
  compiles against Core 0.16.0 after adopting the owner-provided `Creations`
  product. The workspace check now fails in Bombay's obsolete commit wrapper:
  `CreationCustody` and bulk source offering no longer exist, while the current
  Engine `apply -> Result<(), E>` boundary cannot return a complete settlement
  to the Driver. Adding bounds at the 38 diagnostics would merely leak product
  structure through Entity and application APIs; the 0.16
  `BehaviorSettlements` projection is the selected correction.
- Scope checkpoint: the complete working tree currently spans 22 tracked and
  untracked files. Production is `+859 / -658 / net +201`; tests are
  `+124 / -2 / net +122`;
  manifests and locks are `+24 / -32 / net -8`; documentation is
  `+63 / -32 / net +31`; no new public type has been added. The smallest
  coherent Driver settlement stage must additionally revise Engine's
  environment and Driver ports, their shared law fixtures, allocation,
  property, terminal-custody, compile-pass, benchmark, and fuzz witnesses,
  Bombay's terminal projection, and the three current Driver contract
  documents. That end-to-end surface is expected to reach at most 40 tracked
  files while retaining the existing net-production `+500` and new-public-type
  `+3` ceilings. Crossing the previously authorized 24-file limit requires a
  new explicit authorization before those production edits begin.
- Expanded scope authorized: the user explicitly authorized the requested
  DX58 expansion to at most 40 tracked files. The cumulative migration retains
  the existing net-production `+500` and new-public-type `+3` ceilings. This
  authorization covers only the coherent Engine settlement, Bombay custody,
  regression, and contract-document surface described above; it does not
  authorize unrelated redesign.
- Selected Driver model: `CommitActions` performs exactly one total
  `Actions::interpret` and returns its `Interpretation<B::Settlements>`; it
  owns no source loop and no public error vocabulary. The active Environment
  supplies three concrete ports only: offer at most one settlement result to
  its source, obtain the next admitted source-control event without opening
  ordinary ingress, and publish the installed endpoint after initialization
  custody is resolved. Its retirement value receives every still-owned
  settlement. The actor-independent Driver owns a `VecDeque` of private
  `Offer | AwaitSource` turns. New transitive settlements enter at the front;
  the older residual remains behind them. `Exhausted` discharges a product
  only after Core proves that it contains no further source input, `Admitted`
  changes the front turn to `AwaitSource`, and `Closed` transfers the current
  input and untouched suffix into retirement. No recursion, callback, second
  mailbox, erased value, or interpreter-specific traversal is introduced.
- Initialization law: after address installation, accepted initialization
  effects and every admitted transitive source-result turn become quiescent
  before publication and ordinary ingress. Pure initialization rejection
  remains the only pre-installation Behavior error. A post-installation
  rejected or corrupt settlement, source closure, or stop transfers the exact
  pending deque, publishes the installed endpoint so the fact cannot be
  misclassified as `CreationRejection`, and then crosses the normal retirement
  barrier. A stopping actor never admits its final settlements back into
  itself. Active expected rejection remains settlement data and is returned to
  its declared source; only corruption or closed source custody is terminal.
- Required regressions: observe initialization settlement before publication;
  prove `new transitive -> older residual -> ordinary input` ordering; close
  source admission with two untouched results and recover both; inject a
  corrupt interpretation and recover the entire product; stop with a source
  result and prove no self-reopening; replay each terminal case in optimized
  mode; and drive a long generated source chain to prove the loop is iterative
  rather than recursive. Each test must assert the complete ordered transcript
  and retirement product, with a corresponding order/retention inversion.
- Prior-representation proof: the focused pinned-Nix
  `source_settlement_order` regression compiles against the old Engine port and
  fails on its complete transcript for the intended causal inversion. Actual
  execution is `Committed(1) -> RequestedOrdinary -> FoldedOrdinary ->
  Retired`; the governing law requires `Committed(1) -> Returned(1) ->
  Committed(2) -> Returned(2) -> Ordinary -> Retired`. The current Driver has
  no settlement value or source-only acquisition operation with which it could
  select the lawful transcript.
- 40-path implementation checkpoint: the working tree has reached the exact
  authorized ceiling. Production is `+1555 / -1463 / net +92`; tests are
  `+525 / -255 / net +270`, including the untracked 169-line source-order
  regression; manifests and locks are `+15 / -11 / net +4`; documentation is
  `+196 / -34 / net +162`; public API is `+3 / -2` types. The retained Driver
  runtime suites pass in debug and optimized builds: 30 causal laws, three
  generated properties, six terminal-custody cases, the allocation witness,
  and the source-order regression. Bombay's 166 library tests pass in debug
  and optimized builds; strict library Clippy passes; and the `run_with`,
  actor-interface, and semantic-send suites pass. The root-only application now runs the exact root
  Behavior; declared members alone use the typed application composition, so
  no forwarding wrapper remains solely to normalize `NoBirths`.
- Containment checkpoint: the all-target gates expose seven known files outside
  the authorized set. Engine's `send_not_sync` and
  `environment_phase_authority` sources still implement the superseded
  two-leg Environment port, and three compile-fail `.stderr` witnesses retain
  pre-0.16 diagnostic spelling. Bombay's `template_application` and
  `external_customer_templates` fixtures still import actor-catalogue types
  from Behavior Core. Updating those seven witnesses requires explicit scope
  expansion; no production abstraction is implicated. A ceiling of 50 paths
  would cover them and three further verification-only discoveries without
  relaxing the production or public-API budgets.
- Second scope expansion authorized: the user explicitly directed work to
  continue after the 40-path checkpoint, authorizing DX58 up to 50 tracked and
  untracked paths. The unchanged ceilings remain net `+500` production lines
  and `+3` new public types. This expansion covers only the seven identified
  verification witnesses and up to three additional all-target discoveries;
  it does not authorize new production architecture or bypass BEH1.
- 47-path checkpoint: Engine's complete compile-pass and compile-fail suite now
  passes under 0.16.0, including the non-`Sync` environment and prepared/active
  authority denials. The two Bombay actor-catalogue fixtures now import their
  owning Actors API directly. The complete delta is production
  `+1555 / -1463 / net +92`; tests and examples are
  `+612 / -305 / net +307`, including the untracked source-order regression;
  manifests and locks are `+15 / -11 / net +4`; documentation is
  `+221 / -34 / net +187`; public API remains `+3 / -2` types.
- Additional all-target discovery: four public examples—`counter`, `entity`,
  `application-topology`, and `supervision`—still name pre-0.16 terminal bounds,
  actor-catalogue imports, creation syntax, or retirement fields. All four must
  move together because examples are current public guidance. Editing them
  would reach 51 paths, beyond the authorized 50-path ceiling. No example was
  partially migrated. A 60-path ceiling would cover these four examples and
  reserve nine verification-only paths for diagnostics exposed after they
  compile, without relaxing either code-growth ceiling.
- Registry recheck: `cargo search` inside the pinned Nix shell still reports
  Core 0.16.0, Actors 0.16.0, and Macros 0.11.6 as the latest immutable
  releases. BEH1 therefore remains an external selected-contract blocker.
- Third scope expansion authorized: the user explicitly authorized continued
  work after the 47-path checkpoint, raising the DX58 ceiling to 60 tracked and
  untracked paths. The expansion covers the four public examples as one current
  guidance stage and up to nine verification-only discoveries. Net production
  remains capped at `+500` and new public types at `+3`.
- Supervision-example ownership correction: Actors 0.14 removed the former
  `Supervise`, `ChildTopology`, `Proxy`, and restart-wrapper family and replaced
  it with the clean-room `atomic::FixedSupervisor` construction. Bombay's
  catalogue facade exported every other Actors domain module but omitted
  `atomic`, leaving an application unable to name the owning replacement
  without adding a second direct dependency. The smallest correction touches
  the already-counted `crates/bombay/src/lib.rs` with one module re-export and
  rewrites the already-counted supervision example against `fixed`; it adds no
  public Bombay type and is expected to reduce example lines. The executable
  example will exercise construction and deterministic initialization only.
  Runtime recovery remains ineligible until a separately verified Bombay
  capability owns `PrepareWorkers`; the example must not fabricate that
  interpreter or preserve the removed compatibility stack.
- 51-path all-target checkpoint: the three migrated unblocked examples pass
  debug and optimized tests, strict all-target Clippy, and formatting. The
  topology example now fails only on BEH1. The next workspace check exposed
  five stale guidance files: actor templates still import `Machine` vocabulary
  from Core, Axum omits retained settlements, and the worker-pool pair names
  the Actors 0.13 pool family removed by 0.14. The authorized verification
  reserve covers those exact five files, reaching at most 56 paths. The pool
  example will use the owning `atomic::fifo` and `pool_worker` APIs and stop at
  deterministic initialization for the same unimplemented `PrepareWorkers`
  runtime-capability reason as fixed supervision; no compatibility pool,
  callback, or discarded settlement will be added.
- 57-path containment checkpoint: every stale public example outside BEH1 now
  compiles against the owning 0.16 API. Counter, Entity, fixed supervision,
  actor templates, Axum, and FIFO pool pass their focused debug and optimized
  tests; both example batches pass strict all-target Clippy. Bombay's complete
  all-feature library suite passes in debug and optimized builds (`166 passed`,
  `1 ignored`), and strict all-feature library Clippy passes. Formatting and
  whitespace checks pass. The complete all-target workspace check now reaches
  only the preserved BEH1 failures in `application_terminal_custody` and the
  application-topology example; the independently checked Entity application
  fails on the same generated-birth contract. No obsolete import, terminal
  shape, pool, or supervision diagnostic remains ahead of that blocker.
  Production is `+1560 / -1461 / net +99`; tests and examples, including the
  untracked source-order regression, are `+849 / -803 / net +46`; manifests
  and locks are `+26 / -34 / net -8`; documentation is
  `+290 / -34 / net +256`; public API remains `+3 / -2` types. The additional
  catalogue-module and `Machine` vocabulary re-exports expose existing Actors
  owners and add no Bombay type.
- Remaining external failure: after the in-scope library-test migration, the
  only semantic Bombay test failures are the two
  `application_terminal_custody` inversions and Entity's Profile child, all
  failing on BEH1's missing generated creation-settlement disposition. These
  are retained as the caller-visible upstream regression rather than bypassed
  by discarded settlements.
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

## DX61 — Machine test transition policy

- State: `feature-complete`; final fixed-point audit remains pending.
- Selected contracts: the exact owner graph remains unchanged. This stage
  touches only Machine's private executor tests; no Behavior or runtime
  capability consumes the representation.
- Exact blocker and regression: `ExclusiveTestMachine` and `OwnershipMachine`
  each encode the mutually exclusive “return normally or panic” transition
  policy in a field named `panic: bool`, with successor construction resetting
  that policy through an unlabelled `false`. The pre-edit structural oracle
  finds two semantic boolean fields and six boolean policy literals in the
  owning test module. The existing success, panic poisoning, exact input
  rejection, and drop-accounting tests are the observable regression suite.
- Governing invariant: a test machine's transition disposition is a closed sum
  with named alternatives. The successor explicitly selects ordinary return;
  no boolean may erase whether a transition returns or panics.
- Dependency edges: independent of DX53 through DX60.
- Blocked by: none.
- Unblocks: truthful fixed-point review of Machine's test-only state model.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay-machine/src/executor.rs` (`2` files). Production is
  `+0 / -0`; tests are no more than `+16 / -10 / net +6`; public API is
  `+0 / -0` types. One private enum replaces both boolean fields and all six
  policy literals. Any runtime trace, assertion, production item, public
  syntax, or dependency change falsifies the cleanup.
- Result: both private machines now store one shared
  `TestTransition::{Return, Panic}` value. Each transition exhaustively matches
  the disposition, and every successor explicitly selects `Return`. The two
  semantic boolean fields and all six policy literals are gone; no production
  item, dependency, or public interface changed.
- Verification: the pre-edit scan found two `bool` fields and six policy
  literals; the post-edit oracle finds none. All 18 Machine tests, compile
  fixtures, and documentation tests pass in debug and optimized builds, and
  formatting plus strict all-target Machine Clippy pass through pinned Nix.
- Actual checkpoint: tracked files `2`; production source
  `+0 / -0`; tests `+22 / -10 / net +12`; public API `+0 / -0` types;
  documentation `+43 / -0 / net +43`; complete tracked delta
  `+65 / -10 / net +55`. The test delta exceeds the six-line forecast because
  both owning transitions retain explicit exhaustive matches; no automatic
  containment threshold is approached.

## DX62 — Observe sequential model state

- State: `feature-complete`; final fixed-point audit remains pending.
- Selected contracts: the exact owner graph remains unchanged. This stage
  touches only Observe's independent proptest reference model; the production
  completion representation and public API are unchanged.
- Exact blocker and regression: the model passes `migrate: bool` to select
  whether a future reuses its latest waker or installs a new one, and stores
  completed `(key, epoch)` identities in `HashMap<_, bool>`. The first erases a
  mutually exclusive test operation; the second stores a set-membership fact
  as a value and admits meaningless `false` entries. The pre-edit structural
  oracle finds exactly that boolean parameter and completion map. Existing
  sequential-model and churn drop-accounting properties are the behavioral
  regressions.
- Governing invariants: future polling receives a closed named waker policy;
  completion history contains exactly the identities known completed. A local
  predicate may decide whether the selected policy installs a registration,
  but it may not replace the policy value or persist completion state.
- Dependency edges: independent of DX53 through DX61.
- Blocked by: none.
- Unblocks: an independent audit of the exhaustive Observe model rather than a
  mechanically shared testing framework.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/src/observe/external_tests/model.rs` (`2` files). Production
  is `+0 / -0`; tests are no more than `+14 / -10 / net +4`; public API is
  `+0 / -0` types. Existing `HashMap` state, proptest operations, observations,
  and wake probes remain; standard `HashSet` owns completion membership. Any
  property, generated operation distribution, outcome, wake count, drop count,
  production item, or dependency change falsifies the correction.
- Result: `FuturePoll::{Repoll, Migrate}` now preserves the generated poll
  operation through waker selection, while a local predicate answers only
  whether that selected operation installs a registration. Completed
  generations are a `HashSet`; false-valued membership is no longer
  representable. The generated operation distribution, independent oracle,
  production code, dependencies, and public interface are unchanged.
- Verification: the pre-edit scan found the boolean policy parameter and
  boolean-valued completion map; the post-edit oracle finds neither. All three
  model properties pass through both the private Observe crate and Bombay,
  focused model properties pass optimized, and all 121 private Observe tests
  plus documentation tests, formatting, and strict affected-package Clippy
  pass through pinned Nix.
- Actual checkpoint: tracked files `2`; production source
  `+0 / -0`; tests `+22 / -12 / net +10`; public API `+0 / -0` types;
  documentation `+50 / -0 / net +50`; complete tracked delta
  `+72 / -12 / net +60`. The test delta exceeds the four-line forecast because
  the named sum and exhaustive local selection remain visible rather than
  being hidden behind a boolean conversion.

## DX63 — Observe exhaustive model state

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact owner graph remains unchanged. This stage
  touches only Observe's deterministic exhaustive reference model; it shares no
  state representation with the proptest oracle or production implementation.
- Exact blocker and regression: `exhaustive.rs` stores live generation phase
  in two `Option<bool>` values, stores retired/completed generation membership
  in two `HashMap<_, bool>` values, and enumerates completion ordering and
  future cancellation as boolean policies. Those encodings conflate absence,
  phase, ordering, and terminal choice with truth values. The pre-edit oracle
  finds those representations. The exhaustive single-key, drop-order,
  future-order, and waker-drain history tests are the behavioral regressions.
- Governing invariants: absence remains `None`; a retained generation has one
  named pending/completed phase; historical completion is set membership; and
  completion order plus future disposition remain named exhaustive dimensions.
  Local predicates may compare these values but may not replace them.
- Dependency edges: independent of DX53 through DX62.
- Blocked by: none.
- Unblocks: a truthful semantic-state scan of the remaining Observe stress and
  allocator test controls.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/src/observe/external_tests/exhaustive.rs` (`2` files).
  Production is `+0 / -0`; tests are no more than `+52 / -34 / net +18`;
  public API is `+0 / -0` types. Existing exhaustive alphabets, history depths,
  handle permutations, observations, subjects, and wake probes remain exact.
  Any reduced history space, changed oracle, production item, shared model
  abstraction, dependency, or public interface falsifies the correction.
- Result: `GenerationPhase::{Pending, Completed}` replaces both live-generation
  booleans, and completed retired generations are represented only by
  `HashSet` membership. `CompletionOrder::{BeforePolls, AfterPolls}` and
  `FutureDisposition::{Cancel, Resolve}` preserve the names of the two
  exhaustive policy dimensions. No alphabet, depth, permutation, timing
  combination, oracle, production item, dependency, or public API changed.
- Verification: all seven exhaustive tests pass through both crate embeddings;
  the focused `observe-tests` suite also passes optimized; the complete 121-test
  private Observe suite and its docs pass; formatting and strict all-target
  Clippy pass for `observe-tests` and `bombay-rs` through the pinned Nix shell.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+66 / -48 / net +18`; public API `+0 / -0` types; documentation
  `+44 / -0 / net +44`.

## DX64 — Driver allocation fixture input ownership

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: Behavior Core 0.14.0, Behavior Actors 0.14.0, and
  Behavior Macros 0.11.4 remain selected at
  `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`; Address 0.2.0,
  Communication 0.1.2, Bombay-private Observe, and Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0` remain unchanged. Their source,
  public contracts, relevant tests, and current documentation were rechecked;
  none owns this actor-independent fixture state.
- Ownership and law: Engine owns the universal Driver and affine Environment
  port. Under D-TERM-2, an active Environment yields one exact `B::Event` or
  `None` for permanent exhaustion. The allocation regression's private
  Environment therefore owns one queued event or its absence.
- Exact blocker and regression: `ImmediateEnvironment(bool)` stores the
  mutually exclusive available/exhausted input state as an unnamed truth value,
  then uses `mem::replace` and negation to recover the transition. The pre-edit
  structural oracle finds that tuple boolean. The existing zero-allocation
  end-to-end Driver execution is the behavioral and cost regression.
- Proposed representation: store `Option<User<MailAddr, u8>>` and consume it
  with `Option::take`. This is the exact one-value-or-absence algebra; it adds no
  phase type, wrapper, policy, branch, allocation, or public surface.
- Dependency edges: independent of DX53 through DX63.
- Blocked by: none.
- Unblocks: the remaining Engine test-policy scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay-engine/tests/driver_allocation.rs` (`2` files). Production is
  `+0 / -0`; tests are no more than `+6 / -3 / net +3`; public API is
  `+0 / -0` types. The existing `ImmediateEnvironment`, Driver, Behavior,
  allocation counter, one-event execution, and zero-allocation assertion are
  reused. Any production, dependency, public API, allocation count, event,
  terminal disposition, or additional state type falsifies the correction.
- Result: `ImmediateEnvironment` now owns the exact optional `User` event and
  yields it with `Option::take`. The boolean phase, negation, and
  `mem::replace` protocol are gone. The same event produces the same stopped
  Driver retirement without allocation; production, dependencies, public API,
  and test count are unchanged.
- Verification: the allocation regression passes with zero allocations in
  debug and optimized profiles; the complete Engine suite, compile fixtures,
  Driver laws, inversions, properties, custody tests, and docs pass; formatting
  and strict all-target Engine Clippy pass through the pinned Nix shell.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+6 / -3 / net +3`; public API `+0 / -0` types; documentation
  `+47 / -0 / net +47`.

## DX65 — Driver panic injection policy

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact owner graph recorded in DX64 was rechecked at
  its locked revisions. This stage touches only Engine's D-TERM-4 test fixture;
  no Behavior, Behavior Actors, Address, Communication, Observe, Timers,
  runtime, interpreter, or public contract changes.
- Ownership and law: Engine's Driver owns panic terminality under D-TERM-4.
  Its private fixture selects one of two mutually exclusive injection stages:
  Behavior initialization or an ordinary turn. The existing combined test is
  D-TERM-4's positive manifest witness; the turn-only test is deliberate
  adversarial evidence reused by the complete manifest and is not redundant.
- Exact blocker and regression: `PanicBehavior::panic_in_init` and
  `panic_case(bool)` erase the selected injection stage behind a truth value;
  three call sites use unexplained literals. The pre-edit structural oracle
  finds the boolean field, parameter, and literals. The two manifest-accounted
  panic tests are the behavioral regressions.
- Proposed representation: a private `PanicStage::{Initialization, Turn}` sum
  owns the policy. Initialization selects its behavior exhaustively; ordinary
  turn injection remains the fixture's transition law. No production or public
  type is added.
- Dependency edges: independent of DX53 through DX64.
- Blocked by: none.
- Unblocks: the remaining Driver fixture-state scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay-engine/tests/driver_law.rs` (`2` files). Production is
  `+0 / -0`; tests are no more than `+15 / -8 / net +7`; public API is
  `+0 / -0` types. Existing Driver, Behavior, Environment, panic capture,
  ownership probes, test names, and manifest references remain exact. Any
  changed panic point, event-source poll count, drop fact, evidence name,
  production item, dependency, or public API falsifies the correction.
- Result: `PanicStage::{Initialization, Turn}` replaces the boolean field,
  parameter, and three literal selections. Initialization selection is
  exhaustive; the established turn injection remains unchanged. Both test
  names and their distinct positive/adversarial manifest roles remain exact.
- Verification: both panic witnesses pass in debug and optimized profiles with
  their exact source-poll and ownership-drop facts; the complete Engine suite,
  compile fixtures, laws, inversions, properties, custody tests, docs, and the
  explicit ignored manifest-closure gate pass; formatting and strict all-target
  Engine Clippy pass through the pinned Nix shell.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+15 / -8 / net +7`; public API `+0 / -0` types; documentation
  `+46 / -0 / net +46`.

## DX66 — Driver custody failure ownership

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph and complete Driver law
  evidence remain as rechecked for DX64 and DX65. This stage touches only the
  terminal-custody fixture for D-CAP-7 and D-TERM-7; no owning dependency,
  runtime, interpreter, production, or public contract changes.
- Ownership and law: the fixture Behavior owns its optional initialization
  failure; the prepared Environment owns its optional activation failure; the
  active Environment owns its optional action-application failure. Each error
  is an exact fact that the Driver must retain in its distinct `DriverError`
  variant beside final custody.
- Exact blocker and regression: five fields/parameters encode failure presence
  as booleans while the transition bodies reconstruct three hard-coded errors;
  six scenarios use thirteen unexplained truth literals. The pre-edit
  structural oracle finds those boolean policies. The complete six-test custody
  matrix is the behavioral regression, including exact state, residual phase,
  committed prefix, completion, and failure assertions.
- Proposed representation: each owner stores `Option<&'static str>`, exactly one
  error or absence, and returns the stored error without reconstruction. This
  adds no wrapper, enum, public type, or new behavior.
- Dependency edges: independent of DX53 through DX65.
- Blocked by: none.
- Unblocks: the remaining Engine semantic-boolean scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay-engine/tests/terminal_custody.rs` (`2` files). Production is
  `+0 / -0`; tests are expected to be line-neutral and no more than
  `+30 / -30 / net +2`; public API is `+0 / -0` types. Existing Behavior,
  Environment phases, events, committed prefixes, error strings, retirement
  assertions, and all six scenario names remain exact. Any changed trace,
  failure provenance, production item, dependency, public API, or aggregate
  failure policy falsifies the correction.
- Result: initialization, activation, and application failure owners now retain
  their exact optional error and return that same value. All five boolean
  fields/parameters, thirteen truth literals, and three internal error
  reconstructions are gone. The complete custody traces and test names remain
  unchanged; no aggregate policy or new type was introduced.
- Verification: all six custody tests pass in debug and optimized profiles with
  complete state/residual/disposition assertions; the complete Engine suite,
  compile fixtures, laws, inversions, properties, docs, and explicit manifest
  closure pass; formatting and strict all-target Engine Clippy pass through the
  pinned Nix shell.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+33 / -33 / net 0`; public API `+0 / -0` types; documentation
  `+50 / -0 / net +50`. The gross test replacement exceeded the `30`-line
  estimate by three because each failure site now carries its exact error at
  setup; the stage remains line-neutral with no added machinery.

## DX67 — Entity Loom admission algebra

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact owner graph remains unchanged and was
  rechecked against the locked Behavior/Actors/Macros, Address, Communication,
  Observe, and Timers sources. Entity's current lifecycle algebra,
  runtime-capability contract, DX49 ownership record, unit tests, runtime tests,
  and Loom fixture were inspected for this stage.
- Ownership and law: Entity lifecycle owns admission closure, outstanding
  delivery reservations, and ordered-fence progress. The independent Loom model
  must represent exactly open admission with a count, draining with a non-zero
  count, or the fence-enqueued terminal state. A successful reservation grants
  the sole capability to submit one resolution.
- Exact blocker and regression: the Loom `Admission` product independently
  stores `closed`, `reservations`, and `fence_enqueued`, admitting closed-zero
  without a fence, open-with-fence, and fenced-with-outstanding-reservations.
  Delivery admission is also reduced to a boolean. The pre-edit structural
  oracle finds those fields and return type. The two exhaustive Loom tests over
  delivery/drain races are the behavioral regressions.
- Proposed representation: replace the coordinated product with
  `Admission::{Open(usize), Draining(NonZeroUsize), Fenced}` and return an opaque
  private `Reservation` only from successful admission. Closing and resolving
  commit one complete valid state; no production type is shared with the
  independent oracle.
- Dependency edges: independent of DX53 through DX66.
- Blocked by: none.
- Unblocks: the remaining Entity concurrency-fixture scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/tests/entity_loom.rs` (`2` files). Production is `+0 / -0`;
  tests are no more than `+65 / -50 / net +15`; public API is `+0 / -0` types.
  The existing mutex linearization, Loom schedules, two delivery contenders,
  drain thread, test names, joins, and terminal assertions remain. The private
  reservation capability deletes the admitted boolean and proves which path may
  resolve. Any reduced schedule space, copied production transition, changed
  terminal law, dependency, production item, or public API falsifies the model.
- Result: the independent model now has only `Open(count)`,
  `Draining(nonzero)`, and `Fenced` states. Successful admission yields one
  private `Reservation`, and only that capability can resolve the count. The
  coordinated booleans and admitted-return boolean are gone; the test model is
  one line smaller and shares no production transition implementation.
- Verification: both admission/fence Loom regressions pass in debug and
  optimized profiles; the complete five-test Entity Loom target and complete
  `bombay-rs` unit, integration, compile-contract, and documentation suites
  pass; formatting and strict all-target `bombay-rs` Clippy pass through the
  pinned Nix shell. The initial malformed two-filter Cargo invocation was
  rejected before compilation and was rerun with the exact shared filter.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+62 / -63 / net -1`; public API `+0 / -0` types; documentation
  `+52 / -0 / net +52`.

## DX68 — Entity Loom activation claim

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph and Entity lifecycle/runtime
  sources rechecked for DX67 remain unchanged. This stage touches only the
  adjacent independent Loom activation-claim oracle; no production owner,
  dependency, or public contract changes.
- Ownership and law: an inactive logical entity admits exactly one activation
  claim. The model's mutex owns one `ActivationClaim` or its absence, while the
  atomic counter independently observes how many activation starts occurred.
- Exact blocker and regression: `Mutex<bool>` stores the claim phase and the two
  contenders interpret `false`/`true` as unclaimed/claimed. The pre-edit
  structural oracle finds that representation. The exhaustive
  `concurrent_claims_start_exactly_one_activation` Loom test is the behavioral
  regression and must still observe a claimed phase plus exactly one start.
- Proposed representation: `Option<ActivationClaim>` is exactly one private
  claim capability or absence. No enum, shared production model, or additional
  synchronization is introduced.
- Dependency edges: independent of DX53 through DX67.
- Blocked by: none.
- Unblocks: the remaining Entity runtime-fixture scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/tests/entity_loom.rs` (`2` files). Production is `+0 / -0`;
  tests are no more than `+10 / -7 / net +3`; public API is `+0 / -0` types.
  The same mutex, two contenders, start counter, Loom schedules, joins, and
  assertions remain. Any changed schedule, start count, synchronization,
  production item, dependency, or public API falsifies the correction.
- Result: the mutex now contains `Option<ActivationClaim>`, so availability is
  absence and the winning contender installs the one claim capability. The
  boolean phase and truth-value assertion are gone; mutex and counter operations
  remain at the same linearization points.
- Verification: the focused claim race passes in debug and optimized profiles
  with exactly one activation start; the complete five-test Entity Loom target,
  formatting, and strict all-target `bombay-rs` Clippy pass through the pinned
  Nix shell. The complete `bombay-rs` suite passed immediately before this
  adjacent isolated stage in DX67.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+8 / -6 / net +2`; public API `+0 / -0` types; documentation
  `+42 / -0 / net +42`.

## DX69 — Entity hash-gate phase

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the exact locked owner graph and Entity runtime/lifecycle
  contracts rechecked for DX67 and DX68 remain unchanged. This stage touches
  only the real-thread superseded-passivation fixture; no production owner,
  dependency, or public contract changes.
- Ownership and law: `HashGate` owns a deterministic test synchronization
  protocol. It counts selected hash calls, publishes that the third call is
  blocked, and later releases that exact call. Its phase is one of counting,
  blocked, or released; the selected thread identity and call count coexist
  independently with that phase.
- Exact blocker and regression: `HashGateState` coordinates `blocked` and
  `released` booleans, representing blocked-and-released, neither after release,
  and release-before-block combinations. The pre-edit structural oracle finds
  both fields and their wait loops. The caller-visible
  `passivation_reports_superseded_after_incarnation_replacement` race is the
  behavioral regression.
- Proposed representation: one private
  `HashGatePhase::{Counting, Blocked, Released}` sum, selected exhaustively by
  the gate methods and hash implementation. The mutex and condition-variable
  synchronization remain at the same boundaries.
- Dependency edges: independent of DX53 through DX68.
- Blocked by: none.
- Unblocks: the remaining Entity runtime-fixture scan.
- Change ledger: expected tracked files are this ledger and
  `crates/bombay/tests/entity_runtime.rs` (`2` files). Production is `+0 / -0`;
  tests are no more than `+16 / -8 / net +8`; public API is `+0 / -0` types.
  Existing selected thread, third-hash threshold, condition variable,
  passivation/replacement race, test name, and complete outcome assertions
  remain exact. Any changed blocking point, schedule, result, production item,
  dependency, or public API falsifies the correction.
- Result: `HashGateState` now stores exactly one
  `HashGatePhase::{Counting, Blocked, Released}` alongside the independently
  coexisting selected thread and call count. Both coordinated boolean fields
  are gone. The same selected thread blocks on its third hash call and resumes
  only after the test releases that call; no synchronization primitive,
  threshold, or observable assertion changed.
- Verification: the superseded-passivation race passes in debug and optimized
  profiles; all eleven Entity runtime tests pass; formatting and strict
  all-target `bombay-rs` Clippy pass through the pinned Nix shell. The complete
  `bombay-rs` suite passed immediately before this adjacent fixture-only stage.
- Actual checkpoint: tracked files `2`; production `+0 / -0 / net 0`; tests
  `+16 / -8 / net +8`; public API `+0 / -0` types; documentation
  `+48 / -0 / net +48`.

## DX70 — Observe fuzz state ownership

- State: `feature-complete`; final fixed-point minimization remains pending.
- Selected contracts: the locked Behavior 0.14.0, Behavior Actors 0.14.0, and
  Macros 0.11.4 revision remains
  `a272adf8d2cbb6a2784d565f47c74adff3e7d01b`; Address 0.2.0,
  Communication 0.1.2, and Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0` remain unchanged. Their public
  algebras have no edge to this private Observe oracle. Observe's subject,
  completion, observation, future, waker, and exact-outcome APIs and their
  owning tests were rechecked with all four fuzz targets and the pinned fuzz
  shell declaration.
- Ownership and law: one `(key, epoch)` generation either belongs to the set of
  generations that completed or it does not. Completion membership governs
  readiness, exact waker firing, and whether the last retired observation may
  take its outcome. Active and retired generations obey the identical law.
- Exact blocker and regression: `waker_ops` and `promotion_ops` store completion
  in `HashMap<(u8, u64), bool>` values that are only ever inserted as `true`;
  `future_ops` splits the same fact between a current boolean array and a
  retired-generation boolean map. Its `Fut.cancelled` flag is also initialized
  false for every live vector member, written true only after `pop`, and then
  destroyed without another read; vector membership already owns liveness.
  The pre-edit structural oracle finds all three boolean maps, the flag array,
  the active/retired branch, and the redundant cancellation field. The four
  existing coverage-guided operation alphabets and their exact readiness,
  wake-count, generation-tag, cancellation, move, and drop-accounting
  assertions are the behavioral regressions.
- Proposed representation: use `HashSet<(u8, u64)>` membership directly in
  each independent fuzz target. `future_ops` retains one set across active and
  retired generations because retirement does not change the historical fact;
  presence in its live-future vector represents liveness, while the popped
  owned future supplies the cancellation probes before destruction. No shared
  model, production transition, wrapper, or new fuzz operation is introduced.
- Dependency edges: independent of DX53 through DX69.
- Blocked by: none.
- Unblocks: the remaining Observe fuzz-model scan.
- Change ledger: expected tracked files are this ledger plus
  `future_ops.rs`, `promotion_ops.rs`, and `waker_ops.rs` (`4` files).
  Production is `+0 / -0`; tests must be net-negative and remain within
  `+15 / -35`; public API is `+0 / -0` types. The exact key and epoch domains,
  operation alphabets, iteration bounds, subject/observation/future ownership,
  waker migration and cancellation probes, outcome tags, drop accounting, and
  all oracle messages remain. Any changed fuzz input interpretation, weakened
  assertion, retained semantic boolean, production item, dependency, public
  API, or net-positive test delta falsifies the correction.
- Result: all three independent models now store completed generations as set
  membership. `future_ops` uses the same set before and after retirement, and
  live-vector membership plus ownership of the popped future replaces the
  unread cancellation flag. The boolean maps, current flag array, retired map,
  active/retired lookup branch, field, writes, and guards are gone. Test code
  is twenty lines smaller with no shared model or production machinery.
- Verification: all four fuzz binaries build in the pinned nightly fuzz shell;
  `ops`, `future_ops`, `promotion_ops`, and `waker_ops` each completed 10,000
  bounded runs without an artifact, with the final three affected targets
  replayed after their last edits. All 121 private Observe tests and docs pass;
  strict Clippy passes for the complete Observe library and the three affected
  fuzz bins; root formatting and exact formatting of the affected fuzz files
  pass. The independent workspace's whole-format and whole-Clippy probes also
  exposed pre-existing drift and two `ops.rs` lints outside this stage; no
  unrelated source was absorbed.
- Actual checkpoint: tracked files `4`; production `+0 / -0 / net 0`; tests
  `+59 / -79 / net -20`; public API `+0 / -0` types; documentation
  `+69 / -0 / net +69`. Gross test churn exceeded the forecast because the
  strict lint correction flattened the three completion branches and removed
  the independently proven cancellation guards; the required net-negative
  boundary and all stop thresholds remain satisfied.

## DX71 — Observe consumer narrowing

- State: `landed` (PR #315, merged 2026-09-17).
- Selected contracts: the locked Behavior 0.16.0 revision and the private
  Observe implementation are unchanged. The keyed `ObservationSpace`/
  `Subject` enumeration surface is deleted because no production consumer
  exists: no Bombay actor, runtime, or example subscribes by
  `(key, epoch)`; the only caller of the keyed APIs was the observe test
  corpus itself. A future enumeration-API need belongs in an upstream
  bombay-address request, not in dead Bombay surface.
- Ownership and law: one publication is one fresh unkeyed pair — the
  publisher is the unique (non-cloneable) publication authority, every
  observation clone stays attached to the exact completion slot, and the
  slot is reclaimed only when every handle (publisher and all
  observations) is gone. Waker registration fires exactly once iff the
  generation completes; `into_outcome` still requires exclusive slot
  ownership. These laws are unchanged; only the keyed namespace over
  them is gone.
- Exact blocker and regression: `lib.rs:38-42` suppressed `dead_code`
  for the whole private module; the keyed-only external suites
  (`contract`, `exhaustive`, `model`, `pool`) were the sole consumers of
  the deleted surface.
- Result: the keyed surface, pool/map machinery, keyed lock helpers,
  and slot-reset path are deleted; the retained pair/affine corpus
  (blocking/timed waits, waker registration and dedup, future
  cancellation and migration, exactly-once destruction, panic
  propagation, stress, Loom) is restated on pairs; the perf and fuzz
  campaigns are converted (promotion_ops became volume_ops).
  `#[allow(dead_code)]` is removed from `lib.rs` and not replaced.
- Change ledger: expected tracked files are this ledger, `lib.rs`,
  `observe/mod.rs` and its tests, `observe-perf/src/main.rs`, the fuzz
  targets, and `docs/runtime-capability-interfaces.md` (keyed contract
  paragraph rewritten to the pair reality). The generic 15-file stop
  threshold is exceeded by the explicitly scoped corpus shrink; the
  threshold's intent (unreviewed sprawl) does not apply to deletion
  plus one-for-one restatement, and no new public type is added.
- Verification: `cargo check -p observe-tests -j 1` (plain and
  `--all-targets`) exit 0; `cargo check -p observe-perf -j 1` exit 0;
  all four fuzz bins compile and 2,000 libFuzzer runs each found no
  law violation; the full `observe-tests` suite passes 80/80 under
  `--test-threads=1`; strict Clippy and `cargo fmt --check` pass. The
  full-workspace check remains blocked in this sandbox by the pinned
  `bombay-behavior-actors` rustc OOM (environment, not a diagnostic)
  and runs in CI.

## B4 — typed `PrepareWorkers` capability interpreter

- State: `in-progress` on `feat/prepare-workers-capability` (stacked on the
  distillation branch at `bb5b3e9`; Behavior pinned at `8bca837c`).
- Selected contracts (verified this session against the pinned checkout):
  `PrepareWorkers<Source, Role, Worker, Plan>` is a `SourceAction` owned by
  Actors whose `WorkerSource` trait deliberately declares types only; the
  affine attempt protocol (`source_and_role` → prepare → `accept`/`reject`)
  must be driven by Bombay. The supervisor and FIFO pool send products also
  require `ProxyOperation`, `InitializeWorker`, `BeginActivation`,
  `AssignWorker`, and `DiagnosticAction` interpretations before either
  aggregate can commit actions in the Bombay runtime; `ObserveChild`,
  `ScheduleAfter`, `ShutdownEstablished`, `EstablishedDelivery`, and
  `ReportToParent` are already interpreted. Actors templates carry their own
  `EventIngress`/`InjectEvent` impls for every settled return, so no Actors
  change is needed.
- Ownership: Bombay owns (1) the worker-source preparation port — a
  Bombay-published trait with one method producing a `WorkerSubmission` per
  role, mirroring the existing verb-capability grammar (`CompletesAssignments`)
  and invoked only by the runtime interpreter outside every fold, like
  `ActivationPlan::activate` and `DiagnosticRoute`; (2) a pure ordered-role
  driver over the affine attempt protocol; (3) the missing `InterpretItem`
  implementations on the existing application capabilities. The driver returns
  complete settlements only: Bombay never fabricates an application
  `SourceRejection` value, so accepted preparations carry the exhaustive
  `WorkerPreparation` sum (all roles prepared, or prepared prefix + failed
  role + exact reason + untouched suffix) back to the emitter through the
  existing `SourceAdmission`/`EventIngress` custody path.
- Rejected shortcuts: no runtime callback invoked by supervisor folds, no
  erased submission registry, no second effect framework, no discarded
  settlement custody, no relaxation of preparation tickets or ordered roles.
- Follow-up findings (against `8bca837c`): (1) `InitializeWorker` is emitted
  only after the creation settles `Installed`, and Bombay hosting settles
  initialization inside `spawn_owned_with` before activation publication — an
  established creation binding is exact proof of settled initialization, so
  `WorkerInitializationOutcome::ReadyForActivation` is the only truthful
  post-establishment report; initialization rejection fails the creation
  settlement instead, and a post-establishment stop race is detectable through
  the child's retained termination observation (`try_get`) mapping to
  `Stopped(ChildStopped<BehaviorAddr<W>>)`. (2) `BeginActivation` returns
  `started()` to the emitter, polls the plan with `activate().await`, and
  returns the `WorkerActivation` outcome; the emitter's control lane stays live
  during interpretation, so `ActivationStartRejection::OwnerStopped` is
  unreachable for a conforming self-interpreter and binding failure maps to
  `Corrupt`. (3) `AssignWorker` is blocked on an Actors-side seam: every
  non-accepted `ItemSettlement` variant must retain the complete original
  item, but the only post-consumption reconstruction path,
  `AssignWorker::returned`, is `pub(in crate::atomic)`; a non-consuming
  liveness probe (termination `try_get` before consuming) lawfully settles the
  common closed-recipient case exactly like the Actors test flow, but a
  recipient closing between probe and enqueue leaves no lawful settlement.
  Landing `AssignWorker` requires the pinned Actors revision to make
  `AssignWorker::returned` public (one-line visibility change in
  bombay-behavior) or an equivalent public construction seam; until then the
  FIFO assignment lane cannot be interpreted without erasing custody. This
  supersedes the earlier "no Actors change is needed" assumption for the
  assignment lane only.
- Unblocks: real recovery (replacements under stable proxies, FIFO restart
  with backlog retention) in the supervision and worker-pool examples.
## W5 — integration benchmark and memory evidence

- State: `active`; implementation committed on this branch (bench suite,
  allocation test, observe-perf consumer fix, `docs/benchmarks.md`). The
  Observe allocation/retention table is measured and recorded; the
  application-spine, mailbox-lane, and dhat-application rows remain pending
  the distillation base's compile repair (14 `bombay-rs` errors at `ebd79cb`
  verified on the tip), so those rows record no numbers and
  make no claims.
- Selected contracts: Behavior Core 0.16.0, Behavior Actors 0.16.0, and
  Behavior Macros 0.11.6 are selected from
  `8bca837ca5d913bcdfaefbe0dec33d58bfc9ace6` through the user-authorized
  exact-revision exception (`[patch.crates-io]`, commit `bb5b3e9`); Address
  0.2.0, Communication 0.1.2, Bombay-private Observe, and Timers 0.1.0 at
  `13e884da7ab41781f52337b0038060e375b00ee0`; dev-only dhat 0.3.3 and
  Criterion 0.7.0 from the registry. The public Application surface
  (`Application::run_with`, `ApplicationHandle`, `ApplicationLifecycle`,
  terminal projection), the two-lane `mailbox_channel` admission contract,
  and the Observe publication contract were rechecked against this exact
  selection. No owner owns integration-benchmark evidence: the three
  maintained benches cover only Entity directory, Entity lifecycle, and
  Machine executor micro-paths, plus the Engine Driver turn.
- Ownership: benches own measurement only. Bombay owns the composed local
  Environment the end-to-end suite exercises (spawn, tell-shaped send, turn,
  shutdown, retirement through `run_with`). Communication owns the two-lane
  mailbox scenarios' transport. Observe owns the watcher-fanout mechanism,
  already measured by the maintained `observe-perf` harness; registry claims
  are already measured by the maintained `entity_directory` bench. No second
  runtime, mailbox, registry, or observation mechanism is introduced.
- Requirement: the Phase 0 exit gate needs integration evidence that the
  maintained benches do not provide — an end-to-end benchmark over the real
  local Environment and semantic equivalents of the retired old-runtime bench
  (mailbox tell/reply, watcher fanout, registry, channels) — plus recorded
  allocation/retention memory results and a recorded results table whose
  every number names environment, revision, feature set, and method.
- Exact blocker and regression: the old-runtime oracle
  (`benches/mailbox.rs`, realistic ~40 B command, Criterion, recorded in the
  `b0c212a` history) measured `tell` ≈ 5.7 ns and send+recv ≈ 18.4 ns. That
  figure is recovered as a labeled oracle baseline only; no current claim
  borrows its number. The stale `.auto/measure.sh` consumer reference in the
  `observe-perf` module doc names a consumer that no longer exists in the
  repository.
- Dependency edges: stacked on `research/bombay-distillation-2026-09-15` at
  `bb5b3e9`; sibling wave work merges into that branch concurrently, so the
  branch rebases on its live tip immediately before final verification.
  Independent of DX53 through DX70 semantics; touches no runtime code.
- Blocked by: none.
- Unblocks: the Phase 0 exit gate's performance and memory evidence; the
  `observe-perf` harness doc no longer claims a dead consumer.
- Change ledger: expected tracked files are this ledger,
  `crates/bombay/benches/application_spine.rs`,
  `crates/bombay/benches/mailbox_lanes.rs`,
  `crates/bombay/tests/application_allocations.rs`,
  `crates/bombay/Cargo.toml`, `flake.nix`,
  `crates/observe-perf/src/main.rs`, and `docs/benchmarks.md` (`8` files).
  Production is `+0 / -1 / net -1` (the dead consumer reference); tests and
  benches stay within `+500`; public API is `+0 / -0` types. The existing
  `Application` boundary, `mailbox_channel`, `ObservationSpace`, the
  directory bench conventions, and the flake `performance` lane are reused.
  Any new public type, second transport, or production logic change
  falsifies this feature.
- Verification: scoped `cargo check`, focused bench runs, and the dhat
  allocation test pass through the pinned Nix shell; `cargo fmt --all
  -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and `nix flake check -L` pass before the final push; the results table
  records the exact commands, environment, revision, and numbers.

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

## DX71 — one Entity spelling

- State: `active`.
- Unblocks: DX49; retires the last second-spelling Entity surface.
- Driver: the generic `EntityRuntime` facade and its `LocalEntityRuntime` port
  duplicate the one ordinary Entity path behind a second vocabulary —
  activate/deliver/fence/retire callbacks plus a spawn port — that every
  production command crosses twice. No behavioral blocker exists; the smallest
  regression guard is the existing entity lifecycle suite, which must preserve
  every law after the fold, including the two bounded-spin determinism tests.
- Locked default (Phase 0 open question 2): the native application Entity path
  (`EntityDefinition` → `Entities`/`EntityRef` over the Bombay runtime) is the
  one ordinary spelling. `LocalDirectory` and its `EffectInterpreter` seam
  remain the advanced test-host boundary; the folded admission, settlement, and
  shutdown composition stays crate-internal.
- Dependency verification: Behavior core, actors, and macros remain the exact
  patch revision `8bca837ca5d913bcdfaefbe0dec33d58bfc9ace6` selected by the
  documented exact-revision exception. The fold adds no dependency, no macro
  generation, and no second Driver or event loop.
- Expected shape: delete `entity/runtime.rs` (~700 lines); absorb admission
  custody, waiter cancellation, settle-before-drain, passivation
  classification, and family shutdown into the directory composition; rehome
  the lifecycle and family mechanism tests inside the owning crate with both
  spin-budget-sensitive tests intact; implement the `EffectInterpreter` lanes
  natively in the Bombay runtime; record the retained advanced seam in the
  module boundaries and capability documents.
- Expected containment: about eight production files, a net production-line
  deletion far below the 500-line stop threshold, zero new public types, and
  three removed public types (`Activated`, `EntityRuntime`,
  `LocalEntityRuntime`).
