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

DX54 Triomphe feature minimization (feature-complete, independent)
```

There is no unresolved Bombay prerequisite. MNE1 requires Mnesis-Bombay to
select the current Bombay and Behavior graph and implement its own durable
command execution contract. Bombay deliberately does not classify mailbox
admission as durable completion.

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

Nextest passed 501 tests across 48 binaries. All 17 declared flake checks
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
