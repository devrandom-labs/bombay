# Repository Guidelines

## Project Structure & Module Organization

This is a Rust 2024 workspace for the `bombay` runtime. The root `Cargo.toml` defines shared dependencies and lints; implementation code lives in `crates/bombay/src/`. Keep crate-level exports and documentation in `lib.rs`, and add focused runtime modules beside it as the implementation grows. Use `docs/open-design-ledger.md` as the canonical backlog, `docs/module-boundaries.md` for ownership boundaries, `docs/minimal-lifecycle-ownership.md` for lifecycle ordering, and `docs/runtime-blocks.md` for crate scope.

## Architecture & Scope

Preserve the repository boundaries: `behavior` owns the pure behavior algebra, Bombay Communication owns the two-lane mailbox, and `bombay` owns runtime composition such as addresses, `System`, `Handle`, spawning, delivery, and incarnation retirement. Do not add registries, request/reply APIs, lifecycle frameworks, or supervision policy. Behaviors must remain deterministic and free of I/O, channels, and runtime handles. Prefer composition from existing primitives over introducing new machinery.

## Mandatory Cross-Crate Verification

Never assume what any Bombay crate supports, owns, guarantees, or lacks. Before designing, implementing, unblocking, or auditing **every** bombay feature, inspect the current source, public algebra/API, tests, and relevant documentation for all neighboring primitives: Bombay Behavior, Bombay Observe, Bombay Timers, Bombay Communication, and Bombay Address. Check the exact dependency version used by bombay as well as any local checkout; do not treat an bombay adapter, an earlier conversation, the design ledger, memory, or a differently versioned checkout as authoritative evidence about another crate.

Record the cross-crate findings and exact ownership mapping in `docs/open-design-ledger.md` before changing implementation. Keep the feature blocked when any dependency contract, version relationship, protocol consumer, or ownership boundary has not been verified. If the existing Bombay algebra already expresses a capability, compose or interpret it instead of introducing a duplicate bombay protocol. Repeat this verification for every feature even when a previous feature audited the same crates: never assume the prior result still applies.

### Behavior Effect Composition

Before modifying behavior effects, derive the implementation from the exact
locked Bombay Behavior API and its current tests. Do not preserve or introduce
positional `SendProduct` traversal such as `.inner`, `.own`, or nested direct
lane mutation in application behavior. Compose heterogeneous lanes with
Behavior's existing `SendProduct`, define semantic `Own`/`Inner<Path>` aliases,
and emit effects through typed `SendAlgebra::send`. Do not handwrite
application `SendAlgebra`, `SendInput`, `RouteSends`, `ObservesCreations`, or
product-routing error implementations when Behavior's recursive product and
bombay's generic interpreter cover the leaves. Keep application
`DeliveryRouter<A, M>` implementations because those select real endpoints.
Audit the entire repository—not merely the touched file—for obsolete
positional patterns and duplicate Behavior protocol implementations before
completion; existing bombay code is not authoritative evidence that an
older Behavior composition style remains acceptable.

Behavior's `Compose::from_fns`/`BehaviorFn` may replace a simple handwritten
behavior only when the concrete value remains locally inferred. Do not replace
a clear nominal behavior with function-pointer helpers or expanded generic
aliases merely to name it inside `Births`, `Proxy`, `Supervisor`, endpoints, or
routers. Wait for a Behavior-owned naming macro for those positions. Semantic
wrappers coordinating wrapper-owned event protocols remain explicit.

### Documentation Synchronization

When a dependency contract or composition rule changes, audit every tracked
document, example, benchmark, test, research workspace, diagnostic probe, and
public re-export. Update all current guidance in the same change. Mark
historical ledgers, experiments, and captured diagnostics explicitly as
historical when their older API descriptions remain evidence; never leave them
looking normative. Do not claim repository-wide completion from a `crates/`
scan alone.

## Backlog and Completion States

Treat `docs/open-design-ledger.md` as persistent context across sessions. Before starting work, update the selected item's priority, state, blockers, and the items it unblocks. Record newly exposed seams immediately instead of relying on conversation history.

Choose the next item from the dependency graph, never from its row position in the restart table. Treat both directions as normative: every ID that lists an item under `Unblocks` must also appear in that item's `Blocked by` cell until the prerequisite is complete. If the two columns disagree, stop and repair the ledger before selecting work. First discard completed items and any item with an unresolved ledger-ID or external prerequisite. An item whose only blocker is its own mandatory cross-crate verification is eligible for that verification work; it is not eligible for implementation until that verification is recorded and the blocker is cleared. Among eligible items, use priority and the dependencies they unblock to choose the next item. Never describe an item as unblocked while its `Blocked by` cell contains an unresolved prerequisite, and update downstream blocker cells immediately when a prerequisite becomes `feature-complete` or `distilled`.

An item may move from `blocked` to `active` only after its own mandatory cross-crate verification is recorded in the ledger. A global audit does not waive the per-feature verification requirement.

Never mark implementation work `done` merely because it builds or its feature tests pass. Use `feature-complete` while it awaits the project-wide final audit. That final audit must attempt to decompose it further, minimize types and objects, narrow public interfaces, compartmentalize ownership, and prove each remaining primitive independently testable and statically composable. Only then may an item be marked `distilled`; do not use a `done` state.

## Build, Test, and Development Commands

- `nix develop` enters the pinned development shell with Rust and helper tools.
- `cargo build --workspace` builds every workspace crate.
- `cargo test --workspace` runs all unit, integration, and documentation tests.
- `cargo fmt --all -- --check` verifies standard Rust formatting.
- `cargo clippy --workspace --all-targets` applies the workspace's strict Clippy configuration.

The pinned toolchain is declared in `rust-toolchain.toml`, so plain Cargo commands also use Rust 1.96.0 through rustup.

## Coding Style & Naming Conventions

Use rustfmt defaults (four-space indentation) and idiomatic Rust naming: `snake_case` for modules/functions, `CamelCase` for types/traits, and `SCREAMING_SNAKE_CASE` for constants. Choose explicit behavioral names such as `stop_on_abnormal_death`; keep literature citations in documentation, not identifiers. Avoid `unsafe` unless a measured reason is documented.

## Testing Guidelines

Place unit tests in the module they exercise and integration tests under `crates/bombay/tests/`. Name tests after observable behavior, for example `created_child_stays_live`. Add assertions for every new invariant and verify they fail when the corresponding production behavior is deliberately inverted.

## Commit & Pull Request Guidelines

Recent commits use short, imperative subjects with a scope-like prefix, such as `docs:` or `scaffold:`. Follow that pattern and keep each commit focused. Pull requests should explain the runtime or algebra change, link the relevant issue/design decision, identify affected invariants, and report the test, formatting, and Clippy results. Include screenshots only for rendered documentation changes where they clarify the result.
