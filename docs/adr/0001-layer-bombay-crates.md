# ADR 0001: Layer Bombay into Standalone Crates and Single-Technology Adapters

- **Status:** Accepted
- **Date:** 2026-11-26
- **Deciders:** Joel D'Souza

## Context

Measured on `main` (`12c7f008`), `crates/bombay` holds `71,454` `src` lines and `124,020` `tests` lines inside a single crate with a 6-way bidirectional module cycle (`application <-> remote <-> local <-> launch <-> terminal <-> entity`) and direct `zenoh*` and `axum` imports inside `application/`. As a result:

1. No layer inside `crates/bombay` can compile or run tests independently.
2. Foreign transport and HTTP SDK types leak across runtime orchestration boundaries.
3. Every small change recompiles the entire monolith, bottlenecking local verification gates.
4. Downstream repositories (`mnesis-bombay`, `selo`) need stable, standalone Bombay port contracts and conformance test suites so each repository finishes 100% independently in strict order: `bombay` -> `mnesis-bombay` -> `selo`.

## Decision

Decompose Bombay into a **5-layer acyclic workspace pyramid** where every layer compiles and tests completely on its own:

1. **Layer 0 — Pure Algebra (`bombay-behavior`)**: External owner of the pure `Behavior -> Actions` transition algebra (`sans-IO`).
2. **Layer 1 — Standalone Primitives & Causal Engine**: `bombay-behavior-actors`, `bombay-engine` (`Driver`), `bombay-address`, `bombay-communication`, `bombay-timers`, and extracted `bombay-observe`.
3. **Layer 2a — Pure Distributed & Durable Domain Crates (`bombay-domain-{auth,wire,placement,durable}`)**:
   - 100% `sans-IO` value objects, decoders, and state machines returning `#[must_use]` transition decisions.
   - Enforced by per-crate `clippy.toml` (`disallowed-types`: locks, atomics, cells, `tokio::*`; `disallowed-methods`: `Instant::now`, `SystemTime::now`, `std::fs`) and `architecture/rules/domain-is-pure.yml`.
4. **Layer 2b — Execution Kernel & Narrow Ports (`bombay-runtime`)**:
   - Concrete local `Environment`, `Entity` directory, `ApplicationTopology`, narrow static Port traits (`TransportPort`, `IdentityPort`, `PlacementPort`, `DurablePort`, `OutboxPort`, `ReminderPort`), and the deterministic `SIM` multi-host simulator.
   - Exports portable conformance suites (`bombay-durable-conformance`, `bombay-identity-conformance`, `bombay-placement-conformance`) for downstream `mnesis-bombay` and `selo`.
5. **Layer 3 — Single-Technology Adapters (`bombay-zenoh`, `bombay-axum`)**:
   - Enforced by `cargo deny check bans` (`deny.toml`): only `bombay-zenoh` may depend on `bombay-zenoh*`, and only `bombay-axum` may depend on `axum`.
   - Adapters expose explicit consuming `async fn close(self) -> Result<...>` teardown methods alongside best-effort `Drop`.
6. **Layer 4 — Public Facade & Applications (`bombay-rs`, `examples/*`, `ops/k8s/`)**:
   - Pure composition root wiring concrete adapters into `Application` and `Node` without owning domain rules.

## Consequences

- Cargo's acyclic crate graph makes upward and circular layer imports a compile error.
- Layer 2a state machines run fast zero-mock unit tests, stateful `proptest` FSM suites, and `cargo mutants` sweeps in seconds.
- `bombay` completes 100% of its PRD backlog standalone using `SIM` and reference in-memory/crash adapters before `mnesis-bombay` begins, and `mnesis-bombay` completes 100% before `selo` begins.
