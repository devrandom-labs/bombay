# Codebase Map

| Directory | Purpose |
|---|---|
| `crates/bombay` | Main runtime crate — `Application` composition, actor facade, entity integration, address/communication/observe/timers wiring, incarnation tasks, interpreters |
| `crates/bombay-engine` | Universal direct-Behavior Driver and affine Environment port (actor-independent causal sequence) |
| `crates/bombay-machine` | Deterministic state machines and ordered execution for Bombay |
| `crates/bombay-macros` | Proc macros — `#[bombay::actor]` facade and `TerminalProjection` derive |
| `crates/observe-tests` | Private verification harness for the observe module (loom lane) |
| `crates/observe-perf` | Private performance harness for the observe module |
| `crates/observe-fuzz` | Opt-in fuzz targets for observe operations (excluded from workspace) |
| `crates/mutants-gate` | Mutation-test baseline gate CLI (`check` / `emit-baseline`) |
| `examples/actor-templates` | Primary template-selection path — Machine role + ActorExt policy composition |
| `examples/counter` | Level-1 actor authoring — typed request/reply, lifecycle shutdown, terminal projection |
| `examples/entity` | Native Entity installation — hydration, passivation, stable EntityRef reactivation |
| `examples/supervision` | Supervisor topology and restart policy |
| `examples/worker-pool` | Worker-pool construction and capacity policy |
| `examples/application-topology` | Named application/topology declaration |
| `examples/axum` | HTTP boundary via Axum — order book root, `POST /orders`, `DELETE /admin/shutdown` on 127.0.0.1:3000 |
| `docs` | Normative design docs — driver law, module boundaries, capability interfaces, open design ledger, historical decisions |
| `.github/workflows` | CI (`nix flake check`) and release-plz release workflows |
| `.cargo` | cargo-mutants configuration |
| `.config` | cargo-nextest configuration (mutants profile) |
