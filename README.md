# Bombay

Bombay is a runtime for closed, deterministic Bombay Behaviors. Applications
describe actors, topology, routing, supervision, timers, and shutdown through
Behavior and Behavior Actors values. Bombay supplies the local execution
mechanism.

The intended functional boundary is:

```rust,ignore
use bombay::prelude::*;

fn main() -> Result<(), RunError> {
    bombay::run(IoTSystem::new())
}
```

Or use the thin entry attribute over exactly the same path:

```rust,ignore
#[bombay::main]
fn main() {
    IoTSystem::new()
}
```

The functional path remains independently usable and authoritative.

## Architecture

```text
Behavior + bombay-engine::Driver
  inside Environment / ActiveEnvironment
    + Bombay Communication mailbox
    + Bombay Address lease
    + Bombay Observe activation and termination facts
    + actor-owned Bombay Timers queue
    + typed capability-lane interpreters
    + Bombay-owned task hierarchy
```

Behavior decides; runtime capabilities perform; capability results return as
later typed events. Users do not construct a runtime, guardian, Driver,
mailbox, address space, timer queue, observation subject, incarnation, task,
or interpreter.

The standard local runtime uses Address, Communication, Observe, and Timers
directly. Bombay does not wrap them in a second registry, namespace, mailbox,
or timer service. Extension adapters plug into typed effect lanes. The Engine
`Environment<B>` boundary permits complete alternative hosts for testing,
embedded execution, or another executor.

## Current status

The direct Driver and lower lifecycle layers exist. Communication 0.1.2's
affine mailbox admission owner, Observe 0.1.1 pairs, and actor-owned TimerQueue
are integrated. The redundant runtime-stop channel, activation channels,
keyed termination cell, timer task, and timer command channel are gone.

The exact blockers, versions, and implementation order are recorded in
[`docs/open-design-ledger.md`](docs/open-design-ledger.md). Do not treat the
current partially migrated application runtime as a stable public API.

## Documentation

- [User-facing API](docs/user-facing-api.md)
- [Runtime capability interfaces](docs/runtime-capability-interfaces.md)
- [Module boundaries](docs/module-boundaries.md)
- [Driver law](docs/driver-law.md)
- [Driver verification strategy](docs/driver-test-strategy.md)
- [Open design ledger](docs/open-design-ledger.md)
- [Historical decisions](docs/historical-design-decisions.md)

## Development

```console
cargo build --workspace
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

The toolchain is pinned in `rust-toolchain.toml`; `nix develop` provides the
repository development shell.
