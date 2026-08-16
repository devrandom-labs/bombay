# Adversarial verification boundary

Only implemented production layers are normative here. Deleted Bombay
mailbox, address, System, and runtime-composition tests are historical evidence
in the design ledger, not current gates.

| Owner | Current adversarial surface | Executable evidence |
|---|---|---|
| Bombay Behavior/Actors 0.12.0 at `40b39b2605416e3b88427e3289c4dac4568c78e0` | Pure folds, consuming activation, named products, template state and protocols | owner unit, algebra, mutation, and documentation suites; Engine's 49 template executions |
| `bombay-engine::Driver` | Exactly-once initialization and fold, commit-before-next-input, no prefetch/re-entry/retry/rollback, terminal fusion, ownership, panic, and cancellation | `driver_law`, `driver_property`, `driver_inversions`, compile fixtures, allocation oracle, fuzz target, benchmark, and mutation run |
| `bombay::core::Incarnation` | Exact result classification, panic versus cancellation, Driver drop before terminal handoff, and one affine retirement | eight core tests, allocation oracle, structural inversion test, and mutation run |
| crate-private local environment | Initialization commitment before endpoint publication, typed wrapper-safe user injection, exact rejected-message recovery, user/control closure separation, and lease release | `core::local::tests` over real Communication 0.1.1 and Address 0.2.0 |
| Tokio launch and typed reference | No reference before activation, typed delivery, failed-activation non-publication, collision preservation, and retirement closure | `core::launch::tests` and `tests/local_launch.rs` |

Neighboring Communication, Address, Observe, and Timers retain ownership of
their primitive concurrency laws. The local slice composes Communication and
Address without copying or claiming their Loom, Miri, stress, or fuzz evidence;
Observe and Timers remain outside this slice.

## Current commands

```console
cargo test --workspace
cargo test -p bombay-engine --test compile
cargo test -p bombay-engine --test driver_inversions
cargo test -p bombay-rs --lib core::incarnation::tests
cargo test -p bombay-rs --lib core::local::tests
cargo test -p bombay-rs --test incarnation_allocation
cargo check --manifest-path crates/bombay-engine/fuzz/Cargo.toml
cargo mutants -p bombay-rs -f 'crates/bombay/src/core/*.rs' --baseline skip
```

The Driver fuzz binary accepts generated causal-turn programs and compares the
real Driver transcript with its independent model. The core mutation run must
have no viable survivor. Miri is not installed by the pinned shell and is not
claimed as executed.

Future layers must add their own real-executor race and ownership evidence when
they introduce tasks, mailboxes, address generations, or observation
publication. This document deliberately does not reserve a System object,
Tokio task shape, or synthetic lifecycle protocol for them.
