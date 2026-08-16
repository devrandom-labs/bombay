# Performance and allocation gates

Only implemented layers have current gates. Deleted System, timer,
observation, child, and coordinated-shutdown workloads are not benchmarks for
the present repository. The private local mailbox slice has correctness tests
but no performance claim yet; its first meaningful benchmark belongs with the
later external-delivery constructor.

## Driver

```console
cargo bench -p bombay-engine --bench driver
cargo test -p bombay-engine --test driver_allocation
```

`driver/init_commit_turn_commit_stop_retire` measures one complete immediate
execution: initialize, apply initialization actions, acquire and fold one
event, apply terminal actions, and retire the environment. Its environment and
payloads are concrete and statically dispatched.

The allocation oracle proves that the same complete immediate Driver execution
performs no allocation. It does not claim that a future mailbox, address table,
task, timer queue, or observation mechanism allocates nothing.

## Core Incarnation

```console
cargo test -p bombay-rs --test incarnation_allocation
```

The Incarnation allocation oracle executes one Driver through the core
terminal layer and proves Incarnation adds no allocation. Its terminal outcome
and retirement callback are concrete values.

Future ownership layers must add benchmarks only when they exist, with the
exact observable work, excluded setup, toolchain, and machine metadata stated
beside their results. Historical numbers cannot substitute for a current
executable workload.
