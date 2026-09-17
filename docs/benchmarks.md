# Benchmark results

This is the maintained results table for Bombay's benchmark evidence. Every
recorded number names its environment, revision, feature set, and method; a
number may appear here only when a maintained gate (a bench target in the
flake's `performance` lane, a maintained harness, or a dhat-backed test) can
reproduce it. Historical figures are kept in their own section, clearly
labeled as oracle baselines, and are never compared as current measurements.

## Environment (all current rows)

| Field | Value |
|---|---|
| Host | devrandom-labs/bombay repo sandbox (Debian 13, 8 cores, 7.8 GiB RAM) |
| Toolchain | rustc/cargo 1.96.0 (pinned by `rust-toolchain.toml`), Nix 2.35.2 dev shell |
| Revision | distillation base `ebd79cb` + W5 branch `task/w5-integration-benchmarks-memory-evidenc-ncIvZioU` |
| Dependency selection | Behavior Core/Actors/Macros pinned at `8bca837ca5d913bcdfaefbe0dec33d58bfc9ace6` (exact-revision exception); Communication 0.1.2; Criterion 0.7.0; dhat 0.3.3 |
| Profile | release (default `release` profile, no overrides) |

Reproduce any row with the commands in its Method column, run through the
pinned Nix shell (`nix develop -c ...`).

## Observe — publication, fanout, contention, allocation

Maintained gate: `crates/observe-perf` (the full-matrix harness; its module
documentation names this table as the recorded consumer of its
`METRIC name=value` lines).

Method: `nix develop -c cargo run --release -p observe-perf` — one invocation
measures the complete workload matrix.

| Metric | Value |
|---|---|
| `observations_per_second` | 13,719,893 |
| `seq_observe_first_ops_per_second` | 13,529,001 |
| `seq_pair_create_ops_per_second` | 16,046,739 |
| `seq_cancel_observer_ops_per_second` | 14,741,969 |
| `fanout_1_observations_per_second` | 7,937,023 |
| `fanout_2_observations_per_second` | 14,433,096 |
| `fanout_4_observations_per_second` | 24,332,784 |
| `fanout_8_observations_per_second` | 36,144,203 |
| `hot_path_p50_ns` | 71 |
| `hot_path_p99_ns` | 103 |
| `wait_roundtrip_p50_ns` | 560 |
| `wait_roundtrip_p99_ns` | 42,988 |
| `contention_1t_ops_per_second` | 13,819,977 |
| `contention_2t_ops_per_second` | 7,211,190 |
| `contention_4t_ops_per_second` | 7,126,888 |
| `contention_8t_ops_per_second` | 6,060,443 |
| `contention_16t_ops_per_second` | 7,053,795 |
| `alloc_bytes_per_op` | 64 |
| `alloc_blocks_per_op` | 1 |
| `affine_alloc_bytes_per_op` | 64 |
| `affine_alloc_blocks_per_op` | 1 |
| `retained_bytes_per_publisher_observer` | 64 |
| `retained_blocks_per_publisher_observer` | 1 |
| `retained_after_drop_bytes` | 0 |

Reading notes: the 16-thread contention row deliberately oversubscribes the
8-core sandbox to expose scheduling degradation; it is a scaling probe, not a
throughput claim. Retention: one 64-byte block per publisher–observer pair
while retained, zero residual bytes after the pair is dropped.

## Application spine — end-to-end local Environment

Maintained gate: `cargo bench --locked -p bombay-rs --bench application_spine`
(the flake `performance` lane). **Pending:** the distillation base's
application crate does not compile at the pinned Behavior revision (PR #314's
worker-preparation runtime and PR #316's fold residue leave 14 compile errors
in `bombay-rs`; verified on the `ebd79cb` tip). The bench
source is committed and compiles under `--benches` once the base is repaired;
no numbers are recorded here until the maintained gate runs it.

## Mailbox lanes — two-lane transport and tokio baselines

Maintained gate: `cargo bench --locked -p bombay-rs --bench mailbox_lanes`
(the flake `performance` lane). **Pending:** same base blocker as above. The
bench source is committed; no numbers are recorded here until the maintained
gate runs it.

## Application allocation and retention (dhat)

Maintained gate: `cargo test -p bombay-rs --test application_allocations --
--test-threads=1` (dhat 0.3.3 dev-dependency; warm-up window excluded by
equal-sized pre-exercise of the exact measured path). **Pending:** same base
blocker as above; the test's per-run ceilings are provisional until its first
measured run tightens them. No numbers are recorded here until the gate runs.

## Historical oracle baselines (NOT current measurements)

Recovered from the retired old-runtime bench history (`benches/mailbox.rs`,
Criterion, recorded in the `b0c212a` history; realistic ~40-byte command,
bounded(1024) channel):

| Scenario | Oracle figure |
|---|---|
| mailbox `tell` | ≈ 5.7 ns |
| send + recv round trip | ≈ 18.4 ns |

These figures belong to the retired runtime, not to the current owning
transport. They exist so the new benches can be read against the old
expectations; no current claim borrows their numbers, and the committed bench
suites label them oracle-only in their documentation as well.
