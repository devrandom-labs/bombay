# Bombay fuzz target

`runtime_operations` drives the same public-runtime payload and generation
oracle as `tests/adversarial_oracle.rs`. Each input selects a unique payload
permutation. Concurrent producers must all recover successful delivery, the
pure behavior must observe every payload exactly once before normal exit, and
terminal publication must precede reuse of the logical address.

Run the coverage-guided campaign from the repository root:

```console
nix develop .#fuzz
cd crates/bombay/fuzz
cargo fuzz run runtime_operations
```

For a bounded reproducible smoke campaign:

```console
cd crates/bombay/fuzz
cargo fuzz run runtime_operations -- -runs=256
```

Crashes are minimized by libFuzzer and retained under
`crates/bombay/fuzz/artifacts/`.
Corpus and artifacts are operational inputs, not source-controlled runtime
state. Bombay Communication owns payload fate inside the mailbox and Bombay
Address owns claim/release linearization; this target verifies bombay's
composition across their public contracts rather than cloning their models.
