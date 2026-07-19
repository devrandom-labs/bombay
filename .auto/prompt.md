# Autoresearch: bombay-core fuzz/property invariant coverage

## Objective

Strengthen `bombay-core`'s **fuzz and property-test invariant coverage** — the
rubric's #1 testing priority ("the 4 cross-cutting categories come first":
sequence/protocol, lifecycle, defensive-boundary, linearizability) and the
documented weakness in `CLAUDE.md` / card #152: *"fuzz lanes over the wrong
surface look exactly like green lanes over the right one."*

The loop adds **substantive** invariant-covering tests (bolero `check!` fuzz
state-machines and `proptest` boundary suites) to `bombay-core` modules and the
`fuzz/` workspace, asserting the real invariants (register-once atomicity,
weak-no-pinning, dead-reads-absent, FIFO + exactly-once, capacity backpressure,
linearizability under concurrent ops).

## Metrics

- **Primary**: `invariant_tests` — count of *passing* invariant-covering tests
  across `bombay-core` (`cargo nextest`) + `fuzz/` (`cargo test`, bolero corpus
  replay on stable). Higher is better. The loop KEEPS a change only when this
  rises (a new passing invariant test was added) **and** `checks.sh` passes.
- **Secondary (regression guard)**: `mutation_score` — `cargo mutants -p
  bombay-core` caught/(caught+missed). Baseline measured at **100%** (92 viable
  caught, 0 missed, 217 unviable). Adding tests can only preserve or improve it;
  it is re-checked at finalize, not every iteration (mutants are too slow for the
  loop cadence).

## How to Run

`./.auto/measure.sh` — fast: runs `cargo nextest -p bombay-core` + `cargo test`
in `fuzz/`, emits `METRIC` lines. `.auto/checks.sh` is the correctness gate.

## Files in Scope

- `bombay-core/src/registry.rs` — register-once / weak-no-pin / dead-reads-absent
  (fuzz state-machine target is the top priority; only unit concurrency tests
  exist today).
- `bombay-core/src/mailbox.rs` — already has a bolero state-machine fuzz target
  and a `proptest`; extend boundary ranges (0/1/MAX-1/MAX, empty/max/max+1).
- `bombay-core/src/request.rs`, `reply.rs`, `error.rs`, `actor/` — add
  defensive-boundary / lifecycle property tests where thin.
- `fuzz/tests/*.rs` — new bolero `check!` targets (concurrent Registry ops,
  request/reply round-trips).
- `bombay-core/tests/*_bdd.rs`, `*_props_bdd.rs` — extend existing BDD property
  suites with boundary/linearizability cases.

## Off Limits

- `src/` (the kameo reference oracle) — never edit.
- `flake.nix`, `Cargo.lock` dependency set, `clippy.toml` / `[lints]` — no edits
  without explicit human approval.
- `mutants-baseline.json` / `mutants-gate/` — the standing CI ratchet; do not
  lower its floors.

## Constraints

- `nix flake check` is the single authoritative gate; `checks.sh` runs the fast
  scoped equivalent (fmt + clippy-as-law + nextest for `bombay-core` + compile/run
  of `fuzz/`) every iteration, and the FULL `nix flake check` is run at finalize.
- Tests must be **substantive**: assert exact invariants, must be able to fail,
  concurrent tests use real overlap (`thread::scope` + `Barrier`), proptest ranges
  include boundaries (0, 1, MAX-1, MAX; strings empty/max/max+1). No filler.
- Parsing untrusted/fuzzed input never panics — returns `Result`.
- No new crates; use existing `bolero` (fuzz/) and `proptest` (bombay-core) deps.

## Why this target (not mutation score)

The brief's recommended primary metric — mutation catch-rate — was measured
across the whole `bombay-core` package: **309 mutants, 92 viable caught, 0
missed, 217 unviable → 100% mutation score.** The CI ratchet (#181) already
enforces this floor, so a mutation-score loop has *no headroom*. The registry
perf bench (`benches/registry_vs_kameo.rs`) is already ADR-optimized (papaya
beats kameo's `Mutex<HashMap>` 1.25×–5× on reads); mailbox send is zero-alloc and
the `ActorRef` is single-allocation (ADR-0010) — no safe perf win exists either.
The genuine remaining gap is **fuzz/property breadth over bombay's actual
invariants**, exactly the #152 risk. That is what this loop attacks.

## What's Been Tried

- Full `cargo mutants -p bombay-core` sweep: **92/92 viable caught, 0 missed**
  (100%). Mutation loop exhausted — pivoted to fuzz/prop breadth.
- Inspected hot paths (`actor_ref::is_alive`, `mailbox::try_send`,
  `registry::{register,lookup}`): already allocate-last / lock-free / minimal.
  No safe perf optimization available.
- Confirmed `fuzz/` uses **bolero** (stable-Rust, corpus replay via `cargo test`
  in CI's `bombay-fuzz-replay`); deep fuzzing needs nightly `cargo-bolero`
  (not on PATH) — loop stays on the stable replay path.
