# ADR-0006 — Mutation gate: viable-count ratchet, not unviable ratio

**Status:** Accepted (2026-07-17) — decided under cards #165/#171

## Context

Cards #165/#171 build a standing mutation-testing gate: the `mutants-gate`
tool plus the flake's `packages.mutants` derivation run cargo-mutants over
`bombay-core` and `bombay_macros`'s `derive_msg.rs` (the vendored kameo
derives stay out of scope).

cargo-mutants mutates a function by replacing its body with
`Default::default()`. bombay's `Capacity` (rejects 0 — a nexus contract),
`ActorId`, `RunResult`, and `ActorRef` deliberately have **no `Default`** —
so most generated mutants fail to compile, are counted `Unviable`, and
cargo-mutants exits 0 regardless. Measured: `spawn.rs` is 2 viable / 29;
`WeakActorRef::with_sender` is 0 viable / 4. "Zero survivors over 4 viable"
renders identically to "zero survivors over 200" — the type discipline that
prevents bugs is exactly what blinds the tool measuring bug-catching.

Card #179 (merged) fixed the precondition this gate needs: the whole-package
sweep was exiting 3 (3 missed + 5 timeouts), so no standing gate could go
legitimately green before it. Card #148's "0 survivors anywhere in the
whole-package run" traced to an interrupted run (141 of 205 candidates)
narrated as complete.

## Options considered

- **A — unviable-ratio threshold** (e.g. fail if unviable% exceeds N).
  Rejected: `spawn.rs`'s correct 93%-unviable would either trip the
  threshold forever or force it so loose it never fires. A ratio conflates
  "the type system prevents nonsense mutants" (good) with "coverage
  collapsed" (bad) — they produce the same number.
- **B — add `Default` impls to raise viability.** Rejected outright: trades
  real safety (the deliberately-absent `Default` on `Capacity`/`ActorId`/
  `RunResult`/`ActorRef`) for a metric. The gate must not shape production
  types.
- **C — viable-count ratchet, keyed per `file::function`** *(chosen)*. Fail
  when a function's viable-mutant count drops below a committed floor. A
  floor is local to the function it protects, so `with_sender`'s honest 0
  viable never averages away into `actor_ref.rs`'s other 29 mutants — the
  exact "averaged away" blindness #168 flagged for file-level or
  whole-package metrics.

## Decision

Adopt **C**, in three parts.

1. **Viable-count ratchet, not unviable ratio.** The gate fails when a
   per-`file::function` viable-mutant count drops below its committed
   floor — never on an unviable *percentage*. Inflating viability by adding
   `Default` impls is explicitly rejected (option B).
2. **The floor is committed data (`mutants-baseline.json`), and the baseline
   must account for every function the sweep sees.** A function the sweep
   sees but that is in neither `floors` nor `known_zero_viable` is a hard
   error (`Unaccounted`); a floored function the sweep never saw is a hard
   error (`StaleFloor`); an interrupted run — fewer outcomes than candidates
   for a function — is a hard error (`MissingOutcomes`). The tool's
   `emit-baseline` mode regenerates the skeleton for human review.
   Functions that are structurally 0-viable by construction (e.g.
   `with_sender`, a pure field copy) go in `known_zero_viable` and get a
   hand-written compensating test instead of a floor.
3. **Quarantined from `nix flake check`.** cargo-mutants rebuilds and
   re-tests once per mutant — minutes-to-hours over `bombay-core`, far too
   slow for the per-push gate, the same rationale already applied to the
   MIRI and fuzz lanes. It runs as a flake *package* (`nix build .#mutants`)
   on a nightly workflow, not a per-push *check*.

### Consequences

- The gate cannot go vacuously green: a survivor, a timeout, an interrupted
  run, a per-function viability collapse, a stale floor, or an unaccounted
  function all fail it, and the "N viable / M total" ratio is always
  printed — "0 survivors" stops being a PR-body assertion.
- New testable surface must be floored before it is trusted — friction by
  design. Baseline updates are a reviewed `emit-baseline` regeneration, not
  a hand edit.
- The vendored kameo derives stay out of scope; only `derive_msg.rs` is
  added past `bombay-core`.
- Relates to #164 (a probe cargo-mutants structurally cannot provide) and
  #170 (`derive_msg.rs`'s compile-fail doctests — the derive's other
  compensating control, alongside the `known_zero_viable` hand-written
  tests here).
