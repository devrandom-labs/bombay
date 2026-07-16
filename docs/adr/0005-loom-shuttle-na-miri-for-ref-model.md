# ADR-0005 — loom/shuttle justified N/A; MIRI covers the ref-model concurrency

**Status:** Accepted (2026-07-16) — decided under card #150

## Context

Card #150 asked whether the `ActorRef` ref-model (ADR-0003: strong/weak
self-reference, ref-count-driven stop) needs a loom or shuttle model on top of
the deterministic-interleaving suite in `bombay-core/tests/dst_races.rs`
(#116). `dst_races.rs` already carries a "loom: justified N/A" header for the
run-loop's single Relaxed counter; #150 re-examined the question specifically
for the ref-model's liveness mechanism, which is a different piece of state
than that counter.

### Verified facts (primary source, not lore)

1. loom explores interleavings only of code compiled against `loom::sync`
   under `cfg(loom)`; shuttle likewise requires `shuttle::sync` imports
   (awslabs/shuttle README: *"replaces the concurrency-related imports from
   `std` with imports from `shuttle`"*); madsim wraps tokio only (*"replace
   them by our simulators"*). None intercepts an uninstrumented dependency's
   `std::sync`.
2. flume 0.12 ships ZERO loom instrumentation: a case-insensitive search over
   every file of the published crate (including hidden/gitignored) has no
   match; flume master `Cargo.toml` features are exactly
   `spin`/`select`/`async`/`eventual-fairness`; the flume tracker's single
   "loom" hit (issue #55) is a pasted stack trace containing tokio's internal
   `tokio::loom` shim — unrelated.
3. `bombay-core` owns exactly ONE production atomic: `NEXT_ACTOR_ID`
   (`bombay-core/src/actor/spawn.rs:42`, `Relaxed` counter, already card
   #88's). ADR-0003 deliberately delegates 100% of ref-count liveness to
   flume's `sender_count` (*"the sender-count is itself a zero-cost async
   'last handle dropped' signal"*).
4. Therefore a loom/shuttle model of the ref-model would explore a
   near-empty state space and pass while proving nothing.
5. MIRI is an interpreter: it executes flume's real `std::sync::Mutex`/atomics
   with no instrumentation required. The claim "MIRI does not support tokio's
   multi-thread runtime" is false: tokio CI runs
   `cargo miri nextest run --features full --lib` and `--test '*'`;
   `tokio/tests/rt_threaded.rs` has 31 multi-thread tests with zero
   `cfg(not(miri))` gates; every tokio MIRI exclusion is I/O/signals/
   subprocess/sockets/time, never the scheduler.
6. Measured on bombay (2026-07-16): the ADR-0003 self-pin test passes under
   MIRI in 1.67 s (zero UB, zero unsupported ops, `-Zmiri-strict-provenance`,
   isolation on); 81/82 of `bombay-core --lib` pass (the 1 failure was a
   spurious #148 5 s bound vs MIRI's virtual clock —
   `miri/src/clock.rs`: `NANOSECONDS_PER_BASIC_BLOCK = 5000`, i.e.
   5 µs/basic-block; fixed by `terminate_bound()`); a falsifiability probe
   (message-vanishing stub in `send_message`) makes the self-pin test FAIL in
   1.81 s, then reverted.

## Options considered

- **A — loom/shuttle as carded.** Structurally blocked (facts 1-3): neither
  tool can see into flume's real `std::sync` without instrumentation flume
  does not ship.
- **B — `cfg(loom)` seam swapping flume for a bombay-owned loom-visible
  sender-count.** Tests a reimplementation, not the SUT (test-quality rule
  bans it); also adds a production cfg seam for a model that still wouldn't
  cover flume.
- **C — vendor/fork flume and instrument it upstream-style.** Honest but
  expensive; changes ADR-0001's calculus; flume is "Casually Maintained"
  (self-declared badge) with two historical async-shutdown race fixes in its
  CHANGELOG — a real risk worth its own card, not this one.
- **D — MIRI lane** *(chosen)*. Total reach including flume, samples
  schedules via `-Zmiri-many-seeds`.

## Decision

Adopt **D**. loom/shuttle stay N/A for the ref-model, for a stronger reason
than #116's: the liveness mechanism they'd need to see through is flume's
`sender_count`, and flume ships no loom/shuttle instrumentation to see
through. MIRI covers it instead — it interprets flume's actual atomics
directly, no seam required — in the scheduled two-leg `miri.yml` lane (#150):
a full-spine UB/leak sweep plus `-Zmiri-many-seeds` schedule exploration
scoped to the ref-model race tests.

### Consequences

- MIRI **samples**, it does not prove; its weak-memory emulation is
  incomplete (its own README recommends loom for rigorous atomic work —
  advice that presumes you have atomics to instrument, which loom cannot do
  for an uninstrumented flume). A green `miri.yml` run is evidence, not
  proof.
- The `test-support` feature and `terminate_bound()` exist because of this
  decision: MIRI's virtual clock (5 µs/basic-block) makes natively-calibrated
  fail-fast bounds fire spuriously under the interpreter, so every
  "must-terminate" await in the DST suite is scaled per-`cfg(miri)`.
- Triggered the #150/#151/#149-spec correction sweep: prior text describing
  MIRI as incompatible with tokio's multi-thread runtime, or loom as covering
  the ref-model, is corrected against facts 1-5 above.
- If flume ever ships loom instrumentation (or bombay forks it, option C),
  this decision should be revisited — it is not a permanent rejection of
  loom/shuttle in general, only of applying them to an uninstrumented
  dependency.
