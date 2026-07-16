# Card #150 — MIRI ref-model DST Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land a scheduled nightly MIRI lane that exercises bombay's ref-count-stop and drop-mid-backlog races under a real interpreter, and purge the false "loom/shuttle will cover it" premise from the board.

**Architecture:** MIRI interprets flume's *real* `std::sync` atomics, so it reaches the concurrency loom/shuttle structurally cannot (bombay owns one atomic; ADR-0003 delegates all ref-count liveness to flume). The lane runs the **existing** canonical tests — all three #150 scenarios already have them — rather than duplicating invariants. Two legs: a single-seed UB/leak sweep over the whole spine, and a `-Zmiri-many-seeds` schedule exploration scoped to the race tests. Nightly stays out of `nix flake check` (#152's sanctioned pattern); a `nix develop .#miri` devShell (already landed) makes findings reproducible locally.

**Tech Stack:** MIRI (nightly-2026-06-15, pinned), fenix/crane/Nix, tokio multi-thread runtime, flume, GitHub Actions (cesr `fuzz.yml` as template).

**Spec:** `docs/superpowers/specs/2026-07-16-150-miri-ref-model-dst-design.md`

---

## Background the engineer needs

Read the spec first. The three load-bearing facts, all measured on 2026-07-16:

1. **MIRI runs bombay's tokio+flume stack.** `queued_message_is_handled_even_if_last_ref_drops_first` → `ok, 1.67s`, zero UB, zero unsupported ops. Full `--lib`: **81/82 pass**.
2. **MIRI's virtual clock advances 5 µs per basic block** (`miri/src/clock.rs`: `NANOSECONDS_PER_BASIC_BLOCK = 5000`) — ~5000× faster than the work it times. So #148's native 5s fail-fast bounds fire **spuriously**. Proven spurious, not starvation: bound 5s→600s makes the one failing test pass in **20.0s real**.
3. **proptest trips MIRI isolation** (`proptest/src/test_runner/failure_persistence/file.rs:89` — filesystem I/O). Resolution: keep isolation **ON** (determinism is the point) and skip `prop_*`.

**Already done, do not redo:** `devShells.miri` in `flake.nix` (commit `3ebf736`), verified `rustc 1.98.0-nightly` + `miri 0.1.0`, and `nix flake check` green with it present.

**Everything runs through the flake.** Never invoke a `/nix/store` path directly. Local MIRI is:
```bash
nix develop .#miri --command bash -c 'MIRIFLAGS="-Zmiri-strict-provenance" cargo miri test -p bombay-core --lib'
```

## File Structure

| File | Responsibility |
|---|---|
| `bombay-core/src/test_support.rs` *(create)* | One canonical `terminate_bound()`. Test-only, behind the `test-support` feature. #151 reuses this seam for its counting allocator. |
| `bombay-core/src/lib.rs` *(modify)* | Declare the feature-gated module. |
| `bombay-core/Cargo.toml` *(modify)* | `test-support` feature + self dev-dep so integration tests see it. |
| `bombay-core/src/actor/spawn.rs` *(modify)* | 14 inline `from_secs(5)` → `terminate_bound()`. |
| `bombay-core/src/actor/recipient.rs:266` *(modify)* | `const DELIVERY` → `terminate_bound()`. |
| `bombay-core/tests/dst_races.rs:56` *(modify)* | `const TERMINATE` → `terminate_bound()`; extend the loom-N/A header to point at ADR-0005. |
| `bombay-core/tests/invariants.rs:78` *(modify)* | `const TERMINATE` → `terminate_bound()`. |
| `bombay-core/tests/msg_mailbox_compose.rs:13` *(modify)* | `const DELIVERY` → `terminate_bound()`. |
| `docs/adr/0005-loom-shuttle-na-miri-for-ref-model.md` *(create)* | The justified-N/A decision + evidence. |
| `docs/adr/README.md` *(modify)* | Index row. |
| `.github/workflows/miri.yml` *(create)* | The two-leg scheduled lane. |
| `docs/testing/coverage-baseline.md` *(modify)* | Lane note. |

**Why a feature, not `pub(crate)`:** the bounds live in **both** src unit tests (`spawn.rs`, `recipient.rs` — these see `pub(crate)` via `cfg(test)`) **and** integration tests (`tests/*.rs` — these link the lib externally and cannot see `pub(crate)`). CLAUDE.md forbids `#[doc(hidden)]` as access control and requires test-only items be `#[cfg(test)]` **or behind a test feature**. The root crate already uses exactly this self-dev-dep pattern (`Cargo.toml:161`: `bombay = { path = ".", features = ["testing"] }`).

---

### Task 1: The `terminate_bound()` helper behind a `test-support` feature

**Files:**
- Create: `bombay-core/src/test_support.rs`
- Modify: `bombay-core/src/lib.rs`
- Modify: `bombay-core/Cargo.toml`

- [ ] **Step 1: Add the feature and self dev-dep**

In `bombay-core/Cargo.toml`, add above `[dependencies]`:

```toml
# Test-only helpers shared by BOTH the src unit tests (via `cfg(test)`) and the
# integration tests in `tests/` (which link the lib externally and so cannot see
# `pub(crate)`). A feature, not `#[doc(hidden)]` — see CLAUDE.md API-design rules.
# Card #150; #151 extends this seam with its counting allocator.
[features]
test-support = []
```

And inside the existing `[dev-dependencies]` block, add the self-dep that turns the
feature on for the test build (the root crate does this at `Cargo.toml:161`):

```toml
bombay-core = { path = ".", features = ["test-support"] }
```

- [ ] **Step 2: Write the helper**

Create `bombay-core/src/test_support.rs`:

```rust
//! Test-only helpers shared by the unit and integration suites (card #150).
//!
//! Behind the `test-support` feature: `tests/*.rs` link the lib externally and
//! cannot reach `pub(crate)`, and `#[doc(hidden)]` is not access control.

use core::time::Duration;

/// The fail-fast bound for a "this must terminate" await (card #148): a
/// regression that hangs the loop FAILS here instead of stalling the suite.
///
/// Scaled under MIRI. MIRI's virtual clock advances **5 µs per basic block**
/// (`miri/src/clock.rs`: `NANOSECONDS_PER_BASIC_BLOCK = 5000`) — roughly 5000×
/// faster than the work it times — so a natively-calibrated bound fires
/// spuriously under the interpreter, on a test that is making fine progress.
/// Measured (#150): the 8×50-sender race needs ~20 s real under MIRI and passes
/// comfortably inside this bound, while the native 5 s fail-fast is unchanged.
#[must_use]
pub const fn terminate_bound() -> Duration {
    if cfg!(miri) {
        Duration::from_secs(600)
    } else {
        Duration::from_secs(5)
    }
}
```

- [ ] **Step 3: Declare the module**

In `bombay-core/src/lib.rs`, alongside the other `mod` declarations:

```rust
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
```

- [ ] **Step 4: Verify it compiles both ways**

```bash
nix develop --command cargo check -p bombay-core
nix develop --command cargo check -p bombay-core --features test-support
```
Expected: both succeed, no warnings.

- [ ] **Step 5: Commit**

```bash
git add bombay-core/src/test_support.rs bombay-core/src/lib.rs bombay-core/Cargo.toml
git commit -m "test(150): terminate_bound() helper behind a test-support feature

MIRI's virtual clock advances 5us per basic block (clock.rs:
NANOSECONDS_PER_BASIC_BLOCK = 5000), ~5000x faster than the work it
times, so #148's native 5s bounds fire spuriously under the
interpreter. One canonical bound, scaled under cfg(miri); native
fail-fast unchanged at 5s.

Refs #150"
```

---

### Task 2: Adopt the helper across all five files

**Files:**
- Modify: `bombay-core/src/actor/spawn.rs` (14 sites)
- Modify: `bombay-core/src/actor/recipient.rs:266`
- Modify: `bombay-core/tests/dst_races.rs:56`
- Modify: `bombay-core/tests/invariants.rs:78`
- Modify: `bombay-core/tests/msg_mailbox_compose.rs:13`

- [ ] **Step 1: Replace the named consts (3 files)**

In `dst_races.rs`, `invariants.rs`, `msg_mailbox_compose.rs` — delete the local const
and import the helper. The call sites keep their existing names by binding once:

`bombay-core/tests/dst_races.rs` — replace line 56's const with:

```rust
use bombay_core::test_support::terminate_bound;

/// The suite-wide fail-fast bound: any terminal await that exceeds this is a hung
/// loop, and the test fails here rather than stalling the whole run. Scaled under
/// MIRI — see `terminate_bound`.
const TERMINATE: Duration = terminate_bound();
```

Do the same in `invariants.rs:78` (`TERMINATE`) and `msg_mailbox_compose.rs:13`
(`DELIVERY`), keeping each file's existing const name so no call site changes.

Note: `terminate_bound()` is a `const fn`, so a `const` initialiser is legal here.

- [ ] **Step 2: Replace `recipient.rs:266`**

```rust
use crate::test_support::terminate_bound;

const DELIVERY: Duration = terminate_bound();
```

- [ ] **Step 3: Replace the 14 inline sites in `spawn.rs`**

In `spawn.rs`'s `#[cfg(test)] mod tests`, add to the existing imports:

```rust
use crate::test_support::terminate_bound;
```

Then replace every `std::time::Duration::from_secs(5)` / `core::time::Duration::from_secs(5)`
inside a `tokio::time::timeout(...)` with `terminate_bound()`. Sites (verify each is a
timeout bound, not an unrelated duration): lines 326, 372, 454, 464, 523, 599, 760, 802,
850, 904, 983, 991, 1037, 1170.

```bash
# Confirm none remain:
rg -c 'Duration::from_secs\(5\)' bombay-core/src/actor/spawn.rs
```
Expected: no matches (exit 1).

- [ ] **Step 4: Verify the native suite is unchanged**

```bash
nix develop --command cargo test -p bombay-core
```
Expected: all pass, same count as before. The native bound is still 5s.

- [ ] **Step 5: Verify the MIRI blocker is actually fixed**

```bash
nix develop .#miri --command bash -c \
  'MIRIFLAGS="-Zmiri-strict-provenance" cargo miri test -p bombay-core --lib concurrent_senders_single_writer_exact_count'
```
Expected: `test result: ok. 1 passed` (~20s real). This is the test that failed with
`Elapsed(())` before this task.

- [ ] **Step 6: Commit**

```bash
git add bombay-core/src bombay-core/tests
git commit -m "test(150): adopt terminate_bound() at every fail-fast await

18 sites across five files (14 inline in spawn.rs, four named consts).
Native 5s fail-fast unchanged; the MIRI arm unblocks
concurrent_senders_single_writer_exact_count, which previously died on
a spurious Elapsed(()).

Refs #150"
```

---

### Task 3: Prove the lane is falsifiable, then prove it green

A lane that cannot fail is worthless. #149 set this precedent ("falsifiability-verified:
the FIFO assertion fails under a `.rev()` probe, then reverted"). Do the same here.

**Files:** none committed — this is a probe-and-revert.

- [ ] **Step 1: Break the self-pin invariant deliberately**

In `bombay-core/src/mailbox.rs`, find `send_message` (the site that stamps the strong
`self_sender` into `Signal::Message` per ADR-0003). Temporarily downgrade the stamped
sender so a queued message no longer pins the actor — e.g. construct the signal with a
sender clone that is dropped immediately, or stub the send to `Ok(())` without enqueuing.

- [ ] **Step 2: Confirm MIRI's lane CATCHES it**

```bash
nix develop .#miri --command bash -c \
  'MIRIFLAGS="-Zmiri-strict-provenance" cargo miri test -p bombay-core --lib queued_message_is_handled_even_if_last_ref_drops_first'
```
Expected: **FAIL**. If it passes, the lane is not testing what we think — stop and
investigate before going further.

- [ ] **Step 3: Revert the probe**

```bash
git checkout bombay-core/src/mailbox.rs
git diff --exit-code bombay-core/src/mailbox.rs && echo "probe reverted"
```
Expected: `probe reverted`.

- [ ] **Step 4: Run Leg 1 — the full sweep, isolation ON, proptests skipped**

```bash
nix develop .#miri --command bash -c \
  'MIRIFLAGS="-Zmiri-strict-provenance" cargo miri test -p bombay-core --lib -- --skip prop_'
```
Expected: all pass, zero UB, zero leaks. Record the wall-clock — the workflow's timeout
depends on it. **If any test fails, do NOT paper over it**: MIRI has found either a real
bug (valuable — card it) or another environment mismatch (diagnose it like Blocker 2 was
diagnosed; do not guess).

- [ ] **Step 5: Record the measurement in the spec**

Append the measured Leg 1 wall-clock to the spec's Decision table, replacing the
"~11 min (659s measured, isolation off)" estimate with the real isolation-on number.

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-07-16-150-miri-ref-model-dst-design.md
git commit -m "docs(150): record measured Leg 1 wall-clock (isolation on)

Refs #150"
```

---

### Task 4: Choose N for the many-seeds leg, by measurement

**Files:** none committed (measurement feeds Task 5).

- [ ] **Step 1: Time a single race test under one seed**

```bash
nix develop .#miri --command bash -c \
  'MIRIFLAGS="-Zmiri-strict-provenance" time cargo miri test -p bombay-core --lib dropping_last_actor_ref_stops_the_actor'
```
Record the wall-clock.

- [ ] **Step 2: Time the three race tests under a small seed range**

```bash
nix develop .#miri --command bash -c \
  'MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-many-seeds=0..4" time cargo miri test -p bombay-core --lib -- \
     dropping_last_actor_ref_stops_the_actor \
     queued_message_is_handled_even_if_last_ref_drops_first \
     dropping_receiver_mid_backlog_frees_the_queued_message'
```

MIRI runs seeds "with parallel interpreter instances", so this will **not** be 4× the
single-seed time — measure, don't extrapolate.

- [ ] **Step 3: Pick N so the leg fits a ~60 min nightly budget**

Extrapolate from the 0..4 measurement on a 2-core assumption (GitHub's standard runner).
Prefer a smaller N that runs reliably every night over a large N that gets cancelled.
Write the chosen N and the measurement that justifies it into the spec.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-07-16-150-miri-ref-model-dst-design.md
git commit -m "docs(150): pick many-seeds N from measurement

Refs #150"
```

---

### Task 5: The scheduled MIRI workflow

**Files:**
- Create: `.github/workflows/miri.yml`

Template: `~/Code/devrandom/cesr/.github/workflows/fuzz.yml`. **Read it fully before
writing** — the concurrency scoping, the pinned-toolchain-via-env rationale, and the
collapsing gate job are all deliberate and all apply here.

- [ ] **Step 1: Write the workflow**

```yaml
name: miri

on:
  schedule:
    - cron: "0 4 * * *" # nightly 04:00 UTC (an hour after cesr's deep-fuzz)
  pull_request:
    branches:
      - main
  workflow_dispatch:

concurrency:
  # Scope by event so a PR push cancels only the superseded PR run, never a
  # concurrently running nightly on the same ref.
  group: miri-${{ github.event_name }}-${{ github.ref }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}

env:
  # PINNED (not floating `nightly`) so a MIRI finding reproduces on the exact
  # interpreter that produced it — these are unstable `-Z` flags with no
  # cross-version guarantee. MUST equal the date in flake.nix's `miriToolchain`.
  # Kept out of any rust-toolchain.toml on purpose: bombay builds stable-only
  # (#60) and nightly stays quarantined to this lane (#152).
  MIRI_TOOLCHAIN: nightly-2026-06-15

jobs:
  # Leg 1 — UB / data-race / leak sweep over the whole spine, single seed.
  miri-sweep:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: actions/checkout@v4

      - name: Install pinned nightly + miri
        uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ env.MIRI_TOOLCHAIN }}
          components: miri, rust-src

      - name: Sweep
        # RUSTUP_TOOLCHAIN forces this step onto the pinned nightly — the repo-root
        # rust-toolchain.toml otherwise shadows it to stable.
        #
        # Isolation stays ON: it gives a deterministic clock and scheduler, which is
        # the whole point of a race lane. The cost is that proptest cannot run
        # (it writes .proptest-regressions; filesystem I/O is what isolation
        # forbids), hence `--skip prop_`. proptest's value is input-space breadth,
        # which MIRI does not add to — that surface is #149/#152's bolero lane.
        env:
          RUSTUP_TOOLCHAIN: ${{ env.MIRI_TOOLCHAIN }}
          MIRIFLAGS: "-Zmiri-strict-provenance"
        run: cargo miri test -p bombay-core --lib -- --skip prop_

  # Leg 2 — schedule exploration, scoped to the ref-model race tests.
  miri-seeds:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4

      - name: Install pinned nightly + miri
        uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ env.MIRI_TOOLCHAIN }}
          components: miri, rust-src

      - name: Explore schedules over the ref-model races
        # Scoped to the race tests on purpose: MIRI runs seeds with parallel
        # interpreter instances, but many-seeds over the FULL suite is hours on a
        # 2-core runner. These three are the #150 scenarios (they already exist —
        # each invariant lives in one canonical location).
        # -Zmiri-many-seeds-keep-going reports what FRACTION of seeds fail, which
        # is the difference between "flaky race" and "always broken".
        env:
          RUSTUP_TOOLCHAIN: ${{ env.MIRI_TOOLCHAIN }}
          MIRIFLAGS: "-Zmiri-strict-provenance -Zmiri-many-seeds=0..<N> -Zmiri-many-seeds-keep-going"
        run: |
          cargo miri test -p bombay-core --lib -- \
            dropping_last_actor_ref_stops_the_actor \
            queued_message_is_handled_even_if_last_ref_drops_first \
            dropping_receiver_mid_backlog_frees_the_queued_message

  miri-gate:
    # Single stable check name. Requiring the leg jobs directly would mean contexts
    # that silently drift; this collapses them into one. `always()` so it still
    # reports (as a failure) when a needed job fails or is cancelled — a skipped
    # required check leaves a PR stuck at "Expected".
    name: miri-gate
    if: always()
    needs: [miri-sweep, miri-seeds]
    runs-on: ubuntu-latest
    steps:
      - name: Require every MIRI leg green
        run: |
          for result in ${{ join(needs.*.result, ' ') }}; do
            if [ "$result" != "success" ]; then
              echo "::error::a MIRI leg finished with result: $result"
              exit 1
            fi
          done
          echo "all MIRI legs green"
```

Substitute `<N>` with the value measured in Task 4.

- [ ] **Step 2: Lint the workflow**

The tracked `pre-commit` hook lints staged workflows. Ensure hooks are enabled:

```bash
git config core.hooksPath .githooks
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/miri.yml
git commit -m "ci(150): scheduled MIRI lane — UB sweep + many-seeds over the races

Two legs on pinned nightly-2026-06-15 via RUSTUP_TOOLCHAIN (cesr
fuzz.yml's nightly-quarantine pattern); never the nix flake check gate,
per #152. Isolation stays ON for a deterministic clock/scheduler;
proptest is skipped because its failure-persistence file I/O is exactly
what isolation forbids.

Refs #150"
```

- [ ] **Step 4: Watch the PR run go green**

The workflow triggers on `pull_request`. Confirm both legs pass in CI, not just locally.

---

### Task 6: ADR-0005 — loom/shuttle justified N/A

**Files:**
- Create: `docs/adr/0005-loom-shuttle-na-miri-for-ref-model.md`
- Modify: `docs/adr/README.md`
- Modify: `bombay-core/tests/dst_races.rs` (header)

- [ ] **Step 1: Write the ADR**

Follow the house format (`docs/adr/README.md`): **Status** · **Context** · **Options
considered** (with evidence) · **Decision** · **Consequences**. Status: `Accepted`.

It must carry the verified evidence — do not paraphrase it away:
- flume 0.12 ships zero loom instrumentation (whole published crate incl. hidden files;
  flume master's `Cargo.toml` features are only `spin`/`select`/`async`/`eventual-fairness`;
  the tracker's one "loom" hit is a `tokio::loom` stack trace in issue #55).
- shuttle README: "replaces the concurrency-related imports from `std` with imports from
  `shuttle`" — no auto-interception.
- madsim README: "replace them by our simulators" — tokio only.
- bombay owns ONE production atomic: `NEXT_ACTOR_ID` (`spawn.rs:42`), already #88's.
  ADR-0003 delegates all ref-count liveness to flume by design.
- The only loom option would be a `cfg(loom)` seam swapping flume for a bombay-owned
  sender-count — i.e. testing a reimplementation, which the test-quality rule bans.
- MIRI is an interpreter → executes flume's real atomics. tokio's `rt_threaded.rs`: 31
  multi-thread tests, zero `cfg(not(miri))` gates. Measured on bombay: 81/82 `--lib` pass.
- Consequence to state honestly: **MIRI samples, it does not prove.** Its weak-memory
  emulation is incomplete (per its own README, which points at loom for rigorous atomic
  work — advice that presumes you *have* atomics). A green lane is evidence, not proof.

- [ ] **Step 2: Index it**

Add to `docs/adr/README.md`'s table:

```markdown
| [0005](0005-loom-shuttle-na-miri-for-ref-model.md) | loom/shuttle N/A; MIRI for the ref-model | Accepted |
```

- [ ] **Step 3: Point the existing dst_races.rs header at the ADR**

`dst_races.rs` already carries a "# loom: justified N/A (not applied)" header written for
#116. Its reasoning (loom does not model async executor scheduling) remains correct.
Extend it — do not rewrite it — with a sentence noting that #150 re-examined loom for the
**ref-model** and reached the same verdict for an additional, stronger reason (flume owns
the atomics and ships no loom instrumentation), and cite ADR-0005.

- [ ] **Step 4: Commit**

```bash
git add docs/adr bombay-core/tests/dst_races.rs
git commit -m "docs(adr): ADR-0005 — loom/shuttle N/A, MIRI for the ref-model

Records the evidence so this is not re-litigated a fourth time.

Refs #150"
```

---

### Task 7: The correction sweep

The false premise sits in three artifacts that cite each other, so each looks
corroborated. Fix all three or it regrows.

**Files:**
- Modify: `docs/superpowers/specs/2026-07-13-149-bolero-fuzz-workspace-design.md`
- Modify (via `gh`): issues #150, #151

- [ ] **Step 1: Re-scope #150**

Use the `joeldsouzax` account (`gh auth status` must show it active; else
`gh auth switch --user joeldsouzax`). Also retitle it, since the current title names the
wrong tools:

```bash
gh issue edit 150 --repo devrandom-labs/bombay \
  --title "test(actor_ref): MIRI DST for ref-count stop & drop-mid-backlog races" \
  --body-file - <<'EOF'
Sub-task of #117. Deterministic State Testing of the bombay-owned concurrency the
ref-model introduces.

**Re-scoped 2026-07-16: MIRI, not loom/shuttle.** loom and shuttle only permute
primitives the code under test opts into (`cfg(loom)` / `shuttle::sync`). flume 0.12
ships **zero** loom instrumentation, and per ADR-0003 bombay delegates 100% of
ref-count liveness to flume — bombay owns exactly ONE production atomic
(`NEXT_ACTOR_ID`, already #88's). So loom/shuttle would explore a near-empty state
space and prove nothing. MIRI is an interpreter and executes flume's *real*
`std::sync` atomics. Evidence + full reasoning: ADR-0005 and
`docs/superpowers/specs/2026-07-16-150-miri-ref-model-dst-design.md`.

## Scope

Two legs, scheduled nightly (never the `nix flake check` gate — see #152):

- **Leg 1 — sweep.** `cargo miri test -p bombay-core --lib` over the whole spine:
  UB, data races, leaks. Isolation ON; `--skip prop_` (proptest's
  failure-persistence file I/O is what isolation forbids).
- **Leg 2 — schedules.** `-Zmiri-many-seeds` scoped to the ref-model race tests.

## Scenarios (unchanged)

All three already have canonical tests; the lane runs them rather than duplicating
invariants (test-quality rule: each invariant tested once, in one place):

- last-`ActorRef`-drop vs a racing `tell` → `spawn.rs::dropping_last_actor_ref_stops_the_actor`
- `MailboxReceiver::drop` (drain) racing an in-flight send → `mailbox.rs::dropping_receiver_mid_backlog_frees_the_queued_message`
- the "message enqueued just before the last ref drops" self-pin window → `spawn.rs::queued_message_is_handled_even_if_last_ref_drops_first`

Asserts: no lost message under ref-count stop (drain-then-stop), no double-handle, no
deadlock, no leaked `self_sender`.

## Proven

MIRI runs bombay's real tokio + flume stack: the self-pin test passes in **1.67s**
(zero UB, zero unsupported ops), and **81/82** of `bombay-core --lib` pass. The claim
that MIRI cannot drive tokio's multi-thread runtime is false — tokio's
`rt_threaded.rs` has 31 multi-thread tests with zero `cfg(not(miri))` gates.

## Caveat

MIRI **samples** schedules; its weak-memory emulation is incomplete. A green lane is
evidence, not proof.
EOF
```

- [ ] **Step 2: Correct #151**

Delete this sentence from #151's body:

> MIRI does **not** support tokio multi-thread runtime or real I/O, so it targets the
> sync layer; the async run-loop is covered by loom/shuttle (#150).

Replace with: MIRI does not support **real I/O** (confirmed: proptest's
failure-persistence file I/O trips isolation), but it **does** drive the tokio
multi-thread runtime (tokio's `rt_threaded.rs`: 31 multi-thread tests, zero MIRI gates;
81/82 of bombay-core `--lib` pass under MIRI). The async run-loop is covered by #150's
MIRI lane. Also soften "~10-100x slow": measured 1.67s for a single actor test — actor
tests are scheduling-bound, not compute-bound.

- [ ] **Step 3: Annotate #149's merged spec**

Add a dated note to `docs/superpowers/specs/2026-07-13-149-bolero-fuzz-workspace-design.md`
near the "sync surface only" rationale. It must say plainly:

- Reason 2 ("MIRI cannot drive the tokio runtime") is **false** — see the #150 spec.
- Reason 3 ("async concurrency is #150's DST (loom/shuttle) job") is **false** — loom and
  shuttle cannot see flume.
- **Reason 1 — "no runtime in the closure → fast, deterministic replay" — stands on its
  own, so the sync fuzz target remains correct and #149 is NOT reopened.**

Do not edit the historical rationale in place; append a clearly dated correction so the
record of what was believed, and when, survives.

- [ ] **Step 4: Verify no other artifact carries the claim**

```bash
rg -rn -i 'loom|shuttle' docs/ .github/ --glob '!target' | rg -iv 'adr/0005|2026-07-16-150|dst_races'
```
Review each remaining hit. #152 is expected to be clean.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-07-13-149-bolero-fuzz-workspace-design.md
git commit -m "docs(149): annotate — MIRI/tokio and loom/shuttle premises were false

Reasons 2 and 3 for the sync-only fuzz target did not survive #150's
research. Reason 1 (no runtime in the closure -> fast deterministic
replay) stands independently, so the target itself remains correct and
#149 is not reopened.

Refs #150, #149, #151"
```

---

### Task 8: Coverage note, gate, and PR

**Files:**
- Modify: `docs/testing/coverage-baseline.md`

- [ ] **Step 1: Add the lane note**

Append a short section: what the MIRI lane covers (UB / data races / leaks / schedules
over the ref-model races), the two legs, the measured wall-clocks, and the two standing
caveats — proptest is skipped under isolation, and MIRI samples rather than proves.

No README change: this is internal test infra, no public API moved
(CLAUDE.md's per-card README rule).

- [ ] **Step 2: Format**

```bash
nix develop --command cargo fmt --all
nix fmt
```

- [ ] **Step 3: The one gate**

```bash
nix flake check
```
Expected: green. **Commit before running it** — the gate is slow enough to strand an
agent mid-run.

- [ ] **Step 4: Open the PR**

```bash
git push -u origin test/150-miri-ref-model-dst
gh pr create --repo devrandom-labs/bombay --fill
```

The PR body must say **"Closes #150"** and nothing that GitHub's parser will read as
closing #117 — that parser links issue numbers regardless of negation, so never write
"advances #117" or "does not close #117". Mention the parent only as plain prose without
a `#` reference, or omit it.

- [ ] **Step 5: Merge on green**

`main` is protected (ruleset #18433270: PR + `Nix Flake Check` green). Wait for CI —
including the new `miri-gate` on the PR trigger — then merge.

---

## Notes for the engineer

- **If MIRI reports UB or a leak, that is the lane doing its job.** Do not suppress it,
  do not add `#[cfg_attr(miri, ignore)]` to make it quiet. Diagnose it — the systematic
  debugging that found Blocker 2's root cause (reading `miri/src/clock.rs` rather than
  trusting a plausible model of how a virtual clock "must" work) is the standard here. A
  plausible-but-unverified model would have "found" a deadlock in bombay that does not
  exist.
- **Do not add loom or shuttle as a dependency.** ADR-0005 exists precisely to stop that.
- **flume risk, out of scope but worth knowing:** flume self-declares "Casually
  Maintained", ADR-0001 made it load-bearing, and its CHANGELOG records two historical
  async-shutdown races. This lane is bombay's only defence there. Worth its own card;
  not this one's.
