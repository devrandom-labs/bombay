# Card #150 — DST of the ref-model: MIRI, not loom/shuttle

**Status:** Proposed (2026-07-16)
**Card:** [#150](https://github.com/devrandom-labs/bombay/issues/150) · sub-task of #117
**Corrects:** #150, #151, and the merged #149 spec (see "The correction sweep")

## Summary

#150 asked for **loom + shuttle** DST of the ref-count-stop / drop-mid-backlog races.
Neither tool can see the code in question. **MIRI can, and does.** This spec re-scopes
the card to a scheduled nightly MIRI lane, keeps every scenario the card named, and
repairs a false premise that has propagated into three artifacts.

## Context — why loom and shuttle cannot work here

loom and shuttle only permute primitives the code under test **opts into**. Verified
against primary sources, not lore:

1. **flume 0.12 ships zero loom instrumentation.** Case-insensitive search over every
   file of the published crate (including hidden/gitignored) → no match. flume master's
   `Cargo.toml` features are exactly `spin` / `select` / `async` / `eventual-fairness`.
   The flume issue tracker has never carried a loom request — its one "loom" hit
   (issue #55) is a pasted stack trace containing `tokio::loom`, unrelated.
2. **shuttle** (`awslabs/shuttle` README): "replaces the concurrency-related imports
   from `std` with imports from `shuttle`". It does **not** auto-intercept `std::sync`.
3. **madsim** README: "replace them by our simulators" — wraps tokio only; flume
   untouched.

And bombay has almost nothing of its own for them to model:

> **bombay owns exactly ONE atomic in production code**: `NEXT_ACTOR_ID`
> (`spawn.rs:42`, a `Relaxed` counter) — already carded as #88. Every other atomic in
> `bombay-core` is a test spy.

That is by design, not accident. **ADR-0003** deliberately delegated 100% of ref-count
liveness to flume: *"the sender-count is itself a zero-cost async 'last handle dropped'
signal."* The consequence: a loom or shuttle model of bombay's ref-model would explore
a near-empty state space, pass, and prove nothing.

The only honest loom option would be a `cfg(loom)` seam swapping flume for a
bombay-owned, loom-visible sender-count — i.e. **testing a reimplementation instead of
the SUT**, which the test-quality rule bans outright.

## Why MIRI reaches what loom cannot

MIRI is an **interpreter**. Opting in is meaningless to it: it executes flume's real
`std::sync::Mutex` and real atomics as the machine below the code. loom trades reach
for exhaustiveness; MIRI trades exhaustiveness (it *samples* schedules via
`-Zmiri-many-seeds`, and its weak-memory emulation is admittedly incomplete) for total
reach — **including third-party crates**.

For a crate that owns one atomic and delegates the rest, reach is everything and
exhaustiveness is worthless.

### The false premise being purged

> "MIRI does **not** support tokio multi-thread runtime or real I/O, so it targets the
> sync layer; the async run-loop is covered by loom/shuttle (#150)." — card #151

- **"real I/O" — TRUE.** Confirmed the hard way (see "Blocker 1").
- **"tokio multi-thread runtime" — FALSE.** tokio's CI runs
  `cargo miri nextest run --features full --lib` *and* `--test '*'`.
  `tokio/tests/rt_threaded.rs` holds **31 multi-thread-runtime tests with zero
  `cfg(not(miri))` gates**. Every MIRI exclusion in tokio is I/O, signals, subprocess,
  sockets, or *time* — **never the scheduler**.
- **"covered by loom/shuttle" — FALSE**, per the section above.

**Proven empirically on this repo (2026-07-16):**

| Run | Result |
|---|---|
| `queued_message_is_handled_even_if_last_ref_drops_first` (the ADR-0003 self-pin scenario, real tokio + real flume) | `ok. 1 passed` in **1.67s**, zero unsupported ops, zero UB |
| Full `bombay-core --lib` (82 tests, incl. **11 `multi_thread`**), isolation off | **81 passed, 1 failed**, 659s — the one failure being Blocker 2, not a bug |

## Blockers found by measurement (both resolved)

### Blocker 1 — proptest's filesystem I/O trips MIRI isolation

`cargo miri test` with default isolation aborts in
`proptest-1.11.0/src/test_runner/failure_persistence/file.rs:89` — proptest writing its
`.proptest-regressions` file. Filesystem access is exactly what isolation forbids.

**Resolution: keep isolation ON and skip `prop_*` under MIRI.** Rationale: isolation-on
gives a *deterministic* clock and scheduler, which is the entire point of a
race-hunting lane; and proptest's value is input-space breadth, which MIRI does not add
to (it would only multiply 256 cases by interpreter slowness). #151's instinct — "keep
it to unit/sync tests, not big proptest" — was right, for the wrong reason (I/O, not
slowness).

### Blocker 2 — the #148 fail-fast bounds vs MIRI's virtual clock

`concurrent_senders_single_writer_exact_count` (8 senders × 50 msgs, cap-4 backpressured
mailbox, `multi_thread`/4 workers) fails under MIRI with `Elapsed(())` on its 5s
`tokio::time::timeout` — under **both** isolation settings, for **two different reasons**:

| Config | Clock | Why 5s is exceeded |
|---|---|---|
| Isolation **on** | Virtual — `miri/src/clock.rs`: `NANOSECONDS_PER_BASIC_BLOCK = 5000` | Virtual time advances **5 µs per basic block**, ~5000× faster than the work it times. A 5s deadline arrives after only ~1M basic blocks. |
| Isolation **off** | Host (real) | MIRI's 10–100× interpretation slowdown on a natively-millisecond test. |

MIRI's own comment on the constant: *"This number is pretty random."*

**This is spurious, not starvation — proven.** Changing only the bound (5s → 600s),
isolation on: `ok. 1 passed ... finished in 26.73s` (20.0s real). The actor completes
all 400 messages; the bound was simply calibrated for native hardware.

> **Note the trap avoided:** a plausible model — "a virtual clock only advances when all
> threads park, so a firing timer implies deadlock" — is wrong. MIRI's clock ticks on
> *every basic block*, unconditionally. Trusting the plausible model would have meant
> hunting a deadlock in bombay that does not exist.

**Resolution:** the fail-fast bounds become MIRI-aware. This is a real trade-off and is
the one **open decision** below.

## Decision

Re-scope #150 to a **scheduled nightly MIRI lane**, in two legs:

| Leg | Command | Purpose | Measured cost |
|---|---|---|---|
| **1 — sweep** | `cargo miri test -p bombay-core --lib` (single seed, isolation on, `--skip prop_`) | UB, data races, leaks across the whole spine | **42 s real** (79 passed / 0 failed / 3 filtered, measured as specified; the earlier 659 s figure was isolation-off *with* proptests) |
| **2 — schedules** | `-Zmiri-many-seeds=0..N` scoped to the **ref-model race tests only** | The #150 interleavings | N/cores × per-test |

Leg 2 is deliberately **not** the full suite: MIRI runs seeds "with parallel interpreter
instances", but 64 seeds × a 659s suite is ~2–5 h on a 2–4-core runner. Scoping the seed
sweep to the race tests is what makes it viable.

**MIRIFLAGS:** `-Zmiri-strict-provenance` (matches tokio's CI and mnesis's
"zero UB under strict provenance" bar). Isolation stays **on**.

### Scenarios (unchanged from the card)

1. last-`ActorRef`-drop vs a racing `tell`
2. `MailboxReceiver::drop` (drain) racing an in-flight send
3. the "message enqueued just before the last ref drops" self-pin window

Asserting: no lost message under ref-count stop (drain-then-stop), no double-handle, no
deadlock, no leaked `self_sender`.

Scenarios 1 and 3 **already exist** as `dropping_last_actor_ref_stops_the_actor` and
`queued_message_is_handled_even_if_last_ref_drops_first`. Per the test-quality rule
(*each invariant tested once in a canonical location*), the lane **runs the existing
tests** — the mnesis pattern #149 cited — rather than duplicating them. Only genuine
gaps get new tests.

### Toolchain — no gate change

Nightly for MIRI is **already sanctioned**: #152 records *"Nightly is acceptable for
fuzz/MIRI (per maintainer) as long as it lives in this scheduled lane, never the
`nix flake check` gate."*

- **CI** — `.github/workflows/miri.yml`, mirroring `cesr/.github/workflows/fuzz.yml`:
  pinned `dtolnay/rust-toolchain@master` + `miri` component, `RUSTUP_TOOLCHAIN` env to
  shadow the stable `rust-toolchain.toml`. Nightly quarantined to CI.
- **Local** — `nix develop .#miri` (fenix `toolchainOf` nightly-2026-06-15 + `miri`),
  a **devShell only**, absent from `checks`. Verified working: `rustc 1.98.0-nightly`,
  `miri 0.1.0`, aarch64-darwin. Card #60's stable-only build promise is untouched, and
  a finding reproduces locally instead of push-and-pray.
- Pinned nightly date is duplicated in flake + workflow; keep them equal on bump.

## The correction sweep

The false premise is baked into three artifacts that cite each other, so each looks
corroborated:

| Artifact | Fix |
|---|---|
| **#150** | Drop loom/shuttle; re-scope to MIRI per this spec. |
| **#151** | Delete "MIRI does **not** support tokio multi-thread runtime ... the async run-loop is covered by loom/shuttle (#150)". Keep the real-I/O half. Soften "~10-100x slow" — measured 1.67s for a single actor test; actor tests are scheduling-bound, not compute-bound. |
| **#149 spec** (merged) | Note that reason 2 ("MIRI cannot drive the tokio runtime") and reason 3 ("#150's DST (loom/shuttle) job") did not survive. **Reason 1 — "no runtime in the closure → fast, deterministic replay" — stands independently, so the sync fuzz target remains correct.** Not reopening #149. |

**#152 is clean** — it only ever described the fuzz lane.

**ADR-0005 — "loom/shuttle justified N/A for the ref-model"** records the evidence above
so this is not re-litigated a fourth time. It mirrors the existing `dst_races.rs`
loom-N/A header written for #116.

## Resolved — the bounds scale under `cfg(miri)` (option a)

Blocker 2 must be fixed for any of this to run. **Decision (2026-07-16): scale the
bound.** A shared helper returns 5s natively and ~600s under `cfg(miri)`:

```rust
/// Fail-fast bound for "this must terminate" awaits (card #148). MIRI's virtual
/// clock advances 5 µs per BASIC BLOCK (`miri/src/clock.rs`:
/// `NANOSECONDS_PER_BASIC_BLOCK = 5000`) — ~5000× faster than the work it times —
/// so a natively-calibrated bound fires spuriously under the interpreter.
pub(crate) const fn terminate_bound() -> Duration {
    if cfg!(miri) { Duration::from_secs(600) } else { Duration::from_secs(5) }
}
```

**Why, over the alternatives:**

- It keeps the race at **full strength** (8 × 50 stays intact). The rejected
  alternative — `#[cfg(miri)]` lower iteration counts, which is tokio's own documented
  pattern (`tcp_echo.rs`: *"Use a lower iteration count with Miri because it's too slow
  otherwise"*) — would shrink 8×50 to ~2×5. A weaker race in a lane whose entire
  purpose is finding interleavings is self-defeating.
- It preserves #148's **native** fail-fast exactly: 5s on the stable gate, unchanged.
- It is **measured**, not theorised: bound 5s → 600s makes the failing test pass in
  20.0s real.

Cost accepted: under MIRI a genuinely hung loop takes 600s to fail rather than 5s —
confined to the nightly lane, where wall-clock is cheap and a false "no hang" verdict
would not be.

`#[cfg_attr(miri, ignore)]` (tokio's approach for its slow tests) was rejected outright:
it forfeits the single most valuable test in the lane.

**Scope:** the helper replaces the inline `Duration::from_secs(5)` at
`spawn.rs:1170` and the `TERMINATE` const in `dst_races.rs`, plus the equivalent bounds
in `invariants.rs`, `msg_mailbox_compose.rs`, and `recipient.rs` (the #148 hardening
touched all of them). One canonical definition, not a per-file copy.

## Scope of "done"

- [ ] `terminate_bound()` helper adopted across the five test files; `nix flake check` still green on stable.
- [x] `devShells.miri` in `flake.nix` — **done, verified**: not in `checks`, and `nix flake check` green (nixfmt/typos/deadnix) with it present.
- [ ] `.github/workflows/miri.yml` — scheduled + `workflow_dispatch`, two legs, pinned nightly, `miri-gate` collapsing job (cesr's `fuzz-gate` pattern).
- [ ] Leg 1 green: full `--lib` under MIRI, isolation on, `--skip prop_`.
- [ ] Leg 2 green: `-Zmiri-many-seeds` over the ref-model race tests; N chosen **by measurement**.
- [ ] Any genuine scenario gap covered by a new test (falsifiability-verified: it must fail when the invariant breaks).
- [ ] ADR-0005 written; #150/#151 corrected; #149 spec annotated.
- [ ] `docs/testing/coverage-baseline.md` note. No README change (internal test infra).

## Non-goals

- loom/shuttle/madsim — justified N/A (ADR-0005).
- Counting allocator + exact-memory leak assertions — **#151**.
- Nightly sancov/libFuzzer/AFL deep-fuzz — **#152**.
- The `NEXT_ACTOR_ID` loom model — **#88**.
- Re-opening #149's sync fuzz target — its reason 1 stands.

## Risks

- **flume is "Casually Maintained"** (self-declared badge on its README). ADR-0001 chose
  flume on measured evidence and bombay delegates all ref-count liveness to it; its
  CHANGELOG records two historical shutdown races ("Fixed a rare race condition in the
  async implementation"; "Shutdown-related race condition with async recv"). This lane
  interprets flume's real atomics, so it is bombay's only defence there. Out of scope
  for #150 — flagged as worth its own card.
- **MIRI samples, not proves** — its weak-memory emulation is incomplete; a green lane is
  evidence, not a proof of correctness. Claim it as such.
- **Pinned-nightly drift** between flake and workflow.
