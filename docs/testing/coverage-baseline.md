# Coverage baseline (card #85)

Reproducible via crane, with **two engines** selected by system:

```bash
nix build .#coverage -L            # system default: llvm-cov on Darwin, tarpaulin on Linux
nix build .#coverage-llvm -L       # force llvm-cov (any system) -> result/html/index.html
nix build .#coverage-tarpaulin -L  # force tarpaulin (Linux only) -> result/tarpaulin-report.html
```

- **`coverage-llvm`** (crane `cargoLlvmCov`) — works on every system, region/branch accurate,
  instrumented by the version-matched `llvm-cov`/`llvm-profdata` from the toolchain's
  `llvm-tools` component (`rust-toolchain.toml`).
- **`coverage-tarpaulin`** (crane `cargoTarpaulin`) — Linux-only opt-in. NOTE: its ptrace
  engine **hangs on this tokio-multi-threaded / async cucumber suite** (verified — the
  post-merge run wedged 40+ min in the test phase), so it is exposed for completeness only.
- **`coverage`** is **llvm-cov on every system** — the reliable engine that actually
  completes here; it is what the merge workflow and the numbers below use.

All run `cargo … test --workspace` with default features; non-gating (instrumentation
recompiles the world — too slow for the per-push gate). `remote` (libp2p) is off by default, so
it is never compiled or counted (M1 deletes it). On every **merge to `main`**, the
`coverage.yml` workflow rebuilds this and publishes the browsable HTML to GitHub Pages at
`…/bombay/coverage/` (and as the `coverage-html` artifact). The numbers below are from
`coverage-llvm`; tarpaulin's totals differ slightly (different instrumentation granularity).

## bombay-core — the M1 core rebuild (#112+)

The rebuilt spine lives in the `bombay-core` crate (part-by-part, epic #122), born
under the god-level bar (no #61 quarantine). It is measured by the same
`--workspace` coverage run and adds a **reproducible mutation gate**:

```bash
nix build .#mutants -L   # cargo-mutants on bombay-core; fails if any mutant survives
```

Pinned via the flake's `nixpkgs` (never `nix run nixpkgs#…`), mirroring the coverage
package. On-demand, not a per-push gate (rebuilds+tests once per mutant).

### `mailbox` (#112, redesigned #133) — done
Zero-box `Signal<A>` queue behind a **`flume`** channel (chosen on measured evidence —
ADR-0001; `flume` is isolated inside the sender/receiver wrappers = the seam).
Construction hangs off the `Mailbox::<A>::bounded(cap)` namespace (composable, no
free-floating `bounded()`). Pure transport: `send`/`try_send`/`recv`/`downgrade`/`drain`
— **no `close()`**; graceful shutdown is `Signal::Stop` + `drain` at the run-loop (#116).
18 tests: round-trip, backpressure hand-back, `Capacity` boundaries (proptest incl.
`0`/`MAX±1`/`usize::MAX`) + `CapacityError`, `MAX`-constant + `Display` guards, lifecycle
(send-after-drop / recv-none / drain-flush), `LinkDied` boxed-slot `size_of` guard +
monomorphic worst-case demo, weak death-watch, an 8-thread `Barrier` linearizability
test, and a single-sender FIFO proptest. **Mutation: 0 missed** (`nix build .#mutants`).
Criterion (`benches/mailbox.rs`, realistic ~40 B command): `tell` ≈ **5.7 ns**, send+recv
≈ **18.4 ns** (~40 % faster than the tokio v1 on the same bench). Channel eval:
`benches/channels.rs` (ADR-0001) — flume wins at both `u64` and `~40 B` payloads.

**DST posture — loom/shuttle deferred to #116/#120.** loom and shuttle can only
model-check code compiled against *their* primitives; the real `tokio::sync::mpsc` this
mailbox wraps is opaque to both ([loom](https://docs.rs/loom/latest/loom/) requires
"the code being tested specifically uses the loom replacement types";
[shuttle](https://github.com/awslabs/shuttle) requires replacing std primitives with
its equivalents). The mailbox delegates all synchronization to tokio (which loom-tests
its own channel internally), so a loom/shuttle test here would either explore nothing or
test a reimplementation (violates the "test the actual SUT" rule). The first
bombay-owned concurrency is the run-loop `select` over signals (#116) and the death-watch
push (#120) — that is where loom/shuttle land. The 8-thread linearizability test is the
mailbox's concurrency coverage until then.

## Baseline — 2026-06-29 (after #77)

Workspace line coverage **60.85% (5686/9345)** — but that blends the SUT with untested crates
and compile-time-only code. The honest per-area picture:

| Area | Line cov | Note |
|---|---|---|
| **kameo core `src/`** (the #77-wired modules) | **76.7%** (4098/5342) | the wired surface |
| in-tree `src/console/` | 95–98% (minus `demo.rs`, a non-SUT demo at 0%) | #76 |
| `console` crate — `tui.rs` | **93.24%** (1393/1494) | **#82** lifted it 73% → 93%: keystroke render scenarios for every `state_cell` arm, all sort keys + direction toggle, tree collapse/expand, the `+`/`-` poll-interval keys (with clamps), the full inspect-panel field blocks, and the focused-panel scroll edges |
| `console` crate — `poller.rs` | **82.35%** (154/187) | **#82** lifted it ~69.5% → 82%: the reconnect-backoff loop and Ok-poll interval pacing are now covered via injectable-time seams (`retry_until_some` / `pacing_sleep` / `drive_polls`) driven by a fake clock — no real sleeps. The residue is the thin `spawn_poller`/`connect_loop`/`poll_loop` delegating shells (real forever-thread + IO, not run in tests) |
| **`actors` crate** | (re-measure pending) | **#78 wired** the `broker` / `pubsub` / `message_bus` / `message_queue` modules to the SUT via cucumber BDD runners (was 0% / 0–971 at the #77 baseline); the next `coverage-llvm` run on merge refreshes the exact number. `pool` / `scheduler` remain unwired. |
| `macros` crate | ~4% | see "known limitation" below |

### The real gaps inside the #77-wired core (ranked)
| Line cov | File | Read |
|---|---|---|
| **25.0%** (6/24) | `src/request.rs` | tiny module (thin builder/`IntoFuture` glue); ~18 uncovered lines, low value |
| **46.5%** (463/995) | `src/actor/actor_ref.rs` | **biggest real gap** — 532 uncovered lines despite 22 wired scenarios. The many ask/tell/query overloads, `Recipient`/`ReplyRecipient` erasure variants, blocking variants, and error paths are under-exercised. Highest-value place to add scenarios. |
| 71.6% (303/423) | `src/request/tell.rs` | uncovered timeout/blocking/error branches |
| 72.1% (258/358) | `src/error.rs` | uncovered combinator/Display branches |
| 72.4% (184/254) | `src/actor/kind.rs` | run-loop branches |
| 76.0% (127/167) | `src/message.rs` | dispatch edges |

Well-covered (≥80%): `supervision` 95%, `spawn` 93%, `actor` 91%, `links` 89%, `reply` 89%,
`id` 88%, `registry` 87%, `request/ask` 81%, `mailbox` 81%.

### Cross-module integration (#87)
The #77 coverage above is per-module (one isolated `World` each). `tests/core_integration.rs`
adds 5 end-to-end scenarios over the subsystem INTERACTIONS: supervision × mailbox (no message
lost or duplicated across a restart under concurrent producer load), supervision × registry (the
registry entry stays resolvable and alive across a child restart), links × mailbox (`on_link_died`
fires for a dying peer while the watcher keeps draining its own mailbox), and the OneForAll /
RestForOne cascade restart-sets with in-flight messages preserved. These guard regressions that
only manifest in the interaction between modules — which line coverage of isolated scenarios
cannot catch.

### Known limitation — proc-macros read ~0%
The `macros` crate (`messages.rs` 0/437, the `derive_*`) runs at **compile time**, in a separate
process during the build of crates that USE the macros — runtime `llvm-cov` of the test binaries
cannot see it. Covering it needs expansion/`trybuild` tests, a distinct concern (not "write more
runtime scenarios"). Likewise `demo.rs` is a non-SUT demo entrypoint. `console/src/main.rs` and
the literal `event::read()` poll are now exercised by the **Tier-2 PTY smoke test** (#83, below);
note that `llvm-cov` still reports them near-0% because the test drives a *separate* compiled
process, whose instrumentation the test-binary coverage run does not aggregate — the guarantee is
behavioural (the binary boots, polls input, and quits cleanly), not a line-count bump.

### Tier-2 (PTY / "Selenium-for-terminals") — #83
`console/tests/pty_smoke.rs` drives the real `bombay-console --demo` binary through a
pseudo-terminal (`portable-pty`), re-emulates the visible screen from the raw PTY bytes with
`vt100`, and asserts on the rendered grid: dashboard renders → `?` opens the help popup (via the
real `event::read()` poll) → `Esc` dismisses → `/`+query echoes → `q` exits cleanly. This is the
only tier that reaches `main.rs` startup → the input poll → teardown, which are structurally
unreachable by the in-process `TestBackend` tier (#76/#82). Bounded + non-flaky: every wait polls
the grid until a specific string appears with a hard per-step timeout; no fixed sleeps.

## What this tells us
Wiring scenarios (#77) ≠ covering the code: the wired core is a healthy **77%**, but
`actor_ref.rs` at **46%** is the one wired module with a large hole, and four modules sit in the
low-70s. Gap-closing priority: **`actor_ref` scenarios first**, then the low-70s error/edge
branches. The `actors` 0% is the separate big hole (#78).
