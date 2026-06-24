# Console cucumber harness — design (card #76)

> Phase 3 of epic #74: wire the spec to the SUT. Card #76 bootstraps the shared
> `cucumber-rs` harness using the console crate (mostly pure helpers = smallest blast
> radius) and wires every non-`@bug` console scenario green under `nix flake check`.
> The pattern established here is reused by the core/actors wiring cards.

## Goal & scope

Wire the already-authored, spec-only `.feature` files under `tests/features/console/`
to the real system-under-test:

- `tui.feature` + `tui.properties.feature` — pure rendering/format/graph helpers in `console/src/tui.rs`.
- `poller.feature` + `poller.properties.feature` — the TCP snapshot poller in `console/src/poller.rs`.
- `server_wire.feature` + `server_wire.properties.feature` — the in-tree console server +
  registry + wire in the **root `kameo` crate** (`src/console/{server,registry,wire}.rs`).

**Done when:** all non-`@bug` console scenarios are GREEN under `nix flake check`; `cargo
hakari generate` is clean; README updated; the World + step-definition pattern is documented
in `docs/testing/README.md` and reusable.

No console scenario is tagged `@bug`, so the whole console set must go green.

## Key facts that shape the design (grounded in source)

- The console features span **two crates**: `tui`/`poller` test `kameo_console`
  (`console/`); `server_wire` tests the root `kameo` crate (`src/console/`). There is already
  precedent for the latter at root: `tests/console.rs` holds 6 happy-path tests of the same
  server.
- `nix flake check` runs `craneLib.cargoNextest` with **default features only** (flake.nix:100)
  — no `--all-features`, no extra cargo args. Any test-only feature must therefore
  auto-activate for test builds, not depend on a CLI flag.
- All 18 `tui.rs` helpers under test are **private `fn`s** (e.g. `tui.rs:1504 fn fmt_short`);
  `Poller`, its `poll`/`connect`, and `MAX_FRAME_BYTES` are private (`poller.rs`). Integration
  tests cannot reach private items, so a deliberate test-only access surface is required.
- `src/console/registry.rs` uses process-global `static`s `SEQ`, `REAPED_STOPPED`,
  `TOTAL_SPAWNED` (registry.rs:32-34). Scenarios sharing one process must assert **deltas**;
  the one absolute assertion (`total_stopped == 3`, server_wire.feature:194) needs isolation.
- `snapshot(grave_window: Duration)` is already `pub(crate)` and takes the TTL as a parameter
  (registry.rs:422), so the grave-window `> ttl` boundary (registry.rs:470) is drivable from a
  test without manipulating the live server's clock. `serve`/`Console`/`ConsoleHandle`/`wire`
  are already `pub`.

## Decisions (locked with the user)

1. **SUT access = a `testing` feature surface.** Per CLAUDE.md rule 4 ("test-only methods are
   `#[cfg(feature = "testing")]`"). Internals flip to `pub(crate)` and are re-exported only
   through a gated `pub mod testing`. Normal builds never see them.
2. **Runner location = split by SUT owner.** `tui`/`poller` runners live in `console/tests/`;
   `server_wire` runner lives in root `tests/` next to `tests/console.rs`.
3. **Feature files stay unified at root** (`tests/features/...`); runners reference them by
   relative path. Preserves the Phase-1 single-catalog layout.
4. **Scenario Outlines bind via cucumber-native expansion** — each `Examples:` row becomes a
   scenario; steps parse the `<placeholder>` values. The `.feature` file stays the single
   source of truth. `rstest` is still added as a dev-dep (card requirement), used only where a
   non-Gherkin fixture table is genuinely cleaner.

## Architecture

### Layout

```
tests/features/console/*.feature          unchanged (spec catalog)

console/tests/                             NEW — kameo_console SUT
  tui_bdd.rs                               runner target for tui.feature (+ properties)
  poller_bdd.rs                            runner target for poller.feature (+ properties)
  steps/{tui,poller}.rs                    step definitions + World
tests/                                     root kameo SUT
  console_wire_bdd.rs                      runner + World + steps for server_wire (+ properties)
  console.rs                               unchanged (6 happy-path tests)

docs/testing/README.md                     document the World + step pattern
```

### Test-only access surfaces

`console/src/lib.rs`:

```rust
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use crate::tui::{ /* all 18 helpers under test */ };
    pub use crate::poller::{Poller, MAX_FRAME_BYTES /* + decode entry-point */};
}
```

- `console/Cargo.toml`: `[features] testing = []`; the feature auto-activates for test builds
  via a self dev-dependency: `[dev-dependencies] kameo_console = { path = ".", features = ["testing"] }`.
- Root `kameo` crate: a parallel `testing` feature re-exporting `console::registry::snapshot`
  and a `#[cfg(feature = "testing")]` registry **reset** helper that zeroes the global statics,
  used to make the one absolute `total_stopped` assertion isolated and deterministic.

### Runner & nextest integration

- **Primary:** `harness = false` test targets using cucumber's `writer::Libtest::or_basic()`
  (cucumber `libtest` feature). Each scenario becomes a separate nextest test → per-scenario
  pass/fail visibility, and nextest's process-per-test isolation gives each scenario fresh
  global statics (removing the SEQ-sharing hazard).
- **Fallback** (if the Libtest/nextest bridge is finicky): one `#[tokio::test]` per feature
  file that runs cucumber and asserts zero failures — fully libtest-native, coarser
  granularity; server_wire then relies on the reset hook for isolation.
- Spike the primary path on `tui` first; fall back only if it fights us.

### Worlds & steps

One `World` per runner (cucumber requires a single World type per runner), each holding only
that feature's scratch state:

- `TuiWorld` — last input/output of the helper under test (pure functions → trivial state).
- `PollerWorld` — a loopback `TcpListener` + client `TcpStream` pair; the server side is
  scripted byte-for-byte to exercise framing / size cap / truncation; holds the decode result.
- `WireWorld` — a real `Console` started via `serve`, real spawned actors, the `ConsoleHandle`
  and one or more client connections.

### Property / model scenarios

`@property`/`@model` scenarios have prose Given/When/Then (no Examples). Each binds to a step
that runs an inline `proptest!` block using the generator from the `# GEN:` comment and the
oracle from `# ORACLE:`. `detect_deadlocks` (`@model`) uses the prescribed **functional-graph
successor-chase** reference oracle (each node ≤1 out-edge; follow successors, a revisited node
on the walk forms a cycle), normalized to min-id and sorted by first id, asserted ≡ SUT over
random functional graphs.

## Error handling & isolation

- Server-global statics → assert deltas; the single absolute assertion uses the reset hook.
- `poller` truncation / cap / malformed-msgpack scenarios assert the specific
  `io::ErrorKind::InvalidData` / `UnexpectedEof`, never just "an error" (CLAUDE.md rule 8).
- Concurrency scenarios (`@linearizability`) use real overlap (`tokio::spawn` + `Barrier`),
  not sequential-then-check.
- Timing scenarios (`@timing`) drive `snapshot(ttl)` directly rather than sleeping where
  possible; where a real wait is unavoidable it is bounded and documented.

## Build / CI changes

- `[workspace.dependencies]`: add `cucumber = "0.23"`, `rstest = "0.26"`, `proptest = "1.11"`.
- Consuming crates add them under `[dev-dependencies] … workspace = true` plus the `[[test]]`
  targets (`harness = false`) and the self dev-dependency that enables `testing`.
- `cargo hakari generate` after the dependency change.
- README updated each commit (pre-commit hook enforces a staged README change).

## Sequencing

Smallest blast radius first, each its own commit:

1. **tui** — pure helpers; bootstraps the harness, the `testing` feature, the runner pattern.
2. **poller** — TCP framing against a scripted loopback peer.
3. **server_wire** — live server + registry; needs the root-crate `testing` surface + reset hook.

## Risks

- **cucumber `libtest` ↔ nextest bridge** may need tuning; mitigated by the tokio-test fallback.
- **server_wire concurrency/timing** is the hardest part; mitigated by driving `snapshot(ttl)`
  directly and the registry reset hook. Any scenario that cannot be made deterministic will be
  surfaced explicitly (never silently skipped or left as a green test documenting a gap).
- **Self dev-dependency feature trick** must not leak `testing` into normal `cargo build`
  (dev-deps don't affect non-test builds — verified by the cargo feature model).
```
