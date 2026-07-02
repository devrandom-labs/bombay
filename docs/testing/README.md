# Bombay test-coverage specification (card #74)

> **Phase 1 deliverable: scenarios only, no wiring.** This tree captures *every* test
> scenario for the surviving kameo local core as executable specifications, with each
> scenario's invariant made explicit. It does **not** contain step definitions or any
> binding to the system-under-test yet. Wiring is a deliberately separate later phase
> (see *Phased plan*).

## Why Gherkin / `cucumber-rs`

We capture scenarios as [Gherkin](https://cucumber.io/docs/gherkin/) `.feature` files,
to be run later by [`cucumber-rs`](https://github.com/cucumber-rs/cucumber) (async-first,
tokio-native — a fit for an async actor framework).

The decision is driven by the phasing the work requires:

| Need | Gherkin / cucumber-rs | `rstest` / plain `#[test]` |
|---|---|---|
| Write scenarios with **zero** implementation | ✅ a `.feature` file is pure text | ❌ the scenario *is* a function body |
| Separate "what we guarantee" from "how we check it" | ✅ feature vs. step definitions | ❌ fused in one place |
| Force the invariant into the open | ✅ you cannot write a `Then` without naming the observable guarantee | ⚠️ optional |
| Survive the M7 de-handroll | ✅ scenarios are implementation-agnostic | ❌ tied to current API |
| Audit the 4 cross-cutting categories | ✅ scenario tags | ⚠️ by convention only |

**Honest trade-off:** `cucumber-rs` is a heavier dev-dependency than `rstest`, and
step-regex matching adds maintenance cost *at wiring time*. For *scenario capture* (this
phase) that cost is zero — feature files are plain text and pull in no dependency until
we wire. When we wire, individual pure helpers (e.g. the `console` `tui` formatters,
`ActorId` byte round-trips) may be cheaper to bind via `rstest` parameterisation behind a
single Gherkin `Scenario Outline`; that is a wiring-phase decision, not a scenario-phase
one.

## The 4 cross-cutting categories (tags)

Per `CLAUDE.md` rule 7, these come first and every scenario carries exactly one as a tag:

- `@sequence` — multi-step interactions on the same object (protocol order), not ops in isolation.
- `@lifecycle` — create / close / corrupt / reopen; for actors: spawn / stop / kill / panic / restart.
- `@boundary` — defensive: feed each unit inputs that violate its upstream crate's guarantees.
- `@linearizability` — concurrent readers + writers with consistency assertions (real overlap).

Supplementary tags:

- `@bug:<n>` — scenario reproduces a known/suspected defect; **must fail today** (never a green test documenting a bug).
- `@unstable-clock` / `@timing` — depends on wall-clock; needs `tokio::time` pause/advance when wired.
- `@core` / `@actors` / `@console` — owning crate.

## Layout

```
tests/features/
  core/      actor_lifecycle, actor_ref, actor_id, mailbox, message, reply,
             request_ask, request_tell, registry, links, supervision, error
  actors/    broker, pubsub, message_bus, message_queue, pool, scheduler
  console/   poller, tui, server_wire
docs/testing/
  README.md      (this file: methodology + gap report + phased plan)
  invariants.md  (the behavioural invariants every feature encodes)
```

## Verified coverage gap (2026-06, grounded in source)

Existing tests counted directly from the tree (`#[test]`/`#[tokio::test]`):

| Section | Tested today | Notes |
|---|---|---|
| core `actor_ref` | 2 | panic/deadlock only — no ask/tell/refcount |
| core `id` | 4 | eq + hash; not generate()/bytes |
| core `reply` | 3 | `ForwardedReply` ctors only |
| core `request/ask` | 8 | + `request/tell` 6 |
| core `supervision` | 29 | best-covered module |
| core `mailbox` `message` `registry` `links` `error` `actor`/`spawn`/`kind` | **0** each | |
| `actors` (all 7 modules) | **0** | broker, pubsub, message_bus, message_queue, pool, scheduler, lib |
| `console` crate (poller, tui) | **0** | |
| in-tree `src/console` + `tests/console.rs` | 6 integ. (happy path) | no error injection |

Totals reconcile with the card: **52 unit + 8 integration** in core; **0** in `actors`; **0** in the `console` crate.

### Confirmed defect (drives a `@bug` scenario)

`actors/src/message_queue.rs:707` — `Pattern::new(&binding.routing_key).unwrap()` in the
Topic `BasicPublish` arm. The `QueueBind` handler (`:591-642`) validates queue existence,
exchange existence, duplicate bindings, and the Headers `x-match` value, **but never
validates that a Topic `routing_key` is a compilable glob**. A malformed key (e.g.
`"[unclosed"`) is accepted at bind and panics the actor run-loop at publish. The
`message_queue.feature` file carries a `@bug` scenario that reproduces the panic and a
companion scenario asserting the *desired* bind-time rejection.

## Phased plan

1. **This phase — enumerate every scenario.** All `.feature` files below, tagged, with
   invariants explicit. No step definitions, no `World`, no SUT binding, no `Cargo.toml`
   change. Scenarios are the spec.
2. **Next phase — a second kind of scenarios.** Property-based / model-based scenarios
   (proptest oracles, linearizability models) layered on top of the example-based ones
   here; plus any scenarios surfaced while reviewing this set.
3. **Final phase — wire them.** Add `cucumber` as a dev-dependency, implement the `World`
   and step definitions per crate, run under `nix flake check`. Only here do scenarios
   start exercising real code.

This ordering is what unblocks re-tightening the god-level clippy bar safely: refactors
under lint pressure become safe once these scenarios are wired and green.

## Wiring (Phase 3): the World + step-definition pattern

> Card #76 bootstrapped the shared `cucumber-rs` harness on the `console` features
> (smallest blast radius — mostly pure helpers). All 148 console scenarios are green under
> `nix flake check` (tui 93+13, poller 17+4, server_wire 17+4). The pattern below is the
> reusable template the core/actors wiring cards follow. See the design doc
> [`docs/superpowers/specs/2026-06-24-console-cucumber-harness-design.md`](../superpowers/specs/2026-06-24-console-cucumber-harness-design.md)
> and the plan [`docs/superpowers/plans/2026-06-24-console-cucumber-harness.md`](../superpowers/plans/2026-06-24-console-cucumber-harness.md).

These are the hard-won facts (all verified while wiring card #76 — do not relearn them):

### 1. SUT access — a gated `pub mod testing`

Most items under test are private (`fn fmt_short`, `struct Poller`, `MAX_FRAME_BYTES`, the
registry statics). Integration tests cannot reach private items, so each crate exposes a
deliberate test-only surface:

```rust
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use crate::tui::{fmt_short, /* … the helpers under test */};
}
```

- The module that owns the items stays **private** (`mod tui;`) but the items it re-exports
  must be **`pub`, not `pub(crate)`** — `pub use` cannot re-export a `pub(crate)` item out of
  the crate. Flip internals to `pub` and rely on the private module + the `cfg`-gated
  `testing` re-export for encapsulation; normal builds never see `testing`.
- The `testing` feature **auto-activates for the crate's own test builds** via a *self
  dev-dependency*:
  ```toml
  [features]
  testing = []
  [dev-dependencies]
  kameo_console = { path = ".", features = ["testing"] }
  ```
  This is what makes `nix flake check` work: the flake runs `craneLib.cargoNextest` with
  **default features only** (no `--all-features`, no extra cargo args), so a test-only feature
  must turn itself on for test builds — a CLI flag would never reach it. (Dev-deps do not
  affect a plain non-test `cargo build`, so `testing` stays out of release builds.)
- The root `kameo` crate has a parallel `testing` feature re-exporting
  `console::testing::{snapshot, reset_for_test}` (the reset hook zeroes the process-global
  registry statics for the one absolute assertion).

### 2. The runner — standard libtest, NOT `harness = false`

```rust
#[tokio::test(flavor = "multi_thread")]
async fn tui_features() {
    TuiWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit("../tests/features/console/tui.feature", |_, _, _| true)
        .await;
}
```

- Use a plain `#[tokio::test(flavor = "multi_thread")]` runner — **NOT** `harness = false`.
  cucumber 0.23's `harness = false` + libtest-writer does **not** implement nextest's
  `--list`/`--exact` enumeration protocol (its `Cli` has no `--list`; nextest's
  `<bin> --list --format terse` aborts with exit 2). `nix flake check` runs `cargoNextest`
  across the whole workspace, so the runner must be one ordinary test function nextest can
  enumerate. (This supersedes the design doc's "primary = harness=false" — the fallback won.)
- `.with_default_cli()` stops cucumber from parsing the process argv; without it cucumber's
  clap `Cli` aborts on the libtest flags nextest injects (`--list`, `--exact`,
  `--format terse`) with exit code 2.
- `.fail_on_skipped()` + a filter-free `|_, _, _| true` run the **whole** feature file and
  turn any unwired / skipped / undefined scenario into a hard failure — no silent gaps, no
  false greens. Runners carry **no** name-prefix filter.
- **Anchor the feature path to `CARGO_MANIFEST_DIR`, never a bare relative path.** nextest
  does not guarantee the test process's cwd is the workspace root; the nix-sandbox
  `craneLib.cargoNextest` runs from a different cwd than a plain `cargo test`, so a relative
  path like `"tests/features/.../x.feature"` passes locally but fails the gate with cucumber
  `Could not read path` → `1 parsing error`. Use
  `concat!(env!("CARGO_MANIFEST_DIR"), "/tests/features/console/x.feature")` (root crate) or
  `".../../tests/..."` from a sub-crate (`CARGO_MANIFEST_DIR` is that crate's dir). This bit
  the server_wire runner specifically and is easy to miss because it only shows up under nix.
- **Keep the `.feature` catalog in the nix build source.** `flake.nix`'s `src` uses
  `craneLib.fileset.commonCargoSources`, which strips non-Rust/Cargo files — so `.feature`
  files are absent from the sandbox unless explicitly unioned in (`./tests/features`). The
  runners read them at *runtime*, so a missing catalog fails `cargoNextest` with the same
  `Could not read path` even though the path anchoring is correct. Both fixes are required.

### 3. Scenario Outlines + step regexes

- `Examples:` rows expand to scenarios via **cucumber-native** Scenario-Outline expansion;
  the `.feature` file stays the single source of truth.
- Steps use `#[given/when/then(regex = ...)]` — **NOT** `expr`. cucumber's `expr` (Cucumber
  Expressions) treats `(...)` as *optional* groups, which silently breaks any step text with
  parentheses. Always `regex`.
- Reuse one numeric/string `Then` across many helpers via a shared `World` field (e.g.
  `last_output: String`). cucumber re-`default()`s the `World` **per scenario**, so there is
  no state bleed between scenarios.

### 4. `@property` / `@model` laws

- A `@property`/`@model` scenario binds to a single step that runs an inline `proptest!`
  block. The generator must hit the boundaries named in the `# GEN:` comment (`0, 1, MAX-1,
  MAX`, empty/max strings — CLAUDE.md rule 8).
- The oracle is **independent**: it must NOT call the SUT (otherwise it tests nothing). E.g.
  `detect_deadlocks`'s `@model` law uses a functional-graph successor-chase reference written
  from scratch, asserted `≡` the SUT over random functional graphs.
- For async + process-global-state laws where `proptest!` (sync) cannot drive `async`/global
  state cleanly, a **documented bounded boundary-loop** over the GEN-named values is an
  acceptable fallback — stated explicitly in the step, never silently narrowed.

### 5. Process-global state (server_wire only)

`src/console/registry.rs` uses process-global statics (`SEQ`, `TOTAL_SPAWNED`,
`REAPED_STOPPED`). cucumber runs scenarios **concurrently by default**, which races these.
Therefore the server_wire runners:

- pin `.max_concurrent_scenarios(1)` (serialize scenarios), AND
- call `kameo::console::testing::reset_for_test()` at the start of each scenario / proptest
  case.

Scenarios assert **deltas** (strictly-increasing seq, `+1` on spawn) which hold regardless of
the starting value; the one absolute assertion (`total_stopped == 3`) relies on the reset
hook for determinism. `@linearizability` scenarios still use real overlap (`tokio::spawn` +
`Barrier`) *within* a scenario.

## The two console testing tiers

The `console` crate is tested at two tiers, because one cannot reach what the other can:

- **Tier-1 — in-process `TestBackend`** (#76/#82). Drives `App::render_once` / `App::press`
  against ratatui's `TestBackend`, asserting on the captured buffer. Fast, deterministic, and
  where the bulk of the render + `on_key` dispatch coverage lives. **Blind spot:** it bypasses
  the binary entirely — `console/src/main.rs` (arg parsing, `spawn_poller` wiring, the `--demo`
  runtime, `ratatui::run`, teardown) and the literal `event::read()` in `handle_events` are
  structurally unreachable, because `App::press` substitutes for the *dispatch*, never the real
  terminal read.
- **Tier-2 — PTY / "Selenium-for-terminals"** (#83, `console/tests/pty_smoke.rs`). Spawns the
  *compiled* `bombay-console --demo` binary on a pseudo-terminal (`portable-pty`), replays the
  raw PTY bytes through a `vt100` parser, and asserts on the re-emulated grid. This is the only
  tier that exercises startup → the input poll → clean shutdown end to end. It is inherently
  slower/flakier than Tier-1, so it is kept to **one** bounded smoke test with hard per-step
  timeouts and grid-polling waits (never fixed sleeps) — see the design doc
  [`docs/superpowers/specs/2026-07-02-pty-e2e-smoke-design.md`](../superpowers/specs/2026-07-02-pty-e2e-smoke-design.md).
  If the build sandbox ever cannot open a PTY, the documented fallback is `#[ignore]`-by-default
  + an explicit CI opt-in — never a silently-skipped green.
