# bombay

**Bombay** — a Zenoh-native fork of the [kameo](https://github.com/tqwewe/kameo) actor framework: event-sourced, identity-bearing, dataspace-native actors. It replaces kameo's libp2p `remote` layer with a thin layer over a Zenoh `Session`, and adds an adapter that maps [nexus](https://github.com/devrandom-labs/nexus) event-sourced aggregates onto actors (actor = single-writer consistency boundary).

## Bombay + Zenoh + nexus (the co-design)

Bombay sits between two systems and is designed *with* both:

- **Zenoh** owns **transport / addressing / discovery** — actors are addressable in the dataspace by key-expr; `ask` = query/reply, `tell` = put, death-watch = a liveliness token per actor.
- **nexus** owns **consistency / persistence / ordering / replay** — the event log, single-writer-per-aggregate via optimistic concurrency (`Version`), at-least-once + idempotent/exactly-once replay, the forever-driver subscription cursor.
- **Bombay** is the **adapter** that maps nexus aggregates onto Zenoh-addressed actors and builds the runtime nexus ships only primitives for (the loop, dispatch, cursor, lifecycle, supervision, command/projection/saga runners).

Because we control both nexus and bombay, a Zenoh limitation is rarely a Bombay limitation: **for any Zenoh gap, ask whether nexus already covers it, and if not, patch whichever layer gives the best result.** (E.g. Zenoh's per-message "reliability" is only a link-selection marker — real delivery comes from nexus's event log + replay; Zenoh has no in-SDK storage — nexus *is* the store.)

## Status

**M0 → M1 (the fork has landed).** The M0 de-risking is done:
- **#62 walking-skeleton spike — GO.** `ask`/`tell`/liveliness/health validated over Zenoh 1.9.0; sub-ms messaging; crash-detection 2 ms vs partition-detection ~10 s (lease); single-writer must come from nexus, not Zenoh addressing.
- **#64 feature matrix** — Zenoh core vs zenoh-ext vs zenoh-pico mapped, with stability gates and per-card caveats.
- **#63 fork strategy — DECIDED.** Hard-fork (no upstream rebase — the Zenoh rewrite + M7 de-handroll make rebaseability worthless), dual `MIT OR Apache-2.0` (a clean match to kameo). kameo is vendored verbatim from upstream `main` @ `821e247` (latest, identical in code to the v0.21.0 release); see [`NOTICE`](NOTICE) + [`docs/superpowers/specs`](docs/superpowers/specs).

The kameo source (`src/ actors/ console/ macros/` + examples/benches/tests) now sits in-tree, wrapped in the Nix harness (#60). Next: M1 — replace `src/remote/` (libp2p) with a thin Zenoh `Session` layer.

## Reference docs

Distilled, AI-referenceable knowledge lives under [`docs/`](docs/). **Read the relevant doc before working a card:**

- [`docs/zenoh/capabilities.md`](docs/zenoh/capabilities.md) — Zenoh capability matrix (core/ext/pico), stability gates, and the per-card "gap → nexus/bombay coverage" flags. Read before any card touching Zenoh.
- [`docs/testing/`](docs/testing/) — the **card #74 test-coverage spec** for the surviving kameo core (**457 example scenarios + 112 `@property`/`@model` laws** across 42 files) captured as executable `.feature` files. **Card #76 wired the console slice** (PR #81): all 148 console scenarios (tui 93+13, poller 17+4, server_wire 17+4) are green under `nix flake check`, plus an in-process `ratatui::TestBackend` render suite (`console/tests/tui_render.rs`) that drives the real `tui::App` by synthetic keystrokes and lifts its line coverage from 14% to 73%. The reusable World + step-definition pattern (gated `testing` surface, standard-libtest cucumber runner, proptest laws, `max_concurrent_scenarios(1)` for shared global state) is documented in [`docs/testing/README.md`](docs/testing/README.md) for the core/actors cards to follow. The suite is deterministic under CI scheduling/clock variance — ordering invariants assert monotonic `uptime` (an `Instant`), never the best-effort `captured_at` (`SystemTime::now()`, which a CI clock step can regress); and actor-stop assertions use condition-based waiting on the observable registry state, since `wait_for_shutdown()` (mailbox closed) returns before the console monitor records `Stopped`. Catalog: [`invariants.md`](docs/testing/invariants.md), [`properties.md`](docs/testing/properties.md), [`coverage-audit.md`](docs/testing/coverage-audit.md). Scenarios under [`tests/features/`](tests/features/) are tagged `@sequence`/`@lifecycle`/`@boundary`/`@linearizability`; `@bug:<file:line>` probes stay RED (`pubsub.rs:125`, `message_queue.rs:591/707`, `id.rs:140-143`/`:218-221`). **Card #77 wires the core slice** (309 scenarios across 24 files / 12 modules) onto the same harness — design in [`docs/superpowers/specs/2026-06-26-core-cucumber-wiring-design.md`](docs/superpowers/specs/2026-06-26-core-cucumber-wiring-design.md), step-by-step plan in [`docs/superpowers/plans/2026-06-26-core-cucumber-wiring.md`](docs/superpowers/plans/2026-06-26-core-cucumber-wiring.md). The `id.rs` `from_bytes` `@bug` probes are mechanized as `#[should_panic]` demonstrators (green while the defect lives, flip red on fix), and the matching too-short property law in `actor_id.properties.feature` is `@bug`-tagged to keep the gate green.

**Core wiring progress (card #77):** the `actor_id` slice is wired as the reusable example every later module copies — `tests/features/core/actor_id.feature` (17 scenarios) + `actor_id.properties.feature` (5 `@property`/`@model` laws, each driven by a real `proptest!` over boundary-biased `u64` generators `{0, 1, MAX-1, MAX}` plus a deterministic concurrent-contiguity model law) run green via `core_actor_id_bdd` / `core_actor_id_props_bdd`, sharing one `ActorIdWorld` in `tests/core_steps/actor_id.rs`. The three `from_bytes` short-slice `@bug:id.rs:140-143`/`:218-221` scenarios are excluded from the green run and pinned instead by `#[should_panic]` probes (including the real serde `visit_bytes` path) that flip RED when the bounds-check fix lands. The `error` slice is wired next on the same harness — `tests/features/core/error.feature` (20 scenario blocks → 48 Outline rows) + `error.properties.feature` (4 `@property` SendError-algebra laws) run green via `core_error_bdd` / `core_error_props_bdd`, sharing one `ErrorWorld` in `tests/core_steps/error.rs` over `SendError<TestMsg, TestErr>`; it covers `map_msg`/`map_err`/`boxed`/`msg`/`err`/`flatten`, `BoxSendError` downcast round-trip + wrong-type recovery, `unwrap_msg`/`unwrap_err` panic messages, `ActorStopReason::is_normal`, `PanicReason` lifecycle/message classifiers, `PanicError` `with_str`/`with_downcast_ref`/poisoned-mutex access, the lossy-and-non-idempotent `PanicError` serde round-trip (reason prefix compounds; concrete payload type erased), and the local `RegistryError` `BadActorType` vs `NameAlreadyRegistered` domains. No `@bug` scenarios and no `src/` change were needed.

The work is GitHub-project-cards-driven with TDD; see [`CLAUDE.md`](CLAUDE.md) for the working method, milestones, and engineering rules.

## Building

Bombay uses the same Nix-flake setup as the sibling nexus/agency repos (crane + fenix + flake-utils + advisory-db), with **one deliberate deviation: a pinned STABLE toolchain** (`rust-toolchain.toml`, currently 1.96.0) fed to fenix via `fromToolchainFile`, so Nix and plain `rustup` resolve the *same* toolchain and non-Nix users can build with stock stable Rust.

```bash
direnv allow        # or: nix develop
nix flake check     # the single gate: build + clippy + fmt + audit + deny + nextest + doctest + actionlint
```

> **The `clippy` check passes, but the god-level bar is temporarily relaxed.** The vendored kameo code (~19k LOC) is not clean against bombay's full lint config (1200+ findings, mostly pedantic/nursery/restriction). Rather than bury those under scattered `#[allow]`s on code that ships **zero tests upstream** (notably the `actors` crate), the bar in [`clippy.toml`](clippy.toml) + the `[workspace.lints.clippy]` block (adopted from nexus) is **parked at `allow`** so the gate passes over verbatim kameo — kameo's own `#![warn(clippy::all)]` still holds. It will be re-tightened lint-by-lint, with real fixes, as the surviving core gains test coverage (M1/M7). Both files carry restore instructions. The other checks (fmt/audit/deny/nextest/doctest/actionlint) give real signal.

## Continuous integration

Every pull request to `main` (and every push to a non-`main` branch) runs the single gate — `nix flake check -L` — on GitHub Actions ([`.github/workflows/checks.yml`](.github/workflows/checks.yml), mirroring nexus); `-L` streams full build logs so a failed derivation shows the complete output (e.g. which test/scenario failed) instead of nix's truncated "last 25 lines". That runs the **entire workspace test suite (`bombay-nextest`) and the doc-tests (`bombay-doctest`)** on every change, alongside fmt/audit/deny/clippy. The workflow files are themselves linted by `actionlint`, wired in two places: the `bombay-actionlint` flake check, and the `pre-commit` hook (run eagerly, but only when a workflow file is staged). The clippy gate is green (with the bar relaxed as noted above), so CI now passes end-to-end.

## Local setup

Enable the tracked git hooks once per clone:

```bash
git config core.hooksPath .githooks
```

- **`pre-commit`** — blocks any commit that doesn't stage a `README.md` change (the README-with-every-commit discipline), and runs `actionlint` whenever a GitHub Actions workflow is staged.
- **`pre-push`** — runs the full `nix flake check` before a push; **blocks** on failure (so it will block until the clippy gate is green — bypass a push with `git push --no-verify`).
- **`post-merge`** — runs `nix flake check` after a `git pull`/merge (advisory; git ignores its exit code).
