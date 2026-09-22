# devrandom-labs/bombay

Setup skill version: 4.2.1 — installed 2026-09-17.

Bombay is the typed local actor-runtime composition layer over the universal
direct-Behavior Driver: pure, statically typed Behavior actors with
runtime-owned mailboxes, address spaces, observation, timers, and a task
hierarchy. This is a Rust library workspace plus example binaries — there is
no long-running service to deploy and no external infrastructure.

## Stack

- **Language:** Rust 2024 edition; toolchain pinned to **1.96.0** via `rust-toolchain.toml`.
- **Dev environment:** Nix flake dev shell (`nix develop`) — nixpkgs unstable + crane + fenix. Provides cargo/rustc 1.96.0, rustfmt, clippy, rust-analyzer, cargo-nextest, cargo-mutants, bacon, taplo.
- **Package manager:** cargo, always invoked inside the pinned Nix shell (`nix develop -c cargo ...`). AGENTS.md forbids invoking host `cargo`/`rustc` directly.
- **Dependencies:** crates.io plus git-pinned sibling crates via `[patch.crates-io]` (bombay-behavior @ `a272adf`, bombay-timers @ `13e884d`) — fetched from GitHub at build time.
- **External services:** none. No database, Redis, Docker, or HTTP service is required; no env vars are required (no `.env.example` exists).
- **CI:** `nix flake check -L` (crane lanes: build, test, fmt, clippy, doc, doctest, three loom lanes, panic lanes, example runs).
- **Fresh-sandbox setup:** install single-user Nix (`curl -L https://nixos.org/nix/install | sh -s -- --no-daemon`), set `experimental-features = nix-command flakes` + `accept-flake-config = true` in `~/.config/nix/nix.conf`, then `nix develop` once (~7 min cold) — see `.obvious/skills/local-dev/SKILL.md`.

## Commands

| Task | Command (verified 2026-09-17) |
|---|---|
| Enter dev shell | `nix develop` |
| Build workspace | `nix develop -c cargo build --workspace` |
| Test workspace | `nix develop -c cargo test --workspace -- --test-threads=1` |
| Format check | `nix develop -c cargo fmt --all -- --check` |
| Lint | `nix develop -c cargo clippy --workspace --all-targets -- -D warnings` |
| Run primary example | `nix develop -c cargo run -p bombay-example-counter` |
| Run HTTP example | `nix develop -c cargo run -p bombay-example-axum` (serves 127.0.0.1:3000) |
| Full CI locally | `nix flake check -L` |

> **Test-parallelism caveat:** at default parallelism (`cargo test --workspace`
> with no `--test-threads`), two `entity_runtime` tests
> (`passivation_reports_superseded_after_incarnation_replacement`,
> `fence_failures_preserve_the_forced_retirement_stage`) can exceed their
> bounded spin budgets (1,000 `thread::yield_now()` iterations) on hosts with
> many test threads; each passes in isolation and the whole suite passes with
> `--test-threads=1`. CI is green because the Nix build sandbox restricts CPU
> affinity there. Prefer the `--test-threads=1` invocation above on many-core hosts.

## Codebase Map

See `.obvious/codebase-map.md`.

## Repo Guidance

- **`AGENTS.md` is authoritative** (`CLAUDE.md` just points to it). Read it fully before any change.
- **Every Rust/Cargo/example/lint/test command runs through the pinned Nix shell** — never host `cargo`/`rustc`.
- **Behavior fold boundary is absolute:** inside a fold, compute state and return `Actions`; never perform an effect directly. Every communication, creation, lifecycle, timer, or observation goes through a typed `Actions` lane.
- **No inline `use`** inside functions/methods/blocks/tests/macro arms — module-scope imports only. No semantic state in a `bool` — use closed sum types.
- **Naming:** every name must use the Bombay domain language and reveal ownership; `manager`, `handler`, `helper`, `utils`, etc. are forbidden, including private/test code.
- **Macros are last resort:** prototype ordinary Rust alternatives and record the evidence in the ledger before any macro work; generated code must lower to the exact owning Behavior algebra.
- **Authoritative-fact conservation:** never erase or misclassify a typed fact; preservation/discharge must be an explicit typed policy.
- **Change containment stop-thresholds:** >15 changed files, >500 net new production lines, or >3 new public types → stop and get explicit user authorization.
- **Before architecture work:** read `docs/open-design-ledger.md` (live backlog/blockers), `docs/module-boundaries.md` (ownership), `docs/runtime-capability-interfaces.md`, `docs/driver-law.md`. `docs/historical-design-decisions.md` is historical, not current guidance.
- **Mandatory feature verification:** inspect the exact locked dependency versions (`Cargo.lock` + `[patch]`) and record feature ownership/blockers in the ledger before implementing.
- **Commits:** short imperative subjects with a scope-like prefix; report exact verification commands and results in the commit body.

## Local Verification

<!-- local-verification-summary:v1 -->
- **Typecheck command:** `nix develop -c cargo build --workspace`
- **Lint command:** `nix develop -c cargo clippy --workspace --all-targets -- -D warnings`
- **Test command:** `nix develop -c cargo test --workspace -- --test-threads=1`
- **Scoped typecheck:** `nix develop -c cargo check -p bombay-machine`
- **Scoped lint:** `nix develop -c cargo clippy -p bombay-engine --all-targets -- -D warnings`
- **Scoped test:** `nix develop -c cargo test -p bombay-rs --test entity_runtime -- --test-threads=1`
- **Full-repo check safe:** yes — build 21s, clippy ~1 min, sequential tests ~2 min on 8 cores. **Exception since the Behavior pin (`8bca837c`):** any lane that pulls `behavior-actors` metadata (workspace build/check/test/clippy, `nix flake check`, everything under `-p bombay-rs` and the examples) OOM-kills rustc on 8 GB sandboxes (host global-oom at ~7.4 GB rustc RSS, confirmed via `dmesg`); those lanes need CI's 16 GB runners or a larger host. Locally verifiable: scoped `-p bombay-machine`, `-p bombay-engine`, `-p bombay-address` lanes.
- **Scoped alternatives discovered:** yes — cargo supports `-p <package>` for every lane (pattern used by this repo's own CI)
<!-- /local-verification-summary -->

Scoped workflow:

1. Typecheck one package: `nix develop -c cargo check -p <package>`
2. Lint one package: `nix develop -c cargo clippy -p <package> --all-targets -- -D warnings`
3. Test one target: `nix develop -c cargo test -p <package> --test <target> -- --test-threads=1`

Validated 2026-09-17 (result: pass). Build, fmt, clippy, both example flows,
and the full sequential test suite all exited 0; see the caveat under Commands
for the default-parallelism flake. Verified flows: `bombay-example-counter`
(typed external customer → Increment → Read → reply `1` → graceful shutdown,
exit 0) and `bombay-example-axum` (HTTP 202 admit on `POST /orders` with exact
payload echo, HTTP 422 on malformed body, HTTP 202 graceful shutdown on
`DELETE /admin/shutdown`, exit 0).

## Sandbox Snapshot

- **Snapshot ID:** `mojxxu1gj5hbcaqq9is7:default`
- **Captured:** 2026-09-17T15:40:39Z
- **Contents:** single-user Nix 2.35.2 with the flake dev shell (rustc/cargo 1.96.0), warm `target/` build cache, branch `chore/obvious-onboarding`, no running processes.
- **Restore hint:** re-enter with `nix develop` (shell rebuilds from the Nix store in the snapshot; `cargo build --workspace` is then incremental).
