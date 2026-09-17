---
name: local-dev
description: Re-enter the verified local dev environment for devrandom-labs/bombay — Rust 2024 workspace inside the pinned Nix flake dev shell.
version: 1.0.0
author: obvious-autobuild
created: 2026-09-17
---

# local-dev — devrandom-labs/bombay

Installed 2026-09-17. Everything below was actually executed and verified in
the repo sandbox (`cmp_wGrjXzFn`, Debian 13, 8 cores, 7.8 GB RAM); snapshot
`mojxxu1gj5hbcaqq9is7:default` captures this exact state.

## Prerequisites (verified runtime matrix)

| Runtime | Version | Notes |
|---|---|---|
| Nix | 2.35.2 | Not preinstalled on the fresh sandbox — install below |
| rustc | 1.96.0 | Pinned by `rust-toolchain.toml`, provided by the flake dev shell |
| cargo | 1.96.0 | Same — never use a host cargo |
| Network | required at first build | crates.io + github.com for the `[patch.crates-io]` git siblings |

## Install commands (fresh sandbox, in order)

```bash
curl -L https://nixos.org/nix/install -o /tmp/nix-install.sh
sh /tmp/nix-install.sh --no-daemon
mkdir -p ~/.config/nix
cat > ~/.config/nix/nix.conf <<'CFG'
experimental-features = nix-command flakes
accept-flake-config = true
cores = 4
max-jobs = 1
CFG
. ~/.nix-profile/etc/profile.d/nix.sh
nix develop -c cargo --version   # ~7 min cold: builds dev shell closure (~4.4G store)
```

The dev shell closure does NOT include the flake `checks`; `nix develop` alone
is quick after the first build. `nix flake check -L` (full CI lanes) was not
run locally — CI runs it green on main.

## Environment setup

- No env vars required. No `.env` / `.env.example`. `.envrc` only contains `use flake` (direnv).
- AGENTS.md mandate: every cargo/rustc/example/lint/test command goes through `nix develop -c ...`.

## Start commands (verified, actual ports parsed from output)

- `nix develop -c cargo run -p bombay-example-counter` — exits 0 after the
  typed flow completes; no port, no server.
- `nix develop -c cargo run -p bombay-example-axum` — HTTP server, actual
  listen address `127.0.0.1:3000` (confirmed with `ss -tlnp`). Routes:
  `POST /orders` (body `{"order_id":u64,"customer_id":u64,"sku":String,"quantity":u32}`),
  `DELETE /admin/shutdown` (graceful stop, exit 0).

## Primary flow verified (evidence)

1. Counter example: Application boots counter actor → typed external customer
   sends `Increment` then `Read` → reply asserted `1` → lifecycle shutdown →
   terminal asserted → exit 0. Log: `/tmp/` run transcripts; summary PNG:
   `/home/user/.obvious-install/screenshots/dev-stack-evidence.png`.
2. Axum example: server up on 127.0.0.1:3000 → `POST /orders` twice →
   HTTP 202, body echoes the exact admitted order → malformed body → HTTP 422
   → `DELETE /admin/shutdown` → HTTP 202, process exits 0.
   Transcript + PNG: `/home/user/.obvious-install/screenshots/axum-order-flow/`.

## Verified check commands

- Typecheck/build: `nix develop -c cargo build --workspace` (exit 0, 21s, 88 crates)
- Lint: `nix develop -c cargo clippy --workspace --all-targets -- -D warnings` (exit 0)
- Format: `nix develop -c cargo fmt --all -- --check` (exit 0)
- Tests: `nix develop -c cargo test --workspace -- --test-threads=1` (exit 0, 55 result blocks, 0 failed)
- Scoped: `cargo check -p bombay-machine`; `cargo clippy -p bombay-engine --all-targets -- -D warnings`; `cargo test -p bombay-rs --test entity_runtime -- --test-threads=1` (11 passed)

## Known blockers and workarounds

- **No preinstalled toolchain on the fresh sandbox** (`runtime_unavailable` → resolved): install single-user Nix as above; no sudo-free alternative was viable. Passwordless sudo is available.
- **Test-parallelism flake (non-fatal):** `cargo test --workspace` at default
  parallelism can fail 1–2 `entity_runtime` tests
  (`passivation_reports_superseded_after_incarnation_replacement`,
  `fence_failures_preserve_the_forced_retirement_stage`) on many-core hosts —
  their bounded spin budgets (1,000 `thread::yield_now()`) expire while spawned
  retirement threads compete with 8 test threads. Reproduced at 8 threads and
  under `taskset -c 0-3`; passes with `--test-threads=1` (11/11) and per-test
  with `--exact`. CI is green (tests run inside the Nix build sandbox with
  restricted affinity). **Workaround: always run tests with
  `-- --test-threads=1` on this sandbox.**
- Memory: 7.8 GB RAM is comfortable for the whole suite; Nix `cores = 4` keeps
  builds well within budget.

## Snapshot

- Snapshot ID: `mojxxu1gj5hbcaqq9is7:default` (captured 2026-09-17T15:40:39Z)
- After restore: `. ~/.nix-profile/etc/profile.d/nix.sh`, then `nix develop -c cargo build --workspace` (incremental, seconds).
