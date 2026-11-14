---
name: local-dev
description: Use Bombay's repository-owned pinned Nix environment for Rust development and checks.
version: 1.1.0
author: obvious-autobuild
created: 2026-09-17
---

# Local development

Read the repository's AGENTS.md and README.md first. The checked-in .envrc uses
the flake; `direnv allow` is its existing environment entry point. Alternatively,
enter that same environment with `nix develop`. Never run host Cargo, Rust,
formatting, lint, test or example binaries.

The toolchain declaration and Nix lock are authoritative. Cargo.lock selects
the dependency versions; inspect any actual patch entries instead of assuming
a sibling checkout. Do not install another Rust toolchain or replace personal
Nix configuration to reproduce an old sandbox snapshot.

Use `nix develop -c cargo check --locked -p <package>` for focused type checking,
`nix develop -c cargo test --locked -p <package> --test <target>` for focused
tests, and the selected feature PRD for debug/optimized or adversarial gates.
Run formatting and strict lint through the same shell. `nix flake check -L`
is the repository gate; see flake.nix for its actual current targets.

The backlog and feature-local PRDs own blockers, verification and delivery
status. Keep historical observations separate from current source results.
Previous sandbox timings, test counts, package names and parallelism workarounds
remain available in Git history and are not current setup instructions.
