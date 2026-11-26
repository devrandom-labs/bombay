---
name: nix-builds
description: Maintain the Labcast Nix development shell, Crane/Fenix Rust builds, application images, Just recipes and deployment tooling. Use for reproducible setup, packaging and build-check failures.
metadata:
  origin: labcast-local
---

# Nix builds

Read flake.nix, flake.lock, nix/, justfile and the relevant application manifest
before editing setup. Follow [ADR 0015](../../../docs/adr/0015-build-application-images.md)
and [deployment](../../../docs/arch/deployment.md). Inspect Trive's applicable
pattern before changing the shared environment or GCP/Terraform/SOPS workflow.

- Use locked nixpkgs packages and library builders before a new derivation.
  Extend the existing Crane/Fenix configuration; do not introduce a second Rust
  toolchain source or bootstrap script.
- Check source filters. Required migrations, shader assets, model manifests and
  documentation must reach the derivation that owns them. Keep secrets out of
  the Nix store and image layers.
- Reuse Cargo dependency artifacts only when the toolchain, features and target
  are compatible. Distinguish the build host from the Linux deployment target.
- Before deliberate load tests on a shared GitHub Actions builder, check for
  `Runner.Worker` and defer the experiment while CI is active.
- Do not add CI job or whole-suite time caps. Required checks run to completion;
  preserve product timing assertions and defined workload lengths.
- Keep executables and generated capability artifacts from the same release.
  A correct digest or unchanged format source does not prove a match: the
  decode path also checks revisions compiled from the filtered source, toolchain
  and lock. Qualify a pair by starting that executable with that artifact.
- Separate server and Studio image contents. Native GStreamer plugins and GPU
  runtime requirements need target-specific checks, not only Rust compilation.
- Studio's live VM has two containers: Studio and pinned upstream Smelter, privately
  connected. Editorial adapters, LiveKit receivers, GStreamer and Bevy belong in Studio.
  A review or replay execution does not by itself add a third backend app.
- Use the existing Terraform and SOPS ownership. A skill install does not grant
  permission to change cloud resources or decrypt and print secrets.
- Keep ordinary recurring commands in small Just recipes. One-off inspection
  does not justify a new validator, runner or orchestration framework.
- For database checks, follow the
  [test boundaries](../rust-tests/SKILL.md#database-and-application-boundaries).
  Reuse disposable PostgreSQL setup and locked database test packages. Pass SQL
  test files directly to the database check, not through the Rust source filter,
  and run them through a check that fails on assertion errors; do not add Rust
  wrappers for database-only assertions. Create the disposable database with the
  production encoding: SQL_ASCII counts each byte as a character.

Run the relevant existing flake checks. The local Git flake reads the working-tree
contents of tracked files, including unstaged edits, and omits untracked files.
Add only the intended new files to Git (`git add -N` or `git add <file>`); leave
unrelated work untouched. Do not use `path:.`: it copies ignored caches, build
outputs and private files into the store. A dirty tree's identity is its flake
source and check derivations, not `git write-tree`; record those. Stage before a
commit. Report the tested system and any unbuilt targets. A Darwin
document check does not establish that a Linux GPU Studio image works.

## Sources

- [Crane API](https://crane.dev/API.html)
- [Fenix](https://github.com/nix-community/fenix)
- [Nixpkgs manual](https://nixos.org/manual/nixpkgs/stable/)
- [Just manual](https://just.systems/man/en/)
- [SOPS](https://github.com/getsops/sops)
