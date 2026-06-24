# Card #63 — Fork strategy (decision record)

**Status:** Decided + executed (branch `feat/63-fork-and-nix-harness`).
**Date:** 2026-06-23.
**Gates:** unblocks M1 (#1, the fork). Overlaps #60 (Nix harness) and #61 (clippy config), which were executed together with the vendor in this branch.

This is the recorded decision the card asks for. It supersedes the loose
"recommend hard-fork / pin 0.21.0" notes on the issue with confirmed choices and
the exact mechanics that were carried out.

## 1. Fork model — HARD-FORK

Diverge freely; never rebase on upstream kameo. Upstream fixes, if ever wanted,
are manually cherry-picked.

Rationale: M1 rewrites `src/remote/` (libp2p → Zenoh, ~4,200 → ~800 LOC) and M7
de-handrolls the local core. Divergence is heavy enough that rebaseability has
no value, so we do **not** preserve kameo's layout *for rebaseability*. We do
keep kameo's file structure initially — not to rebase, but so the M1
`src/remote/` rewrite reads as a clean diff against a recognizable baseline.

## 2. Pin — kameo upstream `main` @ `821e247a5fe4f10d647c5fc2b4d0fd786f223867`

The user directed **latest**, not the tag. Latest `main` (2026-06-21) is exactly
**1 commit ahead of the `v0.21.0` tag**, and that commit
(`fix(scripts): generate kameo release notes …`) touches neither `src/` nor any
`Cargo.toml`. So "latest main" and the `v0.21.0` release are **identical in
code**; we pin to the SHA for precision.

Sourcing note: vendored from a **fresh pristine clone of the original
`tqwewe/kameo`**, not from the local `../kameo` fork. `src/` verified
byte-identical to upstream (`diff -rq`); member manifests differ only by the
added `[lints] workspace = true` opt-in.

## 3. License + attribution — dual `MIT OR Apache-2.0`

kameo is `MIT OR Apache-2.0`; bombay adopts the same (nexus = MIT/Apache,
agency = Apache-2.0 — all compatible). A clean superset match, no
incompatibility.

Mechanics carried out:
- `LICENSE-MIT` + `LICENSE-APACHE` carried over **verbatim** (kameo copyright
  lines intact).
- `NOTICE` added crediting kameo + the exact fork SHA, declaring bombay's dual
  license and additional copyright holder.

## 4. Repo init — squashed vendor import

bombay already has independent history under `devrandom-labs/bombay` and is
**not** a GitHub-level fork of kameo, so "fork-link" is impossible. kameo's
workspace is vendored **verbatim, structure preserved**, landing on top of the
existing history. Per-commit upstream blame is intentionally dropped in favour
of a single clean, attributable baseline.

Vendored: `src/ actors/ console/ macros/ examples/ benches/ tests/`,
`LICENSE-MIT`, `LICENSE-APACHE`, kameo's `.gitignore` (minus the `Cargo.lock`
and `.envrc` ignores — bombay tracks both). Dropped: kameo's `README.md`,
`.github/`, `CHANGELOG.md`, `cliff.toml`, `banner.png`, `scripts/`, `docs/`,
`docs.json` (bombay-specific or unwanted).

## Harness executed alongside (#60 / #61)

Mirrors the sibling nexus/agency flake setup (crane + fenix + flake-utils +
advisory-db; checks = clippy / doc / fmt / toml-fmt / audit / deny / nextest;
figlet/lolcat devShell + `git config core.hooksPath .githooks`), with **one
deliberate deviation**: a pinned **STABLE** toolchain.

- `rust-toolchain.toml` — `channel = "1.96.0"` (latest stable, 2026-05-25), fed
  to fenix via `fromToolchainFile` so Nix and plain `rustup` resolve the *same*
  toolchain. Manifest `sha256` is system-independent and committed.
- Root `Cargo.toml` — kameo's verbatim + `[workspace.package]`,
  `[workspace.metadata.crane] name = "bombay"`, and the god-level
  `[workspace.lints.clippy]` block (nexus, verbatim). kameo's inline
  `[lints.rust] unexpected_cfgs` lifted into `[workspace.lints.rust]`; every
  member opts in with `[lints] workspace = true`.
- `clippy.toml` — nexus's "elite" config verbatim (#61).
- `Cargo.lock` — generated + committed (kameo gitignored it as a library;
  bombay is an application workspace and needs it for reproducible/crane builds).

**The `clippy` check is intentionally RED** (god-level lints applied
workspace-wide over un-cleaned vendored kameo — user's explicit choice). Other
checks give real signal. The `pre-push` hook runs the full `nix flake check`
and blocks on failure (bypass with `git push --no-verify`); `post-merge` runs it
advisorily after a pull.

Deferred to their own cards: `cargo-hakari`/workspace-hack, `.github` CI
workflows.
