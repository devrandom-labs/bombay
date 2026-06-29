{
  description =
    "Bombay — a Zenoh-native hard-fork of the kameo actor framework";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    fenix = {
      url = "github:nix-community/fenix";
      inputs = { nixpkgs.follows = "nixpkgs"; };
    };
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
  };
  outputs = { self, nixpkgs, utils, crane, fenix, advisory-db, ... }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        inherit (pkgs) lib;

        # STABLE, pinned toolchain — bombay's deliberate deviation from
        # nexus/agency (which run fenix nightly `complete`). Read from
        # rust-toolchain.toml so Nix and plain rustup resolve the SAME
        # toolchain (card #60). The sha256 covers the channel manifest
        # (system-independent, so this one hash is portable); re-bootstrap it
        # on a channel bump by setting `lib.fakeHash` and pasting the hash Nix
        # reports.
        toolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-mvUGEOHYJpn3ikC5hckneuGixaC+yGrkMM/liDIDgoU=";
        };

        # crane 0.18.0+ wants `overrideToolchain` called with a FUNCTION that
        # builds the toolchain for a given pkgs instantiation (correct
        # cross-compilation splicing), not a bare derivation. We don't
        # cross-compile, so ignoring the arg and returning our pinned toolchain
        # is sufficient and current. (nexus/agency still use the older bare
        # form.)
        craneLib = (crane.mkLib pkgs).overrideToolchain (_: toolchain);

        # kameo's root crate does `#![doc = include_str!("../README.md")]`, so
        # the repo-root README.md must be in the sandboxed build source —
        # `cleanCargoSource` would strip it (non-Rust file), breaking both the
        # doc build and ordinary compilation (include_str! is evaluated at
        # compile time, not just by rustdoc). Use a fileset that keeps the cargo
        # sources plus README.md.
        #
        # The cucumber BDD runners (card #74/#76) read `.feature` files at RUNTIME
        # via `filter_run_and_exit(<path>)`; `commonCargoSources` strips non-Rust
        # files, so without this the gate's `cargoNextest` would fail with
        # "Could not read path" even though the runners pass with the full tree
        # checked out. Keep the whole feature catalog in the sandbox.
        src = lib.fileset.toSource {
          root = ./.;
          fileset = lib.fileset.unions [
            (craneLib.fileset.commonCargoSources ./.)
            ./README.md
            ./tests/features
          ];
        };

        commonArgs = {
          inherit src;
          strictDeps = true;
          # Modern nixpkgs darwin stdenv bundles the Apple SDK, so no explicit
          # `darwin.apple_sdk.frameworks.*` are needed (those legacy stubs were
          # removed from nixpkgs). openssl + the usual native tools cover the
          # transitive C deps pulled in by kameo's dev-dependencies (libp2p,
          # criterion, the prometheus exporter).
          buildInputs = with pkgs; [ openssl ];
          nativeBuildInputs = with pkgs; [ cmake pkg-config perl ];
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
      in with pkgs; {
        checks = {
          # Whole-workspace clippy at bombay's god-level bar. The vendored
          # kameo code is NOT yet clean against this config, so this gate is
          # RED by design until M1/M7 bring the surviving core up to standard.
          bombay-clippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          });

          bombay-doc =
            craneLib.cargoDoc (commonArgs // { inherit cargoArtifacts; });

          bombay-fmt = craneLib.cargoFmt { inherit src; };

          bombay-toml-fmt = craneLib.taploFmt {
            src = pkgs.lib.sources.sourceFilesBySuffices src [ ".toml" ];
          };

          bombay-audit = craneLib.cargoAudit {
            inherit src advisory-db;
            # Both advisories are in `hickory-proto`, pulled in transitively by
            # `libp2p` (mDNS/DNS) — the exact layer M1 deletes when it replaces
            # `src/remote/` with Zenoh, so the vulnerable code leaves the tree
            # then. RUSTSEC-2026-0118 has no fixed upgrade available regardless.
            cargoAuditExtraArgs =
              "--ignore RUSTSEC-2026-0118 --ignore RUSTSEC-2026-0119";
          };
          bombay-deny = craneLib.cargoDeny { inherit src; };

          bombay-nextest = craneLib.cargoNextest (commonArgs // {
            inherit cargoArtifacts;
            partitions = 1;
            partitionType = "count";
          });

          # nextest does NOT run doctests, so verify the doc-comment examples
          # separately with `cargo test --doc` (crane's dedicated wrapper).
          bombay-doctest =
            craneLib.cargoDocTest (commonArgs // { inherit cargoArtifacts; });

          # Lint the GitHub Actions workflows themselves. actionlint shells out
          # to shellcheck for `run:` steps, so it is on PATH here too. This keeps
          # the CI definition under the same single gate as the code.
          bombay-actionlint = pkgs.runCommandLocal "bombay-actionlint" {
            nativeBuildInputs = [ pkgs.actionlint pkgs.shellcheck ];
          } ''
            actionlint ${./.github/workflows}/*.yml
            touch "$out"
          '';
        };

        devShells.default = craneLib.devShell {
          checks = self.checks.${system};

          shellHook = ''
            #!/usr/bin/env bash
            # Set git hooks path to tracked .githooks/ directory
            git config core.hooksPath .githooks
            # Create a fancy welcome message
            REPO_NAME=$(basename "$PWD")
            PROPER_REPO_NAME=$(echo "$REPO_NAME" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')
            figlet -f doom "$PROPER_REPO_NAME" | lolcat -a -d 2
            cowsay -f dragon-and-cow "Welcome to the $PROPER_REPO_NAME development environment on ${system}!" | lolcat
          '';

          packages = [
            bacon
            figlet
            lolcat
            cowsay
            tmux
            tree
            cloc
            cargo-edit
            cargo-expand
            gh
          ];
        };

        # On-demand coverage report (card #85), NOT a gating check — coverage
        # instrumentation recompiles the world, too slow for the per-push gate.
        # `nix build .#coverage -L` prints the per-file summary table and writes
        # the browsable HTML report to ./result. Crane's `cargoLlvmCov` brings
        # `cargo-llvm-cov`; the version-matched `llvm-cov`/`llvm-profdata` come
        # from the `llvm-tools` toolchain component (rust-toolchain.toml). The
        # `testing` feature auto-enables via the self dev-dep, so the core/actors
        # cucumber runners build. `remote` (libp2p, deleted in M1) is off by
        # default, so it is neither built nor counted.
        packages.coverage = craneLib.cargoLlvmCov (commonArgs // {
          inherit cargoArtifacts;
          cargoLlvmCovCommand = "test";
          # `--workspace` covers kameo + actors + console + macros. `remote`
          # (libp2p) is off by default so it is never compiled and never counted.
          # The per-file summary prints to the build log (`-L`); HTML lands in
          # ./result. Test-harness files (`tests/`) show in the table too — read
          # the `src/` rows for the SUT signal; an ignore-regex can refine later.
          cargoLlvmCovExtraArgs = "--workspace --html --output-dir $out";
        });
      });
}
