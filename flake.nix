{
  description = "Bombay — a Zenoh-native hard-fork of the kameo actor framework";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    fenix = {
      url = "github:nix-community/fenix";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
  };
  outputs =
    {
      self,
      nixpkgs,
      utils,
      crane,
      fenix,
      advisory-db,
      ...
    }:
    utils.lib.eachDefaultSystem (
      system:
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
            ./fuzz/tests/__fuzz__
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
          nativeBuildInputs = with pkgs; [
            cmake
            pkg-config
            perl
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # The fuzz workspace has its OWN Cargo.lock (bolero + bombay-core path
        # dep). Vendor it separately so the replay check builds offline without
        # touching the root workspace's vendored deps.
        fuzzCargoArtifacts = craneLib.vendorCargoDeps { cargoLock = ./fuzz/Cargo.lock; };

        # A lightweight non-cargo check runner: run `cmd` with `tools` on PATH;
        # the derivation succeeds (touch $out) only if the command does. Keeps
        # the hygiene gates (typos, nixfmt, deadnix, shellcheck, yaml) uniform.
        lintCheck =
          name: tools: cmd:
          pkgs.runCommandLocal name { nativeBuildInputs = tools; } ''
            ${cmd}
            touch "$out"
          '';
      in
      with pkgs;
      {
        checks = {
          # The clippy gate, RESTORED as a ratchet (card #61). The god-level bar
          # (root Cargo.toml `[workspace.lints]` + clippy.toml) is DENY, so all NEW
          # PRODUCTION code is held to it. Vendored kameo lib/bin files that predate
          # the bar carry a documented per-file quarantine header
          # (`#![allow(..., reason = "…#61")]`), removed file-by-file as the code is
          # cleaned or deleted under M1/M7.
          #
          # Scope = default targets (lib + bins), NOT `--all-targets`: test /
          # example / bench code is deliberately held to a lighter bar (consistent
          # with clippy.toml's `allow-unwrap-in-tests` / `allow-expect-in-tests`) —
          # the ~60 BDD-wiring files trip 2k+ pedantic/nursery style findings whose
          # cleanup buys nothing on code whose job is test clarity. Gating the test
          # surface is a separate #61-tail decision. `--all-features` still lints the
          # `remote`/`console` production modules.
          #
          # No `-- --deny warnings`: the `deny`-level `[workspace.lints.clippy]` bar
          # already fails the build on any god-level violation, while rust-level
          # warnings (unused/dead-code in vendored kameo) stay non-blocking until the
          # rust-lints tightening tracked in the #61 tail.
          bombay-clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-features";
            }
          );

          bombay-doc = craneLib.cargoDoc (commonArgs // { inherit cargoArtifacts; });

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
            cargoAuditExtraArgs = "--ignore RUSTSEC-2026-0118 --ignore RUSTSEC-2026-0119";
          };
          bombay-deny = craneLib.cargoDeny { inherit src; };

          bombay-nextest = craneLib.cargoNextest (
            commonArgs
            // {
              inherit cargoArtifacts;
              partitions = 1;
              partitionType = "count";
            }
          );

          # Deterministic corpus-replay + bounded-random fuzz gate. Runs the
          # bolero `check!` targets in the isolated `fuzz/` workspace via plain
          # `cargo test` on the pinned STABLE toolchain (bolero's DefaultEngine
          # needs no nightly — sanitizers, which do, live in the #152 scheduled
          # workflow). `src` already carries the whole tree (parent crate + fuzz
          # sources + corpus); `fuzzCargoArtifacts` vendors the fuzz lock so the
          # build is fully offline/hermetic.
          bombay-fuzz-replay = craneLib.mkCargoDerivation (
            commonArgs
            // {
              cargoVendorDir = fuzzCargoArtifacts;
              cargoArtifacts = null;
              pnameSuffix = "-fuzz-replay";
              buildPhaseCargoCommand = ''
                (cd fuzz && cargo test --no-fail-fast)
              '';
              doInstallCargoArtifacts = false;
              doCheck = false;
            }
          );

          # nextest does NOT run doctests, so verify the doc-comment examples
          # separately with `cargo test --doc` (crane's dedicated wrapper).
          bombay-doctest = craneLib.cargoDocTest (commonArgs // { inherit cargoArtifacts; });

          # Lint the GitHub Actions workflows themselves. actionlint shells out
          # to shellcheck for `run:` steps, so it is on PATH here too. This keeps
          # the CI definition under the same single gate as the code.
          bombay-actionlint =
            pkgs.runCommandLocal "bombay-actionlint"
              {
                nativeBuildInputs = [
                  pkgs.actionlint
                  pkgs.shellcheck
                ];
              }
              ''
                actionlint ${./.github/workflows}/*.yml
                touch "$out"
              '';

          # ── cesr-parity hygiene gates (card #104) ──

          # Spell-check first-party source + docs. Real typos are fixed in-tree;
          # the allow-list + the one M1-doomed exclusion live in _typos.toml.
          bombay-typos = lintCheck "bombay-typos" [ typos ] "typos --config ${./_typos.toml} ${./.}";

          # flake.nix hygiene: nixfmt-formatted and free of dead bindings.
          bombay-nixfmt = lintCheck "bombay-nixfmt" [ nixfmt ] "nixfmt --check ${./flake.nix}";
          bombay-deadnix = lintCheck "bombay-deadnix" [ deadnix ] "deadnix --fail ${./flake.nix}";

          # The tracked git hooks are shell scripts — lint them.
          bombay-shellcheck = lintCheck "bombay-shellcheck" [ shellcheck ] "shellcheck ${./.githooks}/*";

          # Non-workflow YAML (Dependabot + issue-template config); actionlint
          # already covers the workflow YAML.
          bombay-yaml = lintCheck "bombay-yaml" [
            yamllint
          ] "yamllint -d relaxed ${./.github/dependabot.yml} ${./.github/ISSUE_TEMPLATE/config.yml}";
        };

        # `nix fmt` formats the flake with the same nixfmt the gate checks.
        formatter = nixfmt;

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
            cargo-mutants
            gh
          ];
        };

        # On-demand coverage reports (card #85), NOT gating checks — coverage
        # instrumentation recompiles the world, too slow for the per-push gate.
        # `nix build .#coverage -L` writes a browsable HTML report to ./result.
        #
        # TWO engines, both wired via crane (each brings its own cargo subcommand):
        #   * coverage-llvm — `cargoLlvmCov`; works on EVERY system, region/branch
        #     accurate. Uses the version-matched `llvm-cov`/`llvm-profdata` from
        #     the `llvm-tools` toolchain component (rust-toolchain.toml).
        #     Output: $out/html/index.html.
        #   * coverage-tarpaulin — `cargoTarpaulin`; LINUX-ONLY (ptrace engine; no
        #     Darwin support). Output: $out/tarpaulin-report.html. NOTE: tarpaulin's
        #     ptrace engine hangs on this tokio-multi-threaded / async cucumber
        #     suite (verified: the post-merge run wedged for 40+ min on the test
        #     phase) — so it is a deliberate opt-in only, NOT the default.
        #
        # `packages.coverage` is **llvm-cov on every system** — reliable and the
        # one that actually completes here; `coverage-tarpaulin` stays exposed on
        # Linux for anyone who wants the ptrace engine. `--workspace` covers
        # kameo + actors + console + macros; `remote` (libp2p, deleted in M1) is
        # off by default, so it is neither built nor counted. The `testing`
        # feature auto-enables via the self dev-dep, so the cucumber runners build.
        packages =
          let
            # Mutation testing for the rebuilt core (card #112+). On-demand, NOT a
            # gating check — like coverage, cargo-mutants rebuilds+tests once per
            # mutant, far too slow for the per-push gate. Pinned via the flake's
            # nixpkgs input (never `nix run nixpkgs#…`) so the run is reproducible.
            # `nix build .#mutants -L` writes the mutants.out report to ./result and
            # FAILS the build if any mutant survives (zero-survivors is the bar).
            mutants = craneLib.mkCargoDerivation (
              commonArgs
              // {
                inherit cargoArtifacts;
                pnameSuffix = "-mutants";
                nativeBuildInputs = commonArgs.nativeBuildInputs ++ [ cargo-mutants ];
                buildPhaseCargoCommand = ''
                  cargo mutants --package bombay-core --no-shuffle --colors never --output "$out"
                '';
                doInstallCargoArtifacts = false;
                doCheck = false;
              }
            );
            covLlvm = craneLib.cargoLlvmCov (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoLlvmCovCommand = "test";
                cargoLlvmCovExtraArgs = "--workspace --html --output-dir $out";
              }
            );
            covTarpaulin = craneLib.cargoTarpaulin (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoTarpaulinExtraArgs = "--skip-clean --workspace --out Html --output-dir $out";
              }
            );
          in
          {
            inherit mutants;
            coverage-llvm = covLlvm;
            coverage = covLlvm;
          }
          // lib.optionalAttrs stdenv.isLinux {
            coverage-tarpaulin = covTarpaulin;
          };
      }
    );
}
