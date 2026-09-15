{
  description = "bombay — typed local actor runtime composition";
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
  };
  outputs =
    {
      self,
      nixpkgs,
      utils,
      crane,
      fenix,
      ...
    }:
    utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Nix and rustup resolve the same pinned toolchain declaration.
        toolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-mvUGEOHYJpn3ikC5hckneuGixaC+yGrkMM/liDIDgoU=";
        };
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        src =
          let
            withoutCargoTargets = pkgs.lib.fileset.fromSource (
              pkgs.lib.cleanSourceWith {
                name = "bombay-source-without-cargo-targets";
                src = ./.;
                filter = path: type: type != "directory" || baseNameOf path != "target";
              }
            );
            sourceFiles = pkgs.lib.fileset.unions [
              (craneLib.fileset.commonCargoSources ./.)
              (pkgs.lib.fileset.fileFilter (
                file:
                file.hasExt "stderr"
                || file.hasExt "md"
                || file.hasExt "json"
                || file.hasExt "yml"
                || file.hasExt "yaml"
              ) ./.)
              ./.cargo/mutants.toml
              ./.config/nextest.toml
              (pkgs.lib.fileset.maybeMissing ./mutants-baseline.json)
            ];
            buildOutputs = pkgs.lib.fileset.unions [
              (pkgs.lib.fileset.maybeMissing ./.direnv)
              (pkgs.lib.fileset.maybeMissing ./.serena)
              (pkgs.lib.fileset.maybeMissing ./mutants.out)
              (pkgs.lib.fileset.maybeMissing ./mutants.out.old)
            ];
          in
          pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.difference (pkgs.lib.fileset.intersection sourceFiles withoutCargoTargets) buildOutputs;
          };
        commonArgs = {
          inherit src;
          pname = "bombay-workspace";
          version = "0.1.0";
          strictDeps = true;
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # Miri and fuzzing stay opt-in so the default shell remains stable.
        miriToolchain =
          (fenix.packages.${system}.toolchainOf {
            channel = "nightly";
            date = "2026-06-15";
            sha256 = "sha256-oXipquOa/9M0uuo8wGuRaY2+ZqLGywZOOnRK05Mm0a0=";
          }).withComponents
            [
              "cargo"
              "rustc"
              "rust-src"
              "rust-std"
              "miri"
            ];
      in
      {
        checks = {
          bombay-build = craneLib.cargoBuild (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--workspace --all-targets";
            }
          );
          bombay-test = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--workspace --all-targets";
            }
          );
          bombay-fmt = craneLib.cargoFmt commonArgs;
          bombay-clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--workspace --all-targets -- --deny warnings";
            }
          );
          bombay-doc = craneLib.cargoDoc (
            commonArgs
            // {
              inherit cargoArtifacts;
              env.RUSTDOCFLAGS = "--deny warnings";
              cargoDocExtraArgs = "--workspace --no-deps";
            }
          );
          bombay-doctest = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              buildPhaseCargoCommand = "cargo test --locked --workspace --doc";
            }
          );
          bombay-machine-loom = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              RUSTFLAGS = "--cfg loom";
              buildPhaseCargoCommand = "cargo test --locked -p bombay-machine --lib --release";
            }
          );
          bombay-entity-loom = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              RUSTFLAGS = "--cfg bombay_entity_loom";
              buildPhaseCargoCommand = "cargo test --locked -p bombay-rs --test entity_loom_local --release";
            }
          );
          bombay-observe-loom = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              RUSTFLAGS = "--cfg loom";
              LOOM_MAX_PREEMPTIONS = "3";
              buildPhaseCargoCommand = "cargo test --locked -p observe-tests --lib --release";
            }
          );
          bombay-panic-unwind = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              RUSTFLAGS = "-C panic=unwind";
              buildPhaseCargoCommand = "cargo test --locked -p bombay-rs drops_driver_before_exactly_one_terminal_classification";
            }
          );
          bombay-panic-abort-rejected = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              buildPhaseCargoCommand = ''
                set +e
                output="$(RUSTFLAGS='-C panic=abort' cargo check --locked -p bombay-rs 2>&1)"
                status="$?"
                set -e
                test "$status" -ne 0
                printf '%s\n' "$output" | grep -F "bombay requires panic=unwind to classify actor panics and complete terminal retirement"
              '';
            }
          );
          bombay-example-counter = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              buildPhaseCargoCommand = ''
                cargo test --locked -p bombay-example-counter
                cargo run --locked -p bombay-example-counter
              '';
            }
          );
          bombay-example-actor-templates = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              buildPhaseCargoCommand = ''
                cargo test --locked -p bombay-example-actor-templates
                cargo run --locked -p bombay-example-actor-templates
              '';
            }
          );
          bombay-example-application-topology = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              buildPhaseCargoCommand = ''
                cargo test --locked -p bombay-example-application-topology
                cargo run --locked -p bombay-example-application-topology
              '';
            }
          );
          bombay-example-supervision = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              buildPhaseCargoCommand = ''
                cargo test --locked -p bombay-example-supervision
                cargo run --locked -p bombay-example-supervision
              '';
            }
          );
          bombay-example-worker-pool = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              buildPhaseCargoCommand = ''
                cargo test --locked -p bombay-example-worker-pool
                cargo run --locked -p bombay-example-worker-pool
              '';
            }
          );
          bombay-example-axum = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              # The executable is a long-running HTTP server; its deterministic
              # router and Behavior boundary are exercised by package tests.
              buildPhaseCargoCommand = "cargo test --locked -p bombay-example-axum";
            }
          );
        };

        packages = rec {
          default = self.checks.${system}.bombay-build;
          coverage = craneLib.cargoLlvmCov (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoLlvmCovCommand = "test";
              cargoLlvmCovExtraArgs = "--workspace --all-targets --html --output-dir $out";
            }
          );
          # Expensive verification remains available without slowing the
          # required `nix flake check` path.
          mutants = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              pnameSuffix = "-mutants";
              nativeBuildInputs = [
                pkgs.cargo-mutants
                pkgs.cargo-nextest
              ];
              buildPhaseCargoCommand = ''
                set -o pipefail
                PROPTEST_CASES=64 cargo mutants \
                  --package bombay-rs \
                  --test-tool nextest --no-shuffle --colors never \
                  --minimum-test-timeout 180 \
                  --output "$out" -- --profile mutants || true
                cargo run --release -p mutants-gate -- \
                  check "$out/mutants.out" "$PWD/mutants-baseline.json" \
                  | tee "$out/mutants-gate-report.txt"
              '';
              doInstallCargoArtifacts = false;
              doCheck = false;
            }
          );
          # Copy result/mutants-baseline.json into the repository only after
          # reviewing the complete sweep.
          mutants-sweep = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              pnameSuffix = "-mutants-sweep";
              nativeBuildInputs = [
                pkgs.cargo-mutants
                pkgs.cargo-nextest
              ];
              buildPhaseCargoCommand = ''
                PROPTEST_CASES=64 cargo mutants \
                  --package bombay-rs --package bombay-engine --package bombay-machine \
                  --test-tool nextest --no-shuffle --colors never \
                  --minimum-test-timeout 180 \
                  --output "$out" -- --profile mutants || true
                cargo run --release -p mutants-gate -- \
                  emit-baseline "$out/mutants.out" > "$out/mutants-baseline.json"
                cp -f "$out/mutants.out/missed.txt" "$out/missed.txt" 2>/dev/null || true
                cp -f "$out/mutants.out/timeout.txt" "$out/timeout.txt" 2>/dev/null || true
              '';
              doInstallCargoArtifacts = false;
              doCheck = false;
            }
          );
          # On-demand Criterion and allocation verification.
          performance = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              pnameSuffix = "-performance";
              buildPhaseCargoCommand = ''
                cargo bench --locked -p bombay-rs --bench entity_directory
                cargo bench --locked -p bombay-rs --bench entity_lifecycle
                cargo bench --locked -p bombay-rs --bench machine_executor
                mkdir -p "$out"
                cp -R target/criterion "$out/criterion"
                {
                  rustc --version --verbose
                  cargo --version --verbose
                  uname -a
                } > "$out/environment.txt"
              '';
              doInstallCargoArtifacts = false;
              doCheck = false;
            }
          );
        };

        formatter = pkgs.nixfmt;

        devShells.default = craneLib.devShell {
          checks = self.checks.${system};
          packages = with pkgs; [
            toolchain
            fenix.packages.${system}.rust-analyzer
            bacon
            cargo-edit
            cargo-expand
            cargo-llvm-cov
            cargo-nextest
            cargo-mutants
            cowsay
            figlet
            gh
            lolcat
            nixfmt
            taplo
            tree
          ];
          shellHook = ''
            REPO_NAME=$(basename "$PWD")
            DISPLAY_NAME=$(printf '%s' "$REPO_NAME" | awk '{ print toupper(substr($0, 1, 1)) substr($0, 2) }')

            figlet -f doom "$DISPLAY_NAME" | lolcat -a -d 2
            cowsay -f dragon-and-cow "Welcome to $DISPLAY_NAME on ${system}" | lolcat
            printf '  %s\n' "$(rustc --version)" "$(cargo --version)" "$(nix --version)"
          '';
        };

        # `nix develop .#miri` — the MIRI lane's toolchain, on demand.
        devShells.miri = pkgs.mkShell {
          packages = [ miriToolchain ];
          shellHook = ''
            echo "bombay MIRI shell — nightly, on-demand only."
            echo "  cargo miri setup"
            echo "  MIRIFLAGS=\"-Zmiri-many-seeds=0..8\" cargo miri test -p bombay-rs"
          '';
        };

        # `nix develop .#fuzz` — coverage-guided Engine and Observe operations.
        devShells.fuzz = pkgs.mkShell {
          packages = [
            miriToolchain
            pkgs.cargo-fuzz
          ];
          shellHook = ''
            echo "bombay fuzz shell — nightly, on-demand only."
            echo "  cd crates/bombay-engine/fuzz && cargo fuzz run causal_turns"
            echo "  cd crates/observe-fuzz && cargo fuzz run ops"
          '';
        };
      }
    );
}
