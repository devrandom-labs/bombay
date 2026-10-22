# devrandom-labs/bombay

Bombay composes typed Behavior actors with the universal Driver, mailboxes,
addresses, observation, timers and owned actor tasks. The workspace contains
Rust libraries, examples and verification targets. The optional `remote`
feature adds Bombay-owned networking; its complete acceptance and delivery
status is tracked in the static remote actor PRD.

Read [AGENTS.md](../AGENTS.md) before changing the repository. For every feature,
inspect the dependencies selected by Cargo.lock and read that exact Behavior
revision's instructions. rust-toolchain.toml selects Rust; flake.lock and
flake.nix select the shared environment and checks. Do not copy version numbers
or patch assumptions from an older checkout.

Use the pinned Nix environment for every Rust command:

| Task | Command |
| --- | --- |
| Development shell | `nix develop` |
| Workspace build | `nix develop -c cargo build --locked --workspace` |
| Workspace tests | `nix develop -c cargo test --locked --workspace` |
| Formatting | `nix develop -c cargo fmt --all -- --check` |
| Strict lint | `nix develop -c cargo clippy --locked --workspace --all-targets -- -D warnings` |
| Local counter | `nix develop -c cargo run --locked -p bombay-example-counter` |
| Repository gate | `nix flake check -L` |

Default workspace tests do not establish acceptance of every optional feature.
Use the selected PRD and flake checks for feature-specific commands and evidence;
there is no current check count or timing guarantee in this onboarding note.

Before architecture work, read docs/prd-backlog/README.md,
docs/prd-backlog/status.md, the selected docs/prds/ feature record,
docs/module-boundaries.md, docs/runtime-capability-interfaces.md,
docs/driver-law.md and docs/driver-test-strategy.md. Keep decisions, dependency
verification, regressions, measurements and delivery evidence in that feature's
record. A local pass is not a merge or release.

The [codebase map](codebase-map.md) identifies current source owners. Earlier
sandbox installation instructions, measurements and snapshots remain in Git
history; they do not describe the current environment or certify current checks.
