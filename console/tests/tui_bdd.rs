//! Cucumber harness for the `kameo_console` TUI helpers.
//!
//! cucumber 0.23's `harness = false` + libtest-writer path does NOT implement
//! `cargo nextest`'s `--list` test-enumeration protocol (its `Cli` has no
//! `--list`; nextest's `<bin> --list --format terse` aborts with exit 2). Since
//! `nix flake check` runs `cargoNextest` across the whole workspace, the runner
//! must be a STANDARD libtest test so nextest can enumerate it as one function.
//!
//! Task 8 (card #76): wires `detect_deadlocks` example scenarios; tui.feature
//! is now fully wired — the name-prefix filter is removed and `fail_on_skipped`
//! ensures every scenario is covered with no silent gaps.

mod steps;

use cucumber::World;
use steps::tui::TuiWorld;

#[tokio::test(flavor = "multi_thread")]
async fn tui_features() {
    // `with_default_cli` stops cucumber from parsing the process argv. Without
    // it, cucumber's clap `Cli` aborts on the libtest flags nextest injects
    // (`--list`, `--exact`, `--format terse`) with exit code 2.
    //
    // tui.feature is fully wired — run the whole file, failing on any skipped
    // or undefined scenario so false-greens cannot sneak in.
    TuiWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            // Anchor to CARGO_MANIFEST_DIR (= the `console` crate dir): nextest
            // does not guarantee the test cwd, so a bare relative path makes
            // cucumber fail with "Could not read path" under the nix-sandbox
            // `cargoNextest`. `/..` climbs from `console/` to the workspace root.
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/console/tui.feature"
            ),
            |_, _, _| true,
        )
        .await;
}
