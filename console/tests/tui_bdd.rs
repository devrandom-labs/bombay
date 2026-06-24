//! Cucumber harness for the `kameo_console` TUI helpers.
//!
//! cucumber 0.23's `harness = false` + libtest-writer path does NOT implement
//! `cargo nextest`'s `--list` test-enumeration protocol (its `Cli` has no
//! `--list`; nextest's `<bin> --list --format terse` aborts with exit 2). Since
//! `nix flake check` runs `cargoNextest` across the whole workspace, the runner
//! must be a STANDARD libtest test so nextest can enumerate it as one function.
//!
//! Task 4 (card #76): wires `fmt_ago`, `fmt_uptime`, `short_type_name`, and
//! `spark_height` scenario outlines. `filter_run_and_exit` now covers all five
//! named Scenario Outlines; later tasks broaden the filter further.

mod steps;

use cucumber::World;
use steps::tui::TuiWorld;

#[tokio::test(flavor = "multi_thread")]
async fn tui_features() {
    // `with_default_cli` stops cucumber from parsing the process argv. Without
    // it, cucumber's clap `Cli` aborts on the libtest flags nextest injects
    // (`--list`, `--exact`, `--format terse`) with exit code 2.
    TuiWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit("../tests/features/console/tui.feature", |_, _, scenario| {
            ["fmt_short", "fmt_ago", "fmt_uptime", "short_type_name", "spark_height"]
                .iter()
                .any(|p| scenario.name.starts_with(p))
        })
        .await;
}
