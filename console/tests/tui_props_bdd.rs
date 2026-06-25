//! Cucumber harness for the `kameo_console` TUI helpers' laws.
//!
//! Mirrors `tui_bdd.rs` (a STANDARD libtest test, no `harness = false`, so
//! `cargo nextest --list` can enumerate it) but points at the laws file
//! `tui.properties.feature`. This runner wires the WHOLE file: all twelve
//! proptest-backed `@property` scenarios PLUS the `@model` scenario
//! (detect_deadlocks ≡ an independent successor-chase cycle finder, Task 10).
//!
//! The tag filter is gone — the run predicate is `|_, _, _| true`, so every
//! scenario executes and `fail_on_skipped` turns any unwired line into a hard
//! failure (false-green elimination). Step definitions live in
//! `steps/tui_props.rs`.

mod steps;

use cucumber::World;
use steps::tui_props::TuiPropsWorld;

#[tokio::test(flavor = "multi_thread")]
async fn tui_property_laws() {
    TuiPropsWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            // Anchor to CARGO_MANIFEST_DIR (= the `console` crate dir): nextest
            // does not guarantee the test cwd, so a bare relative path makes
            // cucumber fail with "Could not read path" under the nix-sandbox
            // `cargoNextest`. `/..` climbs from `console/` to the workspace root.
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/console/tui.properties.feature"
            ),
            |_, _, _| true,
        )
        .await;
}
