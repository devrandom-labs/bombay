//! Cucumber harness for the `kameo_console` TUI helpers' `@property` laws.
//!
//! Mirrors `tui_bdd.rs` (a STANDARD libtest test, no `harness = false`, so
//! `cargo nextest --list` can enumerate it) but points at the laws file
//! `tui.properties.feature` and wires the proptest-backed `@property`
//! scenarios. The single `@model` scenario (detect_deadlocks ≡ a reference
//! cycle finder) is Task 10 — excluded here by tag so it is filtered out (not
//! counted as skipped) and `fail_on_skipped` stays green.
//!
//! gherkin 0.16 stores tags WITHOUT the leading `@` (parser.rs:470 consumes the
//! `@` literal), so the `@model` tag is the string `"model"`. The closure's 3rd
//! arg is the `gherkin::Scenario`, whose `.tags: Vec<String>` carries the
//! scenario's tags (feature-level tags inherited too) — match on `"model"`.

mod steps;

use cucumber::World;
use steps::tui_props::TuiPropsWorld;

#[tokio::test(flavor = "multi_thread")]
async fn tui_property_laws() {
    TuiPropsWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            "../tests/features/console/tui.properties.feature",
            |_, _, s| !s.tags.iter().any(|t| t == "model"),
        )
        .await;
}
