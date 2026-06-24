//! Cucumber harness for the `kameo_console` poller's framing laws.
//!
//! Mirrors `poller_bdd.rs` / `tui_props_bdd.rs` (a STANDARD libtest test, no
//! `harness = false`, so `cargo nextest --list` can enumerate it) but points at
//! `poller.properties.feature`. This is the LAST runner for that file, so it
//! wires the WHOLE file: the run predicate is `|_, _, _| true`, every
//! `@property` scenario executes, and `fail_on_skipped` turns any unwired line
//! into a hard failure (false-green elimination). Step definitions live in
//! `steps/poller_props.rs`.

mod steps;

use cucumber::World;
use steps::poller_props::PollerPropsWorld;

#[tokio::test(flavor = "multi_thread")]
async fn poller_property_laws() {
    PollerPropsWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            "../tests/features/console/poller.properties.feature",
            |_, _, _| true,
        )
        .await;
}
