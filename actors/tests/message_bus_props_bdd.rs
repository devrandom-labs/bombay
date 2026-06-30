//! Cucumber runner for `actors/message_bus.properties.feature` — the
//! property/model laws over the `bombay_actors::message_bus::MessageBus` SUT.
//!
//! Shares nothing with the example runner: the laws build their own buses across
//! the GEN boundary set, so this uses a dedicated `MessageBusPropsWorld` in
//! `steps/message_bus_props.rs`. Standard `#[tokio::test(flavor =
//! "multi_thread")]` libtest function (NOT `harness = false`) so nextest's
//! `--list` enumerates it; `.max_concurrent_scenarios(1)` keeps the bounded
//! boundary-loops (which stand up many actors) deterministic.

#[path = "steps/message_bus_props.rs"]
mod message_bus_props;

use cucumber::World;
use message_bus_props::MessageBusPropsWorld;

#[tokio::test(flavor = "multi_thread")]
async fn message_bus_props_features() {
    MessageBusPropsWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/message_bus.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
