//! Cucumber runner for `actors/message_bus.feature` — the example scenarios for
//! the `bombay_actors::message_bus::MessageBus` SUT (type-routed broadcast under
//! each `DeliveryStrategy`), driven against REAL SPAWNED ACTORS.
//!
//! Shares the `MessageBusWorld` + step definitions in `steps/message_bus.rs`.
//! Standard `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest's `--list` enumerates it; the `testing` feature
//! (self dev-dependency in Cargo.toml) gates the `CountRegistrations` query the
//! @lifecycle scenarios use to inspect the bus's private `subscriptions`.
//!
//! `.max_concurrent_scenarios(1)`: the full-mailbox / @timing scenarios park real
//! handlers and measure wall-clock bounds, so serializing scenarios keeps the
//! settle/timeout windows deterministic. The @linearizability scenarios still use
//! real overlap (`tokio::spawn` + `Barrier`) WITHIN each scenario.

#[path = "steps/message_bus.rs"]
mod message_bus;

use cucumber::World;
use message_bus::MessageBusWorld;

#[tokio::test(flavor = "multi_thread")]
async fn message_bus_features() {
    MessageBusWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/message_bus.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
