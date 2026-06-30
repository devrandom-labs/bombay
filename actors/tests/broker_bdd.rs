//! Cucumber runner for `actors/broker.feature` — the example scenarios for the
//! `bombay_actors::broker::Broker<M>` SUT (glob-topic pub/sub under each
//! `DeliveryStrategy`), driven against REAL SPAWNED ACTORS.
//!
//! Shares the `BrokerWorld` + step definitions in `steps/broker.rs`. Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest's `--list` enumerates it; the `testing` feature
//! (self dev-dependency in Cargo.toml) gates the `CountSubscriptions` /
//! `HasPatternKey` queries the @lifecycle / @boundary scenarios use to inspect
//! the broker's private `subscriptions` map.
//!
//! `.max_concurrent_scenarios(1)`: the full-mailbox / @timing scenarios park real
//! handlers and measure wall-clock bounds, so serializing scenarios keeps the
//! settle/timeout windows deterministic. The @linearizability scenarios still use
//! real overlap (`tokio::spawn` + `Barrier`) WITHIN each scenario.

#[path = "steps/broker.rs"]
mod broker;

use broker::BrokerWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn broker_features() {
    BrokerWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/broker.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
