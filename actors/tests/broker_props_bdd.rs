//! Cucumber runner for `actors/broker.properties.feature` — the Phase-2
//! property/model laws for `bombay_actors::broker::Broker<M>`, driven against
//! REAL SPAWNED ACTORS.
//!
//! Shares the `BrokerWorld` + step definitions in `steps/broker.rs`. Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest enumerates it. `@property` routing law runs an
//! inline `proptest!` over the `# GEN:` boundary cross-product with the glob
//! crate as an INDEPENDENT oracle; the async / global-state laws use documented
//! bounded boundary-loops over the GEN-named values (see `steps/broker.rs`).
//!
//! `.max_concurrent_scenarios(1)`: each law spawns brokers + recipients and some
//! park real handlers / measure timing, so serialization keeps them deterministic.

#[path = "steps/broker.rs"]
mod broker;

use broker::BrokerWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn broker_property_features() {
    BrokerWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/broker.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
