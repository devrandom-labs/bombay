//! Cucumber runner for `actors/pubsub.feature` — the example scenarios for the
//! `bombay_actors::pubsub::PubSub<M>` SUT (broadcast pub/sub with per-subscriber
//! filters under each `DeliveryStrategy`), driven against REAL SPAWNED ACTORS.
//!
//! Shares the `PubSubWorld` + step definitions in `steps/pubsub.rs`. Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest's `--list` enumerates it; the `testing` feature
//! (self dev-dependency in Cargo.toml) gates the `CountSubscribers` /
//! `ContainsSubscriber` queries the @lifecycle scenarios use to inspect the
//! pubsub's private `subscribers` map.
//!
//! `.max_concurrent_scenarios(1)`: the full-mailbox / @timing scenarios park real
//! handlers and measure wall-clock bounds, and the shared-counter filter reads a
//! process-global atomic, so serializing scenarios keeps the settle/timeout
//! windows deterministic. The @linearizability scenarios still use real overlap
//! (`tokio::spawn` + `Barrier`) WITHIN each scenario.
//!
//! The @bug:actors/src/pubsub.rs:125 scenario is filtered OUT here (the
//! `!t.starts_with("bug")` predicate); the live defect is pinned by the direct
//! probe in `pubsub_bug_bdd.rs`.

#[path = "steps/pubsub.rs"]
mod pubsub;

use cucumber::World;
use pubsub::PubSubWorld;

#[tokio::test(flavor = "multi_thread")]
async fn pubsub_features() {
    PubSubWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/pubsub.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
