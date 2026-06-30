//! Cucumber runner for `actors/message_queue.feature` — the example scenarios for
//! the `bombay_actors::message_queue::MessageQueue` SUT (AMQP-style
//! exchange/queue/binding routing), driven against REAL SPAWNED ACTORS.
//!
//! Shares the `MessageQueueWorld` + step definitions in `steps/message_queue.rs`.
//! Standard `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest's `--list` enumerates it; the `testing` feature
//! (self dev-dependency in Cargo.toml) gates the `QueueExists` / `ExchangeExists`
//! / `CountBindings` queries the @lifecycle scenarios use to inspect the queue's
//! private tables.
//!
//! `.max_concurrent_scenarios(1)`: the full-mailbox scenarios park real handlers
//! and the @linearizability scenarios assert exact post-settle counts, so
//! serializing scenarios keeps the windows deterministic. The @linearizability
//! scenarios still use real overlap (`tokio::spawn` + `Barrier`) WITHIN a scenario.
//!
//! The `!t.starts_with("bug")` filter EXCLUDES the three `@bug:707` / `@bug:591`
//! scenarios, whose `Then`s assert the desired `AmqpError::InvalidRoutingKey`
//! rejection — a variant that does not exist yet (card #79). The real defect is
//! reproduced by the separate `message_queue_bug_bdd.rs` probe.

#[path = "steps/message_queue.rs"]
mod message_queue;

use cucumber::World;
use message_queue::MessageQueueWorld;

#[tokio::test(flavor = "multi_thread")]
async fn message_queue_features() {
    MessageQueueWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/message_queue.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
