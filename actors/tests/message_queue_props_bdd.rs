//! Cucumber runner for `actors/message_queue.properties.feature` — the
//! property/model laws over the `bombay_actors::message_queue::MessageQueue` SUT
//! (per-exchange-type routing laws, headers all/any law, dedup-by-id idempotence,
//! prune-iff-ActorNotRunning, concurrent-fanout refinement).
//!
//! Shares the `MessageQueueWorld` + step definitions in `steps/message_queue.rs`
//! (the law steps live in the same module as the example steps, mirroring broker).
//! Standard `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest's `--list` enumerates it.
//! `.max_concurrent_scenarios(1)` keeps the bounded boundary-loops (which stand up
//! many fresh queues + actors) deterministic.
//!
//! The `!t.starts_with("bug")` filter EXCLUDES the two `@bug:591` / `@bug:707`
//! laws, whose `Then`s assert the desired `AmqpError::InvalidRoutingKey` rejection
//! (card #79, variant not yet present). The real defect is reproduced by the
//! separate `message_queue_bug_bdd.rs` probe.

#[path = "steps/message_queue.rs"]
mod message_queue;

use cucumber::World;
use message_queue::MessageQueueWorld;

#[tokio::test(flavor = "multi_thread")]
async fn message_queue_props_features() {
    MessageQueueWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/message_queue.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
