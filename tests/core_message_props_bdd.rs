//! Cucumber runner for core/message.properties.feature — the @property/@model
//! laws over single-writer ordering, forward round-trips, and ask-vs-tell error
//! routing for the `src/message.rs` SUT.
//!
//! Shares the `MessageWorld` + step definitions with `core_message_bdd.rs` (the
//! example feature). Standard `#[tokio::test(flavor = "multi_thread")]` libtest
//! function (no `harness = false`) so nextest's `--list` enumerates it; built
//! only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the property laws spawn many actors and the
//! tell-error law observes per-actor `on_panic`; serializing scenarios keeps the
//! laws deterministic. message.properties.feature has NO @bug scenarios; the
//! filter predicate is kept identical to the other core runners.

#[path = "core_steps/message.rs"]
mod message;

use cucumber::World;
use message::MessageWorld;

#[tokio::test(flavor = "multi_thread")]
async fn message_props_features() {
    MessageWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/message.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
