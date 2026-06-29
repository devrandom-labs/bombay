//! Cucumber runner for core/request_ask.properties.feature — the @property/@model
//! laws over reply-timeout decision and concurrent per-caller reply isolation for
//! the `src/request/ask.rs` SUT.
//!
//! Shares the `AskWorld` + step definitions with `core_request_ask_bdd.rs` (the
//! example feature). Standard `#[tokio::test(flavor = "multi_thread")]` libtest
//! function (no `harness = false`) so nextest's `--list` enumerates it; built
//! only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the @property laws each stand up a dedicated
//! paused current-thread runtime per boundary case and the @model laws spawn
//! many actors under a barrier; serializing keeps the laws deterministic.
//! request_ask.properties.feature has NO @bug scenarios; the filter predicate is
//! kept identical to the other core runners.

#[path = "core_steps/request_ask.rs"]
mod request_ask;

use cucumber::World;
use request_ask::AskWorld;

#[tokio::test(flavor = "multi_thread")]
async fn request_ask_props_features() {
    AskWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/request_ask.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
