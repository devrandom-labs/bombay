//! Cucumber runner for core/error.properties.feature — the @property/@model
//! laws over the SendError algebra (variant-tag preservation, boxed/downcast
//! round-trip, wrong-type recovery, flatten hoisting).
//!
//! Shares the `ErrorWorld` + step definitions with `core_error_bdd.rs` (the
//! example feature). Standard `#[tokio::test(flavor = "multi_thread")]` libtest
//! function (no `harness = false`) so nextest's `--list` enumerates it; built
//! only with the `testing` feature (see `required-features` in Cargo.toml).
//! The error property feature has NO @bug scenarios, but the filter predicate
//! is kept identical to the actor_id runner for consistency.

#[path = "core_steps/error.rs"]
mod error;

use cucumber::World;
use error::ErrorWorld;

#[tokio::test(flavor = "multi_thread")]
async fn error_props_features() {
    ErrorWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/error.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
