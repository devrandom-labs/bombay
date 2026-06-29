//! Cucumber runner for core/error.feature — the SendError algebra, stop/panic
//! reason classifiers, PanicError downcast + (lossy) serde, and the local-only
//! RegistryError failure domains.
//!
//! Standard `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest's `--list` enumerates it; built only with the
//! `testing` feature (see `required-features` in Cargo.toml). `error.feature`
//! has NO @bug scenarios, but the filter predicate is kept identical to the
//! actor_id runner for consistency (drops any `bug*` tag).

#[path = "core_steps/error.rs"]
mod error;

use cucumber::World;
use error::ErrorWorld;

#[tokio::test(flavor = "multi_thread")]
async fn error_features() {
    ErrorWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/error.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
