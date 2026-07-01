//! Cucumber runner for core/actor_id.feature.
//!
//! The `@boundary` decode-rejection scenarios (a slice shorter than 8 bytes,
//! and a truncated buffer through serde `Deserialize`) exercise the fix from
//! card #80: `from_bytes` bounds-checks before slicing `bytes[0..8]`, so a
//! truncated buffer returns `Err(MissingSequenceID)` / serde `invalid_length`
//! instead of panicking. Before the fix these panicked; they now run green in
//! the ordinary cucumber pass (no tag filter).

#[path = "core_steps/actor_id.rs"]
mod actor_id;

use actor_id::ActorIdWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn actor_id_features() {
    ActorIdWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/actor_id.feature"
            ),
            |_, _, _| true,
        )
        .await;
}
