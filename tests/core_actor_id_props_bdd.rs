//! Cucumber runner for core/actor_id.properties.feature — the @property/@model
//! laws over ActorId byte round-trips, decode rejection, and generation.
//!
//! Shares the `ActorIdWorld` + step definitions with `core_actor_id_bdd.rs`
//! (the example feature). Standard `#[tokio::test]` libtest function (no
//! `harness = false`) so nextest's `--list` enumerates it; built only with the
//! `testing` feature (see `required-features` in Cargo.toml).
//!
//! The `from_bytes rejects any byte string shorter than eight bytes` law
//! asserts Err(MissingSequenceID) over n ∈ [0, 8). It panicked before card #80
//! bounds-checked `from_bytes`; it now runs green in the ordinary pass (no tag
//! filter).

#[path = "core_steps/actor_id.rs"]
mod actor_id;

use actor_id::ActorIdWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn actor_id_props_features() {
    ActorIdWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/actor_id.properties.feature"
            ),
            |_, _, _| true,
        )
        .await;
}
