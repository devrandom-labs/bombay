//! Cucumber runner for core/actor_lifecycle.properties.feature — the
//! @property/@model laws over the default `on_panic`/`on_link_died` hook
//! decisions, the startup-buffer replay ordering, and spawn-variant equivalence
//! for the kameo core actor lifecycle (`src/actor.rs`, `src/actor/spawn.rs`,
//! `src/actor/kind.rs`).
//!
//! Shares the `LifecycleWorld` + step definitions with
//! `core_actor_lifecycle_bdd.rs` (the example feature). Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (no
//! `harness = false`) so nextest's `--list` enumerates it; built only with the
//! `testing` feature (see `required-features` in Cargo.toml).
//!
//! actor_lifecycle.properties.feature has NO @bug scenarios; the filter
//! predicate is kept identical to the other core runners.

#[path = "core_steps/actor_lifecycle.rs"]
mod actor_lifecycle;

use actor_lifecycle::LifecycleWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn actor_lifecycle_props_features() {
    LifecycleWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/actor_lifecycle.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
