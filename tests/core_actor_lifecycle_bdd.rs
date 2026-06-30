//! Cucumber runner for core/actor_lifecycle.feature — the example scenarios for
//! the bombay core actor lifecycle (`src/actor.rs`, `src/actor/spawn.rs`,
//! `src/actor/kind.rs`): the `Actor` trait's lifecycle hooks (`on_start` /
//! `on_panic` / `on_link_died` / `on_stop`), the run-loop in
//! `run_actor_lifecycle`, the startup-buffer replay in `ActorBehaviour`, and the
//! `Spawn` extension trait's spawn variants — driven against REAL SPAWNED ACTORS.
//!
//! Shares the `LifecycleWorld` + step definitions with
//! `core_actor_lifecycle_props_bdd.rs` (the @property/@model laws). Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (no
//! `harness = false`) so nextest's `--list` enumerates it; built only with the
//! `testing` feature (see `required-features` in Cargo.toml).
//!
//! actor_lifecycle.feature has NO @bug scenarios (the `@bug:<file:line>` line in
//! the file header is an authoring-rule COMMENT, not a scenario tag); the filter
//! predicate is the standard `bug*`-tag drop kept identical to the other core
//! runners.

#[path = "core_steps/actor_lifecycle.rs"]
mod actor_lifecycle;

use actor_lifecycle::LifecycleWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn actor_lifecycle_features() {
    LifecycleWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/actor_lifecycle.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
