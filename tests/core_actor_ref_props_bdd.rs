//! Cucumber runner for core/actor_ref.properties.feature — the @property/@model
//! laws over id-based Eq/Hash/Ord, strong/weak refcount refinement +
//! downgrade/upgrade, per-ask reply isolation, and startup-waiter fan-out for
//! the `src/actor/actor_ref.rs` SUT.
//!
//! Shares the `ActorRefWorld` + step definitions with `core_actor_ref_bdd.rs`
//! (the example feature). Standard `#[tokio::test(flavor = "multi_thread")]`
//! libtest function (no `harness = false`) so nextest's `--list` enumerates it;
//! built only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! actor_ref.properties.feature has NO @bug scenarios; the filter predicate is
//! kept identical to the other core runners.

#[path = "core_steps/actor_ref.rs"]
mod actor_ref;

use actor_ref::ActorRefWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn actor_ref_props_features() {
    ActorRefWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/actor_ref.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
