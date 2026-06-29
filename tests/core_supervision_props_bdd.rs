//! Cucumber runner for core/supervision.properties.feature — the @property/@model
//! laws over the `src/supervision.rs` SUT: the `should_restart` decision predicate
//! over (policy × exit-kind × reason), the strategy restart-set as an index-set
//! function over spawn order, and the sliding-window intensity counter under any
//! generated failure burst.
//!
//! Shares the `SupervisionWorld` + step definitions with `core_supervision_bdd.rs`
//! (the example feature). Standard `#[tokio::test(flavor = "multi_thread")]`
//! libtest function (no `harness = false`) so nextest's `--list` enumerates it;
//! built only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the laws sweep boundary-biased policy/limit/
//! window/burst spaces and the @property strategy law spawns real actor trees;
//! serializing keeps the bounded settle deterministic. The whole feature is
//! tagged `@phase2` (not a skip signal — every scenario is wired); the filter
//! predicate is kept identical to the other core runners.

#[path = "core_steps/supervision.rs"]
mod supervision;

use cucumber::World;
use supervision::SupervisionWorld;

#[tokio::test(flavor = "multi_thread")]
async fn supervision_props_features() {
    SupervisionWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/supervision.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
