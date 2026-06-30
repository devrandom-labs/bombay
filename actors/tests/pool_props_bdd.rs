//! Cucumber runner for `actors/pool.properties.feature` — the Phase-2
//! property/model laws for `bombay_actors::pool::ActorPool<A>`, driven against
//! REAL SPAWNED ACTORS.
//!
//! Shares the `PoolWorld` + step definitions in `steps/pool.rs`. Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest enumerates it. Each law is a DOCUMENTED bounded
//! boundary-loop over its `# GEN:` boundary set with an INDEPENDENT integer
//! oracle (size/arity/load model written from scratch) — a sync `proptest!` block
//! cannot `block_on` the async, multi-actor SUT inside cucumber's tokio runtime,
//! so per docs/testing/README.md §"Wiring (Phase 3) §4" the bounded loop is the
//! sanctioned fallback (stated explicitly in each step).
//!
//! `.max_concurrent_scenarios(1)`: each law stands up many pools + workers (up to
//! 1000-worker pools) and some measure concurrent overlap, so serializing scenarios
//! keeps them deterministic.

#[path = "steps/pool.rs"]
mod pool;

use cucumber::World;
use pool::PoolWorld;

#[tokio::test(flavor = "multi_thread")]
async fn pool_property_features() {
    PoolWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/pool.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
