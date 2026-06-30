//! Cucumber runner for `actors/pool.feature` — the example scenarios for the
//! `bombay_actors::pool::ActorPool<A>` SUT (least-connections `Dispatch`, fan-out
//! `Broadcast`, in-place worker replacement on link death), driven against REAL
//! SPAWNED ACTORS.
//!
//! Shares the `PoolWorld` + step definitions in `steps/pool.rs`. Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest's `--list` enumerates it; the `testing` feature
//! (self dev-dependency in Cargo.toml) gates the `PoolSnapshot` / `KillWorker`
//! introspection the @lifecycle scenarios use to observe + drive the pool's
//! private `workers` vec.
//!
//! `.max_concurrent_scenarios(1)`: the @timing large-pool scenario and the
//! busy-worker / replacement scenarios park real handlers and settle on bounded
//! windows, so serializing scenarios keeps those windows deterministic. The
//! @linearizability scenarios still use real overlap (`tokio::spawn` + `Barrier`)
//! WITHIN each scenario.
//!
//! The pool.feature note near line 194 (`@bug:pool.rs:401`) is PROSE, not a real
//! `@bug:` tag — the carrying scenario is tagged `@sequence @review-semantics`. As
//! that note resolves ("the panic arm is unreachable in practice"), the scenario
//! is wired to the SUT's actual current behaviour (all-Ok, no panic), so it runs
//! here in the green runner. No `pool_bug_bdd.rs` is needed.

#[path = "steps/pool.rs"]
mod pool;

use cucumber::World;
use pool::PoolWorld;

#[tokio::test(flavor = "multi_thread")]
async fn pool_features() {
    PoolWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/pool.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
