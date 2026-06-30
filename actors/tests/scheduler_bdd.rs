//! Cucumber runner for `actors/scheduler.feature` — the example scenarios for the
//! `bombay_actors::scheduler::Scheduler` SUT (one-shot `SetTimeout` + repeating
//! `SetInterval` against weak actor refs), driven against REAL SPAWNED ACTORS.
//!
//! Shares the `SchedulerWorld` + step definitions in `steps/scheduler.rs`.
//! Standard `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest's `--list` enumerates it.
//!
//! Every `@timing` scenario drives its Scheduler + target inside a dedicated
//! `start_paused(true)` current-thread runtime on its own OS thread (see
//! `steps/scheduler.rs`), so the delivery counts are deterministic and never
//! depend on wall-clock timing. `.max_concurrent_scenarios(1)` keeps the
//! scenarios serialized; the `@linearizability` scenarios still use real overlap
//! (`tokio::spawn` + `Barrier`) WITHIN their paused runtime.

#[path = "steps/scheduler.rs"]
mod scheduler;

use cucumber::World;
use scheduler::SchedulerWorld;

#[tokio::test(flavor = "multi_thread")]
async fn scheduler_features() {
    SchedulerWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/scheduler.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
