//! Cucumber runner for `actors/scheduler.properties.feature` — the Phase-2
//! property/model laws for `bombay_actors::scheduler::Scheduler`, driven against
//! REAL SPAWNED ACTORS.
//!
//! Shares the `SchedulerWorld` + step definitions in `steps/scheduler.rs`.
//! Standard `#[tokio::test(flavor = "multi_thread")]` libtest function (NOT
//! `harness = false`) so nextest enumerates it. Each law's `Then` drives the SUT
//! over the `# GEN:` boundary set inside the paused-clock runtime and compares
//! against an INDEPENDENT oracle (`oracle_timeout` / `oracle_interval_ticks`,
//! derived from tokio's documented timer semantics — not from the SUT). A sync
//! `proptest!` cannot `block_on` inside cucumber's runtime AND the timing laws
//! need a deterministic paused clock, so the laws use a DOCUMENTED bounded
//! boundary-loop over the GEN-named values (the README's Phase-3 §4 fallback).
//!
//! `.max_concurrent_scenarios(1)`: each law stands up many Schedulers + targets
//! across its boundary cross-product, so serialization keeps them deterministic.

#[path = "steps/scheduler.rs"]
mod scheduler;

use cucumber::World;
use scheduler::SchedulerWorld;

#[tokio::test(flavor = "multi_thread")]
async fn scheduler_property_features() {
    SchedulerWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/scheduler.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
