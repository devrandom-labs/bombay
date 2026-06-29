//! Cucumber runner for core/request_tell.properties.feature — the @property/@model
//! laws over try_send capacity, concurrent exactly-once delivery, bounded-send
//! backpressure, and the send_after one-shot lifecycle for the
//! `src/request/tell.rs` SUT.
//!
//! Shares the `TellWorld` + step definitions with `core_request_tell_bdd.rs` (the
//! example feature). Standard `#[tokio::test(flavor = "multi_thread")]` libtest
//! function (no `harness = false`) so nextest's `--list` enumerates it; built
//! only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the laws park real handlers, spawn many actors
//! under barriers, and the send_after laws stand up dedicated paused current-thread
//! runtimes; serializing keeps the laws deterministic. The whole feature is tagged
//! `@phase2` (not a skip signal — every scenario is wired); the filter predicate
//! is kept identical to the other core runners.

#[path = "core_steps/request_tell.rs"]
mod request_tell;

use cucumber::World;
use request_tell::TellWorld;

#[tokio::test(flavor = "multi_thread")]
async fn request_tell_props_features() {
    TellWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/request_tell.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
