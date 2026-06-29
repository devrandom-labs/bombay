//! Cucumber runner for core/supervision.feature — the example scenarios for the
//! `src/supervision.rs` SUT (Erlang-style supervision): `RestartPolicy`
//! (Permanent/Transient/Never), `SupervisionStrategy` (OneForOne/OneForAll/
//! RestForOne), the restart-intensity sliding window, and the `should_restart`
//! decision on `ErasedChildSpec` (src/links.rs:226-265).
//!
//! Shares the `SupervisionWorld` + step definitions with
//! `core_supervision_props_bdd.rs` (the @property/@model laws). Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (no
//! `harness = false`) so nextest's `--list` enumerates it; built only with the
//! `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the strategy scenarios spawn real supervisor
//! + child actor trees and observe restarts under bounded settle; serializing
//! keeps the bounded waits deterministic.
//!
//! supervision.feature has NO @bug scenarios; the standard `bug*`-tag filter is
//! kept identical to the other core runners.

#[path = "core_steps/supervision.rs"]
mod supervision;

use cucumber::World;
use supervision::SupervisionWorld;

#[tokio::test(flavor = "multi_thread")]
async fn supervision_features() {
    SupervisionWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/supervision.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
