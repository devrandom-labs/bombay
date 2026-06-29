//! Cucumber runner for core/links.properties.feature — the @property/@model
//! laws over the `src/links.rs` SUT: sibling fan-out exactness (each of N linked
//! siblings notified exactly once, no mailbox_rx), the per-child shutdown/wait
//! exactness, and the `parent_shutdown` Release/Acquire ordering under any
//! interleaving.
//!
//! Shares the `LinksWorld` + step definitions with `core_links_bdd.rs` (the
//! example feature). Standard `#[tokio::test(flavor = "multi_thread")]` libtest
//! function (no `harness = false`) so nextest's `--list` enumerates it; built
//! only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the laws fan out over boundary-biased N/K and
//! the @model laws spawn many concurrent deaths under a `Barrier`; serializing
//! keeps the bounded settle/poll deterministic. The whole feature is tagged
//! `@phase2` (not a skip signal — every scenario is wired); the filter predicate
//! is kept identical to the other core runners.

#[path = "core_steps/links.rs"]
mod links;

use cucumber::World;
use links::LinksWorld;

#[tokio::test(flavor = "multi_thread")]
async fn links_props_features() {
    LinksWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/links.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
