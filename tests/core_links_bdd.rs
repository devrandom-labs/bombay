//! Cucumber runner for core/links.feature — the example scenarios for the
//! `src/links.rs` SUT: the per-actor `Links` registry (parent / sibblings /
//! children) and its link/notification machinery — who is notified on death,
//! with or without the dying actor's `mailbox_rx`, and the `parent_shutdown`
//! Release/Acquire ordering that prevents the supervisor shutdown deadlock.
//!
//! Shares the `LinksWorld` + step definitions with `core_links_props_bdd.rs`
//! (the @property/@model laws). Standard `#[tokio::test(flavor =
//! "multi_thread")]` libtest function (no `harness = false`) so nextest's
//! `--list` enumerates it; built only with the `testing` feature (see
//! `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the death-notification scenarios spawn real
//! actors and drive timing-sensitive notify fan-outs under bounded settle;
//! serializing keeps the bounded waits deterministic.
//!
//! links.feature has NO @bug scenarios; the standard `bug*`-tag filter is kept
//! identical to the other core runners.

#[path = "core_steps/links.rs"]
mod links;

use cucumber::World;
use links::LinksWorld;

#[tokio::test(flavor = "multi_thread")]
async fn links_features() {
    LinksWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/links.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
