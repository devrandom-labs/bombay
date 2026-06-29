//! Cucumber runner for core/mailbox.properties.feature — the @property/@model
//! laws over FIFO ordering, bounded-capacity Full boundaries, unbounded
//! never-Full, the push-front drain-before-channel protocol, post-close send
//! return, and concurrent per-sender FIFO refinement for the `src/mailbox.rs`
//! SUT.
//!
//! Shares the `MailboxWorld` + step definitions with `core_mailbox_bdd.rs` (the
//! example feature). Standard `#[tokio::test(flavor = "multi_thread")]` libtest
//! function (no `harness = false`) so nextest's `--list` enumerates it; built
//! only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the laws run `proptest!` loops that fill
//! bounded channels and the @model laws spawn many concurrent senders under a
//! `Barrier`; serializing keeps them deterministic. The whole feature is tagged
//! `@phase2` (not a skip signal — every scenario is wired); the filter predicate
//! is kept identical to the other core runners.

#[path = "core_steps/mailbox.rs"]
mod mailbox;

use cucumber::World;
use mailbox::MailboxWorld;

#[tokio::test(flavor = "multi_thread")]
async fn mailbox_props_features() {
    MailboxWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/mailbox.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
