//! Cucumber runner for core/mailbox.feature — the example scenarios for the
//! `src/mailbox.rs` SUT (the bounded/unbounded `Mailbox` mpsc signal channel,
//! the `MailboxSender`/`MailboxReceiver`/`WeakMailboxSender` handles, and the
//! `front: VecDeque<Signal<A>>` restart push-back buffer).
//!
//! Shares the `MailboxWorld` + step definitions with `core_mailbox_props_bdd.rs`
//! (the @property/@model laws). Standard `#[tokio::test(flavor =
//! "multi_thread")]` libtest function (no `harness = false`) so nextest's
//! `--list` enumerates it; built only with the `testing` feature (see
//! `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the @boundary scenarios deliberately FILL a
//! bounded channel and the @linearizability scenarios spawn many concurrent
//! producers under a `Barrier`; serializing scenarios keeps the bounded
//! settle/poll deterministic and prevents one scenario's parked send from
//! starving another's runtime.
//!
//! mailbox.feature has NO @bug scenarios; the standard `bug*`-tag filter is kept
//! identical to the other core runners.

#[path = "core_steps/mailbox.rs"]
mod mailbox;

use cucumber::World;
use mailbox::MailboxWorld;

#[tokio::test(flavor = "multi_thread")]
async fn mailbox_features() {
    MailboxWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/mailbox.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
