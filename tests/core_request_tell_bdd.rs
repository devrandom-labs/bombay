//! Cucumber runner for core/request_tell.feature — the example scenarios for the
//! `src/request/tell.rs` SUT (the `TellRequest` builder: mailbox_timeout,
//! send/try_send/blocking_send, send_after, IntoFuture), driven against REAL
//! SPAWNED ACTORS.
//!
//! Shares the `TellWorld` + step definitions with `core_request_tell_props_bdd.rs`
//! (the @property/@model laws). Standard `#[tokio::test(flavor =
//! "multi_thread")]` libtest function (no `harness = false`) so nextest's
//! `--list` enumerates it; built only with the `testing` feature (see
//! `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: several @timing scenarios stand up a dedicated
//! paused current-thread runtime on a blocking thread, the full/busy-mailbox
//! scenarios park real handlers, and the self-tell scenario installs a per-thread
//! tracing subscriber; serializing scenarios keeps the bounded settle/poll
//! deterministic and avoids the per-thread subscriber leaking across scenarios.
//!
//! `tell` is fire-and-forget — its dead-actor / full-mailbox failure boxes a BARE
//! message, so the `From<SendError<Signal>>` conversion downcasts `<M>` and
//! returns a graceful `ActorNotRunning(M)` / `MailboxFull(M)` (no caller panic,
//! unlike `ask`'s forward variants). request_tell.feature therefore has NO @bug
//! scenarios; the standard `bug*`-tag filter is kept identical to the other core
//! runners.

#[path = "core_steps/request_tell.rs"]
mod request_tell;

use cucumber::World;
use request_tell::TellWorld;

#[tokio::test(flavor = "multi_thread")]
async fn request_tell_features() {
    TellWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/request_tell.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
