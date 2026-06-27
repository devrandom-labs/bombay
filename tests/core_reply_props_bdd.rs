//! Cucumber runner for core/reply.properties.feature — the @property/@model laws
//! over the reply machinery: Result Ok/Err round-trips, ReplySender::send wire
//! mapping, infallible-type identity, ForwardedReply from_*/into_value
//! round-trips, and N concurrent forwarded asks with no cross-talk.
//!
//! Shares the `ReplyWorld` + step definitions with `core_reply_bdd.rs` (the
//! example feature). Standard `#[tokio::test(flavor = "multi_thread")]` libtest
//! function (no `harness = false`) so nextest's `--list` enumerates it; built
//! only with the `testing` feature (see `required-features` in Cargo.toml). The
//! reply property feature has NO @bug scenarios, but the filter predicate is kept
//! identical to the actor_id/error/message runners for consistency.

#[path = "core_steps/reply.rs"]
mod reply;

use cucumber::World;
use reply::ReplyWorld;

#[tokio::test(flavor = "multi_thread")]
async fn reply_props_features() {
    ReplyWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/reply.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
