//! Cucumber runner for core/reply.feature — the example scenarios for the
//! `src/reply.rs` SUT: the `Reply` trait conversions (to_result / into_any_err /
//! into_value / downcast_ok / downcast_err), the `Result<T,E>` and
//! `impl_infallible_reply!` blanket impls, the single-use `ReplySender`, the
//! `DelegatedReply` marker, and `ForwardedReply` (Forwarded vs Direct ctors and
//! downcast paths).
//!
//! Shares the `ReplyWorld` + step definitions with `core_reply_props_bdd.rs` (the
//! @property/@model laws). Standard `#[tokio::test(flavor = "multi_thread")]`
//! libtest function (no `harness = false`) so nextest's `--list` enumerates it;
//! built only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! `reply.feature` has NO @bug scenarios, but the filter predicate is kept
//! identical to the actor_id/error/message runners for consistency (drops any
//! `bug*` tag). The DelegatedReply / Forwarded(Ok) into_value / wrong-type
//! downcast scenarios assert DOCUMENTED panics (caught in-step), not defects.

#[path = "core_steps/reply.rs"]
mod reply;

use cucumber::World;
use reply::ReplyWorld;

#[tokio::test(flavor = "multi_thread")]
async fn reply_features() {
    ReplyWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/reply.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
