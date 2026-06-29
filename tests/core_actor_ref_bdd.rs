//! Cucumber runner for core/actor_ref.feature — the example scenarios for the
//! `src/actor/actor_ref.rs` SUT (ask/tell messaging, the alive/dead state
//! machine, strong/weak reference counting, downgrade/upgrade, is_current,
//! identity, startup/shutdown waiters, Recipient/ReplyRecipient erasure, and
//! self link/unlink no-ops) driven against REAL SPAWNED ACTORS.
//!
//! Shares the `ActorRefWorld` + step definitions with
//! `core_actor_ref_props_bdd.rs` (the @property/@model laws). Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (no
//! `harness = false`) so nextest's `--list` enumerates it; built only with the
//! `testing` feature (see `required-features` in Cargo.toml).
//!
//! actor_ref.feature has NO @bug scenarios; the filter predicate is the standard
//! `bug*`-tag drop kept identical to the other core runners.

#[path = "core_steps/actor_ref.rs"]
mod actor_ref;

use actor_ref::ActorRefWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn actor_ref_features() {
    ActorRefWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/actor_ref.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
