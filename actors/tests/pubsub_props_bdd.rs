//! Cucumber runner for `actors/pubsub.properties.feature` — the property/model
//! laws layered on the `bombay_actors::pubsub::PubSub<M>` example scenarios.
//!
//! Shares the `PubSubWorld` + step definitions in `steps/pubsub.rs`. Each
//! `@property`/`@model` law binds to a `When` step that runs a documented bounded
//! boundary-loop hitting the `# GEN:` boundaries with an INDEPENDENT oracle
//! (never calling the SUT to compute the expectation) — see
//! `docs/testing/README.md` §4 (async + global-state laws cannot be hosted inside
//! a sync `proptest!`).
//!
//! `.max_concurrent_scenarios(1)`: the @model concurrent law and the shared
//! `FILTER_CALLS` counter require serialised scenarios. The @model
//! @linearizability law still uses real overlap (`tokio::spawn` + `Barrier`)
//! WITHIN the law.
//!
//! The @bug:actors/src/pubsub.rs:125 property is filtered OUT here; the live
//! defect is pinned by the direct probe in `pubsub_bug_bdd.rs`.

#[path = "steps/pubsub.rs"]
mod pubsub;

use cucumber::World;
use pubsub::PubSubWorld;

#[tokio::test(flavor = "multi_thread")]
async fn pubsub_property_features() {
    PubSubWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/actors/pubsub.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
