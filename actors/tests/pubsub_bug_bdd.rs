//! Live-defect probe for `@bug:actors/src/pubsub.rs:125` — the Spawned-delivery
//! dead-subscriber leak.
//!
//! Both `pubsub.feature` (~line 180) and `pubsub.properties.feature` (~line 76)
//! carry a `@bug:actors/src/pubsub.rs:125` scenario asserting the DESIRED
//! behaviour: a Spawned-delivery subscriber whose actor has stopped is eventually
//! pruned from the subscriber set. Those scenarios are excluded from the green
//! `pubsub_bdd` / `pubsub_props_bdd` runners (their `!t.starts_with("bug")`
//! filter), because the desired behaviour does NOT hold today.
//!
//! Defect: `pubsub.rs:125-131` — under `DeliveryStrategy::Spawned` (and
//! `SpawnedWithTimeout`) the per-subscriber delivery is `tokio::spawn`ed and its
//! `Result` is DISCARDED, so a `SendError::ActorNotRunning` is never observed and
//! the dead subscriber is never pruned (the inline strategies prune on
//! ActorNotRunning / ActorStopped at `:137-147`).
//!
//! This probe asserts the CURRENT (buggy) state directly through the real SUT: a
//! stopped Spawned subscriber is STILL present after a publish. It PASSES TODAY
//! (the leak is live) and will START FAILING the moment `pubsub.rs:125` is fixed
//! to prune — at which point the matching `@bug` cucumber scenarios become the
//! green spec and this probe should be deleted. Do NOT weaken it.

#[path = "steps/pubsub.rs"]
mod pubsub;

use pubsub::spawned_dead_subscriber_remains;

#[tokio::test(flavor = "multi_thread")]
async fn bug_pubsub_125_spawned_dead_subscriber_not_pruned() {
    // `spawned_dead_subscriber_remains()` drives the SUT exactly like the @bug
    // scenario: spawn a Spawned PubSub, subscribe S, stop S, publish, then poll
    // membership. It returns `true` while the leak is live (S still present),
    // `false` once S is pruned (bug fixed).
    let still_present = spawned_dead_subscriber_remains().await;
    assert!(
        still_present,
        "REGRESSION-INVERSE: pubsub.rs:125 appears FIXED — the stopped Spawned \
         subscriber was pruned. Move the @bug scenarios into the green runners \
         and delete this probe."
    );
}
