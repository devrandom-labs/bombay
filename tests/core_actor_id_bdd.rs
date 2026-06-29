//! Cucumber runner for core/actor_id.feature, plus the @bug:id.rs probes.
//!
//! The 3 @bug scenarios in actor_id.feature assert the DESIRED behaviour
//! (from_bytes returns Err(MissingSequenceID) on a short slice). The source
//! panics on `bytes[0..8]` (id.rs:140) BEFORE the map_err can run, so those
//! scenarios are excluded from the green cucumber run (the filter predicate
//! drops any @bug tag) and the live defect is pinned by the #[should_panic]
//! tests below instead. They pass GREEN while the bug lives and flip RED the
//! moment fix(actor_id) lands.

#[path = "core_steps/actor_id.rs"]
mod actor_id;

use actor_id::ActorIdWorld;
use cucumber::World;
use kameo::actor::ActorId;

#[tokio::test(flavor = "multi_thread")]
async fn actor_id_features() {
    ActorIdWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/actor_id.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}

#[tokio::test]
#[should_panic(expected = "out of range for slice")]
async fn bug_id_140_from_bytes_panics_on_short_slice() {
    let _ = ActorId::from_bytes(&[0u8; 4]);
}

#[tokio::test]
#[should_panic(expected = "out of range for slice")]
async fn bug_id_140_from_bytes_panics_on_empty_slice() {
    let _ = ActorId::from_bytes(&[]);
}

#[tokio::test]
#[should_panic(expected = "out of range for slice")]
async fn bug_id_218_deserialize_panics_on_short_buffer() {
    // serde's visit_bytes runs from_bytes, which panics before the
    // invalid_length mapping (id.rs:218-221) can run. `serde_bytes::Bytes`
    // serializes to a MessagePack `bin` payload, which rmp_serde routes to
    // `ActorIdVisitor::visit_bytes` — the same root cause (id.rs:140) the
    // direct probes hit, exercised through the real Deserialize path.
    let buf = rmp_serde::to_vec(&serde_bytes::Bytes::new(&[0u8; 4])).unwrap();
    let _: ActorId = rmp_serde::from_slice(&buf).unwrap();
}
