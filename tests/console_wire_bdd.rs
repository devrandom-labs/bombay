//! Cucumber harness for the ROOT `kameo` crate's in-tree console server
//! (`src/console/{server,registry,wire}.rs`) — the source side an instrumented
//! kameo app exposes over TCP. The companion `tests/console.rs` covers the six
//! happy-path integration tests; this file wires the gap scenarios from
//! `tests/features/console/server_wire.feature`.
//!
//! Like the console-crate runners, this MUST be a STANDARD libtest test (no
//! `harness = false`): cucumber 0.23's libtest-writer does not implement
//! nextest's `--list` enumeration, so `nix flake check`'s `cargoNextest` only
//! sees it as one ordinary test function. It builds only with the `testing`
//! feature (see `required-features` in Cargo.toml), which the root crate's self
//! dev-dep auto-activates for its own test builds.
//!
//! The process-global `SEQ`/`TOTAL_SPAWNED`/`REAPED_STOPPED` counters and the
//! global registry persist across scenarios (cucumber shares one process per
//! feature file), so every scenario calls `reset_for_test()` first and asserts
//! DELTAS (strictly-increasing / +1), which hold regardless of the start point.
//!
//! Task 17 (card #76): only the two single-connection seq-monotonicity
//! scenarios are wired; the name-prefix filter keeps the rest for later tasks.
//! `snapshot()` IS the snapshot producer the server frames, so calling it
//! directly exercises the real seq-advance path without a TCP client.

use std::time::Duration;

use cucumber::{World, given, then, when};
use kameo::{error::Infallible, prelude::*};

/// A grave window far larger than any test latency, so a freshly-spawned actor
/// is never reaped out of a snapshot mid-scenario.
const GRAVE_WINDOW: Duration = Duration::from_secs(300);

#[derive(Clone)]
struct Echo;

impl Actor for Echo {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

#[derive(Debug, Default, World)]
pub struct WireWorld {
    seqs: Vec<u64>,
    // Keep spawned actors alive for the scenario's lifetime so they stay in the
    // registry (dropping the ref would let the monitor report a dead probe).
    actors: Vec<ActorRef<Echo>>,
}

async fn reset_and_spawn(world: &mut WireWorld) {
    kameo::console::testing::reset_for_test();
    let actor = Echo::spawn(Echo);
    actor.wait_for_startup().await;
    world.actors.push(actor);
}

#[given(regex = r"^a console server with at least one live actor$")]
async fn given_server_with_live_actor(world: &mut WireWorld) {
    reset_and_spawn(world).await;
}

#[given(regex = r"^a single open client connection$")]
async fn given_single_connection(_world: &mut WireWorld) {
    // Single-connection seq scenarios drive `snapshot()` directly; there is no
    // separate TCP client to open — the producer is the unit under test.
}

#[given(regex = r"^a console server and a single open client connection$")]
async fn given_server_and_single_connection(_world: &mut WireWorld) {
    // No live actor required for the +1 step (seq advances per produced
    // snapshot regardless of actor count), but reset state to a known point.
    kameo::console::testing::reset_for_test();
}

#[when(regex = r"^the client requests 5 snapshots back to back$")]
async fn when_requests_five(world: &mut WireWorld) {
    for _ in 0..5 {
        let snapshot = kameo::console::testing::snapshot(GRAVE_WINDOW).await;
        world.seqs.push(snapshot.seq);
    }
}

#[when(regex = r"^the client requests two snapshots back to back$")]
async fn when_requests_two(world: &mut WireWorld) {
    for _ in 0..2 {
        let snapshot = kameo::console::testing::snapshot(GRAVE_WINDOW).await;
        world.seqs.push(snapshot.seq);
    }
}

#[then(regex = r"^each snapshot's seq is strictly greater than the previous one$")]
async fn then_strictly_increasing(world: &mut WireWorld) {
    assert!(
        world.seqs.len() >= 2,
        "need at least two seqs to compare, got {:?}",
        world.seqs
    );
    assert!(
        world.seqs.windows(2).all(|w| w[1] > w[0]),
        "seqs must strictly increase, got {:?}",
        world.seqs
    );
}

#[then(regex = r"^the second snapshot's seq equals the first snapshot's seq plus one$")]
async fn then_advances_by_one(world: &mut WireWorld) {
    assert_eq!(
        world.seqs.len(),
        2,
        "expected exactly two seqs, got {:?}",
        world.seqs
    );
    assert_eq!(
        world.seqs[1],
        world.seqs[0] + 1,
        "second seq must be first + 1, got {:?}",
        world.seqs
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn server_wire_features() {
    WireWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            "tests/features/console/server_wire.feature",
            |_, _, s| {
                s.name
                    .starts_with("seq strictly increases across rapid sequential polls")
                    || s.name.starts_with("seq advances by exactly one per produced snapshot")
            },
        )
        .await;
}
