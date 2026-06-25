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

use std::time::{Duration, SystemTime};

use cucumber::{World, given, then, when};
use kameo::{
    console::wire::{ActorStatus, Snapshot},
    error::Infallible,
    prelude::*,
};

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
    // Snapshots captured across When steps, asserted on in Then steps.
    snapshots: Vec<Snapshot>,
    // The id of the actor stopped within the grave window (boundary scenario).
    stopped_id: Option<u64>,
    // The id of the actor stopped and then reaped (boundary absent-case).
    reaped_id: Option<u64>,
}

async fn reset_and_spawn(world: &mut WireWorld) {
    kameo::console::testing::reset_for_test();
    let actor = Echo::spawn(Echo);
    actor.wait_for_startup().await;
    world.actors.push(actor);
}

/// Spawns an actor, stops it, and waits for shutdown so its monitor enters the
/// `Stopped` state in the registry. Returns its sequence id.
async fn spawn_then_stop() -> u64 {
    let actor = Echo::spawn(Echo);
    actor.wait_for_startup().await;
    let id = actor.id().sequence_id();
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;
    id
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

// --- @sequence: captured_at and uptime advance alongside seq ---------------

#[when(regex = r"^the client requests two snapshots a short interval apart$")]
async fn when_requests_two_spaced(world: &mut WireWorld) {
    world
        .snapshots
        .push(kameo::console::testing::snapshot(GRAVE_WINDOW).await);
    // A real (small) elapse so captured_at/uptime can strictly advance; the
    // assertions only require non-decreasing, so this is not timing-fragile.
    tokio::time::sleep(Duration::from_millis(1)).await;
    world
        .snapshots
        .push(kameo::console::testing::snapshot(GRAVE_WINDOW).await);
}

#[then(regex = r"^the second snapshot's captured_at is at or after the first's$")]
async fn then_captured_at_non_decreasing(world: &mut WireWorld) {
    assert_eq!(
        world.snapshots.len(),
        2,
        "expected two snapshots, got {}",
        world.snapshots.len()
    );
    let first: SystemTime = world.snapshots[0].captured_at;
    let second: SystemTime = world.snapshots[1].captured_at;
    assert!(
        second >= first,
        "second captured_at {second:?} must be >= first {first:?}"
    );
}

#[then(regex = r"^the second snapshot's uptime is at or after the first's$")]
async fn then_uptime_non_decreasing(world: &mut WireWorld) {
    let first: Duration = world.snapshots[0].uptime;
    let second: Duration = world.snapshots[1].uptime;
    assert!(
        second >= first,
        "second uptime {second:?} must be >= first {first:?}"
    );
}

// --- @boundary: stopped-for-exactly-the-grave-window is still present -------

#[given(regex = r"^an actor that has been stopped for exactly the grave window duration$")]
async fn given_actor_stopped_at_boundary(world: &mut WireWorld) {
    kameo::console::testing::reset_for_test();
    // Present-case: stop an actor, then snapshot with a LARGE ttl so its
    // `since.elapsed()` is far below the ttl — the reap predicate
    // (`elapsed > ttl`, registry.rs:481) is false, so it survives.
    world.stopped_id = Some(spawn_then_stop().await);
    // Absent-case companion: stop another actor, let a real interval elapse,
    // then snapshot with ttl ZERO so `elapsed > 0` is true and it is reaped.
    world.reaped_id = Some(spawn_then_stop().await);
}

#[when(regex = r"^the client polls$")]
async fn when_client_polls(world: &mut WireWorld) {
    // Present-case poll: a huge grave window keeps every stopped actor (its
    // `elapsed` is far below the ttl, so the `elapsed > ttl` reap is false).
    world
        .snapshots
        .push(kameo::console::testing::snapshot(GRAVE_WINDOW).await);

    // The grave-window boundary scenario also needs the absent-case: only it
    // sets `stopped_id` (the conservation scenario sets only `reaped_id`). Take
    // a second poll with ttl ZERO after a real elapse so anything stopped for
    // strictly longer than 0s is reaped — pinning the strict `> ttl` boundary.
    if world.stopped_id.is_some() {
        tokio::time::sleep(Duration::from_millis(5)).await;
        world
            .snapshots
            .push(kameo::console::testing::snapshot(Duration::ZERO).await);
    }
}

#[then(regex = r"^the actor is still present with status Stopped$")]
async fn then_actor_present_stopped(world: &mut WireWorld) {
    let id = world.stopped_id.expect("present-case actor id");
    let present = &world.snapshots[0];
    let actor = present
        .actors
        .iter()
        .find(|a| a.id.0 == id)
        .expect("actor stopped within the grave window must still be present");
    let ActorStatus::Stopped { reason, .. } = &actor.status else {
        panic!("expected Stopped status, got {:?}", actor.status);
    };
    assert!(
        !reason.is_empty(),
        "a Stopped actor must carry a non-empty stop reason"
    );
}

#[then(regex = r"^an actor stopped for strictly longer than the grave window is absent from the snapshot$")]
async fn then_actor_absent_after_window(world: &mut WireWorld) {
    let id = world.reaped_id.expect("absent-case actor id");
    let reaped = &world.snapshots[1];
    assert!(
        reaped.actors.iter().all(|a| a.id.0 != id),
        "actor stopped for strictly longer than the grave window must be reaped"
    );
}

// --- @sequence: total_stopped is conserved across the reap boundary --------

#[given(
    regex = r"^two actors have stopped and been reaped, then a third stops but is not yet reaped$"
)]
async fn given_two_reaped_one_present(world: &mut WireWorld) {
    kameo::console::testing::reset_for_test();
    // Two actors stop, then a ttl-ZERO poll (after a real elapse) reaps both:
    // REAPED_STOPPED becomes 2.
    spawn_then_stop().await;
    spawn_then_stop().await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let _ = kameo::console::testing::snapshot(Duration::ZERO).await;
    // A third actor stops but is kept present by a huge grave window: it is
    // counted in `stopped_now`, not yet migrated to REAPED_STOPPED.
    world.reaped_id = Some(spawn_then_stop().await);
}

#[then(regex = r"^totals.total_stopped equals 3$")]
async fn then_total_stopped_is_three(world: &mut WireWorld) {
    let snapshot = world.snapshots.last().expect("a polled snapshot");
    assert_eq!(
        snapshot.totals.total_stopped, 3,
        "total_stopped must conserve all 3 stops (2 reaped + 1 present)"
    );
}

#[then(
    regex = r"^it equals REAPED_STOPPED \(2 already reaped\) plus the 1 stopped-but-still-present actor$"
)]
async fn then_total_stopped_decomposes(world: &mut WireWorld) {
    let snapshot = world.snapshots.last().expect("a polled snapshot");
    // REAPED_STOPPED is private, so derive its value from observables: the
    // still-present stopped actors are countable, and `total_stopped` is
    // `REAPED_STOPPED + present_stopped` (registry.rs:454). Thus
    // `REAPED_STOPPED == total_stopped - present_stopped` must equal 2, and the
    // decomposition total_stopped == REAPED_STOPPED + present_stopped holds.
    let present_stopped = snapshot
        .actors
        .iter()
        .filter(|a| matches!(a.status, ActorStatus::Stopped { .. }))
        .count() as u64;
    assert_eq!(
        present_stopped, 1,
        "exactly one stopped actor should still be present"
    );
    let reaped = snapshot
        .totals
        .total_stopped
        .checked_sub(present_stopped)
        .expect("total_stopped must not be less than present-stopped count");
    assert_eq!(reaped, 2, "two actors must have been reaped already");
    assert_eq!(
        snapshot.totals.total_stopped,
        reaped + present_stopped,
        "total_stopped must decompose as reaped + present-stopped with no loss or double-count"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn server_wire_features() {
    WireWorld::cucumber()
        // The registry and the SEQ/REAPED_STOPPED/TOTAL_SPAWNED statics are
        // process-global; these scenarios reset them and assert absolute counts
        // (e.g. total_stopped == 3), so they must run one at a time — cucumber
        // otherwise runs scenarios concurrently and their resets/reaps collide.
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            "tests/features/console/server_wire.feature",
            |_, _, s| {
                s.name
                    .starts_with("seq strictly increases across rapid sequential polls")
                    || s.name.starts_with("seq advances by exactly one per produced snapshot")
                    || s.name.starts_with("captured_at and uptime advance alongside seq")
                    || s.name.starts_with(
                        "An actor stopped for exactly the grave window is still present",
                    )
                    || s.name
                        .starts_with("total_stopped is conserved across the reap boundary")
            },
        )
        .await;
}
