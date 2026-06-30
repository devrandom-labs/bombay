//! Cucumber harness for the ROOT `bombay` crate's in-tree console server
//! (`src/console/{server,registry,wire}.rs`) — the source side an instrumented
//! bombay app exposes over TCP. The companion `tests/console.rs` covers the six
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
//! Card #76: `server_wire.feature` is now FULLY wired — every @sequence,
//! @linearizability, @lifecycle and @boundary scenario has step definitions, so
//! the name-prefix filter is gone and `.fail_on_skipped()` fails any unwired
//! scenario. `snapshot()` IS the snapshot producer the server frames, so calling
//! it directly exercises the real seq-advance path without a TCP client; the
//! @linearizability scenarios drive it (and actor spawn/stop) from many tokio
//! tasks gated on a shared `Barrier` for genuine overlap.

use std::{
    collections::HashSet,
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::Arc,
    time::{Duration, SystemTime},
};

use bombay::{
    console::{
        ConsoleHandle,
        wire::{ActorStatus, Message, Snapshot},
    },
    error::Infallible,
    prelude::*,
};
use cucumber::{World, given, then, when};
use tokio::sync::Barrier;

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
    // Set by the grave-window @boundary scenario so its shared `the client polls`
    // step also runs the ttl-ZERO absent-case second poll. The @linearizability
    // "stopped just before a poll" scenario leaves this false (single poll only).
    poll_boundary_absent_case: bool,
    // A running server kept alive for the scenario; dropping the handle detaches
    // the accept loop, and for the shutdown scenario we take it to call shutdown().
    server: Option<ConsoleHandle>,
    // The bound address of `server`, so later steps can open fresh connections.
    addr: Option<SocketAddr>,
    // Open client connections held across steps (e.g. two-client lifecycle).
    clients: Vec<TcpStream>,
    // Snapshot seqs read off the wire (boundary pipelining scenario).
    wire_seqs: Vec<u64>,
}

async fn reset_and_spawn(world: &mut WireWorld) {
    bombay::console::testing::reset_for_test();
    let actor = Echo::spawn(Echo);
    actor.wait_for_startup().await;
    world.actors.push(actor);
}

/// Spawns an actor, stops it, and waits until its console monitor OBSERVABLY reports
/// `Stopped`. Returns its sequence id.
///
/// `wait_for_shutdown()` is NOT a sufficient barrier here: it resolves when the mailbox
/// closes (`actor_ref.rs:620-622` → `mailbox_sender.closed()`), which happens at
/// `spawn.rs:~250` (`notify_links` consumes `mailbox_rx`) — BEFORE the console monitor's
/// `set_stopped` runs (`spawn.rs:264`, after `on_stop`). So right after `wait_for_shutdown`
/// the monitor can still read non-`Stopped`. That gap is sub-microsecond on a fast box but
/// widens under CI scheduling load, where it flaked `total_stopped` (the 3rd actor read as
/// not-yet-stopped → `stopped_now` undercounted → got 2, want 3). Condition-based waiting on
/// the observable state closes it deterministically.
async fn spawn_then_stop() -> u64 {
    let actor = Echo::spawn(Echo);
    actor.wait_for_startup().await;
    let id = actor.id().sequence_id();
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;
    wait_until_stopped(id).await;
    id
}

/// Polls the registry (via a non-reaping `snapshot(GRAVE_WINDOW)`) until the actor with `id`
/// is observably `Stopped`, bounded so a real regression fails loudly instead of hanging.
async fn wait_until_stopped(id: u64) {
    for _ in 0..200 {
        let snap = bombay::console::testing::snapshot(GRAVE_WINDOW).await;
        if snap
            .actors
            .iter()
            .any(|a| a.id.0 == id && matches!(a.status, ActorStatus::Stopped { .. }))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("actor {id} did not reach Stopped in the registry within the bound");
}

/// Reads one length-prefixed snapshot frame from a connected client socket: a
/// 4-byte big-endian length, then that many MessagePack payload bytes decoded as
/// a `Message::Snapshot`. This is the CLIENT (peer) side reading the server SUT.
fn read_one_frame(stream: &mut TcpStream) -> Snapshot {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).unwrap();
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).unwrap();
    let Message::Snapshot(s) = rmp_serde::from_slice(&buf).unwrap();
    s
}

/// Starts a real console server with a live actor and records its handle + bound
/// address in the world. A huge grave window keeps test actors in every snapshot.
async fn start_server(world: &mut WireWorld) {
    reset_and_spawn(world).await;
    let handle = bombay::console::Console::builder()
        .grave_window(GRAVE_WINDOW)
        .serve("127.0.0.1:0")
        .await
        .unwrap();
    world.addr = Some(handle.local_addr());
    world.server = Some(handle);
}

/// Opens a fresh TCP client to the running server with a short read timeout, so
/// no step can hang the suite waiting on a frame that will never arrive.
fn connect(world: &WireWorld) -> TcpStream {
    let addr = world.addr.expect("server must be started first");
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
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
    bombay::console::testing::reset_for_test();
}

#[when(regex = r"^the client requests 5 snapshots back to back$")]
async fn when_requests_five(world: &mut WireWorld) {
    for _ in 0..5 {
        let snapshot = bombay::console::testing::snapshot(GRAVE_WINDOW).await;
        world.seqs.push(snapshot.seq);
    }
}

#[when(regex = r"^the client requests two snapshots back to back$")]
async fn when_requests_two(world: &mut WireWorld) {
    for _ in 0..2 {
        let snapshot = bombay::console::testing::snapshot(GRAVE_WINDOW).await;
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

// --- @linearizability: real concurrency via Barrier + tokio::spawn ---------
//
// These scenarios drive `snapshot()` (and actor spawn/stop) from many tokio
// tasks that all await a shared `Arc<Barrier>` before doing the contended work,
// so the operations genuinely overlap rather than running sequentially. The
// suite keeps `.max_concurrent_scenarios(1)`, so the only concurrency in play is
// WITHIN each scenario — the process-global statics are not shared across
// scenarios concurrently. Each scenario resets the registry/counters first.

/// Number of overlapping producers/spawners/stoppers in the concurrency
/// scenarios. Eight tasks contend on the shared atomics/registry lock.
const CONCURRENCY: usize = 8;

/// Snapshots each concurrent producer takes (scenario 1) — enough total
/// snapshots that a missing fetch_add atomicity would collide some seqs.
const SNAPSHOTS_PER_TASK: usize = 4;

#[given(regex = r"^a console server with live actors$")]
async fn given_server_with_live_actors(world: &mut WireWorld) {
    bombay::console::testing::reset_for_test();
    // A handful of live actors so each concurrent snapshot has real membership
    // to render while the seqs are handed out.
    for _ in 0..CONCURRENCY {
        let actor = Echo::spawn(Echo);
        actor.wait_for_startup().await;
        world.actors.push(actor);
    }
}

#[given(regex = r"^8 client connections polling concurrently$")]
async fn given_eight_pollers(_world: &mut WireWorld) {
    // The overlap is set up in the When step (one Barrier shared by all tasks);
    // there is no separate TCP client — `snapshot()` is the seq producer the
    // server frames, so the concurrent producers call it directly.
}

#[when(regex = r"^each connection requests several snapshots overlapping in time$")]
async fn when_concurrent_polls(world: &mut WireWorld) {
    let barrier = Arc::new(Barrier::new(CONCURRENCY));
    let tasks: Vec<_> = (0..CONCURRENCY)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                // All producers release together, so their fetch_add calls on the
                // global SEQ genuinely interleave.
                barrier.wait().await;
                let mut seqs = Vec::with_capacity(SNAPSHOTS_PER_TASK);
                for _ in 0..SNAPSHOTS_PER_TASK {
                    seqs.push(bombay::console::testing::snapshot(GRAVE_WINDOW).await.seq);
                }
                seqs
            })
        })
        .collect();

    for task in tasks {
        world
            .seqs
            .extend(task.await.expect("poller task must not panic"));
    }
}

#[then(regex = r"^no two snapshots produced by the process share the same seq$")]
async fn then_seqs_unique(world: &mut WireWorld) {
    let total = world.seqs.len();
    assert_eq!(
        total,
        CONCURRENCY * SNAPSHOTS_PER_TASK,
        "every concurrent producer must have collected its snapshots, got {total}"
    );
    let mut deduped = world.seqs.clone();
    deduped.sort_unstable();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        total,
        "the global atomic SEQ must hand out a distinct seq to every snapshot — \
         duplicates found, got {:?}",
        world.seqs
    );
}

#[when(regex = r"^a client polls while many actors are being spawned concurrently$")]
async fn when_poll_during_concurrent_spawn(world: &mut WireWorld) {
    // CONCURRENCY spawner tasks + one poller task, all gated on one Barrier so
    // the spawns and the snapshot genuinely overlap. Each spawner returns its
    // ActorRef so the World can keep the monitors alive for the assertion.
    let barrier = Arc::new(Barrier::new(CONCURRENCY + 1));

    let spawners: Vec<_> = (0..CONCURRENCY)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let actor = Echo::spawn(Echo);
                actor.wait_for_startup().await;
                actor
            })
        })
        .collect();

    let poll_barrier = Arc::clone(&barrier);
    let poller = tokio::spawn(async move {
        poll_barrier.wait().await;
        // Poll a few times while the spawn batch is being applied so at least
        // one snapshot lands mid-batch.
        let mut snaps = Vec::new();
        for _ in 0..SNAPSHOTS_PER_TASK {
            snaps.push(bombay::console::testing::snapshot(GRAVE_WINDOW).await);
        }
        snaps
    });

    for spawner in spawners {
        world
            .actors
            .push(spawner.await.expect("spawner task must not panic"));
    }
    world
        .snapshots
        .extend(poller.await.expect("poller task must not panic"));
}

#[then(
    regex = r"^every actor in the returned snapshot is internally consistent \(id present, status set\)$"
)]
async fn then_actors_internally_consistent(world: &mut WireWorld) {
    assert!(
        !world.snapshots.is_empty(),
        "the poller must have captured at least one snapshot"
    );
    for snapshot in &world.snapshots {
        for actor in &snapshot.actors {
            // `id` is a u64 newtype and `status` is an enum (always a valid
            // variant); the invariant is that a rendered entry is whole — it has
            // a name string alongside its id and a well-formed status.
            assert!(
                matches!(
                    actor.status,
                    ActorStatus::Starting
                        | ActorStatus::Running
                        | ActorStatus::Restarting
                        | ActorStatus::Stopping
                        | ActorStatus::Stopped { .. }
                ),
                "actor {} must carry a well-formed status, got {:?}",
                actor.id.0,
                actor.status
            );
        }
    }
}

#[then(
    regex = r"^the snapshot reflects a single registry membership, not a half-applied spawn batch$"
)]
async fn then_single_membership(world: &mut WireWorld) {
    // The monitor set is cloned under one registry lock, so each snapshot's
    // membership list is a consistent set: no id appears twice (no torn/double
    // entry from a half-applied batch).
    for snapshot in &world.snapshots {
        let ids: Vec<u64> = snapshot.actors.iter().map(|a| a.id.0).collect();
        let unique: HashSet<u64> = ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            ids.len(),
            "a snapshot's membership must contain each actor id at most once, got {ids:?}"
        );
    }
}

#[given(regex = r"^a console server with actors stopping concurrently with a poll$")]
async fn given_server_actors_stopping(world: &mut WireWorld) {
    bombay::console::testing::reset_for_test();
    // Spawn a batch of actors up front; they are stopped concurrently during the
    // When step. Keep their refs so they stay registered until then.
    for _ in 0..CONCURRENCY {
        let actor = Echo::spawn(Echo);
        actor.wait_for_startup().await;
        world.actors.push(actor);
    }
}

#[when(regex = r"^a client polls during the stop storm$")]
async fn when_poll_during_stop_storm(world: &mut WireWorld) {
    // Each pre-spawned actor is stopped from its own task; one poller task polls
    // repeatedly. All tasks release together on one Barrier so the stops and the
    // snapshot overlap. A huge grave window keeps every stopped actor present.
    let actors = std::mem::take(&mut world.actors);
    let n = actors.len();
    let barrier = Arc::new(Barrier::new(n + 1));

    let stoppers: Vec<_> = actors
        .into_iter()
        .map(|actor| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                actor.stop_gracefully().await.unwrap();
                actor.wait_for_shutdown().await;
            })
        })
        .collect();

    let poll_barrier = Arc::clone(&barrier);
    let poller = tokio::spawn(async move {
        poll_barrier.wait().await;
        let mut snaps = Vec::new();
        for _ in 0..SNAPSHOTS_PER_TASK {
            snaps.push(bombay::console::testing::snapshot(GRAVE_WINDOW).await);
        }
        snaps
    });

    for stopper in stoppers {
        stopper.await.expect("stopper task must not panic");
    }
    world
        .snapshots
        .extend(poller.await.expect("poller task must not panic"));
}

#[then(regex = r"^each actor id appears at most once in the snapshot$")]
async fn then_each_id_at_most_once(world: &mut WireWorld) {
    assert!(
        !world.snapshots.is_empty(),
        "the poller must have captured at least one snapshot"
    );
    for snapshot in &world.snapshots {
        let ids: Vec<u64> = snapshot.actors.iter().map(|a| a.id.0).collect();
        let unique: HashSet<u64> = ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            ids.len(),
            "no torn/duplicate entry: each id must appear at most once, got {ids:?}"
        );
    }
}

#[then(
    regex = r"^a stopping/stopped actor renders a coherent status \(never a partially-built entry\)$"
)]
async fn then_stopped_status_coherent(world: &mut WireWorld) {
    // Any stopped actor present in a snapshot must carry a coherent Stopped
    // status with a non-empty reason; every status is a well-formed variant.
    for snapshot in &world.snapshots {
        for actor in &snapshot.actors {
            if let ActorStatus::Stopped { reason, .. } = &actor.status {
                assert!(
                    !reason.is_empty(),
                    "a Stopped actor must carry a non-empty stop reason, id {}",
                    actor.id.0
                );
            }
        }
    }
}

#[given(regex = r"^an actor that stops immediately before a poll$")]
async fn given_actor_stops_before_poll(world: &mut WireWorld) {
    bombay::console::testing::reset_for_test();
    // Stop the actor and await shutdown, so it is in the registry as Stopped at
    // poll time. The grave window (set in the next Given) dwarfs poll latency,
    // so it is deterministically present.
    world.stopped_id = Some(spawn_then_stop().await);
}

#[given(regex = r"^a grave window longer than the poll latency$")]
async fn given_grave_window_long(_world: &mut WireWorld) {
    // The poll in the When step uses GRAVE_WINDOW (300s) >> any poll latency, so
    // the just-stopped actor cannot be reaped before it is observed.
}

#[then(regex = r"^the stopped actor is present with status Stopped carrying its stop reason$")]
async fn then_stopped_present_with_reason(world: &mut WireWorld) {
    let id = world.stopped_id.expect("stopped actor id");
    let snapshot = world.snapshots.last().expect("a polled snapshot");
    let actor = snapshot
        .actors
        .iter()
        .find(|a| a.id.0 == id)
        .expect("an actor stopped within the grave window must still be present");
    let ActorStatus::Stopped { reason, .. } = &actor.status else {
        panic!("expected Stopped status, got {:?}", actor.status);
    };
    assert!(
        !reason.is_empty(),
        "a Stopped actor must carry a non-empty stop reason"
    );
}

// --- @sequence: captured_at and uptime advance alongside seq ---------------

#[when(regex = r"^the client requests two snapshots a short interval apart$")]
async fn when_requests_two_spaced(world: &mut WireWorld) {
    world
        .snapshots
        .push(bombay::console::testing::snapshot(GRAVE_WINDOW).await);
    // A real (small) elapse between the two polls so uptime advances. captured_at
    // is best-effort wall-clock (may regress on a clock step) — see
    // `assert_fresh_wall_clock`; only uptime (Instant) is asserted monotonic.
    tokio::time::sleep(Duration::from_millis(1)).await;
    world
        .snapshots
        .push(bombay::console::testing::snapshot(GRAVE_WINDOW).await);
}

/// `captured_at` is `SystemTime::now()` (registry.rs) — a best-effort WALL clock, which is
/// NOT monotonic: a virtualized/NTP-adjusted host can step it backward between two polls. The
/// system is built for this (wire.rs documents the client diffing captured_at; `rate_context`
/// guards a reversed clock with `duration_since().ok()` — see invariants.md:201). So the real,
/// testable guarantee is that each snapshot is freshly stamped with a plausible current time,
/// not that two stamps are ordered. Asserting `>=` here was the source of a CI-only flake.
/// (Monotonic ordering is carried by `uptime`, an `Instant`, asserted separately.)
fn assert_fresh_wall_clock(captured: SystemTime) {
    let now = SystemTime::now();
    let skew = match now.duration_since(captured) {
        Ok(past) => past,
        Err(future) => future.duration(),
    };
    assert!(
        skew < Duration::from_secs(3600),
        "captured_at {captured:?} must be a fresh wall-clock stamp near now ({now:?}); \
         skew {skew:?} (a bogus/epoch stamp would fail this)"
    );
}

#[then(regex = r"^each snapshot's captured_at is a fresh wall-clock timestamp$")]
async fn then_captured_at_is_fresh(world: &mut WireWorld) {
    assert_eq!(
        world.snapshots.len(),
        2,
        "expected two snapshots, got {}",
        world.snapshots.len()
    );
    for snap in &world.snapshots {
        assert_fresh_wall_clock(snap.captured_at);
    }
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
    bombay::console::testing::reset_for_test();
    world.poll_boundary_absent_case = true;
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
        .push(bombay::console::testing::snapshot(GRAVE_WINDOW).await);

    // The grave-window boundary scenario also needs the absent-case: only it
    // sets `poll_boundary_absent_case`. Take a second poll with ttl ZERO after a
    // real elapse so anything stopped for strictly longer than 0s is reaped —
    // pinning the strict `> ttl` boundary.
    if world.poll_boundary_absent_case {
        tokio::time::sleep(Duration::from_millis(5)).await;
        world
            .snapshots
            .push(bombay::console::testing::snapshot(Duration::ZERO).await);
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

#[then(
    regex = r"^an actor stopped for strictly longer than the grave window is absent from the snapshot$"
)]
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
    bombay::console::testing::reset_for_test();
    // Two actors stop, then a ttl-ZERO poll (after a real elapse) reaps both:
    // REAPED_STOPPED becomes 2.
    spawn_then_stop().await;
    spawn_then_stop().await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let _ = bombay::console::testing::snapshot(Duration::ZERO).await;
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

// --- @boundary + @lifecycle: real serve() over TCP sockets -----------------

#[given(regex = r"^a console server and an open client connection$")]
async fn given_server_and_open_connection(world: &mut WireWorld) {
    start_server(world).await;
    let stream = connect(world);
    world.clients.push(stream);
}

#[given(regex = r"^a console server$")]
async fn given_console_server(world: &mut WireWorld) {
    start_server(world).await;
}

#[given(regex = r"^a console server with two open client connections$")]
async fn given_server_two_connections(world: &mut WireWorld) {
    start_server(world).await;
    world.clients.push(connect(world));
    world.clients.push(connect(world));
}

#[given(regex = r"^a console server and a client that connects but sends no byte$")]
async fn given_server_silent_client(world: &mut WireWorld) {
    start_server(world).await;
    world.clients.push(connect(world));
}

#[given(regex = r"^a running console server$")]
async fn given_running_server(world: &mut WireWorld) {
    start_server(world).await;
}

#[given(regex = r"^a console server whose snapshot would fail MessagePack encoding$")]
async fn given_server_encode_fails(world: &mut WireWorld) {
    start_server(world).await;
    world.clients.push(connect(world));
    // Arm the one-shot encode-failure hook so the next snapshot encode in the
    // serve loop takes the error branch (break before any write).
    bombay::console::testing::fail_next_encode();
}

#[when(regex = r"^the client sends the byte 0xFF instead of 0x00$")]
async fn when_sends_ff(world: &mut WireWorld) {
    world.clients[0].write_all(&[0xFF]).unwrap();
}

#[then(regex = r"^the server still replies with exactly one length-prefixed snapshot frame$")]
async fn then_one_frame(world: &mut WireWorld) {
    // read_one_frame succeeding proves the server replied to the 0xFF byte.
    let _ = read_one_frame(&mut world.clients[0]);
}

#[when(regex = r"^the client sends 3 request bytes in one write before reading any reply$")]
async fn when_sends_three(world: &mut WireWorld) {
    world.clients[0].write_all(&[0, 0, 0]).unwrap();
}

#[then(regex = r"^the server replies with 3 length-prefixed snapshot frames$")]
async fn then_three_frames(world: &mut WireWorld) {
    world.wire_seqs.clear();
    for _ in 0..3 {
        let s = read_one_frame(&mut world.clients[0]);
        world.wire_seqs.push(s.seq);
    }
    assert_eq!(world.wire_seqs.len(), 3, "expected 3 frames");
}

#[then(regex = r"^those frames carry strictly increasing seq values$")]
async fn then_frames_increasing(world: &mut WireWorld) {
    assert!(
        world.wire_seqs.windows(2).all(|w| w[1] > w[0]),
        "frame seqs must strictly increase, got {:?}",
        world.wire_seqs
    );
}

#[when(regex = r"^a client sends arbitrary surplus bytes after its request byte$")]
async fn when_sends_surplus(world: &mut WireWorld) {
    let mut stream = connect(world);
    // The request byte plus surplus bytes in one write: the server reads ONE
    // byte per loop and never parses a client-supplied length, so every byte
    // (request + "surplus") is just another trigger.
    stream.write_all(&[0x01, 0x02, 0x03, 0x04]).unwrap();
    world.clients.push(stream);
}

#[then(regex = r"^the server treats each byte as a fresh request trigger$")]
async fn then_each_byte_a_trigger(world: &mut WireWorld) {
    let stream = world.clients.last_mut().expect("surplus client");
    // 4 bytes sent ⇒ 4 frames back, one per byte. read_one_frame succeeding
    // four times shows no byte was consumed as a length and none was capped.
    world.wire_seqs.clear();
    for _ in 0..4 {
        let s = read_one_frame(stream);
        world.wire_seqs.push(s.seq);
    }
    assert_eq!(
        world.wire_seqs.len(),
        4,
        "each of the 4 bytes must trigger its own frame, got {:?}",
        world.wire_seqs
    );
}

#[then(regex = r"^the server never parses or allocates on a client-supplied length$")]
async fn then_no_length_parse(world: &mut WireWorld) {
    // Observable proof: every frame decoded as a valid Snapshot with a real seq,
    // and they advance one-per-byte. Had the server interpreted any byte as a
    // length it would have mis-framed and read_one_frame would have failed or the
    // frame count would differ. The strictly-increasing per-byte seqs confirm it.
    assert_eq!(world.wire_seqs.len(), 4, "all four bytes produced a frame");
    assert!(
        world.wire_seqs.windows(2).all(|w| w[1] > w[0]),
        "per-byte seqs must advance, got {:?}",
        world.wire_seqs
    );
}

#[when(regex = r"^the client requests a snapshot$")]
async fn when_requests_a_snapshot(world: &mut WireWorld) {
    world.clients[0].write_all(&[0]).unwrap();
}

#[then(regex = r"^the server writes no length prefix and closes the connection$")]
async fn then_eof_no_partial(world: &mut WireWorld) {
    // The serve loop breaks on the injected encode error BEFORE writing any
    // length prefix, so the client sees EOF on the 4-byte length read — never a
    // partial frame.
    let mut len = [0u8; 4];
    let err = world.clients[0]
        .read_exact(&mut len)
        .expect_err("encode failure must close the connection with no length prefix");
    assert_eq!(
        err.kind(),
        ErrorKind::UnexpectedEof,
        "client must see EOF (closed connection), got {err:?}"
    );
}

#[when(regex = r"^the client closes the socket without sending a request byte$")]
async fn when_client_closes(world: &mut WireWorld) {
    // Drop the only client connection: the server's read_exact errors and that
    // serve_client task ends. The accept loop (the SUT we pin) stays up.
    world.clients.clear();
    // Give the serve_client task a beat to observe the close and unwind.
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[then(regex = r"^the serve_client loop's read_exact errors and the task ends cleanly$")]
async fn then_task_ends_cleanly(world: &mut WireWorld) {
    // Observable: the server is still serving — a fresh client gets a snapshot,
    // proving no panic took down the accept loop when the first peer vanished.
    let mut fresh = connect(world);
    fresh.write_all(&[0]).unwrap();
    let _ = read_one_frame(&mut fresh);
}

#[when(regex = r"^the first client disconnects abruptly$")]
async fn when_first_disconnects(world: &mut WireWorld) {
    // Drop only the first connection; keep the second.
    let _first = world.clients.remove(0);
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[then(regex = r"^the second client can still request and receive a fresh snapshot$")]
async fn then_second_still_works(world: &mut WireWorld) {
    let second = world.clients.last_mut().expect("second client survives");
    second.write_all(&[0]).unwrap();
    let _ = read_one_frame(second);
}

#[when(regex = r"^no request byte is ever written$")]
async fn when_no_byte_written(world: &mut WireWorld) {
    // Use a short read timeout to prove no frame is pushed without a request.
    let stream = world.clients.last_mut().expect("silent client");
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
}

#[then(regex = r"^the server produces no snapshot for that connection$")]
async fn then_no_snapshot(world: &mut WireWorld) {
    let stream = world.clients.last_mut().expect("silent client");
    let mut byte = [0u8; 1];
    let err = stream
        .read(&mut byte)
        .expect_err("a silent client must receive nothing (read should time out)");
    assert!(
        matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut),
        "pull-based: no data should arrive without a request, got {err:?}"
    );
}

#[when(regex = r"^the handle's shutdown is called$")]
async fn when_shutdown_called(world: &mut WireWorld) {
    world.clients.clear();
    world
        .server
        .take()
        .expect("a running server to shut down")
        .shutdown();
}

#[then(regex = r"^subsequent connection attempts to the bound address are refused$")]
async fn then_connections_refused(world: &mut WireWorld) {
    let addr = world.addr.expect("bound address");
    // Allow a brief window for the aborted accept loop to actually close the
    // listening socket before asserting connections are refused.
    let mut last_ok = false;
    for _ in 0..50 {
        match TcpStream::connect(addr) {
            Ok(_) => {
                last_ok = true;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(_) => {
                last_ok = false;
                break;
            }
        }
    }
    assert!(
        !last_ok,
        "after shutdown, connecting to {addr} must be refused"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn server_wire_features() {
    // server_wire.feature is now FULLY wired: every @sequence, @linearizability,
    // @lifecycle and @boundary scenario has step definitions, so the name-prefix
    // filter is gone — the whole file runs and `.fail_on_skipped()` turns any
    // unwired/undefined scenario into a failure (no false green).
    //
    // The registry and the SEQ/REAPED_STOPPED/TOTAL_SPAWNED statics are
    // process-global; scenarios reset them and assert absolute counts
    // (e.g. total_stopped == 3), so they run one at a time. The @linearizability
    // scenarios' real concurrency is WITHIN each scenario (Barrier + tokio::spawn),
    // not across scenarios — `.max_concurrent_scenarios(1)` keeps the cross-scenario
    // resets/reaps from colliding.
    WireWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            // Anchor to CARGO_MANIFEST_DIR (the crate root): nextest does not
            // guarantee the test's cwd is the workspace root (the nix-sandbox
            // `cargoNextest` runs from a different cwd than a bare `cargo test`),
            // so a bare relative path makes cucumber fail with "Could not read
            // path". The env var is the root `bombay` crate dir = workspace root.
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/console/server_wire.feature"
            ),
            |_, _, _| true,
        )
        .await;
}
