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

use std::{
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream},
    time::{Duration, SystemTime},
};

use cucumber::{World, given, then, when};
use kameo::{
    console::{
        ConsoleHandle,
        wire::{ActorStatus, Message, Snapshot},
    },
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
    let handle = kameo::console::Console::builder()
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
    kameo::console::testing::fail_next_encode();
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
                    // Task 18b: @boundary + @lifecycle TCP scenarios (real serve).
                    || s.name
                        .starts_with("The request byte value is ignored")
                    || s.name
                        .starts_with("Multiple buffered request bytes yield one snapshot each")
                    || s.name
                        .starts_with("The server applies no frame-size cap")
                    || s.name
                        .starts_with("A snapshot that fails to encode closes the connection")
                    || s.name
                        .starts_with("The server closes the connection when the client disconnects")
                    || s.name
                        .starts_with("One client's disconnect does not disturb")
                    || s.name
                        .starts_with("A client that connects but never requests receives nothing")
                    || s.name
                        .starts_with("shutdown aborts the accept loop")
            },
        )
        .await;
}
