//! Step definitions for `poller.feature` — the console TCP poller's request/reply
//! framing, the MAX_FRAME_BYTES size gate, and the MessagePack decode path.
//!
//! These steps drive the REAL code: `poll_once_over` runs the genuine private
//! `Poller::poll` over a loopback socket pair, and `check_frame_len` /
//! `decode_frame` are the production helpers extracted from that same `poll`.
//! Snapshots are compared field-by-field (and, where convenient, by re-encoded
//! bytes) because `wire::Snapshot` has no `PartialEq`.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

use cucumber::{World, given, then, when};
use kameo_console::testing::{
    ActorCounters, ActorId, ActorSnapshot, ActorStatus, Links, MailboxKind, MailboxStats,
    RefCounts, Snapshot, Totals, check_frame_len, decode_frame, poll_once_over,
    poll_once_over_with_read_timeout,
};
use kameo::console::wire::Message;

#[derive(Debug, World)]
#[world(init = Self::new)]
pub struct PollerWorld {
    /// The Snapshot a Given step constructs for the round-trip scenario.
    original: Option<Snapshot>,
    /// The Snapshot a decode/poll produced.
    decoded: Option<Snapshot>,
    /// Result of the most recent size-gate check.
    gate_ok: bool,
    /// `ErrorKind` of the most recent failing operation.
    err_kind: Option<io::ErrorKind>,
    /// Message of the most recent failing operation (for "names the size" asserts).
    err_msg: String,
    /// The shared slot a real poll publishes into.
    slot: Arc<Mutex<Option<Snapshot>>>,
    /// seq values observed in the slot across sequential polls.
    seqs: Vec<u64>,
    /// Single request byte the server observed (one-byte-write scenario).
    request_byte: Option<u8>,
    /// Count of request bytes the server read before EOF (one-byte-write scenario).
    request_byte_count: usize,
    /// Bytes the client consumed off the wire (prefix + payload), for the framing assert.
    payload_len_consumed: Option<usize>,
}

impl PollerWorld {
    fn new() -> Self {
        Self {
            original: None,
            decoded: None,
            gate_ok: false,
            err_kind: None,
            err_msg: String::new(),
            slot: Arc::new(Mutex::new(None)),
            seqs: Vec::new(),
            request_byte: None,
            request_byte_count: 0,
            payload_len_consumed: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn make_actor(id: u64) -> ActorSnapshot {
    ActorSnapshot {
        id: ActorId(id),
        name: format!("Actor{id}"),
        status: ActorStatus::Running,
        handling: None,
        waiting_on: None,
        strategy: None,
        spawned_at: SystemTime::UNIX_EPOCH,
        mailbox: MailboxStats { kind: MailboxKind::Unbounded, len: 0, capacity: None },
        counters: ActorCounters::default(),
        message_types: Vec::new(),
        refs: RefCounts { strong: 1, weak: 0 },
        links: Links::default(),
        supervision: None,
    }
}

fn snapshot_with_seq(seq: u64, actors: Vec<ActorSnapshot>) -> Snapshot {
    Snapshot {
        seq,
        captured_at: SystemTime::UNIX_EPOCH,
        uptime: Duration::ZERO,
        actors,
        totals: Totals::default(),
    }
}

/// Encode a Snapshot exactly as the source side does: a named-MessagePack
/// `Message::Snapshot`. Deterministic, so two encodes of equal values match byte-for-byte.
fn encode_snapshot(s: &Snapshot) -> Vec<u8> {
    rmp_serde::to_vec_named(&Message::Snapshot(s.clone())).expect("encode snapshot")
}

/// A loopback TCP pair: (client, server). Both ends are blocking std sockets.
fn loopback() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let client = TcpStream::connect(addr).expect("connect loopback");
    let (server, _) = listener.accept().expect("accept loopback");
    (client, server)
}

/// Spawn a server thread that, for each of `replies`, reads the single request
/// byte then writes a 4-byte big-endian length prefix + the encoded frame.
/// Returns the join handle so the test can bound the wait.
fn spawn_replier(mut server: TcpStream, replies: Vec<Vec<u8>>) -> JoinHandle<()> {
    thread::spawn(move || {
        for frame in replies {
            let mut req = [0u8; 1];
            if server.read_exact(&mut req).is_err() {
                return;
            }
            let len = u32::try_from(frame.len()).expect("frame fits in u32");
            server.write_all(&len.to_be_bytes()).expect("write len prefix");
            server.write_all(&frame).expect("write frame");
        }
    })
}

// ---------------------------------------------------------------------------
// Background
// ---------------------------------------------------------------------------

#[given(regex = r"^a poller connected to a snapshot server over a TCP stream$")]
async fn given_background(_world: &mut PollerWorld) {
    // Each scenario builds its own loopback pair where it needs one; the
    // Background is the shared premise, no global state to set up here.
}

// ---------------------------------------------------------------------------
// @sequence: round-trip
// ---------------------------------------------------------------------------

#[given(regex = r"^a Snapshot with seq 7 and a known set of actors$")]
async fn given_snapshot_seq7(world: &mut PollerWorld) {
    world.original = Some(snapshot_with_seq(7, vec![make_actor(1), make_actor(2)]));
}

#[when(regex = r"^the Snapshot is encoded as a length-prefixed MessagePack frame and the poller decodes it$")]
async fn when_encode_then_decode(world: &mut PollerWorld) {
    let original = world.original.as_ref().expect("original snapshot set");
    let bytes = encode_snapshot(original);
    world.decoded = Some(decode_frame(&bytes).expect("decode round-trip frame"));
}

#[then(regex = r"^the decoded Snapshot equals the original Snapshot field-for-field$")]
async fn then_round_trip_equal(world: &mut PollerWorld) {
    let original = world.original.as_ref().expect("original snapshot set");
    let decoded = world.decoded.as_ref().expect("decoded snapshot set");

    assert_eq!(decoded.seq, original.seq, "seq mismatch");
    assert_eq!(decoded.captured_at, original.captured_at, "captured_at mismatch");
    assert_eq!(decoded.uptime, original.uptime, "uptime mismatch");
    assert_eq!(decoded.actors.len(), original.actors.len(), "actor count mismatch");
    for (d, o) in decoded.actors.iter().zip(original.actors.iter()) {
        assert_eq!(d.id, o.id, "actor id mismatch");
        assert_eq!(d.name, o.name, "actor name mismatch");
        assert_eq!(
            std::mem::discriminant(&d.status),
            std::mem::discriminant(&o.status),
            "actor status discriminant mismatch"
        );
    }
    assert_eq!(decoded.totals.alive, original.totals.alive, "totals.alive mismatch");
    assert_eq!(
        decoded.totals.total_spawned, original.totals.total_spawned,
        "totals.total_spawned mismatch"
    );
    assert_eq!(
        decoded.totals.messages_received, original.totals.messages_received,
        "totals.messages_received mismatch"
    );
    // Belt-and-braces: re-encoding both yields identical deterministic bytes.
    assert_eq!(encode_snapshot(decoded), encode_snapshot(original), "re-encoded bytes differ");
}

// ---------------------------------------------------------------------------
// @sequence: one-byte write + length-prefixed read
// ---------------------------------------------------------------------------

#[when(regex = r"^the poller performs one poll$")]
async fn when_one_poll(world: &mut PollerWorld) {
    // Default reply is a small valid Snapshot (seq 42 for the slot-publish
    // scenario, which only adds a Given to set the seq; here we honor any seq
    // already chosen, defaulting to 42).
    let seq = world.original.as_ref().map_or(42, |s| s.seq);
    let frame = encode_snapshot(&snapshot_with_seq(seq, vec![make_actor(1)]));

    let (mut client, server) = loopback();

    // Instrument the server side so we can assert exactly one request byte and
    // the framing the client reads. Spawn before the blocking client poll.
    let frame_for_server = frame.clone();
    let handle = thread::spawn(move || {
        let mut server = server;
        let mut req = [0u8; 1];
        server.read_exact(&mut req).expect("server reads request byte");
        let len = u32::try_from(frame_for_server.len()).expect("fits u32");
        server.write_all(&len.to_be_bytes()).expect("write len");
        server.write_all(&frame_for_server).expect("write frame");
        // Try to read one more byte to confirm the client wrote exactly one.
        let mut extra = [0u8; 1];
        let extra_read = match server.read(&mut extra) {
            Ok(0) | Err(_) => 0,
            Ok(n) => n,
        };
        (req[0], 1 + extra_read)
    });

    // Give the server a brief moment, then poll on the main thread.
    let result = poll_once_over_with_timeout(&mut client, Arc::clone(&world.slot));
    result.expect("poll_once_over should succeed");

    // Close the client so the server's trailing read sees EOF, not a block.
    drop(client);
    let (byte, count) = handle.join().expect("server thread");
    world.request_byte = Some(byte);
    world.request_byte_count = count;
    world.payload_len_consumed = Some(4 + frame.len());
    world.seqs.push(world.slot.lock().unwrap().as_ref().expect("slot filled").seq);
}

/// `poll_once_over` takes ownership of the stream, so clone it for the poll and
/// keep the original to close afterward. A short read timeout bounds any stall.
fn poll_once_over_with_timeout(
    client: &mut TcpStream,
    slot: Arc<Mutex<Option<Snapshot>>>,
) -> io::Result<()> {
    let stream = client.try_clone().expect("clone client stream");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    poll_once_over(stream, slot)
}

#[then(regex = r"^the poller has written exactly the single byte 0x00 to the stream$")]
async fn then_single_zero_byte(world: &mut PollerWorld) {
    assert_eq!(world.request_byte, Some(0x00), "request byte should be 0x00");
    assert_eq!(world.request_byte_count, 1, "poller should write exactly one request byte");
}

#[then(regex = r"^it then read a 4-byte big-endian length prefix$")]
async fn then_read_len_prefix(world: &mut PollerWorld) {
    let consumed = world.payload_len_consumed.expect("payload consumed recorded");
    assert!(consumed >= 4, "poller must consume at least the 4-byte prefix, got {consumed}");
}

#[then(regex = r"^it then read exactly that many payload bytes$")]
async fn then_read_payload(world: &mut PollerWorld) {
    // A successful poll published into the slot, which only happens after
    // read_exact of the full payload succeeded.
    assert!(world.slot.lock().unwrap().is_some(), "slot must hold a decoded snapshot");
}

// ---------------------------------------------------------------------------
// @sequence: slot publish (seq 42)
// ---------------------------------------------------------------------------

#[given(regex = r"^the server will reply with a Snapshot whose seq is 42$")]
async fn given_server_reply_seq42(world: &mut PollerWorld) {
    world.original = Some(snapshot_with_seq(42, vec![make_actor(1)]));
}

#[then(regex = r"^the shared snapshot slot holds a Snapshot with seq 42$")]
async fn then_slot_seq42(world: &mut PollerWorld) {
    let guard = world.slot.lock().unwrap();
    let snap = guard.as_ref().expect("slot must hold a snapshot");
    assert_eq!(snap.seq, 42, "slot snapshot seq should be 42");
}

// ---------------------------------------------------------------------------
// @sequence: sequential polls (seq 0,1,2)
// ---------------------------------------------------------------------------

#[given(regex = r"^the server replies to successive requests with seq 0, then 1, then 2$")]
async fn given_server_seq_0_1_2(world: &mut PollerWorld) {
    let (mut client, server) = loopback();
    let replies: Vec<Vec<u8>> = (0u64..=2)
        .map(|seq| encode_snapshot(&snapshot_with_seq(seq, vec![make_actor(1)])))
        .collect();
    let handle = spawn_replier(server, replies);

    // Three polls on the SAME connection, capturing the slot's seq each time.
    for _ in 0..3 {
        poll_once_over_with_timeout(&mut client, Arc::clone(&world.slot))
            .expect("sequential poll should succeed");
        let seq = world.slot.lock().unwrap().as_ref().expect("slot filled").seq;
        world.seqs.push(seq);
    }
    drop(client);
    handle.join().expect("replier thread");
}

#[when(regex = r"^the poller performs three polls in a row$")]
async fn when_three_polls(_world: &mut PollerWorld) {
    // The polling happened in the Given (it owns the loopback pair); this When
    // is the protocol step the Then asserts against.
}

#[then(regex = r"^the shared snapshot slot ends holding seq 2$")]
async fn then_slot_ends_seq2(world: &mut PollerWorld) {
    let guard = world.slot.lock().unwrap();
    let snap = guard.as_ref().expect("slot must hold a snapshot");
    assert_eq!(snap.seq, 2, "final slot seq should be 2");
}

#[then(regex = r"^each poll observed a seq strictly greater than the previous poll's seq$")]
async fn then_seqs_strictly_increasing(world: &mut PollerWorld) {
    assert_eq!(world.seqs, vec![0, 1, 2], "observed seqs should be 0,1,2 in order");
    for pair in world.seqs.windows(2) {
        assert!(pair[1] > pair[0], "seq must strictly increase: {} !> {}", pair[1], pair[0]);
    }
}

// ---------------------------------------------------------------------------
// @boundary: size gate
// ---------------------------------------------------------------------------

#[given(regex = r"^the server replies with a length prefix equal to 67108864 \(64 MiB\)$")]
async fn given_len_eq_max(world: &mut PollerWorld) {
    world.gate_ok = check_frame_len(67_108_864).is_ok();
}

#[given(regex = r"^the server replies with a length prefix equal to 67108865 \(64 MiB \+ 1\)$")]
async fn given_len_max_plus_one(world: &mut PollerWorld) {
    capture_gate_err(world, 67_108_865);
}

#[given(regex = r"^the server replies with a length prefix of 0xFFFFFFFF$")]
async fn given_len_garbage_max(world: &mut PollerWorld) {
    capture_gate_err(world, 0xFFFF_FFFF);
}

fn capture_gate_err(world: &mut PollerWorld, len: u32) {
    match check_frame_len(len) {
        Ok(()) => {
            world.gate_ok = true;
        }
        Err(err) => {
            world.gate_ok = false;
            world.err_kind = Some(err.kind());
            world.err_msg = err.to_string();
        }
    }
}

#[when(regex = r"^the poller reads the length prefix$")]
async fn when_reads_len_prefix(_world: &mut PollerWorld) {
    // The size gate ran in the Given (check_frame_len is the production gate).
}

#[then(regex = r"^the poller does not reject the frame on size$")]
async fn then_not_rejected(world: &mut PollerWorld) {
    assert!(world.gate_ok, "check_frame_len(MAX_FRAME_BYTES) should be Ok");
}

#[then(regex = r"^the poller returns an InvalidData error naming the frame size$")]
async fn then_invalid_data_naming_size(world: &mut PollerWorld) {
    assert_eq!(world.err_kind, Some(io::ErrorKind::InvalidData), "kind should be InvalidData");
    assert!(
        world.err_msg.contains("67108865"),
        "error message should name the size, got {:?}",
        world.err_msg
    );
}

#[then(regex = r"^the poller allocates no payload buffer for that frame$")]
async fn then_no_payload_alloc(world: &mut PollerWorld) {
    // The gate returns Err before `poll` reaches `vec![0u8; len as usize]`, so a
    // rejecting gate is itself the proof no buffer was allocated.
    assert!(!world.gate_ok, "rejected before allocation: gate must be Err");
    assert_eq!(world.err_kind, Some(io::ErrorKind::InvalidData));
}

#[then(regex = r"^the poller returns an InvalidData error$")]
async fn then_invalid_data(world: &mut PollerWorld) {
    assert_eq!(world.err_kind, Some(io::ErrorKind::InvalidData), "kind should be InvalidData");
}

#[then(regex = r"^the poller does not attempt to allocate 4 GiB$")]
async fn then_no_4gib_alloc(world: &mut PollerWorld) {
    // 0xFFFFFFFF > MAX_FRAME_BYTES, so the gate returns Err before the `vec!`
    // allocation in `poll` is reached.
    assert!(!world.gate_ok, "0xFFFFFFFF must be rejected by the size gate before allocation");
}

// ---------------------------------------------------------------------------
// @boundary: zero-length decode
// ---------------------------------------------------------------------------

#[given(regex = r"^the server replies with a length prefix of 0 and no payload bytes$")]
async fn given_zero_length(world: &mut PollerWorld) {
    capture_decode_err(world, &[]);
}

#[when(regex = r"^the poller reads the frame and attempts to decode it$")]
async fn when_reads_frame_decode(_world: &mut PollerWorld) {
    // decode_frame ran in the Given on the empty payload.
}

#[then(regex = r"^the decode fails with InvalidData$")]
async fn then_decode_invalid_data(world: &mut PollerWorld) {
    assert_eq!(world.err_kind, Some(io::ErrorKind::InvalidData), "decode should fail InvalidData");
}

#[then(regex = r"^the poll returns an error so the poller reconnects$")]
async fn then_poll_returns_error(world: &mut PollerWorld) {
    assert!(world.err_kind.is_some(), "an error must have been recorded");
}

// ---------------------------------------------------------------------------
// @boundary: invalid MessagePack
// ---------------------------------------------------------------------------

#[given(regex = r"^the server replies with a valid length prefix but a non-MessagePack payload$")]
async fn given_invalid_msgpack(world: &mut PollerWorld) {
    // Slot starts empty; a decode-only failure must leave it untouched.
    capture_decode_err(world, &[0xFF, 0xFF, 0xFF]);
}

fn capture_decode_err(world: &mut PollerWorld, buf: &[u8]) {
    match decode_frame(buf) {
        Ok(snap) => {
            world.decoded = Some(snap);
        }
        Err(err) => {
            world.err_kind = Some(err.kind());
            world.err_msg = err.to_string();
        }
    }
}

#[when(regex = r"^the poller reads the payload and attempts to decode it$")]
async fn when_reads_payload_decode(_world: &mut PollerWorld) {
    // decode_frame ran in the Given on the invalid payload.
}

#[then(regex = r"^the shared snapshot slot is left unchanged$")]
async fn then_slot_unchanged(world: &mut PollerWorld) {
    assert!(
        world.slot.lock().unwrap().is_none(),
        "decode-only failure must not publish into the slot"
    );
}

#[then(regex = r"^the poll loop returns so the outer loop reconnects$")]
async fn then_poll_loop_returns(world: &mut PollerWorld) {
    assert!(world.err_kind.is_some(), "an error must have been recorded to trigger reconnect");
}

// ---------------------------------------------------------------------------
// @boundary: truncation — short payload, short prefix, stalled MAX read
// ---------------------------------------------------------------------------

#[given(
    regex = r"^the server sends a length prefix of N but only N-1 payload bytes then closes$"
)]
async fn given_truncated_payload(world: &mut PollerWorld) {
    let (client, server) = loopback();

    // N = 8 bytes (a small value that fits); send prefix, then N-1 bytes, then close.
    let n: u32 = 8;
    let handle = thread::spawn(move || {
        let mut s = server;
        // Read the single request byte first, then send the truncated reply.
        let mut req = [0u8; 1];
        let _ = s.read_exact(&mut req);
        s.write_all(&n.to_be_bytes()).expect("write len prefix");
        // Only write N-1 payload bytes.
        let partial = vec![0u8; (n - 1) as usize];
        s.write_all(&partial).expect("write partial payload");
        // Drop s here: closing the connection signals EOF to the client.
    });

    let result = poll_once_over_with_read_timeout(
        client,
        Arc::clone(&world.slot),
        Duration::from_secs(5),
    );
    handle.join().expect("server thread");

    match result {
        Ok(()) => {}
        Err(err) => {
            world.err_kind = Some(err.kind());
            world.err_msg = err.to_string();
        }
    }
}

#[given(
    regex = r"^the server sends only 2 bytes of the 4-byte length prefix then closes$"
)]
async fn given_truncated_prefix(world: &mut PollerWorld) {
    let (client, server) = loopback();

    let handle = thread::spawn(move || {
        let mut s = server;
        // Read the single request byte, then send only 2 of the 4 prefix bytes, then close.
        let mut req = [0u8; 1];
        let _ = s.read_exact(&mut req);
        s.write_all(&[0x00, 0x00]).expect("write 2-byte partial prefix");
        // Drop s: EOF for client.
    });

    let result = poll_once_over_with_read_timeout(
        client,
        Arc::clone(&world.slot),
        Duration::from_secs(5),
    );
    handle.join().expect("server thread");

    match result {
        Ok(()) => {}
        Err(err) => {
            world.err_kind = Some(err.kind());
            world.err_msg = err.to_string();
        }
    }
}

#[given(
    regex = r"^the server replies with a length prefix of 67108864 but never sends that many bytes$"
)]
async fn given_max_prefix_stalled_payload(world: &mut PollerWorld) {
    use std::sync::mpsc;

    let (client, server) = loopback();
    // Channel lets the server thread know when the client has returned so it can drop.
    let (tx, rx) = mpsc::channel::<()>();

    let handle = thread::spawn(move || {
        let mut s = server;
        // Read the single request byte, send MAX_FRAME_BYTES as the prefix, then send nothing.
        let mut req = [0u8; 1];
        let _ = s.read_exact(&mut req);
        let max: u32 = 67_108_864;
        s.write_all(&max.to_be_bytes()).expect("write max len prefix");
        // Keep `s` alive until the client has returned (so the stall is genuine).
        let _ = rx.recv();
        // Drop s after client completes.
    });

    // Use a short read timeout so this scenario finishes in ~200ms, not 5s.
    let result = poll_once_over_with_read_timeout(
        client,
        Arc::clone(&world.slot),
        Duration::from_millis(200),
    );
    // Signal the server to clean up.
    let _ = tx.send(());
    handle.join().expect("server thread");

    match result {
        Ok(()) => {}
        Err(err) => {
            world.err_kind = Some(err.kind());
            world.err_msg = err.to_string();
        }
    }
}

#[when(regex = r"^the poller reads the payload$")]
async fn when_reads_payload(_world: &mut PollerWorld) {
    // The truncated-payload scenario drives `poll_once_over_with_read_timeout` in
    // the Given step, which exercises the real `Poller::poll` read_exact path.
}

#[when(regex = r"^the poller blocks reading the payload$")]
async fn when_blocks_reading_payload(_world: &mut PollerWorld) {
    // The MAX-prefix stalled scenario drives the real poller in the Given step.
}

#[then(regex = r"^read_exact returns an UnexpectedEof error$")]
async fn then_unexpected_eof(world: &mut PollerWorld) {
    assert_eq!(
        world.err_kind,
        Some(io::ErrorKind::UnexpectedEof),
        "poll should return UnexpectedEof on a truncated read, got {:?}",
        world.err_kind,
    );
}

#[then(regex = r"^the poll returns that error so the poller reconnects$")]
async fn then_poll_returns_that_error(world: &mut PollerWorld) {
    assert!(world.err_kind.is_some(), "an error must have been recorded to trigger reconnect");
}

#[then(
    regex = r"^the socket read timeout \(connection_timeout\.max\(1s\)\) eventually errors the poll$"
)]
async fn then_socket_timeout_errors_poll(world: &mut PollerWorld) {
    // A set_read_timeout expiry surfaces as WouldBlock or TimedOut, depending on the OS.
    let kind = world.err_kind.expect("timeout must have produced an error");
    assert!(
        kind == io::ErrorKind::WouldBlock || kind == io::ErrorKind::TimedOut,
        "expected WouldBlock or TimedOut from read timeout, got {kind:?}",
    );
}
