use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use bombay::console::wire::{Message, Snapshot};

use crate::ConnectionState;

/// Caps a snapshot frame so a misbehaving or wrong-protocol peer can't make us allocate
/// unbounded memory.
pub const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

/// Fixed backoff between failed connect attempts. Not exponential, not jittered — a flat
/// 5-second wait before `connect_loop` retries.
pub const BACKOFF: Duration = Duration::from_secs(5);

/// The frame-size gate: accept iff `len <= MAX_FRAME_BYTES`, else InvalidData. Mirrors the
/// inline check at the original poll() (the gate is `len > MAX_FRAME_BYTES`).
pub fn check_frame_len(len: u32) -> io::Result<()> {
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot frame too large ({len} bytes)"),
        ));
    }
    Ok(())
}

/// Decode a MessagePack payload into a Snapshot, mapping any rmp error to InvalidData.
pub fn decode_frame(buf: &[u8]) -> io::Result<Snapshot> {
    let Message::Snapshot(snapshot) = rmp_serde::from_slice(buf)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok(snapshot)
}

pub fn spawn_poller(
    addr: SocketAddr,
    interval: Arc<AtomicU64>,
    connection_timeout: Duration,
    snapshot: Arc<Mutex<Option<Snapshot>>>,
    connection_state: Arc<Mutex<ConnectionState>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        loop {
            let poller = connect_loop(&addr, connection_timeout, &snapshot, &connection_state);
            poll_loop(poller, &interval, &connection_state);
        }
    })
}

fn connect_loop(
    addr: &SocketAddr,
    connection_timeout: Duration,
    snapshot: &Arc<Mutex<Option<Snapshot>>>,
    connection_state: &Arc<Mutex<ConnectionState>>,
) -> Poller {
    loop {
        if let Some(poller) = connect_attempt(addr, connection_timeout, snapshot, connection_state)
        {
            return poller;
        }
        thread::sleep(BACKOFF);
    }
}

/// One connect attempt: marks `Connecting`, then on success marks `Connected` and returns the
/// `Poller`, or on failure marks `Disconnected { error, since }` and returns `None`. The backoff
/// sleep and retry loop stay in `connect_loop`, so this is the single, testable iteration of the
/// connect cycle (state writes and error formatting are byte-identical to the inline loop body).
pub fn connect_attempt(
    addr: &SocketAddr,
    connection_timeout: Duration,
    snapshot: &Arc<Mutex<Option<Snapshot>>>,
    connection_state: &Arc<Mutex<ConnectionState>>,
) -> Option<Poller> {
    *connection_state.lock().unwrap() = ConnectionState::Connecting;
    match Poller::connect(addr, connection_timeout, Arc::clone(snapshot)) {
        Ok(poller) => {
            *connection_state.lock().unwrap() = ConnectionState::Connected;
            Some(poller)
        }
        Err(err) => {
            *connection_state.lock().unwrap() = ConnectionState::Disconnected {
                error: format!("{err}"),
                since: Instant::now(),
            };
            None
        }
    }
}

fn poll_loop(
    mut poller: Poller,
    interval: &Arc<AtomicU64>,
    connection_state: &Arc<Mutex<ConnectionState>>,
) {
    loop {
        let start = Instant::now();
        match poller.poll() {
            Ok(()) => {
                let interval = Duration::from_millis(interval.load(Ordering::Relaxed));
                let sleep_duration = interval.saturating_sub(start.elapsed());
                thread::sleep(sleep_duration);
            }
            Err(err) => {
                fail_poll(&mut poller, connection_state, &err);
                return;
            }
        }
    }
}

/// The shared "a poll failed" reaction: record `Disconnected { error, since }`, shut the socket
/// down, and (in the real loop) drop the poller. Used by both `poll_loop` and the bounded,
/// testable `poll_loop_until_error`, so the disconnect behaviour stays identical between them.
fn fail_poll(poller: &mut Poller, connection_state: &Arc<Mutex<ConnectionState>>, err: &io::Error) {
    *connection_state.lock().unwrap() = ConnectionState::Disconnected {
        error: format!("{err}"),
        since: Instant::now(),
    };
    let _ = poller.disconnect();
}

/// Bounded, testable twin of `poll_loop`'s drive: poll until the FIRST error, then mark
/// `Disconnected { error, since }`, disconnect, and RETURN the error (the outer `spawn_poller`
/// loop would then reconnect). Behaviour matches `poll_loop` EXCEPT it omits the inter-poll
/// `interval` sleep between successful polls (the sleep is pure pacing, not protocol), so tests
/// don't have to wait on it; the state writes, error formatting, and disconnect are identical.
#[cfg(any(test, feature = "testing"))]
pub fn poll_loop_until_error(
    poller: &mut Poller,
    connection_state: &Arc<Mutex<ConnectionState>>,
) -> io::Error {
    loop {
        if let Err(err) = poller.poll() {
            fail_poll(poller, connection_state, &err);
            return err;
        }
    }
}

/// The console's client-side TCP poller. Opaque: fields and the `connect`/`poll`/`disconnect`
/// methods are private, so a normal build exposes no way to build or drive one — it is only
/// produced via `connect_attempt` and driven by the `#[cfg]`-gated test hooks. It is `pub` only
/// so those public signatures (`connect_attempt -> Option<Poller>`, `poll_loop_until_error(&mut
/// Poller, …)`) type-check without leaking the internals.
pub struct Poller {
    stream: TcpStream,
    snapshot: Arc<Mutex<Option<Snapshot>>>,
}

/// Test-only hook: build a real `Poller` over a caller-supplied stream + slot and run exactly
/// one genuine `poll()`. Exercises the production request/reply/framing/publish path without
/// exposing the private `Poller`/`connect`/`poll` API in normal builds (CLAUDE.md rule 4).
#[cfg(any(test, feature = "testing"))]
pub fn poll_once_over(stream: TcpStream, slot: Arc<Mutex<Option<Snapshot>>>) -> io::Result<()> {
    let mut poller = Poller {
        stream,
        snapshot: slot,
    };
    poller.poll()
}

/// Test-only hook: like `poll_once_over` but with a caller-specified read timeout so
/// boundary tests that expect a stalled read can complete quickly instead of waiting for
/// the 5-second default (CLAUDE.md rule 4).
#[cfg(any(test, feature = "testing"))]
pub fn poll_once_over_with_read_timeout(
    stream: TcpStream,
    slot: Arc<Mutex<Option<Snapshot>>>,
    read_timeout: Duration,
) -> io::Result<()> {
    let clone = stream.try_clone()?;
    clone.set_read_timeout(Some(read_timeout))?;
    let mut poller = Poller {
        stream: clone,
        snapshot: slot,
    };
    poller.poll()
}

impl Poller {
    fn connect(
        addr: &SocketAddr,
        connection_timeout: Duration,
        snapshot: Arc<Mutex<Option<Snapshot>>>,
    ) -> io::Result<Self> {
        let stream = TcpStream::connect_timeout(addr, connection_timeout)?;
        stream.set_read_timeout(Some(connection_timeout.max(Duration::from_secs(1))))?;
        stream.set_write_timeout(Some(connection_timeout.max(Duration::from_secs(1))))?;
        Ok(Poller { stream, snapshot })
    }

    fn disconnect(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Both)
    }

    fn poll(&mut self) -> io::Result<()> {
        self.stream.write_all(&[0])?;

        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len)?;
        let len = u32::from_be_bytes(len);
        check_frame_len(len)?;

        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf)?;

        *self.snapshot.lock().unwrap() = Some(decode_frame(&buf)?);

        Ok(())
    }
}
