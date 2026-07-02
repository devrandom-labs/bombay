// --- #61 quarantine (vendored kameo, pre-god-level-bar) -------------------
// This file predates the workspace god-level clippy bar (root Cargo.toml).
// It is held at the prior standard and is cleaned or deleted file-by-file
// under M1/M7. NEW code is NOT exempt — remove this block when the file is
// brought up to the bar or dropped. De-quarantine checklist: issue #61.
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::disallowed_methods,
    clippy::clone_on_ref_ptr,
    clippy::as_conversions,
    clippy::str_to_string,
    clippy::implicit_clone,
    clippy::shadow_reuse,
    clippy::shadow_same,
    clippy::shadow_unrelated,
    clippy::allow_attributes_without_reason,
    reason = "Vendored kameo predating the #61 god-level clippy bar; held at the prior standard, cleaned or deleted file-by-file under M1/M7. New code is not exempt. See #61."
)]
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
    retry_until_some(
        || connect_attempt(addr, connection_timeout, snapshot, connection_state),
        BACKOFF,
        thread::sleep,
    )
}

/// Retry `attempt` until it yields `Some`, sleeping `backoff` between failures via the injected
/// `sleep`. The generic seam under `connect_loop`: production passes the real `connect_attempt`
/// closure + `thread::sleep`; a test passes a scripted attempt sequence + a recording sleep, so
/// the retry-then-backoff control flow is covered without opening a socket or waiting the real
/// 5-second `BACKOFF`. Behaviour is byte-identical to the inline `loop { attempt; sleep }`.
fn retry_until_some<T>(
    mut attempt: impl FnMut() -> Option<T>,
    backoff: Duration,
    mut sleep: impl FnMut(Duration),
) -> T {
    loop {
        if let Some(value) = attempt() {
            return value;
        }
        sleep(backoff);
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
    let err = drive_polls(|| poller.poll(), interval, thread::sleep);
    fail_poll(&mut poller, connection_state, &err);
}

/// Sleep owed after a successful poll to hold the `interval` cadence: the interval minus the work
/// that already `elapsed`, clamped to zero when the poll overran it (poll again immediately).
/// Pure, so the pacing arithmetic is unit-tested at its boundaries (elapsed <, ==, > interval);
/// `checked_sub` never underflows (`Duration` is unsigned).
fn pacing_sleep(interval: Duration, elapsed: Duration) -> Duration {
    interval.checked_sub(elapsed).unwrap_or(Duration::ZERO)
}

/// Drive `poll` until it returns `Err`: on `Ok`, sleep the `pacing_sleep` remainder of the current
/// `interval` (via the injected `sleep`), then poll again; on `Err`, return that error. The generic
/// seam under `poll_loop`: production passes the real `poller.poll()` + `thread::sleep`, so the
/// live loop paces between snapshots; a test passes a scripted poll sequence + a recording sleep,
/// covering the Ok-pacing branch and the Err exit without a real socket or a real wait. The
/// `interval` is re-read each Ok so an in-flight `+`/`-` adjustment takes effect on the next poll.
fn drive_polls(
    mut poll: impl FnMut() -> io::Result<()>,
    interval: &Arc<AtomicU64>,
    mut sleep: impl FnMut(Duration),
) -> io::Error {
    loop {
        let start = Instant::now();
        match poll() {
            Ok(()) => {
                let interval = Duration::from_millis(interval.load(Ordering::Relaxed));
                sleep(pacing_sleep(interval, start.elapsed()));
            }
            Err(err) => return err,
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
/// loop would then reconnect). Delegates to the same `drive_polls` seam as the live `poll_loop`,
/// with a zero interval and a no-op sleep, so it omits the inter-poll pacing sleep (pure cadence,
/// not protocol) while the state writes, error formatting, and disconnect stay byte-identical.
#[cfg(any(test, feature = "testing"))]
pub fn poll_loop_until_error(
    poller: &mut Poller,
    connection_state: &Arc<Mutex<ConnectionState>>,
) -> io::Error {
    let idle = Arc::new(AtomicU64::new(0));
    let err = drive_polls(|| poller.poll(), &idle, |_| {});
    fail_poll(poller, connection_state, &err);
    err
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

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use super::{BACKOFF, drive_polls, pacing_sleep, retry_until_some};

    #[test]
    fn retry_until_some_returns_first_some_and_backs_off_between_failures() {
        // The `connect_loop` shape: two failed connect attempts, then a success. The seam must
        // return the first `Some` and sleep one `BACKOFF` between each failure — never after the
        // success — all with an injected clock, so no test opens a socket or waits real seconds.
        let mut attempts = [None, None, Some(7u32)].into_iter();
        let mut slept: Vec<Duration> = Vec::new();
        let value = retry_until_some(|| attempts.next().unwrap(), BACKOFF, |d| slept.push(d));

        assert_eq!(value, 7, "returns the first Some payload");
        assert_eq!(
            slept,
            vec![BACKOFF, BACKOFF],
            "one BACKOFF between each of the two failures, none after the success",
        );
    }

    #[test]
    fn pacing_sleep_is_the_interval_remainder_and_clamps_to_zero() {
        // Work left time to spare -> sleep the remainder.
        assert_eq!(
            pacing_sleep(Duration::from_millis(1000), Duration::from_millis(200)),
            Duration::from_millis(800),
        );
        // Work took exactly the interval -> no sleep owed.
        assert_eq!(
            pacing_sleep(Duration::from_millis(1000), Duration::from_millis(1000)),
            Duration::ZERO,
        );
        // Work overran the interval -> clamp to zero (poll again at once); must never underflow.
        assert_eq!(
            pacing_sleep(Duration::from_millis(1000), Duration::from_millis(1500)),
            Duration::ZERO,
        );
    }

    #[test]
    fn drive_polls_paces_between_ok_polls_then_returns_the_first_error() {
        // The `poll_loop` shape: two successful polls, then the connection breaks. The seam must
        // pace once after each Ok (never after the Err) and return the first error for the outer
        // loop to reconnect on — driven by a scripted poll + a counting sleep, no real waiting.
        let mut results = [
            Ok(()),
            Ok(()),
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "server gone")),
        ]
        .into_iter();
        let interval = Arc::new(AtomicU64::new(1000));
        let mut sleeps = 0u32;
        let err = drive_polls(|| results.next().unwrap(), &interval, |_| sleeps += 1);

        assert_eq!(
            sleeps, 2,
            "paces once after each of the two successful polls, not after the error",
        );
        assert_eq!(
            err.kind(),
            io::ErrorKind::BrokenPipe,
            "returns the first poll error",
        );
    }
}
